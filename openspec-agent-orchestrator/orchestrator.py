#!/usr/bin/env python3
from __future__ import annotations
import argparse,json,os,queue,re,shlex,shutil,sqlite3,subprocess,sys,threading,time,traceback,uuid
from dataclasses import dataclass
from datetime import datetime,timezone
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse,parse_qs

TASK_RE=re.compile(r"^\s*[-*]\s*\[(?P<mark>[ xX])\]\s*(?P<title>.+?)\s*$")
def now(): return datetime.now(timezone.utc).isoformat()

@dataclass(frozen=True)
class Task:
    n:int; title:str; checked:bool; line:int
    @property
    def key(self): return f"T{self.n:03d}"

def tasks_file(workspace,change):
    for p in (workspace/"openspec"/"changes"/change/"tasks.md",workspace/"open-spec"/"changes"/change/"tasks.md"):
        if p.is_file(): return p
    raise FileNotFoundError(f"tasks.md not found for change {change}")

def parse_tasks(path):
    out=[]; n=0
    for i,raw in enumerate(path.read_text(encoding="utf-8").splitlines(),1):
        m=TASK_RE.match(raw)
        if not m: continue
        n+=1; out.append(Task(n,m.group("title").strip(),m.group("mark").lower()=="x",i))
    return out

class DB:
    def __init__(self,path):
        path.parent.mkdir(parents=True,exist_ok=True); self.path=path
        self.lock=threading.RLock(); self.c=sqlite3.connect(path,check_same_thread=False); self.c.row_factory=sqlite3.Row
        with self.lock,self.c:self.c.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY,change_id TEXT,workspace TEXT,tasks_file TEXT,adapter TEXT,status TEXT,created_at TEXT,started_at TEXT,finished_at TEXT,error TEXT);
        CREATE TABLE IF NOT EXISTS tasks(id INTEGER PRIMARY KEY,job_id TEXT,task_key TEXT,ordinal INTEGER,title TEXT,source_line INTEGER,status TEXT,started_at TEXT,finished_at TEXT,exit_code INTEGER,error TEXT);
        CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY,job_id TEXT,task_key TEXT,type TEXT,payload TEXT,created_at TEXT);
        CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id,id);
        """)
    def close(self): self.c.close()
    def event(self,j,k,t,p):
        with self.lock,self.c:self.c.execute("INSERT INTO events(job_id,task_key,type,payload,created_at) VALUES(?,?,?,?,?)",(j,k,t,json.dumps(p,ensure_ascii=False),now()))
    def create_job(self,change,workspace,tf,adapter):
        j="job-"+uuid.uuid4().hex[:12]
        with self.lock,self.c:self.c.execute("INSERT INTO jobs VALUES(?,?,?,?,?,'queued',?,?,?,?)",(j,change,str(workspace),str(tf),adapter,now(),None,None,None))
        self.event(j,None,"job.created",{"change_id":change,"adapter":adapter}); return j
    def add_task(self,j,t,status):
        with self.lock,self.c:self.c.execute("INSERT INTO tasks(job_id,task_key,ordinal,title,source_line,status) VALUES(?,?,?,?,?,?)",(j,t.key,t.n,t.title,t.line,status))
    def start_job(self,j):
        with self.lock,self.c:self.c.execute("UPDATE jobs SET status='running',started_at=? WHERE id=?",(now(),j))
        self.event(j,None,"job.started",{})
    def finish_job(self,j,status,error=None):
        with self.lock,self.c:self.c.execute("UPDATE jobs SET status=?,finished_at=?,error=? WHERE id=?",(status,now(),error,j))
        self.event(j,None,"job."+status,{"error":error} if error else {})
    def start_task(self,j,k):
        with self.lock,self.c:self.c.execute("UPDATE tasks SET status='running',started_at=? WHERE job_id=? AND task_key=?",(now(),j,k))
        self.event(j,k,"task.started",{})
    def finish_task(self,j,k,status,code=None,error=None):
        with self.lock,self.c:self.c.execute("UPDATE tasks SET status=?,finished_at=?,exit_code=?,error=? WHERE job_id=? AND task_key=?",(status,now(),code,error,j,k))
        self.event(j,k,"task."+status,{"exit_code":code,"error":error})
    def job(self,j):
        with self.lock:
            r=self.c.execute("SELECT * FROM jobs WHERE id=?",(j,)).fetchone()
            ts=self.c.execute("SELECT task_key,ordinal,title,source_line,status,started_at,finished_at,exit_code,error FROM tasks WHERE job_id=? ORDER BY ordinal",(j,)).fetchall()
        if not r:return None
        d=dict(r); d["tasks"]=[dict(x) for x in ts]; return d
    def jobs(self):
        with self.lock:r=self.c.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT 50").fetchall()
        return [dict(x) for x in r]
    def events(self,j,after=0):
        with self.lock:r=self.c.execute("SELECT * FROM events WHERE job_id=? AND id>? ORDER BY id LIMIT 500",(j,after)).fetchall()
        out=[]
        for x in r:
            d=dict(x)
            try:d["payload"]=json.loads(d["payload"])
            except:pass
            out.append(d)
        return out

class Mock:
    name="mock"
    def run(self,**kw):
        kw["emit"]("agent.mock.output",{"message":"completed "+kw["task"].key}); time.sleep(.05); return 0,"mock completed",False

class Codex:
    name="codex"
    def run(self,workspace,change,tf,task,timeout,emit):
        base=shlex.split(os.getenv("CODEX_CMD","codex"))
        if not base or shutil.which(base[0]) is None: raise RuntimeError("Codex CLI not found. Install/authenticate codex or set CODEX_CMD.")
        cmd=[*base,"exec","--json","--full-auto",*shlex.split(os.getenv("CODEX_EXTRA_ARGS","")),"-"]
        prompt=f"""You are an implementation agent for an OpenSpec repository.
Change: {change}
Tasks file: {tf}
Current task: {task.key} - {task.title}
Complete ONLY this task. Inspect the repo, make the smallest coherent change, run relevant checks, and leave the workspace usable. Do not edit the checkbox in tasks.md. If blocked, explain why.
"""
        emit("agent.codex.spawn",{"command":cmd})
        p=subprocess.Popen(cmd,cwd=workspace,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1)
        assert p.stdin and p.stdout and p.stderr
        p.stdin.write(prompt);p.stdin.close();q=queue.Queue()
        def read(s,ch):
            for line in iter(s.readline,""):q.put((ch,line.rstrip("\n")))
            q.put((ch+"_eof",""))
        threading.Thread(target=read,args=(p.stdout,"out"),daemon=True).start();threading.Thread(target=read,args=(p.stderr,"err"),daemon=True).start()
        deadline=time.monotonic()+timeout; oe=ee=False; last=""
        while True:
            if time.monotonic()>=deadline and p.poll() is None:
                emit("agent.codex.timeout",{"timeout_seconds":timeout});p.terminate()
                try:p.wait(5)
                except subprocess.TimeoutExpired:p.kill();p.wait(5)
                return 124,"Codex timed out",True
            try:ch,line=q.get(timeout=.25)
            except queue.Empty:
                if p.poll() is not None and oe and ee:break
                continue
            if ch=="out_eof":oe=True;continue
            if ch=="err_eof":ee=True;continue
            if not line:continue
            if ch=="err":emit("agent.codex.stderr",{"line":line});continue
            try:e=json.loads(line);typ=str(e.get("type","agent.codex.event"));emit(typ,e)
            except json.JSONDecodeError:emit("agent.codex.stdout",{"line":line});last=line;continue
            item=e.get("item") or e
            if typ in ("item.completed","turn.completed") and isinstance(item,dict) and isinstance(item.get("text"),str):last=item["text"].strip() or last
        code=p.wait();return code,last or f"Codex exited with {code}",False

def adapter(name): return Mock() if name=="mock" else Codex()

def run_change(db,workspace,change,a,timeout):
    workspace=workspace.resolve();tf=tasks_file(workspace,change);ts=parse_tasks(tf);j=db.create_job(change,workspace,tf,a.name)
    for t in ts:db.add_task(j,t,"skipped" if t.checked else "queued")
    pending=[t for t in ts if not t.checked]
    if not pending:db.finish_job(j,"completed");return j
    db.start_job(j)
    for t in pending:
        db.start_task(j,t.key);emit=lambda typ,payload,k=t.key:db.event(j,k,typ,payload)
        try:code,msg,to=a.run(workspace=workspace,change=change,tf=tf,task=t,timeout=timeout,emit=emit)
        except Exception as e:
            err=f"{type(e).__name__}: {e}";db.event(j,t.key,"agent.exception",{"error":err,"traceback":traceback.format_exc()});db.finish_task(j,t.key,"failed",None,err);db.finish_job(j,"failed",err);return j
        if to:db.finish_task(j,t.key,"timed_out",code,msg);db.finish_job(j,"timed_out",msg);return j
        if code:db.finish_task(j,t.key,"failed",code,msg);db.finish_job(j,"failed",msg);return j
        db.finish_task(j,t.key,"completed",code);db.event(j,t.key,"task.summary",{"summary":msg})
    db.finish_job(j,"completed");return j

HTML="""<!doctype html><meta charset=utf-8><title>OpenSpec Orchestrator</title><style>body{font-family:monospace;max-width:1100px;margin:30px auto;background:#111;color:#eee}a{color:#8bd}td,th{padding:8px;border-bottom:1px solid #333}table{width:100%}</style><h1>OpenSpec Agent Orchestrator</h1><table><tbody id=j></tbody></table><pre id=d></pre><script>async function r(){let x=await fetch('/api/jobs').then(r=>r.json());j.innerHTML='';for(let a of x){let tr=document.createElement('tr');tr.innerHTML='<td><a href=#>'+a.id+'</a></td><td>'+a.change_id+'</td><td>'+a.status+'</td>';tr.onclick=async()=>d.textContent=JSON.stringify(await fetch('/api/jobs/'+a.id).then(r=>r.json()),null,2);j.appendChild(tr)}}r();setInterval(r,2000)</script>"""

def serve(db,host,port):
    class H(BaseHTTPRequestHandler):
        def out(self,x,code=200):
            b=json.dumps(x,ensure_ascii=False,indent=2).encode();self.send_response(code);self.send_header("Content-Type","application/json");self.send_header("Content-Length",str(len(b)));self.end_headers();self.wfile.write(b)
        def do_GET(self):
            u=urlparse(self.path)
            if u.path=="/":b=HTML.encode();self.send_response(200);self.send_header("Content-Length",str(len(b)));self.end_headers();self.wfile.write(b);return
            if u.path=="/health":return self.out({"status":"ok","db":str(db.path)})
            if u.path=="/api/jobs":return self.out(db.jobs())
            if u.path.startswith("/api/jobs/"):
                jid=u.path.split("/")[-1];x=db.job(jid)
                if not x:return self.out({"error":"not found"},404)
                x["events"]=db.events(jid);return self.out(x)
            if u.path=="/api/events":
                q=parse_qs(u.query);return self.out(db.events(q.get("job_id",[""])[0],int(q.get("after",["0"])[0])))
            self.out({"error":"not found"},404)
        def log_message(self,*a):pass
    print(f"dashboard: http://{host}:{port}");ThreadingHTTPServer((host,port),H).serve_forever()

def main():
    ap=argparse.ArgumentParser();sp=ap.add_subparsers(dest="cmd",required=True)
    r=sp.add_parser("run");r.add_argument("--workspace",type=Path,default=Path.cwd());r.add_argument("--change",required=True);r.add_argument("--adapter",choices=["mock","codex"],default="mock");r.add_argument("--timeout",type=int,default=1800)
    s=sp.add_parser("status");s.add_argument("job");s.add_argument("--workspace",type=Path,default=Path.cwd())
    e=sp.add_parser("events");e.add_argument("job");e.add_argument("--workspace",type=Path,default=Path.cwd());e.add_argument("--after",type=int,default=0)
    v=sp.add_parser("serve");v.add_argument("--workspace",type=Path,default=Path.cwd());v.add_argument("--host",default="127.0.0.1");v.add_argument("--port",type=int,default=8787)
    d=sp.add_parser("doctor");d.add_argument("--workspace",type=Path,default=Path.cwd());d.add_argument("--change")
    a=ap.parse_args();w=a.workspace.resolve()
    if a.cmd=="doctor":
        x={"python":sys.version.split()[0],"workspace":str(w),"codex":shutil.which("codex")}
        if a.change:
            try:tf=tasks_file(w,a.change);x.update(tasks_file=str(tf),tasks=len(parse_tasks(tf)))
            except Exception as ex:x["tasks_error"]=str(ex)
        print(json.dumps(x,indent=2));return 0
    db=DB(Path(os.getenv("AGENT_ORCHESTRATOR_DB",w/".agent-orchestrator"/"state.sqlite3")))
    try:
        if a.cmd=="run":
            j=run_change(db,w,a.change,adapter(a.adapter),a.timeout);x=db.job(j);print(json.dumps(x,ensure_ascii=False,indent=2));return 0 if x and x["status"]=="completed" else 1
        if a.cmd=="status":print(json.dumps(db.job(a.job),ensure_ascii=False,indent=2));return 0
        if a.cmd=="events":print(json.dumps(db.events(a.job,a.after),ensure_ascii=False,indent=2));return 0
        if a.cmd=="serve":serve(db,a.host,a.port);return 0
    finally:
        if a.cmd!="serve":db.close()
if __name__=="__main__":raise SystemExit(main())
