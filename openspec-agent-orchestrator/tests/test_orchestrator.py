from pathlib import Path
from tempfile import TemporaryDirectory
import os, unittest
from orchestrator import parse_tasks,DB,Mock,run_change,Codex,Task

class Tests(unittest.TestCase):
    def test_parse_and_mock(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp); c=root/"openspec"/"changes"/"demo"; c.mkdir(parents=True)
            f=c/"tasks.md"; f.write_text("- [x] done\n- [ ] first\n- [ ] second\n",encoding="utf-8")
            self.assertEqual([t.key for t in parse_tasks(f)],["T001","T002","T003"])
            db=DB(root/".agent-orchestrator"/"state.sqlite3")
            try:
                j=run_change(db,root,"demo",Mock(),2); x=db.job(j)
                self.assertEqual(x["status"],"completed")
                self.assertEqual([t["status"] for t in x["tasks"]],["skipped","completed","completed"])
            finally: db.close()

    def test_codex_jsonl(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp); fake=root/"fake.py"
            fake.write_text("import json,sys\n_=sys.stdin.read()\nprint(json.dumps({'type':'item.completed','item':{'text':'done'}}),flush=True)\n",encoding="utf-8")
            old=os.environ.get("CODEX_CMD");os.environ["CODEX_CMD"]=f"python {fake}"
            try:
                seen=[];code,msg,to=Codex().run(workspace=root,change="demo",tf=root/"tasks.md",task=Task(1,"x",False,1),timeout=3,emit=lambda t,p:seen.append((t,p)))
                self.assertEqual((code,msg,to),(0,"done",False))
                self.assertTrue(any(t=="item.completed" for t,_ in seen))
            finally:
                if old is None:os.environ.pop("CODEX_CMD",None)
                else:os.environ["CODEX_CMD"]=old

if __name__=="__main__": unittest.main()
