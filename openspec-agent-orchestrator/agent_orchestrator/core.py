from __future__ import annotations
import json,re,sqlite3,subprocess,threading,uuid
from dataclasses import dataclass
from datetime import datetime,timezone
from pathlib import Path
from typing import Any
TASK_RE = re.compile(r"^\s*[-*]\s*\[(?P<mark>[ xX])\]\s*(?P<title>.+?)\s*$")
FINAL_TASK_STATES = {"completed", "skipped", "aborted"}

def now() -> str:
    return datetime.now(timezone.utc).isoformat()

def clip(value: str, limit: int = 40_000) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + f"\n...[truncated {len(value) - limit} chars]"

def run_cmd(args: list[str], cwd: Path, timeout: int | None = None, check: bool = False, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(args, cwd=cwd, text=True, capture_output=True, timeout=timeout, env=env)
    if check and proc.returncode:
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(args)}\n{proc.stderr.strip()}")
    return proc

@dataclass(frozen=True)
class Task:
    n: int
    title: str
    checked: bool
    line: int
    @property
    def key(self) -> str:
        return f"T{self.n:03d}"

@dataclass
class AgentResult:
    ok: bool
    summary: str
    exit_code: int = 0
    timed_out: bool = False
    data: dict[str, Any] | None = None

@dataclass
class VerificationResult:
    verdict: str
    summary: str
    required_fixes: list[str]
    checks: list[dict[str, Any]]
    needs_brain: bool = False
    brain_reason: str = ""
    raw: dict[str, Any] | None = None
    def as_dict(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict, "summary": self.summary,
            "required_fixes": self.required_fixes, "checks": self.checks,
            "needs_brain": self.needs_brain, "brain_reason": self.brain_reason,
            "raw": self.raw,
        }

def tasks_file(workspace: Path, change: str) -> Path:
    for path in (
        workspace / "openspec" / "changes" / change / "tasks.md",
        workspace / "open-spec" / "changes" / change / "tasks.md",
    ):
        if path.is_file():
            return path
    raise FileNotFoundError(f"tasks.md not found for change {change}")

def change_dir(workspace: Path, change: str) -> Path:
    return tasks_file(workspace, change).parent

def parse_tasks(path: Path) -> list[Task]:
    out: list[Task] = []; n = 0
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        match = TASK_RE.match(raw)
        if not match: continue
        n += 1
        out.append(Task(n, match.group("title").strip(), match.group("mark").lower() == "x", line_no))
    return out

def read_change_context(workspace: Path, change: str, limit: int = 45_000) -> str:
    root = change_dir(workspace, change); chunks: list[str] = []
    for path in sorted(root.rglob("*.md")):
        rel = path.relative_to(workspace)
        try: text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError: continue
        chunks.append(f"\n--- {rel} ---\n{text}")
        if sum(len(x) for x in chunks) >= limit: break
    return clip("".join(chunks), limit)

def git_snapshot(workspace: Path) -> dict[str, str]:
    if run_cmd(["git", "rev-parse", "--is-inside-work-tree"], workspace).returncode != 0:
        return {"status": "not-a-git-repository", "diffStat": "", "diff": ""}
    status = run_cmd(["git", "status", "--short"], workspace).stdout
    stat = run_cmd(["git", "diff", "--stat", "HEAD"], workspace).stdout
    diff = run_cmd(["git", "diff", "HEAD"], workspace).stdout
    return {"status": clip(status, 8_000), "diffStat": clip(stat, 8_000), "diff": clip(diff, 40_000)}

class DB:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True); self.path = path
        self.lock = threading.RLock(); self.c = sqlite3.connect(path, check_same_thread=False); self.c.row_factory = sqlite3.Row
        with self.lock, self.c:
            self.c.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS jobs(
                id TEXT PRIMARY KEY, change_id TEXT, workspace TEXT, tasks_file TEXT, adapter TEXT,
                status TEXT, created_at TEXT, started_at TEXT, finished_at TEXT, error TEXT
            );
            CREATE TABLE IF NOT EXISTS tasks(
                id INTEGER PRIMARY KEY, job_id TEXT, task_key TEXT, ordinal INTEGER, title TEXT,
                source_line INTEGER, status TEXT, started_at TEXT, finished_at TEXT, exit_code INTEGER, error TEXT
            );
            CREATE TABLE IF NOT EXISTS events(
                id INTEGER PRIMARY KEY, job_id TEXT, task_key TEXT, type TEXT, payload TEXT, created_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id,id);
            """)
        self._ensure_column("jobs", "verification_adapter", "TEXT")
        self._ensure_column("jobs", "brain_mode", "TEXT")
        self._ensure_column("jobs", "config_json", "TEXT")
        self._ensure_column("tasks", "attempt", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("tasks", "version", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("tasks", "implementation_json", "TEXT")
        self._ensure_column("tasks", "verification_json", "TEXT")
        self._ensure_column("tasks", "rework_json", "TEXT")
        self._ensure_column("tasks", "brain_request_id", "TEXT")

    def _ensure_column(self, table: str, name: str, definition: str) -> None:
        with self.lock:
            cols = {row[1] for row in self.c.execute(f"PRAGMA table_info({table})")}
            if name not in cols:
                with self.c: self.c.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")

    def close(self) -> None: self.c.close()

    def event(self, job_id: str, task_key: str | None, event_type: str, payload: dict[str, Any]) -> None:
        with self.lock, self.c:
            self.c.execute("INSERT INTO events(job_id,task_key,type,payload,created_at) VALUES(?,?,?,?,?)",
                           (job_id, task_key, event_type, json.dumps(payload, ensure_ascii=False), now()))

    def create_job(self, change: str, workspace: Path, tf: Path, implementation: str, verification: str, brain_mode: str, config: dict[str, Any]) -> str:
        job_id = "job-" + uuid.uuid4().hex[:12]
        with self.lock, self.c:
            self.c.execute("""INSERT INTO jobs(
                id,change_id,workspace,tasks_file,adapter,status,created_at,started_at,finished_at,error,
                verification_adapter,brain_mode,config_json
            ) VALUES(?,?,?,?,?,'queued',?,?,?,?,?,?,?)""",
            (job_id, change, str(workspace), str(tf), implementation, now(), None, None, None, verification, brain_mode, json.dumps(config)))
        self.event(job_id, None, "job.created", {"change_id": change, "implementation": implementation, "verification": verification, "brain": brain_mode})
        return job_id

    def add_task(self, job_id: str, task: Task, status: str) -> None:
        with self.lock, self.c:
            self.c.execute("INSERT INTO tasks(job_id,task_key,ordinal,title,source_line,status) VALUES(?,?,?,?,?,?)",
                           (job_id, task.key, task.n, task.title, task.line, status))

    def set_job_status(self, job_id: str, status: str, error: str | None = None, final: bool = False) -> None:
        with self.lock, self.c:
            if status == "running":
                self.c.execute("UPDATE jobs SET status=?, started_at=COALESCE(started_at,?) WHERE id=?", (status, now(), job_id))
            elif final:
                self.c.execute("UPDATE jobs SET status=?,finished_at=?,error=? WHERE id=?", (status, now(), error, job_id))
            else:
                self.c.execute("UPDATE jobs SET status=?,error=? WHERE id=?", (status, error, job_id))
        self.event(job_id, None, "job." + status, {"error": error} if error else {})

    def transition_task(self, job_id: str, task_key: str, status: str, **updates: Any) -> int:
        allowed = {"attempt","implementation_json","verification_json","rework_json","brain_request_id","exit_code","error","started_at","finished_at"}
        bad = set(updates) - allowed
        if bad: raise ValueError(f"unsupported task updates: {sorted(bad)}")
        fields = ["status=?", "version=version+1"]; values: list[Any] = [status]
        for key, value in updates.items(): fields.append(f"{key}=?"); values.append(value)
        if status in {"implementing","verifying"} and "started_at" not in updates:
            fields.append("started_at=COALESCE(started_at,?)"); values.append(now())
        if status in FINAL_TASK_STATES | {"failed","timed_out"} and "finished_at" not in updates:
            fields.append("finished_at=?"); values.append(now())
        values += [job_id, task_key]
        with self.lock, self.c:
            self.c.execute(f"UPDATE tasks SET {', '.join(fields)} WHERE job_id=? AND task_key=?", values)
            row = self.c.execute("SELECT version FROM tasks WHERE job_id=? AND task_key=?", (job_id, task_key)).fetchone()
        version = int(row[0]); self.event(job_id, task_key, "task.state_changed", {"state": status, "version": version}); return version

    def update_task_fields(self, job_id: str, task_key: str, **updates: Any) -> None:
        allowed = {"implementation_json","verification_json","rework_json","brain_request_id","exit_code","error","attempt"}
        bad = set(updates) - allowed
        if bad: raise ValueError(f"unsupported task updates: {sorted(bad)}")
        if not updates: return
        values = list(updates.values()) + [job_id, task_key]
        with self.lock, self.c:
            self.c.execute(f"UPDATE tasks SET {', '.join(f'{k}=?' for k in updates)} WHERE job_id=? AND task_key=?", values)

    def task(self, job_id: str, task_key: str) -> dict[str, Any] | None:
        with self.lock: row = self.c.execute("SELECT * FROM tasks WHERE job_id=? AND task_key=?", (job_id, task_key)).fetchone()
        return dict(row) if row else None

    def job(self, job_id: str) -> dict[str, Any] | None:
        with self.lock:
            row = self.c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            tasks = self.c.execute("SELECT * FROM tasks WHERE job_id=? ORDER BY ordinal", (job_id,)).fetchall()
        if not row: return None
        data = dict(row); data["tasks"] = [dict(x) for x in tasks]; return data

    def jobs(self) -> list[dict[str, Any]]:
        with self.lock: rows = self.c.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT 50").fetchall()
        return [dict(x) for x in rows]

    def events(self, job_id: str, after: int = 0) -> list[dict[str, Any]]:
        with self.lock: rows = self.c.execute("SELECT * FROM events WHERE job_id=? AND id>? ORDER BY id LIMIT 500", (job_id, after)).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            try: item["payload"] = json.loads(item["payload"])
            except Exception: pass
            out.append(item)
        return out
