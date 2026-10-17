# OpenSpec Agent Orchestrator MVP

一个可以直接运行的 OpenSpec 驱动 Agent 编排器 MVP。核心只有 Python 标准库 + SQLite，不依赖 Redis/FastAPI。

## 明天直接用

```bash
git clone https://github.com/jikangzy/Python.git
cd Python
git switch feat/openspec-agent-orchestrator-mvp
cd openspec-agent-orchestrator

python -m unittest discover -s tests -v
python orchestrator.py doctor --workspace examples/demo --change demo
python orchestrator.py run --workspace examples/demo --change demo --adapter mock --timeout 10
```

## 跑你自己的 OpenSpec 项目

项目里需要有：

```text
openspec/changes/<change-id>/tasks.md
```

然后：

```bash
python orchestrator.py doctor --workspace /absolute/path/to/project --change your-change-id

python orchestrator.py run \
  --workspace /absolute/path/to/project \
  --change your-change-id \
  --adapter codex \
  --timeout 1800
```

Codex adapter 使用：

```bash
codex exec --json --full-auto -
```

它会把 JSONL 事件持续写入 SQLite，而不是只看到一个长期 `running`。每个 task 都有硬超时；超时后会 terminate/kill 并落成 `timed_out`。

## 本地状态

默认数据库：

```text
<workspace>/.agent-orchestrator/state.sqlite3
```

状态：

```text
job:  queued -> running -> completed | failed | timed_out
task: queued -> running -> completed | failed | timed_out
OpenSpec 已勾选任务 -> skipped
```

## 看运行状态

```bash
python orchestrator.py serve --workspace /path/to/project --port 8787
```

打开：

```text
http://127.0.0.1:8787
```

CLI：

```bash
python orchestrator.py status <job-id> --workspace /path/to/project
python orchestrator.py events <job-id> --workspace /path/to/project
```

## 环境变量

```bash
export CODEX_CMD="codex"
export CODEX_EXTRA_ARGS="--model gpt-5.3-codex"
export AGENT_ORCHESTRATOR_DB="$HOME/.local/state/agent-orchestrator.sqlite3"
```

注意：`--full-auto` 允许 Codex 修改文件和执行命令，建议只在你允许它修改的 repo/worktree 中运行。

## 下一步

这版先保证“能跑 + 能观察 + 不会无限 running”。后续适合继续加：

- git worktree 隔离
- DAG/task dependency
- reviewer + changes_requested + retry
- lease/heartbeat/resume
- codex app-server persistent session
- Claude / Gemini adapter
