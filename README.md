# durable-workflow

Crash-safe, multi-day business workflows with human approval in Slack. Built on
[LangGraph](https://github.com/langchain-ai/langgraph) with SQLite checkpoints,
exponential retries, and `interrupt()` for human-in-the-loop.

**Why:** chat-style agent loops die the moment the process does. Real workflows
(vendor payments, refunds, access requests) wait days for a human and must pick
up exactly where they left off after a deploy or a crash.

## What it does

```
start ─▶ validate ─▶ request_approval ─▶ pay ─▶ notify
            │              │  (parks here, possibly for days)
         retries        Slack buttons / CLI resume
```

- **Checkpoint per node.** Every node's output is written to SQLite before the next
  node runs. Kill the process anywhere; `dw retry <id>` continues from the last
  completed node without redoing finished work.
- **Exponential backoff.** `validate`, `pay`, `notify` retry on `TransientError`
  (1s, 2s, 4s, ... capped at 60s, 5 attempts). After that the run fails but the
  checkpoint stays, so a later `dw retry` resumes it once the dependency recovers.
- **Human in the loop.** `request_approval` calls `interrupt()`. The run is parked
  in the database with no process needed. An Approve/Reject button in Slack (or
  `dw approve`) resumes it with the decision and the approver's name.
- **Idempotent decisions.** A second click on an already-decided request is
  rejected, not re-executed.

## Run it

```bash
uv sync
uv run dw start --vendor acme --amount 500 --id demo-1
# "waiting for approval ..."  ← kill the process, reboot the machine, whatever
uv run dw status demo-1       # next: ['request_approval']
uv run dw approve demo-1 --by alice
# Request demo-1 (acme, 500) approved by alice, paid PAY-606410fb
```

Watch retries: `DW_FLAKY=1 uv run dw start --vendor globex --amount 42` makes the
vendor check fail half the time.

### With Slack

1. Create a Slack app from `slack-manifest.yml` (Socket Mode, so no public URL).
2. Install it, then copy `.env.example` to `.env` with the bot token (`xoxb-`),
   the app-level token (`xapp-`, scope `connections:write`) and a channel.
3. `uv run dw listen` in one terminal, `uv run dw start ...` in another. The
   approval message with buttons lands in the channel; clicking resumes the run
   and edits the message with the outcome.

The listener is stateless. Restart it any time; pending approvals live in
`workflows.db`, not in memory.

## Layout

| File | Role |
|------|------|
| `durable_workflow/graph.py` | The state machine: state, nodes, retry policy, edges. Pure; side effects are injected via `Services`. |
| `durable_workflow/app.py` | Wiring and CLI: opens the checkpoint store, `start` / `decide` / `retry` / `status`. |
| `durable_workflow/slack_app.py` | Slack Bolt socket-mode listener: posts approval requests, handles button clicks. |
| `tests/` | Approve and reject paths, backoff timing, resume across a simulated crash, resume after exhausted retries. |

`uv run pytest` runs the suite in under a second.

## Swapping in a real workflow

Replace the three callables in `Services` (`check_vendor`, `pay`, `notify`) with
real integrations and raise `TransientError` for anything worth retrying. Add
nodes to `build_graph` for more steps; each one is automatically checkpointed,
and any node can call `interrupt()` to add another approval gate.

For Postgres instead of SQLite, swap `SqliteSaver` for `PostgresSaver` from
`langgraph-checkpoint-postgres`; nothing else changes.
