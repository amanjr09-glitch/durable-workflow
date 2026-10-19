"""Wiring + CLI. Opens the SQLite checkpoint store, plugs in services, exposes start/decide/retry/status."""

import argparse
import os
import random
import sqlite3
import sys
import uuid

from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.types import Command

from durable_workflow.graph import Services, TransientError, build_graph


def _slack_enabled() -> bool:
    return bool(os.environ.get("SLACK_BOT_TOKEN"))


def _check_vendor(vendor: str) -> None:
    # DW_FLAKY=1 makes this fail ~50% of the time so you can watch the backoff in the log.
    if os.environ.get("DW_FLAKY") and random.random() < 0.5:
        raise TransientError(f"vendor registry 503 for {vendor}")


def _pay(vendor: str, amount: int) -> str:
    return f"PAY-{uuid.uuid4().hex[:8]}"


def _notify(message: str) -> None:
    print(message)
    if _slack_enabled():
        from durable_workflow import slack_app

        slack_app.post_text(message)


def _graph():
    db = os.environ.get("DW_DB", "workflows.db")
    # ponytail: one connection per call, check_same_thread off so Bolt's worker threads can use it.
    saver = SqliteSaver(sqlite3.connect(db, check_same_thread=False))
    return build_graph(saver, Services(check_vendor=_check_vendor, pay=_pay, notify=_notify))


def _cfg(request_id: str):
    return {"configurable": {"thread_id": request_id}}


def _run(request_id: str, inp):
    out = _graph().invoke(inp, _cfg(request_id))
    if "__interrupt__" in out:
        return out["__interrupt__"][0].value
    return out


def start(vendor: str, amount: int, request_id: str | None = None):
    """Run until the approval interrupt. Returns the pending approval payload."""
    request_id = request_id or f"req-{uuid.uuid4().hex[:6]}"
    pending = _run(request_id, {"request_id": request_id, "vendor": vendor, "amount": amount, "log": []})
    if _slack_enabled():
        from durable_workflow import slack_app

        slack_app.post_approval_request(pending)
    return pending


def decide(request_id: str, decision: str, by: str):
    """Resume a parked run with approved/rejected. Returns the final state."""
    if decision not in ("approved", "rejected"):
        raise ValueError(f"decision must be approved or rejected, got {decision!r}")
    if _graph().get_state(_cfg(request_id)).next != ("request_approval",):
        raise LookupError(f"{request_id} is not waiting for approval")
    return _run(request_id, Command(resume={"decision": decision, "by": by}))


def retry(request_id: str):
    """Continue a run that died mid-node (crash, or retries exhausted) from its last checkpoint."""
    return _run(request_id, None)


def status(request_id: str) -> dict:
    s = _graph().get_state(_cfg(request_id))
    return {"next": list(s.next), "values": dict(s.values)}


def main(argv=None):
    p = argparse.ArgumentParser(prog="dw", description="Durable approval workflows")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("start", help="start a payment request; pauses for approval")
    s.add_argument("--vendor", required=True)
    s.add_argument("--amount", type=int, required=True)
    s.add_argument("--id")
    for name in ("approve", "reject"):
        d = sub.add_parser(name)
        d.add_argument("id")
        d.add_argument("--by", default=os.environ.get("USER", "cli"))
    sub.add_parser("retry", help="continue a run that crashed or exhausted retries").add_argument("id")
    sub.add_parser("status").add_argument("id")
    sub.add_parser("listen", help="run the Slack socket-mode listener for approval buttons")
    a = p.parse_args(argv)

    if a.cmd == "start":
        pending = start(a.vendor, a.amount, a.id)
        print(f"waiting for approval: {pending}")
        if not _slack_enabled():
            print(f"approve with: dw approve {pending['request_id']}   (or dw reject ...)")
    elif a.cmd in ("approve", "reject"):
        print(decide(a.id, a.cmd + "d" if a.cmd == "approve" else "rejected", a.by))
    elif a.cmd == "retry":
        print(retry(a.id))
    elif a.cmd == "status":
        print(status(a.id))
    elif a.cmd == "listen":
        from durable_workflow import slack_app

        slack_app.listen()


if __name__ == "__main__":
    sys.exit(main())
