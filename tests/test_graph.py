import sqlite3
import time

import pytest
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.types import Command, RetryPolicy

from durable_workflow.graph import Services, TransientError, build_graph

FAST_RETRY = RetryPolicy(initial_interval=0.01, backoff_factor=2, max_attempts=3, jitter=False, retry_on=TransientError)


def saver(path):
    return SqliteSaver(sqlite3.connect(path, check_same_thread=False))


def ok_services(log):
    return Services(
        check_vendor=lambda v: log.append(("check", v)),
        pay=lambda v, a: (log.append(("pay", v, a)), f"PAY-{v}-{a}")[1],
        notify=lambda m: log.append(("notify", m)),
    )


def cfg(rid):
    return {"configurable": {"thread_id": rid}}


def test_approve_pays_and_notifies(tmp_path):
    log = []
    g = build_graph(saver(tmp_path / "w.db"), ok_services(log), FAST_RETRY)
    out = g.invoke({"request_id": "r1", "vendor": "acme", "amount": 500}, cfg("r1"))
    assert "__interrupt__" in out
    assert out["__interrupt__"][0].value == {"request_id": "r1", "vendor": "acme", "amount": 500}
    assert ("pay", "acme", 500) not in log

    out = g.invoke(Command(resume={"decision": "approved", "by": "alice"}), cfg("r1"))
    assert out["decision"] == "approved"
    assert out["paid_ref"] == "PAY-acme-500"
    assert ("pay", "acme", 500) in log
    assert log[-1][0] == "notify" and "approved" in log[-1][1]


def test_reject_skips_payment(tmp_path):
    log = []
    g = build_graph(saver(tmp_path / "w.db"), ok_services(log), FAST_RETRY)
    g.invoke({"request_id": "r2", "vendor": "acme", "amount": 5}, cfg("r2"))
    out = g.invoke(Command(resume={"decision": "rejected", "by": "bob"}), cfg("r2"))
    assert out["decision"] == "rejected"
    assert out.get("paid_ref") is None
    assert not any(e[0] == "pay" for e in log)
    assert "rejected" in log[-1][1]


def test_survives_process_restart_between_interrupt_and_approval(tmp_path):
    db = tmp_path / "w.db"
    log = []
    first = build_graph(saver(db), ok_services(log), FAST_RETRY)
    first.invoke({"request_id": "r3", "vendor": "acme", "amount": 9}, cfg("r3"))
    del first  # "crash": nothing of the old process survives but the sqlite file

    second = build_graph(saver(db), ok_services(log), FAST_RETRY)
    state = second.get_state(cfg("r3"))
    assert state.next == ("request_approval",)
    out = second.invoke(Command(resume={"decision": "approved", "by": "carol"}), cfg("r3"))
    assert out["paid_ref"] == "PAY-acme-9"
    assert log.count(("check", "acme")) == 1  # validate did not re-run


def test_transient_errors_retry_with_exponential_backoff(tmp_path):
    calls = []

    def flaky_check(vendor):
        calls.append(time.monotonic())
        if len(calls) < 3:
            raise TransientError("vendor api 503")

    log = []
    s = ok_services(log)
    s.check_vendor = flaky_check
    g = build_graph(saver(tmp_path / "w.db"), s, FAST_RETRY)
    g.invoke({"request_id": "r4", "vendor": "acme", "amount": 1}, cfg("r4"))
    assert len(calls) == 3
    gaps = [b - a for a, b in zip(calls, calls[1:])]
    assert gaps[1] > gaps[0] * 1.5  # second wait roughly double the first


def test_exhausted_retries_fail_then_resume_from_checkpoint(tmp_path):
    db = tmp_path / "w.db"
    log = []
    s = ok_services(log)

    def always_down(vendor):
        raise TransientError("down")

    s.check_vendor = always_down
    g = build_graph(saver(db), s, FAST_RETRY)
    with pytest.raises(TransientError):
        g.invoke({"request_id": "r5", "vendor": "acme", "amount": 2}, cfg("r5"))

    # service recovers; a fresh process re-invokes with no input to continue
    s2 = ok_services(log)
    g2 = build_graph(saver(db), s2, FAST_RETRY)
    out = g2.invoke(None, cfg("r5"))
    assert "__interrupt__" in out
    out = g2.invoke(Command(resume={"decision": "approved", "by": "dan"}), cfg("r5"))
    assert out["paid_ref"] == "PAY-acme-2"
