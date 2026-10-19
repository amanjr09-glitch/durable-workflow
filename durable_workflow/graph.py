"""The workflow: validate -> request_approval (pauses, maybe for days) -> pay | notify.

Every node boundary is a checkpoint in SQLite, so a crash anywhere resumes from the
last completed node. `interrupt()` parks the run until someone resumes it with a
decision. Transient failures retry with exponential backoff; once retries are
exhausted the run fails but its checkpoint stays, and `invoke(None)` picks it up.
"""

from dataclasses import dataclass
from operator import add
from typing import Annotated, Callable, TypedDict

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import RetryPolicy, interrupt


class TransientError(Exception):
    """Raise this from a service for failures worth retrying (timeouts, 5xx, rate limits)."""


class State(TypedDict, total=False):
    request_id: str
    vendor: str
    amount: int
    decision: str  # "approved" | "rejected"
    approver: str
    paid_ref: str
    log: Annotated[list[str], add]


@dataclass
class Services:
    """The side effects, injected so tests and demos can swap them for fakes."""

    check_vendor: Callable[[str], None]  # raise TransientError to trigger a retry
    pay: Callable[[str, int], str]  # returns a payment reference
    notify: Callable[[str], None]


DEFAULT_RETRY = RetryPolicy(
    initial_interval=1.0, backoff_factor=2.0, max_interval=60.0, max_attempts=5, retry_on=TransientError
)


def build_graph(checkpointer: BaseCheckpointSaver, services: Services, retry: RetryPolicy = DEFAULT_RETRY):
    def validate(s: State) -> State:
        services.check_vendor(s["vendor"])
        return {"log": [f"validated vendor {s['vendor']}"]}

    def request_approval(s: State) -> State:
        # Everything before interrupt() re-runs on resume, so keep it pure: the Slack
        # post happens in the caller (cli/slack_app), not here.
        answer = interrupt({"request_id": s["request_id"], "vendor": s["vendor"], "amount": s["amount"]})
        return {
            "decision": answer["decision"],
            "approver": answer.get("by", "unknown"),
            "log": [f"{answer['decision']} by {answer.get('by', 'unknown')}"],
        }

    def pay(s: State) -> State:
        ref = services.pay(s["vendor"], s["amount"])
        return {"paid_ref": ref, "log": [f"paid {ref}"]}

    def notify(s: State) -> State:
        msg = f"Request {s['request_id']} ({s['vendor']}, {s['amount']}) {s['decision']} by {s['approver']}"
        if s.get("paid_ref"):
            msg += f", paid {s['paid_ref']}"
        services.notify(msg)
        return {"log": ["notified"]}

    g = StateGraph(State)
    g.add_node("validate", validate, retry_policy=retry)
    g.add_node("request_approval", request_approval)
    g.add_node("pay", pay, retry_policy=retry)
    g.add_node("notify", notify, retry_policy=retry)
    g.add_edge(START, "validate")
    g.add_edge("validate", "request_approval")
    g.add_conditional_edges(
        "request_approval", lambda s: "pay" if s["decision"] == "approved" else "notify", ["pay", "notify"]
    )
    g.add_edge("pay", "notify")
    g.add_edge("notify", END)
    return g.compile(checkpointer=checkpointer)
