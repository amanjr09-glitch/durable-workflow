"""Slack human-in-the-loop. Socket Mode, so no public URL is needed.

Scopes: chat:write, connections:write (app-level token). Interactivity must be on. See slack-manifest.yml.
"""

import os

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from durable_workflow import app as workflow

_app = None


def _bolt() -> App:
    global _app
    if _app is None:
        _app = App(token=os.environ["SLACK_BOT_TOKEN"])
        _app.action("approve")(_on_button)
        _app.action("reject")(_on_button)
    return _app


def _channel() -> str:
    return os.environ.get("SLACK_CHANNEL", "#approvals")


def post_text(text: str) -> None:
    _bolt().client.chat_postMessage(channel=_channel(), text=text)


def post_approval_request(pending: dict) -> None:
    rid = pending["request_id"]
    text = f"Payment request *{rid}*: pay *{pending['vendor']}* {pending['amount']}"
    _bolt().client.chat_postMessage(
        channel=_channel(),
        text=text,
        blocks=[
            {"type": "section", "text": {"type": "mrkdwn", "text": text}},
            {
                "type": "actions",
                "elements": [
                    {"type": "button", "action_id": "approve", "value": rid, "style": "primary",
                     "text": {"type": "plain_text", "text": "Approve"}},
                    {"type": "button", "action_id": "reject", "value": rid, "style": "danger",
                     "text": {"type": "plain_text", "text": "Reject"}},
                ],
            },
        ],
    )


def _on_button(ack, body, action, client, logger):
    ack()
    rid = action["value"]
    decision = "approved" if action["action_id"] == "approve" else "rejected"
    by = body["user"].get("username") or body["user"]["id"]
    try:
        workflow.decide(rid, decision, by)
        outcome = f"{decision} by @{by}"
    except LookupError as e:
        outcome = f"already decided ({e})"
    except Exception as e:  # keep the button responsive; the checkpoint is intact for `dw retry`
        logger.exception("workflow failed after decision")
        outcome = f"{decision} by @{by}, but execution failed: {e}. Run `dw retry {rid}`."
    msg = body["message"]
    client.chat_update(
        channel=body["channel"]["id"],
        ts=msg["ts"],
        text=msg["text"],
        blocks=[msg["blocks"][0], {"type": "context", "elements": [{"type": "mrkdwn", "text": outcome}]}],
    )


def listen() -> None:
    SocketModeHandler(_bolt(), os.environ["SLACK_APP_TOKEN"]).start()
