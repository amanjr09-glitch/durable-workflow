from durable_workflow import app


def test_start_decide_status_roundtrip(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("DW_DB", str(tmp_path / "w.db"))
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)

    pending = app.start("acme", 120, request_id="req-1")
    assert pending == {"request_id": "req-1", "vendor": "acme", "amount": 120}
    assert app.status("req-1")["next"] == ["request_approval"]

    final = app.decide("req-1", "approved", by="alice")
    assert final["paid_ref"].startswith("PAY-")
    assert app.status("req-1")["next"] == []
    assert "approved by alice" in capsys.readouterr().out


def test_decide_unknown_request_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("DW_DB", str(tmp_path / "w.db"))
    try:
        app.decide("nope", "approved", by="x")
    except LookupError as e:
        assert "nope" in str(e)
    else:
        raise AssertionError("expected LookupError")
