"""Unit tests for the Slack alert helper."""

from __future__ import annotations

from src.utils import alerts


def test_logs_only_when_no_webhook(monkeypatch, caplog):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    with caplog.at_level("WARNING"):
        sent = alerts.send_slack_alert("hello")
    assert sent is False
    assert "hello" in caplog.text


def test_posts_when_webhook_set(monkeypatch):
    calls = {}

    class _Resp:
        def raise_for_status(self):
            calls["raised"] = True

    def fake_post(url, json, timeout):
        calls["url"] = url
        calls["json"] = json
        return _Resp()

    import httpx

    monkeypatch.setattr(httpx, "post", fake_post)
    sent = alerts.send_slack_alert("boom", webhook_url="http://hook")
    assert sent is True
    assert calls["url"] == "http://hook"
    assert calls["json"] == {"text": "boom"}
