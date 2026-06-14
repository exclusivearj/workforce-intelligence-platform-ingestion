"""Slack alerting helper (offline-first).

If SLACK_WEBHOOK_URL is unset, the message is logged instead of sent so the
pipeline runs end-to-end without any external dependency.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def send_slack_alert(message: str, webhook_url: str | None = None) -> bool:
    """Post ``message`` to Slack. Returns True if sent, False if logged-only."""
    url = webhook_url or os.getenv("SLACK_WEBHOOK_URL", "")
    if not url:
        logger.warning("[slack-disabled] %s", message)
        return False

    import httpx

    resp = httpx.post(url, json={"text": message}, timeout=10.0)
    resp.raise_for_status()
    return True
