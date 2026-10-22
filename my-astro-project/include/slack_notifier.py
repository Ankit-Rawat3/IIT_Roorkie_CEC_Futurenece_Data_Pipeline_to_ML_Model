"""
include/slack_notifier.py
--------------------------
Sends a message to a Slack channel via Incoming Webhook.
"""

import os
import json
import urllib.request


def send_slack_alert(
    message: str = None,
    title: str = "Meridian Pipeline Alert",
    run_time: str = "",
    clean_rows: int = 0,
    quarantined_rows: int = 0,
    details: list = None,
    action_item: str = "Check the quarantine folder and review the failed rows.",
    webhook_url: str = None,
) -> bool:
    """
    Sends message to Slack. Supports both pre-formatted message strings and structured parameters.
    """
    url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")

    # If a pre-formatted message string wasn't passed, build it from structured parameters
    if not message:
        details_formatted = "\n".join([f"• {item}" for item in (details or [])])
        message = (
            f"🚨 *{title}*\n"
            f"*Run time*: `{run_time}`\n"
            f"✅ *Clean rows loaded to warehouse*: {clean_rows}\n"
            f"❌ *Rows quarantined (failed validation)*: {quarantined_rows}\n"
            f"*Details*:\n"
            f"{details_formatted}\n"
            f"👉 *Action*: {action_item}"
        )

    if not url:
        print("[Slack] No SLACK_WEBHOOK_URL set. Printing alert to console instead:")
        print("=" * 60)
        print(message)
        print("=" * 60)
        return False

    payload = json.dumps({"text": message}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"[Slack] Alert sent successfully. HTTP {resp.status}")
            return True
    except Exception as e:
        print(f"[Slack] Failed to send alert: {e}")
        return False