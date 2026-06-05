"""
Thin email sender (Phase 4). Resend HTTP API over httpx — no SDK dependency.

GRACEFUL DEGRADATION: when RESEND_API_KEY is absent the sender is a no-op that
returns a "skipped" result. Callers treat email as best-effort: the month-end
PDF still generates and is downloadable; only the email half is skipped. Drop a
RESEND_API_KEY (+ optional MAIL_FROM) into the environment to enable live sends.
"""
from __future__ import annotations

import base64
import os
from typing import Any

import httpx

_RESEND_ENDPOINT = "https://api.resend.com/emails"


def email_configured() -> bool:
    return bool(os.environ.get("RESEND_API_KEY"))


def _from_address() -> str:
    return os.environ.get("MAIL_FROM", "BookWize <reports@bookwize.ca>")


def send_email(
    *,
    to: list[str],
    subject: str,
    html: str,
    cc: list[str] | None = None,
    attachment_bytes: bytes | None = None,
    attachment_filename: str | None = None,
) -> dict[str, Any]:
    """Send one email, optionally with a single attachment. Returns
    {"sent": bool, "skipped": bool, "id": str|None, "error": str|None}.
    Never raises — email failure must not block the caller."""
    api_key = os.environ.get("RESEND_API_KEY")
    if not api_key:
        return {"sent": False, "skipped": True, "id": None, "error": "RESEND_API_KEY not set"}
    if not to:
        return {"sent": False, "skipped": True, "id": None, "error": "no recipients"}

    payload: dict[str, Any] = {
        "from": _from_address(),
        "to": to,
        "subject": subject,
        "html": html,
    }
    if cc:
        payload["cc"] = cc
    if attachment_bytes is not None and attachment_filename:
        payload["attachments"] = [{
            "filename": attachment_filename,
            "content": base64.b64encode(attachment_bytes).decode("ascii"),
        }]

    try:
        resp = httpx.post(
            _RESEND_ENDPOINT,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=30.0,
        )
        if resp.status_code >= 400:
            return {"sent": False, "skipped": False, "id": None,
                    "error": f"resend {resp.status_code}: {resp.text[:200]}"}
        data = resp.json() if resp.content else {}
        return {"sent": True, "skipped": False, "id": data.get("id"), "error": None}
    except Exception as exc:  # network / serialization — best-effort
        return {"sent": False, "skipped": False, "id": None, "error": repr(exc)[:200]}
