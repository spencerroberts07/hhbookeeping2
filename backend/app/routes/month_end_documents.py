"""
Month-end document endpoints (Phase 4B). Manual (re)generate, fetch the
presigned download URL, and re-send the email. Generation also auto-fires as a
non-fatal background task when a period is approved-to-close
(wired from routes/period_close.py).

Email is best-effort: send_email is a no-op without RESEND_API_KEY, so these
endpoints always produce/refresh the PDF and return a presigned URL; the email
half simply reports skipped when unconfigured.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from ..db import db_session
from ..services_auth import require_role
from ..services_email import email_configured, send_email
from ..services_month_end_pdf import generate_month_end_document

_log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/reports/month-end", tags=["month-end-documents"])


def _resolve_period(session, entity_code: str, period_end: str) -> dict[str, Any]:
    row = session.execute(
        text(
            """
            SELECT ap.id AS period_id, ap.period_end, ap.period_label,
                   e.entity_code, e.id AS entity_id, e.entity_name, e.notification_preferences
              FROM accounting_periods ap
              JOIN entities e ON e.id = ap.entity_id
             WHERE e.entity_code = :ec AND ap.period_end = :pe
            """
        ),
        {"ec": entity_code, "pe": period_end},
    ).mappings().first()
    if not row:
        raise HTTPException(404, "accounting period not found")
    return dict(row)


def _default_recipients(prefs: Any, actor: str | None) -> tuple[list[str], list[str]]:
    to: list[str] = []
    cc: list[str] = []
    if isinstance(prefs, dict):
        mt = prefs.get("month_end_recipients")
        if isinstance(mt, list):
            to = [x for x in mt if isinstance(x, str) and "@" in x]
        mc = prefs.get("month_end_cc")
        if isinstance(mc, list):
            cc = [x for x in mc if isinstance(x, str) and "@" in x]
    if not to and actor and "@" in actor:
        to = [actor]
    return to, cc


def _email_html(entity_name: str, period_label: str) -> str:
    return (
        f"<div style='font-family:Arial,sans-serif;color:#1A2330'>"
        f"<h2 style='color:#00843D'>Month-End Financial Package</h2>"
        f"<p><b>{entity_name}</b> — {period_label}</p>"
        f"<p>The month-end financial package is attached as a PDF. "
        f"This document was generated automatically by BookWize.</p>"
        f"<p style='color:#525B6B;font-size:12px'>Prepared by BookWize</p></div>"
    )


def generate_and_email(
    *, entity_code: str, period_end_iso: str, period_id: str | None = None,
    actor: str | None = None, to: list[str] | None = None, cc: list[str] | None = None,
    send: bool = True, prefs: Any = None, entity_name: str = "",
    period_label: str = "",
) -> dict[str, Any]:
    """Generate (or refresh) the PDF, then best-effort email it. Returns a
    JSON-safe summary (pdf_bytes stripped)."""
    res = generate_month_end_document(
        entity_code=entity_code, period_end=date.fromisoformat(period_end_iso), generated_by=actor)
    pdf_bytes = res.pop("pdf_bytes", None)

    email_result: dict[str, Any] = {"sent": False, "skipped": True, "error": "not attempted"}
    if send and pdf_bytes and email_configured():
        recips, default_cc = _default_recipients(prefs, actor)
        recips = to if to else recips
        cc_list = cc if cc else default_cc
        email_result = send_email(
            to=recips, cc=cc_list,
            subject=f"Month-End Package — {entity_name} — {period_label}",
            html=_email_html(entity_name, period_label),
            attachment_bytes=pdf_bytes,
            attachment_filename=f"month-end-{entity_code}-{period_end_iso}.pdf",
        )
        if email_result.get("sent") and period_id:
            try:
                import json as _json
                with db_session() as s:
                    s.execute(
                        text("""UPDATE month_end_documents
                                   SET email_sent_at=NOW(), email_recipients=CAST(:r AS jsonb), updated_at=NOW()
                                 WHERE accounting_period_id=:pid"""),
                        {"r": _json.dumps({"to": recips, "cc": cc_list}), "pid": period_id},
                    )
            except Exception:
                _log.exception("failed to record email recipients")
    res["email"] = email_result
    return res


def trigger_month_end_document(entity_code: str, period_end: str, actor_email: str | None) -> None:
    """Fully non-fatal background entry point (called on period approve)."""
    try:
        with db_session() as s:
            row = s.execute(
                text(
                    """SELECT ap.id AS pid, ap.period_label, e.entity_name, e.notification_preferences
                         FROM accounting_periods ap JOIN entities e ON e.id = ap.entity_id
                        WHERE e.entity_code=:ec AND ap.period_end=:pe"""
                ),
                {"ec": entity_code, "pe": period_end},
            ).mappings().first()
        generate_and_email(
            entity_code=entity_code, period_end_iso=period_end,
            period_id=str(row["pid"]) if row else None, actor=actor_email,
            prefs=row["notification_preferences"] if row else None,
            entity_name=row["entity_name"] if row else entity_code,
            period_label=row["period_label"] if row else period_end,
        )
    except Exception:
        _log.exception("auto month-end document generation failed for %s/%s — non-fatal",
                       entity_code, period_end)


class GenerateRequest(BaseModel):
    entity_code: str
    period_end: str
    actor_email: str | None = None
    to: list[str] | None = None
    cc: list[str] | None = None
    send_email: bool = True


@router.post("/generate")
def post_generate(body: GenerateRequest,
                  _user: Any = Depends(require_role("bookkeeper"))) -> dict[str, Any]:
    with db_session() as session:
        p = _resolve_period(session, body.entity_code, body.period_end)
    return generate_and_email(
        entity_code=p["entity_code"], period_end_iso=p["period_end"].isoformat(),
        period_id=str(p["period_id"]), actor=body.actor_email, to=body.to, cc=body.cc,
        send=body.send_email, prefs=p["notification_preferences"],
        entity_name=p["entity_name"], period_label=p["period_label"],
    )


@router.get("/document")
def get_document(entity_code: str = Query(...), period_end: str = Query(...),
                 _user: Any = Depends(require_role("viewer"))) -> dict[str, Any]:
    with db_session() as session:
        p = _resolve_period(session, entity_code, period_end)
        period_id = str(p["period_id"])
        row = session.execute(
            text(
                """SELECT id, r2_object_key, status, generated_at, generated_by,
                          email_sent_at, email_recipients, commentary_json, error_msg
                     FROM month_end_documents
                    WHERE accounting_period_id=:pid"""
            ),
            {"pid": period_id},
        ).mappings().first()
    if not row:
        return {"period_id": period_id, "status": "not_generated", "presigned_url": None}
    presigned = None
    if row["r2_object_key"]:
        try:
            from ..services_storage import storage_service
            presigned = storage_service.get_presigned_url(row["r2_object_key"], expires_in=86400)
        except Exception:
            presigned = None
    return {
        "period_id": period_id,
        "entity_code": p["entity_code"],
        "period_label": p["period_label"],
        "status": row["status"],
        "presigned_url": presigned,
        "r2_object_key": row["r2_object_key"],
        "generated_at": row["generated_at"].isoformat() if row["generated_at"] else None,
        "generated_by": row["generated_by"],
        "email_sent_at": row["email_sent_at"].isoformat() if row["email_sent_at"] else None,
        "email_recipients": row["email_recipients"],
        "error_msg": row["error_msg"],
    }


class ResendRequest(BaseModel):
    entity_code: str
    period_end: str
    actor_email: str | None = None
    to: list[str] | None = None
    cc: list[str] | None = None


@router.post("/resend")
def post_resend(body: ResendRequest,
                _user: Any = Depends(require_role("bookkeeper"))) -> dict[str, Any]:
    if not email_configured():
        raise HTTPException(409, "Email not configured (RESEND_API_KEY absent). Document is download-only.")
    with db_session() as session:
        p = _resolve_period(session, body.entity_code, body.period_end)
    return generate_and_email(
        entity_code=p["entity_code"], period_end_iso=p["period_end"].isoformat(),
        period_id=str(p["period_id"]), actor=body.actor_email, to=body.to, cc=body.cc,
        send=True, prefs=p["notification_preferences"],
        entity_name=p["entity_name"], period_label=p["period_label"],
    )
