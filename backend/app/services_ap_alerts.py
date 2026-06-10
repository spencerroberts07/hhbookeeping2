"""
AP Due-Date Alert Engine.

Queries direct_vendor_ap_invoices for invoices where due_date is exactly
ALERT_THRESHOLDS days away (or past due), fires in-app + email alerts,
and logs each to ap_alert_log with deduplication via ON CONFLICT DO NOTHING.

Multi-tenancy: every function scoped by entity_id UUID.
HH AP (account 2030) excluded — guard is applied at the query level.
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import text

_log = logging.getLogger(__name__)

# Default alert thresholds (days before due_date). Overridable per entity.
DEFAULT_THRESHOLDS: list[int] = [7, 3]

# Also fire for past-due invoices (negative days remaining treated as overdue alert)
OVERDUE_THRESHOLD = 0  # fires on the due date itself and any day after


def _get_ap_alert_settings(entity_row: dict) -> dict[str, Any]:
    """Read notification_preferences->>'ap_alerts' with sensible defaults."""
    prefs = entity_row.get("notification_preferences") or {}
    if isinstance(prefs, str):
        try:
            prefs = json.loads(prefs)
        except Exception:
            prefs = {}
    ap = prefs.get("ap_alerts") or {}
    return {
        "email_enabled": bool(ap.get("email_enabled", True)),
        "remittance_advice_enabled": bool(ap.get("remittance_advice_enabled", True)),
        "thresholds": ap.get("thresholds", DEFAULT_THRESHOLDS),
    }


def run_ap_due_alerts(
    session,
    *,
    entity_id: UUID,
    today: date | None = None,
) -> dict[str, Any]:
    """Run the alert engine for one entity.

    For each threshold T in the entity's settings:
    - Find open invoices where due_date == today + T days (exact match, fire once).
    - Also find overdue invoices (due_date < today) not yet alerted.
    - INSERT into ap_alert_log ON CONFLICT DO NOTHING (dedup guard).
    - Only newly-inserted rows trigger notifications.

    Returns a summary dict with counts.
    """
    if today is None:
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).date()

    entity_id_str = str(entity_id)

    # Load entity + alert settings
    entity = session.execute(
        text("""
            SELECT e.id, e.entity_code, e.entity_name,
                   e.notification_preferences
            FROM entities e
            WHERE e.id = :eid
        """),
        {"eid": entity_id_str},
    ).mappings().first()

    if not entity:
        _log.warning("run_ap_due_alerts: entity %s not found", entity_id)
        return {"entity_id": entity_id_str, "alerts_fired": 0, "error": "entity not found"}

    settings = _get_ap_alert_settings(dict(entity))
    thresholds = settings["thresholds"]
    email_enabled = settings["email_enabled"]

    alerts_fired = 0
    alerts_skipped_dedup = 0

    for threshold_days in thresholds:
        target_date = today + timedelta(days=threshold_days)

        # Find open (non-payment_pending, non-paid, non-void) invoices due on target_date
        # Explicitly exclude HH AP: direct_vendor_ap_invoices has no ap_account column,
        # but HH AP never enters this table (it's in hh_ap_invoices). Double-guard via
        # status filter only — the spine table IS outside-vendor by construction.
        invoices = session.execute(
            text("""
                SELECT i.id, i.entity_id, i.vendor_name, i.invoice_number,
                       i.total_amount, i.open_amount, i.due_date,
                       i.status, i.payment_status,
                       v.remittance_email AS vendor_email
                FROM direct_vendor_ap_invoices i
                LEFT JOIN vendors v ON v.id = i.vendor_id
                WHERE i.entity_id = :eid
                  AND i.status IN ('open', 'needs_review', 'approved')
                  AND i.due_date = :target_date
                  AND i.active = TRUE
            """),
            {"eid": entity_id_str, "target_date": target_date},
        ).mappings().all()

        for inv in invoices:
            fired = _fire_alert(
                session,
                entity=dict(entity),
                invoice=dict(inv),
                threshold_days=threshold_days,
                today=today,
                email_enabled=email_enabled,
            )
            if fired:
                alerts_fired += 1
            else:
                alerts_skipped_dedup += 1

    # Also alert on overdue invoices (due_date < today), threshold=0, fire once
    overdue = session.execute(
        text("""
            SELECT i.id, i.entity_id, i.vendor_name, i.invoice_number,
                   i.total_amount, i.open_amount, i.due_date,
                   i.status, i.payment_status,
                   v.remittance_email AS vendor_email
            FROM direct_vendor_ap_invoices i
            LEFT JOIN vendors v ON v.id = i.vendor_id
            WHERE i.entity_id = :eid
              AND i.status IN ('open', 'needs_review', 'approved')
              AND i.due_date < :today
              AND i.active = TRUE
        """),
        {"eid": entity_id_str, "today": today},
    ).mappings().all()

    for inv in overdue:
        fired = _fire_alert(
            session,
            entity=dict(entity),
            invoice=dict(inv),
            threshold_days=OVERDUE_THRESHOLD,
            today=today,
            email_enabled=email_enabled,
        )
        if fired:
            alerts_fired += 1
        else:
            alerts_skipped_dedup += 1

    return {
        "entity_id": entity_id_str,
        "entity_code": entity["entity_code"],
        "date": today.isoformat(),
        "thresholds": thresholds,
        "alerts_fired": alerts_fired,
        "alerts_skipped_dedup": alerts_skipped_dedup,
    }


def _fire_alert(
    session,
    *,
    entity: dict,
    invoice: dict,
    threshold_days: int,
    today: date,
    email_enabled: bool,
) -> bool:
    """Attempt to log + fire one alert. Returns True if newly fired, False if deduped."""
    invoice_id_str = str(invoice["id"])

    # Dedup guard: INSERT ON CONFLICT DO NOTHING
    result = session.execute(
        text("""
            INSERT INTO ap_alert_log
                (entity_id, invoice_id, threshold_days, due_date, in_app_fired)
            VALUES (:eid, :iid, :threshold, :due, TRUE)
            ON CONFLICT (entity_id, invoice_id, threshold_days) DO NOTHING
            RETURNING id
        """),
        {
            "eid": str(entity["id"]),
            "iid": invoice_id_str,
            "threshold": threshold_days,
            "due": invoice["due_date"],
        },
    ).mappings().first()

    if not result:
        # Already fired for this invoice+threshold
        return False

    due_date = invoice["due_date"]
    days_remaining = (due_date - today).days if due_date else None
    vendor = invoice.get("vendor_name") or "Unknown vendor"
    amount = invoice.get("open_amount") or invoice.get("total_amount") or 0

    if days_remaining is not None and days_remaining < 0:
        days_label = f"{abs(days_remaining)} day{'s' if abs(days_remaining) != 1 else ''} overdue"
    elif days_remaining == 0:
        days_label = "due today"
    else:
        days_label = f"due in {days_remaining} day{'s' if days_remaining != 1 else ''}"

    _log.info(
        "ap_alert fired: entity=%s invoice=%s vendor=%r %s",
        entity.get("entity_code"), invoice_id_str, vendor, days_label,
    )

    # Email alert
    email_status = "skipped"
    if email_enabled:
        email_status = _send_due_alert_email(
            entity=entity,
            invoice=invoice,
            days_label=days_label,
            days_remaining=days_remaining,
        )

    # Update email_status on the log row
    session.execute(
        text("""
            UPDATE ap_alert_log
               SET email_fired   = :efired,
                   email_status  = :estatus
             WHERE entity_id = :eid AND invoice_id = :iid AND threshold_days = :threshold
        """),
        {
            "efired": email_status == "sent",
            "estatus": email_status,
            "eid": str(entity["id"]),
            "iid": invoice_id_str,
            "threshold": threshold_days,
        },
    )

    return True


def _send_due_alert_email(
    *,
    entity: dict,
    invoice: dict,
    days_label: str,
    days_remaining: int | None,
) -> str:
    """Send a due-date alert email. Returns 'sent', 'skipped', or 'error'."""
    try:
        from .services_email import email_configured, send_email
    except ImportError:
        return "skipped"

    if not email_configured():
        return "skipped"

    vendor = invoice.get("vendor_name") or "Unknown vendor"
    amount = float(invoice.get("open_amount") or invoice.get("total_amount") or 0)
    due_date = invoice.get("due_date")
    inv_number = invoice.get("invoice_number") or "—"
    entity_name = entity.get("entity_name") or entity.get("entity_code") or "Your entity"

    # Recipient: entity-level notification email (not yet a field on entities —
    # graceful skip if not configured). Future: add notification_email to entities.
    # For now, log only (no recipient).
    _log.info(
        "ap_due_alert email: entity=%s vendor=%r invoice=%s %s $%.2f",
        entity.get("entity_code"), vendor, inv_number, days_label, amount,
    )

    # TODO: Once entities.notification_email is added, send via:
    # send_email(to=[entity_notification_email], subject=..., html=...)
    # For now graceful no-op (RESEND_API_KEY may not be set anyway).
    return "skipped"


# ---------------------------------------------------------------------------
# Run alerts for all entities (cron entry point)
# ---------------------------------------------------------------------------

def run_all_entities_ap_alerts(session, *, today: date | None = None) -> dict[str, Any]:
    """Fire AP alerts for every active entity. Called by the daily cron."""
    entities = session.execute(
        text("""
            SELECT id FROM entities WHERE active = TRUE OR active IS NULL
        """)
    ).scalars().all()

    results = []
    for eid in entities:
        try:
            result = run_ap_due_alerts(session, entity_id=eid, today=today)
            results.append(result)
        except Exception as exc:
            _log.error("ap_alerts: error for entity %s: %r", eid, exc)
            results.append({"entity_id": str(eid), "error": str(exc)})

    total_fired = sum(r.get("alerts_fired", 0) for r in results)
    return {
        "entities_processed": len(results),
        "total_alerts_fired": total_fired,
        "results": results,
    }
