"""
AR aging module — read, write-down, Excel export.

Endpoints (all scoped by entity_id / entity_code):
  GET  /api/ar/aging          — latest + prior snapshot (all 5 buckets + per-customer rows)
  POST /api/ar/write-down     — Dr <bad_debt> / Cr <ar_account> + ar_adjustment_lines audit row
  GET  /api/ar/aging/excel    — openpyxl schedule → R2 presigned URL (inline fallback)

Write endpoints require bookkeeper role.
"""
from __future__ import annotations

import io
import json
from datetime import date as DateType
from decimal import Decimal
from typing import Any
from uuid import UUID

import openpyxl
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field
from sqlalchemy import text

from ..db import db_session
from ..services import get_entity_by_code
from ..services_auth import enforce_entity_code, require_role

router = APIRouter(prefix="/api/ar", tags=["ar"])

_CENT = Decimal("0.01")
_SOURCE_MODULE = "ar_writedown"
_BATCH_LABEL = "ar_writedown"

# Bucket labels returned in every /aging response.
# The frontend uses whatever labels the API returns (never hardcodes them),
# so a different POS system with different bucket names only needs this
# constant changed — no frontend changes required.
_BUCKET_LABELS: dict[str, str] = {
    "current": "Current",
    "over_30": "31–60 days",
    "over_60": "61–90 days",
    "over_90": "91–120 days",
    "over_120": "120+ days",
}
_BUCKET_KEYS: list[str] = ["current", "over_30", "over_60", "over_90", "over_120"]


# --------------------------------------------------------------------------
# Request models
# --------------------------------------------------------------------------


class WriteDownRequest(BaseModel):
    entity_code: str = Field(..., examples=["1877-8"])
    amount: float = Field(..., gt=0, description="Write-off amount (positive)")
    customer_name: str | None = None
    customer_number: str | None = None
    memo: str | None = None
    aged_ar_snapshot_id: str | None = None
    bad_debt_account_code: str = Field(default="6550")
    ar_account_code: str = Field(default="1085")


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _actor_email(user: Any) -> str | None:
    try:
        return user.get("email")
    except AttributeError:
        return getattr(user, "email", None)


def _resolve_entity_or_404(session, entity_code: str) -> dict[str, Any]:
    row = session.execute(
        text("SELECT id, entity_code FROM entities WHERE entity_code = :ec"),
        {"ec": entity_code},
    ).mappings().first()
    if not row:
        raise HTTPException(404, f"Entity {entity_code!r} not found")
    return dict(row)


def _assert_account_active(session, entity_id: Any, code: str) -> None:
    row = session.execute(
        text(
            """
            SELECT 1 FROM accounts
             WHERE entity_id = :eid AND account_code = :code AND is_active = TRUE
             LIMIT 1
            """
        ),
        {"eid": entity_id, "code": code},
    ).first()
    if not row:
        raise HTTPException(409, f"Account {code!r} is not an active account for this entity")


def _resolve_open_period(session, entity_id: Any) -> dict[str, Any]:
    """Tiered open-period lookup — mirrors journal_edits._resolve_current_open_period."""
    for clause in (
        # Tier 1: oldest non-closed with an approved_to_post batch.
        """AND ap.status NOT IN ('closed_locked', 'approved_to_close')
           AND EXISTS (SELECT 1 FROM journal_batches jb
                        WHERE jb.accounting_period_id = ap.id
                          AND jb.status = 'approved_to_post')
           ORDER BY ap.period_end ASC""",
        # Tier 2: oldest non-closed, any batches.
        """AND ap.status NOT IN ('closed_locked', 'approved_to_close')
           ORDER BY ap.period_end ASC""",
    ):
        row = session.execute(
            text(
                f"""
                SELECT ap.id, ap.period_label, ap.period_end, ap.status
                  FROM accounting_periods ap
                 WHERE ap.entity_id = :eid
                   AND ap.period_end <= CURRENT_DATE
                   {clause}
                 LIMIT 1
                """
            ),
            {"eid": entity_id},
        ).mappings().first()
        if row:
            return dict(row)
    raise HTTPException(
        409,
        "No open accounting period available for this write-down. Reopen a period first.",
    )


def _assert_balanced(session, batch_id: Any) -> None:
    r = session.execute(
        text(
            """
            SELECT COALESCE(SUM(debit_amount), 0)  AS d,
                   COALESCE(SUM(credit_amount), 0) AS c
              FROM journal_lines WHERE journal_batch_id = :bid
            """
        ),
        {"bid": batch_id},
    ).mappings().first()
    d = Decimal(str(r["d"] or 0))
    c = Decimal(str(r["c"] or 0))
    if abs(d - c) >= _CENT:
        raise HTTPException(
            409, f"Refusing to post unbalanced batch: debits {d} != credits {c}"
        )


def _snapshot_row_to_dict(row: Any) -> dict[str, Any]:
    """Convert a DB row to the standard snapshot response shape."""
    snap: dict[str, Any] = {
        "id": str(row["id"]),
        "snapshot_date": row["snapshot_date"].isoformat() if row["snapshot_date"] else None,
        "total_ar": float(row["total_ar"] or 0),
        "buckets": {
            "current": float(row["current_amount"] or 0),
            "over_30": float(row["over_30"] or 0),
            "over_60": float(row["over_60"] or 0),
            "over_90": float(row["over_90"] or 0),
            "over_120": float(row["over_120"] or 0),
        },
        "customers": [],
    }
    raw = row["customer_detail_json"]
    if raw:
        customers: list[Any] = raw if isinstance(raw, list) else []
        snap["customers"] = [
            {
                "customer_number": c.get("customer_number"),
                "customer_name": c.get("customer_name"),
                "total": float(c.get("total") or 0),
                "current": float(c.get("current") or 0),
                "over_30": float(c.get("over_30") or 0),
                "over_60": float(c.get("over_60") or 0),
                "over_90": float(c.get("over_90") or 0),
                "over_120": float(c.get("over_120") or 0),
            }
            for c in customers
        ]
    return snap


# --------------------------------------------------------------------------
# Endpoints
# --------------------------------------------------------------------------


@router.get("/aging")
def get_ar_aging(
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    """Latest + prior AR aging snapshot: totals, 5 buckets, per-customer rows.

    Returns bucket_labels so the frontend never hardcodes bucket display names.
    The prior snapshot enables period-over-period comparison."""
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity code: {entity_code}")
        rows = session.execute(
            text(
                """
                SELECT id, snapshot_date, total_ar, current_amount,
                       over_30, over_60, over_90, over_120,
                       customer_detail_json
                  FROM aged_ar_snapshots
                 WHERE entity_id = :eid
              ORDER BY snapshot_date DESC
                 LIMIT 2
                """
            ),
            {"eid": entity["id"]},
        ).mappings().all()

    snapshots = [_snapshot_row_to_dict(r) for r in rows]
    return {
        "entity_code": entity_code,
        "bucket_labels": _BUCKET_LABELS,
        "current": snapshots[0] if snapshots else None,
        "prior": snapshots[1] if len(snapshots) > 1 else None,
    }


@router.post("/write-down")
def ar_write_down(
    payload: WriteDownRequest,
    _user: Any = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """
    Post a manual AR write-down:
      Dr {bad_debt_account_code}  amount
      Cr {ar_account_code}        amount

    Also inserts an ar_adjustment_lines audit row (import_run_id=NULL).
    Raises 409 if either account is inactive, no open period exists, or the
    resulting batch would be unbalanced.
    """
    enforce_entity_code(_user, payload.entity_code)
    amount = Decimal(str(payload.amount)).quantize(Decimal("0.01"))
    if amount <= 0:
        raise HTTPException(400, "amount must be positive")

    memo = payload.memo or f"AR write-down — {payload.customer_name or 'manual'}"
    snap_id: UUID | None = (
        UUID(payload.aged_ar_snapshot_id) if payload.aged_ar_snapshot_id else None
    )

    with db_session() as session:
        entity = _resolve_entity_or_404(session, payload.entity_code)
        eid = entity["id"]

        _assert_account_active(session, eid, payload.bad_debt_account_code)
        _assert_account_active(session, eid, payload.ar_account_code)

        period = _resolve_open_period(session, eid)
        period_id = period["id"]

        summary = {
            "source": _SOURCE_MODULE,
            "customer_name": payload.customer_name,
            "customer_number": payload.customer_number,
            "amount": str(amount),
            "bad_debt_account": payload.bad_debt_account_code,
            "ar_account": payload.ar_account_code,
            "memo": memo,
        }

        # Insert journal batch (immediately posted — canonical path)
        batch_row = session.execute(
            text(
                """
                INSERT INTO journal_batches (
                    entity_id, accounting_period_id, source_module, batch_label,
                    status, workflow_status, total_debits, total_credits, summary_json
                ) VALUES (
                    :eid, :pid, :src, :label,
                    'posted', 'posted', :td, :tc,
                    CAST(:summary AS jsonb)
                )
                RETURNING id
                """
            ),
            {
                "eid": eid,
                "pid": period_id,
                "src": _SOURCE_MODULE,
                "label": _BATCH_LABEL,
                "td": amount,
                "tc": amount,
                "summary": json.dumps(summary),
            },
        ).mappings().first()
        batch_id = batch_row["id"]

        src_json = json.dumps({"source_module": _SOURCE_MODULE, "memo": memo})
        session.execute(
            text(
                """
                INSERT INTO journal_lines (
                    journal_batch_id, line_number, account_code,
                    debit_amount, credit_amount, memo, source_json
                ) VALUES
                    (:bid, 1, :dr, :amount, 0,       :memo, CAST(:sj AS jsonb)),
                    (:bid, 2, :cr, 0,       :amount, :memo, CAST(:sj AS jsonb))
                """
            ),
            {
                "bid": batch_id,
                "dr": payload.bad_debt_account_code,
                "cr": payload.ar_account_code,
                "amount": amount,
                "memo": memo,
                "sj": src_json,
            },
        )

        # Balance guard — raises 409 and rolls back if unbalanced
        _assert_balanced(session, batch_id)

        # AR audit row (import_run_id=NULL marks this as a manual write-down)
        adj_row = session.execute(
            text(
                """
                INSERT INTO ar_adjustment_lines (
                    entity_id, import_run_id, accounting_period_id,
                    transaction_date, customer_number, customer_name,
                    total_amount, reason, journal_batch_id, aged_ar_snapshot_id
                ) VALUES (
                    :eid, NULL, :pid,
                    :txn_date, :cust_num, :cust_name,
                    :amount, :reason, :batch_id, :snap_id
                )
                RETURNING id
                """
            ),
            {
                "eid": eid,
                "pid": period_id,
                "txn_date": DateType.today(),
                "cust_num": payload.customer_number,
                "cust_name": payload.customer_name,
                "amount": -amount,  # negative = written-down direction
                "reason": memo,
                "batch_id": batch_id,
                "snap_id": snap_id,
            },
        ).mappings().first()

        # Audit trail in journal_line_change_events
        session.execute(
            text(
                """
                INSERT INTO journal_line_change_events (
                    entity_id, journal_batch_id, journal_line_id, accounting_period_id,
                    action, from_account_code, to_account_code,
                    before_json, after_json, reason, actor_email
                ) VALUES (
                    :eid, :bid, NULL, :pid,
                    'ar_writedown', NULL, :dr_acct,
                    CAST(:before AS jsonb), CAST(:after AS jsonb), :reason, :actor
                )
                """
            ),
            {
                "eid": eid,
                "bid": batch_id,
                "pid": period_id,
                "dr_acct": payload.bad_debt_account_code,
                "before": json.dumps({}),
                "after": json.dumps(summary),
                "reason": memo,
                "actor": _actor_email(_user),
            },
        )

    return {
        "entity_code": payload.entity_code,
        "journal_batch_id": str(batch_id),
        "adjustment_line_id": str(adj_row["id"]),
        "period_label": period["period_label"],
        "amount": str(amount),
        "dr_account": payload.bad_debt_account_code,
        "cr_account": payload.ar_account_code,
        "memo": memo,
    }


@router.get("/aging/excel")
def get_ar_aging_excel(
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("bookkeeper")),
) -> Any:
    """Generate AR aging schedule as Excel, upload to R2, return presigned URL."""
    enforce_entity_code(_user, entity_code)
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity code: {entity_code}")
        row = session.execute(
            text(
                """
                SELECT id, snapshot_date, total_ar, current_amount,
                       over_30, over_60, over_90, over_120,
                       customer_detail_json
                  FROM aged_ar_snapshots
                 WHERE entity_id = :eid
              ORDER BY snapshot_date DESC
                 LIMIT 1
                """
            ),
            {"eid": entity["id"]},
        ).mappings().first()

    if not row:
        raise HTTPException(404, "No AR aging snapshot found for this entity")

    snap = _snapshot_row_to_dict(row)
    xlsx_bytes = _build_ar_aging_excel(snap, entity_code)

    from ..services_storage import content_type_for, storage_service  # noqa: PLC0415

    snapshot_date = snap["snapshot_date"] or "unknown"
    filename = f"ar_aging_{snapshot_date}_{entity_code}.xlsx"
    r2_key = storage_service.upload_file(
        file_bytes=xlsx_bytes,
        original_filename=filename,
        entity_code=entity_code,
        document_type="ar-aging",
        content_type=content_type_for(filename),
    )
    if r2_key:
        url = storage_service.get_presigned_url(r2_key, expires_in=3600)
        return {"url": url, "r2_key": r2_key, "filename": filename}

    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _build_ar_aging_excel(snap: dict[str, Any], entity_code: str) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "AR Aging"

    ws.append([f"AR Aging Schedule — {entity_code}"])
    ws.append([f"As of: {snap['snapshot_date']}"])
    ws.append([])

    col_labels = [_BUCKET_LABELS[k] for k in _BUCKET_KEYS]
    ws.append(["Customer", "Customer #"] + col_labels + ["Total"])

    for c in snap.get("customers", []):
        row_data = [c.get("customer_name"), c.get("customer_number")]
        row_data += [c.get(k, 0) for k in _BUCKET_KEYS]
        row_data.append(c.get("total", 0))
        ws.append(row_data)

    buckets = snap.get("buckets", {})
    totals: list[Any] = ["TOTAL", ""]
    totals += [buckets.get(k, 0) for k in _BUCKET_KEYS]
    totals.append(snap.get("total_ar", 0))
    ws.append(totals)

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
