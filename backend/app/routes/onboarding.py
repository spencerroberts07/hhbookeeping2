"""
Dealer onboarding routes.

Drives the 8-step wizard at /onboarding. Two parallel paths into the
same write pipeline:

  - QBO connected: pull chart, TB, GL automatically
  - File upload:   parse via Claude (sonnet-4-6), preview, confirm, write

Long-running endpoints (GL history) return immediately with a job_id;
the wizard polls /gl-history/progress/{job_id} every few seconds.
"""
from __future__ import annotations

import asyncio
import calendar
import json
import logging
from datetime import date as DateType, datetime
from typing import Any
from uuid import UUID

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    File,
    Form,
    HTTPException,
    Path,
    Query,
    UploadFile,
)
from pydantic import BaseModel
from sqlalchemy import text

from ..db import db_session, SessionLocal
from ..services import get_entity_by_code, import_chart_of_accounts
from ..services_auth import enforce_entity_code, require_role
from ..services_onboarding import (
    detect_existing_data,
    import_gl_history_from_lines,
    import_gl_history_from_qbo,
    import_opening_balances,
    import_trial_balance_from_qbo,
    learn_from_gl_history,
    parse_chart_of_accounts,
    parse_gl_file,
    parse_trial_balance,
    save_chart_of_accounts,
)
from ..services_storage import storage_service

# Tokens-per-minute on the Claude API are generous but a TB with
# thousands of rows can still push the parse request past the 30s
# request timeout. parse_* now runs in a BackgroundTask; the upload
# endpoints return a job_id immediately and the wizard polls progress.

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/onboarding", tags=["onboarding"])


# --------------------------------------------------------------------------
# 1. Status
# --------------------------------------------------------------------------


@router.get("/status")
def get_status(
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    enforce_entity_code(_user, entity_code)
    try:
        with db_session() as session:
            entity = get_entity_by_code(session, entity_code)
            if not entity:
                raise HTTPException(404, f"Unknown entity: {entity_code}")
            data = detect_existing_data(session, str(entity["id"]))
            data["entity_code"] = entity_code
            data["entity_name"] = entity["entity_name"]
            return data
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


# --------------------------------------------------------------------------
# 2. Chart of accounts
# --------------------------------------------------------------------------


@router.post("/chart-of-accounts/upload")
async def upload_chart(
    background_tasks: BackgroundTasks,
    entity_code: str = Form(...),
    actor_email: str = Form(...),
    file: UploadFile = File(...),
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    """Kick off chart-of-accounts parsing in the background. Returns
    immediately with a job_id; the wizard polls
    /chart-of-accounts/progress/{job_id} until status='complete'.
    """
    enforce_entity_code(_user, entity_code)
    try:
        file_bytes = await file.read()
        filename = file.filename or ""
        with db_session() as session:
            entity = get_entity_by_code(session, entity_code)
            if not entity:
                raise HTTPException(404, f"Unknown entity: {entity_code}")
            job_id = _create_job(
                session,
                entity_id=str(entity["id"]),
                job_type="parse_chart_of_accounts",
                actor_email=actor_email,
            )
        background_tasks.add_task(
            _run_parse_chart,
            job_id=job_id,
            file_bytes=file_bytes,
            filename=filename,
            entity_code=entity_code,
        )
        return {"job_id": job_id, "status": "pending"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("upload_chart failed")
        raise HTTPException(400, str(exc)) from exc


@router.get("/chart-of-accounts/progress/{job_id}")
def chart_progress(
    job_id: str = Path(...),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    with db_session() as session:
        row = session.execute(
            text(
                """
                SELECT id, job_type, status, pct_complete, current_step,
                       result, error_message
                  FROM background_jobs
                 WHERE id = :id
                 LIMIT 1
                """
            ),
            {"id": job_id},
        ).mappings().first()
        if not row:
            raise HTTPException(404, f"Unknown job: {job_id}")
        result = dict(row.get("result") or {})
        return {
            "job_id": str(row["id"]),
            "job_type": row["job_type"],
            "status": row["status"],
            "pct_complete": int(row["pct_complete"] or 0),
            "current_step": row.get("current_step"),
            "preview": result.get("preview"),
            "filename": result.get("filename"),
            "entity_code": result.get("entity_code"),
            "error": row.get("error_message"),
        }


class ChartConfirmRequest(BaseModel):
    entity_code: str
    actor_email: str
    accounts: list[dict[str, Any]]


@router.post("/chart-of-accounts/confirm")
def confirm_chart(
    request: ChartConfirmRequest,
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    """Persist the confirmed (and possibly edited) chart preview."""
    enforce_entity_code(_user, request.entity_code)
    try:
        with db_session() as session:
            entity = get_entity_by_code(session, request.entity_code)
            if not entity:
                raise HTTPException(404, f"Unknown entity: {request.entity_code}")
            result = save_chart_of_accounts(
                session, str(entity["id"]), request.accounts
            )
            return {
                "entity_code": request.entity_code,
                "saved_count": result["saved_count"],
                "conflicts": result["conflicts"],
                "source": "file",
            }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


class QboChartRequest(BaseModel):
    entity_code: str
    actor_email: str


@router.post("/chart-of-accounts/qbo")
async def pull_chart_qbo(
    request: QboChartRequest,
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    """Pull chart of accounts from QBO. Thin wrapper over the existing
    import_chart_of_accounts service (reuse, don't duplicate).

    Records the sync to quickbooks_sync_runs so the data-import surface
    can show "last synced" persistently across sessions.
    """
    enforce_entity_code(_user, request.entity_code)
    try:
        with db_session() as session:
            entity = get_entity_by_code(session, request.entity_code)
            if not entity:
                raise HTTPException(404, f"Unknown entity: {request.entity_code}")
            result = await import_chart_of_accounts(session, request.entity_code)

            # Log to quickbooks_sync_runs. The active QBO connection is
            # the one import_chart_of_accounts just used; we re-query
            # rather than thread it through the service signature.
            conn = session.execute(
                text(
                    """
                    SELECT id FROM quickbooks_connections
                     WHERE entity_id = :eid AND is_active = TRUE
                     ORDER BY connected_at DESC LIMIT 1
                    """
                ),
                {"eid": entity["id"]},
            ).mappings().first()
            if conn:
                session.execute(
                    text(
                        """
                        INSERT INTO quickbooks_sync_runs (
                            entity_id, quickbooks_connection_id, sync_type,
                            status, summary_json, started_at, finished_at
                        ) VALUES (
                            :eid, :cid, 'chart_of_accounts', 'complete',
                            :sj, NOW(), NOW()
                        )
                        """
                    ),
                    {
                        "eid": entity["id"],
                        "cid": conn["id"],
                        "sj": json.dumps({
                            "imported_count": int(result["imported_count"]),
                            "bank_account_count": int(result["bank_account_count"]),
                        }),
                    },
                )

            return {
                "entity_code": request.entity_code,
                "account_count": result["imported_count"],
                "bank_account_count": result["bank_account_count"],
                "source": "qbo",
            }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


# --------------------------------------------------------------------------
# 3. Opening balances
# --------------------------------------------------------------------------


@router.post("/opening-balances/upload")
async def upload_opening_balances(
    background_tasks: BackgroundTasks,
    entity_code: str = Form(...),
    actor_email: str = Form(...),
    as_of_date: str = Form(...),
    file: UploadFile = File(...),
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    """Kick off trial-balance parsing in the background. Returns
    immediately with a job_id; the wizard polls
    /opening-balances/progress/{job_id} until status='complete'.
    """
    enforce_entity_code(_user, entity_code)
    try:
        _parse_iso_date(as_of_date, "as_of_date")
        file_bytes = await file.read()
        filename = file.filename or ""
        with db_session() as session:
            entity = get_entity_by_code(session, entity_code)
            if not entity:
                raise HTTPException(404, f"Unknown entity: {entity_code}")
            job_id = _create_job(
                session,
                entity_id=str(entity["id"]),
                job_type="parse_trial_balance",
                actor_email=actor_email,
            )
        background_tasks.add_task(
            _run_parse_tb,
            job_id=job_id,
            file_bytes=file_bytes,
            filename=filename,
            entity_code=entity_code,
            as_of_date=as_of_date,
        )
        return {"job_id": job_id, "status": "pending"}
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:
        logger.exception("upload_opening_balances failed")
        raise HTTPException(400, str(exc)) from exc


@router.get("/opening-balances/progress/{job_id}")
def opening_balances_progress(
    job_id: str = Path(...),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    with db_session() as session:
        row = session.execute(
            text(
                """
                SELECT id, job_type, status, pct_complete, current_step,
                       result, error_message
                  FROM background_jobs
                 WHERE id = :id
                 LIMIT 1
                """
            ),
            {"id": job_id},
        ).mappings().first()
        if not row:
            raise HTTPException(404, f"Unknown job: {job_id}")
        result = dict(row.get("result") or {})
        preview = result.get("preview") or {}
        return {
            "job_id": str(row["id"]),
            "job_type": row["job_type"],
            "status": row["status"],
            "pct_complete": int(row["pct_complete"] or 0),
            "current_step": row.get("current_step"),
            "preview": preview or None,
            "filename": result.get("filename"),
            "entity_code": result.get("entity_code"),
            "as_of_date": result.get("as_of_date"),
            "error": row.get("error_message"),
        }


class OpeningConfirmRequest(BaseModel):
    entity_code: str
    actor_email: str
    as_of_date: str
    tb_lines: list[dict[str, Any]]


@router.post("/opening-balances/confirm")
def confirm_opening_balances(
    request: OpeningConfirmRequest,
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    enforce_entity_code(_user, request.entity_code)
    try:
        as_of = _parse_iso_date(request.as_of_date, "as_of_date")
        with db_session() as session:
            entity = get_entity_by_code(session, request.entity_code)
            if not entity:
                raise HTTPException(404, f"Unknown entity: {request.entity_code}")
            result = import_opening_balances(
                session,
                entity_id=str(entity["id"]),
                entity_code=request.entity_code,
                as_of_date=as_of,
                tb_lines=request.tb_lines,
                actor_email=request.actor_email,
            )
            return {"entity_code": request.entity_code, **result}
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


class QboOpeningRequest(BaseModel):
    entity_code: str
    actor_email: str
    as_of_date: str


@router.post("/opening-balances/qbo")
async def pull_opening_balances_qbo(
    request: QboOpeningRequest,
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    enforce_entity_code(_user, request.entity_code)
    try:
        as_of = _parse_iso_date(request.as_of_date, "as_of_date")
        with db_session() as session:
            entity = get_entity_by_code(session, request.entity_code)
            if not entity:
                raise HTTPException(404, f"Unknown entity: {request.entity_code}")
            result = await import_trial_balance_from_qbo(
                session,
                entity_id=str(entity["id"]),
                entity_code=request.entity_code,
                as_of_date=as_of,
                actor_email=request.actor_email,
            )
            return {"entity_code": request.entity_code, **result}
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


# --------------------------------------------------------------------------
# 4. GL history — background jobs
# --------------------------------------------------------------------------


@router.post("/gl-history/upload")
async def upload_gl_history(
    background_tasks: BackgroundTasks,
    entity_code: str = Form(...),
    actor_email: str = Form(...),
    date_from: str = Form(...),
    date_to: str = Form(...),
    file: UploadFile = File(...),
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    """Kick off a background GL import from a file. Returns immediately
    with a job_id; the wizard polls /gl-history/progress/{job_id}.
    """
    enforce_entity_code(_user, entity_code)
    try:
        df = _parse_iso_date(date_from, "date_from")
        dt = _parse_iso_date(date_to, "date_to")
        file_bytes = await file.read()
        filename = file.filename or ""

        with db_session() as session:
            entity = get_entity_by_code(session, entity_code)
            if not entity:
                raise HTTPException(404, f"Unknown entity: {entity_code}")
            job_id = _create_job(
                session,
                entity_id=str(entity["id"]),
                job_type="gl_import_file",
                actor_email=actor_email,
            )
        background_tasks.add_task(
            _run_file_gl_import,
            job_id=job_id,
            entity_code=entity_code,
            entity_id=str(entity["id"]),
            file_bytes=file_bytes,
            filename=filename,
            actor_email=actor_email,
        )
        return {"job_id": job_id, "status": "pending"}
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


class QboGLRequest(BaseModel):
    entity_code: str
    actor_email: str
    date_from: str
    date_to: str


@router.post("/gl-history/qbo")
def pull_gl_history_qbo(
    request: QboGLRequest,
    background_tasks: BackgroundTasks,
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    enforce_entity_code(_user, request.entity_code)
    try:
        df = _parse_iso_date(request.date_from, "date_from")
        dt = _parse_iso_date(request.date_to, "date_to")
        with db_session() as session:
            entity = get_entity_by_code(session, request.entity_code)
            if not entity:
                raise HTTPException(404, f"Unknown entity: {request.entity_code}")
            job_id = _create_job(
                session,
                entity_id=str(entity["id"]),
                job_type="gl_import_qbo",
                actor_email=request.actor_email,
            )
        background_tasks.add_task(
            _run_qbo_gl_import,
            job_id=job_id,
            entity_code=request.entity_code,
            entity_id=str(entity["id"]),
            date_from=df,
            date_to=dt,
            actor_email=request.actor_email,
        )
        return {"job_id": job_id, "status": "pending"}
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/gl-history/preview")
async def preview_gl_history(
    entity_code: str = Form(...),
    actor_email: str = Form(...),
    file: UploadFile = File(...),
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    """Parse a GL file and return a summary WITHOUT writing anything.
    Used by /settings/data-import to show the dealer what they're about
    to import before they commit to it.

    Returns:
      - periods_detected: [{month, transaction_count}, ...]
      - total_transactions, date_range
      - accounts_found, unmatched_accounts, unmatched_codes
    """
    enforce_entity_code(_user, entity_code)
    del actor_email  # accepted for symmetry with /upload, not used in preview
    try:
        file_bytes = await file.read()
        filename = file.filename or ""

        from .. import services_onboarding as _svc

        # parse_gl_file already tries the regex/CSV fallback first and
        # falls through to Claude. Synchronous — preview is one round
        # trip, no job polling. Big files may be slow but the upload
        # path has the same constraint.
        lines = _svc.parse_gl_file(file_bytes, filename)

        if not lines:
            return {
                "entity_code": entity_code,
                "periods_detected": [],
                "total_transactions": 0,
                "date_range": None,
                "accounts_found": 0,
                "unmatched_accounts": 0,
                "unmatched_codes": [],
            }

        # Periods — bucket by (year, month) and count.
        from collections import Counter
        month_counts: Counter[tuple[int, int]] = Counter()
        codes_seen: set[str] = set()
        earliest = None
        latest = None
        for line in lines:
            d = line["transaction_date"]
            month_counts[(d.year, d.month)] += 1
            code = str(line.get("account_code") or "").strip()
            if code:
                codes_seen.add(code)
            if earliest is None or d < earliest:
                earliest = d
            if latest is None or d > latest:
                latest = d

        import calendar as _cal
        periods_detected = [
            {
                "month": f"{_cal.month_abbr[m]} {y}",
                "year": y,
                "month_num": m,
                "transaction_count": int(n),
            }
            for (y, m), n in sorted(month_counts.items())
        ]

        # Compare against the entity's active chart so we can warn
        # about codes that'll route to suspense at import time.
        with db_session() as session:
            entity = get_entity_by_code(session, entity_code)
            if not entity:
                raise HTTPException(404, f"Unknown entity: {entity_code}")
            valid_codes = _svc._load_valid_account_codes(
                session, str(entity["id"])
            )

        unmatched_codes = sorted(c for c in codes_seen if c not in valid_codes)

        return {
            "entity_code": entity_code,
            "periods_detected": periods_detected,
            "total_transactions": len(lines),
            "date_range": {
                "start": earliest.isoformat() if earliest else None,
                "end": latest.isoformat() if latest else None,
            },
            "accounts_found": len(codes_seen),
            "unmatched_accounts": len(unmatched_codes),
            "unmatched_codes": unmatched_codes[:50],
        }
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:
        logger.exception("preview_gl_history failed")
        raise HTTPException(400, str(exc)) from exc


@router.get("/gl-history/progress/{job_id}")
def gl_history_progress(
    job_id: str = Path(...),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    with db_session() as session:
        row = session.execute(
            text(
                """
                SELECT id, job_type, status, pct_complete, current_step,
                       result, error_message, created_at, started_at, completed_at
                  FROM background_jobs
                 WHERE id = :id
                 LIMIT 1
                """
            ),
            {"id": job_id},
        ).mappings().first()
        if not row:
            raise HTTPException(404, f"Unknown job: {job_id}")
        result = dict(row.get("result") or {})
        # suspense_entries is written by import_gl_history_from_lines
        # when account codes from the file aren't in the entity's
        # chart of accounts. Surfaces in the post-import warning card
        # so the dealer knows which codes to add or reclassify.
        suspense = result.get("suspense_entries") or []
        return {
            "job_id": str(row["id"]),
            "job_type": row["job_type"],
            "status": row["status"],
            "pct_complete": int(row["pct_complete"] or 0),
            "current_step": row.get("current_step"),
            "months_imported": int(result.get("months_imported") or 0),
            "lines_created": int(result.get("lines_created") or 0),
            "batches_created": int(result.get("batches_created") or 0),
            "suspense_entries": suspense if isinstance(suspense, list) else [],
            "error": row.get("error_message"),
        }


# --------------------------------------------------------------------------
# 5. Complete
# --------------------------------------------------------------------------


class CompleteOnboardingRequest(BaseModel):
    entity_code: str
    actor_email: str


@router.post("/complete")
def complete_onboarding(
    request: CompleteOnboardingRequest,
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    """Mark onboarding complete. Creates the first live period (today's
    month) if no open period exists, and runs the assistant memory
    bootstrap from any imported GL history.
    """
    enforce_entity_code(_user, request.entity_code)
    try:
        with db_session() as session:
            entity = get_entity_by_code(session, request.entity_code)
            if not entity:
                raise HTTPException(404, f"Unknown entity: {request.entity_code}")

            session.execute(
                text(
                    """
                    UPDATE entities
                       SET onboarding_complete = TRUE,
                           onboarding_completed_at = NOW()
                     WHERE id = :eid
                    """
                ),
                {"eid": entity["id"]},
            )

            # First live period — current month, status 'draft' (open).
            today = _today()
            period_start = DateType(today.year, today.month, 1)
            last_day = calendar.monthrange(today.year, today.month)[1]
            period_end = DateType(today.year, today.month, last_day)
            existing = session.execute(
                text(
                    """
                    SELECT id FROM accounting_periods
                     WHERE entity_id = :eid AND period_start = :ps AND period_end = :pe
                    """
                ),
                {"eid": entity["id"], "ps": period_start, "pe": period_end},
            ).mappings().first()
            if not existing:
                session.execute(
                    text(
                        """
                        INSERT INTO accounting_periods (
                            entity_id, period_label, period_start, period_end, status
                        ) VALUES (
                            :eid, :label, :ps, :pe, 'draft'
                        )
                        """
                    ),
                    {
                        "eid": entity["id"],
                        "label": period_start.strftime("%b %Y"),
                        "ps": period_start,
                        "pe": period_end,
                    },
                )

            # Memory bootstrap from any historical GL we've imported.
            try:
                learn = learn_from_gl_history(
                    session,
                    entity_id=str(entity["id"]),
                    entity_code=request.entity_code,
                )
            except Exception:
                logger.exception("learn_from_gl_history failed — non-fatal")
                learn = {"vendors_learned": 0, "patterns_found": 0, "observations_created": 0}

            counts = session.execute(
                text(
                    """
                    SELECT
                      (SELECT COUNT(*) FROM accounts WHERE entity_id = :eid) AS accounts_loaded,
                      (SELECT COUNT(jl.id)
                         FROM journal_lines jl
                         JOIN journal_batches jb ON jb.id = jl.journal_batch_id
                        WHERE jb.entity_id = :eid) AS journal_lines_loaded
                    """
                ),
                {"eid": entity["id"]},
            ).mappings().first() or {}

            return {
                "entity_code": request.entity_code,
                "first_period": {
                    "period_start": period_start.isoformat(),
                    "period_end": period_end.isoformat(),
                },
                "accounts_loaded": int(counts.get("accounts_loaded") or 0),
                "journal_lines_loaded": int(counts.get("journal_lines_loaded") or 0),
                "vendors_learned": int(learn.get("vendors_learned") or 0),
            }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


# --------------------------------------------------------------------------
# Background-job helpers
# --------------------------------------------------------------------------


def _create_job(
    session, *, entity_id: str, job_type: str, actor_email: str
) -> str:
    """Insert a pending job row and return its id."""
    row = session.execute(
        text(
            """
            INSERT INTO background_jobs (entity_id, job_type, actor_email)
            VALUES (:eid, :jt, :ae)
            RETURNING id
            """
        ),
        {"eid": entity_id, "jt": job_type, "ae": actor_email},
    ).mappings().first()
    return str(row["id"])


def _update_job(
    job_id: str,
    *,
    status: str | None = None,
    pct: int | None = None,
    current_step: str | None = None,
    result: dict[str, Any] | None = None,
    error_message: str | None = None,
    started: bool = False,
    completed: bool = False,
) -> None:
    """Update a job row in its own short transaction so the polling
    endpoint can read live progress while the import is still running.
    """
    sets: list[str] = []
    params: dict[str, Any] = {"id": job_id}
    if status is not None:
        sets.append("status = :status")
        params["status"] = status
    if pct is not None:
        sets.append("pct_complete = :pct")
        params["pct"] = max(0, min(100, int(pct)))
    if current_step is not None:
        sets.append("current_step = :step")
        params["step"] = current_step[:200]
    if result is not None:
        sets.append("result = :result")
        params["result"] = json.dumps(_jsonable(result))
    if error_message is not None:
        sets.append("error_message = :err")
        params["err"] = error_message[:1000]
    if started:
        sets.append("started_at = COALESCE(started_at, NOW())")
    if completed:
        sets.append("completed_at = NOW()")

    if not sets:
        return

    session = SessionLocal()
    try:
        session.execute(
            text(f"UPDATE background_jobs SET {', '.join(sets)} WHERE id = :id"),
            params,
        )
        session.commit()
    except Exception:
        session.rollback()
        logger.exception("Failed to update background_job %s", job_id)
    finally:
        session.close()


def _run_qbo_gl_import(
    *,
    job_id: str,
    entity_code: str,
    entity_id: str,
    date_from: DateType,
    date_to: DateType,
    actor_email: str,
) -> None:
    """Background worker for QBO GL pulls. Runs in its own event loop
    + session. Updates job progress along the way.
    """
    _update_job(job_id, status="running", started=True, pct=0,
                current_step="Starting QuickBooks GL pull")

    def progress(label: str, pct: int) -> None:
        _update_job(job_id, current_step=label, pct=pct)

    try:
        result = asyncio.run(_qbo_import_async(
            entity_id=entity_id,
            entity_code=entity_code,
            date_from=date_from,
            date_to=date_to,
            actor_email=actor_email,
            progress_callback=progress,
        ))
        _update_job(
            job_id,
            status="complete",
            pct=100,
            current_step="Import complete",
            result=result,
            completed=True,
        )
    except Exception as exc:
        logger.exception("QBO GL import failed for job %s", job_id)
        _update_job(
            job_id,
            status="error",
            error_message=str(exc),
            completed=True,
        )


async def _qbo_import_async(
    *,
    entity_id: str,
    entity_code: str,
    date_from: DateType,
    date_to: DateType,
    actor_email: str,
    progress_callback,
) -> dict[str, Any]:
    with db_session() as session:
        return await import_gl_history_from_qbo(
            session,
            entity_id=entity_id,
            entity_code=entity_code,
            date_from=date_from,
            date_to=date_to,
            actor_email=actor_email,
            progress_callback=progress_callback,
        )


def _run_parse_chart(
    *,
    job_id: str,
    file_bytes: bytes,
    filename: str,
    entity_code: str,
) -> None:
    """Background worker for chart-of-accounts file parsing."""
    _update_job(job_id, status="running", started=True, pct=5,
                current_step="Archiving source file")
    # Best-effort R2 archive — failures here don't block the parse.
    object_key = storage_service.upload_file(
        file_bytes=file_bytes,
        original_filename=filename,
        entity_code=entity_code,
        document_type="onboarding-chart",
        content_type=_content_type_for(filename),
    )
    _update_job(job_id, pct=10, current_step="Parsing chart of accounts")
    try:
        preview = parse_chart_of_accounts(file_bytes, filename)
        _update_job(
            job_id,
            status="complete",
            pct=100,
            current_step="Parse complete",
            result={
                "preview": preview,
                "filename": filename,
                "entity_code": entity_code,
                "file_path": object_key,
            },
            completed=True,
        )
    except ValueError as exc:
        logger.warning("Chart parse failed for job %s: %r", job_id, exc)
        _update_job(
            job_id, status="error", error_message=str(exc), completed=True
        )
    except Exception as exc:
        logger.exception("Chart parse worker crashed for job %s", job_id)
        _update_job(
            job_id, status="error", error_message=str(exc), completed=True
        )


def _run_parse_tb(
    *,
    job_id: str,
    file_bytes: bytes,
    filename: str,
    entity_code: str,
    as_of_date: str,
) -> None:
    """Background worker for trial-balance file parsing."""
    _update_job(job_id, status="running", started=True, pct=5,
                current_step="Archiving source file")
    object_key = storage_service.upload_file(
        file_bytes=file_bytes,
        original_filename=filename,
        entity_code=entity_code,
        document_type="onboarding-opening-balance",
        content_type=_content_type_for(filename),
    )
    _update_job(job_id, pct=10, current_step="Parsing trial balance")
    try:
        preview = parse_trial_balance(file_bytes, filename)
        _update_job(
            job_id,
            status="complete",
            pct=100,
            current_step=(
                "Parse complete — balanced"
                if preview.get("balanced")
                else "Parse complete — out of balance"
            ),
            result={
                "preview": preview,
                "filename": filename,
                "entity_code": entity_code,
                "as_of_date": as_of_date,
                "file_path": object_key,
            },
            completed=True,
        )
    except ValueError as exc:
        logger.warning("TB parse failed for job %s: %r", job_id, exc)
        _update_job(
            job_id, status="error", error_message=str(exc), completed=True
        )
    except Exception as exc:
        logger.exception("TB parse worker crashed for job %s", job_id)
        _update_job(
            job_id, status="error", error_message=str(exc), completed=True
        )


def _content_type_for(filename: str) -> str:
    name = (filename or "").lower()
    if name.endswith(".csv"):
        return "text/csv"
    if name.endswith(".xlsx") or name.endswith(".xls") or name.endswith(".xlsm"):
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if name.endswith(".ods"):
        return "application/vnd.oasis.opendocument.spreadsheet"
    if name.endswith(".pdf"):
        return "application/pdf"
    if name.endswith(".txt"):
        return "text/plain"
    return "application/octet-stream"


def _run_file_gl_import(
    *,
    job_id: str,
    entity_code: str,
    entity_id: str,
    file_bytes: bytes,
    filename: str,
    actor_email: str,
) -> None:
    """Background worker for file-upload GL imports."""
    _update_job(job_id, status="running", started=True, pct=0,
                current_step="Archiving source file")
    object_key = storage_service.upload_file(
        file_bytes=file_bytes,
        original_filename=filename,
        entity_code=entity_code,
        document_type="onboarding-gl",
        content_type=_content_type_for(filename),
    )
    _update_job(job_id, pct=5, current_step="Parsing file with Claude")

    def progress(label: str, pct: int) -> None:
        _update_job(job_id, current_step=label, pct=pct)

    try:
        lines = parse_gl_file(file_bytes, filename)
        _update_job(job_id, pct=40, current_step=f"Parsed {len(lines)} lines — writing journals")
        with db_session() as session:
            result = import_gl_history_from_lines(
                session,
                entity_id=entity_id,
                entity_code=entity_code,
                lines=lines,
                actor_email=actor_email,
                progress_callback=lambda label, pct: progress(label, 40 + int(pct * 0.6)),
            )
        result["file_path"] = object_key
        _update_job(
            job_id,
            status="complete",
            pct=100,
            current_step="Import complete",
            result=result,
            completed=True,
        )
    except Exception as exc:
        logger.exception("File GL import failed for job %s", job_id)
        _update_job(
            job_id,
            status="error",
            error_message=str(exc),
            completed=True,
        )


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _parse_iso_date(value: str, field_name: str) -> DateType:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD (got {value!r})") from exc


def _today() -> DateType:
    return datetime.utcnow().date()


def _jsonable(value: Any) -> Any:
    from decimal import Decimal
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (DateType, datetime)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value
