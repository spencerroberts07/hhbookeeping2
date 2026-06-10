"""
Wage Cost Planner — HTTP routes.

Prefix: /api/wage-planner
Tier:   professional (role: bookkeeper minimum; admin for settings writes)

Endpoints:
    GET    /settings                      get annual settings + salaried roster
    PUT    /settings                      upsert annual settings
    GET    /pay-periods                   get canonical pay-period calendar
    PUT    /pay-periods/{period_number}   upsert one calendar row
    POST   /pay-periods/backfill          backfill calendar from payroll_runs
    GET    /plan                          compute + return 26-period planner table
    POST   /refresh                       manually refresh actuals for one period
    POST   /override                      apply manual override fields for a period
    POST   /min-wage-impact               roster impact of a proposed min-wage rate
    GET    /snapshots                     list archived Excel snapshots
    GET    /snapshots/latest              latest snapshot for a fiscal year
    GET    /snapshots/{snapshot_id}/download  presigned R2 download URL
    GET    /snapshots/{snapshot_id}/excel     inline Excel bytes fallback
"""
from __future__ import annotations

import logging
from datetime import date as DateType
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Path, Response
from pydantic import BaseModel, Field

from ..db import db_session
from ..services import get_entity_by_code
from ..services_auth import enforce_entity_code, require_role
from ..services_wage_planner import (
    apply_manual_override,
    backfill_calendar_from_runs,
    compute_plan,
    get_pay_period_calendar,
    get_settings,
    min_wage_impact,
    refresh_period_actuals,
    upsert_pay_period,
    upsert_settings,
    _fiscal_year_for_date,
)

router = APIRouter(prefix="/api/wage-planner", tags=["wage-planner"])
_log = logging.getLogger(__name__)


def _entity_or_404(session, entity_code: str) -> dict:
    row = session.execute(
        __import__("sqlalchemy").text(
            """
            SELECT e.id, e.entity_code, e.entity_name,
                   COALESCE(es.fiscal_year_end_month, 9) AS fy_end_month,
                   COALESCE(es.fiscal_year_end_day, 30)  AS fy_end_day
            FROM entities e
            LEFT JOIN entity_settings es ON es.entity_id = e.id
            WHERE e.entity_code = :ec
            """
        ),
        {"ec": entity_code},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail=f"Entity not found: {entity_code}")
    return dict(row)


def _current_fiscal_year(entity_row: dict) -> int:
    today = DateType.today()
    return _fiscal_year_for_date(
        today, entity_row["fy_end_month"], entity_row["fy_end_day"]
    )


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

class SalariedStaffItem(BaseModel):
    employee_name: str
    annual_salary: float = 0.0
    bonus: float = 0.0
    assumed_hours_per_period: int = 80


class SettingsRequest(BaseModel):
    entity_code: str
    fiscal_year: int | None = None          # defaults to current FY
    target_wage_pct: float                  # e.g. 0.11
    forecast_sales_change: float = 0.0      # e.g. -0.10
    avg_hourly_wage: float
    benefits_pct: float = 0.04
    distribution_basis: str = "prior_year"
    notes: str | None = None
    salaried_staff: list[SalariedStaffItem] = Field(default_factory=list)


@router.get("/settings")
def get_settings_route(
    entity_code: str = Query(...),
    fiscal_year: int | None = Query(None),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, entity_code)
    with db_session() as session:
        entity = _entity_or_404(session, entity_code)
        fy = fiscal_year or _current_fiscal_year(entity)
        result = get_settings(session, entity_id=entity["id"], fiscal_year=fy)
        if result is None:
            return {"settings": None, "fiscal_year": fy}
        return {"settings": result, "fiscal_year": fy}


@router.put("/settings")
def put_settings(
    body: SettingsRequest,
    _user: dict = Depends(require_role("admin")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    if body.target_wage_pct <= 0 or body.target_wage_pct > 1:
        raise HTTPException(status_code=400, detail="target_wage_pct must be between 0 and 1")
    if body.avg_hourly_wage <= 0:
        raise HTTPException(status_code=400, detail="avg_hourly_wage must be positive")
    try:
        with db_session() as session:
            entity = _entity_or_404(session, body.entity_code)
            fy = body.fiscal_year or _current_fiscal_year(entity)
            result = upsert_settings(
                session,
                entity_id=entity["id"],
                fiscal_year=fy,
                target_wage_pct=body.target_wage_pct,
                forecast_sales_change=body.forecast_sales_change,
                avg_hourly_wage=body.avg_hourly_wage,
                benefits_pct=body.benefits_pct,
                distribution_basis=body.distribution_basis,
                notes=body.notes,
                salaried_staff=[s.model_dump() for s in body.salaried_staff],
            )
        return {"settings": result, "fiscal_year": fy}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Pay-period calendar
# ---------------------------------------------------------------------------

class PayPeriodRequest(BaseModel):
    entity_code: str
    fiscal_year: int | None = None
    period_start: str   # YYYY-MM-DD
    period_end: str
    pay_date: str | None = None


@router.get("/pay-periods")
def get_pay_periods(
    entity_code: str = Query(...),
    fiscal_year: int | None = Query(None),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, entity_code)
    with db_session() as session:
        entity = _entity_or_404(session, entity_code)
        fy = fiscal_year or _current_fiscal_year(entity)
        rows = get_pay_period_calendar(session, entity_id=entity["id"], fiscal_year=fy)
        return {"fiscal_year": fy, "periods": rows}


@router.put("/pay-periods/{period_number}")
def put_pay_period(
    period_number: int = Path(..., ge=1, le=26),
    body: PayPeriodRequest = ...,
    _user: dict = Depends(require_role("admin")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        ps = DateType.fromisoformat(body.period_start)
        pe = DateType.fromisoformat(body.period_end)
        pd = DateType.fromisoformat(body.pay_date) if body.pay_date else None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date: {exc}") from exc
    with db_session() as session:
        entity = _entity_or_404(session, body.entity_code)
        fy = body.fiscal_year or _current_fiscal_year(entity)
        row = upsert_pay_period(
            session,
            entity_id=entity["id"],
            fiscal_year=fy,
            period_number=period_number,
            period_start=ps,
            period_end=pe,
            pay_date=pd,
        )
        return row


class BackfillRequest(BaseModel):
    entity_code: str


@router.post("/pay-periods/backfill")
def post_backfill(
    body: BackfillRequest,
    _user: dict = Depends(require_role("admin")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    with db_session() as session:
        entity = _entity_or_404(session, body.entity_code)
        count = backfill_calendar_from_runs(
            session,
            entity_id=entity["id"],
            fy_end_month=entity["fy_end_month"],
            fy_end_day=entity["fy_end_day"],
        )
        return {"inserted": count, "message": f"Backfilled {count} rows from payroll_runs"}


# ---------------------------------------------------------------------------
# Plan (the dashboard table)
# ---------------------------------------------------------------------------

@router.get("/plan")
def get_plan(
    entity_code: str = Query(...),
    fiscal_year: int | None = Query(None),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, entity_code)
    with db_session() as session:
        entity = _entity_or_404(session, entity_code)
        fy = fiscal_year or _current_fiscal_year(entity)
        plan = compute_plan(session, entity_id=entity["id"], fiscal_year=fy)

    # Serialize Decimals to strings for JSON transport
    def _serial(obj):
        from decimal import Decimal as _D
        if isinstance(obj, _D):
            return str(obj)
        if isinstance(obj, DateType):
            return obj.isoformat()
        raise TypeError(f"Not serializable: {type(obj)}")

    import json
    return json.loads(json.dumps(plan, default=_serial))


# ---------------------------------------------------------------------------
# Refresh actuals (manual trigger)
# ---------------------------------------------------------------------------

class RefreshRequest(BaseModel):
    entity_code: str
    fiscal_year: int | None = None
    period_number: int
    payroll_run_id: str
    actor_email: str | None = None


@router.post("/refresh")
def post_refresh(
    body: RefreshRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    try:
        with db_session() as session:
            entity = _entity_or_404(session, body.entity_code)
            fy = body.fiscal_year or _current_fiscal_year(entity)
            result = refresh_period_actuals(
                session,
                entity_id=entity["id"],
                fiscal_year=fy,
                period_number=body.period_number,
                payroll_run_id=body.payroll_run_id,
                actor_email=body.actor_email or _user.get("email"),
            )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Manual override
# ---------------------------------------------------------------------------

class OverrideRequest(BaseModel):
    entity_code: str
    fiscal_year: int | None = None
    period_number: int
    actual_sales: float | None = None
    actual_gross_wages: float | None = None
    actual_stat_pay: float | None = None
    actual_hours: float | None = None


@router.post("/override")
def post_override(
    body: OverrideRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    overrides = {
        k: v for k, v in {
            "actual_sales": body.actual_sales,
            "actual_gross_wages": body.actual_gross_wages,
            "actual_stat_pay": body.actual_stat_pay,
            "actual_hours": body.actual_hours,
        }.items() if v is not None
    }
    if not overrides:
        raise HTTPException(status_code=400, detail="At least one override field is required")
    try:
        with db_session() as session:
            entity = _entity_or_404(session, body.entity_code)
            fy = body.fiscal_year or _current_fiscal_year(entity)
            result = apply_manual_override(
                session,
                entity_id=entity["id"],
                fiscal_year=fy,
                period_number=body.period_number,
                overrides=overrides,
            )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Minimum-wage impact calculator
# ---------------------------------------------------------------------------

class MinWageRequest(BaseModel):
    entity_code: str
    new_min_wage: float


@router.post("/min-wage-impact")
def post_min_wage_impact(
    body: MinWageRequest,
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, body.entity_code)
    if body.new_min_wage <= 0:
        raise HTTPException(status_code=400, detail="new_min_wage must be positive")
    with db_session() as session:
        entity = _entity_or_404(session, body.entity_code)
        result = min_wage_impact(
            session, entity_id=entity["id"], new_min_wage=body.new_min_wage
        )
    return result


# ---------------------------------------------------------------------------
# Snapshots — list, latest, download
# ---------------------------------------------------------------------------

@router.get("/snapshots")
def get_snapshots(
    entity_code: str = Query(...),
    fiscal_year: int | None = Query(None),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, entity_code)
    from sqlalchemy import text
    with db_session() as session:
        entity = _entity_or_404(session, entity_code)
        fy = fiscal_year or _current_fiscal_year(entity)
        rows = session.execute(
            text(
                """
                SELECT id, fiscal_year, pay_period_number, r2_object_key,
                       status, generated_at, generated_by, error_msg, created_at
                FROM wage_planner_snapshots
                WHERE entity_id = :eid AND fiscal_year = :fy
                ORDER BY pay_period_number DESC
                """
            ),
            {"eid": entity["id"], "fy": fy},
        ).mappings().all()
        snaps = []
        for r in rows:
            snaps.append({
                "id": str(r["id"]),
                "fiscal_year": r["fiscal_year"],
                "pay_period_number": r["pay_period_number"],
                "status": r["status"],
                "generated_at": r["generated_at"].isoformat() if r["generated_at"] else None,
                "generated_by": r["generated_by"],
                "error_msg": r["error_msg"],
                "has_file": bool(r["r2_object_key"]),
            })
    return {"fiscal_year": fy, "snapshots": snaps}


@router.get("/snapshots/latest")
def get_latest_snapshot(
    entity_code: str = Query(...),
    fiscal_year: int | None = Query(None),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    enforce_entity_code(_user, entity_code)
    from sqlalchemy import text
    from ..services_storage import storage_service
    with db_session() as session:
        entity = _entity_or_404(session, entity_code)
        fy = fiscal_year or _current_fiscal_year(entity)
        row = session.execute(
            text(
                """
                SELECT id, pay_period_number, r2_object_key, status, generated_at
                FROM wage_planner_snapshots
                WHERE entity_id = :eid AND fiscal_year = :fy AND status = 'ready'
                ORDER BY pay_period_number DESC
                LIMIT 1
                """
            ),
            {"eid": entity["id"], "fy": fy},
        ).mappings().first()
        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"No ready snapshot found for entity {entity_code} FY{fy}",
            )
        url = storage_service.get_presigned_url(row["r2_object_key"], expires_in=3600)
        return {
            "id": str(row["id"]),
            "pay_period_number": row["pay_period_number"],
            "status": row["status"],
            "generated_at": row["generated_at"].isoformat() if row["generated_at"] else None,
            "download_url": url,
        }


@router.get("/snapshots/{snapshot_id}/download")
def get_snapshot_download(
    snapshot_id: str = Path(...),
    entity_code: str = Query(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> dict[str, Any]:
    """Return a presigned R2 URL for the snapshot. Falls back to /excel if R2
    is not configured."""
    enforce_entity_code(_user, entity_code)
    from sqlalchemy import text
    from ..services_storage import storage_service
    with db_session() as session:
        entity = _entity_or_404(session, entity_code)
        row = session.execute(
            text(
                """
                SELECT id, pay_period_number, fiscal_year, r2_object_key, status
                FROM wage_planner_snapshots
                WHERE id = :sid AND entity_id = :eid
                """
            ),
            {"sid": snapshot_id, "eid": entity["id"]},
        ).mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="Snapshot not found")
        if row["status"] != "ready":
            raise HTTPException(
                status_code=409,
                detail=f"Snapshot status is '{row['status']}' — not ready for download",
            )
        url = storage_service.get_presigned_url(row["r2_object_key"], expires_in=3600)
        if url:
            return {"download_url": url}
        # R2 offline — redirect to inline excel endpoint
        return {
            "download_url": None,
            "fallback": (
                f"/api/wage-planner/snapshots/{snapshot_id}/excel"
                f"?entity_code={entity_code}"
            ),
        }


@router.get("/snapshots/{snapshot_id}/excel")
def get_snapshot_excel_inline(
    snapshot_id: str = Path(...),
    entity_code: str = Query(...),
    _user: dict = Depends(require_role("bookkeeper")),
) -> Any:
    """Generate and return Excel bytes inline (fallback when R2 is unavailable)."""
    enforce_entity_code(_user, entity_code)
    from sqlalchemy import text
    from ..services_wage_planner_excel import generate_wage_planner_excel
    with db_session() as session:
        entity = _entity_or_404(session, entity_code)
        row = session.execute(
            text(
                "SELECT fiscal_year, pay_period_number FROM wage_planner_snapshots "
                "WHERE id = :sid AND entity_id = :eid"
            ),
            {"sid": snapshot_id, "eid": entity["id"]},
        ).mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="Snapshot not found")
        try:
            xlsx_bytes = generate_wage_planner_excel(
                session, entity_id=entity["id"], fiscal_year=row["fiscal_year"]
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    filename = (
        f"wage_planner_FY{row['fiscal_year']}"
        f"_p{row['pay_period_number']:02d}_{entity_code}.xlsx"
    )
    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# On-demand Excel (not a saved snapshot — always fresh)
# ---------------------------------------------------------------------------

@router.get("/excel")
def get_excel_fresh(
    entity_code: str = Query(...),
    fiscal_year: int | None = Query(None),
    _user: dict = Depends(require_role("bookkeeper")),
) -> Any:
    """Generate and return a fresh Excel workbook (upload to R2 + presign,
    or inline bytes if R2 is not configured)."""
    enforce_entity_code(_user, entity_code)
    from ..services_wage_planner_excel import generate_wage_planner_excel
    from ..services_storage import content_type_for, storage_service
    with db_session() as session:
        entity = _entity_or_404(session, entity_code)
        fy = fiscal_year or _current_fiscal_year(entity)
        try:
            xlsx_bytes = generate_wage_planner_excel(
                session, entity_id=entity["id"], fiscal_year=fy
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    filename = f"wage_planner_FY{fy}_{entity_code}.xlsx"
    r2_key = storage_service.upload_file(
        file_bytes=xlsx_bytes,
        original_filename=filename,
        entity_code=entity_code,
        document_type="wage-planner",
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
