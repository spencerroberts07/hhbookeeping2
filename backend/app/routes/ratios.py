"""
Ratio engine endpoints (Phase 2C). Read-only analytics; computes the built-in
ratio library current vs prior-year, with per-entity enable/threshold config.
"""
from __future__ import annotations

from datetime import date as DateType
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text

from ..db import db_session
from ..services import get_entity_by_code
from ..services_auth import require_role
from ..services_ratios import (
    RATIO_META,
    build_financials_context,
    compute_builtin_ratios,
    get_account_roles,
    account_roles_detail,
    seed_account_roles,
    resolve_annual_debt_service,
)

router = APIRouter(prefix="/api/dashboard/ratios", tags=["ratios"])


def _shift_year(d: DateType, years: int) -> DateType:
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        return d.replace(year=d.year + years, day=28)


def _resolve_period(session, entity_id: str, period_end: str | None) -> dict[str, Any]:
    if period_end:
        row = session.execute(
            text(
                "SELECT period_start, period_end, period_label, status FROM accounting_periods "
                "WHERE entity_id=:e AND period_end=:pe"
            ),
            {"e": entity_id, "pe": period_end},
        ).mappings().first()
        if not row:
            raise HTTPException(404, f"No accounting period ending {period_end}")
        return dict(row)
    # default: latest closed period (the most recent finalized month)
    row = session.execute(
        text(
            "SELECT period_start, period_end, period_label, status FROM accounting_periods "
            "WHERE entity_id=:e AND status='closed_locked' AND period_end<=CURRENT_DATE "
            "ORDER BY period_end DESC LIMIT 1"
        ),
        {"e": entity_id},
    ).mappings().first()
    if not row:
        row = session.execute(
            text(
                "SELECT period_start, period_end, period_label, status FROM accounting_periods "
                "WHERE entity_id=:e AND period_end<=CURRENT_DATE ORDER BY period_end DESC LIMIT 1"
            ),
            {"e": entity_id},
        ).mappings().first()
    if not row:
        raise HTTPException(404, "No accounting periods for this entity")
    return dict(row)


def _breach(value: float | None, cfg: dict[str, Any] | None) -> bool:
    if value is None or not cfg:
        return False
    direction = cfg.get("threshold_direction")
    tmin, tmax = cfg.get("threshold_min"), cfg.get("threshold_max")
    if direction == "min" and tmin is not None and value < float(tmin):
        return True
    if direction == "max" and tmax is not None and value > float(tmax):
        return True
    return False


@router.get("")
def get_ratios(
    entity_code: str = Query(...),
    period_end: str | None = Query(default=None),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity code: {entity_code}")
        eid = entity["id"]
        period = _resolve_period(session, eid, period_end)
        ps, pe = period["period_start"], period["period_end"]
        roles = get_account_roles(session, eid)

        inputs_rows = session.execute(
            text("SELECT key, value FROM entity_ratio_inputs WHERE entity_id=:e"),
            {"e": eid},
        ).mappings().all()
        inputs = {r["key"]: float(r["value"]) for r in inputs_rows}

        # Debt service is GL-derived per TTM window (override from
        # entity_ratio_inputs wins). Resolve separately for current vs PY.
        ads_cur, ads_src, ads_breakdown = resolve_annual_debt_service(session, eid, pe)
        ads_py, _, _ = resolve_annual_debt_service(session, eid, _shift_year(pe, -1))

        cfg_rows = session.execute(
            text(
                "SELECT ratio_key, enabled, threshold_min, threshold_max, threshold_direction "
                "FROM entity_ratio_config WHERE entity_id=:e"
            ),
            {"e": eid},
        ).mappings().all()
        cfg = {r["ratio_key"]: dict(r) for r in cfg_rows}

        ctx = build_financials_context(session, entity_id=eid, period_start=ps, period_end=pe, roles=roles)
        cur = compute_builtin_ratios(ctx, {**inputs, "annual_debt_service": ads_cur})
        ctx_py = build_financials_context(
            session, entity_id=eid, period_start=_shift_year(ps, -1), period_end=_shift_year(pe, -1), roles=roles,
        )
        prior = compute_builtin_ratios(ctx_py, {**inputs, "annual_debt_service": ads_py})

    ratios = []
    for key, meta in RATIO_META.items():
        c = cfg.get(key)
        enabled = c["enabled"] if c else True
        value = cur.get(key)
        ratios.append({
            "key": key,
            "label": meta["label"],
            "category": meta["category"],
            "format": meta["format"],
            "value": value,
            "py_value": prior.get(key),
            "enabled": enabled,
            "threshold_min": float(c["threshold_min"]) if c and c["threshold_min"] is not None else None,
            "threshold_max": float(c["threshold_max"]) if c and c["threshold_max"] is not None else None,
            "threshold_direction": c["threshold_direction"] if c else None,
            "breached": _breach(value, c),
        })

    # cross-period retail ratios (need both windows)
    def growth(a, b):
        return None if not b else (a - b) / b * 100.0
    ratios.append({
        "key": "sales_growth_yoy_pct", "label": "Sales growth (YoY)", "category": "Retail",
        "format": "percent", "value": growth(ctx["revenue"], ctx_py["revenue"]),
        "py_value": None, "enabled": True, "threshold_min": None, "threshold_max": None,
        "threshold_direction": None, "breached": False,
    })
    ratios.append({
        "key": "bank_balance_vs_py_pct", "label": "Bank balance vs prior year", "category": "Retail",
        "format": "percent", "value": growth(ctx["cash"], ctx_py["cash"]),
        "py_value": None, "enabled": True, "threshold_min": None, "threshold_max": None,
        "threshold_direction": None, "breached": False,
    })

    return {
        "entity_code": entity_code,
        "period_label": period["period_label"],
        "period_start": ps.isoformat(),
        "period_end": pe.isoformat(),
        "ttm_start": ctx["ttm_start"],
        "ttm_end": ctx["ttm_end"],
        "context": {
            "ttm_ebitda": ctx["ttm_ebitda"],
            "ttm_ebitda_excl_dgip": ctx["ttm_ebitda_excl_dgip"],
            "total_debt": ctx["total_debt"],
            "overdraft_reclassified": ctx["overdraft_reclassified"],
            "equity_reclassified": ctx["equity_reclassified"],
            "balances_balanced": ctx["balances_balanced"],
            "annual_debt_service": ads_cur,
            "annual_debt_service_source": ads_src,
            "debt_service_breakdown": ads_breakdown,
        },
        "ratios": ratios,
    }


@router.get("/roles")
def list_roles(
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("viewer")),
) -> dict[str, Any]:
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity code: {entity_code}")
        return {"entity_code": entity_code, "roles": account_roles_detail(session, entity["id"])}


@router.post("/seed-roles")
def seed_roles(
    entity_code: str = Query(...),
    _user: Any = Depends(require_role("admin")),
) -> dict[str, Any]:
    """Initial auto-seed of account roles from QBO type/subtype heuristics.
    Idempotent (ON CONFLICT DO NOTHING); preserves admin edits."""
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity code: {entity_code}")
        inserted = seed_account_roles(session, entity["id"])
    return {"entity_code": entity_code, "seeded": inserted}
