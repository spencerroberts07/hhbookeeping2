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
    resolve_fixed_charges,
    build_token_namespace,
    evaluate_custom_ratio,
    list_tokens,
    BUILTIN_FORMULAS,
)
from pydantic import BaseModel, Field

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

        # Debt service + fixed charges are GL-derived per TTM window (override
        # from entity_ratio_inputs wins). Resolve separately for current vs PY.
        ads_cur, ads_src, ads_breakdown = resolve_annual_debt_service(session, eid, pe)
        ads_py, _, _ = resolve_annual_debt_service(session, eid, _shift_year(pe, -1))
        fc_cur, fc_src, fc_breakdown = resolve_fixed_charges(session, eid, pe, roles)
        fc_py, _, _ = resolve_fixed_charges(session, eid, _shift_year(pe, -1), roles)
        inputs_cur = {**inputs, "annual_debt_service": ads_cur, "fixed_charges": fc_cur}
        inputs_py = {**inputs, "annual_debt_service": ads_py, "fixed_charges": fc_py}

        cfg_rows = session.execute(
            text(
                "SELECT ratio_key, enabled, threshold_min, threshold_max, threshold_direction "
                "FROM entity_ratio_config WHERE entity_id=:e"
            ),
            {"e": eid},
        ).mappings().all()
        cfg = {r["ratio_key"]: dict(r) for r in cfg_rows}

        ctx = build_financials_context(session, entity_id=eid, period_start=ps, period_end=pe, roles=roles)
        cur = compute_builtin_ratios(ctx, inputs_cur)
        ctx_py = build_financials_context(
            session, entity_id=eid, period_start=_shift_year(ps, -1), period_end=_shift_year(pe, -1), roles=roles,
        )
        prior = compute_builtin_ratios(ctx_py, inputs_py)

        # Custom ratios (safe-evaluated over the token namespace)
        custom_defs = session.execute(
            text(
                "SELECT key, label, numerator_expr, denominator_expr, output_type, enabled, "
                "threshold_min, threshold_max, threshold_direction "
                "FROM custom_ratio_definitions WHERE entity_id=:e ORDER BY label"
            ),
            {"e": eid},
        ).mappings().all()
        ns_cur = build_token_namespace(session, eid, ctx, pe, inputs_cur)
        ns_py = build_token_namespace(session, eid, ctx_py, _shift_year(pe, -1), inputs_py)

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

    # custom ratios
    for d in custom_defs:
        defn = dict(d)
        try:
            value = evaluate_custom_ratio(defn, ns_cur)
            py_value = evaluate_custom_ratio(defn, ns_py)
            err = None
        except ValueError as exc:
            value = py_value = None
            err = str(exc)
        cfg_like = {
            "threshold_min": defn["threshold_min"], "threshold_max": defn["threshold_max"],
            "threshold_direction": defn["threshold_direction"],
        }
        ratios.append({
            "key": f"custom:{defn['key']}", "label": defn["label"], "category": "Custom",
            "format": defn["output_type"], "value": value, "py_value": py_value,
            "enabled": defn["enabled"],
            "threshold_min": float(defn["threshold_min"]) if defn["threshold_min"] is not None else None,
            "threshold_max": float(defn["threshold_max"]) if defn["threshold_max"] is not None else None,
            "threshold_direction": defn["threshold_direction"],
            "breached": _breach(value, cfg_like), "error": err, "custom": True,
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
            "fixed_charges": fc_cur,
            "fixed_charges_source": fc_src,
            "fixed_charges_breakdown": fc_breakdown,
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
    Initial-only — never resurrects admin-deleted role rows."""
    with db_session() as session:
        entity = get_entity_by_code(session, entity_code)
        if not entity:
            raise HTTPException(404, f"Unknown entity code: {entity_code}")
        inserted = seed_account_roles(session, entity["id"], force=True)
    return {"entity_code": entity_code, "seeded": inserted}


# --------------------------------------------------------------------------
# Config CRUD (admin) — the /settings/ratios page
# --------------------------------------------------------------------------


def _eid(session, entity_code: str) -> str:
    entity = get_entity_by_code(session, entity_code)
    if not entity:
        raise HTTPException(404, f"Unknown entity code: {entity_code}")
    return entity["id"]


class RatioConfigIn(BaseModel):
    ratio_key: str
    enabled: bool = True
    threshold_min: float | None = None
    threshold_max: float | None = None
    threshold_direction: str | None = None  # 'min' | 'max' | None


class InputIn(BaseModel):
    key: str  # 'annual_debt_service' | 'fixed_charges'
    value: float


class RoleIn(BaseModel):
    role: str
    account_code: str


class CustomRatioIn(BaseModel):
    key: str
    label: str
    numerator_expr: str
    denominator_expr: str | None = None
    output_type: str = Field(default="ratio")  # ratio | percent | dollar
    enabled: bool = True
    threshold_min: float | None = None
    threshold_max: float | None = None
    threshold_direction: str | None = None


@router.put("/config")
def upsert_config(payload: RatioConfigIn, entity_code: str = Query(...),
                  _user: Any = Depends(require_role("admin"))) -> dict[str, Any]:
    with db_session() as session:
        eid = _eid(session, entity_code)
        session.execute(
            text(
                """
                INSERT INTO entity_ratio_config
                    (entity_id, ratio_key, enabled, threshold_min, threshold_max, threshold_direction)
                VALUES (:e,:k,:en,:tmin,:tmax,:dir)
                ON CONFLICT (entity_id, ratio_key) DO UPDATE SET
                    enabled=EXCLUDED.enabled, threshold_min=EXCLUDED.threshold_min,
                    threshold_max=EXCLUDED.threshold_max, threshold_direction=EXCLUDED.threshold_direction,
                    updated_at=NOW()
                """
            ),
            {"e": eid, "k": payload.ratio_key, "en": payload.enabled,
             "tmin": payload.threshold_min, "tmax": payload.threshold_max, "dir": payload.threshold_direction},
        )
    return {"ok": True}


@router.put("/inputs")
def set_input(payload: InputIn, entity_code: str = Query(...),
              _user: Any = Depends(require_role("admin"))) -> dict[str, Any]:
    """Override a derived input (annual_debt_service / fixed_charges)."""
    with db_session() as session:
        eid = _eid(session, entity_code)
        session.execute(
            text(
                """
                INSERT INTO entity_ratio_inputs (entity_id, key, value)
                VALUES (:e,:k,:v)
                ON CONFLICT (entity_id, key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()
                """
            ),
            {"e": eid, "k": payload.key, "v": payload.value},
        )
    return {"ok": True}


@router.delete("/inputs/{key}")
def clear_input(key: str, entity_code: str = Query(...),
                _user: Any = Depends(require_role("admin"))) -> dict[str, Any]:
    """Remove an override so the value reverts to the GL-derived figure."""
    with db_session() as session:
        eid = _eid(session, entity_code)
        session.execute(
            text("DELETE FROM entity_ratio_inputs WHERE entity_id=:e AND key=:k"),
            {"e": eid, "k": key},
        )
    return {"ok": True}


@router.post("/roles")
def add_role(payload: RoleIn, entity_code: str = Query(...),
             _user: Any = Depends(require_role("admin"))) -> dict[str, Any]:
    with db_session() as session:
        eid = _eid(session, entity_code)
        session.execute(
            text(
                "INSERT INTO ratio_account_roles (entity_id, role, account_code) VALUES (:e,:r,:c) "
                "ON CONFLICT (entity_id, role, account_code) DO NOTHING"
            ),
            {"e": eid, "r": payload.role, "c": payload.account_code},
        )
    return {"ok": True}


@router.delete("/roles")
def remove_role(payload: RoleIn, entity_code: str = Query(...),
                _user: Any = Depends(require_role("admin"))) -> dict[str, Any]:
    with db_session() as session:
        eid = _eid(session, entity_code)
        session.execute(
            text("DELETE FROM ratio_account_roles WHERE entity_id=:e AND role=:r AND account_code=:c"),
            {"e": eid, "r": payload.role, "c": payload.account_code},
        )
    return {"ok": True}


@router.get("/tokens")
def get_tokens(entity_code: str = Query(...), period_end: str | None = Query(default=None),
               _user: Any = Depends(require_role("viewer"))) -> dict[str, Any]:
    with db_session() as session:
        eid = _eid(session, entity_code)
        period = _resolve_period(session, eid, period_end)
        return {"entity_code": entity_code, "tokens": list_tokens(session, eid, period["period_end"]),
                "builtin_formulas": BUILTIN_FORMULAS}


@router.get("/custom")
def list_custom(entity_code: str = Query(...),
                _user: Any = Depends(require_role("viewer"))) -> dict[str, Any]:
    with db_session() as session:
        eid = _eid(session, entity_code)
        rows = session.execute(
            text(
                "SELECT key,label,numerator_expr,denominator_expr,output_type,enabled,"
                "threshold_min,threshold_max,threshold_direction FROM custom_ratio_definitions "
                "WHERE entity_id=:e ORDER BY label"
            ),
            {"e": eid},
        ).mappings().all()
    return {"entity_code": entity_code, "custom": [dict(r) for r in rows]}


@router.post("/custom")
def upsert_custom(payload: CustomRatioIn, entity_code: str = Query(...),
                  _user: Any = Depends(require_role("admin"))) -> dict[str, Any]:
    if payload.output_type not in ("ratio", "percent", "dollar"):
        raise HTTPException(400, "output_type must be ratio|percent|dollar")
    with db_session() as session:
        eid = _eid(session, entity_code)
        period = _resolve_period(session, eid, None)
        # validate expressions against the live token namespace before saving
        roles = get_account_roles(session, eid)
        ctx = build_financials_context(session, entity_id=eid,
                                       period_start=period["period_start"], period_end=period["period_end"], roles=roles)
        ns = build_token_namespace(session, eid, ctx, period["period_end"])
        from ..services_ratios import safe_eval
        try:
            safe_eval(payload.numerator_expr, ns)
            if payload.denominator_expr:
                safe_eval(payload.denominator_expr, ns)
        except ValueError as exc:
            raise HTTPException(400, f"Invalid formula: {exc}")
        session.execute(
            text(
                """
                INSERT INTO custom_ratio_definitions
                    (entity_id,key,label,numerator_expr,denominator_expr,output_type,enabled,
                     threshold_min,threshold_max,threshold_direction)
                VALUES (:e,:k,:l,:num,:den,:ot,:en,:tmin,:tmax,:dir)
                ON CONFLICT (entity_id,key) DO UPDATE SET
                    label=EXCLUDED.label, numerator_expr=EXCLUDED.numerator_expr,
                    denominator_expr=EXCLUDED.denominator_expr, output_type=EXCLUDED.output_type,
                    enabled=EXCLUDED.enabled, threshold_min=EXCLUDED.threshold_min,
                    threshold_max=EXCLUDED.threshold_max, threshold_direction=EXCLUDED.threshold_direction,
                    updated_at=NOW()
                """
            ),
            {"e": eid, "k": payload.key, "l": payload.label, "num": payload.numerator_expr,
             "den": payload.denominator_expr, "ot": payload.output_type, "en": payload.enabled,
             "tmin": payload.threshold_min, "tmax": payload.threshold_max, "dir": payload.threshold_direction},
        )
    return {"ok": True}


@router.delete("/custom/{key}")
def delete_custom(key: str, entity_code: str = Query(...),
                  _user: Any = Depends(require_role("admin"))) -> dict[str, Any]:
    with db_session() as session:
        eid = _eid(session, entity_code)
        session.execute(
            text("DELETE FROM custom_ratio_definitions WHERE entity_id=:e AND key=:k"),
            {"e": eid, "k": key},
        )
    return {"ok": True}


@router.post("/custom/preview")
def preview_custom(payload: CustomRatioIn, entity_code: str = Query(...),
                   _user: Any = Depends(require_role("viewer"))) -> dict[str, Any]:
    """Live-evaluate a draft custom formula without saving."""
    with db_session() as session:
        eid = _eid(session, entity_code)
        period = _resolve_period(session, eid, None)
        roles = get_account_roles(session, eid)
        ctx = build_financials_context(session, entity_id=eid,
                                       period_start=period["period_start"], period_end=period["period_end"], roles=roles)
        ns = build_token_namespace(session, eid, ctx, period["period_end"])
    try:
        value = evaluate_custom_ratio(payload.model_dump(), ns)
        return {"ok": True, "value": value, "output_type": payload.output_type}
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
