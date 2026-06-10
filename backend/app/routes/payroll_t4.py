"""
T4 generation endpoints (Phase 5D). Calendar-year (Jan 1 - Dec 31, by pay_date).

Pre-flight (CP5b) lists every payroll run in the year and reports, per T4 box,
whether the value is derivable / partial / missing — surfacing the known gaps
(Box 22 income tax is FEDERAL ONLY, Box 17 CPP2 = 0, SIN not stored). Generation
proceeds regardless (per the overnight instruction) but stamps each T4 PDF with a
"REVIEW BEFORE FILING" box carrying those caveats, stores to R2 entity-scoped,
and NEVER marks filed_with_cra. SIN is never stored, logged, or put in metadata.
"""
from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text

from ..db import db_session
from ..services import get_entity_by_code
from ..services_auth import require_role
from ..services_payroll_t4 import compute_t4_figures, generate_t4_pdf

router = APIRouter(prefix="/api/payroll/t4", tags=["payroll-t4"])

# Per-box derivability for the pre-flight report.
_BOX_DERIVABILITY = [
    ("14", "Employment income", "derivable", "SUM(gross_pay) over the calendar year."),
    ("16", "Employee CPP", "derivable", "SUM(cpp_ee)."),
    ("17", "Employee CPP2", "missing", "Engine does not compute CPP2 yet — reported as 0.00."),
    ("18", "Employee EI premiums", "derivable", "SUM(ei_ee)."),
    ("22", "Income tax deducted", "derivable",
     "SUM(fed_tax) — combined federal + Ontario provincial. "
     "Individual federal_tax / provincial_tax split stored in migration 064 for reporting."),
    ("24", "EI insurable earnings", "derivable", "SUM(gross - life benefit), capped."),
    ("26", "CPP pensionable earnings", "derivable", "SUM(taxable_gross), capped."),
    ("40", "Other taxable benefits", "derivable", "SUM(life_taxable_benefit)."),
    ("SIN", "Social insurance number", "missing",
     "Not stored by design — T4 PDFs omit SIN; add before CRA filing."),
]

_T4_CAVEATS = [
    "Box 22 income tax is the combined federal + provincial amount — correct for CRA T4 filing.",
    "Box 17 CPP2 = $0.00 — not computed by the engine.",
    "SIN is not included — add before CRA filing.",
]


def _entity(session, entity_code: str) -> dict[str, Any]:
    row = get_entity_by_code(session, entity_code)
    if not row:
        raise HTTPException(404, "entity not found")
    return dict(row)


@router.get("/{calendar_year}/preflight")
def preflight(calendar_year: int, entity_code: str = Query(...),
              _user: Any = Depends(require_role("bookkeeper"))) -> dict[str, Any]:
    with db_session() as session:
        ent = _entity(session, entity_code)
        runs = session.execute(
            text(
                """
                SELECT pay_run_number, pay_date, status, workflow_status,
                       total_gross, total_net_pay
                  FROM payroll_runs
                 WHERE entity_id=:e AND pay_date BETWEEN :s AND :en
              ORDER BY pay_date
                """
            ),
            {"e": ent["id"], "s": date(calendar_year, 1, 1), "en": date(calendar_year, 12, 31)},
        ).mappings().all()
        figures = compute_t4_figures(session, entity_id=ent["id"], calendar_year=calendar_year)

    run_list = [{
        "pay_run_number": r["pay_run_number"],
        "pay_date": r["pay_date"].isoformat() if r["pay_date"] else None,
        "status": r["workflow_status"] or r["status"],
        "total_gross": float(r["total_gross"] or 0),
        "total_net_pay": float(r["total_net_pay"] or 0),
        "included_in_t4": (r["workflow_status"] or r["status"]) not in
                          ("voided", "draft", "draft_ready", "rejected"),
    } for r in runs]

    preview = [{
        "employee_name": f.employee_name, "employee_number": f.employee_number,
        "box_14": float(f.box_14_employment_income), "box_16": float(f.box_16_cpp_employee),
        "box_17": float(f.box_17_cpp2_employee), "box_18": float(f.box_18_ei_premiums),
        "box_22": float(f.box_22_income_tax), "box_24": float(f.box_24_ei_insurable),
        "box_26": float(f.box_26_cpp_pensionable), "box_40": float(f.box_40_other_benefits),
    } for f in figures]

    return {
        "entity_code": entity_code,
        "calendar_year": calendar_year,
        "runs": run_list,
        "runs_total": len(run_list),
        "runs_included": sum(1 for r in run_list if r["included_in_t4"]),
        "employees_with_pay": len(figures),
        "box_derivability": [
            {"box": b, "label": lbl, "status": st, "note": note}
            for (b, lbl, st, note) in _BOX_DERIVABILITY
        ],
        "figures_preview": preview,
        "caveats": _T4_CAVEATS,
    }


@router.get("/{calendar_year}")
def generate(calendar_year: int, entity_code: str = Query(...),
             actor_email: str | None = Query(default=None),
             _user: Any = Depends(require_role("approver"))) -> dict[str, Any]:
    with db_session() as session:
        ent = _entity(session, entity_code)
        figures = compute_t4_figures(session, entity_id=ent["id"], calendar_year=calendar_year)
        if not figures:
            raise HTTPException(404, f"No payroll with pay in {calendar_year}")

        from ..services_storage import storage_service
        out = []
        totals = {k: 0.0 for k in
                  ("box_14", "box_16", "box_17", "box_18", "box_22", "box_24", "box_26", "box_40")}
        for f in figures:
            pdf = generate_t4_pdf(figures=f, entity=ent, calendar_year=calendar_year, caveats=_T4_CAVEATS)
            fn = f"T4-{calendar_year}-{(f.employee_name or 'employee').replace(' ', '_')}.pdf"
            r2_key = None
            try:
                r2_key = storage_service.upload_file(
                    file_bytes=pdf, original_filename=fn, entity_code=entity_code,
                    document_type="payroll-t4", content_type="application/pdf")
            except Exception:
                r2_key = None
            session.execute(
                text(
                    """
                    INSERT INTO payroll_t4s (
                        entity_id, employee_id, calendar_year,
                        box_14_employment_income, box_16_cpp_employee, box_17_cpp2_employee,
                        box_18_ei_premiums, box_22_income_tax, box_24_ei_insurable,
                        box_26_cpp_pensionable, box_40_other_benefits,
                        r2_object_key, file_name, generated_by, filed_with_cra
                    ) VALUES (
                        :e, :emp, :yr, :b14, :b16, :b17, :b18, :b22, :b24, :b26, :b40,
                        :k, :fn, :by, FALSE
                    )
                    ON CONFLICT (entity_id, employee_id, calendar_year) DO UPDATE SET
                        box_14_employment_income=EXCLUDED.box_14_employment_income,
                        box_16_cpp_employee=EXCLUDED.box_16_cpp_employee,
                        box_17_cpp2_employee=EXCLUDED.box_17_cpp2_employee,
                        box_18_ei_premiums=EXCLUDED.box_18_ei_premiums,
                        box_22_income_tax=EXCLUDED.box_22_income_tax,
                        box_24_ei_insurable=EXCLUDED.box_24_ei_insurable,
                        box_26_cpp_pensionable=EXCLUDED.box_26_cpp_pensionable,
                        box_40_other_benefits=EXCLUDED.box_40_other_benefits,
                        r2_object_key=EXCLUDED.r2_object_key, file_name=EXCLUDED.file_name,
                        generated_at=NOW(), generated_by=EXCLUDED.generated_by,
                        filed_with_cra=FALSE
                    """
                ),
                {"e": ent["id"], "emp": f.employee_id, "yr": calendar_year,
                 "b14": f.box_14_employment_income, "b16": f.box_16_cpp_employee,
                 "b17": f.box_17_cpp2_employee, "b18": f.box_18_ei_premiums,
                 "b22": f.box_22_income_tax, "b24": f.box_24_ei_insurable,
                 "b26": f.box_26_cpp_pensionable, "b40": f.box_40_other_benefits,
                 "k": r2_key, "fn": fn, "by": actor_email},
            )
            presigned = None
            if r2_key:
                try:
                    presigned = storage_service.get_presigned_url(r2_key, expires_in=86400)
                except Exception:
                    presigned = None
            out.append({
                "employee_name": f.employee_name, "employee_number": f.employee_number,
                "r2_object_key": r2_key, "presigned_url": presigned,
                "box_14": float(f.box_14_employment_income), "box_22": float(f.box_22_income_tax),
            })
            for key, val in (("box_14", f.box_14_employment_income), ("box_16", f.box_16_cpp_employee),
                             ("box_17", f.box_17_cpp2_employee), ("box_18", f.box_18_ei_premiums),
                             ("box_22", f.box_22_income_tax), ("box_24", f.box_24_ei_insurable),
                             ("box_26", f.box_26_cpp_pensionable), ("box_40", f.box_40_other_benefits)):
                totals[key] += float(val)

    return {
        "entity_code": entity_code,
        "calendar_year": calendar_year,
        "t4_count": len(out),
        "t4s": out,
        "summary_totals": {k: round(v, 2) for k, v in totals.items()},
        "filed_with_cra": False,
        "caveats": _T4_CAVEATS,
        "note": "T4 PDFs stored in R2 only. NOT filed with CRA. Review the caveats before filing.",
    }
