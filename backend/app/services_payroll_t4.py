"""
T4 Statement of Remuneration Paid — calendar-year compute + PDF.

CRA T4s are CALENDAR YEAR (Jan 1 – Dec 31) regardless of the
employer's accounting fiscal year. Totals are computed dynamically
from payroll_run_lines filtered by pay_date — the run lines are the
source of truth.

EI insurable earnings (Box 24) derivation:
    SUM(gross_pay - life_taxable_benefit) capped at
    EI_MAX_INSURABLE_ANNUAL for the year.
    Bridlewood assumption: there are no EI-exempt-by-class bonus
    payments. Every dollar of gross_pay minus the life-insurance
    taxable benefit (which is reported separately in Box 40) counts
    toward EI insurable, up to the annual cap.

CPP pensionable earnings (Box 26):
    SUM(taxable_gross) capped at CPP_MAX_EARNINGS_ANNUAL.
    CPP exempt employees: 0.

SIN is NOT printed on the PDF — BookWize doesn't store SIN, so it
can't leak even by accident.
"""
from __future__ import annotations

import io
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from sqlalchemy import text

from .services_payroll_calc import (
    CPP_MAX_EARNINGS_ANNUAL,
    EI_MAX_INSURABLE_ANNUAL,
)


BRIDLEWOOD_ADDRESS_LINE = "90 Michael Cowpland Dr, Kanata ON K2M 1P6"
PAYROLL_BUSINESS_NUMBER = "753391010RP0001"


@dataclass
class T4Figures:
    employee_id: str
    employee_name: str
    employee_number: int | None
    address: str | None
    box_14_employment_income: Decimal
    box_16_cpp_employee: Decimal
    box_17_cpp2_employee: Decimal
    box_18_ei_premiums: Decimal
    box_22_income_tax: Decimal
    box_24_ei_insurable: Decimal
    box_26_cpp_pensionable: Decimal
    box_40_other_benefits: Decimal


def compute_t4_figures(
    session, *, entity_id: Any, calendar_year: int,
) -> list[T4Figures]:
    """Aggregate payroll_run_lines for every employee that received
    any pay in the calendar year. Excludes voided runs.

    `calendar_year` = e.g. 2025 means Jan 1 2025 – Dec 31 2025
    based on pay_date (NOT period_end — the date the money moved
    is what CRA cares about)."""
    rows = session.execute(
        text(
            """
            SELECT pe.id AS employee_id,
                   pe.full_name,
                   pe.employee_number,
                   pe.address,
                   pe.cpp_exempt,
                   pe.ei_exempt,
                   COALESCE(SUM(prl.gross_pay), 0)             AS gross,
                   COALESCE(SUM(prl.cpp_ee), 0)                AS cpp_ee,
                   COALESCE(SUM(prl.ei_ee), 0)                 AS ei_ee,
                   COALESCE(SUM(prl.fed_tax), 0)               AS fed_tax,
                   COALESCE(SUM(prl.taxable_gross), 0)         AS taxable_gross,
                   COALESCE(SUM(prl.life_taxable_benefit), 0)  AS life_benefit
              FROM payroll_run_lines prl
              JOIN payroll_runs pr ON pr.id = prl.payroll_run_id
              JOIN payroll_employees pe ON pe.id = prl.employee_id
             WHERE pr.entity_id = :eid
               AND pe.entity_id = :eid
               AND pr.pay_date >= :start
               AND pr.pay_date <= :end
               AND COALESCE(pr.workflow_status, pr.status) NOT IN
                   ('voided', 'draft', 'draft_ready', 'rejected')
             GROUP BY pe.id, pe.full_name, pe.employee_number,
                      pe.address, pe.cpp_exempt, pe.ei_exempt
             HAVING COALESCE(SUM(prl.gross_pay), 0) > 0
             ORDER BY pe.employee_number, pe.full_name
            """
        ),
        {
            "eid": entity_id,
            "start": date(calendar_year, 1, 1),
            "end": date(calendar_year, 12, 31),
        },
    ).mappings().all()

    out: list[T4Figures] = []
    for r in rows:
        gross = Decimal(str(r["gross"] or 0))
        life_benefit = Decimal(str(r["life_benefit"] or 0))
        taxable_gross = Decimal(str(r["taxable_gross"] or 0))

        # Box 24 — EI insurable. gross - life benefit, capped at annual max.
        if r["ei_exempt"]:
            ei_insurable = Decimal("0.00")
        else:
            raw_ei_insurable = max(Decimal("0"), gross - life_benefit)
            ei_insurable = min(raw_ei_insurable, EI_MAX_INSURABLE_ANNUAL)

        # Box 26 — CPP pensionable. taxable_gross capped at annual max.
        if r["cpp_exempt"]:
            cpp_pensionable = Decimal("0.00")
        else:
            cpp_pensionable = min(taxable_gross, CPP_MAX_EARNINGS_ANNUAL)

        out.append(T4Figures(
            employee_id=str(r["employee_id"]),
            employee_name=r["full_name"],
            employee_number=r["employee_number"],
            address=r["address"],
            box_14_employment_income=gross.quantize(Decimal("0.01")),
            box_16_cpp_employee=Decimal(str(r["cpp_ee"] or 0)).quantize(Decimal("0.01")),
            box_17_cpp2_employee=Decimal("0.00"),  # engine doesn't compute CPP2 yet
            box_18_ei_premiums=Decimal(str(r["ei_ee"] or 0)).quantize(Decimal("0.01")),
            box_22_income_tax=Decimal(str(r["fed_tax"] or 0)).quantize(Decimal("0.01")),
            box_24_ei_insurable=ei_insurable.quantize(Decimal("0.01")),
            box_26_cpp_pensionable=cpp_pensionable.quantize(Decimal("0.01")),
            box_40_other_benefits=life_benefit.quantize(Decimal("0.01")),
        ))
    return out


def _money(v: Any) -> str:
    if v is None:
        return "$0.00"
    try:
        d = Decimal(str(v))
    except Exception:
        return "$0.00"
    return f"${d:,.2f}"


def generate_t4_pdf(
    *,
    figures: T4Figures,
    entity: dict[str, Any],
    calendar_year: int,
    caveats: list[str] | None = None,
) -> bytes:
    """Single-page T4 PDF for one employee.

    Mirrors the visual structure of the CRA paper T4. Box numbers are
    explicit on every cell. SIN deliberately not rendered — see module
    docstring."""
    from reportlab.lib.pagesizes import LETTER
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    )

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=LETTER,
        topMargin=0.6 * inch, bottomMargin=0.6 * inch,
        leftMargin=0.6 * inch, rightMargin=0.6 * inch,
        title=f"T4 {calendar_year} — {figures.employee_name}",
        author="BookWize",
    )
    styles = getSampleStyleSheet()
    h1 = ParagraphStyle(
        "h1", parent=styles["Heading1"], fontSize=14, leading=18,
        textColor=colors.HexColor("#0B2E72"), spaceAfter=2,
    )
    body = ParagraphStyle("body", parent=styles["Normal"], fontSize=9, leading=12)
    cell_label = ParagraphStyle(
        "cell_label", parent=styles["Normal"], fontSize=7,
        textColor=colors.HexColor("#525B6B"),
    )

    el: list[Any] = []
    entity_name = entity.get("entity_name") or "Employer"
    el.append(Paragraph(f"T4 Statement of Remuneration Paid", h1))
    el.append(Paragraph(f"Tax Year: {calendar_year}", body))
    el.append(Spacer(1, 12))

    # Employer + employee header block
    header_table = Table(
        [
            [
                Paragraph(
                    f"<b>Employer:</b><br/>{entity_name}<br/>"
                    f"{BRIDLEWOOD_ADDRESS_LINE}<br/>"
                    f"BN: {PAYROLL_BUSINESS_NUMBER}",
                    body,
                ),
                Paragraph(
                    f"<b>Employee:</b><br/>{figures.employee_name}<br/>"
                    f"EE#: {figures.employee_number or '—'}<br/>"
                    f"{(figures.address or '').strip() or '(address on file)'}<br/>"
                    f"<font color='#9BA3B1'>SIN: not stored in BookWize</font>",
                    body,
                ),
            ],
        ],
        colWidths=[3.5 * inch, 3.9 * inch],
    )
    header_table.setStyle(TableStyle([
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#C1C7D3")),
        ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#D8DDE6")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    el.append(header_table)
    el.append(Spacer(1, 12))

    # Box grid — 4 columns × 4 rows; each cell shows box number, label,
    # and amount. Matches the visual feel of the CRA paper T4.
    def boxcell(num: str, label: str, value: Decimal) -> Any:
        return Paragraph(
            f"<font size='8' color='#0B2E72'><b>Box {num}</b></font><br/>"
            f"<font size='7' color='#525B6B'>{label}</font><br/>"
            f"<font size='12'><b>{_money(value)}</b></font>",
            body,
        )

    box_grid = Table(
        [
            [
                boxcell("14", "Employment income", figures.box_14_employment_income),
                boxcell("22", "Income tax deducted", figures.box_22_income_tax),
            ],
            [
                boxcell("16", "Employee's CPP", figures.box_16_cpp_employee),
                boxcell("18", "Employee's EI premiums", figures.box_18_ei_premiums),
            ],
            [
                boxcell("24", "EI insurable earnings", figures.box_24_ei_insurable),
                boxcell("26", "CPP pensionable earnings", figures.box_26_cpp_pensionable),
            ],
            [
                boxcell("17", "Employee's CPP2", figures.box_17_cpp2_employee),
                boxcell("40", "Other taxable allowances", figures.box_40_other_benefits),
            ],
        ],
        colWidths=[3.7 * inch, 3.7 * inch],
    )
    box_grid.setStyle(TableStyle([
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#C1C7D3")),
        ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#D8DDE6")),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    el.append(box_grid)
    el.append(Spacer(1, 14))

    el.append(Paragraph(
        f"<b>Province of employment:</b> ON &nbsp;&nbsp;"
        f"<b>Pay periods worked:</b> calendar year {calendar_year}",
        body,
    ))
    el.append(Spacer(1, 12))

    # CP5b review flags (e.g. Box 22 federal-only, Box 17 CPP2 = 0). Rendered as
    # a visible review box so a T4 is never silently mistaken as filing-ready.
    if caveats:
        warn = ParagraphStyle("warn", parent=body, fontSize=7.5,
                              textColor=colors.HexColor("#92400E"), leading=11)
        flag_rows = [[Paragraph("<b>REVIEW BEFORE FILING</b>", warn)]]
        for c in caveats:
            flag_rows.append([Paragraph(f"• {c}", warn)])
        flag_tbl = Table(flag_rows, colWidths=[doc.width if hasattr(doc, "width") else 460])
        flag_tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#FEF3C7")),
            ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#F59E0B")),
            ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("LEFTPADDING", (0, 0), (-1, -1), 6), ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ]))
        el.append(flag_tbl)
        el.append(Spacer(1, 12))

    el.append(Paragraph(
        f"Generated by BookWize on {date.today().strftime('%B %d, %Y')}. "
        "This T4 must be reviewed against the employer's CRA filing "
        "before being distributed to the employee.",
        ParagraphStyle("foot", parent=body, fontSize=7,
                       textColor=colors.HexColor("#9BA3B1")),
    ))

    doc.build(el)
    return buf.getvalue()
