"""
Employment / income verification letter PDF.

Used by employees for mortgage applications, ROE prep, rental
references, etc. Generates a single-page PDF summarizing:
  - Identity + start date + employment type
  - Current compensation (rate, average hours, annualized gross)
  - YTD earnings (calendar year)
  - Last-12-periods pay history table

Pure compute — routes/payroll.py is responsible for R2 upload +
DB row (if we ever decide to archive these letters).

No SIN — BookWize doesn't store SIN, so it can't leak via the PDF
even by accident.
"""
from __future__ import annotations

import io
from datetime import date
from decimal import Decimal
from typing import Any

from .services import format_entity_address

HISTORY_PERIODS_ON_LETTER = 12


def _money(v: Any) -> str:
    if v is None:
        return "$0.00"
    try:
        d = Decimal(str(v))
    except Exception:
        return "$0.00"
    return f"${d:,.2f}"


def _fmt_date(d: Any) -> str:
    if d is None:
        return ""
    if isinstance(d, date):
        return d.strftime("%b %d, %Y")
    return str(d)


def generate_employment_record(
    *,
    employee: dict[str, Any],
    entity: dict[str, Any],
    history_lines: list[dict[str, Any]],   # ordered period_end DESC
    calendar_year_totals: dict[str, Any],
    actor_email: str,
) -> bytes:
    """Build the PDF and return its bytes. The caller queries the
    history rows + calendar-year totals; this is rendering only."""
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
        leftMargin=0.7 * inch, rightMargin=0.7 * inch,
        title=(
            f"Employment & Income Verification — "
            f"{employee.get('full_name') or 'Employee'}"
        ),
        author="BookWize",
    )
    styles = getSampleStyleSheet()
    h1 = ParagraphStyle(
        "h1", parent=styles["Heading1"], fontSize=14, leading=18,
        textColor=colors.HexColor("#0B2E72"), spaceAfter=2,
    )
    h2 = ParagraphStyle(
        "h2", parent=styles["Heading2"], fontSize=11, leading=14,
        textColor=colors.HexColor("#0B2E72"), spaceBefore=12, spaceAfter=4,
    )
    body = ParagraphStyle(
        "body", parent=styles["Normal"], fontSize=10, leading=14,
    )
    addr = ParagraphStyle(
        "addr", parent=styles["Normal"], fontSize=8,
        textColor=colors.HexColor("#525B6B"),
    )

    el: list[Any] = []
    entity_name = entity.get("entity_name") or "Employer"
    el.append(Paragraph(entity_name.upper(), h1))
    el.append(Paragraph("Employment &amp; Income Verification", body))
    el.append(Paragraph(format_entity_address(entity), addr))
    el.append(Paragraph(f"BN: {entity.get('payroll_business_number') or ''}", addr))
    el.append(Spacer(1, 14))

    el.append(Paragraph(
        f"This letter confirms the employment of the individual named "
        f"below at {entity_name}.", body,
    ))

    # Identity block
    el.append(Paragraph("Employee", h2))
    avg_hours, avg_gross, avg_net = _average_recent(history_lines, periods=6)
    annualized = (avg_gross * Decimal("26")).quantize(Decimal("0.01"))
    identity_rows = [
        ["Name", employee.get("full_name") or "—"],
        ["Employee #", str(employee.get("employee_number") or "—")],
        ["Pay type", (employee.get("employment_type") or "—").title()],
        ["Start date", _fmt_date(employee.get("start_date"))],
        ["Province", employee.get("province") or "ON"],
        ["Status", "Active" if employee.get("is_active") else "Inactive"],
    ]
    t1 = Table(identity_rows, colWidths=[1.4 * inch, 4.6 * inch])
    t1.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("TEXTCOLOR", (0, 0), (0, -1), colors.HexColor("#525B6B")),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    el.append(t1)

    # Compensation block
    el.append(Paragraph("Compensation", h2))
    if (employee.get("employment_type") or "").lower() == "salary":
        rate_line = (
            f"Bi-weekly salary: {_money(employee.get('biweekly_salary'))}"
        )
    else:
        rate_line = f"Hourly rate: {_money(employee.get('hourly_rate'))} / hour"
    comp_rows = [
        [rate_line],
        [f"Average hours (last 6 periods): {avg_hours:.1f}h"],
        [f"Average bi-weekly gross: {_money(avg_gross)}"],
        [f"Average bi-weekly net: {_money(avg_net)}"],
        [f"Annualized gross (×26): {_money(annualized)}"],
    ]
    t2 = Table(comp_rows, colWidths=[6.0 * inch])
    t2.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    el.append(t2)

    # YTD calendar
    el.append(Paragraph(
        f"Calendar-Year Earnings ({calendar_year_totals.get('year', '—')})",
        h2,
    ))
    ytd_rows = [
        [f"Gross: {_money(calendar_year_totals.get('gross'))}"],
        [f"Net: {_money(calendar_year_totals.get('net'))}"],
        [f"Income tax withheld: {_money(calendar_year_totals.get('fed_tax'))}"],
        [f"CPP contributions: {_money(calendar_year_totals.get('cpp'))}"],
        [f"EI premiums: {_money(calendar_year_totals.get('ei'))}"],
    ]
    t3 = Table(ytd_rows, colWidths=[6.0 * inch])
    t3.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    el.append(t3)

    # History
    el.append(Paragraph(
        f"Pay History (last {min(HISTORY_PERIODS_ON_LETTER, len(history_lines))} periods)",
        h2,
    ))
    hist_data: list[list[Any]] = [["Period", "Hours", "Gross", "Net"]]
    for line in history_lines[:HISTORY_PERIODS_ON_LETTER]:
        period = (
            f"{_fmt_date(line.get('period_start'))} – {_fmt_date(line.get('period_end'))}"
            if line.get("period_start") and line.get("period_end")
            else line.get("pay_run_number") or "—"
        )
        hist_data.append([
            period,
            f"{Decimal(str(line.get('total_hours') or 0)):.2f}",
            _money(line.get("gross_pay")),
            _money(line.get("net_pay")),
        ])
    t4 = Table(hist_data, colWidths=[2.6 * inch, 1.0 * inch, 1.2 * inch, 1.2 * inch])
    t4.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0B2E72")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
        ("ALIGN", (0, 0), (0, -1), "LEFT"),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#E1E5EC")),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    el.append(t4)

    el.append(Spacer(1, 18))
    el.append(Paragraph(
        f"Issued: {date.today().strftime('%B %d, %Y')}", body,
    ))
    if actor_email:
        el.append(Paragraph(f"Issued by: {actor_email}", body))
    el.append(Spacer(1, 28))
    el.append(Paragraph("_____________________________", body))
    el.append(Paragraph("Authorized Signature", body))
    el.append(Paragraph(entity_name, body))
    el.append(Spacer(1, 12))
    el.append(Paragraph(
        "Generated by BookWize. This letter is informational and does "
        "not constitute legal or financial advice.",
        ParagraphStyle("foot", parent=body, fontSize=7,
                       textColor=colors.HexColor("#9BA3B1")),
    ))

    doc.build(el)
    return buf.getvalue()


def _average_recent(
    history_lines: list[dict[str, Any]], *, periods: int = 6
) -> tuple[Decimal, Decimal, Decimal]:
    """Return (avg_hours, avg_gross, avg_net) across the last `periods`
    lines. Zero-pad if there's less history."""
    sample = history_lines[:periods] if history_lines else []
    if not sample:
        return Decimal("0"), Decimal("0"), Decimal("0")
    n = Decimal(len(sample))
    h = sum((Decimal(str(l.get("total_hours") or 0)) for l in sample), Decimal("0")) / n
    g = sum((Decimal(str(l.get("gross_pay") or 0)) for l in sample), Decimal("0")) / n
    netv = sum((Decimal(str(l.get("net_pay") or 0)) for l in sample), Decimal("0")) / n
    return (
        h.quantize(Decimal("0.01")),
        g.quantize(Decimal("0.01")),
        netv.quantize(Decimal("0.01")),
    )
