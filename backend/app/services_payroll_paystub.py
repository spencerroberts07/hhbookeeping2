"""
Pay-stub PDF generator. Bridlewood-flavoured layout — header carries
the entity name + Kanata address + payroll BN; body is a two-column
EARNINGS/DEDUCTIONS table with Current + YTD side by side; footer
shows vacation accrual + masked direct-deposit account.

Uses reportlab's Table flowable for structure — simpler than
coordinate-drawn PDFs and trivially restyle-able if the layout needs
to change later. No external dependencies beyond reportlab itself
(pure Python wheel).

R2 storage is the caller's job (routes/payroll.py uploads the bytes
returned by generate_pay_stub). This module is pure compute.
"""
from __future__ import annotations

import io
from datetime import date
from decimal import Decimal
from typing import Any


# --------------------------------------------------------------------------
# Constants — Bridlewood-specific until entities table carries them
# --------------------------------------------------------------------------

BRIDLEWOOD_ADDRESS_LINE = "90 Michael Cowpland Dr, Kanata ON K2M 1P6"
PAYROLL_BUSINESS_NUMBER = "753391010RP0001"


def _money(v: Any) -> str:
    """Render a Decimal-or-numeric as '$X,XXX.XX'. Empty values
    become '$0.00' — pay stubs never show dashes."""
    if v is None:
        return "$0.00"
    try:
        d = Decimal(str(v))
    except Exception:
        return "$0.00"
    return f"${d:,.2f}"


def _mask_account(account: str | None) -> str:
    if not account:
        return "****"
    digits = "".join(c for c in account if c.isdigit())
    if not digits:
        return "****"
    return "****" + digits[-4:]


def _fmt_date(d: Any) -> str:
    if d is None:
        return ""
    if isinstance(d, date):
        return d.strftime("%b %d, %Y")
    return str(d)


def generate_pay_stub(
    *,
    run_line: dict[str, Any],
    employee: dict[str, Any],
    run: dict[str, Any],
    entity: dict[str, Any],
    ytd: dict[str, Any] | None = None,
) -> bytes:
    """Build a PDF pay stub and return its bytes.

    Inputs are dict-shaped rows (mapping or sqlalchemy RowMapping
    converted with dict(...)). The caller is responsible for pulling
    `ytd` snapshot values — see routes/payroll.py."""
    from reportlab.lib.pagesizes import LETTER
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import (
        SimpleDocTemplate,
        Paragraph,
        Spacer,
        Table,
        TableStyle,
    )

    ytd = ytd or {}
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=LETTER,
        topMargin=0.5 * inch,
        bottomMargin=0.5 * inch,
        leftMargin=0.6 * inch,
        rightMargin=0.6 * inch,
        title=f"Pay Stub — {employee.get('full_name') or 'Employee'}",
        author="BookWize",
    )

    styles = getSampleStyleSheet()
    h1 = ParagraphStyle(
        "h1",
        parent=styles["Heading1"],
        fontSize=14,
        leading=18,
        textColor=colors.HexColor("#0B2E72"),
        spaceAfter=2,
    )
    addr = ParagraphStyle(
        "addr",
        parent=styles["Normal"],
        fontSize=8,
        textColor=colors.HexColor("#525B6B"),
        spaceAfter=2,
    )
    section = ParagraphStyle(
        "section",
        parent=styles["Normal"],
        fontSize=9,
        textColor=colors.HexColor("#525B6B"),
        spaceAfter=4,
    )
    label_pair = ParagraphStyle(
        "label_pair",
        parent=styles["Normal"],
        fontSize=9,
        leading=12,
    )

    elements: list[Any] = []

    # Header
    entity_name = entity.get("entity_name") or "Employer"
    elements.append(Paragraph(entity_name.upper(), h1))
    elements.append(Paragraph(BRIDLEWOOD_ADDRESS_LINE, addr))
    elements.append(Paragraph(f"BN: {PAYROLL_BUSINESS_NUMBER}", addr))
    elements.append(Spacer(1, 10))

    # Employee + period block
    employee_block = [
        [
            Paragraph(
                f"<b>Employee:</b> {employee.get('full_name') or '—'}",
                label_pair,
            ),
            Paragraph(
                f"<b>EE#:</b> {employee.get('employee_number') or '—'}",
                label_pair,
            ),
        ],
        [
            Paragraph(
                f"<b>Pay Period:</b> "
                f"{_fmt_date(run.get('period_start'))} — "
                f"{_fmt_date(run.get('period_end'))}",
                label_pair,
            ),
            Paragraph(
                f"<b>Pay Date:</b> {_fmt_date(run.get('pay_date'))}",
                label_pair,
            ),
        ],
        [
            Paragraph(
                f"<b>Pay Type:</b> {(employee.get('employment_type') or '—').title()}",
                label_pair,
            ),
            Paragraph(
                f"<b>Province:</b> {employee.get('province') or 'ON'}",
                label_pair,
            ),
        ],
    ]
    t = Table(employee_block, colWidths=[3.6 * inch, 3.2 * inch])
    t.setStyle(
        TableStyle([
            ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#D8DDE6")),
            ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#D8DDE6")),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ])
    )
    elements.append(t)
    elements.append(Spacer(1, 10))

    # Earnings + Deductions table.
    def row(label: str, current: Any, ytd_v: Any) -> list[str]:
        return [label, _money(current), _money(ytd_v)]

    reg_hours = run_line.get("total_hours") or 0
    stat_hours = ""  # we don't track stat hours separately yet
    table_data: list[list[Any]] = [
        ["EARNINGS", "CURRENT", "YTD"],
        row(
            f"Regular Hours ({reg_hours})",
            (run_line.get("reg_hours_pay") or 0) + (run_line.get("salary_pay") or 0),
            ytd.get("regular_pay") or 0,
        ),
        row("Stat Pay", run_line.get("stat_pay") or 0, ytd.get("stat_pay") or 0),
        row("Vacation Pay", run_line.get("vacation_paid") or 0, ytd.get("vacation_paid") or 0),
        row("Overtime", run_line.get("overtime_pay") or 0, ytd.get("overtime_pay") or 0),
        ["GROSS PAY", _money(run_line.get("gross_pay") or 0), _money(ytd.get("gross") or 0)],
        ["DEDUCTIONS", "CURRENT", "YTD"],
        row("Federal Tax", run_line.get("fed_tax") or 0, ytd.get("fed_tax") or 0),
        row("CPP", run_line.get("cpp_ee") or 0, ytd.get("cpp_employee") or 0),
        row("EI", run_line.get("ei_ee") or 0, ytd.get("ei_employee") or 0),
        row(
            "Add'l Fed Tax",
            run_line.get("additional_fed_tax") or 0,
            ytd.get("additional_fed_tax") or 0,
        ),
        row(
            "Add'l Prov Tax",
            run_line.get("additional_prov_tax") or 0,
            ytd.get("additional_prov_tax") or 0,
        ),
        [
            "TOTAL DEDUCTIONS",
            _money(
                (run_line.get("fed_tax") or 0)
                + (run_line.get("cpp_ee") or 0)
                + (run_line.get("ei_ee") or 0)
                + (run_line.get("additional_fed_tax") or 0)
                + (run_line.get("additional_prov_tax") or 0)
            ),
            "",
        ],
        ["NET PAY", _money(run_line.get("net_pay") or 0), ""],
    ]

    pay_table = Table(
        table_data,
        colWidths=[3.2 * inch, 1.8 * inch, 1.8 * inch],
        hAlign="LEFT",
    )
    # Indices of header / total rows for styling
    HEADER_ROWS = [0, 6]
    TOTAL_ROWS = [5, 12, 13]
    style = TableStyle([
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#C1C7D3")),
        ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#E1E5EC")),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
        ("ALIGN", (0, 0), (0, -1), "LEFT"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ])
    for r_idx in HEADER_ROWS:
        style.add("BACKGROUND", (0, r_idx), (-1, r_idx), colors.HexColor("#0B2E72"))
        style.add("TEXTCOLOR", (0, r_idx), (-1, r_idx), colors.white)
        style.add("FONTNAME", (0, r_idx), (-1, r_idx), "Helvetica-Bold")
    for r_idx in TOTAL_ROWS:
        style.add("BACKGROUND", (0, r_idx), (-1, r_idx), colors.HexColor("#EDF1F7"))
        style.add("FONTNAME", (0, r_idx), (-1, r_idx), "Helvetica-Bold")
    pay_table.setStyle(style)
    elements.append(pay_table)
    elements.append(Spacer(1, 8))

    # Vacation footer
    vac_accrued = run_line.get("vacation_earned") or 0
    vac_balance = employee.get("vacation_dollars_balance") or 0
    vac_block = Table(
        [
            [
                Paragraph(
                    f"<b>Vacation accrued this period:</b> {_money(vac_accrued)}",
                    label_pair,
                ),
                Paragraph(
                    f"<b>Vacation balance:</b> {_money(vac_balance)}",
                    label_pair,
                ),
            ],
        ],
        colWidths=[3.6 * inch, 3.2 * inch],
    )
    vac_block.setStyle(
        TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#F4F7FB")),
            ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#D8DDE6")),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ])
    )
    elements.append(vac_block)
    elements.append(Spacer(1, 8))

    masked_acct = _mask_account(employee.get("bank_account"))
    elements.append(
        Paragraph(
            f"Direct Deposit — {masked_acct}",
            ParagraphStyle("dd", parent=styles["Normal"], fontSize=8,
                           textColor=colors.HexColor("#525B6B")),
        )
    )
    elements.append(
        Paragraph(
            "Generated by BookWize. Last 4 digits of account shown for verification only.",
            ParagraphStyle("foot", parent=styles["Normal"], fontSize=7,
                           textColor=colors.HexColor("#9BA3B1")),
        )
    )

    doc.build(elements)
    return buf.getvalue()
