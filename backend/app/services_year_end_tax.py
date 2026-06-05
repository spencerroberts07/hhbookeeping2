"""
Year-end tax package (Phase 5C). A preparer-formatted PDF: income statement and
balance sheet with every account listed plus a CRA/GIFI note column, and a
fixed-asset continuity schedule.

Per the C4 decision, the structured depreciation module is VOIDED for Bridlewood,
so fixed-asset continuity is built straight from the GL: 15xx asset cost
(opening / FY additions / FY disposals / closing), 16xx accumulated depreciation
(opening / closing), and FY depreciation from the 6900 lump (shown as one line,
not split per asset). PDF via the same ReportLab engine as 4A; stored in R2.
"""
from __future__ import annotations

import io
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import text

from .routes.reports import (
    _account_sums,
    _account_type,
    get_balance_sheet,
    get_income_statement,
)
from .services_month_end_pdf import HAIRLINE, INK, SLATE, TD_GREEN, _money

# Best-effort CRA/GIFI note hints by account code. The preparer confirms the
# final GIFI codes; blank where not mapped.
_CRA_NOTES: dict[str, str] = {
    "1020": "GIFI 1001/1060 — Cash/bank",
    "1090": "GIFI 1060 — Accounts receivable",
    "1120": "GIFI 1120 — Inventory",
    "1125": "GIFI 1120 — Inventory",
    "1510": "GIFI 1740/8230 — Equipment (CCA cl.8)",
    "1520": "GIFI 1770 — Vehicles (CCA cl.10)",
    "1530": "GIFI 1680 — Leasehold improvements",
    "1540": "GIFI 1774 — Computers (CCA cl.50)",
    "2020": "GIFI 2620 — Accounts payable",
    "2030": "GIFI 2620 — Accounts payable",
    "2301": "GIFI 2680 — GST/HST payable",
    "2320": "GIFI 2700 — Payroll source deductions payable",
    "2500": "GIFI 2620/3140 — Long-term debt",
    "2510": "GIFI 2700 — DGIP loan",
    "4000": "GIFI 8000 — Sales/revenue",
    "5010": "GIFI 8320 — Cost of goods sold",
    "6900": "GIFI 8670 — Amortization/depreciation",
    "7000": "GIFI 8230 — Other income (DGIP forgiveness)",
}


def _D(v) -> Decimal:
    return Decimal(str(v or 0))


def _cra_note(code: str) -> str:
    return _CRA_NOTES.get(code, "")


def _fixed_asset_continuity(session, entity_id: str, fy: int) -> dict[str, Any]:
    fy_start = date(fy - 1, 10, 1)
    fy_end = date(fy, 9, 30)
    prior_close = fy_start - timedelta(days=1)

    opening = {r["account_code"]: r for r in
               _account_sums(session, entity_id=entity_id, period_end_from=None, period_end_to=prior_close)}
    closing = {r["account_code"]: r for r in
               _account_sums(session, entity_id=entity_id, period_end_from=None, period_end_to=fy_end)}
    movement = {r["account_code"]: r for r in
                _account_sums(session, entity_id=entity_id, period_end_from=fy_start, period_end_to=fy_end)}

    def cum(m, code, credit_natural=False):
        r = m.get(code)
        if not r:
            return Decimal("0")
        d, c = _D(r["sum_debit"]), _D(r["sum_credit"])
        return (c - d) if credit_natural else (d - c)

    def name(code):
        r = closing.get(code) or opening.get(code) or {}
        return r.get("account_name") or code

    cost_codes = sorted(c for c in set(opening) | set(closing) if c.startswith("15"))
    accum_codes = sorted(c for c in set(opening) | set(closing) if c.startswith("16"))

    assets = []
    tot_open_cost = tot_add = tot_disp = tot_close_cost = Decimal("0")
    for code in cost_codes:
        open_cost = cum(opening, code)
        mr = movement.get(code, {})
        additions = _D(mr.get("sum_debit"))
        disposals = _D(mr.get("sum_credit"))
        close_cost = cum(closing, code)
        if open_cost == 0 and close_cost == 0 and additions == 0 and disposals == 0:
            continue
        assets.append({"account_code": code, "account_name": name(code),
                       "opening_cost": float(open_cost), "additions": float(additions),
                       "disposals": float(disposals), "closing_cost": float(close_cost)})
        tot_open_cost += open_cost; tot_add += additions; tot_disp += disposals; tot_close_cost += close_cost

    accum = []
    tot_open_accum = tot_close_accum = Decimal("0")
    for code in accum_codes:
        open_a = cum(opening, code, credit_natural=True)
        close_a = cum(closing, code, credit_natural=True)
        if open_a == 0 and close_a == 0:
            continue
        accum.append({"account_code": code, "account_name": name(code),
                      "opening_accum": float(open_a), "closing_accum": float(close_a)})
        tot_open_accum += open_a; tot_close_accum += close_a

    # FY depreciation from the 6900 lump (C4) — debit-natural expense movement.
    dep_6900 = cum(movement, "6900")

    closing_nbv = tot_close_cost - tot_close_accum
    return {
        "fy": fy,
        "cost_accounts": assets,
        "accum_accounts": accum,
        "totals": {
            "opening_cost": float(tot_open_cost), "additions": float(tot_add),
            "disposals": float(tot_disp), "closing_cost": float(tot_close_cost),
            "opening_accum": float(tot_open_accum), "closing_accum": float(tot_close_accum),
            "closing_nbv": float(closing_nbv),
        },
        "fy_depreciation_6900_lump": float(dep_6900),
        "note": "Depreciation is the 6900 GL lump (per JE), not split per asset "
                "(structured depreciation module not used for this entity).",
    }


def get_tax_package_data(session, *, entity_code: str, fy: int) -> dict[str, Any]:
    entity_id = session.execute(
        text("SELECT id, entity_name FROM entities WHERE entity_code=:ec"), {"ec": entity_code}
    ).mappings().first()
    if not entity_id:
        raise ValueError(f"entity {entity_code} not found")
    eid = str(entity_id["id"])
    fy_start = date(fy - 1, 10, 1)
    fy_end = date(fy, 9, 30)

    income_statement = get_income_statement(
        entity_code=entity_code, preset="custom", period_end=None,
        date_from=fy_start.isoformat(), date_to=fy_end.isoformat(), _user=None)
    balance_sheet = get_balance_sheet(entity_code=entity_code, as_of_date=fy_end.isoformat(), _user=None)
    continuity = _fixed_asset_continuity(session, eid, fy)
    return {
        "entity_code": entity_code,
        "entity_name": entity_id["entity_name"] or entity_code,
        "fy": fy,
        "fy_start": fy_start.isoformat(),
        "fy_end": fy_end.isoformat(),
        "income_statement": income_statement,
        "balance_sheet": balance_sheet,
        "fixed_asset_continuity": continuity,
    }


# --------------------------------------------------------------------------
# PDF (same ReportLab engine/styling as 4A)
# --------------------------------------------------------------------------

def _render_tax_pdf(data: dict[str, Any]) -> bytes:
    from reportlab.lib.pagesizes import LETTER
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import (
        BaseDocTemplate, PageTemplate, Frame, Paragraph, Spacer, Table, TableStyle, PageBreak,
    )

    green = colors.HexColor(TD_GREEN); ink = colors.HexColor(INK)
    slate = colors.HexColor(SLATE); hair = colors.HexColor(HAIRLINE)
    styles = getSampleStyleSheet()
    h1 = ParagraphStyle("h1", parent=styles["Heading1"], fontSize=19, textColor=green, spaceAfter=4)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=12, textColor=ink, spaceBefore=12, spaceAfter=6)
    small = ParagraphStyle("small", parent=styles["Normal"], fontSize=8, textColor=slate, leading=11)
    note = ParagraphStyle("note", parent=styles["Normal"], fontSize=8, textColor=slate, italic=True, leading=11)

    entity_name = data["entity_name"]
    buf = io.BytesIO()

    def hf(canvas, doc):
        canvas.saveState()
        canvas.setStrokeColor(green); canvas.setLineWidth(2)
        canvas.line(0.75*inch, LETTER[1]-0.6*inch, LETTER[0]-0.75*inch, LETTER[1]-0.6*inch)
        canvas.setFont("Helvetica-Bold", 8); canvas.setFillColor(green)
        canvas.drawString(0.75*inch, LETTER[1]-0.5*inch, f"{entity_name.upper()} — TAX PACKAGE")
        canvas.setStrokeColor(hair); canvas.setLineWidth(0.5)
        canvas.line(0.75*inch, 0.62*inch, LETTER[0]-0.75*inch, 0.62*inch)
        canvas.setFont("Helvetica", 7.5); canvas.setFillColor(slate)
        canvas.drawString(0.75*inch, 0.45*inch, "Prepared by BookWize")
        canvas.drawRightString(LETTER[0]-0.75*inch, 0.45*inch, f"Page {doc.page}")
        canvas.restoreState()

    doc = BaseDocTemplate(buf, pagesize=LETTER, leftMargin=0.75*inch, rightMargin=0.75*inch,
                          topMargin=0.85*inch, bottomMargin=0.8*inch, title="Year-End Tax Package")
    frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="m")
    doc.addPageTemplates([PageTemplate(id="all", frames=[frame], onPage=hf)])
    W = doc.width

    def tbl(rows, cw, align_right_from=1):
        t = Table(rows, colWidths=cw, repeatRows=1)
        t.setStyle(TableStyle([
            ("FONTSIZE", (0,0), (-1,-1), 7.5), ("TEXTCOLOR", (0,0), (-1,-1), ink),
            ("LINEBELOW", (0,0), (-1,-1), 0.25, hair),
            ("TOPPADDING", (0,0), (-1,-1), 2.5), ("BOTTOMPADDING", (0,0), (-1,-1), 2.5),
            ("ALIGN", (align_right_from,0), (-1,-1), "RIGHT"),
            ("BACKGROUND", (0,0), (-1,0), green), ("TEXTCOLOR", (0,0), (-1,0), colors.white),
            ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ]))
        return t

    story: list[Any] = []
    story += [Spacer(1, 70), Paragraph("Year-End Tax Package", h1),
              Paragraph(f"{entity_name} — FY{data['fy']} ({data['fy_start']} to {data['fy_end']})",
                        ParagraphStyle("s", parent=h2, spaceBefore=0)),
              Paragraph("For preparer use. CRA/GIFI notes are hints; confirm final GIFI codes.", note),
              PageBreak()]

    # Income statement (all accounts + CRA note)
    story.append(Paragraph("Income Statement — preparer detail", h2))
    isd = data["income_statement"]
    rows = [["Account", "Amount", "CRA / GIFI note"]]
    for sec in isd.get("sections", []):
        rows.append([sec.get("section", ""), "", ""])
        for a in sec.get("accounts", []):
            if a.get("is_group_header") or a.get("is_group_subtotal"):
                continue
            code = a.get("account_code", "")
            rows.append([f'  {code} {a.get("account_name","")}',
                         _money(a.get("current_amount"), blank_zero=True), _cra_note(code)])
    story.append(tbl(rows, [W*0.46, W*0.18, W*0.36]))
    story.append(PageBreak())

    # Balance sheet (all accounts + CRA note)
    story.append(Paragraph("Balance Sheet — preparer detail", h2))
    bs = data["balance_sheet"]
    rows = [["Account", "Balance", "CRA / GIFI note"]]

    def bs_group(title, accounts):
        rows.append([title, "", ""])
        for a in accounts:
            rows.append([f'  {a["account_code"]} {a["account_name"]}',
                         _money(a["balance"]), _cra_note(a["account_code"])])

    bs_group("CURRENT ASSETS", bs["assets"]["current"])
    bs_group("FIXED ASSETS", bs["assets"]["fixed"])
    bs_group("CURRENT LIABILITIES", bs["liabilities"]["current"])
    bs_group("LONG-TERM LIABILITIES", bs["liabilities"]["long_term"])
    bs_group("EQUITY", bs["equity"]["accounts"])
    story.append(tbl(rows, [W*0.46, W*0.18, W*0.36]))
    story.append(PageBreak())

    # Fixed asset continuity
    story.append(Paragraph("Fixed Asset Continuity", h2))
    fa = data["fixed_asset_continuity"]
    rows = [["Asset (cost)", "Opening", "Additions", "Disposals", "Closing"]]
    for a in fa["cost_accounts"]:
        rows.append([f'{a["account_code"]} {a["account_name"]}', _money(a["opening_cost"]),
                     _money(a["additions"]), _money(a["disposals"]), _money(a["closing_cost"])])
    t = fa["totals"]
    rows.append(["TOTAL COST", _money(t["opening_cost"]), _money(t["additions"]),
                 _money(t["disposals"]), _money(t["closing_cost"])])
    story.append(tbl(rows, [W*0.34, W*0.16, W*0.16, W*0.16, W*0.18]))
    story.append(Spacer(1, 8))

    rows = [["Accumulated depreciation", "Opening", "Closing"]]
    for a in fa["accum_accounts"]:
        rows.append([f'{a["account_code"]} {a["account_name"]}',
                     _money(a["opening_accum"]), _money(a["closing_accum"])])
    rows.append(["TOTAL ACCUM. DEP.", _money(t["opening_accum"]), _money(t["closing_accum"])])
    story.append(tbl(rows, [W*0.5, W*0.25, W*0.25]))
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        f'FY depreciation (6900 lump): {_money(fa["fy_depreciation_6900_lump"])} &nbsp;·&nbsp; '
        f'Closing net book value: {_money(t["closing_nbv"])}', small))
    story.append(Paragraph(fa["note"], note))

    doc.build(story)
    return buf.getvalue()


def generate_tax_package(session, *, entity_code: str, fy: int) -> dict[str, Any]:
    data = get_tax_package_data(session, entity_code=entity_code, fy=fy)
    pdf_bytes = _render_tax_pdf(data)
    r2_key = None
    presigned = None
    try:
        from .services_storage import storage_service
        r2_key = storage_service.upload_file(
            file_bytes=pdf_bytes, original_filename=f"tax-package-{entity_code}-FY{fy}.pdf",
            entity_code=entity_code, document_type="tax-package", content_type="application/pdf")
        if r2_key:
            presigned = storage_service.get_presigned_url(r2_key, expires_in=86400)
    except Exception:
        r2_key = None
    return {
        "entity_code": entity_code, "fy": fy, "status": "ready",
        "r2_object_key": r2_key, "presigned_url": presigned,
        "pdf_bytes_len": len(pdf_bytes),
        "fixed_asset_continuity": data["fixed_asset_continuity"],
        "pdf_bytes": pdf_bytes,
    }
