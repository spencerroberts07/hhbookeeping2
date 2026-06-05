"""
Month-end document assembly (Phase 4A). Gathers the close-period financials into
a single professional PDF via ReportLab Platypus (the same engine as paystubs /
T4s — chosen over HTML->PDF because no HTML->PDF lib is installed and weasyprint
won't import on the Windows dev box that gates pytest; see plan C1).

Sections (D4-3 a-j): cover, 4-column income statement, balance sheet (cur vs PY),
bank-rec summary, HH AP reconciliation, AP aging, JE summary, ratios snapshot,
Haiku variance commentary (graceful degradation), close checklist.

Every section gatherer is defensive: a missing table / empty data degrades to a
note rather than blocking the document. The PDF bytes go to R2; only the object
key is persisted (month_end_documents). Never stores bytes in Postgres.
"""
from __future__ import annotations

import io
import json
import uuid as _uuid
from datetime import date
from decimal import Decimal
from typing import Any

from sqlalchemy import text

from .db import db_session

TD_GREEN = "#00843D"
INK = "#1A2330"
SLATE = "#525B6B"
HAIRLINE = "#D8DDE6"

_EXCLUDED_JE_MODULES = (
    "bank_auto_journal", "historical_import", "cash_balancing", "hh_ap_auto_match",
)


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------

def _D(v) -> Decimal:
    return Decimal(str(v or 0))


def _money(v: Any, *, blank_zero: bool = False) -> str:
    if v is None:
        return ""
    n = _D(v)
    if blank_zero and n == 0:
        return ""
    neg = n < 0
    s = f"{abs(n):,.2f}"
    return f"(${s})" if neg else f"${s}"


def _pct(v: Any) -> str:
    if v is None:
        return ""
    return f"{float(v):.1f}%"


def _shift_12mo(d: date) -> date:
    try:
        return d.replace(year=d.year - 1)
    except ValueError:
        return d.replace(year=d.year - 1, day=28)


# --------------------------------------------------------------------------
# section gatherers (each defensive; returns {"available": bool, ...})
# --------------------------------------------------------------------------

def _gather_period(session, entity_id: str, period_end: date) -> dict[str, Any]:
    row = session.execute(
        text(
            """
            SELECT id, period_label, period_start, period_end, status,
                   fiscal_year, closed_at, closed_by
              FROM accounting_periods
             WHERE entity_id = :e AND period_end = :pe
            """
        ),
        {"e": entity_id, "pe": period_end},
    ).mappings().first()
    return dict(row) if row else {}


def _gather_income_statement(entity_code: str, period_end: date) -> dict[str, Any]:
    try:
        from .routes.reports import get_income_statement
        return {"available": True, "data": get_income_statement(
            entity_code=entity_code, preset="month",
            period_end=period_end.isoformat(), date_from=None, date_to=None, _user=None)}
    except Exception as exc:
        return {"available": False, "note": f"Income statement unavailable: {exc!r}"[:200]}


def _gather_balance_sheet(entity_code: str, period_end: date) -> dict[str, Any]:
    try:
        from .routes.reports import get_balance_sheet
        cur = get_balance_sheet(entity_code=entity_code, as_of_date=period_end.isoformat(), _user=None)
        try:
            prior = get_balance_sheet(
                entity_code=entity_code, as_of_date=_shift_12mo(period_end).isoformat(), _user=None)
        except Exception:
            prior = None
        return {"available": True, "current": cur, "prior": prior}
    except Exception as exc:
        return {"available": False, "note": f"Balance sheet unavailable: {exc!r}"[:200]}


def _gather_bank_rec(session, entity_id: str, period_id: str) -> dict[str, Any]:
    rows = session.execute(
        text(
            """
            SELECT source_account_code, book_balance, outstanding_cheques_total,
                   outstanding_deposits_total, bank_only_items_total, variance,
                   ties, status, statement_closing_balance, summary_json
              FROM bank_reconciliations
             WHERE entity_id = :e AND accounting_period_id = :pid
          ORDER BY source_account_code
            """
        ),
        {"e": entity_id, "pid": period_id},
    ).mappings().all()
    if not rows:
        return {"available": False, "note": "Bank rec not completed for this period."}
    recs = []
    for r in rows:
        summ = r["summary_json"] or {}
        named = summ.get("named_items", {}) if isinstance(summ, dict) else {}
        recs.append({
            "account": r["source_account_code"],
            "book_balance": float(r["book_balance"] or 0),
            "statement_closing": float(r["statement_closing_balance"] or 0),
            "outstanding_cheques": float(r["outstanding_cheques_total"] or 0),
            "deposits_in_transit": float(named.get("deposits_in_transit", r["outstanding_deposits_total"] or 0)),
            "bank_only": float(r["bank_only_items_total"] or 0),
            "variance": float(r["variance"] or 0),
            "ties": bool(r["ties"]),
            "locked": r["status"] == "locked",
        })
    return {"available": True, "reconciliations": recs}


def _account_balance_credit(session, entity_id: str, codes: list[str], as_of: date) -> Decimal:
    """Sum of (credit - debit) for the given liability accounts as-of date."""
    from .routes.reports import _account_sums
    rows = _account_sums(session, entity_id=entity_id, period_end_from=None, period_end_to=as_of)
    total = Decimal("0")
    for r in rows:
        if r["account_code"] in codes:
            total += _D(r["sum_credit"]) - _D(r["sum_debit"])
    return total


def _gather_hh_ap_rec(session, entity_id: str, period_end: date) -> dict[str, Any]:
    try:
        stmt = session.execute(
            text(
                """
                SELECT statement_date, statement_month_end, total_open_balance, raw_json
                  FROM hh_ap_statements
                 WHERE entity_id = :e
              ORDER BY statement_month_end DESC NULLS LAST, created_at DESC
                 LIMIT 1
                """
            ),
            {"e": entity_id},
        ).mappings().first()
        book = _account_balance_credit(session, entity_id, ["2030", "2020"], period_end)
        if not stmt:
            return {"available": True, "statement_total": None,
                    "book_2030_2020": float(book), "variance": None,
                    "note": "No HH AP statement uploaded — book 2030+2020 shown for reference."}
        stmt_total = _D(stmt["total_open_balance"])
        variance = book - stmt_total
        return {
            "available": True,
            "statement_total": float(stmt_total),
            "statement_month_end": stmt["statement_month_end"].isoformat() if stmt["statement_month_end"] else None,
            "book_2030_2020": float(book),
            "variance": float(variance),
        }
    except Exception as exc:
        return {"available": False, "note": f"HH AP reconciliation unavailable: {exc!r}"[:200]}


def _gather_ap_aging(session, entity_id: str, period_end: date) -> dict[str, Any]:
    """AP aging buckets. Best-effort: read invoice-level due dates if the table
    exists; otherwise fall back to the HH AP statement open balance."""
    try:
        exists = session.execute(
            text("SELECT to_regclass('public.hh_ap_invoices')")
        ).scalar()
        rows = None
        if exists:
            try:
                rows = session.execute(
                    text(
                        """
                        SELECT due_date, total_amount
                          FROM hh_ap_invoices
                         WHERE entity_id = :e
                           AND COALESCE(match_status,'') <> 'matched'
                        """
                    ),
                    {"e": entity_id},
                ).mappings().all()
            except Exception:
                rows = None
                try:
                    session.rollback()
                except Exception:
                    pass
        if rows is not None:
            buckets = {"current": Decimal("0"), "31-60": Decimal("0"),
                       "61-90": Decimal("0"), "90+": Decimal("0")}
            for r in rows:
                amt = _D(r["total_amount"])
                due = r["due_date"]
                if due is None:
                    buckets["current"] += amt
                    continue
                days = (period_end - due).days
                if days <= 30:
                    buckets["current"] += amt
                elif days <= 60:
                    buckets["31-60"] += amt
                elif days <= 90:
                    buckets["61-90"] += amt
                else:
                    buckets["90+"] += amt
            return {"available": True, "source": "invoices",
                    "buckets": {k: float(v) for k, v in buckets.items()},
                    "total": float(sum(buckets.values()))}
        # fall back to statement open balance
        stmt = session.execute(
            text("""SELECT total_open_balance FROM hh_ap_statements WHERE entity_id=:e
                    ORDER BY statement_month_end DESC NULLS LAST, created_at DESC LIMIT 1"""),
            {"e": entity_id},
        ).scalar()
        return {"available": True, "source": "statement",
                "note": "Invoice-level aging not available; HH AP statement open balance shown.",
                "total": float(_D(stmt)) if stmt is not None else None}
    except Exception as exc:
        return {"available": False, "note": f"AP aging unavailable: {exc!r}"[:200]}


def _gather_je_summary(session, entity_id: str, period_id: str) -> dict[str, Any]:
    excl = ", ".join(f"'{m}'" for m in _EXCLUDED_JE_MODULES)  # constants, safe to inline
    rows = session.execute(
        text(
            f"""
            SELECT created_at, batch_label, source_module, total_debits,
                   status, approved_by
              FROM journal_batches
             WHERE entity_id = :e AND accounting_period_id = :pid
               AND COALESCE(source_module,'') NOT IN ({excl})
               AND status NOT IN ('voided','rejected')
          ORDER BY created_at
            """
        ),
        {"e": entity_id, "pid": period_id},
    ).mappings().all()
    entries = [{
        "date": r["created_at"].date().isoformat() if r["created_at"] else "",
        "description": r["batch_label"] or "",
        "source_module": r["source_module"] or "",
        "total_debits": float(r["total_debits"] or 0),
        "approved_by": r["approved_by"] or "",
    } for r in rows]
    return {"available": True, "entries": entries,
            "total": float(sum(_D(e["total_debits"]) for e in entries))}


def _gather_ratios(session, entity_id: str, period_start: date, period_end: date) -> dict[str, Any]:
    try:
        from .services_ratios import build_financials_context, compute_builtin_ratios, RATIO_META
        cur_ctx = build_financials_context(session, entity_id=entity_id,
                                           period_start=period_start, period_end=period_end)
        cur = compute_builtin_ratios(cur_ctx)
        try:
            p_start, p_end = _shift_12mo(period_start), _shift_12mo(period_end)
            prior = compute_builtin_ratios(build_financials_context(
                session, entity_id=entity_id, period_start=p_start, period_end=p_end))
        except Exception:
            prior = {}
        return {"available": True, "current": cur, "prior": prior, "meta": RATIO_META}
    except Exception as exc:
        return {"available": False, "note": f"Ratios unavailable: {exc!r}"[:200]}


def _top_is_movers(is_data: dict[str, Any], n: int = 10) -> list[dict[str, Any]]:
    movers: list[dict[str, Any]] = []
    for sec in is_data.get("sections", []):
        for acct in sec.get("accounts", []):
            if acct.get("is_group_header") or acct.get("is_group_subtotal"):
                continue
            cur = acct.get("current_amount")
            pri = acct.get("prior_amount")
            if cur is None and pri is None:
                continue
            delta = float(cur or 0) - float(pri or 0)
            if abs(delta) < 0.01:
                continue
            pct = None
            if pri:
                pct = (delta / abs(float(pri))) * 100.0
            movers.append({
                "account": f'{acct.get("account_code","")} {acct.get("account_name","")}'.strip(),
                "section": sec.get("section"),
                "current": float(cur or 0), "prior": float(pri or 0),
                "delta": round(delta, 2), "pct_change": round(pct, 1) if pct is not None else None,
            })
    movers.sort(key=lambda m: abs(m["delta"]), reverse=True)
    return movers[:n]


def _gather_commentary(is_section: dict[str, Any], period_label: str) -> dict[str, Any]:
    """Haiku variance commentary. Graceful: any failure -> unavailable."""
    if not is_section.get("available"):
        return {"available": False}
    movers = _top_is_movers(is_section["data"], 10)
    if not movers:
        return {"available": False}
    try:
        from .services_onboarding import _claude_parse_json
        system = (
            "You are a CPA writing the variance commentary section of a month-end "
            "financial package for a Home Hardware dealer. Given the top income-statement "
            "account movers (current period vs prior-year same period, in CAD), pick the 5 "
            "most significant and explain each in 1-2 plain-English business sentences. "
            "Return STRICT JSON: {\"commentary\": [{\"account\": str, \"movement\": str, "
            "\"explanation\": str}]}. 'movement' is a short $ and % summary. No preamble."
        )
        payload = json.dumps({"period": period_label, "movers": movers})
        result = _claude_parse_json(system, payload)
        if not result or not isinstance(result.get("commentary"), list):
            return {"available": False, "movers": movers}
        return {"available": True, "commentary": result["commentary"][:5], "movers": movers}
    except Exception:
        return {"available": False, "movers": movers}


def _gather_close_checklist(session, entity_code: str, period_end: date) -> dict[str, Any]:
    try:
        from .services_month_end_close import get_month_end_close_status
        status = get_month_end_close_status(session, entity_code=entity_code, period_end=period_end.isoformat())
        items = []
        for name, sec in (status.get("sections") or {}).items():
            if not isinstance(sec, dict):
                continue
            items.append({"module": name, "status": sec.get("status", "no_data"),
                          "summary": sec.get("summary", "")})
        return {"available": True, "overall": status.get("overall_close_readiness"), "items": items}
    except Exception as exc:
        return {"available": False, "note": f"Close checklist unavailable: {exc!r}"[:200]}


# --------------------------------------------------------------------------
# PDF rendering (ReportLab Platypus)
# --------------------------------------------------------------------------

def _render_pdf(*, entity_name: str, sections: dict[str, Any]) -> bytes:
    from reportlab.lib.pagesizes import LETTER
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import (
        BaseDocTemplate, PageTemplate, Frame, Paragraph, Spacer, Table, TableStyle, PageBreak,
    )

    green = colors.HexColor(TD_GREEN)
    ink = colors.HexColor(INK)
    slate = colors.HexColor(SLATE)
    hair = colors.HexColor(HAIRLINE)

    styles = getSampleStyleSheet()
    h1 = ParagraphStyle("h1", parent=styles["Heading1"], fontSize=20, textColor=green, spaceAfter=4)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=13, textColor=ink,
                        spaceBefore=14, spaceAfter=6)
    body = ParagraphStyle("body", parent=styles["Normal"], fontSize=9, textColor=ink, leading=13)
    small = ParagraphStyle("small", parent=styles["Normal"], fontSize=8, textColor=slate, leading=11)
    note = ParagraphStyle("note", parent=styles["Normal"], fontSize=8.5, textColor=slate,
                          leading=12, italic=True)

    buf = io.BytesIO()

    def _header_footer(canvas, doc):
        canvas.saveState()
        # header rule
        canvas.setStrokeColor(green)
        canvas.setLineWidth(2)
        canvas.line(0.75 * inch, LETTER[1] - 0.6 * inch, LETTER[0] - 0.75 * inch, LETTER[1] - 0.6 * inch)
        canvas.setFont("Helvetica-Bold", 8)
        canvas.setFillColor(green)
        canvas.drawString(0.75 * inch, LETTER[1] - 0.5 * inch, entity_name.upper())
        # footer
        canvas.setStrokeColor(hair)
        canvas.setLineWidth(0.5)
        canvas.line(0.75 * inch, 0.62 * inch, LETTER[0] - 0.75 * inch, 0.62 * inch)
        canvas.setFont("Helvetica", 7.5)
        canvas.setFillColor(slate)
        canvas.drawString(0.75 * inch, 0.45 * inch, "Prepared by BookWize")
        canvas.drawRightString(LETTER[0] - 0.75 * inch, 0.45 * inch, f"Page {doc.page}")
        canvas.restoreState()

    doc = BaseDocTemplate(buf, pagesize=LETTER,
                          leftMargin=0.75 * inch, rightMargin=0.75 * inch,
                          topMargin=0.85 * inch, bottomMargin=0.8 * inch,
                          title="Month-End Financial Package")
    frame = Frame(doc.leftMargin, doc.bottomMargin,
                  doc.width, doc.height, id="main")
    doc.addPageTemplates([PageTemplate(id="all", frames=[frame], onPage=_header_footer)])

    def tbl(data, col_widths, *, header=True, align_right_from=1):
        t = Table(data, colWidths=col_widths, repeatRows=1 if header else 0)
        ts = [
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("TEXTCOLOR", (0, 0), (-1, -1), ink),
            ("LINEBELOW", (0, 0), (-1, -1), 0.25, hair),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("ALIGN", (align_right_from, 0), (-1, -1), "RIGHT"),
        ]
        if header:
            ts += [
                ("BACKGROUND", (0, 0), (-1, 0), green),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ALIGN", (align_right_from, 0), (-1, 0), "RIGHT"),
            ]
        t.setStyle(TableStyle(ts))
        return t

    story: list[Any] = []
    W = doc.width

    # (a) Cover
    cov = sections["cover"]
    story += [Spacer(1, 80),
              Paragraph("Month-End Financial Package", h1),
              Spacer(1, 6),
              Paragraph(entity_name, ParagraphStyle("ent", parent=h2, fontSize=16, spaceBefore=0)),
              Spacer(1, 20)]
    cover_rows = [
        ["Period", cov.get("period_label", "")],
        ["Period status", cov.get("status", "")],
        ["Closed by", cov.get("closed_by") or "—"],
        ["Generated at", cov.get("generated_at", "")],
    ]
    ct = Table(cover_rows, colWidths=[1.8 * inch, W - 1.8 * inch])
    ct.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 10), ("TEXTCOLOR", (0, 0), (0, -1), slate),
        ("TEXTCOLOR", (1, 0), (1, -1), ink), ("FONTNAME", (1, 0), (1, -1), "Helvetica-Bold"),
        ("TOPPADDING", (0, 0), (-1, -1), 5), ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LINEBELOW", (0, 0), (-1, -1), 0.25, hair),
    ]))
    story += [ct, PageBreak()]

    # (b) Income statement (4-column)
    story.append(Paragraph("Income Statement", h2))
    is_s = sections["income_statement"]
    if is_s.get("available"):
        d = is_s["data"]
        story.append(Paragraph(
            f'{d.get("period_label","")} vs {d.get("prior_label","")} &nbsp;·&nbsp; % of sales',
            small))
        rows = [["Account", d.get("period_label", "Current"), d.get("prior_label", "Prior"),
                 "% Sales", "PY %"]]
        for sec in d.get("sections", []):
            rows.append([f'  {sec.get("section","")}', "", "", "", ""])
            for a in sec.get("accounts", []):
                indent = "    " * (1 + int(a.get("depth", 0)))
                rows.append([
                    f'{indent}{a.get("account_name","")}',
                    _money(a.get("current_amount"), blank_zero=True),
                    _money(a.get("prior_amount"), blank_zero=True),
                    _pct(a.get("current_pct")), _pct(a.get("prior_pct")),
                ])
            rows.append([f'  {sec.get("section","")} TOTAL',
                         _money(sec.get("section_total")), _money(sec.get("prior_total")),
                         _pct(sec.get("section_pct")), _pct(sec.get("prior_pct"))])
        cw = [W * 0.40, W * 0.16, W * 0.16, W * 0.14, W * 0.14]
        story.append(tbl(rows, cw))
    else:
        story.append(Paragraph(is_s.get("note", "Unavailable."), note))
    story.append(PageBreak())

    # (c) Balance sheet
    story.append(Paragraph("Balance Sheet", h2))
    bs_s = sections["balance_sheet"]
    if bs_s.get("available"):
        cur = bs_s["current"]
        prior = bs_s.get("prior") or {}
        pri_lookup: dict[str, float] = {}
        if prior:
            for grp in ("assets", "liabilities"):
                for sub in cur.get(grp, {}):
                    pass
            for grp, subs in (("assets", ("current", "fixed")),
                              ("liabilities", ("current", "long_term"))):
                for sub in subs:
                    for a in (prior.get(grp, {}).get(sub) or []):
                        pri_lookup[a["account_code"]] = a["balance"]
            for a in (prior.get("equity", {}).get("accounts") or []):
                pri_lookup[a["account_code"]] = a["balance"]
        story.append(Paragraph(
            f'As of {cur.get("as_of_date","")}'
            + (f' vs {prior.get("as_of_date","")}' if prior else ''), small))
        rows = [["Account", "Current", "Prior year"]]

        def add_group(title, accounts, total, total_label):
            rows.append([f"  {title}", "", ""])
            for a in accounts:
                rows.append([f'    {a["account_name"]}', _money(a["balance"]),
                             _money(pri_lookup.get(a["account_code"])) if prior else ""])
            rows.append([f"  {total_label}", _money(total), ""])

        add_group("CURRENT ASSETS", cur["assets"]["current"], cur["assets"]["current_total"], "Total current assets")
        add_group("FIXED ASSETS", cur["assets"]["fixed"], cur["assets"]["fixed_total"], "Total fixed assets")
        rows.append(["TOTAL ASSETS", _money(cur["assets"]["total"]), ""])
        add_group("CURRENT LIABILITIES", cur["liabilities"]["current"], cur["liabilities"]["current_total"], "Total current liabilities")
        add_group("LONG-TERM LIABILITIES", cur["liabilities"]["long_term"], cur["liabilities"]["long_term_total"], "Total long-term liabilities")
        add_group("EQUITY", cur["equity"]["accounts"], cur["equity"]["total"], "Total equity")
        rows.append(["TOTAL LIABILITIES & EQUITY", _money(cur["liabilities_and_equity_total"]), ""])
        cw = [W * 0.56, W * 0.22, W * 0.22]
        story.append(tbl(rows, cw))
        if not cur.get("balanced", True):
            story.append(Paragraph(f'⚠ Balance sheet out of balance by {_money(cur.get("variance"))}', note))
    else:
        story.append(Paragraph(bs_s.get("note", "Unavailable."), note))
    story.append(PageBreak())

    # (d) Bank rec summary
    story.append(Paragraph("Bank Reconciliation Summary", h2))
    br = sections["bank_rec"]
    if br.get("available"):
        for rec in br["reconciliations"]:
            badge = "TIES ✓" if rec["ties"] else f'OUT BY {_money(rec["variance"])}'
            lock = "locked" if rec["locked"] else "draft"
            story.append(Paragraph(f'Account {rec["account"]} — {badge} ({lock})', body))
            rows = [["Line", "Amount"],
                    ["Book balance (GL)", _money(rec["book_balance"])],
                    ["Less: deposits in transit", _money(-abs(rec["deposits_in_transit"]))],
                    ["Add: outstanding cheques", _money(rec["outstanding_cheques"])],
                    ["Bank-only items", _money(rec["bank_only"])],
                    ["Statement closing", _money(rec["statement_closing"])],
                    ["Variance", _money(rec["variance"])]]
            story.append(tbl(rows, [W * 0.6, W * 0.4]))
            story.append(Spacer(1, 8))
    else:
        story.append(Paragraph(br.get("note", "Bank rec not completed."), note))
    story.append(Spacer(1, 6))

    # (e) HH AP reconciliation
    story.append(Paragraph("HH AP Reconciliation", h2))
    ap = sections["hh_ap_rec"]
    if ap.get("available"):
        rows = [["Line", "Amount"],
                ["HH AP statement open balance", _money(ap.get("statement_total"))],
                ["Book (2030 + 2020)", _money(ap.get("book_2030_2020"))],
                ["Variance", _money(ap.get("variance"))]]
        story.append(tbl(rows, [W * 0.6, W * 0.4]))
        if ap.get("note"):
            story.append(Paragraph(ap["note"], note))
        if ap.get("variance") not in (None,) and abs(float(ap.get("variance") or 0)) >= 0.01:
            story.append(Paragraph("⚠ HH AP variance — review before close.", note))
    else:
        story.append(Paragraph(ap.get("note", "Unavailable."), note))
    story.append(Spacer(1, 6))

    # (f) AP aging
    story.append(Paragraph("AP Aging Summary", h2))
    aging = sections["ap_aging"]
    if aging.get("available") and aging.get("buckets"):
        b = aging["buckets"]
        rows = [["Current", "31–60", "61–90", "90+", "Total"],
                [_money(b["current"]), _money(b["31-60"]), _money(b["61-90"]),
                 _money(b["90+"]), _money(aging["total"])]]
        story.append(tbl(rows, [W * 0.2] * 5))
    elif aging.get("available"):
        story.append(Paragraph(
            (aging.get("note", "") + f' Open balance: {_money(aging.get("total"))}').strip(), note))
    else:
        story.append(Paragraph(aging.get("note", "Unavailable."), note))
    story.append(PageBreak())

    # (g) JE summary
    story.append(Paragraph("Journal Entry Summary", h2))
    je = sections["je_summary"]
    story.append(Paragraph(
        "Excludes automated bank, historical import, cash-balancing and HH AP auto-match batches.",
        small))
    if je["entries"]:
        rows = [["Date", "Description", "Module", "Debits", "Approved by"]]
        for e in je["entries"]:
            rows.append([e["date"], e["description"][:42], e["source_module"],
                         _money(e["total_debits"]), e["approved_by"][:22]])
        rows.append(["", "TOTAL", "", _money(je["total"]), ""])
        cw = [W * 0.12, W * 0.36, W * 0.20, W * 0.16, W * 0.16]
        story.append(tbl(rows, cw, align_right_from=3))
    else:
        story.append(Paragraph("No manual / adjusting journal entries this period.", note))
    story.append(PageBreak())

    # (h) Ratios snapshot
    story.append(Paragraph("Ratios Snapshot", h2))
    rt = sections["ratios"]
    if rt.get("available"):
        meta = rt["meta"]
        cur, prior = rt["current"], rt.get("prior", {})
        rows = [["Ratio", "Category", "Current", "Prior year"]]
        for key, m in meta.items():
            cv, pv = cur.get(key), prior.get(key)
            fmt = m.get("format")
            def fnum(v):
                if v is None:
                    return ""
                if fmt == "percent":
                    return f"{float(v):.1f}%"
                if fmt == "dollar":
                    return _money(v)
                if fmt == "days":
                    return f"{float(v):.0f}"
                return f"{float(v):.2f}"
            rows.append([m["label"], m["category"], fnum(cv), fnum(pv)])
        cw = [W * 0.34, W * 0.22, W * 0.22, W * 0.22]
        story.append(tbl(rows, cw, align_right_from=2))
    else:
        story.append(Paragraph(rt.get("note", "Unavailable."), note))
    story.append(PageBreak())

    # (i) Variance commentary
    story.append(Paragraph("Variance Commentary", h2))
    com = sections["commentary"]
    if com.get("available"):
        for c in com["commentary"]:
            story.append(Paragraph(f'<b>{c.get("account","")}</b> — {c.get("movement","")}', body))
            story.append(Paragraph(c.get("explanation", ""), small))
            story.append(Spacer(1, 6))
    else:
        story.append(Paragraph("Commentary unavailable.", note))
    story.append(PageBreak())

    # (j) Close checklist
    story.append(Paragraph("Close Checklist", h2))
    cc = sections["close_checklist"]
    if cc.get("available"):
        story.append(Paragraph(f'Overall readiness: <b>{cc.get("overall","")}</b>', body))
        glyph = {"ready": "✓", "needs_review": "⚠", "blocked": "✗", "no_data": "–",
                 "not_started": "–", "closed_locked": "✓"}
        rows = [["", "Module", "Status", "Summary"]]
        for it in cc["items"]:
            rows.append([glyph.get(it["status"], "–"), it["module"], it["status"], it["summary"][:60]])
        cw = [W * 0.05, W * 0.22, W * 0.16, W * 0.57]
        story.append(tbl(rows, cw, align_right_from=4))
    else:
        story.append(Paragraph(cc.get("note", "Unavailable."), note))

    doc.build(story)
    return buf.getvalue()


# --------------------------------------------------------------------------
# public entry point
# --------------------------------------------------------------------------

def generate_month_end_document(
    *, entity_code: str, period_end: date, generated_by: str | None = None,
) -> dict[str, Any]:
    """Assemble + render the month-end PDF, store to R2, upsert the
    month_end_documents row. Returns a summary dict (never raises on section
    failure; raises only on a hard entity/period lookup error)."""
    from datetime import datetime, timezone

    with db_session() as session:
        entity = session.execute(
            text("SELECT id, entity_name FROM entities WHERE entity_code = :ec"),
            {"ec": entity_code},
        ).mappings().first()
        if not entity:
            raise ValueError(f"entity {entity_code} not found")
        entity_id = str(entity["id"])
        entity_name = entity["entity_name"] or entity_code

        period = _gather_period(session, entity_id, period_end)
        if not period:
            raise ValueError(f"no accounting period ending {period_end} for {entity_code}")
        period_id = str(period["id"])
        period_start = period["period_start"]

        # mark generating
        session.execute(
            text(
                """
                INSERT INTO month_end_documents (entity_id, accounting_period_id, status, generated_by)
                VALUES (:e, :pid, 'generating', :by)
                ON CONFLICT (entity_id, accounting_period_id) DO UPDATE
                  SET status='generating', generated_by=:by, error_msg=NULL, updated_at=NOW()
                """
            ),
            {"e": entity_id, "pid": period_id, "by": generated_by},
        )

    # gather all sections (defensive). Each gatherer that hits the DB runs in its
    # OWN session so a failed query can't poison a shared transaction.
    is_section = _gather_income_statement(entity_code, period_end)

    def _in_session(fn, *args):
        try:
            with db_session() as s:
                return fn(s, *args)
        except Exception as exc:
            return {"available": False, "note": f"{fn.__name__} failed: {exc!r}"[:200]}

    sections = {
        "cover": {
            "period_label": period.get("period_label", ""),
            "status": period.get("status", ""),
            "closed_by": period.get("closed_by"),
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        },
        "income_statement": is_section,
        "balance_sheet": _gather_balance_sheet(entity_code, period_end),
        "bank_rec": _in_session(_gather_bank_rec, entity_id, period_id),
        "hh_ap_rec": _in_session(_gather_hh_ap_rec, entity_id, period_end),
        "ap_aging": _in_session(_gather_ap_aging, entity_id, period_end),
        "je_summary": _in_session(_gather_je_summary, entity_id, period_id),
        "ratios": _in_session(_gather_ratios, entity_id, period_start, period_end),
        "commentary": _gather_commentary(is_section, period.get("period_label", "")),
        "close_checklist": _in_session(_gather_close_checklist, entity_code, period_end),
    }

    section_list = _section_status_list(sections)

    try:
        pdf_bytes = _render_pdf(entity_name=entity_name, sections=sections)
    except Exception as exc:
        with db_session() as session:
            session.execute(
                text("""UPDATE month_end_documents SET status='failed', error_msg=:m, updated_at=NOW()
                        WHERE entity_id=:e AND accounting_period_id=:pid"""),
                {"m": f"render failed: {exc!r}"[:500], "e": entity_id, "pid": period_id},
            )
        raise

    # store to R2 (fail-tolerant)
    r2_key = None
    try:
        from .services_storage import storage_service
        fn = f"month-end-{entity_code}-{period_end.isoformat()}.pdf"
        r2_key = storage_service.upload_file(
            file_bytes=pdf_bytes, original_filename=fn, entity_code=entity_code,
            document_type="month-end-documents", content_type="application/pdf")
    except Exception:
        r2_key = None

    commentary_json = sections["commentary"].get("commentary") if sections["commentary"].get("available") else None

    with db_session() as session:
        session.execute(
            text(
                """
                UPDATE month_end_documents
                   SET status='ready', r2_object_key=:k, generated_at=NOW(),
                       commentary_json=CAST(:cj AS jsonb), updated_at=NOW()
                 WHERE entity_id=:e AND accounting_period_id=:pid
                """
            ),
            {"k": r2_key, "cj": json.dumps(commentary_json) if commentary_json else None,
             "e": entity_id, "pid": period_id},
        )

    presigned = None
    if r2_key:
        try:
            from .services_storage import storage_service
            presigned = storage_service.get_presigned_url(r2_key, expires_in=86400)
        except Exception:
            presigned = None

    return {
        "entity_code": entity_code,
        "period_end": period_end.isoformat(),
        "status": "ready",
        "r2_object_key": r2_key,
        "presigned_url": presigned,
        "pdf_bytes_len": len(pdf_bytes),
        "sections": section_list,
        "commentary_available": sections["commentary"].get("available", False),
        "pdf_bytes": pdf_bytes,  # for in-process callers (email); not serialized to API
    }


def _section_status_list(sections: dict[str, Any]) -> list[dict[str, str]]:
    out = []
    for name, s in sections.items():
        if name == "cover":
            out.append({"section": name, "state": "ready"})
        elif isinstance(s, dict):
            out.append({"section": name, "state": "ready" if s.get("available") else "degraded"})
    return out
