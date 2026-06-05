"""Month-end document tests (Phase 4A).

Pure-function tests cover money formatting and IS-mover extraction. A live-DB
integration test renders the Bridlewood Feb 2026 PDF and asserts the section
list comes back (graceful degradation means a degraded section is acceptable,
but the document must still render to bytes).
"""
from datetime import date

import pytest

from app.services_month_end_pdf import _money, _top_is_movers
from app.services_email import send_email, email_configured


def test_money_formatting():
    assert _money(0) == "$0.00"
    assert _money(1234.5) == "$1,234.50"
    assert _money(-1234.5) == "($1,234.50)"
    assert _money(None) == ""
    assert _money(0, blank_zero=True) == ""


def test_top_is_movers_ranks_by_abs_delta():
    is_data = {
        "sections": [
            {"section": "INCOME", "accounts": [
                {"account_code": "4000", "account_name": "Sales", "current_amount": 1000.0,
                 "prior_amount": 600.0},
                {"account_code": "4010", "account_name": "Svc", "current_amount": 100.0,
                 "prior_amount": 100.0},  # no movement -> excluded
                {"account_code": "H", "account_name": "hdr", "is_group_header": True,
                 "current_amount": 9999.0, "prior_amount": 0.0},  # header -> excluded
            ]},
            {"section": "EXPENSES", "accounts": [
                {"account_code": "6000", "account_name": "Rent", "current_amount": 500.0,
                 "prior_amount": 50.0},
            ]},
        ]
    }
    movers = _top_is_movers(is_data, 10)
    assert [m["account"] for m in movers] == ["6000 Rent", "4000 Sales"]  # 450 delta > 400
    assert movers[0]["delta"] == 450.0
    assert movers[0]["pct_change"] == 900.0


def test_email_no_op_when_unconfigured(monkeypatch):
    monkeypatch.delenv("RESEND_API_KEY", raising=False)
    assert email_configured() is False
    res = send_email(to=["a@b.com"], subject="x", html="<p>x</p>")
    assert res["skipped"] is True and res["sent"] is False


def _has_db() -> bool:
    try:
        from app.db import db_session
        from sqlalchemy import text
        with db_session() as s:
            s.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _has_db(), reason="no live DB connection")
def test_feb_2026_month_end_pdf_renders():
    from app.services_month_end_pdf import generate_month_end_document
    res = generate_month_end_document(entity_code="1877-8", period_end=date(2026, 2, 28))
    assert res["status"] == "ready"
    assert res["pdf_bytes_len"] > 5000
    assert res["pdf_bytes"][:4] == b"%PDF"
    ready = [s for s in res["sections"] if s["state"] == "ready"]
    # core financial sections must render (commentary may degrade w/o Claude)
    by_name = {s["section"]: s["state"] for s in res["sections"]}
    for core in ("cover", "income_statement", "balance_sheet", "je_summary", "close_checklist"):
        assert by_name[core] == "ready", f"{core} degraded"
    assert len(ready) >= 8
