"""Year-end statements tests (Phase 5B)."""
from datetime import date

import pytest

from app.services_year_end_reports import _fy_bounds


def test_fy_bounds():
    start, end, prior_close = _fy_bounds(2026)
    assert start == date(2025, 10, 1)
    assert end == date(2026, 9, 30)
    assert prior_close == date(2025, 9, 30)


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
def test_fy2026_cash_flow_reconciles():
    from app.db import db_session
    from app.services_year_end_reports import get_year_end_statements
    with db_session() as s:
        st = get_year_end_statements(s, entity_code="1877-8", fy=2026)
    cf = st["cash_flow"]
    # the three sections must reconcile to the actual change in cash (1020)
    assert abs(cf["net_change_in_cash"] - cf["actual_change_in_cash_1020"]) <= 1.00
    assert cf["ties"] is True
    # statements present
    assert st["income_statement"]["sections"]
    assert st["balance_sheet"]["current"]["balanced"] is True
