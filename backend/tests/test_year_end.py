"""Year-end workflow tests (Phase 5A)."""
from datetime import date

import pytest

from app.services_year_end import fy_of, is_fiscal_year_end


def test_fy_of():
    # Oct-Dec roll into the next fiscal year; Jan-Sep stay
    assert fy_of(date(2025, 10, 1)) == 2026
    assert fy_of(date(2025, 12, 31)) == 2026
    assert fy_of(date(2026, 1, 1)) == 2026
    assert fy_of(date(2026, 9, 30)) == 2026
    assert fy_of(date(2026, 10, 1)) == 2027


def test_is_fiscal_year_end():
    assert is_fiscal_year_end(date(2026, 9, 30)) is True
    assert is_fiscal_year_end(date(2026, 2, 28)) is False
    assert is_fiscal_year_end(date(2026, 9, 29)) is False


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
def test_get_year_end_status_smoke():
    from app.db import db_session
    from app.services_year_end import get_year_end_status
    with db_session() as s:
        st = get_year_end_status(s, entity_code="1877-8", fy=2026)
    assert st["fy"] == 2026
    assert st["fy_start"] == "2025-10-01"
    assert st["fy_end"] == "2026-09-30"
    assert "year_end_status" in st
    assert st["periods_total"] >= 0
