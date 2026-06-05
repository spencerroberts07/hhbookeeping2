"""Year-end tax package tests (Phase 5C)."""
import pytest

from app.services_year_end_tax import _cra_note


def test_cra_note_mapping():
    assert "GIFI" in _cra_note("1020")
    assert _cra_note("9999_unknown") == ""


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
def test_fy2026_tax_package_generates():
    from app.db import db_session
    from app.services_year_end_tax import generate_tax_package
    with db_session() as s:
        res = generate_tax_package(s, entity_code="1877-8", fy=2026)
    assert res["status"] == "ready"
    assert res["pdf_bytes"][:4] == b"%PDF"
    fa = res["fixed_asset_continuity"]
    # GL-based continuity: closing NBV = closing cost - closing accum dep
    t = fa["totals"]
    assert abs(t["closing_nbv"] - (t["closing_cost"] - t["closing_accum"])) < 0.01
    # FY 6900 depreciation lump is present (C4 — module voided, GL is source)
    assert "fy_depreciation_6900_lump" in fa
