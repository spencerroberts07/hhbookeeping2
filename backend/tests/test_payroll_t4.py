"""T4 generation tests (Phase 5D)."""
import pytest


def test_box_derivability_flags():
    from app.routes.payroll_t4 import _BOX_DERIVABILITY
    by_box = {b: (st, note) for (b, _l, st, note) in _BOX_DERIVABILITY}
    # the two known gaps the pre-flight must surface
    assert by_box["22"][0] == "partial"           # federal-only income tax
    assert "FEDERAL ONLY" in by_box["22"][1]
    assert by_box["17"][0] == "missing"           # CPP2 not computed
    assert by_box["SIN"][0] == "missing"          # SIN not stored


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
def test_t4_preflight_and_generate_2026():
    from app.routes.payroll_t4 import preflight, generate
    pf = preflight(2026, entity_code="1877-8", _user=None)
    assert pf["employees_with_pay"] > 0
    assert any(b["box"] == "22" and b["status"] == "partial" for b in pf["box_derivability"])

    res = generate(2026, entity_code="1877-8", actor_email="test@bookwize.ca", _user=None)
    assert res["t4_count"] == pf["employees_with_pay"]
    assert res["filed_with_cra"] is False           # never filed with CRA
    assert len(res["caveats"]) == 3
    assert res["summary_totals"]["box_17"] == 0.0   # CPP2 zeroed
