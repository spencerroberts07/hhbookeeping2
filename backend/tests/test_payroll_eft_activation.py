"""CPA-005 activation + CRA remittance tests (Phase 6B)."""
from decimal import Decimal

import pytest

from app.routes.payroll import _create_cra_remittance_draft


def test_remittance_draft_guards_no_db():
    # amount <= 0 short-circuits before any DB access (session unused)
    r = _create_cra_remittance_draft(
        None, entity_id="e", run={"id": "1", "pay_run_number": "P1", "cra_remittance_amount": 0},
        actor_email=None)
    assert r["created"] is False
    # amount present but no accounting period -> still no DB access
    r = _create_cra_remittance_draft(
        None, entity_id="e",
        run={"id": "1", "pay_run_number": "P1", "cra_remittance_amount": Decimal("100"),
             "accounting_period_id": None},
        actor_email=None)
    assert r["created"] is False
    assert "accounting_period_id" in r["reason"]


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
def test_eft_settings_resolved_from_entity_settings():
    from sqlalchemy import text
    from app.db import db_session
    from app.routes.payroll import _resolve_eft_settings
    with db_session() as s:
        eid = s.execute(text("SELECT id FROM entities WHERE entity_code='1877-8'")).scalar()
        cfg = _resolve_eft_settings(s, eid)
    # seeded in migration 051 — values come from entity_settings row, not constants
    assert cfg["originator_id"] == "TPBHC10203"
    assert cfg["short_name"] == "BRIDLEWOOD HH"
    assert cfg["return_account"] == "06905660371"


@pytest.mark.skipif(not _has_db(), reason="no live DB connection")
def test_eft_settings_missing_raises_400():
    """Entity with no entity_settings row must raise HTTP 400 (not silently use Bridlewood's originator)."""
    from fastapi import HTTPException
    from sqlalchemy import text
    from app.db import db_session
    from app.routes.payroll import _resolve_eft_settings
    with db_session() as s:
        # Use a synthetic UUID that will never match an entity_settings row
        fake_eid = "00000000-0000-0000-0000-000000000001"
        with pytest.raises(HTTPException) as exc_info:
            _resolve_eft_settings(s, fake_eid)
    assert exc_info.value.status_code == 400
    assert "EFT originator not configured" in exc_info.value.detail
