"""Versioned CRA rate tests (Phase 6A)."""
import pytest

from scripts.seed_cra_rate_versions import RATES


def test_known_2026_changes_present():
    by_key = {k: (v25, v26) for (k, v25, v26, _n) in RATES}
    # the headline 2026 changes verified against canada.ca
    assert by_key["CPP_MAX_EARNINGS_ANNUAL"] == (71300.00, 74600.00)
    assert by_key["EI_MAX_INSURABLE_ANNUAL"] == (65700.00, 68900.00)
    assert by_key["FEDERAL_BRKT1_RATE"] == (0.15, 0.14)        # bottom rate cut
    assert by_key["FEDERAL_BPA"][1] == 16452.00
    assert by_key["ON_BPA"][1] == 12989.00
    assert by_key["CPP2_UPPER_CEILING"][1] == 85000.00


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
def test_versioned_lookup_by_effective_date():
    from datetime import date
    from app.db import db_session
    from sqlalchemy import text

    def rate_asof(key: str, d: date):
        with db_session() as s:
            return s.execute(
                text("""SELECT rate_value FROM cra_rate_versions
                         WHERE rate_key=:k AND effective_date <= :d
                      ORDER BY effective_date DESC LIMIT 1"""),
                {"k": key, "d": d},
            ).scalar()

    # historical reproducibility: a mid-2025 pay date still sees the 2025 YMPE
    assert float(rate_asof("CPP_MAX_EARNINGS_ANNUAL", date(2025, 6, 1))) == 71300.00
    # a 2026 pay date sees the 2026 YMPE
    assert float(rate_asof("CPP_MAX_EARNINGS_ANNUAL", date(2026, 6, 1))) == 74600.00
