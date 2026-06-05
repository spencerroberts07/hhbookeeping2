"""
Seed the versioned CRA rate table (Phase 6A / CP6a).

Effective 2025-01-01 = the values currently hard-coded in services_payroll_calc.py
(the engine's present truth). Effective 2026-01-01 = the official CRA 2026 values
verified against canada.ca in June 2026 (CPP/CPP2/EI maximums, federal + Ontario
brackets and basic personal amounts; note the federal bottom rate dropped 15% -> 14%).

This seeds the REFERENCE table only. It does NOT change the live calc engine —
wiring services_payroll_calc.py to read by pay_date is a separate, T4127-verified
step. Re-runnable (ON CONFLICT upsert).

Run: backend/.venv/Scripts/python.exe -m scripts.seed_cra_rate_versions
"""
from __future__ import annotations

from datetime import date

from sqlalchemy import text

from app.db import db_session

# (rate_key, value_2025_stored, value_2026_cra, note)
RATES: list[tuple[str, float, float, str]] = [
    # --- CPP ---
    ("CPP_RATE_EE", 0.0595, 0.0595, "CPP employee contribution rate"),
    ("CPP_RATE_ER", 0.0595, 0.0595, "CPP employer contribution rate"),
    ("CPP_EXEMPTION_ANNUAL", 3500.00, 3500.00, "CPP basic exemption"),
    ("CPP_MAX_EARNINGS_ANNUAL", 71300.00, 74600.00, "YMPE — year's max pensionable earnings"),
    ("CPP_MAX_CONTRIB_ANNUAL", 4034.10, 4230.45, "Max base CPP contribution (employee)"),
    # --- CPP2 ---
    ("CPP2_RATE_EE", 0.04, 0.04, "CPP2 employee rate"),
    ("CPP2_RATE_ER", 0.04, 0.04, "CPP2 employer rate"),
    ("CPP2_LOWER_CEILING", 71300.00, 74600.00, "CPP2 lower ceiling (= YMPE)"),
    ("CPP2_UPPER_CEILING", 73200.00, 85000.00, "YAMPE — CPP2 upper ceiling (2025 stored 73,200 is a known placeholder; correct CRA 2025 was 81,200)"),
    ("CPP2_MAX_CONTRIB_ANNUAL", 396.00, 416.00, "Max CPP2 contribution (employee)"),
    # --- EI ---
    ("EI_RATE_EE", 0.01657, 0.0163, "EI employee premium rate"),
    ("EI_RATE_ER_MULTIPLIER", 1.4, 1.4, "EI employer multiplier"),
    ("EI_MAX_INSURABLE_ANNUAL", 65700.00, 68900.00, "MIE — max insurable earnings"),
    ("EI_MAX_CONTRIB_EE_ANNUAL", 1088.65, 1123.07, "Max EI premium (employee)"),
    # --- Federal tax (bottom rate 15% -> 14% for 2026) ---
    ("FEDERAL_BPA", 16129.00, 16452.00, "Federal basic personal amount (max)"),
    ("FEDERAL_BRKT1_UPPER", 57375.00, 58523.00, "Federal bracket 1 upper"),
    ("FEDERAL_BRKT2_UPPER", 114750.00, 117045.00, "Federal bracket 2 upper"),
    ("FEDERAL_BRKT3_UPPER", 158519.00, 181440.00, "Federal bracket 3 upper"),
    ("FEDERAL_BRKT4_UPPER", 220000.00, 258482.00, "Federal bracket 4 upper"),
    ("FEDERAL_BRKT1_RATE", 0.15, 0.14, "Federal bracket 1 rate (LOWERED 15% -> 14%)"),
    ("FEDERAL_BRKT2_RATE", 0.205, 0.205, "Federal bracket 2 rate"),
    ("FEDERAL_BRKT3_RATE", 0.26, 0.26, "Federal bracket 3 rate"),
    ("FEDERAL_BRKT4_RATE", 0.29, 0.29, "Federal bracket 4 rate"),
    ("FEDERAL_BRKT5_RATE", 0.33, 0.33, "Federal bracket 5 rate (top)"),
    # --- Ontario tax ---
    ("ON_BPA", 11865.00, 12989.00, "Ontario basic personal amount"),
    ("ON_BRKT1_UPPER", 51446.00, 53891.00, "Ontario bracket 1 upper"),
    ("ON_BRKT2_UPPER", 102894.00, 107785.00, "Ontario bracket 2 upper"),
    ("ON_BRKT3_UPPER", 150000.00, 150000.00, "Ontario bracket 3 upper (not indexed)"),
    ("ON_BRKT4_UPPER", 220000.00, 220000.00, "Ontario bracket 4 upper (not indexed)"),
    ("ON_BRKT1_RATE", 0.0505, 0.0505, "Ontario bracket 1 rate"),
    ("ON_BRKT2_RATE", 0.0915, 0.0915, "Ontario bracket 2 rate"),
    ("ON_BRKT3_RATE", 0.1116, 0.1116, "Ontario bracket 3 rate"),
    ("ON_BRKT4_RATE", 0.1216, 0.1216, "Ontario bracket 4 rate"),
    ("ON_BRKT5_RATE", 0.1316, 0.1316, "Ontario bracket 5 rate (top)"),
]

_SRC_2025 = "Engine constants (services_payroll_calc.py), pre-audit"
_SRC_2026 = "CRA 2026 — canada.ca, verified June 2026 (T4127 122nd ed.)"


def _upsert(session, key: str, value: float, eff: date, note: str) -> None:
    session.execute(
        text(
            """
            INSERT INTO cra_rate_versions (rate_key, rate_value, effective_date, source_note)
            VALUES (:k, :v, :d, :n)
            ON CONFLICT (rate_key, effective_date) DO UPDATE
              SET rate_value = EXCLUDED.rate_value, source_note = EXCLUDED.source_note
            """
        ),
        {"k": key, "v": value, "d": eff, "n": note},
    )


def main() -> None:
    d2025 = date(2025, 1, 1)
    d2026 = date(2026, 1, 1)
    changed = 0
    with db_session() as session:
        for key, v25, v26, note in RATES:
            _upsert(session, key, v25, d2025, f"{note} — {_SRC_2025}")
            _upsert(session, key, v26, d2026, f"{note} — {_SRC_2026}")
            if abs(v25 - v26) > 1e-9:
                changed += 1
    print(f"seeded {len(RATES)} rate keys x 2 years; {changed} values changed 2025->2026")
    # comparison table
    print(f"\n{'RATE KEY':<28}{'2025 (stored)':>16}{'2026 (CRA)':>16}  CHANGED")
    print("-" * 76)
    for key, v25, v26, _ in RATES:
        flag = "  <== CHANGED" if abs(v25 - v26) > 1e-9 else ""
        print(f"{key:<28}{v25:>16,.5g}{v26:>16,.5g}{flag}")


if __name__ == "__main__":
    main()
