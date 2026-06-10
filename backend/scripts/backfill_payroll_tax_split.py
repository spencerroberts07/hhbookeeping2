"""
Backfill payroll_run_lines.federal_tax / provincial_tax for existing rows.

Added by migration 064. FY2026 Bridlewood rows have federal_tax=0 and
provincial_tax=0 (DEFAULT 0) until this script is run. The invariant is:
    federal_tax + provincial_tax == fed_tax  (per row)

Derivation method (register path — authoritative source):
    provincial_tax = approximate Ontario provincial tax from taxable_gross
                     via ON_BRACKETS / ON_BPA_2026 + employee's
                     provincial_td1_claim_code.
    federal_tax    = max(0, fed_tax - provincial_tax)

This is an approximation because the ENetEmployer register only carries a
single combined FED TAX figure. The split is informational only; it does
not change Box 22 on the T4, nor any posted GL journal entry.

USAGE (dry-run first — inspect output):
    python -m scripts.backfill_payroll_tax_split --entity-code 1877-8

Then request user approval, then run with --write to persist:
    python -m scripts.backfill_payroll_tax_split --entity-code 1877-8 --write

REQUIRES:
    This script must be run from the backend/ directory (or with backend/ on sys.path)
    so that the app package is importable.
    DATABASE_URL must be set in .env or environment.

SAFETY:
    Only touches payroll_run_lines.federal_tax and payroll_run_lines.provincial_tax
    (the new columns from migration 064). No other columns are modified.
    Does NOT touch payroll_runs, journal_batches, journal_lines, or any GL data.
    Re-running is safe and idempotent — only rows where federal_tax+provincial_tax==0
    and fed_tax>0 are processed.
"""
import argparse
import os
import sys
from decimal import Decimal, ROUND_HALF_UP

# Make the app package importable when run directly
_here = os.path.dirname(os.path.abspath(__file__))
_backend = os.path.join(_here, "..")
if _backend not in sys.path:
    sys.path.insert(0, _backend)

from dotenv import load_dotenv
load_dotenv(os.path.join(_backend, ".env"))

from sqlalchemy import create_engine, text
from app.services_payroll_calc import (
    ON_BPA_2026,
    ON_BRACKETS,
    BIWEEKLY_PERIODS,
    _apply_brackets,
    _claim_amount,
)


def _derive_prov_tax(taxable_gross: Decimal, pay_periods: int, provincial_td1_claim_code: int = 1) -> Decimal:
    annual = (taxable_gross * Decimal(pay_periods)).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    prov_gross = _apply_brackets(annual, ON_BRACKETS)
    prov_credit = _claim_amount(ON_BPA_2026, provincial_td1_claim_code) * Decimal("0.0505")
    prov_annual = max(Decimal("0.00"), prov_gross - prov_credit)
    return (prov_annual / Decimal(pay_periods)).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )


def run(entity_code: str, write: bool) -> None:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set.")
        sys.exit(1)

    engine = create_engine(db_url)
    mode = "WRITE" if write else "DRY-RUN"
    print(f"\n=== backfill_payroll_tax_split  entity={entity_code!r}  mode={mode} ===\n")

    with engine.connect() as conn:
        # Fetch all lines where the split is still zero but fed_tax > 0
        rows = conn.execute(
            text(
                """
                SELECT prl.id,
                       prl.fed_tax,
                       prl.taxable_gross,
                       pe.provincial_td1_claim_code,
                       pr.pay_run_number,
                       pe.full_name
                  FROM payroll_run_lines prl
                  JOIN payroll_runs pr ON pr.id = prl.payroll_run_id
                  JOIN payroll_employees pe ON pe.id = prl.employee_id
                  JOIN entities e ON e.id = pr.entity_id
                 WHERE e.entity_code = :ec
                   AND prl.fed_tax > 0
                   AND prl.federal_tax = 0
                   AND prl.provincial_tax = 0
                 ORDER BY pr.pay_run_number, pe.full_name
                """
            ),
            {"ec": entity_code},
        ).mappings().all()

        if not rows:
            print("Nothing to backfill — all rows already have a split, or no rows with fed_tax > 0.")
            return

        print(f"{'Run':>10}  {'Employee':<30}  {'fed_tax':>10}  {'federal':>10}  {'provincial':>10}  {'check':>8}")
        print("-" * 90)

        to_update = []
        ok = True
        for r in rows:
            fed_tax      = Decimal(str(r["fed_tax"]))
            taxable      = Decimal(str(r["taxable_gross"]))
            claim_code   = int(r["provincial_td1_claim_code"] or 1)
            prov_t       = _derive_prov_tax(taxable, BIWEEKLY_PERIODS, claim_code)
            fed_t        = max(Decimal("0.00"), fed_tax - prov_t)
            check        = fed_t + prov_t
            match        = "OK" if abs(check - fed_tax) < Decimal("0.02") else "MISMATCH"
            if match != "OK":
                ok = False
            print(f"{r['pay_run_number']:>10}  {r['full_name']:<30}  "
                  f"{fed_tax:>10.2f}  {fed_t:>10.2f}  {prov_t:>10.2f}  {match:>8}")
            to_update.append({"id": r["id"], "federal_tax": fed_t, "provincial_tax": prov_t})

        print(f"\nTotal rows to update: {len(to_update)}")
        if not ok:
            print("\nWARNING: Some rows have a split mismatch > $0.02. Review before writing.")

        if not write:
            print("\nDRY-RUN complete. Inspect output above. Re-run with --write to persist.")
            return

        # --- WRITE (requires explicit --write flag) ---
        print("\nWriting split values to payroll_run_lines ...")
        for row in to_update:
            conn.execute(
                text(
                    """
                    UPDATE payroll_run_lines
                       SET federal_tax    = :ft,
                           provincial_tax = :pt
                     WHERE id = :id
                       AND federal_tax  = 0
                       AND provincial_tax = 0
                    """
                ),
                {"id": row["id"], "ft": row["federal_tax"], "pt": row["provincial_tax"]},
            )
        conn.commit()
        print(f"Done. {len(to_update)} rows updated.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill payroll_run_lines federal/provincial tax split.")
    parser.add_argument("--entity-code", required=True, help="Entity code (e.g. 1877-8)")
    parser.add_argument("--write", action="store_true",
                        help="Persist the computed split. Without this flag the script is a dry-run.")
    args = parser.parse_args()
    run(entity_code=args.entity_code, write=args.write)


if __name__ == "__main__":
    main()
