# -*- coding: utf-8 -*-
"""
Backfill FY2025 payroll registers (Oct 2024 – Sep 2025) for Bridlewood.

Imports the 26 bi-weekly eNetEmployer register PDFs into payroll_runs +
payroll_run_lines WITHOUT building GL journals or approving runs.  The runs
stay at status=draft_confirmed so no 6120 debit is written to the GL.

After importing, the script populates wage_planner_periods (FY2025) with
actual figures drawn from the run lines and cash_balancing_days so the
dashboard summary's prior-year comparison works correctly.

USAGE (dry-run first — inspect what would be imported, no DB writes):
    python -m scripts._backfill_fy2025_registers --entity-code 1877-8

Then review output and run with --write to persist:
    python -m scripts._backfill_fy2025_registers --entity-code 1877-8 --write

REQUIRES:
    Run from backend/ directory (or with backend/ on sys.path).
    DATABASE_URL set in backend/.env.
    Paths in REGISTERS below populated with the 26 FY2025 register PDFs.

SAFETY:
    - DRY-RUN is the default; --write is an explicit opt-in.
    - payroll_runs upserts on (entity_id, pay_run_number) — safe to re-run.
    - wage_planner_periods upserts on (entity_id, fiscal_year, period_number).
    - Does NOT touch journal_batches, journal_lines, or any GL data.
    - Does NOT call approve_payroll_run or build_payroll_journal.

PRECONDITIONS:
    1. Payroll employees must exist in payroll_employees for Bridlewood.
    2. Spencer Roberts must have is_management=TRUE (migration 068).
    3. FY2025 wage_planner_settings must exist (target_wage_pct, etc.).
       If not set, run settings first in the UI, then re-run this script.
"""
import argparse
import os
import sys
from decimal import Decimal, ROUND_HALF_UP

sys.stdout.reconfigure(encoding="utf-8")
_here = os.path.dirname(os.path.abspath(__file__))
_backend = os.path.join(_here, "..")
if _backend not in sys.path:
    sys.path.insert(0, _backend)

from dotenv import load_dotenv
load_dotenv(os.path.join(_backend, ".env"))

from sqlalchemy import create_engine, text

from app.services_payroll import (
    parse_payroll_register_pdf,
    build_payroll_run_from_register,
)
from app.services_wage_planner import (
    backfill_calendar_from_runs,
    get_pay_period_calendar,
    get_settings,
    _compute_salaried_totals,
    _fiscal_year_for_date,
)

# ---------------------------------------------------------------------------
# CONFIGURE: populate with the 26 FY2025 register PDFs (Oct 2024 – Sep 2025).
# Adjust the BASE path and filenames to match where you've extracted the ZIPs.
# ---------------------------------------------------------------------------
BASE = r"C:\Users\spenc\Downloads\Payroll registers\FY2025"

# Each tuple: (path_relative_to_BASE, descriptive_label_for_dry_run)
# Add all 26 bi-weekly runs here in chronological order.
REGISTERS: list[tuple[str, str]] = [
    # ---- Oct 2024 (FY2025 P01-P02) ----
    # ("Oct 2024\\Payroll Register Oct 5 - Oct 18.pdf",  "2024-P01 Oct 5–18"),
    # ("Oct 2024\\Payroll Register Oct 19 - Nov 1.pdf",  "2024-P02 Oct 19–Nov 1"),
    # ---- Nov 2024 (FY2025 P03-P04) ----
    # ... add all 26 entries ...
    # ---- EXAMPLE (remove this and populate with real paths) ----
    # ("Example\\register.pdf", "EXAMPLE — remove me"),
]

EC = "1877-8"
ACTOR = "spencer7roberts@gmail.com"
FY_2025 = 2025
FY_END_MONTH = 9   # September
FY_END_DAY   = 30


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_entity(conn, entity_code: str) -> dict:
    row = conn.execute(
        text(
            "SELECT id, entity_code FROM entities WHERE entity_code = :ec"
        ),
        {"ec": entity_code},
    ).mappings().first()
    if not row:
        raise SystemExit(f"ERROR: Entity not found: {entity_code}")
    return dict(row)


def _assert_spencer_is_management(conn, entity_id) -> None:
    """Fail-fast if Spencer (is_management=TRUE) is not seeded."""
    row = conn.execute(
        text(
            """
            SELECT COUNT(*) AS cnt
            FROM payroll_employees
            WHERE entity_id = :eid AND is_management = TRUE
            """
        ),
        {"eid": entity_id},
    ).mappings().first()
    if (row["cnt"] or 0) == 0:
        raise SystemExit(
            "ERROR: No employee with is_management=TRUE found for this entity.\n"
            "Run migration 068 or seed Spencer Roberts before importing."
        )
    print(f"  Precondition OK: {row['cnt']} salaried/management employee(s) flagged.")


def _dry_run_parse(path: str, label: str) -> dict | None:
    """Parse a register PDF and print results; return parsed dict or None on error."""
    if not os.path.exists(path):
        print(f"  [SKIP] File not found: {path}")
        return None
    with open(path, "rb") as fh:
        data = fh.read()
    try:
        parsed = parse_payroll_register_pdf(data)
    except Exception as exc:
        print(f"  [ERROR] Parse failed for {label}: {exc}")
        return None
    return parsed


def _print_parsed(label: str, parsed: dict) -> None:
    print(f"\n  {'─'*58}")
    print(f"  {label}")
    print(f"  {'─'*58}")
    print(f"    Run #   : {parsed.get('pay_run_number')}")
    print(f"    Period  : P{parsed.get('period_number')} "
          f"  {parsed.get('period_start')} → {parsed.get('period_end')}")
    print(f"    Pay date: {parsed.get('pay_date')}")
    print(f"    Gross   : ${parsed.get('summary_total_gross')}")
    print(f"    Net     : ${parsed.get('summary_total_net_pay')}")
    employees = parsed.get("employees") or []
    print(f"    Employees ({len(employees)}):")
    for emp in employees:
        print(f"      #{emp.get('employee_number')} {emp.get('full_name'):<25}  "
              f"gross=${emp.get('gross_pay')}  hrs={emp.get('total_hours')}")
    if parsed.get("warnings"):
        print(f"    WARNINGS: {parsed['warnings']}")


def _sum_runline_wages(conn, entity_id, period_start, period_end) -> Decimal:
    """Non-management gross_pay sum for a pay window (no status gate)."""
    row = conn.execute(
        text(
            """
            SELECT COALESCE(SUM(prl.gross_pay), 0) AS total
            FROM payroll_run_lines prl
            JOIN payroll_runs pr           ON pr.id = prl.payroll_run_id
            LEFT JOIN payroll_employees pe ON pe.id = prl.employee_id
            WHERE pr.entity_id = :eid
              AND pr.period_end BETWEEN :start AND :end
              AND COALESCE(pe.is_management, FALSE) = FALSE
            """
        ),
        {"eid": entity_id, "start": period_start, "end": period_end},
    ).mappings().first()
    return Decimal(str(row["total"]))


def _sum_runline_stat_pay(conn, entity_id, period_start, period_end) -> Decimal:
    row = conn.execute(
        text(
            """
            SELECT COALESCE(SUM(prl.stat_pay), 0) AS total
            FROM payroll_run_lines prl
            JOIN payroll_runs pr ON pr.id = prl.payroll_run_id
            WHERE pr.entity_id = :eid
              AND pr.period_end BETWEEN :start AND :end
            """
        ),
        {"eid": entity_id, "start": period_start, "end": period_end},
    ).mappings().first()
    return Decimal(str(row["total"]))


def _sum_runline_hours(conn, entity_id, period_start, period_end) -> Decimal:
    """Hourly (non-salaried) total_hours for the period."""
    row = conn.execute(
        text(
            """
            SELECT COALESCE(SUM(
                CASE WHEN prl.employment_type <> 'salaried' THEN prl.total_hours ELSE 0 END
            ), 0) AS total
            FROM payroll_run_lines prl
            JOIN payroll_runs pr ON pr.id = prl.payroll_run_id
            WHERE pr.entity_id = :eid
              AND pr.period_end BETWEEN :start AND :end
            """
        ),
        {"eid": entity_id, "start": period_start, "end": period_end},
    ).mappings().first()
    return Decimal(str(row["total"]))


def _sum_sales(conn, entity_id, period_start, period_end) -> Decimal:
    row = conn.execute(
        text(
            """
            SELECT COALESCE(SUM(total_sales), 0) AS total
            FROM cash_balancing_days
            WHERE entity_id = :eid
              AND business_date BETWEEN :start AND :end
            """
        ),
        {"eid": entity_id, "start": period_start, "end": period_end},
    ).mappings().first()
    return Decimal(str(row["total"]))


def _upsert_wage_planner_period(
    conn,
    entity_id,
    fiscal_year: int,
    period_number: int,
    actual_gross_wages: Decimal,
    actual_stat_pay: Decimal,
    actual_hours: Decimal,
    actual_sales: Decimal,
    hours_over_under,
    actual_sph,
) -> None:
    conn.execute(
        text(
            """
            INSERT INTO wage_planner_periods
                (entity_id, fiscal_year, period_number,
                 actual_gross_wages, actual_stat_pay, actual_hours,
                 actual_sales, hours_over_under, actual_sales_per_hour,
                 locked, computed_at)
            VALUES
                (:eid, :fy, :pn,
                 :wages, :stat, :hrs,
                 :sales, :hou, :sph,
                 TRUE, NOW())
            ON CONFLICT (entity_id, fiscal_year, period_number)
            DO UPDATE SET
                actual_gross_wages   = EXCLUDED.actual_gross_wages,
                actual_stat_pay      = EXCLUDED.actual_stat_pay,
                actual_hours         = EXCLUDED.actual_hours,
                actual_sales         = EXCLUDED.actual_sales,
                hours_over_under     = EXCLUDED.hours_over_under,
                actual_sales_per_hour = EXCLUDED.actual_sales_per_hour,
                locked               = TRUE,
                computed_at          = NOW()
            """
        ),
        {
            "eid": entity_id,
            "fy": fiscal_year,
            "pn": period_number,
            "wages": actual_gross_wages,
            "stat": actual_stat_pay,
            "hrs": actual_hours,
            "sales": actual_sales,
            "hou": hours_over_under,
            "sph": actual_sph,
        },
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(entity_code: str, write: bool) -> None:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set.")
        sys.exit(1)

    if not REGISTERS:
        print(
            "\nERROR: REGISTERS list is empty.\n"
            "Populate the REGISTERS constant at the top of this script\n"
            "with the 26 FY2025 eNetEmployer register PDF paths.\n"
        )
        sys.exit(1)

    mode = "WRITE" if write else "DRY-RUN"
    print(f"\n{'='*70}")
    print(f"  FY2025 payroll register backfill  entity={entity_code!r}  mode={mode}")
    print(f"{'='*70}")

    engine = create_engine(db_url)

    with engine.connect() as conn:
        entity = _get_entity(conn, entity_code)
        entity_id = entity["id"]
        print(f"\nEntity: {entity_code}  id={entity_id}")

        # Precondition: Spencer must have is_management=TRUE
        _assert_spencer_is_management(conn, entity_id)

        # Precondition: FY2025 settings should exist (needed for hours_over_under calc)
        settings = get_settings(conn, entity_id=entity_id, fiscal_year=FY_2025)
        has_settings = settings is not None
        if not has_settings:
            print(
                "\nWARNING: No wage_planner_settings found for FY2025.\n"
                "  hours_over_under cannot be computed.\n"
                "  To fix: create FY2025 settings in the UI, then re-run with --write."
            )

        # ---- DRY-RUN: parse PDFs and print what would happen ----------------
        print(f"\n{'─'*70}")
        print("  PARSING REGISTERS (no DB writes in dry-run mode)")
        print(f"{'─'*70}")

        parsed_results: list[tuple[str, str, dict | None]] = []
        errors = 0
        for rel_path, label in REGISTERS:
            full_path = os.path.join(BASE, rel_path)
            parsed = _dry_run_parse(full_path, label)
            parsed_results.append((rel_path, label, parsed))
            if parsed:
                _print_parsed(label, parsed)
            else:
                errors += 1

        print(f"\n{'─'*70}")
        print(f"  SUMMARY: {len(REGISTERS)} registers, {errors} parse errors")
        print(f"{'─'*70}")
        print(f"\n  {'Run #':<12}  {'Period':<24}  {'Gross':>10}  {'Net':>10}  "
              f"{'Employees':>10}")
        print(f"  {'─'*12}  {'─'*24}  {'─'*10}  {'─'*10}  {'─'*10}")
        for _rp, label, parsed in parsed_results:
            if parsed:
                pn = parsed.get("period_number", "?")
                ps = parsed.get("period_start", "?")
                pe = parsed.get("period_end", "?")
                run_num = parsed.get("pay_run_number", "?")
                gross = parsed.get("summary_total_gross", "?")
                net = parsed.get("summary_total_net_pay", "?")
                n_emp = len(parsed.get("employees") or [])
                period_str = f"P{pn} {ps}→{pe}"
                print(f"  {str(run_num):<12}  {period_str:<24}  "
                      f"{str(gross):>10}  {str(net):>10}  {n_emp:>10}")
            else:
                print(f"  [PARSE ERROR] {label}")

        if not write:
            print(
                f"\n  DRY-RUN complete.\n"
                f"  Review the output above, then re-run with --write to persist.\n"
                f"  Command: python -m scripts._backfill_fy2025_registers "
                f"--entity-code {entity_code} --write\n"
            )
            return

        # ---- WRITE: import registers -----------------------------------------
        if errors > 0:
            print(
                f"\nABORTING: {errors} parse error(s) — fix missing files before writing."
            )
            sys.exit(1)

        print(f"\n{'='*70}")
        print("  WRITING TO DATABASE")
        print(f"{'='*70}")

        import_results = []
        for _rp, label, parsed in parsed_results:
            if parsed is None:
                continue
            full_path = os.path.join(BASE, _rp)
            fname = os.path.basename(full_path)
            with open(full_path, "rb") as fh:
                file_bytes = fh.read()

            try:
                result = build_payroll_run_from_register(
                    conn,
                    entity_code=entity_code,
                    file_bytes=file_bytes,
                    file_name=fname,
                    actor_email=ACTOR,
                )
                run_id = result["payroll_run_id"]
                print(
                    f"  IMPORTED  {label}  run_id={run_id}"
                    f"  run#={result.get('pay_run_number')}"
                    f"  period=P{result.get('period_number')}"
                )
                if result.get("warnings"):
                    print(f"            WARNINGS: {result['warnings']}")
                import_results.append((label, result, run_id))
            except Exception as exc:
                print(f"  ERROR importing {label}: {exc}")
                import traceback
                traceback.print_exc()
                conn.rollback()
                print("  ABORTED (rolled back). Fix the error and re-run.")
                sys.exit(1)

        # Commit all imports before populating wage_planner_periods
        conn.commit()
        print(f"\n  Committed {len(import_results)} payroll_run imports.")

        # ---- Backfill pay-period calendar (payroll_pay_periods) ---------------
        n_cal = backfill_calendar_from_runs(
            conn,
            entity_id=entity_id,
            fy_end_month=FY_END_MONTH,
            fy_end_day=FY_END_DAY,
        )
        conn.commit()
        print(f"  Backfilled {n_cal} payroll_pay_periods rows.")

        # ---- Populate wage_planner_periods (FY2025) with actuals -------------
        if not has_settings:
            print(
                "\n  SKIPPING wage_planner_periods population — no FY2025 settings.\n"
                "  Create settings in the UI and re-run with --write to populate actuals."
            )
        else:
            # Reload settings now that they're confirmed present
            settings = get_settings(conn, entity_id=entity_id, fiscal_year=FY_2025)
            twp = Decimal(str(settings["target_wage_pct"]))
            ahw = Decimal(str(settings["avg_hourly_wage"]))
            bp  = Decimal(str(settings["benefits_pct"]))
            B   = Decimal("1") + bp
            avg_wage_wb = ahw * B
            salaried_pp, _ = _compute_salaried_totals(settings)

            cal = get_pay_period_calendar(
                conn, entity_id=entity_id, fiscal_year=FY_2025
            )
            planner_updated = 0

            print(f"\n  Populating wage_planner_periods (FY{FY_2025}):")
            print(
                f"  {'P#':<5}  {'Period':<22}  {'GrossWages':>12}  "
                f"{'Sales':>12}  {'Hours':>8}  {'O/U':>8}"
            )
            print(f"  {'─'*5}  {'─'*22}  {'─'*12}  {'─'*12}  {'─'*8}  {'─'*8}")

            for c in cal:
                pn = c["period_number"]
                ps = c["period_start"]
                pe = c["period_end"]
                fy = _fiscal_year_for_date(ps, FY_END_MONTH, FY_END_DAY)
                if fy != FY_2025:
                    continue  # safety — only FY2025 rows

                wages = _sum_runline_wages(conn, entity_id, ps, pe)
                stat  = _sum_runline_stat_pay(conn, entity_id, ps, pe)
                hrs   = _sum_runline_hours(conn, entity_id, ps, pe)
                sales = _sum_sales(conn, entity_id, ps, pe)

                # hours_over_under: rebase on actual sales (same formula as
                # refresh_period_actuals in services_wage_planner.py)
                if sales > 0 and avg_wage_wb > 0:
                    target_hrs_on_actual = (sales * twp - salaried_pp) / avg_wage_wb
                    hours_ou = (hrs - target_hrs_on_actual).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP
                    )
                else:
                    hours_ou = None

                actual_sph = (
                    (sales / hrs).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    if hrs > 0 else None
                )

                _upsert_wage_planner_period(
                    conn, entity_id, FY_2025, pn,
                    wages, stat, hrs, sales, hours_ou, actual_sph,
                )
                planner_updated += 1

                ou_str = f"{hours_ou:+.1f}" if hours_ou is not None else "N/A"
                print(
                    f"  P{pn:<4}  {str(ps):<10} → {str(pe):<10}  "
                    f"{wages:>12.2f}  {sales:>12.2f}  {hrs:>8.1f}  {ou_str:>8}"
                )

            conn.commit()
            print(f"\n  Updated {planner_updated} wage_planner_periods rows for FY{FY_2025}.")

        print(f"\n{'='*70}")
        print(f"  DONE — FY{FY_2025} backfill complete.")
        print(
            f"  Prior-year comparison in the dashboard summary will now use\n"
            f"  runline_gross basis (non-management gross_pay, no employer burden).\n"
            f"  The UI labels this as 'Gross wages, non-management' to distinguish\n"
            f"  it from the current-FY GL 6120 basis.\n"
        )
        print(f"{'='*70}\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill FY2025 payroll registers into payroll_runs + wage_planner_periods. "
            "DRY-RUN (default) — inspect output first, then add --write to persist."
        )
    )
    parser.add_argument("--entity-code", required=True, help="Entity code (e.g. 1877-8)")
    parser.add_argument(
        "--write",
        action="store_true",
        help="Persist to DB. Without this flag the script is a dry-run (parse only).",
    )
    args = parser.parse_args()
    run(entity_code=args.entity_code, write=args.write)


if __name__ == "__main__":
    main()
