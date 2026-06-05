"""
CP6b — CPA-005 EFT end-to-end dry-run (Phase 6C).

Builds (in memory, no DB writes, NOT submitted to any bank) a CPA-005 file for
Bridlewood's most recent approved payroll run and prints a human-readable
breakdown: A header (with the originator ID from entity_settings), one C record
per employee (name, routing, amount), and the Z trailer (item count, control
totals). Verifies the EFT total ties to the run's net pay and to the payroll
journal's Cr 1020 (net pay).

Run: backend/.venv/Scripts/python.exe -m scripts.cp6b_eft_dryrun
"""
from __future__ import annotations

from datetime import date as DateType
from decimal import Decimal

from sqlalchemy import text

from app import services_payroll_eft as eft
from app.db import db_session
from app.routes.payroll import _resolve_eft_settings


def main() -> None:
    with db_session() as s:
        ent = s.execute(
            text("SELECT id, entity_code, entity_name FROM entities WHERE entity_code='1877-8'")
        ).mappings().first()
        eid = ent["id"]

        run = s.execute(
            text(
                """
                SELECT id, pay_run_number, pay_date, period_start, period_end,
                       status, workflow_status, total_net_pay, cra_remittance_amount,
                       accounting_period_id
                  FROM payroll_runs
                 WHERE entity_id = :e
                   AND COALESCE(workflow_status, status) IN ('approved','approved_to_post','posted')
              ORDER BY pay_date DESC NULLS LAST
                 LIMIT 1
                """
            ),
            {"e": eid},
        ).mappings().first()
        if not run:
            print("No approved payroll run found for Bridlewood.")
            return

        emps = s.execute(
            text(
                """
                SELECT pe.full_name, pe.employee_number, pe.bank_transit, pe.bank_institution,
                       pe.bank_account, prl.net_pay
                  FROM payroll_run_lines prl
                  JOIN payroll_employees pe ON pe.id = prl.employee_id
                 WHERE prl.payroll_run_id = :rid AND prl.net_pay > 0
              ORDER BY pe.employee_number
                """
            ),
            {"rid": run["id"]},
        ).mappings().all()

        cfg = _resolve_eft_settings(s, eid)

        # payroll journal Cr 1020 (net pay) for this run, to tie out
        je_net = s.execute(
            text(
                """
                SELECT COALESCE(SUM(jl.credit_amount - jl.debit_amount), 0)
                  FROM journal_lines jl
                  JOIN journal_batches jb ON jb.id = jl.journal_batch_id
                 WHERE jb.id = (SELECT journal_batch_id FROM payroll_runs WHERE id = :rid)
                   AND jl.account_code = '1020'
                """
            ),
            {"rid": run["id"]},
        ).scalar()

    missing = [e["full_name"] for e in emps
               if not (e["bank_transit"] and e["bank_institution"] and e["bank_account"])]

    header = eft.EFTHeader(
        originator_id=cfg["originator_id"], file_creation_number=9999,  # dry-run sentinel
        creation_date=DateType.today(), originator_short_name=cfg["short_name"],
        originator_long_name=cfg["long_name"], return_institution=cfg["return_institution"],
        return_transit=cfg["return_transit"], return_account=cfg["return_account"])
    employees = [
        eft.EFTEmployee(name=e["full_name"], transit=e["bank_transit"],
                        institution=e["bank_institution"], account=e["bank_account"],
                        amount=Decimal(str(e["net_pay"]))) for e in emps
        if e["bank_transit"] and e["bank_institution"] and e["bank_account"]]

    print("=" * 74)
    print("CP6b — CPA-005 EFT DRY-RUN (in memory; NOT submitted to any bank)")
    print("=" * 74)
    print(f"Entity        : {ent['entity_name']} ({ent['entity_code']})")
    print(f"Payroll run   : {run['pay_run_number']}  pay_date={run['pay_date']}  "
          f"status={run['workflow_status'] or run['status']}")
    if missing:
        print(f"\n!! {len(missing)} employee(s) missing bank info (excluded): {', '.join(missing)}")

    if not employees:
        print("\nNo employees with complete banking — cannot build EFT.")
        return

    built = eft.build_eft_file(header=header, employees=employees,
                               payment_date=run["pay_date"],
                               cross_reference=f"PAYROLL-{run['pay_run_number']}")

    print("\n-- A RECORD (header) --")
    print(f"  Originator ID    : {cfg['originator_id']}")
    print(f"  Originator (short/long): {cfg['short_name']} / {cfg['long_name']}")
    print(f"  File creation #  : 9999 (dry-run sentinel)")
    print(f"  Creation date    : {DateType.today()}")
    print(f"  Return routing   : inst {cfg['return_institution']} transit "
          f"{cfg['return_transit']} acct {cfg['return_account']}")

    print(f"\n-- C RECORDS (credits, one per employee) — {len(employees)} records --")
    total = Decimal("0")
    for e in employees:
        total += e.amount
        print(f"  {e.name:<24} {e.institution}-{e.transit}-{e.account:<12} "
              f"$ {e.amount:>10,.2f}")

    print("\n-- Z RECORD (trailer) --")
    print(f"  Record count     : {built.record_count}")
    print(f"  Credit count     : {built.credit_count}")
    print(f"  Total value      : $ {built.total_amount:,.2f}")

    print("\n-- TIE-OUT --")
    run_net = Decimal(str(run["total_net_pay"] or 0))
    je_net_d = Decimal(str(je_net or 0))
    print(f"  EFT total            : $ {built.total_amount:,.2f}")
    print(f"  Run total_net_pay    : $ {run_net:,.2f}  "
          f"({'TIES' if abs(built.total_amount - run_net) <= Decimal('0.01') else 'MISMATCH'})")
    print(f"  Payroll JE Cr 1020   : $ {je_net_d:,.2f}  "
          f"({'TIES' if abs(built.total_amount - je_net_d) <= Decimal('0.01') else 'see note'})")
    print(f"\n  CRA remittance (would be DRAFT Dr 2320 / Cr 1020): "
          f"$ {Decimal(str(run['cra_remittance_amount'] or 0)):,.2f}  [draft only, never auto-posted]")
    print("\nDRY-RUN GATE: file generated for manual review. NEVER auto-submitted to the bank.")


if __name__ == "__main__":
    main()
