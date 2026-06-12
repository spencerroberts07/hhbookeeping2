# -*- coding: utf-8 -*-
"""
Re-import test: rebuild journal for run 2026-72 (Mar 1-14) using the new
QBO-aligned structure (Dr 6110/6120/6160, Cr 2220/6130/1020, no 2320).

STOP after this run — do not re-import the remaining 6 until user confirms.
"""
import sys, os
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
from decimal import Decimal
from sqlalchemy import create_engine, text
from app.services_payroll import build_payroll_journal, approve_payroll_run

engine = create_engine(os.environ['DATABASE_URL'])
EID   = "0bab9284-68d9-4769-bfc6-4dac5bd1f5e4"
EC    = "1877-8"
ACTOR = "spencer7roberts@gmail.com"

# eNet fee for 2026-72 (Mar 1-14) from QBO GL Dr 6160
ENET_FEE_72 = Decimal("40.25")

with engine.connect() as session:
    # Step 1: Set enet_fee on the run row
    run_row = session.execute(text(
        "SELECT id, pay_run_number, period_start, period_end, "
        "total_gross, total_net_pay, cra_remittance_amount, "
        "total_cpp_er, total_ei_er, total_vacation_earned, "
        "total_fed_tax, total_cpp_ee, total_ei_ee, "
        "workflow_status "
        "FROM payroll_runs "
        "WHERE entity_id = :eid AND pay_run_number = '2026-72'"
    ), {"eid": EID}).mappings().first()

    if not run_row:
        print("ERROR: run 2026-72 not found"); sys.exit(1)

    run_id = str(run_row["id"])
    print("=" * 70)
    print(f"Run 2026-72  {run_row['period_start']} to {run_row['period_end']}")
    print(f"  DB gross={run_row['total_gross']}  net={run_row['total_net_pay']}  "
          f"cra={run_row['cra_remittance_amount']}  wf={run_row['workflow_status']}")
    print(f"  Setting enet_fee = {ENET_FEE_72}")
    print()

    # Verify benefit_offset = 0 for this run (user expectation)
    gross = Decimal(str(run_row["total_gross"]))
    net   = Decimal(str(run_row["total_net_pay"]))
    ft    = Decimal(str(run_row["total_fed_tax"]))
    cpp   = Decimal(str(run_row["total_cpp_ee"]))
    ei    = Decimal(str(run_row["total_ei_ee"]))
    benefit_offset_expected = max(Decimal("0"), gross - net - ft - cpp - ei)
    print(f"  Benefit offset check: gross({gross}) - net({net}) - fed_tax({ft}) "
          f"- cpp_ee({cpp}) - ei_ee({ei}) = {gross - net - ft - cpp - ei}")
    print(f"  => benefit_offset = {benefit_offset_expected}  "
          f"{'OK (expected $0)' if benefit_offset_expected == 0 else 'WARNING: non-zero!'}")
    print()

    session.execute(text(
        "UPDATE payroll_runs SET enet_fee = :fee WHERE id = :id AND entity_id = :eid"
    ), {"fee": ENET_FEE_72, "id": run_id, "eid": EID})

    # Step 2: Build journal (resets batch to draft with new structure)
    print("Building journal...")
    jresult = build_payroll_journal(
        session, entity_code=EC, payroll_run_id=run_id, actor_email=ACTOR
    )
    batch_id = jresult["journal_batch_id"]
    print(f"  journal_batch_id = {batch_id}")
    print(f"  total_debits     = {jresult['total_debits']}")
    print(f"  total_credits    = {jresult['total_credits']}")
    print()

    # Step 3: Show journal lines
    print(f"  {'Acct':<8} {'Dr':>12} {'Cr':>12}  Component / Memo")
    print("  " + "-" * 65)
    total_dr = Decimal("0")
    total_cr = Decimal("0")
    for l in jresult["lines"]:
        dr = Decimal(l["debit_amount"])
        cr = Decimal(l["credit_amount"])
        total_dr += dr
        total_cr += cr
        print(f"  {l['account_code']:<8} {float(dr):>12,.2f} {float(cr):>12,.2f}  "
              f"[{l['component']}] {l['memo']}")
    print("  " + "-" * 65)
    print(f"  {'TOTALS':<8} {float(total_dr):>12,.2f} {float(total_cr):>12,.2f}  "
          f"  {'BALANCED' if total_dr == total_cr else f'MISMATCH {total_dr - total_cr}'}")
    print()

    # Step 4: Plug reconciliation
    totals = jresult["summary"]["totals"]
    plug = Decimal(totals["bank_plug"])
    net_pay = Decimal(totals["net_pay"])
    cra = Decimal(totals["cra_remittance"])
    fee = Decimal(totals["enet_fee"])
    expected = net_pay + cra + fee
    print(f"  Plug reconciliation:")
    print(f"    plug (1020 Cr)       = {plug:,.2f}")
    print(f"    net_pay + CRA + fee  = {net_pay:,.2f} + {cra:,.2f} + {fee:,.2f} = {expected:,.2f}")
    diff = abs(plug - expected)
    print(f"    Difference           = {diff:,.4f}  {'OK' if diff <= Decimal('0.01') else 'WARNING'}")
    print()

    # Step 5: Confirm no 2320 lines
    accts_used = {l["account_code"] for l in jresult["lines"]}
    print(f"  Accounts used: {sorted(accts_used)}")
    if "2320" in accts_used:
        print("  *** ERROR: 2320 line present — must not appear ***")
    else:
        print("  No 2320 line — CORRECT")
    print()

    # Step 6: Approve
    print("Approving run...")
    aresult = approve_payroll_run(
        session, entity_code=EC, payroll_run_id=run_id, actor_email=ACTOR
    )
    print(f"  workflow_status = {aresult.get('workflow_status')}")
    print()

    session.commit()
    print("COMMITTED.")
    print()
    print("=" * 70)
    print("TEST RUN 2026-72 COMPLETE — STOP. Review before re-importing 2026-73 through 2026-78.")
    print("=" * 70)
