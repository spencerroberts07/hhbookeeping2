# -*- coding: utf-8 -*-
"""
Re-import 6 remaining voided payroll runs (2026-73 through 2026-78)
with corrected QBO-aligned journal structure.
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

RUNS = [
    ("2026-73", Decimal("38.50"), "Mar 15-28 staff"),
    ("2026-74", Decimal("17.50"), "Mar 15-28 Spencer"),
    ("2026-75", Decimal("42.00"), "Mar 29-Apr 11"),
    ("2026-76", Decimal("42.00"), "Apr 12-25"),
    ("2026-77", Decimal("42.00"), "Apr 26-May 9"),
    ("2026-78", Decimal("42.00"), "May 10-23"),
]


def reimport_one(session, run_number, enet_fee, label):
    run_row = session.execute(text("""
        SELECT id, pay_run_number, period_start, period_end,
               total_gross, total_net_pay, cra_remittance_amount,
               total_fed_tax, total_cpp_ee, total_ei_ee,
               workflow_status
        FROM payroll_runs
        WHERE entity_id = :eid AND pay_run_number = :rn
    """), {"eid": EID, "rn": run_number}).mappings().first()

    if not run_row:
        raise ValueError(f"Run {run_number} not found")

    run_id = str(run_row["id"])
    gross = Decimal(str(run_row["total_gross"]))
    net   = Decimal(str(run_row["total_net_pay"]))
    cra   = Decimal(str(run_row["cra_remittance_amount"]))
    ft    = Decimal(str(run_row["total_fed_tax"]))
    cpp   = Decimal(str(run_row["total_cpp_ee"]))
    ei    = Decimal(str(run_row["total_ei_ee"]))
    benefit_offset = max(Decimal("0"), gross - net - ft - cpp - ei)

    print(f"\n{'=' * 70}")
    print(f"Run {run_number}  [{label}]  {run_row['period_start']} to {run_row['period_end']}")
    print(f"  gross={gross}  net={net}  cra={cra}  wf={run_row['workflow_status']}")
    print(f"  Benefit offset: {gross}-{net}-{ft}(fed)-{cpp}(cpp_ee)-{ei}(ei_ee) = "
          f"{gross - net - ft - cpp - ei} => clamped {benefit_offset}")
    print(f"  Setting enet_fee = {enet_fee}")

    # Set enet_fee
    session.execute(text(
        "UPDATE payroll_runs SET enet_fee = :fee WHERE id = :id AND entity_id = :eid"
    ), {"fee": enet_fee, "id": run_id, "eid": EID})

    # Build journal
    jresult = build_payroll_journal(
        session, entity_code=EC, payroll_run_id=run_id, actor_email=ACTOR
    )
    batch_id = jresult["journal_batch_id"]
    lines    = jresult["lines"]
    totals   = jresult["summary"]["totals"]

    print(f"\n  journal_batch_id = {batch_id}")
    print(f"  {'Acct':<8} {'Dr':>12} {'Cr':>12}  Component / Memo")
    print("  " + "-" * 68)
    total_dr = Decimal("0")
    total_cr = Decimal("0")
    for l in lines:
        dr = Decimal(l["debit_amount"])
        cr = Decimal(l["credit_amount"])
        total_dr += dr
        total_cr += cr
        print(f"  {l['account_code']:<8} {float(dr):>12,.2f} {float(cr):>12,.2f}  "
              f"[{l['component']}] {l['memo']}")
    print("  " + "-" * 68)
    balanced = "BALANCED" if total_dr == total_cr else f"MISMATCH {total_dr - total_cr}"
    print(f"  {'TOTALS':<8} {float(total_dr):>12,.2f} {float(total_cr):>12,.2f}  {balanced}")

    # Plug check
    plug     = Decimal(totals["bank_plug"])
    expected = net + cra + enet_fee
    diff     = abs(plug - expected)
    print(f"\n  Plug: {float(plug):,.2f}  =?  net({float(net):,.2f}) + CRA({float(cra):,.2f}) + fee({float(enet_fee):,.2f}) = {float(expected):,.2f}  "
          f"diff={float(diff):.4f}  {'OK' if diff <= Decimal('0.01') else 'WARNING'}")

    # 2320 guard
    accts = {l["account_code"] for l in lines}
    if "2320" in accts:
        raise ValueError(f"2320 line found in {run_number} — aborting")
    print(f"  Accounts used: {sorted(accts)}  No 2320 ✓")

    # 6110 check for 2026-74 (Spencer)
    if run_number == "2026-74":
        has_6110 = "6110" in accts
        has_6120 = "6120" in accts
        print(f"  Management check: 6110={'YES' if has_6110 else 'NO'}  "
              f"6120={'YES (should be empty or wage-only)' if has_6120 else 'NO (expected if Spencer-only)'}")

    # Approve
    aresult = approve_payroll_run(
        session, entity_code=EC, payroll_run_id=run_id, actor_email=ACTOR
    )
    print(f"  Approved: workflow_status = {aresult.get('workflow_status')}")

    return {
        "run_number": run_number,
        "batch_id": batch_id,
        "gross": str(gross),
        "net": str(net),
        "cra": str(cra),
        "fee": str(enet_fee),
        "plug": str(plug),
        "expected": str(expected),
        "balanced": balanced,
        "benefit_offset": str(benefit_offset),
        "accounts": sorted(accts),
    }


results = []
with engine.connect() as session:
    for run_number, fee, label in RUNS:
        r = reimport_one(session, run_number, fee, label)
        results.append(r)

    session.commit()
    print(f"\n{'=' * 70}")
    print("ALL 6 RUNS COMMITTED")
    print(f"{'=' * 70}")

# Final summary table
print(f"\n{'Run':<10} {'Gross':>12} {'Net':>12} {'CRA':>10} {'Fee':>7} {'Plug':>12} {'BenOffset':>10}  Accounts")
print("-" * 95)
for r in results:
    print(f"{r['run_number']:<10} {float(r['gross']):>12,.2f} {float(r['net']):>12,.2f} "
          f"{float(r['cra']):>10,.2f} {float(r['fee']):>7,.2f} {float(r['plug']):>12,.2f} "
          f"{float(r['benefit_offset']):>10,.2f}  {r['accounts']}")
