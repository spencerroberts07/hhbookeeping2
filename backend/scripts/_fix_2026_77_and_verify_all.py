# -*- coding: utf-8 -*-
"""
Rebuild 2026-77 journal (fix negative 2220 credit -> Dr 2220 drawdown),
then verify all 7 runs 2026-72 through 2026-78.
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

with engine.connect() as session:
    # --- Rebuild 2026-77 ---
    run_row = session.execute(text(
        "SELECT id, workflow_status FROM payroll_runs "
        "WHERE entity_id=:eid AND pay_run_number='2026-77'"
    ), {"eid": EID}).mappings().first()
    run_id = str(run_row["id"])

    print("Rebuilding 2026-77 journal (fix Dr 2220 vacation drawdown)...")
    jresult = build_payroll_journal(
        session, entity_code=EC, payroll_run_id=run_id, actor_email=ACTOR
    )
    lines_77 = jresult["lines"]
    print(f"  {'Acct':<8} {'Dr':>12} {'Cr':>12}  Component")
    print("  " + "-" * 55)
    for l in lines_77:
        print(f"  {l['account_code']:<8} {float(l['debit_amount']):>12,.2f} "
              f"{float(l['credit_amount']):>12,.2f}  [{l['component']}]")
    td = sum(Decimal(l['debit_amount']) for l in lines_77)
    tc = sum(Decimal(l['credit_amount']) for l in lines_77)
    print(f"  {'TOTALS':<8} {float(td):>12,.2f} {float(tc):>12,.2f}  "
          f"{'BALANCED' if td==tc else f'MISMATCH {td-tc}'}")

    totals = jresult["summary"]["totals"]
    plug = Decimal(totals["bank_plug"])
    expected = Decimal(totals["net_pay"]) + Decimal(totals["cra_remittance"]) + Decimal(totals["enet_fee"])
    print(f"  Plug check: {float(plug):,.2f} =? {float(expected):,.2f}  "
          f"{'OK' if abs(plug-expected)<=Decimal('0.01') else 'WARNING'}")
    accts = {l['account_code'] for l in lines_77}
    print(f"  Accounts: {sorted(accts)}  2320={'PRESENT - ERROR' if '2320' in accts else 'absent OK'}")
    neg_cr = [l for l in lines_77 if Decimal(l['credit_amount']) < 0]
    print(f"  Negative credits: {len(neg_cr)}  {'NONE - OK' if not neg_cr else [(l['account_code'], l['credit_amount']) for l in neg_cr]}")

    approve_payroll_run(session, entity_code=EC, payroll_run_id=run_id, actor_email=ACTOR)
    print("  Approved.")

    session.commit()
    print("  COMMITTED.")

# --- Verify all 7 runs ---
print("\n" + "=" * 80)
print("FINAL VERIFICATION — All 7 runs (2026-72 through 2026-78)")
print("=" * 80)

RUN_FEES = {
    "2026-72": Decimal("40.25"),
    "2026-73": Decimal("38.50"),
    "2026-74": Decimal("17.50"),
    "2026-75": Decimal("42.00"),
    "2026-76": Decimal("42.00"),
    "2026-77": Decimal("42.00"),
    "2026-78": Decimal("42.00"),
}

with engine.connect() as c:
    for rn in sorted(RUN_FEES.keys()):
        lines = c.execute(text("""
            SELECT jl.account_code, jl.debit_amount, jl.credit_amount, jl.memo
            FROM journal_lines jl
            JOIN journal_batches jb ON jb.id = jl.journal_batch_id
            JOIN payroll_runs pr ON pr.journal_batch_id = jb.id
            WHERE jb.entity_id=:eid AND pr.pay_run_number=:rn
            AND jb.status != 'voided'
            ORDER BY jl.line_number
        """), {"eid": EID, "rn": rn}).mappings().all()

        pr = c.execute(text("""
            SELECT total_gross, total_net_pay, cra_remittance_amount, enet_fee,
                   total_vacation_earned, workflow_status
            FROM payroll_runs WHERE entity_id=:eid AND pay_run_number=:rn
        """), {"eid": EID, "rn": rn}).mappings().first()

        td = sum(Decimal(str(l['debit_amount'])) for l in lines)
        tc = sum(Decimal(str(l['credit_amount'])) for l in lines)
        accts = sorted({l['account_code'] for l in lines})
        neg_cr_count = sum(1 for l in lines if Decimal(str(l['credit_amount'])) < 0)
        neg_dr_count = sum(1 for l in lines if Decimal(str(l['debit_amount'])) < 0)

        net = Decimal(str(pr['total_net_pay']))
        cra = Decimal(str(pr['cra_remittance_amount']))
        fee = Decimal(str(pr['enet_fee']))
        plug_line = next((l for l in lines if l['account_code'] == '1020'), None)
        plug = Decimal(str(plug_line['credit_amount'])) if plug_line else Decimal("0")
        expected_plug = net + cra + fee
        plug_ok = abs(plug - expected_plug) <= Decimal("0.01")

        bal_ok = td == tc
        no_2320 = "2320" not in accts
        no_neg = neg_cr_count == 0 and neg_dr_count == 0

        status = "OK" if (bal_ok and no_2320 and plug_ok and no_neg) else "ISSUES"
        vac = Decimal(str(pr['total_vacation_earned']))
        print(f"\n{rn}  gross={pr['total_gross']}  wf={pr['workflow_status']}  [{status}]")
        print(f"  vac_earned={vac:+.2f}  enet_fee={fee}")
        print(f"  Accounts: {accts}")
        print(f"  Balanced: {'YES' if bal_ok else f'NO Dr={td} Cr={tc}'}")
        print(f"  No 2320:  {'YES' if no_2320 else 'NO - ERROR'}")
        print(f"  No neg amounts: {'YES' if no_neg else f'NO ({neg_cr_count} neg Cr, {neg_dr_count} neg Dr)'}")
        print(f"  Plug: 1020_Cr={float(plug):,.2f}  net+CRA+fee={float(expected_plug):,.2f}  {'OK' if plug_ok else 'MISMATCH'}")
