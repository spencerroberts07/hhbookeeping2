import sys, os
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
from decimal import Decimal
from sqlalchemy import create_engine, text
e = create_engine(os.environ['DATABASE_URL'])
EID='0bab9284-68d9-4769-bfc6-4dac5bd1f5e4'
with e.connect() as c:
    r = c.execute(text("""
        SELECT pay_run_number, total_vacation_earned, total_vacation_paid,
               total_gross, total_net_pay
        FROM payroll_runs WHERE entity_id=:eid
        AND pay_run_number IN ('2026-76','2026-77','2026-78')
        ORDER BY pay_run_number
    """), {'eid': EID}).mappings().all()
    print("Run          vac_earned    vac_paid       gross         net")
    for row in r:
        print(f"  {row['pay_run_number']}  {str(row['total_vacation_earned']):>12}  {str(row['total_vacation_paid']):>10}  {str(row['total_gross']):>10}  {str(row['total_net_pay']):>10}")
    print()
    lines = c.execute(text("""
        SELECT jl.account_code, jl.debit_amount, jl.credit_amount, jl.memo
        FROM journal_lines jl
        JOIN journal_batches jb ON jb.id = jl.journal_batch_id
        JOIN payroll_runs pr ON pr.journal_batch_id = jb.id
        WHERE jb.entity_id=:eid AND pr.pay_run_number='2026-77'
        ORDER BY jl.line_number
    """), {'eid': EID}).mappings().all()
    print("2026-77 live journal lines:")
    td=Decimal('0'); tc=Decimal('0')
    for l in lines:
        td+=Decimal(str(l['debit_amount'])); tc+=Decimal(str(l['credit_amount']))
        print(f"  {l['account_code']}  Dr={str(l['debit_amount']):>12}  Cr={str(l['credit_amount']):>12}  {l['memo']}")
    print(f"  TOTALS  Dr={td}  Cr={tc}  {'BALANCED' if td==tc else 'MISMATCH'}")
    print()
    # Per-line vacation for 2026-77
    plines = c.execute(text("""
        SELECT pe.first_name, pe.last_name, prl.vacation_earned, prl.vacation_paid, prl.gross_pay
        FROM payroll_run_lines prl
        JOIN payroll_runs pr ON pr.id = prl.payroll_run_id
        LEFT JOIN payroll_employees pe ON pe.id = prl.employee_id
        WHERE pr.entity_id=:eid AND pr.pay_run_number='2026-77'
        ORDER BY pe.last_name
    """), {'eid': EID}).mappings().all()
    print("Per-line vacation detail for 2026-77:")
    print(f"  {'Name':<25} {'vac_earned':>12} {'vac_paid':>10} {'gross':>10}")
    sum_ve=Decimal('0'); sum_vp=Decimal('0')
    for l in plines:
        ve=Decimal(str(l['vacation_earned'])); vp=Decimal(str(l['vacation_paid']))
        sum_ve+=ve; sum_vp+=vp
        name=f"{l['first_name']} {l['last_name']}" if l['first_name'] else '?'
        print(f"  {name:<25} {str(ve):>12} {str(vp):>10} {str(l['gross_pay']):>10}")
    print(f"  {'TOTALS':<25} {str(sum_ve):>12} {str(sum_vp):>10}")
