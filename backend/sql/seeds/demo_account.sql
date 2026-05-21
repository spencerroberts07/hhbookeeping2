-- Seed: DEMO-1 demo account for prospects to explore.
--
-- Safe to re-run — every INSERT uses ON CONFLICT DO NOTHING.
--
-- Server-side middleware blocks all writes to entity_code='DEMO-1' so
-- the data here stays clean for every visitor.

BEGIN;

-- 1. Demo organization + entity. Province ON, fiscal year-end Sep 30
-- so the demo period (Feb 2026) falls in fiscal year 2026 like a real
-- HH dealer.
INSERT INTO organizations (id, name, created_at)
SELECT '00000000-0000-0000-0000-000000000d10'::uuid,
       'Demo Home Hardware',
       NOW()
WHERE NOT EXISTS (
    SELECT 1 FROM organizations WHERE id = '00000000-0000-0000-0000-000000000d10'::uuid
);

INSERT INTO entities (
    organization_id, entity_code, entity_name,
    fiscal_year_end_month, fiscal_year_end_day,
    base_currency, province, onboarding_complete,
    onboarding_completed_at, created_at
) VALUES (
    '00000000-0000-0000-0000-000000000d10'::uuid,
    'DEMO-1', 'Demo Home Hardware',
    9, 30, 'CAD', 'ON', TRUE, NOW(), NOW()
)
ON CONFLICT (entity_code) DO NOTHING;

-- 2. Internal billing subscription so /settings/billing shows the
-- Owner card and no Stripe call is ever made for DEMO-1.
INSERT INTO billing_customers (clerk_user_id, stripe_customer_id, name)
VALUES (
    'internal_demo', 'internal_demo',
    'BookWize Internal — Demo Account'
)
ON CONFLICT (clerk_user_id) DO NOTHING;

INSERT INTO billing_subscriptions (
    entity_id, billing_customer_id, stripe_subscription_id,
    plan_tier, status, current_period_end, cancel_at_period_end
)
SELECT
    e.id, c.id,
    'internal_demo:' || e.entity_code,
    'internal', 'active',
    NOW() + INTERVAL '100 years', FALSE
FROM entities e
CROSS JOIN billing_customers c
WHERE e.entity_code = 'DEMO-1'
  AND c.clerk_user_id = 'internal_demo'
ON CONFLICT (entity_id) DO NOTHING;

-- 3. Accounting period — Feb 2026 (matches the rest of the demo data).
INSERT INTO accounting_periods (
    entity_id, period_label, period_start, period_end, status
)
SELECT e.id, 'FY2026-P05 Feb 2026', '2026-02-01'::date, '2026-02-28'::date, 'draft'
FROM entities e WHERE e.entity_code = 'DEMO-1'
ON CONFLICT (entity_id, period_start, period_end) DO NOTHING;

-- 4. Bank transactions (20). Mix of reviewed + needs_review. The
-- row_number() in the CTE generates a unique source_transaction_id
-- per row so the (entity_id, source_system, source_transaction_id)
-- unique constraint is satisfied.
WITH eid AS (
    SELECT id FROM entities WHERE entity_code = 'DEMO-1' LIMIT 1
),
period AS (
    SELECT id FROM accounting_periods
     WHERE entity_id = (SELECT id FROM eid) AND period_end = '2026-02-28' LIMIT 1
),
rows AS (
    SELECT *, row_number() OVER () AS rn FROM (VALUES
      ('2026-02-03'::date, 'Moneris Settlement',  8421.55, 'inflow',  'reviewed',    'deposit'),
      ('2026-02-07'::date, 'Moneris Settlement', 14782.30, 'inflow',  'reviewed',    'deposit'),
      ('2026-02-12'::date, 'Moneris Settlement', 22154.80, 'inflow',  'reviewed',    'deposit'),
      ('2026-02-18'::date, 'Moneris Settlement', 31992.40, 'inflow',  'reviewed',    'deposit'),
      ('2026-02-24'::date, 'Moneris Settlement', 45120.10, 'inflow',  'reviewed',    'deposit'),
      ('2026-02-10'::date, 'HOME HARDWARE WIRE',  85240.00,'outflow','reviewed','payment'),
      ('2026-02-20'::date, 'HOME HARDWARE WIRE',  87510.00,'outflow','reviewed','payment'),
      ('2026-02-13'::date, 'ENetEmployer Payroll',12180.37,'outflow','reviewed','payment'),
      ('2026-02-27'::date, 'ENetEmployer Payroll',11863.27,'outflow','reviewed','payment'),
      ('2026-02-01'::date, 'Bridlewood Rent',      6471.66,'outflow','reviewed','payment'),
      ('2026-02-05'::date, 'ROGERS PAYMENT',        340.00,'outflow','reviewed','payment'),
      ('2026-02-08'::date, 'HYDRO ONE',             892.00,'outflow','reviewed','payment'),
      ('2026-02-15'::date, 'ONTARIO MUTUAL INS',   1240.00,'outflow','reviewed','payment'),
      ('2026-02-04'::date, 'STAPLES PURCHASE',      247.30,'outflow','reviewed','payment'),
      ('2026-02-11'::date, 'PUROLATOR',             182.50,'outflow','reviewed','payment'),
      ('2026-02-22'::date, 'CINTAS SERVICES',      1875.00,'outflow','reviewed','payment'),
      ('2026-02-09'::date, 'E-TRANSFER FROM PMT',  3500.00,'inflow','needs_review','deposit'),
      ('2026-02-14'::date, 'CHQ #1027',            2150.00,'outflow','needs_review','payment'),
      ('2026-02-19'::date, 'INTERAC FEE',            12.50,'outflow','new','payment'),
      ('2026-02-25'::date, 'UNKNOWN VENDOR',         475.00,'outflow','new','payment')
    ) AS t(txn_date, descr, amt, dir, rstatus, ttype)
)
INSERT INTO bank_transactions (
    entity_id, accounting_period_id, source_system,
    source_account_id, source_account_name, source_account_code,
    source_transaction_id, source_transaction_type,
    transaction_date, posted_date, description,
    amount, direction, review_status
)
SELECT (SELECT id FROM eid), (SELECT id FROM period),
       'demo_seed', 'demo-checking', 'TD Operating', '1020',
       'demo:' || rn::text, ttype,
       txn_date, txn_date, descr,
       amt, dir, rstatus
FROM rows
ON CONFLICT (entity_id, source_system, source_transaction_id) DO NOTHING;

-- 5. Journal batches (3). Use approved_to_post status so the demo
-- looks like a partially-closed period.
WITH eid AS (
    SELECT id FROM entities WHERE entity_code = 'DEMO-1' LIMIT 1
),
period AS (
    SELECT id FROM accounting_periods
     WHERE entity_id = (SELECT id FROM eid) AND period_end = '2026-02-28' LIMIT 1
)
INSERT INTO journal_batches (
    entity_id, accounting_period_id, source_module, batch_label,
    status, workflow_status, total_debits, total_credits,
    approved_by, approved_at
)
SELECT (SELECT id FROM eid), (SELECT id FROM period),
       t.source_module, t.batch_label,
       t.status, t.status,
       t.total_dr, t.total_dr,
       'demo@bookwize.ca', NOW()
FROM (VALUES
    ('cogs',                       'monthly_cogs',           94500.00, 'approved_to_post'),
    ('payroll',                    'biweekly_payroll_p4',    11200.00, 'approved_to_post'),
    ('hh_ap_remittance_clearing',  'hh_ap_remittance',       52000.00, 'approved_to_post')
) AS t(source_module, batch_label, total_dr, status)
ON CONFLICT (entity_id, accounting_period_id, source_module, batch_label) DO NOTHING;

-- 6. Cash balancing day — closing balance for Feb 28, 2026.
INSERT INTO cash_balancing_days (
    entity_id, business_date, opening_cash, closing_cash, total_sales
)
SELECT e.id, '2026-02-28'::date, 165500.00, 167842.00, 122471.15
FROM entities e WHERE e.entity_code = 'DEMO-1'
ON CONFLICT DO NOTHING;

COMMIT;
