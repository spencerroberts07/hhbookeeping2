-- Migration 021: hard_skip flag on bank_transaction_rules
--
-- Adds a per-rule "hard skip" flag so a DB-driven rule can declare
-- "this transaction belongs to another module — never post a journal".
-- Lets us mark inflow patterns (Moneris EFT batch deposits, TD Express
-- Deposits) that were already booked by the cash_balancing module as
-- skipped at the bank_auto_journal layer.

ALTER TABLE bank_transaction_rules
    ADD COLUMN IF NOT EXISTS hard_skip BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN bank_transaction_rules.hard_skip IS
    'When TRUE, bank_auto_journal records the txn as matched_status=skipped with the rule_code as the reason and does not post a journal entry. Use for inflow patterns already booked by cash_balancing or another upstream module.';
