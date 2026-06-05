-- Migration 049: year-end soft-close lifecycle (Phase 5A). Additive.
--
-- A NULLABLE column on accounting_periods. Only the September (fiscal year-end)
-- period ever carries a value; non-September periods are unaffected. This does
-- NOT change accounting_periods.status — the normal close lifecycle is untouched.
--
--   year_end_status:  NULL          - not a year-end period / not triggered
--                     'draft'       - set when the Sep period closes normally
--                     'in_review'   - accountant posting year_end_adjustment JEs
--                     'final_locked'- blocks ALL further JEs incl. adjustments

ALTER TABLE accounting_periods
    ADD COLUMN IF NOT EXISTS year_end_status TEXT
        CHECK (year_end_status IN ('draft', 'in_review', 'final_locked'));
