-- Migration 023: Inventory shrinkage adjustments on COGS journal
--
-- The monthly COGS journal now includes shrinkage from
-- inventory_adjustment_lines (cycle counts, broken/expired/stolen,
-- loss-other, recount). Greeting card returns are tracked separately
-- because their treatment (COGS vs vendor return AP) varies.

ALTER TABLE cogs_journal_inputs
    ADD COLUMN IF NOT EXISTS shrinkage_cogs NUMERIC(15, 2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS greeting_card_adj NUMERIC(15, 2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS shrinkage_included BOOLEAN NOT NULL DEFAULT TRUE;

COMMENT ON COLUMN cogs_journal_inputs.shrinkage_cogs IS
    'ABS sum of inventory_adjustment_lines.adjustment_cost for the period where reason matches a shrinkage family (cycle counts, broken, expired, stolen, recount, loss-other) AND adjustment_cost < 0. Always stored positive.';

COMMENT ON COLUMN cogs_journal_inputs.greeting_card_adj IS
    'Sum (signed) of inventory_adjustment_lines for GREETING CARD RETURNS in the period. Surfaced for bookkeeper review — not posted to the journal automatically.';

COMMENT ON COLUMN cogs_journal_inputs.shrinkage_included IS
    'Whether shrinkage_cogs was added to the COGS journal. Defaults TRUE; bookkeeper can disable per period if shrinkage is being booked elsewhere.';
