-- 293_strategy_capital_modes.sql
--
-- #2469 makes the two money semantics explicit and auditable. Existing rows
-- preserve their historical meaning: a fixed principal ceiling and a
-- percentage ticket bounded by max_ticket_amount.

ALTER TABLE strategy_paper_pool_events
    ADD COLUMN IF NOT EXISTS capital_mode TEXT NOT NULL DEFAULT 'fixed';

ALTER TABLE strategy_paper_pool_events
    DROP CONSTRAINT IF EXISTS strategy_paper_pool_capital_mode_check;
ALTER TABLE strategy_paper_pool_events
    ADD CONSTRAINT strategy_paper_pool_capital_mode_check
    CHECK (capital_mode IN ('fixed', 'compound'));

ALTER TABLE strategy_execution_policies
    ADD COLUMN IF NOT EXISTS ticket_sizing_mode TEXT NOT NULL DEFAULT 'percent',
    ADD COLUMN IF NOT EXISTS fixed_ticket_amount NUMERIC(18,6);

ALTER TABLE strategy_execution_policies
    ALTER COLUMN ticket_fraction DROP NOT NULL;
ALTER TABLE strategy_execution_policies
    DROP CONSTRAINT IF EXISTS strategy_execution_policies_ticket_fraction_check;
ALTER TABLE strategy_execution_policies
    DROP CONSTRAINT IF EXISTS strategy_execution_policy_ticket_shape;
ALTER TABLE strategy_execution_policies
    ADD CONSTRAINT strategy_execution_policy_ticket_shape CHECK (
        (ticket_sizing_mode = 'percent'
         AND ticket_fraction > 0 AND ticket_fraction <= 1
         AND fixed_ticket_amount IS NULL)
        OR
        (ticket_sizing_mode = 'fixed'
         AND ticket_fraction IS NULL
         AND fixed_ticket_amount > 0)
    );

ALTER TABLE strategy_execution_policy_events
    ADD COLUMN IF NOT EXISTS ticket_sizing_mode TEXT NOT NULL DEFAULT 'percent',
    ADD COLUMN IF NOT EXISTS fixed_ticket_amount NUMERIC(18,6);

ALTER TABLE strategy_execution_policy_events
    ALTER COLUMN ticket_fraction DROP NOT NULL;
ALTER TABLE strategy_execution_policy_events
    DROP CONSTRAINT IF EXISTS strategy_execution_policy_event_ticket_shape;
ALTER TABLE strategy_execution_policy_events
    ADD CONSTRAINT strategy_execution_policy_event_ticket_shape CHECK (
        (ticket_sizing_mode = 'percent'
         AND ticket_fraction > 0 AND ticket_fraction <= 1
         AND fixed_ticket_amount IS NULL)
        OR
        (ticket_sizing_mode = 'fixed'
         AND ticket_fraction IS NULL
         AND fixed_ticket_amount > 0)
    );

COMMENT ON COLUMN strategy_paper_pool_events.capital_mode IS
    'fixed: principal is the hard ceiling; compound: reconciled realised strategy P&L changes the risk base.';
COMMENT ON COLUMN strategy_execution_policies.ticket_sizing_mode IS
    'percent sizes from the effective deployment base; fixed uses fixed_ticket_amount. Both remain risk capped.';
