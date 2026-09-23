-- #2961 window B — record WHO entered the core broker verb, and when.
--
-- `mark_core_submission_entered` commits `submission_phase = 'broker_verb_entered'`
-- before the provider call. A process that dies after that commit leaves a row no
-- unattended path may resolve. The attended release
-- (`app/services/strategy_core_window_b_release.py`, spec
-- `docs/proposals/execution/2026-09-23-core-window-b-attended-release.md`) must first
-- prove the SENDER is dead. `CORE_SUBMISSION_ADVISORY_LOCK` being free proves only that
-- its Postgres backend is gone. A process that lost its connection can still be inside
-- `ResilientClient`'s POST retry, so the release also checks the sender's pid on this
-- host, and times its wait from the marker's own commit instant.
--
-- NULL on all three means NOT RECORDED: every row marked before this migration. The
-- release refuses such a row. No backfill, because the identity of a past sender cannot
-- be recovered. Census before this migration:
--   select submission_phase, state, count(*) from strategy_order_reconciliation_state
--   group by 1,2
-- returned `broker_verb_entered / resolved / 1`, so no non-terminal row is affected.

ALTER TABLE strategy_order_reconciliation_state
    ADD COLUMN IF NOT EXISTS submission_entered_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS submission_entered_pid INTEGER,
    ADD COLUMN IF NOT EXISTS submission_entered_host TEXT;
