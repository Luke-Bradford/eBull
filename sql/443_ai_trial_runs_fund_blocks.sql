-- 443_ai_trial_runs_fund_blocks.sql
--
-- #3515 slice 3b-iii (spec docs/proposals/execution/2026-09-30-3515-ai-discretionary-fund-v1.md §2, §3, §5).
--
-- fund-v1's per-run record of its two pack blocks: the snapshot time, both coverage counts and
-- each shown name's audit counts (`ai_trial_fund_blocks.NameAudit`). Spec: the audit goes to the
-- RUN RECORD, never the pack (`withheld_after_as_of` is post-cutoff information), and both
-- coverage counts are recorded on every run whatever the verdict.
--
-- Nullable, written only by fund-v1's pack builder through the one legal claimed -> decided |
-- refused transition (sql/432's trigger does not constrain it). v1 never writes it, so every
-- existing row stays NULL and no v1 CHECK changes.

ALTER TABLE ai_trial_runs ADD COLUMN IF NOT EXISTS fund_blocks JSONB;

COMMENT ON COLUMN ai_trial_runs.fund_blocks IS
    '#3515 fund-v1 run record: snapshot_at, coverage counts and per-name audit counts. NULL for v1.';
