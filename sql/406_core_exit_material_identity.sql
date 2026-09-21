-- #3284 — an APPLIED exit repair must not make the same repair impossible forever.
--
-- `sql/289` created `idx_strategy_position_operation_material_identity` as a
-- PERMANENT uniqueness rule over (ownership, operation_type, desired stop, desired
-- take, completed bar). That was right for the arm it was written for: a
-- `stop_ratchet`'s stop is strictly monotonic (`sql/289`'s own CHECK requires
-- `desired_stop_rate > prior_stop_rate`), so an identical pair recurring really does
-- mean a duplicate.
--
-- #3284 breaks that assumption for `fixed_exit_repair` on the core arm. The core
-- sleeve's desired rates are a pure function of the position's ENTRY price, so they
-- are STABLE across cycles — a repair that applied last week matches today's intent
-- byte for byte. #3284's own acceptance is "clearing the stop manually at the broker
-- is detected on the next check and repaired", and under the permanent index that
-- second repair cannot be written at all: the INSERT raises `UniqueViolation` and the
-- position stays NAKED. Codex checkpoint 2 found the query-level half of this
-- (`_prior_same_edit` returning the applied row as `rejected`); the index is the half
-- underneath it, and fixing only the query leaves the wedge in place one layer down.
--
-- The fix excludes `applied` rows from the index, and nothing else:
--
--   * `rejected` / `reconcile_required` rows STAY indexed, so re-entering a verb the
--     broker already refused is still impossible at the schema level. That was the
--     property worth keeping — hammering the broker every five minutes with an edit it
--     rejected is not repair.
--   * concurrent in-flight duplicates are NOT this index's job and never were:
--     `idx_strategy_position_one_unresolved_operation` already admits at most one
--     `intent_persisted`/`submitted` row per ownership, independently of the rates.
--     That guard is untouched.
--
-- So the weakening is exactly "an applied repair no longer forbids a later identical
-- one", which is the behaviour #3284 requires and the one the permanent index
-- accidentally forbade.
--
-- ⚠ `NULLS NOT DISTINCT` is preserved. `completed_bar_at` and
-- `desired_take_profit_rate` are NULL on a fixed-exit repair, and under the default
-- NULLS DISTINCT every such row would be unique to Postgres and the index would guard
-- nothing at all on the very arm this ticket exercises.

DROP INDEX IF EXISTS idx_strategy_position_operation_material_identity;

CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_position_operation_material_identity
    ON strategy_position_operations (
        ownership_id, operation_type, desired_stop_rate,
        desired_take_profit_rate, completed_bar_at
    ) NULLS NOT DISTINCT
    WHERE operation_type IN ('fixed_exit_repair', 'stop_ratchet')
      AND status <> 'applied';
