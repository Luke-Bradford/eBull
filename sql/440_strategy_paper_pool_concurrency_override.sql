-- 440_strategy_paper_pool_concurrency_override.sql
--
-- #3471 §8 supervisor answer (2026-09-30): the demo trial runs 12 slots per leg, and both legs
-- plus every open core lifecycle count against the POOL's `max_concurrent_positions`
-- (`strategy_paper_executor._MANDATE_OBSERVATION_SQL`). The largest v1 profile, `growth`, caps
-- that at 12 in total, so the trial would get at most 5 per leg and the executor would refuse
-- legs mid-trial, breaking pairs.
--
-- ⚠ WHY AN OVERRIDE AND NOT A NEW PROFILE VALUE. `mandate_for_profile` resolves a profile label
-- to immutable v1 limits; raising `growth` to 30 would silently widen every future growth
-- mandate, live included. The override is a per-REVISION column on the append-only pool table,
-- so it inherits that table's audit trail (`changed_by`, `reason`, `changed_at`) and the
-- authority in force at any instant is still the latest event.
--
-- Effective cap = COALESCE(max_concurrent_positions_override, max_concurrent_positions); every
-- reader uses `strategy_control_plane.EFFECTIVE_MAX_CONCURRENT_SQL`, not its own spelling.
--
-- Demo-only: `configure_paper_pool` refuses a non-null override outside `etoro_env = demo` or
-- while system-wide live trading is on, and `app.api.config.patch_config` refuses to enable live
-- trading while the latest pool revision carries one. The CHECK below is the backstop for shape
-- only; the environment is not a database fact.
--
-- Every existing row takes NULL, i.e. the profile's own cap, unchanged.

ALTER TABLE strategy_paper_pool_events
    ADD COLUMN IF NOT EXISTS max_concurrent_positions_override INTEGER;

-- DROP-then-ADD so a replay converges (sql/365's pattern).
ALTER TABLE strategy_paper_pool_events
    DROP CONSTRAINT IF EXISTS strategy_paper_pool_concurrency_override;
ALTER TABLE strategy_paper_pool_events
    ADD CONSTRAINT strategy_paper_pool_concurrency_override CHECK (
        max_concurrent_positions_override IS NULL
        OR (
            max_concurrent_positions_override > 0
            -- An unconfigured mandate authorises nothing, so it cannot carry a widened cap.
            AND risk_profile <> 'unconfigured'
        )
    );

COMMENT ON COLUMN strategy_paper_pool_events.max_concurrent_positions_override IS
    'Demo-only per-revision replacement for the profile''s max_concurrent_positions (#3471 §8). '
    'NULL = the profile cap. Effective cap = COALESCE(override, max_concurrent_positions).';
