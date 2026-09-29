-- 439_ai_trial_structure_plans.sql
--
-- #3471 slice v6-3a — spec v6 structure-based trade plans: the model names a setup and two level
-- ids, the server derives every figure from the pack (§16.3), and the control applies the SAME
-- ids to its own name's levels (§16.4). The library row is recorded as the decision's baseline
-- (§16.11). Spec: docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md §16.3, §16.4,
-- §16.8, §16.11.
-- Writer: app/services/ai_trial_guard.py (validate_response, plan_pairs) via ai_trial_run.py;
-- the derivation is app/services/ai_trial_plan.py (plan_figures, derive_plan).
--
-- Replaces sql/433's two verify functions: `stop_atr_multiple` and `r_multiple` have new
-- definitions (on PRICES, not on the model's percentages), and the control's levels are no
-- longer the arm's multiples applied to its ATR but its own levels for the arm's ids.
--
-- ⚠ THE DATABASE NEVER DIVIDES (sql/433's rule, unchanged). A stored `x = qs(n / d)` is checked
-- by the grid + multiplication bracket on |x| and |n| (r2-12); `stop_price = invalidation −
-- atr14/4` is checked as `4 * (invalidation − stop_price) = atr14`. Triggers REFUSE; they never
-- overwrite a supplied value. Level PROVENANCE (a price equals the stored pack's
-- `levels[id].price`) needs the pack and is the validator's check, not this file's (§16.3).
--
-- ⚠ A stored DOUBLE is read as `x::text::numeric` (sql/433's note on Postgres 17's 15-digit cast).
--
-- The dev DB held 0 rows in ai_trial_decisions and ai_trial_pairs when this was written
-- (`SELECT count(*) FROM ai_trial_decisions` / `ai_trial_pairs`), so nothing needs a backfill; the
-- new columns are nullable because ALTER TABLE validates existing rows, and the triggers require
-- them on every new row.


-- ---------------------------------------------------------------------------
-- 0. Shared checks
-- ---------------------------------------------------------------------------
-- §16.2 `qs(x) = sign(x) × q(|x|)`: the sql/433 bracket applied to |x| and |n| (d > 0).
CREATE OR REPLACE FUNCTION ai_trial_is_quantized_signed(x NUMERIC, n NUMERIC, d NUMERIC)
RETURNS BOOLEAN LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN n >= 0 THEN ai_trial_is_quantized(x, n, d)
        WHEN n < 0 THEN ai_trial_is_quantized(-x, -n, d)
        ELSE false
    END
$$;

-- §16.1 stop floor in ATR14 by horizon (the random-entry MAE p50 rounded up to 0.5).
CREATE OR REPLACE FUNCTION ai_trial_stop_floor_atr(horizon_days INTEGER)
RETURNS NUMERIC LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE horizon_days WHEN 5 THEN 1.0 WHEN 10 THEN 1.5 WHEN 20 THEN 2.0 END
$$;

-- §16.3 derivation re-check: NULL when every recorded figure is the derivation of its inputs,
-- else what is wrong. Each figure is NULL exactly when an input is NULL or its denominator ≤ 0.
CREATE OR REPLACE FUNCTION ai_trial_plan_derivation_error(
    atr14 NUMERIC, close NUMERIC, invalidation_price NUMERIC, target_price NUMERIC,
    stop_price NUMERIC, stop_atr_multiple NUMERIC, stop_pct NUMERIC, target_pct NUMERIC,
    r_multiple NUMERIC
) RETURNS TEXT LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF atr14 IS NULL OR close IS NULL THEN
        IF num_nonnulls(invalidation_price, target_price, stop_price, stop_atr_multiple, stop_pct,
                        target_pct, r_multiple) <> 0 THEN
            RETURN 'a plan without a valid ATR measurement carries no levels or figures';
        END IF;
        RETURN NULL;
    END IF;
    IF (invalidation_price IS NULL) <> (stop_price IS NULL) THEN
        RETURN 'stop_price is present exactly when invalidation_price is';
    END IF;
    IF stop_price IS NOT NULL AND 4 * (invalidation_price - stop_price) <> atr14 THEN
        RETURN format('stop_price %s is not invalidation_price - atr14 / 4', stop_price);
    END IF;
    IF (stop_price IS NULL) <> (stop_atr_multiple IS NULL) OR (stop_price IS NULL) <> (stop_pct IS NULL) THEN
        RETURN 'stop_atr_multiple and stop_pct are present exactly when stop_price is';
    END IF;
    IF (target_price IS NULL) <> (target_pct IS NULL) THEN
        RETURN 'target_pct is present exactly when target_price is';
    END IF;
    IF stop_price IS NOT NULL THEN
        IF NOT ai_trial_is_quantized_signed(stop_atr_multiple, close - stop_price, atr14) THEN
            RETURN format('stop_atr_multiple %s is not qs((close - stop_price) / atr14)', stop_atr_multiple);
        END IF;
        IF NOT ai_trial_is_quantized_signed(stop_pct, 100 * (close - stop_price), close) THEN
            RETURN format('stop_pct %s is not qs(100 * (close - stop_price) / close)', stop_pct);
        END IF;
    END IF;
    IF target_price IS NOT NULL
       AND NOT ai_trial_is_quantized_signed(target_pct, 100 * (target_price - close), close) THEN
        RETURN format('target_pct %s is not qs(100 * (target_price - close) / close)', target_pct);
    END IF;
    IF stop_price IS NULL OR target_price IS NULL OR close - stop_price <= 0 THEN
        IF r_multiple IS NOT NULL THEN
            RETURN 'r_multiple is NULL when an input is NULL or close - stop_price <= 0';
        END IF;
    ELSIF NOT ai_trial_is_quantized_signed(r_multiple, target_price - close, close - stop_price) THEN
        RETURN format('r_multiple %s is not qs((target_price - close) / (close - stop_price))', r_multiple);
    END IF;
    RETURN NULL;
END $$;

-- §16.3 orders 8–12 on the recorded figures: what an accepted decision and a control must pass.
CREATE OR REPLACE FUNCTION ai_trial_plan_admissible(
    horizon_days INTEGER, close NUMERIC, invalidation_price NUMERIC, target_price NUMERIC,
    stop_price NUMERIC, stop_atr_multiple NUMERIC, stop_pct NUMERIC, target_pct NUMERIC,
    r_multiple NUMERIC
) RETURNS BOOLEAN LANGUAGE sql IMMUTABLE AS $$
    SELECT coalesce(
        stop_price > 0 AND invalidation_price < close AND target_price > close
        AND stop_atr_multiple >= ai_trial_stop_floor_atr(horizon_days)
        AND stop_atr_multiple <= 4
        AND r_multiple >= 2 AND target_pct >= 2 * stop_pct
        AND stop_pct BETWEEN 2 AND 25 AND target_pct BETWEEN 2 AND 100,
        false
    )
$$;


-- ---------------------------------------------------------------------------
-- 1. Decisions: the model's choice, the derived prices, the baseline
-- ---------------------------------------------------------------------------
ALTER TABLE ai_trial_decisions
    ADD COLUMN IF NOT EXISTS setup_type                   TEXT CHECK (setup_type IN (
        'breakout_donchian20', 'pullback_rising_sma20', 'pullback_rising_sma50',
        'range_support_bounce', 'trend_continuation_flag', 'none')),
    ADD COLUMN IF NOT EXISTS invalidation_level_id        TEXT CHECK (invalidation_level_id IN (
        'swing_low_1', 'swing_low_2', 'swing_low_3', 'donchian20_low', 'donchian55_low',
        'sma20', 'sma50', 'sma200', 'vwap20_proxy')),
    ADD COLUMN IF NOT EXISTS target_level_id              TEXT CHECK (target_level_id IN (
        'swing_high_1', 'swing_high_2', 'swing_high_3', 'donchian20_high', 'donchian55_high',
        'range20_projection', 'mm_up')),
    ADD COLUMN IF NOT EXISTS invalidation_price           NUMERIC,
    ADD COLUMN IF NOT EXISTS target_price                 NUMERIC,
    ADD COLUMN IF NOT EXISTS stop_price                   NUMERIC,
    ADD COLUMN IF NOT EXISTS base_rate_train_mean_net_r   TEXT,
    ADD COLUMN IF NOT EXISTS base_rate_holdout_mean_net_r TEXT;

-- §16.8 (r2-13): `stop_pct` / `target_pct` are server-derived now; a refused row may carry NULL
-- or a non-positive value, so the v4 bounds bind ACCEPTED rows only.
ALTER TABLE ai_trial_decisions
    ALTER COLUMN stop_pct DROP NOT NULL,
    ALTER COLUMN target_pct DROP NOT NULL,
    DROP CONSTRAINT IF EXISTS ai_trial_decisions_stop_pct_check,
    DROP CONSTRAINT IF EXISTS ai_trial_decisions_target_pct_check;
ALTER TABLE ai_trial_decisions DROP CONSTRAINT IF EXISTS ai_trial_decisions_accepted_within_bounds;
ALTER TABLE ai_trial_decisions ADD CONSTRAINT ai_trial_decisions_accepted_within_bounds CHECK (
    verdict <> 'accepted' OR coalesce(stop_pct BETWEEN 2 AND 25 AND target_pct BETWEEN 2 AND 100, false));

-- `target_not_above_stop` stays admissible (sql/432) but is not emitted; order 7's
-- `setup_negative_base_rate` was never implemented and is not added (§16.11 v63-41).
ALTER TABLE ai_trial_decisions DROP CONSTRAINT IF EXISTS ai_trial_decisions_reason_code_check;
ALTER TABLE ai_trial_decisions ADD CONSTRAINT ai_trial_decisions_reason_code_check CHECK (reason_code IN (
    'not_in_shortlist', 'duplicate_symbol', 'already_held', 'target_not_above_stop',
    'no_valid_plan', 'setup_not_detected', 'setup_base_rate_missing', 'level_unavailable',
    'stop_below_horizon_floor', 'stop_outside_atr_band', 'reward_risk_below_min',
    'plan_outside_bounds', 'thesis_too_long', 'control_pool_exhausted'));

-- §16.11: both baselines or neither; a canonical fraction each; both on every accepted row.
-- Stage (set exactly when order 6 passed) and value provenance need the library: validator.
ALTER TABLE ai_trial_decisions DROP CONSTRAINT IF EXISTS ai_trial_decisions_baseline_paired;
ALTER TABLE ai_trial_decisions ADD CONSTRAINT ai_trial_decisions_baseline_paired CHECK (
    (base_rate_train_mean_net_r IS NULL) = (base_rate_holdout_mean_net_r IS NULL));
ALTER TABLE ai_trial_decisions DROP CONSTRAINT IF EXISTS ai_trial_decisions_baseline_shape;
ALTER TABLE ai_trial_decisions ADD CONSTRAINT ai_trial_decisions_baseline_shape CHECK (
    (base_rate_train_mean_net_r IS NULL OR base_rate_train_mean_net_r ~ '^(-?[1-9][0-9]*/[1-9][0-9]*|0/1)$')
    AND (base_rate_holdout_mean_net_r IS NULL
         OR base_rate_holdout_mean_net_r ~ '^(-?[1-9][0-9]*/[1-9][0-9]*|0/1)$'));
ALTER TABLE ai_trial_decisions DROP CONSTRAINT IF EXISTS ai_trial_decisions_accepted_has_baseline;
ALTER TABLE ai_trial_decisions ADD CONSTRAINT ai_trial_decisions_accepted_has_baseline CHECK (
    verdict <> 'accepted' OR base_rate_train_mean_net_r IS NOT NULL);

CREATE OR REPLACE FUNCTION ai_trial_decisions_verify_metrics()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    stop_n   NUMERIC := NEW.stop_pct::text::numeric;
    target_n NUMERIC := NEW.target_pct::text::numeric;
    problem  TEXT;
BEGIN
    IF num_nonnulls(NEW.setup_type, NEW.invalidation_level_id, NEW.target_level_id) <> 3 THEN
        RAISE EXCEPTION 'a v6 decision names its setup and both level ids';
    END IF;
    IF num_nonnulls(NEW.atr14, NEW.close, NEW.atr14_pct) NOT IN (0, 3) THEN
        RAISE EXCEPTION 'decision ATR fields are all present or all NULL';
    END IF;
    IF NEW.atr14 IS NOT NULL THEN
        IF NEW.instrument_id IS NULL THEN
            RAISE EXCEPTION 'an unmapped symbol has no ATR measurement';
        END IF;
        IF NOT coalesce(NEW.atr14 > 0 AND NEW.close > 0 AND NEW.atr14_pct > 0, false)
           OR NOT ai_trial_is_quantized(NEW.atr14_pct, 100 * NEW.atr14, NEW.close) THEN
            RAISE EXCEPTION 'decision atr14_pct % is not a valid q(100 * atr14 / close)', NEW.atr14_pct;
        END IF;
    END IF;
    problem := ai_trial_plan_derivation_error(
        NEW.atr14, NEW.close, NEW.invalidation_price, NEW.target_price, NEW.stop_price,
        NEW.stop_atr_multiple, stop_n, target_n, NEW.r_multiple);
    IF problem IS NOT NULL THEN
        RAISE EXCEPTION 'decision plan: %', problem;
    END IF;
    IF NEW.verdict = 'accepted' AND (
        NEW.setup_type = 'none'
        OR NOT ai_trial_plan_admissible(
            NEW.horizon_days, NEW.close, NEW.invalidation_price, NEW.target_price, NEW.stop_price,
            NEW.stop_atr_multiple, stop_n, target_n, NEW.r_multiple)
    ) THEN
        RAISE EXCEPTION 'an accepted decision needs a named setup and a plan passing §16.3 orders 8-12';
    END IF;
    RETURN NEW;
END $$;
-- trg_ai_trial_decisions_verify_metrics (sql/433) already executes this function.


-- ---------------------------------------------------------------------------
-- 2. Pairs: the control's own levels for the arm's ids (§16.4)
-- ---------------------------------------------------------------------------
-- `stop_pct` / `target_pct` stay the ARM's (copied, sql/432 checks the copy); the control's are
-- `control_stop_pct` / `control_target_pct`. The two recorded quotients are kept too, so the
-- floor, ceiling and R checks run on the same quantized values the guard judged.
ALTER TABLE ai_trial_pairs
    ADD COLUMN IF NOT EXISTS control_invalidation_price NUMERIC,
    ADD COLUMN IF NOT EXISTS control_target_price       NUMERIC,
    ADD COLUMN IF NOT EXISTS control_stop_price         NUMERIC,
    ADD COLUMN IF NOT EXISTS control_stop_atr_multiple  NUMERIC,
    ADD COLUMN IF NOT EXISTS control_r_multiple         NUMERIC;

CREATE OR REPLACE FUNCTION ai_trial_pairs_verify_control_levels()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    d       ai_trial_decisions%ROWTYPE;
    problem TEXT;
BEGIN
    SELECT * INTO d FROM ai_trial_decisions WHERE decision_id = NEW.arm_decision_id;
    IF num_nonnulls(NEW.control_atr14, NEW.control_close, NEW.control_atr14_pct, NEW.control_stop_pct,
                    NEW.control_target_pct, NEW.control_invalidation_price, NEW.control_target_price,
                    NEW.control_stop_price, NEW.control_stop_atr_multiple, NEW.control_r_multiple) <> 10 THEN
        RAISE EXCEPTION 'a pair carries the control''s measurement, levels and derived figures';
    END IF;
    IF NOT coalesce(NEW.control_atr14 > 0 AND NEW.control_close > 0 AND NEW.control_atr14_pct > 0, false)
       OR NOT ai_trial_is_quantized(NEW.control_atr14_pct, 100 * NEW.control_atr14, NEW.control_close) THEN
        RAISE EXCEPTION 'control atr14_pct % is not a valid q(100 * atr14 / close)', NEW.control_atr14_pct;
    END IF;
    problem := ai_trial_plan_derivation_error(
        NEW.control_atr14, NEW.control_close, NEW.control_invalidation_price, NEW.control_target_price,
        NEW.control_stop_price, NEW.control_stop_atr_multiple, NEW.control_stop_pct,
        NEW.control_target_pct, NEW.control_r_multiple);
    IF problem IS NOT NULL THEN
        RAISE EXCEPTION 'control plan: %', problem;
    END IF;
    IF NOT ai_trial_plan_admissible(
        d.horizon_days, NEW.control_close, NEW.control_invalidation_price, NEW.control_target_price,
        NEW.control_stop_price, NEW.control_stop_atr_multiple, NEW.control_stop_pct,
        NEW.control_target_pct, NEW.control_r_multiple
    ) THEN
        RAISE EXCEPTION 'control plan for arm decision % does not pass §16.3 orders 8-12', d.decision_id;
    END IF;
    RETURN NEW;
END $$;
-- trg_ai_trial_pairs_verify_control_levels (sql/433) already executes this function.
