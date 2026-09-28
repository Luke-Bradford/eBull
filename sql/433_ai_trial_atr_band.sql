-- 433_ai_trial_atr_band.sql
--
-- #3471 slice 2b-ATR — spec v5 (supervisor rule 2026-09-28 15:45Z): the ATR-relative stop band,
-- the reward/risk floor, and the control leg's levels derived from its OWN ATR.
-- Spec: docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md §6 "ATR band",
-- §7 "Shared terms" / "Pool (per decision)", §11 (v5 columns).
-- Writer: app/services/ai_trial_decision.py (measure_atr, decision_metrics, derive_control_levels).
--
-- ⚠ THE DATABASE NEVER DIVIDES. A `numeric` quotient keeps finitely many digits and a rational
-- quotient can sit arbitrarily close to a rounding tie, so a stored `x = q(n / d)` (4 decimals,
-- half up) is verified by the grid check `x = round(x, 4)` plus the exact multiplication
-- bracket `(x - 0.00005) * d <= n < (x + 0.00005) * d`, which admit exactly one x. Products are
-- recomputed exactly and compared with `round(., 4)` (half away from zero = half up here, all
-- values being positive). Triggers REFUSE; they never overwrite a supplied value.
--
-- ⚠ A stored DOUBLE is read as `x::text::numeric`, never `x::numeric`: on Postgres 17 the
-- direct cast rounds to 15 significant digits (`2.7100000000000004::float8::numeric` = 2.71),
-- the text cast keeps the shortest round-trip form that Python's `repr` gives.
--
-- The dev DB held 0 rows in every ai_trial_* table when this was written
-- (`SELECT count(*) FROM ai_trial_decisions` / `ai_trial_pairs`), so the new columns need no
-- backfill; they are nullable because ALTER TABLE validates existing rows, and the triggers
-- require them on every new row.


-- ---------------------------------------------------------------------------
-- 0. The quantization check: is x the 4-decimal half-up rounding of n / d?
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ai_trial_is_quantized(x NUMERIC, n NUMERIC, d NUMERIC)
RETURNS BOOLEAN LANGUAGE sql IMMUTABLE AS $$
    SELECT coalesce(
        d > 0 AND x = round(x, 4) AND (x - 0.00005) * d <= n AND n < (x + 0.00005) * d,
        false
    )
$$;


-- ---------------------------------------------------------------------------
-- 1. Decisions: the §6 recorded figures and the two new refusal codes
-- ---------------------------------------------------------------------------
ALTER TABLE ai_trial_decisions
    ADD COLUMN IF NOT EXISTS atr14             NUMERIC,
    ADD COLUMN IF NOT EXISTS close             NUMERIC,
    ADD COLUMN IF NOT EXISTS atr14_pct         NUMERIC,
    ADD COLUMN IF NOT EXISTS stop_atr_multiple NUMERIC,
    ADD COLUMN IF NOT EXISTS r_multiple        NUMERIC;

-- `target_not_above_stop` stays admissible (sql/432 vocabulary) but is no longer emitted (§6 v5).
ALTER TABLE ai_trial_decisions DROP CONSTRAINT IF EXISTS ai_trial_decisions_reason_code_check;
ALTER TABLE ai_trial_decisions ADD CONSTRAINT ai_trial_decisions_reason_code_check CHECK (reason_code IN (
    'not_in_shortlist', 'duplicate_symbol', 'already_held', 'target_not_above_stop',
    'stop_outside_atr_band', 'reward_risk_below_min', 'thesis_too_long', 'control_pool_exhausted'));

CREATE OR REPLACE FUNCTION ai_trial_decisions_verify_metrics()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    stop_n   NUMERIC := NEW.stop_pct::text::numeric;
    target_n NUMERIC := NEW.target_pct::text::numeric;
BEGIN
    IF NOT ai_trial_is_quantized(NEW.r_multiple, target_n, stop_n) THEN
        RAISE EXCEPTION 'decision r_multiple % is not q(target / stop)', NEW.r_multiple;
    END IF;
    IF num_nonnulls(NEW.atr14, NEW.close, NEW.atr14_pct, NEW.stop_atr_multiple) NOT IN (0, 4) THEN
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
        IF NOT ai_trial_is_quantized(NEW.stop_atr_multiple, stop_n, NEW.atr14_pct) THEN
            RAISE EXCEPTION 'decision stop_atr_multiple % is not q(stop / atr14_pct)', NEW.stop_atr_multiple;
        END IF;
    END IF;
    IF NEW.verdict = 'accepted' AND NOT coalesce(
        NEW.stop_atr_multiple BETWEEN 1 AND 4 AND NEW.r_multiple >= 1.5, false
    ) THEN
        RAISE EXCEPTION 'an accepted decision needs 1 <= stop_atr_multiple <= 4 and r_multiple >= 1.5';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ai_trial_decisions_verify_metrics ON ai_trial_decisions;
CREATE TRIGGER trg_ai_trial_decisions_verify_metrics
BEFORE INSERT ON ai_trial_decisions
FOR EACH ROW EXECUTE FUNCTION ai_trial_decisions_verify_metrics();


-- ---------------------------------------------------------------------------
-- 2. Pairs: the control's measurement and its derived, placeable levels (§7 v5)
-- ---------------------------------------------------------------------------
ALTER TABLE ai_trial_pairs
    ADD COLUMN IF NOT EXISTS control_atr14      NUMERIC,
    ADD COLUMN IF NOT EXISTS control_close      NUMERIC,
    ADD COLUMN IF NOT EXISTS control_atr14_pct  NUMERIC,
    ADD COLUMN IF NOT EXISTS control_stop_pct   NUMERIC,
    ADD COLUMN IF NOT EXISTS control_target_pct NUMERIC;

CREATE OR REPLACE FUNCTION ai_trial_pairs_verify_control_levels()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    d ai_trial_decisions%ROWTYPE;
BEGIN
    SELECT * INTO d FROM ai_trial_decisions WHERE decision_id = NEW.arm_decision_id;
    IF num_nonnulls(NEW.control_atr14, NEW.control_close, NEW.control_atr14_pct,
                    NEW.control_stop_pct, NEW.control_target_pct) <> 5 THEN
        RAISE EXCEPTION 'a pair carries the control''s measurement and derived levels';
    END IF;
    IF NOT coalesce(NEW.control_atr14 > 0 AND NEW.control_close > 0 AND NEW.control_atr14_pct > 0, false)
       OR NOT ai_trial_is_quantized(NEW.control_atr14_pct, 100 * NEW.control_atr14, NEW.control_close) THEN
        RAISE EXCEPTION 'control atr14_pct % is not a valid q(100 * atr14 / close)', NEW.control_atr14_pct;
    END IF;
    IF NEW.control_stop_pct IS DISTINCT FROM round(d.stop_atr_multiple * NEW.control_atr14_pct, 4)
       OR NEW.control_target_pct IS DISTINCT FROM round(d.r_multiple * NEW.control_stop_pct, 4) THEN
        RAISE EXCEPTION 'control levels %/% are not derived from arm decision %''s multiples',
            NEW.control_stop_pct, NEW.control_target_pct, d.decision_id;
    END IF;
    IF NOT coalesce(
        NEW.control_stop_pct BETWEEN 2 AND 25
        AND NEW.control_target_pct BETWEEN 2 AND 100
        AND NEW.control_atr14_pct <= NEW.control_stop_pct
        AND NEW.control_stop_pct <= 4 * NEW.control_atr14_pct
        AND NEW.control_target_pct >= 1.5 * NEW.control_stop_pct,
        false
    ) THEN
        RAISE EXCEPTION 'control levels %/% are not placeable', NEW.control_stop_pct, NEW.control_target_pct;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ai_trial_pairs_verify_control_levels ON ai_trial_pairs;
CREATE TRIGGER trg_ai_trial_pairs_verify_control_levels
BEFORE INSERT ON ai_trial_pairs
FOR EACH ROW EXECUTE FUNCTION ai_trial_pairs_verify_control_levels();
