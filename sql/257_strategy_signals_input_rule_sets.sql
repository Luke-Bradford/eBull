-- 257_strategy_signals_input_rule_sets.sql
--
-- #2333 — the signal ledger's producer identity did not cover the indicator
-- rule set. Registry: app/services/strategy_registry.py (INPUT_RULE_SETS).
-- Table: sql/255_strategy_signals.sql. Writer: app/services/signal_ledger.py.
--
--
-- WHAT WAS WRONG
-- ---------------------------------------------------------------------------
-- `strategy_version` hashes the strategy's own module, params, universe and
-- cost model. It did NOT hash `indicator_series.RULE_SET_VERSION`, and a
-- strategy is its indicators: S-1 is `sma_series(fast) > sma_series(slow)` and
-- has no other content. So an edit to how the SMA, RSI, ATR, MACD, Bollinger
-- or the stochastic is COMPUTED produced different signals under an UNCHANGED
-- `strategy_version` — and the uniqueness key
-- (strategy_id, strategy_version, instrument_id, signal_bar_date, signal_kind)
-- then treats the old and the new row as the same row.
--
-- Parent criterion 11: "the same parameters with changed filter logic … is a
-- different strategy. The identity hash covers all of them." An indicator
-- definition IS the filter logic.
--
-- Fixed in the HASH (strategy_registry.INPUT_RULE_SETS is now part of the
-- version payload), which is what makes the corrected row storable. This
-- column is the QUERYABLE half.
--
--
-- WHY A COLUMN AS WELL AS THE HASH
-- ---------------------------------------------------------------------------
-- The hash alone is sufficient for correctness: a changed indicator rule set
-- moves `strategy_version`, so the new row has a new key and nothing collides.
-- The column answers a question the hash cannot — "which indicator rule set
-- produced these rows" — because a 12-hex digest of a JSON payload does not
-- decompose.
--
-- Two precedents in this same schema point the same way: `universe` is stored
-- as a column while living inside `strategy_version` ("for querying and
-- labelling, never for identity"), and sql/256 stores `input_rule_set_version`
-- for the outcome ledger's analogous input.
--
-- ⚠ It is added NOW because it is free now and impossible later:
-- `select count(*), count(distinct strategy_version) from strategy_signals`
-- returns (0, 0) as of 2026-08-06, so there is nothing to backfill. Once rows
-- exist, the indicator version that produced them is recoverable only by
-- guessing which historical module source hashes to the digest inside
-- `strategy_version` — i.e. not recoverable.
--
-- ⚠ NOT NULL with NO DEFAULT, so this statement FAILS LOUDLY on a non-empty
-- table rather than inventing an input version for rows whose real one is
-- unknown. Same reasoning as `universe` in sql/255: a column with a default is
-- a column a writer can forget.
--
--
-- ⚠ DELIBERATELY NOT IN THE UNIQUENESS KEY
-- ---------------------------------------------------------------------------
-- It is INSIDE `strategy_version`, exactly as `universe` is. Adding it to the
-- key would permit one strategy identity to span two indicator rule sets,
-- which criterion 11 says is not one strategy — the same error sql/255's
-- header rejects for `universe`.
--
-- ⚠ This is the one structural difference from sql/256, where
-- `input_rule_set_version` IS a key member. There the input version is NOT
-- inside the resolver's `rule_set_version` (a source hash of one file), so
-- without it in the key the corrected outcome is UNSTORABLE. Here it is inside
-- the hash, so the corrected signal already has a distinct key. Two different
-- answers because the two hashes cover different things, not because one of
-- them is wrong.
--
--
-- ⚠ WHAT THE CHECK PROVES AND WHAT IT DOES NOT
-- ---------------------------------------------------------------------------
-- It proves the value is a non-empty JSON OBJECT whose every value is a
-- non-blank STRING. The blank-value half matters: NOT NULL does not catch
-- `{"indicator_series": ""}`, which is the #2286 shape — present, typed,
-- recording nothing — and sql/256 rejects the same state for its TEXT column.
--
-- ⚠ "Non-blank", not "non-empty": the jsonpath is `^[[:space:]]*$`, so
-- `{"indicator_series": "  "}` is rejected too. sql/256's `<> ''` admits a
-- whitespace version; this is deliberately one notch stricter, because the
-- Python mirror in `signal_ledger.LedgerRow` uses `.strip()` and a mirror is
-- only worth having if the two agree exactly.
--
-- It does NOT prove the keys name real modules, that the versions were the
-- ones actually used to compute the bars, or that the set is complete. Those
-- are the writer's, and the writer takes the mapping from
-- `StrategyIdentity.input_rule_set_versions` — the same object the hash is
-- built from — so the column cannot disagree with the version beside it.
--
-- `jsonb_path_exists` is IMMUTABLE (`pg_proc.provolatile = 'i'`, verified on
-- the dev cluster 2026-08-06) and so is legal in a CHECK. A subquery is not,
-- which is why the blank test is a jsonpath rather than `jsonb_each_text`.

ALTER TABLE strategy_signals
    ADD COLUMN IF NOT EXISTS input_rule_set_versions JSONB NOT NULL;

ALTER TABLE strategy_signals
    DROP CONSTRAINT IF EXISTS strategy_signals_input_rule_sets_shape;

ALTER TABLE strategy_signals
    ADD CONSTRAINT strategy_signals_input_rule_sets_shape
        CHECK (
            jsonb_typeof(input_rule_set_versions) = 'object'
            AND input_rule_set_versions <> '{}'::jsonb
            AND NOT jsonb_path_exists(input_rule_set_versions, '$.* ? (@.type() != "string")')
            AND NOT jsonb_path_exists(
                input_rule_set_versions,
                '$.* ? (@.type() == "string" && @ like_regex "^[[:space:]]*$")'
            )
        );

COMMENT ON COLUMN strategy_signals.input_rule_set_versions IS
    'The RULE_SET_VERSION of every pipeline whose output the strategy read, '
    'keyed by module — today {"indicator_series": …}. Also hashed INTO '
    'strategy_version (#2333), so it is not key material here: it is the '
    'queryable form of an identity component, exactly like `universe`. A '
    'changed indicator rule set is a different strategy under criterion 11, '
    'because a strategy IS its indicators.';
