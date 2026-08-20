-- 354_survivorship_free_universe_records.sql
--
-- #2721 step 3 — the survivorship-free universe becomes producible, and its
-- results must be auditable: WHICH dead series were evaluated, and WHAT the
-- termination rule did to every position on a terminating series.
--
-- Two pieces:
--
-- 1. `strategy_result_universe.evaluated_series_ids` — a survivorship-free
--    run evaluates series that resolve to NO instrument (the dead names). The
--    engine keys them in-pass as `-series_id`, and that synthetic key must
--    never be stored as an instrument id — so the frozen record splits: real
--    instrument ids stay in `evaluated_instrument_ids`, and the unlinked
--    names' SERIES ids land here. Empty on every survivor-only record.
--
-- 2. `strategy_result_termination_census` — criterion 9 applied to the
--    survivorship treatment. One row per (result, stratum): how many
--    positions each termination class realised, how many opens the rule
--    declined to touch and why, and the universe-selection strata
--    (admitted / excluded counts) the acceptance reconciles back to the
--    vendor's series total. The ledger writer refuses to store a
--    survivorship_free result without its census; rows are immutable (no
--    UPDATE path exists in app code; the PK forbids re-insert).
--
-- The stratum vocabulary is CLOSED (the CHECK below): `terminated_<class>`
-- for the six `series_termination.TerminationClass` values,
-- `termination_skipped_<open_reason>` for the three open reasons the rule
-- deliberately does not touch, `termination_price_unlocatable`, and the five
-- `universe_*` selection strata. Free text cannot be counted (criterion 9),
-- so it cannot be stored.

ALTER TABLE strategy_result_universe
    ADD COLUMN IF NOT EXISTS evaluated_series_ids BIGINT[] NOT NULL DEFAULT '{}';

COMMENT ON COLUMN strategy_result_universe.evaluated_series_ids IS
    'research_price_series ids of evaluated names with NO instrument link '
    '(#2721 step 3 — the dead names). Empty on survivor-only records. The '
    'in-pass synthetic key -series_id is never stored; this column is its '
    'only persisted form.';

-- The prior backstop bounded two arrays at 20,000; the survivorship-free
-- corpus admits ~16.6k series of which ~12k are unlinked, so the same bound
-- covers the third array as well.
ALTER TABLE strategy_result_universe
    DROP CONSTRAINT IF EXISTS strategy_result_universe_size_backstop;
ALTER TABLE strategy_result_universe
    ADD CONSTRAINT strategy_result_universe_size_backstop CHECK (
        GREATEST(
            cardinality(evaluated_instrument_ids),
            cardinality(validated_universe_ids),
            cardinality(evaluated_series_ids)
        ) <= 20000
    );

CREATE TABLE IF NOT EXISTS strategy_result_termination_census (
    result_id  BIGINT NOT NULL
        REFERENCES strategy_results_store(result_id) ON DELETE RESTRICT,
    stratum    TEXT NOT NULL CHECK (stratum IN (
        'terminated_exchange_failure',
        'terminated_exchange_failure_a4',
        'terminated_operation_of_law',
        'terminated_linked_unparsed_provision',
        'terminated_q_suffix_otc_unverified',
        'terminated_unknown_termination',
        'termination_skipped_series_break',
        'termination_skipped_unresolved_outcome',
        'termination_skipped_close_bar_unfillable',
        'termination_price_unlocatable',
        'universe_admitted_total',
        'universe_unlinked_alive_excluded',
        'universe_linked_early_reuse_suspect',
        'universe_unharvested_excluded',
        'universe_vendor_series_total'
    )),
    count      BIGINT NOT NULL CHECK (count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (result_id, stratum)
);

COMMENT ON TABLE strategy_result_termination_census IS
    'Criterion 9 over the survivorship treatment (#2721 step 3): per result '
    'row, what the termination rule realised (terminated_<class>), what it '
    'deliberately declined (termination_skipped_<open_reason>, '
    'termination_price_unlocatable), and the universe-selection strata the '
    'acceptance reconciles to the vendor''s series total. Written in the '
    'result''s own transaction; a survivorship_free row without one is '
    'refused by the writer. Immutable — no UPDATE path exists.';
