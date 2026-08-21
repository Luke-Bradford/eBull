-- #2811 — the decision dates a strategy version may act on, as the SCAN computed
-- them.
--
-- The card cannot read a periodic strategy's zero without knowing whether the
-- scan ever reached one of its decision dates. Recomputing the calendar at read
-- time resolves a historical version against TODAY's universe, corpus and
-- quarantine state, which is not what that version's census rows were written
-- under -- the #2809 shape, where the card recomputed a lookalike of a producer
-- statistic and disagreed with it. It also costs 0.61s against an endpoint that
-- answers in 75-146ms. So the producer publishes what it used.
--
-- ⚠ KEYED ON strategy_version, NOT strategy_id ALONE. The rebalance rule is part
-- of the strategy's identity and #2797 changed S-2's, so one calendar per
-- strategy would pool two rules under one name.
--
-- ⚠ NOT a signal, an observation or a promotion input. It is a republication of
-- a pure function of the union calendar, so it carries no verdict, no
-- instrument and no fill, and it is safe to REPLACE wholesale on every scan.

-- ⚠⚠ THE HEADER EXISTS SO THAT "KNOWN, AND EMPTY" IS REPRESENTABLE. Without it a
-- strategy whose rule names no date in this corpus stores zero rows -- which is
-- byte-identical to a strategy that never published at all, and the reader's whole
-- contract rests on telling those apart (`decision_days = 0` means "it had chances
-- and the scan reached none"; `NULL` means "no calendar is known"). A row count
-- cannot carry a distinction whose two sides are both zero rows. Found by Codex at
-- checkpoint 2, against a first cut that documented the distinction and then
-- stored it in the one place that cannot hold it.
CREATE TABLE IF NOT EXISTS strategy_decision_calendar_publications (
    strategy_id TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    -- The corpus state this calendar describes: the frontier of the scan that
    -- computed it. A reader comparing this against the version's census can see
    -- that the two describe different corpora -- see `load_fire_rate`, which warns
    -- rather than silently mixing them.
    frontier_date DATE NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (strategy_id, strategy_version)
);

CREATE TABLE IF NOT EXISTS strategy_decision_calendar (
    strategy_id TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    decision_date DATE NOT NULL,
    PRIMARY KEY (strategy_id, strategy_version, decision_date),
    FOREIGN KEY (strategy_id, strategy_version)
        REFERENCES strategy_decision_calendar_publications (strategy_id, strategy_version)
        ON DELETE CASCADE
);

-- The reader's access path: every date for the versions currently on the card.
CREATE INDEX IF NOT EXISTS strategy_decision_calendar_version_idx
    ON strategy_decision_calendar (strategy_version, decision_date);
