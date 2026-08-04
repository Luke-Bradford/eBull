-- #2262 (phase 0a of #2240) — "eligible but supply-less" marker.
--
-- THE PROBLEM THIS EXISTS FOR. ~108 instruments are price-eligible (per design
-- decision 9) and eToro serves nothing for them: 81 PRICED instruments that get
-- fetched and never advance (78 us_equity — delisted/acquired names, oldest last
-- bar 2021-05-21 — plus 2 crypto and 1 fx), and 27 unpriced gate-passers that
-- return no bars at all.
--
-- ⚠⚠ eToro returns HTTP 200 WITH NOTHING NEW for these. It does not error, does
-- not 404, does not raise. A marker keyed on HTTP status or on an exception
-- NEVER FIRES for any of them — which is why this is a table and not a
-- try/except. The only observable is: a fetch was ATTEMPTED, and the series did
-- not advance, N consecutive times.
--
-- ⚠ It must cover PRICED instruments. S6's probe could only see the 27 because
-- it sampled the unpriced set; the 78 priced ones were invisible to it. Keying
-- this on "did MAX(price_date) move" rather than "does the instrument have bars"
-- is what makes the priced ones visible.
--
-- Until it existed, all 108 were indistinguishable from "not yet refreshed",
-- were re-probed nightly forever, and every coverage number counted them as
-- refreshable.

CREATE TABLE IF NOT EXISTS instrument_price_supply (
    instrument_id           BIGINT NOT NULL PRIMARY KEY REFERENCES instruments(instrument_id),

    -- Consecutive ATTEMPTED fetches after which the series had not moved.
    -- Reset to 0 the moment it does. An errored fetch is NEUTRAL — it leaves
    -- this untouched, because "the provider was unreachable" is a different
    -- signal from "the provider answered and had nothing".
    consecutive_no_advance  INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_no_advance >= 0),

    last_attempt_at         TIMESTAMPTZ NOT NULL,
    last_advance_at         TIMESTAMPTZ,
    -- MAX(price_date) as at the last attempt. NULL = the instrument has never
    -- had a bar, which is the 27-instrument case.
    last_known_bar          DATE,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Read pattern: "which eligible instruments look supply-less?" and the T3 scope
-- query's per-instrument lookup.
CREATE INDEX IF NOT EXISTS idx_instrument_price_supply_no_advance
    ON instrument_price_supply (consecutive_no_advance DESC, last_attempt_at);
