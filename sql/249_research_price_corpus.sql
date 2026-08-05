-- 249_research_price_corpus.sql
--
-- #2282 stage 2a (phase 1 of #2240) — the RESEARCH corpus, and the
-- per-instrument coverage census that makes its survivorship auditable.
--
-- ⚠⚠ THIS IS NOT `price_daily`, AND MUST NEVER BE MERGED INTO IT.
-- `price_daily` is the eToro-sourced EXECUTION view: bid-derived CFD candles,
-- ~4 years deep (1,000-bar API cap, #603), survivors only. This is the
-- RESEARCH view: deep history, third-party provenance, delisted names retained
-- once the paid half lands. The two roles are separated deliberately — see
-- `docs/proposals/ta/strategy-catalogue-and-backtest-validity.md` §0 and
-- `.claude/skills/data-sources/research-price-corpus.md`. Merging them would
-- put an unspecified-licence Yahoo derivative into the table the order path
-- reads, which is both a provenance failure and a correctness one (the two
-- series differ by a consistent half-spread, −0.14% to −0.22%).
--
--
-- WHY THE SERIES IS NOT KEYED ON instrument_id
-- ---------------------------------------------------------------------------
-- The obvious schema is `(instrument_id, bar_date)`, mirroring `price_daily`.
-- It is wrong here, and wrong in the direction that destroys the evidence.
--
-- The corpus is keyed on the VENDOR'S symbol, and a large share of vendor
-- symbols have no `instruments` row at all — the HF archive carries 7,693
-- symbols against our 6,733 tradable US stocks, and the overlap is partial in
-- both directions. Those unmatched symbols are not junk to be dropped: they
-- are companies that were listed and are not on eToro's book today, i.e. they
-- are the direct measure of **eToro-listing bias** (§4.0), a survivorship-
-- shaped bias that #2284 did NOT measure and that buying a delisted corpus
-- does NOT fix. An `instrument_id`-keyed table discards exactly that
-- population at ingest and then cannot answer the question it was built for.
--
-- So: series identity is `(vendor, vendor_symbol)`. `instrument_id` is a
-- NULLABLE resolution recorded with its method. **Unresolved is data, not an
-- error.**
--
--
-- WHY first_bar / last_bar / bar_count LIVE ON THE SERIES ROW
-- ---------------------------------------------------------------------------
-- This is the gap #2282 was filed to close. `instrument_price_supply`
-- (sql/248) is a FRESHNESS tracker — `last_known_bar`,
-- `consecutive_no_advance`, `last_attempt_at` — and carries neither a first
-- bar nor a bar count. So "is this corpus survivor-biased?" was a 40-minute
-- investigation during the #2284 spike, where `lse-data`'s catalog answered
-- the same question in ONE call because it returns first/last/ticks per
-- symbol.
--
-- Maintaining them at ingest rather than as a view over `research_price_daily`
-- is deliberate: the archive is ~25.8M rows, and a census that requires a full
-- scan is a census nobody runs. Denormalised, the census is an aggregate over
-- ~7,693 narrow rows.
--
-- ⚠ The consequence is that they are DERIVED STATE and can drift from the
-- bars. `research_series_census_drift` below is the reconciliation query; run
-- it after any bulk load. Do not add a trigger — a per-row trigger on a 25.8M
-- row COPY is the wrong trade, and the ingest owns the maintenance.

-- ---------------------------------------------------------------------------
-- Series identity + provenance
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS research_price_series (
    series_id       BIGSERIAL PRIMARY KEY,

    -- Identity. The vendor's own symbol, verbatim — NOT normalised to ours.
    vendor          TEXT NOT NULL,
    vendor_symbol   TEXT NOT NULL,

    -- Provenance, recorded honestly. The HF archive is a Yahoo derivative
    -- whose dataset card licence field reads "other"; #2284's close-out is
    -- explicit that laundering that into "public data" is the failure the
    -- prevention log names. `upstream_source` exists so a derivative can never
    -- be mistaken for an independent source: two vendors that both resolve to
    -- 'yahoo' are ONE observation, not two, and any cross-source agreement
    -- between them is circular.
    upstream_source TEXT NOT NULL
        CHECK (upstream_source IN
            ('yahoo', 'yahoo_derivative', 'firstratedata',
             'historicaldata_net', 'etoro', 'other', 'unknown')),
    licence         TEXT NOT NULL,

    -- Adjustment basis. 'unknown' is a legitimate and common value and must
    -- NOT default to an assumption: an unadjusted series with unknown splits
    -- is not a usable TA input (§0.1(c2)), and silently treating one as
    -- adjusted is how a corpus poisons every downstream indicator.
    adjustment_basis TEXT NOT NULL
        CHECK (adjustment_basis IN
            ('unadjusted', 'split_adjusted',
             'split_and_dividend_adjusted', 'unknown')),

    -- Resolution to our universe. NULL instrument_id = "this series is a
    -- company we do not carry", which is a MEASUREMENT, not a defect.
    instrument_id   BIGINT REFERENCES instruments(instrument_id),
    resolution_method TEXT
        CHECK (resolution_method IN ('symbol_exact', 'cik', 'manual')),

    -- Census columns — maintained by the ingest, reconciled by the drift
    -- query below. NULL across all three = series row exists, no bars loaded.
    -- ⚠ `bar_count = 0` is FORBIDDEN, not merely discouraged (see the
    -- all-or-nothing constraint below). It would be a SECOND spelling of "no
    -- bars", and two spellings of one state is how an audit metric goes quietly
    -- wrong: `research_corpus_census.series_without_bars` counts
    -- `bar_count IS NULL`, so a zero-valued row would vanish from it while the
    -- drift view also considered it reconciled. Forbidding the ambiguity is
    -- cheaper than teaching both views about it.
    first_bar       DATE,
    last_bar        DATE,
    bar_count       INTEGER,

    -- Delisting, where known. Populated from the Form 25 register (#2282 2c)
    -- for US names and from #2290's forward record for non-US.
    -- ⚠ This is the SUSPENSION date — the last tradable day — not the filing
    -- date and not the removal-effective date. A Form 25 carries three
    -- distinct dates plus master.idx's `filed`; picking the wrong one
    -- mistruncates every series it touches. See sec-edgar.md §2.6 trap 5.
    delisting_date  DATE,
    delisting_source TEXT
        CHECK (delisting_source IN ('sec_form25', 'universe_membership', 'vendor')),

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT research_price_series_vendor_symbol_uq
        UNIQUE (vendor, vendor_symbol),
    -- Ordered range, same invariant shape as instrument_symbol_history (103).
    CONSTRAINT research_price_series_bars_ordered
        CHECK (first_bar IS NULL OR last_bar IS NULL OR last_bar >= first_bar),
    -- The census is all-or-nothing, and a populated census means >= 1 bar.
    -- This is what makes "no bars" have exactly ONE representation.
    CONSTRAINT research_price_series_census_all_or_nothing
        CHECK (
            (first_bar IS NULL AND last_bar IS NULL AND bar_count IS NULL)
            OR (first_bar IS NOT NULL AND last_bar IS NOT NULL
                AND bar_count IS NOT NULL AND bar_count > 0)
        ),
    -- A resolution without its method is an unauditable join — the exact
    -- failure class the CUSIP/CIK identity work already guards against.
    CONSTRAINT research_price_series_resolution_evidenced
        CHECK ((instrument_id IS NULL) = (resolution_method IS NULL)),
    CONSTRAINT research_price_series_delisting_evidenced
        CHECK ((delisting_date IS NULL) = (delisting_source IS NULL))
);

-- Resolution is many-to-one at most: two vendor symbols may resolve to the
-- same instrument only if they come from different vendors. Within one
-- vendor, two symbols resolving to one instrument means the resolver has
-- attached a ticker-reuse pair to a single company — the `SI` / Silvergate
-- failure mode, where a 2025 series is welded onto a company that died in
-- 2023.
CREATE UNIQUE INDEX IF NOT EXISTS uq_research_price_series_vendor_instrument
    ON research_price_series (vendor, instrument_id)
    WHERE instrument_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_research_price_series_instrument
    ON research_price_series (instrument_id)
    WHERE instrument_id IS NOT NULL;

COMMENT ON TABLE research_price_series IS
    'Research-corpus series identity, keyed on (vendor, vendor_symbol) NOT on '
    'instrument_id — unmatched vendor symbols are the measure of eToro-listing '
    'bias and must not be dropped at ingest. Carries the coverage census '
    '(first_bar/last_bar/bar_count) that instrument_price_supply (sql/248) '
    'lacks, plus provenance and the Form 25 SUSPENSION date.';

COMMENT ON COLUMN research_price_series.upstream_source IS
    'The ORIGINAL source, not the immediate vendor. Two vendors resolving to '
    'yahoo are one observation — cross-source agreement between them is '
    'circular. The HF archive is yahoo_derivative (29/29 identical first-bar '
    'dates vs Yahoo, including Yahoo artefacts).';

COMMENT ON COLUMN research_price_series.delisting_date IS
    'SUSPENSION date — the last tradable day. NOT the Form 25 filing date and '
    'NOT the removal-effective date; a Form 25 carries three distinct dates '
    '(sec-edgar.md 2.6 trap 5). Using the wrong one mistruncates the series.';

-- ---------------------------------------------------------------------------
-- Bars
-- ---------------------------------------------------------------------------
--
-- Deliberately NOT carrying the indicator columns price_daily has (sma_50,
-- rsi_14, macd_*, ...). vectorbt/TA-Lib compute indicators over a series
-- directly, so persisting an indicator history for the research corpus is
-- storage and drift for no read — see the spec's §8 phase-2 note. Bars only.

CREATE TABLE IF NOT EXISTS research_price_daily (
    series_id   BIGINT NOT NULL
        REFERENCES research_price_series(series_id) ON DELETE CASCADE,
    bar_date    DATE NOT NULL,
    open        NUMERIC,
    high        NUMERIC,
    low         NUMERIC,
    close       NUMERIC NOT NULL,
    volume      BIGINT,
    PRIMARY KEY (series_id, bar_date)
);

COMMENT ON TABLE research_price_daily IS
    'Research-corpus bars, keyed on series_id. No indicator columns by design '
    '(vectorbt computes them over the series); no instrument_id by design '
    '(see research_price_series).';

-- ---------------------------------------------------------------------------
-- The census — acceptance item 1 of #2282
-- ---------------------------------------------------------------------------
--
-- Reports per asset class: instruments, first bar, last bar, bar count, and
-- the count carrying a delisting record. Unresolved series are reported under
-- a '(unresolved)' class rather than dropped — that row IS the eToro-listing
-- bias measurement, and a census that hid it would defeat its own purpose.
--
-- ⚠ Per #2289/§4.0 the VALIDATED universe is `us_equity` AND
-- instrument_type_id = 5 (US stocks ex-ETF, 6,733). The census reports by
-- asset class alone, so a us_equity row here still mixes 555 ETFs in. Consumers
-- gating on validation must apply the type filter themselves; this view is
-- deliberately the wider picture.

CREATE OR REPLACE VIEW research_corpus_census AS
SELECT
    COALESCE(e.asset_class, CASE WHEN s.instrument_id IS NULL
                                 THEN '(unresolved)' ELSE '(unmapped)' END)
                                                       AS asset_class,
    s.vendor,
    count(*)                                           AS series,
    count(s.instrument_id)                             AS resolved_series,
    min(s.first_bar)                                   AS earliest_first_bar,
    max(s.last_bar)                                    AS latest_last_bar,
    sum(s.bar_count)                                   AS bars,
    count(*) FILTER (WHERE s.bar_count IS NULL)        AS series_without_bars,
    count(s.delisting_date)                            AS series_with_delisting
FROM research_price_series s
LEFT JOIN instruments i ON i.instrument_id = s.instrument_id
-- Both sides are TEXT; no cast. Matches the five existing call sites in app/
-- (instruments.py, calendar.py). An earlier draft carried `e.exchange_id::text`,
-- which is a no-op that reads as a papered-over type mismatch.
LEFT JOIN exchanges   e ON e.exchange_id = i.exchange
GROUP BY 1, 2;

COMMENT ON VIEW research_corpus_census IS
    'Per asset-class corpus coverage: series, resolved series, first/last bar, '
    'bar count, series carrying a delisting record. The (unresolved) row is '
    'the eToro-listing-bias measurement and is reported, never filtered out.';

-- Reconciliation for the denormalised census columns. The ingest maintains
-- them; this is how you prove it did. Expected to return zero rows — any row
-- is a series whose stored census disagrees with its bars.
CREATE OR REPLACE VIEW research_series_census_drift AS
SELECT
    s.series_id,
    s.vendor,
    s.vendor_symbol,
    s.first_bar   AS stored_first_bar,
    b.first_bar   AS actual_first_bar,
    s.last_bar    AS stored_last_bar,
    b.last_bar    AS actual_last_bar,
    s.bar_count   AS stored_bar_count,
    b.bar_count   AS actual_bar_count
FROM research_price_series s
LEFT JOIN (
    SELECT series_id,
           min(bar_date)  AS first_bar,
           max(bar_date)  AS last_bar,
           count(*)::int  AS bar_count
    FROM research_price_daily
    GROUP BY series_id
) b ON b.series_id = s.series_id
-- ⚠ No COALESCE on bar_count. An earlier draft wrote
-- `COALESCE(s.bar_count, 0) IS DISTINCT FROM COALESCE(b.bar_count, 0)`, which
-- silently reconciled a series storing `bar_count = 0` against a series with no
-- bar rows at all — a row that `series_without_bars` (keyed on IS NULL) also
-- missed, so it was absent from both audits at once. The all-or-nothing
-- constraint above now makes `bar_count = 0` unrepresentable, so a plain
-- IS DISTINCT FROM is both correct and sufficient: NULL vs NULL reconciles,
-- NULL vs a real count drifts.
WHERE s.first_bar IS DISTINCT FROM b.first_bar
   OR s.last_bar  IS DISTINCT FROM b.last_bar
   OR s.bar_count IS DISTINCT FROM b.bar_count;

COMMENT ON VIEW research_series_census_drift IS
    'Reconciles the denormalised census columns against the actual bars. '
    'Expected empty; run after any bulk load. Deliberately a view and not a '
    'trigger — a per-row trigger on a 25.8M-row COPY is the wrong trade.';
