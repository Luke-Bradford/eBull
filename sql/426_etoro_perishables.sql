-- 426_etoro_perishables.sql
--
-- #3381 slice 3 — a forward-only daily record of the trading conditions eToro serves no history
-- for: instrument rates (bid/ask), account eligibility incl. x1 short availability, and what-if
-- open costs. Spec: docs/proposals/etl/2026-09-25-3381-perishables-recorder.md.
--
-- One `etoro_perishable_snapshots` row per run, failed ones included. Every HTTP request is one
-- `etoro_perishable_requests` row carrying the body as served; the observation tables are thin
-- projections of those bodies and FK back to the request they came from. Everything a run
-- collected commits in ONE transaction with its header.
--
-- Clocks: `observed_at` = receipt of the response a row came from; `quote_at` / `last_updated`
-- are the provider's own (possibly stale) clocks; a snapshot's knowledge time is `finished_at`.
-- `instrument_id` carries no FK: the sticky universe keeps ids the catalogue has dropped.
--
-- The only writer is the `etoro_perishables_snapshot` job (app/services/perishables_recorder.py).
--
-- ⚠ Every table refuses UPDATE (sql/424's `etoro_crowd_append_only`). DELETE stays possible for
-- the test harness's cleanup, as sql/424 and sql/425.

CREATE TABLE IF NOT EXISTS etoro_perishable_snapshots (
    snapshot_id            BIGSERIAL   PRIMARY KEY,
    started_at             TIMESTAMPTZ NOT NULL,
    finished_at            TIMESTAMPTZ NOT NULL CHECK (finished_at >= started_at),
    status                 TEXT        NOT NULL CHECK (status IN ('complete', 'partial', 'failed')),
    recorder_version       TEXT        NOT NULL,
    request_params         JSONB       NOT NULL,
    universe_size          INTEGER     CHECK (universe_size >= 0),
    -- The investor snapshot the panel additions were read from; NULL when none qualified.
    whatif_panel_source_id BIGINT      REFERENCES etoro_investor_snapshots(snapshot_id),
    whatif_panel_added     INTEGER     CHECK (whatif_panel_added >= 0),
    whatif_panel_refused   INTEGER     CHECK (whatif_panel_refused >= 0),
    -- Per phase: requests planned / answered ok / errored. NULL until the phase was planned.
    eligibility_expected   INTEGER     CHECK (eligibility_expected >= 0),
    eligibility_ok         INTEGER     CHECK (eligibility_ok >= 0),
    eligibility_errored    INTEGER     CHECK (eligibility_errored >= 0),
    whatif_expected        INTEGER     CHECK (whatif_expected >= 0),
    whatif_ok              INTEGER     CHECK (whatif_ok >= 0),
    whatif_errored         INTEGER     CHECK (whatif_errored >= 0),
    rates_expected         INTEGER     CHECK (rates_expected >= 0),
    rates_ok               INTEGER     CHECK (rates_ok >= 0),
    rates_errored          INTEGER     CHECK (rates_errored >= 0),
    error                  TEXT,
    CHECK ((status = 'failed') = (error IS NOT NULL)),
    CHECK (
        (eligibility_expected IS NULL) = (eligibility_ok IS NULL)
        AND (eligibility_expected IS NULL) = (eligibility_errored IS NULL)
        AND (whatif_expected IS NULL) = (whatif_ok IS NULL)
        AND (whatif_expected IS NULL) = (whatif_errored IS NULL)
        AND (rates_expected IS NULL) = (rates_ok IS NULL)
        AND (rates_expected IS NULL) = (rates_errored IS NULL)
    ),
    CHECK (eligibility_expected IS NULL OR eligibility_ok + eligibility_errored <= eligibility_expected),
    CHECK (whatif_expected IS NULL OR whatif_ok + whatif_errored <= whatif_expected),
    CHECK (rates_expected IS NULL OR rates_ok + rates_errored <= rates_expected),
    -- complete: every phase planned and every request answered ok.
    CHECK (
        status <> 'complete'
        OR (
            eligibility_ok = eligibility_expected
            AND whatif_ok = whatif_expected
            AND rates_ok = rates_expected
        )
    ),
    -- partial: every phase ran to its end, and something errored.
    CHECK (
        status <> 'partial'
        OR (
            eligibility_ok + eligibility_errored = eligibility_expected
            AND whatif_ok + whatif_errored = whatif_expected
            AND rates_ok + rates_errored = rates_expected
            AND eligibility_errored + whatif_errored + rates_errored > 0
        )
    )
);

CREATE INDEX IF NOT EXISTS etoro_perishable_snapshots_started_at_idx ON etoro_perishable_snapshots (started_at);

-- Sticky universe: an instrument once asked about is asked about every run after.
CREATE TABLE IF NOT EXISTS etoro_perishable_universe (
    instrument_id     BIGINT PRIMARY KEY CHECK (instrument_id > 0),
    first_snapshot_id BIGINT NOT NULL REFERENCES etoro_perishable_snapshots(snapshot_id)
);

-- Sticky what-if panel: added from an investor snapshot's holdings, never evicted.
CREATE TABLE IF NOT EXISTS etoro_whatif_panel (
    instrument_id               BIGINT  PRIMARY KEY CHECK (instrument_id > 0),
    first_snapshot_id           BIGINT  NOT NULL REFERENCES etoro_perishable_snapshots(snapshot_id),
    source_investor_snapshot_id BIGINT  NOT NULL REFERENCES etoro_investor_snapshots(snapshot_id),
    -- Distinct cohort holders on each side that selected it; NULL for a side it did not rank on.
    long_holders                INTEGER CHECK (long_holders >= 1),
    short_holders               INTEGER CHECK (short_holders >= 1),
    CHECK (long_holders IS NOT NULL OR short_holders IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS etoro_perishable_requests (
    request_id     BIGSERIAL   PRIMARY KEY,
    snapshot_id    BIGINT      NOT NULL REFERENCES etoro_perishable_snapshots(snapshot_id),
    phase          TEXT        NOT NULL CHECK (phase IN ('eligibility', 'whatif', 'rates')),
    seq            INTEGER     NOT NULL CHECK (seq >= 0),
    instrument_ids BIGINT[]    NOT NULL CHECK (cardinality(instrument_ids) >= 1),
    request_body   JSONB       NOT NULL,
    observed_at    TIMESTAMPTZ NOT NULL,
    outcome        TEXT        NOT NULL CHECK (outcome IN ('ok', 'error')),
    -- Any response received, a malformed 200 included; NULL only for a transport error.
    http_status    INTEGER,
    -- The decoded body in full; a non-JSON body as a JSON string; a transport error's text.
    raw            JSONB       NOT NULL,
    CHECK (outcome = 'error' OR http_status BETWEEN 200 AND 299),
    UNIQUE (snapshot_id, phase, seq),
    UNIQUE (request_id, snapshot_id, phase)
);

CREATE TABLE IF NOT EXISTS etoro_rate_observations (
    snapshot_id         BIGINT      NOT NULL,
    instrument_id       BIGINT      NOT NULL CHECK (instrument_id > 0),
    request_id          BIGINT      NOT NULL,
    phase               TEXT        NOT NULL DEFAULT 'rates' CHECK (phase = 'rates'),
    observed_at         TIMESTAMPTZ NOT NULL,
    -- Published units as served; NULL when absent or not a number. No validity filter.
    quote_at            TIMESTAMPTZ,
    bid                 NUMERIC,
    ask                 NUMERIC,
    last_execution      NUMERIC,
    conversion_rate_bid NUMERIC,
    conversion_rate_ask NUMERIC,
    PRIMARY KEY (snapshot_id, instrument_id),
    FOREIGN KEY (request_id, snapshot_id, phase)
        REFERENCES etoro_perishable_requests (request_id, snapshot_id, phase)
);

CREATE INDEX IF NOT EXISTS etoro_rate_observations_instrument_idx
    ON etoro_rate_observations (instrument_id, observed_at);

CREATE TABLE IF NOT EXISTS etoro_eligibility_observations (
    snapshot_id           BIGINT      NOT NULL,
    instrument_id         BIGINT      NOT NULL CHECK (instrument_id > 0),
    request_id            BIGINT      NOT NULL,
    phase                 TEXT        NOT NULL DEFAULT 'eligibility' CHECK (phase = 'eligibility'),
    observed_at           TIMESTAMPTZ NOT NULL,
    answer                TEXT        NOT NULL CHECK (answer IN ('found', 'not_found')),
    -- NULL when missing or malformed, never defaulted to false. All NULL for 'not_found'.
    allow_open_position   BOOLEAN,
    allow_close_position  BOOLEAN,
    min_position_exposure NUMERIC,
    max_units_per_order   NUMERIC,
    -- Local projections of `leverageConfigs` (spec §"Local projections").
    long_x1               TEXT        CHECK (long_x1 IN ('available', 'potential', 'absent')),
    short_x1              TEXT        CHECK (short_x1 IN ('available', 'potential', 'absent')),
    long_x1_settlement    TEXT,
    max_short_leverage    INTEGER,
    CHECK (
        answer = 'found'
        OR (
            allow_open_position IS NULL AND allow_close_position IS NULL AND min_position_exposure IS NULL
            AND max_units_per_order IS NULL AND long_x1 IS NULL AND short_x1 IS NULL
            AND long_x1_settlement IS NULL AND max_short_leverage IS NULL
        )
    ),
    CHECK ((long_x1_settlement IS NOT NULL) = (long_x1 IS NOT DISTINCT FROM 'available')),
    PRIMARY KEY (snapshot_id, instrument_id),
    FOREIGN KEY (request_id, snapshot_id, phase)
        REFERENCES etoro_perishable_requests (request_id, snapshot_id, phase)
);

CREATE INDEX IF NOT EXISTS etoro_eligibility_observations_instrument_idx
    ON etoro_eligibility_observations (instrument_id, observed_at);

CREATE TABLE IF NOT EXISTS etoro_whatif_observations (
    snapshot_id            BIGINT      NOT NULL REFERENCES etoro_perishable_snapshots(snapshot_id),
    instrument_id          BIGINT      NOT NULL CHECK (instrument_id > 0),
    arm                    TEXT        NOT NULL CHECK (arm IN ('long', 'short')),
    outcome                TEXT        NOT NULL CHECK (
        outcome IN ('ok', 'error', 'unattempted', 'not_offered', 'not_eligible', 'undecided')
    ),
    -- The eligibility answer the arm decision read; NULL only when there was none (an omitted id or
    -- an errored request), which is always 'undecided'.
    eligibility_request_id BIGINT      REFERENCES etoro_perishable_requests(request_id),
    request_id             BIGINT,
    phase                  TEXT        NOT NULL DEFAULT 'whatif' CHECK (phase = 'whatif'),
    -- The planned order; NULL unless the arm was planned (ok / error / unattempted).
    transaction            TEXT,
    settlement_type        TEXT,
    amount_usd             NUMERIC,
    leverage               INTEGER,
    -- Provider `lastUpdated` of an ok response; NULL if absent or unparseable.
    last_updated           TIMESTAMPTZ,
    CHECK ((request_id IS NOT NULL) = (outcome IN ('ok', 'error'))),
    CHECK ((transaction IS NOT NULL) = (outcome IN ('ok', 'error', 'unattempted'))),
    CHECK ((transaction IS NULL) = (settlement_type IS NULL)),
    CHECK ((transaction IS NULL) = (amount_usd IS NULL)),
    CHECK ((transaction IS NULL) = (leverage IS NULL)),
    CHECK (outcome = 'ok' OR last_updated IS NULL),
    CHECK (eligibility_request_id IS NOT NULL OR outcome = 'undecided'),
    PRIMARY KEY (snapshot_id, instrument_id, arm),
    FOREIGN KEY (request_id, snapshot_id, phase)
        REFERENCES etoro_perishable_requests (request_id, snapshot_id, phase)
);

DROP TRIGGER IF EXISTS etoro_perishable_snapshots_append_only ON etoro_perishable_snapshots;
CREATE TRIGGER etoro_perishable_snapshots_append_only
    BEFORE UPDATE ON etoro_perishable_snapshots
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();

DROP TRIGGER IF EXISTS etoro_perishable_universe_append_only ON etoro_perishable_universe;
CREATE TRIGGER etoro_perishable_universe_append_only
    BEFORE UPDATE ON etoro_perishable_universe
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();

DROP TRIGGER IF EXISTS etoro_whatif_panel_append_only ON etoro_whatif_panel;
CREATE TRIGGER etoro_whatif_panel_append_only
    BEFORE UPDATE ON etoro_whatif_panel
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();

DROP TRIGGER IF EXISTS etoro_perishable_requests_append_only ON etoro_perishable_requests;
CREATE TRIGGER etoro_perishable_requests_append_only
    BEFORE UPDATE ON etoro_perishable_requests
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();

DROP TRIGGER IF EXISTS etoro_rate_observations_append_only ON etoro_rate_observations;
CREATE TRIGGER etoro_rate_observations_append_only
    BEFORE UPDATE ON etoro_rate_observations
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();

DROP TRIGGER IF EXISTS etoro_eligibility_observations_append_only ON etoro_eligibility_observations;
CREATE TRIGGER etoro_eligibility_observations_append_only
    BEFORE UPDATE ON etoro_eligibility_observations
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();

DROP TRIGGER IF EXISTS etoro_whatif_observations_append_only ON etoro_whatif_observations;
CREATE TRIGGER etoro_whatif_observations_append_only
    BEFORE UPDATE ON etoro_whatif_observations
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();
