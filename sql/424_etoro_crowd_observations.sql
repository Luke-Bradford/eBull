-- 424_etoro_crowd_observations.sql
--
-- #3381 slice 1 — a forward-only daily record of eToro crowd positioning per instrument.
-- eToro serves no history for these fields (`.claude/skills/data-sources/etoro-api.md`,
-- measured 2026-09-25), so the recording clock IS the dataset: a day not recorded is lost.
--
-- One row in `etoro_crowd_snapshots` per collection attempt, failed ones included, with the
-- request parameters that produced it. `etoro_crowd_observations` holds every instrument row
-- of a COMPLETE collection, parsed columns plus the raw item exactly as served.
--
-- `observed_at` is the only clock: the fetch time of the page the row arrived on. Search rows
-- carry no source timestamp. No backfill is possible and none is claimed.
--
-- `instrument_id` is eToro's id with no FK: the recorder keeps rows for ids we do not hold.
--
-- The only writer is the `etoro_crowd_snapshot` job (app/services/crowd_recorder.py).
--
-- ⚠ Both tables refuse UPDATE: the history is the product, so a correction is a new
-- snapshot, never an edit. DELETE stays possible for the test harness's DELETE-based
-- cleanup (same trade-off as sql/411's intents).

CREATE TABLE IF NOT EXISTS etoro_crowd_snapshots (
    snapshot_id          BIGSERIAL   PRIMARY KEY,
    started_at           TIMESTAMPTZ NOT NULL,
    finished_at          TIMESTAMPTZ NOT NULL CHECK (finished_at >= started_at),
    status               TEXT        NOT NULL CHECK (status IN ('complete', 'failed')),
    request_params       JSONB       NOT NULL,
    pages                INTEGER     CHECK (pages >= 1),
    reported_total_items INTEGER     CHECK (reported_total_items >= 0),
    discarded_items      INTEGER     CHECK (discarded_items >= 0),
    recorded_items       INTEGER     NOT NULL CHECK (recorded_items >= 0),
    error                TEXT,
    CHECK ((status = 'failed') = (error IS NOT NULL)),
    CHECK (
        status = 'failed'
        OR (pages IS NOT NULL AND reported_total_items IS NOT NULL AND discarded_items IS NOT NULL)
    ),
    CHECK (status = 'complete' OR recorded_items = 0)
);

CREATE INDEX IF NOT EXISTS etoro_crowd_snapshots_started_at_idx ON etoro_crowd_snapshots (started_at);

CREATE TABLE IF NOT EXISTS etoro_crowd_observations (
    snapshot_id            BIGINT      NOT NULL REFERENCES etoro_crowd_snapshots(snapshot_id),
    instrument_id          BIGINT      NOT NULL CHECK (instrument_id > 0),
    observed_at            TIMESTAMPTZ NOT NULL,
    -- Published units, unconverted (percentages and counts as served).
    buy_holding_pct        NUMERIC,
    sell_holding_pct       NUMERIC,
    holding_pct            NUMERIC,
    popularity_uniques_7d  NUMERIC,
    popularity_uniques_14d NUMERIC,
    popularity_uniques_30d NUMERIC,
    traders_change_7d      NUMERIC,
    traders_change_14d     NUMERIC,
    traders_change_30d     NUMERIC,
    raw                    JSONB       NOT NULL,
    PRIMARY KEY (snapshot_id, instrument_id)
);

CREATE INDEX IF NOT EXISTS etoro_crowd_observations_instrument_idx
    ON etoro_crowd_observations (instrument_id, observed_at);

CREATE OR REPLACE FUNCTION etoro_crowd_append_only()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END $$;

DROP TRIGGER IF EXISTS etoro_crowd_snapshots_append_only ON etoro_crowd_snapshots;
CREATE TRIGGER etoro_crowd_snapshots_append_only
    BEFORE UPDATE ON etoro_crowd_snapshots
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();

DROP TRIGGER IF EXISTS etoro_crowd_observations_append_only ON etoro_crowd_observations;
CREATE TRIGGER etoro_crowd_observations_append_only
    BEFORE UPDATE ON etoro_crowd_observations
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();
