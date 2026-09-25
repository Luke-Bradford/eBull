-- 425_etoro_investor_cohort.sql
--
-- #3381 slice 2 — a forward-only daily record of eToro's popular-investor ranking and the live
-- positions of a frozen, sticky top-investor cohort. eToro serves no history of an investor's
-- positions (`.claude/skills/data-sources/etoro-api.md`, measured 2026-09-25), so the recording
-- clock is the dataset. Spec: docs/proposals/etl/2026-09-25-3381-investor-cohort-recorder.md.
--
-- One `etoro_investor_snapshots` row per collection attempt, failed ones included. A snapshot's
-- ranking rows, new cohort members, per-member fetches and positions commit in ONE transaction
-- with its header. A run that aborts after its ranking completed still commits everything it
-- collected under a `failed` header, so membership and evidence are never lost to a late error.
--
-- `observed_at` is the row-level clock: receipt of the response a row came from. A snapshot's
-- knowledge time as a whole is `finished_at`.
-- `instrument_id` carries no FK: the recorder keeps positions in ids we do not hold (as sql/424).
--
-- The only writer is the `etoro_investor_snapshot` job (app/services/investor_cohort_recorder.py).
--
-- ⚠ Every table refuses UPDATE (the history is the product; a correction is a new snapshot).
-- DELETE stays possible for the test harness's cleanup, as sql/424.

CREATE TABLE IF NOT EXISTS etoro_investor_snapshots (
    snapshot_id           BIGSERIAL   PRIMARY KEY,
    started_at            TIMESTAMPTZ NOT NULL,
    finished_at           TIMESTAMPTZ NOT NULL CHECK (finished_at >= started_at),
    status                TEXT        NOT NULL CHECK (status IN ('complete', 'partial', 'failed')),
    cohort_rule_version   TEXT        NOT NULL,
    request_params        JSONB       NOT NULL,
    -- Every ranking page's `pagination` envelope, in page order. NULL when the ranking walk failed.
    ranking_envelopes     JSONB,
    -- Whole-walk attempts the census needed (a walk that saw the population move is re-walked).
    ranking_attempts      INTEGER     CHECK (ranking_attempts >= 1),
    ranking_pages         INTEGER     CHECK (ranking_pages >= 1),
    ranking_total_items   INTEGER     CHECK (ranking_total_items >= 0),
    ranking_recorded      INTEGER     CHECK (ranking_recorded >= 0),
    -- Fetch-set size: today's selection plus every ledger member.
    investors_expected    INTEGER     CHECK (investors_expected >= 0),
    investors_attempted   INTEGER     CHECK (investors_attempted >= 0),
    investors_fetched     INTEGER     CHECK (investors_fetched >= 0),
    investors_unavailable INTEGER     CHECK (investors_unavailable >= 0),
    investors_errored     INTEGER     CHECK (investors_errored >= 0),
    error                 TEXT,
    CHECK ((status = 'failed') = (error IS NOT NULL)),
    -- The ranking columns are all-or-nothing; a non-failed snapshot always has them.
    CHECK (
        (ranking_envelopes IS NULL) = (ranking_pages IS NULL)
        AND (ranking_pages IS NULL) = (ranking_attempts IS NULL)
        AND (ranking_pages IS NULL) = (ranking_total_items IS NULL)
        AND (ranking_pages IS NULL) = (ranking_recorded IS NULL)
    ),
    -- The member counters are all-or-nothing, balance, and need a completed ranking.
    CHECK (
        (investors_expected IS NULL) = (investors_attempted IS NULL)
        AND (investors_expected IS NULL) = (investors_fetched IS NULL)
        AND (investors_expected IS NULL) = (investors_unavailable IS NULL)
        AND (investors_expected IS NULL) = (investors_errored IS NULL)
    ),
    CHECK (investors_expected IS NULL OR ranking_pages IS NOT NULL),
    CHECK (
        investors_expected IS NULL
        OR (
            investors_fetched + investors_unavailable + investors_errored = investors_attempted
            AND investors_attempted <= investors_expected
        )
    ),
    CHECK (status = 'failed' OR (investors_expected IS NOT NULL AND investors_attempted = investors_expected)),
    CHECK (status = 'failed' OR investors_fetched > 0),
    CHECK (status <> 'complete' OR investors_errored = 0),
    CHECK (status <> 'partial' OR investors_errored > 0)
);

CREATE INDEX IF NOT EXISTS etoro_investor_snapshots_started_at_idx ON etoro_investor_snapshots (started_at);

CREATE TABLE IF NOT EXISTS etoro_investor_rankings (
    snapshot_id  BIGINT      NOT NULL REFERENCES etoro_investor_snapshots(snapshot_id),
    -- 1-based position by (copiers DESC, missing last; cid) over the complete census — computed by the
    -- recorder, not served: eToro's copiers-sorted paging is unstable across ties (spec §Cohort rule).
    rank         INTEGER     NOT NULL CHECK (rank >= 1),
    cid          BIGINT      NOT NULL,
    username     TEXT        NOT NULL,
    observed_at  TIMESTAMPTZ NOT NULL,
    copiers      INTEGER,
    risk_score   INTEGER,
    -- Published unit: a decimal fraction (0.1234 = 12.34%).
    gain         NUMERIC,
    raw          JSONB       NOT NULL,
    PRIMARY KEY (snapshot_id, rank),
    UNIQUE (snapshot_id, cid)
);

CREATE INDEX IF NOT EXISTS etoro_investor_rankings_cid_idx ON etoro_investor_rankings (cid, observed_at);

-- Membership ledger: a cid enters on its first selection under a rule version and never leaves. The
-- fetch set is the union over ALL versions, so a new version never stops observing an old member.
CREATE TABLE IF NOT EXISTS etoro_investor_cohort (
    cohort_rule_version TEXT        NOT NULL,
    cid                 BIGINT      NOT NULL,
    first_snapshot_id   BIGINT      NOT NULL REFERENCES etoro_investor_snapshots(snapshot_id),
    first_selected_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (cohort_rule_version, cid)
);

CREATE TABLE IF NOT EXISTS etoro_investor_fetches (
    snapshot_id           BIGINT      NOT NULL REFERENCES etoro_investor_snapshots(snapshot_id),
    cid                   BIGINT      NOT NULL,
    username              TEXT        NOT NULL,
    -- False when the cid was absent from this snapshot's ranking and its last recorded username was used:
    -- the live response carries no cid, so such a fetch cannot prove it reached the same account.
    username_from_ranking BOOLEAN     NOT NULL,
    observed_at           TIMESTAMPTZ NOT NULL,
    selected_today        BOOLEAN     NOT NULL,
    outcome               TEXT        NOT NULL CHECK (outcome IN ('ok', 'unavailable', 'error')),
    -- NULL only when no response arrived (a transport error).
    http_status           INTEGER,
    realized_credit_pct   NUMERIC,
    unrealized_credit_pct NUMERIC,
    position_count        INTEGER     CHECK (position_count >= 0),
    social_trade_count    INTEGER     CHECK (social_trade_count >= 0),
    -- The body as served (JSON, or the text as a JSON string); the error text for a transport error.
    raw                   JSONB       NOT NULL,
    PRIMARY KEY (snapshot_id, cid),
    CHECK ((outcome = 'ok') = (position_count IS NOT NULL AND social_trade_count IS NOT NULL)),
    CHECK (outcome <> 'ok' OR http_status BETWEEN 200 AND 299)
);

CREATE INDEX IF NOT EXISTS etoro_investor_fetches_cid_idx ON etoro_investor_fetches (cid, observed_at);

CREATE TABLE IF NOT EXISTS etoro_investor_positions (
    snapshot_id        BIGINT      NOT NULL,
    cid                BIGINT      NOT NULL,
    position_id        BIGINT      NOT NULL,
    observed_at        TIMESTAMPTZ NOT NULL,
    instrument_id      BIGINT      NOT NULL,
    is_buy             BOOLEAN     NOT NULL,
    leverage           INTEGER,
    -- Published units, unconverted.
    investment_pct     NUMERIC,
    open_rate          NUMERIC,
    open_timestamp     TIMESTAMPTZ,
    net_profit         NUMERIC,
    stop_loss_rate     NUMERIC,
    take_profit_rate   NUMERIC,
    trailing_stop_loss BOOLEAN,
    social_trade_id    BIGINT,
    parent_position_id BIGINT,
    PRIMARY KEY (snapshot_id, cid, position_id),
    FOREIGN KEY (snapshot_id, cid) REFERENCES etoro_investor_fetches(snapshot_id, cid)
);

CREATE INDEX IF NOT EXISTS etoro_investor_positions_instrument_idx
    ON etoro_investor_positions (instrument_id, observed_at);

-- Reuses sql/424's trigger function.
DROP TRIGGER IF EXISTS etoro_investor_snapshots_append_only ON etoro_investor_snapshots;
CREATE TRIGGER etoro_investor_snapshots_append_only
    BEFORE UPDATE ON etoro_investor_snapshots
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();

DROP TRIGGER IF EXISTS etoro_investor_rankings_append_only ON etoro_investor_rankings;
CREATE TRIGGER etoro_investor_rankings_append_only
    BEFORE UPDATE ON etoro_investor_rankings
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();

DROP TRIGGER IF EXISTS etoro_investor_cohort_append_only ON etoro_investor_cohort;
CREATE TRIGGER etoro_investor_cohort_append_only
    BEFORE UPDATE ON etoro_investor_cohort
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();

DROP TRIGGER IF EXISTS etoro_investor_fetches_append_only ON etoro_investor_fetches;
CREATE TRIGGER etoro_investor_fetches_append_only
    BEFORE UPDATE ON etoro_investor_fetches
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();

DROP TRIGGER IF EXISTS etoro_investor_positions_append_only ON etoro_investor_positions;
CREATE TRIGGER etoro_investor_positions_append_only
    BEFORE UPDATE ON etoro_investor_positions
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();
