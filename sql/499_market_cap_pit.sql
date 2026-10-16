-- 499_market_cap_pit.sql
--
-- #3841 slice 2 — point-in-time market cap on a measured price basis.
-- Spec: docs/specs/fundamentals/2026-10-16-3841-pit-market-cap.md ("Selection and steps", "Build and publication").
--
-- `market_cap_pit_rebuild` (app/services/pit_market_cap_build.py) replaces every step in one REPEATABLE READ
-- transaction and appends its build row in the same transaction, so a build that raises leaves no row and the
-- previous build stays active. `instrument_market_cap_pit` multiplies at read time and refuses a step whose price
-- basis has moved since the build (`basis_stale`).

BEGIN;

-- Append-only: one row per COMPLETED build. A build is one transaction, so a failed build writes nothing here.
CREATE TABLE IF NOT EXISTS market_cap_pit_builds (
    build_id              BIGSERIAL PRIMARY KEY,
    started_at            TIMESTAMPTZ NOT NULL,
    finished_at           TIMESTAMPTZ NOT NULL,
    status                TEXT NOT NULL CHECK (status = 'complete'),
    build_date            DATE NOT NULL,
    code_sha              TEXT,
    rule_version          TEXT NOT NULL,
    submissions_sha256    TEXT NOT NULL,
    holdings_max_filed_at TIMESTAMPTZ,
    summary               JSONB NOT NULL
);

CREATE OR REPLACE FUNCTION market_cap_pit_builds_insert_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'market_cap_pit_builds is append-only (#3841)';
END $$;

DROP TRIGGER IF EXISTS market_cap_pit_builds_insert_only ON market_cap_pit_builds;
CREATE TRIGGER market_cap_pit_builds_insert_only BEFORE UPDATE OR DELETE ON market_cap_pit_builds
    FOR EACH ROW EXECUTE FUNCTION market_cap_pit_builds_insert_only();

-- One row per step: a maximal run of sessions with one selected result, half-open [valid_from, valid_to).
CREATE TABLE IF NOT EXISTS instrument_share_count_pit (
    instrument_id   BIGINT NOT NULL REFERENCES instruments (instrument_id),
    valid_from      DATE NOT NULL,
    valid_to        DATE NOT NULL,
    shares_concept  TEXT NOT NULL CHECK (shares_concept IN ('cover', 'balance_sheet')),
    accession       TEXT NOT NULL,
    context_date    DATE NOT NULL,
    accepted_at     TIMESTAMPTZ NOT NULL,
    -- NULL when the candidates at the selected key disagree (`shares_ambiguous`; the values are in evidence).
    shares_reported NUMERIC,
    split_factor    DOUBLE PRECISION,
    refusal         TEXT CHECK (refusal IN (
        'no_left_anchor', 'basis_unresolved', 'shares_ambiguous', 'shares_scale_conflict',
        'shares_turnover_implausible', 'shares_discontinuity', 'multiclass_unavailable', 'invalid_input',
        'anchor_contradicted', 'float_bound'
    )),
    evidence        JSONB NOT NULL,
    -- price_daily_revision rows for the instrument visible to the build's snapshot (the view's basis_stale test).
    revision_count  BIGINT NOT NULL,
    -- price_daily_backdated_insert rows for the instrument in the snapshot: a bar inserted below the series frontier
    -- writes no revision row, so it is counted here.
    backdated_insert_count BIGINT NOT NULL,
    -- The instrument's last stored bar in the snapshot; NULL with none. A later bar was never seen by the build.
    bars_through    DATE,
    -- Deferred: the build row is appended last, in the transaction that wrote its steps.
    build_id        BIGINT NOT NULL REFERENCES market_cap_pit_builds (build_id) DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (instrument_id, valid_from),
    CHECK (valid_from < valid_to),
    CHECK ((split_factor IS NULL) <> (refusal IS NULL))
);

-- Steps never overlap within an instrument (spec "Selection and steps").
CREATE EXTENSION IF NOT EXISTS btree_gist;
ALTER TABLE instrument_share_count_pit DROP CONSTRAINT IF EXISTS instrument_share_count_pit_no_overlap;
ALTER TABLE instrument_share_count_pit ADD CONSTRAINT instrument_share_count_pit_no_overlap
    EXCLUDE USING gist (instrument_id WITH =, daterange(valid_from, valid_to, '[)') WITH &&);

COMMENT ON TABLE instrument_share_count_pit IS
    '#3841: the selected share count per session run, its measured split factor or refusal. Replaced whole by '
    'each market_cap_pit_rebuild; read through instrument_market_cap_pit.';

-- price_daily joined to the step valid at price_date. market_cap = close x shares_reported x split_factor, or NULL
-- with reason: the step's refusal, invalid_input for an unusable close, or basis_stale when the instrument's
-- revision or backdated-insert count moved since the build, the bar is later than the last bar the build saw, or an
-- unresolved price_series_break exists. A session with no step has no row.
CREATE OR REPLACE VIEW instrument_market_cap_pit AS
WITH revisions AS (
    SELECT r.instrument_id, count(*) AS revision_count
    FROM price_daily_revision r
    WHERE r.instrument_id IN (SELECT DISTINCT instrument_id FROM instrument_share_count_pit)
    GROUP BY r.instrument_id
),
backdated AS (
    SELECT d.instrument_id, count(*) AS backdated_insert_count
    FROM price_daily_backdated_insert d
    WHERE d.instrument_id IN (SELECT DISTINCT instrument_id FROM instrument_share_count_pit)
    GROUP BY d.instrument_id
),
breaks AS (
    SELECT DISTINCT b.instrument_id FROM price_series_break b WHERE b.resolved_by IS NULL
),
joined AS (
    SELECT p.instrument_id, p.price_date, p.close, s.valid_from, s.valid_to, s.shares_concept, s.accession,
           s.context_date, s.accepted_at, s.shares_reported, s.split_factor, s.refusal, s.evidence, s.build_id,
           s.revision_count AS step_revision_count,
           CASE
               WHEN s.refusal IS NOT NULL THEN s.refusal
               WHEN p.close IS NULL OR p.close = 'NaN'::numeric OR p.close <= 0 THEN 'invalid_input'
               WHEN coalesce(r.revision_count, 0) <> s.revision_count
                 OR coalesce(d.backdated_insert_count, 0) <> s.backdated_insert_count
                 OR s.bars_through IS NULL OR p.price_date > s.bars_through
                 OR k.instrument_id IS NOT NULL
                   THEN 'basis_stale'
           END AS reason
    FROM instrument_share_count_pit s
    JOIN price_daily p
      ON p.instrument_id = s.instrument_id AND p.price_date >= s.valid_from AND p.price_date < s.valid_to
    LEFT JOIN revisions r ON r.instrument_id = s.instrument_id
    LEFT JOIN backdated d ON d.instrument_id = s.instrument_id
    LEFT JOIN breaks k ON k.instrument_id = s.instrument_id
)
SELECT instrument_id, price_date, close, valid_from, valid_to, shares_concept, accession, context_date, accepted_at,
       shares_reported, split_factor, evidence, build_id, step_revision_count, reason,
       CASE WHEN reason IS NULL THEN close::double precision * shares_reported::double precision * split_factor
       END AS market_cap
FROM joined;

COMMENT ON VIEW instrument_market_cap_pit IS
    '#3841: point-in-time market cap per (instrument, session), or NULL with reason. A current estimate, not an '
    'append-only record: a consumer that decides on it copies the whole row into its own decision row.';

COMMIT;
