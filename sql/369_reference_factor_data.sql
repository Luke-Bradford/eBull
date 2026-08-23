-- 369_reference_factor_data.sql
--
-- #2912 — immutable raw snapshots plus typed observations for the free
-- reference datasets used to validate eBull factor constructions. A response
-- is committed before parsing; rejected bytes remain auditable, and readers
-- select accepted snapshots only. Historical upstream revisions and parser
-- upgrades therefore never rewrite the input behind an old validation result.

BEGIN;

CREATE TABLE IF NOT EXISTS reference_data_snapshots (
    snapshot_id      BIGSERIAL PRIMARY KEY,
    source           TEXT NOT NULL
        CHECK (source IN ('kenneth_french', 'aqr', 'fred')),
    dataset_key      TEXT NOT NULL CHECK (dataset_key <> ''),
    source_url       TEXT NOT NULL CHECK (source_url ~ '^https://'),
    fetched_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    etag             TEXT,
    last_modified    TEXT,
    content_type     TEXT,
    response_sha256  TEXT NOT NULL
        CHECK (response_sha256 ~ '^[0-9a-f]{64}$'),
    payload          BYTEA NOT NULL CHECK (octet_length(payload) > 0),
    parser_version   TEXT NOT NULL CHECK (parser_version <> ''),
    parse_status     TEXT NOT NULL DEFAULT 'pending'
        CHECK (parse_status IN ('pending', 'accepted', 'rejected')),
    parse_error      TEXT,
    parsed_at        TIMESTAMPTZ,
    row_count        INTEGER,
    missing_count    INTEGER,
    first_observation DATE,
    last_observation  DATE,
    CONSTRAINT reference_data_snapshot_identity_uq
        UNIQUE (source, dataset_key, response_sha256, parser_version),
    CONSTRAINT reference_data_snapshot_parse_shape CHECK (
        (parse_status = 'pending'
         AND parse_error IS NULL AND parsed_at IS NULL
         AND row_count IS NULL AND missing_count IS NULL
         AND first_observation IS NULL AND last_observation IS NULL)
        OR
        (parse_status = 'rejected'
         AND parse_error IS NOT NULL AND parsed_at IS NOT NULL
         AND row_count IS NULL AND missing_count IS NULL
         AND first_observation IS NULL AND last_observation IS NULL)
        OR
        (parse_status = 'accepted'
         AND parse_error IS NULL AND parsed_at IS NOT NULL
         AND row_count IS NOT NULL AND row_count > 0
         AND missing_count IS NOT NULL AND missing_count >= 0
         AND first_observation IS NOT NULL AND last_observation IS NOT NULL
         AND last_observation >= first_observation)
    )
);

CREATE INDEX IF NOT EXISTS idx_reference_data_snapshots_latest_accepted
    ON reference_data_snapshots
       (source, dataset_key, fetched_at DESC, snapshot_id DESC)
    WHERE parse_status = 'accepted';

CREATE TABLE IF NOT EXISTS reference_data_observations (
    snapshot_id      BIGINT NOT NULL
        REFERENCES reference_data_snapshots(snapshot_id) ON DELETE RESTRICT,
    series_key       TEXT NOT NULL CHECK (series_key <> ''),
    observation_date DATE NOT NULL,
    value            NUMERIC NOT NULL,
    unit             TEXT NOT NULL CHECK (
        unit IN ('decimal_return', 'percent_per_annum', 'binary_indicator')
    ),
    PRIMARY KEY (snapshot_id, series_key, observation_date)
);

COMMENT ON TABLE reference_data_snapshots IS
    'Immutable exact HTTP responses for #2912 factor/macro reference data. '
    'Raw bytes commit before parsing; accepted/rejected status is terminal for '
    'one parser version, and a parser upgrade creates a new snapshot identity.';

COMMENT ON TABLE reference_data_observations IS
    'Typed observations normalized from one immutable accepted reference-data '
    'snapshot. Values are Decimal/NUMERIC and units are explicit; missing '
    'provider values are counted on the parent and never stored as zero.';

COMMIT;
