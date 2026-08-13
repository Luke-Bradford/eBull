-- 298_strategy_intraday_harvest.sql
--
-- #2477 -- bounded, versioned eToro intraday research collection.
--
-- Membership is symbolic so a clean install can declare the research universe
-- before the eToro universe has populated instrument ids.  The harvester
-- resolves each symbol point-in-time and refuses missing or ambiguous matches.
-- Observations continue to live only in the partitioned/capped tables created
-- by 276-278; this migration adds compact control and gap metadata, not another
-- bar store.

CREATE TABLE IF NOT EXISTS strategy_intraday_universe_versions (
    universe_version TEXT PRIMARY KEY,
    provider          TEXT NOT NULL CHECK (provider = 'etoro'),
    session_rule      TEXT NOT NULL CHECK (session_rule = 'nyse_rth'),
    status            TEXT NOT NULL CHECK (status IN ('draft', 'active', 'retired')),
    rationale         TEXT NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    activated_at      TIMESTAMPTZ,
    retired_at        TIMESTAMPTZ,
    CONSTRAINT strategy_intraday_universe_status_times CHECK (
        (status = 'draft' AND activated_at IS NULL AND retired_at IS NULL)
        OR (status = 'active' AND activated_at IS NOT NULL AND retired_at IS NULL)
        OR (status = 'retired' AND activated_at IS NOT NULL AND retired_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_strategy_intraday_one_active_universe
    ON strategy_intraday_universe_versions ((status))
    WHERE status = 'active';

CREATE TABLE IF NOT EXISTS strategy_intraday_universe_members (
    universe_version TEXT NOT NULL REFERENCES strategy_intraday_universe_versions(universe_version),
    ordinal          SMALLINT NOT NULL CHECK (ordinal > 0),
    timeframe        TEXT NOT NULL CHECK (timeframe IN ('30m', '5m', '1m')),
    symbol           TEXT NOT NULL CHECK (symbol = upper(symbol) AND btrim(symbol) <> ''),
    purpose          TEXT NOT NULL CHECK (btrim(purpose) <> ''),
    PRIMARY KEY (universe_version, timeframe, symbol),
    UNIQUE (universe_version, ordinal)
);

CREATE TABLE IF NOT EXISTS strategy_intraday_harvest_cursors (
    universe_version TEXT PRIMARY KEY REFERENCES strategy_intraday_universe_versions(universe_version),
    last_ordinal     SMALLINT NOT NULL DEFAULT 0 CHECK (last_ordinal >= 0),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS strategy_intraday_gaps (
    universe_version TEXT NOT NULL REFERENCES strategy_intraday_universe_versions(universe_version),
    timeframe        TEXT NOT NULL CHECK (timeframe IN ('30m', '5m', '1m')),
    instrument_id    BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    gap_start        TIMESTAMPTZ NOT NULL,
    gap_end          TIMESTAMPTZ NOT NULL,
    first_detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_detected_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT strategy_intraday_gap_order CHECK (gap_end > gap_start),
    PRIMARY KEY (universe_version, timeframe, instrument_id, gap_start, gap_end)
);

CREATE INDEX IF NOT EXISTS idx_strategy_intraday_gaps_recent
    ON strategy_intraday_gaps (last_detected_at DESC);

INSERT INTO strategy_intraday_universe_versions (
    universe_version, provider, session_rule, status, rationale, activated_at
) VALUES (
    'ETORO-RTH-V1',
    'etoro',
    'nyse_rth',
    'active',
    'Small prospective research panel: liquid ETFs, one liquid equity and one low-volume contrast. #2477.',
    now()
) ON CONFLICT DO NOTHING;

INSERT INTO strategy_intraday_universe_members (
    universe_version, ordinal, timeframe, symbol, purpose
) VALUES
    ('ETORO-RTH-V1',  1, '30m', 'SPY',  'liquid market control'),
    ('ETORO-RTH-V1',  2, '30m', 'QQQ',  'liquid growth-market control'),
    ('ETORO-RTH-V1',  3, '30m', 'IWM',  'liquid small-cap market control'),
    ('ETORO-RTH-V1',  4, '30m', 'AAPL', 'liquid individual-equity control'),
    ('ETORO-RTH-V1',  5, '30m', 'CENN', 'low-volume individual-equity contrast'),
    ('ETORO-RTH-V1',  6, '5m',  'SPY',  'liquid market execution path'),
    ('ETORO-RTH-V1',  7, '5m',  'QQQ',  'liquid growth-market execution path'),
    ('ETORO-RTH-V1',  8, '5m',  'IWM',  'liquid small-cap execution path'),
    ('ETORO-RTH-V1',  9, '5m',  'AAPL', 'liquid equity execution path'),
    ('ETORO-RTH-V1', 10, '5m',  'CENN', 'low-volume equity execution contrast'),
    ('ETORO-RTH-V1', 11, '1m',  'SPY',  'liquid market micro-path'),
    ('ETORO-RTH-V1', 12, '1m',  'AAPL', 'liquid equity micro-path')
ON CONFLICT DO NOTHING;

INSERT INTO strategy_intraday_harvest_cursors (universe_version, last_ordinal)
VALUES ('ETORO-RTH-V1', 0)
ON CONFLICT DO NOTHING;

COMMENT ON TABLE strategy_intraday_universe_versions IS
    'Immutable/versioned scope for bounded prospective intraday research. At most one version is active. #2477.';
COMMENT ON TABLE strategy_intraday_universe_members IS
    'Predeclared symbolic eToro collection members; no all-instrument crawl. #2477.';
COMMENT ON TABLE strategy_intraday_gaps IS
    'Compact missing completed-RTH interval ranges detected by the harvester; gaps are never imputed. #2477.';
