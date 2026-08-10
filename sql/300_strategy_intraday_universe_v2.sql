-- 300_strategy_intraday_universe_v2.sql
--
-- #2477/#2508 -- retain the small V1 evidence identity, then broaden the
-- prospective panel with explicit NYSE stock and nominal-price contrasts.
-- The new scope is a new immutable version; V1 rows/gaps remain attributable.

UPDATE strategy_intraday_universe_versions
SET status = 'retired', retired_at = now()
WHERE universe_version = 'ETORO-RTH-V1'
  AND status = 'active';

INSERT INTO strategy_intraday_universe_versions (
    universe_version, provider, session_rule, status, rationale
) VALUES (
    'ETORO-RTH-V2',
    'etoro',
    'nyse_rth',
    'draft',
    'Prospective stock/ETF, Nasdaq/NYSE, nominal-price and liquidity research contrasts. #2477/#2508.'
);

INSERT INTO strategy_intraday_universe_members (
    universe_version, ordinal, timeframe, symbol, purpose
) VALUES
    ('ETORO-RTH-V2',  1, '30m', 'SPY',  'NYSE liquid broad-market ETF control'),
    ('ETORO-RTH-V2',  2, '30m', 'QQQ',  'Nasdaq liquid growth ETF control'),
    ('ETORO-RTH-V2',  3, '30m', 'IWM',  'NYSE liquid small-cap ETF control'),
    ('ETORO-RTH-V2',  4, '30m', 'AAPL', 'Nasdaq liquid high-price stock control'),
    ('ETORO-RTH-V2',  5, '30m', 'CENN', 'Nasdaq sub-$5 low-volume stock contrast'),
    ('ETORO-RTH-V2',  6, '30m', 'F',    'NYSE $5-$20 liquid stock contrast'),
    ('ETORO-RTH-V2',  7, '30m', 'KO',   'NYSE $50-$150 liquid stock contrast'),
    ('ETORO-RTH-V2',  8, '30m', 'JPM',  'NYSE above-$150 liquid stock contrast'),
    ('ETORO-RTH-V2',  9, '5m',  'SPY',  'NYSE liquid broad-market execution path'),
    ('ETORO-RTH-V2', 10, '5m',  'QQQ',  'Nasdaq liquid growth execution path'),
    ('ETORO-RTH-V2', 11, '5m',  'IWM',  'NYSE liquid small-cap execution path'),
    ('ETORO-RTH-V2', 12, '5m',  'AAPL', 'Nasdaq liquid high-price execution path'),
    ('ETORO-RTH-V2', 13, '5m',  'CENN', 'Nasdaq low-volume execution contrast'),
    ('ETORO-RTH-V2', 14, '5m',  'F',    'NYSE low-price liquid execution contrast'),
    ('ETORO-RTH-V2', 15, '5m',  'KO',   'NYSE mid-price liquid execution contrast'),
    ('ETORO-RTH-V2', 16, '5m',  'JPM',  'NYSE high-price liquid execution contrast'),
    ('ETORO-RTH-V2', 17, '1m',  'SPY',  'liquid market micro-path'),
    ('ETORO-RTH-V2', 18, '1m',  'AAPL', 'liquid stock micro-path');

INSERT INTO strategy_intraday_harvest_cursors (universe_version, last_ordinal)
VALUES ('ETORO-RTH-V2', 0);

UPDATE strategy_intraday_universe_versions
SET status = 'active', activated_at = now()
WHERE universe_version = 'ETORO-RTH-V2'
  AND status = 'draft';
