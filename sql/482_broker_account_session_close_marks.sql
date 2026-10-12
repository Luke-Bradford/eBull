-- 482_broker_account_session_close_marks.sql
--
-- #3612 slice 2 (spec: docs/proposals/risk/2026-10-12-3612-marked-daily-loss.md, Design 1).
--
-- The anchor S of the marked daily-loss check: the broker's own per-position marks, captured once per
-- NYSE session after its regular close. The check's daily figure is the broker's unrealised P&L now
-- minus the same position's unrealised P&L at S, so S must be a snapshot that is never replaced.
-- `broker_account_position_marks` (sql/383) cannot serve: it is keyed by UTC date and its child set is
-- replaced wholesale on every newer same-day observation.
--
-- Writer: `account_equity_evidence.record_session_close_snapshot`, called by `daily_portfolio_sync`.
-- It writes only a snapshot observed on P's New York date after P's regular close, where P is
-- `latest_completed_us_session(observed_at)`, with every direct position carrying a mark time in
-- [P's close, observed_at] and a reported `totalFees`. The parent insert is ON CONFLICT DO NOTHING and
-- the children are inserted only when the parent was, so S is the first qualifying snapshot.
--
-- Mark count completeness is the consumer's precondition, as sql/383 states for its own children: the
-- child count must equal official_direct_long_positions + official_direct_short_positions, and a NULL
-- declared count refuses.
--
-- PostgreSQL `numeric` admits 'NaN' and, since PostgreSQL 14, '±Infinity'. Every numeric column is
-- CHECKed finite. `NaN = NaN` holds for numeric (sql/374), so `<> 'NaN'` is the working test. The
-- columns carry no typmod, as sql/479: a finite value the writer's gate accepts cannot then overflow
-- at INSERT, and the broker's values are stored as served.
--
-- UPDATE, DELETE and TRUNCATE are refused on both tables (sql/424's `etoro_crowd_append_only`, with
-- sql/441's statement-level TRUNCATE guard), so S can be neither edited nor removed and replaced by a
-- later snapshot. The test harness empties tables under `session_replication_role = replica`, which
-- disables all three. `instrument_id` carries no FK, as
-- sql/383.

CREATE TABLE IF NOT EXISTS broker_account_session_close_snapshots (
    environment                     TEXT        NOT NULL,
    session_date                    DATE        NOT NULL,
    observed_at                     TIMESTAMPTZ NOT NULL,
    account_currency_id             INTEGER     NOT NULL,
    currency                        TEXT,
    official_direct_long_positions  INTEGER     NOT NULL CHECK (official_direct_long_positions >= 0),
    official_direct_short_positions INTEGER     NOT NULL CHECK (official_direct_short_positions >= 0),
    recorded_at                     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (environment, session_date)
);

CREATE TABLE IF NOT EXISTS broker_account_session_close_marks (
    environment           TEXT          NOT NULL,
    session_date          DATE          NOT NULL,
    position_id           BIGINT        NOT NULL,
    instrument_id         INTEGER       NOT NULL,
    is_buy                BOOLEAN       NOT NULL,
    units                 NUMERIC       NOT NULL,
    amount                NUMERIC       NOT NULL,
    unrealized_pnl        NUMERIC       NOT NULL,
    market_value          NUMERIC       NOT NULL,
    is_partially_altered  BOOLEAN       NOT NULL,
    close_rate            NUMERIC       NOT NULL,
    close_conversion_rate NUMERIC       NOT NULL,
    asset_currency_id     INTEGER       NOT NULL CHECK (asset_currency_id > 0),
    pnl_timestamp         TIMESTAMPTZ   NOT NULL,
    total_fees            NUMERIC       NOT NULL,
    PRIMARY KEY (environment, session_date, position_id),
    FOREIGN KEY (environment, session_date)
        REFERENCES broker_account_session_close_snapshots (environment, session_date),
    CHECK (units <> 'NaN' AND units <> 'Infinity' AND units <> '-Infinity' AND units > 0),
    CHECK (amount <> 'NaN' AND amount <> 'Infinity' AND amount <> '-Infinity'),
    CHECK (unrealized_pnl <> 'NaN' AND unrealized_pnl <> 'Infinity' AND unrealized_pnl <> '-Infinity'),
    CHECK (market_value <> 'NaN' AND market_value <> 'Infinity' AND market_value <> '-Infinity'),
    CHECK (close_rate <> 'NaN' AND close_rate <> 'Infinity' AND close_rate > 0),
    CHECK (close_conversion_rate <> 'NaN' AND close_conversion_rate <> 'Infinity' AND close_conversion_rate > 0),
    CHECK (total_fees <> 'NaN' AND total_fees <> 'Infinity' AND total_fees <> '-Infinity'),
    -- The same identity sql/383 stores for its marks.
    CHECK (abs(market_value - (amount + unrealized_pnl)) <= 0.000001)
);

DROP TRIGGER IF EXISTS broker_account_session_close_snapshots_append_only ON broker_account_session_close_snapshots;
CREATE TRIGGER broker_account_session_close_snapshots_append_only
    BEFORE UPDATE OR DELETE ON broker_account_session_close_snapshots
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();

DROP TRIGGER IF EXISTS broker_account_session_close_marks_append_only ON broker_account_session_close_marks;
CREATE TRIGGER broker_account_session_close_marks_append_only
    BEFORE UPDATE OR DELETE ON broker_account_session_close_marks
    FOR EACH ROW EXECUTE FUNCTION etoro_crowd_append_only();

DROP TRIGGER IF EXISTS broker_account_session_close_snapshots_no_truncate ON broker_account_session_close_snapshots;
CREATE TRIGGER broker_account_session_close_snapshots_no_truncate
    BEFORE TRUNCATE ON broker_account_session_close_snapshots
    FOR EACH STATEMENT EXECUTE FUNCTION etoro_crowd_append_only();

DROP TRIGGER IF EXISTS broker_account_session_close_marks_no_truncate ON broker_account_session_close_marks;
CREATE TRIGGER broker_account_session_close_marks_no_truncate
    BEFORE TRUNCATE ON broker_account_session_close_marks
    FOR EACH STATEMENT EXECUTE FUNCTION etoro_crowd_append_only();

COMMENT ON TABLE broker_account_session_close_snapshots IS
    '#3612 slice 2: the first official account snapshot observed on a NYSE session''s New York date after '
    'its regular close, with every direct position marked inside [close, observed_at]. The anchor S of the '
    'marked daily-loss check. Insert-once (UPDATE, DELETE and TRUNCATE refused).';

COMMENT ON TABLE broker_account_session_close_marks IS
    '#3612 slice 2: the per-position broker terms of S, one row per direct position, shorts included. '
    'Child count must equal the parent''s official_direct_long_positions + official_direct_short_positions '
    '(consumer precondition). Insert-once (UPDATE, DELETE and TRUNCATE refused).';
