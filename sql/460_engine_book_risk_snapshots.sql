-- 460_engine_book_risk_snapshots.sql
-- #3543 (gap register P4) slice 1: one row per completed NYSE session and policy version for the
-- engine book -- volatility, beta, concentration, stress and stale marks, compared with the mandate.
-- Measurement only: nothing reads these rows to refuse, size or rebalance. Spec:
-- docs/proposals/risk/2026-10-02-3543-engine-book-risk-snapshot.md.
--
-- Percentages are in percent units. A refused run writes no row (its reason is the job's error).

CREATE TABLE IF NOT EXISTS engine_book_risk_snapshots (
    session_date               DATE NOT NULL,
    policy_version             TEXT NOT NULL CHECK (btrim(policy_version) <> ''),
    measured_at                TIMESTAMPTZ NOT NULL,
    pool_event_id              BIGINT NOT NULL REFERENCES strategy_paper_pool_events (strategy_paper_pool_event_id),
    capital_usd                NUMERIC(18,6) NOT NULL CHECK (capital_usd > 0 AND capital_usd <> 'NaN'),
    gross_usd                  NUMERIC(18,6) NOT NULL CHECK (gross_usd >= 0 AND gross_usd <> 'NaN'),
    position_count             INTEGER NOT NULL CHECK (position_count >= 0),
    instrument_count           INTEGER NOT NULL CHECK (instrument_count >= 0 AND instrument_count <= position_count),
    open_trade_count           INTEGER NOT NULL CHECK (open_trade_count >= 0),
    cost_marked_count          INTEGER NOT NULL CHECK (cost_marked_count >= 0),
    stale_count                INTEGER NOT NULL CHECK (stale_count >= cost_marked_count),
    largest_share_pct          NUMERIC(12,8) CHECK (largest_share_pct BETWEEN 0 AND 100),
    top5_share_pct             NUMERIC(12,8) CHECK (top5_share_pct BETWEEN 0 AND 100),
    hhi                        NUMERIC(14,8) CHECK (hhi BETWEEN 0 AND 10000),
    hist_vol_pct               NUMERIC(14,8) CHECK (hist_vol_pct >= 0 AND hist_vol_pct <> 'NaN'),
    ewma_vol_pct               NUMERIC(14,8) CHECK (ewma_vol_pct >= 0 AND ewma_vol_pct <> 'NaN'),
    beta                       NUMERIC(14,8) CHECK (beta <> 'NaN'),
    vol_n_obs                  INTEGER NOT NULL CHECK (vol_n_obs >= 0),
    beta_n_obs                 INTEGER NOT NULL CHECK (beta_n_obs >= 0),
    sample_first               DATE,
    sample_last                DATE,
    history_status             TEXT NOT NULL CHECK (history_status IN ('ok', 'insufficient_history', 'degenerate', 'empty_book')),
    beta_defaulted_count       INTEGER NOT NULL CHECK (beta_defaulted_count >= 0),
    beta_defaulted_weight_pct  NUMERIC(14,8) NOT NULL CHECK (beta_defaulted_weight_pct >= 0 AND beta_defaulted_weight_pct <> 'NaN'),
    stress_2020_pct            NUMERIC(14,8) NOT NULL CHECK (stress_2020_pct <> 'NaN'),
    stress_2022_pct            NUMERIC(14,8) NOT NULL CHECK (stress_2022_pct <> 'NaN'),
    checks                     JSONB NOT NULL CHECK (jsonb_typeof(checks) = 'object'),
    positions                  JSONB NOT NULL CHECK (jsonb_typeof(positions) = 'array'),
    PRIMARY KEY (session_date, policy_version),
    CONSTRAINT engine_book_risk_snapshots_vol_shape CHECK (
        (history_status <> 'insufficient_history') = (hist_vol_pct IS NOT NULL AND ewma_vol_pct IS NOT NULL)
    ),
    CONSTRAINT engine_book_risk_snapshots_sample_shape CHECK ((sample_first IS NULL) = (sample_last IS NULL)
        AND (sample_first IS NULL OR sample_first <= sample_last))
);

COMMENT ON TABLE engine_book_risk_snapshots IS
    'Engine-book risk measured once per completed NYSE session and policy version (#3543). Measurement only: '
    'no refusal, sizing or rebalancing reads it. The book is as held at measured_at, marked at the session close.';

CREATE OR REPLACE FUNCTION prevent_engine_book_risk_snapshot_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% of engine book risk snapshot % is refused: a snapshot is stated once (#3543)',
        TG_OP, OLD.session_date;
END $$;

-- Row-level, so TRUNCATE (test isolation only) is not refused.
DROP TRIGGER IF EXISTS trg_engine_book_risk_snapshots_append_only ON engine_book_risk_snapshots;
CREATE TRIGGER trg_engine_book_risk_snapshots_append_only
BEFORE UPDATE OR DELETE ON engine_book_risk_snapshots
FOR EACH ROW EXECUTE FUNCTION prevent_engine_book_risk_snapshot_mutation();
