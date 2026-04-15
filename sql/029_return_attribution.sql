-- Migration 029: return attribution tables
--
-- return_attribution: per-position decomposition of realised returns.
-- Computed when a position is fully closed (current_units = 0 after EXIT fill).
--
-- return_attribution_summary: rolling-window aggregation of attribution
-- components across all attributed positions.

CREATE TABLE IF NOT EXISTS return_attribution (
    attribution_id       BIGSERIAL PRIMARY KEY,
    instrument_id        BIGINT NOT NULL REFERENCES instruments(instrument_id),
    hold_start           DATE NOT NULL,
    hold_end             DATE NOT NULL,
    hold_days            INTEGER NOT NULL,
    -- Return components (all as decimal fractions, e.g. 0.05 = 5%)
    gross_return_pct     NUMERIC(12, 6) NOT NULL,
    market_return_pct    NUMERIC(12, 6) NOT NULL,
    sector_return_pct    NUMERIC(12, 6) NOT NULL,
    model_alpha_pct      NUMERIC(12, 6) NOT NULL,
    timing_alpha_pct     NUMERIC(12, 6) NOT NULL,
    cost_drag_pct        NUMERIC(12, 6) NOT NULL,
    residual_pct         NUMERIC(12, 6) NOT NULL,
    -- Score snapshot at entry (from the recommendation's score_id)
    score_at_entry       NUMERIC(10, 4),
    score_components     JSONB,
    -- Computation metadata
    entry_fill_id        BIGINT REFERENCES fills(fill_id),
    exit_fill_id         BIGINT REFERENCES fills(fill_id),
    recommendation_id    BIGINT REFERENCES trade_recommendations(recommendation_id),
    attribution_method   TEXT NOT NULL DEFAULT 'sector_relative_v1',
    computed_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_return_attribution_instrument
    ON return_attribution(instrument_id);
CREATE INDEX IF NOT EXISTS idx_return_attribution_computed
    ON return_attribution(computed_at);

CREATE TABLE IF NOT EXISTS return_attribution_summary (
    summary_id           BIGSERIAL PRIMARY KEY,
    window_days          INTEGER NOT NULL,
    positions_attributed INTEGER NOT NULL,
    avg_gross_return_pct    NUMERIC(12, 6),
    avg_market_return_pct   NUMERIC(12, 6),
    avg_sector_return_pct   NUMERIC(12, 6),
    avg_model_alpha_pct     NUMERIC(12, 6),
    avg_timing_alpha_pct    NUMERIC(12, 6),
    avg_cost_drag_pct       NUMERIC(12, 6),
    computed_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
