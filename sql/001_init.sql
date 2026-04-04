CREATE TABLE IF NOT EXISTS instruments (
    instrument_id BIGINT PRIMARY KEY,
    symbol TEXT NOT NULL,
    company_name TEXT NOT NULL,
    exchange TEXT,
    currency TEXT,
    sector TEXT,
    industry TEXT,
    country TEXT,
    is_tradable BOOLEAN NOT NULL DEFAULT TRUE,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_instruments_symbol ON instruments(symbol);
CREATE INDEX IF NOT EXISTS idx_instruments_tradable ON instruments(is_tradable);

CREATE TABLE IF NOT EXISTS price_daily (
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    price_date DATE NOT NULL,
    open NUMERIC(18,6),
    high NUMERIC(18,6),
    low NUMERIC(18,6),
    close NUMERIC(18,6),
    volume NUMERIC(20,4),
    PRIMARY KEY (instrument_id, price_date)
);

CREATE TABLE IF NOT EXISTS fundamentals_snapshot (
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    as_of_date DATE NOT NULL,
    revenue_ttm NUMERIC(20,4),
    gross_margin NUMERIC(10,4),
    operating_margin NUMERIC(10,4),
    fcf NUMERIC(20,4),
    cash NUMERIC(20,4),
    debt NUMERIC(20,4),
    net_debt NUMERIC(20,4),
    shares_outstanding NUMERIC(20,4),
    book_value NUMERIC(20,4),
    eps NUMERIC(20,4),
    custom_json JSONB,
    PRIMARY KEY (instrument_id, as_of_date)
);

CREATE TABLE IF NOT EXISTS filing_events (
    filing_event_id BIGSERIAL PRIMARY KEY,
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    filing_date DATE NOT NULL,
    filing_type TEXT,
    source_url TEXT,
    extracted_summary TEXT,
    red_flag_score NUMERIC(10,4),
    raw_payload_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_filing_events_instrument_date
    ON filing_events(instrument_id, filing_date DESC);

CREATE TABLE IF NOT EXISTS news_events (
    news_event_id BIGSERIAL PRIMARY KEY,
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    event_time TIMESTAMPTZ NOT NULL,
    source TEXT,
    headline TEXT NOT NULL,
    category TEXT,
    sentiment_score NUMERIC(10,4),
    importance_score NUMERIC(10,4),
    url_hash TEXT,
    raw_payload_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_events_instrument_time
    ON news_events(instrument_id, event_time DESC);

CREATE TABLE IF NOT EXISTS coverage (
    instrument_id BIGINT PRIMARY KEY REFERENCES instruments(instrument_id),
    coverage_tier SMALLINT NOT NULL,
    last_reviewed_at TIMESTAMPTZ,
    review_frequency TEXT,
    analyst_status TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS theses (
    thesis_id BIGSERIAL PRIMARY KEY,
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    thesis_version INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    thesis_type TEXT NOT NULL,
    confidence_score NUMERIC(10,4),
    stance TEXT NOT NULL,
    buy_zone_low NUMERIC(18,6),
    buy_zone_high NUMERIC(18,6),
    base_value NUMERIC(18,6),
    bull_value NUMERIC(18,6),
    bear_value NUMERIC(18,6),
    break_conditions_json JSONB,
    memo_markdown TEXT NOT NULL,
    UNIQUE (instrument_id, thesis_version)
);

CREATE INDEX IF NOT EXISTS idx_theses_instrument_created
    ON theses(instrument_id, created_at DESC);

CREATE TABLE IF NOT EXISTS scores (
    score_id BIGSERIAL PRIMARY KEY,
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    scored_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    quality_score NUMERIC(10,4),
    value_score NUMERIC(10,4),
    turnaround_score NUMERIC(10,4),
    momentum_score NUMERIC(10,4),
    sentiment_score NUMERIC(10,4),
    confidence_score NUMERIC(10,4),
    total_score NUMERIC(10,4),
    model_version TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_scores_instrument_scored
    ON scores(instrument_id, scored_at DESC);

CREATE TABLE IF NOT EXISTS trade_recommendations (
    recommendation_id BIGSERIAL PRIMARY KEY,
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    action TEXT NOT NULL,
    target_entry NUMERIC(18,6),
    suggested_size_pct NUMERIC(10,4),
    rationale TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'proposed'
);

CREATE TABLE IF NOT EXISTS orders (
    order_id BIGSERIAL PRIMARY KEY,
    broker_order_ref TEXT,
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    action TEXT NOT NULL,
    order_type TEXT NOT NULL,
    requested_amount NUMERIC(18,6),
    requested_units NUMERIC(18,6),
    status TEXT NOT NULL,
    raw_payload_json JSONB
);

CREATE TABLE IF NOT EXISTS fills (
    fill_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(order_id),
    filled_at TIMESTAMPTZ NOT NULL,
    price NUMERIC(18,6) NOT NULL,
    units NUMERIC(18,6) NOT NULL,
    gross_amount NUMERIC(18,6) NOT NULL,
    fees NUMERIC(18,6) NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS positions (
    instrument_id BIGINT PRIMARY KEY REFERENCES instruments(instrument_id),
    open_date DATE,
    avg_cost NUMERIC(18,6),
    current_units NUMERIC(18,6) NOT NULL DEFAULT 0,
    cost_basis NUMERIC(18,6) NOT NULL DEFAULT 0,
    realized_pnl NUMERIC(18,6) NOT NULL DEFAULT 0,
    unrealized_pnl NUMERIC(18,6) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cash_ledger (
    event_id BIGSERIAL PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_type TEXT NOT NULL,
    amount NUMERIC(18,6) NOT NULL,
    currency TEXT NOT NULL,
    note TEXT
);

CREATE TABLE IF NOT EXISTS tax_lots (
    tax_lot_id BIGSERIAL PRIMARY KEY,
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    event_time TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    quantity NUMERIC(18,6) NOT NULL,
    cost_or_proceeds NUMERIC(18,6) NOT NULL,
    matching_rule TEXT,
    tax_year TEXT NOT NULL,
    reference_fill_id BIGINT REFERENCES fills(fill_id)
);

CREATE INDEX IF NOT EXISTS idx_tax_lots_instrument_time
    ON tax_lots(instrument_id, event_time);

CREATE TABLE IF NOT EXISTS decision_audit (
    decision_id BIGSERIAL PRIMARY KEY,
    decision_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    instrument_id BIGINT REFERENCES instruments(instrument_id),
    stage TEXT NOT NULL,
    model_version TEXT,
    pass_fail TEXT NOT NULL,
    explanation TEXT NOT NULL,
    evidence_json JSONB
);
