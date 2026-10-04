-- 467_ibkr_borrow_archive.sql
--
-- #3622 slice 1 — daily forward archive of Interactive Brokers' public stock-borrow file (`usa.txt` on the
-- anonymous `shortstock` FTP). It has no history: a day not archived is lost. It is an indicative borrow-cost
-- proxy and a long-side filter input (Drechsler & Drechsler 2014); it does NOT price eToro CFD shorts, which
-- carry eToro's own overnight / hard-to-borrow terms.
--
-- Clocks: `provider_as_of` is the file's own `#BOF` stamp (US/Eastern wall clock, measured: a 12:50Z fetch on
-- 2026-10-04 carried 08:36:44); `observed_at` is our receipt. A snapshot and its rows commit in one
-- transaction. Append-only: rows are never updated.

BEGIN;

CREATE TABLE IF NOT EXISTS ibkr_borrow_snapshots (
    snapshot_id      BIGSERIAL PRIMARY KEY,
    file_name        TEXT NOT NULL CHECK (file_name <> ''),
    provider_as_of   TIMESTAMPTZ NOT NULL,
    observed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    response_sha256  TEXT NOT NULL CHECK (response_sha256 ~ '^[0-9a-f]{64}$'),
    payload_gzip     BYTEA NOT NULL CHECK (octet_length(payload_gzip) > 0),
    row_count        INTEGER NOT NULL CHECK (row_count > 0),
    CONSTRAINT ibkr_borrow_snapshot_identity_uq UNIQUE (file_name, response_sha256)
);

CREATE INDEX IF NOT EXISTS idx_ibkr_borrow_snapshots_as_of
    ON ibkr_borrow_snapshots (file_name, provider_as_of DESC);

CREATE TABLE IF NOT EXISTS ibkr_borrow_rates (
    snapshot_id        BIGINT NOT NULL REFERENCES ibkr_borrow_snapshots(snapshot_id) ON DELETE RESTRICT,
    conid              BIGINT NOT NULL,
    symbol             TEXT NOT NULL CHECK (symbol <> ''),
    currency           TEXT NOT NULL CHECK (currency <> ''),
    name               TEXT NOT NULL,
    isin               TEXT,
    figi               TEXT,
    -- Percent per annum as published; NULL where the file says NA.
    rebate_rate_pct    NUMERIC,
    fee_rate_pct       NUMERIC,
    -- Shares available; the file caps the figure at ">10000000", kept as 10000000 + available_capped.
    available_shares   BIGINT NOT NULL CHECK (available_shares >= 0),
    available_capped   BOOLEAN NOT NULL,
    PRIMARY KEY (snapshot_id, conid)
);

CREATE INDEX IF NOT EXISTS idx_ibkr_borrow_rates_symbol ON ibkr_borrow_rates (symbol, snapshot_id);

CREATE OR REPLACE FUNCTION ibkr_borrow_append_only()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END $$;

DROP TRIGGER IF EXISTS ibkr_borrow_snapshots_append_only ON ibkr_borrow_snapshots;
CREATE TRIGGER ibkr_borrow_snapshots_append_only
    BEFORE UPDATE ON ibkr_borrow_snapshots
    FOR EACH ROW EXECUTE FUNCTION ibkr_borrow_append_only();

DROP TRIGGER IF EXISTS ibkr_borrow_rates_append_only ON ibkr_borrow_rates;
CREATE TRIGGER ibkr_borrow_rates_append_only
    BEFORE UPDATE ON ibkr_borrow_rates
    FOR EACH ROW EXECUTE FUNCTION ibkr_borrow_append_only();

COMMENT ON TABLE ibkr_borrow_snapshots IS
    '#3622 daily archive of IBKR shortstock usa.txt: exact gzip payload + the provider #BOF as-of. Append-only.';
COMMENT ON TABLE ibkr_borrow_rates IS
    '#3622 typed rows of one ibkr_borrow_snapshots payload. A borrow-cost PROXY, not eToro CFD pricing.';

COMMIT;
