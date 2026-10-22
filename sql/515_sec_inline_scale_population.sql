-- 515_sec_inline_scale_population.sql
--
-- #3947 Amendment 1 (A1, A4) — the scan's durable population, its discovery state, and the re-normalise queue as a
-- ledger. Spec: docs/specs/fundamentals/2026-10-21-3947-dps-inline-scale.md §Amendment 1.
--
-- `sec_inline_scale_population`: (accession, instrument) pairs holding `CommonStockDividendsPerShareDeclared`.
-- Insert-only; a NULL `filed_date` is filled once a date is known. Writers: `upsert_facts_for_instrument` (every
-- companyfacts write into raw) and the `sec_dps_inline_scale_scan` job's per-run seed from raw and from the
-- `source_ref` of `sec_edgar` canonical rows. Raw is a retention window (02:45 sweep), so the population outlives it.
--
-- `sec_inline_scale_discovery`: one row per instrument; `discovered_at` is set by a companyfacts write of the CIK's
-- whole payload (`complete_payload=True`). The scan fetches companyfacts for rows without it, with backoff.
--
-- `sec_inline_scale_renormalise.drained_revision`: a drain no longer deletes its row; it sets
-- `drained_revision = revision` at the revision it read. An instrument is queued while the two differ, and the rows
-- are the ledger of every instrument a decision could have changed.

CREATE TABLE IF NOT EXISTS sec_inline_scale_population (
    accession_number TEXT   NOT NULL,
    instrument_id    BIGINT NOT NULL REFERENCES instruments (instrument_id),
    filed_date       DATE,  -- NULL: undated
    PRIMARY KEY (accession_number, instrument_id)
);
CREATE INDEX IF NOT EXISTS idx_sec_inline_scale_population_instrument
    ON sec_inline_scale_population (instrument_id);

CREATE TABLE IF NOT EXISTS sec_inline_scale_discovery (
    instrument_id   BIGINT  PRIMARY KEY REFERENCES instruments (instrument_id),
    discovered_at   TIMESTAMPTZ,
    attempts        INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ,
    last_error      TEXT
);

ALTER TABLE sec_inline_scale_renormalise ADD COLUMN IF NOT EXISTS drained_revision BIGINT;
