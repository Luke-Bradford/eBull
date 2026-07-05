-- #1975 — 424B2 volume-gated parse (child of #1816).
--
-- Widens the prospectus_offerings.subtype CHECK to admit '424B2': B2 is now
-- mapped to the sec_424b manifest source, with the parser's pre-fetch volume
-- gate (_424B2_VOLUME_CAP = 100 lifetime B2s per instrument) tombstoning the
-- bank/ETN structured-note factories before any SEC fetch. The subtype stays
-- a filing-trigger bucket, not a taxonomy — equity-vs-debt comes from the
-- parsed Item 501(b)(3) cover. '424B8' remains out of scope (late-filing
-- duplicate of another 424(b) paragraph).
--
-- Full enum list carried forward from sql/216 (the constraint's origin).

BEGIN;

ALTER TABLE prospectus_offerings
    DROP CONSTRAINT IF EXISTS prospectus_offerings_subtype_check;
ALTER TABLE prospectus_offerings
    ADD CONSTRAINT prospectus_offerings_subtype_check
    CHECK (subtype IN ('424B1', '424B2', '424B3', '424B4', '424B5', '424B7'));

-- The gate COUNTs a filer's lifetime B2s once per manifest B2 row (parser +
-- prefetch hook). Without a targeted index the whale drains (JPM 30k rows)
-- re-scan the issuer's whole filing_events history per row. Partial index
-- keeps the COUNT an index-only scan on exactly the gated predicate.
CREATE INDEX IF NOT EXISTS idx_filing_events_424b2_by_instrument
    ON filing_events (instrument_id)
    WHERE filing_type = '424B2';

COMMIT;
