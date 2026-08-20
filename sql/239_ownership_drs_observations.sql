-- 239_ownership_drs_observations.sql
--
-- #844 PR-2 (spec docs/specs/etl/2026-07-23-drs-rsu-issuer-disclosures.md)
-- — issuer-disclosed registered-vs-street (DRS) share split, extracted from
-- 10-K / 10-Q Item-5-style narrative for the curated cohort
-- (drs_disclosure.DRS_DISCLOSURE_CIKS). Voluntary disclosure: absence of
-- rows means "issuer does not disclose", never zero.
--
-- One row per (instrument, accession); re-parse updates in place. Overlay
-- data only — never joins the ownership pie (registered shares are still
-- owned, just held in book form at the transfer agent).

BEGIN;

CREATE TABLE IF NOT EXISTS ownership_drs_observations (
    instrument_id      BIGINT NOT NULL REFERENCES instruments(instrument_id),
    source_accession   TEXT NOT NULL,
    form_type          TEXT NOT NULL,
    filed_at           TIMESTAMPTZ NOT NULL,
    -- The disclosure's own "as of" date; NULL when the sentence carries no
    -- date (readers fall back to filed_at for staleness).
    as_of_date         DATE,
    registered_shares  NUMERIC(24, 4) NOT NULL,
    registered_pct     NUMERIC(8, 4),
    street_shares      NUMERIC(24, 4),
    street_pct         NUMERIC(8, 4),
    holders_of_record  BIGINT,
    parser_version     TEXT NOT NULL,
    fetched_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (instrument_id, source_accession)
);

-- Reader: latest disclosure per instrument.
CREATE INDEX IF NOT EXISTS idx_drs_observations_read
    ON ownership_drs_observations(instrument_id, as_of_date DESC NULLS LAST, filed_at DESC);

COMMIT;
