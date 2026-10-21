-- 514_sec_inline_dps_scale.sql
--
-- #3947 — inline `scale` of each `us-gaap:CommonStockDividendsPerShareDeclared` occurrence (XBRL US
-- DQC.US.0179.10141). Spec: docs/specs/fundamentals/2026-10-21-3947-dps-inline-scale.md §Schema.
--
-- Writer: the `sec_dps_inline_scale_scan` job (app/services/dps_inline_scale_scan.py), one transaction per
-- accession. Readers: the normaliser's DPS decisions (behind EBULL_DPS_INLINE_SCALE_ENABLED, a later PR) and
-- scripts/ab_3947_dps_inline_scale.py.
--
-- `sec_inline_scale_scan` holds two things per accession:
--   * the ACTIVE generation (outcome, documents, parser_version, scanned_at): NULL until a scan succeeds, and a
--     failed attempt never changes it;
--   * the WORK state at `attempt_version` (attempts, next_attempt_at, last_error): reset once per version, when
--     the first attempt at a new version starts.

CREATE TABLE IF NOT EXISTS sec_inline_scale_scan (
    accession_number TEXT PRIMARY KEY,
    outcome          TEXT CHECK (outcome IN ('inline', 'not_inline')),
    documents        INTEGER CHECK (documents >= 0),
    parser_version   TEXT,
    scanned_at       TIMESTAMPTZ,
    attempt_version  TEXT,
    attempts         INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at  TIMESTAMPTZ,
    last_error       TEXT,
    -- The active generation is all-or-nothing.
    CHECK ((outcome IS NULL) = (parser_version IS NULL)
           AND (outcome IS NULL) = (scanned_at IS NULL)
           AND (outcome IS NULL) = (documents IS NULL))
);

-- One row per inline element of the concept, scale 0 included, so each decision traces to its elements.
CREATE TABLE IF NOT EXISTS sec_inline_dps_occurrence (
    accession_number TEXT    NOT NULL REFERENCES sec_inline_scale_scan ON DELETE CASCADE,
    document         TEXT    NOT NULL,
    -- document order among the concept's elements in `document`
    element_ordinal  INTEGER NOT NULL CHECK (element_ordinal >= 0),
    fact_id          TEXT,
    context_ref_raw  TEXT,
    unit_ref_raw     TEXT,
    scale_raw        TEXT,
    sign_raw         TEXT,
    format_raw       TEXT,
    decimals_raw     TEXT,
    display_text     TEXT    NOT NULL,
    -- The resolved fields below are NULL when unresolved.
    entity_cik       TEXT    CHECK (entity_cik ~ '^[0-9]{10}$'),
    period_start     DATE,
    period_end       DATE,
    dimensional      BOOLEAN,
    unit             TEXT    CHECK (unit IN ('USD/shares', 'unsupported_unit')),
    scale            INTEGER CHECK (scale BETWEEN -30 AND 30),
    instance_value   NUMERIC,
    -- NULL when every field above resolved; otherwise the reason.
    unresolved       TEXT,
    PRIMARY KEY (accession_number, document, element_ordinal),
    CHECK (unresolved IS NOT NULL
           OR (entity_cik IS NOT NULL AND period_end IS NOT NULL AND dimensional IS NOT NULL
               AND unit IS NOT NULL AND scale IS NOT NULL AND instance_value IS NOT NULL))
);

-- Instruments whose DPS decisions may have changed with an occurrence change, drained by the job. A later
-- scan never clears a row; draining deletes only the revision it read.
CREATE SEQUENCE IF NOT EXISTS sec_inline_scale_renormalise_revision;
CREATE TABLE IF NOT EXISTS sec_inline_scale_renormalise (
    instrument_id BIGINT  PRIMARY KEY REFERENCES instruments (instrument_id),
    revision      BIGINT  NOT NULL,
    failures      INTEGER NOT NULL DEFAULT 0 CHECK (failures >= 0)
);
