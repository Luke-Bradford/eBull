-- 441_periodic_report_sections.sql
--
-- #3518 — MD&A text of original periodic reports (10-K Item 7 / 10-Q Part I Item 2, Reg S-K Item 303),
-- the store the fund-v1 `mdna` pack block reads (docs/proposals/execution/2026-09-30-3515-ai-discretionary-
-- fund-v1.md §4). Spec: docs/proposals/etl/2026-09-30-3518-periodic-report-sections.md §3.
--
-- One row per extraction OUTCOME, successes and failures alike. The identity is
-- (instrument_id, accession_number, section_id, extractor); `row_id` order is the chronology among the rows
-- of one identity. A correction is a new row: an `invalidated` row names the row it withdraws.
--
-- Writers: the `sec_periodic_report_sections` job (app/services/periodic_report_sections.py) for every
-- status except `invalidated`, and the operator script scripts/invalidate_periodic_report_section.py for
-- `invalidated` only.
--
-- ⚠ Append-only, stricter than sql/424-426: UPDATE, DELETE and TRUNCATE all raise. No grant beyond the
-- owner is issued, and the dev stack runs as the owner, so the triggers are the enforcement. The test
-- harness empties the table under `session_replication_role = replica`, which disables all three.

CREATE TABLE IF NOT EXISTS periodic_report_sections (
    row_id             BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    instrument_id      BIGINT      NOT NULL REFERENCES instruments (instrument_id),
    -- filing_events.provider_filing_id
    accession_number   TEXT        NOT NULL,
    section_id         TEXT        NOT NULL CHECK (section_id IN ('10-K:Item 7', '10-Q:Part I, Item 2')),
    -- edgartools==<version>/mdna-<MDNA_EXTRACTOR_REVISION> (app/services/mdna_extraction.py)
    extractor          TEXT        NOT NULL,
    status             TEXT        NOT NULL
        CHECK (status IN ('extracted', 'item_absent', 'fetch_failed', 'parse_failed', 'invalidated')),
    -- The accessor text as returned; the reader normalises it (mdna_extraction.normalise_mdna).
    body               TEXT,
    -- len(normalise_mdna(body)): the length of the EXTRACTED text, not of the MD&A (spec §6).
    -- Equality with the normalisation is the writer's, pinned by test; SQL cannot run it.
    full_chars         INTEGER,
    detail             TEXT,
    retryable          BOOLEAN     NOT NULL,
    invalidates_row_id BIGINT      UNIQUE REFERENCES periodic_report_sections (row_id),
    source_url         TEXT,
    -- sha256 of the decoded body's UTF-8 encoding (the house client returns response.text).
    source_text_sha256 TEXT        CHECK (source_text_sha256 ~ '^[0-9a-f]{64}$'),
    source_chars       INTEGER     CHECK (source_chars >= 0),
    fetched_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT periodic_report_sections_body_iff_extracted
        CHECK ((status = 'extracted') = (body IS NOT NULL)),
    CONSTRAINT periodic_report_sections_body_not_blank
        CHECK (body IS NULL OR length(btrim(body)) > 0),
    CONSTRAINT periodic_report_sections_full_chars_iff_extracted
        CHECK ((status = 'extracted') = (full_chars IS NOT NULL)),
    CONSTRAINT periodic_report_sections_full_chars_positive
        CHECK (full_chars > 0),
    CONSTRAINT periodic_report_sections_detail_iff_not_extracted
        CHECK ((status = 'extracted') = (detail IS NULL)),
    CONSTRAINT periodic_report_sections_retryable_only_failures
        CHECK (NOT retryable OR status IN ('fetch_failed', 'parse_failed')),
    CONSTRAINT periodic_report_sections_invalidates_iff_invalidated
        CHECK ((status = 'invalidated') = (invalidates_row_id IS NOT NULL)),
    CONSTRAINT periodic_report_sections_source_url_present
        CHECK (status = 'invalidated' OR coalesce(detail, '') = 'no_url' OR source_url IS NOT NULL),
    CONSTRAINT periodic_report_sections_hash_iff_chars
        CHECK ((source_text_sha256 IS NULL) = (source_chars IS NULL)),
    CONSTRAINT periodic_report_sections_extracted_provenance
        CHECK (status <> 'extracted' OR (source_url IS NOT NULL AND source_text_sha256 IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS periodic_report_sections_identity_idx
    ON periodic_report_sections (instrument_id, accession_number, section_id, extractor, row_id DESC);

CREATE OR REPLACE FUNCTION periodic_report_sections_append_only()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only (#3518): an outcome, once recorded, stays as recorded; '
                    'withdraw a row with scripts/invalidate_periodic_report_section.py', TG_TABLE_NAME;
END $$;

DROP TRIGGER IF EXISTS periodic_report_sections_no_update_delete ON periodic_report_sections;
CREATE TRIGGER periodic_report_sections_no_update_delete
    BEFORE UPDATE OR DELETE ON periodic_report_sections
    FOR EACH ROW EXECUTE FUNCTION periodic_report_sections_append_only();

DROP TRIGGER IF EXISTS periodic_report_sections_no_truncate ON periodic_report_sections;
CREATE TRIGGER periodic_report_sections_no_truncate
    BEFORE TRUNCATE ON periodic_report_sections
    FOR EACH STATEMENT EXECUTE FUNCTION periodic_report_sections_append_only();

-- Knowledge time: `fetched_at` is the INSERT's clock_timestamp() whatever the writer supplied, so no row
-- can carry a stamp earlier than its own outcome (spec §3).
-- Invalidation integrity (a CHECK cannot read another row): the target exists, carries the same identity,
-- is not itself an invalidation, and is not the new row. UNIQUE(invalidates_row_id) makes it once-only.
CREATE OR REPLACE FUNCTION periodic_report_sections_before_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    target periodic_report_sections%ROWTYPE;
BEGIN
    NEW.fetched_at := clock_timestamp();
    IF NEW.status <> 'invalidated' THEN
        RETURN NEW;
    END IF;
    IF NEW.invalidates_row_id = NEW.row_id THEN
        RAISE EXCEPTION 'periodic_report_sections: row % cannot invalidate itself', NEW.row_id;
    END IF;
    SELECT * INTO target FROM periodic_report_sections WHERE row_id = NEW.invalidates_row_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'periodic_report_sections: invalidated row % does not exist', NEW.invalidates_row_id;
    END IF;
    IF target.status = 'invalidated' THEN
        RAISE EXCEPTION 'periodic_report_sections: row % is itself an invalidation', NEW.invalidates_row_id;
    END IF;
    IF (target.instrument_id, target.accession_number, target.section_id, target.extractor)
       IS DISTINCT FROM (NEW.instrument_id, NEW.accession_number, NEW.section_id, NEW.extractor) THEN
        RAISE EXCEPTION 'periodic_report_sections: row % belongs to a different identity', NEW.invalidates_row_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS periodic_report_sections_before_insert ON periodic_report_sections;
CREATE TRIGGER periodic_report_sections_before_insert
    BEFORE INSERT ON periodic_report_sections
    FOR EACH ROW EXECUTE FUNCTION periodic_report_sections_before_insert();
