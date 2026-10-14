-- 490_tax_character.sql
--
-- #3616 slice 3d-3a — tax character from listed-class evidence; funds withheld.
-- Spec: docs/specs/metrics/2026-10-14-3616-slice3d3a-tax-character.md (Designs 1, 4). Writers:
-- `app.services.tax_character.refresh_cover_instances` (cover instances) and
-- `app.services.tax_matching.run_tax_matching` (runs and character rows).
--
-- Runs gain `character_policy_id` and `as_of`, NULL together for runs before this slice. The run key includes the
-- character policy (NULLS NOT DISTINCT), the seal compares both new columns, and a refused run has no character row.

BEGIN;

ALTER TABLE tax_matching_runs ADD COLUMN IF NOT EXISTS character_policy_id TEXT CHECK (character_policy_id <> '');
ALTER TABLE tax_matching_runs ADD COLUMN IF NOT EXISTS as_of TIMESTAMPTZ;
ALTER TABLE tax_matching_runs DROP CONSTRAINT IF EXISTS tax_matching_runs_character_as_of_paired;
ALTER TABLE tax_matching_runs ADD CONSTRAINT tax_matching_runs_character_as_of_paired
    CHECK ((character_policy_id IS NULL) = (as_of IS NULL));

ALTER TABLE tax_matching_runs DROP CONSTRAINT IF EXISTS tax_matching_runs_evidence_run_id_matching_policy_id_class__key;
ALTER TABLE tax_matching_runs DROP CONSTRAINT IF EXISTS tax_matching_runs_run_key;
ALTER TABLE tax_matching_runs ADD CONSTRAINT tax_matching_runs_run_key
    UNIQUE NULLS NOT DISTINCT (evidence_run_id, matching_policy_id, class_policy_id, character_policy_id, inputs_digest);

ALTER TABLE tax_matching_dependencies DROP CONSTRAINT IF EXISTS tax_matching_dependencies_reason_check;
ALTER TABLE tax_matching_dependencies ADD CONSTRAINT tax_matching_dependencies_reason_check CHECK (reason IN (
    'unresolved_subject', 'identity_unestablished', 'lot_unavailable', 'subsequent_acquisition_rule', 'unplaceable',
    'character_unestablished'));

-- One row per security of a run (checked in code before the seal).
CREATE TABLE IF NOT EXISTS tax_matching_characters (
    matching_run_id  BIGINT NOT NULL,
    tax_security_id  BIGINT NOT NULL,
    character        TEXT NOT NULL CHECK (character IN ('share', 'unestablished')),
    detail           TEXT CHECK (detail IN (
                         'identity_unestablished', 'member_conflict', 'exchange_traded_fund', 'instrument_type',
                         'cik_unestablished', 'sec_profile_unavailable', 'sec_profile_conflict', 'entity_type', 'sic',
                         'cover_metadata_conflict', 'cover_unavailable', 'cover_unusable',
                         'listed_class_not_common_stock', 'holding_start_cover_unavailable',
                         'holding_start_cover_unusable', 'holding_start_class_not_common_stock')),
    -- The single CIK, else NULL.
    cik              TEXT CHECK (cik ~ '^[0-9]{10}$'),
    -- The accessions selected, else NULL.
    latest_cover     TEXT,
    start_cover      TEXT,
    PRIMARY KEY (matching_run_id, tax_security_id),
    FOREIGN KEY (matching_run_id, tax_security_id) REFERENCES tax_matching_securities (matching_run_id, tax_security_id),
    CHECK ((character = 'unestablished') = (detail IS NOT NULL))
);

DROP TRIGGER IF EXISTS tax_matching_characters_guard ON tax_matching_characters;
CREATE TRIGGER tax_matching_characters_guard
    BEFORE INSERT OR UPDATE OR DELETE ON tax_matching_characters
    FOR EACH ROW EXECUTE FUNCTION tax_matching_child_guard();

-- SEC's extracted inline-XBRL instance of each cover tax reads (spec "Build correction"): the shared
-- `filing_raw_documents` kind `xbrl_cover_instance` is born-compacted, so it holds no body to read. Only structurally
-- sound instances are stored; an accession's documents are immutable, so a row is never replaced.
CREATE TABLE IF NOT EXISTS tax_cover_instances (
    accession_number  TEXT PRIMARY KEY CHECK (accession_number ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    source_url        TEXT NOT NULL CHECK (source_url <> ''),
    fetched_at        TIMESTAMPTZ NOT NULL,
    body              BYTEA NOT NULL CHECK (octet_length(body) BETWEEN 1 AND 16777216),
    body_sha256       TEXT NOT NULL,
    CHECK (body_sha256 = encode(sha256(body), 'hex'))
);

CREATE OR REPLACE FUNCTION tax_cover_instances_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'tax_cover_instances: rows are never updated or deleted';
END $$;

DROP TRIGGER IF EXISTS tax_cover_instances_append_only ON tax_cover_instances;
CREATE TRIGGER tax_cover_instances_append_only
    BEFORE UPDATE OR DELETE ON tax_cover_instances
    FOR EACH ROW EXECUTE FUNCTION tax_cover_instances_append_only();

-- 489's seal, with the two new columns in the compared tuple and the character rows in the refused-run check.
CREATE OR REPLACE FUNCTION tax_matching_runs_seal() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'building' THEN
            RAISE EXCEPTION 'tax_matching_runs: a run is inserted building, not %', NEW.state;
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'tax_matching_runs: run % cannot be deleted', OLD.matching_run_id;
    END IF;
    IF OLD.state <> 'building' THEN
        RAISE EXCEPTION 'tax_matching_runs: run % is % and cannot change', OLD.matching_run_id, OLD.state;
    END IF;
    IF NEW.state NOT IN ('published', 'refused') THEN
        RAISE EXCEPTION 'tax_matching_runs: run % is sealed published or refused, not %', OLD.matching_run_id, NEW.state;
    END IF;
    -- The identity is in the tuple: an identity column accepts `SET matching_run_id = DEFAULT` on UPDATE.
    IF (NEW.matching_run_id, NEW.evidence_run_id, NEW.matching_policy_id, NEW.class_policy_id, NEW.inputs_digest,
        NEW.inputs_document, NEW.computed_at, NEW.history_fetch_started_at, NEW.character_policy_id, NEW.as_of)
       IS DISTINCT FROM
       (OLD.matching_run_id, OLD.evidence_run_id, OLD.matching_policy_id, OLD.class_policy_id, OLD.inputs_digest,
        OLD.inputs_document, OLD.computed_at, OLD.history_fetch_started_at, OLD.character_policy_id, OLD.as_of) THEN
        RAISE EXCEPTION 'tax_matching_runs: sealing run % changes only state, reason, counts and withheld_all',
            OLD.matching_run_id;
    END IF;
    IF NEW.state = 'refused' AND (
        EXISTS (SELECT 1 FROM tax_matching_instruments WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_matching_lot_classes WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_matching_securities WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_matching_characters WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_matching_dependencies WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_statutory_disposals WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_identifications WHERE matching_run_id = OLD.matching_run_id)
    ) THEN
        RAISE EXCEPTION 'tax_matching_runs: run % has rows and cannot be refused', OLD.matching_run_id;
    END IF;
    RETURN NEW;
END $$;

COMMIT;
