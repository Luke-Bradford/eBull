-- 489_tax_matching.sql
--
-- #3616 slice 3d-2 — statutory matching from the effective generation of tax evidence.
-- Spec: docs/specs/metrics/2026-10-14-3616-slice3d2-statutory-matching.md (Design 9). Writer:
-- `app.services.tax_matching.run_tax_matching`; selection: `app.services.tax_matching.effective_tax_matching`.
--
-- One sealed, append-only snapshot per (evidence run, policy ids, inputs_digest). A run is inserted `building`, takes
-- its rows, and is updated exactly once to `published` or `refused`; a refused run has no rows, a sealed run never
-- changes, and a commit leaving a run `building` is refused. So a security, day or pool absent from a run is absent.
--
-- The legacy `disposal_matches` / `s104_pool` lose their writer here (the matcher moves to tax_matching.py); they
-- stay, empty, with their readers until slice 3d-4 (spec Design 11).

BEGIN;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM disposal_matches) OR EXISTS (SELECT 1 FROM s104_pool) THEN
        RAISE EXCEPTION 'sql/489: disposal_matches and s104_pool must be empty: their writer is retired (#3616 slice 3d-2)';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS tax_matching_runs (
    matching_run_id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    evidence_run_id           BIGINT NOT NULL REFERENCES tax_evidence_runs (run_id),
    matching_policy_id        TEXT NOT NULL CHECK (matching_policy_id <> ''),
    class_policy_id           TEXT NOT NULL CHECK (class_policy_id <> ''),
    inputs_digest             TEXT NOT NULL CHECK (inputs_digest ~ '^[0-9a-f]{64}$'),
    -- The exact canonical bytes hashed, so a run's inputs are reconstructable after a later insert or
    -- `instruments` change.
    inputs_document           TEXT NOT NULL,
    state                     TEXT NOT NULL CHECK (state IN ('building', 'published', 'refused')),
    reason                    TEXT,
    computed_at               TIMESTAMPTZ NOT NULL,
    -- NULL: the generation has no `ok` history fetch.
    history_fetch_started_at  TIMESTAMPTZ,
    withheld_all              BOOLEAN NOT NULL,
    counts                    JSONB,
    CHECK (encode(sha256(convert_to(inputs_document, 'UTF8')), 'hex') = inputs_digest),
    CHECK ((state = 'refused') = (reason IS NOT NULL)),
    CHECK (state = 'building' OR counts IS NOT NULL),
    UNIQUE (evidence_run_id, matching_policy_id, class_policy_id, inputs_digest),
    UNIQUE (matching_run_id, evidence_run_id)
);

CREATE TABLE IF NOT EXISTS tax_matching_instruments (
    matching_run_id          BIGINT NOT NULL REFERENCES tax_matching_runs (matching_run_id),
    instrument_id            BIGINT NOT NULL,
    -- As read; NULL when the id has no `instruments` row (and then no pointer either).
    symbol                   TEXT,
    canonical_instrument_id  BIGINT,
    -- The smallest member id.
    component_id             BIGINT NOT NULL,
    -- NULL: unestablished or unplaceable.
    tax_security_id          BIGINT,
    PRIMARY KEY (matching_run_id, instrument_id)
);

CREATE TABLE IF NOT EXISTS tax_matching_lot_classes (
    matching_run_id  BIGINT NOT NULL,
    lot_key          TEXT NOT NULL,
    evidence_run_id  BIGINT NOT NULL,
    class            TEXT NOT NULL CHECK (class IN ('final_eligible', 'provisional', 'unavailable')),
    -- Consideration reasons; for `unavailable`, the failing fields.
    reasons          TEXT[] NOT NULL,
    PRIMARY KEY (matching_run_id, lot_key),
    FOREIGN KEY (evidence_run_id, lot_key) REFERENCES tax_evidence_lots (run_id, lot_key),
    FOREIGN KEY (matching_run_id, evidence_run_id) REFERENCES tax_matching_runs (matching_run_id, evidence_run_id),
    CHECK ((class = 'final_eligible') = (cardinality(reasons) = 0))
);

CREATE TABLE IF NOT EXISTS tax_matching_securities (
    matching_run_id        BIGINT NOT NULL REFERENCES tax_matching_runs (matching_run_id),
    tax_security_id        BIGINT NOT NULL,
    instrument_ids         BIGINT[] NOT NULL CHECK (cardinality(instrument_ids) > 0),
    status                 TEXT NOT NULL CHECK (status IN ('computed', 'withheld')),
    pool_units             NUMERIC(24, 8),
    pool_cost_gbp          NUMERIC(44, 14),
    pool_missing_lot_keys  TEXT[],
    pool_reasons           TEXT[],
    window_open_since      DATE,
    PRIMARY KEY (matching_run_id, tax_security_id),
    CHECK ((status <> 'computed' OR (
        pool_units >= 0 AND pool_missing_lot_keys IS NOT NULL AND pool_reasons IS NOT NULL
        AND (pool_cost_gbp IS NULL) = (cardinality(pool_missing_lot_keys) > 0)
        AND (pool_cost_gbp IS NULL OR pool_cost_gbp >= 0)
    )) IS TRUE),
    CHECK ((status <> 'withheld' OR
        num_nonnulls(pool_units, pool_cost_gbp, pool_missing_lot_keys, pool_reasons, window_open_since) = 0) IS TRUE)
);

CREATE TABLE IF NOT EXISTS tax_matching_dependencies (
    matching_run_id  BIGINT NOT NULL REFERENCES tax_matching_runs (matching_run_id),
    seq              INTEGER NOT NULL CHECK (seq >= 1),
    -- NULL: every security (`unplaceable`).
    tax_security_id  BIGINT,
    reason           TEXT NOT NULL CHECK (reason IN (
                         'unresolved_subject', 'identity_unestablished', 'lot_unavailable',
                         'subsequent_acquisition_rule', 'unplaceable')),
    subject          TEXT,
    lot_key          TEXT,
    instrument_id    BIGINT,
    detail           TEXT,
    PRIMARY KEY (matching_run_id, seq),
    FOREIGN KEY (matching_run_id, tax_security_id) REFERENCES tax_matching_securities (matching_run_id, tax_security_id),
    CHECK ((reason = 'unplaceable') = (tax_security_id IS NULL)),
    CHECK (num_nonnulls(subject, lot_key, instrument_id, detail) > 0)
);

CREATE TABLE IF NOT EXISTS tax_statutory_disposals (
    matching_run_id   BIGINT NOT NULL,
    tax_security_id   BIGINT NOT NULL,
    uk_date           DATE NOT NULL,
    tax_year          TEXT NOT NULL CHECK (tax_year ~ '^[0-9]{4}/[0-9]{2}$'),
    lot_keys          TEXT[] NOT NULL CHECK (cardinality(lot_keys) > 0),
    units             NUMERIC(24, 8) NOT NULL CHECK (units > 0),
    proceeds_gbp      NUMERIC(44, 14),
    cost_gbp          NUMERIC(44, 14),
    gain_gbp          NUMERIC(44, 14),
    status            TEXT NOT NULL CHECK (status IN ('computed', 'amount_missing')),
    missing_lot_keys  TEXT[] NOT NULL,
    reasons           TEXT[] NOT NULL,
    PRIMARY KEY (matching_run_id, tax_security_id, uk_date),
    FOREIGN KEY (matching_run_id, tax_security_id) REFERENCES tax_matching_securities (matching_run_id, tax_security_id),
    CHECK ((status <> 'computed' OR (
        proceeds_gbp IS NOT NULL AND cost_gbp IS NOT NULL AND gain_gbp = proceeds_gbp - cost_gbp
        AND cardinality(missing_lot_keys) = 0
    )) IS TRUE),
    CHECK ((status <> 'amount_missing' OR (
        gain_gbp IS NULL AND (proceeds_gbp IS NULL OR cost_gbp IS NULL) AND cardinality(missing_lot_keys) > 0
    )) IS TRUE)
);

CREATE TABLE IF NOT EXISTS tax_identifications (
    matching_run_id       BIGINT NOT NULL,
    tax_security_id       BIGINT NOT NULL,
    disposal_uk_date      DATE NOT NULL,
    seq                   INTEGER NOT NULL CHECK (seq >= 1),
    rule                  TEXT NOT NULL CHECK (rule IN ('same_day', 'bed_and_breakfast', 's104_pool')),
    acquisition_uk_date   DATE,
    acquisition_lot_keys  TEXT[] NOT NULL,
    units                 NUMERIC(24, 8) NOT NULL CHECK (units > 0),
    cost_gbp              NUMERIC(44, 14) CHECK (cost_gbp >= 0),
    missing_lot_keys      TEXT[] NOT NULL,
    reasons               TEXT[] NOT NULL,
    PRIMARY KEY (matching_run_id, tax_security_id, disposal_uk_date, seq),
    FOREIGN KEY (matching_run_id, tax_security_id, disposal_uk_date)
        REFERENCES tax_statutory_disposals (matching_run_id, tax_security_id, uk_date),
    CHECK ((rule <> 'same_day' OR (
        acquisition_uk_date = disposal_uk_date AND cardinality(acquisition_lot_keys) > 0
    )) IS TRUE),
    CHECK ((rule <> 'bed_and_breakfast' OR (
        acquisition_uk_date > disposal_uk_date AND acquisition_uk_date <= disposal_uk_date + 30
        AND cardinality(acquisition_lot_keys) > 0
    )) IS TRUE),
    CHECK ((rule <> 's104_pool' OR (acquisition_uk_date IS NULL AND cardinality(acquisition_lot_keys) = 0)) IS TRUE),
    CHECK ((cost_gbp IS NULL) = (cardinality(missing_lot_keys) > 0))
);

-- A run is inserted `building` and updated exactly once, to `published` or `refused`, changing only state, reason,
-- counts and withheld_all; a refused run has no child row; nothing is deleted.
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
        NEW.inputs_document, NEW.computed_at, NEW.history_fetch_started_at)
       IS DISTINCT FROM
       (OLD.matching_run_id, OLD.evidence_run_id, OLD.matching_policy_id, OLD.class_policy_id, OLD.inputs_digest,
        OLD.inputs_document, OLD.computed_at, OLD.history_fetch_started_at) THEN
        RAISE EXCEPTION 'tax_matching_runs: sealing run % changes only state, reason, counts and withheld_all',
            OLD.matching_run_id;
    END IF;
    IF NEW.state = 'refused' AND (
        EXISTS (SELECT 1 FROM tax_matching_instruments WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_matching_lot_classes WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_matching_securities WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_matching_dependencies WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_statutory_disposals WHERE matching_run_id = OLD.matching_run_id)
        OR EXISTS (SELECT 1 FROM tax_identifications WHERE matching_run_id = OLD.matching_run_id)
    ) THEN
        RAISE EXCEPTION 'tax_matching_runs: run % has rows and cannot be refused', OLD.matching_run_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS tax_matching_runs_seal ON tax_matching_runs;
CREATE TRIGGER tax_matching_runs_seal
    BEFORE INSERT OR UPDATE OR DELETE ON tax_matching_runs
    FOR EACH ROW EXECUTE FUNCTION tax_matching_runs_seal();

-- A commit that leaves a run `building` is refused.
CREATE OR REPLACE FUNCTION tax_matching_runs_sealed_at_commit() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM tax_matching_runs WHERE matching_run_id = NEW.matching_run_id AND state = 'building') THEN
        RAISE EXCEPTION 'tax_matching_runs: run % is still building at commit', NEW.matching_run_id;
    END IF;
    RETURN NULL;
END $$;

DROP TRIGGER IF EXISTS tax_matching_runs_sealed_at_commit ON tax_matching_runs;
CREATE CONSTRAINT TRIGGER tax_matching_runs_sealed_at_commit
    AFTER INSERT ON tax_matching_runs
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION tax_matching_runs_sealed_at_commit();

-- A child row is inserted only while its run is `building`, and is never updated or deleted. A statutory disposal
-- belongs to a `computed` security.
CREATE OR REPLACE FUNCTION tax_matching_child_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    run_state TEXT;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION '%: rows of a matching run are never updated or deleted', TG_TABLE_NAME;
    END IF;
    SELECT state INTO run_state FROM tax_matching_runs WHERE matching_run_id = NEW.matching_run_id FOR SHARE;
    IF run_state IS DISTINCT FROM 'building' THEN
        RAISE EXCEPTION '%: run % is % and takes no rows', TG_TABLE_NAME, NEW.matching_run_id,
            coalesce(run_state, 'missing');
    END IF;
    IF TG_TABLE_NAME = 'tax_statutory_disposals' THEN
        IF NOT EXISTS (
            SELECT 1 FROM tax_matching_securities
            WHERE matching_run_id = NEW.matching_run_id AND tax_security_id = NEW.tax_security_id
              AND status = 'computed'
        ) THEN
            RAISE EXCEPTION 'tax_statutory_disposals: security % of run % is not computed', NEW.tax_security_id,
                NEW.matching_run_id;
        END IF;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS tax_matching_instruments_guard ON tax_matching_instruments;
CREATE TRIGGER tax_matching_instruments_guard
    BEFORE INSERT OR UPDATE OR DELETE ON tax_matching_instruments
    FOR EACH ROW EXECUTE FUNCTION tax_matching_child_guard();

DROP TRIGGER IF EXISTS tax_matching_lot_classes_guard ON tax_matching_lot_classes;
CREATE TRIGGER tax_matching_lot_classes_guard
    BEFORE INSERT OR UPDATE OR DELETE ON tax_matching_lot_classes
    FOR EACH ROW EXECUTE FUNCTION tax_matching_child_guard();

DROP TRIGGER IF EXISTS tax_matching_securities_guard ON tax_matching_securities;
CREATE TRIGGER tax_matching_securities_guard
    BEFORE INSERT OR UPDATE OR DELETE ON tax_matching_securities
    FOR EACH ROW EXECUTE FUNCTION tax_matching_child_guard();

DROP TRIGGER IF EXISTS tax_matching_dependencies_guard ON tax_matching_dependencies;
CREATE TRIGGER tax_matching_dependencies_guard
    BEFORE INSERT OR UPDATE OR DELETE ON tax_matching_dependencies
    FOR EACH ROW EXECUTE FUNCTION tax_matching_child_guard();

DROP TRIGGER IF EXISTS tax_statutory_disposals_guard ON tax_statutory_disposals;
CREATE TRIGGER tax_statutory_disposals_guard
    BEFORE INSERT OR UPDATE OR DELETE ON tax_statutory_disposals
    FOR EACH ROW EXECUTE FUNCTION tax_matching_child_guard();

DROP TRIGGER IF EXISTS tax_identifications_guard ON tax_identifications;
CREATE TRIGGER tax_identifications_guard
    BEFORE INSERT OR UPDATE OR DELETE ON tax_identifications
    FOR EACH ROW EXECUTE FUNCTION tax_matching_child_guard();

COMMIT;
