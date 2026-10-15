-- 493_tax_fund_determinations.sql
--
-- #3616 slice 3d-3c-2 — reviewed fund determinations and their applicability runs (observations only).
-- Spec: docs/specs/metrics/2026-10-14-3616-slice3d3c2-fund-determination.md (Design 4). Writer:
-- `refresh_fund_determinations` (part B2); the pure format, rules and outcome order are `app.services.tax_fund_determination`.
--
-- Eight tables: the loaded determinations, their authority snapshots and links, the runs, each run's loads, the
-- unreviewed accessions the runs discover, and one observation per fund security per run with its document rows.
-- Every table is insert only except a run's single transition; a finished run is sealed. Matching reads none of them
-- in this slice, so no character, run or figure changes.

BEGIN;

-- The determination step stores the cited SEC documents and the CIK's filing list through `tax_fund_documents`.
ALTER TABLE tax_fund_fetches DROP CONSTRAINT IF EXISTS tax_fund_fetches_source_check;
ALTER TABLE tax_fund_fetches ADD CONSTRAINT tax_fund_fetches_source_check CHECK (source IN (
    'sec_ncen', 'gleif_record', 'gleif_isins', 'openfigi_mapping', 'hmrc_content_item', 'hmrc_reporting_list',
    'sec_governing_document', 'sec_submissions'));

-- A determination file as loaded: its id is `policy_id:cik:version`, the body agrees with the columns, and policy v1
-- holds one version per CIK. `file_sha256` is the committed file's bytes; a stored id never takes another hash.
CREATE TABLE IF NOT EXISTS tax_fund_determinations (
    determination_id  TEXT PRIMARY KEY,
    policy_id         TEXT NOT NULL CHECK (policy_id = 'fund_determination_v1'),
    cik               TEXT NOT NULL UNIQUE CHECK (cik ~ '^[0-9]{10}$'),
    version           INTEGER NOT NULL CHECK (version = 1),
    file_sha256       TEXT NOT NULL CHECK (file_sha256 ~ '^[0-9a-f]{64}$'),
    body              JSONB NOT NULL,
    loaded_at         TIMESTAMPTZ NOT NULL,
    CHECK (determination_id = policy_id || ':' || cik || ':' || version),
    -- JSON equality, so a number never stands in for a string or a boolean for the version.
    CHECK ((jsonb_typeof(body) = 'object'
            AND body -> 'determination_id' = to_jsonb(determination_id)
            AND body -> 'policy_id' = to_jsonb(policy_id)
            AND body -> 'cik' = to_jsonb(cik)
            AND body -> 'version' = to_jsonb(version)
            AND jsonb_typeof(body -> 'version') = 'number') IS TRUE)
);

CREATE TABLE IF NOT EXISTS tax_fund_authority_snapshots (
    content_sha256  TEXT PRIMARY KEY
                    CHECK (content_sha256 ~ '^[0-9a-f]{64}$' AND content_sha256 = encode(sha256(body), 'hex')),
    body            BYTEA NOT NULL CHECK (octet_length(body) > 0)
);

-- Which snapshot each determination's authority is; the writer checks the hash is the body's.
CREATE TABLE IF NOT EXISTS tax_fund_determination_authorities (
    determination_id  TEXT NOT NULL REFERENCES tax_fund_determinations (determination_id),
    authority_id      TEXT NOT NULL CHECK (authority_id ~ '^[a-z0-9_]+$'),
    content_sha256    TEXT NOT NULL REFERENCES tax_fund_authority_snapshots (content_sha256),
    PRIMARY KEY (determination_id, authority_id)
);

-- One per step invocation; the newest is the greatest run_id. `fund_observed_at` is the fund step's `observed_at` in
-- the same job run (NULL when that step failed or wrote nothing); `cutoff` is set on success, `failure` on failure.
CREATE TABLE IF NOT EXISTS tax_fund_determination_runs (
    run_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- NULL when the job's `job_runs` row could not be recorded.
    job_run_id        BIGINT REFERENCES job_runs (run_id),
    fund_observed_at  TIMESTAMPTZ,
    started_at        TIMESTAMPTZ NOT NULL,
    cutoff            TIMESTAMPTZ CHECK (cutoff >= started_at),
    status            TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    failure           TEXT CHECK (failure <> ''),
    CHECK ((cutoff IS NOT NULL) = (status = 'succeeded')),
    CHECK ((failure IS NOT NULL) = (status = 'failed'))
);

-- The run's active determinations: those whose file it loaded.
CREATE TABLE IF NOT EXISTS tax_fund_determination_run_loads (
    run_id            BIGINT NOT NULL REFERENCES tax_fund_determination_runs (run_id),
    determination_id  TEXT NOT NULL REFERENCES tax_fund_determinations (determination_id),
    PRIMARY KEY (run_id, determination_id)
);

-- A governing filing listed on or after `monitor_from` that the determination did not review. Permanent: it latches
-- staleness whatever later inventories show, and survives its run's later failure.
CREATE TABLE IF NOT EXISTS tax_fund_determination_unreviewed (
    determination_id      TEXT NOT NULL,
    accession             TEXT NOT NULL CHECK (accession ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$'),
    form                  TEXT NOT NULL CHECK (form <> ''),
    filed                 DATE NOT NULL,
    first_run_id          BIGINT NOT NULL,
    submissions_fetch_id  BIGINT NOT NULL REFERENCES tax_fund_fetches (fetch_id),
    PRIMARY KEY (determination_id, accession),
    FOREIGN KEY (first_run_id, determination_id)
        REFERENCES tax_fund_determination_run_loads (run_id, determination_id)
);

-- One per fund security per run (Design 2's first applying outcome). The first four outcomes carry no determination;
-- `binding_mismatch` onwards carry a refusal except `determined`, which alone carries the offshore answer.
CREATE TABLE IF NOT EXISTS tax_fund_determination_observations (
    observation_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                BIGINT NOT NULL REFERENCES tax_fund_determination_runs (run_id),
    policy_id             TEXT NOT NULL CHECK (policy_id <> ''),
    tax_security_id       BIGINT NOT NULL,
    fund_observation_id   BIGINT REFERENCES tax_fund_observations (observation_id),
    determination_id      TEXT REFERENCES tax_fund_determinations (determination_id),
    outcome               TEXT NOT NULL CHECK (outcome IN (
                              'fund_observation_unavailable', 'not_candidate', 'no_determination', 'binding_mismatch',
                              'documents_unavailable', 'document_changed', 'quote_unverified', 'review_out_of_date',
                              'coverage_unavailable', 'stale', 'determined')),
    refusal               TEXT CHECK (refusal <> ''),
    -- The CIK's newest filing-list fetch at the cutoff: NULL before `coverage_unavailable`, or when none exists.
    submissions_fetch_id  BIGINT REFERENCES tax_fund_fetches (fetch_id),
    offshore_fund         TEXT CHECK (offshore_fund IN ('true', 'false', 'unresolved')),
    offshore_basis        TEXT CHECK (offshore_basis IN ('established', 'assumed', 'unresolved')),
    UNIQUE (run_id, tax_security_id),
    CHECK ((fund_observation_id IS NULL) = (outcome = 'fund_observation_unavailable')),
    CHECK ((determination_id IS NULL) = (outcome IN (
        'fund_observation_unavailable', 'not_candidate', 'no_determination', 'binding_mismatch'))),
    CHECK ((refusal IS NULL) = (outcome IN (
        'fund_observation_unavailable', 'not_candidate', 'no_determination', 'determined'))),
    CHECK (submissions_fetch_id IS NULL OR outcome IN ('coverage_unavailable', 'stale', 'determined')),
    CHECK (submissions_fetch_id IS NOT NULL OR outcome NOT IN ('stale', 'determined')),
    CHECK ((offshore_fund IS NOT NULL) = (outcome = 'determined')),
    CHECK ((offshore_basis IS NOT NULL) = (outcome = 'determined'))
);

-- The fetch each cited document of a `determined` observation rests on.
CREATE TABLE IF NOT EXISTS tax_fund_determination_documents (
    observation_id  BIGINT NOT NULL REFERENCES tax_fund_determination_observations (observation_id),
    doc_id          TEXT NOT NULL,
    fetch_id        BIGINT NOT NULL REFERENCES tax_fund_fetches (fetch_id),
    PRIMARY KEY (observation_id, doc_id)
);

-- Every table is insert only (the shared sql/492 function); a run's transition has its own trigger below.
DROP TRIGGER IF EXISTS tax_fund_determinations_insert_only ON tax_fund_determinations;
CREATE TRIGGER tax_fund_determinations_insert_only
    BEFORE UPDATE OR DELETE ON tax_fund_determinations
    FOR EACH ROW EXECUTE FUNCTION tax_fund_insert_only();

DROP TRIGGER IF EXISTS tax_fund_authority_snapshots_insert_only ON tax_fund_authority_snapshots;
CREATE TRIGGER tax_fund_authority_snapshots_insert_only
    BEFORE UPDATE OR DELETE ON tax_fund_authority_snapshots
    FOR EACH ROW EXECUTE FUNCTION tax_fund_insert_only();

DROP TRIGGER IF EXISTS tax_fund_determination_authorities_insert_only ON tax_fund_determination_authorities;
CREATE TRIGGER tax_fund_determination_authorities_insert_only
    BEFORE UPDATE OR DELETE ON tax_fund_determination_authorities
    FOR EACH ROW EXECUTE FUNCTION tax_fund_insert_only();

DROP TRIGGER IF EXISTS tax_fund_determination_run_loads_insert_only ON tax_fund_determination_run_loads;
CREATE TRIGGER tax_fund_determination_run_loads_insert_only
    BEFORE UPDATE OR DELETE ON tax_fund_determination_run_loads
    FOR EACH ROW EXECUTE FUNCTION tax_fund_insert_only();

DROP TRIGGER IF EXISTS tax_fund_determination_unreviewed_insert_only ON tax_fund_determination_unreviewed;
CREATE TRIGGER tax_fund_determination_unreviewed_insert_only
    BEFORE UPDATE OR DELETE ON tax_fund_determination_unreviewed
    FOR EACH ROW EXECUTE FUNCTION tax_fund_insert_only();

DROP TRIGGER IF EXISTS tax_fund_determination_observations_insert_only ON tax_fund_determination_observations;
CREATE TRIGGER tax_fund_determination_observations_insert_only
    BEFORE UPDATE OR DELETE ON tax_fund_determination_observations
    FOR EACH ROW EXECUTE FUNCTION tax_fund_insert_only();

DROP TRIGGER IF EXISTS tax_fund_determination_documents_insert_only ON tax_fund_determination_documents;
CREATE TRIGGER tax_fund_determination_documents_insert_only
    BEFORE UPDATE OR DELETE ON tax_fund_determination_documents
    FOR EACH ROW EXECUTE FUNCTION tax_fund_insert_only();

-- A run is inserted `running` and moves once, to `succeeded` or `failed`, changing nothing else; it is never deleted.
CREATE OR REPLACE FUNCTION tax_fund_determination_runs_transition() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'tax_fund_determination_runs: a run is never deleted';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'running' THEN
            RAISE EXCEPTION 'tax_fund_determination_runs: a run starts running';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.status <> 'running' THEN
        RAISE EXCEPTION 'tax_fund_determination_runs: run % is finished (%)', OLD.run_id, OLD.status;
    END IF;
    IF NEW.status NOT IN ('succeeded', 'failed') THEN
        RAISE EXCEPTION 'tax_fund_determination_runs: a run moves only to succeeded or failed';
    END IF;
    IF (NEW.run_id, NEW.job_run_id, NEW.fund_observed_at, NEW.started_at)
       IS DISTINCT FROM (OLD.run_id, OLD.job_run_id, OLD.fund_observed_at, OLD.started_at) THEN
        RAISE EXCEPTION 'tax_fund_determination_runs: a transition changes only status, cutoff and failure';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS tax_fund_determination_runs_transition ON tax_fund_determination_runs;
CREATE TRIGGER tax_fund_determination_runs_transition
    BEFORE INSERT OR UPDATE OR DELETE ON tax_fund_determination_runs
    FOR EACH ROW EXECUTE FUNCTION tax_fund_determination_runs_transition();

-- A child row is added only under a running run. FOR SHARE makes a concurrent transition wait for the child's
-- transaction, and a child wait for an in-flight transition and then see its outcome.
CREATE OR REPLACE FUNCTION tax_fund_determination_run_open(child TEXT, rid BIGINT) RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
    state TEXT;
BEGIN
    SELECT status INTO state FROM tax_fund_determination_runs WHERE run_id = rid FOR SHARE;
    IF state IS DISTINCT FROM 'running' THEN
        RAISE EXCEPTION '%: run % is not running (%)', child, rid, coalesce(state, 'absent');
    END IF;
END $$;

-- The filing-list URL of a CIK, as the step requests it (`tax_fund_determination.SUBMISSIONS_URL`).
CREATE OR REPLACE FUNCTION tax_fund_submissions_url(cik TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT 'https://data.sec.gov/submissions/CIK' || cik || '.json'
$$;

-- A fetch id names a filing-list fetch of the determination's CIK; returns its fetch.
CREATE OR REPLACE FUNCTION tax_fund_submissions_fetch(child TEXT, fid BIGINT, determination TEXT)
RETURNS tax_fund_fetches
LANGUAGE plpgsql AS $$
DECLARE
    f tax_fund_fetches%ROWTYPE;
BEGIN
    SELECT * INTO f FROM tax_fund_fetches WHERE fetch_id = fid;
    IF f.source IS DISTINCT FROM 'sec_submissions' OR f.request_key IS DISTINCT FROM tax_fund_submissions_url(
        (SELECT cik FROM tax_fund_determinations WHERE determination_id = determination)) THEN
        RAISE EXCEPTION '%: fetch % is not the filing list of %''s CIK', child, fid, determination;
    END IF;
    RETURN f;
END $$;

CREATE OR REPLACE FUNCTION tax_fund_determination_run_loads_check() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM tax_fund_determination_run_open(TG_TABLE_NAME, NEW.run_id);
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS tax_fund_determination_run_loads_check ON tax_fund_determination_run_loads;
CREATE TRIGGER tax_fund_determination_run_loads_check
    BEFORE INSERT ON tax_fund_determination_run_loads
    FOR EACH ROW EXECUTE FUNCTION tax_fund_determination_run_loads_check();

-- A discovery is found by a running run that loaded the determination (the composite FK), in a filing list of the
-- determination's CIK, and is dated no later than that fetch's UTC date.
CREATE OR REPLACE FUNCTION tax_fund_determination_unreviewed_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    f tax_fund_fetches%ROWTYPE;
BEGIN
    PERFORM tax_fund_determination_run_open(TG_TABLE_NAME, NEW.first_run_id);
    f := tax_fund_submissions_fetch(TG_TABLE_NAME, NEW.submissions_fetch_id, NEW.determination_id);
    IF NEW.filed > (f.fetched_at AT TIME ZONE 'UTC')::DATE THEN
        RAISE EXCEPTION '%: % is dated after its filing list''s fetch date', TG_TABLE_NAME, NEW.accession;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS tax_fund_determination_unreviewed_check ON tax_fund_determination_unreviewed;
CREATE TRIGGER tax_fund_determination_unreviewed_check
    BEFORE INSERT ON tax_fund_determination_unreviewed
    FOR EACH ROW EXECUTE FUNCTION tax_fund_determination_unreviewed_check();

-- An observation's determination is of its policy and loaded by its run; its fund observation is of the same
-- security, at the run's `fund_observed_at`, under the one identity policy v1 accepts; its filing list is the CIK's.
CREATE OR REPLACE FUNCTION tax_fund_determination_observations_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    fund  tax_fund_observations%ROWTYPE;
BEGIN
    PERFORM tax_fund_determination_run_open(TG_TABLE_NAME, NEW.run_id);
    IF NEW.determination_id IS NOT NULL THEN
        IF NEW.policy_id IS DISTINCT FROM (
            SELECT policy_id FROM tax_fund_determinations WHERE determination_id = NEW.determination_id) THEN
            RAISE EXCEPTION '%: policy % is not the determination''s', TG_TABLE_NAME, NEW.policy_id;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM tax_fund_determination_run_loads
                       WHERE run_id = NEW.run_id AND determination_id = NEW.determination_id) THEN
            RAISE EXCEPTION '%: run % did not load %', TG_TABLE_NAME, NEW.run_id, NEW.determination_id;
        END IF;
    END IF;
    IF NEW.fund_observation_id IS NOT NULL THEN
        SELECT * INTO fund FROM tax_fund_observations WHERE observation_id = NEW.fund_observation_id;
        IF fund.tax_security_id IS DISTINCT FROM NEW.tax_security_id THEN
            RAISE EXCEPTION '%: fund observation % is of another security', TG_TABLE_NAME, NEW.fund_observation_id;
        END IF;
        IF fund.observed_at IS DISTINCT FROM (
            SELECT fund_observed_at FROM tax_fund_determination_runs WHERE run_id = NEW.run_id) THEN
            RAISE EXCEPTION '%: fund observation % is not at the run''s fund_observed_at', TG_TABLE_NAME,
                NEW.fund_observation_id;
        END IF;
        IF fund.policy_id IS DISTINCT FROM 'fund_identity_v1' THEN
            RAISE EXCEPTION '%: fund observation % is under %, not fund_identity_v1', TG_TABLE_NAME,
                NEW.fund_observation_id, fund.policy_id;
        END IF;
    END IF;
    IF NEW.submissions_fetch_id IS NOT NULL THEN
        PERFORM tax_fund_submissions_fetch(TG_TABLE_NAME, NEW.submissions_fetch_id, NEW.determination_id);
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS tax_fund_determination_observations_check ON tax_fund_determination_observations;
CREATE TRIGGER tax_fund_determination_observations_check
    BEFORE INSERT ON tax_fund_determination_observations
    FOR EACH ROW EXECUTE FUNCTION tax_fund_determination_observations_check();

-- A document row is of a `determined` observation, names one of its determination's documents, and rests on a
-- governing-document fetch of that document's URL. The writer checks the fetch's content hash is the body's.
CREATE OR REPLACE FUNCTION tax_fund_determination_documents_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    obs      tax_fund_determination_observations%ROWTYPE;
    doc_url  TEXT;
BEGIN
    SELECT * INTO obs FROM tax_fund_determination_observations WHERE observation_id = NEW.observation_id;
    PERFORM tax_fund_determination_run_open(TG_TABLE_NAME, obs.run_id);
    IF obs.outcome IS DISTINCT FROM 'determined' THEN
        RAISE EXCEPTION '%: observation % is not determined', TG_TABLE_NAME, NEW.observation_id;
    END IF;
    SELECT d ->> 'url' INTO doc_url
    FROM tax_fund_determinations AS t, jsonb_array_elements(t.body -> 'documents') AS d
    WHERE t.determination_id = obs.determination_id AND d -> 'doc_id' = to_jsonb(NEW.doc_id);
    IF doc_url IS NULL THEN
        RAISE EXCEPTION '%: % is not a document of %', TG_TABLE_NAME, NEW.doc_id, obs.determination_id;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM tax_fund_fetches
                   WHERE fetch_id = NEW.fetch_id AND source = 'sec_governing_document' AND request_key = doc_url) THEN
        RAISE EXCEPTION '%: fetch % is not a governing-document fetch of %', TG_TABLE_NAME, NEW.fetch_id, NEW.doc_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS tax_fund_determination_documents_check ON tax_fund_determination_documents;
CREATE TRIGGER tax_fund_determination_documents_check
    BEFORE INSERT ON tax_fund_determination_documents
    FOR EACH ROW EXECUTE FUNCTION tax_fund_determination_documents_check();

-- At commit, a `determined` observation has exactly one document row per document of its determination.
CREATE OR REPLACE FUNCTION tax_fund_determination_documents_complete() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.outcome = 'determined' AND (
        SELECT count(*) FROM tax_fund_determination_documents WHERE observation_id = NEW.observation_id
    ) IS DISTINCT FROM (
        SELECT jsonb_array_length(body -> 'documents') FROM tax_fund_determinations
        WHERE determination_id = NEW.determination_id
    ) THEN
        RAISE EXCEPTION '%: determined observation % lacks a document row per document', TG_TABLE_NAME,
            NEW.observation_id;
    END IF;
    RETURN NULL;
END $$;

DROP TRIGGER IF EXISTS tax_fund_determination_documents_complete ON tax_fund_determination_observations;
CREATE CONSTRAINT TRIGGER tax_fund_determination_documents_complete
    AFTER INSERT ON tax_fund_determination_observations
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION tax_fund_determination_documents_complete();

COMMIT;
