-- 492_tax_fund_evidence.sql
--
-- #3616 slice 3d-3c-1 — fund evidence and candidate identity (observations only).
-- Spec: docs/specs/metrics/2026-10-14-3616-slice3d3c1-fund-identity.md (Design 3). Writer: `refresh_fund_evidence`
-- (part B2); the pure readers and decisions are `app.services.tax_fund_evidence`.
--
-- Five insert-only tables: the public documents fetched, each fetch, each list version's assessment, each list
-- search, and one observation per fund security per step run. Matching reads none of them in this slice, so no
-- character, run or figure changes. A trigger refuses every update and delete.

BEGIN;

-- ISO 6166: the shape, then Luhn over the digit string with letters as 10 … 35 (the check digit included, so a
-- valid ISIN sums to 0 mod 10). Mirrors `tax_fund_evidence.valid_isin`.
CREATE OR REPLACE FUNCTION tax_fund_valid_isin(value TEXT) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    digits TEXT := '';
    total  INTEGER := 0;
    d      INTEGER;
    i      INTEGER;
BEGIN
    IF value !~ '^[A-Z]{2}[A-Z0-9]{9}[0-9]$' THEN
        RETURN FALSE;
    END IF;
    FOR i IN 1 .. 12 LOOP
        d := position(substr(value, i, 1) IN '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ') - 1;
        digits := digits || d::TEXT;
    END LOOP;
    FOR i IN 0 .. length(digits) - 1 LOOP
        d := substr(digits, length(digits) - i, 1)::INTEGER * (CASE WHEN i % 2 = 1 THEN 2 ELSE 1 END);
        total := total + d / 10 + d % 10;
    END LOOP;
    RETURN total % 10 = 0;
END $$;

-- A canonical name-key set: a one-dimensional, 1-based, non-empty array of `[A-Z0-9]+` keys (so no NULL or empty
-- element), strictly ascending by code point (so no duplicate). Name keys are ASCII by construction (spec Design 2).
CREATE OR REPLACE FUNCTION tax_fund_name_keys_canonical(keys TEXT[]) RETURNS BOOLEAN
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT cardinality(keys) > 0 AND array_ndims(keys) = 1 AND array_lower(keys, 1) = 1
       AND NOT EXISTS (
           SELECT 1 FROM unnest(keys) WITH ORDINALITY AS k (key, n)
           WHERE key IS NULL OR key !~ '^[A-Z0-9]+$'
              OR (n > 1 AND (keys[n - 1] COLLATE "C" < key COLLATE "C") IS NOT TRUE))
$$;

-- GOV.UK's content item, sound per Design 1's table: its single `.ods` attachment object, or NULL when the body is
-- not a JSON object, `public_updated_at` is not RFC 3339 with a zone or is after `fetched_at`, or the attachments do
-- not hold exactly one `.ods` object on the asset host (HTTPS, no credentials, port, query or fragment) with a
-- positive integer `file_size` of at most ten digits (a body is at most 32 MiB, and the list check casts it). A
-- backstop for `tax_fund_evidence.read_content_item`, which decides first.
CREATE OR REPLACE FUNCTION tax_fund_content_attachment(body BYTEA, fetched_at TIMESTAMPTZ) RETURNS JSONB
LANGUAGE plpgsql STABLE STRICT AS $$
DECLARE
    item      JSONB;
    published TEXT;
    ods       JSONB[];
BEGIN
    BEGIN
        item := convert_from(body, 'UTF8')::JSONB;
    EXCEPTION WHEN OTHERS THEN
        RETURN NULL;
    END;
    IF jsonb_typeof(item) IS DISTINCT FROM 'object' OR jsonb_typeof(item -> 'public_updated_at') IS DISTINCT FROM 'string'
    THEN
        RETURN NULL;
    END IF;
    published := item ->> 'public_updated_at';
    IF published !~ ('^[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.[0-9]+)?'
                     '(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$') THEN
        RETURN NULL;
    END IF;
    BEGIN
        IF published::TIMESTAMPTZ > fetched_at THEN
            RETURN NULL;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        RETURN NULL;
    END;
    IF jsonb_typeof(item -> 'details') IS DISTINCT FROM 'object'
       OR jsonb_typeof(item -> 'details' -> 'attachments') IS DISTINCT FROM 'array' THEN
        RETURN NULL;
    END IF;
    SELECT coalesce(array_agg(a), '{}') INTO ods
    FROM jsonb_array_elements(item -> 'details' -> 'attachments') AS a
    WHERE jsonb_typeof(a) = 'object' AND jsonb_typeof(a -> 'url') = 'string' AND a ->> 'url' LIKE '%.ods';
    IF cardinality(ods) <> 1
       OR ods[1] ->> 'url' !~ '^https://assets\.publishing\.service\.gov\.uk/[^?#@:]+\.ods$'
       OR jsonb_typeof(ods[1] -> 'file_size') IS DISTINCT FROM 'number'
       OR ods[1] ->> 'file_size' !~ '^[1-9][0-9]{0,9}$' THEN
        RETURN NULL;
    END IF;
    RETURN ods[1];
END $$;

CREATE TABLE IF NOT EXISTS tax_fund_documents (
    body_sha256  TEXT PRIMARY KEY
                 CHECK (body_sha256 ~ '^[0-9a-f]{64}$' AND body_sha256 = encode(sha256(body), 'hex')),
    body         BYTEA NOT NULL CHECK (octet_length(body) BETWEEN 1 AND 33554432)
);

CREATE TABLE IF NOT EXISTS tax_fund_fetches (
    fetch_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source           TEXT NOT NULL CHECK (source IN (
                         'sec_ncen', 'gleif_record', 'gleif_isins', 'openfigi_mapping', 'hmrc_content_item',
                         'hmrc_reporting_list')),
    request_key      TEXT NOT NULL CHECK (request_key <> ''),
    url              TEXT NOT NULL CHECK (url <> ''),
    fetched_at       TIMESTAMPTZ NOT NULL,
    body_sha256      TEXT NOT NULL REFERENCES tax_fund_documents (body_sha256),
    -- A list's content item; NULL for every other source.
    parent_fetch_id  BIGINT UNIQUE REFERENCES tax_fund_fetches (fetch_id),
    -- OpenFIGI's reset header as received.
    rate_reset       TEXT,
    CHECK ((parent_fetch_id IS NOT NULL) = (source = 'hmrc_reporting_list'))
);

-- N-CEN documents are fetched once per accession.
CREATE UNIQUE INDEX IF NOT EXISTS tax_fund_fetches_one_ncen
    ON tax_fund_fetches (request_key) WHERE source = 'sec_ncen';

CREATE INDEX IF NOT EXISTS tax_fund_fetches_newest
    ON tax_fund_fetches (source, request_key, fetched_at DESC, fetch_id DESC);

-- The assessments: a list fetch with no row for a policy is pending.
CREATE TABLE IF NOT EXISTS tax_fund_list_versions (
    list_fetch_id  BIGINT NOT NULL REFERENCES tax_fund_fetches (fetch_id),
    policy_id      TEXT NOT NULL CHECK (policy_id <> ''),
    usable         BOOLEAN NOT NULL,
    data_rows      INTEGER CHECK (data_rows BETWEEN 1 AND 1000000),
    refusal        TEXT CHECK (refusal <> ''),
    -- The size floor's basis; NULL for the bootstrap floor.
    baseline_rows  INTEGER CHECK (baseline_rows BETWEEN 1 AND 1000000),
    assessed_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (list_fetch_id, policy_id),
    CHECK (usable = (data_rows IS NOT NULL) AND usable = (refusal IS NULL))
);

CREATE TABLE IF NOT EXISTS tax_fund_list_searches (
    policy_id                  TEXT NOT NULL CHECK (policy_id <> ''),
    isin                       TEXT NOT NULL CHECK (tax_fund_valid_isin(isin)),
    name_keys                  TEXT[] NOT NULL CHECK (tax_fund_name_keys_canonical(name_keys)),
    list_fetch_id              BIGINT NOT NULL,
    -- Completion time.
    searched_at                TIMESTAMPTZ NOT NULL,
    identifier_count           INTEGER NOT NULL CHECK (identifier_count >= 0),
    identifier_in_text_count   INTEGER NOT NULL CHECK (identifier_in_text_count >= 0),
    near_identifier_count      INTEGER NOT NULL CHECK (near_identifier_count >= 0),
    name_count                 INTEGER NOT NULL CHECK (name_count >= 0),
    -- Per method, the first least(count, 25) matching rows in row order: {"method", "key", "cells"}.
    examples                   JSONB NOT NULL,
    PRIMARY KEY (policy_id, isin, name_keys, list_fetch_id),
    FOREIGN KEY (list_fetch_id, policy_id) REFERENCES tax_fund_list_versions (list_fetch_id, policy_id)
);

CREATE TABLE IF NOT EXISTS tax_fund_observations (
    observation_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- The source cutoff, read after every fetch was tried.
    observed_at           TIMESTAMPTZ NOT NULL,
    policy_id             TEXT NOT NULL CHECK (policy_id <> ''),
    evidence_run_id       BIGINT NOT NULL REFERENCES tax_evidence_runs (run_id),
    tax_security_id       BIGINT NOT NULL,
    member_ids            BIGINT[] NOT NULL CHECK (cardinality(member_ids) > 0),
    symbol                TEXT,
    outcome               TEXT NOT NULL CHECK (outcome IN (
                              'candidate', 'cik_unestablished', 'registration_metadata_conflict',
                              'registration_unavailable', 'registration_amended', 'registration_unusable',
                              'registration_final', 'fund_listing_unmatched', 'lei_record_unavailable',
                              'lei_record_unsupported', 'isin_unavailable', 'isin_set_unsupported',
                              'isin_set_unusable', 'isin_mapping_unavailable', 'isin_unresolved',
                              'isin_unestablished')),
    -- The evaluated inputs: kept as far as evaluation reached, NULL beyond.
    cik                   TEXT CHECK (cik ~ '^[0-9]{10}$'),
    newest_period         DATE,
    amendment_accessions  TEXT[],
    ncen_accession        TEXT,
    ncen_report_date      DATE,
    -- As stored in `filing_events`.
    ncen_source_url       TEXT,
    ncen_fetch_id         BIGINT REFERENCES tax_fund_fetches (fetch_id),
    lei                   TEXT,
    record_fetch_id       BIGINT REFERENCES tax_fund_fetches (fetch_id),
    isins_fetch_id        BIGINT REFERENCES tax_fund_fetches (fetch_id),
    figi_fetch_id         BIGINT REFERENCES tax_fund_fetches (fetch_id),
    -- The N-CEN listings as [mic, ticker] pairs, tickers as filed.
    listings              JSONB,
    mic                   TEXT,
    isin_set              TEXT[],
    trustee_countries     TEXT[],
    registrant_country    TEXT,
    short_period          TEXT,
    -- The bound ISIN and the fund's name keys: a candidate's search key.
    isin                  TEXT,
    name_keys             TEXT[],
    search_outcome        TEXT CHECK (search_outcome IN ('listed', 'unresolved', 'unavailable', 'not_found')),
    -- The newest content fetch at observed_at, and its child list.
    content_fetch_id      BIGINT REFERENCES tax_fund_fetches (fetch_id),
    list_fetch_id         BIGINT REFERENCES tax_fund_fetches (fetch_id),
    refusal               TEXT,
    -- When the decision completed.
    evaluated_at          TIMESTAMPTZ NOT NULL CHECK (evaluated_at >= observed_at),
    -- A candidate's per-list-fetch coverage at evaluation: [{"list_fetch_id", "state"}] in fetch-id order.
    coverage              JSONB,
    UNIQUE (observed_at, tax_security_id),
    CHECK ((outcome <> 'candidate' OR (
        num_nulls(cik, ncen_accession, ncen_report_date, ncen_fetch_id, lei, record_fetch_id, isins_fetch_id,
                  figi_fetch_id, listings, mic, isin_set, isin, name_keys, search_outcome, coverage) = 0
        AND isin = ANY (isin_set)
        AND tax_fund_name_keys_canonical(name_keys)
    )) IS TRUE),
    CHECK ((outcome = 'candidate' OR
        num_nonnulls(isin, name_keys, search_outcome, content_fetch_id, list_fetch_id, coverage) = 0) IS TRUE),
    CHECK (list_fetch_id IS NULL OR content_fetch_id IS NOT NULL),
    CHECK ((search_outcome IS DISTINCT FROM 'not_found') OR list_fetch_id IS NOT NULL)
);

-- Every table is insert only.
CREATE OR REPLACE FUNCTION tax_fund_insert_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '%: rows are never updated or deleted', TG_TABLE_NAME;
END $$;

DROP TRIGGER IF EXISTS tax_fund_documents_insert_only ON tax_fund_documents;
CREATE TRIGGER tax_fund_documents_insert_only
    BEFORE UPDATE OR DELETE ON tax_fund_documents
    FOR EACH ROW EXECUTE FUNCTION tax_fund_insert_only();

DROP TRIGGER IF EXISTS tax_fund_fetches_insert_only ON tax_fund_fetches;
CREATE TRIGGER tax_fund_fetches_insert_only
    BEFORE UPDATE OR DELETE ON tax_fund_fetches
    FOR EACH ROW EXECUTE FUNCTION tax_fund_insert_only();

DROP TRIGGER IF EXISTS tax_fund_list_versions_insert_only ON tax_fund_list_versions;
CREATE TRIGGER tax_fund_list_versions_insert_only
    BEFORE UPDATE OR DELETE ON tax_fund_list_versions
    FOR EACH ROW EXECUTE FUNCTION tax_fund_insert_only();

DROP TRIGGER IF EXISTS tax_fund_list_searches_insert_only ON tax_fund_list_searches;
CREATE TRIGGER tax_fund_list_searches_insert_only
    BEFORE UPDATE OR DELETE ON tax_fund_list_searches
    FOR EACH ROW EXECUTE FUNCTION tax_fund_insert_only();

DROP TRIGGER IF EXISTS tax_fund_observations_insert_only ON tax_fund_observations;
CREATE TRIGGER tax_fund_observations_insert_only
    BEFORE UPDATE OR DELETE ON tax_fund_observations
    FOR EACH ROW EXECUTE FUNCTION tax_fund_insert_only();

-- A content item is stored only when sound. A list's parent is a content item, its request key and URL are that
-- item's attachment URL, and its body is exactly the attachment's `file_size` bytes.
CREATE OR REPLACE FUNCTION tax_fund_fetches_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    parent      tax_fund_fetches%ROWTYPE;
    attachment  JSONB;
BEGIN
    IF NEW.source = 'hmrc_content_item' THEN
        IF tax_fund_content_attachment(
               (SELECT body FROM tax_fund_documents WHERE body_sha256 = NEW.body_sha256), NEW.fetched_at) IS NULL THEN
            RAISE EXCEPTION 'tax_fund_fetches: content item % is not sound', NEW.body_sha256;
        END IF;
    ELSIF NEW.source = 'hmrc_reporting_list' THEN
        SELECT * INTO parent FROM tax_fund_fetches WHERE fetch_id = NEW.parent_fetch_id;
        IF parent.source IS DISTINCT FROM 'hmrc_content_item' THEN
            RAISE EXCEPTION 'tax_fund_fetches: a list''s parent % is not a content item', NEW.parent_fetch_id;
        END IF;
        attachment := tax_fund_content_attachment(
            (SELECT body FROM tax_fund_documents WHERE body_sha256 = parent.body_sha256), parent.fetched_at);
        IF NEW.request_key IS DISTINCT FROM attachment ->> 'url' OR NEW.url IS DISTINCT FROM attachment ->> 'url' THEN
            RAISE EXCEPTION 'tax_fund_fetches: a list''s request key and URL are its parent''s attachment URL';
        END IF;
        IF (SELECT octet_length(body) FROM tax_fund_documents WHERE body_sha256 = NEW.body_sha256)
           IS DISTINCT FROM (attachment ->> 'file_size')::BIGINT THEN
            RAISE EXCEPTION 'tax_fund_fetches: a list is exactly its parent''s file_size bytes';
        END IF;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS tax_fund_fetches_check ON tax_fund_fetches;
CREATE TRIGGER tax_fund_fetches_check
    BEFORE INSERT ON tax_fund_fetches
    FOR EACH ROW EXECUTE FUNCTION tax_fund_fetches_check();

CREATE OR REPLACE FUNCTION tax_fund_list_versions_check() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF (SELECT source FROM tax_fund_fetches WHERE fetch_id = NEW.list_fetch_id)
       IS DISTINCT FROM 'hmrc_reporting_list' THEN
        RAISE EXCEPTION 'tax_fund_list_versions: fetch % is not a reporting list', NEW.list_fetch_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS tax_fund_list_versions_check ON tax_fund_list_versions;
CREATE TRIGGER tax_fund_list_versions_check
    BEFORE INSERT ON tax_fund_list_versions
    FOR EACH ROW EXECUTE FUNCTION tax_fund_list_versions_check();

-- An example is an object with exactly `method` (one of the four), `key` (a string) and `cells` (nine strings).
-- Nested CASEs, because SQL does not promise to short-circuit AND and the JSON functions raise on the wrong type.
CREATE OR REPLACE FUNCTION tax_fund_example_shape(example JSONB) RETURNS BOOLEAN
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT CASE WHEN jsonb_typeof(example) <> 'object' THEN FALSE
                WHEN (SELECT coalesce(array_agg(k ORDER BY k COLLATE "C"), '{}') FROM jsonb_object_keys(example) AS k)
                     <> ARRAY['cells', 'key', 'method'] THEN FALSE
                WHEN jsonb_typeof(example -> 'method') <> 'string' OR jsonb_typeof(example -> 'key') <> 'string'
                     OR jsonb_typeof(example -> 'cells') <> 'array' THEN FALSE
                ELSE example ->> 'method' IN ('identifier', 'identifier_in_text', 'near_identifier', 'name')
                     AND jsonb_array_length(example -> 'cells') = 9
                     AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(example -> 'cells') AS c
                                     WHERE jsonb_typeof(c) <> 'string')
           END
$$;

-- A search is of a usable version; its counts sum to no more than the version's rows; its examples are well formed,
-- keyed by a target (the ISIN, or a US/CA ISIN's NSIN) or, for `name`, a name key, and number exactly
-- least(count, 25) per method.
CREATE OR REPLACE FUNCTION tax_fund_list_searches_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    version  tax_fund_list_versions%ROWTYPE;
    targets  TEXT[];
BEGIN
    SELECT * INTO version FROM tax_fund_list_versions
    WHERE list_fetch_id = NEW.list_fetch_id AND policy_id = NEW.policy_id;
    IF version.usable IS NOT TRUE THEN
        RAISE EXCEPTION 'tax_fund_list_searches: version % is not usable under %', NEW.list_fetch_id, NEW.policy_id;
    END IF;
    IF NEW.identifier_count::BIGINT + NEW.identifier_in_text_count + NEW.near_identifier_count + NEW.name_count
       > version.data_rows THEN
        RAISE EXCEPTION 'tax_fund_list_searches: counts sum past the version''s % rows', version.data_rows;
    END IF;
    IF jsonb_typeof(NEW.examples) <> 'array' THEN
        RAISE EXCEPTION 'tax_fund_list_searches: examples are a JSON array';
    END IF;
    IF EXISTS (SELECT 1 FROM jsonb_array_elements(NEW.examples) AS e WHERE tax_fund_example_shape(e) IS NOT TRUE) THEN
        RAISE EXCEPTION 'tax_fund_list_searches: an example is not {method, key, nine cells}';
    END IF;
    targets := ARRAY[NEW.isin]
        || CASE WHEN left(NEW.isin, 2) IN ('US', 'CA') THEN ARRAY[substr(NEW.isin, 3, 9)] ELSE '{}'::TEXT[] END;
    IF EXISTS (
        SELECT 1 FROM jsonb_array_elements(NEW.examples) AS e
        WHERE CASE WHEN e ->> 'method' = 'name' THEN e ->> 'key' <> ALL (NEW.name_keys)
                   ELSE e ->> 'key' <> ALL (targets) END
    ) THEN
        RAISE EXCEPTION 'tax_fund_list_searches: an example''s key is not a target or name key of the search';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM (VALUES ('identifier', NEW.identifier_count), ('identifier_in_text', NEW.identifier_in_text_count),
                     ('near_identifier', NEW.near_identifier_count), ('name', NEW.name_count)) AS m (method, n)
        WHERE (SELECT count(*) FROM jsonb_array_elements(NEW.examples) AS e WHERE e ->> 'method' = m.method)
              <> least(m.n, 25)
    ) THEN
        RAISE EXCEPTION 'tax_fund_list_searches: each method keeps exactly least(count, 25) examples';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS tax_fund_list_searches_check ON tax_fund_list_searches;
CREATE TRIGGER tax_fund_list_searches_check
    BEFORE INSERT ON tax_fund_list_searches
    FOR EACH ROW EXECUTE FUNCTION tax_fund_list_searches_check();

-- Each fetch id names its source, was stored by `observed_at`, and has this observation's request key; a candidate's
-- content fetch is the newest at `observed_at` and its list that fetch's child; a `not_found` list is usable; and a
-- candidate's coverage is exactly the stored state of every list fetch at `observed_at`, in fetch-id order.
CREATE OR REPLACE FUNCTION tax_fund_observations_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    ref       RECORD;
    f         tax_fund_fetches%ROWTYPE;
    expected  JSONB;
BEGIN
    FOR ref IN
        SELECT * FROM (VALUES
            ('ncen_fetch_id', NEW.ncen_fetch_id, 'sec_ncen', NEW.ncen_accession),
            ('record_fetch_id', NEW.record_fetch_id, 'gleif_record', NEW.lei),
            ('isins_fetch_id', NEW.isins_fetch_id, 'gleif_isins', NEW.lei),
            ('figi_fetch_id', NEW.figi_fetch_id, 'openfigi_mapping', NULL),
            ('content_fetch_id', NEW.content_fetch_id, 'hmrc_content_item', NULL),
            ('list_fetch_id', NEW.list_fetch_id, 'hmrc_reporting_list', NULL)
        ) AS r (col, id, source, request_key)
        WHERE r.id IS NOT NULL
    LOOP
        SELECT * INTO f FROM tax_fund_fetches WHERE fetch_id = ref.id;
        IF f.source IS DISTINCT FROM ref.source THEN
            RAISE EXCEPTION 'tax_fund_observations: % % is not a % fetch', ref.col, ref.id, ref.source;
        END IF;
        IF f.fetched_at > NEW.observed_at THEN
            RAISE EXCEPTION 'tax_fund_observations: % % was fetched after observed_at', ref.col, ref.id;
        END IF;
        IF ref.request_key IS NOT NULL OR ref.source IN ('sec_ncen', 'gleif_record', 'gleif_isins') THEN
            IF f.request_key IS DISTINCT FROM ref.request_key THEN
                RAISE EXCEPTION 'tax_fund_observations: % % is for another request key', ref.col, ref.id;
            END IF;
        END IF;
        IF ref.source = 'openfigi_mapping' AND (f.request_key::JSONB IS DISTINCT FROM (
            SELECT jsonb_agg(jsonb_build_object('idType', 'ID_ISIN', 'idValue', i, 'micCode', NEW.mic) ORDER BY n)
            FROM unnest(NEW.isin_set) WITH ORDINALITY AS s (i, n)
        )) THEN
            RAISE EXCEPTION 'tax_fund_observations: figi_fetch_id % is for other jobs', ref.id;
        END IF;
    END LOOP;

    IF NEW.outcome <> 'candidate' THEN
        RETURN NEW;
    END IF;

    IF NEW.content_fetch_id IS DISTINCT FROM (
        SELECT fetch_id FROM tax_fund_fetches
        WHERE source = 'hmrc_content_item' AND fetched_at <= NEW.observed_at
        ORDER BY fetched_at DESC, fetch_id DESC LIMIT 1
    ) THEN
        RAISE EXCEPTION 'tax_fund_observations: content_fetch_id is not the newest content fetch at observed_at';
    END IF;
    IF NEW.list_fetch_id IS DISTINCT FROM (
        SELECT fetch_id FROM tax_fund_fetches WHERE parent_fetch_id = NEW.content_fetch_id AND fetched_at <= NEW.observed_at
    ) THEN
        RAISE EXCEPTION 'tax_fund_observations: list_fetch_id is not the content fetch''s child';
    END IF;
    IF NEW.search_outcome = 'not_found' AND NOT EXISTS (
        SELECT 1 FROM tax_fund_list_versions
        WHERE list_fetch_id = NEW.list_fetch_id AND policy_id = NEW.policy_id AND usable
    ) THEN
        RAISE EXCEPTION 'tax_fund_observations: not_found needs a usable list';
    END IF;

    SELECT coalesce(jsonb_agg(jsonb_build_object(
               'list_fetch_id', l.fetch_id,
               'state', CASE WHEN v.list_fetch_id IS NULL THEN 'pending'
                             WHEN NOT v.usable THEN 'unusable'
                             WHEN s.list_fetch_id IS NULL THEN 'usable_unsearched'
                             ELSE 'searched' END) ORDER BY l.fetch_id), '[]'::JSONB)
    INTO expected
    FROM tax_fund_fetches AS l
    LEFT JOIN tax_fund_list_versions AS v ON v.list_fetch_id = l.fetch_id AND v.policy_id = NEW.policy_id
    LEFT JOIN tax_fund_list_searches AS s
           ON s.list_fetch_id = l.fetch_id AND s.policy_id = NEW.policy_id AND s.isin = NEW.isin
          AND s.name_keys = NEW.name_keys
    WHERE l.source = 'hmrc_reporting_list' AND l.fetched_at <= NEW.observed_at;
    IF NEW.coverage IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'tax_fund_observations: coverage % is not the stored state %', NEW.coverage, expected;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS tax_fund_observations_check ON tax_fund_observations;
CREATE TRIGGER tax_fund_observations_check
    BEFORE INSERT ON tax_fund_observations
    FOR EACH ROW EXECUTE FUNCTION tax_fund_observations_check();

COMMIT;
