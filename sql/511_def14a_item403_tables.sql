-- 511_def14a_item403_tables.sql
--
-- #3899 slice 2: the #2351 recipient job's Item 403 table evidence, cached so the job
-- never needs a retained DEF 14A body. Spec: docs/specs/etl/2026-10-21-3899-def14a-pre14a-body-sweep.md
-- (D3).
--
-- tables = sec_def14a.item403_table_htmls(body): the parser's own table selection, which
-- changes with the parser, hence parser_version in the key. body_sha256 is the SHA-256 of
-- exactly the bytes the tables were extracted from, so a re-stored body under the same
-- parser version is a different key. Rows are derived and immutable per key: a miss is
-- recomputed from a verified body, and nothing ever updates a row.

CREATE TABLE IF NOT EXISTS def14a_item403_tables (
    accession_number TEXT        NOT NULL,
    parser_version   TEXT        NOT NULL,
    body_sha256      TEXT        NOT NULL CHECK (body_sha256 ~ '^[0-9a-f]{64}$'),
    tables           TEXT[]      NOT NULL,
    computed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (accession_number, parser_version, body_sha256)
);
