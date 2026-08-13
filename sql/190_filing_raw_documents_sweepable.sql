-- 190_filing_raw_documents_sweepable.sql
--
-- #1014 — raw-payload retention sweep, schema half.
-- Spec: docs/specs/etl/2026-06-10-raw-payload-retention-sweep.md
--
-- Makes ``filing_raw_documents.payload`` sweepable: the retention
-- sweep nulls the payload of parsed 10-K / 8-K ``primary_doc`` rows
-- (16 GB raw on dev, ~99.8% of the kind's bytes) after recording a
-- SHA-256 of the bytes being destroyed. Re-parse is safe by
-- construction: no rewash parser reads ``primary_doc`` payload, and
-- the manifest rebuild path re-fetches from EDGAR unconditionally.
--
-- Schema decisions:
--
--   * ``payload`` DROP NOT NULL — a swept row keeps its identity,
--     metadata and hash; only the bytes go.
--   * ``payload_sha256`` TEXT hex (not BYTEA): greppable, comparable
--     in SQL and Python without encode/decode asymmetry. Shape pinned
--     by CHECK. Hash semantics: SHA-256 of the UTF-8 encoding of the
--     TEXT payload as stored — server-side
--     ``encode(sha256(convert_to(payload, 'UTF8')), 'hex')`` equals
--     Python ``hashlib.sha256(text.encode('utf-8')).hexdigest()``
--     (verified on dev PG 17, 2026-06-10).
--   * ``payload_swept_at`` — when the bytes were destroyed. NULL on
--     live rows and on rehydrated rows.
--   * ``chk_swept_rows_carry_hash`` — the DB cannot represent
--     "bytes gone, no proof": a payload-less row MUST carry hash +
--     sweep timestamp.
--   * Hash is computed AT SWEEP TIME, not at ingest — zero ingest-path
--     change, no 16 GB backfill. Rows never swept don't need a stored
--     hash (derivable from the payload at any time).
--   * ``byte_count`` (GENERATED from octet_length(payload)) becomes
--     NULL on swept rows — intentional: the operator storage chip
--     reports LIVE payload bytes; reclaimed bytes are reported in the
--     sweep job summary.
--   * No new index: the sweep is rare + manual; candidates resolve via
--     the existing kind index + manifest PK join at ~28k rows.

ALTER TABLE filing_raw_documents
    ALTER COLUMN payload DROP NOT NULL;

ALTER TABLE filing_raw_documents
    ADD COLUMN payload_sha256 TEXT
        CONSTRAINT chk_payload_sha256_shape
        CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    ADD COLUMN payload_swept_at TIMESTAMPTZ;

ALTER TABLE filing_raw_documents
    ADD CONSTRAINT chk_swept_rows_carry_hash CHECK (
        payload IS NOT NULL
        OR (payload_sha256 IS NOT NULL AND payload_swept_at IS NOT NULL)
    );

COMMENT ON COLUMN filing_raw_documents.payload_sha256 IS
    'SHA-256 (lowercase hex) of the UTF-8 bytes of payload, recorded '
    'by the retention sweep immediately before nulling the payload. '
    'The reproducibility guard: any re-fetch must hash-match or fail '
    'loud (SEC silently changed the document). NULL on rows the sweep '
    'has never touched.';

COMMENT ON COLUMN filing_raw_documents.payload_swept_at IS
    'When the retention sweep nulled this row''s payload. NULL on '
    'live rows; cleared again on rehydrate / re-store.';
