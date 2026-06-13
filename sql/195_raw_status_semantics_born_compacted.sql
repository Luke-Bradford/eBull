-- 195_raw_status_semantics_born_compacted.sql
--
-- #1615 — redefine sec_filing_manifest.raw_status semantics on live DBs.
--
-- store_raw now writes write-only kinds (primary_doc) BORN-COMPACTED:
-- payload NULL + payload_sha256 + payload_swept_at, so the bytes are
-- never persisted. The callers still flip raw_status='stored' (the
-- born-compaction is centralized in store_raw, not the 7 manifest
-- writers). So raw_status='stored' no longer implies the payload bytes
-- are present.
--
-- This is a COMMENT-only migration: it carries the redefined meaning to
-- \d+ / catalog introspection. No data or constraint change. The
-- authoritative "are the bytes here" predicate is
-- filing_raw_documents.payload_swept_at IS NULL (NULL = bytes present).

COMMENT ON COLUMN sec_filing_manifest.raw_status IS
    'Whether a filing_raw_documents raw-evidence ROW exists for this '
    'accession — NOT whether the payload bytes are present. '
    'stored = a raw row exists (bytes may be present, #1014-swept, or '
    'born-compacted for write-only kinds like primary_doc, #1615). '
    'compacted = the #1014 retention sweep nulled a previously-stored '
    'payload. absent = no raw row. The authoritative "bytes present" '
    'signal is filing_raw_documents.payload_swept_at IS NULL / byte_count.';
