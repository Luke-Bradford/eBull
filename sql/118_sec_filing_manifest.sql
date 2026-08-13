-- 118_sec_filing_manifest.sql
--
-- Issue #864 — single source of truth for "is filing X already on file?".
-- Spec: docs/superpowers/specs/2026-05-04-etl-coverage-model.md §"sec_filing_manifest".
--
-- Replaces the per-source bespoke joins against def14a_ingest_log /
-- institutional_holdings_ingest_log / insider_filings.is_tombstone /
-- blockholder_filings.accession_number / unresolved_13f_cusips with one
-- canonical accession-level table. Every manifest row carries:
--
--   - subject identity (subject_type, subject_id, instrument_id, cik)
--   - filing identity (form, source, filed_at, accepted_at, primary_document_url)
--   - amendment chain (is_amendment, amends_accession self-FK)
--   - lifecycle (ingest_status state machine, parser_version, raw_status,
--     last_attempted_at, next_retry_at, error)
--
-- The manifest is read by:
--   - the freshness scheduler (#865) to seed scheduler rows from history
--   - the manifest-driven worker (#869) to pull ``ingest_status='pending'``
--     and ``ingest_status='failed' AND next_retry_at<=NOW()`` work
--   - the targeted-rebuild job (#872) to flip rows back to ``pending``
--   - the first-install drain (#871) to UPSERT discovered accessions
--
-- ``source`` uses the ``sec_*`` / ``finra_*`` naming convention pinned
-- in the spec (NOT the legacy short ``form4`` / ``13d`` names used by
-- ``ownership_observations.source``); see the comment on the column.

BEGIN;

CREATE TABLE sec_filing_manifest (
    accession_number        TEXT PRIMARY KEY,
    cik                     TEXT NOT NULL,
    form                    TEXT NOT NULL,
        -- raw SEC form code: '3', '4', '5', '13D', '13D/A', '13G',
        -- '13G/A', '13F-HR', '13F-HR/A', 'DEF 14A', 'PRE 14A',
        -- '10-K', '10-Q', '8-K', etc.
    source                  TEXT NOT NULL CHECK (source IN (
        'sec_form3', 'sec_form4', 'sec_form5',
        'sec_13d', 'sec_13g',
        'sec_13f_hr',
        'sec_def14a',
        'sec_n_port', 'sec_n_csr',
        'sec_10k', 'sec_10q', 'sec_8k',
        'sec_xbrl_facts',
        'finra_short_interest'
    )),
        -- Coarser bucket than ``form`` — collapses amendments into the
        -- parent (13D + 13D/A both -> 'sec_13d'). Aligns with
        -- data_freshness_index.source for the scheduler join.
    subject_type            TEXT NOT NULL CHECK (subject_type IN (
        'issuer',
        'institutional_filer',
        'blockholder_filer',
        'fund_series',
        'finra_universe'
    )),
    subject_id              TEXT NOT NULL,
        -- For ``issuer``: str(instrument_id) — string for portability
        -- across PK types per the scheduler model.
        -- For ``institutional_filer``/``blockholder_filer``: filer's CIK.
        -- For ``fund_series``: series_id (Phase 3).
        -- For ``finra_universe``: 'FINRA_SI' singleton.
    instrument_id           BIGINT REFERENCES instruments(instrument_id) ON DELETE CASCADE,
        -- Non-null when the manifest row is issuer-scoped (Form 3/4/5,
        -- 13D/G, DEF 14A, XBRL facts). Null for 13F-HR rows where the
        -- subject is the filer and the issuer dimension is per-holding
        -- inside the body.
        --
        -- ON DELETE CASCADE (Claude bot review BLOCKING on PR #878):
        -- ``SET NULL`` would violate ``chk_manifest_issuer_has_instrument``
        -- on issuer-scoped rows (the CHECK requires non-null
        -- instrument_id when subject_type='issuer'); the DELETE FROM
        -- instruments would abort. CASCADE is semantically correct —
        -- if the instrument is deleted, the manifest entry is no longer
        -- reachable from operator UI.
    filed_at                TIMESTAMPTZ NOT NULL,
    accepted_at             TIMESTAMPTZ,
        -- Precise SEC accept timestamp from getcurrent feed when known;
        -- nullable because submissions.json + daily-index don't carry it.
    primary_document_url    TEXT,
    is_amendment            BOOLEAN NOT NULL DEFAULT FALSE,
    amends_accession        TEXT REFERENCES sec_filing_manifest(accession_number) ON DELETE SET NULL,
        -- self-FK for the amendment chain. Null on the original; set on
        -- the amendment to its predecessor's accession.
    ingest_status           TEXT NOT NULL DEFAULT 'pending' CHECK (ingest_status IN (
        'pending',
        'fetched',
        'parsed',
        'tombstoned',
        'failed'
    )),
        -- State machine:
        --   pending    -> fetched (worker downloads body)
        --   fetched    -> parsed | tombstoned | failed (parser outcome)
        --   failed     -> pending (after backoff window) | tombstoned (give-up)
        --   parsed     -> pending (rebuild flips back; preserves history)
    parser_version          TEXT,
        -- Pin of which parser wrote the typed-table rows. Bumping this
        -- without flipping ``ingest_status`` to ``pending`` is the
        -- explicit signal for "rewash needed"; the rebuild job (#872)
        -- compares latest known parser version to per-row stored version
        -- and resets the diff back to ``pending``.
    raw_status              TEXT NOT NULL DEFAULT 'absent' CHECK (raw_status IN (
        'absent',
        'stored',
        'compacted'
    )),
        -- Tracks whether ``filing_raw_documents`` has the body for this
        -- accession. ``compacted`` reserved for a future hot-storage
        -- eviction path.
    last_attempted_at       TIMESTAMPTZ,
    next_retry_at           TIMESTAMPTZ,
        -- Backoff for ``failed`` rows. Worker filters
        -- ``ingest_status='failed' AND (next_retry_at IS NULL OR next_retry_at <= NOW())``.
    error                   TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Subject-type / instrument cross-check: issuer-scoped rows must
    -- carry an instrument_id; non-issuer rows must not (the issuer
    -- dimension lives inside the body of 13F-HR / N-PORT etc.).
    CONSTRAINT chk_manifest_issuer_has_instrument CHECK (
        (subject_type = 'issuer' AND instrument_id IS NOT NULL)
        OR (subject_type <> 'issuer' AND instrument_id IS NULL)
    )
);

-- Subject-scoped lookup: "what filings do I have for this filer/issuer?"
CREATE INDEX idx_manifest_subject
    ON sec_filing_manifest (subject_type, subject_id, form, filed_at DESC);

-- Worker queue: pending + retryable failures, ordered by retry/filed time.
CREATE INDEX idx_manifest_status_retry
    ON sec_filing_manifest (ingest_status, next_retry_at)
    WHERE ingest_status IN ('pending', 'failed');

-- Rewash discovery: "which parsed accessions are on a stale parser?"
CREATE INDEX idx_manifest_parser_version
    ON sec_filing_manifest (source, parser_version)
    WHERE ingest_status = 'parsed';

-- Issuer rollup access: "every filing affecting AAPL, newest first."
CREATE INDEX idx_manifest_instrument
    ON sec_filing_manifest (instrument_id, form, filed_at DESC)
    WHERE instrument_id IS NOT NULL;

-- CIK lookup for raw-feed reconcile (Atom + daily-index both key by CIK).
CREATE INDEX idx_manifest_cik
    ON sec_filing_manifest (cik, source, filed_at DESC);

-- Touch ``updated_at`` on every UPDATE.
CREATE OR REPLACE FUNCTION sec_filing_manifest_touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_sec_filing_manifest_touch
    BEFORE UPDATE ON sec_filing_manifest
    FOR EACH ROW
    EXECUTE FUNCTION sec_filing_manifest_touch_updated_at();

COMMIT;
