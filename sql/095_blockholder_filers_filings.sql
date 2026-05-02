-- 095_blockholder_filers_filings.sql
--
-- Issue #766 PR 1 of 3 — schema for SEC Schedule 13D / 13G
-- blockholder ingest. Mirrors migration 090 (institutional_filers /
-- institutional_holdings, #730) in shape but with 13D/G semantics:
--
--   * 13F-HR is filed by managers with >$100M discretionary AUM and
--     enumerates *every* position quarterly. 13D/G are filed by any
--     beneficial owner who crosses the 5% threshold on a single
--     issuer — one accession ⇒ one issuer (not a portfolio dump).
--   * 13D ("active") signals an intent to influence; 13G ("passive")
--     signals a fund/index posture that will not engage. The
--     active/passive enum is intrinsic to the form type and is set by
--     the parser, not by a downstream classifier (this is why #766
--     ships in 3 PRs vs #730's 4 — there is no separate filer-type
--     classifier round).
--   * A single 13D/G accession can carry 1..N reporting persons
--     (joint filings — see e.g. SCHEDULE 13G accession
--     0001193125-25-270277, three reporters: Silver Point Capital +
--     two of its principals). Each reporting person is a row in
--     ``blockholder_filings``.
--
-- Schema decisions:
--
--   * ``blockholder_filers`` keys on the *primary* filer's CIK from
--     ``headerData/filerInfo/filer/filerCredentials/cik``. Joint-
--     filing co-reporters that have their own CIK still write
--     ``reporter_cik`` on the filing row but do NOT get their own
--     ``blockholder_filers`` record — the seed list curates the
--     primary filer (the entity actually submitting on EDGAR).
--   * ``blockholder_filings`` carries one row per
--     reporting-person-per-accession. The PR 2 amendment-chain
--     aggregator picks the latest non-superseded filing per
--     ``(reporter_identity, issuer_cik)`` regardless of form type
--     where ``reporter_identity = COALESCE(reporter_cik,
--     reporter_name)`` so natural-person / family-trust reporters
--     (which have no EDGAR CIK) still chain correctly. A 13D filed
--     after a prior 13G/A by the same reporter on the same issuer
--     supersedes the 13G chain (the SEC's actual semantics for a
--     passive→active conversion).
--   * ``status`` is a derived enum (``13D|13D/A → active``,
--     ``13G|13G/A → passive``) constrained via CHECK so a parser
--     regression cannot smuggle a third value into the canonical
--     store.
--   * ``instrument_id`` is nullable: the issuer's CUSIP may not yet
--     resolve via ``external_identifiers`` (the same gap #740 is
--     tracking for 13F-HR). Persisting the row with ``NULL``
--     instrument_id keeps the audit trail intact and lets the PR 2
--     reader skip rows that haven't been resolved yet.
--   * ``shares`` columns are NUMERIC(24, 4) to match
--     ``institutional_holdings.shares`` so cross-source aggregation
--     queries stay arithmetic-clean.
--   * ``percent_of_class`` is NUMERIC(8, 4) — SEC reports allow up
--     to 4 decimals (e.g. "47.6843"). 8 total digits gives
--     headroom for the rare "100.0000" without overflow.
--   * ``filed_at`` is nullable: the primary_doc.xml signature block
--     may be missing on a malformed filing; the parser returns
--     ``None`` rather than raising so the rest of the reporters can
--     still be ingested. The ingester (PR 2) decides whether to
--     persist filings with NULL filed_at or to tombstone them.
--   * ``reporter_cik`` is nullable because individual reporting
--     persons (natural persons, family trusts, foreign holdcos) often
--     do not have their own EDGAR CIK; the schema records that case
--     via ``reporter_no_cik = TRUE`` and falls back to
--     ``reporter_name`` for identity.
--
-- _PLANNER_TABLES in tests/fixtures/ebull_test_db.py is updated in
-- the same PR per the prevention-log entry "When a migration adds
-- any table with a FK relationship, update _PLANNER_TABLES …".

CREATE TABLE IF NOT EXISTS blockholder_filers (
    filer_id      BIGSERIAL PRIMARY KEY,
    cik           TEXT NOT NULL UNIQUE,
    name          TEXT NOT NULL,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS blockholder_filings (
    filing_id                 BIGSERIAL PRIMARY KEY,
    filer_id                  BIGINT NOT NULL REFERENCES blockholder_filers(filer_id),
    accession_number          TEXT NOT NULL,
    submission_type           TEXT NOT NULL,
    status                    TEXT NOT NULL,
    -- ``status`` is parser-derived from ``submission_type`` (13D and
    -- 13D/A are active; 13G and 13G/A are passive). Enforce the
    -- invariant at the DB layer with a single cross-column CHECK so
    -- a future ingester bug or a manual INSERT cannot persist
    -- e.g. ``submission_type='SCHEDULE 13D' AND status='passive'``
    -- and silently mislabel an activist filer as passive. Two
    -- independent enum CHECKs would let that combination through.
    -- Codex pre-push review caught this on PR review.
    CONSTRAINT blockholder_filings_submission_type_status_consistent
        CHECK (
            (submission_type IN ('SCHEDULE 13D', 'SCHEDULE 13D/A') AND status = 'active')
            OR
            (submission_type IN ('SCHEDULE 13G', 'SCHEDULE 13G/A') AND status = 'passive')
        ),
    instrument_id             BIGINT REFERENCES instruments(instrument_id),
    issuer_cik                TEXT NOT NULL,
    issuer_cusip              TEXT NOT NULL,
    securities_class_title    TEXT,

    -- Per-reporter identity. ``reporter_cik`` may be NULL for natural
    -- persons / foreign trusts that have no EDGAR CIK; in that case
    -- ``reporter_no_cik = TRUE`` and identity falls back to the name.
    reporter_cik              TEXT,
    reporter_no_cik           BOOLEAN NOT NULL DEFAULT FALSE,
    reporter_name             TEXT NOT NULL,
    member_of_group           TEXT,
    type_of_reporting_person  TEXT,
    citizenship               TEXT,

    -- Beneficial-ownership block. NULL when the source filing
    -- references the prior cover page rather than restating the
    -- numbers (rare on initial filings; common on amendments).
    sole_voting_power         NUMERIC(24, 4),
    shared_voting_power       NUMERIC(24, 4),
    sole_dispositive_power    NUMERIC(24, 4),
    shared_dispositive_power  NUMERIC(24, 4),
    aggregate_amount_owned    NUMERIC(24, 4),
    percent_of_class          NUMERIC(8, 4),

    date_of_event             DATE,
    filed_at                  TIMESTAMPTZ,
    fetched_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Idempotent re-ingest for joint filings: one accession × one
-- reporter == one row. ``reporter_cik`` is nullable, so use
-- COALESCE(reporter_cik, '') to keep the unique-index expression
-- safe — Postgres treats two NULL values as distinct under a plain
-- UNIQUE constraint, which would let the same reporter insert twice
-- on a re-ingest of a noCIK row. ``reporter_name`` is the
-- tie-breaker for accessions where two natural-person reporters both
-- file with no CIK.
CREATE UNIQUE INDEX IF NOT EXISTS uq_blockholder_filings_accession_reporter
    ON blockholder_filings (
        accession_number,
        COALESCE(reporter_cik, ''),
        reporter_name
    );

-- Hot path for the per-instrument ownership reader (PR 3): walk
-- filings for one instrument across the most recent dates first.
CREATE INDEX IF NOT EXISTS idx_blockholder_filings_instrument_filed_at
    ON blockholder_filings (instrument_id, filed_at DESC);

-- Hot path for the amendment-chain aggregator (PR 2): walk every
-- filing for a given (reporter, issuer) ordered by filed date so the
-- latest non-superseded row can be picked in one query.
--
-- Reporter identity is ``COALESCE(reporter_cik, reporter_name)``: when
-- the reporter has an EDGAR CIK, that CIK alone identifies the chain
-- (cover-page name changes across amendments must NOT split the
-- chain — e.g. an LLC rebranding). Only when ``reporter_cik IS
-- NULL`` (natural persons, family trusts, foreign holdcos) does the
-- name carry the identity. Without the COALESCE, every no-CIK
-- reporter collapses under ``reporter_cik IS NULL`` in a plain
-- B-tree index and the aggregator's chain walk silently misses
-- them. Codex pre-push review caught this on PR review (and a
-- follow-up review caught the over-strict variant that included
-- ``reporter_name`` even when ``reporter_cik`` was present).
CREATE INDEX IF NOT EXISTS idx_blockholder_filings_reporter_issuer_filed_at
    ON blockholder_filings (
        COALESCE(reporter_cik, reporter_name),
        issuer_cik,
        filed_at DESC
    );

-- Hot path for the per-filer view (operator audit, ops monitor):
-- walk every filing for a primary filer ordered by filed date.
CREATE INDEX IF NOT EXISTS idx_blockholder_filings_filer_filed_at
    ON blockholder_filings (filer_id, filed_at DESC);
