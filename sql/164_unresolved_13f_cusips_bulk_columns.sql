-- 164_unresolved_13f_cusips_bulk_columns.sql
--
-- #1233 PR-1a (spec §4) — Bulk-path unresolved-CUSIP capture.
--
-- The legacy per-filing writer ``_record_unresolved_cusip``
-- (app/services/institutional_holdings.py) writes one row per CUSIP
-- with ``name_of_issuer`` + ``last_accession_number`` (both NOT NULL
-- at schema 099). The bulk-path ingesters
-- (``sec_13f_dataset_ingest`` / ``sec_nport_dataset_ingest``) iterate
-- millions of INFOTABLE / FUND_REPORTED_HOLDING rows and want to
-- record the *(cusip, filer_cik, period_end)* triple for every
-- unresolved CUSIP so the PR-1b OpenFIGI sweep can fill in the
-- ticker + issuer name later. The bulk path does **not** have the
-- issuer name (the dataset TSV lacks it on the per-holding row;
-- only filer name + accession + period are available).
--
-- This migration:
--
--   1. Drops the legacy PRIMARY KEY on ``cusip``. A given CUSIP can
--      now have multiple bulk-path rows (one per
--      ``(filer_cik, period_end, source)``) plus AT MOST one legacy
--      per-filing row. Existing call sites that previously assumed
--      single-row-per-CUSIP (DELETE / UPDATE WHERE cusip = $1) keep
--      working in practice because: (a) the legacy path still owns
--      the ``source IS NULL`` partition under its own partial UNIQUE
--      (see below), and (b) the resolver helpers ``DELETE … WHERE
--      cusip = …`` will simply remove every row for that CUSIP after
--      a successful promotion — which is the correct semantics once
--      OpenFIGI / fuzzy match resolves it.
--   2. Drops NOT NULL on ``name_of_issuer`` and
--      ``last_accession_number``. Bulk-path rows leave these NULL;
--      the OpenFIGI sweep (PR-1b) fills ``name_of_issuer`` from
--      OpenFIGI's response.
--   3. Adds nullable ``filer_cik TEXT``, ``period_end DATE``,
--      ``source TEXT``.
--   4. Adds partial UNIQUE INDEX
--      ``unresolved_13f_cusips_bulk_idx`` on
--      ``(cusip, COALESCE(filer_cik,''), COALESCE(period_end,'0001-01-01'),
--        COALESCE(source,''))`` WHERE ``source IS NOT NULL`` —
--      the bulk-path ON CONFLICT target.
--   5. Adds partial UNIQUE INDEX
--      ``unresolved_13f_cusips_legacy_idx`` on ``(cusip)`` WHERE
--      ``source IS NULL`` — preserves the legacy
--      ``ON CONFLICT (cusip) DO UPDATE`` shape used by
--      ``_record_unresolved_cusip``.
--
-- Idempotent: every step uses IF EXISTS / IF NOT EXISTS so a re-run
-- against an already-migrated database is a no-op.

BEGIN;

-- Step 1: drop legacy PRIMARY KEY (constraint name follows PG's
-- default `_pkey` convention from sql/099). PG names the constraint
-- ``unresolved_13f_cusips_pkey`` regardless of column ordering.
ALTER TABLE unresolved_13f_cusips
    DROP CONSTRAINT IF EXISTS unresolved_13f_cusips_pkey;

-- Step 2: relax NOT NULL on legacy columns. The legacy writer keeps
-- supplying both (the helper still requires them via Python types);
-- the bulk writer leaves them NULL.
ALTER TABLE unresolved_13f_cusips
    ALTER COLUMN name_of_issuer DROP NOT NULL,
    ALTER COLUMN last_accession_number DROP NOT NULL;

-- Step 3: add bulk-path columns. All nullable to preserve legacy
-- rows that pre-date this migration.
ALTER TABLE unresolved_13f_cusips
    ADD COLUMN IF NOT EXISTS filer_cik TEXT,
    ADD COLUMN IF NOT EXISTS period_end DATE,
    ADD COLUMN IF NOT EXISTS source TEXT;

-- Step 3a: constrain ``source`` to the two bulk-source values plus
-- NULL (legacy). PR-1b may extend this set; the CHECK is named so
-- a future migration can DROP + re-ADD cleanly.
ALTER TABLE unresolved_13f_cusips
    DROP CONSTRAINT IF EXISTS unresolved_13f_cusips_source_check;
ALTER TABLE unresolved_13f_cusips
    ADD CONSTRAINT unresolved_13f_cusips_source_check
    CHECK (source IS NULL OR source IN (
        'bulk_13f_dataset',
        'bulk_nport_dataset'
    ));

-- Step 4: bulk-path partial UNIQUE INDEX. COALESCE expressions
-- collapse NULLs into deterministic sentinels so the index treats
-- the (cusip, filer_cik, period_end, source) tuple as a single key
-- — PostgreSQL would otherwise treat NULLs as never-equal and the
-- partial index would not enforce uniqueness on missing values.
-- (Codex review §H on PR-1a: spec said "ON CONFLICT against the
-- new partial UNIQUE INDEX" — the inference target must match the
-- index expression list exactly, see test in helper docstring.)
CREATE UNIQUE INDEX IF NOT EXISTS unresolved_13f_cusips_bulk_idx
    ON unresolved_13f_cusips (
        cusip,
        COALESCE(filer_cik, ''),
        COALESCE(period_end, '0001-01-01'::date),
        COALESCE(source, '')
    )
    WHERE source IS NOT NULL;

-- Step 5: legacy-path partial UNIQUE INDEX. Replaces the dropped
-- PRIMARY KEY on (cusip) for the legacy ``source IS NULL``
-- partition. Keeps ``_record_unresolved_cusip``'s
-- ``ON CONFLICT (cusip) DO UPDATE …`` shape working (PG infers the
-- index by matching the unique column list under a WHERE predicate
-- — we'll add an explicit WHERE to the helper's ON CONFLICT clause
-- in the call-site change so the inference is unambiguous).
CREATE UNIQUE INDEX IF NOT EXISTS unresolved_13f_cusips_legacy_idx
    ON unresolved_13f_cusips (cusip)
    WHERE source IS NULL;

COMMIT;
