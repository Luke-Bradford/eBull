-- 172_sec_cik_submissions_files_index.sql
--
-- Stream A PR-B T1.3 (#1233): sidecar table for SEC submissions
-- ``filings.files[]`` page-descriptor enumeration.
--
-- WHAT IT REPLACES
-- ----------------
-- Pre-PR-B, ``sec_submissions_files_walk`` (S14) re-fetches each
-- CIK's primary ``data.sec.gov/submissions/CIK<10>.json`` JUST to
-- read ``filings.files[]`` again — even though ``sec_submissions_ingest``
-- (S8) already read that same JSON from ``submissions.zip`` minutes
-- earlier. At Run #7 cohort sizes (~8.7k CIK post-#1222) this is
-- ~5,105 redundant primary HTTP calls / ~12 min wall-clock at SEC's
-- 7 req/s budget.
--
-- WHAT IT ADDS
-- ------------
-- One row per (cik, secondary-page-name) populated by S8 during the
-- per-CIK transaction at ``sec_submissions_ingest.py:147`` from the
-- in-memory submissions payload. S14 consumes the sidecar instead
-- of re-fetching the primary; secondary-page bodies are still
-- fetched over HTTP (they are NOT in the bulk archive — confirmed
-- at ``sec_submissions_files_walk.py:1-7`` docstring).
--
-- SENTINEL ROW PATTERN
-- --------------------
-- A CIK with ZERO overflow pages (e.g. AAPL — ``recent`` array fits
-- under the 1000-cap) writes ONE sentinel row with
-- ``page_name='__no_overflow_pages__'`` instead of zero rows. This
-- distinguishes "CIK processed; no overflow" from "CIK not yet
-- populated". S14 + the Stream-C C7 gate honour this explicitly:
--
--   sidecar state for CIK X        | meaning              | S14 / C7 action
--   ------------------------------ | -------------------- | ---------------
--   1+ real-page rows              | overflow exists      | walk pages
--   exactly 1 sentinel row         | processed; no over-  | skip walk
--                                  | flow                 | (C7 passes)
--   zero rows                      | not yet populated    | S14 fail-closed;
--                                  |                      | C7 fails
--
-- Without the sentinel, AAPL (0 overflow) and a never-populated CIK
-- look identical; either C7 false-fails everyone or S14 silently
-- does nothing for valid CIKs.
--
-- AGENT-CIK FILTER
-- ----------------
-- The populate path (S8) skips CIKs in
-- ``KNOWN_FILING_AGENT_CIKS`` (``app/providers/implementations/sec_edgar.py``)
-- so the sidecar stays a "real-filer-only" index. S14 + C7 know to
-- expect zero rows for agent CIKs; they are NOT in the populated
-- set.
--
-- PER-CIK DELETE + INSERT
-- -----------------------
-- Idempotent rebuild per S8 ingest of that CIK. NOT a global
-- TRUNCATE — S8 is per-CIK at the transaction level; global TRUNCATE
-- would leave the sidecar empty between CIK 1 and CIK N during a
-- long-running ingest, breaking S14 fail-closed semantics for all
-- not-yet-processed CIKs. On per-CIK transaction rollback (INSERT
-- raises mid-CIK), the DELETE rolls back too — prior committed rows
-- for that CIK SURVIVE.
--
-- INDEX BUDGET
-- ------------
-- 1 index (PK only). PG B-tree handles cik-only equality + range
-- via PK prefix scan; a standalone ``cik`` index would be dead
-- weight.
--
-- Spec: docs/proposals/etl/stream-a-run-8-fixes.md v2.3 §4 + §14
-- (post-Codex-1 re-pass + 3-lens code review 2026-05-24).

BEGIN;

CREATE TABLE IF NOT EXISTS sec_cik_submissions_files_index (
    -- 10-digit zero-padded CIK (CHECK-enforced).
    cik              TEXT       NOT NULL,
    -- Either a sentinel ``__no_overflow_pages__`` OR a SEC overflow
    -- page name shaped ``CIKnnnnnnnnnn-submissions-nnn.json``
    -- (CHECK-enforced via disjunction).
    page_name        TEXT       NOT NULL,
    -- Inclusive date range covered by the page; NULL for sentinel.
    filing_from      DATE,
    filing_to        DATE,
    -- Wall-clock stamp of the row write. ``clock_timestamp()``
    -- (NOT ``NOW()`` / ``transaction_timestamp()``) per
    -- ``.claude/skills/data-engineer/SKILL.md`` §6.5.8 — avoids
    -- artificially-old stamps if a future caller hoists the write
    -- into a longer-running transaction.
    discovered_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    -- Bootstrap-run lineage. NULL when populated by a steady-state
    -- S8 refresh (outside a tracked bootstrap_runs row). FK on
    -- ``id`` (NOT ``run_id`` — verified at sql/129:66).
    bootstrap_run_id BIGINT     REFERENCES bootstrap_runs(id) ON DELETE SET NULL,
    -- Audit-lineage discriminator. Distinguishes "NULL bootstrap_run_id
    -- because steady-state refresh" (origin='steady_state') from
    -- "NULL bootstrap_run_id because buggy code path forgot to thread
    -- it" (origin='bootstrap' + NULL run id = violation worth noting).
    populate_origin  TEXT       NOT NULL DEFAULT 'bootstrap'
                                CHECK (populate_origin IN ('bootstrap', 'steady_state')),
    PRIMARY KEY (cik, page_name),
    -- CIK shape: 10 digits, zero-padded. Single regex CHECK is
    -- cheap and pre-empts a future writer inserting unpadded.
    CHECK (cik ~ '^[0-9]{10}$'),
    -- Page-name shape: either sentinel OR tight regex matching the
    -- actual SEC overflow-page naming convention. ``LIKE`` patterns
    -- were too loose — they admitted garbage like ``CIK-submissions-.json``.
    CHECK (
        page_name = '__no_overflow_pages__'
        OR page_name ~ '^CIK[0-9]{10}-submissions-[0-9]{3}\.json$'
    ),
    -- Real-page rows MUST have both date columns populated;
    -- sentinel rows MUST have both NULL. Composite CHECK pins the
    -- disjunction so a partial-population bug can't slip past.
    CHECK (
        (page_name = '__no_overflow_pages__' AND filing_from IS NULL AND filing_to IS NULL)
        OR (
            page_name <> '__no_overflow_pages__'
            AND filing_from IS NOT NULL
            AND filing_to IS NOT NULL
            AND filing_from <= filing_to
        )
    )
);

COMMENT ON TABLE sec_cik_submissions_files_index IS
    'Sidecar cache of SEC submissions filings.files[] page descriptors per CIK (#1233 Stream A PR-B). Populated by sec_submissions_ingest (S8) during per-CIK ingest; consumed by sec_submissions_files_walk (S14) instead of re-fetching primary submissions.json. SINGLE-WRITER: S8 only. Sentinel-row pattern (page_name=__no_overflow_pages__) distinguishes "processed, no overflow" from "not yet populated".';

COMMIT;
