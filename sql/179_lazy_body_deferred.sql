-- 179_lazy_body_deferred.sql
--
-- Issue #1343 (Phase 3 PR2 of bootstrap-sub-1h) — defer 10-K Item 1 +
-- 8-K item bodies out of first-install bootstrap; fetch lazily on first
-- user view. Spec: docs/proposals/etl/1343-s18-s21-lazy-on-click.md.
--
-- ## Three concerns
--
--   1. ``instrument_business_summary.body_deferred`` /
--      ``eight_k_filings.body_deferred`` — TRUE = metadata seeded, body
--      NOT fetched (distinct from ``body=''`` tombstone and ``body=<text>``
--      real). Readers (get_business_summary / get_parse_status) +
--      capabilities checks branch on it so a deferred placeholder neither
--      reads as ``parse_failed`` nor over-claims body-readiness.
--   2. ``filing_events.report_date`` — captures submissions.json
--      ``reportDate`` (parsed at sec_edgar.py but previously dropped — a
--      latent "every structured field lands in SQL" gap, prevention-log
--      §903). Gives the 8-K metadata seed the true event date with NO
--      body fetch, so the events rail orders correctly while deferred.
--   3. ``sec_filing_manifest.ingest_status`` gains ``'deferred'`` — a
--      terminal status the manifest worker's ``iter_pending`` /
--      ``iter_retryable`` selectors (WHERE ingest_status IN
--      ('pending','failed')) never pick. S16 seeds sec_10k/sec_8k rows
--      ``'deferred'`` so the post-bootstrap ``catch_up_on_boot`` worker
--      never eagerly drains the body backlog. ``'deferred'`` is
--      raw-not-required, so the #938 "parsed ⇒ raw stored" invariant
--      (enforced application-side at sec_manifest_worker.py) is untouched.
--      The lazy fill flips ``'deferred'→'parsed'`` (with raw stored).
--
-- ## Lock impact
--
-- (1)+(2) are PG14+ ADD COLUMN with constant/no DEFAULT → metadata-only,
-- no table rewrite. (3) DROP+ADD CONSTRAINT briefly takes ACCESS
-- EXCLUSIVE + a validating scan; the new IN-list is a strict SUPERSET of
-- the old, so every existing row trivially passes. Matches the proven
-- house pattern for widening this table's CHECK enums (sql/153 widened
-- ``sec_filing_manifest_source_check`` the same way). No ``lock_timeout``
-- (a timeout here would turn transient contention into a boot-lifespan
-- failure, worse than a brief wait) and no NOT VALID/VALIDATE (gives no
-- benefit inside the runner's single transaction). The inline CHECK from
-- sql/118 is unnamed → PG auto-names it ``sec_filing_manifest_ingest_status_check``
-- (same convention sql/153 relies on for the source check); DROP IF
-- EXISTS tolerates the name.
--
-- ## Paired Python widenings (same PR)
--   - IngestStatus Literal at app/services/sec_manifest.py:145 (+ 'deferred').
--   - _ALLOWED_TRANSITIONS at app/services/sec_manifest.py:153 (pending/failed
--     gain 'deferred'; new 'deferred' key → {pending, parsed, tombstoned}).
--   - transition_status gains a 'deferred' SET branch (clears error/next_retry).
--   - record_manifest_entry gains initial_ingest_status (INSERT-only) for the
--     S16 sec_10k/sec_8k deferred-seed.
--   NOTE: ParseStatus (sec_manifest_worker.py) is NOT widened — the worker never
--   emits 'deferred' (S16 seeds it directly; the worker only drains 'pending').

BEGIN;

ALTER TABLE instrument_business_summary
    ADD COLUMN IF NOT EXISTS body_deferred BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE eight_k_filings
    ADD COLUMN IF NOT EXISTS body_deferred BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE filing_events
    ADD COLUMN IF NOT EXISTS report_date DATE;

ALTER TABLE sec_filing_manifest
    DROP CONSTRAINT IF EXISTS sec_filing_manifest_ingest_status_check;

ALTER TABLE sec_filing_manifest
    ADD CONSTRAINT sec_filing_manifest_ingest_status_check
    CHECK (ingest_status IN ('pending', 'fetched', 'parsed', 'tombstoned', 'failed', 'deferred'));

COMMENT ON COLUMN instrument_business_summary.body_deferred IS
    '#1343 — TRUE = metadata seeded at bootstrap, 10-K Item 1 body NOT '
    'fetched; filled lazily on first business-panel view. Distinct from '
    'body='''' tombstone. Readers + capabilities must branch on this first.';

COMMENT ON COLUMN eight_k_filings.body_deferred IS
    '#1343 — TRUE = metadata seeded at bootstrap (item codes/dates from '
    'filing_events), item bodies + exhibits NOT fetched; filled lazily on '
    'first 8-K detail open. Rail renders from metadata; detail triggers fill.';

COMMENT ON COLUMN filing_events.report_date IS
    '#1343 — submissions.json filings.recent[].reportDate (event/period '
    'date). For 8-K = "date of earliest event reported". Lets the 8-K '
    'metadata seed set eight_k_filings.date_of_report with no body fetch.';

COMMENT ON COLUMN sec_filing_manifest.ingest_status IS
    'Lifecycle: pending → fetched → parsed | tombstoned | failed. '
    'deferred (#1343) = metadata seeded, body fetch deferred to first '
    'user view; terminal + raw-not-required; never selected by the '
    'worker queue; flipped to parsed (with raw stored) by the lazy fill.';

COMMIT;
