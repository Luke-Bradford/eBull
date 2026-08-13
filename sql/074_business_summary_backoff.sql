-- Migration 074 — failure-reason taxonomy + exponential backoff
-- + quarantine for ``instrument_business_summary`` (#533).
--
-- Pre-#533: the ingester wrote a ``body=''`` tombstone on every
-- parse failure and the candidate query had a hard-coded 7-day
-- TTL retry. Hopeless cases (10-K/A amendments missing Item 1,
-- broken document URLs, etc.) cycled through the limit slot
-- every week forever.
--
-- Post-#533: each row carries an attempt_count + last_failure_reason
-- + next_retry_at. The candidate query filters on next_retry_at so
-- quarantined rows fall out of the hot set automatically.
--
-- Backoff schedule (encoded in Python, not SQL):
--   attempt 1 → next_retry NOW + 1 day
--   attempt 2 → NOW + 7 days
--   attempt 3 → NOW + 30 days
--   attempt 4+ → NOW + 365 days (effective quarantine)
--
-- Backfill of existing tombstones (body = '' rows): attempt_count=1,
-- next_retry_at = last_parsed_at + 1 day. One fresh first-attempt
-- retry on the existing TTL cadence; if that fails, the backoff
-- schedule kicks in. Real bodies (body != '') get NULL retry — the
-- ``source_accession <> latest`` check in the candidate query
-- continues to be the (correct) trigger for re-parsing on a new
-- 10-K, regardless of next_retry_at.

BEGIN;

ALTER TABLE instrument_business_summary
    ADD COLUMN IF NOT EXISTS attempt_count       INTEGER     NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_failure_reason TEXT,
    ADD COLUMN IF NOT EXISTS next_retry_at       TIMESTAMPTZ;

-- Backfill existing tombstones (failed parses).
UPDATE instrument_business_summary
   SET attempt_count = 1,
       last_failure_reason = 'legacy_tombstone',
       next_retry_at = last_parsed_at + INTERVAL '1 day'
 WHERE body = ''
   AND attempt_count = 0;

COMMENT ON COLUMN instrument_business_summary.attempt_count IS
    'Consecutive failed parse attempts (#533). 0 = success or '
    'never attempted. Reset to 0 on successful parse.';

COMMENT ON COLUMN instrument_business_summary.last_failure_reason IS
    'Last failure category (#533). Closed taxonomy in '
    'app/services/business_summary.py FailureReason. NULL = no '
    'failure recorded.';

COMMENT ON COLUMN instrument_business_summary.next_retry_at IS
    'Earliest UTC time the ingester will re-attempt this filing '
    '(#533). Computed from attempt_count via exponential backoff. '
    'Candidate query filters out rows where NOW() < next_retry_at. '
    'NULL = candidate query ignores the gate (real-body rows or '
    'never-attempted instruments).';

CREATE INDEX IF NOT EXISTS instrument_business_summary_next_retry_at_idx
    ON instrument_business_summary (next_retry_at)
    WHERE next_retry_at IS NOT NULL;

COMMIT;
