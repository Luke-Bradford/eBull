-- 389_freshness_next_poll_at.sql
--
-- #3109 slices 1-3 — separate the poll-eligibility clock from the
-- reconciliation deadline derived from the last filing.
--
-- Spec: docs/proposals/etl/2026-09-17-3109-poll-rotation-clock.md
--
-- ``expected_next_at`` was doing two jobs: a reconciliation deadline
-- anchored to ``last_known_filed_at`` (``_CADENCE`` — its own comments
-- call it "Layer 3 reconcile cadence"), AND the poll selector's
-- eligibility clock. For a filer whose last filing was 1994 the first
-- is permanently in the past, so the row won ``ORDER BY ... ASC`` on
-- every hourly tick and nothing else was ever reached.
--
-- Measured on dev 2026-09-16T23:30Z, by running the production
-- selector and diffing it against the CIKs the last run actually
-- polled: intersection 48, **polled-only 0**. A completed poll removed
-- nothing from the queue head. 95 of 55,775 rows had ever been polled
-- since 2026-06-05.
--
-- Reproduce: PYTHONPATH=. uv run python scripts/measure_3109_batching.py --rotation

BEGIN;

-- NOT NULL DEFAULT now() is load-bearing, not tidiness. A nullable
-- column ordered NULLS FIRST would let a continuous stream of newly
-- seeded rows preempt the tail forever; with this declaration a new
-- subject enters stamped with its arrival instant and sorts BEHIND
-- everything already eligible. There are no NULLs to reason about.
--
-- The backfill IS the default: every existing row becomes eligible at
-- migration time, which is correct (we have never asked about 99.83%
-- of them) and makes the first cycle one complete deterministic sweep,
-- tie-broken on the padded CIK. On PG17 a non-volatile DEFAULT is a
-- catalogue-only backfill, so no table rewrite.
ALTER TABLE data_freshness_index
    ADD COLUMN next_poll_at TIMESTAMPTZ NOT NULL DEFAULT now();

COMMENT ON COLUMN data_freshness_index.next_poll_at IS
    'Poll-eligibility clock: the earliest instant we may spend another '
    'submissions.json request on this row. Advanced unconditionally by '
    'record_poll_outcome (NOW() + POLL_REPOLL_INTERVAL, 7 days per the '
    'cold-tier rule in .claude/skills/data-sources/sec-edgar.md), and '
    'reset to NOW() by sec_rebuild and the operator full-wash. Distinct '
    'from expected_next_at, which stays the reconciliation deadline '
    'derived from last_known_filed_at. #3109.';

-- Replace the queue index rather than adding beside it.
--
-- ⚠ The old index was on expected_next_at, and NOTHING reads that
-- column any more. The "overdue-filing visibility" reader it was
-- assumed to serve does not exist: the actual producer,
-- app/services/processes/scheduled_adapter.py::_source_watermark_behind,
-- says in its own docstring that it is "deliberately ... NOT the
-- per-subject ``expected_next_at`` timing probe" and probes
-- state='error' instead. An index with no reader costs write
-- amplification on a column this change touches on every poll.
-- IF EXISTS: the CREATE immediately below establishes the desired state either
-- way, so a hard failure here buys nothing over a no-op (review NITPICK).
DROP INDEX IF EXISTS idx_freshness_due_for_poll;

CREATE INDEX idx_freshness_due_for_poll
    ON data_freshness_index (next_poll_at, source)
    WHERE state IN ('unknown', 'current', 'expected_filing_overdue');

COMMIT;
