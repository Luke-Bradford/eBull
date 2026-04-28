-- 077_financial_periods_fiscal_label_unique.sql
--
-- #624: Harden financial_periods against future-arrival duplicate
-- rows. #558 fixed existing data + same-run DEI pollution at extract;
-- this migration closes the remaining gap where a late amendment
-- arrives with a different period_end_date than the row already on
-- file, and the previous canonical merge keyed on period_end_date
-- inserted a NEW row instead of updating the existing one.
--
-- Add a partial unique index on the fiscal-label tuple
-- (instrument_id, source, fiscal_year, fiscal_quarter, period_type)
-- so the DB itself rejects a second row for the same fiscal period
-- per source. Second-arrival inserts must come through ON CONFLICT
-- UPDATE — the canonical merge is rewritten in the matching code
-- change.
--
-- ``COALESCE(fiscal_quarter, 0)`` is needed because PostgreSQL
-- treats NULLs in unique indexes as distinct (every NULL is a new
-- value). FY rows have fiscal_quarter=NULL and we WANT them to
-- collide with each other under the same (instrument, source,
-- fiscal_year, period_type) tuple, so substitute a sentinel.
-- 0 cannot collide with the legitimate range 1..4.
--
-- ``WHERE superseded_at IS NULL`` keeps the supersede mechanism
-- intact: a row marked superseded is still on disk for audit but
-- excluded from the unique constraint, so a fresh insert for the
-- same fiscal label can replace it.
--
-- Idempotent: ``IF NOT EXISTS`` makes re-running the file a no-op.

CREATE UNIQUE INDEX IF NOT EXISTS uniq_financial_periods_fiscal_label
    ON financial_periods (
        instrument_id,
        source,
        fiscal_year,
        COALESCE(fiscal_quarter, 0),
        period_type
    )
    WHERE superseded_at IS NULL;

COMMENT ON INDEX uniq_financial_periods_fiscal_label IS
    '#624: enforces one row per (instrument, source, fiscal_label) so '
    'late-arriving amendments cannot insert duplicates when '
    'period_end_date differs from the row already on file. '
    'Migration 076 dedupe + canonical-merge rewrite are the matching '
    'code-side guards.';
