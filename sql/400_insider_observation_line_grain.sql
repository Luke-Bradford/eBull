-- #3227 item 2 — carry the Table I line-grain discriminators onto the insider
-- observations layer. Evidence columns only: no key changes, no row changes, and
-- `ownership_insiders_current` is untouched by this migration.
--
-- ## Why these three columns
--
-- Form 3 General Instruction 5(b)(iii) — and Form 4's 4(b)(iii) / Form 5's
-- 4(b)(iii) to the same effect — require that "different forms of indirect
-- ownership" be reported "on separate lines", under Table I's printed reminder
-- "Report on a separate line for each class of securities beneficially owned
-- directly or indirectly". So the SOURCE's line grain is
--
--     (class of security) x (direct | each distinct form of indirect)
--
-- `ownership_insiders_current` is keyed (instrument_id, holder_identity_key,
-- ownership_nature) and carries NEITHER axis on a dataset row, because on a
-- `:NDH:` row `ownership_nature` holds the DERA *relationship* flag
-- (officer/director -> direct, ten-percent-owner -> beneficial) and never the
-- Table I D/I field — `sec_insider_dataset_ingest._map_relationship`, recorded in
-- .claude/skills/data-sources/sec-edgar.md section 2.3 (#2385/#2386). Lines the
-- regulation REQUIRES to be separate therefore collide on one key and the
-- DISTINCT ON keeps exactly one.
--
-- These columns are the prerequisite the repair needs; the repair itself (the
-- `_current` re-key) is #3227 item 3 and is NOT in this migration.
--
-- ## Nullable, deliberately, and NULL is overloaded
--
-- All three are nullable and there is no backfill inside this migration. A NULL
-- means any of: an `:NDT:` transaction row (not carried — see the spec), a row
-- written before the backfill ran, a holding whose source value was genuinely
-- empty (NATURE_OF_OWNERSHIP is 72.66% populated across the cached corpus), or a
-- row from the XML writer, which emits COLLAPSED groups and so has no single line
-- value to record. Do NOT read NULL as "collapsed group" — a consumer that needs
-- that distinction needs explicit grain metadata, not an inference from NULL.
--
-- ## The CHECK is load-bearing, not decorative
--
-- DIRECT_INDIRECT_OWNERSHIP is exactly {D, I} across all 2,367,536 cached
-- NONDERIV_HOLDING rows, so the constraint costs nothing today. It is here to
-- fail a future drift loudly. ⚠ The ingest sanitises to NULL before staging
-- (mirroring app/services/insider_transactions.py:1241) because
-- `COPY ... ON_ERROR ignore` protects the copy into staging but NOT the
-- `INSERT ... SELECT` into this table: an unexpected value reaching this CHECK
-- would abort the entire drain, not skip one row.
--
-- Reproduce the figures quoted above:
--   PYTHONPATH=. uv run python -m scripts.census_3227_insider_holding_line_collapse --source-columns

-- ⚠⚠ A MIGRATION THAT IS WAITING FOR A LOCK IS NOT PASSIVE (#2363, prevention log).
-- ---------------------------------------------------------------------------
-- A pending ACCESS EXCLUSIVE lock queues AHEAD of new readers, so an ALTER that is
-- merely waiting stops every subsequent SELECT on this relation from starting. On a
-- 125-partition, 5.58M-row table that is the operator's whole ownership read path.
--
-- This is not a hypothetical here. Applying the first draft of this migration on dev
-- (2026-09-19) queued for 611 seconds behind pid 68498 — an ad-hoc #3146 line-order
-- concordance SELECT that had been running since 2026-09-17 09:37Z, 2d 9h, holding
-- ACCESS SHARE. Cancelling our own backend was the fix, exactly as #2363 records.
-- Assume a long reader is the NORMAL state on this table, not an unlucky collision.
--
-- `lock_timeout` turns an unbounded stall-everything wait into a clean, retryable
-- LockNotAvailable that bounds each attempt's reader impact to five seconds. Note the
-- consequence at app boot: the FastAPI lifespan runs migrations, so a locked-out
-- migration fails the boot loudly instead of hanging it — the better of the two.
SET LOCAL lock_timeout = '5s';

ALTER TABLE ownership_insiders_observations
    ADD COLUMN IF NOT EXISTS security_title      TEXT,
    ADD COLUMN IF NOT EXISTS direct_indirect     TEXT,
    ADD COLUMN IF NOT EXISTS nature_of_ownership TEXT;

-- ⚠ NOT VALID, and it costs nothing to be.
-- ---------------------------------------------------------------------------
-- A validated CHECK scans every row across the whole partition tree WHILE still
-- holding the ACCESS EXCLUSIVE lock the column ALTER took — on 5.58M rows that is a
-- long blocking window for a guard that cannot possibly fire. Every pre-existing row
-- receives NULL for a column added in this same statement, so the constraint is
-- satisfied by construction and there is nothing for a validation scan to discover.
--
-- NOT VALID still enforces the constraint on every INSERT and UPDATE from here on,
-- which is the entire purpose: catching a future DERA drift away from {D, I}. So this
-- is not a weakened constraint, it is the same guard without a pointless scan. No
-- follow-up VALIDATE is needed or planned.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ownership_insiders_observations_direct_indirect_check'
    ) THEN
        ALTER TABLE ownership_insiders_observations
            ADD CONSTRAINT ownership_insiders_observations_direct_indirect_check
            CHECK (direct_indirect IS NULL OR direct_indirect IN ('D', 'I')) NOT VALID;
    END IF;
END
$$;

COMMENT ON COLUMN ownership_insiders_observations.security_title IS
    '#3227 — Table I "Title of Security" (Form 3 Instr. 4). The CLASS axis of the '
    'source line grain. NONDERIV_HOLDING.SECURITY_TITLE; 100% populated, 9,963 distinct '
    'values across the cached corpus. NOT cross-source verified (it is the join '
    'predicate in the check that verified direct_indirect, so that test is circular '
    'for this column).';

COMMENT ON COLUMN ownership_insiders_observations.direct_indirect IS
    '#3227 — Table I "Ownership Form: Direct (D) or Indirect (I)" (Form 3 Instr. 5). '
    'NONDERIV_HOLDING.DIRECT_INDIRECT_OWNERSHIP. Distinct from ownership_nature, which '
    'on a :NDH: row is the DERA relationship flag. Cross-source confirmed 50,313/50,313 '
    'against our own XML parse of the same filings.';

COMMENT ON COLUMN ownership_insiders_observations.nature_of_ownership IS
    '#3227 — Table I "Nature of Indirect Beneficial Ownership" (Form 3 Instr. 5(b)(iii)). '
    'NONDERIV_HOLDING.NATURE_OF_OWNERSHIP. ⚠ QUARANTINED AS A KEY: it disagrees with our '
    'XML parse of the same filings on 9,329 of 50,313 cross-checked rows (18.5%). Carry '
    'it as evidence; do NOT key _current on it until that is explained (#3227 item 3).';
