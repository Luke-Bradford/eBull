-- #2840 — record WHEN this database's capture semantics became trustworthy, exactly.
--
-- `sql/402` changed `strategy_intraday_bars.captured_at`'s default from `now()` to
-- `clock_timestamp()`. `app/services/bar_capture_certificate.capture_certificate` needs the
-- instant that took effect, and the first implementation read
-- `schema_migrations.applied_at` for 402 — which is `now()`, i.e. the MIGRATION
-- TRANSACTION'S START, recorded before its `ALTER` acquires `ACCESS EXCLUSIVE`. A writer
-- that inserted after that transaction began but before the lock landed carries OLD
-- semantics and a `captured_at` at or after `applied_at`, so the gate would admit it.
-- Caught at Codex checkpoint 2.
--
-- ⚠ THIS IS A SEPARATE MIGRATION AND NOT AN EDIT TO 402, WHICH IS ALREADY APPLIED. The
-- runner stores each file's SHA-256 and RAISES on a mismatch for an applied filename
-- (`app/db/migrations.py` §content-drift guard, #1333), so editing 402 would wedge every
-- environment that already ran it. The cost is that the recorded instant is 403's rather
-- than 402's — later, therefore STRICTER, therefore the safe direction.
--
-- ⚠ `clock_timestamp()` here is evaluated AFTER the preceding DDL in this transaction has
-- executed, which is the property the whole fix rests on: statements run in order, so by
-- the time this INSERT evaluates it, 402's ALTER has long since committed.
--
-- ⚠ A TABLE AND NOT A CONSTANT. The instant is per-environment — dev, test and any future
-- database each cross over when their own migrations run — and a literal in Python would be
-- right in exactly one of them. It is an ERA table rather than a single row so a later
-- semantics change (a v2) records its own era instead of overwriting this one.
--
-- Refs #2840, #2437.

-- ⚠ NO `lock_timeout` HERE, and that is considered rather than missed: every statement
-- below touches only the brand-new `intraday_capture_semantics` relation, which no reader
-- can be holding. The #2363 rule binds DDL on a HOT relation; `sql/402`'s ALTER carries it.

CREATE TABLE IF NOT EXISTS intraday_capture_semantics (
    rule_id        TEXT PRIMARY KEY,
    effective_from TIMESTAMPTZ NOT NULL,
    note           TEXT NOT NULL
);

COMMENT ON TABLE intraday_capture_semantics IS
    'When each captured_at stamping rule became effective in THIS database. Read by '
    'bar_capture_certificate; a bar stamped before its era cannot be certified. #2840.';

INSERT INTO intraday_capture_semantics (rule_id, effective_from, note)
VALUES (
    'clock_timestamp',
    clock_timestamp(),
    'sql/402 set strategy_intraday_bars.captured_at DEFAULT clock_timestamp() and the writer '
    'names it explicitly. Bars stamped before this instant may carry a transaction timestamp '
    'that precedes their observation.'
);
