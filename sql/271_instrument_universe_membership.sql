-- 271_instrument_universe_membership.sql
--
-- Append-only record of when each instrument was a member of the
-- tradable universe (#2290, adopted as option (3) of the #2289
-- validation-universe decision; §4.0 of
-- docs/proposals/ta/strategy-catalogue-and-backtest-validity.md).
--
-- WHY
--
-- ``sync_universe`` overwrote the transition in place: an instrument
-- that left the provider feed got ``is_tradable = FALSE`` and nothing
-- else. ``is_tradable`` is a single current-state boolean, so after
-- the update "was instrument X tradable on date D?" was unanswerable
-- for every past D, and a relisting flipped it back with no record
-- that it had ever gone away. Phase 3.1 of the strategy runner
-- (docs/proposals/ta/2026-08-08-strategy-runner-and-manifest.md
-- §3.1.1) is blocked on that question: resolving a past signal date
-- against TODAY's membership is a look-ahead fact.
--
-- For the 4,749 non-US instruments there is no dated delisting
-- register at any price — Form 25 is US-only — so recording
-- transitions forward from today is the only path that ever closes
-- the gap.
--
-- ⚠⚠ WHY ``last_confirmed_on`` EXISTS — the whole point of the table
--
-- #2290 originally prescribed taking the close date from the prior
-- ``instruments.last_seen_at``. That premise is FALSE and the column
-- must not be used for this. The upsert in
-- ``app/services/universe.py`` is
--
--     ON CONFLICT (instrument_id) DO UPDATE
--        SET ..., last_seen_at = NOW()
--      WHERE (symbol IS DISTINCT FROM ... OR company_name IS DISTINCT FROM ... OR ...)
--
-- and that WHERE guard suppresses the ENTIRE update — the
-- ``last_seen_at`` bump included — whenever no metadata changed. So
-- the column means "last time this row's metadata changed", not "last
-- time we saw it". Measured on the full dev population 2026-08-08:
-- 12,696 instruments carry only NINE distinct ``last_seen_at`` dates,
-- 9,850 of them frozen at 2026-06-12, with the table-wide maximum at
-- 2026-08-05. Closing a membership row at that date would stamp it
-- weeks-to-months before the instrument actually left the feed —
-- the same defect the table exists to fix, wearing a different wrong
-- date.
--
-- So the row carries its own confirmation date, written by the same
-- code path that observes presence: ``last_confirmed_on`` is bumped
-- on every sync whose feed returns the instrument, and the close sets
-- ``effective_to := last_confirmed_on``. That is exactly "the last
-- date the provider actually returned it", by construction rather
-- than by inference. The
-- ``instrument_universe_membership_closed_at_last_confirmed`` CHECK
-- pins it, so a future regression cannot smuggle a detection date
-- into ``effective_to`` — the constraint rejects the row.
--
-- ⚠ This is Form 25 trap 5 in a different costume
-- (.claude/skills/data-sources/sec-edgar.md §2.6): a Form 25 carries
-- filed / suspension / removal-effective dates and only the
-- suspension date truncates a price series. Same shape — the
-- detection date is never the date the consumer needs.
--
-- NO BACKFILL. The transitions were destroyed as they happened and
-- inventing dates for the existing ``is_tradable = FALSE`` rows would
-- be laundering (#2284's prevention lesson). The record starts empty
-- and accrues. ``source_event = 'imported'`` marks rows whose true
-- membership start predates the table and is unknown, so a consumer
-- knows not to read "no row before ``effective_from``" as evidence of
-- non-membership.
--
-- Temporal invariants are copied from
-- sql/103_instrument_symbol_history.sql, per the #2290 "reuse, do not
-- reinvent" instruction: ordered ranges, single-current per
-- instrument, no overlap.
--
-- ⚠ ONE convention deliberately DIVERGES from sql/103: the range is
-- INCLUSIVE-inclusive (``'[]'``), not half-open (``'[)'``). That is
-- forced by what ``effective_to`` means here. sql/103's
-- ``effective_to`` is the date the old symbol STOPPED applying — a
-- half-open upper bound. #2290 defines this one as "the last date the
-- provider actually returned the instrument", which is a date the
-- instrument WAS a member, so it belongs inside the range. Under
-- ``'[)'`` the closed row would assert the instrument was absent on
-- the very last day we confirmed it present, and the
-- ``effective_to = last_confirmed_on`` CHECK below would be
-- incoherent rather than load-bearing.
--
-- The convention also decides a real edge case. An instrument that
-- appears on Monday and is gone by Tuesday has
-- ``last_confirmed_on = effective_from``. Under ``'[)'`` that row
-- cannot be closed at all (a zero-duration range violates ordered
-- dates) and the only options are to delete it — destroying the
-- record of a genuine one-day membership, which is precisely the
-- class of transition this table exists to capture — or to stamp a
-- date the provider never confirmed. Under ``'[]'`` it closes
-- cleanly as the single-day range [Mon, Mon].
--
-- Verified against this Postgres before adopting:
--   daterange(D, D, '[]')     -> [D, D+1)  isempty=false, @> D = true
--   daterange(D, NULL, '[]')  -> [D, )     unbounded above

--
-- ⚠ ``_PLANNER_TABLES`` in tests/fixtures/ebull_test_db.py is
-- deliberately NOT updated. Per the #1568 update to the prevention-log
-- entry, ``_build_cleanup_plan`` derives the wipe set from
-- ``pg_constraint``, so a table with an inbound FK path to
-- ``instruments`` — as this one has — is picked up automatically. The
-- list's own comment records that ``strategy_signals`` /
-- ``strategy_outcomes`` "are NOT listed and must not be" for exactly
-- this reason; every surviving entry means "underivable".

CREATE TABLE IF NOT EXISTS instrument_universe_membership (
    instrument_id     BIGINT NOT NULL
        REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    effective_from    DATE NOT NULL,
    effective_to      DATE,
    last_confirmed_on DATE NOT NULL,
    source_event      TEXT NOT NULL
        CHECK (source_event IN ('imported', 'listing', 'relisting')),
    PRIMARY KEY (instrument_id, effective_from),
    CONSTRAINT instrument_universe_membership_dates_ordered
        CHECK (effective_to IS NULL OR effective_to >= effective_from),
    CONSTRAINT instrument_universe_membership_confirmed_after_start
        CHECK (last_confirmed_on >= effective_from),
    CONSTRAINT instrument_universe_membership_closed_at_last_confirmed
        CHECK (effective_to IS NULL OR effective_to = last_confirmed_on)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_instrument_universe_membership_current
    ON instrument_universe_membership (instrument_id)
    WHERE effective_to IS NULL;

ALTER TABLE instrument_universe_membership
    ADD CONSTRAINT instrument_universe_membership_no_overlap
    EXCLUDE USING GIST (
        instrument_id WITH =,
        daterange(effective_from, effective_to, '[]') WITH &&
    );

COMMENT ON TABLE instrument_universe_membership IS
    'Append-only record of tradable-universe membership episodes per '
    'instrument (#2290). One open row (effective_to IS NULL) per '
    'instrument at most; a relisting after a real gap opens a NEW row '
    'rather than reopening the closed one, because ticker reuse is '
    'real (#2284 measured 10 of Yahoo''s 48 cohort hits as a later '
    'occupant of the symbol). The one exception is a same-day '
    'close-then-reappear, which is a provider flip-flop and not a '
    'relisting — there was no day on which the instrument was absent — '
    'and is undone by reopening, exactly as reconcile_symbol_history '
    'pass 2 undoes a same-day symbol flip. Ranges are '
    'INCLUSIVE-inclusive: effective_to is the last date of membership, '
    'not the first date after it. Point-in-time query — write it as a '
    'range containment so it CAN use the GIST index the no-overlap '
    'EXCLUDE already builds: SELECT instrument_id FROM '
    'instrument_universe_membership WHERE daterange(effective_from, '
    'effective_to, ''[]'') @> DATE ''2026-08-08''. Measured on 12,687 '
    'rows: Bitmap Index Scan on ..._no_overlap for a selective date '
    '(0.58 ms); the planner correctly falls back to a seq scan (1.6 ms) '
    'for a date most rows contain.';

COMMENT ON COLUMN instrument_universe_membership.last_confirmed_on IS
    'Most recent date on which the provider feed actually returned this '
    'instrument. Bumped by every sync that returns it; the close sets '
    'effective_to := last_confirmed_on. This column exists because '
    'instruments.last_seen_at does NOT track feed presence — its bump '
    'sits inside the upsert''s changed-metadata WHERE guard, so it '
    'means "last time this row changed" (measured 2026-08-08: nine '
    'distinct dates across 12,696 rows). See the migration header.';

COMMENT ON COLUMN instrument_universe_membership.effective_to IS
    'Last date the provider returned the instrument, NOT the date we '
    'detected its absence. CHECK-pinned equal to last_confirmed_on so '
    'a detection date cannot be stamped here by a later regression.';

COMMENT ON COLUMN instrument_universe_membership.source_event IS
    'How this row OPENED: imported (tradable on the ONE run that first '
    'populated this table — true start unknown and truncated here), '
    'listing (the instrument itself first appeared in the feed on '
    'effective_from), relisting (it came back after an absence). '
    '⚠ A relisting row does NOT always have a closed predecessor row: '
    'an instrument that was already is_tradable = FALSE when the table '
    'was seeded got no row at all, so its return is a relisting whose '
    'absence predates the record. Labelling that case imported would '
    'claim it was tradable at seed time, which is exactly false. '
    'CHECK-constrained so a regression cannot smuggle a fourth value. '
    'There is deliberately no close-reason value: a close has exactly '
    'one cause, the provider omitting the instrument.';
