-- 272_strategy_scan_watermark.sql
--
-- Resume point for the daily signal scan (#2394 §3.1; spec
-- docs/proposals/ta/2026-08-08-strategy-signal-scan.md §3.1).
--
-- WHY A WATERMARK AT ALL
--
-- ``signal_ledger.store_signals`` carries no ``ON CONFLICT``, deliberately:
-- *"DO UPDATE would let a re-run overwrite a recorded decision"* and
-- ``DO NOTHING`` would hide corpus drift. So a second run on the same day must
-- write nothing at all, and the only way to know that without asking the
-- uniqueness key (which answers by RAISING, after the work is done) is to
-- record how far the scan has got.
--
-- ⚠⚠ IT HOLDS A CALENDAR FRONTIER, NOT A ``signal_bar_date``.
--
-- The scan's write date is the bar before the frontier **on each instrument's
-- own calendar**, and ``BarSeries`` allows gaps and never interpolates them, so
-- that is a distribution rather than a constant. Measured on the live corpus
-- 2026-08-08 by ``scripts/verify_2394_signal_scan_cost.py --cost``:
--
--     write-date distribution across eligible series:
--       2026-08-06=5779, 2026-07-31=1, 2026-08-05=1, 2026-08-04=1, 2026-07-28=1
--
-- A watermark holding one signal date could not describe that write. Holding the
-- frontier instead settles three things at once: a same-day re-run has
-- ``watermark = frontier`` and does nothing; an instrument that missed sessions
-- gets every unwritten bar in the window rather than only the newest; and a
-- market holiday leaves the frontier unmoved, so the scan writes nothing and is
-- NOT wedged. That last one is the prevention-log entry *"the per-day watermark
-- only advances on success, so the date was re-requested every run and wedged
-- phase 1 indefinitely"* avoided by construction rather than by tolerance — this
-- watermark tracks a frontier the corpus supplies, never a date the scan hoped
-- to find.
--
-- ⚠ KEYED ON ``(strategy_id, strategy_version)``, matching the ledger's own key
-- prefix. A version bump is a new track record beside the old one (spec §10), so
-- it must start from its own resume point rather than inherit one that describes
-- rows written under different code.
--
-- ⚠ ``frontier_date`` is NOT a foreign key to anything and must not become one.
-- It is a date the corpus held at the moment of the scan; the bar it names can
-- later be revised or removed, and the watermark still correctly records how far
-- the scan got. (#2414 is the open ticket on what a REVISED bar does to an
-- already-written signal — that is a decision about the ledger, not this table.)

CREATE TABLE IF NOT EXISTS strategy_scan_watermark (
    strategy_id      TEXT        NOT NULL,
    strategy_version TEXT        NOT NULL,
    -- The last frontier the scan COMPLETED for this identity. The next run
    -- writes each instrument's bars strictly after this date and strictly
    -- before that instrument's own last bar.
    frontier_date    DATE        NOT NULL,
    -- Wall clock of the advance, for the operator surface only. Never read as
    -- a resume point — a run that completes with zero rows still advances the
    -- frontier, and "wrote nothing" and "failed" have to stay distinguishable
    -- through frontier_date, not through a timestamp.
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (strategy_id, strategy_version)
);

COMMENT ON TABLE strategy_scan_watermark IS
    'Per-(strategy_id, strategy_version) resume point for strategy_signal_scan: the last frontier date '
    'completed. #2394 §3.1.';
