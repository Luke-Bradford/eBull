-- #2414 item 2 — record the bar that appeared BEHIND the frontier the writer saw.
--
-- WHY A SIBLING TABLE AND NOT A COLUMN ON `price_daily_revision`
--
-- `sql/387`'s header names this exact gap and declines it: an OHLCV overwrite is
-- the only class it records, while "a HISTORICAL BAR IS INSERTED (stale
-- re-observation, interior gap fill) … produce NO row here, yet they move every
-- later recursive indicator". It also says widening that table "would make its
-- name a lie", so a rename (`price_daily_mutation` + a `mutation_kind` column)
-- was the shape specced for this slice.
--
-- ⚠⚠ Codex checkpoint 1 killed the rename on a DEPLOYMENT ground, and it is the
-- reason this file adds a table instead. The audit write lives INSIDE the bar
-- write's transaction (that is `sql/387`'s whole point). Migrations apply at API
-- boot; the jobs child keeps running OLD code until it respawns, and
-- `app.jobs.dev_reload` can legitimately DEFER a respawn while a 6,000-second
-- sweep is in flight (observed 2026-09-15, #2274's heartbeat holding a deploy
-- off run 131002). So a rename opens a window in which the old writer INSERTs
-- into a table that no longer exists, its audit write raises, and it takes the
-- BAR WRITE down with it. An audit row must never be able to destroy the thing
-- it audits. Adding a table has no such window: old code writes only to
-- `price_daily_revision`, which is untouched here.
--
-- `price_daily_corpus_mutation` (below) is the completeness surface, so a
-- consumer cannot read one class and silently miss the other.
--
-- WHAT "BACKDATED" MEANS, AND WHAT IT DOES NOT
--
-- `backdated  ⟺  bar.price_date < frontier_before`, where `frontier_before` is
-- `MAX(price_date)` for that instrument as the writer observed it. A bar ABOVE
-- the frontier extends the series; one below lands behind history that was
-- already committed and therefore already readable by a decision.
--
-- ⚠ `frontier_before` is STORED rather than implied. Without it the
-- classification cannot be reconstructed, and the classification is not a
-- property of the corpus alone — it is a property of what one writer observed.
-- Under Read Committed a concurrent writer can commit bars between this writer's
-- frontier read and its own insert, so two runs can classify the same date
-- differently and both be honest. Storing the frontier is what makes that
-- legible instead of a contradiction.
--
-- ⚠⚠ THIS IS NOT "DECISIONS THAT CONSUMED THIS BAR MOVED".
--
--   * The raw stored maximum is not the frontier a scan uses.
--     `strategy_signal_scan` derives a POPULATION frontier, then masks
--     (`load_masked_bars`), applies eligibility and segments. A future-dated or
--     quarantined bar can be the stored maximum and be consumed by nothing.
--   * A FORWARD extension is not universally irrelevant either: a
--     cross-sectional or benchmark-gated decision can move when some OTHER
--     instrument gains a bar. This table is per-instrument and says nothing
--     about that. (#2414 item 3 — latent, both cross-sectional strategies are
--     retired under #2845.)
--   * An insert does not necessarily change every later value, only that no
--     structural cut-off bounds how far it CAN reach. `atr_series` / `adx_series`
--     are Wilder recursions seeded once at bar `period` (SOURCE RULE: J. Welles
--     Wilder Jr., *New Concepts in Technical Trading Systems* (1978), ch. 4 —
--     cited in `app/services/indicator_series.py`), so the dependence decays
--     geometrically without ever reaching zero. Segment restarts, bounded
--     windows (SMA, Bollinger), masked fields and identical values all defeat
--     the stronger "every value changes" claim.
--   * It does not record DELETIONS. `price_daily` has no delete path outside
--     test fixtures (grepped), so a bar's lifecycle is reconstructible only under
--     that assumption. Named, not assumed away.
--
-- So this is the second half of the INPUT #2414's supersession half needs, and it
-- is still not sufficient on its own — `sql/387`'s header enumerates the ten
-- reasons the obvious `strategy_signals` join is neither an upper nor a lower
-- bound, and none of them is repaired by adding a mutation class.

CREATE TABLE IF NOT EXISTS price_daily_backdated_insert (
    insert_id       BIGSERIAL PRIMARY KEY,
    instrument_id   BIGINT      NOT NULL REFERENCES instruments(instrument_id),
    -- The date of the bar that was inserted.
    price_date      DATE        NOT NULL,
    -- MAX(price_date) for this instrument as the writer observed it, immediately
    -- before the upsert and inside the same transaction. NOT NULL: a writer with
    -- no prior history produces no row here at all, so every row has a frontier.
    -- ⚠ `price_date < frontier_before` holds for every row by construction, and
    -- `scripts/verify_2414_backdated_insert_census.py --census` asserts it —
    -- which is possible only because the frontier is stored.
    frontier_before DATE        NOT NULL,
    -- Transaction start time. ⚠ NOT a commit order and NOT a read-visibility
    -- oracle; see `sql/387`'s header on `revised_at`.
    inserted_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Same closed vocabulary as `price_daily_revision.cause` — it is the WRITE
    -- BRANCH, not an economic cause, and one fetch can insert bars for more than
    -- one underlying reason at once. A test pins this set equal to
    -- `RevisionCause` in `app/services/market_data.py`.
    -- ⚠ `initial_backfill` is NOT unreachable here. It looks impossible (no
    -- prior history ⇒ no frontier ⇒ no backdated bar) and is not:
    -- `_candles_fetch_count` and the frontier read are two separate statements,
    -- so a concurrent writer committing history between them yields
    -- `initial_backfill` with a real frontier. A nonzero count is a concurrency
    -- observation, not a classifier defect.
    cause           TEXT        NOT NULL CHECK (cause IN (
                        'initial_backfill', 'stale_reobservation', 'incremental',
                        'adjustment_heal', 'force_backfill', 'unknown'))
);

-- APPEND-ONLY, and no UNIQUE (instrument_id, price_date). Unlike a revision, the
-- same date cannot be inserted twice WITHIN one call — the second occurrence of a
-- duplicated date conflicts and becomes a revision or an `IS DISTINCT FROM`
-- no-op. Across calls it can, if the row is ever removed and re-observed, which
-- is why there is no natural key.
CREATE INDEX IF NOT EXISTS idx_price_daily_backdated_insert_instrument_date
    ON price_daily_backdated_insert (instrument_id, price_date);

COMMENT ON TABLE price_daily_backdated_insert IS
    'One row per bar INSERTED below the frontier the writer observed (#2414 item 2). '
    'Sibling of price_daily_revision, which records OVERWRITES only. Read both '
    'through price_daily_corpus_mutation. NOT a record of which decisions moved.';
COMMENT ON COLUMN price_daily_backdated_insert.frontier_before IS
    'MAX(price_date) for this instrument as this writer observed it, in-transaction. '
    'Stored so the classification is reconstructible: under Read Committed two '
    'concurrent writers can classify the same date differently and both be honest.';
COMMENT ON COLUMN price_daily_backdated_insert.inserted_at IS
    'Transaction start time. Not a commit order, and not a read-visibility oracle.';
COMMENT ON COLUMN price_daily_backdated_insert.cause IS
    'The WRITE BRANCH that inserted the bar, not an economic cause — an upper bound '
    'on attribution. initial_backfill is reachable here only via a concurrency race.';

-- The completeness surface. A consumer asking "did the corpus under this decision
-- move?" needs BOTH classes, always, together — reading one is the defect this
-- slice exists to close. The view exists so that is one relation, not a UNION a
-- future reader has to remember to write.
--
-- ⚠ The per-table surrogate keys are DELIBERATELY NOT EXPOSED. They are separate
-- sequences, so ordering by either across kinds is meaningless, and `mutated_at`
-- is a transaction start time shared by every row one transaction wrote. Nothing
-- here establishes write order or commit order between the two kinds.
CREATE OR REPLACE VIEW price_daily_corpus_mutation AS
    SELECT 'revision'::text         AS mutation_kind,
           instrument_id,
           price_date,
           revised_at               AS mutated_at,
           cause,
           NULL::date               AS frontier_before
      FROM price_daily_revision
    UNION ALL
    SELECT 'backdated_insert'::text AS mutation_kind,
           instrument_id,
           price_date,
           inserted_at              AS mutated_at,
           cause,
           frontier_before
      FROM price_daily_backdated_insert;

COMMENT ON VIEW price_daily_corpus_mutation IS
    'Both recorded price_daily mutation classes in one relation (#2414). Surrogate '
    'keys are omitted on purpose: they are separate sequences and establish no '
    'cross-kind order. Still does NOT cover quarantine-verdict or series-break '
    'changes, rule-set bumps, or deletions.';
