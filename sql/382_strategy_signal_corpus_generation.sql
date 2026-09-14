-- #2414 — record WHICH corpus state produced a stored strategy decision.
--
-- WHAT THIS IS, AND WHAT IT DELIBERATELY IS NOT
--
-- `strategy_version` hashes the producer (module, params, universe, cost model,
-- and since #2333/#3031 the input RULE SETS). It records nothing about the
-- corpus VALUES the decision read. `price_daily` is retroactively
-- split-adjusted, so a bar can change under a stored decision at an age no
-- embargo would cover -- measured max 275 days (#2414, `ef93efcc` histogram).
--
-- ⚠⚠ THIS COLUMN IS THE PREREQUISITE HALF OF #2414, NOT THE WHOLE TICKET.
--
-- #2414 comes verbatim from §12 of docs/proposals/ta/2026-08-08-strategy-signal-
-- scan.md: *"A corrected historical bar cannot be reflected in an already-written
-- signal ... It needs its own ticket, and the shape of the fix (a corpus version
-- in the key, or an explicit supersede-and-record path) is a decision about the
-- ledger, not about the scan."*
--
-- Both candidate shapes need a COMPUTABLE CORPUS IDENTITY and neither can be
-- built without one. This migration stores that identity BESIDE the key rather
-- than in it, so it prejudges neither shape:
--
--   * `strategy_signals_unique` is UNCHANGED -- five columns. NOT key material.
--   * `signal_ledger.store_signals` still has no `ON CONFLICT` and still raises.
--   * On a ledger row the column is WRITE-ONCE: a row's generation is the corpus
--     that produced that decision, for the life of the row. (The watermark below
--     is the one deliberate exception, and says why.)
--
-- ⚠ §11 of the same spec ("It does not backfill") is a DIFFERENT rule and does
-- NOT settle §12. §11 governs the COLD START -- deriving a decision for a bar
-- that was never decided. A first draft of this migration read §11 as forbidding
-- the supersession half outright; it does not, and conflating the two would have
-- closed the ticket on a misreading.
--
-- What the column buys today is the one question the ledger could not answer:
-- were these two stored decisions computed against the same corpus state? That is
-- provenance, and it is comparable across rows without restating any of them.
-- The supersession half stays open on #2414: two checkpoint-1 passes established
-- it reaches eight tables, five foreign-key dependents of `signal_id`, and a
-- retraction case (a revision that makes a fired signal NOT fire) that has no
-- encoding yet.
--
-- NULLABLE, NO DEFAULT.
--
-- The 58,711 rows that exist at migration time cannot be stamped: which corpus
-- state produced them is unrecoverable, exactly as pre-`ef93efcc` revision ages
-- are. NULL is the honest encoding of "unknown". Because the column is neither
-- key material nor ever updated, a NULL needs no conflict semantics at all --
-- a mixed-version rollout in which an older writer inserts an unstamped row
-- degrades to "provenance unrecorded" and to nothing else. A sentinel string
-- would instead have to be admitted by the CHECK below, which would stop the
-- CHECK from saying "a generation is sixteen hex characters" and would force the
-- `LedgerRow` mirror to diverge from it.
--
-- WIDTH: 16 hex (64 bits), NOT `strategy_version`'s 12.
--
-- 12 hex is a legibility choice for a value an operator reads and types. A
-- generation is only ever compared, so width costs nothing. A 64-bit collision
-- is expected at ~2^32 distinct corpus states and would make two different
-- corpora LOOK identical in an audit; it cannot admit a wrong row, because the
-- column is not key material. Stating the exposure is possible precisely because
-- the consequence is bounded.

ALTER TABLE strategy_signals
    ADD COLUMN IF NOT EXISTS corpus_generation TEXT
        CHECK (corpus_generation IS NULL OR corpus_generation ~ '^[0-9a-f]{16}$');

-- The routine tier gets the same stamp for the same reason: 813,194 of the
-- 871,905 stored decisions are here, and a `not_fired` computed against revised
-- bars is exactly as provenance-free as a `fired` one. ⚠ Partitioned parent --
-- ADD COLUMN propagates to every existing leaf.
ALTER TABLE strategy_signal_observations
    ADD COLUMN IF NOT EXISTS corpus_generation TEXT
        CHECK (corpus_generation IS NULL OR corpus_generation ~ '^[0-9a-f]{16}$');

-- The durable census outlives the 90-day observation detail it summarises, so
-- without this the only surviving record of a scan pass would carry no
-- provenance at all. ⚠ NOT key material here either: the primary key stays
-- (strategy_id, strategy_version, signal_bar_date, signal_kind, verdict,
-- reason_code), so a census row remains one row per day per bucket. Under §11 a
-- second pass cannot re-decide a covered day, so a second generation for one
-- census bucket is unreachable rather than merely unhandled.
ALTER TABLE strategy_signal_daily_counts
    ADD COLUMN IF NOT EXISTS corpus_generation TEXT
        CHECK (corpus_generation IS NULL OR corpus_generation ~ '^[0-9a-f]{16}$');

-- ⚠⚠ THE ONE PLACE THE STAMP IS NOT WRITE-ONCE, AND THE DIFFERENCE IS THE POINT.
--
-- A ledger row records the corpus that produced THAT decision and must never
-- move. The watermark records where an identity got to and against WHICH corpus
-- it last got there, so `advance_watermark` overwrites it on every pass. That is
-- what makes "has the corpus moved under this strategy since it last scanned?"
-- answerable by comparing one stored value against the next pass's stamp --
-- without re-deciding anything, which spec §11 forbids. A run that writes zero
-- rows still advances the watermark, so the comparison does not go stale on a
-- quiet day.
--
-- This is the DETECTION half of the scan spec's §12 residual ("a corpus version
-- in the key, or an explicit supersede-and-record path"). Neither of those two
-- shapes can be built without a computable corpus identity, and neither is
-- prejudged by storing one.
ALTER TABLE strategy_scan_watermark
    ADD COLUMN IF NOT EXISTS corpus_generation TEXT
        CHECK (corpus_generation IS NULL OR corpus_generation ~ '^[0-9a-f]{16}$');

COMMENT ON COLUMN strategy_scan_watermark.corpus_generation IS
    'The corpus this identity last scanned against (#2414). ⚠ UPDATED every '
    'pass, unlike the write-once stamp on strategy_signals -- comparing it to '
    'the next pass''s generation answers "has the corpus moved under this '
    'strategy?" without re-deciding any bar.';

COMMENT ON COLUMN strategy_signals.corpus_generation IS
    'Provenance: the 16-hex digest of the corpus the scan pass that wrote this '
    'row read (app/services/corpus_generation.py). WRITE-ONCE and NOT key '
    'material -- it answers "were these two decisions computed against the same '
    'corpus?" and deliberately does NOT permit a corrected row, because spec §11 '
    'forbids deriving a historical decision from today''s bars. NULL on rows '
    'written before #2414.';

COMMENT ON COLUMN strategy_signal_observations.corpus_generation IS
    'See strategy_signals.corpus_generation. Same stamp, same pass, same '
    'write-once rule.';

COMMENT ON COLUMN strategy_signal_daily_counts.corpus_generation IS
    'See strategy_signals.corpus_generation. The census outlives the 90-day '
    'observation detail, so this is the surviving provenance for a scan pass.';
