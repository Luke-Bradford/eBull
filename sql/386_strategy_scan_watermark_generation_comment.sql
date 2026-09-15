-- #2414 — correct what `strategy_scan_watermark.corpus_generation` CAN answer.
--
-- COMMENT-ONLY. No column, no constraint, no data. A new migration rather than
-- an edit to `sql/382_strategy_signal_corpus_generation.sql`, because editing an
-- APPLIED migration moves its sha256 and `app/db/migrations.py` raises
-- `Migration content drift` at boot.
--
-- WHAT WAS WRONG
--
-- 382's COMMENT on this column read:
--
--   '... ⚠ UPDATED every pass, unlike the write-once stamp on strategy_signals
--    -- comparing it to the next pass''s generation answers "has the corpus
--    moved under this strategy?" without re-deciding any bar.'
--
-- That last clause is FALSE BY CONSTRUCTION, and `advance_watermark`'s docstring
-- carried the same claim at length.
--
--   * `CorpusGenerationBuilder.finish()` puts `frontier_date` in the hashed
--     payload (`app/services/corpus_generation.py:362`). That is CORRECT and
--     deliberate: the stamp identifies what a pass READ, and the frontier is
--     that read's boundary (construction doc §2's payload table names it "the
--     corpus boundary").
--   * `_ADVANCE_WATERMARK` only updates on a STRICTLY GREATER `frontier_date`,
--     and `run_signal_scan` refuses a frontier at or behind the watermark before
--     doing any work.
--
-- So every stored advance necessarily carries a new frontier and therefore a new
-- stamp. Comparing the stored value against the next pass's is a constant TRUE
-- on a corpus where not one bar changed. Pinned by
-- `tests/test_corpus_generation.py::
--  test_the_frontier_alone_rotates_the_stamp_so_the_watermark_comparison_is_constant`
-- (identical corpus, frontier one day apart, digests differ).
--
-- ⚠⚠ THE GENERAL RESULT, so nobody rebuilds this on the next attempt: NO
-- PASS-SCOPED DIGEST CAN ANSWER "did data I already decided on change?". Even
-- with `frontier_date` removed from the payload, the pass reads one more day of
-- bars than the previous pass did, so the bars digest rotates on ordinary growth
-- alone. That question is about the OVERLAP -- the bars at or before the
-- previous frontier -- and needs a prefix-scoped digest, which is a different
-- and more expensive computation than the one this column stores.
--
-- WHAT THE COLUMN DOES BUY, unchanged and still worth storing: the corpus a
-- given identity last scanned against, comparable to the write-once stamp on the
-- rows that pass wrote. Construction doc §3.5(4) states the direction that
-- holds: equal stamps imply one corpus; different stamps do NOT imply different
-- corpora.
--
-- The supersession half of #2414 stays open and is NOT prejudged by this.

COMMENT ON COLUMN strategy_scan_watermark.corpus_generation IS
    'The corpus this identity last scanned against (#2414). ⚠ UPDATED every '
    'pass, unlike the write-once stamp on strategy_signals. ⚠⚠ It CANNOT answer '
    '"has the corpus moved under this strategy?" — sql/382''s COMMENT claimed it '
    'could and was wrong: frontier_date is inside the hashed payload and the '
    'watermark only advances on a strictly greater frontier, so the stored and '
    'next stamps ALWAYS differ. What it does buy is the corpus identity of this '
    'identity''s last pass, comparable to the write-once stamp on the rows that '
    'pass wrote — equal stamps imply one corpus, different stamps do not imply '
    'different corpora. See sql/386 for the full reasoning.';
