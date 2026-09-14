-- #3040 — record the ``as_of`` that produced a research coverage row.
--
-- WHY THIS COLUMN EXISTS
--
-- ``research_corpus_ingest.run_quarantine(conn, *, vendor, as_of)`` takes an
-- ``as_of`` and it is an INPUT to the stored verdicts, not a label:
-- ``price_quarantine.py`` computes ``provisional_from = as_of - 5 days`` and a
-- provisional bar suppresses T3's volume read and propagates through the
-- transition verdict. The coverage table recorded ``rule_set_version`` only, so
-- two runs under one version could store different verdicts and nothing — not
-- the coverage table, not the census view, not ``research_price_read_canary``'s
-- ``coverage_current`` — could tell.
--
-- That is the defect the prevention log states as *"a versioned key on a
-- derived table where the pipeline feeding it is ALSO versioned and that
-- version is not in the key"* (#2898-2900, written for strategy_outcomes and
-- the same shape here). The concrete hole it leaves open: an operator running
-- ``scripts/ingest_2597_intrader_archive.py --quarantine --as-of 2020-01-01``
-- writes divergent verdicts under an unchanged rule-set version, after which
-- every freshness check reports the corpus current, forever.
--
-- BACKFILL
--
-- The two archive vendors' declared capture policies live on
-- ``ArchiveProvenance.quarantine_as_of`` and are pinned literals, not
-- ``max(last_bar)`` — a later-ending series must not silently re-date a whole
-- vendor's policy. Reproduce these two numbers with:
--
--   select vendor, max(last_bar) from research_price_series
--   where vendor in ('icyDenev/Intrader','paperswithbacktest/Stocks-Daily-Price')
--   group by 1;
--
-- Intrader's rows were written at its capture date and are backfilled to it.
--
-- ⚠ The HF rows were written with the script's ``date.today()`` default, NOT
-- with the declared 2026-07-14, so they are deliberately backfilled to a
-- sentinel that CANNOT match the declaration (1970-01-01). Both dates produce
-- identical verdicts — nothing in an archive whose last bar is 2026-07-08 is
-- provisional under either — but backfilling the declared value would assert a
-- provenance this migration cannot verify. The sentinel makes the vendor read
-- as not-at-declared-policy, so the new job re-evaluates it once (165 s,
-- measured) and writes the value it actually used. A claim we can check beats a
-- claim that happens to be true.
--
-- Series belonging to any other vendor have no coverage row at all (19 series
-- across ``cboe`` and ``etoro/etoro-comparators-2026-07-08-v1``), so the
-- ``NOT NULL`` add needs no third branch.

ALTER TABLE research_price_quarantine_coverage
    ADD COLUMN IF NOT EXISTS quarantine_as_of DATE;

UPDATE research_price_quarantine_coverage c
   SET quarantine_as_of = CASE s.vendor
        WHEN 'icyDenev/Intrader' THEN DATE '2024-09-27'
        ELSE DATE '1970-01-01'
       END
  FROM research_price_series s
 WHERE s.series_id = c.series_id
   AND c.quarantine_as_of IS NULL;

-- Any coverage row whose series has since been deleted cannot exist (the FK
-- cascades), so the UPDATE above reaches every row. Belt-and-braces for a
-- re-run against a partially-migrated DB.
UPDATE research_price_quarantine_coverage
   SET quarantine_as_of = DATE '1970-01-01'
 WHERE quarantine_as_of IS NULL;

ALTER TABLE research_price_quarantine_coverage
    ALTER COLUMN quarantine_as_of SET NOT NULL;

COMMENT ON COLUMN research_price_quarantine_coverage.quarantine_as_of IS
    'The as_of passed to run_quarantine for this series. An INPUT to the '
    'verdicts (provisional_from = as_of - PROVISIONAL_WINDOW_DAYS), so '
    'freshness matches on (rule_set_version, quarantine_as_of) — a version '
    'match alone cannot distinguish a declared-policy run from an operator '
    '--as-of override (#3040).';

CREATE INDEX IF NOT EXISTS idx_research_quarantine_coverage_version_as_of
    ON research_price_quarantine_coverage (rule_set_version, quarantine_as_of);
