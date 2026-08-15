-- 353_form25_evidence_without_suspension_date.sql
--
-- #2721 step 1 — let Form 25 evidence persist WITHOUT a suspension date.
--
--
-- THE STRUCTURAL GAP THIS CLOSES
-- ---------------------------------------------------------------------------
-- sql/249 CHECK-paired `delisting_date` with `delisting_source` in BOTH
-- directions, and sql/253 biconditionally paired `delisting_provision` with
-- `delisting_source = 'sec_form25'`. Together those two constraints make
-- "linked to a Form 25 that states no suspension date" UNREPRESENTABLE: no
-- date -> no source -> no provision. Measured on the 2023 register
-- (#2721, 2026-08-15): `(b)` — exchange-initiated delisting for
-- non-compliance, the one provision where a held position's termination
-- treatment matters most — states a suspension date on 0 of 105 cohort rows
-- (the date lives in the EX-99 exhibit and exchanges attach a stub,
-- sec-edgar.md §2.6 trap 5). So every one of the 88 `(b)` symbols present in
-- the Intrader corpus linked to NOTHING, while all 39 written links are
-- `(a)(3)` — the merger/reorg provision where a continuous series is usually
-- correct. The schema itself enforced exactly the wrong asymmetry.
--
-- What changes and what does not:
--
--   * `delisting_source` + `delisting_provision` may now exist with
--     `delisting_date` NULL — "a Form 25 removed this security; the filing
--     states no suspension date". The date is NEVER back-filled from
--     `filed_date`: that is a different event (a Form 25 carries up to three
--     dates) and substituting it mistruncates every series it touches.
--   * A bare `delisting_date` (date without source) stays impossible.
--   * A DATED 'sec_form25' row without a provision stays impossible —
--     sql/253's loaded-gun argument is about truncating on a date whose
--     provision you cannot read, and that guard is kept verbatim.
--   * `delisting_provision` may now be NULL on an undated 'sec_form25' row.
--     The 2023 cohort happens to contain zero NULL-provision rows
--     (`SELECT coalesce(rule_provision,'(NULL)'), count(*) FROM
--     sec_form25_common_equity_delistings GROUP BY 1` -> (a)(3) 212, (b) 105)
--     but the 25-NSE form carries no <ruleProvision> element by design
--     (app/services/sec_form25_register.py::classify_provision), so the
--     2013-2024 expansion can produce them. "Linked, provision unparsed" is a
--     real evidence state and gets its own class downstream, not a fabricated
--     paragraph.
--
-- `delisting_filed_date` is added because the earliest linked filing date is
-- evidence in its own right (the series ended BEFORE any Form 25 on the
-- symbol = a genuine termination), and sql/253's "read it by joining
-- resolved_symbol = vendor_symbol" no longer works: #2597 replaced that join
-- with the Q-suffix candidate ladder in Python, so SQL readers cannot
-- reproduce the resolution.

ALTER TABLE research_price_series
    ADD COLUMN IF NOT EXISTS delisting_filed_date DATE;

-- Was biconditional (date IS NULL) = (source IS NULL). What remains:
--   1. a date with no source is still a fabrication;
--   2. the undated-evidence state is opened for 'sec_form25' ONLY — the other
--      sources (#2290's forward record, vendor flags) have no filing to be
--      evidence OF, so an undated row under them would be the old integrity
--      hole reopened under a different name (Codex ckpt-2 on #2721).
ALTER TABLE research_price_series
    DROP CONSTRAINT IF EXISTS research_price_series_delisting_evidenced;
ALTER TABLE research_price_series
    ADD CONSTRAINT research_price_series_delisting_evidenced
        CHECK (
            (delisting_date IS NULL OR delisting_source IS NOT NULL)
            AND (delisting_source IS NULL
                OR delisting_source = 'sec_form25'
                OR delisting_date IS NOT NULL)
        );

-- Was biconditional (source = 'sec_form25') = (provision IS NOT NULL).
-- Split into the two directions that are still true:
--   1. a provision only ever comes from a Form 25 (#2290's forward record and
--      vendor flags have no Rule 12d2-2 paragraph);
--   2. a DATED Form 25 row must carry its provision — nobody may truncate on
--      a date whose provision class they cannot read (sql/253's argument).
-- An UNDATED 'sec_form25' row with a NULL provision is now legal: linked,
-- provision unparsed.
ALTER TABLE research_price_series
    DROP CONSTRAINT IF EXISTS research_price_series_provision_from_form25;
ALTER TABLE research_price_series
    ADD CONSTRAINT research_price_series_provision_from_form25
        CHECK (
            (delisting_provision IS NULL
                OR delisting_source IS NOT DISTINCT FROM 'sec_form25')
            AND (delisting_date IS NULL
                OR delisting_source IS DISTINCT FROM 'sec_form25'
                OR delisting_provision IS NOT NULL)
        );

-- The filed date is Form 25 evidence and nothing else supplies one. One-way
-- (not biconditional) so the constraint holds over the 37 pre-existing
-- 'sec_form25' rows between this migration and the re-link that back-fills
-- them; the linkage writes it on every row it touches.
ALTER TABLE research_price_series
    DROP CONSTRAINT IF EXISTS research_price_series_filed_date_from_form25;
ALTER TABLE research_price_series
    ADD CONSTRAINT research_price_series_filed_date_from_form25
        CHECK (delisting_filed_date IS NULL
               OR delisting_source IS NOT DISTINCT FROM 'sec_form25');

COMMENT ON COLUMN research_price_series.delisting_filed_date IS
    'Earliest filed_date across the Form 25 filings linked to this series '
    '(min() over the symbol''s cohort filings — the conservative bound). '
    'Present whenever delisting_source = ''sec_form25''. NOT a suspension '
    'date and never a truncation point: a Form 25 carries up to three dates '
    'and the filing date is the one that is always stated, not the one that '
    'ends trading.';

COMMENT ON COLUMN research_price_series.delisting_source IS
    'Source of the delisting EVIDENCE on this row, not merely of '
    'delisting_date. ''sec_form25'' rows may carry a NULL delisting_date: '
    '(b) exchange-initiated filings state a suspension date on 0 of 105 '
    '2023 cohort rows (sec-edgar.md §2.6 trap 5), and refusing to store the '
    'link because the date is unstated is how the exchange-failure class — '
    'the one a termination rule needs most — stayed at zero coverage '
    '(#2721).';

-- The census view counted `count(s.delisting_date)` as "series with a
-- delisting record". Under the evidence model that undercounts: an undated
-- (b) link IS a delisting record. Count the source instead; the dated subset
-- stays visible as its own column.
CREATE OR REPLACE VIEW research_corpus_census AS
SELECT
    COALESCE(e.asset_class, CASE WHEN s.instrument_id IS NULL
                                 THEN '(unresolved)' ELSE '(unmapped)' END)
                                                       AS asset_class,
    s.vendor,
    count(*)                                           AS series,
    count(s.instrument_id)                             AS resolved_series,
    min(s.first_bar)                                   AS earliest_first_bar,
    max(s.last_bar)                                    AS latest_last_bar,
    sum(s.bar_count)                                   AS bars,
    count(*) FILTER (WHERE s.bar_count IS NULL)        AS series_without_bars,
    count(s.delisting_source)                          AS series_with_delisting,
    count(s.delisting_date)                            AS series_with_delisting_date
FROM research_price_series s
LEFT JOIN instruments i ON i.instrument_id = s.instrument_id
LEFT JOIN exchanges   e ON e.exchange_id = i.exchange
GROUP BY 1, 2;

COMMENT ON VIEW research_corpus_census IS
    'Per asset-class corpus coverage: series, resolved series, first/last bar, '
    'bar count, series carrying a delisting record (any Form 25 evidence, '
    'dated or not — see 353) plus the dated subset. The (unresolved) row is '
    'the eToro-listing-bias measurement and is reported, never filtered out.';
