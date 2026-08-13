-- 253_research_series_delisting_provision.sql
--
-- #2297 — wire the Form 25 register (sql/252) to the research corpus (sql/249)
-- and make the stored delisting date SAFE TO READ.
--
--
-- WHY A PROVISION COLUMN AND NOT JUST THE DATE
-- ---------------------------------------------------------------------------
-- sql/249 shipped `delisting_date` + `delisting_source` and nothing populated
-- them. The obvious fix — join the register's `resolved_symbol` to
-- `vendor_symbol` and write the suspension date — creates a WORSE bug than the
-- one it closes, because of an asymmetry measured on the full 2023 register:
--
--     provision   cohort rows   with a suspension_date
--     (a)(3)          212              62
--     (b)             105               0
--
-- `(b)` is exchange-initiated delisting for non-compliance: the ticker died,
-- and truncating there is unambiguously right. It NEVER states a suspension
-- date, because the date lives in the EX-99 rule-provision exhibit and
-- exchanges attach a stub (sec-edgar.md §2.6 trap 5).
--
-- `(a)(3)` is "instruments now evidence OTHER securities by operation of law"
-- — merger, holdco reorganisation, redomiciliation. The same economic entity
-- very often keeps trading under the same ticker, so a continuous series is
-- the CORRECT answer, not contamination. Every suspension date the cohort
-- supplies is on this provision.
--
-- So a bare `delisting_date` is a loaded gun: it can only ever be populated
-- where truncating on it is likely to be wrong. Measured on the corpus, the
-- two series it reaches are `LIN` (Linde plc, 8,572 bars, 1992-2026) and
-- `AMRX` (Amneal, 2,051 bars) — BOTH `instruments.is_tradable = true` today.
-- Truncating either deletes ~3 years of correct history from a live name.
--
-- The provision travels with the date so no reader can lose that distinction.
-- It is deliberately NOT a boolean: `(b)` and `(a)(3)` are the whole point,
-- and sql/252's header makes the same argument for the register itself.
--
--
-- WHAT IS **NOT** DENORMALISED HERE, AND WHY
-- ---------------------------------------------------------------------------
-- The filing date, the accession and the issuer stay in `sec_form25_register`
-- and are read by joining `resolved_symbol = vendor_symbol`. Both tables live
-- in this database, the join is 16 rows today, and copying those fields across
-- would be three more things to drift. Only the provision is copied, because
-- it is the one field a reader of `delisting_date` cannot safely be without.

ALTER TABLE research_price_series
    ADD COLUMN IF NOT EXISTS delisting_provision TEXT;

-- BICONDITIONAL, both directions load-bearing:
--
--   → A Form 25-sourced delisting MUST carry its provision. A one-way check
--     ("provision only from sec_form25") would still permit the bare date this
--     migration exists to abolish, reachable by a manual UPDATE or a future
--     writer — i.e. the schema would still be able to represent the unsafe
--     state.
--   ← #2290's forward record for non-US names and any vendor flag have no
--     Rule 12d2-2 paragraph, so a provision on those is a fabricated citation.
--
-- ``IS NOT DISTINCT FROM`` rather than ``=`` because delisting_source is
-- nullable and a NULL comparison would make the whole CHECK evaluate to NULL,
-- which Postgres accepts as satisfied — the constraint would silently pass on
-- exactly the rows that carry no source.
ALTER TABLE research_price_series
    DROP CONSTRAINT IF EXISTS research_price_series_provision_from_form25;
ALTER TABLE research_price_series
    ADD CONSTRAINT research_price_series_provision_from_form25
        CHECK (
            (delisting_source IS NOT DISTINCT FROM 'sec_form25')
            = (delisting_provision IS NOT NULL)
        );

COMMENT ON COLUMN research_price_series.delisting_provision IS
    'Rule 12d2-2 paragraph from the Form 25 that supplied delisting_date, e.g. '
    '''(a)(3)'' or ''(b)''. ⚠ READ THIS BEFORE TRUNCATING ON delisting_date. '
    '(b) is an exchange-initiated failure and the series should end; (a)(3) is '
    'a merger/holdco reorganisation where the same ticker very often keeps '
    'trading and a continuous series is correct. Measured on the 2023 register: '
    'all 62 cohort suspension dates are (a)(3), and (b) supplies none at all '
    '(sec-edgar.md 2.6 trap 5), so a provision-blind truncation fires only '
    'where it is most likely to be wrong.';
