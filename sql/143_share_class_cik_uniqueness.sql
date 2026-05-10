-- Migration 143: relax external_identifiers (sec, cik) uniqueness for share-class siblings (#1102)
--
-- Background. The original `uq_external_identifiers_provider_value` table
-- constraint enforced a single global UNIQUE on
-- `(provider, identifier_type, identifier_value)`. This is correct for CUSIP /
-- symbol / accession_no — those are per-security identifiers — but wrong for
-- SEC CIK, which identifies an issuer (legal entity) not a security. Two
-- share-class siblings (GOOG + GOOGL, BRK.A + BRK.B, etc.) legitimately share
-- a CIK and have distinct CUSIPs. Under the old constraint
-- `upsert_cik_mapping`'s ON CONFLICT clause rewrote the row's `instrument_id`
-- to whichever sibling SEC's ticker map iterated last, so `daily_cik_refresh`
-- flapped the binding on every run. One sibling always lost its 10-K /
-- fundamentals / filings drill-in.
--
-- Decision (operator-locked 2026-05-10, settled-decisions §"CIK = entity;
-- CUSIP = security"): allow N rows for the same `(provider='sec',
-- identifier_type='cik', identifier_value=X)` triple as long as their
-- `instrument_id` differs. Every reputable feed (CRSP, Bloomberg, Yahoo, IEX,
-- OpenFIGI) encodes the same shape. Research detail at #1094 comment.
--
-- Implementation. Drop the global table-constraint UNIQUE and replace with
-- two partial UNIQUE INDEXes:
--
--   1. `uq_external_identifiers_provider_value_non_cik` — global UNIQUE on
--      `(provider, identifier_type, identifier_value)` for every NON-CIK
--      identifier. CUSIP / symbol / accession_no remain globally unique as
--      they were under the old constraint.
--
--   2. `uq_external_identifiers_cik_per_instrument` — UNIQUE on
--      `(provider, identifier_type, identifier_value, instrument_id)` for
--      `(provider='sec', identifier_type='cik')` rows. One row per (CIK,
--      instrument) pair, but multiple instruments may share a CIK.
--
-- Postgres ON CONFLICT inference against partial indexes requires the
-- predicate be supplied on the upsert. Every existing
-- `ON CONFLICT (provider, identifier_type, identifier_value) DO ...` site is
-- updated in the same PR (#1102 PR-A) to attach the matching predicate. CIK
-- upserts move to the 4-tuple target with the CIK predicate; non-CIK upserts
-- keep the 3-tuple target with the NON-CIK predicate. Empirically verified
-- against Postgres 17 — without the predicate, the insert fails with
-- "no unique or exclusion constraint matching the ON CONFLICT specification".
--
-- The `uq_external_identifiers_primary` partial index on
-- `(instrument_id, provider, identifier_type) WHERE is_primary=TRUE` is
-- unaffected — it operates on a different shape (per-instrument primacy)
-- and was always orthogonal to the global value-uniqueness rule.

ALTER TABLE external_identifiers
    DROP CONSTRAINT uq_external_identifiers_provider_value;

CREATE UNIQUE INDEX uq_external_identifiers_provider_value_non_cik
    ON external_identifiers (provider, identifier_type, identifier_value)
    WHERE NOT (provider = 'sec' AND identifier_type = 'cik');

CREATE UNIQUE INDEX uq_external_identifiers_cik_per_instrument
    ON external_identifiers (provider, identifier_type, identifier_value, instrument_id)
    WHERE provider = 'sec' AND identifier_type = 'cik';
