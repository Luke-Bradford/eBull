-- #2436 — record the subject-identity verdict ON the thesis row.
--
-- #2431's gate (app/services/thesis_subject_identity.py) refuses a memo that
-- never names its own instrument, but only at WRITE time. 1,512 of 2,652 stored
-- rows predate it; 178 of those are the latest for their instrument, which is
-- what portfolio.py reads, and 14 EXIT recommendations had already fired
-- "Valuation target reached" against a base_value written about a different
-- company (measured on the dev corpus, 2026-08-12).
--
-- ⚠ The VERDICT is stored, not re-derived at read time. The rule will evolve,
-- and a row's verdict must record what was decided when it was checked and by
-- WHICH rule — hence the rule-version column beside it, carrying
-- thesis_subject_identity.RULE_SET_VERSION ("rule-set id + code hash, not an
-- int", the price_quarantine form).
--
-- ⚠ Nothing is deleted or repaired. The rows are a truthful record of what the
-- writer produced and are the evidence base for #2431's real fix
-- (docs/settled-decisions.md:147 — "do not overwrite prior thesis rows").
--
-- ⚠ NULL means NOT YET CHECKED, which is not the same as PASSED. Consumers
-- fail closed on it (`subject_identity_ok IS NOT TRUE`). Backfill with:
--     PYTHONPATH=. uv run python scripts/backfill_thesis_subject_identity.py --apply

ALTER TABLE theses
    ADD COLUMN IF NOT EXISTS subject_identity_ok           boolean,
    ADD COLUMN IF NOT EXISTS subject_identity_rule_version text,
    ADD COLUMN IF NOT EXISTS subject_identity_checked_at   timestamptz;

-- The triple moves together: all three NULL (never checked) or all three set.
-- A verdict without its rule version is unattributable, and a rule version
-- without a verdict records nothing. The non-blank guard stops '' from
-- satisfying "set" while meaning nothing.
ALTER TABLE theses
    DROP CONSTRAINT IF EXISTS theses_subject_identity_triple_ck;

ALTER TABLE theses
    ADD CONSTRAINT theses_subject_identity_triple_ck CHECK (
        (
            subject_identity_ok IS NULL
            AND subject_identity_rule_version IS NULL
            AND subject_identity_checked_at IS NULL
        )
        OR (
            subject_identity_ok IS NOT NULL
            AND subject_identity_rule_version IS NOT NULL
            AND btrim(subject_identity_rule_version) <> ''
            AND subject_identity_checked_at IS NOT NULL
        )
    );
