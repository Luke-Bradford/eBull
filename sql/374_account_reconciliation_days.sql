-- #2844 acceptance clause 3: "reconciliation job green for 5 consecutive days on demo".
--
-- Before this, the official/local reconciliation verdict was computed on page load by
-- `load_account_equity_evidence` and discarded. Nothing persisted a day, so the countdown
-- had no record to count, no way to fail and nothing to audit.
--
-- One row per (environment, reconciliation_rule_version, snapshot_date).
--
-- The rule version is IN THE KEY on purpose. Bumping it starts a parallel series rather
-- than overwriting the old one, and the counter reads only the current version -- so a
-- bump resets the countdown to zero. That removes the "bump the version, then re-verdict
-- only the days that came out red" move entirely, because a bump invalidates the greens
-- along with the reds.
--
-- Spec: docs/specs/ops/2026-09-13-account-reconciliation-countdown.md

CREATE TABLE IF NOT EXISTS account_reconciliation_days (
    environment                 TEXT        NOT NULL,
    reconciliation_rule_version TEXT        NOT NULL,
    snapshot_date               DATE        NOT NULL,
    countdown_rule_version      TEXT        NOT NULL,
    reconciliation_state        TEXT        NOT NULL,
    comparable                  BOOLEAN     NOT NULL,
    difference                  NUMERIC,
    tolerance                   NUMERIC,
    incomplete_reasons          TEXT[]      NOT NULL DEFAULT '{}',
    revision_count              INTEGER     NOT NULL DEFAULT 0,
    first_observed_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_at                  TIMESTAMPTZ,
    PRIMARY KEY (environment, reconciliation_rule_version, snapshot_date),
    CONSTRAINT account_reconciliation_days_environment
        CHECK (environment IN ('demo', 'real')),
    CONSTRAINT account_reconciliation_days_state
        CHECK (reconciliation_state IN ('refused', 'reconciled', 'diverged')),
    -- The loader's own implication, made structural: `comparable` is the single
    -- load-bearing flag, and a comparable row always carries both money terms.
    CONSTRAINT account_reconciliation_days_decided_shape
        CHECK (
            (comparable
                AND reconciliation_state IN ('reconciled', 'diverged')
                AND decided_at IS NOT NULL
                AND difference IS NOT NULL
                AND tolerance IS NOT NULL)
            OR
            -- A refusal with no reason is not a refusal.
            (NOT comparable
                AND reconciliation_state = 'refused'
                AND decided_at IS NULL
                AND cardinality(incomplete_reasons) > 0)
        ),
    -- ⚠ PG NUMERIC NaN is NOT IEEE: 'NaN' >= 0 is TRUE and NaN = NaN is TRUE, so a
    -- one-sided bound admits NaN and the usual `col <> col` test cannot detect it.
    -- `<> 'NaN'::numeric` is the working test precisely BECAUSE equality holds.
    CONSTRAINT account_reconciliation_days_tolerance_finite
        CHECK (tolerance IS NULL OR (tolerance >= 0 AND tolerance <> 'NaN'::numeric)),
    CONSTRAINT account_reconciliation_days_difference_finite
        CHECK (difference IS NULL OR difference <> 'NaN'::numeric),
    CONSTRAINT account_reconciliation_days_revision_count
        CHECK (revision_count >= 0),
    -- A verdict cannot predate the day it judges.
    CONSTRAINT account_reconciliation_days_decided_not_before_day
        CHECK (decided_at IS NULL OR (decided_at AT TIME ZONE 'UTC')::date >= snapshot_date)
);

COMMENT ON TABLE account_reconciliation_days IS
    'Per-day official/local reconciliation verdicts (#2844 clause 3). Decided rows are '
    'frozen by trg_account_reconciliation_days_freeze; undecided rows stay upgradeable so '
    'the measured 0-3 day local-snapshot lag does not permanently redden a day.';

COMMENT ON COLUMN account_reconciliation_days.revision_count IS
    'Undecided re-records. Non-zero means the day was judged more than once before it '
    'decided -- ordinary under the local-snapshot lag, and the only signal that it was.';

-- The freeze. An ON CONFLICT ... WHERE predicate protects one write path; the invariant
-- has to hold against every path, so it lives in a trigger.
CREATE OR REPLACE FUNCTION freeze_decided_account_reconciliation_day()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.comparable THEN
        RAISE EXCEPTION
            'account_reconciliation_days %/%/% is decided and cannot be modified',
            OLD.environment, OLD.reconciliation_rule_version, OLD.snapshot_date
            USING ERRCODE = '23514';
    END IF;
    -- An undecided row may take new evidence, but never a new identity or a rewritten
    -- history: the key, the first observation and the countdown must not move, and the
    -- revision counter must advance.
    IF NEW.first_observed_at IS DISTINCT FROM OLD.first_observed_at
       OR NEW.revision_count <= OLD.revision_count THEN
        RAISE EXCEPTION
            'account_reconciliation_days %/%/% update must preserve first_observed_at and advance revision_count',
            OLD.environment, OLD.reconciliation_rule_version, OLD.snapshot_date
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_account_reconciliation_days_freeze ON account_reconciliation_days;
CREATE TRIGGER trg_account_reconciliation_days_freeze
BEFORE UPDATE ON account_reconciliation_days
FOR EACH ROW EXECUTE FUNCTION freeze_decided_account_reconciliation_day();

COMMENT ON FUNCTION freeze_decided_account_reconciliation_day() IS
    'A decided reconciliation verdict never moves. Re-verdicting is a '
    'RECONCILIATION_RULE_VERSION bump, which starts a new series and resets the countdown.';

-- The counter walks the ledger descending by date within one environment + version.
CREATE INDEX IF NOT EXISTS idx_account_reconciliation_days_walk
    ON account_reconciliation_days (environment, reconciliation_rule_version, snapshot_date DESC);
