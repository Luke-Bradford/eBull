-- 481_engine_book_risk_stress_2008.sql
-- #3612 risk lens: the engine book risk snapshot gains a 2008 stress scenario (SPY close 2007-10-09 ->
-- 2009-03-09), under policy engine-book-risk-v2. Rows measured under v1 predate the scenario and keep
-- NULL; every row of any later policy must carry it. Spec:
-- docs/proposals/risk/2026-10-02-3543-engine-book-risk-snapshot.md ("Source rule", Stress).
--
-- ADD COLUMN without a default rewrites no row, so the append-only trigger (sql/460) is not fired.

ALTER TABLE engine_book_risk_snapshots
    ADD COLUMN IF NOT EXISTS stress_2008_pct NUMERIC(14,8) CHECK (stress_2008_pct <> 'NaN');

ALTER TABLE engine_book_risk_snapshots
    DROP CONSTRAINT IF EXISTS engine_book_risk_snapshots_stress_2008_shape;
ALTER TABLE engine_book_risk_snapshots
    ADD CONSTRAINT engine_book_risk_snapshots_stress_2008_shape
    CHECK (policy_version = 'engine-book-risk-v1' OR stress_2008_pct IS NOT NULL);
