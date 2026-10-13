-- 483_engine_book_risk_net_exposure.sql
-- #3612 risk lens: the engine book risk snapshot measures x1 CFD shorts with signed weights instead of
-- refusing them, under policy engine-book-risk-v3. gross_usd stays the sum of every lot's |MV|; net_usd is
-- the signed sum (negative for a net-short book). Rows measured under v1/v2 predate the column and keep
-- NULL; every row of any later policy must carry it. Spec:
-- docs/proposals/risk/2026-10-02-3543-engine-book-risk-snapshot.md ("v3: signed weights").
--
-- ADD COLUMN without a default rewrites no row, so the append-only trigger (sql/460) is not fired.

ALTER TABLE engine_book_risk_snapshots
    ADD COLUMN IF NOT EXISTS net_usd NUMERIC(18,6) CHECK (net_usd <> 'NaN');

ALTER TABLE engine_book_risk_snapshots
    DROP CONSTRAINT IF EXISTS engine_book_risk_snapshots_net_usd_shape;
ALTER TABLE engine_book_risk_snapshots
    ADD CONSTRAINT engine_book_risk_snapshots_net_usd_shape
    CHECK (policy_version IN ('engine-book-risk-v1', 'engine-book-risk-v2') OR net_usd IS NOT NULL);
