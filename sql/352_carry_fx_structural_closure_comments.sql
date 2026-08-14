-- 352_carry_fx_structural_closure_comments.sql
--
-- #2720: carry + FX closed as structural zero for the declared lane; the
-- second cost model (static-p75-insession-v3+…+carry-fx-structural-zero-long-
-- x1-real-usd). COMMENT-ONLY — no schema or data change. The prior comments
-- defined the flags via cost_model.CARRY_BPS / FX_BPS, which #2720 DELETED
-- (never zeroed), so left standing they would describe constants that do not
-- exist. Spec: docs/proposals/ta/2026-08-14-carry-fx-structural-closure.md.
--
-- ⚠ Applied migrations are never edited; comment drift is corrected by a new
-- idempotent migration, which COMMENT ON is by construction.

BEGIN;

COMMENT ON COLUMN strategy_results.carry_unmodelled IS
    'True when the run''s cost model left carry unmodelled '
    '(cost_model.CARRY_CLOSURE = unmodelled). False from #2720''s v3 model on: '
    'carry is STRUCTURALLY ZERO for the declared lane (long x1 real-settlement '
    'USD — the position is the underlying, no financing leg exists); any other '
    'lane is unpriced, not free. ⚠ Stamped AS AT COMPUTE TIME and never '
    're-derived: rows computed under an earlier model stay unpromotable. The '
    'promotion gate refuses on it (§5.1).';

COMMENT ON COLUMN strategy_results.fx_unmodelled IS
    'True when the run''s cost model left FX unmodelled '
    '(cost_model.FX_CLOSURE = unmodelled). False from #2720''s v3 model on: FX '
    'is STRUCTURALLY ZERO for the all-USD lane — no conversion event occurs. '
    '⚠ This column carried NO comment before #2720 (sql/335 commented only the '
    'store table); added here for symmetry. Stamped AS AT COMPUTE TIME and '
    'never re-derived.';

COMMENT ON COLUMN strategy_results_store.carry_unmodelled IS
    'True when the run''s cost model left carry unmodelled '
    '(cost_model.CARRY_CLOSURE = unmodelled). False from #2720''s v3 model on: '
    'carry is STRUCTURALLY ZERO for the declared lane (long x1 real-settlement '
    'USD). ⚠ NARROWED by #2363 (FX has its own column; promotion requires BOTH '
    'false). ⚠ Stamped AS AT COMPUTE TIME and never re-derived.';

COMMENT ON COLUMN strategy_results_store.fx_unmodelled IS
    'True when the run''s cost model left FX unmodelled '
    '(cost_model.FX_CLOSURE = unmodelled). False from #2720''s v3 model on: FX '
    'is STRUCTURALLY ZERO for the all-USD lane (USD account measured on '
    'account_equity_evidence, USD-quoted universe re-asserted per run in '
    'load_corpus, USD orders) — no conversion event occurs. ⚠ Stamped AS AT '
    'COMPUTE TIME and never re-derived, for the reason carry_unmodelled is: a '
    'gate reading today''s module constant would silently promote a row that '
    'never charged it.';

COMMIT;
