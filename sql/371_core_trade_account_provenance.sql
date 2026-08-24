-- 371_core_trade_account_provenance.sql
--
-- #2603 attended core submitter. A core trade must retain the exact eligibility
-- proof whose credential ids were held and compared before broker I/O. The
-- signal arm has different deployment provenance and must not claim this field.

ALTER TABLE strategy_trades
    ADD COLUMN IF NOT EXISTS core_eligibility_proof_id BIGINT
        REFERENCES strategy_core_eligibility_proofs(core_eligibility_proof_id)
        ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS idx_strategy_trades_core_eligibility_proof
    ON strategy_trades (core_eligibility_proof_id)
    WHERE core_eligibility_proof_id IS NOT NULL;

COMMENT ON COLUMN strategy_trades.core_eligibility_proof_id IS
    'Exact account-specific proof admitted by the attended core executor. Nullable '
    'for historical/directly-seeded rows; the production writer always supplies it. The proof retains '
    'the api_key/user_key credential row ids; reconciliation must use those ids '
    'rather than inheriting whichever account is current later.';
