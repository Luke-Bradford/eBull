-- An unresolved core order can be looked up only through the exact account
-- recorded on its immutable eligibility proof. Do not allow any writer to
-- revoke/delete either credential until reconciliation is terminal.

CREATE OR REPLACE FUNCTION prevent_unresolved_core_credential_removal()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NOT (OLD.revoked_at IS NULL AND NEW.revoked_at IS NOT NULL) THEN
        RETURN NEW;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM strategy_core_eligibility_proofs proof
        JOIN strategy_trades trade
          ON trade.core_eligibility_proof_id=proof.core_eligibility_proof_id
        JOIN strategy_trade_orders link ON link.strategy_trade_id=trade.strategy_trade_id
        JOIN strategy_order_reconciliation_state state ON state.order_id=link.order_id
        WHERE OLD.id IN (proof.api_key_credential_id, proof.user_key_credential_id)
          AND state.state NOT IN ('resolved','rejected')
    ) THEN
        RAISE EXCEPTION 'credential is required by an unresolved core order'
            USING ERRCODE = '23503';
    END IF;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

DROP TRIGGER IF EXISTS trg_prevent_unresolved_core_credential_removal ON broker_credentials;
CREATE TRIGGER trg_prevent_unresolved_core_credential_removal
BEFORE UPDATE OF revoked_at OR DELETE ON broker_credentials
FOR EACH ROW EXECUTE FUNCTION prevent_unresolved_core_credential_removal();

COMMENT ON FUNCTION prevent_unresolved_core_credential_removal() IS
    'Keeps exact account credentials available until every referencing core order is reconciled.';
