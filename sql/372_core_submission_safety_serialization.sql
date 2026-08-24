-- Serialize every durable safety-control change against core broker mutation.
-- Statement-level BEFORE triggers acquire the advisory lock before PostgreSQL
-- takes any target-row lock, preserving the executor's lock order.

CREATE OR REPLACE FUNCTION serialize_core_submission_safety_write()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(2603, 3);
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_core_safety_runtime_config ON runtime_config;
CREATE TRIGGER trg_core_safety_runtime_config
BEFORE INSERT OR UPDATE OR DELETE ON runtime_config
FOR EACH STATEMENT EXECUTE FUNCTION serialize_core_submission_safety_write();

DROP TRIGGER IF EXISTS trg_core_safety_execution_blocks ON strategy_execution_blocks;
CREATE TRIGGER trg_core_safety_execution_blocks
BEFORE INSERT OR UPDATE OR DELETE ON strategy_execution_blocks
FOR EACH STATEMENT EXECUTE FUNCTION serialize_core_submission_safety_write();

DROP TRIGGER IF EXISTS trg_core_safety_kill_switch ON kill_switch;
CREATE TRIGGER trg_core_safety_kill_switch
BEFORE INSERT OR UPDATE OR DELETE ON kill_switch
FOR EACH STATEMENT EXECUTE FUNCTION serialize_core_submission_safety_write();

DROP TRIGGER IF EXISTS trg_core_safety_broker_credentials ON broker_credentials;
CREATE TRIGGER trg_core_safety_broker_credentials
BEFORE INSERT OR UPDATE OR DELETE ON broker_credentials
FOR EACH STATEMENT EXECUTE FUNCTION serialize_core_submission_safety_write();

COMMENT ON FUNCTION serialize_core_submission_safety_write() IS
    'Orders safety-control writes before core broker mutation using advisory key (2603,3).';
