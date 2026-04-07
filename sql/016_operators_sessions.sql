-- 016_operators_sessions.sql
--
-- Adds operator identity and browser session storage for ticket #98
-- (ADR 0001). eBull is a single-operator tool in v1, but the operators table
-- is still the identity anchor for sessions and (in ticket #99) encrypted
-- broker secrets — multi-operator is out of scope but the schema does not
-- preclude it.
--
-- Tables:
--   operators -- one row in v1, identity anchor for the human operator
--   sessions  -- opaque server-side session ids, one row per browser login
--
-- Notes:
--   * password_hash stores the full Argon2id PHC string (algorithm + params
--     + salt + hash). Tuning parameters are encoded inside the hash itself
--     so future parameter changes do not require a schema migration.
--   * session id is stored as TEXT (opaque random base64) rather than UUID
--     so we can rotate the encoding without a column type change.
--   * idle and absolute timeouts are enforced server-side by comparing
--     last_seen_at and expires_at against now() in the require_session
--     dependency. There is no DB trigger / job that reaps expired rows;
--     they are simply ignored on lookup. A periodic cleanup can be added
--     later but is not required for correctness.

CREATE TABLE IF NOT EXISTS operators (
    operator_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username       TEXT NOT NULL UNIQUE,
    password_hash  TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_login_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id     TEXT PRIMARY KEY,
    operator_id    UUID NOT NULL REFERENCES operators(operator_id) ON DELETE CASCADE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at     TIMESTAMPTZ NOT NULL,
    last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    user_agent     TEXT,
    ip             TEXT
);

CREATE INDEX IF NOT EXISTS sessions_operator_id_idx ON sessions(operator_id);
CREATE INDEX IF NOT EXISTS sessions_expires_at_idx  ON sessions(expires_at);
