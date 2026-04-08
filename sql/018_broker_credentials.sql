-- 018_broker_credentials.sql
--
-- Encrypted broker credential storage for ticket #99 (ADR 0001).
--
-- Depends on 016_operators_sessions.sql for the operators table.
--
-- Tables:
--   broker_credentials           -- encrypted broker secrets, one row per
--                                   (operator, provider, label) active tuple
--   broker_credential_access_log -- forensic record of every decryption
--                                   attempt, including failures
--
-- Design notes:
--
--   * ciphertext layout is nonce (12 bytes) || ciphertext || GCM tag,
--     produced by app.security.secrets_crypto.encrypt(). The column is
--     BYTEA so the raw bytes survive a round-trip without base64 framing.
--
--   * key_version is captured per-row so re-keying can be done by adding
--     a new version and re-encrypting outstanding rows one at a time. It
--     is also part of the AEAD additional-authenticated-data string, so a
--     row written under version N cannot be decrypted under version M.
--
--   * last_four is stored in plaintext for display ("••••1234") so the
--     UI can identify a credential without ever decrypting. Capturing it
--     at write time avoids any "decrypt just to show the suffix" path.
--
--   * The unique constraint is partial on WHERE revoked_at IS NULL so a
--     label can be re-used once the previous credential has been revoked.
--     Revoked rows are kept forever for audit.
--
--   * broker_credential_access_log has credential_id NULLABLE: a lookup
--     that finds no matching row must still be able to write a failure
--     audit entry, and there is no credential_id to attach. Successful
--     and wrong-key-style failures both have a credential_id.

CREATE TABLE IF NOT EXISTS broker_credentials (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    operator_id    UUID NOT NULL REFERENCES operators(operator_id) ON DELETE CASCADE,
    provider       TEXT NOT NULL CHECK (provider IN ('etoro')),
    label          TEXT NOT NULL CHECK (length(label) > 0),
    ciphertext     BYTEA NOT NULL,
    last_four      TEXT NOT NULL CHECK (length(last_four) = 4),
    key_version    SMALLINT NOT NULL DEFAULT 1,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at   TIMESTAMPTZ,
    revoked_at     TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS broker_credentials_unique_active
    ON broker_credentials(operator_id, provider, label)
    WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS broker_credentials_operator_provider_idx
    ON broker_credentials(operator_id, provider);

CREATE TABLE IF NOT EXISTS broker_credential_access_log (
    id              BIGSERIAL PRIMARY KEY,
    credential_id   UUID REFERENCES broker_credentials(id) ON DELETE CASCADE,
    operator_id     UUID NOT NULL,
    accessed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    caller          TEXT NOT NULL,
    success         BOOLEAN NOT NULL,
    failure_reason  TEXT,
    CONSTRAINT broker_credential_access_log_failure_shape CHECK (
        (success = true  AND failure_reason IS NULL)
     OR (success = false AND failure_reason IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS broker_credential_access_log_credential_idx
    ON broker_credential_access_log(credential_id, accessed_at DESC);

CREATE INDEX IF NOT EXISTS broker_credential_access_log_operator_idx
    ON broker_credential_access_log(operator_id, accessed_at DESC);
