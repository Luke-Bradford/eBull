-- 486_tax_order_lookups.sql
--
-- #3616 slice 3c, part 2a — the broker's order lookups, captured early because they may expire (spec fact 2).
-- Spec: docs/specs/metrics/2026-10-13-3616-slice3c-broker-tax-evidence.md (Design 1 `tax_order_lookups`, Design 3).
-- The run, subject and lot tables ship with the derivation in part 2b.
--
-- ⚠ The repository is public: a body carries account identifiers, so no body, account id or customer id is ever
-- committed; they live here and in the gitignored var/.

BEGIN;

-- One row per lookup attempt, committed as it lands. Never updated or deleted.
CREATE TABLE IF NOT EXISTS tax_order_lookups (
    lookup_id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    environment         TEXT NOT NULL CHECK (environment IN ('demo', 'real')),
    order_id            BIGINT NOT NULL CHECK (order_id > 0),
    requested_at        TIMESTAMPTZ NOT NULL,
    policy_id           TEXT NOT NULL CHECK (policy_id <> ''),
    outcome             TEXT NOT NULL CHECK (outcome IN (
                            'found', 'malformed', 'not_found', 'rate_limited', 'auth_failed', 'http_error',
                            'transport_error')),
    -- NULL for a transport error.
    http_status         INTEGER,
    error_detail        TEXT,
    -- The response text as received (never re-serialised), NULL when no body arrived.
    body_text           TEXT,
    body_sha256         TEXT CHECK (body_sha256 ~ '^[0-9a-f]{64}$'),
    -- The body's accountId, when found.
    account_id          BIGINT CHECK (account_id > 0),
    -- The order's largest trade_events.event_id when the attempt was made.
    order_event_max_id  BIGINT NOT NULL,
    CHECK ((http_status IS NULL) = (outcome = 'transport_error')),
    CHECK ((body_text IS NULL) = (body_sha256 IS NULL)),
    CHECK (outcome = 'found' OR account_id IS NULL),
    CHECK (outcome <> 'found' OR body_text IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_tax_order_lookups_order
    ON tax_order_lookups (environment, order_id, lookup_id);

CREATE OR REPLACE FUNCTION tax_order_lookups_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END $$;

DROP TRIGGER IF EXISTS tax_order_lookups_append_only ON tax_order_lookups;
CREATE TRIGGER tax_order_lookups_append_only
    BEFORE UPDATE OR DELETE ON tax_order_lookups
    FOR EACH ROW EXECUTE FUNCTION tax_order_lookups_append_only();

COMMIT;
