-- 485_tax_broker_ledger_provenance.sql
--
-- #3616 slice 3c, part 1 — the trade ledger's provenance, and retiring the fills route into `tax_lots`.
-- Spec: docs/specs/metrics/2026-10-13-3616-slice3c-broker-tax-evidence.md (Design 1, 2 and 8). The evidence tables
-- of Design 1 (`tax_order_lookups`, `tax_evidence_runs`, `tax_evidence_subjects`, `tax_evidence_lots`) ship with
-- their writer in part 2.
--
-- Guards first (Design 1): no sync may write `broker_positions` while they run, and no `_ingest_fills` insert may
-- land between the emptiness check and the trigger that refuses it.

BEGIN;

-- The portfolio sync's transaction lock (`app.services.portfolio_sync._SYNC_PORTFOLIO_XACT_LOCK_KEY`).
SELECT pg_advisory_xact_lock(7305530654083469395);
LOCK TABLE tax_lots IN SHARE ROW EXCLUSIVE MODE;

DO $$
DECLARE
    n BIGINT;
    bad TEXT;
BEGIN
    SELECT count(*) INTO n FROM tax_lots;
    IF n > 0 THEN
        RAISE EXCEPTION 'sql/485: tax_lots holds % row(s); the fills route cannot be retired over existing lots', n;
    END IF;
    IF to_regclass('public.fx_rates') IS NOT NULL THEN
        EXECUTE 'SELECT count(*) FROM fx_rates' INTO n;
        IF n > 0 THEN
            RAISE EXCEPTION 'sql/485: fx_rates holds % row(s); it is dropped only when empty', n;
        END IF;
    END IF;
    -- Legacy fee evidence the ledger never observed must be known to be zero (spec Source rules). Every row, whether
    -- or not its position is yet in trade_events. Recovery: a reviewed migration importing the values as observations.
    SELECT string_agg(DISTINCT t.position_id::text, ', ' ORDER BY t.position_id::text) INTO bad
    FROM (
        SELECT position_id, total_fees FROM broker_positions
        UNION ALL
        SELECT position_id, total_fees FROM broker_positions_closed
    ) t
    WHERE t.total_fees IS DISTINCT FROM 0;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'sql/485: positions with total_fees NULL or non-zero: %', bad;
    END IF;
END $$;

-- Design 8: no code or migration writes fx_rates; its one reader (`tax_ledger._load_fx_rate`) is deleted with it.
DROP TABLE IF EXISTS fx_rates;

-- Design 8: `_ingest_fills` was the only writer of tax_lots. A jobs process still on the old code, or an on-demand
-- daily_tax_reconciliation started before the reload, cannot write a fill lot after this migration. Slice 3d drops or
-- reshapes tax_lots and this trigger with it.
CREATE OR REPLACE FUNCTION tax_lots_refuse_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'tax_lots is retired as a write target (#3616 slice 3c); tax evidence comes from the broker record';
END $$;

DROP TRIGGER IF EXISTS tax_lots_refuse_insert ON tax_lots;
CREATE TRIGGER tax_lots_refuse_insert BEFORE INSERT ON tax_lots
    FOR EACH ROW EXECUTE FUNCTION tax_lots_refuse_insert();

-- Design 2: one environment and one account per ledger, fixed before anything is written. Changed only by a
-- reviewed migration.
CREATE TABLE IF NOT EXISTS trade_ledger_scope (
    singleton       BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    environment     TEXT NOT NULL CHECK (environment IN ('demo', 'real')),
    account_id      BIGINT NOT NULL CHECK (account_id > 0),
    established_at  TIMESTAMPTZ NOT NULL
);

ALTER TABLE trade_events ADD COLUMN IF NOT EXISTS environment TEXT;

DO $$
DECLARE
    n_events BIGINT;
    n_sync BIGINT;
    n_bad_cid BIGINT;
    n_cids BIGINT;
    n_nondemo BIGINT;
    n_log BIGINT;
    n_log_null BIGINT;
    log_first TIMESTAMPTZ;
    ledger_first TIMESTAMPTZ;
BEGIN
    SELECT count(*), min(recorded_at) INTO n_events, ledger_first FROM trade_events;
    IF n_events = 0 THEN
        RETURN;
    END IF;

    -- The environment backfill (spec Design 2). A classification resting on three stated assumptions, not a proof.
    SELECT count(*) INTO n_nondemo FROM broker_credentials WHERE environment <> 'demo';
    IF n_nondemo > 0 THEN
        RAISE EXCEPTION 'sql/485: % non-demo broker credential(s); the ledger environment cannot be backfilled', n_nondemo;
    END IF;
    SELECT count(*), count(*) FILTER (WHERE credential_id IS NULL), min(accessed_at)
        INTO n_log, n_log_null, log_first
    FROM broker_credential_access_log;
    IF n_log = 0 OR n_log_null > 0 THEN
        RAISE EXCEPTION 'sql/485: credential access log is empty or has % NULL credential(s)', n_log_null;
    END IF;
    IF log_first >= ledger_first THEN
        RAISE EXCEPTION 'sql/485: the credential access log starts at %, not before the ledger (%)', log_first, ledger_first;
    END IF;
    UPDATE trade_events SET environment = 'demo' WHERE environment IS NULL;

    -- The scope backfill: every sync row carries one valid CID.
    SELECT count(*),
           count(*) FILTER (WHERE NOT (raw_payload ? 'CID')
                             OR jsonb_typeof(raw_payload->'CID') <> 'number'
                             OR (raw_payload->>'CID') !~ '^[1-9][0-9]*$'),
           count(DISTINCT raw_payload->>'CID')
        INTO n_sync, n_bad_cid, n_cids
    FROM trade_events WHERE source = 'etoro_sync';
    IF n_sync = 0 THEN
        RAISE EXCEPTION 'sql/485: the ledger has % row(s) but no sync row; state the scope in a reviewed migration', n_events;
    END IF;
    IF n_bad_cid > 0 OR n_cids <> 1 THEN
        RAISE EXCEPTION 'sql/485: sync rows carry % invalid CID(s) and % distinct CID(s); the scope is not one account',
            n_bad_cid, n_cids;
    END IF;
    INSERT INTO trade_ledger_scope (singleton, environment, account_id, established_at)
    SELECT TRUE, 'demo', (raw_payload->>'CID')::bigint, now()
    FROM trade_events WHERE source = 'etoro_sync'
    LIMIT 1
    ON CONFLICT (singleton) DO NOTHING;
END $$;

ALTER TABLE trade_events ALTER COLUMN environment SET NOT NULL;
-- `ingest_trade_events`, the one production writer, always passes the environment, after `record_trade_events` has
-- checked the batch against trade_ledger_scope. The default serves only writers that bypass it: the jobs process
-- still on the pre-485 code between this migration (applied by the pre-push smoke run) and its reload, whose
-- credentials are all demo (the guard above), and test fixtures that seed rows directly.
ALTER TABLE trade_events ALTER COLUMN environment SET DEFAULT 'demo';
ALTER TABLE trade_events DROP CONSTRAINT IF EXISTS trade_events_environment_check;
ALTER TABLE trade_events ADD CONSTRAINT trade_events_environment_check CHECK (environment IN ('demo', 'real'));

-- Design 2: one row per record_trade_events call, in the sync's transaction. The latest row is the identity of the
-- portfolio refresh `broker_positions` holds.
CREATE TABLE IF NOT EXISTS trade_history_fetches (
    fetch_id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    environment               TEXT NOT NULL CHECK (environment IN ('demo', 'real')),
    account_id                BIGINT CHECK (account_id > 0),
    portfolio_fetched_at      TIMESTAMPTZ NOT NULL,
    portfolio_real_positions  INTEGER NOT NULL CHECK (portfolio_real_positions >= 0),
    -- NULL when no history fetch was attempted.
    min_date                  TIMESTAMPTZ,
    started_at                TIMESTAMPTZ,
    finished_at               TIMESTAMPTZ,
    outcome                   TEXT NOT NULL CHECK (outcome IN ('ok', 'failed', 'scope_unverified', 'scope_refused')),
    -- NULL when the fetch failed.
    row_count                 INTEGER CHECK (row_count >= 0),
    recorded_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (outcome <> 'ok' OR row_count IS NOT NULL),
    CHECK (outcome <> 'failed' OR row_count IS NULL)
);

-- Design 2: observations the ledger does not keep as rows. The stored trade_events row is never changed.
CREATE TABLE IF NOT EXISTS trade_event_observations (
    observation_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id             BIGINT REFERENCES trade_events(event_id),
    position_id          BIGINT NOT NULL CHECK (position_id >= 0),
    event_kind           TEXT NOT NULL CHECK (event_kind IN ('open', 'close')),
    executed_at          TIMESTAMPTZ,
    fetch_id             BIGINT NOT NULL REFERENCES trade_history_fetches(fetch_id),
    kind                 TEXT NOT NULL CHECK (kind IN ('supplemental', 'amendment', 'lifecycle', 'unscoped')),
    observed_at          TIMESTAMPTZ NOT NULL,
    source               TEXT NOT NULL CHECK (source IN ('etoro_sync', 'etoro_history')),
    stored_normalised    JSONB,
    observed_normalised  JSONB NOT NULL,
    observed_raw         JSONB NOT NULL,
    CHECK ((kind = 'unscoped') = (event_id IS NULL)),
    CHECK ((kind = 'unscoped') = (stored_normalised IS NULL)),
    CHECK (event_kind = 'open' OR executed_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_trade_event_observations_event
    ON trade_event_observations (event_id, source, event_kind, observation_id);
CREATE INDEX IF NOT EXISTS idx_trade_event_observations_unscoped
    ON trade_event_observations (position_id, source, event_kind, executed_at, observation_id)
    WHERE kind = 'unscoped';

COMMIT;
