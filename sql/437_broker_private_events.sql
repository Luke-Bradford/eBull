-- 437_broker_private_events.sql
--
-- #3471 slice 2c-iii-c — a raw, append-only record of every eToro WS `private` topic push
-- (supervisor, #2437 comment 5873325695 item 2). The trial's audit trail then carries the
-- broker's own account of fills, order errors and SL/TP closes, not only our REST polls.
--
-- One row per inner message of a frame, as served. `message` is the inner message object;
-- its `content` member is a JSON-encoded STRING on the eToro wire (topics.md), so
-- `message->>'content'` preserves the pushed payload byte-for-byte. `content` is that string
-- parsed, NULL when it is absent or not JSON — the row is written either way.
--
-- `received_at` is the host clock at frame receipt, the only clock we own: the event
-- payload's own timestamps are left inside `message`, unparsed, because no private event
-- has yet been observed on the demo key (45s idle window, 2026-09-28) and a column chosen
-- from the docs alone would be a guess. `frame_id` groups the messages of one frame.
--
-- The only writer is `EtoroWebSocketSubscriber._listen` (app/services/etoro_websocket.py),
-- through `record_private_events`.
--
-- ⚠ UPDATE is refused: the record is the product, a correction is a new row. DELETE stays
-- possible for the test harness's DELETE-based cleanup (same trade-off as sql/424).

CREATE TABLE IF NOT EXISTS broker_private_events (
    event_id      BIGSERIAL   PRIMARY KEY,
    received_at   TIMESTAMPTZ NOT NULL,
    environment   TEXT        NOT NULL CHECK (environment IN ('demo', 'real')),
    frame_id      UUID        NOT NULL,
    message_index INTEGER     NOT NULL CHECK (message_index >= 0),
    topic         TEXT,
    message_type  TEXT,
    message       JSONB       NOT NULL,
    content       JSONB,
    UNIQUE (frame_id, message_index)
);

CREATE INDEX IF NOT EXISTS broker_private_events_received_at_idx
    ON broker_private_events (received_at);

CREATE OR REPLACE FUNCTION broker_private_events_append_only()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only (#3471): a broker push, once recorded, stays as recorded', TG_TABLE_NAME;
END $$;

DROP TRIGGER IF EXISTS broker_private_events_append_only ON broker_private_events;
CREATE TRIGGER broker_private_events_append_only
    BEFORE UPDATE ON broker_private_events
    FOR EACH ROW EXECUTE FUNCTION broker_private_events_append_only();
