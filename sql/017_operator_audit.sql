-- 017_operator_audit.sql
--
-- Operator-lifecycle audit log for ticket #106 (ADR 0002).
--
-- Operator add/delete events change *who* can place trades on this
-- instance and are therefore audit-relevant. This table is the forensic
-- record of every operator-lifecycle event:
--
--   setup        -- the first operator was created via /auth/setup
--   create       -- an authenticated operator created another operator
--   delete       -- an operator deleted a different operator
--   self_delete  -- an operator deleted themselves (only possible when
--                   another operator exists)
--
-- Design notes:
--
--   * No FK back to operators on either actor_operator_id or
--     target_operator_id. The audit row must survive the deletion of
--     either the actor or the target. ON DELETE CASCADE would lose
--     forensic history; ON DELETE SET NULL would silently null out the
--     actor on historical rows when the actor is later deleted, which
--     is the same forensic loss in a quieter form. Capturing usernames
--     at write time is the right pattern -- it's how the broker
--     credential audit (ticket #99) will work too.
--
--   * actor_username and actor_operator_id are nullable but only when
--     event_type = 'setup' (because there is no logged-in actor for the
--     first-run flow). The CHECK constraint enforces the shape so the
--     application cannot accidentally write a row with one but not the
--     other.
--
--   * target_* are NOT NULL for every event type. There is always a
--     target row (even setup creates an operator).

CREATE TABLE IF NOT EXISTS operator_audit (
    id                  BIGSERIAL PRIMARY KEY,
    event_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_type          TEXT NOT NULL CHECK (
                            event_type IN ('setup', 'create', 'delete', 'self_delete')
                        ),
    actor_operator_id   UUID,
    actor_username      TEXT,
    target_operator_id  UUID  NOT NULL,
    target_username     TEXT  NOT NULL,
    request_ip          TEXT,
    user_agent          TEXT,
    CONSTRAINT operator_audit_actor_shape CHECK (
        (event_type =  'setup'
            AND actor_operator_id IS NULL
            AND actor_username    IS NULL)
     OR (event_type <> 'setup'
            AND actor_operator_id IS NOT NULL
            AND actor_username    IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS operator_audit_event_at_idx ON operator_audit(event_at DESC);
CREATE INDEX IF NOT EXISTS operator_audit_target_idx   ON operator_audit(target_operator_id);
