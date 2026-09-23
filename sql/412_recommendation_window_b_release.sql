-- #2942 — record what the attended recommendation window-B release must prove.
--
-- Spec: docs/proposals/execution/2026-09-23-recommendation-window-b-attended-release.md
-- (Deltas 1 and 3). Two stranded shapes of the recommendation claim have no
-- unattended path: W (`status='submitted'`, phase `broker_verb_entered`, sender
-- died) and U (`status='uncertain'`, the provider call raised and the attempt was
-- parked). The attended release needs, per shape:
--
--   * W: WHO entered the broker verb and WHEN, so it can prove the sender dead by
--     ESRCH and time its wait from the marker's commit. Written by
--     `mark_recommendation_submission_entered`, as sql/410 does for core.
--   * U: WHEN the park committed and WHAT the exception said. The park commit is the
--     sends-ended proof (the provider call had already returned by raising), and the
--     message is scanned for a broker reference. The park's `decision_audit` row
--     cannot anchor the wait: its `decision_time` is `execute_order`'s step-1 `now`.
--   * both: the ACCOUNT the attempt was sent to. `execute_approved_orders` loads one
--     credential pair per run, so no current-state rule recovers it. A credential
--     row's ciphertext is written once and rotation revokes + inserts, so an id is an
--     immutable account binding.
--
-- NULL on every column means NOT RECORDED: every row written before this migration,
-- and every row that never reached a broker. The release refuses such a row. No
-- backfill: a past sender, park instant or account cannot be recovered. Census before
-- this migration: `select count(*) from orders where recommendation_id is not null`
-- returned 0.

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS recommendation_submission_entered_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS recommendation_submission_entered_pid INTEGER,
    ADD COLUMN IF NOT EXISTS recommendation_submission_entered_host TEXT,
    ADD COLUMN IF NOT EXISTS recommendation_parked_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS recommendation_park_message TEXT,
    ADD COLUMN IF NOT EXISTS recommendation_api_key_credential_id UUID
        REFERENCES broker_credentials (id),
    ADD COLUMN IF NOT EXISTS recommendation_user_key_credential_id UUID
        REFERENCES broker_credentials (id);

COMMENT ON COLUMN orders.recommendation_submission_entered_at IS
    'clock_timestamp() of the recommendation broker-verb marker commit (#2942). NULL = not recorded.';
COMMENT ON COLUMN orders.recommendation_parked_at IS
    'clock_timestamp() of the uncertain park commit: the provider call had returned by raising, '
    'so its sends had ended (#2942). NULL = not recorded.';
COMMENT ON COLUMN orders.recommendation_park_message IS
    'str() of the exception that parked this attempt uncertain (#2942). NULL = not recorded.';
COMMENT ON COLUMN orders.recommendation_api_key_credential_id IS
    'broker_credentials row whose plaintext built the broker this attempt was sent through (#2942). '
    'NULL = not recorded (non-live or pre-412).';
