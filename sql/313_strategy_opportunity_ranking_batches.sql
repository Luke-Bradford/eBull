-- 313_strategy_opportunity_ranking_batches.sql
--
-- #2549: compact immutable evidence for opportunity-bearing ranking cycles.
-- An unchanged candidate set reuses its digest; empty polling cycles write no
-- row and no feature vectors or heartbeat payloads are retained.

CREATE TABLE IF NOT EXISTS strategy_opportunity_ranking_batches (
    ranking_batch_id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    decided_at                    TIMESTAMPTZ NOT NULL,
    ranking_policy_version        TEXT NOT NULL CHECK (ranking_policy_version <> ''),
    strategy_paper_pool_event_id  BIGINT NOT NULL
        REFERENCES strategy_paper_pool_events(strategy_paper_pool_event_id) ON DELETE RESTRICT,
    selection_limit               INTEGER NOT NULL CHECK (selection_limit > 0),
    considered_count              INTEGER NOT NULL CHECK (considered_count > 0),
    selected_count                INTEGER NOT NULL CHECK (
        selected_count > 0 AND selected_count <= considered_count
    ),
    candidate_set_sha256          TEXT NOT NULL CHECK (candidate_set_sha256 ~ '^[0-9a-f]{64}$'),
    created_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (
        ranking_policy_version,strategy_paper_pool_event_id,
        selection_limit,candidate_set_sha256
    )
);

CREATE TABLE IF NOT EXISTS strategy_opportunity_ranking_members (
    ranking_member_id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ranking_batch_id              BIGINT NOT NULL
        REFERENCES strategy_opportunity_ranking_batches(ranking_batch_id) ON DELETE RESTRICT,
    forecast_id                   BIGINT NOT NULL
        REFERENCES strategy_opportunity_forecasts(forecast_id) ON DELETE RESTRICT,
    rank                          INTEGER NOT NULL CHECK (rank > 0),
    conservative_net_expectancy_pct NUMERIC(12,8) NOT NULL CHECK (
        conservative_net_expectancy_pct > 0
    ),
    selected                      BOOLEAN NOT NULL,
    reason_code                   TEXT NOT NULL CHECK (
        reason_code IN ('selected_for_execution','below_execution_batch_limit')
    ),
    UNIQUE (ranking_batch_id,forecast_id),
    UNIQUE (ranking_batch_id,rank),
    CHECK (
        (selected AND reason_code='selected_for_execution')
        OR (NOT selected AND reason_code='below_execution_batch_limit')
    )
);

CREATE INDEX IF NOT EXISTS idx_strategy_opportunity_ranking_batches_recent
    ON strategy_opportunity_ranking_batches (decided_at DESC,ranking_batch_id DESC);
CREATE INDEX IF NOT EXISTS idx_strategy_opportunity_ranking_members_forecast
    ON strategy_opportunity_ranking_members (forecast_id,ranking_batch_id DESC);

ALTER TABLE strategy_entry_preflights
    ADD COLUMN IF NOT EXISTS ranking_member_id BIGINT
        REFERENCES strategy_opportunity_ranking_members(ranking_member_id) ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS idx_strategy_entry_preflights_ranking_member
    ON strategy_entry_preflights (ranking_member_id) WHERE ranking_member_id IS NOT NULL;

COMMENT ON TABLE strategy_opportunity_ranking_batches IS
    'One deduplicated immutable header per materially distinct positive opportunity set; never a polling heartbeat.';
COMMENT ON TABLE strategy_opportunity_ranking_members IS
    'Narrow ranked forecast decisions, including the reason each current positive opportunity was selected or declined.';
