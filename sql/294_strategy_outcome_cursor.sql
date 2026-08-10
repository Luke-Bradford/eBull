-- 294_strategy_outcome_cursor.sql
--
-- Bounded round-robin progress for forward strategy outcome resolution (#2474).
-- An immature/delisted fill remains pending by design. Without a cursor, a
-- full oldest batch of such rows would be selected forever and starve every
-- later fill. This table is bounded metadata: one mutable row per strategy and
-- resolver/input-version identity, never one row per poll or instrument.

CREATE TABLE IF NOT EXISTS strategy_outcome_cursor (
    strategy_id           TEXT        NOT NULL,
    strategy_version      TEXT        NOT NULL,
    rule_set_version      TEXT        NOT NULL,
    input_rule_set_version TEXT       NOT NULL,
    last_signal_id        BIGINT      NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        strategy_id,
        strategy_version,
        rule_set_version,
        input_rule_set_version
    ),
    CONSTRAINT strategy_outcome_cursor_versions_non_empty CHECK (
        strategy_id <> '' AND
        strategy_version <> '' AND
        rule_set_version <> '' AND
        input_rule_set_version <> ''
    ),
    CONSTRAINT strategy_outcome_cursor_signal_non_negative CHECK (last_signal_id >= 0)
);

COMMENT ON TABLE strategy_outcome_cursor IS
    'One bounded round-robin cursor per strategy/outcome/input version; prevents immature fills starving later outcomes. #2474.';
