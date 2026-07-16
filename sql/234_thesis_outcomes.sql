-- 234_thesis_outcomes.sql
--
-- #2002 — calibration-ledger core: realized outcomes per thesis version.
-- Spec: docs/proposals/thesis/2026-07-16-calibration-ledger-schema.md.
--
-- One row per (thesis_id, horizon_days) — append-only, insert-once
-- (ON CONFLICT DO NOTHING; rows are never updated). The ledger scores
-- THESIS VERSIONS, not instruments: theses are append-only dated
-- forecasts, so supersession does not cancel measurement.
--
-- anchor_date is the minting run's price_anchor.as_of (#2017, persisted
-- PRE-LLM); anchor_close is re-read deterministically from price_daily
-- at-or-before that date (never trusted from a JSON copy — the #2014
-- contract). realized_date is the actual trading day used
-- (max price_date <= anchor_date + horizon).
--
-- method_version is provenance, deliberately NOT part of the identity:
-- v1 is a single-method table ('oc_v1'). A definition change ships as
-- its own migration with an explicit recompute-or-new-table decision.
--
-- Positive-close CHECKs guard the realized_return division and the
-- read-side MAPE denominator at write time; a zero/negative close in
-- price_daily is a data defect, never a ledger row.

BEGIN;

CREATE TABLE IF NOT EXISTS thesis_outcomes (
    thesis_id       BIGINT      NOT NULL REFERENCES theses(thesis_id),
    horizon_days    SMALLINT    NOT NULL CHECK (horizon_days IN (30, 90, 365)),
    anchor_date     DATE        NOT NULL,
    anchor_close    NUMERIC(18,6) NOT NULL,
    realized_date   DATE        NOT NULL,
    realized_close  NUMERIC(18,6) NOT NULL,
    realized_return NUMERIC     NOT NULL,
    method_version  TEXT        NOT NULL,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (thesis_id, horizon_days),
    CHECK (anchor_close > 0),
    CHECK (realized_close > 0)
);

CREATE INDEX IF NOT EXISTS idx_thesis_outcomes_horizon
    ON thesis_outcomes (horizon_days);

COMMIT;
