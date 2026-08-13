-- 178_bootstrap_stages_target_cohort_fingerprint.sql
--
-- Issue #1273 PR2 — long-pole stage cohort-fingerprint plumbing.
--
-- Spec: docs/proposals/etl/phase-0-pr2-stage-progress-instrumentation.md §2.
--
-- ## Why
--
-- PR1 (#1361, 792291e) shipped `set_stage_target` / `set_stage_processed` /
-- `_current_running_stage_key` helpers. PR2 wires the 8 long-pole stages
-- (S14/15/16/17/18/22/23/25) to those helpers and surfaces a
-- cohort-definition fingerprint to the operator timeline as an additive
-- tooltip so reviewers can audit "we walked the right slice" without
-- re-greping eight source files.
--
-- Format: `key=value;key=value;...` (semicolon-separated). Operator
-- eyeballs the tooltip — no SHA hash, no JSON, no escaping. Per-stage
-- fingerprint composition documented in spec §4.
--
-- ## Lock impact
--
-- PG 14+ ADD COLUMN with no DEFAULT or with a constant DEFAULT is a
-- metadata-only change, no table rewrite. The column is nullable so a
-- pre-PR2 row reads as NULL (frontend renders no tooltip — same as a
-- stage that never set a fingerprint).

BEGIN;

ALTER TABLE bootstrap_stages
    ADD COLUMN IF NOT EXISTS target_cohort_fingerprint TEXT;

COMMIT;
