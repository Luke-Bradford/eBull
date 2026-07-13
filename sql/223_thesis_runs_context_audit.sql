-- 223: persist assembled thesis writer context per run (#2017)
--
-- Spec: docs/superpowers/specs/2026-07-13-thesis-context-audit-design.md
--
-- Altered tables:
--   thesis_runs — context_sha256 (content-identity fingerprint of the
--                 assembled writer context) + context_summary (per-block
--                 availability/status/as-of JSONB). Both nullable; written
--                 at run-insert BEFORE the LLM call so failed/guard-rejected
--                 runs are captured too (the #2007 AMSC debugging class).
--                 Historical rows stay NULL — past contexts are
--                 non-reconstructable, so there is no backfill.
--
-- Not a context-SHAPE change (the writer prompt is byte-identical), so
-- _PROMPT_VERSION is NOT bumped; the version is recorded inside the summary.

BEGIN;

ALTER TABLE thesis_runs
    ADD COLUMN IF NOT EXISTS context_sha256  TEXT,
    ADD COLUMN IF NOT EXISTS context_summary JSONB;

COMMIT;
