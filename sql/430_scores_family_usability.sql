-- 430_scores_family_usability.sql
--
-- #3389 slice (c) — per-family data usability, recorded when the row is scored.
-- Spec: docs/proposals/ta/2026-09-25-evidence-ranking-and-instrument-report.md, "Principle":
-- "a per-row data-usability state ... so a validated signal with no usable observation for
-- this instrument never looks like a verdict".
-- Writer: app/services/scoring.py (`_insert_score`). Reader: GET /rankings/verdict/{id}.
--
-- Shape: {"quality": "usable" | "missing" | "stale" | "quarantined", ...} over the six
-- families. ``missing`` means the family observed no input and its stored score is the
-- default fill (0.25 for quality, 0.5 elsewhere); ``quarantined`` is the same default fill
-- on a thesis-fed family whose thesis was rejected (#2436). NULL on rows scored before
-- this column existed: usability was not recorded, which the reader shows as unknown,
-- never as usable.

ALTER TABLE scores ADD COLUMN IF NOT EXISTS family_usability JSONB;
