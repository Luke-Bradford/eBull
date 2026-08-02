-- 244_report_snapshot_cap_remediation.sql
--
-- #2180 — cap the six pre-#2178 `report_snapshots` rows that still carry an
-- uncapped `score_changes` array, and add the retention this table never had.
--
-- Background: #2178 capped `score_changes` to a 20-row top-N exhibit with the
-- pre-cap count travelling alongside as `score_changes_total`. Rows written
-- BEFORE that fix still hold the full run x instrument product — up to 14,473
-- elements in one row. `app/api/reports.py` serves `ORDER BY period_start DESC`
-- with `limit` up to 100, so every one of them ships on any call with a
-- sufficient limit.
--
-- WHY IN PLACE, NOT REGENERATED: the stored arrays already contain everything
-- the capped form needs. `_select_rank_movers` takes top_n from EACH direction,
-- and `score_changes_total` is just the pre-cap instrument count — which is
-- exactly `jsonb_array_length` of what is already stored. No source data is
-- required, so a regeneration path would be strictly more machinery for the
-- same result.
--
-- ⚠ THE TRAP: DO NOT SLICE THE STORED ARRAY.
-- The stored order is `ABS(rank_delta) DESC`, not `rank_delta DESC` — the
-- pre-#2178 "top N by magnitude" shape. Verified on dev: on 2026-07-13 element
-- 1 is `rank_delta = -2779` while the array's max RISE is only +1757, and the
-- tail elements are +5. So first-10 ++ last-10 would yield the biggest fallers
-- plus the SMALLEST risers — a wrong exhibit that looks entirely plausible.
-- This must RE-DERIVE: top 10 by `rank_delta DESC` among positives, top 10 by
-- `rank_delta ASC` among negatives.
--
-- Output ordering reproduces the builder exactly. `_select_rank_movers`
-- returns `risers ++ fallers` where `rows` is already `rank_delta DESC`, so
-- risers run large→small positive and fallers run least→most negative. A
-- single `ORDER BY rank_delta DESC` over the union of both groups is
-- byte-identical to that concatenation.
--
-- Idempotent: the WHERE clause selects only rows still above the cap, so a
-- second run is a no-op. `idx_report_snapshots_type_period` is UNIQUE on
-- (report_type, period_start) and the writer is already ON CONFLICT DO UPDATE,
-- so this cannot race the report jobs into a duplicate.

BEGIN;

-- ── 1. Re-derive the capped exhibit for every over-cap row ─────────────

WITH over_cap AS (
    SELECT snapshot_id, snapshot_json->'score_changes' AS arr
    FROM report_snapshots
    WHERE jsonb_array_length(snapshot_json->'score_changes') > 20
),
elems AS (
    SELECT o.snapshot_id, e.elem, (e.elem->>'rank_delta')::int AS rank_delta
    FROM over_cap o, LATERAL jsonb_array_elements(o.arr) AS e(elem)
),
ranked AS (
    SELECT snapshot_id, elem, rank_delta,
           row_number() OVER (PARTITION BY snapshot_id ORDER BY rank_delta DESC) AS rn_riser,
           row_number() OVER (PARTITION BY snapshot_id ORDER BY rank_delta ASC)  AS rn_faller
    FROM elems
),
kept AS (
    SELECT snapshot_id, elem, rank_delta
    FROM ranked
    WHERE (rank_delta > 0 AND rn_riser  <= 10)
       OR (rank_delta < 0 AND rn_faller <= 10)
),
capped AS (
    SELECT snapshot_id,
           jsonb_agg(elem ORDER BY rank_delta DESC) AS score_changes
    FROM kept
    GROUP BY snapshot_id
),
totals AS (
    -- Pre-cap instrument count = the length of what is stored TODAY. Read
    -- before the UPDATE rewrites it.
    SELECT snapshot_id, jsonb_array_length(arr) AS score_changes_total
    FROM over_cap
)
UPDATE report_snapshots rs
SET snapshot_json = jsonb_set(
        jsonb_set(rs.snapshot_json, '{score_changes}', c.score_changes, true),
        '{score_changes_total}', to_jsonb(t.score_changes_total), true
    )
FROM capped c
JOIN totals t USING (snapshot_id)
WHERE rs.snapshot_id = c.snapshot_id;

-- ── 2. Backfill the total on legacy rows that were never over the cap ──
--
-- A pre-#2178 snapshot whose period happened to produce 1..20 movers has a
-- non-empty `score_changes` and NO `score_changes_total`: it was never
-- selected above, because it was never over the cap. Its array IS the whole
-- set, so its pre-cap total is exactly that array's length. Without this the
-- assertion below would abort the migration on any install holding such a row
-- (Codex ckpt-2) — and the API contract would keep serving a non-empty
-- exhibit with no total beside it.
UPDATE report_snapshots
SET snapshot_json = jsonb_set(
        snapshot_json,
        '{score_changes_total}',
        to_jsonb(jsonb_array_length(snapshot_json->'score_changes')),
        true
    )
WHERE jsonb_array_length(snapshot_json->'score_changes') > 0
  AND snapshot_json->'score_changes_total' IS NULL;

-- ── 3. Assert the table is uniformly capped ────────────────────────────
--
-- Fail loudly rather than leave a half-capped table: every row must now be
-- at or under the cap, and every non-empty exhibit must carry its pre-cap
-- total.
DO $$
DECLARE offending INTEGER;
BEGIN
    SELECT count(*) INTO offending
    FROM report_snapshots
    WHERE jsonb_array_length(snapshot_json->'score_changes') > 20
       OR (jsonb_array_length(snapshot_json->'score_changes') > 0
           AND snapshot_json->'score_changes_total' IS NULL);
    IF offending > 0 THEN
        RAISE EXCEPTION
            '#2180: % report_snapshots row(s) still over the 20-row cap or '
            'missing score_changes_total after remediation.', offending;
    END IF;
END $$;

COMMIT;
