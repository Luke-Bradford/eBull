-- 517_sec_inline_scale_scan_path.sql
--
-- #3947 Amendment 2 B2.4 — "each generation records which path produced it". Spec:
-- docs/specs/fundamentals/2026-10-21-3947-dps-inline-scale.md §Amendment 2.
--
-- `path` belongs to the ACTIVE generation (sql/514): 'archive' when the documents came from `{accession}-xbrl.zip`,
-- 'listing' when a B2.4 fallback took the `index.json` listing. NULL for a v1 generation, which predates the column
-- (v1 read the listing only), and while there is no generation.

ALTER TABLE sec_inline_scale_scan ADD COLUMN IF NOT EXISTS path TEXT CHECK (path IN ('archive', 'listing'));
ALTER TABLE sec_inline_scale_scan DROP CONSTRAINT IF EXISTS sec_inline_scale_scan_path_needs_generation;
ALTER TABLE sec_inline_scale_scan ADD CONSTRAINT sec_inline_scale_scan_path_needs_generation
    CHECK (path IS NULL OR outcome IS NOT NULL);
