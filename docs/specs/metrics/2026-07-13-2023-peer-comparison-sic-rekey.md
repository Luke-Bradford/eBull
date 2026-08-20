# #2023 — Re-key peer_comparison cohort: eToro-sector → SEC SIC (4→3→2 walk)

Parent #2009 (fair-value band) §11.4. Sibling of #2021/#2022/#2024/#2025.

## Problem

Two cohort keys coexist and drift:
- `fair_value_band.py` (#2009) keys peer cohorts on **SEC SIC**, walked SIC-4→3→2 to the first level with ≥`MIN_PEERS`.
- `peer_comparison.py` (#1751) keys on **eToro `instruments.sector`** — 9 opaque codes `"1".."9"`, exact-match, **no walk, no min-cohort gate**.

Peer grades (radar/heatmap) and band cohorts therefore disagree on which names are "peers".

## Source rule

Not an SEC-reg treatment decision — this is a **cohort-key choice** between two internal keys. The governing rule is our own settled invariant: **#2009 fixed SEC SIC (walked 4→3→2, MIN_PEERS=8) as the peer-cohort key** (`fair_value_band.py:604-707`, `peer_pct_for`). This ticket brings peer_comparison onto the **same SIC key + same 4→3→2 walk** — ending the eToro-vs-SIC drift. It does **not** claim value-identical cohorts: fair_value_band reads a per-multiple, freshness-filtered *materialized* member set and goes comparator-absent (`sic_level=0`) under threshold, whereas peer_comparison reads a live all-complete-TTM set and renders-thin under threshold (see Design step 2). Same key + walk philosophy, different downstream tolerance. SIC columns + indexes already exist: `instrument_sec_profile.sic` (sql/051), generated `sic3`/`sic2` (sql/221:102-107, `left(sic,3)`/`left(sic,2)` STORED + btree indexes).

## Full-population verification (dev DB, complete-TTM eligible pop, N=3,927)

Eligible pop = `financial_periods_ttm.is_complete_ttm = TRUE` (the population peer_comparison actually serves), **not** all 12,603 tradable (a first scan on all-tradable gave SIC 41.8% < eToro 73.6% because foreign/ETF names lack SIC and are not peer-eligible anyway — misleading).

Coverage on the correct pop:
| key | present | pct |
|---|---|---|
| eToro `sector` | 2,902 | 73.9% |
| SEC `sic` | 3,867 | **98.5%** |

Re-key **gains 1,012** names (null sector → SIC present; today 404) and **loses 47** (sector present → null SIC → now 404). `null_sic = 60` (1.5%) → 404, same failure mode as today's null-sector 404.

Walk level distribution with `MIN_COHORT = 8`:
| SIC level | targets | cohort n (min/avg/max) |
|---|---|---|
| 4 (finest) | 3,083 (79.7%) | 8 / 102 / 378 |
| 3 | 293 (7.6%) | 8 / 30 / 231 |
| 2 | 437 (11.3%) | 8 / 81 / 623 |
| fallback (no level ≥8) | 54 (1.4%) | 1 / 6 / 7 |

96.5% of eligible targets get a ≥8-peer cohort at some SIC level; the 1.4% tail (54 names) falls to the widest (SIC-2) cohort, `cohort_sic_level=0`, and renders **thin/greyed** (existing `is_factor_thin` disclosure path), never an error. Strictly better than eToro-sector, where 26% have no sector at all.

**What the ≥8 threshold gates.** It gates the **median base** — every cohort member contributes to `percentile_cont` medians, and medians need only the factor value, **not** `total_assets`. `total_assets` coverage on complete-TTM is 99.3% (3,898/3,927), so the size-refine step (`_rank_peers`, which drops non-positive `total_assets`) removes at most a handful. The **peer SET** is separately best-effort: it may return <8 members after the `total_assets>0` filter, which is fine here — peer_comparison does not gate rendering on peer count (unlike `fair_value_band`, where <8 usable peers → no valid band number and it re-checks post-rank at `fair_value_band.py:691`). No post-rank re-walk is needed; the walk resolves the median-base granularity, the peer list is a nearest-N display.

## Gating — SELF-MERGEABLE (no operator gate)

- **Not scoring / not model_version.** `scoring.py` does NOT import `peer_comparison` (grep: 0 hits). Scoring's `peer_grade` comes from a separate path `instrument_analytics.compute_peer_grades` (`scoring.py:35,1805,1822`), also eToro-sector-keyed but INDEPENDENT and untouched here. Re-keying peer_comparison cannot move any score. (#1815/#1823 "walled out" confirmed.)
- **No backfill.** peer_comparison is computed **on-demand** at `GET /{symbol}/peer-comparison` (`instruments.py:976`) — no stored table. Pure code change; dev-verify the endpoint, no DoD-8-12 backfill/rebuild.
- **No band recompute.** fair_value_band only borrows `_rank_peers` (size-refine, pure), NOT peer_comparison's cohort key. Re-key does not perturb the 567 stored bands.

## Design

### Cohort resolution (new — replaces exact eToro-sector match)

1. Fetch target SIC + label + the three candidate **peer** counts (self-excluded — `MIN_COHORT` means N peers, not N-1 peers + self, matching `fair_value_band._MEMBER_SQL`'s `instrument_id <> target`) in ONE query:
   ```sql
   WITH tgt AS (
     SELECT sic, sic3, sic2, sic_description
     FROM instrument_sec_profile WHERE instrument_id = %(id)s
   )
   SELECT t.sic, t.sic3, t.sic2, t.sic_description,
     (SELECT count(*) FROM financial_periods_ttm f
        JOIN instrument_sec_profile p USING (instrument_id)
        WHERE f.is_complete_ttm AND p.sic  = t.sic  AND f.instrument_id <> %(id)s) AS n4,
     (SELECT count(*) FROM financial_periods_ttm f
        JOIN instrument_sec_profile p USING (instrument_id)
        WHERE f.is_complete_ttm AND p.sic3 = t.sic3 AND f.instrument_id <> %(id)s) AS n3,
     (SELECT count(*) FROM financial_periods_ttm f
        JOIN instrument_sec_profile p USING (instrument_id)
        WHERE f.is_complete_ttm AND p.sic2 = t.sic2 AND f.instrument_id <> %(id)s) AS n2
   FROM tgt t
   ```
   `tgt` empty **or** `sic IS NULL` → return `None` → caller 404 (same as today's null-sector).
2. Resolve level in Python: narrowest of `(4, 3, 2)` whose peer count ≥ `MIN_COHORT (=8)`. If none clears, **fall back to the SIC-2 prefix** (widest available cohort) and mark `cohort_sic_level = 0` — a below-threshold sentinel matching `fair_value_band.peer_pct_for`'s `sic_level=0`. The band goes comparator-absent there because it feeds a *number*; peer_comparison instead **still renders** the widened cohort (greyed via `is_factor_thin`), because it is a disclosure surface, not a scoring input — it must show *something*, flagged thin. `cohort_sic_level ∈ {4,3,2}` = cleared threshold at that granularity; `0` = widened SIC-2 fallback, thin. (Full-pop: 54 of 3,867 targets, 1.4%, land here.)
3. `prefix = {4: sic, 3: sic3, 2: sic2}[level]`; `sic_col = {4: "sp.sic", 3: "sp.sic3", 2: "sp.sic2"}[level]` — column from a **frozen whitelist**, never interpolated from input (same injection-safe pattern as `fair_value_band._MEMBER_SQL`).

### `_FACTORS_CTE` change

Replace the `factors` CTE's sector filter:
```
-    JOIN instruments i USING (instrument_id)
     ...
-    WHERE t.is_complete_ttm = TRUE
-      AND i.sector = %(sector)s
+    JOIN instruments i USING (instrument_id)
+    JOIN instrument_sec_profile sp USING (instrument_id)
     ...
+    WHERE t.is_complete_ttm = TRUE
+      AND {sic_col} = %(sic_prefix)s
```
`_FACTORS_CTE` becomes a small builder `_factors_cte(level: int) -> str` (or `.format(sic_col=...)` on a template) so `sic_col` is substituted from the whitelist; the `%(sic_prefix)s` value is bound. The inner JOIN naturally drops null-SIC members from cohorts (correct — they belong to no SIC cohort). Used for both the rows SELECT and the medians SELECT with the same level/prefix.

`i` join stays (still supplies `symbol`, `company_name`).

### No shared-helper extraction

The prefixes come directly from the target's generated `sic3`/`sic2` columns and the counts from the DB — the "walk" is Python-picking the narrowest level ≥ threshold. No `_sic_prefix` helper needed, and **no import from `fair_value_band`** (that module already imports `_rank_peers` FROM peer_comparison; a reverse import would risk a cycle). Neither extract nor duplicate — the generated columns are the single source of truth for the prefix ladder.

### Contract change (`PeerComparisonResult` + `PeerComparison` model + endpoint)

The `sector` value changes meaning (eToro code → SIC), so **rename** rather than leave a magic-meaning field:

Both the container fields AND the per-factor median fields change meaning (eToro sector → SIC cohort), so **rename both** — a field named `sector_*` holding a SIC value is a magic-meaning trap (#1955 class):

Container (`PeerComparisonResult` + `PeerComparison`):

| old | new | meaning |
|---|---|---|
| `sector: str` | `cohort_sic: str` | the SIC prefix that defined the cohort (4/3/2 digits) |
| — | `cohort_sic_label: str \| None` | `sic_description` of the target's full SIC (human-readable) |
| — | `cohort_sic_level: int` | `4 \| 3 \| 2` cleared threshold at that granularity; `0` = SIC-2 fallback (thin) |
| `sector_member_count: int` | `cohort_member_count: int` | cohort peer count (median base) |

Per-factor (`PeerFactor`, `app/api/instruments.py:328`):

| old | new |
|---|---|
| `sector_median: float \| None` | `cohort_median: float \| None` |
| `sector_n: int` | `cohort_n: int` |

`is_factor_thin` gains a keyword-only `cohort_is_fallback: bool = False` (Codex ckpt-2 P2): when `cohort_sic_level == 0`, the medians rest on a below-`MIN_COHORT` ABSOLUTE base, so **every** factor greys regardless of its relative coverage ratio (a 4-member fallback cohort with 100% coverage would otherwise read not-thin). The API passes `cohort_is_fallback=(result.cohort_sic_level == 0)`.

### API (`app/api/instruments.py`)

- `PeerFactor` model (:328): `sector_median`→`cohort_median`, `sector_n`→`cohort_n`.
- `PeerComparison` model (:344): swap container fields per table above.
- endpoint mapping (:1017): pass renamed fields through.
- 404 detail string unchanged in spirit ("no SIC classification or no complete-TTM fundamentals").

### Frontend (change-coupled FE-QA required)

- `frontend/src/api/types.ts:490,492,511-512`: `sector_median`→`cohort_median`, `sector_n`→`cohort_n`; `sector`/`sector_member_count` → `cohort_sic` + `cohort_sic_label` + `cohort_sic_level` + `cohort_member_count`.
- `frontend/src/lib/peerComparison.ts` (:78,83,86,88,132,139,270): map from the renamed `f.cohort_median`/`f.cohort_n`. Internal aliases (`sectorN`/`medianRaw`) → rename to `cohortN`/`cohortMedian` (small file, keep it honest).
- `frontend/src/pages/PeersPage.tsx:120`: header `Sector {pc.sector} · {count} members` → `SIC {cohort_sic}{label ? ` · ${label}` : ""} · {cohort_member_count} peers` (show `SIC-{level}` or a "broad cohort" note when `cohort_sic_level===0`). UX upgrade: bare eToro `"3"` → e.g. `SIC 2834 · Pharmaceutical Preparations`.
- `peerComparisonCharts.tsx:78,81,194`: user-facing copy "sector median" / "sector n=" → "cohort median" / "peer n=" (still a cohort median; the word "sector" now lies).

## Tests

- Pure-policy table test for the level-resolution function `resolve_sic_level(n4, n3, n2, min_cohort) -> (level, prefix)` (extract as a pure fn, no I/O). Cover the invariant-risk tail explicitly:
  - `n4≥8` → `(4, sic)`; `n4<8, n3≥8` → `(3, sic3)`; `n4<8, n3<8, n2≥8` → `(2, sic2)`.
  - **self-count boundary**: counts are peer counts (self already excluded upstream), so `n4=8` → level 4 = 8 real peers (regression guard against re-introducing self in the count).
  - **no level ≥8 fallback**: `n4=n3=n2=3` → `(0, sic2)` sentinel (widened, thin), NOT a raise/None.
- Keep existing `is_factor_thin` table tests (logic unchanged; arg rename only).
- ONE DB-backed integration test (`-m db`): a known instrument resolves a non-empty cohort at the expected level, returns ≥1 peer, medians computed, `cohort_sic_level ∈ {4,3,2}`. Panel: AAPL (SIC 3571) or a pharma name for a clean SIC-4 cohort. (Peer set may be <8 after `total_assets` filtering — assert ≥1, not ≥8; peer count is best-effort, per Design.)

## Dev-verify (DoD)

`GET /instruments/AAPL/peer-comparison` (via vite `:5173/api/...` or `:8000/...` — note `:8000/api/...` returns empty, the `/api` prefix is vite-proxy only) → confirm `cohort_sic`, `cohort_sic_label`, `cohort_sic_level`, non-empty `peers`, medians render. Then FE-QA the Peers page header + radar for AAPL and one gained name (a null-eToro-sector instrument now resolving via SIC).

## Out of scope

Scoring's `compute_peer_grades` (separate eToro-sector path — its own ticket if ever re-keyed). fair_value_band (already SIC). The other #2009 v2 siblings.
