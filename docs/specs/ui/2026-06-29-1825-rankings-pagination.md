# #1825 — Rankings polish: real pagination + completeness column (P4 of #1815)

**Status:** spec · **Type:** FE + read endpoint extension · **Risk:** read-only; no
schema, no scoring change.

## Problem (verified, file:line)

- `GET /rankings` already supports `offset`/`limit`/`total` server-side, but the FE
  (`RankingsPage.tsx`) fetches a single page of `RANKINGS_PAGE_LIMIT = 200` and does
  **search + min-score filter + column sort CLIENT-SIDE over that page**, with a
  truncation banner (`api/rankings.ts:31`, `RankingsPage.tsx:64-88`). The scored
  population is now ~3,896 (all tiers, #1823 verified), so names beyond rank 200 are
  invisible and search/filter/sort silently operate on the first page only.
- `RankingItem` (`app/api/scores.py`) omits `data_completeness` / `completeness_tier`
  (on `scores` since #1820) — the operator can't see that a high-ranked name is
  thin-coverage.

## Fix — make the rankings list fully server-authoritative

Move every control that the page applies (search, min-score, sort) to the server so a
single page is a correct slice of the WHOLE ranked, filtered, sorted population. Add
the completeness column. Real prev/next pagination.

### Backend — extend `list_rankings` (`app/api/scores.py`)

New query params (all optional, back-compat — existing callers unaffected):
- `q: str | None` — case-insensitive `ILIKE %q%` on `i.symbol` OR `i.company_name`.
- `min_total_score: float | None` — `s.total_score >= %(min)s`.
- `sort: SortField` (Literal allowlist, default `"rank"`) — one of `rank`, `rank_delta`,
  `symbol`, `coverage_tier`, `total_score`, `quality_score`, `value_score`,
  `turnaround_score`, `momentum_score`, `sentiment_score`, `confidence_score`,
  `data_completeness`. **`gics_sector` is NOT sortable** (Codex ckpt-1 #3): the column
  displays the GICS *label* resolved in Python, while the only SQL expression is
  `sector_spdr_case_sql()` returning the *SPDR symbol* (`XLK`) — sorting on it would not
  match the visible order. Sector is a FILTER (`sector_spdr`), not a sort. The Sector
  header is non-sortable (like the company-name column).
- `sort_dir: Literal["asc", "desc"]` (default `"asc"`) — **also a Literal** (Codex
  ckpt-1 #1); mapped to a hardcoded `"ASC"`/`"DESC"` string, never interpolated raw.

**SQL-injection guard:** both `sort` and `sort_dir` are `Literal`s (FastAPI 422s
off-list) AND both resolve through hardcoded maps (`_SORT_COLUMNS` → column SQL,
`_SORT_DIR` → `ASC`/`DESC`) — neither request value reaches the SQL string. **Stable
deterministic order** (Codex ckpt-1 #2): `ORDER BY <col> <dir> NULLS LAST, s.rank ASC
NULLS LAST, s.instrument_id ASC` — the `instrument_id` final key makes every page a
unique, drift-free slice even when the sort value (and rank) tie.

Add `s.data_completeness, s.completeness_tier` to the SELECT, `RankingItem`, and
`_parse_ranking_item`. The `q` / `min_total_score` predicates join the existing
`where_clauses` list (parameterised) so COUNT + items stay consistent.

`MAX_PAGE_LIMIT` stays 200 (a page cap, not a universe cap). Default page `limit`
becomes 50.

### Frontend

- `api/rankings.ts`: `RankingsQuery` gains OPTIONAL `q`, `min_total_score`, `sort`,
  `sort_dir` (optional so the `RightRail` caller's `{coverage_tier, sector_spdr, stance}`
  literal still type-checks — Codex ckpt-1 #5). `fetchRankings` keeps its **positional**
  `(query, limit = 50, offset = 0)` signature — `RightRail`'s `fetchRankings(q, 6)` is
  unaffected. Drop the `RANKINGS_PAGE_LIMIT`-single-page assumption + the
  contract-violation console.warn that policed it.
- `RankingsPage.tsx`: page is now server-authoritative.
  - State: `query` (filters + q + min-score + sort) and `offset`. ANY query change
    resets `offset` to 0 **in the same event handler** (`setQuery(...)` + `setOffset(0)`
    batched into one render → one fetch; never reset offset in a `useEffect` reacting to
    `query`, which would double-fetch the new query at the stale offset — Codex ckpt-1
    #4). The `useAsync` dep is the serialized `(query, offset)` pair.
  - Debounced search feeds `query.q` (server-side) — the page-local search + truncation
    banner + client `filteredItems`/`sortItems` are removed.
  - Pagination footer: `‹ Prev` / `Next ›` + "showing X–Y of N", disabled at ends.
  - Empty/loading/error/401 states preserved (the `computeView` discriminator stays, but
    operates on the server page directly — no client filtering layer).
- `RankingsTable.tsx`: header click calls an `onSortChange(field, dir)` prop (server
  sort) instead of local `useState`; the active sort + direction come from props. Add a
  **Completeness** column rendering `completeness_tier` as a tier chip
  (`full` / `thin_data` / `insufficient_data`) with `data_completeness` % in the title.
  Remove `sortItems` (server orders now).
- The 6 sub-scores already render — spec §7 "surface sub-scores" already satisfied; the
  per-family hybrid peer grade stays on the Verdict tab (#1824), not duplicated into the
  dense ranking row.

### Tests

- Backend (`tests/test_api_scores.py::TestListRankings`): `q` predicate added,
  `min_total_score` predicate added, `sort`/`sort_dir` map to the right ORDER BY column,
  invalid `sort` → 422, completeness fields surfaced + nullable.
- Frontend (`RankingsPage.test.tsx`, new): pagination advances offset + refetches; a
  filter/search/sort change resets offset to 0; completeness chip renders; header click
  drives server sort.

## Verification (change-coupled FE-QA, #1825 done-criterion)

`/rankings` already populated (#1824 ran `compute_rankings`, 3,896 rows). Exercise the
live page: page through (Next/Prev), search a name beyond page 1, sort by a sub-score,
confirm the completeness chip + numbers match `/rankings?...`. Screenshot in PR.
