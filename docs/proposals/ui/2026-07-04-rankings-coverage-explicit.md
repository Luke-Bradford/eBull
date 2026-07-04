# Rankings coverage — make the denominator explicit (#1918)

## Problem
Rankings renders only the scored subset (3,904 rows) of a 12,597-instrument
tradable universe with no statement of what's excluded or why. Operator can't
tell "not ranked" from "ranked low" from "missing data" — reads as a bug
("missing 10k items").

## Full-population verification (dev DB, model `v1.3-balanced`, 2026-07-04)
Tradable = 12,597. `filings_status` partitions it MECE (every tradable
instrument has exactly one coverage row + status; 0 NULL coverage rows):

| filings_status        | count  | meaning |
|-----------------------|--------|---------|
| analysable            | 3,914  | enough SEC filings to fundamentally analyse |
| no_primary_sec_cik    | 7,254  | no SEC CIK — non-US listing / no US filer |
| fpi                   | 1,088  | foreign private issuer (20-F/6-K, no US-GAAP 10-K/10-Q) |
| insufficient          | 185    | US filer, too few filings, backfill exhausted |
| structurally_young    | 156    | insufficient because genuinely recently listed |

Of the 3,914 analysable, **3,904 are ranked** (in the latest `compute_rankings`
run and still analysable). The remaining **10** are analysable but fail
eligibility's third clause — full-pop check: all 10 lack a thesis AND a
fundamentals snapshot AND price history (`scoring.py:1757-1761`), so they have
no scoring inputs. They are NOT merely "awaiting the next run"; the honest bucket
is "analysable — not in the latest ranking run" (reason `analysable_unranked`),
which covers no-input names today and would also cover a scoring failure or a
between-runs insert in future without overclaiming.

`ranked` counts `COUNT(DISTINCT s.instrument_id)` (defensive — `scores` has no
uniqueness constraint on `(instrument_id, model_version, scored_at)`; full-pop
today shows rows == distinct == 3,904, no dupes). Ranked ≤ analysable holds
because ranked is a filtered subset of analysable instruments.

**Verdict: not a bug.** Exclusion is correct — 8,683 instruments have no
US-GAAP SEC fundamentals to score. The fix is to state it honestly, not to rank
them. Matches settled decision (list endpoint gates on
`coverage.filings_status='analysable'`, #268 Chunk J) + eligibility SQL
(`scoring.py:1722` `compute_rankings`).

## Source rule
Rank-eligibility is code-defined at `app/services/scoring.py:1749-1764`:
`is_tradable=TRUE AND coverage.filings_status='analysable' AND (thesis OR
fundamentals_snapshot OR price_daily)`. The list endpoint
(`app/api/scores.py:332-336`) re-gates on `filings_status='analysable'` so stale
score rows for regressed instruments never surface. **This PR also adds
`i.is_tradable = TRUE` to the list endpoint** (full-pop check: 0 ranked rows are
non-tradable today, so a no-op now, but it guarantees `ranked` == the table's
`total` and that a delisted instrument's stale score can never surface). The
coverage endpoint reuses the SAME gate for `ranked`.

`filings_status` meanings are the classifier's own documented rules, not
inferred: `app/services/coverage.py` `probe_status` / `_finalise` /
`_is_structurally_young` (`coverage.py:1844-1893`) — `fpi` = classifier detected
a foreign-private-issuer form family (20-F/6-K), `no_primary_sec_cik` = no SEC
CIK resolved, `insufficient` = US filer with too few filings after backfill
exhausted, `structurally_young` = insufficient but genuinely recently listed.
Labels below avoid the stronger "US-GAAP" claim (the classifier keys on SEC form
families/counts, not accounting basis).

## Scope (this PR — shippable slice)
Items 1 + 2 of #1918. Item 3 (unranked reachable via explorer with a
"not ranked — why" treatment) defers to the unified-explorer ticket #1917 — no
explorer surface exists yet. Item 4 (interaction with #1857 value 0.5): the
breakdown buckets by `filings_status`, orthogonal to the value sub-score gate,
so it neither bundles nor masks #1857.

### Backend — `GET /rankings/coverage`
New read endpoint in `app/api/scores.py`. Response:
```
{
  "model_version": "v1.3-balanced",
  "scored_at": "2026-07-04T..." | null,
  "universe": 12597,          // COUNT(*) instruments WHERE is_tradable
  "ranked": 3904,             // == GET /rankings unfiltered total for this run
  "not_ranked": [             // MECE over (universe - ranked), each a reason code + operator label
    {"reason": "no_primary_sec_cik",  "label": "No SEC filer (non-US listing)", "count": 7254},
    {"reason": "fpi",                 "label": "Foreign private issuer (20-F/6-K filer)", "count": 1088},
    {"reason": "insufficient",        "label": "Insufficient filing history", "count": 185},
    {"reason": "structurally_young",  "label": "Recently listed — too little history", "count": 156},
    {"reason": "analysable_unranked", "label": "Analysable — not in latest ranking run", "count": 10}
    // {"reason": "other", "label": "Unclassified coverage", "count": N} — only if residual > 0
  ]
}
```
Computation — **one CTE query** (atomic snapshot; two separate READ COMMITTED
queries are not one snapshot):
- `universe` = `COUNT(*) FROM instruments WHERE is_tradable`.
- per-status counts = `GROUP BY coverage.filings_status` over tradable
  (LEFT JOIN coverage, so a missing coverage row folds to NULL status → `other`).
- `ranked` = `COUNT(DISTINCT s.instrument_id)` over the latest run of
  `model_version` gated `i.is_tradable AND c.filings_status='analysable'`
  (0 when no run exists → `scored_at=null`; then `analysable_unranked` = full
  analysable count since nothing is ranked yet).

Pure helper `build_coverage(universe, ranked, status_counts)` assembles buckets:
- `analysable_unranked = analysable_count - ranked`
- one bucket per known non-analysable status present
- `other = universe - ranked - analysable_unranked - Σ(known non-analysable)` —
  a catch-all absorbing NULL / `unknown` / missing-coverage / any future status,
  so the invariant `ranked + Σ not_ranked == universe` holds **definitionally**.
- `assert analysable_unranked >= 0 and other >= 0`; on a negative (a genuine data
  anomaly — e.g. duplicate score rows making ranked > analysable), log a warning
  and clamp the offending bucket to 0 while the `other` residual keeps the total
  reconciled (prevention-log "bucket-arithmetic double-counting" — every bucket
  mutually exclusive, residual explicit, never a silent negative).
Zero-count buckets are omitted.

### Frontend
- `frontend/src/api/types.ts`: `RankingsCoverage` + `RankingsCoverageBucket`.
- `frontend/src/api/rankings.ts`: `fetchRankingsCoverage(): Promise<RankingsCoverage>`.
- `RankingsPage.tsx`: header line "Ranked {ranked} of {universe}" with an
  expandable "coverage" disclosure listing each `not_ranked` bucket
  (label + count). Loads independently of the table page query (its own
  `useQuery`/async hook); a coverage fetch failure degrades to hiding the
  denominator line, never blocks the table. Dark-mode + tabular-nums per
  operator-ui conventions.

## Tests
- Pure: a `_coverage_from_counts(universe, ranked, status_counts)` helper builds
  the response from raw counts — table-test MECE + invariant + awaiting math +
  no-run (ranked=0) case. No DB.
- One API test (auto-`db`) hitting `/rankings/coverage` on a seeded run to prove
  wiring + `ranked == /rankings total`.

## Out of scope
Explorer "not ranked — why" per-instrument treatment (#1917). Model/value-gate
change (#1857). Model-version selector.
