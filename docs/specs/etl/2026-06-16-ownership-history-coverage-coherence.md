# Ownership history — coverage-coherence envelope, reject the NT port (#1648)

Status: spec · 2026-06-16 · read-path only · display-only surface
Part of epic #788 (ownership DQ audit). Sequenced after #1639 (NT table) + #1647 (rollup machine-trust envelope).

## 1. Problem + falsified premise

`ownership_history.py` powers the operator-facing trend chart
(`GET /instruments/{symbol}/ownership-history`, `aggregate=true` →
`get_ownership_category_totals`). Sole consumer is the FE
`OwnershipHistoryChart` — **zero engine consumers** (grep: thesis /
portfolio / ranking / scoring do not read it). It is a display surface a
human (or a future agent reading the chart) consults for "accumulation vs
distribution".

#1648 as filed proposes: port #1639's rollup NT-supersession filter
(`NT.period_end > HR.period_end`) into the history aggregation.

**Live dev data (AAPL, 2026-06-16) falsifies that fix.**

The rollup **snapshot** double-counts a reorganised filer because
`_current` holds one row per filer (its latest HR) and the snapshot sums
the stale parent's latest quarter alongside its successors' latest quarter
*as if simultaneous* — the time axis is collapsed. The NT filter is the
fix for that lost axis.

The **history series keeps the axis.** Empirically (Vanguard on AAPL):

- Parent `0000102909` files HR through 2025-12-31 (1,426M sh), then files
  an NT for 2026-03-31 (book moves to sub-entities).
- The 10 sub-entities file their own HRs from 2026-03-31. The NT structure
  guarantees parent and subs never both file an HR for the **same**
  quarter — a quarter is either an HR or an NT, not both.
- So parent-Q4 and subs-Q1 land in **different period buckets** → no
  per-period double-count.

Porting the snapshot filter verbatim (`NT.period_end > obs.period_end`)
against the append-only observations would suppress the parent's HR at
**every** quarter earlier than its NT — i.e. delete real history:

| AAPL aggregate period | current (M sh) | verbatim NT port (M sh) |
|---|---|---|
| 2025-06-30 | 4672.2 | **3318.7** (−1353) |
| 2025-12-31 | 4792.8 | **3363.3** (−1429) |
| 2026-03-31 | 4818.1 | 4818.1 |

A "same-period only" port is also pointless: across the whole DB there is
exactly **1** `(filer_cik, period_end)` pair with both an HR observation
and an NT for that same period. A later NT does not retract an earlier
true holding.

## 2. Decision (data-correctness)

**Do NOT apply 13F-NT supersession to the history series — neither
aggregate nor per-filer.** A 13F-HR for period P is a valid-time fact
("filer reported S shares as of P"); a later NT changes who the filer is
*going forward*, it does not retract P. This is the inverse of the rollup
snapshot, where supersession is mandatory. The distinction is the time
axis: snapshot collapses it (needs NT), series preserves it (must not).

The genuine dishonesty in the trend is **coverage incoherence**, not
NT-blindness: aggregate filer coverage swings (AAPL: 209 → 5,577 → 5,587
→ 6,069 → 6,011 filers across five quarters) so the line's slope is
dominated by ingest coverage, not net flow. The first jump (368M → 4,672M
sh) is purely the 209 → 5,577 filer ramp.

## 3. Deliverable

### 3.1 Coverage-coherence envelope (aggregate series only)

New frozen dataclass in `ownership_history.py`, facts-not-thresholds
(mirrors #1647's `SliceCoherence` / `SanityChecks`):

```python
@dataclass(frozen=True)
class AggregateCoverage:
    bucket_count: int               # number of period buckets in the series
    as_of_min: date | None          # earliest period_end
    as_of_max: date | None          # latest period_end
    holder_count_min: int | None    # min filers in any bucket (None ⇔ issuer-level, e.g. treasury)
    holder_count_max: int | None    # max filers in any bucket
    holder_count_latest: int | None # filers in the latest bucket
```

Pure helper `summarise_aggregate_coverage(points) -> AggregateCoverage`
(table-testable). Contract (hardened per Codex ckpt-1):

- `bucket_count` = `COUNT(DISTINCT period_end)` over the points — **not**
  `len(points)`. The readers emit one point per period today, but the
  helper contract must not depend on that (duplicate-period input is
  table-tested).
- `as_of_min` / `as_of_max` = min / max `period_end`.
- `holder_count_min` / `holder_count_max` = min / max over the **non-None**
  per-point counts.
- `holder_count_latest` = the holder_count of the **latest bucket**
  (`period_end == as_of_max`), NOT "latest non-null". If the latest bucket
  is issuer-level (`None`), `holder_count_latest` is `None` even when older
  buckets carry ints — pinned by test so a mixed series can never report a
  stale older count as "latest" (Codex ckpt-1 HIGH).
- Within ONE category's aggregate series `holder_count` is uniform —
  all-int (institutions) or all-`None` (treasury, issuer-level). An
  all-`None` series yields `None` for the three holder fields but still
  reports `bucket_count` + `as_of_*`. Empty series →
  `AggregateCoverage.empty()` (0 / None).

The API includes it on `OwnershipHistoryResponse.coverage` **only when
`aggregate=true`** (None on per-holder responses — coverage spread is
meaningless for a single filer, mirrors the existing `holder_count`
precedent).

### 3.2 Reject-NT, documented + pinned

- Module docstring + an inline comment in `_institutions_aggregate_history`
  (and `_institutions_history`) state the decision + the −1.4B/qtr evidence
  + the time-axis reasoning, so a future agent re-reading "NT-blind" does
  not re-introduce the undercount.
- A fast-tier guard test asserts the aggregate reader's SQL does **not**
  reference `institutional_filer_13f_notices` / notices — fails the moment
  someone ports the filter.

### 3.3 FE (minimal, honest surfacing)

- Mirror `AggregateCoverage` in `frontend/src/api/ownershipHistory.ts`
  field-for-field; add optional `coverage` to `OwnershipHistoryResponse`.
- Thread the institutions aggregate's `coverage` through
  `OwnershipHistoryChart`'s fetch result; render ONE caption line under the
  chart when `holder_count_max != null`:
  *"Coverage {min}–{max} filers across {bucket_count} quarters — quarter-
  over-quarter changes may reflect filing coverage, not net flow."*
  Treasury (holder fields `null`) renders no caption.

## 4. Out of scope (filed as follow-ups)

- **Per-filer "superseded, not sold" annotation** — annotate per-filer
  points after the filer's NT so the cliff reads as supersession.
  Deferred (operator scope decision 2026-06-16).
- **Per-period institutional family identity (#1644)** in each history
  bucket. Deferred.
- **Defective near-zero winner** — Vanguard parent's 2025-Q3 AAPL HR is
  99k sh (vs 1.34B / 1.43B neighbours): a bad/partial winner, NOT NT, NOT
  coverage. The literal "drops to ~0 then reaccumulates" in the per-filer
  view. Separate winner-selection bug → new issue.

## 5. Files

- `app/services/ownership_history.py` — `AggregateCoverage` +
  `summarise_aggregate_coverage` + docstring/comments. No SQL behaviour
  change to the readers.
- `app/api/instruments.py` — `AggregateCoverageResponse` Pydantic +
  `coverage` field on `OwnershipHistoryResponse`; populate when aggregate.
- `frontend/src/api/ownershipHistory.ts` — type mirror.
- `frontend/src/components/instrument/OwnershipHistoryChart.tsx` — caption.
- Tests: `tests/test_ownership_history_coverage.py` (pure-logic table
  test + SQL-guard) + a FE unit test for the caption.

## 6. Verification (DoD)

- Pure-logic + guard tests green; pyright/ruff/format; FE typecheck +
  test:unit.
- Dev-verify the endpoint on the panel (AAPL / GME / MSFT / JPM / HD):
  `aggregate=true&category=institutions` returns `coverage` with the
  expected spread; the parent's full pre-NT history is still present
  (cross-check AAPL 2025-06-30 ≈ 4,672M unchanged, NOT 3,319M).
- Chart renders the coverage caption; trend line unchanged vs pre-PR
  (read-path: no value moves).
