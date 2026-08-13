# FY/quarter flow binding by XBRL context duration, not Frames-API `frame` (#1835)

## Problem

`financial_periods` FY rows have flow columns (revenue/net_income/operating_income)
largely NULL (80–88%) or bound to a wrong-duration value. Full-population (dev DB
2026-06-29): of 46,992 FY rows only 46.4% have `months_covered=12`; 2,270 FY rows
(1,613 instruments) are 3-month windows mislabeled `period_type='FY'`. Downstream
`revenue_growth_yoy` computable for only 8.8% of instruments.

## Root cause (verified, not inferred)

`app/services/fundamentals/__init__.py::_derive_periods_from_facts` (L987):

```python
# YTD disambiguation: for duration items, require frame to be set.
if is_duration and fact.frame is None:
    continue
```

This uses the SEC **Frames-API `frame` label** as the discriminator for "real period
flow vs YTD-cumulative duplicate." That is the wrong signal.

- AAPL `(fy=2024, fp=FY)` revenue facts in `financial_facts_raw`:
  - `pe=2022-09-24 mo=12 frame=CY2022` → kept (2-yr comparative)
  - `pe=2023-09-30 mo=12 frame=None` → **dropped**
  - `pe=2024-09-28 mo=12 frame=None val=391,035M` → **dropped** ← the real FY2024 annual
  FY2025 works only because its `pe=2025-09-27` fact happens to carry `frame=CY2025`.
- Old `SalesRevenueNet` facts are mislabeled `fp=FY` with **3-month durations** and a
  quarterly frame (e.g. `CY2010Q4`) → they survive the frame filter and pollute FY rows
  (failure mode 2 — a quarter-magnitude value in a 12-month FY column).

Full-population: of 35,800 annual-duration (330–380 day) FY-context revenue facts,
**15,477 (43%) have `frame=NULL`** → currently dropped though they ARE the genuine
annual fact. **5,445** quarter-duration facts are mislabeled `fp=FY`.

## Source rule

SEC EDGAR XBRL.

**Documented rule (SEC).** The **Frames API**
(`data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{period}.json`,
[SEC EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces))
"aggregates **one fact for each reporting entity** that is **last filed** for a given
period." The `frame` key in companyfacts is therefore the Frames-API selection — populated
on the single, *last-filed* instance per (concept, calendar period: `CY{year}` annual /
`CY{year}Q{n}` quarterly). It is a **cross-sectional comparability tag, not a per-issuer
period-identity key**, and must not be used to select an issuer's own flow facts.

**Full-population-observed consequence (dev DB 2026-06-29, not inferred).** Because the
Frames selection is the *last-filed* instance, the issuer's own original annual fact (the
one filed in the year it closes) frequently lacks a `frame` — the frame lands on a later
filing's comparative re-stamp of the same period. Of 40,004 annual-duration
(335–395 day) FY-context revenue facts, **17,132 (43%) carry `frame=NULL`** and were being
dropped by the old filter though they are the genuine annual fact. **5,835** quarter-
duration (60–120 day) facts are mislabeled `fp=FY` (legacy 8-K facts) and carried a
quarterly frame, so they survived the old filter and polluted FY flow columns.

**Authoritative signal: XBRL context duration** (`endDate − startDate` in days). Settled
codebase convention already filters quarters by duration to `[60, 120]` days
(`app/providers/implementations/sec_fundamentals.py::_ttm_from_quarters`, "60 and 120 days
to avoid picking up YTD or annual values"). The annual analog is the SEC-frames annual
tolerance 365 ± 30 days → `[335, 395]`, which cleanly separates the 12-month annual fact
from the 6-month (≈182d, Q2 YTD), 9-month (≈273d, Q3 YTD) and 3-month (≈91d) durations.

## Fix

Replace the frame-based YTD filter with a **day-duration guard** keyed to the fiscal
period's canonical length, applied to **every duration fact** (flow, EPS, weighted-average
shares, dividends-per-share — all duration-tagged concepts; gating them all is intentional,
each has the same annual-vs-YTD ambiguity). In the grouping loop:

```python
# days inclusive: quarter [60,120] (settled, _ttm_from_quarters);
# annual 365±30 = [335,395] (SEC frames annual tolerance)
_FLOW_DURATION_DAYS = {"FY": (335, 395), "Q1": (60, 120), "Q2": (60, 120),
                       "Q3": (60, 120), "Q4": (60, 120)}
...
if is_duration:
    lo, hi = _FLOW_DURATION_DAYS[_FP_MAP[fp][0]]
    days = (fact.period_end - fact.period_start).days
    if not (lo <= days <= hi):
        continue  # YTD cumulative (6mo/9mo) or wrong-duration (3mo tagged FY)
```

Instant (balance-sheet, `period_start is None`) facts carry no duration and are always
kept (unchanged). The existing `canonical_facts = [f for f in mapped_facts if f.period_end
== period_end]` + `filed_date DESC` priority continue to resolve the #682 comparative
re-stamp (multiple same-duration annual facts at different period_ends → latest period_end
wins).

`months_covered` for FY flow rows is then computed from ~12-month duration facts (→ 12).
Note an FY group whose `period_end` is anchored by a mislabeled-`fp=FY` *instant* fact (no
duration to gate) and which has no 12-month duration fact at that end still yields a row,
but with `period_start=NULL → months_covered=NULL` and NULL flow columns — not a 3-month
value.

### Stale-row cleanup (required — the derivation fix alone is not enough)

The derivation change stops *creating* bad FY rows, but the ~2,270 already-persisted
mislabeled 3-month FY rows (and the NULL-flow orphans) would survive a re-normalize:
`normalize_financial_periods` upserts `financial_periods_raw` additively, and the canonical
merge (`_canonical_merge_instrument`) only deletes rows whose fiscal label *collides* with a
raw winner. A fiscal label whose only raw fact is now dropped by the duration guard has **no
raw winner**, so the stale canonical row is invisible to the collision delete and persists —
and even the full `POST /jobs/sec_rebuild/run` path does not clear periods (it only resets
manifest rows to re-ingest facts). Verified: both `financial_periods_raw` and
`financial_periods` are 100% `source='sec_edgar'`, written exclusively through this module,
so idempotent-replace is safe.

Two changes make re-normalization clean up the persisted bad rows:

1. **Step 3 periods_raw rewash** — `DELETE FROM financial_periods_raw WHERE instrument_id=%s
   AND source='sec_edgar'` before re-inserting the freshly derived rows, so `periods_raw` is
   authoritative for the current derivation and `best_source` cannot re-create a mislabeled
   3-month FY row from a stale staging row. (`periods_raw` is staging, not history — safe to
   replace.)
2. **Phase B2 structural-invalid delete** — in `_canonical_merge_instrument`, `DELETE FROM
   financial_periods fp WHERE fp.instrument_id=%s AND fp.source='sec_edgar' AND
   fp.period_type='FY' AND fp.months_covered IS NOT NULL AND fp.months_covered < 11` —
   removes the ≈2,270 already-persisted FY rows whose duration is impossibly sub-annual.

**Why the predicate is STRUCTURAL, not "absent from raw" (Codex ckpt-2 catch).**
`financial_facts_raw` is itself retention-swept — only the latest few 10-K/10-Q accessions
survive (`app/services/financial_facts_retention.py`, daily). So a *legitimate* older annual
row's facts age out of raw while its canonical row is the durable history. A "delete labels
absent from raw" predicate would therefore truncate real history on every steady-state
normalize. Deleting only `months_covered < 11` FY rows touches exactly the impossible-
duration rows and never a valid `months_covered≈12` annual row, in any retention state.
`best_source` never produces an FY `periods_raw` row with `months_covered < 11` (the duration
guard requires ≥335-day facts), so this delete never fights the re-insert.

**Aged-out wrong-VALUE rows** (a `months_covered≈12` row a pre-fix derivation bound to a
quarter-magnitude value, whose facts have since aged out) are *not* caught by the structural
predicate. They are corrected when a full `POST /jobs/sec_rebuild/run` re-ingests their
filings (temporarily repopulating `facts_raw` with full history) and re-derives — the rewash
+ `best_source` upsert then overwrites them with the correct annual value. This is the
operator backfill step (clause 10), documented in the runbook.

Both deletes are scoped to `source='sec_edgar'` so a future companies_house fundamentals
path (no rows today) is preserved.

### Why this is strictly correct, not just better

- **Annual fact recovered regardless of frame** — the 43% `frame=NULL` annual facts now
  bind (fixes failure mode 1).
- **Quarter-duration facts rejected from FY columns** — 3-month durations tagged `fp=FY`
  no longer populate FY flow columns (fixes failure mode 2).
- **YTD cumulatives still excluded** — 6-month (Q2) and 9-month (Q3) durations fall outside
  the ±1 band for a quarter, exactly as the old frame filter intended.
- **53-week years / 14-week Q4 tolerated** — 371 days → round(371/30.44)=12; 98 days →
  round(98/30.44)=3; both inside the band.
- **No quarterly regression** — quarterly rows previously bound the frame-bearing 3-month
  fact; the same 3-month fact is now selected by duration (now also catching `frame=NULL`
  standalone quarters that the old filter wrongly dropped).

## Tolerance edge cases (full-population quantified)

- Annual facts with a context just **below** the window (300–334 days, e.g. a short
  transition fiscal year after a fiscal-year-end change) are dropped → FY flow NULL for that
  rare year. Full-pop count of such FY-context revenue facts = **28** (vs 40,004 in-window).
  Just-**above** (396–430 days) = **0**; >430 days = 3 (junk). Binding a non-annual value to
  an annual column is worse than NULL, so dropping these 28 is acceptable. (No special-casing
  — KISS.)

## Verification (full-population)

1. **Pre-merge harness** — for the panel (AAPL, MSFT, HD, GME, JPM) pull raw facts and run
   `_derive_periods_from_facts`; assert each FY row binds the correct ~12-month annual
   revenue and `months_covered==12`, cross-check AAPL FY2024 revenue = $391.035B against
   SEC EDGAR.
2. **Backfill** — `POST /jobs/sec_rebuild/run` scoped `{"source":"sec_edgar"}` fundamentals
   on dev DB; re-run the full-population scan and confirm: (a) FY revenue/net_income/
   operating_income NULL rate drops sharply, (b) FY rows with `months_covered=3` reach ~0,
   (c) **no quarterly regression** — Q-row count and a sampled set of Q revenue values are
   retained or increased (the guard now also admits `frame=NULL` standalone quarters),
   (d) `revenue_growth_yoy`-computable instrument fraction rises well above the 8.8% baseline.
3. **Operator-visible** — `GET /instrument/AAPL/peers` "Revenue growth YoY" renders for the
   panel (was `—`).

## Out of scope

- `fiscal_period` mislabeling upstream (8-K facts tagged `fp=FY` with quarterly durations)
  is left as-is; the duration guard makes the FY builder robust to it. Re-tagging raw facts
  is a separate, higher-blast change.
- #1836 (peers UX missingness disclosure) is the front-end follow-up, gated separately.
