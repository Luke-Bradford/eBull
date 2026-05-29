# #1347 — S17 + S18 bootstrap recency-bound cohort

Status: proposal. Phase 3 PR1 of the bootstrap-sub-1h plan. Window decision:
**13 months** (operator-confirmed 2026-05-29; master plan §Phase 3; supersedes
the 4-year alternative floated in #1342's comment).

## 0. TL;DR

S17 `sec_def14a_bootstrap` + S18 `sec_business_summary_bootstrap` walk the FULL
historical `filing_events` backlog and deadline-cut at 60 min (Run #8). Add an
OPTIONAL `min_filing_date` recency bound to both discovery selectors. The bound
is applied ONLY under a true orchestrated bootstrap run (`resolve_progress_context()
is not None`); the WEEKLY safety-net auto-fire + manual POST + post-outage
catch-up (same invoker, but progress_ctx None) stay UNBOUNDED. Cohort ~50k →
~5-10k → drains inside budget. ~60 min combined saving. #1342's parallelise
premise is already shipped (#1045 PipelinedSecFetcher in both invokers) → close
#1342 as superseded.

**Gate correction (Codex checkpoint-1):** the invokers are NOT first-install
only — they auto-fire weekly (S17 Sun 02:30 UTC, S18 Sun 04:00 UTC) as
unbounded safety-nets and run on manual POST / post-outage catch-up. Keying the
bound on invoker identity would wrongly bound those. `active_bootstrap_run` is
entered only by the orchestrator (`bootstrap_orchestrator.py:1472`), so
`resolve_progress_context()` is the correct first-install discriminator — same
gate P2 #1377 / the S15 design use.

## 1. Surface (optional param + chunker-resolved gate)

Single source of truth — new helper in `app/services/filings.py` (already holds
`SEC_INGEST_KEEP_FORMS` + retention helpers):
```python
BOOTSTRAP_FILINGS_RECENCY_DAYS = 396  # 13 months ≈ latest annual proxy / 10-K + FY-boundary buffer

def bootstrap_filings_recency_floor(now: datetime | None = None) -> date:
    """First-install cohort floor for S17/S18. Mirrors
    thirteen_f_retention_cutoff (institutional_holdings.py:105-126):
    reject naive now, normalise to UTC. Never date.today() (local-TZ
    drift — Codex catch on #1010's cutoff)."""
    if now is None:
        now = datetime.now(tz=UTC)
    if now.tzinfo is None:
        raise ValueError("bootstrap_filings_recency_floor requires tz-aware now")
    return now.astimezone(UTC).date() - timedelta(days=BOOTSTRAP_FILINGS_RECENCY_DAYS)
```

DEF 14A (S17):
- `discover_pending_def14a(conn, *, instrument_id=None, limit=100, min_filing_date: date | None = None)` — `app/services/def14a_ingest.py:194`
- `ingest_def14a(..., min_filing_date: date | None = None)` — `:968`, passes to discover
- `bootstrap_def14a(..., min_filing_date: date | None = None)` — `:1164`. Resolves the gate: when its own `progress_ctx is not None`, default `min_filing_date` to `bootstrap_filings_recency_floor()`; else `None`. Passes to `ingest_def14a` + records the value in the #1273 cohort fingerprint.
- invoker `sec_def14a_bootstrap` — `app/workers/scheduler.py:4326` — UNCHANGED (chunker owns the gate).

Business summary (S18):
- `ingest_business_summaries(..., min_filing_date: date | None = None)` — `app/services/business_summary.py:1610`
- `bootstrap_business_summaries(..., min_filing_date: date | None = None)` — `:1452`. Same chunker-resolved gate on its own `progress_ctx`; passes through + records in fingerprint.
- invoker `sec_business_summary_bootstrap` — `app/workers/scheduler.py:4156` — UNCHANGED.

Why the chunker, not the invoker: the bound must apply only to TRUE orchestrated
bootstrap, not the weekly safety-net / manual catch-up that share the invoker.
The chunkers ALREADY call `resolve_progress_context()` for instrumentation, so
the gate lives there. `min_filing_date` stays an explicit param (default `None`)
so tests + per-instrument triage can override without a bootstrap context.

## 2. SQL predicate (NULL-guarded — one shape, all branches)

Add to each candidate query's WHERE, inert when the param is NULL:
```sql
AND (%(min_filing_date)s::date IS NULL OR fe.filing_date >= %(min_filing_date)s)
```
- DEF 14A: add to the `per_accession` CTE WHERE in BOTH the per-instrument
  (`:266`) and universe-wide (`:319`) branches, and the CIK-MISSING legacy
  branch (`:241`). Placing it in the CTE bounds the rank universe before
  `ROW_NUMBER()` — equivalent result to filtering post-rank (we want
  `rank ≤ cap AND filed ≥ floor`), cheaper. The latest-N-per-CIK cap
  (`DEF14A_LATEST_PER_FILER_CAP`) is unchanged and composes.
- Business summary: add to the `latest_per_instrument` CTE WHERE (`:1665-1667`).
  Already `DISTINCT ON (instrument_id)` newest 10-K, so the bound EXCLUDES
  instruments whose newest 10-K is staler than 13 months (delisted / lapsed
  filers). Active filers always have a ≤15-month-old 10-K (annual cadence +
  ~90d filing lag); 13 months risks excluding a late-cycle filer until its
  next 10-K — self-heals via the unbounded steady-state path. Acceptable v1
  floor (same posture as #1305 depth floor).

## 3. Instrumentation (#1273 — no silent caps)

Both chunkers stamp a `cohort_fingerprint` via `set_stage_target`. Append the
bound so the operator sees it:
- `bootstrap_def14a` fingerprint (`def14a_ingest.py:1216-1225`): add
  `min_filing_date={iso-or-'none'}`.
- `bootstrap_business_summaries` fingerprint (if present): same.
Terminal log lines already report counts; no cap is applied silently.

## 4. Bootstrap-vs-steady-state correctness

The bound is keyed by `resolve_progress_context()` inside the chunker, NOT by
invoker identity (Codex checkpoint-1 fix):
- **Orchestrated first-install bootstrap** (`active_bootstrap_run` set by
  `bootstrap_orchestrator.py:1472`) → `progress_ctx` non-None → 13mo bound.
- **Weekly safety-net auto-fire** (S17 Sun 02:30, S18 Sun 04:00 UTC), **manual
  POST**, **post-outage catch-up** → same invoker, but fired directly (no
  orchestrator) → `progress_ctx` None → UNBOUNDED. Full historical backlog
  drained, behaviour unchanged.
- **Manifest worker + `sec_10k.py`** (S18 steady-state) / **daily DEF 14A cron**
  (S17) / **per-instrument operator triage** → never enter the chunker with a
  bootstrap context → unbounded.

This resolves the S18 active-but-delinquent-filer undercoverage: a filer whose
latest 10-K is >13mo (NT-extended / delinquent) is skipped by the first-install
bootstrap, but the WEEKLY unbounded safety-net repairs the gap within ≤7 days —
not "next annual filing." Same applies to S17 DEF 14A. Tail-history backfill is
preserved exactly where the issue requires it.

## 5. Test plan

Unit (extend `tests/test_def14a_ingest.py`, `tests/test_business_summary*.py`):
1. `discover_pending_def14a(min_filing_date=floor)` excludes a pre-floor
   accession; includes a post-floor one; boundary (`= floor`) included (`>=`).
2. `min_filing_date=None` → unbounded (existing behaviour, regression guard).
3. per-instrument triage branch with `min_filing_date=None` → unbounded.
4. `ingest_business_summaries(min_filing_date=floor)` excludes instrument whose
   latest 10-K is pre-floor; includes within-floor.
5. chunkers (`bootstrap_def14a` / `bootstrap_business_summaries`) apply the
   floor ONLY under a bootstrap context: with `progress_ctx` set (patched), the
   threaded `min_filing_date` equals `bootstrap_filings_recency_floor()`; with
   `progress_ctx` None (weekly/manual path), it stays `None` (unbounded). Plus a
   direct unit test on `bootstrap_filings_recency_floor`: naive `now` raises;
   aware non-UTC `now` normalises to the correct UTC calendar day.

DoD 8-12 (ETL clauses): smoke panel AAPL/GME/MSFT/JPM/HD — confirm each has a
DEF 14A + business summary within the 13mo cohort post-bootstrap; backfill via
the bootstrap invoker on dev DB; operator-visible figure (ownership rollup +
business panel render). Batched into the end-of-ETL clean bootstrap per the
#1337 epic convention (operator-driven).

## 6. #1342 disposition

Close as superseded: parallelise (PipelinedSecFetcher `prefetch_urls=True`) is
already in both invokers (#1045 — verified `scheduler.py:4337`, `:4185`); the
only residual (cohort tightening) IS this PR. No separate code.

## 7. References

- Plan: `docs/proposals/etl/bootstrap-sub-1h-plan.md` §Phase 3
- Selectors: `def14a_ingest.py:194`, `business_summary.py:1610`
- UTC-cutoff prevention: memory `project_1010_13f_cohort_bound` (date.today vs UTC)
- Cohort-bound precedent: #1010 (13F), #1222 (NPORT)
