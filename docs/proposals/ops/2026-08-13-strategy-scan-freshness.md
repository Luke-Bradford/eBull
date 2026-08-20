# The strategy-scan freshness check (#2624 scope 3)

Scope 3 of #2624, and **only** scope 3. Scopes 1 and 2 change the overview payload and the
page that renders it; this is a backend check with no render, independently verifiable
against `:8000`, which is why it is separable. #2624 stays open.

## Two premises in the ticket are falsified, and both change the design

### 1. The signal table is the wrong observable

> a freshness check that alerts on "current version has no signal newer than N days …"

Measured on dev, 2026-08-13. `strategy_signals` grouped by `(strategy_id,
strategy_version)` returns rows for s1, s3 and s4 only. **s2-cross-sectional-momentum has
zero rows under any version** — including `+83967fcb1fca`, the current one, which carries a
watermark at `frontier_date = 2026-08-11` written `2026-08-12 18:59:02`. s2 scans
successfully and emits nothing.

A signal is an **output whose emptiness is a legitimate outcome**, and the scan job's own
docstring says so (`scheduler.py::strategy_signal_scan`: *"`row_count` is signals WRITTEN,
and zero is a legitimate success"*). Ageing signals would fire permanently for s2 and could
never clear — the false-positive class the ticket rules out, one table over from where it
looked for it.

**`strategy_scan_watermark` is the observable that means "a scan ran".** It gets a row per
`(strategy_id, strategy_version)` on every successful scan regardless of signal count, which
is exactly why s2 has one and no signals.

### 2. "…while prices are fresh" cannot be built on the prices layer, and is not needed

The obvious reading — conjoin `check_layer_staleness(conn, "prices") == ok` — ships a check
that never evaluates. Measured 2026-08-13 19:10 UTC on a healthy system:

```
prices layer NOW: stale   latest 2026-08-12 00:00:00+00   age 1 day 19:10:14   max_age 4:00:00
```

`_LAYER_QUERIES["prices"]` is `MAX(price_date)::timestamptz`, i.e. **midnight of the last
trading date**, aged against a **4-hour** threshold. It is therefore `stale` from ~04:00 UTC
every day and for the whole of every weekend. A conjunct on it would suppress this check
essentially always, which is a control that cannot fire.

**The conjunct is unnecessary, not merely unbuildable.** Measuring the lag in *trading days
against `price_daily` itself* makes the check self-suppressing by construction: a corpus that
stops advancing cannot grow the lag, so a price outage holds the strategy at its current lag
instead of blaming the scan for it. The ticket's conjunct was compensating for a calendar-day
formulation this spec does not use. The prices layer keeps its own alarm; this one does not
restate it.

## Source rule

No published formulation governs this, so per `.claude/CLAUDE.md` the threshold is fixed **by
construction**, with the construction stated rather than the number picked. Every term is a
property of the scan job:

| term | value | where it comes from |
| --- | --- | --- |
| by-design arrears | 1 trading bar | `SCHEDULED_JOBS[strategy_signal_scan].description`: *"Runs one bar in ARREARS: a signal on the last bar of a series has no t+1"*. A healthy frontier is **always** one bar behind the corpus. |
| tolerance for one missed run | 1 trading bar | the job runs **daily at 06:45 UTC**, so one missed tick costs exactly one bar. Matches `_STALENESS_THRESHOLDS`' own convention — *"2 days allows for a missed night"*. |
| **`_MAX_SCAN_LAG_BARS`** | **2** | the sum. Alert strictly above it. |

`lag = count(distinct price_date) from price_daily where price_date > basis_frontier`.

⚠ **Trading days, not calendar days.** That is what removes the weekend from the threshold
rather than padding it away: over a weekend `price_daily` does not advance either, so a
healthy Monday reads the same `1` it read on Friday. A calendar threshold would have to
absorb a three-day weekend plus holidays and would then be too loose to catch a two-day
outage — the failure it exists to catch.

⚠ **Detection latency, stated so it is not mistaken for the tolerance.** Strict `> 2` means
the baseline `1` plus one missed session reads `2` and stays healthy; the check turns red
only once a *second* missed bar becomes visible in `price_daily`. So "one missed run of
tolerance" is the design intent and "red on the second missed bar's appearance" is the
observable behaviour. Raised by Codex at checkpoint 1.

Verified against the live steady state: all four strategies read lag **1** today
(`price_daily` max `2026-08-12`, current-version frontier `2026-08-11`, one distinct trading
date between them). The healthy value is the by-design arrears, so the threshold sits one
missed run above the observed norm.

## The basis frontier, and why the fallback is sound

Per strategy in `STRATEGY_MANIFEST`, against its **current** identity version:

```
basis = frontier_date for (strategy, current_version)          -- "current"
     or max(frontier_date) over all versions of that strategy  -- "fallback"
```

The fallback is what makes a rotation legible. `strategy_scan_watermark` stores **no
rotation timestamp**, so a state machine keyed on "how long since the version rotated" is not
buildable from this table — Codex raised this at checkpoint 1 and it is correct.

It is also the wrong question. Whether a strategy is dark because a version rotated or
because the job is broken, the operator-visible truth is the same: **no scan under this
strategy has reached within N bars of the corpus.** Ageing the newest watermark on *any*
version measures exactly that and needs no rotation time. The status name then records which
basis was used, so a rotation is distinguishable from an outage without the classification
depending on it.

## States, and that each is reachable

| status | condition | alert? |
| --- | --- | --- |
| `ok` | current-version watermark, lag ≤ 2 | no |
| `stale` | current-version watermark, lag > 2 | **yes** |
| `rotated_awaiting_scan` | no current-version watermark; fallback basis, lag ≤ 2 | no — #2624's "new track record starting" |
| `rotated_scan_overdue` | no current-version watermark; fallback basis, lag > 2 | **yes** — the 2026-08-12 symptom |
| `frontier_regressed` | basis frontier is **ahead** of `price_daily`'s max | **yes** |
| `never_scanned` | no watermark under any version | no — see below |
| `error` | the reader raised | **yes** |

⚠ **`never_scanned` reports but does not alert, and that is a decision this repo already
settled one component over.** `_derive_overall_status`' own docstring refuses to degrade the
headline for a job with `last_status is None`: *"a fresh deploy would otherwise always
report 'degraded' purely because no jobs have fired yet … A fresh deploy will still report
'degraded' via the empty data layers, which is the more meaningful signal anyway."*
`never_scanned` is the strategy-scan analogue of exactly that. The first draft alerted on it
and `test_api_system`'s healthy-system fixtures — which stage no watermarks — went
`degraded`, which is how it surfaced. The 2026-08-12 symptom is unaffected: that is
`rotated_scan_overdue`, which by definition has prior watermarks.

⚠ Reachability checked for each, because a state nothing can produce is the defect this
milestone keeps finding (`sql/342`, #2653):

- `ok` — the live state of all four strategies today.
- `rotated_awaiting_scan` — the state between a registry-touching merge and the next 06:45
  tick. **A rotation does not trigger a scan**: `strategy_signal_scan` is a scheduled daily
  job with no rotation hook, so a new version has no track record for up to ~24h. Without
  this state the check would fire on every registry merge, which is the false positive the
  #2624 triage warned about.
- `rotated_scan_overdue` — #2624's own 2026-08-12 measurement: four versions rotated, 0 rows
  under them, `max signal_bar_date 2026-08-06` against a corpus days ahead.
- `stale` — the daily job missing twice.
- `frontier_regressed` — **not hypothetical**: `run_signal_scan` has an explicit branch for
  it (`strategy_signal_scan.py:492`, *"the corpus regressed … declining to write"*) covering
  a rewash, a restore, or a rule-set bump that emptied the coverage table. Codex flagged that
  the first draft's `ok` would have swallowed this; it would have, and silently, since a
  regressed corpus makes the lag read `0`.
- `never_scanned` — a strategy newly added to `STRATEGY_MANIFEST`. ⚠ It also absorbs a
  renamed `strategy_id` and a purged watermark history; the state means "no track record at
  all" and does not claim to say why.
- `error` — any reader failure.

The statuses are evaluated in the order listed and are mutually exclusive by construction:
`frontier_regressed` is tested before the lag comparison, and the current/fallback split is
a total partition on "does a current-version watermark exist".

## What this deliberately does NOT restate

- **Job failure.** `check_job_health(conn, "strategy_signal_scan")` already surfaces a failed
  or stalled run. The distinct signal here is the one nothing else has: the job **succeeded**
  and left the current version with no track record — the #2218 invisible-no-op class the
  ticket names.
- **Price-corpus health.** The `prices` layer owns it, and see above.
- **Kill switch.** `overall_status` is already `down` while it is active, so a scan alarm
  under it would be redundant noise.
- **Per-strategy enablement.** `STRATEGY_MANIFEST` is the set of live strategies and carries
  no enabled flag, so there is nothing to filter on. If one is added, this check must gain a
  filter with it.

## Shape

- A **pure** function over already-measured inputs — the manifest's current versions, the
  watermark rows, and the corpus trading dates. Table-tested with no DB, per the repo's
  lean-test rule. The reader is one query.
- Surfaced on `/system/status` as a new `strategy_scan_freshness` list, one entry per
  strategy, each carrying its status, basis, `frontier_date` and lag — so the operator sees
  the arithmetic, not just a colour.
- Folded into `overall_status` as a **degraded** contributor, never `down`. An alert that
  reaches no headline is a control on a path the decision does not take (#2437 R4); `down` is
  reserved for "nothing is updating".
- **Contained like the per-layer checks**: a reader failure yields `status = "error"` for the
  strategies rather than a 503 on the whole status endpoint, matching `check_all_layers`'
  existing per-layer containment. Additive response field, so existing clients are unaffected.
