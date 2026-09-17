# Re-deriving `CORE_MAX_QUOTE_AGE_SECONDS` from the producer that actually writes the quote (#3157)

Status: proposal, revised after Codex ckpt-1 (23 findings). Ticket #3157, carried forward
from #3118.

## 1. The defect

`CORE_MAX_QUOTE_AGE_SECONDS = _freshness_bound(3600) = 5400 s`
(`app/services/strategy_core_preflight.py`) is derived from `quotes_refresh`'s hourly period.
Since #3118 the core cohort's `quotes` row is maintained by `core_candidate_quote_refresh`
every 300 s — and that job's scope (`CORE_QUOTE_REFRESH_SCOPE_SQL`) is the mandate instrument
plus the proved candidates, which is exactly the set `_PREFLIGHT_SQL` can be asked about. So
the bound is 18× looser than its own producer.

**What that actually costs, stated precisely.** It is NOT that a submission is *sized* off a
stale quote: the executor takes `amount` from `broker_verdict`, not from `db_preflight.price`.
It is that the admission decision itself is reached on a row that may be 90 minutes old.

⚠ **Narrower than the first draft of this document claimed** (Codex ckpt-2, P2, correct). The
quote-quality refusals `core_quote_price_invalid`, `core_quote_crossed` and
`core_quote_spread_flagged` are returned by `_decide` BEFORE `_age_ok` runs, so the bound does
not govern the row behind them: a stale *and* crossed quote reports `core_quote_crossed`. Left
that way deliberately — every one of those branches refuses, so the safety property is
identical and only the diagnosis label differs, and the return order is itself a frozen
contract (`CorePreflightRefusal`) that #3157 has no evidence to re-open.

**And it is NOT producer-health detection.** `quotes` is also written by `quotes_refresh`
(hourly) and by `etoro_websocket.upsert_quote`, so a dead `core_candidate_quote_refresh` does
not age the row without limit; the hourly write keeps it under 3600 s. What a tighter bound
buys is the age of the row a verdict is reached on, not a liveness signal for the job.

#3118 deferred this deliberately and named the trigger: *"once this job has a measured run
history, re-derive from its cadence"*. The history exists (29 h) and it **falsifies the naive
re-derivation**, which is why this is a proposal and not a constant swap.

## 2. Source rule

There is no published formulation for a quote-staleness bound. Per `.claude/CLAUDE.md` ("where
a published formulation genuinely does NOT exist … say so explicitly and fix the rule **by
construction**, freezing the constants in a version hash"), the bound is fixed by construction
from the producer's registered cadence and frozen in `CORE_PREFLIGHT_POLICY_VERSION`.

The construction already in the repo (`_freshness_bound`) is:

- a bound below one period refuses a healthy state in the tail of every cycle;
- a bound at or above two periods defers detection by a further period;
- so the usable interval is `[period, 2·period)`, and the position within it is a CHOICE
  frozen in the policy version, not a derivation.

⚠ **Both endpoints are constructions, not physics.** Detection latency varies continuously
with the bound; nothing happens discontinuously at `2·period`. The interval is a statement of
which trade-offs we are willing to make, and the midpoint is where inside it we sit.

⚠ **And the construction assumes every scheduled fire lands.** §3 measures that it does not.

## 3. Verification

All read-only against the dev DB, 2026-09-17.

### 3a. Producer health — `job_runs`, full history (first fire 2026-09-16 12:34:40Z)

```sql
WITH r AS (SELECT started_at, lag(started_at) OVER (ORDER BY started_at) prev
           FROM job_runs WHERE job_name = 'core_candidate_quote_refresh')
SELECT (SELECT count(*) FROM job_runs WHERE job_name='core_candidate_quote_refresh') AS runs,
       count(*) FILTER (WHERE prev IS NOT NULL)                                      AS intervals,
       count(*) FILTER (WHERE extract(epoch FROM started_at-prev) > 450)             AS over_450,
       count(*) FILTER (WHERE extract(epoch FROM started_at-prev) BETWEEN 590 AND 610) AS about_600
FROM r;
-- 353 | 352 | 2 | 2
```

353 runs, 352 adjacent intervals, **every run `status = 'success'`**, inter-arrival p50 300.0 s
and p99 300.1 s (so dispatch jitter is ~0.1 s, not a contributor), and **2 intervals at
600.0 s**. There is no manual/boot-run provenance column on `job_runs` to exclude; every row is
the recurring job.

⚠⚠ **Each 600 s interval is a slot for which NO `job_runs` row of any status was written** —
not `success`, not `skipped`, not a misfire-marked skip. At 2026-09-16 21:20Z and
2026-09-17 12:50Z other jobs recorded rows in the same minute (`strategy_halt_feed_refresh`,
`strategy_intraday_harvest`), so the daemon was alive.

⚠ Stated no further than that. **Missing telemetry is not proof of a dispatched-then-lost
fire** — never scheduled, dispatched and lost, and run-but-untracked all produce the same
absence, and nothing here discriminates them. What is established is the thing the bound needs:
*the row this cohort's verdict reads can be 600 s old while the producer is otherwise healthy.*

⚠ Nor is it invisible to *everything*: `stale_detection.SCHEDULE_MISS_FLOOR_S = 300` gives this
job an overdue threshold of `max(300, 300) = 300 s`, which the adapter anchors such that
`schedule_missed` surfaces ~2 cadences after the last run. A 600 s gap therefore sits at that
detector's boundary, and the detector is evaluated on demand, so a gap that self-heals at the
next fire is typically gone before anything looks. The narrow claim: **no durable record of the
slot exists**.

⚠ **Both observed losses are OUT of the US regular session** (21:20Z, 12:50Z). No in-session
loss has been observed in 29 h. The job carries no session prerequisite, so its scheduling is
session-independent, but lane contention is not — this is a limit on the evidence, and it is
the reason `k` is described below as a policy tolerance rather than a measured maximum.

### 3b. The realised age at an arbitrary minute — the quantity the bound is evaluated on

A producer's inter-arrival census bounds age from above without measuring it: at a perfect
300 s cadence the age at a random instant is uniform on `[0, 300)`, and longer intervals carry
proportionally more of the time axis. The bound is evaluated at whatever minute an operator
runs a rebalance, so that is the distribution it has to survive.

`scripts/census_3157_core_quote_age.py` samples `now() - quotes.quoted_at` across the whole
cohort every 30 s and labels each sample with `venue_session_is_open`. Run 2026-09-17
18:02:44Z → 18:24:45Z (NYSE open), 45 samples × 10 instruments:

```
PYTHONPATH=. uv run python -m scripts.census_3157_core_quote_age --minutes 22 --every-seconds 30
```

```
symbol        n  n_open     min     p50     max  max_open
QQQ.RTH      45      45      14     140     287       287
SPY.RTH      45      45      15     139     288       288
CSPX.L       45       0    9223    9886   10549         -
…the other seven are .L / .DE, venue shut, 9,222-11,878 s

max in-session age over the run: 288s (90 samples)
```

The in-session trace is the expected sawtooth — ages climb 0 → ~300 s and reset at each
producer fire (visible at 18:05:14Z, 18:15:17Z, 18:20:18Z) — and **the realised maximum over
90 in-session samples is 288 s**, against a 300 s cadence. So under a healthy producer the
750 s bound has ~2.6× headroom and even `k = 0`'s 450 s would not have refused in this window.
The case for `k = 1` rests entirely on §3a's lost fire, not on steady-state age.

⚠ What this cannot establish, recorded rather than implied: it does not re-prove the stamps are
provider-supplied rather than `etoro.py`'s `now()` fallback (#3118 established that for this
cohort by sub-second ordering against the fetching job's own `started_at`); it is one session;
and it says nothing out of session, which is correct — `core_market_session_closed` precedes
`core_quote_stale` in `_decide`, so an out-of-session age never reaches the bound. The
out-of-session rows in the run (the `.L`/`.DE` names at ~9,200-10,900 s) are visible in the raw
output for exactly that reason.

⚠ `row_count = 10` on every run is NOT evidence that ten stamps advanced: `refresh_quotes`
counts a quote as updated even where the monotonic upsert rejects the write. §3b measures the
stored stamps directly, which is why it is the load-bearing measurement and the run census is
not.

### 3c. What the sibling producer does and does not show

`strategy_halt_feed_refresh` is also 5-minutely and already carries `_freshness_bound(300) =
450 s`. Restricted to `status = 'success'` (its out-of-session rows are `skipped`, with
`error_msg` "outside the US pre-open/regular-session halt safety window"), its gaps reach:

```
2026-09-15 17:30:06Z -> 17:55:14Z   1508.1 s
2026-09-15 14:30:07Z -> 14:54:51Z   1484.5 s
2026-09-16 15:30:00Z -> 15:54:37Z   1476.8 s
2026-09-16 16:30:00Z -> 16:52:29Z   1349.6 s
```

All four sit inside 13:30-20:00Z on a weekday, i.e. inside an NYSE session.

⚠ **This does NOT validate 750 s** — these gaps exceed 750 too, and that job has different
scheduling, a session prerequisite and different work. What it shows is narrower and is all
that is claimed: **a 1.5·period bound is exceeded in normal operation by a 5-minute producer in
this daemon**, so `k = 0` is not a safe default here merely because it is the incumbent.

## 4. The change

1. `_freshness_bound(period_seconds, *, tolerated_missed_fires=0)`. With `k` **consecutive**
   losses tolerated the same reasoning gives `[(k+1)·period, (k+2)·period)`, and the midpoint
   remains the frozen choice:

   ```
   period_seconds * (2 * k + 3) // 2
   ```

   `k = 0` returns today's value for every existing caller, bit-for-bit, including odd periods
   (pinned by a test over `{1, 2, 3, 5, 7, 60, 300, 301, 3599, 3600}`). A non-positive period or
   a negative `k` raises rather than returning a nonsense bound.

2. `CORE_MAX_QUOTE_AGE_SECONDS = _freshness_bound(300, tolerated_missed_fires=1)` = **750 s**.

   ⚠ **`k = 1` is a POLICY TOLERANCE, not a derivation and not a prediction.** Choosing the
   smallest `k` that survives an observed maximum is fitting to that sample, and it is labelled
   as such here and in the code. What makes it the right *policy* is the asymmetry: refusing on
   a single lost fire is the "refuses on the clock rather than on the market" defect #3118
   exists to remove, while tolerating one costs 300 s of additional worst-case age on a gate
   that is not a liveness detector anyway (§1). 750 s also leaves 150 s of headroom above the
   600 s observed maximum, against measured dispatch jitter of ~0.1 s (§3a).

3. `CORE_MAX_HALT_FEED_AGE_SECONDS` stays **450 s, byte-identical**. §3c says it is exposed to
   the same defect, but raising it WIDENS a halt gate — the opposite direction of harm from
   tightening a quote bound — so it needs its own evidence and its own ticket. Noted in the
   constant's comment, not folded in.

4. `CORE_PREFLIGHT_POLICY_VERSION` → `core-preflight-v3`: the bound is part of what it freezes.

5. The coupling test reads `JOB_CORE_CANDIDATE_QUOTE_REFRESH`'s registered cadence and asserts
   **exact equality** `bound == _freshness_bound(period, tolerated_missed_fires=k)` with an
   explicit per-producer `k`, plus the interval. Equality is load-bearing: a cadence change from
   five to six minutes leaves 750 inside `[720, 1080)`, so interval membership alone would miss
   the drift the test exists to catch. A second test pins the concrete seconds `(750, 450)` and
   the version string, so editing `_freshness_bound` cannot move the served policy while the
   derivation keeps agreeing with itself.

## 5. Blast radius

- **Direction:** the quote bound TIGHTENS 5400 → 750 s. At a given observation that can only
  refuse more, never admit more. ⚠ That is a statement about one verdict, not a claim that
  overall execution risk falls or that availability is unchanged — §3b is what bounds the
  availability cost.
- **Only binds in an open session.** `core_market_session_closed` precedes `core_quote_stale`.
- **#2833's evidence window is untouched.** The observation lane has its own `MAX_QUOTE_AGE`
  (`strategy_core_quote_observation.py`, one hour, from its own sample interval) and does not
  read the preflight constant; `scripts/verify_2833_core_selection.py` reads no policy version.
- **The opening-lag window is unchanged.** #3118 measured the provider's stamp not advancing
  until ~5 min after the 13:30Z open; that age (~63,000 s) is refused by 5400 s and 750 s alike.
  ⚠ The two bounds are NOT equivalent in general — they differ for every age in `(750, 5400]`,
  which includes a delayed recovery after the first fresh opening quote.
- **Consumers touched:** `tests/test_2603_core_mandate_api.py` pinned the literal `"core-
  preflight-v2"` and now reads the constant; `scripts/census_3118_core_quote_freshness.py`
  prints the bound beside the HOURLY lane's uncovered window and now labels which producer each
  number belongs to; the `core_candidate_quote_refresh` docstring promised 5400/v2 and is
  corrected. Frontend types carry the *field* but no version literal
  (`StrategyPortfolioLens.test.tsx` uses `"n"`), so no frontend change.
- **Not fixed here:** `CoreExecutionResult.preflight_policy_version` defaults to the current
  constant even for a held/resumed result that reached no new preflight, so the stamp is not
  durable proof of the policy actually applied. Pre-existing; noted on the PR.
- **Freshness holds at check time only.** Durable writes and a blocking reconciliation lock sit
  between preflight and broker submission, and no quote-age recheck happens there. A 750 s
  preflight bound is not a 750 s submission-time guarantee — it is strictly better than 5400 s
  at the same point in the path.
- **Cohort membership** is unchanged and shared: `CORE_QUOTE_REFRESH_SCOPE_SQL` and
  `QUOTES_REFRESH_SCOPE_SQL` are built from the same two predicate fragments that
  `_PREFLIGHT_SQL` resolves against, pinned by `tests/test_2603_core_preflight_db.py`. A newly
  proved candidate enters the cohort at the next 5-minute fire; before its first quote exists
  `core_quote_missing` fires, which precedes the staleness check.

## 6. What this does NOT do

- It does not diagnose the lost fires. §3a is the evidence for `k = 1`, not a root cause.
- It does not touch `_HALT_COVERED_ASSET_CLASSES` or venue admission. ⚠ It does change the
  premise of the comment at `market_session_support.py` (which still said the only scheduled
  `quotes` producer is the hourly job, and reasoned from that to "a quote-clock gate is not
  reachable at today's cadence"). The comment is corrected to state today's producer and bound;
  the conclusion is left standing, because the three limits recorded on #2312 — broker-supplied
  not exchange-origin, `etoro.py`'s `now()` fallback manufacturing freshness, and
  session-staleness not being halt detection — are unanswered and are not answered here.
