# Re-deriving `CORE_MAX_QUOTE_AGE_SECONDS` from the producer that actually writes the quote (#3157)

Status: proposal. Ticket #3157, carried forward from #3118.

## 1. The defect

`CORE_MAX_QUOTE_AGE_SECONDS = _freshness_bound(3600) = 5400 s`
(`app/services/strategy_core_preflight.py:114`) is derived from `quotes_refresh`'s hourly
period. Since #3118 the core cohort's `quotes` row is written by
`core_candidate_quote_refresh` every 300 s. So the bound no longer bounds its producer: a
STOPPED `core_candidate_quote_refresh` goes undetected for up to 90 minutes, and
`decide_core_preflight` admits a submission sized off that quote.

#3118 deferred this deliberately and named the trigger: *"once this job has a measured run
history, re-derive from its cadence"*. The history exists (29 h, 351 runs) and it
**falsifies the naive re-derivation**, which is why this is a proposal and not a constant
swap.

## 2. Source rule

There is no published formulation for a quote-staleness bound — this is not a Bollinger
Squeeze with a book behind it. Per `.claude/CLAUDE.md` ("where a published formulation
genuinely does NOT exist … say so explicitly and fix the rule **by construction**, freezing
the constants in a version hash"), the bound is fixed by construction from the producer's
registered cadence and frozen in `CORE_PREFLIGHT_POLICY_VERSION`.

The construction already in the repo (`_freshness_bound`'s docstring) is:

- a bound below one period refuses a healthy state in the tail of every cycle;
- a bound at or above two periods defers detection of a stopped producer by a further
  period;
- so the usable interval is `[period, 2·period)`, and the position within it is a CHOICE
  frozen in the policy version, not a derivation.

That construction assumes **every scheduled fire lands**. §3 measures that it does not.

## 3. Full-population verification

All read-only against the dev DB, 2026-09-17.

### 3a. Producer health — `job_runs`, 2026-09-16 12:34:40Z → 2026-09-17 17:50:00Z

| | |
| --- | ---: |
| runs | 351 |
| `success` / not-`success` | 351 / 0 |
| `row_count` on every run | 10 |
| inter-arrival p50 / p99 | 300.0 s / 300.1 s |
| inter-arrival **max** | **600.0 s**, 2 of 349 intervals |

⚠⚠ Each 600 s gap is a fire that wrote **no `job_runs` row of any status** — not `success`,
not `skipped`, not a misfire. At 2026-09-16 21:20Z and 2026-09-17 12:50Z other jobs fired in
the same minute (`strategy_halt_feed_refresh`, `strategy_intraday_harvest` both recorded
rows), so the daemon was alive and the loss is this job's alone. A single lost fire is part
of normal operation, and it is invisible to every health signal in this repo.

### 3b. What the preflight actually bounds

In an open US session the provider's stamp is the fetch instant. Over 44 in-session
`strategy_core_quote_observations` rows for the two `us_equity` candidates (SPY.RTH,
QQQ.RTH), spanning both sides of #3118, `observed_at - quote_at` lies in **[-2 s, 0 s]**.
So realised preflight age ≈ time since the last successful fire, and §3a's inter-arrival
distribution IS the age distribution.

⚠ That lane cannot measure anything finer, and the reason is the trap #2312 logged: it
samples at `:23`, the same minute `quotes_refresh` fires, so it is phase-locked to a
different producer and reads ~1 s of age at every tick regardless of what the 5-minute job
does. Checking a reading against its collector's period is what makes the inter-arrival
census the right instrument here.

### 3c. The sibling producer already demonstrates the failure

`strategy_halt_feed_refresh` is also 5-minutely and already carries
`_freshness_bound(300) = 450 s`. Its **in-session** successful-run gaps (status `success`,
so its out-of-session `skipped` rows are excluded) reach 1,349–1,508 s:

```
2026-09-15 17:30:06Z -> 17:55:14Z   1508.1 s
2026-09-15 14:30:07Z -> 14:54:51Z   1484.5 s
2026-09-16 15:30:00Z -> 15:54:37Z   1476.8 s
2026-09-16 16:30:00Z -> 16:52:29Z   1349.6 s
```

A 1.5·period bound is exceeded in normal operation on a 5-minute producer in this daemon.

## 4. The change

1. `_freshness_bound(period_seconds, *, tolerated_missed_fires=0)`. With `k` tolerated
   losses the interval generalises to `[(k+1)·period, (k+2)·period)` — below it refuses
   whenever `k` fires are lost, at or above it defers detection by a further period — and
   the midpoint remains the frozen choice:

   ```
   period_seconds * (2 * k + 3) // 2
   ```

   `k = 0` returns today's value for every existing caller, bit-for-bit.

2. `CORE_MAX_QUOTE_AGE_SECONDS = _freshness_bound(300, tolerated_missed_fires=1)` =
   **750 s**. `k = 1` is not a fitted constant: it is the smallest `k` that does not refuse
   on the loss measured in §3a, and §3a is the only evidence about this producer that
   exists.

3. `CORE_MAX_HALT_FEED_AGE_SECONDS` stays **450 s, byte-identical**. §3c says it is exposed
   to the same defect, but raising it WIDENS a halt gate — the opposite direction of harm
   from tightening a quote bound — so it needs its own evidence and its own ticket. Noted,
   not folded in.

4. `CORE_PREFLIGHT_POLICY_VERSION` → `core-preflight-v3`: the bound interval is part of what
   that version freezes.

5. The coupling test reads `JOB_CORE_CANDIDATE_QUOTE_REFRESH`'s registered cadence rather
   than `JOB_QUOTES_REFRESH`'s, and asserts the generalised interval
   `(k+1)·period <= bound < (k+2)·period`.

## 5. Blast radius

- **Direction of change:** the quote bound TIGHTENS 5400 → 750 s. Tightening a fail-closed
  gate can only refuse more, never admit more.
- **Only binds in an open session.** `core_market_session_closed` precedes `core_quote_stale`
  in `_decide`'s precedence order, so an out-of-session stale quote never reaches the bound.
- **#2833's evidence window is untouched.** The observation lane has its own
  `MAX_QUOTE_AGE` (`strategy_core_quote_observation.py:57`, one hour, derived from its own
  sample interval) and does not read the preflight constant. `scripts/verify_2833_core_selection.py`
  reads no policy version.
- **The opening-lag window is unchanged.** #3118 measured the provider's stamp not advancing
  until ~5 minutes after the 13:30Z open; that age (~63,000 s) is refused by 5400 s and by
  750 s alike.
- **Policy-version consumers:** `strategy_core_executor` stamps it onto a result and
  `app/api/strategies.py` surfaces it. No sealed artefact keys on it.

## 6. What this does NOT do

- It does not diagnose the lost fires. §3a is evidence for `k = 1`, not a root cause.
- It does not touch `_HALT_COVERED_ASSET_CLASSES` or venue admission. ⚠ A 5-minute producer
  plus a 750 s bound does change the premise of the stale comment at
  `market_session_support.py:176-188` (which still says the only scheduled producer is the
  hourly `quotes_refresh`, and reasons from that to "not reachable at today's cadence"). The
  comment is corrected to state today's producer; the conclusion — no venue is admitted on a
  quote clock — is left standing, because the three limits recorded on #2312 (broker-supplied
  not exchange-origin; `etoro.py`'s `now()` fallback manufacturing freshness; session-staleness
  is not halt detection) are unanswered and are not answered here.
