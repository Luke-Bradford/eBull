# #2840 — the head is RE-OBSERVATION, not capture; and the evidence is already being discarded

Status: **analysis, nothing built.** Two Codex checkpoint-1 refusals on this line's spec
(35 then 47 findings — the eleventh and twelfth on this ticket). The design was not
landed. What survives the refusals is recorded here, because two of it are corrections to
the QUEUE HEAD itself and the next session would otherwise re-derive them.

## 0. The queue head was aimed at the wrong mechanism

`eb50b5af` set the head as *"pre-open capture CADENCE first, then breadth"*.

**"Capture" is the wrong object, and no cadence change can fix that.** Capturing new bars
pre-open is impossible and would be irrelevant if it were not:

- `_completed_rth_bars` (`app/services/strategy_intraday_harvest.py:100-102`) bounds each
  bar against its own date's session and rejects `stamp < bounds[0]`, where `bounds[0]` is
  09:30 ET. Pre-open, no RTH bar has completed, so there is nothing new to capture.
- A new capture says nothing about provider rewrite timing in any case. The question is
  what the provider now says about a bar we ALREADY HOLD.

The mechanism the experiment actually needs is **re-observation**: re-fetching a stored
bar and comparing the delivered value against the stored one. Capture and re-observation
are different mechanisms, and the queue has been conflating them since `9246f77c`.

⚠ This is the third inherited prerequisite on this ticket to be falsified by deriving it
rather than accepting it — after the forward corporate-action calendar (`cbfd8144`) and
the "no confirmed split available" blocker (`308d1e38`). The pattern is now the ticket's
most reliable feature.

## 1. The finding: the harvester already fetches the evidence, and discards it unread

`_fetch_count` (`strategy_intraday_harvest.py:110-116`) returns
`min(MAX_PROVIDER_BARS, max(_OVERLAP_BARS, estimated))` with `_OVERLAP_BARS = 3`
(line 35), so a steady-state fire requests bars reaching behind the durable watermark.
`_completed_rth_bars` (line 83-107) keeps them — its only filters are "still forming"
(line 98) and "outside NYSE RTH" (line 100-102), neither of which excludes a stored bar.
Line 301 then drops them:

```python
new = [bar for bar in rth if watermark is None or bar.timestamp.astimezone(UTC) > watermark]
```

The only equality check in the path (line 103-105) compares two bars **within a single
response** and raises on a conflicting duplicate. **Nothing anywhere compares a delivered
bar against the row already in `strategy_intraday_bars`.**

So the provider re-states its answer for bars we hold, on every fire, at zero additional
rate cost, and the comparison is thrown away.

⚠ **Overlap is typical, not guaranteed, and this must not be asserted without measuring.**
`_fetch_count` requests a COUNT; the provider returns the latest N candles, so reach-back
is `N × interval` in wall-clock including extended hours, not in stored-bar space. At the
`MAX_PROVIDER_BARS = 1000` cap a 1m tier reaches back roughly 1000 minutes, so a 1m
watermark older than about a day can yield zero overlap. Any build here needs a measured
per-tier comparison count as its denominator, never an assumed one.

## 2. The experiment's real requirement — the bracket must EXCLUDE the open

This is the half both refusals attacked hardest, and the corrected version is not what
either the spec or the queue assumed.

`bar_capture_certificate.PROVIDER_REWRITE_TIMING_VERIFIED` (line 287) asserts, when true,
that **no re-base precedes the effective session's open** — which is what makes the
certificate's open test sufficient rather than merely necessary. For a corporate action
effective at instant `T` with the following open `O`:

| observation pattern | conclusion |
| --- | --- |
| agreement at `t1`, divergence at `t2`, **both** in `(T, O)` | **refutes** the flag — an early re-base happened |
| agreement at the last observation in `(T, O)`, divergence only after `O` | **consistent with** the flag |
| agreement before `T`, divergence at 09:35 | **inconclusive** — the rewrite could have been 03:00 or 09:31 |

⚠⚠ **A bracket that STRADDLES the open is inconclusive, however narrow.** Agreement at
09:25 and divergence at 09:35 permits a rewrite at 09:26. The bracket must lie *entirely
inside* `(T, O)`, which needs **two or more pre-open re-observations**, not one.

⇒ Post-open observation is **not discriminating** for this question, and the existing
09:35 window start cannot produce a decisive bracket at all. A pre-open re-observation
cadence is therefore genuinely required — but for this reason, and not for the reason the
spec gave (it had argued pre-open observation "creates" a straddling bracket; yesterday's
agreement and today's 09:35 divergence already straddle the open, so that justification
was wrong while the conclusion happened to survive).

## 3. The comparison's baseline must be contemporaneous, or it proves nothing

Change-detection answers "the provider's answer moved since we first saw it". The
certificate asks about the **absolute** level. Those coincide only when the first
observation was itself contemporaneous — captured while the bar was current and before
the action existed. For a backfilled baseline the provider may have delivered an already
adjusted value the first time, in which case no transition is ever recorded and "no
rewrite observed" is silently the wrong conclusion.

Measured: `strategy_intraday_bars.captured_at` is non-null on all 29,234 rows, but
**4,842 of 6,536** 30m bars (74.1%) arrived more than a day after their own bar, per the
`3c6bb73f` census. So the majority of the existing store is ineligible as a baseline, and
any build must restrict the experiment to bars in the `before_next_open` bucket.

## 4. Why nothing can be admitted meanwhile

`bar_capture_certificate.py:416-420` downgrades **every** certifying verdict while the
flag is false, so the store admits nothing and would admit nothing after a year of
harvesting — the flag is evaluated per verdict and does not depend on bar count.
Reproduce the store state with:

```sql
SELECT count(*) AS bars,
       count(*) FILTER (WHERE captured_at >= (SELECT effective_from
                                              FROM intraday_capture_semantics
                                              WHERE rule_id = 'clock_timestamp')) AS post_cutover
FROM strategy_intraday_bars;
```

Measured this session: **29,234 / 0**, with
`intraday_capture_semantics.effective_from = 2026-09-20 15:54:30.302942+00`.

## 5. What a build must carry — the refusals' surviving requirements

Recorded so the next attempt starts from the corrected position rather than from the
spec's first draft. Not a design; a list of constraints any design must satisfy.

1. **Per-response time bounds, never the job clock.** `app/workers/scheduler.py:6432-6433`
   computes `observed_at = datetime.now(tz=UTC)` once, *before* `run_intraday_harvest`
   fetches anything, so it precedes every provider call. Stamping a re-observation with it
   reproduces exactly the defect `sql/402` moved `captured_at` to `clock_timestamp()` to
   fix. Each comparison needs `requested_at` and `received_at` around its own call, and a
   request spanning 09:30 must be excluded from any pre-open claim rather than resolved
   one way silently.
2. **Transitions, not values collapsed by digest.** Keying on the delivered value loses
   history: for baseline A, observations B→C→B lose the C→B transition, and a reversion
   A→B→A never records the return to A. A transition row carrying both the previous
   agreeing observation's bounds and the first divergent observation's bounds *is* the
   bracket.
3. **A denominator with bar identity.** Zero transitions is indistinguishable from zero
   comparisons. Lifetime per-member counters cannot say which bar was checked inside a
   given `(T, O)` window; `job_runs` cannot either — its note is aggregate and carries no
   bar identity.
4. **No upsert on `strategy_intraday_bars`.** Its immutability is load-bearing for
   `captured_at` semantics (`bar_capture_certificate.py:27-37`). Evidence is recorded
   beside the bar; the bar is untouched.
5. **The claim the evidence supports is "no change detected between these observations"**,
   never "the provider did not rewrite in that interval". A→B→A between two polls is
   invisible to any polling scheme, and the write-up must say so in those terms.
6. **Named unsolved:** replay idempotency (no response identity exists today), retention
   racing a comparison against partition drop (existing retention takes tier advisory
   locks; a new writer must participate), digest collision vs exact equality, and a
   durable record for invalid or failed observations. These are why nothing was built.

## 6. Prior art — the same invariant at the other writer

`sql/387_price_daily_revision.sql` (#2414) records per-entity identity of an OHLCV
overwrite on the **daily** layer, inside the bar-write transaction.
`strategy_intraday_bars` is the second writer and has no detection at all — the
second-writer gap the engineering rule predicts. The shapes must differ: `price_daily` is
upserted so a revision is observable at write time, whereas the intraday bar is never
rewritten by contract.

## 7. Claims made and withdrawn during this session

| claim | status |
| --- | --- |
| "widening the collection window to pre-open writes zero rows" | **withdrawn** — a lagging watermark makes a pre-open fire a legitimate backfill (round-robin selects 12 of 18 members, so watermarks routinely lag), and the same job's quote capture writes coverage rows independently |
| "always at least three stored bars behind the watermark" | **withdrawn** — count-based fetch; zero overlap is reachable at the 1m cap |
| "the member is the transaction unit" | **withdrawn** — `connect_job(autocommit=True)`; bar write plus watermark commit atomically, gaps and cursor advancement separately |
| "the stored row is the level it traded at" | **withdrawn as stated** — true only for a contemporaneous baseline (§3) |
| "pre-open observation creates a straddling bracket" | **withdrawn** — it narrows one that already exists; the correct justification is that the bracket must exclude the open (§2) |
| "pre-open cadence is vacuous until a producer exists" | **holds**, on the corrected ground that no comparison is recorded anywhere |

## 8. The corrected head

**Re-observation comparison with per-response bounds, on the existing in-session cadence,
restricted to contemporaneous baselines — then two or more pre-open re-observation fires
so a bracket can lie inside `(T, O)`.** Not capture, and not breadth first: breadth only
raises the chance an event lands on a panel member, which is worth nothing until a
comparison is recorded at all.

Nothing here is operator-gated.

Refs #2840. Refs #2437.
