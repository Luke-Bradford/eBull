# #2946 step 2 — the representative-interval load census

Spec, revision 2. Written 2026-09-13 from `a555fe2b`. Revision 1 went through Codex
checkpoint 1 and 30 of its findings are folded in below; the ones that changed the SHAPE
of the work rather than a sentence are marked ⚠C.

Step 1 (`bb1c2806`, PR #2969) produced the LANE MAP — what the portal documents. This
step produces the LOAD — what we actually spend against it, and whether the missing
cross-instance coordination costs anything. Step 1's own closing line sets the boundary:
*"Documentation proves a number is DOCUMENTED, never that it is ENFORCED. #2946 step 2
is the measurement."*

## Scope

#2946 acceptance item 2, verbatim:

> Measure one representative operating interval: requests/rolling minute, throttle wait,
> 429s and reconciliation latency. A fake-clock concurrent test is required; deliberately
> exceeding a real broker quota is not.

Plus item 4's verdict — *whether an implementation is warranted, the concrete affected
workload and measurable acceptance*. Item 3's comparison of candidate gates is written
only as far as the measurement supports; building one is NOT in this unit.

**No real broker request is made by anything in this change.** The census reads the dev
DB and the retained jobs-daemon log; the test uses `httpx.MockTransport`.

## Source rule

The governing numbers are the portal's, already read by step 1 and recorded in
`app/providers/implementations/etoro_quota_lanes.py` (`VERIFIED_ON = 2026-09-13`). This
step cites that module rather than re-reading the portal: a second fetch would produce a
second transcript of the same pages and two sources of truth for one table.

⚠C **Step 1 is explicit that its evidence is a DATED TRANSCRIPT, not a pinned artefact**
— *"WebFetch returns a rendered reading, not raw bytes, and no response hash was
captured"* (`etoro_quota_lanes.py:38-40`). Revision 1 of this spec called it "pinned".
It is not, and the census must not upgrade its provenance.

Where a budget is quoted it is `CallSite.conservative_per_minute` — the LOWER of the
per-endpoint number and the general page's operation tier, which is step 1's recorded
policy because the portal states no precedence between the two readings.

⚠C **Lane G is NOT aggregated.** Its membership is `UNENUMERATED`
(`etoro_quota_lanes.py:131-141`) and its two call sites carry different conservative
readings, one of them flagged `membership_ambiguous`. Summing them into "lane G load"
would invent a data treatment the source does not license. Lane G is reported per call
site, with the comparison against a budget stated as **undefined**, not estimated.

The measurement side has no external source rule to find: this is our own traffic. Its
rules are the repo's own, cited inline.

## What is OBSERVED vs what is DERIVED vs what is UNCOUNTED

⚠C Revision 1 had two buckets. Three are needed, because the third is where a census
quietly becomes a false clean bill of health.

| bucket | meaning | reported as |
| --- | --- | --- |
| OBSERVED | one durable artefact per HTTP request | a count |
| DERIVED | code-path reading × measured fire count | a bound, with the algorithm stated |
| UNCOUNTED | traffic with neither artefact nor fire record | named, never zero-filled |

Only ONE lane is OBSERVED: lane B. `strategy_core_eligibility_proofs` writes one row per
`check_instrument_eligibility` call, carrying `observed_at` and `recorded_by`.

⚠C The UNCOUNTED set, enumerated so the verdict cannot be read as account-wide:

- the **API/uvicorn process** — `app/main.py:79` starts `EtoroWebSocketSubscriber`, whose
  `_default_reconcile_runner` (`etoro_websocket.py:979-1011`) builds its OWN
  `EtoroBrokerProvider` and calls `get_portfolio` + `get_trade_history`. It is
  event-driven off private WS events (`_schedule_reconcile`, line 1082), writes no
  `job_runs` row, and runs in a different OS process from the jobs daemon. Its retained
  log ends 2026-08-26.
- the **two API endpoints** `close_strategy_owned_position` and `rebalance_core_sleeve`
  (`app/api/strategies.py:3357, 3852`).
- the **six research/operator scripts** that construct a broker provider. Lane B's
  `recorded_by` does capture `prove_2603_core_eligibility`, so that one is OBSERVED for
  lane B only; the rest are uncounted.
- step 1's **four `KNOWN_UNTHROTTLED` raw-httpx sites** (`etoro_quota_lanes.py:363-388`),
  which bypass every throttle.
- **suppressed fires** — the scheduler drops a fire with `max_instances active` and no
  `job_runs` row is written, so demand that was never serviced is invisible.

⚠C Consequently the census measures **SERVICED DEMAND**, not offered load. Queued,
suppressed and cancelled demand is out of reach. The word "offered" does not appear in
the output.

## The measurements

### M1 — lane B, OBSERVED rolling-60s peak (full history)

Over ALL `strategy_core_eligibility_proofs` rows, not a window:

- max count in any rolling 60 s window, overall and per `recorded_by`;
- the same split by UTC day, so one batch cannot be reported as a standing rate;
- the minimum observed inter-request gap, compared against
  `_ETORO_WRITE_INTERVAL_S = 3.5` (`etoro_broker.py:103`).

Lane B's conservative budget is 20/min. One provider instance's write clock is floored at
3.5 s, i.e. **17.14 req/min is the per-instance ceiling across lanes A+B+C+G-write
COMBINED** — so the question is not whether one caller breaches, it is how little
headroom one caller leaves for a second.

### M2 — per-lane DERIVED serviced demand, with the algorithm stated

⚠C Revision 1 gave fire counts and durations and called the result a rolling-minute
load. It is not one: totals do not locate requests inside a window. The algorithm is
stated here or the number is not reported.

For a fire of duration `d` on a client with floor `f`, holding at most `k` requests, the
most requests it can place in ANY 60 s window is

    bound(fire) = min(k, floor(min(d, 60) / f) + 1)

⚠C The `+ 1` matters and revision 1 omitted it: requests at 0.0, 1.1, 2.2 and 3.3 s all
fit inside a 3.7 s run, so a 3.7 s job at a 1.1 s floor bounds to **4**, not 3. The bound
further assumes every request lies inside the recorded `[started_at, finished_at]` and
that one clock was retained for the whole fire.

For a set of fires overlapping a window, the window bound is the SUM of their individual
bounds — and only for fires on **independent clocks**; fires sharing a clock contribute
one floor between them. ⚠C Peaks are never summed across callers whose peaks occur at
different times; the bound is computed per window, not per caller.

Per-fire request counts `k` come from the code, cited `file:line`, and distinguish four
different things that revision 1 ran together: instruments, logical calls, HTTP attempts,
and persisted rows. `job_runs.row_count` is none of them.

⚠C Three `job_runs` corrections that materially move the numbers:

1. **Reaped rows carry a false `finished_at`.** `app/services/ops_monitor.py:648` rewrites
   an orphaned `running` row at boot, so its duration spans the OUTAGE, not the work. Any
   row whose message matches the reaper's is excluded from duration and overlap
   arithmetic and reported as a separate count. The host was down 2026-08-26 → 09-12, so
   this is not hypothetical.
2. **A recorded fire need not perform HTTP.** Prereq skips (`_record_prereq_skip`), empty
   scopes (`quotes_refresh`'s 0-instrument branch) and gated runs make zero requests. Fires
   are split by whether the code path they took can reach a broker call.
3. **Historical fires may not reflect current code.** Provider lifetime, batching and
   retry policy have all changed inside the retained history. Derived counts are computed
   only over the interval since the current revision of each call path, and that interval
   is stated per job.

Reported over two intervals, never pooled: full retained history, and the post-recovery
interval. ⚠C The post-recovery interval is 2026-09-12 → 09-13 and **contains a weekend
and a boot catch-up**. It is reported as what it is — a low-trading, catch-up-inflated
sample — and no verdict rests on its representativeness.

### M3 — concurrency census (possible overlap, not contention)

⚠C Revision 1 called this "clock overlap" and classified by job name. Both are wrong.

- Overlap of two runs is **possible** request overlap. It does not demonstrate
  contention, and non-overlapping runs can still share a rolling minute. The census
  reports it as an upper bound on co-occurrence and says so.
- Clock identity is a property of the **construction site**, not the job. It is read from
  the constructor: `EtoroMarketDataProvider` passes the module-level
  `_ETORO_RATE_LIMIT_CLOCK` / `_LOCK` (`etoro.py:77-78, 126-130`) so every instance in a
  process shares one clock; `EtoroBrokerProvider` builds a fresh `shared_ts` and lock per
  instance (`etoro_broker.py:225-236`). The census maps each job to its construction site
  by `file:line` and derives clock identity from that.
- ⚠C Independent clocks do NOT by themselves imply competing quotas — the quota is per
  **user key**, so two instances contend only if they hold the same key. The census
  reports the credential identity each caller loads.
- ⚠C Report **N-way** maximum concurrency (the largest number of eToro-touching runs live
  at any instant), not only pairwise counts, which both miss 3-way saturation and
  double-count occasions.

### M4 — retryable-response census

`ResilientClient` logs `Retryable %d from %s %s` on every retried attempt
(`resilient_client.py:213-216`). Parse the whole retained daemon log.

⚠C **Host-based classification is broken and revision 1 relied on it.** The `%s %s` is
the METHOD and the URL ARGUMENT AS PASSED — and the broker passes relative paths
(`etoro_broker.py`, e.g. `get_trade_history`), so an eToro 429 logs no hostname at all.
Classification is therefore: absolute URL → host; leading `/` → path-prefix match against
the `CALL_SITES` templates in the lane map; anything else → an explicit **unmatched
bucket**, reported, never silently dropped. Whole-file accounting is printed: lines read,
lines matched, lines unmatched.

⚠C **This log is not the only 429 evidence.** `app/services/sync_orchestrator/exception_classifier.py:55`
classifies `RATE_LIMITED`, and a final-attempt failure raises past the warning into
`job_runs.error_msg` (`resilient_client.py:201-203`). Both are queried, and the two
sources are reconciled rather than added — a single incident can appear in both.

⚠C Stated limits: the count is a **lower bound** (the final attempt emits no warning);
its scope is the **jobs process only**; retention is reported as first/last timestamp,
byte size and whether any rotation or format change is visible in the file. ⚠C Log
timestamps are naive local-time; DB timestamps are tz-aware UTC. The offset used to align
them is stated, and any window that straddles the retention boundary is marked incomplete
rather than reported as a low count.

### M5 — the fake-clock concurrent test (required by acceptance)

⚠C Revision 1 proposed one mechanism for three different questions. Codex is right that a
virtual clock shared across real threads can itself serialise independent clocks and hide
the defect. The three questions get three mechanisms:

**A — identity, asserted directly.** Before anything is measured, assert the constructor
choice with `is` comparisons: two `EtoroMarketDataProvider` instances share one clock
object and one lock object; two `EtoroBrokerProvider` instances do not. This is the
defect in its simplest form and needs neither threads nor a clock. ⚠C Recording wrappers
are attached only AFTER these assertions, so the instrumentation cannot impose the
sharing under test; module-level state is restored in a fixture teardown, because
`_ETORO_RATE_LIMIT_CLOCK` is global and leaks between tests.

**B — rate, measured single-threaded on a patched clock.** The defect (two instances,
same user key, no coordination) is deterministic and does not need real concurrency to
exhibit. Requests are driven through the shipped entry points — `provider._http_write.post(...)`
into an `httpx.MockTransport` — alternating between two instances built with the SAME
api/user key, with `time.monotonic` and `time.sleep` patched to a virtual clock. Measured:
max requests in any rolling 60 s window, and total throttle wait per caller.
⚠C The quantity asserted is the **stamp** sequence recorded from inside
`_throttle_and_stamp`'s critical section (via a `list` subclass whose `__setitem__`
records — prevention log §3665), NOT transport arrival order. Stamp spacing and arrival
spacing are distinct invariants and the rate claim is about stamping.

**C — occupancy, asserted with a real-thread barrier.** Per prevention log §4318, a
"did these overlap?" question is decided by a `threading.Barrier(2)` placed inside the
mock transport handler — reachable only after the throttle has returned. If the two
broker instances coordinate, occupancy caps at 1, the barrier cannot form and
`BrokenBarrierError` fires. Bounded `join`, worker exceptions re-raised in the main
thread.

**D — the within-instance over-restriction.** On ONE `EtoroBrokerProvider` starting from
an idle clock, a lane-A request issued after a lane-B batch waits behind the 3.5 s write
floor, because lanes A, B and C share one clock despite having three INDEPENDENT
documented budgets. ⚠C Requires an explicitly idle starting clock and controlled timing,
or the measured wait includes prior debt. The measured wait goes in the census.

⚠C B/C/D measure opposite-signed defects on the same clock — across instances we
under-restrict, within an instance we over-restrict. Both must be stated, or the verdict
reads as "add coordination" when half the evidence says the coordination we have is
applied at the wrong granularity.

⚠C **Each assertion gets its OWN revert probe.** Revision 1 proposed one mutation for all
of them, which cannot invalidate D (flipping module-vs-instance sharing leaves lanes A
and B on one instance's clock either way). Probes: for A/B/C, give `EtoroBrokerProvider`
a module-level clock; for D, split the write clock per lane. Each probe must fail its own
test and, per the prevention log, be checked for toppling neighbours.

⚠C **Not covered, and said so rather than implied:** cross-PROCESS market-data traffic
(the module clock is process-local, and the API process has its own), sequential instance
churn, retry/429 cooldown interaction, and lane G's accepted floor exception. These are
mechanisms the test cannot reach; they are listed as residual unknowns in the verdict.

## Deliverables

1. `scripts/measure_2946_quota_load.py` — M1-M4, read-only, one arm per measurement,
   writing a dated artefact under `var/quota-census/`. ⚠C The artefact records the git
   SHA, the DB cutoff timestamp, the log path + byte size + line accounting, and every
   query it ran, so the run is reproducible rather than merely dated.
2. `tests/test_2946_quota_contention.py` — M5.
3. This file, rewritten from spec into the census + verdict once the numbers exist.
4. An issue comment carrying the verdict for item 4.

## Non-goals

- No coordinator, no `RateGate` injection, no floor change. Item 3/4 decides that and
  this unit produces its input.
- No real broker request, no portal re-fetch.
- No change to `etoro_quota_lanes.py`'s table. If the census contradicts it, that is a
  finding to report, not an edit to make silently.

## Acceptance

- Every reported figure is labelled OBSERVED, DERIVED or UNCOUNTED, and no UNCOUNTED
  source is zero-filled.
- Lane B's peak is computed over full history; the derived arms state their algorithm and
  their interval, and never pool across the outage.
- Reaped `job_runs` rows are excluded from duration/overlap arithmetic and counted
  separately.
- The 429 census prints whole-file line accounting with an unmatched bucket, states its
  process scope, and states that it is a lower bound.
- The test drives the shipped entry points, asserts stamps rather than arrivals, and each
  assertion is revert-probed with its own mutation.
- ⚠C The verdict has THREE permitted outcomes, not two: *warranted*, *not warranted at
  the measured load*, or **`insufficient_evidence`**. Sparse retained traffic and zero
  logged 429s in one process cannot by themselves establish safe headroom, enforcement,
  or the absence of reconciliation harm. If the evidence only supports the third, that is
  the answer, and it names what would have to be instrumented to reach one of the others.
