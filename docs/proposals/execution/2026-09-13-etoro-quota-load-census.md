# #2946 step 2 — the eToro quota load census, and the verdict for item 4

Measured 2026-09-13 from `a555fe2b`. Step 1 (`bb1c2806`, PR #2969) produced the lane MAP
— what the portal documents — and closed with the boundary this step crosses:
*"Documentation proves a number is DOCUMENTED, never that it is ENFORCED. #2946 step 2
is the measurement."*

Instrument: `scripts/measure_2946_quota_load.py` (read-only; one REPEATABLE READ
snapshot plus one log file). Tests: `tests/test_2946_quota_load.py`. Artefact:
`var/quota-census/2026-09-13-census.txt`.

**No real broker request was made.** No trading write, no portal re-fetch, no floor
change, no coordinator.

## The headline

Two findings, and neither is the one the ticket expected.

1. **On the entire lane-B path the `ResilientClient` floor is unreachable.** All three
   eligibility callers construct an `EtoroBrokerProvider` *inside* the request loop
   (`app/workers/scheduler.py:6290`, `scripts/prove_2603_core_eligibility.py:119` and
   `:191`), so every request is the first on a virgin clock and
   `_ETORO_WRITE_INTERVAL_S = 3.5 s` paces nothing. The actual spacing is a hand-written
   `time.sleep(CORE_ELIGIBILITY_REQUEST_INTERVAL_S)` where that constant is **3.2 s**
   (`app/services/strategy_core_eligibility.py:67`) — **looser than the floor it stands
   in for**, in a different module from it.
2. **That leaves one request of headroom.** A caller spaced at 3.2 s places
   `floor(60/3.2) + 1 = 19` requests in a rolling minute against lane B's conservative
   budget of **20**. Two lane-B callers — the hourly job and an operator running the
   script — reach **38, or 190% of the budget**, and nothing serialises them: the job
   takes the `etoro` job lane and a script takes no lane at all.

⚠ The `+ 1` is load-bearing. Sustained throughput is `60/3.2 = 18.75`, which rounds to a
comfortable-looking 18. A rolling-minute quota counts *stamps*, and a caller firing at
0, 3.2, … 57.6 places nineteen of them.

## What could NOT be measured, and why that matters

⚠⚠ **No eToro lane has a per-request artefact.** The first draft of this work treated
`strategy_core_eligibility_proofs` as a request log for lane B and computed a "17
requests in a rolling 60 s" figure from it. That was wrong four ways, each checkable:

1. `observed_at TIMESTAMPTZ NOT NULL DEFAULT now()` (`sql/346_core_eligibility_proofs.sql:47`)
   — and Postgres's `now()` is **transaction-start** time. The insert runs inside
   `with core_submission_lock(conn), conn.transaction():` (`scheduler.py:6316`), a
   transaction beginning *after* the HTTP round-trip and *after* a lock wait. The column
   times the recording, not the request.
2. A request failing in transport writes no row — eligibility is explicitly
   non-persisting (`etoro_broker.py:867`) and transport failures are excluded
   (`strategy_core_eligibility.py:273`). Failures still spend quota.
3. Other eligibility callers write no proof at all (`strategy_paper_executor.py:1241`,
   `strategy_position_manager.py:408`).
4. The table's own comment: *"Evidence of an observation, not an attestation of one …
   nothing can stop a caller asserting an account it never contacted."*

So the census reports **CONFIGURED** (a pacing constant read by import), **DERIVED** (an
arithmetic bound from a code reading and a `job_runs` fire) and **UNCOUNTED** (traffic
with neither). It never reports an observed request rate, because none exists.

**UNCOUNTED, named every run so it cannot be read as zero:** the WS reconcile runner
(`etoro_websocket.py:999`, own provider, *different OS process*), subscriber REST quote
polling (`:1622`), API candle requests (`app/api/instruments.py:1390`), two
operator-triggered endpoints (`app/api/strategies.py:3357`, `:3852`), six broker
constructions across five scripts, and step 1's four raw-`httpx` sites that bypass every
throttle.

⚠ Consequently every rate here is a **lower** bound on account-wide load while every
per-fire figure is an **upper** bound on that fire. The two point opposite ways and
neither becomes a headroom claim on its own.

## 429s — zero, and the exact scope of that zero

| source | result |
| --- | --- |
| jobs-daemon log, 1,200,426 lines, 2026-06-29 → 09-13 | 3,107 retryable lines = 3,105 `www.sec.gov` 503 + 2 eToro (one 500, one 504) + 0 malformed. **eToro 429s: 0** |
| `job_runs.error_category = 'rate_limited'` | 15 rows, **all `sec_atom_fast_lane`**, none eToro |

⚠ Classifying by hostname would have found none of this. `ResilientClient` logs the URL
**as passed** (`resilient_client.py:213`), and the eToro providers pass *relative paths*
against an httpx `base_url` — so an eToro 429 carries no hostname. The census matches
absolute → host, leading `/` → path prefix, and prints an explicit malformed bucket with
whole-file accounting.

⚠ The count is a **lower bound**: the final attempt raises via `raise_for_status()`
without emitting the warning line (`resilient_client.py:201-203`), and callers swallow
terminal failures (`scheduler.py:4492`; `strategy_paper_executor.py:1242` turns an
eligibility exception into a rejection). Its scope is the **jobs process**; the API
process logs elsewhere and its retained log ends 2026-08-26. Nothing here is account-wide.

## Concurrency — possible overlap, not demonstrated contention

Full retained history. Clock scope is read from the *construction site*, never the job
name.

| pair | n | max overlap | clocks | shared lanes |
| --- | ---: | ---: | --- | --- |
| `daily_portfolio_sync` × `strategy_paper_cycle` | 764 | 4.5 s | INDEPENDENT | E |
| `daily_candle_refresh` × `quotes_refresh` | 50 | 84.1 s | **SHARED floor** | F |
| `daily_portfolio_sync` × `execute_approved_orders` | 20 | 0.0 s | INDEPENDENT | E |
| `core_eligibility_refresh` × `strategy_paper_cycle` | 7 | 2.5 s | INDEPENDENT | **B** |
| `execute_approved_orders` × `strategy_paper_cycle` | 1 | 0.5 s | INDEPENDENT | **A, B, C, E** |

Max simultaneous eToro-touching job runs: **3**.

The lane-F row is #2934 working: `EtoroMarketDataProvider` passes the module-level
`_ETORO_RATE_LIMIT_CLOCK` (`etoro.py:77-78`), so fifty overlaps cost one floor between
them. Every other row is `EtoroBrokerProvider`, which builds a fresh clock per instance
(`etoro_broker.py:225-236`), so those rates add.

⚠ Job concurrency is neither instance concurrency nor quota concurrency: one job can fan
out to several providers (every per-request caller does), two jobs can share one clock,
and two overlapping jobs can touch disjoint lanes. The table is an upper bound on
co-occurrence *between jobs that write a `job_runs` row*, and blind to every UNCOUNTED
caller.

⚠ The quota is per **user key**, so independent clocks contend only on a shared key. Two
live non-revoked demo credential rows exist; row inequality does not prove key
inequality, and historical calls cannot be attributed to today's rows at all.

## Reconciliation latency — no population

Acceptance item 2's fourth quantity has a governing rule already, and the census uses it
rather than inventing one: `enforce_reconciliation_slo` measures unresolved order
identity from `strategy_order_reconciliation_state.first_unresolved_at` against a
deployment-supplied `max_unresolved_seconds`
(`app/services/strategy_order_reconciliation.py:1024`). A whole-job duration is not this.

`strategy_order_reconciliation_state` holds **0 rows**.

⚠ Zero unresolved rows means the SLO has no population, **not** that reconciliation is
fast. With the demo account holding one manual filled order and no recommendation-origin
orders, this quantity has no observations at all. That is an evidence gap.

## `job_runs` corrections applied

- **134 reaped rows excluded** from duration and overlap arithmetic. `ops_monitor.py:648`
  rewrites an orphaned `running` row at boot with `finished_at = now()`, so its
  "duration" spans the 2026-08-26 → 09-12 outage, not the work — `daily_candle_refresh`
  alone had 107 such rows and a 15,410 s maximum. ⚠ Excluding them removes real requests
  too: the affected windows are incomplete, not clean.
- ⚠ Counting stranded `running` rows is the wrong detector — the reaper has already
  converted them. Match the message.
- **Suppressed fires are NOT invisible.** An earlier draft said they were;
  `app/jobs/runtime.py:1636` records a `max_instances_active` skip row and `:1665`
  records misfires.
- The credential loader is deliberately **not** used: it writes an access-audit row and
  bumps `last_used_at` (`app/api/broker_credentials.py:469`), which would make a
  read-only census a writer.

## Tests

`tests/test_2946_quota_load.py`, pure, driving the shipped `ResilientClient._request`
into an `httpx.MockTransport`. The sibling `tests/test_2946_etoro_throttle_lock.py` owns
item 2's lock fix and the identity assertions; this file owns the load questions.

1. **`test_a_provider_built_per_request_never_reaches_its_own_floor`** — four fresh
   providers place four requests with zero spread; the identical sequence through one
   reused provider pays 3.5 s between every pair.
2. **`test_two_instances_on_one_user_key_stamp_at_the_same_virtual_instant`** —
   fake-clock **and** concurrent, which is what item 2 asks for in those words. Two
   instances on the same key, released together by a `threading.Barrier`, stamp at the
   same virtual instant; a second arm forcing them onto one clock and lock pays the
   floor, so the assertion cannot pass for an unrelated reason.
3. **`test_the_lane_b_pacing_constant_is_looser_than_the_floor_it_substitutes_for`** —
   pins 3.2 < 3.5, 19 vs 18 stamps per minute, and 2 × 19 > 20.

Assertions read the stamp the throttle wrote **from inside its critical section** (a
`list` subclass whose `__setitem__` records — prevention log §3665), never a reading
taken afterwards. Transport arrival order is a different invariant and is deliberately
not asserted: a pacing lock is released before the HTTP call, so arrival says nothing
about stamping.

⚠ **Revert-probed, and the first probe found a defect in the test rather than the code.**
Giving `EtoroBrokerProvider` a module-level clock left test 1 passing, because
`attach_recorders` was being called per loop iteration and handed each provider its own
recorder — manufacturing the independence under test. Fixed by attaching all recorders
once, keyed on object identity, so a shared clock yields one recorder. Re-probed: that
mutation now fails tests 1 and 2 plus the sibling file's
`test_two_broker_instances_do_not_share_a_budget`, and nothing else. A second probe
(raising the pacing constant above the floor) fails test 3 alone.

## Verdict — item 4

**`insufficient_evidence` for a coordinator. One bounded, non-coordinator correction is
justified on arithmetic alone.**

Why not "warranted": zero eToro 429s in 76 days across both independent sources, and
aggregate serviced demand is far under every lane budget. Why not "not warranted": that
zero is a lower bound in one process, the reconciliation-latency population is empty, and
the largest traffic sources are UNCOUNTED by construction. Neither bound supports a
headroom claim, which is precisely why the third outcome exists.

**The correction that does not need more evidence** is finding 1. It is not a
coordination problem and does not wait on one:

- `CORE_ELIGIBILITY_REQUEST_INTERVAL_S = 3.2` substitutes for
  `_ETORO_WRITE_INTERVAL_S = 3.5` on a path where the latter cannot apply, and is looser
  than it. Two mechanisms pace one path and they disagree.
- Preferred fix: **hoist the provider out of the request loop** so the floor applies and
  the duplicate constant disappears. ⚠ This is a behaviour change — a provider would then
  live across a batch that has run for 1,022 s — so it belongs to step 3/4 with its own
  review, not to this measurement.
- Cheaper fix if hoisting is rejected: raise the constant to at least the floor. It
  removes the inversion but leaves two mechanisms in place.

**Measurable acceptance for whichever is chosen:** for every lane-B caller,
`floor(60 / pacing) + 1 ≤ CallSite.conservative_per_minute`; and two concurrent lane-B
callers on one user key must not exceed it — which does require cross-instance
coordination *on lane B specifically*.

That last clause is now safe to build and was not before. The prior spec
(`2026-09-13-etoro-trading-throttle-coordination.md`) rejected a per-user-key registry
because eligibility and what-if costs ride `_http_write` on **dedicated** portal quotas,
so one pool per credential would make a research census delay order writes on endpoints
the portal documents as independent — and it named lane-splitting as the prerequisite.
Step 1's lane map **is** that split. The rejection's stated blocker is gone; the
remaining question is whether the measured load justifies the machinery, and on this
evidence it does not yet.

**Residual unknowns the verdict does not cover:** cross-process market-data traffic (the
module clock is process-local and the API process holds its own), sequential instance
churn under sustained load, retry/429 cooldown interaction (a `Retry-After` pauses only
the failing request while siblings keep spending the same quota — already recorded as out
of scope on the prior spec), and lane G's accepted floor exception.

**What would have to be instrumented to reach a stronger verdict:** a per-request counter
on the eToro path. `ResilientClient` already accepts an `on_429` callback
(`resilient_client.py:74`) and **only SEC providers wire it** — so eToro 429s increment
no counter anywhere, and the log is the sole witness. Wiring that callback plus a
per-lane request counter is the smallest change that would turn every DERIVED number
above into an OBSERVED one.
