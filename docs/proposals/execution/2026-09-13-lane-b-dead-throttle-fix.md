# #2946 step 3 item 1 — lane B's pacer, sourced from lane B's own budget (FINDING 1)

Status: proposal. Author: autonomy loop, 2026-09-13, from `9d8b4305`.
Input: `docs/proposals/execution/2026-09-13-etoro-quota-lane-map.md` (step 1) and
`docs/proposals/execution/2026-09-13-etoro-quota-load-census.md` (step 2).

## The verdict changed at Codex checkpoint 1 — read this first

Step 2's close-out named *"hoist the provider out of the lane-B request loop so the floor
applies"* as the preferred fix, and this spec's first draft went further and proposed a
process-wide clock for `EtoroBrokerProvider` mirroring #2934's `etoro.py`. **Both are
wrong, and in the same direction: they make a dead floor live on a lane where that floor
does not belong.** Recording why, because the fix looks obviously right and the next
session will otherwise ship it.

1. **Lane B is documented DEDICATED — "not shared (pooled across) any other endpoint"**
   (`etoro_quota_lanes.py:84-91`, portal `check-instrument-trading-eligibility`).
   `_ETORO_WRITE_INTERVAL_S` paces `_http_write`, which also carries lane A (order
   submission) and lane C. Pacing lane B from that clock means a research eligibility
   sweep delays **order submission** for no quota reason. Step 1 already found exactly
   this and called it item 1's blocker; the earlier coordination proposal
   (`2026-09-13-etoro-trading-throttle-coordination.md`) rejected a per-user-key registry
   on the same ground. Making the floor reach lane B is that rejected coupling, arrived
   at from the other end.
2. **It can turn into a refusal on the capital path.**
   `strategy_core_broker_preflight.py:106` derives `CORE_MAX_ACCOUNT_RISK_AGE_SECONDS`
   from *one nominal 3.5 s throttle wait* plus the HTTP timeout. Cross-provider queueing
   adds waits that derivation does not model, so a stale-risk refusal becomes reachable
   without any broker retry. A pacing change that can fail-close the execution path is
   not "over-restriction in the safe direction".
3. **The bounded-blocking argument in the first draft was false.** A reader behind a
   writer pays the writer's 3.5 s sleep *and then its own* 1.1 s; an intervening read
   resets the shared stamp so eligibility spacing becomes 7 s, not 3.5 s, and that
   accumulates across a 100-instrument batch; and `_request` re-acquires the throttle on
   every retry, inside the attempt loop, where the caller's outer sleep cannot interleave.

So the floor being unreachable on lane B is **not** the defect. The defect is that
nothing says so — which is how a correct accident became a "dead code" finding, and how
the obvious repair became a regression.

## What the defect actually is

Three things, all provenance:

**(a) Lane B's pacer is a hand-picked number.** `CORE_ELIGIBILITY_REQUEST_INTERVAL_S = 3.2`
(`app/services/strategy_core_eligibility.py:67`). Its docstring justifies it from *"at
most 100 ids per request"* — which bounds ONE request and is no source rule at all for a
job making singleton requests, as step 2 established. The lane map holds the governing
figure (20/min dedicated) and the constant does not reference it.

**(b) The lane map's own interval property is off by one.**

```python
@property
def conservative_min_interval_s(self) -> float:
    return LANES[self.lane].window_s / self.conservative_per_minute   # :172
```

Step 2 established the counting rule: a caller spaced at `i` fires at 0, i, 2i … so a
rolling window holds `floor(window / i) + 1` stamps, one MORE than sustained throughput.
At lane B that property returns `60 / 20 = 3.0`, and a caller paced at exactly 3.0 places
**21** stamps against a budget of **20**. `QuotaLane.min_interval_s` (`:71`) has the same
shape.

This is not academic: `conservative_min_interval_s` is the required value in
`test_configured_floor_is_at_least_the_conservative_minimum`
(`tests/test_etoro_quota_lanes.py:116`) — the one test in that file that guards live
configuration. It currently admits a floor that is over budget by one request.

**(c) Two mechanisms pace one path and neither is derived.** With (a) fixed the second
mechanism (`_ETORO_WRITE_INTERVAL_S`, reached only if a provider is reused) is still
there. It cannot be removed — long-lived callers legitimately use it — so the invariant
to pin is the RELATIONSHIP: lane B's own pacer is the binding one, deliberately, and the
write floor must never be treated as lane B's pacer.

## Source rule

- Lane B budget: `QuotaLane("B_eligibility").documented_per_minute = 20`, `scope="dedicated"`,
  `source_url` = the portal's own eligibility page. `CallSite.conservative_per_minute`
  takes the lower of that and the general tier (also 20) → **20**.
- Counting rule: `stamps(i) = floor(window_s / i) + 1`. From step 2, and the reason 3.2 s
  yields 19 rather than the 18.75 that sustained throughput suggests.
- Nothing here is invented. The one CONSTRUCTED choice is the headroom — one spare
  request — and it is constructed rather than cited because the portal publishes a budget,
  not a recommended utilisation. It is stated in the code, derived from the budget, and
  pinned by a test, per the "fix it by construction and freeze it" rule.

## Change

### 1. `etoro_quota_lanes.py` — one derivation, correct by the counting rule

```python
def min_interval_for_stamps(window_s: float, max_stamps: int) -> float:
    """Smallest spacing whose ROLLING-window stamp count is <= ``max_stamps``.

    ⚠ NOT ``window / max_stamps``.  A caller spaced at ``i`` fires at 0, i, 2i …, so a
    window-long rolling window holds ``floor(window / i) + 1`` stamps -- one MORE than
    sustained throughput.  Compliance needs ``floor(window/i) <= max_stamps - 1``, which
    holds for every ``i > window / max_stamps``; the bound is OPEN, so the smallest value
    expressible from the budget that satisfies it is ``window / (max_stamps - 1)``, which
    lands exactly ON ``max_stamps``.
    """
    return window_s / (max_stamps - 1)
```

`QuotaLane.min_interval_s` and `CallSite.conservative_min_interval_s` both route through
it. Every currently configured floor still passes (read 1.1 ≥ 60/59 = 1.017; write
3.5 ≥ 60/19 = 3.158); the lane-G history exception is unaffected. So this tightens the
guard without moving any floor — verified in the test plan, not asserted here.

### 2. `strategy_core_eligibility.py` — derive the pacer from lane B

```python
CORE_ELIGIBILITY_REQUEST_INTERVAL_S = min_interval_for_stamps(
    _LANE_B.window_s, _LANE_B.conservative_per_minute - 1
)   # 60 / 18 = 3.333…s -> floor(60/3.333) + 1 = 19 stamps against a budget of 20
```

`3.2 → 3.333 s`. One request of headroom, same as the status quo the census measured, now
derived from the budget instead of coinciding with it.

⚠ Layering (Codex ckpt-1 #18): the import is `app.providers.implementations.etoro_quota_lanes`,
which is a **pure data module** — stdlib-only imports (`dataclasses`, `datetime`,
`typing`), no provider, no `httpx`, no `settings`. It references provider modules by
name STRING and resolves them with `importlib` in tests. Importing the lane map into a
service pulls in no transport configuration. Importing `etoro_broker` would have, which
is the version this rejects.

### 3. Pin the relationship as an invariant, not a coincidence

A test asserting lane B is paced by its own constant and NOT by `_ETORO_WRITE_INTERVAL_S`,
carrying the reason in its docstring. This is the artefact that stops the next session
shipping the hoist.

### 4. `strategy_core_eligibility_refresh.py:52` — a derived statistic in prose

`"100 * CORE_ELIGIBILITY_REQUEST_INTERVAL_S is about 5.3 minutes"` is hand-computed from
3.2 and goes stale here (→ 333 s). Replaced with the inequality it exists to assert, and
pinned in a test instead of written down.

### 5. `scripts/measure_2946_quota_load.py`

Its FINDING-1 narrative states the 3.2 s constant and "looser than the floor it stands in
for" as a live conclusion. That text becomes stale. Updated to the post-fix statement,
computed from the constants it already imports dynamically rather than re-written by hand.

## Explicitly NOT in this change

- **No floor moves.** `_ETORO_READ_INTERVAL_S` and `_ETORO_WRITE_INTERVAL_S` keep their
  values, and no client's clock changes scope.
- **No lane split.** Step 2 verdicted it `insufficient_evidence` (0 eToro 429s over 76
  days from two independent sources; demand far under every budget).
- **No coordinator.** Which means step 2's acceptance clause 2 — *"two concurrent lane-B
  callers on one user key must not exceed it"* — is **NOT satisfied by this change, and is
  not claimed to be.** The hourly `core_eligibility_refresh` job and an operator running
  `prove_2603_core_eligibility.py` are separate PROCESSES; a process-local clock cannot
  reach across them, and neither can any pacing constant. Satisfying it needs either a
  cross-process gate (`PostgresFloorGate` exists but is wired for SEC only and fails OPEN)
  or serialising the two callers on an advisory lock. Both are the coordinator, whose
  verdict stands. What this change does is make the SINGLE-caller bound true by
  construction instead of by coincidence, which is clause 1.
- **No claim that lane B is isolated from lanes A/C.** It is not: eligibility rides
  `_http_write`, so within any long-lived provider it already shares a clock with order
  writes. This change neither creates nor removes that. It only declines to WIDEN it to
  process scope.

## Test plan

Pure. No DB, no network, no broker request.

| test | what |
| --- | --- |
| `test_etoro_quota_lanes.py` — new | `min_interval_for_stamps` satisfies its own contract across every lane: `floor(window / i) + 1 <= max_stamps`, and `window / budget` does NOT (the off-by-one it replaces, asserted as the revert probe). |
| `test_etoro_quota_lanes.py::test_configured_floor_is_at_least_the_conservative_minimum` | unchanged assertion, tightened required value. Must still pass for every configured floor — that is the regression check on change 1. |
| `test_2946_quota_load.py` — replaces the "looser than the floor" test | acceptance clause 1, for every lane-B call site: `floor(60 / CORE_ELIGIBILITY_REQUEST_INTERVAL_S) + 1 <= CallSite.conservative_per_minute`, read from the lane map, never a literal. Plus the recorded shortfall: `2 ×` that exceeds the budget, which is clause 2 and is left open deliberately. |
| new — the lane-B pacing invariant | `CORE_ELIGIBILITY_REQUEST_INTERVAL_S` is derived from lane B's `CallSite`, and is NOT `_ETORO_WRITE_INTERVAL_S`; docstring carries the dedicated-quota reason. |
| new — per-run work bound | `CORE_ELIGIBILITY_REFRESH_MAX_PER_RUN * CORE_ELIGIBILITY_REQUEST_INTERVAL_S` is inside the job's REGISTERED cadence, read from the scheduler's `ScheduledJob` (the coupling `tests/test_2603_core_eligibility_refresh.py:89` already uses) rather than a literal `3600`. ⚠ It bounds the SLEEPS only — HTTP, retries, `Retry-After` and lock waits are excluded, and the docstring says so rather than calling it a runtime bound. |
| `test_2946_quota_load.py` — the two characterisation tests | KEPT and re-framed. They correctly describe the mechanism (a per-request provider never reaches its own floor); what changes is the verdict attached to it, from "defect" to "deliberate, because lane B is dedicated — see this spec". |

⚠ Prevention log §3665 governs every spacing assertion and the existing harness already
obeys it: stamps are read from a `list` subclass whose `__setitem__` fires inside
`_throttle_and_stamp`'s critical section, never from a wall-clock reading taken after the
call returns.

No autouse clock-reset fixture is needed — the first draft required one only because it
promoted the clock to module scope, which this does not.

## Definition-of-done clauses 8-12

Not applicable. No parser, no ETL, no schema migration, no ownership/observations data,
no backfill, and no stored figure moves. The change is a pacing constant, its derivation,
and the guard that checks it.

## Security

Unchanged. No credential handling, no new endpoint, no authorisation surface, no change to
the unattended broker-mutation guard. Eligibility is an informational call; this change
only spaces it, slightly more slowly.
