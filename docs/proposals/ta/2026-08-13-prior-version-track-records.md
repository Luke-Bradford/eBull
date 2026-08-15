# Prior-version track records on the Strategies overview (#2624 scopes 1 + 2)

## Problem

The identity hash covers registry BYTES (#2394 §2), so **any** registry-touching merge mints a
new `strategy_version`, and `sql/272_strategy_scan_watermark.sql` deliberately keys the watermark
on `(strategy_id, strategy_version)` — a new version starts *"a new track record beside the old
one"* (its own docstring). That versioning is correct and is not being weakened here.

What is broken is that "beside the old one" never renders. `get_strategy_overview` filters every
one of its reads on `strategy_version = ANY(%(versions)s)` where `versions = _current_versions()`,
so at a rotation the operator sees an empty card — no evidence, no scan, no history —
indistinguishable from a broken system. At today's merge cadence (7 registry-adjacent merges in
one day) that is the page's steady state.

## Premise re-checked before designing (working-order 3c)

The issue's snapshot has **healed**, and the spec is written against the current data, not the
issue's text:

```
$ current versions vs stored rows (dev, 2026-08-13)
  s1-time-series-momentum             +67dbf07c9d72  frontier=2026-08-11  results=32
  s2-cross-sectional-momentum         +83967fcb1fca  frontier=2026-08-11  results=32
  s3-mean-reversion-in-trend          +d58989368716  frontier=2026-08-11  results=32
  s4-volatility-compression-breakout  +91aadde63f07  frontier=2026-08-11  results=32
```

A backtest re-ran under the live versions on 08-12 18:59, so the current version is populated and
the blank page is not visible right now. **The defect is structural, not a stuck state**: it
returns at the next registry-touching merge. Acceptance therefore has to simulate a rotation;
"the page is blank today" is not available as evidence and must not be claimed.

One correction to the issue's text, measured: `legacy_result_count` is **not** a prior-version
summary. `_RESULT_COUNTS_SQL` filters on the current versions, so the field counts rows under the
CURRENT version that do not match a declared window — it reads 0 for all four strategies today.
Prior-version track records are invisible entirely, not "one integer".

## Source rule — what counts as a prior track record, and why it needs no threshold

A cap would be a picked number, and this repo fixes such things by construction or not at all. The
construction comes from what the two tables mean:

| table | written by | cadence |
|---|---|---|
| `strategy_scan_watermark` | `strategy_signal_scan` | one row per scan-day, per version — **unbounded** over time |
| `strategy_results_store` | `result_ledger` only (`app/services/result_ledger.py:460`) | one batch per deliberate, charged, #2600-registered trial |

So:

- **A prior version appears in `prior_versions` when it holds ≥1 stored result row.** That is what
  "track record" means — evidence someone paid a trial-register charge to produce. The set is
  bounded by deliberate runs, not by merges, which is the bound the payload needs and the reason
  no N has to be chosen. Measured today: exactly 1 prior results-bearing version per strategy.
- **A watermark-only version is NOT a track record.** It scanned and produced no evidence. It is
  still load-bearing for scope 2's copy, which is about the *scan*, so it is carried in the
  rotation block instead. ⚠ The first implementation admitted them (`set(counts) | set(scans)`),
  which contradicted this rule and reintroduced exactly the unbounded growth it exists to avoid —
  caught at Codex checkpoint 2. `scan_rows` only ENRICHES a result-bearing version.

## ⚠ Scope 1 narrowed, on measurement — a pointer, not a metrics splice

The issue asks for prior-version track records *"beside the current version"*. Measured on the
full population (324 rows, every `(strategy_id, strategy_version)` group), that cannot be shown as
comparable numbers, and two independent facts say so:

**1. No prior version is on the current measurement basis today.** ⚠ That is a property of the
data at this moment, not of rotation — immediately after a registry-only merge the version just
replaced IS comparable, so the flag is computed per version and never assumed. Grouping every results row by its
identity pins: 128 of 324 match today's constants, and those 128 are exactly the four CURRENT
versions. Every prior version differs on at least `cost_model_id`
(`static-p75-insession-v1` vs `v2+split-adjusted-max`) and `return_basis`
(`raw-close-price-return-v1` vs `split-dividend-adjusted-wealth-v1`); most also differ on
`position_rule_set_version` and `outcome_rule_set_version`, and some on `benchmark_rule` and
`namespace`. Those pins ARE `ResultIdentity`. Rendering an old expectancy beside a new one is the
cross-basis splice the repo forbids, and it would be least visible exactly where it misleads most.

**2. `promotion_refusals` cannot be reconstructed for a historical row.** It is computed at read
time by `_promotion_refusals` from today's gate, and the values that were true when the row was
written are not stored. Reusing `ResultArm` verbatim would therefore re-judge history under rules
it never faced (Codex checkpoint 1).

So `prior_versions` carries **no `ResultArm`**. It answers "where did my track record go?" — which
version, how many stored results, when it last scanned, and *why its numbers are not shown* —
naming the pins that differ. This is the same refusal-state posture #2602 item 5 fixes for
benchmark fields: name the refusal, never substitute. It also removes the response-size question
(the payload is O(1) per prior version, not O(arms)).

## Rotation state — CONSUME scope 3's verdict, do not re-derive

The first draft of this spec said "reuse" and then proposed a second derivation. That is the drift
it claimed to avoid, and Codex flagged it.

`app/services/strategy_scan_freshness.py` (#2624 scope 3, merged `b83c253e`) already owns the
rotation model: a `current` vs `fallback` watermark basis, and the `rotated_awaiting_scan` /
`rotated_scan_overdue` statuses.

`get_strategy_overview` therefore calls `check_scan_freshness(conn)` — the same function
`/system/status` uses — and maps its verdict onto `ScanHealth`. One rotation model, two surfaces.
Concretely, `basis == "fallback"` IS the rotation signal (the current version has no watermark and
some prior version does), and scope 3 already fixes the selection rule: the greatest frontier date,
which is what `assess_scan_freshness` computes into `newest_by_strategy`.

⚠ Two scan-state models exist and this does not merge them: `ScanHealth.status` grades the frontier
against `research_price_series`' max bar, while `check_scan_freshness` grades it in trading days
against `price_daily` with a lag tolerance. Merging them is out of scope here and is NOT done
silently — `ScanHealth` gains `rotated` only, and the divergence is named at the call site.

## Payload

```python
class ScanRotation(BaseModel):
    previous_version: str
    previous_frontier_date: date | None
    previous_scanned_at: datetime | None

class ScanHealth(BaseModel):
    status: Literal["never_run", "rotated", "current", "stale"]   # "rotated" is new
    rotation: ScanRotation | None                                  # non-null iff status == "rotated"
    ...unchanged

class PriorVersionTrackRecord(BaseModel):
    strategy_version: str
    result_count: int                      # stored rows under this version, any basis
    last_scan_frontier_date: date | None
    last_scan_at: datetime | None
    comparable: bool                       # per version; false for all of today's
    incomparable_reasons: list[str]        # the identity pins that differ, sorted

class StrategyOverview(BaseModel):
    prior_versions: list[PriorVersionTrackRecord]   # newest activity first
    ...unchanged
```

Ordering is `(last_scan_at, strategy_version)` descending — `strategy_version` breaks the tie so
the list is deterministic when two versions share a scan time or neither ever scanned.

## Render (scope 2)

Per control card, when `scan.status == "rotated"`:

> Version rotated — scanned through {previous_frontier_date} under the previous version. New track
> record starting.

and never "never run". The frontier date is nullable in the type but cannot be null in this branch
(a rotation is *defined* by a prior watermark existing); the copy still degrades to "under the
previous version" rather than interpolating `null`.

When `prior_versions` is non-empty the card gains a **Previous versions** block: one line per prior
version with its short hash, stored-result count, last scan date, and the reason its numbers are
not shown. ⚠ The rotated copy does NOT promise "previous track record below" — a rotation is
defined by a watermark, and a watermark-only prior version contributes no `prior_versions` entry.
The two blocks are independent and each renders on its own condition.

⚠ The `scan` block is currently **not rendered anywhere in `frontend/src`** — it is typed in
`api/types.ts:2456` and never read. Scope 2 is therefore the first render of it, not an edit to an
existing one.

## Also fixed here (the 09:01 triage's parked finding)

`StrategiesPage.tsx:50` — `primaryEvidence()` matches `window.window_id === "primary"`. No such id
exists: the declared ids are `primary-2022-plus`, `rolling-36m`, `rolling-24m`, `year-2022`,
`year-2023`, `year-2024`, `year-2025`, `year-2026-ytd`. This was the denominator
at the time of this document; #2721's activation later removes the post-archive
2025/2026 members because Intrader is frozen at 2024-09-27 (see
`2026-08-15-2721-survivorship-free-activation.md`).
(`app/services/strategy_recent_evidence.py:29`). The first branch is dead, so the function always
falls through to "first window with `status === "complete"`". That is order-dependent and silently
picks a **different window** whenever the primary window is `partial` while a calendar-year window
is `complete` — the headline "Expected / trade" then describes 2022 alone while the label says
primary. Fixed to the declared id.

## Acceptance

1. `/strategies/overview` carries `prior_versions` and `scan.rotation`, verified against dev.
2. A simulated rotation — a current version with **no watermark** while a prior version has one —
   reports `status="rotated"` with the previous frontier date, not `never_run`. ⚠ Rotation is
   defined by the WATERMARK, not by stored results: a version can have results and no watermark,
   or a watermark and no results, and those are independent states.
2a. `rotation` is non-null **iff** `status == "rotated"`, asserted as an invariant test.
2b. Prior-version rows change no current-version figure: evidence completeness, allocation
   readiness, attribution, P&L and the promotion refusals are asserted unchanged.
3. `/strategies` renders the rotated copy and the previous-track-record block; screenshotted,
   scrolled to the bottom, with `scrollHeight` vs `innerHeight` checked on a 1400px viewport.
4. `primaryEvidence()` resolves the declared id.
