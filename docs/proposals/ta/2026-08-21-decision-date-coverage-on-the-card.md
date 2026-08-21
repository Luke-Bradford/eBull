# Decision-date coverage on the strategy card (#2811)

## The corrected root cause

The issue proposes item 1 as *"why was 2026-08-03 skipped while 07-31 and 08-04 were
written"*, and its follow-up comment frames it as a caught-up window omitting one
interior bar. **Both are wrong, and the shape of the error matters more than the fact.**

`signal_bar_date` is **per instrument**, not a per-scan calendar. `write_window_indices`
bounds a cold start to the last bar *strictly before the frontier* **in that instrument's
own date sequence**. A sparse series therefore contributes an older date than a dense one,
and N sparse names manufacture N distinct `signal_bar_date` values that read as scanned
days. Measured on dev, 2026-08-21 — every census bar date before 08-06 is one instrument:

| symbol | bars ≥ 2026-07-24 | census date it manufactured |
| --- | --- | --- |
| JTKWY | 07-28, 08-07 | 2026-07-28 |
| MARK | 07-30, 07-31, 08-07 | 2026-07-31 |
| SICP | 07-24, 07-28, 07-29, 07-30, **08-04**, 08-07, 08-18, 08-19 | 2026-08-04 |
| TRVN | 07-29, **08-05**, 08-07 | 2026-08-05 |

Each carries `row_count = 1`, all written by the cold start at 08-09 14:43 against
frontier 2026-08-07. So the "hole between two covered days" is not one: 07-31 and 08-04
were never *scanned* days, they are MARK's and SICP's last pre-frontier bars.

Full population — every census bar date ever written, against the `price_daily` calendar
and the `strategy_signal_scan` run history:

```
date         dow    bars  strat  rows      obs        note
2026-07-20..27       (7 dates)   0     0        0      before any scan run
2026-07-28   Tue   11028      4     7        7        JTKWY only
2026-07-29   Wed   11018      0     0        0
2026-07-30   Thu   10971      0     0        0
2026-07-31   Fri   10965      4     7        7        MARK only
2026-08-03   Mon   11014      0     0        0    <-- S-2/S-10's only rebalance date
2026-08-04   Tue   10965      4     7        7        SICP only
2026-08-05   Wed   11016      4     7        7        TRVN only
2026-08-06   Thu   11042      4    26    40453    <-- first real scanned day
2026-08-10   Mon   11036      4    22    34734
2026-08-11   Tue   10757     10    41    34939        S-5..S-10 join
2026-08-12 .. 2026-08-18     10  16-92  126-145525
```

The first `strategy_signal_scan` run of any kind is **2026-08-09 06:45** (`job_runs`, 13
runs total). 2026-08-03 predates every scan, and `write_window_indices` bounds a cold
start to one bar per instrument, so no run has ever had 08-03 in a window.

⚠ There is deliberately **no claim of a contiguous "left edge"**. A cold start writes each
instrument's last pre-frontier bar, which can be arbitrarily old, so the set of written
dates is not an interval and must not be reasoned about as one. That is precisely the
error this section corrects, and restating it as "coverage starts at 08-06" would repeat
it one sentence later.

## Source rule

The decision cadence is not inferred here. It is the producer's own:

- `strategy_manifest.StrategyEntry.decision_calendar` — the `DecisionCalendar` protocol,
  already declared per strategy, returning `frozenset[date] | None`, where **`None` means
  "no calendar", not "no dates"** (its own docstring keeps that distinction).
- `strategy_signal_scan.run_signal_scan` builds
  `calendar = load_union_calendar(conn, sorted(spans))`, then
  `plan_dates = plan.entry.decision_calendar(calendar)`, per plan and deliberately not
  shared between plans.

Measured with that exact chain on dev, 2026-08-21: S-2 and S-10 each hold **73** decision
dates, the last three **2026-06-01, 2026-07-01, 2026-08-03**. Both are monthly
(`s2_cross_sectional_momentum.rebalance_dates`,
`s10_relative_strength_leader.s10_rebalance_dates`).

### What is NOT changing, and why

`strategy_registry.py:643` rule 3 — *"a non-decision bar is `not_fired`"* — stays. It is
settled, with its rationale on `s2_member`: *"THE PRICE FLOOR IS AN ELIGIBILITY RULE, NOT
AN EVALUABILITY ONE … calling that `not_evaluable` would inflate the refusal counts
criterion 9 reads with bars that were judged perfectly well."* Minting a `not_evaluable`
code for a non-decision bar would reverse that. The ledger is right; the **reader**
conflates the two.

## The defect, stated precisely

`_FIRE_RATE_SQL` sums `row_count FILTER (WHERE verdict IN ('fired','not_fired'))` over
**every** scanned bar date into `evaluable_entry_decisions`. For a periodic strategy that
denominator is dominated by bars the strategy never judged, so a version that has not seen
a decision date reports `0 / N = 0.0000` with `share_unavailable_reason = null` — a
confident measured zero standing in for a non-measurement. `docs/review-prevention-log.md`
already carries the lesson from #2797 — *"When a cadence is coarser than daily, check the
version's expected lifetime against it before concluding a zero is the market talking"* —
but only as a note to a human. This mechanises it.

## Why the calendar is STORED, not recomputed at read time

The obvious read-layer fix — have the card call
`load_validated_universe → load_union_calendar → decision_calendar` per request — was
specced first and rejected on two independent grounds.

1. **It is a reconstruction, not the producer's value.** It resolves every historical
   `strategy_version` against *today's* universe, corpus and quarantine state, none of
   which is what that version's census rows were written under. A backfilled bar landing
   earlier in a month silently reclassifies which date was "first of the month" without
   any census row or version changing. That is the #2809 shape exactly — the card
   recomputing a lookalike of a producer statistic and disagreeing with it.
2. **It costs more than the endpoint.** Measured on dev: `/strategies/overview` answers in
   **75-146 ms**; the union calendar alone is **0.61 s**, and the cost is intrinsic
   (`SELECT DISTINCT price_date` with no join or filter is 1.02 s; the coverage-joined
   form over all instruments 0.36 s). Date-bounding does not rescue it — a one-year bound
   is still 0.56 s — and would need a lookback constant long enough for the coarsest rule
   any future `DecisionCalendar` may declare, which nothing sources.

So the scan **writes the calendar it actually used**, and the card reads it. This is
retroactive in the way that matters: a decision calendar is a function of the corpus, not
of scan coverage, so one scan run publishes all 73 of S-2's historical dates at once and
the card is immediately right about census rows written days earlier. That is what
separates this from stamping decision-ness onto each census row, which could only ever
describe rows written after the change.

## Change

### Schema — `sql/362_strategy_decision_calendar.sql`

```
strategy_decision_calendar(strategy_id, strategy_version, decision_date, computed_at)
PRIMARY KEY (strategy_id, strategy_version, decision_date)
```

Keyed on `strategy_version`, not `strategy_id` alone: the rebalance rule is part of the
strategy's identity and #2797 changed S-2's, so one calendar per strategy would mix two
rules. A version with no rows is a version whose calendar we do not know — which is
distinct from one with a known empty calendar, and the reader keeps them distinct.

### Producer — `strategy_signal_scan`

Inside the existing per-strategy transaction, replace that `(strategy_id,
strategy_version)`'s calendar rows with the `plan_dates` it just computed. Only
cross-sectional plans have one; per-series plans write nothing, and their absence is read
as "no calendar" rather than "unknown", because the manifest is authoritative on which
class a *current* strategy is.

### Reader — `strategy_monitoring`

1. `_FIRE_RATE_SQL` also groups by `signal_bar_date`, so rows can be partitioned by
   decision-date membership. Its aggregates are unchanged.
2. `load_fire_rate` left-joins the stored calendar for the versions it was asked for.
3. A census bar date counts as a **decision day** only if it is in the stored calendar
   **and** carries at least `min_participants` evaluable entry rows. The second half is
   not a threshold this spec invents: `StrategyEntry.min_participants` is the declared
   panel floor (`MIN_CROSS_SECTION = 10` for S-2), and below it the strategy refuses with
   `thin_cross_section` and *cannot* fire. Without it a single sparse instrument whose
   last pre-frontier bar happened to fall on 2026-09-01 would set `decision_days = 1` and
   re-enable exactly the fake share this ticket exists to remove — the same
   one-instrument-looks-like-a-day error as the root cause above, one layer up.
4. `evaluable_entry_decisions`, `not_evaluable_entry_decisions`, `fired_entry_signals` and
   `fired_days` are all folded over the decision-day partition, so the share is the ratio
   of the two numbers rendered beside it (#2802's one-population rule) and the weekly rate
   is measured on the same population as the share.
5. Fires are *unrepresentable* off-calendar under registry rule 3, so any that appear are
   a producer-invariant violation, not a rounding concern. They are counted and
   `logger.warning`-ed naming strategy, version and dates — dropped silently is how a
   corruption stays invisible.
6. `derive_fire_rate` gains `decision_days: int | None`, using the protocol's own
   convention: `None` = no calendar known (behaviour identical to today), `0` = a calendar
   is known and none of it has been covered.
7. `scanned_days` stays **coverage** and is deliberately not restricted: "how much corpus
   this version saw" and "how many chances it had" are different questions and the card
   shows both.

Invariants, asserted rather than documented: `fired_days ≤ decision_days ≤ scanned_days`
and `fired_entry_signals ≤ evaluable_entry_decisions`.

### Reason enum

`ShareUnavailableReason` gains `no_decision_date_scanned`, ordered after `never_scanned`
(absent census rows never reach `derive_fire_rate`) and before `no_evaluable_decisions`.
API response model, `frontend/src/api/types.ts` and `SHARE_UNAVAILABLE_LABELS` gain the
member; the frontend `Record<>` keyed on the union makes the label non-optional at
typecheck.

## Acceptance

- `/strategies` renders S-2 and S-10 as "No decision date scanned" rather than `0.00%`.
- S-1/S-3/S-4/S-5..S-9 (per-series, no calendar) render exactly as before — same share,
  same denominator, byte-identical response apart from the new field.
- A full-population check that the stored calendar equals the in-process one for every
  current version, and that no current census row carries an off-calendar fire.

⚠ The 2026-09-01 opportunity is **conditional**, not scheduled: it requires that the
version still exists, that a scan runs with a frontier past it, and that the date clears
`min_participants`. The card will say which of those is true; this spec does not promise
the date.

## Out of scope

Backfilling 2026-08-03. The no-backfill rule in `write_window_indices` is settled and its
look-ahead reason still holds.
