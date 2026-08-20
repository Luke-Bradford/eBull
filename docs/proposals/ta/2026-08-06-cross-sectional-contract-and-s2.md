# Cross-sectional strategy contract + S-2 (12-1 momentum)

Parent: `docs/proposals/ta/strategy-catalogue-and-backtest-validity.md` §4 (S-2),
§3.5, §4.0, §9 Q2/Q3, §5 criteria 1, 8, 9, 11. Registry:
`app/services/strategy_registry.py` (phase 3a). Ledger: `sql/255`, `sql/260`,
`app/services/signal_ledger.py` (3b/3c). Refs #2240, #2288, #2289.

## Why a design step at all

Phase 3a's contract is **one pure function per series**: `evaluate(body, inputs,
n_bars)` runs a per-bar predicate over one instrument. S-1, S-3 and S-4 fit it
because their rules read only that instrument's own bars.

S-2 does not. *"Hold the top decile"* is a statement about the cross-section on a
date, so the verdict for instrument A at date D depends on B..Z at D. Nothing in
3a can express that, and the three guarantees 3a buys — evaluability decided
before the condition runs, no fill expressible, closed reason vocabulary — must
survive the extension rather than be re-argued per strategy.

## Source rule

| decision | governing rule | where it comes from |
| --- | --- | --- |
| ranking window | **prior (2-12) returns** — cumulate eleven months, skip the most recent | Fama-French momentum factor construction, [Ken French data library](https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/Data_Library/det_mom_factor.html) (fetched 2026-08-06); Jegadeesh & Titman (1993) |
| in bars | `close(t-21) / close(t-252) - 1` | parent §4 verbatim: *"return over `t-252 .. t-21`"* — the 11-month/1-skip form above |
| rebalance trigger | first bar whose calendar month differs from the previous bar's | parent §4 verbatim, and it is the causal form: the last session of a month is not knowable at that session |
| price basis | **price returns, not total returns** | parent §4; and `research_price_daily.close` is the SPLIT-adjusted close consistent with OHLC, while the dividend-adjusted series is `adj_close` (`sql/251`). Reading `close` is spec-conformant, not convenient |
| eligibility | ≥273 bars **and** `close ≥ $1`, both as-of the decision date | parent §9 Q3's recommendation + §3.5 rule 5 |
| decile cut, tie-break, thin cross-section | **no published rule** — fixed **by construction** below | §4 gives "top decile" and nothing else. Same posture as S-4's "bottom quartile" |

⚠ **What is NOT borrowed from Fama-French.** FF sorts on NYSE breakpoints, within
size buckets, value-weighted, on monthly returns. S-2 ranks every eligible name in
the §4.0 validated universe equally, on daily bars, at a plain top-decile cut. The
*window* is theirs; the portfolio construction is §4's. Citing the factor for the
window invites assuming the rest, so the departures are written down.

⚠ **The parent's two numbers do not agree, and both are honoured.** The window
needs 253 bars (index `t-252` must exist); the stated eligibility is *"≥273 bars
of history at that date"*. 273 = 252 + 21, i.e. it was computed as though the
window ran `t-273 .. t-21`. Taking the window literally (which is also the
published form) and the eligibility literally is the only reading under which
neither sentence is contradicted, so both ship: score from `t-252`, refuse until
273 bars, first scored index 272. It is a 20-bar-per-series **narrowing** —
**99,469 bars** over the validated universe, measured, not asserted harmless.

## §9 Q3's price floor — an open question, answered by taking its recommendation

§9 Q3 is *open*, with a recommendation: *"≥273 bars and close ≥ $1, both evaluated
as-of each decision date"*, on the evidence that #2266 measured sub-$1 names
running to 800× p99.99 daily moves on tick quantisation alone. That
recommendation ships, because the alternative is worse than usual **here
specifically**: a ranked strategy selects on extremes, so a tick-quantised penny
name is not a rare contaminant of the top decile — it *is* the top decile. It is
hashed into the identity, so reversing it later is a new strategy version rather
than a silent redefinition. Measured cost: **31,746 decision bars rejected** — bars the floor alone excludes, i.e. ones that were otherwise evaluable and on a rebalance date.

⚠ **On split-adjusted closes this is an adjusted-price floor, not a nominal one,
and the deviation runs one way.** A name that traded at $0.20 and later did a
1-for-10 reverse split appears at $2.00 in these bars and passes a floor it would
have failed at the time — and reverse splits happen *because* a price fell under
$1, so the names it lets through are exactly the distressed ones. Unadjusting
needs per-series split factors the corpus does not store (`sql/251`). Stated
rather than quietly enjoyed.

## The contract extension

Added to `strategy_registry.py`, not to a new module — the fill rule and the
reason vocabulary live there and a second home for either is how they drift.

```python
@dataclass(frozen=True)
class CrossSectionalMember:
    dates: tuple[date, ...]           # this member's own bar dates
    inputs: tuple[StrategyInput, ...] # 3a evaluability, unchanged
    score: IndicatorSeries            # the ranking statistic, per bar
    decision_indices: frozenset[int]  # bars at which this member ranks

def stage_cross_sectional_member(member, *, kind) -> StagedMember
def evaluate_cross_sectional(*, members: Mapping[int, CrossSectionalMember],
                             select, min_participants: int, kind) -> dict[int, list[StrategySignal]]
```

Per member, per bar, in order:

1. last bar → `not_evaluable(no_fill_bar)` (3a, unchanged — no `t+1`);
2. `_unevaluable_reason_at(inputs, i)` → `not_evaluable(reason)` (3a, unchanged);
3. not a decision bar → `not_fired`. The rule is *"fire iff a decision bar and
   selected"*, so a non-decision bar did not fire. It is a verdict, not an absence;
4. otherwise the member **participates** at `dates[i]`.

Participants are grouped **by date, never by index** — index `i` is a different
date on every member. `select` is then called once per date with `{instrument_id:
score}` and returns the winners; selected → `fired`, participating and not
selected → `not_fired`.

Validated at construction, because each of these was a hole Codex found at
checkpoint 1: the score must be **one of the declared inputs** (otherwise
`_unevaluable_reason_at` passes a bar whose score is `None` and the member is
ranked on a value it does not have); every input and the score must be the same
length as `dates`; dates must be strictly ascending; decision indices must be in
range. `select` returning a key that did not participate **raises** — ignoring it
would hide a selector bug and honouring it would fire on a bar already judged
unevaluable.

What survives from 3a:

- **Evaluability precedes the condition.** `select` never sees a member whose
  inputs were unevaluable at that bar.
- **No fill is expressible.** `select` receives a date and scores; it cannot name
  a bar, a price or a fill. ⚠ That is a narrower claim than "look-ahead is
  impossible" — `select` is ordinary code and could close over anything. What is
  structural is that every score reaching it is a causal per-bar value and the
  runner hands it no route to the future.
- **The vocabulary stays closed** — one Python `Literal`, mirrored by `sql/260`.

⚠ **`stage_cross_sectional_member` is public on purpose.** A full-corpus census
cannot hold 5,266 members' bars at once, so it stages one series at a time and
keeps only the per-date scores. Without the split, the census would re-implement
the staging pass — and a census that re-implements the strategy it measures can
agree with nothing.

## Decisions fixed by construction (no published rule exists)

1. **Rebalance calendar is panel-level.** `D` is a rebalance date iff `month(D) ≠
   month(previous date in the panel's union calendar)`. The first date in the
   calendar is not one.
   ⚠ The parent's wording is per-series. Read that way, a name resuming after a
   halt on the 4th ranks against whoever else resumed that day — a cross-section
   of two. The panel calendar is the same rule evaluated on the panel and is
   equally causal; it is written down here because it is a reading, not a
   quotation. On the validated universe it yields **774 rebalance dates** over a
   16,236-date calendar (762 of them with any participant).
2. **`k = N // 10`**, floor: the largest whole number of names not exceeding 10%.
3. **Tie-break: score descending, then instrument id ascending.** ⚠ Exact ties are
   NOT impossible — equal endpoint pairs are all it takes, and the corpus is full
   of low-priced names quantised onto the same ticks. Measured: the cut lands on a
   tie on **5 of 762** rebalance dates, so the rule is load-bearing rather than
   decorative.
4. **`N < 10` → every participant is `not_evaluable(thin_cross_section)`**, a
   ninth reason code (`sql/260`). A decile of six names is undefined; `k =
   max(1, N//10)` silently becomes "best of six", and `k = N//10 → 0` reporting
   `not_fired` is criterion 8's exact prohibition — a data-availability fact
   wearing a rule verdict's clothes.
   ⚠ **Measured: it never fires on today's validated universe** — the smallest
   cross-section is 18. It ships anyway, fixture-covered and probed, because the
   rule has to be right before the panel narrows (a sector sleeve, a smaller
   universe, an earlier corpus), and because the alternative is silent.

## One leg, not two

Entry only. *"Hold the top decile"* makes the exit the **exact complement of the
entry over the participants at a rebalance bar**, so an exit row could never
disagree with the entry row beside it — a second copy of one fact, on a ledger
whose key would then carry both. (The complement claim is about participants; a
member that is unevaluable or has no bar that day carries no exit information
either.) S-1's and S-3's exits are not complements of their entries — both legs
can be false on the same bar — which is why they have two.

⚠ Consequences, stated rather than left to be discovered:

- pairing an entry with the rebalance that ends it, **and collapsing a name
  selected in consecutive months into one hold rather than two entries**, is phase
  5's;
- S-2 declares **no** `max_hold_bars`. Its hold is *"until the next rebalance"* — a
  calendar fact, not a bar count — so the phase-4 resolver's fixed-bar machinery
  does not apply to it as-is. Approximating it as 21 bars would be inventing a
  parameter §4 does not give.

## What the measurements say (2026-08-06, full population)

`scripts/verify_2240_s2_cross_sectional.py`, three arms. Every figure below was
computed by that script on this branch; none is carried over from a prior run.

Stamped `s2-cross-sectional-momentum` at **`strategy-registry-v1+9ec1890a1b4f`**.
⚠ Four different stamps printed the SAME figures during this branch
(`…+4fd0dc03925c` → `…+d50ee9e5152a` → `…+6624b41cf98d` → this one): #2333 merged
mid-branch and put `indicator_series.RULE_SET_VERSION` inside `strategy_version`,
so #2311's vectorisation moved it once; then this migration was renumbered twice
(`sql/258` → `259` → `260`, as #2340 and #2232 landed their own), and each rename
edited a registry comment. **Every figure below is identical across all four** — S-2 reads no indicator series, only bar closes. That is the
deliberate over-invalidation the identity is built on: it moves whenever anything
it hashes moves, whether or not the verdicts do, so stored signals go visibly
stale rather than silently mixed.


| arm | population | result |
| --- | --- | --- |
| `--census` | §4.0 validated universe, masked bars: 5,266 series / 23,339,583 bars | fired 101,318 (0.434%) · not_fired 21,833,866 · not_evaluable 1,404,399 (warm-up 1,398,958 · no_fill_bar 5,266 · quarantined_bar 175) |
| `--equivalence` | both corpora, raw bars | the 12-1 score vs `lag(close, 21)` / `lag(close, 252)` |
| `--equivalence` result | 25,818,944 research bars + 6,711,834 `price_daily` bars | **0 mismatches, 0 ties** on both |
| `--ranking` | validated universe, masked | the decile cut re-derived end to end in SQL — masking, eligibility, price floor, month boundary, `row_number()` — compared set-for-set per rebalance date: **762 dates, 101,318 picks, 0 mismatched dates** |

Narrowings and refusals, each counted rather than argued away: eligibility gap
**99,469** bars · price floor **31,746** decision bars · masked decision bars
**3** · listed-but-silent member/date pairs **218** (a member with no bar on a
rebalance date cannot participate, which quietly shrinks `N`) · boundary ties
**5** · thin cross-sections **0**.

⚠ **The narrowing counters are "uniquely rejected by this gate", and the first
draft was not** (Codex, checkpoint 2). A bar in the 20-bar eligibility gap whose
lookback close is also masked, or a sub-$1 rebalance bar still inside warm-up,
would have been refused either way. Requiring the bar to be otherwise evaluable
moved the floor's cost from 35,740 to **31,746** and the eligibility gap's from
99,473 to **99,469** — an 11% overstatement on the floor. A census that
overstates a rejection is the same defect as one that hides it.

⚠ **The ranking arm failed first, and it was the ARM that was wrong.** Its
initial form ranked on `series_id` and mapped to `instrument_id` afterwards, so
its tie-break was on a surrogate key rather than on the rule's. It disagreed with
the module on exactly the two rebalance dates whose decile cut lands on an
**exact** tie — 1989-01-03 and 1999-10-01, scores of 0.5 and 1.0 in *both*
arithmetics, so not float drift. **A re-derivation has to re-derive the tie-break
KEY, not just the ordering statistic**; ordering on a surrogate id is a different
rule that agrees almost everywhere.

⚠ The non-positive-close guard is not hypothetical: `research_price_daily` has
**2** bars with `close <= 0` (both already quarantined, so the masked loader never
shows them) and `price_daily` has **154**, which the raw-bar `--equivalence` arm
does reach. A zero denominator raises; a negative one returns a sign-flipped
number that ranks like a winner.

## What this does NOT claim

No performance number. §4's table grades S-2's omission bias **high** and it is
the **only** strategy with rank contamination — a delisted loser missing from the
corpus promotes a survivor into the top decile that never belonged there.
Criterion 1 needs point-in-time **listing** membership, which for US stocks is
*reconstructable* (corpus first bar + the Form 25 register) rather than available,
and is not reconstructed yet; #2284's purchase is necessary and not sufficient.
Every row this produces is labelled `universe = 'survivor_only'` (#2288).
