# ARM A (#2834) — tilt-ETF **cost-bar** verdict: declaration + shared rule

Status: spec. Refs #2834, #2833, #2832, #2829.

⚠ **Title wording is deliberate.** This produces a COST-BAR verdict, not an
executability proof. Passing says the p75 full round-trip spread is under the declared bar
on a proved real-long-x1 product. It does not prove the order path will accept an LSE
listing (session handling, minimum size, venue hours are all unchecked here).

## 1. Why now

#2834's ARM A step 3 is *"wait five trading days, run the half-spread percentile query from
#2833's handoff against `(15445, 15446, 14465)`, add the documented GBP→USD conversion fee,
compare against the ≤0.50% bar."* The clock started at `13e00540` (2026-08-23).

Measured 2026-09-16T02:5xZ on `strategy_core_quote_observations`, dates common to all three
candidates (reproducible query in §9):

```
2026-08-24 | 3
2026-08-25 | 3
2026-08-26 | 3
2026-09-14 | 3
2026-09-15 | 3
```

The clock has closed and the ticket still carries *"ARM A … remains on the five-trading-day
quote clock"*. **But there is no declaration and no verifier for ARM A at all**, so the clock
closing changes nothing on its own: nothing can read it out.

## 2. Two facts the date count hides — both stated before any rule is chosen

1. **A 16-day hole (2026-08-27 → 2026-09-11 inclusive).** `job_runs` retains zero rows for
   *every* job across those dates (2,313 on 08-24, 183 on 09-12). ⚠ That is the
   measurement; it does not by itself prove a daemon outage rather than retention or an
   environment difference. Either way the five dates are two clusters 23 calendar days apart.
2. **It is five only under a boundary of 2026-08-24.** #2833's sealed declaration sets
   `evidence_not_before = 2026-08-25T00:00:00Z`; under that boundary ARM A has **four**.

**No spread statistic has been read.** Everything above is date/status cardinality.

## 3. ⚠⚠ This is a POST-HOC declaration and is labelled as one

ARM A's observations began 2026-08-24. A declaration written today is **after** the data
exists, and no amount of conservatism converts that into a preregistration. Saying otherwise
would be the #2599 defect (*a declaration passed as an argument cannot prove it was made
before the look*) wearing a date.

**What is actually claimed, and nothing beyond it:**

- No ARM A spread statistic has been read by any session. The verifier refuses to compute one
  before the boundary, and this spec was written from date/status counts only.
- Every threshold and method field is **copied** from an artefact that predates the first ARM A
  observation (§4). There is no field whose value this session picked.
- Where two boundaries were available, the **stricter already-frozen** one is taken
  (`2026-08-25T00:00:00Z`), which is the one that does *not* permit a readout today.

**What is NOT claimed:** that this satisfies #2829's *declaration freezes before first look*.
It does not and cannot, retroactively.

**Why that is tolerable here, stated rather than assumed:** ARM A's claim is a cost
measurement, not a return hypothesis. It charges no trial-register budget and asserts no
alpha. Once the percentile, population and missingness rules are fixed — and all three are
inherited verbatim — `p75(spread_bps) ≤ 50` has no researcher degree of freedom left. The
one genuinely new field is `verdict_mode` (§5), which is a reporting shape and cannot make a
failing candidate pass.

## 4. Source rule — where each declared field comes from

| field | value | frozen where, when |
| --- | --- | --- |
| `candidate_ids` | `[14465, 15445, 15446]` | #2834 comment `ARM A step 1`, 2026-08-22 (amended set: R1VL.L / IUMO.L / IUQA.L) |
| `pass_bar_bps` | `"50"` | #2834 body, 2026-08-22 (*"all-in one-off cost ≤ 0.50%/instrument"*) |
| `evidence_not_before` | `2026-08-25T00:00:00Z` | #2833 declaration, sha `5f3929e0…`, 2026-08-24 |
| `required_common_utc_dates` | `5` | #2834 body + #2833 declaration |
| `binding_percentile` `0.75`, `descriptive_percentile` `0.50`, `percentile_method`, `population_rule`, `missingness_rule`, `completion_rule`, `fx_rule`, `spread_metric`, `all_in_cost_rule`, `eligibility_*` | verbatim | #2833 declaration |

⚠ Issue bodies and comments are mutable. The two #2834-sourced values are additionally
pinned by the measurement artefacts they were produced from (the eligibility proof table rows
for the amended candidate set), and the #2833-sourced values by that declaration's sha256.

### 4.1 Half-spread vs full spread — a factor of two, resolved by inheritance not by choice

ARM A step 3 says *"half-spread"*; #2833's `spread_metric` is *"the full bid-ask spread
divided by midpoint"* and its `all_in_cost_rule` is `p75_full_round_trip_spread_bps`. That is
a 2× difference and it must not be settled by preference.

It is settled by #2833's own body, which uses **the same two words in the same order**:
*"2. Measure effective half-spread from ~5 trading days of stored quote snapshots… 3. All-in
one-off entry cost = measured round-trip + documented GBP→USD conversion fee"*, under a bar
of *"All-in one-off cost ≤ 0.60%"*. #2833 resolved that phrasing to full round-trip against
60 bps and sealed it. ARM A step 3 explicitly points at *"the half-spread percentile query
from #2833's handoff"* — i.e. that query, which computes the full spread.

So ARM A charges the **full round-trip** spread against a **one-off** 50 bps bar. That is the
conservative direction (it overcharges by roughly 2×), and it is inherited rather than
selected. Stated here because a reader comparing the ticket's wording to the declaration will
otherwise find a silent 2× and be right to.

### 4.2 FX — the source rule is `sql/366`, not the ticket's instruction

⚠ **ARM A step 3's "add the documented GBP→USD conversion fee" is superseded by this
ticket's own 2026-08-23 measurement**, and the governing rule is already written down.

`sql/366_strategy_core_quote_observations.sql:37-50` (live-portal verified 2026-08-23) records
that `conversion_rate` is the mid of eToro's `conversionRateAsk`/`conversionRateBid` — *"the
conversion rate from the INSTRUMENT'S currency to USD"* — and that this is **the only
per-instrument denomination signal the API exposes**, because `get-instrument-display-data`
carries no currency field and `instruments.currency` is a VENUE lookup off
`exchanges.currency`. It further records the measurement: *"IUMO.L / IUQA.L / R1VL.L all
return EXACTLY 1.0 (USD-denominated) while stored as GBP; IUSA.L returns 0.0136315 (GBX)"*.

So `.L` does not mean GBP-denominated, and the fee is not added. #2833's frozen
`all_in_cost_rule` already declares *"documented entry-sizing conversion markup is 0 bps"*,
and its `fx_rule` **fails** any candidate whose included rows carry a rate other than 1. A
non-unit rate is a refusal, not a cost adjustment.

Three limitations, named rather than left to be found:

- ⚠ `conversion_rate = 1` is tested on a **mid** (`etoro.py:730` averages ask and bid). Unequal
  sides can average to exactly 1, so unit-mid is a necessary condition, not proof that no
  conversion spread exists. It is what the API exposes.
- ⚠ `NULL` means the provider omitted the rate — **not** USD (`sql/366` says so explicitly).
  The predicate must compare `= 1`, never treat NULL as passing.
- ⚠ The refusal protects legs this measurement does not model — closing conversion, dividend
  conversion, corporate actions — not the sizing leg, which the frozen rule already prices at
  0 bps. ARM A claims nothing about those legs.
- ⚠ Codex ckpt-1 offered eToro's published *"No FX mark up applies when converting non-USD
  denominated assets for unit amount purposes"* (Cost and Charges PDF). **Not cited as
  authority here** — `.claude/skills/data-sources/etoro-api.md` requires live-portal
  verification for any eToro capability claim and carries no such line. `sql/366` is the
  verified source and says the same thing for these instruments.

## 5. The one genuinely new field — `verdict_mode`

#2833 picks **one** core instrument (lowest p75, tie-break ascending `instrument_id`). ARM A
is not choosing between momentum, quality and value — a tilt sleeve wants all three, so
reporting a "winner" would be a misleading output.

- `"select_one"` — #2833's existing behaviour, unchanged.
- `"per_candidate"` — ARM A. Each candidate passes or fails on its own refusals. Outcome is
  `"pass"` when every candidate in the declaration passes, `"partial"` when some do, `"fail"`
  when none do. `passing_instrument_ids` is always emitted, sorted ascending, and
  `selected_instrument_id` / `selected_symbol` are absent in this mode.
- **An empty `candidate_ids` is a hard refusal in both modes**, never a vacuous "all pass".

⚠ #2833's frozen declaration has **no** `verdict_mode` key and its digest forbids adding one.
The default must therefore be keyed on identity, not on absence: `schema_version ==
"core-selection-2833-v1"` → `select_one`. **Any other `schema_version` must carry an explicit,
recognised `verdict_mode` or the run raises.** A blanket "missing means select_one" fallback
would silently reinterpret a future declaration, so it is barred.

## 6. Implementation — extract the rule, do not copy the script

`scripts/verify_2833_core_selection.py` (366 lines) already implements the whole rule. A
second near-copy is precisely `docs/review-prevention-log.md`'s *a hand-copied predicate has
no compiler*.

1. Move the rule to `scripts/_core_selection_rule.py`: the dataclasses, `percentile_cont`,
   `_common_dates`, `_population_for`, `evaluate`, the two SQL constants, `load_declaration`,
   `assert_verifier_sources_clean`, `run_verifier`. **Provenance (declaration path, sha,
   verifier path) becomes an argument** — it is identity, not rule. No mutable default
   carries a declaration, a path or a digest; the digest is bound explicitly on **every**
   return path of `evaluate`.
2. `verify_2833_core_selection.py` keeps its module constants and CLI and delegates, still
   exporting `load_declaration` / `evaluate` / `percentile_cont` with unchanged signatures.
   `tests/test_2833_core_selection_verdict.py` imports from it and is **not edited** — its
   continuing to pass unchanged is part of the equivalence evidence.
3. `scripts/verify_2834_arm_a_selection.py` is the second thin caller.

### 6.1 Three things that are currently baked in, not two

The earlier draft of this spec said two. It was wrong, and the extra one matters:

- the declaration **path and sha256** (module constants);
- the refusal label `"cost_above_60_bps"`, which **names** 60 while **reading** `pass_bar`
  from the declaration — two sources for one number, linked only by someone remembering;
- the **percentiles**: `evaluate` calls `percentile_cont(spreads, Decimal("0.50"))` and
  `Decimal("0.75")` as literals while the declaration carries `descriptive_percentile` and
  `binding_percentile` describing exactly those values. Same defect, one line up.

Fixes, each chosen so #2833's output is byte-identical:

- label → `f"cost_above_{declaration['pass_bar_bps']}_bps"`. `"60"` renders
  `cost_above_60_bps` exactly; ARM A's `"50"` renders `cost_above_50_bps`. ⚠ The label is
  declaration-faithful, so `"60.0"` would render `cost_above_60.0_bps` — stated rather than
  normalised, because normalising re-introduces a second source for the number.
- percentiles → read `binding_percentile` / `descriptive_percentile`. `"0.75"` / `"0.50"` are
  the literals already in the code.

### 6.2 A declaration the code does not implement must be REFUSED, not ignored

Most declared fields are prose the code implements but never reads (`population_rule`,
`missingness_rule`, `completion_rule`, `fx_rule`, `spread_metric`, `percentile_method`,
`all_in_cost_rule`, `eligibility_account_rule`). Today nothing stops a future declaration
stating a rule the code does not perform.

So the shared module pins those fields against the implemented rule and **raises** on any
divergence. ARM A's declaration therefore has to copy them verbatim or fail to load — which
is the property §3 leans on.

`selection_rule` and `no_pass_action` are excluded from that pin (they legitimately differ per
mode) and are validated against the mode instead.

### 6.3 Provenance must cover the shared module

`assert_verifier_sources_clean` currently diffs the declaration and the one verifier against
`HEAD`. With the rule extracted, un-committed edits to `_core_selection_rule.py` would produce
a verdict labelled with a clean `HEAD`. The check takes the **full set** of source paths, and
both callers pass `_core_selection_rule.py` in it.

### 6.4 ⚠⚠ #2833's verdict opens 2026-09-18, two days from now

Touching its verifier is a real hazard. Mitigations, all of them mechanical: the extraction is
a move; both behaviour changes render byte-identically for `pass_bar_bps = "60"`,
`binding_percentile = "0.75"`, `descriptive_percentile = "0.50"`; #2833's existing test file is
untouched; and §7 adds a differential test that runs both declarations through the shared rule
in one process.

## 7. Inherited limitations — stated, not fixed

These are #2833's sealed behaviour. Changing any of them two days before its verdict opens is
out of scope; a reader of ARM A's result needs to know them.

- **The first five common dates are permanent.** `common_dates[:required_dates]` takes the
  earliest five at or after the boundary. A date that is thin or internally broken consumes a
  slot and fails the candidate with `incomplete_population`; a later clean date cannot replace
  it. ⚠ Live for ARM A: 2026-08-26 carries only 24 observed rows across 8 instruments
  corpus-wide and is inside the window.
- **A common date needs only one observed row per candidate.** Five common dates are not five
  complete sessions; leading and trailing hours outside each day's first→last observed bucket
  are invisible to the missingness rule.
- **Readiness is shared.** One candidate that never accrues observations blocks the verdict for
  all of them, with no timeout and no terminal missing-evidence state.
- **No maximum elapsed span.** The inherited `completion_rule` has no gap limit, so ARM A's
  window legitimately spans 23 calendar days across the §2 hole. ⚠ Declaring a limit today,
  knowing the gap, would be exactly the tuning §3 avoids — so none is declared and the
  staleness is recorded as a weakness of the evidence instead.
- **Eligibility is read live, not pinned.** The proof query takes the latest proof under the
  current non-revoked credentials, with no age limit, so a re-run after a credential rotation
  can move the verdict. Observations and proofs are also read in separate statements with no
  snapshot pin, and the observation table's immutability trigger bars UPDATE but not DELETE or
  a backfilled INSERT, either of which could change which five dates are first.
- **"All-in" is the frozen rule's word, and its exclusions are real:** exit conversion,
  dividend conversion and corporate actions are unmeasured; the figure is one p75 over hourly
  bid/ask snapshots, not an observed open-plus-close cost.

## 8. Tests (pure logic, no DB) — `tests/test_2834_arm_a_selection_verdict.py`

- ARM A's frozen declaration digest is intact.
- **Differential**: both declarations evaluated through the shared rule in one process, each
  keeping its own `declaration_sha256`, `schema_version` and mode (cross-arm identity leak).
- `per_candidate`, all three under the bar → `"pass"`, all three in `passing_instrument_ids`,
  no `selected_instrument_id` key.
- one over → `"partial"`, the other two listed ascending, the failing one carrying
  `cost_above_50_bps` and **not** `cost_above_60_bps`.
- all three over → `"fail"`, `passing_instrument_ids` empty.
- `select_one` still names one `selected_instrument_id`; all-fail still yields `"cash"`.
- exactly-at-bar passes, one bps over fails (the boundary is `>`, not `>=`).
- fifth date sealed until the following `00:00Z`; pre-boundary rows excluded; a sixth common
  date does not displace the first five.
- `verdict_mode` missing on a non-2833 `schema_version` raises; an unrecognised mode raises;
  absent on `core-selection-2833-v1` resolves to `select_one`.
- a declaration whose pinned prose field diverges from the implemented rule raises.
- empty `candidate_ids` raises rather than reporting a vacuous pass.
- FX: `conversion_rate` NULL and non-unit both refuse `fx_unmodelled` on an otherwise cheap
  candidate; eligibility failure refuses on a cheap candidate.
- readiness payloads before five dates and before midnight contain **no** candidate metric.

## 9. Acceptance — with the query, so it is reproducible

```sql
-- run at a stated UTC time; the count moves as collection continues
WITH cd AS (
  SELECT o.instrument_id, (o.sample_bucket AT TIME ZONE 'UTC')::date AS d
  FROM strategy_core_quote_observations o
  WHERE o.instrument_id = ANY(ARRAY[14465,15445,15446])
    AND o.observation_status = 'observed'
    AND o.sample_bucket >= '2026-08-25T00:00:00Z'
  GROUP BY 1,2)
SELECT d, count(DISTINCT instrument_id) FROM cd GROUP BY d HAVING count(DISTINCT instrument_id) = 3 ORDER BY d;
```

- At 2026-09-16T02:5xZ this returns 4 rows (08-25, 08-26, 09-14, 09-15), each with 3 distinct
  candidates. `PYTHONPATH=. uv run python scripts/verify_2834_arm_a_selection.py` therefore
  prints `evidence_collecting` with `common_dates_observed: 4` and **no** candidate metric.
- After a fifth such date and the following `00:00Z`, the same command emits the verdict.
- `uv run pytest tests/test_2833_core_selection_verdict.py tests/test_2834_arm_a_selection_verdict.py`.

## 10. Out of scope

ARM B (blocked on the vectorized weighting prototype), #2833's verdict, the
`strategy_core_quote_observations` writer, any broker mutation, any capital allocation.
