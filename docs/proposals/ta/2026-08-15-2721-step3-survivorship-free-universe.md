# #2721 step 3 — the survivorship-free universe: vendor pin, termination wiring, the single identity move

Status: ckpt-1 reviewed 2026-08-15; this revision folds in every finding.
Refs #2721 (step 3), #2698, #2597, #2437. Parent:
`2026-08-07-bounded-backtester.md` §6, which promised exactly this: *"the
result row therefore carries the corpus version alongside the basis, and
`survivorship_free` is not a value any current corpus can produce"* — written
when the corpus was one survivor-only vendor. The corpus now carries
`icyDenev/Intrader` (22,879 series, 50.1M bars, survivorship-free, fully
quarantine-judged), Form 25 evidence linkage (#2728), and an unwired
termination rule (`app/services/series_termination.py`). This spec wires them.

## Source rules

- Termination realisation: Shumway JF 1997 Table V (−30% NYSE/AMEX) and
  Shumway & Warther JF 1999 (−55% Nasdaq) — already frozen in
  `series_termination.py` (`SHUMWAY_HAIRCUT = 0.55`, adverse anchor, venue
  unknown). This spec does NOT reopen the rule table; ckpt-1 reviewed it in
  steps 1-2.
- Universe admission and the alive/terminated cut have **no published
  formulation** — fixed by construction below and frozen in
  `UNIVERSE_SELECTION_RULE_VERSION`, which joins `INPUT_RULE_SETS` (below).
- Capture date: measured as `max(last_bar)` per vendor
  (`verify_2597_survivorship_acceptance._capture_date` — "not stored as a
  column: derived state that can drift"). The engine DECLARES the frozen value
  and ASSERTS it against the measurement at load (the #2720 FX-gate idiom:
  re-assert a mutable premise on the run's own data, refuse loudly).

## Measured premises (dev DB, 2026-08-15, full population)

| fact | value |
| --- | --- |
| `research_price_series` by vendor | Intrader 22,879 · PWB 7,693 · comparators 18 · cboe 1 |
| linked to instruments, within-vendor duplicates | Intrader 5,172 · PWB 5,269 · **0 instruments with two series within either vendor**; 4,549 across vendors |
| Intrader capture (`max(last_bar)`) | **2024-09-27**; 9,946 series end exactly there, 292 within 4 days, thin tail (~10/day) beyond |
| Intrader cohorts at a 7-day alive cut | linked-alive 4,663 · linked-early-end **509** (62 with Form 25 evidence — ticker-reuse suspects, the "505 all `is_tradable`" anomaly from the steps 1-2 evidence base) · unlinked-terminating 11,991 · unlinked-alive 5,716 |
| stored results | `strategy_results_store` 324 rows, all `survivor_only`, latest 2026-08-12. `corpus_version` on every row names the PWB-only constant; the Intrader linkage landed 08-14. ⚠ Provenance argued from the stamped `corpus_version` + timestamps — the frozen per-row universe membership (#2621's record) was never written for these rows (`strategy_result_universe` is empty), so an exact replay is impossible and is not claimed. What IS claimed: no row can be re-produced under its stored `result_version` (`assert_no_existing_results`), so the contamination window is closed by the vendor pin before any future run. |
| quarantine coverage | both vendors fully judged (22,879 / 7,693) |

## Defect this spec must fix first — the unpinned vendor

`_SERIES_SQL` / `_AXIS_SQL` / `_INSAMPLE_AXIS_SQL` select on
`instrument_id = ANY(validated)` with **no vendor filter**, and the engine
assumes one series per instrument (`raw_closes_by_instrument[instrument_id] =`
overwrites; `series={instrument_id: series}`). Since #2597 linked 5,172
Intrader series, 4,549 validated instruments carry TWO series — a re-run today
would double-enter live names and clobber close maps nondeterministically by
`ORDER BY series_id`. No corpus run may happen again until the pin lands.

**Fix — admission becomes SERIES-based everywhere (ckpt-1).** `load_corpus`
first computes the ADMITTED SERIES SET (one query per universe rule, below),
applies `limit` to that set, and derives EVERYTHING downstream — axis,
in-sample axis + bar counts, pairs, breaks, ranking passes, benchmark books —
from the admitted `series_id` list, never from `instrument_id = ANY(...)`
again. A smoke-limited run's axis therefore describes exactly the names it
evaluates. At load, assert **one admitted series per name key** — the
within-vendor uniqueness is a measured premise, not a guarantee, and a future
ingest could recreate the clobbering silently.

## Universe selection rule (frozen as `UNIVERSE_SELECTION_RULE_VERSION`)

One versioned mapping, vendor literals included in the version string:

| `Universe` | vendor | corpus freeze | admission |
| --- | --- | --- | --- |
| `survivor_only` | `paperswithbacktest/Stocks-Daily-Price` | `CORPUS_FROZEN_LAST_BAR` 2026-07-08 (existing `CORPUS_VERSION`) | linked ∩ §4.0 validated — exactly the stored 324's intended population |
| `survivorship_free` | `icyDenev/Intrader` | `INTRADER_CAPTURE_DATE` 2024-09-27 | below |

`CORPUS_VERSION` becomes per-universe: `strategy_result.CORPUS_VENDORS`'s own
docstring already declares *"A SECOND VENDOR MOVES THIS STRING"* — the
survivorship-free runs stamp `icyDenev/Intrader@2024-09-27`, survivor-only
rows keep the existing constant. One universe value per invocation, threaded
as an explicit `run_backtest(universe=...)` parameter into corpus load,
identity construction, result stamping, `_expected_refusals` and both arm
passes — never read back from the module global mid-run. `BACKTEST_UNIVERSE`
stays as the DEFAULT only.

### Survivorship-free admission (Intrader only)

1. **Live overlap** — `instrument_id ∩ load_validated_universe()` AND
   `last_bar > capture − 7 days` (4,663). The validated intersection keeps
   §4.0's class cut (US stocks ex-ETF) for the names where we can check it.
2. **Terminating** — `last_bar ≤ capture − 7 days`, regardless of any
   instrument link (11,991 unlinked + 509 linked-early). ⚠ The 509 linked
   ones are admitted as TERMINATING, not as live: a live instrument whose
   series stopped a year ago is a symbol-reuse suspect by construction, and
   its termination class comes from the SERIES' own evidence
   (`delisting_source` / `delisting_provision` / Q-suffix), never from the
   suspect link. Censused as their own stratum.

Excluded, counted, and named in the census (never silently dropped):

- **unlinked-alive (5,716)** — alive at capture but not resolvable to a
  validated instrument. The eToro-listing bias §6 of the parent names; it
  remains, and the census + corpus version carry it.
- Quarantine handling is unchanged — the arms machinery consumes the judged
  bars for both vendors identically.

The 7-day alive cut: 9,946 of 22,879 series end on the capture date itself and
292 more within 4 calendar days (weekend + short halts); the tail beyond is
~10/day, so any cut in 5–20 days moves tens of series in 22,879. Frozen at
**7 calendar days** inside `UNIVERSE_SELECTION_RULE_VERSION` — changing it is
a new selection rule version by construction.

**Class residue, stated honestly:** unlinked terminating series have no
security-class check — dead ETFs/preferreds/units are admissible. The Form 25
linkage already refuses non-common-equity classes on the evidence side; the
unlinked side cannot be filtered without a class source that does not exist
for dead names. This widens the population relative to §4.0's "US stocks
ex-ETF"; the honest handling is the label + census, and no narrower claim is
stamped.

**Scale-break asymmetry, stated:** `price_series_break` is a LIVE-table
overlay keyed by real instrument ids; unlinked series get no entries from it.
Their scale integrity comes from the corpus's own judged transition
quarantine (`research_transition_quarantine`), which both arms already
consume. Documented asymmetry, not a silent gap.

## Termination wiring

Placement (ckpt-1): termination is applied **in the position-building stage,
before costing** — a terminating series' still-open positions are converted to
realised closes so they flow through `cost_positions` and the wealth/raw
adjustment exactly like any other close. Precisely:

- A position whose `close_bar_date` is set by the builder (including a normal
  exit on the last bar) is NEVER touched — the builder's own close outranks
  termination, and the invariant is tested.
- Every remaining open position on a TERMINATING series (all of them, if a
  regime permits several) closes at the series' last bar with
  `exit_price = last_close × terminal_value_fraction(class, ambiguity_arm)`
  and `close_source = "series_termination"`. One class per series, computed
  once from `TerminationEvidence(linked=(delisting_source='sec_form25'),
  provision=delisting_provision, q_suffix=archive_symbol_candidates' trailing-Q
  rule)`.
- **Return basis:** the haircut applies to the series' own raw last close —
  the same price space every other exit fill uses — and the existing
  wealth/raw scaling then applies unchanged. No mixed-basis arithmetic.
- **Costs:** the standard exit-side band cost is charged on the terminal
  close, uniform with every other exit. The Shumway prescription is a return
  anchor, not a cost waiver, and exempting the exit would make termination the
  only uncosted exit in the engine.
- **Masked terminal bar:** under the `masked` quarantine arm the terminal bar
  may carry no admissible close; the position then takes the LAST ADMISSIBLE
  close at or before the terminal bar as its termination price, and if none
  exists between fill and terminal bar it is excluded and counted
  (`termination_price_unlocatable`), never dropped silently.
- Positions open at window end on a LIVE series stay `open_at_end` (hold-out
  only), unchanged. The in-sample `open_at_end == 0` invariant HOLDS — a
  terminated position has a close date.
- The three-date trap stands: no filing/suspension/removal date is a clock;
  the last bar is.
- Two-armed classes (`linked_unparsed`, `unknown_termination`) make the
  ambiguity arms genuinely diverge — the §3.4 machinery and
  `ambiguity_material` comparison already exist and need no change.
- A `survivor_only` run constructs no termination evidence and realises
  nothing (PWB has 1 early-ending series; it stays `open_at_end`/marked as
  today) — behaviour-preserving for the stored basis.
- ⚠ A narrowed evaluation window can end BEFORE a terminating series' last
  bar. The series then simply has no bars past the window and its positions
  are window-end opens, NOT terminations — termination fires only when the
  series' actual last bar lies inside the window.

## Window bounds

A `survivorship_free` run REFUSES (never clamps) an evaluation window ending
after the capture date: a name that dies between capture and window end is
invisible, which is survivorship bias re-entering through the calendar
(#2721's hard bound). Frozen `INTRADER_CAPTURE_DATE = date(2024, 9, 27)`,
asserted equal to `max(last_bar)` over the vendor at `load_corpus` — a
re-harvested archive that moves the measurement refuses rather than silently
shifting the label's meaning. `EVALUATION_WINDOW_START` (1962-01-02) and
`HOLDOUT_BOUNDARY` (2021-06-29, frozen FIRST hold-out bar) are date-anchored
and universe-independent; the hold-out span for this universe is 2021-06-29 →
2024-09-27.

## Engine keying — synthetic name keys, in-pass only, with a write-boundary guard

The pass keys books/closes/scores by an `int` name key. Unlinked series get
**`-series_id`** (instrument ids are positive; collision impossible). The
boundary is guarded, not asserted in prose (ckpt-1):

- `strategy_result_universe` for a `survivorship_free` result stores
  `universe_rule_version = UNIVERSE_SELECTION_RULE_VERSION` and its payload
  carries the admitted **series ids** (the replayable membership #2621's
  record exists for) alongside the linked instrument ids. No negative key is
  ever written as an "instrument id".
- `evaluated_instrument_count` counts admitted NAMES; the census (below) is
  what decomposes it.
- A test walks every persistence path reachable from a survivorship-free run
  (result rows, universe record, census, fold rows) and asserts no negative
  key appears in any column typed as an instrument id.
- `load_unresolved_breaks` receives only the REAL instrument ids (positive
  keys); synthetic keys contribute no live-table breaks by construction.

## Persistence — census, not new metric columns

New table `strategy_result_termination_census` (migration), one row per
`(result_id, stratum)` where stratum ∈ the closed union of
`TerminationClass` values + `unlinked_alive_excluded` +
`linked_early_reuse_suspect` + `termination_price_unlocatable`, with:
`CHECK (count >= 0)`, a CHECK pinning the closed vocabulary, uniqueness on
`(result_id, stratum)`, FK to the result store, and no UPDATE path (immutable
rows, the sql/333 idiom). Written in the same transaction as the result rows;
the writer REFUSES to store a `survivorship_free` row without its census, and
refuses a census whose strata do not sum to the vendor's series total minus
window-inadmissible series (each reconciliation term recorded on the row).
`strategy_results_store` itself gains NOTHING — `universe_basis` and
`corpus_version` already exist.

## The identity move

`INPUT_RULE_SETS` gains TWO entries at the same commit that wires the call:

- `"series_termination": TERMINATION_RULE_VERSION` — the invariant
  `series_termination.py`'s docstring freezes.
- `"universe_selection": UNIVERSE_SELECTION_RULE_VERSION` — ckpt-1: the
  vendor pins, admission rule, 7-day cut and capture constant all decide what
  a result contains, and the bare `universe` label on the identity does not
  version them.

Both are hand-maintained entries (strategies import neither module), pinned by
`test_the_stored_mapping_is_the_hashed_one` exactly like
`market_regime_provider`. Every strategy version moves ONCE (the two
exact-mapping test pins move with it). This over-invalidates survivor-only
identities whose behaviour is unchanged — accepted deliberately: it is the
same global-rule-set over-invalidation every prior `INPUT_RULE_SETS` entry
made, and a second identity move later (when someone notices the selection
rule was unhashed) would cost more than the one now. Existing 324 rows keep
their versions and are never rewritten — pointer-not-splice: new runs write
new rows under new versions; nothing reads back through old ones.

## Refusals, the policy version, and the declaration gate

- `universe_basis_not_survivorship_free` LEAVES `STANDING_REFUSALS` and is
  added conditionally in `_expected_refusals(universe=...)` (unless the run's
  universe is `survivorship_free`). The gate side needs NO change:
  `check_promotable` already derives it via
  `structural_promotion_refusals(universe_basis=row)`, which is
  basis-parameterised — the ckpt-1 correction is that the deliberate
  duplication is between `_expected_refusals` and `check_promotable`, and
  BOTH already exist; this spec only threads the universe into the former.
- **`STRUCTURAL_REFUSAL_POLICY_VERSION` bumps to v3** (ckpt-1): the policy's
  reachable outcomes change (the universe refusal becomes satisfiable), and
  frozen declarations pin the policy version precisely so a change like this
  cannot reinterpret them silently.
- `prereg_contract` needs no schema change — `declared_universe_basis` and
  the basis-parameterised recompute already exist.
- **Coverage does not gate promotion numerically, and that is a design
  decision, not an omission** (ckpt-1 asked): low Form 25 coverage degrades
  UNKNOWN-class terminations to the two-armed wide bounds, which diverges the
  ambiguity arms, which the existing `ambiguity_material` machinery already
  surfaces and refuses on. Poor coverage produces honest wide bounds, not a
  lie — a numeric floor would duplicate that mechanism with a magic number.
- ⚠⚠ After this lands, a declared `survivorship_free` run has **no standing
  structural refusal** — promotion becomes evidence-gated for the first time.
  That is the point of #2721, and why the acceptance run below is
  `purpose="harness_validation"` (rows carry `harness_validation_only`; no
  trial-register budget burned, per the #2599 sealed-outcome rule).

## What this spec does NOT do

- Does not touch the live paper-scan path. ⚠ Verified, not assumed (ckpt-1):
  `strategy_paper_runtime` and the scheduler stamp identities off the
  `BACKTEST_UNIVERSE` constant, which keeps its value — plus a pin test that
  the paper path's identity universe stays `survivor_only`.
- Does not change `series_termination.py`'s class table or haircut.
- Does not raise `delisting_source` coverage (that is the running register
  expansion + re-link, same ticket, different mechanism).
- Does not run a promotable survivorship-free trial.
- Does not re-derive `HOLDOUT_BOUNDARY`.

## Acceptance (after register re-link completes)

1. Local gates + the exact-mapping identity pins + the negative-key
   write-boundary test + the builder-close-outranks-termination test.
2. `survivor_only` restoration, **full-population set assertion** (ckpt-1: no
   "-ish"): the pinned admitted-series set equals EXACTLY the set from the
   legacy predicate restricted to the PWB vendor, and contains zero Intrader
   series ids. Both sides computed on the live dev DB in one script run.
3. `survivorship_free` harness-validation run (limited smoke, then full):
   - refuses a window ending after 2024-09-27;
   - census rows present; strata reconcile exactly to 22,879 with each
     exclusion term named (recorded in the PR at the register state of the
     run, alongside the register's row count + a note that the 2013-2021
     re-link moves the class split — the acceptance pins THIS run's census,
     not a lower bound);
   - the two ambiguity arms differ on every result whose census shows ≥1
     two-armed-class termination that closed a position, and are equal when
     none did (both directions asserted);
   - edge cases exercised: a series ending exactly at `capture − 7`, a
     weekend-end series, a terminating series with no open position at its
     last bar, a masked terminal bar, a narrowed window that excludes a
     terminating series' last bar;
   - `expected_structural_refusals` on every stored row excludes
     `universe_basis_not_survivorship_free` and includes
     `harness_validation_only`.
4. Runtime + row counts reported on the PR.
