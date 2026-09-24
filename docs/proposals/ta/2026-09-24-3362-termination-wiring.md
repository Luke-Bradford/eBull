# #3362 — wire series termination into the selection harness

Step 3 of `2026-09-24-selection-programme-v2.md` ("Termination", lines 68-70). Refs #2899 #2721 #2908 #3361.
Security: none (research harness; no broker, auth or order path). Codex ckpt-1: 40 findings, folded in below.

## Premise check (the issue's "ships UNWIRED" is half false)
- `app/services/series_termination.py` **is** wired into the TA backtest by #2721 step 3 (`d7cb3767`):
  `backtest_run._terminate_open_positions` (≈line 1099) emits close source `series_termination`, and
  `universe_selection.load_universe_selection` builds `TerminationEvidence` from `research_price_series`.
  `TERMINATION_RULE_VERSION` is already in strategy identity (`strategy_registry.py:118`).
- Its docstring ("SHIPS UNWIRED, ON PURPOSE") is stale, but **this ticket does not edit that file**:
  `_code_hash()` hashes the whole module, comments included, so a prose fix moves `TERMINATION_RULE_VERSION`
  and every strategy identity that carries it, including any preregistered one. The correction is recorded
  here and in a comment in `r6_exclusion_trial.py`. The prose fix should ride the next change that moves the
  rule for a real reason.
- What is unwired is the **selection harness**, `app/services/r6_exclusion_trial.py`. `_event_value`
  (line 163) treats every held symbol with no bar on the valuation session the same way, whether it is a gap
  or a termination: `TerminationCase = Literal["best", "worst"]` gives the stale last close, or `0.0`. That
  `0.0` is the "#2908 zero-recovery worst case".

## Source rule
The terminal fractions are `series_termination.py`'s frozen construction (#2721, ckpt-1-reviewed). Only the
failure haircut is a published prescription: Shumway (1997, Table V) and Shumway & Warther (1999) anchor it at
−30% / −55%, and the adverse −55% binds. Three treatments are the module's own construction, not the papers':
100% for (a)(3) operation of law, 100% for the unverified Q-suffix class, and (a)(4) ≡ (b) ("asserted, not
demonstrated"). The two-armed classes (100% / 45%) are **not encompassing bounds**: an unknown termination
can be a total loss (below 45%) or an acquisition premium (above 100%). That is why zero recovery governs.
The zero-recovery case is #2908's preregistered worst case
(`docs/proposals/ta/2026-08-24-r6-exclusion-preregistration.md`). The three-date trap stands: termination
fires at the stored last bar and prices off the last close; no Form 25 date is a clock. "Alive at capture" is
`universe_selection`'s existing rule (`last_bar > INTRADER_CAPTURE_DATE − ALIVE_CUT_DAYS`).

## Full-population verification (dev DB, 2026-09-24)
Population: `universe_selection._SERIES_ROWS_SQL` for vendor `icyDenev/Intrader` (22,879 harvested series; 0
NULL `last_bar`), excluding exchange test issues, terminating = `last_bar ≤ 2024-09-20` (the alive cut).
Classified with `classify_termination` on `(delisting_source = 'sec_form25', delisting_provision,
vendor_symbol_has_bankruptcy_suffix(vendor_symbol))`. 12,636 terminate:

| class | series |
| --- | --- |
| `unknown_termination` | 11,383 |
| `operation_of_law` ((a)(3)) | 699 |
| `q_suffix_otc_unverified` | 342 |
| `exchange_failure` ((b)) | 211 |
| `exchange_failure_a4` | 1 |
| `linked_unparsed_provision` | 0 |

On the same population: last bar in 2013–2018 → 4,058 terminations, **1** Form-25-linked; 2019–2024 → 8,578,
910 linked. **No terminating series has a last bar before 2013.** A further 292 series have a last bar
inside the alive cut (2024-09-21 → 2024-09-26): alive by the existing rule, but without a bar on the
2024-09-27 window end. The register spans 2013-01-02 → 2024-12-31 (11,367 rows). Reproduced by the
acceptance-2 script.

Declared residuals, for #2901's declaration to consume (not fixed here):
- **Classes are mostly unknown.** 90.1% of terminations take the two-armed bounds.
- **No death before 2013 is observable.** A name that died between the 2011-06 window start and 2012 is absent
  from the corpus, not terminated in it. Formations before 2013 carry survivor-only exposure. That establishes
  the early gap only. It does not establish that dead-name coverage is complete from 2013 onwards.
- `UnlinkedStratum` has **no producer**. The module requires the unlinked split for "the census that stamps a
  result", meaning a class-dependent survivorship label. This harness's verdict does not depend on class
  evidence (see Verdict), so the census reports by `TerminationClass` and the stratifier stays unbuilt.
- The harness has no series-break detection (unlike `backtest_run`, which refuses to haircut across a
  `series_break`). It values the vendor adjusted close as given, identically for arm and control.

## Class evidence and its linkage provenance
Evidence comes from the same stored columns the TA path uses (`research_price_series.delisting_source`,
`delisting_provision`, `vendor_symbol`, `first_bar`, `last_bar`), with the same capture assertion
(`max(last_bar) = INTRADER_CAPTURE_DATE`). The reader lives in `r6_exclusion_trial.py`, **not**
`universe_selection.py`: that module is hashed into `UNIVERSE_SELECTION_RULE_VERSION` exactly as
`series_termination.py` is, so editing it would move every strategy identity. The evidence construction
is therefore spelled twice. The guard is a full-population parity check
(`scripts/measure_3362_termination_census.py`): the reader's evidence equals `load_universe_selection`'s on
every terminating series (12,636 of 12,636, 0 failures, 2026-09-24).

It is **not** re-derived from the #3361 bundle's `Form25Flag`s. Those carry no provision, and the #3361 spec
(lines 61-68) says a Form 25 flag is never given economic meaning. The stored association is symbol-based
(`symbol_exact`). #3361 cross-check (b) is its provenance: it built the dated CIK link without Form 25 and
compared it with the Form 25 `issuer_cik` over **1,041** Form-25-associated Intrader series. Result: 879
agree, 3 disagree (all (a)(3) holdco successions), and 159 not comparable (77 vendor-symbol collisions, 50
never seen, 20 with no recent evidence, 11 conflicting, 1 with several register CIKs).
Artefact: `crosscheck-2026-09-24-f6ae1edd.json`, sha256
`63bb68ee21e535663f24e7f9903113bd438d89c9456073ae106d3d004c45ae40`; bundle manifest
`32a281d8e3188aed9a8b985b06a838f2a1a116a2d9b676918c5cf84cc80d4eb6`.
This is a source cross-check on a subset. It does not certify the population. CIK agreement is also
entity-level, not security-level (share class, ADR). The three disagreements keep their stored
`operation_of_law` class. Class evidence can only pull the binding figure **below** the zero-recovery one,
never above it (see Verdict), so a class error cannot manufacture a pass.

Symbol mapping: price-mirror file stem ↔ `research_price_series.vendor_symbol`, vendor `icyDenev/Intrader`,
one-to-one. The loaded file must match its row's **stored `first_bar` and `last_bar`** (the first and last
valid bars as loaded). A missing, duplicated or mismatched symbol raises. That refuses a mirror/DB snapshot
drift and a window-clipped load, which would otherwise move the termination clock. Measured over the
whole mirror (2026-09-24): 22,879 of 22,879 series match their stored bounds.

## Construction
1. **Evidence-bound status** at a valuation session `day`, for a held symbol with shares > 0 and no bar on
   `day`:
   - **terminated** ⇔ stored `last_bar < day` and the series is not alive at capture;
   - **alive at capture** (stored `last_bar` in the alive cut) and `day` is the window end ⇔ valued at its last
     close under every policy. It did not terminate; it is missing the final bar because of capture timing;
   - otherwise a **gap**: later bars exist, or the series is alive at capture and `day` is before the window
     end (an alive series never terminates). "Gap" is the neutral name: a missing bar may be a halt, a vendor
     omission or an invalid row. The harness does not claim which.
   A bar on `day` is always an ordinary price. Termination never overrides an existing bar, including a fill
   or liquidation on the last bar itself.
2. **A `TerminationPolicy`** replaces `TerminationCase`. It is an immutable value: `label`, `gap_fraction`,
   and terminal fractions as a sorted tuple of `(class, fraction)` pairs, or one uniform fraction. Fractions
   must be finite and in [0, 1]. The realised value is `shares × last valid adjusted close × fraction`.

   | policy | terminal fraction | gap | needs evidence | role |
   | --- | --- | --- | --- | --- |
   | `zero_recovery` | 0 | 0 | yes | **governing** |
   | `classified_worst` | `terminal_value_fraction(class, "worst_case")` | 0 | yes | sensitivity |
   | `classified_best` | `terminal_value_fraction(class, "best_case")` | 1 | yes | sensitivity |
   | `legacy_worst` | 0, no status split | 0 | no | #2908 replay only |
   | `legacy_best` | 1, no status split | 1 | no | #2908 replay only |

   The two legacy policies reproduce `case="worst"` / `case="best"` exactly: no status split, and a
   window-end symbol that is alive at capture is valued like any other missing bar. `zero_recovery` differs
   from `legacy_worst` **only** in the alive-at-capture window-end rule.
   A gap has no class, so each classified policy keeps #2908's gap bound on its own side. Between
   `classified_worst` and `zero_recovery`, the only input that differs is the terminal fraction of terminated
   classes. The return difference still includes second-order reinvestment and cost effects.
3. **Lifecycle.** A terminated holding is realised at the first valuation session after its last bar (the next
   rebalance or the window end), at the value above. It is then sold like any exit: it enters `pre_cost`
   wealth and pays the half-spread once, as part of traded notional (the TA path's "costed exactly like every
   other exit"). It cannot re-enter a target, because `signal_sets` admits only symbols with a post-formation
   bar. The harness computes no intermediate marks, so realising at the next session equals realising at the
   last bar into zero-return cash. The realisation log records both dates.
4. **Identical application to arm and control, by construction.** A new
   `simulate_under_policies(schedules, prices, evidence, policies, half_spread, window_end)` validates its
   inputs and runs every named schedule under each policy. Validation covers: unique policy labels;
   `zero_recovery` present unless every policy is legacy; evidence for every priced symbol when any policy
   needs it; schedules sorted and unique by formation; and no fill after `window_end`. Execution days stay
   per schedule, as today (`_execution_day` raises on asynchronous fills).
5. **Realisation log.** `PortfolioResult` gains realisations:
   `(session, last_bar, symbol, status ∈ {terminated, gap}, class | None, shares, last_close_value,
   realised_value)`. The values are position values in wealth units, before cost. They are logged only for
   shares > 0. `class` is `None` under legacy policies.
6. **Verdict.** The #2908 lesson is relative: a lower recovery can hurt the control more than the arm, so the
   governing policy is not automatically the arm's worst case. A result records every policy's
   arm-minus-control figure, and the **binding** figure is the minimum across the programme policies.
   `zero_recovery` must be among them and is the mandated governing policy. The classified policies can only
   turn a pass into a fail. They can never rescue one. `binding_policy(edges)` (pure) returns the label and
   value of the minimum.
7. **Result identity** records:
   - `TERMINATION_RULE_VERSION` and the sha256 of `r6_exclusion_trial.py`;
   - each policy serialised canonically (label, per-class terminal fractions, gap fraction, status split on or
     off);
   - the sha256 of the canonical evidence map (sorted by symbol: `series_id, first_bar, last_bar, linked,
     provision, q_suffix, class`);
   - `INTRADER_CAPTURE_DATE` and `ALIVE_CUT_DAYS`;
   - the full linkage-provenance digests above.
8. **Census per run.**
   - Universe: priced symbols split into live, alive at capture, and terminated before the window end, the last
     by class. Each split reconciles to the priced-symbol count.
   - Per policy × portfolio: realisation count and summed values, by status and class.
9. **#2908 stays reproducible.** `scripts/evaluate_2908_exclusion.py` maps its two literals to `legacy_best`
   and `legacy_worst`. It keeps its output keys and needs no evidence.

## Out of scope
- Generalising the harness input beyond the R6 bundle (2022+) to the #3360/#3361 inputs from 2011-06. That is
  #2901's first slice.
- Building `UnlinkedStratum`.
- Series-break detection.
- Editing `series_termination.py`.
- Any strategy look.

## Acceptance
1. **Replay equivalence, pinned before the refactor.** On the current code, commit a multi-rebalance fixture
   with golden `total_return`, events and censored counts, covering: live, gapped and terminated holdings;
   partial turnover; non-zero spread; and an alive-at-capture symbol. `legacy_best` / `legacy_worst` must
   reproduce them exactly after the change. The existing bounds tests stay.
2. **Reader parity, full population.** For every Intrader series, the new reader's evidence equals the
   `TerminationEvidence` that `load_universe_selection(survivorship_free)` builds, on each series it admits as
   terminating. A script prints the class table and the span counts above; the PR records its output.
   Fixture tests: missing, duplicate and bar-mismatched symbols raise; a linked Q-suffix symbol takes its
   Form 25 class; NULL, unrecognised and missing provisions give `linked_unparsed_provision`.
3. **Programme policies, end to end** (pure fixture): a termination mid-window, a gap, an alive-at-capture
   window end, a last-bar fill, and a zero-wealth path. Checks: realisation log, census reconciliation,
   identity fields, `binding_policy` choosing the minimum, and arm and control seeing one policy object.
4. A comment in `r6_exclusion_trial.py` records that the `series_termination` docstring is stale, and why it
   is not edited.

Rung: behavioural change with data semantics (evaluation-harness input and result identity), so Codex ckpt-2
before first push. It is not a corpus change: nothing is parsed, stored or migrated.
