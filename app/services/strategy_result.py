"""Phase 5c — the result model, and the promotion refusal that makes its label mean something.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §5.2 (the frozen
hold-out split), §5.4 (sizing as a result input), §3.4 (the ambiguity arms) and
§6 (#2288 clauses 2-4). Parent:
``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md`` criteria 1, 2,
5, 6 and 11. Refs #2240, #2288, #2284.

⚠⚠ THE GATE RETURNS REFUSALS. IT DOES NOT RAISE.

Every other validating class in this epic raises from ``__post_init__`` — the
registry on an unknown verdict, ``CostedPosition`` on a half-costed row. This
module's gate deliberately does not, and the inversion is load-bearing rather
than a lapse. §6 clause 4 puts the caller in phase 7: *"one function, returning
a reason, failing closed, that phase 7's guard calls"*. ``execution_guard``'s
own contract is one audited row per invocation with a stated reason, so an
exception reaching it is a DIFFERENT failure mode from a refusal — it has no
reason string to write, and "the gate crashed" and "the gate said no" would
land in ``decision_audit`` as the same absence.

So: a malformed candidate is REFUSED, never rejected at construction.
``check_promotable`` treats an unrecognised universe basis exactly as it treats
``survivor_only`` — the allowlist below has one member, and everything else is a
refusal with a code.

⚠⚠ EVERY REFUSAL IS RETURNED, NOT THE FIRST.

Short-circuiting on the first failure makes fixing the gate a five-round
discover-one-at-a-time loop, and it hides HOW FAR a result is from promotable —
which is the number an operator actually needs. A candidate today returns
several codes at once, and that is the honest picture.

⚠ #2288's own warning is the reason this module exists at all: *"Do not
implement the label without clause 4. A label nobody gates on is worse than no
label: it looks like control and provides none."* ``strategy_signals.universe``
(clause 1) shipped in 3b; the refusal is the half that makes it enforcement.

WHAT THIS MODULE IS NOT
-----------------------
⚠ NOT the hard pre-trade order gate. §4.0's allocation invariant 2 puts that in
``execution_guard`` (phase 7) — *"A ledger label is observability; this needs
enforcement"*. This is the RESULT-layer refusal that the guard consults.

⚠ NOT a statistics module. Criterion 7's metric set is COMPUTED in
``strategy_statistics`` (stage 5d) and merely CARRIED here, on
``StrategyResult.metrics``, so that the gate can refuse a result whose
effective sample size was never computed. ``sql/262`` deliberately shipped with
no metric column and ``sql/263`` added them afterwards, in that order: 5d writes
its numbers INTO a row whose basis is already ``NOT NULL``, rather than
inventing a metrics shape with the basis bolted on later.

⚠ NOT the hold-out access log. Stage 5e owns the mechanically-inaccessible
namespace and the access records (criterion 5). This module's gate READS two
counts and refuses when they do not line up; today nothing produces them, so
they are zero and the gate refuses — which is §6's *"the gate's initial state is
'nothing is promotable'. That is correct, not a bug to work around."*

⚠ NEAR-LEAF BY DESIGN. It imports ``cost_model`` (a leaf) and
``strategy_statistics`` (which imports only ``equity_curve``, also a leaf) and
nothing else from the app. ``position_builder``'s rule-set version reaches a
result row as a string the WRITER stamps, not as an import — the same split that
let ``strategy_registry`` depend on ``cost_model`` without dragging phase 5 into
phase 3a.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Final, Literal, cast, get_args

from app.services.cost_model import COST_MODEL_ID
from app.services.deflated_sharpe import DeflatedSharpeResult
from app.services.equity_curve import BENCHMARK_RULE_ID, SIZING_RULE_ID
from app.services.random_entry_cohort import SyntheticControl
from app.services.research_price_structure_store import QUARANTINE_ARMS, QuarantineArm
from app.services.strategy_promotion_evidence import PromotionEvidence, evidence_refusals
from app.services.strategy_statistics import METRIC_SET_ID, StrategyMetrics
from app.services.trial_register import TRIAL_REGISTER, TRIAL_REGISTER_VERSION

# ---------------------------------------------------------------------------
# The frozen corpus and window (§5.2)
# ---------------------------------------------------------------------------

#: The research corpus this evaluation is pinned to: VENDOR at FROZEN LAST BAR.
#:
#: ⚠⚠ NOT A CODE HASH, and the difference matters. #2288 names
#: ``price_adjustments.detector_version`` (``sql/246``) as the prior art — a
#: rule-set id plus a hash of the code. That construction versions CODE. A
#: corpus is DATA, and no hash of any module changes when the archive gains a
#: year of bars, so a code hash here would be a version stamp that cannot move
#: for the only reason it would ever need to.
#:
#: So it is constructed from what actually identifies the data: the vendor
#: (``research_price_series.vendor``, one value on this corpus) and the last bar
#: the evaluation is frozen at. §5.2: appended data *"sits outside the frozen
#: window until a deliberate re-freeze, which is a corpus-version event that
#: invalidates prior hold-out results and must be visible as one"* — bumping
#: this literal IS that event.
#:
#: ⚠ A SECOND VENDOR MOVES THIS STRING. The corpus carries one today; #2284's
#: paid delisted half would be a second, and pooling the two under an unchanged
#: version is exactly the silent re-basing this constant prevents. Asserted on
#: the full population by ``scripts/verify_2240_result_model.py --frozen``.
CORPUS_VENDORS: tuple[str, ...] = ("paperswithbacktest/Stocks-Daily-Price",)
CORPUS_FROZEN_LAST_BAR = date(2026, 7, 8)
CORPUS_VERSION = f"{'+'.join(CORPUS_VENDORS)}@{CORPUS_FROZEN_LAST_BAR.isoformat()}"

#: The evaluation window, closed on both ends, over the §4.0 validated universe.
#:
#: ⚠ THE START IS MEASURED, NOT CHOSEN. It is the first bar the corpus holds for
#: any instrument in the validated universe. §5.2 freezes the boundary, the
#: corpus version and the END date and says nothing about a start, because there
#: is no decision to take: the window opens where the data does.
#:
#: ⚠ §10 of the spec applies to it anyway — *"1962 is not 64 years of usable
#: evidence for any strategy"*. Median per-series depth in this slice is 3,072
#: bars. Depth is per-series, never per-corpus, and this literal is the corpus's.
EVALUATION_WINDOW_START = date(1962, 1, 2)
EVALUATION_WINDOW_END = CORPUS_FROZEN_LAST_BAR

#: §5.2's bar-weighted 75/25 split point, FROZEN.
#:
#: ⚠⚠ THIS IS THE FIRST HOLD-OUT BAR, NOT THE LAST IN-SAMPLE ONE, and the
#: distinction is not pedantry — it is 4,021 bars. The SELECTION rule and the
#: SPLIT rule are two rules that a single date hides:
#:
#:   selection — the first trading date whose CUMULATIVE bar count strictly
#:               exceeds 75% of the slice's total;
#:   split     — that date and everything after it is HOLD-OUT; strictly before
#:               it is in-sample.
#:
#: Encode only the first and the boundary date's own 4,021 bars land in
#: training, which is a 0.02% leak of withheld data — far too small to show up
#: in any summary statistic and exactly the leak criterion 5 exists to prevent.
#:
#: Measured 2026-08-07 over the corpus ∩ validated-universe slice (5,266 series,
#: 23,339,583 bars): the cumulative crosses 75% at 2021-06-29 with 17,505,079,
#: of which 4,021 fall ON that date — giving 17,501,058 in-sample (75.0%) and
#: 5,838,525 hold-out (25.0%) across 1,261 hold-out dates. Reproduces spec M14
#: and M18 exactly. Re-derive with::
#:
#:     PYTHONPATH=. uv run python scripts/verify_2240_result_model.py --frozen
#:
#: ⚠ FROZEN, and a recomputation is the defect. §5.2: *"It is a function of the
#: corpus, and the corpus grows; a recomputed boundary walks forward silently
#: and re-admits hold-out data into training between runs."* The verify arm
#: asserts the literal still equals the derivation and FAILS rather than
#: re-splitting (acceptance C5); it does not update it.
HOLDOUT_BOUNDARY = date(2021, 6, 29)

#: ⚠ Weighted by BAR, not by trading date, and the rejected reading is recorded
#: because it is the one an unwary re-derivation reaches for. This panel is
#: unbalanced — 30 series in 1970 against 5,245 in 2026 — so "the final 25% of
#: history" has two readings eleven years apart. Date-weighting puts the
#: boundary at 2010-05-18 and yields a hold-out LARGER than the training set
#: (9,392,777 / 13,946,806): it would train on thin 1960s-2000s data and
#: withhold the dense modern era. Not a defensible alternative (§5.2).
HOLDOUT_WEIGHTING = "bar"

#: §5.4's declared v1 sizing rule. ⚠ AN INPUT TO THE RESULT IDENTITY, not a
#: detail: *"equal-weight-per-signal, fixed-fraction and volatility-targeted
#: sizing give materially different drawdowns from identical signals … Naming it
#: as an input is what stops a later sizing change reading as a performance
#: improvement."*
#:
#: ⚠ RE-EXPORTED, NOT RESTATED. Stage 5c shipped this as its own string literal
#: while nothing implemented it; stage 5d built the engine, and a second copy of
#: the id would let the rule change in ``equity_curve`` while the hash on every
#: result row kept claiming the old one. The engine owns it.
SIZING_RULE = SIZING_RULE_ID

#: How criterion 7's buy-and-hold comparator is composed. Re-exported from the
#: engine for the same reason ``SIZING_RULE`` is.
#:
#: ⚠⚠ IT IS A SEPARATE ID BECAUSE THE BENCHMARK USED TO INHERIT ``SIZING_RULE``,
#: WHICH REBALANCES (#2426). A rebalanced comparator is not buy-and-hold — Blume
#: & Stambaugh (JFE 12, 1983, 387-404) — and on our own full population that
#: inheritance added 23.2 points of annual return to the bar every strategy is
#: measured against. Hashed into ``ResultIdentity.version`` on the same argument
#: §5.4 makes for the sizing rule: a comparator that can change without the
#: identity moving is a comparator that can be tuned invisibly.
BENCHMARK_RULE = BENCHMARK_RULE_ID


# ---------------------------------------------------------------------------
# Closed vocabularies
# ---------------------------------------------------------------------------

#: #2288 clause 1's vocabulary, reused verbatim from ``sql/255``'s ``universe``
#: column rather than restated with different spellings.
#:
#: ⚠ ``survivorship_free`` IS NOT A VALUE ANY CURRENT CORPUS CAN PRODUCE, and §6
#: says so: US survivorship is only PARTIALLY correctable (86.2% issuer
#: resolution, CEF/FPI-shaped residue, eToro-listing bias) and non-US is not
#: correctable at all. It is in the vocabulary because the gate needs something
#: to allow; it is not a label today's writer may stamp.
UniverseBasis = Literal["survivor_only", "survivorship_free"]
UNIVERSE_BASES: frozenset[str] = frozenset(get_args(UniverseBasis))

#: ⚠ THE ALLOWLIST HAS ONE MEMBER, and it is an EXPLICIT LITERAL rather than
#: anything derived from ``UniverseBasis``. That is the point: widening the
#: vocabulary must not widen this. A basis added above lands on the refused side
#: because it is simply absent here, so promoting it takes a deliberate edit to
#: this line — which is what ``check_promotable``'s allowlist reading buys.
#:
#: ⚠ Deriving it (``UNIVERSE_BASES - {"survivor_only"}``) would invert exactly
#: that property: a new label would arrive PROMOTABLE by default, and the
#: default is the direction #2288 clause 2 is about.
PROMOTABLE_UNIVERSE_BASES: frozenset[str] = frozenset({"survivorship_free"})

#: §5.4's three levels. ⚠ ``signal`` is absent DELIBERATELY: *"Drawdown and
#: Sharpe are computed at the latter two ONLY — a per-trade max drawdown does
#: not compose."* Per-signal results are the ledger, not this table.
ResultScope = Literal["sleeve", "portfolio"]
RESULT_SCOPES: frozenset[str] = frozenset(get_args(ResultScope))

#: Criterion 5's two namespaces, plus the state §5.2 gives a signal that belongs
#: to neither. ⚠ ``purged`` is not a third result namespace — no result row may
#: carry it (``StrategyResult`` refuses) — it is the verdict
#: ``namespace_for_signal`` returns for a signal that must be dropped.
ResultNamespace = Literal["in_sample", "hold_out"]
RESULT_NAMESPACES: frozenset[str] = frozenset(get_args(ResultNamespace))

SignalNamespace = Literal["in_sample", "hold_out", "purged"]

#: §3.4's declared sensitivity pair. ⚠ NOT "assume the stop for conservatism",
#: which §3.5 rule 4 and spike S5 explicitly reject — *"it is not conservative,
#: it is a different bias"*. Both arms are computed and both are reported; a
#: declared two-sided bound is not a point estimate dressed as caution.
AmbiguityArm = Literal["worst_case", "best_case"]
AMBIGUITY_ARMS: frozenset[str] = frozenset(get_args(AmbiguityArm))

#: Why a result is not promotable. ⚠ A CLOSED vocabulary for criterion 9's
#: reason — a refusal that cannot be counted cannot be reported, and an operator
#: needs to know which of these is the one blocking every strategy at once.
PromotionRefusal = Literal[
    "harness_validation_only",
    "universe_basis_absent",
    "universe_basis_not_survivorship_free",
    #: §5.1's cost components. ⚠ TWO CODES, NOT ONE, since #2363. The single
    #: `carry_unmodelled` was derived from `CARRY_BPS is None or FX_BPS is
    #: None`, so it could not say WHICH component was missing — and the two
    #: close on unrelated evidence: carry on a per-order eToro product
    #: eligibility proving underlying-at-x1, FX on the funding account's
    #: currency and a measured conversion markup. Promotion still requires both
    #: absent; what the split buys is that an operator can act on the one that
    #: is actually blocking, and that whichever evidence lands first can be
    #: banked. Same argument as the `synthetic_control_*` trio below.
    #: (#2720 closed both for the declared lane — new rows stamp false — but
    #: the codes stay: every pre-#2720 row still carries true and refuses here.)
    "carry_unmodelled",
    "fx_unmodelled",
    "no_instruments_evaluated",
    "instrument_outside_validated_universe",
    "holdout_never_evaluated",
    "holdout_accesses_unrecorded",
    "deflated_sharpe_not_computed",
    "trial_count_undeclared",
    "trial_register_superseded",
    #: Criterion 3's overlap-corrected sample size, from stage 5e's block
    #: bootstrap. ⚠ SEPARATE from ``deflated_sharpe_not_computed`` even though
    #: both are null today and both come from 5e: criterion 6's DSR CONSUMES the
    #: effective sample size (§5.2), so a DSR present with the sample size
    #: missing is a DSR computed on a nominal n — which criterion 3 forbids
    #: outright. Collapsing them would make that state unreportable.
    "effective_sample_size_not_computed",
    "ambiguity_arms_not_compared",
    "ambiguity_material",
    #: Criterion 9's sensitivity arm (stage 5e-5a). ⚠ There is NO
    #: ``quarantine_material`` twin, and the asymmetry with the ambiguity pair
    #: above is deliberate: §3.4 declares a materiality rule for the ambiguity
    #: arms, and criterion 9 declares none — it requires the exclusion to be
    #: *"visible rather than assumed harmless"*. A threshold invented here would
    #: be the made-up constant the instruction set forbids, so the gate refuses
    #: on the comparison being ABSENT and never on its size.
    "quarantine_arms_not_compared",
    #: §9's synthetic control (stage 5e-5b). ⚠ THREE CODES, NOT ONE, and the
    #: split is the same argument that keeps `deflated_sharpe_not_computed`
    #: apart from `effective_sample_size_not_computed`: each names a different
    #: broken thing and a different operator action.
    #:
    #:   - `synthetic_control_not_run` — no cohort exists for this result. The
    #:     WRITER has not run §9's control.
    #:   - `synthetic_control_cohort_shows_edge` — §9's FIRST threshold failed:
    #:     the random cohort's own mean net return does not lie within its 95%
    #:     bootstrap interval of zero. ⚠ This is a verdict on the HARNESS (or on
    #:     the threshold), not on the strategy — *"a harness that finds edge in
    #:     noise is broken regardless of what else it explains"* — so it blocks
    #:     EVERY strategy measured under that cohort at once, which is precisely
    #:     what an operator needs to be able to see as one cause.
    #:   - `synthetic_control_sharpe_below_cohort` — §9's SECOND threshold
    #:     failed: this strategy's Sharpe does not exceed the cohort's 95th
    #:     percentile, so it does *"not count as evidence at all"*.
    #:
    #: ⚠ Unlike criterion 9's arm — which has no `quarantine_material` twin
    #: because no source fixes a blocking magnitude — §9 DOES declare both
    #: magnitudes, verbatim. So the magnitude refusals here are the spec's, not
    #: invented, and their absence would be the omission.
    "synthetic_control_not_run",
    "synthetic_control_cohort_shows_edge",
    "synthetic_control_sharpe_below_cohort",
    "promotion_evidence_missing",
    "expectancy_lower_bound_not_positive",
    "profit_factor_not_computed",
    "profit_factor_invalid",
    "profit_factor_not_above_one",
    "recent_year_instability",
    "recent_year_evidence_incomplete",
    "excluding_best_1_not_positive",
    "tail_or_concentration_limits_failed",
    "probability_calibration_failed",
    "path_diagnostics_incomplete",
    "executable_cost_inputs_missing",
    "executable_cost_inputs_stale",
    "broker_ineligible",
    "challenger_evidence_incomplete",
    "challenger_population_not_comparable",
    "candidate_does_not_beat_challengers",
    "ev_bucket_evidence_incomplete",
    "ev_bucket_ranking_not_monotonic",
    "outcome_contrast_evidence_incomplete",
    "outcome_contrast_population_not_comparable",
]
PROMOTION_REFUSALS: frozenset[str] = frozenset(get_args(PromotionRefusal))


# ---------------------------------------------------------------------------
# The frozen split (§5.2)
# ---------------------------------------------------------------------------


def namespace_for_bar(bar_date: date) -> ResultNamespace:
    """Which side of the frozen boundary a single bar falls on.

    ⚠ ``>=`` and not ``>``: ``HOLDOUT_BOUNDARY`` is the FIRST HOLD-OUT BAR. The
    4,021 bars stamped that date are withheld, not trained on.
    """
    return "hold_out" if bar_date >= HOLDOUT_BOUNDARY else "in_sample"


def namespace_for_signal(signal_bar_date: date, fill_bar_date: date) -> SignalNamespace:
    """§5.2's inclusivity rule for a signal, including the purge.

    *"A signal whose ``signal_bar_date`` is in-sample but whose
    ``fill_bar_date`` is on or after the boundary is PURGED — it is neither,
    because acting on it needs a price from the withheld side."*

    ⚠ The purged case is a THIRD verdict and not a rounding of either side.
    Assigning it to the in-sample arm imports a hold-out price into a training
    number; assigning it to the hold-out arm credits the hold-out with a
    decision taken on training data. It is dropped.
    """
    signal_side = namespace_for_bar(signal_bar_date)
    fill_side = namespace_for_bar(fill_bar_date)
    if signal_side == fill_side:
        return signal_side
    # `fill_bar_date > signal_bar_date` is a CHECK on sql/255, so the only
    # mixed pair reachable through the ledger is in-sample signal / hold-out
    # fill. The reverse would mean a fill preceding its signal, which is a
    # corrupt row rather than a namespace question — purging it is the
    # fail-closed answer and keeps this function total.
    return "purged"


def namespace_for_position(entry_fill_bar_date: date, close_bar_date: date | None) -> ResultNamespace:
    """§5.2's rule for a whole position: a position SPANNING the boundary is hold-out.

    *"A position that spans the boundary belongs to the hold-out and its entry
    is purged from the in-sample result. Splitting its return across namespaces
    would put hold-out prices into an in-sample number."*

    ⚠⚠ ``close_bar_date is None`` is an OPEN position (§3.2 rule 5) and it is
    ALWAYS hold-out, whichever side its entry is on — which is why this branch
    ignores ``entry_fill_bar_date`` entirely rather than testing it. An open
    position's mark is taken at the last usable close of the EVALUATION WINDOW,
    and ``EVALUATION_WINDOW_END`` is itself on or after ``HOLDOUT_BOUNDARY``
    (2026-07-08 against 2021-06-29 — asserted by
    ``tests/test_strategy_result.py::TestFrozenSplit``). So the mark is priced
    off a withheld bar in every case. Treating "no close" as "no span" would put
    that mark into a training number, which is the same leak the closed case
    guards.

    ⚠ A CLOSE BEFORE ITS ENTRY RAISES. Unreachable through
    ``position_builder`` — every close it emits is at or after the entry fill
    bar, and ``sql/256`` bounds ``bars_held >= 0`` — but this function is public
    and takes two bare dates, so nothing at the call site says which is which.
    ``namespace_for_signal`` already refuses its own corrupt pair (it returns
    ``purged``); the asymmetry was the real finding, not the reachability.
    ⚠ It RAISES rather than returning a verdict because there is no third state
    to return: ``ResultNamespace`` is two members, and silently answering
    ``namespace_for_bar(close_bar_date)`` on a reversed pair is a number with no
    signal attached. Same shape as ``EntryFill``/``ExitFill`` refusing a
    non-positive price (#2354).

    ⚠ ``entry_fill_bar_date`` is therefore unused on the OPEN path and is kept
    in the signature deliberately: it is what a caller has, it is what the
    ordering check below reads, and a function taking only the close would push
    the "is it open" test back out to every call site.
    """
    if close_bar_date is None:
        return "hold_out"
    if close_bar_date < entry_fill_bar_date:
        raise ValueError(
            f"position closes {close_bar_date} before its entry fill {entry_fill_bar_date} — a reversed pair has "
            "no namespace, and answering on the close alone would be a verdict with no signal attached"
        )
    return namespace_for_bar(close_bar_date)


# ---------------------------------------------------------------------------
# Result identity (criterion 11, §5.4)
# ---------------------------------------------------------------------------

#: Prefix on legacy ``ResultIdentity.version`` values. Corrected total-return
#: rows use ``TOTAL_RETURN_RESULT_SET_ID`` below. Same construction as
#: ``STRATEGY_SET_ID`` / ``RULE_SET_ID`` elsewhere in the epic: a readable id so
#: a stored hash says what KIND of thing produced it, plus 12 hex of payload.
RESULT_SET_ID = "strategy-result-v1"
TOTAL_RETURN_RESULT_SET_ID = "strategy-result-v2"
LEGACY_RETURN_BASIS = "raw-close-price-return-v1"
TOTAL_RETURN_BASIS = "split-dividend-adjusted-wealth-v1"
RETURN_BASES: Final[frozenset[str]] = frozenset({LEGACY_RETURN_BASIS, TOTAL_RETURN_BASIS})


@dataclass(frozen=True)
class ResultIdentity:
    """Everything that makes this a distinct RESULT, hashed into one version.

    ⚠⚠ WHY THIS IS NOT FOLDED INTO ``strategy_version``, which is the obvious
    reading of criterion 11 and is wrong.

    C11 warns that *"the position-sizing rule and the ambiguity arm are
    execution assumptions and are hashed too; a sizing change that did not move
    the version would let a different strategy inherit a track record."* Putting
    them inside ``StrategyIdentity.version`` would satisfy the letter and wreck
    the ledger: ``strategy_signals`` keys on ``strategy_version``, and the
    SIGNALS are byte-identical under either sizing rule. The whole signal ledger
    would be duplicated once per sizing rule and once per ambiguity arm, for
    rows that do not differ.

    The coherent split is that these are properties of the EVALUATION, not of
    the strategy — so they get their own hash, and C11's requirement holds
    against it: change any member below and ``version`` moves. Asserted member
    by member in ``tests/test_strategy_result.py``, not once in aggregate.

    ⚠ ``cost_model_id`` is here even though it is ALREADY inside
    ``strategy_version`` (5b hashed it). That is not redundancy for its own
    sake: this hash must stand alone as the answer to *"what produced this
    row"*, and a reader holding a result should not have to reverse a strategy
    hash to learn what costs were charged. It is stored as a column too, for
    the same reason ``sql/255`` stores ``universe`` beside a version that
    contains it.

    ⚠ NOT hashed: ``universe_basis``. It is an OBSERVATION about the corpus, not
    a knob somebody set — two runs on the same corpus version cannot disagree
    about it, and hashing it would let a mislabelled row claim a distinct
    identity instead of colliding with the correctly-labelled one.
    """

    strategy_id: str
    strategy_version: str
    result_scope: ResultScope
    namespace: ResultNamespace
    ambiguity_arm: AmbiguityArm
    #: Criterion 9's arm (stage 5e-5a). ⚠⚠ ON THE RESULT KEY AND HASHED, for the
    #: reason ``ambiguity_arm`` is: two arms over the same corpus, same code and
    #: same quarantine rule set are two MEASUREMENTS, and without this field
    #: they hash to the same ``result_version`` and the second silently
    #: overwrites the first. ⚠ It sits here rather than in the STRATEGY identity
    #: because it is a property of how a result was measured, not of the rule
    #: the strategy applies — the same place, and for the same reason, as
    #: ``input_rule_set_version`` below.
    quarantine_arm: QuarantineArm
    sizing_rule: str
    #: How the buy-and-hold comparator is composed —
    #: ``equity_curve.BENCHMARK_RULE_ID``. ⚠ HASHED, and #2426 is why it has to
    #: be: it did not exist, so the benchmark silently inherited ``sizing_rule``
    #: and rebalanced. Two runs whose comparator differs are two different
    #: measurements of "did this beat buying and holding", and without this field
    #: they hash to the same ``result_version``.
    benchmark_rule: str
    cost_model_id: str
    corpus_version: str
    window_start: date
    window_end: date
    #: ``position_builder.RULE_SET_VERSION`` — the pyramiding collapse, the
    #: same-bar ordering and the close-source precedence all live there, and all
    #: three change which trades a result is computed over.
    position_rule_set_version: str
    #: ``outcome_resolver.RULE_SET_VERSION``.
    outcome_rule_set_version: str
    #: The bars' own stamp — ``price_quarantine.RULE_SET_VERSION`` for a masked
    #: read. ⚠ ``sql/256``'s third key member, for the reason its header gives:
    #: re-run the quarantine under a changed rule set and the same signal
    #: resolves differently with the resolver byte-identical.
    input_rule_set_version: str
    #: Accounting basis for returns and equity marks. The legacy value keeps
    #: the historical v1 hash byte-identical; any corrected total-return result
    #: is a v2 identity. Raw OHLC execution remains an independent invariant.
    return_basis: str

    @property
    def version(self) -> str:
        """A stable hash over every current field, preserving legacy hashes.

        ⚠ ``sort_keys=True`` and explicit separators, matching
        ``StrategyIdentity.version``: the hash must not move because a field was
        reordered in the source or because a Python version changed dict
        iteration. The sole compatibility branch is the explicit legacy return
        basis: those rows predate the column and retain their already-stored v1
        hash; every corrected basis includes the field and uses the v2 prefix.
        """
        fields = {
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "result_scope": self.result_scope,
            "namespace": self.namespace,
            "ambiguity_arm": self.ambiguity_arm,
            "quarantine_arm": self.quarantine_arm,
            "sizing_rule": self.sizing_rule,
            "benchmark_rule": self.benchmark_rule,
            "cost_model_id": self.cost_model_id,
            "corpus_version": self.corpus_version,
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "position_rule_set_version": self.position_rule_set_version,
            "outcome_rule_set_version": self.outcome_rule_set_version,
            "input_rule_set_version": self.input_rule_set_version,
        }
        if self.return_basis != LEGACY_RETURN_BASIS:
            fields["return_basis"] = self.return_basis
        payload = json.dumps(
            fields,
            sort_keys=True,
            separators=(",", ":"),
        )
        prefix = RESULT_SET_ID if self.return_basis == LEGACY_RETURN_BASIS else TOTAL_RETURN_RESULT_SET_ID
        return f"{prefix}+{hashlib.sha256(payload.encode()).hexdigest()[:12]}"


# ---------------------------------------------------------------------------
# The row
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StrategyResult:
    """One ``strategy_results`` row: its provenance, its metrics and the gate inputs.

    ⚠ ``metrics`` IS REQUIRED AND HAS NO DEFAULT, which is #2288 clause 2's
    argument applied to criterion 7: *"a result missing any of the twelve is
    incomplete"*, and ``sql/263`` makes sixteen of the columns ``NOT NULL`` with
    no default, so a row cannot be written without them. A defaulted or optional
    field here would be a shape the table refuses — the mismatch would surface
    as an integrity error at write time instead of a type error at assembly
    time, which is strictly later and strictly less informative.

    ⚠ THIS ONE RAISES, unlike the gate. It is a WRITER-side shape: a caller
    assembling a malformed row has a bug, and the loud failure is what stops it
    reaching the table. The gate's caller is phase 7 and cannot use an
    exception — see the module docstring.
    """

    identity: ResultIdentity
    purpose: Literal["harness_validation", "capital_candidate"]
    #: ``strategy_statistics.compute_metrics`` output. ⚠ Its own
    #: ``effective_sample_size`` is ``None`` until stage 5e, and the gate below
    #: refuses on that — the metric set being PRESENT is not the same as it
    #: being COMPLETE.
    metrics: StrategyMetrics
    universe_basis: str
    #: ``cost_model.CARRY_UNMODELLED`` AS AT COMPUTE TIME, stamped per row and
    #: never re-read from the module at gate time. ⚠ Deliberate: when carry is
    #: finally measured, every row computed before that measurement must STAY
    #: unpromotable, and a gate reading today's module constant would silently
    #: promote a two-year-old result that never charged it.
    carry_unmodelled: bool
    #: ``cost_model.FX_UNMODELLED``, same contract. ⚠ REQUIRED AND UNDEFAULTED,
    #: like every other stamp here: a default would let a writer that never
    #: considered FX inherit a verdict about it, and #2363 exists because a
    #: single flag was standing in for two facts.
    fx_unmodelled: bool
    #: ``len`` of the evaluated set. ⚠ Stored for the record and for criterion
    #: 9's census; the GATE reads the ids themselves, because a count cannot
    #: answer "is every one of them in the validated universe".
    evaluated_instrument_count: int
    #: Criterion 6. Both NULLABLE, and NULL is the fail-closed default: a result
    #: with no Deflated Sharpe is refused rather than treated as unmeasured-but-
    #: fine. ⚠ An undeclared trial count *"fails; it does not default to the
    #: number of shipped strategies"*.
    trial_count: int | None = None
    deflated_sharpe: Decimal | None = None
    #: Stage 5e-3's declared inputs (``sql/266``). ⚠ OPTIONAL and SEPARATE from
    #: the two scalars above rather than replacing them, which keeps both of the
    #: gate's criterion-6 refusals reachable: a caller may declare a trial count
    #: with no DSR yet (the register exists, the evaluation has not run), and
    #: the gate must still be able to say which of the two is missing. When it
    #: IS present, ``__post_init__`` binds the scalars to it — the same
    #: all-or-nothing ``sql/266`` enforces, checked at assembly time where the
    #: error names the field.
    deflated: DeflatedSharpeResult | None = None
    #: §9's random-entry synthetic control (stage 5e-5b, ``sql/268``). NULLABLE,
    #: and NULL is again the fail-closed default: the gate refuses a result with
    #: no null distribution to read its Sharpe against.
    #:
    #: ⚠ It carries the strategy's OWN Sharpe and return as well as the cohort's
    #: thresholds, because the verdict is a comparison and a comparison with one
    #: side missing is not checkable. ``__post_init__`` binds those two to
    #: ``metrics`` — ``sql/268`` stores only the cohort side and expresses the
    #: verdict as a CHECK over the columns already on the row, which is the
    #: "one number, one column" rule the DSR's effective-sample-size binding
    #: exists to enforce.
    synthetic_control: SyntheticControl | None = None

    def __post_init__(self) -> None:
        if self.purpose not in {"harness_validation", "capital_candidate"}:
            raise ValueError(
                f"unknown strategy purpose {self.purpose!r}; must be harness_validation or capital_candidate"
            )
        if self.identity.result_scope not in RESULT_SCOPES:
            raise ValueError(
                f"unknown result scope {self.identity.result_scope!r}; must be one of {sorted(RESULT_SCOPES)}"
            )
        if self.identity.namespace not in RESULT_NAMESPACES:
            raise ValueError(
                f"unknown result namespace {self.identity.namespace!r}; must be one of {sorted(RESULT_NAMESPACES)} "
                "— `purged` is a signal verdict and never a result row (§5.2)"
            )
        if self.identity.ambiguity_arm not in AMBIGUITY_ARMS:
            raise ValueError(
                f"unknown ambiguity arm {self.identity.ambiguity_arm!r}; must be one of {sorted(AMBIGUITY_ARMS)}"
            )
        if self.identity.quarantine_arm not in QUARANTINE_ARMS:
            raise ValueError(
                f"unknown quarantine arm {self.identity.quarantine_arm!r}; must be one of {sorted(QUARANTINE_ARMS)}"
            )
        # ⚠ A BLANK version is PRESENT and meaningless, and a NOT NULL column
        # does not catch it — the #2286 shape, where an empty
        # `EBULL_SERVICE_TOKEN=` won an alias race against a real credential.
        # Every field here is identity, so an empty one silently merges two
        # rule sets into one bucket. `sql/256` makes the same check in SQL.
        for field_name in (
            "strategy_id",
            "strategy_version",
            "sizing_rule",
            "benchmark_rule",
            "cost_model_id",
            "corpus_version",
            "position_rule_set_version",
            "outcome_rule_set_version",
            "input_rule_set_version",
            "return_basis",
        ):
            if not getattr(self.identity, field_name):
                raise ValueError(f"{field_name} is blank — a present-but-empty identity field merges two results")
        if self.identity.return_basis not in RETURN_BASES:
            raise ValueError(
                f"unknown return basis {self.identity.return_basis!r}; must be one of {sorted(RETURN_BASES)}"
            )
        if self.identity.window_end < self.identity.window_start:
            raise ValueError(f"window {self.identity.window_start} → {self.identity.window_end} ends before it starts")
        if self.evaluated_instrument_count < 0:
            raise ValueError(f"evaluated_instrument_count must be >= 0, got {self.evaluated_instrument_count}")
        if self.trial_count is not None and self.trial_count < 1:
            raise ValueError(
                f"trial_count must be >= 1 when declared, got {self.trial_count} — criterion 6 counts abandoned "
                "branches and discarded parameter values, so zero trials is not a state that can be reached"
            )
        # ⚠ THE DSR PROVENANCE BINDS THE TWO SCALARS, ONE WAY ONLY. A `deflated`
        # object present alongside a null or disagreeing `trial_count` /
        # `deflated_sharpe` is the exact row `sql/266`'s all-or-nothing CHECK
        # refuses — caught here, where the message names the field, rather than
        # as an integrity error at write time. The converse is deliberately NOT
        # required: a declared trial count with no DSR yet is a real state and
        # the gate has a refusal for it.
        if self.deflated is not None:
            if self.trial_count != self.deflated.declared_trials:
                raise ValueError(
                    f"trial_count {self.trial_count} disagrees with the {self.deflated.declared_trials} trials the "
                    "Deflated Sharpe was deflated against — the stored count would not describe the correction"
                )
            if self.deflated_sharpe is None or float(self.deflated_sharpe) != self.deflated.deflated_sharpe:
                raise ValueError(
                    f"deflated_sharpe {self.deflated_sharpe} disagrees with the computed "
                    f"{self.deflated.deflated_sharpe} — two copies of one number is how they diverge"
                )
            # ⚠⚠ ONE SAMPLE SIZE, ONE COLUMN. `sql/266` gives the DSR no
            # `effective_sample_size` of its own — it consumes criterion 3's,
            # and `result_ledger` therefore rebuilds this field FROM that
            # column. Without this check a caller could deflate against one
            # sample size and store a row declaring another, and the round trip
            # would silently replace the first with the second: the stored DSR
            # would then be a number no stored input produces. Caught by
            # `tests/test_strategy_holdout_namespace.py`'s criterion-6 round
            # trip, which is what surfaced it.
            if self.deflated.effective_sample_size != self.metrics.effective_sample_size:
                raise ValueError(
                    f"the Deflated Sharpe was computed on an effective sample size of "
                    f"{self.deflated.effective_sample_size} but the metric set carries "
                    f"{self.metrics.effective_sample_size} — criterion 6 consumes criterion 3's number, and there is "
                    "only one column for it"
                )
        # ⚠⚠ THE CONTROL'S STRATEGY-SIDE FIGURES ARE THE ROW'S OWN, and this is
        # the same defect the DSR binding above catches: a control evaluated
        # against one Sharpe and stored beside another describes a comparison
        # nobody made. `sql/268` cannot catch it — it stores only the cohort
        # side, deliberately — so the binding has to hold here.
        if self.synthetic_control is not None:
            control = self.synthetic_control
            if control.strategy_sharpe != self.metrics.sharpe:
                raise ValueError(
                    f"the synthetic control was evaluated against a Sharpe of {control.strategy_sharpe} but the "
                    f"metric set carries {self.metrics.sharpe} — the stored verdict would describe a comparison "
                    "against a number this row does not report"
                )
            if control.strategy_return_pct != self.metrics.total_return_pct:
                raise ValueError(
                    f"the synthetic control was evaluated against a total return of {control.strategy_return_pct} "
                    f"but the metric set carries {self.metrics.total_return_pct}"
                )


# ---------------------------------------------------------------------------
# The promotion gate (#2288 clause 4, §6)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PromotionCandidate:
    """A result row plus the three facts the gate needs that are not on it.

    ⚠ Each of the three is OFF the row on purpose:

    - the evaluated instrument ids and the validated universe are SETS of
      thousands, and a row cannot carry either; the gate compares them and the
      row keeps only the count;
    - the hold-out access counts are properties of the STRATEGY's history, not
      of one row — they live in stage 5e's access log;
    - ``ambiguity_material`` is a property of the ARM PAIR (§3.4 computes the
      equity curve twice and compares), so it belongs to neither arm's row.

    ⚠ NOTHING PRODUCES THE HOLD-OUT COUNTS TODAY, so they default to zero and
    the gate refuses. That is §6's stated initial state — *"nothing is
    promotable. That is correct, not a bug to work around."*
    """

    result: StrategyResult
    #: The instruments the result was computed over.
    evaluated_instrument_ids: frozenset[int]
    #: ``strategies.validated_universe.load_validated_universe`` at check time.
    #: ⚠ Passed IN rather than loaded here: the gate is pure, and phase 7's
    #: guard must be able to evaluate it without a database round-trip in the
    #: order path.
    validated_universe_ids: frozenset[int]
    #: How many times this strategy's hold-out arm has been evaluated (5e).
    holdout_evaluations: int = 0
    #: How many of those evaluations have a recorded access (timestamp +
    #: strategy id), per criterion 5.
    recorded_accesses: int = 0
    #: §3.4: ``True`` when the two ambiguity arms' Sharpe differ by more than
    #: the gap between the strategy and the random cohort's 95th percentile.
    #: ⚠ ``None`` means the comparison was never run, which is refused
    #: separately from a comparison that came back material — "not measured" and
    #: "measured and bad" are different states and collapsing them is how a
    #: phase ships that cannot demonstrate it works.
    ambiguity_material: bool | None = None
    #: Criterion 9 (stage 5e-5a): ``True`` once this strategy's masked and
    #: admitted arms have both been measured and their delta reported.
    #: ⚠ A BOOLEAN AND NOT A MAGNITUDE. Criterion 9 asks for the exclusion to be
    #: visible, not for it to be small, and no source rule anywhere fixes a
    #: "large enough to block" cut — so the gate refuses on the comparison being
    #: missing and stops there. Compare ``ambiguity_material`` directly above,
    #: which DOES carry a magnitude verdict because §3.4 declares one.
    quarantine_arms_compared: bool = False
    #: #2505's compact viability and edge-attribution record. A result without
    #: it may be reproducible backtest evidence but cannot be capital evidence.
    promotion_evidence: PromotionEvidence | None = None


#: The version under which ``structural_promotion_refusals`` was computed.
#: ⚠ Bump this whenever that function's RULE changes — not when its comments do.
#: A preregistration freezes its expected refusals under a named version, and
#: #2599 refuses a declaration frozen under a superseded one rather than
#: re-interpreting it. Same shape as ``trial_register_superseded`` above.
#:
#: ⚠ Bumped by #2363, which split the single cost refusal in two. A declaration
#: frozen under ``-v1`` expected at most one cost code and cannot be
#: re-interpreted under a rule that may emit two, so it is refused —
#: `strategy_live_gate.assess_live_gate` reads the same verdict and will drop
#: such a trial's forward-shadow floor. ⚠⚠ That refusal is PERMANENT for the
#: affected trial: declarations are immutable, undeletable and unique on
#: (strategy_id, strategy_version), so the remedy is a new ``strategy_version``,
#: not a re-freeze. Measured 2026-08-12 before the bump —
#: ``select count(*) from strategy_preregistration_declarations`` returned 0, so
#: no trial pays that cost here.
STRUCTURAL_REFUSAL_POLICY_VERSION: Final = "structural-refusal-policy-2026-08-12-v2-carry-fx-split"


def structural_promotion_refusals(
    *, universe_basis: str | None, carry_unmodelled: bool, fx_unmodelled: bool
) -> tuple[PromotionRefusal, ...]:
    """The refusals fully determined by the corpus/cost stamps a run will carry.

    ⚠ THE POINT OF THE EXTRACTION IS THAT THERE IS ONE COPY. #2599 needs to know,
    at preregistration-freeze time, whether a trial is structurally unpromotable
    before it starts — which is exactly this subset of ``check_promotable``, and
    a second hand-written copy of it would drift silently the first time the
    corpus rule changed.

    ⚠ These four and no others. Every remaining refusal in the vocabulary
    depends on what the run PRODUCES (a Deflated Sharpe, a synthetic control, an
    evaluated instrument set), so it cannot be known at freeze time and must not
    be pre-declared.
    """
    refusals: list[PromotionRefusal] = []

    # §6 clause 2 — basis missing, or survivor_only. #2288: "An unlabelled
    # result is treated as survivor_only, never as validated."
    #
    # ⚠ Two codes rather than one. They are the same verdict and different
    # operator actions: absent means the WRITER is broken, and survivor_only
    # means the CORPUS is (and #2284's purchase is the fix).
    if not universe_basis:
        refusals.append("universe_basis_absent")
    elif universe_basis not in PROMOTABLE_UNIVERSE_BASES:
        refusals.append("universe_basis_not_survivorship_free")

    # §5.1 — carry and FX are NULL, not zero, so a result charging neither is
    # not promotable. Read off the ROW, never off `cost_model` (see the field).
    #
    # ⚠ TWO INDEPENDENT CLAUSES, AND-COMPLETE (#2363). Either one alone refuses,
    # so promotion is exactly as hard as it was under the coupled flag; the
    # split only makes the reason legible and lets the first evidence to arrive
    # close its own half.
    if carry_unmodelled:
        refusals.append("carry_unmodelled")
    if fx_unmodelled:
        refusals.append("fx_unmodelled")

    return tuple(refusals)


def purpose_promotion_refusals(purpose: str | None) -> tuple[PromotionRefusal, ...]:
    """A harness-validation control is never promotable, whatever it measured.

    ⚠ EXTRACTED SO THE TRANSITION CAN REPLAY IT (#2639). ``promote_strategy``
    refuses on ``registered_strategy_purpose`` — the MANIFEST's purpose — while
    the pinned ROW carries its own stamped one, and nothing compared them. They
    agree today (all 324 stored rows and all four registered strategies are
    ``harness_validation``, measured 2026-08-13), but the moment a manifest entry
    becomes ``capital_candidate`` its older harness rows are pinnable and the
    transition would not notice.
    """
    return ("harness_validation_only",) if purpose == "harness_validation" else ()


def holdout_count_promotion_refusals(
    *, holdout_evaluations: int, recorded_accesses: int
) -> tuple[PromotionRefusal, ...]:
    """Criterion 5, from the two counts ``holdout_access_counts`` returns (#2639).

    ⚠ IMPLEMENTED STRICTER THAN THE LITERAL WORDING, deliberately. Read
    literally, "more than once" would let a SINGLE unrecorded evaluation pass —
    and a single unrecorded look at the hold-out is exactly the governance
    failure criterion 5 describes, just the first one. The rule applied is that
    every evaluation must have an access record.

    ⚠⚠ THE TRANSITION REPLAYS THIS AGAINST TODAY'S COUNTS, NOT AGAINST A FROZEN
    PAIR, and freezing would DEFEAT the criterion. Both counts are scoped to
    ``(strategy_id, strategy_version)``, so a pair frozen when result #1 was
    written records the looks that had happened by then: a strategy that later
    evaluates its hold-out four more times without recording would replay
    result #1 as ``(1, 1)`` — consistent, promotable, and blind to precisely the
    repeated unlogged look this clause exists to catch. The reasoning is
    recorded in full in ``app.services.strategy_promotion_replay``.
    """
    if holdout_evaluations < 1:
        return ("holdout_never_evaluated",)
    if recorded_accesses < holdout_evaluations:
        return ("holdout_accesses_unrecorded",)
    return ()


def deflation_promotion_refusals(
    *,
    deflated_sharpe: object | None,
    trial_count: int | None,
    deflated: DeflatedSharpeResult | None,
    effective_sample_size: float | None,
) -> tuple[PromotionRefusal, ...]:
    """Criteria 6 and 3, from the values a stored row also carries (#2639).

    ⚠ THE ONE COPY, called by ``check_promotable`` and by the transition's
    ``result_ledger.stored_result_promotion_refusals`` — the extraction argument
    ``structural_promotion_refusals`` makes. A second hand-written copy would
    drift the first time the deflation rule changed.

    ⚠ FOUR INDEPENDENT ``if``s, never an ``elif`` and never an early return. A
    DSR with no trial count is as refused as no DSR at all, and both may fire at
    once; the gate's contract is that every reason is returned.

    ⚠ ``trial_register_superseded`` is guarded on ``deflated_sharpe is not
    None``, NOT on ``deflated``. A row with a probability but no reconstructed
    object is exactly the state the clause is for, and guarding on the object
    would let it pass.

    ⚠ ``deflated_sharpe`` is typed ``object`` because the only thing done with
    it is a ``None`` test: in memory it is a float, off a stored row it is a
    psycopg ``Decimal``, and narrowing the type here would force a conversion
    that the clause does not need and that could raise where the gate refuses.
    """
    refusals: list[PromotionRefusal] = []

    # Criterion 6 — "DSR not computed, or computed on an undeclared trial
    # count". Independent checks: a DSR with no trial count is as refused as no
    # DSR at all, because the count is what the deflation divides by.
    if deflated_sharpe is None:
        refusals.append("deflated_sharpe_not_computed")
    if trial_count is None:
        refusals.append("trial_count_undeclared")
    if deflated_sharpe is not None and (
        deflated is None
        or deflated.trial_register_version != TRIAL_REGISTER_VERSION
        or deflated.declared_trials != TRIAL_REGISTER.declared_count
    ):
        refusals.append("trial_register_superseded")

    # Criterion 3 — the effective sample size that criterion 6's deflation
    # consumes. ⚠ Checked SEPARATELY from the DSR: a DSR present with no
    # effective sample size is a DSR deflated on a nominal n, and criterion 3
    # forbids reporting a nominal n anywhere. Stage 5e's block bootstrap fills
    # it; until then this refusal fires on every result.
    if effective_sample_size is None:
        refusals.append("effective_sample_size_not_computed")

    return tuple(refusals)


def synthetic_control_promotion_refusals(control: SyntheticControl | None) -> tuple[PromotionRefusal, ...]:
    """§9's acceptance, from the control a stored row also carries (#2639).

    ⚠ THE COHORT-LEVEL AND STRATEGY-LEVEL FAILURES ARE REPORTED SEPARATELY AND
    BOTH CAN FIRE: a cohort that shows edge invalidates the scale, and a Sharpe
    below the threshold is not evidence on any scale. Returning only the first
    would hide from an operator that one broken cohort is blocking every
    strategy.

    ⚠ DERIVED FROM THE CONTROL'S OWN PROPERTIES, never from the row's stored
    ``synthetic_control_passed``. That column is the CONJUNCTION, so reading it
    would collapse the two codes into one and lose which threshold failed.
    """
    if control is None:
        return ("synthetic_control_not_run",)
    refusals: list[PromotionRefusal] = []
    if not control.mean_return_ci_contains_zero:
        refusals.append("synthetic_control_cohort_shows_edge")
    if not control.sharpe_exceeds_cohort:
        refusals.append("synthetic_control_sharpe_below_cohort")
    return tuple(refusals)


def check_promotable(candidate: PromotionCandidate) -> tuple[PromotionRefusal, ...]:
    """Every reason this result may not be promoted. Empty means promotable.

    Pure — reads no database, raises nothing, and returns ALL refusals rather
    than the first. §6 clause 4's five bullets, plus §3.4's two, in the spec's
    own order so a missing check is visible as a missing block.

    ⚠ FAIL CLOSED IS AN ALLOWLIST, NOT A DENYLIST. The universe check asks
    whether the basis is in ``PROMOTABLE_UNIVERSE_BASES`` (one member), so a
    typo, a future label and ``survivor_only`` are all refused identically.
    Enumerating the bad values would let a value nobody anticipated through.
    """
    refusals: list[PromotionRefusal] = []
    result = candidate.result

    refusals.extend(purpose_promotion_refusals(result.purpose))

    # §6 clause 2 (universe basis) and §5.1 (carry). Both are decided by the
    # stamps alone, so they live in `structural_promotion_refusals` — the same
    # function #2599's preregistration freeze calls, which is what keeps the
    # frozen expectation and the gate from drifting apart.
    refusals.extend(
        structural_promotion_refusals(
            universe_basis=result.universe_basis,
            carry_unmodelled=result.carry_unmodelled,
            fx_unmodelled=result.fx_unmodelled,
        )
    )

    # §6 — "instrument outside the §4.0 validated universe". §4.0's allocation
    # invariant 2 is a universe rule, not only a survivorship one.
    #
    # ⚠ An EMPTY evaluated set is refused separately and is not vacuously fine:
    # `set() - anything` is empty, so a result over no instruments would sail
    # through the subset test while being no evidence at all.
    if not candidate.evaluated_instrument_ids:
        refusals.append("no_instruments_evaluated")
    elif candidate.evaluated_instrument_ids - candidate.validated_universe_ids:
        refusals.append("instrument_outside_validated_universe")

    # Criterion 5 — "hold-out never evaluated, or evaluated more than once
    # without a recorded access", in `holdout_count_promotion_refusals` so the
    # promotion transition applies the same single copy (#2639).
    refusals.extend(
        holdout_count_promotion_refusals(
            holdout_evaluations=candidate.holdout_evaluations,
            recorded_accesses=candidate.recorded_accesses,
        )
    )

    # Criteria 6 and 3, in `deflation_promotion_refusals` — the same single copy
    # the promotion transition replays off the stored row (#2639), for the
    # reason `structural_promotion_refusals` above is shared.
    refusals.extend(
        deflation_promotion_refusals(
            deflated_sharpe=result.deflated_sharpe,
            trial_count=result.trial_count,
            deflated=result.deflated,
            effective_sample_size=result.metrics.effective_sample_size,
        )
    )

    # §3.4 — the ambiguity arms. ⚠ NOT one of §6's five bullets; its source is
    # §3.4's "the result is `ambiguity_material` and is not promotable", and it
    # is enforced here rather than left in prose because a rule with no gate is
    # the thing #2288 clause 4 exists to prevent.
    if candidate.ambiguity_material is None:
        refusals.append("ambiguity_arms_not_compared")
    elif candidate.ambiguity_material:
        refusals.append("ambiguity_material")

    # Criterion 9 — the sensitivity arm. ⚠ ONE refusal, not two: the criterion
    # requires the exclusion measured and reported, and declares no size at
    # which it blocks. See the `quarantine_arms_not_compared` comment on
    # PromotionRefusal for why a `quarantine_material` twin is absent.
    if not candidate.quarantine_arms_compared:
        refusals.append("quarantine_arms_not_compared")

    # §9 — the harness's own acceptance, read per result. Shared with the
    # transition's replay (#2639); the two-code split is argued there.
    refusals.extend(synthetic_control_promotion_refusals(result.synthetic_control))

    # #2505 — positive mean return is not enough. The candidate must have a
    # positive clustered lower bound, measured tails/concentration, complete
    # executable costs, calibrated ranking and same-path challenger attribution.
    evidence = candidate.promotion_evidence
    if evidence is None:
        refusals.append("promotion_evidence_missing")
    else:
        refusals.extend(
            cast(
                tuple[PromotionRefusal, ...],
                evidence_refusals(
                    evidence,
                    profit_factor=result.metrics.profit_factor,
                    as_of=result.identity.window_end,
                ),
            )
        )

    return tuple(refusals)


def is_promotable(candidate: PromotionCandidate) -> bool:
    """``check_promotable`` with the reasons discarded.

    ⚠ Provided for a caller that genuinely only branches, and named so the
    reason-losing is visible at the call site. Phase 7's guard must use
    ``check_promotable`` — ``execution_guard`` writes one audited row per
    invocation and a bare ``False`` gives it nothing to write.
    """
    return not check_promotable(candidate)


#: Everything a writer must stamp on a row that today's pipeline cannot make
#: promotable, gathered so 5d does not have to rediscover which constants go
#: where. ⚠ NOT a default: it is passed explicitly, because #2288 clause 2's
#: whole argument is that a column with a default is a column a writer can
#: forget.
CURRENT_RESULT_PROVENANCE: Mapping[str, object] = {
    "universe_basis": "survivor_only",
    "corpus_version": CORPUS_VERSION,
    "cost_model_id": COST_MODEL_ID,
    "metric_set_id": METRIC_SET_ID,
    "sizing_rule": SIZING_RULE,
    "benchmark_rule": BENCHMARK_RULE,
    "return_basis": TOTAL_RETURN_BASIS,
    "window_start": EVALUATION_WINDOW_START,
    "window_end": EVALUATION_WINDOW_END,
}


__all__ = [
    "AMBIGUITY_ARMS",
    "BENCHMARK_RULE",
    "CORPUS_FROZEN_LAST_BAR",
    "CORPUS_VENDORS",
    "CORPUS_VERSION",
    "LEGACY_RETURN_BASIS",
    "RETURN_BASES",
    "TOTAL_RETURN_BASIS",
    "TOTAL_RETURN_RESULT_SET_ID",
    "CURRENT_RESULT_PROVENANCE",
    "EVALUATION_WINDOW_END",
    "EVALUATION_WINDOW_START",
    "HOLDOUT_BOUNDARY",
    "HOLDOUT_WEIGHTING",
    "PROMOTABLE_UNIVERSE_BASES",
    "PROMOTION_REFUSALS",
    "RESULT_NAMESPACES",
    "RESULT_SCOPES",
    "RESULT_SET_ID",
    "SIZING_RULE",
    "STRUCTURAL_REFUSAL_POLICY_VERSION",
    "UNIVERSE_BASES",
    "AmbiguityArm",
    "PromotionCandidate",
    "PromotionRefusal",
    "ResultIdentity",
    "ResultNamespace",
    "ResultScope",
    "SignalNamespace",
    "StrategyResult",
    "UniverseBasis",
    "check_promotable",
    "deflation_promotion_refusals",
    "holdout_count_promotion_refusals",
    "is_promotable",
    "purpose_promotion_refusals",
    "structural_promotion_refusals",
    "synthetic_control_promotion_refusals",
    "namespace_for_bar",
    "namespace_for_position",
    "namespace_for_signal",
]
