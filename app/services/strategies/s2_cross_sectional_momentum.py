"""S-2 — cross-sectional momentum (12-1). The catalogue's ranked strategy.

Parent spec: ``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md``
§4 (S-2), §3.5 (execution semantics), §4.0 (validated universe), §9 Q2/Q3,
§5 criteria 1, 4, 8, 9 and 11. Contract:
``app/services/strategy_registry.py`` — ``evaluate_cross_sectional`` (the
cross-sectional half of phase 3a, added for this strategy). Design:
``docs/proposals/ta/2026-08-06-cross-sectional-contract-and-s2.md``.
Refs #2240, #2288, #2289.

THE RULE, VERBATIM FROM §4
--------------------------
    Rebalance trigger, defined causally: the **first bar whose calendar month
    differs from the previous bar's** — i.e. act at the start of the new month.
    Ranking uses return over ``t-252 .. t-21`` (skipping the last ~month, which
    reverses); hold the top decile; fill at ``open(t+1)``.
    Params: 3 (lookback, skip, decile).
    Eligibility, evaluated as-of each rebalance date (§3.5 rule 5): ≥273 bars
    of history at that date.

SOURCE RULE FOR THE WINDOW
--------------------------
The skip-a-month formation window is not ours and is not inferred: it is the
**prior (2-12) return** of the Fama-French momentum factor — cumulate eleven
months, skip the most recent — per the Ken French data library's construction
note (fetched 2026-08-06), and Jegadeesh & Titman (1993) before it. In bars that
is ``close(t-21) / close(t-252) - 1``, which is §4's window read literally.

⚠ WHAT IS *NOT* BORROWED FROM FAMA-FRENCH, stated because citing the factor for
the window invites assuming the rest. FF sorts on NYSE breakpoints, within size
buckets, value-weighted, on monthly returns. This ranks **every eligible name in
the §4.0 validated universe equally**, on daily bars, at a plain top-decile cut.
The window is theirs; the portfolio construction is §4's.

⚠ THE PARENT'S TWO NUMBERS DISAGREE AND BOTH ARE HONOURED.
The window needs 253 bars (index ``t-252`` must exist); the stated eligibility is
273. 273 = 252 + 21, i.e. it was computed as though the window ran
``t-273 .. t-21``. Taking the window literally (which is also the published form)
and the eligibility literally is the only reading that contradicts neither
sentence, so both ship: score from ``t-252``, refuse until 273 bars. It is a
20-bar-per-series NARROWING and is counted on the full population by
``scripts/verify_2240_s2_cross_sectional.py --census``, never asserted harmless.

⚠ THIS MODULE NEVER RESOLVES A FILL, AND CANNOT.
A ``StrategySignal`` carries a bar index and no fill field (3a's module
docstring); ``signal_ledger.resolve_fills`` turns the index into ``open(t+1)``.
``s2_select`` is handed a date and a mapping of scores — it cannot name a bar, a
price, or anything after ``t``.

⚠ ONE LEG, NOT TWO. "Hold the top decile" makes an exit the exact complement of
the entry **over the participants at a rebalance bar**, so an exit row could
never disagree with the entry row beside it — a second copy of one fact on a
ledger keyed to carry both. S-1's and S-3's exits are not complements of their
entries (both legs can be false on the same bar), which is why they have two.
Pairing an entry with the rebalance that ends it — including collapsing a name
selected in consecutive months into one hold rather than two entries — is phase
5's. S-2 therefore declares **no** ``max_hold_bars``: its hold is *"until the
next rebalance"*, a calendar fact, and approximating it as 21 bars would invent
a parameter §4 does not give.

⚠ PRICE RETURNS, NOT TOTAL RETURNS — §4 says so explicitly, and the corpus
agrees: the dividend-adjusted series lives in ``research_price_daily.adj_close``
and nothing here reads it. Reading the price close is therefore the
spec-conformant choice rather than a convenient one. It systematically
understates high-yield names over an 11-month lookback, which §4 also says.

⚠⚠ THE TWO CLOSES THIS MODULE READS ARE ON DIFFERENT BASES ON PURPOSE (#2834
§7 slice C). An earlier version of this paragraph said ``close`` "is the
SPLIT-adjusted close ... (sql/251)". That cited the WRONG VENDOR: `sql/251` is
the `paperswithbacktest` archive, and the `icyDenev/Intrader` half of the same
table is ``adjustment_basis = 'unadjusted'`` — 5,151 of the 10,415 series in the
§4.0 universe, measured 2026-09-21. A ratio taken across a split on those bars
is not a return, it IS the split.

So the module takes two aligned series and reads a different one for each
question, because they are different questions:

* the momentum RATIO reads ``ratio_basis`` — split-corrected, and point-in-time
  safe because the anchor's future factors cancel in a quotient
  (``research_split_corrected_reader``, which proves it);
* the ``MIN_CLOSE`` floor and every evaluability test read the AS-TRADED bars,
  because a corrected level embeds future events and is not a price anyone
  quoted. See ``MIN_CLOSE``.

A caller whose corpus is already split-adjusted passes the same series twice,
and that is a declaration rather than a shortcut.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Set
from datetime import date
from pathlib import Path

from app.services.indicator_series import BarSeries, IndicatorSeries, Universe
from app.services.strategy_registry import (
    NOT_EVALUABLE_REASONS,
    CrossSectionalMember,
    NotEvaluableReason,
    StrategyIdentity,
    StrategyInput,
    StrategySignal,
    evaluate_cross_sectional,
)

S2_STRATEGY_ID = "s2-cross-sectional-momentum"

#: §4's three free parameters. ⚠ FIXED, NEVER TUNED (§6: *"Forbidden —
#: continuous re-optimisation"*). Module constants rather than arguments, for
#: S-1's reason: a period that can be passed in is a period that can be swept,
#: and criterion 11 would then need every swept value registered as its own
#: strategy.
LOOKBACK_BARS = 252
SKIP_BARS = 21
DECILE = 10

#: §4's as-of eligibility. First eligible index is ``ELIGIBILITY_BARS - 1`` =
#: 272 — a bar is eligible when ``i + 1 >= 273`` bars of history exist including
#: itself. Written mechanically because "≥273 bars of history at that date" has
#: an off-by-one in it either way it is read.
ELIGIBILITY_BARS = 273

#: §9 Q3's price floor, evaluated as-of the decision bar (§3.5 rule 5).
#:
#: ⚠⚠ §9 Q3 IS AN OPEN QUESTION WITH A RECOMMENDATION, AND THE RECOMMENDATION IS
#: WHAT SHIPS: *"≥273 bars and close ≥ $1, both evaluated as-of each decision
#: date"*, on the evidence that #2266 measured sub-$1 names running to 800× p99.99
#: daily moves on tick quantisation alone. Momentum ranks on extremes, so a
#: tick-quantised penny name is not a rare contaminant of the top decile — it is
#: the top decile. It is hashed into the identity, so reversing it later is a new
#: strategy version rather than a silent redefinition.
#:
#: ⚠⚠ THE FLOOR READS THE AS-TRADED CLOSE, NEVER THE SPLIT-CORRECTED ONE —
#: #2834 §7 item 3, settled here. Two independent reasons, either sufficient:
#:
#: 1. **THE CORRECTED LEVEL THIS CORPUS PRODUCES IS LOOK-AHEAD.**
#:    ``research_split_corrected_reader`` anchors every series at the END of the
#:    loaded window, so for any earlier bar ``scale(d)`` is a product over
#:    events AFTER ``d``. Comparing that level against a constant tests
#:    something no observer knew at ``d`` — including that the name would later
#:    reverse-split, the most informative single fact about a distressed penny
#:    stock. A corrected RATIO is safe because the anchor cancels; the level is
#:    not, and this constant is the level test.
#:    ⚠ Narrower than "a restated level is never point-in-time", which is false
#:    — a level restated using only events known by ``t`` is available at ``t``.
#:    The claim is about the anchor we actually build.
#: 2. **§9 Q3 IS WRITTEN AS A QUOTED-DOLLAR RULE.** *"close >= $1, evaluated
#:    as-of each decision date"* — a dollar amount as-of a date is the amount
#:    that was quoted on it. The estimand names the basis; the basis is not
#:    inferred from a mechanism.
#:
#: ⚠⚠ A REG NMS RULE 612 CITATION STOOD HERE AND IS WITHDRAWN (Codex ckpt-1,
#: 2026-09-21). The argument was that #2266's tick-quantisation evidence is
#: keyed on the quoted price because Rule 612 sets a $0.01 minimum increment at
#: or above $1.00 and $0.0001 below. Every clause of that is true and the
#: INFERENCE does not hold:
#:
#: * Rule 612 governs quotations, orders and indications of interest — not
#:    archive rounding, and not every execution price (sub-penny executions are
#:    permitted). So it cannot establish that the artefact #2266 MEASURED
#:    originated in regulated quote ticks rather than in vendor rounding, bad
#:    prints or residual split error.
#: * Relative tick coarseness does not jump the way the argument needs: just
#:    below $1.00 the minimum increment is ~0.01% of price, at $1.00 it is ~1%.
#:    The rule does not say "below $1 is coarser" at the boundary at all.
#: * Its compliance date is 2006-01-31 and its scope is NMS stock, so it reaches
#:    neither the corpus's pre-2006 bars nor its non-NMS instruments. (The
#:    half-cent amendment is 2024, not 2025 as the withdrawn note said.)
#:
#: This is precisely the failure `docs/review-prevention-log.md` already records
#: for this same rule — *a correct citation aimed at a question it does not
#: govern*, which reads as source-rule compliance to every reader including the
#: author. It is recorded rather than deleted because the log's own test ("did a
#: reg get cited for the right question?") passed on the way in, and the thing
#: that caught it was an adversarial read, not the checklist.
#:
#: Reason 1 does not depend on any of it and is sufficient on its own.
#:
#: Measured consequence, full population, 50,134,060 bars — reproduce with
#: ``PYTHONPATH=. uv run python -m scripts.measure_2834_split_adjustment --floor``:
#: reading the corrected close instead would move 1,196,262 bar-slots (2.386%)
#: across this threshold — **949,062 admitted-that-are-now-rejected against
#: 247,200 the other way**, so BOTH directions are populated and the corrected
#: basis is the more permissive one on net.
#: ⚠ BAR-SLOTS ARE NOT DECISIONS. They overweight long histories and are not
#: rebalance-eligible bars, evaluable names or decile members, and a change in
#: one name's eligibility moves every other name's rank. Treat this as the
#: blast radius of the basis choice, never as its strategy impact — that number
#: is owed by the per-regime readout and does not exist yet.
#:
#: ⚠ ONE HALF OF THE CORPUS CANNOT HONOUR THIS AND THAT IS NOT FIXED HERE.
#: ``research_price_series`` carries 5,264 `split_adjusted` series in the §4.0
#: universe against 5,151 `unadjusted` ones (measured 2026-09-21). On the
#: `split_adjusted` half the stored close IS restated and no as-traded close
#: exists, so a name that traded at $0.20 before a 1-for-10 reverse split
#: appears at $2.00 and passes a floor it would have failed at the time.
#: ⚠ An earlier draft said that bias "runs one way". It does not: a REVERSE
#: split admits history the as-traded basis would reject, a FORWARD split
#: rejects history it would admit, and the corpus has both. What is true is
#: only that the reverse direction is the larger one on the measured counts
#: above. The bias is unchanged by this decision either way; it is stated
#: rather than quietly enjoyed, and the count the floor DOES reject is measured
#: by ``--census``.
#:
#: ⚠ The prior version of this note cited `sql/251` for "the corpus's OHLC carry
#: the split adjustment" and said "unadjusting would need per-series split
#: factors the corpus does not store". Both were wrong for the `unadjusted`
#: half: `sql/251` is the OTHER vendor, and `fc72804b` stores the factors.
MIN_CLOSE = 1.0

#: The smallest cross-section a decile is defined on — **by construction, not
#: from a published rule**, exactly as S-4's "bottom quartile" had to be. Below
#: ten names, ``N // 10`` is zero and no name can be in the top tenth of the
#: panel, so every participant that date is ``not_evaluable(thin_cross_section)``
#: rather than a fake ``not_fired``.
MIN_CROSS_SECTION = DECILE

#: ⚠ Six entries for §4's "Params: 3". The three free parameters are the first
#: three; the rest are by-construction constants recorded so the identity hash
#: moves if any of them is edited, and so a reader does not have to diff the
#: source to see them. ``max_hold_bars`` is deliberately ABSENT — see the module
#: docstring.
S2_PARAMS: Mapping[str, object] = {
    "lookback_bars": LOOKBACK_BARS,
    "skip_bars": SKIP_BARS,
    "decile": DECILE,
    "eligibility_bars": ELIGIBILITY_BARS,
    "min_close": MIN_CLOSE,
    "min_cross_section": MIN_CROSS_SECTION,
}


def _source_hash() -> str:
    """Hash of THIS module — the ``source_hash`` half of criterion 11."""
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


def s2_identity(*, universe: Universe, cost_model_id: str) -> StrategyIdentity:
    """The registered identity of S-2 on one universe under one cost model.

    Both arguments are required and neither has a default, for the reason S-1
    gives at length: criterion 11 puts universe and cost model *inside* the
    identity, so a default would silently register a strategy the caller never
    declared. The model is ``app.services.cost_model.COST_MODEL_ID`` (stage 5b);
    it stays an argument rather than a module constant for the reason S-1 gives.
    """
    if not cost_model_id.strip():
        raise ValueError(
            "cost_model_id must be a non-empty declaration (criterion 11 hashes it); "
            "pass app.services.cost_model.COST_MODEL_ID rather than an empty string"
        )
    return StrategyIdentity(
        strategy_id=S2_STRATEGY_ID,
        params=S2_PARAMS,
        universe=universe,
        cost_model_id=cost_model_id,
        source_hash=_source_hash(),
    )


def rebalance_dates(calendar: Iterable[date]) -> frozenset[date]:
    """First WEEKDAY bar of each new month, from the panel's union calendar.

    §4: *"the first bar whose calendar month differs from the previous bar's —
    i.e. act at the start of the new month"*. Causal by construction: the last
    session of a month is not knowable at that session (you cannot tell the 30th
    is the last until the 31st fails to appear), which is why the rule triggers
    at the start of the new month and not the end of the old one.

    ⚠ THE CALENDAR IS THE PANEL'S, NOT ONE MEMBER'S, AND THAT IS A READING.
    §4's wording is per-series. Read that way, a name that resumes trading on the
    4th after a halt rebalances on the 4th and ranks against whoever else
    happened to resume that day — a cross-section of two, and a decile of none.
    Evaluating the same rule on the union of the panel's bar dates keeps one
    rebalance date for everyone, is equally causal (it reads only dates that have
    happened), and is stated here because it is a reading rather than a
    quotation.

    ⚠⚠ SATURDAYS AND SUNDAYS ARE DROPPED BEFORE THE MONTH RULE RUNS (#2797).
    Source rule: the validated universe is US-listed stock (§4.0) and US equity
    venues hold no regular weekend sessions (NYSE/Nasdaq holiday-and-hours
    calendars), so a weekend row is a corpus artefact and not a bar §4 can mean.
    ``price_daily`` carries them anyway, and because the FIRST qualifying bar
    takes the month, one artefact hands the whole month's rebalance to a handful
    of names and the real first trading day then never rebalances at all.

    Measured on the validated universe at 2026-08-20 (6,774 instruments,
    3,673,648 bars): **3,669 weekend bars** across 389 instruments and 329
    distinct weekend dates — 0.0998% of the corpus, controlling **13 of 73**
    rebalance dates (17.8%). Reproduce::

        select count(*), count(distinct instrument_id), count(distinct price_date)
        from price_daily where extract(isodow from price_date) >= 6;

    ⚠ TWO POPULATIONS ANSWER "HOW BIG WAS THE PANEL", AND ONLY ONE OF THEM
    DECIDES ANYTHING. The query above counts raw ``price_daily`` rows; the
    number this rule is judged on is the DECISION population — masked bars, past
    the 273-bar warm-up, above the $1 floor — which is what
    ``scripts/ab_2797_s2_weekday_rebalance.py`` reports and what
    ``MIN_CROSS_SECTION`` is compared against. On that population the 13 weekend
    dates ranked **0–11** names and yielded **7** entry signals across five
    years; the 13 weekday dates that replace them rank up to **3,346** and yield
    **2,419**. Sat 2026-08-01 ranked **0** and Mon 2026-08-03 ranks **3,204**,
    which is the whole of S-2's zero fired signals in production. Quoting the raw
    row count here instead would state a larger, truer-sounding number that no
    gate reads.

    ⚠ The failure this removes was SILENT: the junk instruments are not
    frontier-eligible, so the scan wrote no row of any kind for 2026-08-01 — not
    even a ``thin_cross_section`` refusal. An absent month and a quiet month
    render identically.

    ⚠ A corpus-hole WEEKDAY still takes its month and is then refused by
    ``MIN_CROSS_SECTION``; that month simply does not rebalance. Deliberately not
    fixed here, for S-10's reason: teaching this pure function participation
    counts it cannot verify is worse than a self-healing hole.

    ⚠ DUPLICATED, NOT SHARED, with ``s10_relative_strength_leader
    .s10_rebalance_dates`` — identical rule, two modules, and that is a choice
    rather than an oversight. Both identities move in #2797 regardless (a
    docstring edit moves ``_source_hash``), so co-location was not ruled out on
    cost. What is duplicated is the four-line rule AND this rationale prose; the
    thresholds are NOT — S-2 cuts at ``MIN_CROSS_SECTION`` 10 and 273 warm-up
    bars, S-10 at 1000 with no warm-up constant, deliberately (its own comment
    says "much larger than S-2's 10 on purpose"). So there is no lockstep
    requirement on the constants, and the test below covers the rule but not the
    prose. Co-location is ruled out because the binding that catches drift has to be
    behavioural: two independent implementations checked against each other is
    evidence, whereas a test over one shared import is the tautology this repo
    has already shipped once (prevention log, *"a reference that IMPORTS the
    constant it validates"*). The binding is ``TestNoDriftAgainstS10`` in
    ``tests/test_2797_s2_weekday_rebalance.py``, which compares them over four
    years of DENSE calendar (every date, weekends included) and again over one
    punched with holes — not the real union calendar, which is sparser than
    both and would exercise fewer month-boundary shapes.

    ⚠ The FIRST weekday in the calendar is not a rebalance — there is no previous
    bar for its month to differ from. Unreachable in practice (every member is
    inside its 273-bar warm-up there) and defined anyway, because "unreachable"
    is a property of today's data.
    """
    weekdays = [when for when in sorted(set(calendar)) if when.weekday() < 5]
    return frozenset(
        when
        for previous, when in zip(weekdays, weekdays[1:], strict=False)
        if (when.year, when.month) != (previous.year, previous.month)
    )


def _close_input(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """The bar closes, in the shape the runner checks for evaluability.

    ⚠ S-2 READS ``close(t)`` — the §9 Q3 price floor is evaluated on it — so it
    is declared. That is the one difference from a first draft of this module,
    which read only ``t-21`` and ``t-252`` and therefore would have ranked a name
    on a bar whose own close was quarantined. Declaring it makes such a bar
    ``not_evaluable``, which is what the rest of this codebase does with a masked
    bar (``price_structure._atr_at`` fails closed) and what S-1 and S-3 do.
    """
    closes = series.float_closes
    return IndicatorSeries(
        values=tuple(closes),
        universe=universe,
        not_evaluable_indices=tuple(i for i, value in enumerate(closes) if value is None),
    )


def momentum_series(ratio_basis: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """``close(t-21) / close(t-252) - 1`` per bar, refused before 273 bars.

    ⚠⚠ ``ratio_basis`` IS THE SPLIT-CORRECTED SERIES, NOT THE AS-TRADED ONE
    (#2834 §7 slice C) — the parameter is named for it so a call site that
    passes the wrong one reads wrong. On an `unadjusted` series a window
    spanning a 4:1 split returns a ratio four times too small, which ranks a
    flat name in the bottom decile on the split alone. Passing the as-traded
    series is CORRECT only where it is already split-adjusted, which is a
    property of the vendor and not of this function.

    Two kinds of absence, kept apart because criterion 8 needs them apart:

    - a bar inside the warm-up is ``None`` and NOT in ``not_evaluable_indices``,
      which the runner reports as ``insufficient_warmup``;
    - a bar whose window closes are missing (masked) or **non-positive** is in
      ``not_evaluable_indices``, which the runner reports with the caller's own
      reason code.

    ⚠ THE NON-POSITIVE GUARD IS NOT HYPOTHETICAL AND IS NOT REACHED THROUGH THE
    MASKED LOADER. **Every** ``research_price_daily`` bar with ``close <= 0`` is
    already quarantined (``return_usable = false``), so ``load_masked_series``
    hands this function ``None`` for all of them; ``price_daily`` carries a much
    larger population that a raw-bar caller such as the ``--equivalence`` arm
    does reach. Run the queries rather than trusting a count here — an earlier
    draft wrote "two ... bars" beside these very commands and the answer had
    moved to three by 2026-09-21, which is what a hand-written derived statistic
    does next to its own reproduction step::

        select count(*) from research_price_daily where close <= 0;
        select count(*) from price_daily where close <= 0;
        -- the claim above, as a query that returns zero when it holds:
        select count(*) from research_price_daily d
          left join research_bar_quarantine q
            on q.series_id = d.series_id and q.bar_date = d.bar_date
         where d.close <= 0 and coalesce(q.return_usable, true);

    A zero denominator would be a ``ZeroDivisionError`` and a negative one a
    sign-flipped return that ranks like a winner, which is the worse failure of
    the two — it is a plausible number.

    ⚠ THE RATIO IS TAKEN ON THE ``Decimal`` CLOSES AND CONVERTED ONCE (#2834).
    A split-corrected close is ``as_traded / scale`` rounded to 28 digits, so
    converting each close to float first rounds twice and the division a third
    time, and two exactly equal ratios come out an ulp apart. Measured on the
    dev DB, 1997-08-01: series 8918 (two stamps) scored ``0.7500000000000002``
    and series 11616 ``0.75`` on the same ``8.75 / 5`` as-traded pair, so
    ``s2_select`` ranked on arithmetic residue instead of its frozen
    key-ascending tie-break. In ``Decimal`` the shared scale cancels to within
    the 28th digit and the one final ``float()`` rounds that away.
    """
    closes = ratio_basis.closes
    values: list[float | None] = []
    unevaluable: list[int] = []
    for index in range(len(closes)):
        if index < ELIGIBILITY_BARS - 1:
            values.append(None)
            continue
        past = closes[index - LOOKBACK_BARS]
        recent = closes[index - SKIP_BARS]
        if past is None or recent is None or past <= 0 or recent <= 0:
            values.append(None)
            unevaluable.append(index)
            continue
        values.append(float(recent / past - 1))
    return IndicatorSeries(values=tuple(values), universe=universe, not_evaluable_indices=tuple(unevaluable))


def s2_member(
    series: BarSeries,
    *,
    ratio_basis: BarSeries,
    panel_rebalance_dates: Set[date],
    universe: Universe,
    close_reason: NotEvaluableReason,
) -> CrossSectionalMember:
    """One instrument's contribution to the ranked panel.

    ``series`` is the AS-TRADED bars and ``ratio_basis`` the split-corrected
    ones (``research_split_corrected_reader.CorrectedSeries`` carries both,
    aligned). Every level test below — the floor, the evaluability declaration,
    the dates — reads ``series``; only the momentum ratio reads ``ratio_basis``.
    The module docstring says why the split is not cosmetic.

    ⚠ ``ratio_basis`` IS KEYWORD-ONLY AND HAS NO DEFAULT, deliberately, for the
    reason ``research_split_adjustment.split_scales`` gives about
    ``stamps_marker``: a default would let a caller inherit the as-traded basis
    silently, which is the failure this parameter exists to make impossible. A
    caller on an already-split-adjusted corpus passes ``ratio_basis=series``
    and has thereby declared it.

    ``close_reason`` is the code recorded when a close is missing and comes from
    the caller because only the caller knows why: bars from
    ``load_masked_series`` are missing because the quarantine masked them
    (``quarantined_bar``), and a different loader would owe a different code.

    ⚠ THE PRICE FLOOR IS AN ELIGIBILITY RULE, NOT AN EVALUABILITY ONE, so a
    sub-$1 bar is simply not a decision bar and its verdict is ``not_fired``. The
    data is present and the rule is what excludes it — calling that
    ``not_evaluable`` would inflate the refusal counts criterion 9 reads with
    bars that were judged perfectly well.
    """
    if close_reason not in NOT_EVALUABLE_REASONS:
        raise ValueError(f"unknown reason code {close_reason!r}; must be one of {sorted(NOT_EVALUABLE_REASONS)}")
    # ⚠ Dates, not lengths. Two series of equal length drawn from different
    # windows would index-align silently and score a name on another name's
    # calendar; `decision_indices` below indexes `series.dates` while `score`
    # indexes `ratio_basis`, so the two must be the same bars and not merely
    # the same count.
    if series.dates != ratio_basis.dates:
        raise ValueError(
            f"the as-traded series carries {len(series.dates)} bars and the ratio basis "
            f"{len(ratio_basis.dates)}, or they differ in date; they must be the same bars on two bases"
        )

    closes = series.float_closes
    score = momentum_series(ratio_basis, universe=universe)
    return CrossSectionalMember(
        dates=series.dates,
        inputs=(
            StrategyInput(series=_close_input(series, universe=universe), reason=close_reason),
            StrategyInput(series=score, reason=close_reason),
        ),
        score=score,
        decision_indices=frozenset(
            index
            for index, when in enumerate(series.dates)
            if when in panel_rebalance_dates and (close := closes[index]) is not None and close >= MIN_CLOSE
        ),
    )


def s2_select(when: date, scores: Mapping[int, float]) -> frozenset[int]:
    """The top decile of one rebalance date's cross-section.

    ⚠ FIXED BY CONSTRUCTION — "top decile" has no published cut, tie-break or
    small-panel rule, exactly as S-4's "bottom quartile" had none. All three are
    frozen here and hashed into the identity through ``DECILE``:

    - ``k = N // 10``, floor: the largest whole number of names that does not
      exceed a tenth of the panel;
    - ties break on **score descending, then instrument id ascending**. Keys are
      ints for this reason — the rule needs a total order that is the same on
      every run, and dict insertion order is not one;
    - ``N < MIN_CROSS_SECTION`` never reaches here: the runner refuses it as
      ``thin_cross_section``. The ``k == 0`` guard below is the backstop for a
      direct caller.

    ⚠ Exact ties are NOT impossible and the census counts them rather than
    assuming them away. Equal 231-day ratios need only equal endpoint pairs, and
    the corpus is full of low-priced names quantised onto the same few ticks.

    ``when`` is unused by the rule and is in the signature because the contract
    hands it to every selector; a selector that needs the date (a calendar-aware
    cut) must not have to change the contract to get it.
    """
    count = len(scores) // DECILE
    if count <= 0:
        return frozenset()
    ordered = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    return frozenset(key for key, _ in ordered[:count])


def s2_signals(
    panel: Mapping[int, BarSeries],
    *,
    ratio_panel: Mapping[int, BarSeries],
    universe: Universe,
    close_reason: NotEvaluableReason,
) -> dict[int, list[StrategySignal]]:
    """S-2 over a whole panel: one entry verdict per member per bar.

    ``ratio_panel`` is the split-corrected counterpart of ``panel``, keyed
    identically — see ``s2_member``. Required and undefaulted for the same
    reason, and checked key-for-key here because a panel is the one place a
    missing member would otherwise drop a name from the cross-section
    (changing ``N`` and therefore the decile cut) rather than raise.

    ⚠ THIS HOLDS THE WHOLE PANEL IN MEMORY and is the right entry point for a
    bounded one (a watchlist, a test, one sector). A full-corpus sweep must not
    call it — it would materialise every bar of every member at once. That is
    what ``rebalance_dates`` / ``s2_member`` / ``s2_select`` are public for:
    ``scripts/verify_2240_s2_cross_sectional.py`` streams one series at a time
    through the same functions, via the contract's own ``StagedMember``, rather
    than re-implementing the staging pass.
    """
    if panel.keys() != ratio_panel.keys():
        raise ValueError(
            f"the as-traded panel has {len(panel)} members and the ratio panel {len(ratio_panel)}, or they differ "
            "in key; a member present in one and not the other would silently change the cross-section size N "
            "and therefore the decile cut"
        )
    # ⚠ The calendar comes from the AS-TRADED panel. The two bases carry
    # identical dates by construction (`s2_member` enforces it per member), so
    # this is a choice of source and not of content — stated because a reader
    # checking causality should not have to establish that for themselves.
    calendar = {when for series in panel.values() for when in series.dates}
    dates = rebalance_dates(calendar)
    members = {
        key: s2_member(
            series,
            ratio_basis=ratio_panel[key],
            panel_rebalance_dates=dates,
            universe=universe,
            close_reason=close_reason,
        )
        for key, series in panel.items()
    }
    return evaluate_cross_sectional(
        members=members,
        select=s2_select,
        min_participants=MIN_CROSS_SECTION,
    )


__all__ = [
    "DECILE",
    "ELIGIBILITY_BARS",
    "LOOKBACK_BARS",
    "MIN_CLOSE",
    "MIN_CROSS_SECTION",
    "S2_PARAMS",
    "S2_STRATEGY_ID",
    "SKIP_BARS",
    "momentum_series",
    "rebalance_dates",
    "s2_identity",
    "s2_member",
    "s2_select",
    "s2_signals",
]
