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
agrees by construction: ``research_price_daily.close`` is the SPLIT-adjusted
close that is consistent with OHLC, while the dividend-adjusted series lives in
``adj_close`` (sql/251). Reading ``close`` is therefore the spec-conformant
choice rather than a convenient one. It systematically understates high-yield
names over an 11-month lookback, which §4 also says.
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
#: ⚠ ON SPLIT-ADJUSTED CLOSES THIS IS AN ADJUSTED-PRICE FLOOR, NOT A NOMINAL ONE,
#: and the deviation runs one way. sql/251: the corpus's OHLC carry the split
#: adjustment, so a name that traded at $0.20 and later did a 1-for-10 reverse
#: split appears at $2.00 in these bars and passes a floor it would have failed
#: at the time — and reverse splits happen *because* a price fell under $1, so
#: the names it lets through are exactly the distressed ones. Unadjusting would
#: need per-series split factors the corpus does not store. Stated rather than
#: quietly enjoyed; the count the floor DOES reject is measured by ``--census``.
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


def momentum_series(series: BarSeries, *, universe: Universe) -> IndicatorSeries:
    """``close(t-21) / close(t-252) - 1`` per bar, refused before 273 bars.

    Two kinds of absence, kept apart because criterion 8 needs them apart:

    - a bar inside the warm-up is ``None`` and NOT in ``not_evaluable_indices``,
      which the runner reports as ``insufficient_warmup``;
    - a bar whose window closes are missing (masked) or **non-positive** is in
      ``not_evaluable_indices``, which the runner reports with the caller's own
      reason code.

    ⚠ THE NON-POSITIVE GUARD IS NOT HYPOTHETICAL AND IS NOT REACHED THROUGH THE
    MASKED LOADER. Measured 2026-08-06 on the full population: two
    ``research_price_daily`` bars have ``close <= 0`` and **both are already
    quarantined** (``return_usable = false``), so ``load_masked_series`` hands
    this function ``None`` for them; ``price_daily`` has 154, which a raw-bar
    caller such as the ``--equivalence`` arm does reach. Reproduce with::

        select count(*) from research_price_daily where close <= 0;
        select count(*) from price_daily where close <= 0;

    A zero denominator would be a ``ZeroDivisionError`` and a negative one a
    sign-flipped return that ranks like a winner, which is the worse failure of
    the two — it is a plausible number.
    """
    closes = series.float_closes
    values: list[float | None] = []
    unevaluable: list[int] = []
    for index in range(len(closes)):
        if index < ELIGIBILITY_BARS - 1:
            values.append(None)
            continue
        past = closes[index - LOOKBACK_BARS]
        recent = closes[index - SKIP_BARS]
        if past is None or recent is None or past <= 0.0 or recent <= 0.0:
            values.append(None)
            unevaluable.append(index)
            continue
        values.append(recent / past - 1.0)
    return IndicatorSeries(values=tuple(values), universe=universe, not_evaluable_indices=tuple(unevaluable))


def s2_member(
    series: BarSeries,
    *,
    panel_rebalance_dates: Set[date],
    universe: Universe,
    close_reason: NotEvaluableReason,
) -> CrossSectionalMember:
    """One instrument's contribution to the ranked panel.

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

    closes = series.float_closes
    score = momentum_series(series, universe=universe)
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
    universe: Universe,
    close_reason: NotEvaluableReason,
) -> dict[int, list[StrategySignal]]:
    """S-2 over a whole panel: one entry verdict per member per bar.

    ⚠ THIS HOLDS THE WHOLE PANEL IN MEMORY and is the right entry point for a
    bounded one (a watchlist, a test, one sector). A full-corpus sweep must not
    call it — it would materialise every bar of every member at once. That is
    what ``rebalance_dates`` / ``s2_member`` / ``s2_select`` are public for:
    ``scripts/verify_2240_s2_cross_sectional.py`` streams one series at a time
    through the same functions, via the contract's own ``StagedMember``, rather
    than re-implementing the staging pass.
    """
    calendar = {when for series in panel.values() for when in series.dates}
    dates = rebalance_dates(calendar)
    members = {
        key: s2_member(series, panel_rebalance_dates=dates, universe=universe, close_reason=close_reason)
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
