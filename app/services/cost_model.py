"""Phase 5b — the static cost model.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §5.1 and §8 (stage
5b). Parent: ``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md``
§5 criterion 2 (the shape) and criterion 11 (identity). Refs #2240, #2277.

⚠⚠ THE BAND TABLE IS A FROZEN LITERAL AND MUST STAY ONE.

``quotes`` holds ONE ROW PER INSTRUMENT, overwritten on every refresh
(``market_data._upsert_quote``), so a percentile read off it measures a moment,
not a history. Recomputing this table at run time would make
``StrategyIdentity.version`` — which hashes ``cost_model_id`` — mean a different
model every day while claiming the same version, and two runs' results would be
incomparable without anything saying so. Criterion 2 asks for the opposite:
*"the model is a declared input to the strategy identity hash … so changing it
is a new evaluation, not a silent improvement"*.

That is also why there is NO database table here. A migration would put the
numbers a strategy's identity depends on into mutable rows.

**Recalibration is therefore a code change plus a new** ``COST_MODEL_ID``, which
moves every strategy version. Reproduce the measurement with::

    PYTHONPATH=. uv run python scripts/verify_2240_cost_model.py --calibrate

WHAT IS CHARGED, AND WHAT IS NOT
--------------------------------
Charged: the half-spread, both sides, as ADJUSTED FILL PRICES. §5.1: *"a buy
fills at ``fill_price × (1 + h)`` and a sell at ``fill_price × (1 − h)`` … Net
return is computed from those adjusted prices, never by subtracting a cost from
``gross_return_pct``"* — ``sql/256`` names that column GROSS precisely so
nothing averages it as performance.

NOT charged: carry (eToro's overnight/weekend CFD fee) and FX. Both are
``None``, not zero — see ``CARRY_BPS``.

For an **as-traded** entry price the band is keyed on that entry and fixed for
the life of the position. A split-adjusted research price cannot select a
nominal threshold, so ``cost_band_for`` uses the maximum calibrated band. In
both cases ``buy_price`` / ``sell_price`` take the selected ``half_spread`` as
an argument; a caller cannot accidentally re-key mid-hold.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, get_args

#: ⚠ FROZEN. A recalibration ships under a NEW id — §5.1: *"so a recalibration
#: is a new strategy version rather than a silent improvement (criterion 11)"*.
#:
#: ``insession`` is load-bearing and is what the id records: the calibration
#: admits only quotes captured inside a real NYSE session, resolved from
#: ``market_calendar`` rather than from a UTC-hour literal (see
#: ``SESSION_RULE``).
COST_MODEL_ID = "static-p75-insession-v2+split-adjusted-max"

#: Whether a price can honestly select a nominal-price spread band.  The
#: research corpus is split-adjusted, not as-traded; treating those two as the
#: same is #2400's two-sided cost-attribution error.
PriceBasis = Literal["as_traded", "split_adjusted"]
PRICE_BASES: frozenset[str] = frozenset(get_args(PriceBasis))

#: When the frozen table below was measured, and against what.
#:
#: ⚠ ``quotes`` is a live snapshot, so these numbers are NOT reproducible from a
#: later database — the calibration arm of the verify script reports today's
#: figures BESIDE the frozen ones rather than asserting equality, because a gate
#: that fails whenever the quote refresh runs is a gate nobody keeps.
CALIBRATION_RUN_DATE = "2026-08-07"

#: The session rule the calibration applied, stated because §5.1 shipped a
#: literal (``14–19 UTC``) and flagged it as wrong in both DST regimes.
#:
#: ⚠⚠ MEASURED, NOT ASSUMED: on this snapshot the calendar rule and the
#: ``14–19 UTC`` literal select the SAME 1,159 rows — symmetric difference 0.
#: The in-session stamps land on 9 distinct NY dates, none of them an NYSE
#: closure or half day, and none inside 13:30–13:59 UTC (the half-hour the
#: literal wrongly drops under EDT). So the correction §5.1 asked for is right
#: by construction and null on this data; it is applied anyway, because a rule
#: that happens to agree today is not the same as a rule that is right.
SESSION_RULE = "NYSE regular session 09:30–16:00 ET (13:00 ET on a half day), from app.services.market_calendar"

#: The calibration population, and its size. §5.1's own limit 2 says the
#: ``us_equity`` set it used *"is not even the right set"* — the model is applied
#: to the §4.0 validated universe (``us_equity`` **and** ``instrument_type_id =
#: Stocks``), so that is what it is calibrated on here. Measured this run, the
#: two differ only in the top two bands and by ≤0.006 percentage points.
CALIBRATION_POPULATION = "§4.0 validated universe (app.services.strategies.validated_universe)"
CALIBRATION_QUOTES_IN_SESSION = 1_159

#: ⚠ FIVE LIMITS, EVERY ONE STILL LIVE — §5.1's four, with its fourth split in
#: two by #2363 because carry and FX close on different evidence and one of them
#: will be live while the other is not. They are constants rather than prose so
#: stage 5c can put them on the result row.
#:
#: 1. The sample is one hour of the day — 1,149 of the 1,159 in-session captures
#:    sit at UTC hour 19 (15:00–15:59 ET), the session's final and most liquid
#:    hour. A closing-hour spread is at the favourable end of the day.
#: 2. It is a sample: 1,159 in-session quotes against a 6,735-instrument
#:    universe, and the ``<$5`` band rests on 76 of them.
#: 3. Those captures land on 9 distinct NY dates in one summer. Nothing here
#:    observes a volatile regime.
#: 4. Carry is NULL, not zero.
#: 5. FX is NULL, not zero — and it is a SEPARATE limit, not a clause of 4:
#:    carry closes on a per-order product eligibility proving underlying-at-x1,
#:    FX on the funding account's currency and a measured conversion markup.
#:
#: ⚠ Every figure in this tuple is printed by ``--calibrate``. It is written
#: down because 5c has to put it on a result row, and a hand-written statistic
#: goes stale silently — the run that produced this table corrected two of these
#: numbers, which had been carried over from §5.1's ``us_equity`` measurement
#: rather than measured on the population actually used.
CALIBRATION_LIMITS: tuple[str, ...] = (
    "one hour of the day — 1,149 of 1,159 in-session captures are at UTC hour 19 (15:00-15:59 ET)",
    "a sample — 1,159 in-session quotes against a 6,735-instrument universe; the <$5 band rests on 76",
    "9 distinct NY capture dates in one summer; no volatile regime is observed",
    "carry is unmodelled (CARRY_BPS is None, not zero)",
    "FX is unmodelled (FX_BPS is None, not zero)",
)

#: ⚠⚠ NULL, NOT ZERO, AND THE DIFFERENCE IS THE #2286 SHAPE — *"a value that is
#: present and wrong beats a value that is absent and refused"*.
#:
#: Criterion 2 requires eToro's overnight/weekend CFD fee and FX conversion, and
#: says their magnitude *"is not established here"*. The eToro portal is
#: unreachable from this environment, so it is not established here either.
#: Zero is a measurement nobody made. #2277 carries the standing re-check.
#:
#: ⚠ FX is NULL for a second, independent reason: §4.0 restricts the universe to
#: ``us_equity``, which quotes in USD — but *"quotes in USD"* and *"needs no
#: conversion"* coincide only if the ACCOUNT currency is USD, and that has not
#: been verified.
CARRY_BPS: Decimal | None = None
FX_BPS: Decimal | None = None


#: ⚠ DERIVED, never hand-written. When carry is finally measured, setting
#: ``CARRY_BPS`` flips this by itself — a hand-written ``True`` would have to be
#: remembered, and §5.1 makes this marker the thing the promotion gate refuses
#: on: *"statistics are computed and published with an explicit
#: ``carry_unmodelled`` marker; they are not promotable"*.
#:
#: ⚠⚠ #2363 NARROWED THIS. It used to be ``CARRY_BPS is None or FX_BPS is
#: None`` — one flag for two components, so a stored ``True`` could not say
#: which was missing and neither half could be banked when its evidence
#: arrived. Promotion still requires BOTH clear; the split is diagnostic, not a
#: relaxation.
def unmodelled_markers(carry_bps: Decimal | None, fx_bps: Decimal | None) -> tuple[bool, bool]:
    """``(carry_unmodelled, fx_unmodelled)`` — EACH FROM ITS OWN AMOUNT ONLY.

    ⚠⚠ A FUNCTION RATHER THAN TWO INLINE EXPRESSIONS, AND THE REASON IS A FAILED
    REVERT-PROBE. Re-coupling the carry marker to FX — restoring the pre-#2363
    ``CARRY_BPS is None or FX_BPS is None`` — changed the value of no test,
    because both amounts are ``None`` today and the coupled and uncoupled
    expressions are indistinguishable while that holds. The de-coupling this
    ticket exists to deliver was therefore unguarded at the point it lives.

    Taking the amounts as ARGUMENTS is what makes it observable: the test drives
    all four combinations, so a marker that consults the other component's
    amount fails immediately rather than in whichever quarter one of them is
    finally measured.
    """
    return carry_bps is None, fx_bps is None


#: The FX half is separate evidence, a separate owner and a separate arrival
#: date — see the module docstring and #2363.
CARRY_UNMODELLED, FX_UNMODELLED = unmodelled_markers(CARRY_BPS, FX_BPS)


def _check_unmodelled_components_are_not_charged() -> None:
    """Neither component may carry an amount until something CHARGES it.

    ⚠⚠ THIS IS THE HOLE #2363's CODEX PASS FOUND, AND IT IS NOT HYPOTHETICAL.
    ``CARRY_BPS`` and ``FX_BPS`` are referenced nowhere except the two flags
    above, ``__all__``, and a test asserting they are ``None`` — no price, no
    return and no equity mark has ever added either of them. So setting one to a
    measured number would clear its promotion refusal *without charging the
    cost*, and every result under that model would become promotable while
    modelling exactly what it modelled the day before. That is #2286's shape
    ("a value that is present and wrong beats a value that is absent and
    refused") aimed straight at the gate.

    The guard is at IMPORT for the reason ``_check_bands_are_total`` is: this is
    an edit somebody makes to the literal above, so the check belongs beside the
    literal rather than in a test file they may not run.

    ⚠ Removing this guard is part of the work of charging a component, not a
    prerequisite to be waived: charge it in the position arithmetic, ship a new
    ``COST_MODEL_ID`` (the module rule — a change to what is charged is a new
    model, not a silent improvement), then delete the clause that names it here.
    """
    charged_nowhere = [name for name, value in (("CARRY_BPS", CARRY_BPS), ("FX_BPS", FX_BPS)) if value is not None]
    if charged_nowhere:
        raise ValueError(
            f"{', '.join(charged_nowhere)} is set but nothing in this module adds it to a price — clearing the "
            "promotion refusal without charging the cost would promote every result under this model while "
            "modelling nothing new. Charge it in the position arithmetic and ship a new COST_MODEL_ID first."
        )


_check_unmodelled_components_are_not_charged()


@dataclass(frozen=True)
class PriceBand:
    """One price band's calibrated round-trip spread.

    ⚠ ``sample_size`` is a FIELD, not a comment, so a band cannot be read
    without the *n* it rests on. The ``<$5`` band is 76 quotes and the model is
    applied to every penny stock in the universe; that has to travel with the
    number rather than sit in a spec section nobody opens.
    """

    label: str
    #: Inclusive. ``None`` on the lowest band, which starts just above zero.
    lower: Decimal | None
    #: EXCLUSIVE. ``None`` on the highest band.
    upper: Decimal | None
    #: p75 of ``quotes.spread_pct`` over the calibration population, in PERCENT.
    p75_spread_pct: Decimal
    sample_size: int

    def __post_init__(self) -> None:
        if self.p75_spread_pct <= 0:
            raise ValueError(
                f"band {self.label}: p75_spread_pct must be > 0, got {self.p75_spread_pct} — a zero-cost band "
                "is criterion 2's 'fictional model'"
            )
        if self.sample_size < 1:
            raise ValueError(f"band {self.label}: sample_size must be >= 1, got {self.sample_size}")
        if self.lower is not None and self.lower <= 0:
            raise ValueError(f"band {self.label}: lower bound must be > 0, got {self.lower}")
        if self.lower is not None and self.upper is not None and self.upper <= self.lower:
            raise ValueError(f"band {self.label}: upper {self.upper} does not exceed lower {self.lower}")

    @property
    def half_spread_pct(self) -> Decimal:
        """Half the round trip, in PERCENT — one side's cost.

        ⚠ A PROPERTY, never a stored field. The prevention rule is
        ``.claude/CLAUDE.md``'s *"never hardcode a derived statistic"*: a
        hand-written half beside a p75 goes stale silently the moment the p75
        moves, and it goes stale in the place a reader trusts most.
        """
        return self.p75_spread_pct / 2

    @property
    def half_spread(self) -> Decimal:
        """The same, as a FRACTION — the ``h`` of §5.1's arithmetic."""
        return self.p75_spread_pct / 200

    def contains(self, price: Decimal) -> bool:
        return (self.lower is None or price >= self.lower) and (self.upper is None or price < self.upper)


#: §5.1's model, recalibrated this run against the population and session rule
#: above. ⚠ The p75s are QUANTISED TO 0.001 PERCENTAGE POINTS (0.1 bp) with
#: ROUND_CEILING, so the frozen model is never CHEAPER than the measurement it
#: came from — criterion 2 puts the model *"deliberately at the pessimistic
#: end"*, and rounding a cost down is the one direction that flatters a result.
#:
#: Raw measurement, 2026-08-07, ``--calibrate`` reproduces it:
#:
#: ===========  =====  ==============  =========
#: band         n      p75 (measured)  frozen
#: ===========  =====  ==============  =========
#: ``<$5``      76     1.449275        1.450
#: ``$5–20``    244    0.570162        0.571
#: ``$20–100``  625    0.508671        0.509
#: ``>=$100``   210    0.321569        0.322
#: ===========  =====  ==============  =========
#:
#: ⚠ These are NOT §5.1's printed figures (1.600 / 0.564 / 0.509 / 0.316), and
#: the difference is snapshot drift, not a disagreement: ``quotes`` gained rows
#: between the spec run and this one (1,528 → 1,557 total), which moves a
#: percentile most in the thinnest band. ``<$5`` fell 1.600 → 1.450 on n=76.
#: Acceptance C2(c) says the table is *"pinned by test to the §5.1 figures"*;
#: it is pinned by test to THESE, because the §5.1 snapshot no longer exists and
#: cannot be reproduced from any later database. What the pin buys is the same
#: either way — no silent recalibration.
BANDS: tuple[PriceBand, ...] = (
    PriceBand(label="<$5", lower=None, upper=Decimal("5"), p75_spread_pct=Decimal("1.450"), sample_size=76),
    PriceBand(label="$5-20", lower=Decimal("5"), upper=Decimal("20"), p75_spread_pct=Decimal("0.571"), sample_size=244),
    PriceBand(
        label="$20-100", lower=Decimal("20"), upper=Decimal("100"), p75_spread_pct=Decimal("0.509"), sample_size=625
    ),
    PriceBand(label=">=$100", lower=Decimal("100"), upper=None, p75_spread_pct=Decimal("0.322"), sample_size=210),
)

#: A split-adjusted historical price cannot select a nominal price band without
#: the point-in-time split factor.  The corpus has no such factors, so use the
#: most expensive measured band.  This is an adverse sensitivity suitable for
#: falsification, not a claim that the resulting cost is the historical quote.
UNKNOWN_NOMINAL_PRICE_BAND: PriceBand = max(BANDS, key=lambda band: band.p75_spread_pct)


def _check_bands_are_total(bands: tuple[PriceBand, ...]) -> None:
    """Every positive price falls in exactly one band, or this module will not import.

    ⚠ CHECKED AT IMPORT, not in a test. A gap between two bands makes
    ``half_spread_for`` raise on a price that is perfectly ordinary, and an
    overlap makes the cost depend on iteration order. Both are edits somebody
    makes to the literal above, so the guard belongs beside the literal.
    """
    if not bands:
        raise ValueError("no price bands declared")
    if bands[0].lower is not None:
        raise ValueError(f"the lowest band {bands[0].label} must be open below (lower=None), got {bands[0].lower}")
    if bands[-1].upper is not None:
        raise ValueError(f"the highest band {bands[-1].label} must be open above (upper=None), got {bands[-1].upper}")
    for lower_band, upper_band in zip(bands, bands[1:], strict=False):
        if lower_band.upper != upper_band.lower:
            raise ValueError(
                f"bands {lower_band.label} and {upper_band.label} are not contiguous: {lower_band.label} ends at "
                f"{lower_band.upper} and {upper_band.label} starts at {upper_band.lower}"
            )
    if len({band.label for band in bands}) != len(bands):
        raise ValueError("duplicate band label")


_check_bands_are_total(BANDS)


def band_for(price: Decimal) -> PriceBand:
    """The band a price falls in.

    ⚠ A NON-POSITIVE PRICE RAISES rather than defaulting to the cheapest or the
    dearest band. ``EntryFill`` and ``ExitFill`` already require ``> 0``
    (``position_builder``), so reaching here with one means the caller assembled
    a position from something that is not a price.
    """
    if price <= 0:
        raise ValueError(f"price must be > 0 to carry a cost band, got {price}")
    for band in BANDS:
        if band.contains(price):
            return band
    # Unreachable while `_check_bands_are_total` holds; kept so a future edit to
    # BANDS fails loudly here rather than returning a wrong band.
    raise ValueError(f"no cost band covers price {price} — the band table has a gap")


def half_spread_for(entry_fill_price: Decimal) -> Decimal:
    """``h`` for a position, keyed on its ENTRY fill price (§5.1).

    ⚠ THE ONLY FUNCTION HERE THAT READS A PRICE TO CHOOSE A BAND. Call it once
    per position and pass the result to both ``buy_price`` and ``sell_price``;
    a position that crosses a band boundary mid-hold does not re-key.
    """
    return band_for(entry_fill_price).half_spread


def cost_band_for(entry_fill_price: Decimal, *, price_basis: PriceBasis) -> PriceBand:
    """Select a cost band without confusing adjusted and nominal prices.

    ``as_traded`` may use the price thresholds. ``split_adjusted`` cannot: in
    the absence of a point-in-time split factor it receives the maximum frozen
    spread.  The price is still validated because callers must never cost a
    non-price, even when its scale cannot choose a band.
    """
    if price_basis not in PRICE_BASES:
        raise ValueError(f"unknown price basis {price_basis!r}; must be one of {sorted(PRICE_BASES)}")
    if entry_fill_price <= 0:
        raise ValueError(f"price must be > 0 to carry a cost band, got {entry_fill_price}")
    if price_basis == "split_adjusted":
        return UNKNOWN_NOMINAL_PRICE_BAND
    return band_for(entry_fill_price)


def buy_price(price: Decimal, *, half_spread: Decimal) -> Decimal:
    """§5.1's buy side: ``fill_price × (1 + h)``.

    ⚠ NOT QUANTISED. These are analytics inputs, not stored prices, and a
    rounding rule nobody specified is a second cost model hiding inside the
    first.
    """
    _check_half_spread(half_spread)
    return price * (Decimal(1) + half_spread)


def sell_price(price: Decimal, *, half_spread: Decimal) -> Decimal:
    """§5.1's sell side: ``fill_price × (1 − h)``."""
    _check_half_spread(half_spread)
    return price * (Decimal(1) - half_spread)


def _check_half_spread(half_spread: Decimal) -> None:
    """A half-spread must be a positive fraction well under 1.

    ⚠ The upper bound is not decoration: at ``h >= 1`` the sell side goes to
    zero or negative, so a return computed from it is not a loss — it is
    nonsense that would still divide cleanly and be averaged into a result.
    """
    if half_spread <= 0:
        raise ValueError(f"half_spread must be > 0, got {half_spread} — a zero-cost side is criterion 2's fiction")
    if half_spread >= 1:
        raise ValueError(f"half_spread must be < 1, got {half_spread} — a sell side at or below zero is not a price")


__all__ = [
    "BANDS",
    "CALIBRATION_LIMITS",
    "CALIBRATION_POPULATION",
    "CALIBRATION_QUOTES_IN_SESSION",
    "CALIBRATION_RUN_DATE",
    "CARRY_BPS",
    "CARRY_UNMODELLED",
    "COST_MODEL_ID",
    "FX_BPS",
    "FX_UNMODELLED",
    "PRICE_BASES",
    "SESSION_RULE",
    "PriceBand",
    "PriceBasis",
    "UNKNOWN_NOMINAL_PRICE_BAND",
    "band_for",
    "buy_price",
    "cost_band_for",
    "half_spread_for",
    "sell_price",
    "unmodelled_markers",
]
