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

Carry and FX are STRUCTURALLY ZERO for the one lane this model prices —
long, x1, ``real`` settlement, USD order, USD account, USD-quoted universe —
because eToro's own product rule says no overnight/weekend fee exists on a
non-leveraged underlying-asset BUY and no currency conversion event occurs on
an all-USD path. See ``CARRY_CLOSURE`` / ``FX_CLOSURE``: a closure state, never
a ``Decimal("0")`` pretending to be a measurement (#2720). Any OTHER lane —
short, leveraged, CFD-resolved, non-USD — is UNPRICED, not free. Dividends and
corporate-action cash remain outside the model as before: trade costs only.

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
#:
#: ⚠ v3 (#2720): carry and FX closed as structural zero for the declared lane,
#: which the id now names. The band table, session rule and calibration figures
#: are UNCHANGED from v2 — what changed is what the model claims about carry
#: and FX, and the module rule (a change to what is charged is a new model)
#: applies to a claim exactly as it does to a number.
#:
#: ⚠ #2833 DELIBERATELY DID NOT MOVE THIS, and the reasoning is recorded here
#: because it is the obvious objection. That change added
#: ``CostLane.instrument_denomination`` and an import guard, so a reviewer can
#: fairly say "the same id now admits a narrower set of lanes". It does — but
#: the set of PRICED POSITIONS is identical, because the docstring above
#: already named the lane as a "USD-quoted universe" and ``FX_EVIDENCE`` leg 2
#: already asserted it full-population. Nothing was charged differently, no
#: stored result changes meaning, and the lane is a module constant no caller
#: constructs, so the domain that narrowed is the space of future SOURCE EDITS
#: rather than of runs. Moving the id would rehash every strategy version to
#: announce a change that did not happen, which is its own false signal.
#: The line: a claim moves the id (#2720); ENFORCING a claim already made does
#: not.
COST_MODEL_ID = "static-p75-insession-v3+split-adjusted-max+carry-fx-structural-zero-long-x1-real-usd"

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

#: ⚠ SIX LIMITS, EVERY ONE STILL LIVE — §5.1's four, with its fourth split in
#: two by #2363, and 4/5 REWRITTEN plus 6 ADDED by #2720 when carry and FX
#: closed as structural zero for the declared lane. They are constants rather
#: than prose so stage 5c can put them on the result row.
#:
#: 1. The sample is one hour of the day — 1,149 of the 1,159 in-session captures
#:    sit at UTC hour 19 (15:00–15:59 ET), the session's final and most liquid
#:    hour. A closing-hour spread is at the favourable end of the day.
#: 2. It is a sample: 1,159 in-session quotes against a 6,735-instrument
#:    universe, and the ``<$5`` band rests on 76 of them.
#: 3. Those captures land on 9 distinct NY dates in one summer. Nothing here
#:    observes a volatile regime.
#: 4. Carry is structurally zero FOR THE DECLARED LANE ONLY; any other lane is
#:    unpriced, not free.
#: 5. FX is structurally zero for the same lane, and the account-currency half
#:    of it is measured on ONE account (the configured demo account).
#: 6. Real-settlement fills are ASSUMED, not observed, in the backtest.
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
    "carry is structurally zero for the declared lane ONLY (long x1 real-settlement USD); any other lane"
    " — short, leveraged, CFD-resolved, non-USD — is UNPRICED, not free",
    "FX is structurally zero for the same lane; the account currency is measured on the configured demo"
    " account (account_equity_evidence), not proven for any other account, and the lane's"
    " instrument_denomination is USD — a non-USD-denominated instrument converts on unit sizing and is"
    " outside this model however small the fee",
    "the backtest ASSUMES real-settlement fills: eToro resolves some non-leveraged buys as CFDs,"
    " historical resolution is unobservable, and the order path closes this only forward"
    " (settlementType is an assertion the platform rejects on mismatch)",
)

#: How a cost component is closed. ⚠ A CLOSED vocabulary, guarded at import:
#: an unknown value would read as "not unmodelled" to the markers below and
#: clear a promotion refusal while modelling nothing — #2286's shape arriving
#: through a typo. A future measured nonzero fee (a CFD lane, a non-USD
#: account) adds a "charged" member TOGETHER WITH the arithmetic that adds it
#: to a price and a new ``COST_MODEL_ID`` — there is deliberately no dormant
#: scalar to set (#2720 deleted ``CARRY_BPS`` / ``FX_BPS`` rather than zeroing
#: them).
CostComponentClosure = Literal["unmodelled", "structural_zero"]
COST_COMPONENT_CLOSURES: frozenset[str] = frozenset(get_args(CostComponentClosure))


@dataclass(frozen=True)
class CostLane:
    """The ONE execution lane this model prices. INTRINSICALLY SINGLE-LANE.

    ⚠ There is exactly one instance (``STRUCTURAL_ZERO_LANE``) and the model id
    names it, so there is no per-component lane to diverge and no
    lane-conditional marker state: a consumer outside the lane needs a
    DIFFERENT cost model, which the identity hash makes a different strategy
    version. The lane is pinned to the live order writer by test
    (``tests/test_cost_model.py`` — the executor payload states its literals
    independently; neither side imports the other's constant).
    """

    direction: str
    leverage: int
    settlement: str
    order_currency: str
    account_currency: str
    #: The currency the INSTRUMENT ITSELF is denominated in — a third currency,
    #: not implied by the other two. #2833 is the case that needed it: an eToro
    #: order for a GBP-denominated LSE UCITS ETF is still placed with
    #: ``orderCurrency: usd`` from a USD account, so ``order_currency`` and
    #: ``account_currency`` are both USD while a conversion event DOES occur.
    #: Without this field the lane cannot state the difference and a non-USD
    #: sleeve would inherit an FX closure measured on an all-USD path.
    instrument_denomination: str


#: Matches, field for field, what ``EtoroBrokerProvider.place_demo_strategy_order``
#: puts on the wire (``transaction: buy`` ⇔ long, ``leverage: 1``,
#: ``settlementType: real``, ``orderCurrency: usd``) and what
#: ``BrokerStrategyOrder`` refuses at type level (``settlement_type`` is
#: ``Literal["real"]``). Held together by test, not by import.
#:
#: ⚠ ``instrument_denomination`` is the one field with NO counterpart on the
#: wire — the order payload never states it. It comes from the calibration
#: population instead (``FX_EVIDENCE`` leg 2: the validated universe is
#: uniformly USD-quoted), which is exactly why it has to be written down here:
#: an unstated property of the population is the one a later consumer silently
#: assumes still holds.
STRUCTURAL_ZERO_LANE = CostLane(
    direction="long",
    leverage=1,
    settlement="real",
    order_currency="USD",
    account_currency="USD",
    instrument_denomination="USD",
)

#: ⚠ The evidence a structural-zero claim stands on, DATED, one tuple per
#: component because they close on unrelated facts (#2363). Leg 3 of the carry
#: tuple is deliberately labelled consistent-only: an all-zero component has an
#: undecodable unit (`.claude/skills/data-sources/etoro-api.md`), so it can
#: fail to falsify the rule but can never be the rule.
CARRY_EVIDENCE: tuple[str, ...] = (
    "etoro.com/trading/fees — 'Overnight fee: Free' for non-leveraged stock/ETF BUYs; overnight/weekend"
    " financing is a CFD property ('Short-selling orders and leveraged positions on stocks are executed as"
    " CFDs and incur CFD spreads and overnight fees') (verified 2026-08-14)",
    "api-portal.etoro.com create-an-order (OpenAPI v1.342.0) — settlementType 'is an assertion, not a"
    " selector … a mismatch is rejected during execution'; the strategy order writer pins"
    " settlementType='real', so an order in this lane holds the underlying or is rejected"
    " (verified 2026-08-14)",
    "what-if cost census — overnightFee 0.0 on every real-settlement buy observation (n=28);"
    " CONSISTENT-ONLY, unit undecodable (skill §band-census, 2026-08-12/13)",
)
FX_EVIDENCE: tuple[str, ...] = (
    "account_currency_id = 1 (USD) measured on account_equity_evidence for the configured demo account"
    " (#2698, 2026-08-14)",
    "validated universe uniformly USD-quoted — asserted full-population by"
    " scripts/measure_2605_universe_scope.py (#2605) and re-asserted per run at the stamping site",
    "order payload pins orderCurrency='usd'; DEPLOYMENT_CURRENCY='USD' enforced at the sql/290 + sql/338"
    " CHECKs and the executor eligibility/cost currency checks (strategy_base_currency)",
)

#: ⚠⚠ STRUCTURAL ZERO IS A CLOSURE STATE, NEVER A ``Decimal("0")`` — *"a value
#: that is present and wrong beats a value that is absent and refused"*
#: (#2286). For the declared lane no overnight/weekend fee EXISTS (the position
#: is the underlying, not a financing contract) and no conversion event OCCURS
#: (USD in, USD held, USD out), so there is no amount to measure and writing
#: zero would claim a measurement nobody made. #2277's standing re-check
#: closes with this; the evidence tuples above are its record.
CARRY_CLOSURE: CostComponentClosure = "structural_zero"
FX_CLOSURE: CostComponentClosure = "structural_zero"


#: ⚠ DERIVED, never hand-written — §5.1 makes these markers the thing the
#: promotion gate refuses on: *"statistics are computed and published with an
#: explicit ``carry_unmodelled`` marker; they are not promotable"*.
#:
#: ⚠⚠ #2363 NARROWED THIS to one flag per component; #2720 moved the inputs
#: from bps amounts to closure states. Promotion semantics are unchanged: a
#: ``True`` marker still hard-refuses via ``structural_promotion_refusals``.
def unmodelled_markers(carry_closure: str, fx_closure: str) -> tuple[bool, bool]:
    """``(carry_unmodelled, fx_unmodelled)`` — EACH FROM ITS OWN CLOSURE ONLY.

    ⚠⚠ A FUNCTION RATHER THAN TWO INLINE EXPRESSIONS, AND THE REASON IS A FAILED
    REVERT-PROBE (#2363): a re-coupled expression was indistinguishable while
    both inputs held the same value. Taking the closures as ARGUMENTS keeps the
    de-coupling observable — the test drives all four combinations.

    ⚠ An UNKNOWN closure value RAISES rather than defaulting: ``value ==
    "unmodelled"`` alone would read a typo as "modelled", which clears a
    promotion refusal while modelling nothing. Validation reads both arguments;
    each MARKER still derives from its own argument only.
    """
    for name, value in (("carry", carry_closure), ("fx", fx_closure)):
        if value not in COST_COMPONENT_CLOSURES:
            raise ValueError(
                f"unknown {name} closure {value!r}; must be one of {sorted(COST_COMPONENT_CLOSURES)} — "
                "an unknown closure must never read as 'modelled'"
            )
    return carry_closure == "unmodelled", fx_closure == "unmodelled"


#: The FX half is separate evidence, a separate owner and a separate arrival
#: date — see the module docstring and #2363.
CARRY_UNMODELLED, FX_UNMODELLED = unmodelled_markers(CARRY_CLOSURE, FX_CLOSURE)


def _check_closures() -> None:
    """A structural-zero closure must carry its lane and its evidence.

    Replaces ``_check_unmodelled_components_are_not_charged`` (#2363→#2720):
    the bps scalars are deleted, so the clause naming them went with them —
    which is the ticket's own step 4, performed by removing the subject rather
    than waiving the guard. What still needs guarding at import:

    - the closure VALUES are in the vocabulary (an unknown value would clear a
      refusal — the markers raise too, but this fires beside the literal, in
      the import of whoever edited it);
    - a ``structural_zero`` claim carries non-empty, dated evidence — a claim
      with no record is ceremony;
    - the single lane is unleveraged and long: leverage above x1 is a CFD and a
      short accrues financing by construction (risk posture,
      ``.claude/CLAUDE.md``), so EITHER edit needs a new model, not this one
      relabelled;
    - an FX ``structural_zero`` names ONE currency in all three positions.
      ``FX_CLOSURE`` says no conversion event OCCURS, which is a claim about
      three currencies and not two: order, account AND the instrument's own
      denomination. #2833's beta-sleeve candidates are the case that separates
      them — CSPX.L / IUSA.L / IUMO.L / IUQA.L / R1VL.L are stored
      ``instruments.currency = 'GBP'`` yet their eligibility proofs answer
      ``response_currency = 'usd'`` from a USD account, so order and account
      currency alone read exactly as an all-USD path does and cannot tell the
      two apart. eToro's Cost & Charges worked examples size a non-USD
      instrument through the GBP→USD rate, so on that lane a conversion is part
      of the arithmetic; whether it carries a cost is a SEPARATE and open
      question, and "structurally zero" is the one answer it cannot have. The
      honest closure there is ``unmodelled`` — which this guard leaves
      available and only refuses to let anyone skip.

    The guard is at IMPORT for the reason ``_check_bands_are_total`` is: these
    are edits somebody makes to the literals above, so the check belongs beside
    the literals rather than in a test file they may not run.
    """
    for name, closure, evidence in (
        ("CARRY_CLOSURE", CARRY_CLOSURE, CARRY_EVIDENCE),
        ("FX_CLOSURE", FX_CLOSURE, FX_EVIDENCE),
    ):
        if closure not in COST_COMPONENT_CLOSURES:
            raise ValueError(
                f"{name} = {closure!r} is not in the closure vocabulary {sorted(COST_COMPONENT_CLOSURES)} — "
                "an unknown closure would read as 'not unmodelled' and clear a promotion refusal while "
                "modelling nothing new (#2286's shape). Fix the literal or widen the vocabulary together "
                "with the arithmetic and a new COST_MODEL_ID."
            )
        if closure == "structural_zero" and not evidence:
            raise ValueError(
                f"{name} claims structural_zero with no evidence — a structural claim with no dated record "
                "is not a closure, it is the flag-clearing shortcut the promotion gate exists to refuse."
            )
    if STRUCTURAL_ZERO_LANE.leverage != 1 or STRUCTURAL_ZERO_LANE.direction != "long":
        raise ValueError(
            f"the structural-zero lane must be long and unleveraged, got direction="
            f"{STRUCTURAL_ZERO_LANE.direction!r} leverage={STRUCTURAL_ZERO_LANE.leverage} — a short is a CFD "
            "and leverage above x1 is a CFD, both accrue financing by construction; that lane needs a NEW "
            "cost model, never this one relabelled."
        )
    if FX_CLOSURE == "structural_zero":
        currencies = {
            "order_currency": STRUCTURAL_ZERO_LANE.order_currency,
            "account_currency": STRUCTURAL_ZERO_LANE.account_currency,
            "instrument_denomination": STRUCTURAL_ZERO_LANE.instrument_denomination,
        }
        if len(set(currencies.values())) != 1:
            raise ValueError(
                f"FX_CLOSURE is structural_zero but the lane names more than one currency ({currencies}) — "
                "'no conversion event occurs' is a claim about all three, and a differing instrument "
                "denomination means the event happens whatever it costs. That lane needs a NEW cost model "
                "with a measured or documented FX component and a new COST_MODEL_ID, never this one relabelled."
            )


_check_closures()


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
    "CARRY_CLOSURE",
    "CARRY_EVIDENCE",
    "CARRY_UNMODELLED",
    "COST_COMPONENT_CLOSURES",
    "COST_MODEL_ID",
    "CostComponentClosure",
    "CostLane",
    "FX_CLOSURE",
    "FX_EVIDENCE",
    "FX_UNMODELLED",
    "STRUCTURAL_ZERO_LANE",
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
