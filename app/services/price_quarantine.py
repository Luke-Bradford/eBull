"""
Impossible-bar quarantine — rules B1-B4, T1-T3, W1-W2 as a pure function
over bars (#2261, phase 0a of #2240; spec = the S7 verdict on #2247).

WHAT QUARANTINE IS. It identifies "this return is not a return". It NEVER
identifies a cause. A split and a bad print produce the same defect, so the
rules never have to tell them apart — which is exactly why they work
independently of the unbuilt #2231 split detector, and why #2226's falsified
drop-magnitude discriminator is not being re-proposed. **Magnitude is a
trigger, not a verdict.**

WHY IT IS A PURE FUNCTION. Both bugs Codex caught in the S7 spike were "the SQL
is not the written rule" — a raw ``high/low`` test where the prose said *wick*,
and range-only rules feeding the *return* quarantine (which over-rejected by 587
windows). A rejection census is plausible at any magnitude, so nothing in the
output fires when the implementation drifts from the prose. One implementation,
per-rule table tests, and a census that reads what THIS code stored is the
structural fix.

TWO VERDICTS PER BAR (S7 §4). XPER 2024-06-03 is
``o 8.497 h 8.737 l 0.010 c 8.298``: a perfect close on a bar claiming the stock
traded at one cent. Returns are untouched; every stop-loss in the phase-4
outcome resolver reads as touched. So:

    B1, B4  ->  return_usable = False AND range_usable = False
    B2, B3  ->  range_usable = False only

CONTAINMENT, NOT CLASSIFICATION. T3 rejects legitimate data at every threshold —
demonstrably-real level breaks outnumber split-like ones ~10:1 — and turnover
corroboration reaches only ~30% of the population (volume is equity-only, S3).
So T3 is justified as containment and its bias must be PUBLISHED: see
``census`` and the ``/admin`` figure it feeds. An admitted transition is
*not-known-bad*, never *known-good*.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

# ---------------------------------------------------------------------------
# Rule-set version
# ---------------------------------------------------------------------------
# "rule-set id + code hash, not an int" (S7 §7). An integer version cannot tell
# you whether two stored rows were produced by the same code; a source hash can.
# Any edit to this module changes the version, which makes every previously
# stored verdict visibly stale rather than silently mixed.
RULE_SET_ID = "price-quarantine-v1"


def _code_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


RULE_SET_VERSION = f"{RULE_SET_ID}+{_code_hash()}"


# ---------------------------------------------------------------------------
# Per-asset-class parameters
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ClassParams:
    """Calendar and magnitude parameters for one asset class."""

    magnitude_threshold: Decimal
    """T — a same-scale move this large is a level-break TRIGGER (never a verdict)."""

    hole_days: int
    """T2 — a calendar gap wider than this is a series hole, not a one-day return."""

    contiguous_days: int
    """B4 — how far apart two bars may sit and still count as adjacent sessions."""

    calendar_days_per_bar: Decimal
    """W2 — nominal calendar days one trading bar spans (7/5 for exchange
    sessions, 1 for the 7-day crypto/FX markets)."""


_FIVE_DAY = Decimal("1.4")  # 7 calendar days / 5 trading days
_SEVEN_DAY = Decimal("1")

# Thresholds sit ABOVE the p99.99 daily move measured on the POST-quarantine
# population (S7 §5), so they are not calibrated on the defects they exist to
# catch: us_equity p99.99 = 2.93x (T=5), crypto 1.88x (T=5), commodity 1.31x
# (T=2), index 1.16x (T=2), fx 1.05x (T=2).
_EQUITY = ClassParams(Decimal(5), hole_days=10, contiguous_days=4, calendar_days_per_bar=_FIVE_DAY)
_SEVEN_DAY_LAX = ClassParams(Decimal(5), hole_days=4, contiguous_days=2, calendar_days_per_bar=_SEVEN_DAY)
_SEVEN_DAY_STRICT = ClassParams(Decimal(2), hole_days=4, contiguous_days=2, calendar_days_per_bar=_SEVEN_DAY)
_EXCHANGE_STRICT = ClassParams(Decimal(2), hole_days=10, contiguous_days=4, calendar_days_per_bar=_FIVE_DAY)

# DEFAULT IS STRICT, DELIBERATELY. Unknown metadata gets the 2x gate, not the
# 5x one: an instrument we cannot classify is one whose normal move size we do
# not know, and the safe error there is over-containment (visible in the
# census) rather than admitting a defect (invisible everywhere).
_DEFAULT = _EXCHANGE_STRICT

_CLASS_PARAMS: dict[str, ClassParams] = {
    "us_equity": _EQUITY,
    # DEVIATION FROM S7 §5, STATED EXPLICITLY. S7's table lists only the five
    # classes that had bars to measure: us_equity, crypto, commodity, index, fx.
    # The non-US equity classes are given the EQUITY parameters rather than
    # falling to the strict default, because they are known equities, not
    # unknown metadata, and a 2x gate on an asset class whose measured p99.99 is
    # 2.93x would quarantine an enormous amount of legitimate data.
    # They carry ~0 bars today (the 4,749 non-US equities are exactly the
    # population #2262 admits), so this changes nothing in the S7 reproduction —
    # but it WILL matter the moment #2262's seeding lands, at which point
    # per-class p99.99 recalibration is the follow-up.
    "eu_equity": _EQUITY,
    "uk_equity": _EQUITY,
    "asia_equity": _EQUITY,
    "mena_equity": _EQUITY,
    "crypto": _SEVEN_DAY_LAX,
    "fx": _SEVEN_DAY_STRICT,
    "commodity": _EXCHANGE_STRICT,
    "index": _EXCHANGE_STRICT,
    # 'unknown' is a real stored value on exchanges.asset_class and means the
    # operator has not curated the row (#503 PR 4). Same treatment as NULL.
    "unknown": _DEFAULT,
}


def params_for(asset_class: str | None) -> ClassParams:
    """Parameters for an asset class. NULL and unrecognised values get the strict default."""
    if asset_class is None:
        return _DEFAULT
    return _CLASS_PARAMS.get(asset_class, _DEFAULT)


# The trailing window in which a stored bar may still be rewritten by an
# incremental refresh. ``market_data._INCREMENTAL_FETCH_BARS`` is 3 bars
# (yesterday + today + correction buffer); across a weekend that is up to 5
# calendar days. Bars inside it are PROVISIONAL: today's AAPL bar carried volume
# 87,572 against 53,121,635 the prior day, and T3's corroboration reads volume,
# so a genuine move today would read as turnover ~0.002 and be quarantined as
# split-like. Provisional bars are never verdict-bearing corroboration.
PROVISIONAL_WINDOW_DAYS = 5

# T3 corroboration bands (S7 §5 census).
_TURNOVER_SPIKE = Decimal("2")
_TURNOVER_COLLAPSE = Decimal("0.5")

# B3 phantom-wick ratio. Must be a WICK test, not raw high/low: CNTM 2025-09-17
# (0.16 -> 4.96, a real +3,000% day) has high/low = 39 with no wick and is
# correctly kept.
_WICK_RATIO = Decimal(3)

# B4 reversion window. Exactly symmetric in log space (ln 1.25 = 0.223,
# ln 0.8 = -0.223) — a spike that comes back is a spike, not a level change.
_REVERSION_LO = Decimal("0.8")
_REVERSION_HI = Decimal("1.25")


# ---------------------------------------------------------------------------
# Inputs and verdicts
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Bar:
    """One stored ``price_daily`` row, exactly as stored. Never adjusted."""

    price_date: date
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume: Decimal | None = None


@dataclass(frozen=True)
class BarVerdict:
    price_date: date
    return_usable: bool
    range_usable: bool
    provisional: bool
    rules: tuple[str, ...] = ()

    @property
    def notable(self) -> bool:
        """True when the row says something and therefore must be stored.

        The verdict tables are sparse; an all-clean, non-provisional bar has no
        row. ``price_bar_quarantine``'s CHECK enforces the same invariant at the
        storage layer.
        """
        return not self.return_usable or not self.range_usable or self.provisional


@dataclass(frozen=True)
class TransitionVerdict:
    price_date: date
    """The LATER bar of the (prior_date -> price_date) pair."""

    prior_date: date
    observed_ratio: Decimal | None
    provisional: bool
    corroboration: str
    turnover_ratio: Decimal | None = None
    rules: tuple[str, ...] = ()

    @property
    def quarantined(self) -> bool:
        return bool(self.rules)

    @property
    def notable(self) -> bool:
        """True when the row must be stored.

        Includes transitions that T3 TRIGGERED on and then ADMITTED BACK
        (``corroboration == 'spike'``). Storing only the rejected side would make
        the narrowing-gate census unmeasurable: a gate is measured by what it
        rejects *against what it saw*, and an admitted transition is the
        denominator. It is also the audit trail for the one signal that can
        overturn a quarantine, so it has to survive the run that made the call.

        ``provisional`` ALONE is deliberately NOT enough. Every instrument has a
        few transitions inside the trailing correction window on any given run —
        16,907 of them corpus-wide — and an ordinary recent transition that never
        approached the magnitude threshold has nothing to say. Storing them made
        ``transitions_provisional_deferred`` count all of them while the API
        described the figure as T3-deferred: an operator-visible number that did
        not match its own stated rule, which is the exact defect this rule set
        exists to prevent. A genuinely deferred transition is identified by
        ``corroboration != 'not_applicable'``, which is set only when the
        magnitude threshold was crossed.
        """
        return bool(self.rules) or self.corroboration != "not_applicable"


@dataclass(frozen=True)
class SeriesVerdicts:
    bars: list[BarVerdict] = field(default_factory=list)
    transitions: list[TransitionVerdict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Bar rules
# ---------------------------------------------------------------------------


def _ohlc(bar: Bar) -> tuple[Decimal, Decimal, Decimal, Decimal] | None:
    """``(open, high, low, close)`` when all four are present and strictly positive, else None.

    Written as explicit ``is None`` + ``<= 0`` tests rather than a negated
    conjunction because ``NOT (col > 0 AND ...)`` is NULL — not TRUE — when a
    column is NULL, so the negated form admits precisely the population it
    cannot verify. Returning None (rather than a bool) also gives the callers
    real type narrowing, so no downstream arithmetic needs an ``assert``.
    """
    o, h, low, c = bar.open, bar.high, bar.low, bar.close
    if o is None or h is None or low is None or c is None:
        return None
    if o <= 0 or h <= 0 or low <= 0 or c <= 0:
        return None
    return o, h, low, c


def _usable_close(bar: Bar) -> Decimal | None:
    close = bar.close
    if close is None or close <= 0:
        return None
    return close


def _usable_volume(bar: Bar) -> Decimal | None:
    volume = bar.volume
    if volume is None or volume <= 0:
        return None
    return volume


def rule_b1(bar: Bar) -> bool:
    """B1 — any of open/high/low/close NULL or <= 0. Verdict: BOTH false.

    NOTE what this does NOT catch: the taxonomy-class-1 sentinel closes are
    ``0.01`` and ``0.0001``, which are strictly POSITIVE and pass B1. They are
    caught by B4, as reverting spikes. B1 is the 137 bars / 5 instruments whose
    stored OHLC is null or non-positive outright.
    """
    return _ohlc(bar) is None


def rule_b2(bar: Bar) -> bool:
    """B2 — containment: high < low, or close/open outside [low, high]. RANGE only.

    Small in magnitude (140 bars) and invisible to returns; it breaks touch
    logic, which is the phase-4 outcome resolver's entire input.
    """
    values = _ohlc(bar)
    if values is None:
        return False  # already both-false; B1 owns it
    o, h, low, c = values
    if h < low:
        return True
    return not (low <= c <= h) or not (low <= o <= h)


def rule_b3(bar: Bar) -> bool:
    """B3 — phantom wick: min(open,close)/low >= 3 or high/max(open,close) >= 3. RANGE only.

    A WICK test, not raw high/low. CNTM 2025-09-17 ran 0.16 -> 4.96 — a real
    +3,000% day with high/low = 39 and no wick — and is correctly kept.
    """
    values = _ohlc(bar)
    if values is None or rule_b2(bar):
        return False
    o, h, low, c = values
    return min(o, c) / low >= _WICK_RATIO or h / max(o, c) >= _WICK_RATIO


def rule_b4(prev: Bar, bar: Bar, nxt: Bar, params: ClassParams) -> bool:
    """B4 — reverting spike. Verdict: BOTH false.

    ``r_in >= T or r_in <= 1/T``, both sides contiguous, and
    ``r_in * r_out`` back inside [0.8, 1.25]. The reversion term is what
    separates a one-bar sentinel/misprint (class 1) from a genuine level change
    (class 3) — the latter does NOT come back, and is a transition-level concern
    handled by T3, not a bar defect.

    Defined on CLOSES only, as written. A bar whose close is sound but whose
    high is NULL is B1's business, not B4's, and B1 has already both-falsed it.
    """
    prev_close, close, next_close = _usable_close(prev), _usable_close(bar), _usable_close(nxt)
    if prev_close is None or close is None or next_close is None:
        return False
    if (bar.price_date - prev.price_date).days > params.contiguous_days:
        return False
    if (nxt.price_date - bar.price_date).days > params.contiguous_days:
        return False
    r_in = close / prev_close
    if not (r_in >= params.magnitude_threshold or r_in <= 1 / params.magnitude_threshold):
        return False
    r_out = next_close / close
    return _REVERSION_LO <= r_in * r_out <= _REVERSION_HI


def evaluate_bars(bars: Sequence[Bar], params: ClassParams, *, as_of: date) -> list[BarVerdict]:
    """B1-B4 over one instrument's series, ascending by date."""
    provisional_from = as_of - timedelta(days=PROVISIONAL_WINDOW_DAYS)
    verdicts: list[BarVerdict] = []
    for idx, bar in enumerate(bars):
        rules: list[str] = []
        if rule_b1(bar):
            rules.append("B1")
        if rule_b2(bar):
            rules.append("B2")
        if rule_b3(bar):
            rules.append("B3")
        if 0 < idx < len(bars) - 1 and rule_b4(bars[idx - 1], bar, bars[idx + 1], params):
            rules.append("B4")
        return_broken = "B1" in rules or "B4" in rules
        range_broken = return_broken or "B2" in rules or "B3" in rules
        verdicts.append(
            BarVerdict(
                price_date=bar.price_date,
                return_usable=not return_broken,
                range_usable=not range_broken,
                provisional=bar.price_date >= provisional_from,
                rules=tuple(rules),
            )
        )
    return verdicts


# ---------------------------------------------------------------------------
# Transition rules
# ---------------------------------------------------------------------------


def _corroboration(prev: Bar, bar: Bar) -> tuple[str, Decimal | None]:
    """Turnover corroboration for a level break.

    ``close x volume`` is SPLIT-INVARIANT, which is what makes it a usable
    admit-back signal: a rescale moves close and volume in opposite directions
    and leaves the product alone, while a real move on real interest moves it a
    lot. (Verified that eToro ``volume`` is a SHARE COUNT, not currency: AAPL
    2026-08-03 = 53,121,635 at $303.50 = $16.1B, against a real share volume of
    ~50M.)

    It reaches only ~30% of level breaks — volume is absent on ~70% of this
    population and is equity-only (S3) — so it is an ADMIT-BACK signal, never
    the gate, and "no volume -> quarantine" embeds an asset-class bias against
    non-equity and illiquid names. That bias is why the census is published.
    """
    prev_volume, volume = _usable_volume(prev), _usable_volume(bar)
    prev_close, close = _usable_close(prev), _usable_close(bar)
    if prev_volume is None or volume is None or prev_close is None or close is None:
        return "unclassifiable", None
    ratio = (close * volume) / (prev_close * prev_volume)
    if ratio > _TURNOVER_SPIKE:
        return "spike", ratio
    if ratio < _TURNOVER_COLLAPSE:
        return "collapse", ratio
    return "flat", ratio


def evaluate_transitions(
    bars: Sequence[Bar],
    bar_verdicts: Sequence[BarVerdict],
    params: ClassParams,
) -> list[TransitionVerdict]:
    """T1-T3 over consecutive bar pairs."""
    out: list[TransitionVerdict] = []
    for idx in range(1, len(bars)):
        prev, bar = bars[idx - 1], bars[idx]
        prev_v, bar_v = bar_verdicts[idx - 1], bar_verdicts[idx]
        provisional = prev_v.provisional or bar_v.provisional
        rules: list[str] = []

        # T1 — either endpoint bar is return-unusable. The ratio across an
        # unusable close is not a return regardless of its size.
        if not prev_v.return_usable or not bar_v.return_usable:
            rules.append("T1")

        # T2 — calendar gap wider than the per-class hole threshold. Per class
        # because crypto and FX trade 7 days: a 3-day gap is a weekend on one
        # market and a hole on the other.
        if (bar.price_date - prev.price_date).days > params.hole_days:
            rules.append("T2")

        ratio: Decimal | None = None
        corroboration = "not_applicable"
        turnover_ratio: Decimal | None = None
        prev_close, close = _usable_close(prev), _usable_close(bar)
        if prev_close is not None and close is not None:
            ratio = close / prev_close
            magnitude = max(ratio, 1 / ratio)
            # "not explained by T1" (S7 §5) generalises to "not already
            # explained". A ratio spanning a series HOLE is not a same-scale
            # comparison at all, so "is this a level break?" is not a meaningful
            # question about it — and a price_series_break minted from a gap
            # would strand history behind a break that never happened. S7's own
            # measured census excluded T2-overlapping transitions (its 148
            # triggers reproduce exactly once they are excluded here; including
            # them adds 32).
            if magnitude >= params.magnitude_threshold and not rules:
                if provisional:
                    # T3 reads volume, and a provisional bar's volume is a
                    # part-session count. DEFER the verdict — do not quarantine
                    # a genuine move because today's bar is half-formed.
                    corroboration = "unclassifiable"
                else:
                    corroboration, turnover_ratio = _corroboration(prev, bar)
                    if corroboration != "spike":
                        rules.append("T3")

        verdict = TransitionVerdict(
            price_date=bar.price_date,
            prior_date=prev.price_date,
            observed_ratio=ratio,
            provisional=provisional,
            corroboration=corroboration,
            turnover_ratio=turnover_ratio,
            rules=tuple(rules),
        )
        out.append(verdict)
    return out


def evaluate_series(bars: Sequence[Bar], asset_class: str | None, *, as_of: date) -> SeriesVerdicts:
    """Full rule set over one instrument's series. Bars MUST be ascending by date."""
    params = params_for(asset_class)
    bar_verdicts = evaluate_bars(bars, params, as_of=as_of)
    return SeriesVerdicts(
        bars=bar_verdicts,
        transitions=evaluate_transitions(bars, bar_verdicts, params),
    )


# ---------------------------------------------------------------------------
# Window rules
# ---------------------------------------------------------------------------


def rule_w1(
    window_start: date,
    window_end: date,
    quarantined_transition_dates: Sequence[date],
) -> bool:
    """W1 — any quarantined transition inside the window.

    ``quarantined_transition_dates`` are the LATER bar of each quarantined
    transition, so a transition counts as inside the window when that date is in
    ``(window_start, window_end]`` — the transition INTO the first bar happened
    before the window opened and does not contaminate it.
    """
    return any(window_start < d <= window_end for d in quarantined_transition_dates)


def rule_w2(window_start: date, window_end: date, bar_count: int, params: ClassParams) -> bool:
    """W2 — window calendar span more than 2x its nominal trading-day span.

    20 *bars* is not 20 *days*: SP.24-7 has a single 20-bar window spanning
    2025-04-09 to 2026-04-18. A forward-return horizon that silently becomes a
    year is not the horizon the strategy was tested at.
    """
    if bar_count < 2:
        return False
    nominal = Decimal(bar_count - 1) * params.calendar_days_per_bar
    return Decimal((window_end - window_start).days) > 2 * nominal
