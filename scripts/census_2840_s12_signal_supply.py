"""Census for #2840 arm 2 step 2 — S-12's in-sample SIGNAL supply, to derive the floor.

Read-only. Evaluates the RULE over the corpus and counts verdicts. It reads no
``gross_return_pct``, resolves no fill, computes no expectancy and writes no row. ⚠ It
DOES compute bar returns, for the dispersion populations below; what it never computes
is a return attributable to a position —
the same class of pre-freeze fact as #2582's *"963 clean 13D events over 331 distinct
public filing dates"*, which ``docs/review-prevention-log.md``'s #2614 entry names as
the outcome-free half of a derived forward-shadow floor.

WHY THIS EXISTS
---------------
``prereg_contract.ForwardShadowFloor`` forbids a default and ``sql/333`` CHECKs both
numbers ``> 0``, so arm 2's declaration cannot be frozen without deriving them — and
no derivation can be honest without knowing what this rule's in-sample supply IS. This
script measures that supply. It does NOT derive the floor; see the note above
``_concentration`` for why the arithmetic was deliberately taken back out.

⚠⚠ THE MEASUREMENT SIZES THE FLOOR AND MUST NOT MOVE THE GATE. S-12 is merged and its
identity is already ``strategy-registry-v1+f6100a890599``; a supply count cannot
retro-tune a rule whose hash is fixed. ⚠ That is a narrower claim than it first reads
(Codex ckpt-1, finding 13): the hash fixes the RULE, not the floor, the census window
or the decision to continue, all of which are still open when this runs. The spec's
§"Measured premise" note that S-4's entry count above the edge *"stays unmeasured until
the declaration is frozen"* is superseded here and only here, because a floor nobody
measured is the #2600 padded floor this contract exists to forbid.

⚠⚠ NO HOLD-OUT BAR IS LOADED, LET ALONE EVALUATED. The corpus window ends the day
BEFORE ``HOLDOUT_BOUNDARY``, and ``through_date`` follows it on BOTH readers — the price
arms and ``MarketRegimeProvider.load_research``, whose default of ``None`` silently
loaded the whole benchmark chain until Codex ckpt-1 finding 26 — so the rule is never
run over a withheld bar. Filtering post-boundary VERDICTS after computing them would give
the same counts — every indicator here is causal — but "the numbers would have come out
the same" is not the access rule, and a census that opens the hold-out to discard it is
one edit away from reporting it (Codex ckpt-1, finding 21).

⚠ FIRED SIGNALS, NOT RESOLVED ONES, AND NOT TRADES. ``strategy_live_gate`` counts
forward decision dates over signals that RESOLVED (``strategy_live_gate.py:392-395``),
and the pass bar's own substrate is COSTED TRADES. Between a fired signal and a trade
sit: an unusable fill open (``signal_ledger.resolve_fills`` → ``unusable_fill_price``),
``superseded_open_position`` collapse, and ``namespace_for_signal``'s purge of a
pre-boundary signal whose FILL lands on or after the boundary. So a fired count is an
UPPER bound on the trade count, in both arms, and any derivation built on it must say
so rather than call a fire a trade (Codex ckpt-1, findings 4 and 22).

⚠ THE SERIES IS BUILT EXACTLY AS THE RUN BUILDS IT. ``evaluate_arm`` takes
``_dense_price_history(masked, ...)[0]``, and that tuple element IS
``_to_series(source.bars)`` (``backtest_run.py:1509``) whatever the return basis — the
dense arrays are the benchmark's, not the strategy's. So loading both arms off one
fetch and projecting with ``_to_series`` is the same input, at half the round trips.

⚠ S-4 IS COUNTED BESIDE S-12 and is not decoration: it is the control the pass bar
compares against, and the ratio of the two fired counts is the gate's SIGNAL-level
admission rate. The spec's census measured the BAR-level rate (3.10%) and said
explicitly that bar supply is an upper bound on signal supply and nothing more.

⚠⚠ ``masked`` IS THE PRODUCTION ARM, AND ``admitted`` IS NOT. ``price_masked_bars``,
the reader the live scan uses, carries ONE arm on purpose — *"criterion 9's `admitted`
arm is a sensitivity measurement that has no place in a scan"* (``price_masked_bars.py``
module header). Both arms are reported here because the pass bar requires both, but any
claim about what a FORWARD window would supply has to read the masked figures. An
earlier draft of the derivation had this exactly backwards (Codex ckpt-1, finding 1).

⚠ SAME-DAY CONCENTRATION IS REPORTED, because it is the entire reason the floor is
denominated in distinct dates rather than signals (``prereg_contract.py:127-132``): a
supply that arrives 70 names at a time on a handful of dates is not the evidence a
per-date count implies.

WHAT THE SECOND PASS ADDS — AND WHAT IT DID NOT BUY (#2840 step 2)
------------------------------------------------------------------
⚠⚠ READ THIS BEFORE REUSING THE FIGURES. These measurements were taken to instantiate
``2026-08-11-portfolio-alpha-viability-plan.md`` §5 — route 1 of the step-2 floor — and
**that derivation was refused at Codex checkpoint 1, the second refusal in a row.** The
root cause is not in this file: arm 2's pass bar is a pair of point-estimate
inequalities with no error model, so there is no test to power, and the leg that binds
compares TWO strategies, whose difference has a variance no pre-look measurement can
supply. See the spec's §"Route 1 was attempted and REFUSED".

The measurements below stand on their own — they are charged-band and supply facts, and
any future derivation needs them — but **nothing here sizes a floor, and the charge
difference must not be re-adopted as an effect size without the contract first
declaring a test.**

The per-fire charged band is the one quantity that did not exist in the repo: #3238
measured **0.7057% bar-weighted** and its own note forbids quoting it per trade. Three
facts make recording it outcome-free:

1. the fill is ``open(signal_index + 1)`` unconditionally (``resolve_fills``);
2. the band keys on the ENTRY fill and is frozen for the hold
   (``cost_model``'s module docstring: *"a caller cannot accidentally re-key
   mid-hold"*), so one band per position prices BOTH legs;
3. the charged round trip is therefore ``2h / (1 + h)`` — gross is multiplied by
   ``(1 - h) / (1 + h)`` — a function of the entry band alone, with no return in it.

⚠⚠ THE GATE READS ``close(t)`` AND THE CHARGE READS ``open(t+1)``, so S-12 is NOT
guaranteed the cheapest band. The spec's §"Why ``close(t)``" measured that leak at the
BAR level (6,815 of 1,025,067 gated bars gap below the edge overnight). Measured here
on the legs that actually fire, which is the population the effect size is denominated
in.

⚠ FIRES ARE NOT TRADES, and this pass does not pretend otherwise. It reports the band
mix under TWO weightings and the derivation must carry both:

- ``all_fires`` — every fire, which over-counts by the ``superseded_open_position``
  collapse (≈2.80 fires/trade on S-4's stored in-sample cell, and that ratio must NOT
  be transferred to S-12: the gate deliberately breaks runs).
- ``max_hold_collapse`` — a greedy pass that, after accepting a fillable fire at index
  ``i``, refuses every fire through ``i + MAX_HOLD_BARS - 1``. A real position exits at or
  BEFORE the hold cap (stop, target or cap — ``s4_exit_bracket``), so this quarantine
  is the longest one any position can impose and the arm OVER-collapses by
  construction. ⚠ It is a second WEIGHTING, not a bracket: greedy sets under different
  quarantine lengths are not nested, so the true trade-weighted mix is not arithmetically
  trapped between the two. What agreement between them buys is evidence that the band
  mix is insensitive to the collapse, which is the only claim made from it.

⚠ RETURN DISPERSION IS MEASURED IN TWO PLACES, because the variance input §5 needs is
S-12's and the only stored bootstrap CI is S-4's. Whether that borrow is conservative is
a DIRECTION, and a direction is measurable rather than assertable:

- ``decision_bar_dispersion`` — the TRAILING return at each fire, per strategy and arm.
  This is the like-for-like pair the derivation uses: the two rules' own decision bars,
  not the corpus at large.
- ``bar_return_dispersion`` — the unconditional close-to-close population, split on the
  gate edge. Kept as a secondary reading, and ⚠ it is NOT a good comparator on its own:
  measured over the full corpus its ``all_bars`` standard deviation is dominated by raw
  bar-return tails on names neither strategy trades.

⚠⚠ TRAILING, NOT FORWARD, AND THAT IS WHAT KEEPS IT OUTCOME-FREE. ``close(i)/close(i-1)``
at a fire is the move INTO the decision bar, which the rule already has. The forward
return at a fire would be the first bar of that position's own P&L — the thing a
pre-freeze census must not open. ⚠ The unconditional population is a plain price fact
about the corpus, the same class as arm 1's regime priors.

WHAT THE THIRD PASS ADDS — THE NAME-DAY DENOMINATOR (#2840 step 1, sizing)
--------------------------------------------------------------------------
Step 1's open item is SIZING, and it is not "how many names": the pass bar's axes are
daily sleeve returns and entry-fill-date CLUSTERS, so two names can generate many dates
and a thousand names can generate one. The question is how many clusters a forward
``<= 50``-name panel accrues per unit time, and that needs a FIRE RATE PER NAME-DAY.

⚠ HALF THE SCOPING THIS INHERITED WAS ALREADY BUILT. The handoff asked for two new
counters — name-days evaluated, and name-days at or above the gate. The first already
existed: ``segmented_signals`` emits exactly one entry verdict per bar and RAISES
otherwise (``strategy_segmented_evaluation.py:66-70``), so the ``verdicts`` tally IS a
name-day count over precisely the population the fires came from. Only the second was
missing, and the nearest-looking substitute is not it — ``bar_return_dispersion``'s
``gated_bars`` count is a RETURN-PAIR population that requires a usable successor bar
and includes bars the rule could not evaluate.

⚠ AND IT CANNOT BE READ OFF THE VERDICTS. S-12's gate is a bare ``return False`` inside
``entry(index)``; it stamps no reason, so a gate rejection and a failed breakout are
both plain ``not_fired``. The counter has to be taken where the verdict is.

``name_day_supply`` therefore reports, per ``(strategy, arm)`` and per calendar YEAR:
``evaluable``, ``gate_clearing_evaluable``, ``fired``, ``fired_gate_clearing`` and the
distinct signal-date count. Two identities make it self-checking — S-12's
``fired_gate_clearing`` must equal its ``fired`` (its gate admits nothing else), and
S-4's must equal S-12's ``fired`` exactly (S-12 IS S-4 and the gate).

⚠ THE YEAR KEY IS NOT DECORATION. A single 58.9-year rate would hand a forward 24-month
projection a stationarity assumption with nowhere to state it. ⚠⚠ AND NO RATE IS
DIVIDED HERE: counts only, for the reason the floor arithmetic was taken out — a rate
carries a homogeneity-across-names assumption, and an assumption belongs where the
projection is made. ``scripts/project_2840_panel_accrual.py`` owns that arithmetic.

Refs #2840, #2832, #2437, #2829, #3238.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Final

import psycopg

from app.config import settings
from app.services.backtest_run import (
    BACKTEST_UNIVERSE,
    EVALUATION_WINDOW_START,
    Window,
    _to_series,
    load_corpus,
)
from app.services.cost_model import COST_MODEL_ID, PriceBasis, cost_band_for
from app.services.indicator_series import BarSeries
from app.services.market_regime_provider import MarketRegimeProvider
from app.services.research_price_structure_store import load_arms
from app.services.strategies.s4_volatility_compression_breakout import MAX_HOLD_BARS, S4_PARAMS
from app.services.strategies.s12_cheapest_band_price_gated_breakout import (
    CHEAPEST_BAND,
    GATE_EDGE,
    PRICE_FLOOR,
    S12_PARAMS,
    S12_STRATEGY_ID,
)
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_result import HOLDOUT_BOUNDARY
from app.services.strategy_segmented_evaluation import segmented_signals

S4_STRATEGY_ID: Final = "s4-volatility-compression-breakout"

#: ⚠ ASSERTED, NOT ASSUMED. The collapse arm quarantines ``MAX_HOLD_BARS`` bars after
#: an accepted fire, and that cap is only the longest possible hold if BOTH strategies
#: carry it — S-12 delegates ``s4_exit_bracket`` today, so they do, and this raises at
#: import if a later edit gives either its own cap. Reading it from the two params
#: mappings rather than from the one module is the point: the params are what the
#: identity hashes, so a divergence that moves a strategy version also reds this.
if not (S4_PARAMS["max_hold_bars"] == S12_PARAMS["max_hold_bars"] == MAX_HOLD_BARS):
    raise RuntimeError(
        f"S-4 and S-12 no longer share a hold cap (S-4 {S4_PARAMS['max_hold_bars']!r}, "
        f"S-12 {S12_PARAMS['max_hold_bars']!r}, module {MAX_HOLD_BARS!r}) — the collapse arm "
        "quarantines one cap for both strategies and would over- or under-collapse one of them"
    )

#: ⚠ ORDER MATTERS ONLY FOR THE REPORT. The derivation names each arm explicitly;
#: it does not read "the first arm".
ARMS: Final[tuple[str, ...]] = ("masked", "admitted")
STRATEGIES: Final[tuple[str, ...]] = (S12_STRATEGY_ID, S4_STRATEGY_ID)


#: ⚠⚠ NO FLOOR IS COMPUTED HERE, DELIBERATELY. The first draft of this script carried
#: the arithmetic, which put a candidate derivation inside the measurement that is
#: supposed to constrain it. Codex ckpt-1 refused that derivation on two structural
#: grounds (the production arm was the wrong one; "reproduce the whole historical
#: supply" is a design choice the pass bar never declared), and a measurement script
#: that ships a refuted formula is how the formula gets re-adopted by whoever reads it
#: next. The census measures; the freeze script owns the arithmetic, once there is one.


def _positive_int(raw: str) -> int:
    """An argparse type that refuses 0 and negatives, naming the argument."""
    value = int(raw)
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1, got {value}")
    return value


def _concentration(dates: Mapping[date, int]) -> dict[str, int]:
    """Fires per distinct signal date — max, median and the top date's share.

    The floor is denominated in DATES precisely because signals fan out within one,
    so the spread of this distribution is what says whether a date count is evidence.
    """
    if not dates:
        return {"max_fires_on_one_date": 0, "median_fires_per_date": 0, "dates_with_one_fire": 0}
    counts = sorted(dates.values())
    return {
        "max_fires_on_one_date": counts[-1],
        "median_fires_per_date": counts[len(counts) // 2],
        "dates_with_one_fire": sum(1 for count in counts if count == 1),
    }


#: The two weightings the charged-band mix is reported under. ⚠ Neither is the
#: trade-weighted mix and they are NOT a bracket around it — see the module docstring.
WEIGHTINGS: Final[tuple[str, ...]] = ("all_fires", "max_hold_collapse")

#: ⚠ THE STRATEGY'S OWN FLOAT GATE, IMPORTED — not a local ``float(GATE_EDGE)``.
#: This file used to carry its own copy, which is a second definition of the rule's
#: threshold sitting in the script that measures the rule. It happened to agree, and
#: a recalibration that moved one and not the other would have produced a census of a
#: gate nobody declared. The licence for comparing on floats at all is a measurement
#: rather than an assumption — 0 of 75,972,669 stored prices sit within 1e-9 of a band
#: edge without equalling it (#3238) — and the BAND selection below stays on
#: ``Decimal`` regardless, because that is what is charged.
#:
#: ⚠ A masked close arrives as NaN and every NaN comparison is False, so a masked bar
#: fails ``>= PRICE_FLOOR`` untested — the same treatment the strategy gives it.
#:
#: There is deliberately no module-local alias: an alias is the same second definition
#: with a shorter lifetime, so the two call sites below read ``PRICE_FLOOR`` directly.

#: The per-(strategy, arm, year) name-day tallies. ⚠ THE DENOMINATOR AND THE NUMERATOR
#: COME OFF THE SAME EVALUATOR CALL, which is the whole reason they live here rather
#: than in a standalone query: ``segmented_signals`` emits exactly one entry verdict per
#: bar and RAISES otherwise (``strategy_segmented_evaluation.py:66-70``), so a verdict
#: count IS a name-day count over precisely the population the fires came from.
#: Reconstructing either half separately is the "a subtraction is not a gap count"
#: error — a previous attempt put 18,865,624 name-days over 4,593 series against this
#: census's 14,260 evaluated, which are different populations.
NAME_DAY_METRICS: Final[tuple[str, ...]] = (
    # Bars the rule could judge — every verdict that is not ``not_evaluable``.
    "evaluable",
    # …of those, the ones clearing S-12's gate: the population S-12 can fire on at all.
    # ⚠ NOT obtainable from the verdict stream: the gate is a bare ``return False``
    # inside ``entry(index)`` and stamps no reason, so a gate rejection and a failed
    # breakout are both plain ``not_fired``.
    "gate_clearing_evaluable",
    "fired",
    # ⚠ A CONSISTENCY CHECK, not a new fact. S-12 is S-4 AND the gate, so S-4's fires
    # restricted to gate-clearing bars must equal S-12's fired count exactly, and
    # S-12's own ``fired_gate_clearing`` must equal its ``fired``. Two identities for
    # one counter; the projection script refuses if either fails.
    "fired_gate_clearing",
)


def _round_trip_drag(half_spread: Decimal) -> Decimal:
    """The charged round trip as a fraction of the gross multiple: ``2h / (1 + h)``.

    A buy fills at ``P(1 + h)`` and the matching sell at ``Q(1 - h)``, so the net
    multiple is the gross one times ``(1 - h) / (1 + h)`` and the drag is what that
    removes. ⚠ NOT ``2h`` and NOT ``p75_spread_pct``: both are the first-order
    approximation, and the direction is the opposite of what it first looks —
    ``2h > 2h/(1+h)``, so the approximation errs DEAR. The exact form is used because
    it is the one the charge actually takes, not because it is the safer rounding
    (Codex ckpt-1, finding 30, which corrected this comment).
    """
    return 2 * half_spread / (1 + half_spread)


@dataclass
class _Dispersion:
    """Count / mean / M2 over one population of close-to-close returns.

    ⚠ WELFORD, MERGED IN CHUNKS (Chan et al.), not ``sum`` and ``sumsq``. The
    population is tens of millions of returns whose tails reach several hundred
    percent, and a naive second moment is the one place this script could quietly
    lose precision. The merge form lets each series be reduced in numpy and folded
    in once.
    """

    count: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def merge(self, *, count: int, mean: float, m2: float) -> None:
        if count == 0:
            return
        if self.count == 0:
            self.count, self.mean, self.m2 = count, mean, m2
            return
        total = self.count + count
        delta = mean - self.mean
        self.m2 += m2 + delta * delta * self.count * count / total
        self.mean += delta * count / total
        self.count = total

    @property
    def sd(self) -> float | None:
        """The sample SD, or ``None`` below two observations — never 0.0 by default."""
        return math.sqrt(self.m2 / (self.count - 1)) if self.count > 1 else None

    def add(self, value: float) -> None:
        """One observation. The merge form with a single-element chunk."""
        self.merge(count=1, mean=value, m2=0.0)

    def as_json(self) -> dict[str, object]:
        return {"count": self.count, "mean": self.mean, "sd": self.sd}


@dataclass
class _BandMix:
    """The charged-band tally over one (strategy, arm, weighting) cell."""

    fires: int = 0
    #: A successor bar exists but cannot be filled on (``resolve_fills`` refusal 2).
    unusable_fill_price: int = 0
    #: No successor bar at all (refusal 1). Should be 0 — the registry already stamps
    #: the final bar ``not_evaluable`` — so a non-zero count is a finding, not noise.
    no_fill_bar: int = 0
    by_band: Counter[str] = field(default_factory=Counter)
    drag_total: Decimal = Decimal(0)
    #: ⚠ FILL dates, not signal dates, and the distinction is the reason this exists.
    #: ``block_bootstrap.cluster_by_date`` clusters on ``trades.entry_fill_date``, so
    #: the stored ``bootstrap_cluster_count`` — the denominator any borrowed standard
    #: error is measured at — is a FILL-date count. The floor's own unit is
    #: ``signal_bar_date`` (``strategy_live_gate.py:392-395``). Reporting both makes
    #: the conversion between them measurable instead of assumed, and lets S-4's
    #: collapsed fill-date count be compared against its stored cluster count, which
    #: is the only available check on whether the collapse arm over-collapses as
    #: claimed.
    fill_dates: Counter[date] = field(default_factory=Counter)

    def as_json(self) -> dict[str, object]:
        charged = sum(self.by_band.values())
        return {
            "fires": self.fires,
            "charged_legs": charged,
            "unusable_fill_price": self.unusable_fill_price,
            "no_fill_bar": self.no_fill_bar,
            "distinct_fill_dates": len(self.fill_dates),
            "concentration": _concentration(self.fill_dates),
            "by_band": dict(sorted(self.by_band.items())),
            # ⚠ Denominated in CHARGED legs, not in fires: a fire with no usable fill
            # opens no position and pays nothing, so averaging it in as a zero would
            # understate the charge the mechanism buys down.
            "mean_round_trip_charge_pct": (float(self.drag_total / charged * 100) if charged else None),
            "cheapest_band_share": (self.by_band[CHEAPEST_BAND.label] / charged if charged else None),
        }


def _absorb_bands(
    *,
    fired_indices: Sequence[int],
    series: BarSeries,
    price_basis: PriceBasis,
    mixes: Mapping[str, _BandMix],
    decision_bar_dispersion: _Dispersion,
) -> None:
    """Tally the band each fire's FILL selects, under both weightings.

    The collapse arm is a greedy quarantine: after a FILLABLE fire at ``i`` nothing
    through ``i + MAX_HOLD_BARS`` is accepted, because a position opened at ``i + 1``
    can still be open at ``i + 1 + MAX_HOLD_BARS`` and every fire whose own fill lands
    inside that span is ``superseded_open_position``.

    ⚠ ONLY A FILLABLE FIRE QUARANTINES. A fire whose successor cannot be priced opens
    no position, so it supersedes nothing — treating it as if it did would drop real
    later entries from the collapse arm.

    ⚠⚠ THE DISPERSION TAKEN AT A FIRE IS THE TRAILING RETURN, ``close(i)/close(i-1)``,
    AND THE DIRECTION IS THE WHOLE POINT. It is the move INTO the decision bar —
    information the rule itself already has at decision time — so conditioning on a
    fire adds no look-ahead and reads no outcome. The FORWARD return at a fire would
    be the first bar of the position's own P&L, which is exactly the thing that must
    not be opened before the declaration is frozen.
    """
    rows = series.rows
    dates = series.dates
    closes = series.array_closes
    quarantined_until: int | None = None
    for index in sorted(fired_indices):
        if index >= 1:
            previous, current = closes[index - 1], closes[index]
            # NaN (a masked close) fails both comparisons, so it drops out untested.
            if previous > 0 and current > 0:
                decision_bar_dispersion.add(float(current / previous - 1.0))
        collapsible = quarantined_until is None or index > quarantined_until
        cells = [mixes["all_fires"]] + ([mixes["max_hold_collapse"]] if collapsible else [])
        for cell in cells:
            cell.fires += 1
        fill_index = index + 1
        if fill_index >= len(rows):
            for cell in cells:
                cell.no_fill_bar += 1
            continue
        fill_open = rows[fill_index].get("open")
        # ⚠ `<= 0`, not `== 0` — `resolve_fills`'s own two-sided test, copied rather
        # than approximated so this census refuses exactly what the run refuses.
        if fill_open is None or fill_open <= 0:
            for cell in cells:
                cell.unusable_fill_price += 1
            continue
        band = cost_band_for(fill_open, price_basis=price_basis)
        drag = _round_trip_drag(band.half_spread)
        for cell in cells:
            cell.by_band[band.label] += 1
            cell.drag_total += drag
            cell.fill_dates[dates[fill_index]] += 1
        if collapsible:
            # ⚠ `- 1`, and it is not cosmetic (Codex ckpt-1, finding 20). A position
            # opened here fills at `index + 1` and, at the cap, closes at
            # `index + 1 + MAX_HOLD_BARS`. `position_builder` supersedes on
            # `entry.fill_bar_date < open_until` — STRICTLY before — and states the
            # rule beside it: *"A closed position whose close date equals a later
            # entry's fill bar does NOT suppress it — rule 4, exit before entry."* So
            # the fire at `index + MAX_HOLD_BARS`, whose fill lands exactly on that
            # close, is NOT superseded, and the quarantine must end one bar earlier.
            quarantined_until = index + MAX_HOLD_BARS - 1


def _absorb_dispersion(series: BarSeries, *, dispersion: Mapping[tuple[str, str], _Dispersion], arm: str) -> None:
    """Fold one series' close-to-close returns into the two dispersion populations.

    The split is on the LEFT bar's close against the gate edge — i.e. the forward
    one-bar return CONDITIONAL on the decision bar clearing the gate, which is the
    conditioning the borrowed variance has to survive.

    ⚠ IT CONDITIONS ON THE PRICE LEVEL, NOT ON THE RULE. S-12 fires on compression
    and a breakout as well, so this is not "the dispersion of S-12's trades" and must
    not be reported as one. What it answers is narrower and is the only thing the
    derivation asks of it: does restricting to the cheapest band raise or lower
    per-bar dispersion, and by how much.

    ⚠ A masked close arrives as NaN through ``array_closes``, and every NaN
    comparison is False, so masked bars drop out of BOTH populations without a
    branch — the same treatment the strategy gives them.
    """
    closes = series.array_closes
    if closes.size < 2:
        return
    previous, following = closes[:-1], closes[1:]
    usable = (previous > 0) & (following > 0)
    if not usable.any():
        return
    base = previous[usable]
    returns = following[usable] / base - 1.0
    for population, sample in (("all", returns), ("gated", returns[base >= PRICE_FLOOR])):
        if sample.size == 0:
            continue
        dispersion[(arm, population)].merge(
            count=int(sample.size),
            mean=float(sample.mean()),
            # ⚠ ddof=0 here ON PURPOSE: `merge` accumulates M2 (the sum of squared
            # deviations), and the sample correction is applied once at the end in
            # `_Dispersion.sd`. Passing a corrected variance per chunk would apply it
            # once per series instead.
            m2=float(sample.var() * sample.size),
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="evaluate only the first N admitted series — a TIMING SLICE, never a population figure",
    )
    parser.add_argument(
        "--progress-every",
        # ⚠ VALIDATED AT PARSE TIME, not guarded at the modulo. `0` is the value a
        # reader reaches for to mean "no progress lines" and it would raise
        # ZeroDivisionError thirty minutes into a full-corpus pass — the worst place
        # for an argument error to surface (review NITPICK, PR #3243).
        type=_positive_int,
        default=500,
        help="series between progress lines; must be >= 1",
    )
    args = parser.parse_args(argv)

    fired: Counter[tuple[str, str]] = Counter()
    verdicts: Counter[tuple[str, str, str]] = Counter()
    #: ``(strategy_id, arm, calendar_year, metric)`` — see ``NAME_DAY_METRICS``. ⚠ KEYED
    #: BY YEAR because the projection it feeds is about a FORWARD 24-month window, and a
    #: single 58.9-year rate would hand that projection a stationarity assumption with
    #: nowhere to state it. Per-year counts make the spread of the rate a measurement.
    name_days: Counter[tuple[str, str, int, str]] = Counter()
    #: Every distinct bar date the corpus carries, per arm. ⚠ THE HORIZON'S UNIT,
    #: MEASURED. A projection over "24 months" has to turn that into panel NAME-DAYS,
    #: and the obvious 252-trading-days convention is an invented constant of exactly
    #: the kind "source-rule before design" forbids. Counting the dates the rates were
    #: measured ON keeps numerator, denominator and horizon on one population.
    #: ⚠ Not S-4's ``distinct_signal_dates``, which is a FIRE-date count and a lower
    #: bound: 11,616 over 58.9 years is ~197/year, well under any session count.
    bar_dates: dict[str, set[date]] = {arm: set() for arm in ARMS}
    signal_dates: dict[tuple[str, str], Counter[date]] = {(s, a): Counter() for s in STRATEGIES for a in ARMS}
    band_mix: dict[tuple[str, str, str], _BandMix] = {
        (s, a, w): _BandMix() for s in STRATEGIES for a in ARMS for w in WEIGHTINGS
    }
    dispersion: dict[tuple[str, str], _Dispersion] = {(a, p): _Dispersion() for a in ARMS for p in ("all", "gated")}
    #: The trailing return at each FIRE, per strategy and arm — the like-for-like
    #: comparator the unconditional populations above cannot be. See `_absorb_bands`.
    decision_bar_dispersion: dict[tuple[str, str], _Dispersion] = {
        (s, a): _Dispersion() for s in STRATEGIES for a in ARMS
    }
    entries = {strategy_id: STRATEGY_MANIFEST[strategy_id] for strategy_id in STRATEGIES}

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        # ⚠ The window ENDS BEFORE the boundary, so no withheld bar is loaded. The
        # default window would end at the vendor's capture date (2024-09-27) and hand
        # every hold-out bar to the rule for verdicts this census then discards.
        corpus = load_corpus(
            conn,
            universe_basis=BACKTEST_UNIVERSE,
            limit=args.limit,
            evaluation_window=Window(start=EVALUATION_WINDOW_START, end=HOLDOUT_BOUNDARY - timedelta(days=1)),
        )
        # ⚠ The spec's guard 3, asserted here too: a run whose archive provenance went
        # missing is charged the maximum band on every leg, and a supply census taken
        # over a corpus that failed closed would be describing a different population
        # from the one the declaration names.
        if corpus.cost_price_basis != "as_traded":
            raise RuntimeError(
                f"corpus cost_price_basis resolved to {corpus.cost_price_basis!r}, not 'as_traded' — "
                "the pinned archive's provenance is missing and S-12's nominal gate is not measurable here"
            )
        # ⚠⚠ `through_date` IS REQUIRED HERE AND WAS MISSING (Codex ckpt-1, finding 26).
        # `load_research` defaults it to None, which loads and CLASSIFIES the whole
        # benchmark chain — including every post-boundary bar. Neither S-4 nor S-12
        # reads the regime input, so no number this script printed was contaminated;
        # the claim in the header ("no hold-out bar is loaded") was nonetheless false,
        # and a rule that later took a regime condition would have been handed withheld
        # data with nothing to say so.
        regime_provider = MarketRegimeProvider.load_research(conn, through_date=corpus.window.end)
        opportunity = corpus.opportunity_records["in_sample"]
        opportunity_keys = set(opportunity.evaluated_instrument_ids) | {
            -series_id for series_id in opportunity.evaluated_series_ids
        }
        corpus_window_end = corpus.window.end
        corpus_price_basis = corpus.cost_price_basis
        # ⚠ The RUN's universe, not ``BACKTEST_UNIVERSE`` re-imported: the identity has
        # to describe what this pass evaluated, and the universe sits inside it.
        corpus_universe = corpus.universe_basis
        total = len(corpus.pairs)
        evaluated = 0
        for series_seen, (name_key, series_id) in enumerate(corpus.pairs, start=1):
            if series_seen % args.progress_every == 0:
                print(f"... {series_seen:,}/{total:,} series seen, {evaluated:,} evaluated", flush=True)
            if name_key not in opportunity_keys:
                continue
            arms = load_arms(conn, series_id, through_date=corpus.window.end)
            breaks = corpus.unresolved_breaks.get(name_key, ())
            for arm in ARMS:
                loaded = arms[arm]
                if not loaded.bars:
                    continue
                series = _to_series(loaded.bars)
                if len(series) < 2:
                    continue
                regime = regime_provider.for_dates(series.dates)
                _absorb_dispersion(series, dispersion=dispersion, arm=arm)
                bar_dates[arm].update(series.dates)
                # ⚠ Bound once per (series, arm): the gate test below runs on every
                # evaluable bar of both strategies, so re-reading the property per
                # signal would double the array lookups for nothing.
                series_closes = series.array_closes
                for strategy_id, entry in entries.items():
                    fired_indices: list[int] = []
                    for signal in segmented_signals(
                        entry,
                        series,
                        universe=corpus.universe_basis,
                        masked_reason="quarantined_bar",
                        unresolved_breaks=breaks,
                        regime=regime,
                    ):
                        if signal.kind != "entry":
                            continue
                        when = series.dates[signal.signal_index]
                        # ⚠ The window already stops before the boundary, so this is a
                        # belt-and-braces assertion of the same fact rather than the
                        # filter it used to be.
                        assert when < HOLDOUT_BOUNDARY
                        verdicts[(strategy_id, arm, signal.verdict)] += 1
                        # ⚠ The gate is tested ONLY on bars the rule could judge. A
                        # ``not_evaluable`` bar is not a name-day either strategy could
                        # have fired on, so counting it would inflate the denominator
                        # and understate the rate — and a masked bar's close is NaN,
                        # which would silently fail the comparison anyway.
                        if signal.verdict != "not_evaluable":
                            clears_gate = bool(series_closes[signal.signal_index] >= PRICE_FLOOR)
                            name_days[(strategy_id, arm, when.year, "evaluable")] += 1
                            if clears_gate:
                                name_days[(strategy_id, arm, when.year, "gate_clearing_evaluable")] += 1
                            if signal.verdict == "fired":
                                name_days[(strategy_id, arm, when.year, "fired")] += 1
                                if clears_gate:
                                    name_days[(strategy_id, arm, when.year, "fired_gate_clearing")] += 1
                        if signal.verdict == "fired":
                            fired[(strategy_id, arm)] += 1
                            signal_dates[(strategy_id, arm)][when] += 1
                            fired_indices.append(signal.signal_index)
                    # ⚠ PER SERIES AND PER ARM. The collapse quarantine is a property
                    # of one instrument's own bar index; carrying it across series
                    # would let one name's position supersede another's entry.
                    _absorb_bands(
                        fired_indices=fired_indices,
                        series=series,
                        price_basis=corpus.cost_price_basis,
                        mixes={w: band_mix[(strategy_id, arm, w)] for w in WEIGHTINGS},
                        decision_bar_dispersion=decision_bar_dispersion[(strategy_id, arm)],
                    )
            evaluated += 1
        conn.rollback()

    def _supply(strategy_id: str, arm: str) -> dict[str, object]:
        by_date = signal_dates[(strategy_id, arm)]
        ordered = sorted(by_date)
        return {
            "fired": fired[(strategy_id, arm)],
            "distinct_signal_dates": len(by_date),
            "first_signal_date": ordered[0].isoformat() if ordered else None,
            "last_signal_date": ordered[-1].isoformat() if ordered else None,
            "span_days": (ordered[-1] - ordered[0]).days if len(ordered) > 1 else 0,
            "concentration": _concentration(by_date),
            "verdicts": {
                verdict: count
                for (sid, a, verdict), count in sorted(verdicts.items())
                if sid == strategy_id and a == arm
            },
        }

    def _name_day_supply(strategy_id: str, arm: str) -> dict[str, object]:
        """Name-day counts for one cell, in total and by calendar year.

        ⚠ COUNTS ONLY — no rate is divided here, deliberately. The module header's
        rule that this census measures and the downstream script owns the arithmetic
        binds a fire RATE exactly as it binds a floor: a rate is a projection input
        with a homogeneity assumption attached, and the assumption has to be stated
        where the projection is made, not buried in a report key.
        """
        years = sorted({year for (sid, a, year, _metric) in name_days if sid == strategy_id and a == arm})
        # ⚠ Iterating a date-keyed Counter yields DISTINCT dates, which is the unit the
        # floor is denominated in (``prereg_contract.py:127-132``) — not its fire count.
        dates_by_year: Counter[int] = Counter(when.year for when in signal_dates[(strategy_id, arm)])
        return {
            "totals": {
                metric: sum(name_days[(strategy_id, arm, year, metric)] for year in years)
                for metric in NAME_DAY_METRICS
            },
            "by_year": {
                str(year): {
                    **{metric: name_days[(strategy_id, arm, year, metric)] for metric in NAME_DAY_METRICS},
                    "distinct_signal_dates": dates_by_year[year],
                }
                for year in years
            },
        }

    def _dispersion_report(arm: str) -> dict[str, object]:
        everything, gated = dispersion[(arm, "all")], dispersion[(arm, "gated")]
        sd_all, sd_gated = everything.sd, gated.sd
        return {
            "all_bars": everything.as_json(),
            "gated_bars": gated.as_json(),
            # ⚠ The ratio is the whole point: >= 1 means the borrowed S-4 standard
            # error UNDERSTATES S-12's and the derivation must inflate it; < 1 means
            # the borrow is conservative. Neither direction is assumed anywhere.
            "sd_ratio_gated_over_all": (sd_gated / sd_all if sd_all and sd_gated else None),
        }

    report: dict[str, object] = {
        "universe": BACKTEST_UNIVERSE,
        "holdout_boundary": HOLDOUT_BOUNDARY.isoformat(),
        "evaluation_window_end": corpus_window_end.isoformat(),
        "production_arm": "masked",
        "cost_price_basis": corpus_price_basis,
        "cost_model_id": COST_MODEL_ID,
        "gate_edge": str(GATE_EDGE),
        "cheapest_band": CHEAPEST_BAND.label,
        "max_hold_bars": MAX_HOLD_BARS,
        "limited_to_series": args.limit,
        "series_evaluated": evaluated,
        # ⚠⚠ THE IDENTITY OF WHAT WAS ACTUALLY RUN, so a downstream reader can tell a
        # stale census from a current one. ``gate_edge`` and ``max_hold_bars`` are the
        # two parameters a consumer is most likely to check, and neither moves when S-4's
        # body, S-12's body or the shared indicator code changes — so a census taken
        # before such an edit would pass every other provenance test and publish its
        # rates under the current strategy names. ``StrategyIdentity.version`` hashes the
        # registry module, the source hash, the params, the universe and the cost model
        # together, which is exactly the set that has to move.
        "strategy_versions": {
            strategy_id: entry.identity(universe=corpus_universe, cost_model_id=COST_MODEL_ID).version
            for strategy_id, entry in entries.items()
        },
        "supply": {f"{strategy_id}/{arm}": _supply(strategy_id, arm) for strategy_id in STRATEGIES for arm in ARMS},
        "charged_band_mix": {
            f"{strategy_id}/{arm}/{weighting}": band_mix[(strategy_id, arm, weighting)].as_json()
            for strategy_id in STRATEGIES
            for arm in ARMS
            for weighting in WEIGHTINGS
        },
        # ⚠⚠ THE DENOMINATOR THE SIZING QUESTION NEEDS, AND THE REASON IT IS NOT THE
        # ``gated_bars`` COUNT BELOW. ``bar_return_dispersion``'s gated population is a
        # RETURN-PAIR count: it requires a usable SUCCESSOR bar and it includes bars the
        # rule could not evaluate. Close to this, and not the same population — which is
        # the one property a fire-rate denominator has to have.
        # ⚠ The horizon's unit, measured on the same population as the rates. See
        # ``bar_dates``. A year here is a CALENDAR year of the corpus, so the first and
        # last are partial and a projection must not read them as full years.
        "corpus_bar_dates": {
            arm: {
                "total": len(bar_dates[arm]),
                "by_year": {
                    str(year): count for year, count in sorted(Counter(d.year for d in bar_dates[arm]).items())
                },
            }
            for arm in ARMS
        },
        "name_day_supply": {
            f"{strategy_id}/{arm}": _name_day_supply(strategy_id, arm) for strategy_id in STRATEGIES for arm in ARMS
        },
        "bar_return_dispersion": {arm: _dispersion_report(arm) for arm in ARMS},
        # ⚠ THE COMPARATOR THE DERIVATION USES. The two populations above are
        # unconditional and the corpus's raw bar-return tails dominate them; these are
        # conditioned on the two rules' own fires, which is the like-for-like pair.
        "decision_bar_dispersion": {
            f"{strategy_id}/{arm}": decision_bar_dispersion[(strategy_id, arm)].as_json()
            for strategy_id in STRATEGIES
            for arm in ARMS
        },
    }
    if args.limit is not None:
        report["WARNING"] = (
            "LIMITED SLICE — a timing measurement, not a population figure; no floor may be frozen from it"
        )
    print(json.dumps(report, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["ARMS", "NAME_DAY_METRICS", "S4_STRATEGY_ID", "STRATEGIES", "WEIGHTINGS", "main"]
