"""Project what a forward ``<= 50``-name straddling panel ACCRUES — #2840 arm 2 step 1.

Reads ``scripts/census_2840_s12_signal_supply.py``'s report and turns its measured
name-day counts into the quantities step 1's sizing item asks for: daily sleeve
observations and entry-fill-date CLUSTERS, per unit time, for a declared panel.

⚠⚠ THIS PROJECTS SUPPLY. IT DOES NOT SAY WHETHER THE SUPPLY IS ENOUGH, AND IT DOES NOT
SAY THE SUPPLY "CLEARS" ANYTHING. ``MIN_SUPPORTING_CLUSTERS = MIN_CLUSTERS = 2`` is a
computability guard on an OBSERVED count; an expectation above it still leaves positive
probability of a realised count below it, and no acquisition-probability model is
supplied here. ``strategy_s12_paired_trial`` says in terms why no other floor exists:
*"No sample-size floor certifying 2.5%-tail coverage is derivable here, and none is
invented."* The spec's power half is separately blocked — the declared ``d = 0.5`` has
no defined denominator for a difference between two strategies' books. So this script
reports accrual, names the guard's value, and passes no verdict. **Deriving a required
n here would re-open the route checkpoint 1 refused twice.**

THE ESTIMAND, AND WHY THE CONTROL IS SPLIT TOO (Codex ckpt-1, findings 3-5)
---------------------------------------------------------------------------
The first draft divided S-12 by gate-clearing name-days and S-4 by ALL name-days, and
called that "the denominator each strategy can fire on". It is not a coherent pair: on
ONE shared panel the control trades the same names the candidate does, so the gate
thins S-4's population too, and S-4's pooled corpus rate is the wrong number to
multiply by the panel. The fix is to split the control's rate on the same edge::

    r_above = S-12 fired / gate-clearing evaluable name-days      (== S-4's above rate)
    r_below = (S-4 fired - S-4 fired-on-gate-clearing) / (evaluable - gate-clearing)

    S-12 expected fires = above_name_days x r_above
    S-4  expected fires = above_name_days x r_above + below_name_days x r_below

⚠ THAT PAIR IS SELF-CHECKING AT THE DEGENERATE END, which the first draft's was not.
Drive the sub-gate share to zero and the two expressions become IDENTICAL — which is
exactly what must happen, because a panel where every member clears the gate makes the
gate reject nothing and the two books the same book. The projection asserts it.

⚠ ``r_above`` is ONE number, not two, and that is an identity rather than an
approximation: S-12 IS S-4 AND the gate, so S-4's fires restricted to gate-clearing
bars ARE S-12's fires. ``_check`` refuses a census where they differ.

FIRES ARE PRIMARY; POSITIONS ARE A LABELLED SCENARIO (finding 14)
------------------------------------------------------------------
The fire projection needs only linearity of expectation. Converting fires to POSITIONS
needs the census's ``max_hold_collapse`` factor, which is pooled over each strategy's
own corpus population — S-12's over gate-clearing bars, S-4's over all bars. Those are
different populations, so the two factors do NOT coincide at the degenerate end and the
position projection breaks the identity above by a measurable amount. That residual is
REPORTED rather than argued away: it is the size of the transport assumption.

⚠ The factor is taken over ``charged_legs``, not ``fires`` (finding 15). ``_absorb_bands``
increments ``fires`` BEFORE testing the successor bar, so ``fires`` includes entries that
open no position; ``charged_legs`` is the fillable count.

DISTINCT DATES ARE A SCENARIO, NOT A BOUND (findings 19-21)
------------------------------------------------------------
The first draft called the independence estimate an upper bound. **That is false.** For
two names each firing on half of 100 days: independence gives 75 occupied days, perfect
synchronisation 50, and mutual exclusion 100 — independence sits between them and bounds
neither side. The formula additionally assumes IDENTICAL per-name probabilities
(independent 0.2 and 0.8 give 84 per 100 where their pooled 0.5 gives 75). So it is
reported as a scenario under two named assumptions.

⚠ The one genuine upper bound is arithmetic and is reported beside it: a date carries at
least one position, so distinct dates cannot exceed ``min(sessions, positions)``.

BOTH ARMS, BECAUSE THE PASS BAR IS A CONJUNCTION (finding 33)
--------------------------------------------------------------
``masked`` is the production arm and a forward CLAIM must read it — but the evaluator
requires ``masked`` AND ``admitted`` by name, so supply is projected for both.

WHAT REMAINS ASSUMED, AND IS NOT FIXABLE HERE
----------------------------------------------
Stated so a reader does not have to infer them; every one is carried into the output.

1. **Transfer.** The rates are conditional on an evaluable historical name-day in a
   survivorship-free 1962-2021 cross-section. A panel of currently-listed names is not a
   draw from that population, and price eligibility does not establish equal propensity.
   ⚠ DIRECTION UNKNOWN, and not asserted.
2. **Membership lifecycle.** Names cross the gate, split, delist, merge and get
   suspended. Holding the declared share fixed implies a replacement policy that this
   projection does not specify and that would itself change the experiment.
3. **Stationarity.** Per-year rates are reported with their spread and a pooled
   recent-window rate beside the pooled full-history one, so the size of the assumption
   is visible. The projection still multiplies a pooled historical rate.
4. **Positions are not usable trade clusters.** Unresolved exits, window-end censoring
   and termination handling all remove realised returns between an opened position and a
   supported cluster. Entry supply is an upper bound on the evaluator's supported dates.
5. **Straddling buys disagreement only if the sub-gate names actually fire.** A panel
   can straddle on price and still produce two near-identical books.
6. **The forward collection path is not established by this.** ``s12_signals`` refuses
   ``SCAN_UNIVERSE = survivor_only``, so a declared panel does not by itself let S-12
   emit a forward signal. Panel capacity is not exposure.

Read-only. Touches no database, opens no outcome, writes no row.

Refs #2840, #2437.
"""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Callable
from pathlib import Path
from statistics import median
from typing import Any, Final

from app.services.cost_model import COST_MODEL_ID
from app.services.strategies.s12_cheapest_band_price_gated_breakout import (
    AS_TRADED_UNIVERSES,
    GATE_EDGE,
    MAX_HOLD_BARS,
    S12_STRATEGY_ID,
)
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_quote_observation import MAX_PANEL_INSTRUMENTS
from app.services.strategy_s12_paired_trial import MIN_SUPPORTING_CLUSTERS

S4_STRATEGY_ID: Final = "s4-volatility-compression-breakout"

#: ⚠ THE PRODUCTION ARM. ``price_masked_bars`` carries one arm on purpose and it is this
#: one; ``admitted`` is a sensitivity measurement with no place in a forward claim. An
#: earlier derivation on this ticket was refused for reading the other one. Both are
#: projected because the pass bar is a conjunction over both.
PRODUCTION_ARM: Final = "masked"
ARMS: Final[tuple[str, ...]] = ("masked", "admitted")

#: ⚠ DECLARED AT 24 MONTHS by the spec's §"Relevance horizon and planning effect", NOT
#: derived — and the spec is explicit that adopting the current-quote-evidence horizon
#: for strategy-return evidence is a choice it declares rather than an inheritance.
#: ⚠ Its start date and reset semantics are an open gap in the spec, so the horizon here
#: is a LENGTH and not a dated window.
DEFAULT_HORIZON_MONTHS: Final = 24

#: The sub-gate shares the grid reports. ⚠ Neither endpoint is included: a share of 0
#: makes every member clear the gate and the two books identical; a share of 1 leaves
#: S-12 with no eligible name at all. ⚠ The two are NOT the same null — the first is an
#: identical-books zero, the second an undefined expectancy.
SUB_GATE_SHARE_GRID: Final[tuple[float, ...]] = (0.25, 0.5, 0.75)

#: Whole calendar years the pooled recent-window rate reads. ⚠ A REPORTING choice with
#: no threshold attached: it exists so the pooled 58.9-year figure can be seen against a
#: shorter one, not to select between them.
RECENT_YEARS: Final = 10

#: ⚠ DERIVED FROM THE GRID, not typed. The smallest panel that can express EVERY declared
#: share with whole names on both sides of the gate. A literal floor of 2 was accepted at
#: the CLI and then blew up inside the loop on the 0.75 share (review WARNING 3), which is
#: a crash rather than a refusal and drops the answerable grid points with it. Tying the
#: floor to the grid means adding a more extreme share moves the floor with it.
MIN_PANEL_SIZE: Final[int] = next(
    size
    for size in range(2, MAX_PANEL_INSTRUMENTS + 1)
    if all(1 <= math.ceil(size * share) <= size - 1 for share in SUB_GATE_SHARE_GRID)
)


class ProjectionRefused(ValueError):
    """The census report cannot support a projection, and saying so beats a number."""


def _load(path: Path) -> dict[str, Any]:
    """The census report, tolerating the progress lines it prints before the JSON."""
    raw = path.read_text()
    start, end = raw.find("{"), raw.rfind("}")
    if start < 0 or end < start:
        raise ProjectionRefused(f"{path} contains no JSON object")
    # ⚠ BOTH ENDS ARE TRIMMED. The census prints progress lines BEFORE the report, and a
    # run captured with a shell wrapper carries an exit code and a timestamp AFTER it —
    # so slicing only the head leaves trailing text and json refuses the whole file.
    loaded = json.loads(raw[start : end + 1])
    if not isinstance(loaded, dict):
        raise ProjectionRefused(f"{path} does not contain a JSON object")
    return loaded


def _cell(report: dict[str, Any], strategy_id: str, arm: str) -> dict[str, Any]:
    return report["name_day_supply"][f"{strategy_id}/{arm}"]


def _check(report: dict[str, Any]) -> None:
    """Refuse every state in which a projection would mean something it does not say.

    ⚠ PROVENANCE IS PINNED HERE, NOT ASSUMED (Codex ckpt-1, finding 35). A census taken
    on a different universe, a split-adjusted basis or a moved gate edge produces
    perfectly well-formed counts describing a different experiment, and nothing
    downstream would notice.
    """
    if report.get("limited_to_series") is not None:
        raise ProjectionRefused(
            f"census was limited to {report['limited_to_series']} series — a TIMING SLICE. "
            "Its counts are not a population figure and no projection may be built on them."
        )
    if report.get("production_arm") != PRODUCTION_ARM:
        raise ProjectionRefused(
            f"census names {report.get('production_arm')!r} as the production arm, not {PRODUCTION_ARM!r}"
        )
    if report.get("cost_price_basis") != "as_traded":
        raise ProjectionRefused(
            f"census ran on cost_price_basis {report.get('cost_price_basis')!r}; S-12's gate is a NOMINAL price "
            "threshold and is not measurable on a split-adjusted corpus"
        )
    if str(report.get("gate_edge")) != str(GATE_EDGE):
        raise ProjectionRefused(
            f"census measured gate edge {report.get('gate_edge')!r} against the rule's {GATE_EDGE!r}; "
            "the band table has moved and every count describes a gate nobody declared"
        )
    # ⚠ THE RULE'S OWN DECLARED SET, not a guess at which universes are as-traded.
    # ``AS_TRADED_UNIVERSES`` is hashed into S-12's identity and ``s12_signals`` refuses
    # every bar outside it, so a census taken elsewhere measured a rule that refuses.
    if report.get("universe") not in AS_TRADED_UNIVERSES:
        raise ProjectionRefused(
            f"census ran on universe {report.get('universe')!r}, which S-12 does not accept as as-traded "
            f"(AS_TRADED_UNIVERSES = {sorted(AS_TRADED_UNIVERSES)}); the rule refuses every bar there"
        )
    # ⚠ The collapse factor quarantines exactly this many bars, so a census taken under a
    # different cap converted fires to positions under a different rule.
    if report.get("max_hold_bars") != MAX_HOLD_BARS:
        raise ProjectionRefused(
            f"census ran at max_hold_bars {report.get('max_hold_bars')!r} against the rule's {MAX_HOLD_BARS}; "
            "its collapse arm quarantines a span this projection does not describe"
        )
    # ⚠⚠ THE CHECK THE PARAMETER PINS ABOVE CANNOT MAKE (Codex ckpt-2). Neither the gate
    # edge nor the hold cap moves when S-4's body, S-12's body, the shared indicator code
    # or the registry changes — so without this a census taken before such an edit passes
    # every other test and has its rates published under the current strategy names.
    # ``StrategyIdentity.version`` hashes exactly the set that has to move.
    stored = report.get("strategy_versions")
    if not isinstance(stored, dict):
        raise ProjectionRefused(
            "census carries no strategy_versions block; it predates the provenance pin and cannot be shown to "
            "describe the rules running now — re-run the census"
        )
    universe = report["universe"]
    for strategy_id in (S12_STRATEGY_ID, S4_STRATEGY_ID):
        live = STRATEGY_MANIFEST[strategy_id].identity(universe=universe, cost_model_id=COST_MODEL_ID).version
        if stored.get(strategy_id) != live:
            raise ProjectionRefused(
                f"census measured {strategy_id} at version {stored.get(strategy_id)!r} against the live "
                f"{live!r}; the rule has changed and its stored counts describe a different strategy"
            )
    for arm in ARMS:
        _check_arm(report, arm)


def _check_arm(report: dict[str, Any], arm: str) -> None:
    """The count identities, per arm. Each is exact — none is an approximation."""
    s12, s4 = _cell(report, S12_STRATEGY_ID, arm), _cell(report, S4_STRATEGY_ID, arm)
    totals12, totals4 = s12["totals"], s4["totals"]
    # ⚠ S-12 IS S-4 AND THE GATE, so these are identities. A violation means the census
    # counted a different population from the one the projection describes, and every
    # rate below would be wrong in a way no output makes visible.
    if totals12["fired_gate_clearing"] != totals12["fired"]:
        raise ProjectionRefused(
            f"{arm}: S-12 fired {totals12['fired']} but only {totals12['fired_gate_clearing']} on gate-clearing "
            "bars; its gate admits nothing else, so the census and the rule disagree"
        )
    if totals4["fired_gate_clearing"] != totals12["fired"]:
        raise ProjectionRefused(
            f"{arm}: S-4 fired {totals4['fired_gate_clearing']} times on gate-clearing bars against S-12's "
            f"{totals12['fired']}; S-12 is S-4 AND the gate, so these must be equal"
        )
    if totals12["evaluable"] != totals4["evaluable"]:
        raise ProjectionRefused(
            f"{arm}: S-12 evaluated {totals12['evaluable']} name-days against S-4's {totals4['evaluable']}; the "
            "gate is a body condition that turns fired into not_fired and introduces no unevaluable state"
        )
    if totals12["gate_clearing_evaluable"] != totals4["gate_clearing_evaluable"]:
        raise ProjectionRefused(
            f"{arm}: the two strategies disagree on the gate-clearing population "
            f"({totals12['gate_clearing_evaluable']} against {totals4['gate_clearing_evaluable']})"
        )
    for strategy_id, totals, cell in ((S12_STRATEGY_ID, totals12, s12), (S4_STRATEGY_ID, totals4, s4)):
        if not 0 < totals["gate_clearing_evaluable"] < totals["evaluable"]:
            raise ProjectionRefused(
                f"{arm}/{strategy_id}: gate-clearing name-days ({totals['gate_clearing_evaluable']}) must sit "
                f"strictly inside the evaluable population ({totals['evaluable']}); both sides of the gate are "
                "needed and neither may be empty"
            )
        if totals["fired"] > totals["evaluable"]:
            raise ProjectionRefused(f"{arm}/{strategy_id}: {totals['fired']} fires over {totals['evaluable']} bars")
        # ⚠ The per-year cells must reconcile with the totals they claim to decompose;
        # a by-year block that does not sum makes every per-year rate a different
        # measurement from the pooled one it is printed beside.
        for metric, total in totals.items():
            summed = sum(year_cell[metric] for year_cell in cell["by_year"].values())
            if summed != total:
                raise ProjectionRefused(
                    f"{arm}/{strategy_id}: by_year {metric} sums to {summed} against a total of {total}"
                )


def _whole_years(by_year: dict[str, int]) -> list[int]:
    """The census's complete calendar years — interior, contiguous, and checked.

    ⚠ The first and last are PARTIAL (the window opens mid-1962 and is cut the day
    before the hold-out boundary), so both are dropped; keeping them would drag any
    session count down by two half-years. ⚠ Dropping them does not by itself prove the
    interior is complete (Codex ckpt-1, finding 31), so contiguity is asserted.
    """
    years = sorted(int(year) for year in by_year)
    interior = years[1:-1]
    if not interior:
        raise ProjectionRefused("census carries fewer than three calendar years; no whole year to count sessions on")
    if interior != list(range(interior[0], interior[-1] + 1)):
        raise ProjectionRefused(f"census is missing calendar years inside {interior[0]}-{interior[-1]}")
    return interior


def _sessions_per_year(report: dict[str, Any], arm: str) -> float:
    """Trading days per year, MEASURED — the median over the census's whole years.

    ⚠ NOT 252. A trading-day convention is an invented constant of exactly the kind
    "source-rule before design" forbids, and it would also be the wrong population: the
    rates below are per name-day on THIS corpus, so the horizon is counted on the same
    dates. ⚠ It remains a PLANNING unit — the forward window's start date, holiday
    calendar and reset semantics are an open gap in the spec, so this is a length and
    not a dated calendar.
    """
    by_year = report["corpus_bar_dates"][arm]["by_year"]
    return float(median([by_year[str(year)] for year in _whole_years(by_year)]))


#: ⚠ EXTRACTORS, NOT COLUMN NAMES (Codex ckpt-2). The control's projection is driven by
#: its SUB-GATE rate, which is a difference of two stored counts and has no column of its
#: own — and reporting the annual spread of its pooled ``fired / evaluable`` instead
#: would let a shifting above/sub-gate mix keep the diagnostic flat while the rate the
#: projection actually multiplies moved. A stationarity diagnostic has to cover the rate
#: being projected, so the spread takes the same expressions the projection does.
def _fired(year_cell: dict[str, Any]) -> int:
    return year_cell["fired"]


def _gate_clearing_name_days(year_cell: dict[str, Any]) -> int:
    return year_cell["gate_clearing_evaluable"]


def _sub_gate_fired(year_cell: dict[str, Any]) -> int:
    return year_cell["fired"] - year_cell["fired_gate_clearing"]


def _sub_gate_name_days(year_cell: dict[str, Any]) -> int:
    return year_cell["evaluable"] - year_cell["gate_clearing_evaluable"]


_Extractor = Callable[[dict[str, Any]], int]


def _rate_by_year(cell: dict[str, Any], *, numerator: _Extractor, denominator: _Extractor) -> dict[int, float]:
    """Per-year rate, skipping years whose denominator is 0 rather than emitting 0.0."""
    return {
        int(year): numerator(year_cell) / denominator(year_cell)
        for year, year_cell in cell["by_year"].items()
        if denominator(year_cell) > 0
    }


def _spread(cell: dict[str, Any], *, numerator: _Extractor, denominator: _Extractor) -> dict[str, Any]:
    """The shape of a rate across years — the stationarity assumption, made visible.

    ⚠ ``recent`` is a POOLED ratio over the last contiguous whole years, not a mean of
    annual rates (Codex ckpt-1, finding 13). The first draft averaged rates, which
    equally weights a thin year against a dense one and silently included partial years.
    """
    rates = _rate_by_year(cell, numerator=numerator, denominator=denominator)
    if not rates:
        return {"years": 0, "min": None, "median": None, "max": None, "pooled_recent": None, "recent_window": None}
    values = sorted(rates.values())
    recent = _whole_years(cell["by_year"])[-RECENT_YEARS:]
    numerator_total = sum(numerator(cell["by_year"][str(year)]) for year in recent)
    denominator_total = sum(denominator(cell["by_year"][str(year)]) for year in recent)
    return {
        "years": len(rates),
        "min": values[0],
        "median": median(values),
        "max": values[-1],
        "pooled_recent": (numerator_total / denominator_total if denominator_total else None),
        "recent_window": f"{recent[0]}-{recent[-1]}",
    }


def _occupied_dates(*, rate: float, names: int, days: float) -> float:
    """Days carrying at least one entry, under independence and IDENTICAL per-name rates.

    ⚠⚠ NOT A BOUND IN EITHER DIRECTION (Codex ckpt-1, findings 20-21). Two names each
    firing on half of 100 days give 75 occupied days under independence, 50 under
    perfect synchronisation and 100 under mutual exclusion — independence sits between
    them and bounds neither. It additionally assumes every name shares one rate:
    independent 0.2 and 0.8 give 84 per 100 where their pooled 0.5 gives 75. A scenario,
    labelled as one.
    """
    if names <= 0 or rate <= 0:
        return 0.0
    return days * (1.0 - math.pow(1.0 - min(rate, 1.0), names))


def _expected_fires(
    *,
    above: int,
    below: int,
    horizon_days: float,
    evaluability: float,
    rate_above_candidate: float,
    rate_above_control: float,
    rate_below: float,
) -> tuple[float, float]:
    """Expected fires for the two books on one panel. Linearity only — no independence.

    ⚠⚠ THE TWO ABOVE-GATE RATES ARE SEPARATE PARAMETERS ON PURPOSE. They are equal by
    identity on any census that clears ``_check_arm`` — S-12 IS S-4 AND the gate — but
    taking them as one argument is what made the degenerate-end check tautological
    (review WARNING 1): with ``below = 0`` the control's second term vanishes, so a
    single shared rate meant two expressions in one variable were being compared.
    Separate parameters make the check a real comparison, and make it TESTABLE by
    passing rates that disagree.
    """
    above_days, below_days = above * horizon_days * evaluability, below * horizon_days * evaluability
    return above_days * rate_above_candidate, above_days * rate_above_control + below_days * rate_below


def _split_panel(panel_size: int, share: float) -> tuple[int, int]:
    """Whole names either side of the gate. ⚠ A panel holds names, not fractions.

    The sub-gate side is rounded UP, so the above-gate count — which drives the
    candidate's whole book — is never rounded in the optimistic direction. Both sides
    must be non-empty or the panel does not straddle.
    """
    below = max(1, math.ceil(panel_size * share))
    above = panel_size - below
    if above < 1:
        raise ProjectionRefused(
            f"a sub-gate share of {share} on {panel_size} names leaves {above} above the gate; "
            "the panel must straddle, and a panel with no eligible name gives S-12 no book at all"
        )
    return above, below


def _project_arm(report: dict[str, Any], *, arm: str, panel_size: int, horizon_months: int) -> dict[str, Any]:
    """The whole projection for one quarantine arm."""
    s12, s4 = _cell(report, S12_STRATEGY_ID, arm), _cell(report, S4_STRATEGY_ID, arm)
    totals12, totals4 = s12["totals"], s4["totals"]
    sessions = _sessions_per_year(report, arm)
    horizon_days = sessions * horizon_months / 12.0

    # ⚠ THE PANEL'S CALENDAR DAYS ARE NOT ITS EVALUABLE NAME-DAYS (finding 2). The rates
    # below are per EVALUABLE name-day, so multiplying raw panel-days would overstate
    # exposure by everything the rule cannot judge — warm-up, holes, quarantined bars.
    # ⚠ Measured from this census, and it is CONSERVATIVE for a continuing panel: a
    # 113-bar warm-up is paid once per series here and once ever by a live panel.
    # ⚠ The FULL verdict sum is checked across both strategies, not just the ``evaluable``
    # sub-count (review WARNING 2). ``_check_arm`` asserts the evaluable halves agree;
    # this ratio's denominator is the whole tally, so a census whose two strategies saw
    # different bar counts would have one strategy's evaluability applied to both.
    seen = {
        strategy_id: sum(report["supply"][f"{strategy_id}/{arm}"]["verdicts"].values())
        for strategy_id in (S12_STRATEGY_ID, S4_STRATEGY_ID)
    }
    if seen[S12_STRATEGY_ID] != seen[S4_STRATEGY_ID]:
        raise ProjectionRefused(
            f"{arm}: the two strategies saw different name-day counts ({seen}); segmented_signals emits one "
            "entry verdict per bar for each, so a disagreement means they did not evaluate the same series"
        )
    name_days_seen = seen[S12_STRATEGY_ID]
    evaluability = totals12["evaluable"] / name_days_seen

    # ⚠⚠ EACH BOOK'S ABOVE-GATE RATE COMES FROM ITS OWN COUNTS, and that is what gives
    # the degenerate-end check below any content (review WARNING 1). Reusing one shared
    # `rate_above` for both made the check tautological — with no sub-gate name the
    # control's second term is zero, so two expressions built on the same variable were
    # compared and could not differ. Derived separately, the check compares the census's
    # candidate counts against its control counts and can actually fail.
    rate_above_candidate = totals12["fired"] / totals12["gate_clearing_evaluable"]
    rate_above_control = totals4["fired_gate_clearing"] / totals4["gate_clearing_evaluable"]
    below_name_days = totals4["evaluable"] - totals4["gate_clearing_evaluable"]
    rate_below = (totals4["fired"] - totals4["fired_gate_clearing"]) / below_name_days

    mix = report["charged_band_mix"]

    def _collapse(strategy_id: str) -> float:
        """Positions per FILLABLE fire — ``max_hold_collapse`` over ``all_fires``.

        ⚠ ``charged_legs``, not ``fires`` (finding 15): ``_absorb_bands`` counts a fire
        before testing its successor bar, so ``fires`` includes entries that open no
        position. ⚠ A LOWER bound on positions, because the collapse arm quarantines the
        full hold cap where a real position exits at or before it — and even that is not
        airtight, since ``position_builder`` suppresses the max-hold close on an
        unresolved outcome, which can quarantine longer than the cap (finding 16).
        """
        everything = mix[f"{strategy_id}/{arm}/all_fires"]["charged_legs"]
        collapsed = mix[f"{strategy_id}/{arm}/max_hold_collapse"]["charged_legs"]
        if everything == 0:
            raise ProjectionRefused(f"{arm}/{strategy_id} charged 0 legs in the census; there is no rate to project")
        return collapsed / everything

    collapse12, collapse4 = _collapse(S12_STRATEGY_ID), _collapse(S4_STRATEGY_ID)

    def _fires(above: int, below: int) -> tuple[float, float]:
        return _expected_fires(
            above=above,
            below=below,
            horizon_days=horizon_days,
            evaluability=evaluability,
            rate_above_candidate=rate_above_candidate,
            rate_above_control=rate_above_control,
            rate_below=rate_below,
        )

    # ⚠⚠ THE DEGENERATE-END IDENTITY, ASSERTED. With no sub-gate name the gate rejects
    # nothing and the two books ARE the same book, so the two fire projections must
    # coincide exactly. The first draft's denominators failed this and the failure was
    # hidden by excluding the endpoint from the grid.
    # ⚠ THIS IS A STRUCTURAL GUARD, NOT A DATA ONE, and the distinction is worth stating
    # plainly: ``_check_arm`` already refuses any census whose two strategies disagree on
    # the gate-clearing counts, so on data that reaches here the two rates are equal by
    # identity and this cannot fire. What it catches is a CODE change — splitting the two
    # rates apart is the obvious "improvement" and would pass every count check while
    # silently breaking the estimand. The repo has the same shape at the census's
    # import-time hold-cap assertion, which also cannot fire today.
    all_above = _fires(panel_size, 0)
    if not math.isclose(all_above[0], all_above[1], rel_tol=1e-12):
        raise ProjectionRefused(
            f"{arm}: at a zero sub-gate share the two books must be identical, but the projection gives "
            f"S-12 {all_above[0]} against S-4 {all_above[1]}"
        )

    grid = []
    for share in SUB_GATE_SHARE_GRID:
        above, below = _split_panel(panel_size, share)
        fires12, fires4 = _fires(above, below)
        positions12, positions4 = fires12 * collapse12, fires4 * collapse4
        rate12 = rate_above_candidate * evaluability * collapse12
        # ⚠ A PANEL-AVERAGE per-name rate over two structurally different sub-populations
        # (review NITPICK). ``_occupied_dates`` already assumes one shared per-name rate;
        # for the control that assumption is compounded here, because its panel genuinely
        # holds names of two kinds. The candidate's rate has no such blend — its names are
        # all above the gate — so only the control's occupancy figure carries this.
        rate4 = (above * rate_above_control + below * rate_below) / panel_size * evaluability * collapse4
        dates12 = _occupied_dates(rate=rate12, names=above, days=horizon_days)
        dates4 = _occupied_dates(rate=rate4, names=panel_size, days=horizon_days)
        grid.append(
            {
                "declared_sub_gate_share": share,
                "names_below_gate": below,
                "names_at_or_above_gate": above,
                "realised_sub_gate_share": below / panel_size,
                "s12_expected_fires": fires12,
                "s4_expected_fires": fires4,
                "s12_expected_positions_collapse_scenario": positions12,
                "s4_expected_positions_collapse_scenario": positions4,
                "s12_occupied_fill_dates_independence_scenario": dates12,
                "s4_occupied_fill_dates_independence_scenario": dates4,
                # ⚠ The only rigorous statement available: a date carries at least one
                # position, so distinct dates cannot exceed either the sessions in the
                # horizon or the positions opened.
                "s12_distinct_fill_dates_hard_upper_bound": min(horizon_days, positions12),
                "s4_distinct_fill_dates_hard_upper_bound": min(horizon_days, positions4),
                # ⚠ The per-trade axis clusters the UNION of the two books' fill dates.
                # Their overlap is NOT inferable from marginal rates (finding 24), so
                # only the arithmetic envelope is given, over the scenario estimates.
                "union_fill_dates_envelope": {
                    "low_if_fully_overlapping": max(dates12, dates4),
                    "high_if_disjoint": min(horizon_days, dates12 + dates4),
                },
            }
        )

    return {
        "arm": arm,
        "is_production_arm": arm == PRODUCTION_ARM,
        "sessions_per_year_measured": sessions,
        "horizon_trading_days": horizon_days,
        # ⚠ T equity marks give T-1 returns (finding 28), and the axis is a union of
        # stored dates rather than a multiple of panel width — so widening the panel
        # does not multiply this, though it can change what the returns contain.
        "portfolio_axis_observations": max(0.0, horizon_days - 1.0),
        "evaluable_share_of_name_days": evaluability,
        "corpus_sub_gate_share_of_evaluable_name_days": 1.0
        - totals12["gate_clearing_evaluable"] / totals12["evaluable"],
        "rates": {
            # ⚠ ONE key, because the two are an identity rather than two measurements —
            # and ``_check_arm`` plus the degenerate-end assertion both enforce it.
            "fires_per_gate_clearing_evaluable_name_day": rate_above_candidate,
            "s4_fires_per_sub_gate_evaluable_name_day": rate_below,
            "s12_positions_per_fillable_fire": collapse12,
            "s4_positions_per_fillable_fire": collapse4,
            # ⚠ THE SIZE OF THE COLLAPSE TRANSPORT ASSUMPTION, reported rather than
            # argued away: the two factors are pooled over different populations, so at
            # a zero sub-gate share — where the books are identical — the POSITION
            # projections still differ by this ratio. The fire projections do not.
            "collapse_factor_disagreement_at_zero_sub_gate_share": collapse12 / collapse4,
        },
        "rate_spread_by_year": {
            "fires_per_gate_clearing_evaluable_name_day": _spread(
                s12, numerator=_fired, denominator=_gate_clearing_name_days
            ),
            # ⚠ The CONTROL'S SUB-GATE rate, which is what its projection multiplies —
            # not its pooled fired/evaluable, whose spread can stay flat while this moves.
            "s4_fires_per_sub_gate_evaluable_name_day": _spread(
                s4, numerator=_sub_gate_fired, denominator=_sub_gate_name_days
            ),
        },
        "grid": grid,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--census", type=Path, required=True, help="path to the census report (JSON, or its log)")
    parser.add_argument(
        "--panel-size",
        type=int,
        default=MAX_PANEL_INSTRUMENTS,
        help=f"panel names; defaults to the COLLECTOR's cap ({MAX_PANEL_INSTRUMENTS}), which binds before any store's",
    )
    parser.add_argument("--horizon-months", type=int, default=DEFAULT_HORIZON_MONTHS)
    args = parser.parse_args(argv)
    if not MIN_PANEL_SIZE <= args.panel_size <= MAX_PANEL_INSTRUMENTS:
        raise ProjectionRefused(
            f"--panel-size must sit in [{MIN_PANEL_SIZE}, {MAX_PANEL_INSTRUMENTS}], got {args.panel_size}: below "
            "that the panel cannot straddle the gate, above it the collector "
            "(strategy_quote_observation.MAX_PANEL_INSTRUMENTS) cannot observe the panel"
        )
    if args.horizon_months < 1:
        raise ProjectionRefused(f"--horizon-months must be >= 1, got {args.horizon_months}")

    report = _load(args.census)
    _check(report)
    print(
        json.dumps(
            {
                "census": str(args.census),
                "census_series_evaluated": report["series_evaluated"],
                "census_window_end": report["evaluation_window_end"],
                "universe": report["universe"],
                "cost_model_id": report["cost_model_id"],
                "cost_price_basis": report["cost_price_basis"],
                "gate_edge": report["gate_edge"],
                "max_hold_bars": report["max_hold_bars"],
                "panel_size": args.panel_size,
                "panel_cap_source": "strategy_quote_observation.MAX_PANEL_INSTRUMENTS",
                "horizon_months": args.horizon_months,
                "min_supporting_clusters": MIN_SUPPORTING_CLUSTERS,
                "min_supporting_clusters_source": (
                    "strategy_s12_paired_trial.MIN_SUPPORTING_CLUSTERS (= block_bootstrap.MIN_CLUSTERS)"
                ),
                "by_arm": {
                    arm: _project_arm(report, arm=arm, panel_size=args.panel_size, horizon_months=args.horizon_months)
                    for arm in ARMS
                },
                "ASSUMPTIONS": [
                    "TRANSFER: rates are conditional on an evaluable name-day in a survivorship-free 1962-2021 "
                    "cross-section; a panel of currently-listed names is not a draw from it and price eligibility "
                    "does not establish equal propensity. Direction unknown.",
                    "LIFECYCLE: names cross the gate, split, delist and merge. Holding the declared share fixed "
                    "implies a replacement policy this projection does not specify.",
                    "STATIONARITY: a pooled historical rate is multiplied by a forward horizon. The per-year "
                    "spread and the pooled recent-window rate are reported so the size of this is visible.",
                    "COLLAPSE TRANSPORT: the fires-to-positions factor is pooled per strategy over different "
                    "populations; its disagreement at a zero sub-gate share is reported as a ratio.",
                    "OCCUPANCY: the occupied-date figures assume independence across names AND one shared "
                    "per-name rate. They are scenarios, not bounds in either direction.",
                    "POSITIONS ARE NOT CLUSTERS: unresolved exits, window-end censoring and termination handling "
                    "all sit between an opened position and a supported entry-fill-date cluster.",
                    "STRADDLING IS NOT DISAGREEMENT: a panel can straddle on price and still produce two "
                    "near-identical books if its sub-gate names rarely fire.",
                    "BOUNDARY CONDITIONS: a fresh panel needs warm-up before its first evaluable bar, and a "
                    "signal on the last session fills outside the window; neither end is modelled here.",
                    "CAPACITY IS NOT EXPOSURE: s12_signals refuses SCAN_UNIVERSE = survivor_only, so a declared "
                    "panel does not by itself let S-12 emit a forward signal.",
                ],
                "WHAT_THIS_DOES_NOT_SAY": (
                    "Whether the accrual is SUFFICIENT, and nothing here 'clears' anything. "
                    "MIN_SUPPORTING_CLUSTERS is a computability guard on an OBSERVED count (block_bootstrap: 'a "
                    "single date cannot be autocorrelated with anything'); an expectation above it still leaves "
                    "positive probability of a realised count below it, and no acquisition-probability model is "
                    "supplied. strategy_s12_paired_trial records that no sample-size floor certifying 2.5%-tail "
                    "coverage is derivable and none is invented, and the spec's power half is blocked because the "
                    "declared d = 0.5 has no defined denominator for a difference between two strategies' books. "
                    "Clearing a computability guard is also not the same as computability: the evaluator can still "
                    "refuse on block length, an empty resampled book or a zero-variance bootstrap."
                ),
            },
            indent=2,
            sort_keys=True,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["ARMS", "MIN_PANEL_SIZE", "PRODUCTION_ARM", "SUB_GATE_SHARE_GRID", "ProjectionRefused", "main"]
