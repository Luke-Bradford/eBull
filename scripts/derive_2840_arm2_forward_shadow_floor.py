"""#2840 arm 2 — the `ForwardShadowFloor` derivation, route 1, computed not chosen.

Reads the census JSON produced by ``scripts/census_2840_s12_signal_supply.py`` and the
one stored result row the variance is borrowed from, and prints the two numbers
``prereg_contract.ForwardShadowFloor`` requires plus the ``derivation`` string that
names every input. It writes nothing and freezes nothing.

    PYTHONPATH=. uv run python -m scripts.census_2840_s12_signal_supply \
        --progress-every 1000 > /tmp/census_2840_full.json
    PYTHONPATH=. uv run python -m scripts.derive_2840_arm2_forward_shadow_floor \
        --census /tmp/census_2840_full.json

WHY THIS IS A SCRIPT AND NOT A PARAGRAPH
----------------------------------------
``.claude/CLAUDE.md``: *"Never hardcode a derived statistic into prose, a comment or a
docstring — compute it, or omit it."* Every figure below is read from the census file
or from ``strategy_results_store`` at run time, so a re-measured census moves the floor
rather than leaving a stale number in a document a reader trusts.

THE MODEL, AND WHY EACH PIECE IS READ RATHER THAN PICKED
--------------------------------------------------------
``docs/proposals/ta/2026-08-11-portfolio-alpha-viability-plan.md`` §5 is the repo's
general formulation: *"planning SE target <= minimum net effect_j / (critical_value_j +
z_(power))"*, power 0.8 unless justified, and ``data_infeasible`` as a legitimate
verdict when the available dates cannot reach it. It needs three things arm 2's
contract did not contain. Each is supplied here from a measurement:

1. **Minimum net effect.** §5 defines it as *"return needed to improve F-0 after the
   mandate's risk penalty"*, and F-0 is NOT available: it is the reconciled LIVE
   account (#2559, #2602 item 4 still open), so no F-0 exists over 1962-2021. The
   substitute is not free either — ``expectancy_per_trade_pct`` is already net of
   cost, so *"expectancy > 0"* IS the break-even test and powering a test against its
   own null is degenerate. What is left is the hypothesis's OWN mechanism: the gate
   claims to trade where the cost model charges least, so the smallest effect worth
   detecting is the charged round trip it buys down — S-4's realised mix minus
   S-12's, both measured per fire on the bar each fill lands on. ⚠ It is NOT the
   cheapest band's headline 0.322%: the gate reads ``close(t)`` and the charge reads
   ``open(t+1)``, so S-12 pays whatever band its fills actually select.
2. **Critical value from a declared multiplicity and sampling model.** The pass bar's
   mechanism leg (*"S-12's figure exceeds S-4's in the same cell"*) is required in BOTH
   quarantine arms, and a conjunction of required tests is an intersection-union test:
   its size is the maximum of the parts, so **no alpha correction is due** (Berger
   1982, *Technometrics* 24(4)). Power does not come free the same way — joint power is
   at least the product under positive dependence (Slepian 1962; Šidák 1967), and these
   two arms are the same trades under two quarantine rules, so the independence reading
   is the conservative one: per-leg power ``0.8 ** (1/2)``.
3. **A variance estimate matching the dependence and tail shape.** S-12 has no stored
   result and must not acquire one before the freeze, so the only available estimate is
   S-4's block-bootstrap interval on the SAME cell the pass bar reads. Borrowing it is
   adjusted twice, in the strict direction each time, and both adjustments are
   arithmetic or measured rather than asserted — see ``_stale_cost_inflation`` and
   ``_dispersion_inflation``.

⚠⚠ WHAT THE FLOOR IS AND IS NOT. It is a PLANNING requirement — §5 is explicit that the
expression *"is never the acceptance test"*. Arm 2's acceptance test stays the frozen
pass bar. The floor answers one question: how much forward evidence would make a
difference the size of the gate's own cost saving distinguishable from noise at the
declared alpha and power.

⚠⚠ AND THIS ``strategy_version`` CAN NEVER REACH IT. The scan runs
``SCAN_UNIVERSE = survivor_only`` and ``s12_signals`` refuses every bar outside
``AS_TRADED_UNIVERSES = {survivorship_free}``, so this identity accumulates no forward
decision dates at all. The floor states what a forward confirmation of the hypothesis
would require, under a different identity with its own declaration — arm 1's freeze
script records the same structure. It is frozen saying so.

Refs #2840, #2829, #2437, #3238.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from statistics import NormalDist
from typing import Any, Final

import psycopg

from app.config import settings
from app.services.cost_model import BANDS
from app.services.strategies.s12_cheapest_band_price_gated_breakout import S12_STRATEGY_ID
from app.services.strategy_result import HOLDOUT_BOUNDARY

S4_STRATEGY_ID: Final = "s4-volatility-compression-breakout"

#: The cell the variance is borrowed from, named in full rather than filtered loosely:
#: it is the cell arm 2's pass bar decides on, and a `best_case` or `hold_out` row would
#: be a different measurement wearing the same column names.
BORROWED_CELL: Final = {
    "strategy_id": S4_STRATEGY_ID,
    "namespace": "in_sample",
    "quarantine_arm": "masked",
    "ambiguity_arm": "worst_case",
    "universe_basis": "survivorship_free",
}

#: The production arm. ``price_masked_bars`` carries one arm on purpose — *"criterion
#: 9's `admitted` arm is a sensitivity measurement that has no place in a scan"*.
PRODUCTION_ARM: Final = "masked"

#: ⚠ The CONSERVATIVE weighting is chosen per quantity, not once for the whole
#: derivation, and the rule is stated rather than applied silently: for the EFFECT take
#: the weighting that makes it smallest (a smaller effect needs more evidence); for the
#: SUPPLY take the weighting that makes the conversion demand most dates.
WEIGHTINGS: Final[tuple[str, ...]] = ("all_fires", "max_hold_collapse")

#: Two-sided, matching #2616's two frozen floors. ⚠ One-sided 0.05 would be the
#: cheaper choice and the pass bar IS one-sided; the precedents' constant is kept
#: because a floor is the one place to prefer the dearer convention.
ALPHA_TWO_SIDED: Final = 0.05

#: §5: *"Power is 80% unless the preregistration justifies higher."*
JOINT_POWER: Final = 0.8

#: The pass bar's mechanism leg is required in BOTH quarantine arms.
POWERED_LEGS: Final = 2


def _critical_quantiles() -> tuple[float, float]:
    """``(z_(1 - alpha/2), z_(per-leg power))`` — the two halves of §5's denominator.

    Returned separately rather than summed so the derivation string can print each,
    which is what makes the model auditable: a reader can see the multiplicity choice
    and the power choice without re-deriving either from the total.
    """
    normal = NormalDist()
    per_leg_power = JOINT_POWER ** (1 / POWERED_LEGS)
    return normal.inv_cdf(1 - ALPHA_TWO_SIDED / 2), normal.inv_cdf(per_leg_power)


def _stale_cost_inflation() -> float:
    """How far re-costing under #3238 can move the borrowed SE. An exact bound.

    The borrowed interval was measured under ``COST_MODEL_ID`` v3, which charged every
    leg the maximum band. Its MEAN is therefore unusable and is not used anywhere here.
    Its WIDTH is bounded arithmetically rather than argued: a position's net multiple is
    its gross multiple times ``(1 - h) / (1 + h)``, so a per-trade standard deviation
    scales by exactly that factor. Across the whole band table the factor lies between
    the dearest and the cheapest band, and the worst case for a BORROWED SE is that the
    v3 rows were scaled by the dearest and the true rows by the cheapest.

    ⚠ This bounds the RE-COSTING, not the re-run. #3238 also changed which trades the
    corpus produces on other axes; nothing here claims otherwise, and the stored row is
    labelled stale wherever it is quoted.
    """
    factors = [float((1 - band.half_spread) / (1 + band.half_spread)) for band in BANDS]
    return max(factors) / min(factors)


def _series_sd(block: dict[str, Any], population: str) -> float:
    sd = block[population]["sd"]
    if sd is None or sd <= 0:
        raise RuntimeError(f"census reports no usable sd for {population!r}; the floor cannot be derived from it")
    return float(sd)


def _dispersion_inflation(census: dict[str, Any], *, fan_out_s12: float, fan_out_s4: float) -> tuple[float, float]:
    """How much S-12's per-DATE dispersion can exceed S-4's, measured on both terms.

    The borrowed standard error is denominated in fill-date clusters, so the quantity
    that has to transfer is dispersion per DATE, not per name. Two measured terms move
    it in opposite directions and the census supplies both:

    - gated bars are far LESS volatile per name than the corpus (large, high-priced
      names), which lowers S-12's;
    - S-12 fires on far FEWER names per date than S-4, so it averages less within a
      date, which raises it.

    Under within-date independence a date's mean return has dispersion
    ``sd_name / sqrt(names)``, so the transfer factor is the ratio of those two. ⚠ The
    independence reading is the CONSERVATIVE one and that is why it is used: positive
    cross-name correlation pushes both sides toward a common market factor, which
    collapses the ratio toward ``sd_gated / sd_all`` — smaller, i.e. a lower floor.

    Returns ``(inflation, raw_ratio)``. The inflation is floored at 1: a borrowed SE
    that is already conservative is not talked down.
    """
    block = census["bar_return_dispersion"][PRODUCTION_ARM]
    sd_all, sd_gated = _series_sd(block, "all_bars"), _series_sd(block, "gated_bars")
    ratio = (sd_gated / math.sqrt(fan_out_s12)) / (sd_all / math.sqrt(fan_out_s4))
    return max(1.0, ratio), ratio


def _borrowed_interval(conn: psycopg.Connection) -> dict[str, Any]:
    """The one stored row the variance comes from, read at run time, never pasted."""
    cursor = conn.execute(
        """
        select trade_count, effective_sample_size, expectancy_per_trade_pct,
               expectancy_ci_low_pct, expectancy_ci_high_pct, bootstrap_block_length,
               bootstrap_cluster_count, bootstrap_resamples, bootstrap_design_effect,
               bootstrap_model_id, cost_model_id, strategy_version, corpus_version
          from strategy_results_store
         where strategy_id = %(strategy_id)s and namespace = %(namespace)s
           and quarantine_arm = %(quarantine_arm)s and ambiguity_arm = %(ambiguity_arm)s
           and universe_basis = %(universe_basis)s and expectancy_ci_low_pct is not null
        """,
        BORROWED_CELL,
    )
    assert cursor.description is not None
    columns = [column.name for column in cursor.description]
    rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
    if len(rows) != 1:
        # ⚠ REFUSES on 0 AND on >1. A second row means two runs wrote the same cell and
        # picking either would be a silent choice between measurements.
        raise RuntimeError(f"expected exactly 1 borrowed interval row for {BORROWED_CELL}, found {len(rows)}")
    return rows[0]


def _cell(census: dict[str, Any], strategy_id: str, weighting: str) -> dict[str, Any]:
    return census["charged_band_mix"][f"{strategy_id}/{PRODUCTION_ARM}/{weighting}"]


def _supply(census: dict[str, Any], strategy_id: str) -> dict[str, Any]:
    return census["supply"][f"{strategy_id}/{PRODUCTION_ARM}"]


def derive(census: dict[str, Any], borrowed: dict[str, Any]) -> dict[str, Any]:
    """Every number, computed. No literal below is a choice; each is read or derived."""
    if census.get("limited_to_series") is not None:
        raise RuntimeError("the census was run with --limit: a timing slice cannot size a floor")
    if census.get("cost_price_basis") != "as_traded":
        raise RuntimeError(
            f"census cost_price_basis is {census.get('cost_price_basis')!r}, not 'as_traded' — "
            "the bands it recorded are the maximum-band fallback and are not a charged mix"
        )

    # --- the minimum net effect -------------------------------------------------
    charges = {
        weighting: {
            strategy_id: _cell(census, strategy_id, weighting)["mean_round_trip_charge_pct"]
            for strategy_id in (S4_STRATEGY_ID, S12_STRATEGY_ID)
        }
        for weighting in WEIGHTINGS
    }
    # ⚠ `mean_round_trip_charge_pct` is None when a cell charged NO leg — every fire
    # unfillable, or none at all. Refusing here names the empty cell; subtracting would
    # raise a bare TypeError several frames away from the reason.
    for weighting, pair in charges.items():
        empty = sorted(strategy_id for strategy_id, charge in pair.items() if charge is None)
        if empty:
            raise RuntimeError(
                f"no charged leg on the {weighting} weighting for {', '.join(empty)} — the census found no "
                "fillable fire there, so no effect can be read from it"
            )
    deltas = {weighting: pair[S4_STRATEGY_ID] - pair[S12_STRATEGY_ID] for weighting, pair in charges.items()}
    binding_weighting = min(deltas, key=lambda weighting: deltas[weighting])
    minimum_net_effect_pct = deltas[binding_weighting]
    if minimum_net_effect_pct <= 0:
        raise RuntimeError(
            f"the gate does not buy down a charge on the {binding_weighting} weighting "
            f"({minimum_net_effect_pct:+.4f} pp) — route 1's anchor does not exist and the floor "
            "cannot be derived this way"
        )

    # --- the variance ------------------------------------------------------------
    half_width = float(borrowed["expectancy_ci_high_pct"] - borrowed["expectancy_ci_low_pct"]) / 2
    # ⚠ The stored interval is a percentile block-bootstrap interval; reading its half
    # width as 1.96 SE is the #2616 construction verbatim, and where the interval is
    # asymmetric or heavy-tailed that read OVERSTATES the SE — which raises the floor.
    standard_error_pct = half_width / NormalDist().inv_cdf(1 - ALPHA_TWO_SIDED / 2)
    clusters_observed = int(borrowed["bootstrap_cluster_count"])

    s12_supply, s4_supply = _supply(census, S12_STRATEGY_ID), _supply(census, S4_STRATEGY_ID)
    fan_out_s12 = s12_supply["fired"] / s12_supply["distinct_signal_dates"]
    fan_out_s4 = s4_supply["fired"] / s4_supply["distinct_signal_dates"]
    dispersion_inflation, dispersion_ratio = _dispersion_inflation(
        census, fan_out_s12=fan_out_s12, fan_out_s4=fan_out_s4
    )
    stale_inflation = _stale_cost_inflation()
    effective_se_pct = standard_error_pct * stale_inflation * dispersion_inflation

    # --- the requirement ---------------------------------------------------------
    z_alpha, z_power = _critical_quantiles()
    critical_sum = z_alpha + z_power
    required_clusters = math.ceil(clusters_observed * (effective_se_pct * critical_sum / minimum_net_effect_pct) ** 2)

    # --- clusters -> decision dates ----------------------------------------------
    # The borrowed SE is denominated in FILL-date clusters (`cluster_by_date` keys on
    # `trades.entry_fill_date`); the gate counts SIGNAL dates. S-12's own measured ratio
    # converts between them. ⚠ The weighting taken is the one that demands MORE signal
    # dates per cluster, which is the strict direction for a floor.
    conversions = {}
    for weighting in WEIGHTINGS:
        fill_dates = _cell(census, S12_STRATEGY_ID, weighting)["distinct_fill_dates"]
        if fill_dates < 1:
            raise RuntimeError(f"S-12 has no fill dates under the {weighting} weighting; nothing can be converted")
        conversions[weighting] = s12_supply["distinct_signal_dates"] / fill_dates
    conversion_weighting = max(conversions, key=lambda weighting: conversions[weighting])
    signal_dates_per_cluster = conversions[conversion_weighting]
    min_independent_decision_dates = math.ceil(required_clusters * signal_dates_per_cluster)

    # --- decision dates -> calendar weeks ----------------------------------------
    # S-12's OWN realised arrival rate, over its own span. ⚠ `n - 1` intervals, not `n`:
    # five dates span four gaps, and using `n` shortens the implied wait.
    intervals = s12_supply["distinct_signal_dates"] - 1
    if intervals < 1 or s12_supply["span_days"] < 1:
        raise RuntimeError("S-12's in-sample supply spans fewer than two dates; no arrival rate can be read from it")
    days_per_decision_date = s12_supply["span_days"] / intervals
    min_calendar_weeks = math.ceil(min_independent_decision_dates * days_per_decision_date / 7)

    return {
        "minimum_net_effect_pct": minimum_net_effect_pct,
        "effect_weighting": binding_weighting,
        "charges_pct": charges,
        "standard_error_pct": standard_error_pct,
        "stale_cost_inflation": stale_inflation,
        "dispersion_ratio": dispersion_ratio,
        "dispersion_inflation": dispersion_inflation,
        "effective_standard_error_pct": effective_se_pct,
        "clusters_observed": clusters_observed,
        "critical_sum": critical_sum,
        "z_alpha": z_alpha,
        "z_power": z_power,
        "per_leg_power": JOINT_POWER ** (1 / POWERED_LEGS),
        "required_clusters": required_clusters,
        "signal_dates_per_cluster": signal_dates_per_cluster,
        "conversion_weighting": conversion_weighting,
        "fan_out_s12": fan_out_s12,
        "fan_out_s4": fan_out_s4,
        "days_per_decision_date": days_per_decision_date,
        "min_independent_decision_dates": min_independent_decision_dates,
        "min_calendar_weeks": min_calendar_weeks,
        "observed_decision_dates": s12_supply["distinct_signal_dates"],
        "observed_span_days": s12_supply["span_days"],
        # ⚠ COMPUTED, never asserted. §5 names `data_infeasible` as an admissible
        # verdict, and the honest test is whether the requirement exceeds what the
        # WHOLE in-sample archive supplied — 59 years of it. A floor that clears that
        # comparison is reachable in principle and must not be labelled infeasible to
        # make the write-up tidier.
        "verdict": (
            "data_infeasible"
            if min_independent_decision_dates > s12_supply["distinct_signal_dates"]
            else "reachable_in_principle"
        ),
        "borrowed": {
            key: str(value)
            for key, value in borrowed.items()
            if key
            in (
                "trade_count",
                "expectancy_ci_low_pct",
                "expectancy_ci_high_pct",
                "bootstrap_cluster_count",
                "bootstrap_block_length",
                "bootstrap_design_effect",
                "cost_model_id",
                "strategy_version",
                "corpus_version",
            )
        },
    }


def derivation_string(result: dict[str, Any]) -> str:
    """The ``derivation`` field. ⚠ ``sql/333`` caps this column at 1000 characters."""
    weighting = result["effect_weighting"]
    return (
        "No power calc for arm 2 (pass bar = 2 inequalities, no magnitude); instantiates "
        "portfolio-alpha-viability-plan §5. Min net effect = charged round trip the gate buys down, "
        f"per fire on its fill bar (census_2840_s12_signal_supply, masked, {weighting}): "
        f"S-4 {result['charges_pct'][weighting][S4_STRATEGY_ID]:.4f} - S-12 "
        f"{result['charges_pct'][weighting][S12_STRATEGY_ID]:.4f} = {result['minimum_net_effect_pct']:.4f}pp. "
        "F-0 unavailable (#2559); expectancy is net, so its break-even is degenerate. "
        "Var: S-4 in_sample/masked/worst_case block-bootstrap 95% CI half-width/1.96 = "
        f"{result['standard_error_pct']:.4f}% at {result['clusters_observed']} fill-date clusters, "
        f"x{result['stale_cost_inflation']:.4f} (v3 staleness, (1-h)/(1+h) bound) x"
        f"{result['dispersion_inflation']:.4f} (per-date dispersion). IUT over the 2 quarantine arms: "
        "no alpha correction (Berger 1982); per-leg power 0.8^(1/2); z="
        f"{result['z_alpha']:.4f}+{result['z_power']:.4f}={result['critical_sum']:.4f}. clusters=ceil("
        f"{result['clusters_observed']}x({result['effective_standard_error_pct']:.4f}x{result['critical_sum']:.4f}/"
        f"{result['minimum_net_effect_pct']:.4f})^2)={result['required_clusters']}; dates=ceil(x"
        f"{result['signal_dates_per_cluster']:.4f})={result['min_independent_decision_dates']}; weeks=ceil(x"
        f"{result['days_per_decision_date']:.4f}/7)={result['min_calendar_weeks']}. Supply "
        f"{result['observed_decision_dates']} dates/{result['observed_span_days']}d => {result['verdict']}. "
        "s12_signals refuses SCAN_UNIVERSE=survivor_only, so THIS version accrues no forward date; a forward "
        "shadow is a separate identity+declaration. falsification_only."
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--census",
        type=Path,
        required=True,
        help="the JSON written by scripts.census_2840_s12_signal_supply over the FULL population",
    )
    args = parser.parse_args(argv)

    text = args.census.read_text()
    # ⚠ The census prints progress lines before its JSON, so the file is not pure JSON
    # when it was redirected whole (which is the only way it should be captured — a
    # pipe would background it on timeout and lose the report).
    census = json.loads(text[text.index("{") :])

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        borrowed = _borrowed_interval(conn)
        conn.rollback()

    result = derive(census, borrowed)
    derivation = derivation_string(result)
    result["derivation"] = derivation
    result["derivation_length"] = len(derivation)
    result["holdout_boundary"] = HOLDOUT_BOUNDARY.isoformat()
    if len(derivation) > 1000:
        # ⚠ Reported, not raised: the caller needs to SEE the string to shorten it, and
        # a script that dies before printing it makes that a guessing game.
        result["WARNING"] = f"derivation is {len(derivation)} characters; sql/333 caps the column at 1000"
    print(json.dumps(result, indent=2, sort_keys=True, default=str), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["BORROWED_CELL", "derivation_string", "derive", "main"]
