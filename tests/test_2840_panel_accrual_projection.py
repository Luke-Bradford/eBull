"""Pure tests for #2840 arm 2 step 1's panel-accrual projection.

No database. Three things are pinned, and they are different in kind:

1. **The refusals.** Every branch of ``_check`` is a state in which the script could
   print a perfectly plausible projection that does not describe the panel it names — a
   timing slice read as a population, the sensitivity arm read as production, a
   split-adjusted corpus under a nominal-price gate, or a census whose counts and the
   rule have drifted apart. A wrong number with no error is the failure mode this ticket
   keeps hitting, so each one gets a test.
2. **The arithmetic the first draft got wrong**, all of it caught at Codex checkpoint 1:
   the control's rate must be split on the same gate edge or the two books do not
   coincide when the panel stops straddling; the independence occupancy figure is not a
   bound in either direction; ``fires`` is not the fillable count.
3. **The constants that must not be re-typed as literals.** The panel cap, the gate and
   the cluster floor live in the modules that own them.

Refs #2840, #2437.
"""

from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
from typing import Any

import pytest

from app.services.block_bootstrap import MIN_CLUSTERS
from app.services.cost_model import COST_MODEL_ID
from app.services.strategies import s12_cheapest_band_price_gated_breakout as s12_module
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_quote_observation import MAX_PANEL_INSTRUMENTS
from app.services.strategy_s12_paired_trial import MIN_SUPPORTING_CLUSTERS
from scripts import census_2840_s12_signal_supply as census
from scripts.project_2840_panel_accrual import (
    ARMS,
    MIN_PANEL_SIZE,
    PRODUCTION_ARM,
    SUB_GATE_SHARE_GRID,
    ProjectionRefused,
    _expected_fires,
    _occupied_dates,
    _sessions_per_year,
    _split_panel,
    _whole_years,
    main,
)

S12 = "s12-cheapest-band-price-gated-breakout"
S4 = "s4-volatility-compression-breakout"
UNIVERSE = "survivorship_free"


def live_versions() -> dict[str, str]:
    """⚠ COMPUTED, never typed out. A hardcoded identity hash goes stale the moment the
    rule moves, and would then pin the fixture to a strategy that no longer exists."""
    return {
        strategy_id: STRATEGY_MANIFEST[strategy_id].identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID).version
        for strategy_id in (S12, S4)
    }


#: Five whole years so every total below divides exactly into its ``by_year`` cells.
SUPPLY_YEARS = ("2016", "2017", "2018", "2019", "2020")


def _supply_cell(*, evaluable: int, gate_clearing: int, fired: int, fired_gate_clearing: int) -> dict[str, Any]:
    """One ``name_day_supply`` cell whose per-year cells sum EXACTLY to its totals.

    ⚠ A fixture that does not reconcile would describe a report the census cannot emit,
    and ``_check`` refuses exactly that — so the fixture has to be honest or the refusal
    tests would pass for the wrong reason.
    """
    per_year = {
        "evaluable": evaluable // len(SUPPLY_YEARS),
        "gate_clearing_evaluable": gate_clearing // len(SUPPLY_YEARS),
        "fired": fired // len(SUPPLY_YEARS),
        "fired_gate_clearing": fired_gate_clearing // len(SUPPLY_YEARS),
        "distinct_signal_dates": 40,
    }
    return {
        "totals": {
            "evaluable": evaluable,
            "gate_clearing_evaluable": gate_clearing,
            "fired": fired,
            "fired_gate_clearing": fired_gate_clearing,
        },
        "by_year": {year: dict(per_year) for year in SUPPLY_YEARS},
    }


def report(**overrides: Any) -> dict[str, Any]:
    """A census report whose rates are round, so every expected figure is checkable.

    Above the gate: 3,000 fires over 300,000 gate-clearing evaluable name-days = 1.0%,
    and by the identity that is BOTH strategies' rate there.
    Below it: S-4's 84,000 - 3,000 = 81,000 fires over 3,000,000 - 300,000 = 2,700,000
    sub-gate name-days = 3.0%.
    Nothing is ``not_evaluable``, so the evaluability ratio is exactly 1 and the panel
    exposure arithmetic can be read off; one test overrides that deliberately.
    Collapse: S-12 keeps 600 of 3,000 charged legs (0.20), S-4 21,000 of 84,000 (0.25),
    which are DELIBERATELY unequal so the transport residual is a real number.
    """
    supply = {}
    name_day_supply = {}
    for arm in ARMS:
        name_day_supply[f"{S12}/{arm}"] = _supply_cell(
            evaluable=3_000_000, gate_clearing=300_000, fired=3_000, fired_gate_clearing=3_000
        )
        name_day_supply[f"{S4}/{arm}"] = _supply_cell(
            evaluable=3_000_000, gate_clearing=300_000, fired=84_000, fired_gate_clearing=3_000
        )
        supply[f"{S12}/{arm}"] = {"verdicts": {"fired": 3_000, "not_fired": 2_997_000}}
        supply[f"{S4}/{arm}"] = {"verdicts": {"fired": 84_000, "not_fired": 2_916_000}}
    base: dict[str, Any] = {
        "limited_to_series": None,
        "production_arm": PRODUCTION_ARM,
        "series_evaluated": 14_260,
        "evaluation_window_end": "2021-06-28",
        "universe": UNIVERSE,
        "strategy_versions": live_versions(),
        "cost_model_id": "test-cost-model",
        "cost_price_basis": "as_traded",
        "gate_edge": "100",
        "max_hold_bars": 40,
        "supply": supply,
        "name_day_supply": name_day_supply,
        # ⚠ 2015 and 2021 are PARTIAL and must be dropped; the median of the whole years
        # is 250, which every expected figure below is built on.
        "corpus_bar_dates": {
            arm: {
                "total": 1_220,
                "by_year": {
                    "2015": 100,
                    "2016": 250,
                    "2017": 250,
                    "2018": 250,
                    "2019": 250,
                    "2020": 250,
                    "2021": 120,
                },
            }
            for arm in ARMS
        },
        "charged_band_mix": {
            key: value
            for arm in ARMS
            for key, value in (
                (f"{S12}/{arm}/all_fires", {"fires": 3_100, "charged_legs": 3_000}),
                (f"{S12}/{arm}/max_hold_collapse", {"fires": 620, "charged_legs": 600}),
                (f"{S4}/{arm}/all_fires", {"fires": 84_500, "charged_legs": 84_000}),
                (f"{S4}/{arm}/max_hold_collapse", {"fires": 21_100, "charged_legs": 21_000}),
            )
        },
    }
    base.update(overrides)
    return base


def run(tmp_path: Path, payload: dict[str, Any], *extra: str) -> dict[str, Any]:
    """Run the projection over a report file and return its parsed output."""
    path = tmp_path / "census.json"
    path.write_text(json.dumps(payload))
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        assert main(["--census", str(path), *extra]) == 0
    return json.loads(buffer.getvalue())


def cells(out: dict[str, Any], arm: str = PRODUCTION_ARM) -> dict[float, dict[str, Any]]:
    return {cell["declared_sub_gate_share"]: cell for cell in out["by_arm"][arm]["grid"]}


# --------------------------------------------------------------------------- constants


def test_the_census_carries_no_second_copy_of_the_gate() -> None:
    """⚠ The census used to define its own ``float(GATE_EDGE)`` beside the strategy's.

    Two definitions of one threshold agreed by luck. A recalibration moving one and not
    the other would have produced a census of a gate nobody declared — silently, because
    both numbers are plausible.
    """
    assert census.PRICE_FLOOR == s12_module.PRICE_FLOOR
    assert not hasattr(census, "_GATE_EDGE_FLOAT")


def test_the_panel_cap_is_the_collectors_and_is_not_typed_here() -> None:
    """⚠ 50 is ``strategy_quote_observation``'s cap, which binds BEFORE any fetch.

    The earlier draft of this item compared against #2477's ~1,000-instrument retention
    cap, which is a different lane entirely.
    """
    assert MAX_PANEL_INSTRUMENTS == 50


def test_the_cluster_floor_is_block_bootstraps_own() -> None:
    assert MIN_SUPPORTING_CLUSTERS == MIN_CLUSTERS == 2


def test_the_share_grid_excludes_both_endpoints() -> None:
    """⚠⚠ THE CHECKPOINT-1 FINDING, PINNED — and the two ends are not the same null.

    A share of 0 means every member clears the gate, the gate rejects nothing and the two
    books are IDENTICAL, so the paired difference is identically zero. A share of 1
    leaves S-12 no eligible name and its expectancy undefined. Different failures, both
    manufactured by the panel rather than measured in the market.
    """
    assert SUB_GATE_SHARE_GRID
    assert all(0.0 < share < 1.0 for share in SUB_GATE_SHARE_GRID)


# --------------------------------------------------------------------------- refusals


def test_a_limited_slice_is_refused(tmp_path: Path) -> None:
    """The ``--limit`` flag's own help calls it a timing slice, never a population."""
    with pytest.raises(ProjectionRefused, match="TIMING SLICE"):
        run(tmp_path, report(limited_to_series=400))


def test_the_sensitivity_arm_is_refused_as_production(tmp_path: Path) -> None:
    with pytest.raises(ProjectionRefused, match="production arm"):
        run(tmp_path, report(production_arm="admitted"))


def test_a_split_adjusted_corpus_is_refused(tmp_path: Path) -> None:
    """⚠ A ``>= $100`` gate is a nominal-price gate only on an as-traded corpus."""
    with pytest.raises(ProjectionRefused, match="NOMINAL price"):
        run(tmp_path, report(cost_price_basis="split_adjusted"))


def test_a_census_taken_at_a_different_gate_edge_is_refused(tmp_path: Path) -> None:
    """⚠ The cheapest CHARGED band can move under a cost recalibration.

    Every count in such a census is well-formed and describes a different experiment.
    """
    with pytest.raises(ProjectionRefused, match="band table has moved"):
        run(tmp_path, report(gate_edge="50"))


def test_a_universe_the_rule_refuses_is_refused(tmp_path: Path) -> None:
    """⚠ ``AS_TRADED_UNIVERSES`` is the rule's OWN declared set, hashed into its identity.

    ``s12_signals`` refuses every bar outside it, so a census taken elsewhere measured a
    strategy that returns ``not_evaluable`` on every row — and would still print counts.
    """
    with pytest.raises(ProjectionRefused, match="does not accept as as-traded"):
        run(tmp_path, report(universe="survivor_only"))


def test_a_census_taken_under_a_different_hold_cap_is_refused(tmp_path: Path) -> None:
    """⚠ The collapse arm quarantines exactly ``MAX_HOLD_BARS``; a different cap is a
    different fires-to-positions conversion wearing the same name."""
    with pytest.raises(ProjectionRefused, match="quarantines a span"):
        run(tmp_path, report(max_hold_bars=20))


def test_a_census_measured_under_a_different_strategy_version_is_refused(tmp_path: Path) -> None:
    """⚠⚠ THE CHECK NO PARAMETER PIN CAN MAKE (Codex ckpt-2).

    Neither the gate edge nor the hold cap moves when S-4's body, S-12's body, the shared
    indicator code or the registry module changes — so without this a census taken before
    such an edit passes every other provenance test and has its rates published under the
    current strategy names.
    """
    payload = report()
    payload["strategy_versions"][S4] = "strategy-registry-v1+deadbeefcafe"
    with pytest.raises(ProjectionRefused, match="describe a different strategy"):
        run(tmp_path, payload)


def test_a_census_predating_the_provenance_pin_is_refused(tmp_path: Path) -> None:
    payload = report()
    del payload["strategy_versions"]
    with pytest.raises(ProjectionRefused, match="predates the provenance pin"):
        run(tmp_path, payload)


def test_s12_firing_off_a_sub_gate_bar_is_refused(tmp_path: Path) -> None:
    """S-12's gate admits nothing below the edge, so these two counts must agree."""
    payload = report()
    payload["name_day_supply"][f"{S12}/{PRODUCTION_ARM}"]["totals"]["fired_gate_clearing"] = 2_900
    with pytest.raises(ProjectionRefused, match="gate admits nothing else"):
        run(tmp_path, payload)


def test_the_control_and_the_candidate_must_agree_on_gate_clearing_fires(tmp_path: Path) -> None:
    """⚠ S-12 IS S-4 AND THE GATE. A disagreement here is not a rounding difference.

    This identity is what makes ``r_above`` ONE number rather than two, which is in turn
    what makes the books coincide when the panel stops straddling.
    """
    payload = report()
    payload["name_day_supply"][f"{S4}/{PRODUCTION_ARM}"]["totals"]["fired_gate_clearing"] = 3_001
    with pytest.raises(ProjectionRefused, match="must be equal"):
        run(tmp_path, payload)


def test_a_gate_that_changed_evaluability_is_refused(tmp_path: Path) -> None:
    """The gate is a body condition: it turns ``fired`` into ``not_fired`` and no more.

    If S-12 ever gained a fifth declared input its ``not_evaluable`` set would diverge
    from S-4's and the two books would stop being comparable on the same bars.
    """
    payload = report()
    payload["name_day_supply"][f"{S12}/{PRODUCTION_ARM}"]["totals"]["evaluable"] = 2_900_000
    with pytest.raises(ProjectionRefused, match="introduces no unevaluable state"):
        run(tmp_path, payload)


def test_a_census_with_no_sub_gate_population_is_refused(tmp_path: Path) -> None:
    """⚠ Both sides of the gate are needed; an empty one makes the difference a null."""
    payload = report()
    for strategy_id in (S12, S4):
        totals = payload["name_day_supply"][f"{strategy_id}/{PRODUCTION_ARM}"]["totals"]
        totals["gate_clearing_evaluable"] = totals["evaluable"]
        for year in SUPPLY_YEARS:
            cell = payload["name_day_supply"][f"{strategy_id}/{PRODUCTION_ARM}"]["by_year"][year]
            cell["gate_clearing_evaluable"] = cell["evaluable"]
    with pytest.raises(ProjectionRefused, match="strictly inside"):
        run(tmp_path, payload)


def test_by_year_cells_that_do_not_sum_to_their_totals_are_refused(tmp_path: Path) -> None:
    """⚠ Otherwise every per-year rate measures something else from the pooled one."""
    payload = report()
    payload["name_day_supply"][f"{S12}/{PRODUCTION_ARM}"]["by_year"]["2018"]["fired"] += 1
    with pytest.raises(ProjectionRefused, match="sums to"):
        run(tmp_path, payload)


def test_the_admitted_arm_is_checked_too(tmp_path: Path) -> None:
    """⚠ The pass bar is a CONJUNCTION over both arms, so both are validated."""
    payload = report()
    payload["name_day_supply"][f"{S4}/admitted"]["totals"]["fired_gate_clearing"] = 3_001
    with pytest.raises(ProjectionRefused, match="admitted"):
        run(tmp_path, payload)


@pytest.mark.parametrize("size", [MIN_PANEL_SIZE - 1, MAX_PANEL_INSTRUMENTS + 1])
def test_a_panel_that_cannot_straddle_or_cannot_be_observed_is_refused(tmp_path: Path, size: int) -> None:
    with pytest.raises(ProjectionRefused, match="panel-size must sit"):
        run(tmp_path, report(), "--panel-size", str(size))


def test_a_strategy_with_no_fillable_fire_is_refused(tmp_path: Path) -> None:
    """A 0 denominator is a refusal, not a 0.0 rate dressed as a measurement."""
    payload = report()
    payload["charged_band_mix"][f"{S12}/{PRODUCTION_ARM}/all_fires"]["charged_legs"] = 0
    with pytest.raises(ProjectionRefused, match="no rate to project"):
        run(tmp_path, payload)


# ----------------------------------------------------------------------------- helpers


def test_partial_calendar_years_are_dropped_before_the_session_count() -> None:
    """⚠ The census opens mid-1962 and is cut the day before the hold-out boundary.

    Keeping both partial years would drag the median session count down by two
    half-years and shorten every projected horizon with it.
    """
    assert _sessions_per_year(report(), PRODUCTION_ARM) == 250.0


def test_dropping_the_ends_is_not_taken_as_proof_the_middle_is_complete() -> None:
    """⚠ Codex ckpt-1, finding 31 — a missing interior year is a hole, not a shorter span."""
    with pytest.raises(ProjectionRefused, match="missing calendar years"):
        _whole_years({"2015": 100, "2016": 250, "2018": 250, "2019": 120})


def test_a_census_too_short_to_have_a_whole_year_is_refused() -> None:
    with pytest.raises(ProjectionRefused, match="no whole year"):
        _whole_years({"2020": 100, "2021": 100})


def test_the_panel_holds_whole_names_and_rounds_the_candidates_side_down() -> None:
    """⚠ 50 names at a 25% sub-gate share is 12.5 names, which no panel can hold.

    The sub-gate side rounds UP so the above-gate count — which drives the candidate's
    entire book — is never rounded in the optimistic direction.
    """
    assert _split_panel(50, 0.25) == (37, 13)
    assert _split_panel(50, 0.5) == (25, 25)
    assert _split_panel(2, 0.5) == (1, 1)


def test_a_split_that_leaves_no_eligible_name_is_refused() -> None:
    with pytest.raises(ProjectionRefused, match="must straddle"):
        _split_panel(4, 0.99)


def test_the_occupancy_figure_is_not_a_bound_in_either_direction() -> None:
    """⚠⚠ THE FIRST DRAFT CALLED THIS AN UPPER BOUND. IT IS NOT.

    Two names each firing on half of 100 days occupy 75 days under independence — but 50
    under perfect synchronisation and 100 under mutual exclusion. Independence sits
    between the two and bounds neither, so the figure is a scenario and the docstring
    and output both say so.
    """
    assert _occupied_dates(rate=0.5, names=2, days=100) == pytest.approx(75.0)
    assert _occupied_dates(rate=0.5, names=0, days=100) == 0.0
    assert _occupied_dates(rate=0.0, names=2, days=100) == 0.0


def test_more_names_never_reduce_the_occupancy_figure() -> None:
    assert _occupied_dates(rate=0.002, names=40, days=500) > _occupied_dates(rate=0.002, names=10, days=500)


# ------------------------------------------------------------------------- projection


def test_the_control_rate_is_split_on_the_same_gate_edge(tmp_path: Path) -> None:
    """⚠⚠ THE CHECKPOINT-1 CORRECTION. The gate thins the CONTROL's population too.

    The first draft divided S-4 by all evaluable name-days, which left its projection
    unchanged across panel splits — so the two books never coincided however far the
    panel stopped straddling.
    """
    rates = run(tmp_path, report())["by_arm"][PRODUCTION_ARM]["rates"]
    assert rates["fires_per_gate_clearing_evaluable_name_day"] == pytest.approx(0.01)
    assert rates["s4_fires_per_sub_gate_evaluable_name_day"] == pytest.approx(0.03)


def test_the_stationarity_diagnostic_covers_the_rate_that_is_actually_projected(tmp_path: Path) -> None:
    """⚠ The control's projection multiplies its SUB-GATE rate, not its pooled one.

    Reporting the annual spread of ``fired / evaluable`` would let a shifting above/
    sub-gate mix keep the diagnostic flat while the rate being multiplied moved. The
    sub-gate rate has no column of its own — it is a difference of two stored counts —
    which is why the spread takes extractors rather than column names.
    """
    spread = run(tmp_path, report())["by_arm"][PRODUCTION_ARM]["rate_spread_by_year"]
    assert "s4_fires_per_sub_gate_evaluable_name_day" in spread
    assert "s4_fires_per_evaluable_name_day" not in spread
    # Every synthetic year is identical, so the spread collapses onto the pooled rate.
    assert spread["s4_fires_per_sub_gate_evaluable_name_day"]["median"] == pytest.approx(0.03)
    assert spread["fires_per_gate_clearing_evaluable_name_day"]["median"] == pytest.approx(0.01)


def test_the_degenerate_end_check_compares_two_independently_derived_rates() -> None:
    """⚠⚠ REVIEW WARNING 1 — the first cut's assertion was tautological.

    ``_expected_fires`` took ONE shared above-gate rate, so with no sub-gate name the
    control's second term vanished and the check compared two expressions in the same
    variable. It could not fail. Taking the two rates as separate parameters makes the
    comparison real — and testable, which is what this proves: feed rates that disagree
    and the degenerate end diverges.
    """

    def at_zero_sub_gate_share(control_rate: float) -> tuple[float, float]:
        return _expected_fires(
            above=10,
            below=0,
            horizon_days=500.0,
            evaluability=1.0,
            rate_above_candidate=0.01,
            rate_above_control=control_rate,
            rate_below=0.03,
        )

    candidate, control = at_zero_sub_gate_share(0.01)
    assert candidate == pytest.approx(control)
    candidate, control = at_zero_sub_gate_share(0.02)
    assert candidate != pytest.approx(control)


def test_the_smallest_panel_can_express_every_declared_share() -> None:
    """⚠ REVIEW WARNING 3 — a literal floor of 2 was accepted and then crashed at 0.75.

    The floor is derived from the grid, so adding a more extreme share moves it.

    ⚠ The PROPERTY is asserted, not the value. Writing ``MIN_PANEL_SIZE == 4`` would
    couple this test to today's grid and need updating in lockstep with it — the same
    hardcoded-derived-value trap the constant was introduced to remove.
    """
    for share in SUB_GATE_SHARE_GRID:
        above, below = _split_panel(MIN_PANEL_SIZE, share)
        assert above >= 1 and below >= 1 and above + below == MIN_PANEL_SIZE
    # …and it is the SMALLEST such size: one share below it cannot be expressed.
    assert MIN_PANEL_SIZE >= 2
    with pytest.raises(ProjectionRefused, match="must straddle"):
        for share in SUB_GATE_SHARE_GRID:
            _split_panel(MIN_PANEL_SIZE - 1, share)


def test_a_census_whose_strategies_saw_different_bar_counts_is_refused(tmp_path: Path) -> None:
    """⚠ REVIEW WARNING 2 — the evaluability ratio's denominator is the FULL verdict sum.

    ``_check_arm`` asserts the ``evaluable`` halves agree, which leaves the denominator
    unchecked: one strategy's evaluability would then be applied to both projections.
    """
    payload = report()
    payload["supply"][f"{S4}/{PRODUCTION_ARM}"]["verdicts"]["not_fired"] += 5
    with pytest.raises(ProjectionRefused, match="different name-day counts"):
        run(tmp_path, payload)


def test_the_two_books_coincide_when_the_panel_stops_straddling(tmp_path: Path) -> None:
    """⚠⚠ THE DEGENERATE-END IDENTITY. The script asserts it; this proves the assert bites.

    Corrupting the sub-gate rate leaves the zero-share projections unequal, which is
    exactly the inconsistency the first draft hid by excluding the endpoint from the grid.
    A projection that cannot reproduce "no sub-gate name means one book" is describing
    some other experiment.
    """
    run(tmp_path, report())  # the honest fixture passes the assertion
    payload = report()
    supply = payload["name_day_supply"][f"{S4}/{PRODUCTION_ARM}"]
    # Break the identity WITHOUT tripping the count checks: move gate-clearing name-days
    # only, which changes r_above for S-4 alone.
    supply["totals"]["gate_clearing_evaluable"] = 250_000
    for year in SUPPLY_YEARS:
        supply["by_year"][year]["gate_clearing_evaluable"] = 50_000
    with pytest.raises(ProjectionRefused, match="disagree on the gate-clearing population"):
        run(tmp_path, payload)


def test_expected_fires_follow_the_declared_sub_gate_share(tmp_path: Path) -> None:
    """At 50 names, a 0.5 share and a 500-day horizon with full evaluability:

    25 eligible names x 500 days x 1.0% = 125 expected S-12 fires; S-4 adds its sub-gate
    side, 25 x 500 x 3.0% = 375, for 500.
    """
    grid = cells(run(tmp_path, report()))
    assert grid[0.5]["s12_expected_fires"] == pytest.approx(125.0)
    assert grid[0.5]["s4_expected_fires"] == pytest.approx(500.0)
    assert grid[0.75]["s12_expected_fires"] < grid[0.25]["s12_expected_fires"]
    # ⚠ The control's book now DOES move with the share, because the gate changes which
    # names it trades — the whole point of the correction.
    assert grid[0.75]["s4_expected_fires"] > grid[0.25]["s4_expected_fires"]


def test_positions_use_the_fillable_leg_count_not_the_fire_count(tmp_path: Path) -> None:
    """⚠ ``_absorb_bands`` counts a fire BEFORE testing its successor bar.

    The fixture's ``fires`` and ``charged_legs`` differ deliberately; a projection built
    on ``fires`` would give 620/3,100 = 0.2 here by coincidence and 21,100/84,500 for
    S-4, so only the S-4 figure discriminates.
    """
    rates = run(tmp_path, report())["by_arm"][PRODUCTION_ARM]["rates"]
    assert rates["s12_positions_per_fillable_fire"] == pytest.approx(600 / 3_000)
    assert rates["s4_positions_per_fillable_fire"] == pytest.approx(21_000 / 84_000)
    grid = cells(run(tmp_path, report()))
    assert grid[0.5]["s12_expected_positions_collapse_scenario"] == pytest.approx(25.0)
    assert grid[0.5]["s4_expected_positions_collapse_scenario"] == pytest.approx(125.0)


def test_the_collapse_transport_residual_is_reported_rather_than_hidden(tmp_path: Path) -> None:
    """⚠ The two factors are pooled over different populations, so they disagree.

    At a zero sub-gate share the FIRE projections coincide exactly but the POSITION ones
    do not, and the size of that gap is the size of the transport assumption. Reporting
    it beats arguing it away.
    """
    rates = run(tmp_path, report())["by_arm"][PRODUCTION_ARM]["rates"]
    assert rates["collapse_factor_disagreement_at_zero_sub_gate_share"] == pytest.approx(0.2 / 0.25)


def test_panel_exposure_is_discounted_by_measured_evaluability(tmp_path: Path) -> None:
    """⚠ Calendar name-days are not evaluable name-days (Codex ckpt-1, finding 2).

    The rates are per EVALUABLE name-day, so multiplying raw panel-days would credit the
    panel with warm-up bars, holes and quarantined bars the rule cannot judge.
    """
    payload = report()
    for arm in ARMS:
        payload["supply"][f"{S12}/{arm}"]["verdicts"]["not_evaluable"] = 1_000_000
        payload["supply"][f"{S4}/{arm}"]["verdicts"]["not_evaluable"] = 1_000_000
    out = run(tmp_path, payload)
    assert out["by_arm"][PRODUCTION_ARM]["evaluable_share_of_name_days"] == pytest.approx(0.75)
    # 125 expected fires at full evaluability, three quarters of that at 0.75.
    assert cells(out)[0.5]["s12_expected_fires"] == pytest.approx(93.75)


def test_the_hard_upper_bound_on_dates_is_the_only_one_claimed(tmp_path: Path) -> None:
    """A date carries at least one position, so dates cannot exceed positions or sessions.

    That is arithmetic and holds without any dependence assumption — unlike the
    occupancy scenario printed beside it.
    """
    cell = cells(run(tmp_path, report()))[0.5]
    assert cell["s12_distinct_fill_dates_hard_upper_bound"] == pytest.approx(
        min(500.0, cell["s12_expected_positions_collapse_scenario"])
    )
    assert cell["s12_occupied_fill_dates_independence_scenario"] <= cell["s12_distinct_fill_dates_hard_upper_bound"]


def test_the_union_axis_is_an_envelope_over_scenarios_not_a_bound(tmp_path: Path) -> None:
    """The per-trade axis clusters the UNION of both books' fill dates.

    Their overlap is not inferable from marginal rates, so only the arithmetic envelope
    is given and neither end is asserted as the answer.
    """
    cell = cells(run(tmp_path, report()))[0.5]
    envelope = cell["union_fill_dates_envelope"]
    assert envelope["low_if_fully_overlapping"] <= envelope["high_if_disjoint"]
    assert envelope["low_if_fully_overlapping"] == max(
        cell["s12_occupied_fill_dates_independence_scenario"], cell["s4_occupied_fill_dates_independence_scenario"]
    )


def test_the_portfolio_axis_is_marks_minus_one_and_does_not_scale_with_the_panel(tmp_path: Path) -> None:
    """⚠ T equity marks give T-1 returns (Codex ckpt-1, finding 28).

    And the axis is a union of stored dates, so widening the panel does not multiply it —
    which is why "how many names" was the wrong question.
    """
    narrow = run(tmp_path, report(), "--panel-size", "10")
    wide = run(tmp_path, report(), "--panel-size", "50")
    assert narrow["by_arm"][PRODUCTION_ARM]["portfolio_axis_observations"] == 499.0
    assert wide["by_arm"][PRODUCTION_ARM]["portfolio_axis_observations"] == 499.0
    assert cells(wide)[0.5]["s12_expected_fires"] > cells(narrow)[0.5]["s12_expected_fires"]


def test_the_horizon_is_measured_not_a_252_day_convention(tmp_path: Path) -> None:
    """⚠ 252 is an invented constant; the sessions here are counted on the corpus."""
    out = run(tmp_path, report())
    assert out["by_arm"][PRODUCTION_ARM]["sessions_per_year_measured"] == 250.0
    assert out["by_arm"][PRODUCTION_ARM]["horizon_trading_days"] == 500.0
    assert out["horizon_months"] == 24


def test_both_arms_are_projected(tmp_path: Path) -> None:
    """⚠ The evaluator requires ``masked`` AND ``admitted`` by name."""
    out = run(tmp_path, report())
    assert sorted(out["by_arm"]) == sorted(ARMS)
    assert out["by_arm"][PRODUCTION_ARM]["is_production_arm"] is True
    assert out["by_arm"]["admitted"]["is_production_arm"] is False


def test_the_output_passes_no_verdict_on_sufficiency(tmp_path: Path) -> None:
    """⚠⚠ A supply projection is not a sufficiency verdict and must not read as one.

    An expectation above a guard on an OBSERVED count is not the guard being met, and
    ``MIN_SUPPORTING_CLUSTERS`` is a computability guard rather than a power floor. The
    first draft printed a boolean that read as a pass; there is no boolean now.
    """
    out = run(tmp_path, report())
    assert "SUFFICIENT" in out["WHAT_THIS_DOES_NOT_SAY"]
    assert out["min_supporting_clusters"] == MIN_SUPPORTING_CLUSTERS
    # ⚠ Asserted over the grid's KEYS, not the serialised text: the disclaimer itself
    # has to be able to use the word "clears" in order to deny it.
    for cell in out["by_arm"][PRODUCTION_ARM]["grid"]:
        assert not any("clear" in key for key in cell)
        assert not any(isinstance(value, bool) for value in cell.values())
    assert out["ASSUMPTIONS"]
