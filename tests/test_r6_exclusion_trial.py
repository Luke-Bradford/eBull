from __future__ import annotations

import math
from datetime import date, datetime

import pytest

from app.services.r6_exclusion_trial import (
    CLASSIFIED_BEST,
    CLASSIFIED_WORST,
    HALF_SPREAD,
    LEGACY_WORST,
    PROGRAMME_POLICIES,
    ZERO_RECOVERY,
    PriceBar,
    PriceSeries,
    SeriesEvidence,
    TerminationPolicy,
    binding_policy,
    evidence_sha256,
    haircut_net_return,
    realisation_census,
    simulate_legacy_case,
    simulate_portfolio,
    simulate_under_policies,
    termination_identity,
    universe_census,
    validate_factor,
)
from app.services.series_termination import TERMINATION_RULE_VERSION, TerminationClass, TerminationEvidence


def _series(symbol: str, final: float = 100.0) -> PriceSeries:
    return PriceSeries(
        symbol=symbol,
        bars=(
            PriceBar(date(2022, 6, 30), 100.0, 100.0),
            PriceBar(date(2022, 7, 1), 100.0, 100.0),
            PriceBar(date(2024, 9, 27), final, final),
        ),
        invalid_rows=0,
    )


def test_flat_one_period_portfolio_charges_exact_round_trip() -> None:
    schedule = ((datetime(2022, 6, 30, 16), frozenset({"AAA", "BBB"})),)
    result = simulate_legacy_case(
        schedule=schedule,
        prices={"AAA": _series("AAA"), "BBB": _series("BBB")},
        case="worst",
        half_spread=HALF_SPREAD,
    )
    expected = (1 - HALF_SPREAD) / (1 + HALF_SPREAD) - 1
    assert result.total_return == pytest.approx(expected)
    assert sum(event.spread_cost for event in result.events) > 0


def test_termination_bounds_are_explicit() -> None:
    terminated = PriceSeries(
        "AAA",
        (
            PriceBar(date(2022, 6, 30), 100.0, 100.0),
            PriceBar(date(2022, 7, 1), 100.0, 100.0),
            PriceBar(date(2023, 1, 3), 120.0, 120.0),
        ),
        0,
    )
    schedule = ((datetime(2022, 6, 30, 16), frozenset({"AAA"})),)
    best = simulate_legacy_case(schedule=schedule, prices={"AAA": terminated}, case="best", half_spread=0)
    worst = simulate_legacy_case(schedule=schedule, prices={"AAA": terminated}, case="worst", half_spread=0)
    assert best.total_return == pytest.approx(0.20)
    assert worst.total_return == -1.0


def test_in_series_halt_uses_same_declared_bounds() -> None:
    halted = PriceSeries(
        "AAA",
        (
            PriceBar(date(2022, 6, 30), 100.0, 100.0),
            PriceBar(date(2022, 7, 1), 100.0, 100.0),
            PriceBar(date(2023, 1, 3), 120.0, 120.0),
            PriceBar(date(2025, 1, 2), 80.0, 80.0),
        ),
        0,
    )
    schedule = ((datetime(2022, 6, 30, 16), frozenset({"AAA"})),)
    best = simulate_legacy_case(schedule=schedule, prices={"AAA": halted}, case="best", half_spread=0)
    worst = simulate_legacy_case(schedule=schedule, prices={"AAA": halted}, case="worst", half_spread=0)
    assert best.total_return == pytest.approx(0.20)
    assert worst.total_return == -1.0
    assert best.events[-1].censored_holdings == 1


def test_haircut_never_rescues_negative_gross_edge() -> None:
    assert haircut_net_return(
        strategy_gross=0.05,
        strategy_net=0.04,
        buy_hold_gross=0.10,
        haircut=0.58,
    ) == pytest.approx(0.04)
    assert haircut_net_return(
        strategy_gross=0.15,
        strategy_net=0.13,
        buy_hold_gross=0.10,
        haircut=0.58,
    ) == pytest.approx(0.101)


def test_factor_gate_requires_contemporaneous_positive_identity() -> None:
    keys = [(2022 + index // 12, index % 12 + 1) for index in range(24)]
    reference = {key: math.sin(index * 1.7) for index, key in enumerate(keys)}
    ours = {key: 0.003 + 1.2 * reference[key] for key in keys}
    result = validate_factor(ours, reference)
    assert result.passed
    assert result.correlation == pytest.approx(1.0)
    assert result.beta == pytest.approx(1.2)


def test_factor_gate_rejects_a_one_month_displacement() -> None:
    keys = [(2022 + index // 12, index % 12 + 1) for index in range(24)]
    reference = {key: math.sin(index * 1.7) for index, key in enumerate(keys)}
    ours = {key: reference[keys[index - 1]] if index else 0.0 for index, key in enumerate(keys)}
    assert not validate_factor(ours, reference).passed


def _bars(symbol: str, rows: tuple[tuple[str, float, float], ...]) -> PriceSeries:
    return PriceSeries(symbol, tuple(PriceBar(date.fromisoformat(day), o, c) for day, o, c in rows), 0)


#: #3362 acceptance 1 — a multi-rebalance fixture pinned on the PRE-refactor harness (``case="best"`` /
#: ``case="worst"``): a live name, a gap at a rebalance (GAP), a mid-window termination (DED), a
#: second-formation entry (NEW) and a name whose last bar sits inside the alive-at-capture cut (ALV).
REPLAY_PRICES = {
    "LIV": _bars(
        "LIV",
        (
            ("2022-06-30", 100, 100),
            ("2022-07-01", 100, 101),
            ("2023-07-03", 110, 111),
            ("2024-07-01", 120, 119),
            ("2024-09-27", 125, 126),
        ),
    ),
    "GAP": _bars(
        "GAP",
        (
            ("2022-06-30", 50, 50),
            ("2022-07-01", 50, 52),
            ("2023-06-29", 40, 41),
            ("2024-07-01", 45, 46),
            ("2024-09-27", 47, 48),
        ),
    ),
    "DED": _bars("DED", (("2022-06-30", 20, 20), ("2022-07-01", 20, 21), ("2023-01-03", 15, 14))),
    "NEW": _bars(
        "NEW",
        (("2023-06-30", 30, 30), ("2023-07-03", 31, 32), ("2024-07-01", 33, 34), ("2024-09-27", 35, 36)),
    ),
    "ALV": _bars(
        "ALV",
        (("2023-06-30", 10, 10), ("2023-07-03", 10, 11), ("2024-07-01", 12, 13), ("2024-09-25", 14, 15)),
    ),
}
REPLAY_SCHEDULE = (
    (datetime(2022, 6, 30, 16), frozenset({"LIV", "GAP", "DED"})),
    (datetime(2023, 6, 30, 16), frozenset({"LIV", "NEW", "ALV"})),
    (datetime(2024, 6, 28, 16), frozenset({"LIV", "GAP", "ALV"})),
)
#: (total_return, per-event (pre_cost_wealth, traded_notional, spread_cost, target_count, censored_holdings)).
REPLAY_GOLDEN = {
    "best": (
        0.06470303112516507,
        (
            (1.0, 0.9928021841648053, 0.007197815835194838, 3, 0),
            (0.8670472408372634, 1.1532759043475114, 0.008361250306519458, 3, 2),
            (0.9604188762123922, 0.6544779443800937, 0.004744965096755679, 3, 0),
            (1.072478500251992, 1.072478500251992, 0.007775469126826943, 0, 1),
        ),
    ),
    "worst": (
        -0.7189573474261477,
        (
            (1.0, 0.9928021841648053, 0.007197815835194838, 3, 0),
            (0.36402746752709525, 0.4841998071688024, 0.0035104486019738174, 3, 2),
            (0.4032292991731417, 0.27478081634278734, 0.0019921609184852082, 3, 0),
            (0.2830950919907854, 0.2830950919907854, 0.0020524394169331942, 0, 1),
        ),
    ),
}


@pytest.mark.parametrize("case", ["best", "worst"])
def test_multi_rebalance_replay_is_pinned(case: str) -> None:
    result = simulate_legacy_case(
        schedule=REPLAY_SCHEDULE,
        prices=REPLAY_PRICES,
        case=case,  # type: ignore[arg-type]
        half_spread=HALF_SPREAD,
    )
    total, events = REPLAY_GOLDEN[case]
    assert result.total_return == pytest.approx(total, rel=1e-12, abs=1e-15)
    assert len(result.events) == len(events)
    for event, expected in zip(result.events, events, strict=True):
        observed = (event.pre_cost_wealth, event.traded_notional, event.spread_cost)
        assert observed == pytest.approx(expected[:3], rel=1e-12, abs=1e-15)
        assert (event.target_count, event.censored_holdings) == expected[3:]


# ---- #3362 programme policies -------------------------------------------------------------------


def _evidence(
    prices: dict[str, PriceSeries],
    *,
    linked: dict[str, str | None] | None = None,
    q_suffix: frozenset[str] = frozenset(),
) -> dict[str, SeriesEvidence]:
    linked = linked or {}
    return {
        symbol: SeriesEvidence(
            series_id=index,
            first_bar=series.bars[0].day,
            last_bar=series.bars[-1].day,
            evidence=TerminationEvidence(
                linked=symbol in linked, provision=linked.get(symbol), q_suffix=symbol in q_suffix
            ),
        )
        for index, (symbol, series) in enumerate(sorted(prices.items()), start=1)
    }


def test_policy_rejects_out_of_range_and_incomplete_class_maps() -> None:
    with pytest.raises(ValueError, match="outside"):
        TerminationPolicy("bad", 1.5, None)
    with pytest.raises(ValueError, match="outside"):
        TerminationPolicy("nan", math.nan, None)
    with pytest.raises(ValueError, match="every TerminationClass"):
        TerminationPolicy("partial", 0.0, ((TerminationClass.UNKNOWN, 0.0),))


def test_classified_fractions_are_the_module_classes() -> None:
    assert CLASSIFIED_WORST.terminal_fraction(TerminationClass.EXCHANGE_FAILURE) == pytest.approx(0.45)
    assert CLASSIFIED_BEST.terminal_fraction(TerminationClass.EXCHANGE_FAILURE) == pytest.approx(0.45)
    assert CLASSIFIED_WORST.terminal_fraction(TerminationClass.UNKNOWN) == pytest.approx(0.45)
    assert CLASSIFIED_BEST.terminal_fraction(TerminationClass.UNKNOWN) == 1.0
    assert CLASSIFIED_WORST.terminal_fraction(TerminationClass.OPERATION_OF_LAW) == 1.0
    assert all(ZERO_RECOVERY.terminal_fraction(c) == 0.0 for c in TerminationClass)


def test_zero_recovery_splits_status_and_spares_only_the_alive_at_capture_window_end() -> None:
    evidence = _evidence(REPLAY_PRICES, linked={"DED": "(b)"})
    zero = simulate_portfolio(
        schedule=REPLAY_SCHEDULE, prices=REPLAY_PRICES, policy=ZERO_RECOVERY, half_spread=HALF_SPREAD, evidence=evidence
    )
    legacy = simulate_portfolio(
        schedule=REPLAY_SCHEDULE, prices=REPLAY_PRICES, policy=LEGACY_WORST, half_spread=HALF_SPREAD
    )
    # Identical until the window end, where ALV (last bar 2024-09-25, inside the alive cut) is
    # valued at its last close instead of zero.
    assert [e.pre_cost_wealth for e in zero.events[:-1]] == [e.pre_cost_wealth for e in legacy.events[:-1]]
    assert zero.total_return > legacy.total_return
    statuses = {(r.session.isoformat(), r.symbol): (r.status, r.termination_class) for r in zero.realisations}
    assert statuses == {
        ("2023-07-03", "DED"): ("terminated", TerminationClass.EXCHANGE_FAILURE),
        ("2023-07-03", "GAP"): ("gap", None),
        ("2024-09-27", "ALV"): ("alive_at_capture", None),
    }
    alive = next(r for r in zero.realisations if r.symbol == "ALV")
    assert alive.realised_value == alive.last_close_value > 0
    assert {r.status for r in legacy.realisations} == {"unsplit"}


def test_classified_policies_price_terminations_off_the_last_close() -> None:
    evidence = _evidence(REPLAY_PRICES)  # DED unlinked: unknown_termination, two-armed
    results = {
        policy.label: simulate_portfolio(
            schedule=REPLAY_SCHEDULE, prices=REPLAY_PRICES, policy=policy, half_spread=0.0, evidence=evidence
        )
        for policy in PROGRAMME_POLICIES
    }
    ded = {label: next(r for r in result.realisations if r.symbol == "DED") for label, result in results.items()}
    for label, fraction in (("zero_recovery", 0.0), ("classified_worst", 0.45), ("classified_best", 1.0)):
        item = ded[label]
        assert item.last_bar == date(2023, 1, 3)
        assert item.termination_class is TerminationClass.UNKNOWN
        assert item.last_close_value == pytest.approx(item.shares * 14)
        assert item.realised_value == pytest.approx(item.last_close_value * fraction)
    gap = {label: next(r for r in result.realisations if r.symbol == "GAP") for label, result in results.items()}
    assert gap["classified_worst"].realised_value == 0.0
    assert gap["classified_best"].realised_value == gap["classified_best"].last_close_value


def test_last_bar_fill_is_an_ordinary_price_then_terminates() -> None:
    prices = {
        "LIV": _bars("LIV", (("2022-06-30", 10, 10), ("2022-07-01", 10, 10), ("2024-09-27", 10, 10))),
        "END": _bars("END", (("2022-06-30", 10, 10), ("2022-07-01", 10, 12))),
    }
    schedule = ((datetime(2022, 6, 30, 16), frozenset({"LIV", "END"})),)
    result = simulate_portfolio(
        schedule=schedule, prices=prices, policy=CLASSIFIED_BEST, half_spread=0.0, evidence=_evidence(prices)
    )
    (item,) = result.realisations
    assert (item.symbol, item.status, item.session) == ("END", "terminated", date(2024, 9, 27))
    assert result.total_return == pytest.approx(0.5 * 1.0 + 0.5 * 1.2 - 1.0)


def test_zero_wealth_path_logs_no_phantom_realisations() -> None:
    prices = {
        "DIE": _bars("DIE", (("2022-06-30", 10, 10), ("2022-07-01", 10, 10), ("2022-08-01", 9, 9))),
        "LIV": _bars(
            "LIV", (("2022-06-30", 10, 10), ("2023-07-03", 10, 10), ("2024-07-01", 10, 10), ("2024-09-27", 10, 10))
        ),
    }
    prices["DIE2"] = _bars("DIE2", (("2022-06-30", 5, 5), ("2022-07-01", 5, 5), ("2022-09-01", 4, 4)))
    schedule = (
        (datetime(2022, 6, 30, 16), frozenset({"DIE", "DIE2"})),
        (datetime(2023, 6, 30, 16), frozenset({"LIV"})),
    )
    result = simulate_portfolio(
        schedule=schedule, prices=prices, policy=ZERO_RECOVERY, half_spread=0.0, evidence=_evidence(prices)
    )
    assert result.total_return == -1.0
    assert {r.symbol for r in result.realisations} == {"DIE", "DIE2"}


def test_under_policies_runs_arm_and_control_under_each_policy() -> None:
    evidence = _evidence(REPLAY_PRICES)
    schedules = {"arm": REPLAY_SCHEDULE, "control": REPLAY_SCHEDULE[:2]}
    results = simulate_under_policies(
        schedules=schedules,
        prices=REPLAY_PRICES,
        policies=PROGRAMME_POLICIES,
        half_spread=HALF_SPREAD,
        evidence=evidence,
    )
    assert list(results) == [policy.label for policy in PROGRAMME_POLICIES]
    for policy in PROGRAMME_POLICIES:
        for name, schedule in schedules.items():
            direct = simulate_portfolio(
                schedule=schedule, prices=REPLAY_PRICES, policy=policy, half_spread=HALF_SPREAD, evidence=evidence
            )
            assert results[policy.label][name] == direct


def test_under_policies_refuses_unsafe_inputs() -> None:
    evidence = _evidence(REPLAY_PRICES)
    ok = {"arm": REPLAY_SCHEDULE}

    def run(**overrides: object) -> None:
        kwargs: dict[str, object] = {
            "schedules": ok,
            "prices": REPLAY_PRICES,
            "policies": PROGRAMME_POLICIES,
            "half_spread": 0.0,
            "evidence": evidence,
        }
        kwargs.update(overrides)
        simulate_under_policies(**kwargs)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="unique"):
        run(policies=(ZERO_RECOVERY, ZERO_RECOVERY))
    with pytest.raises(ValueError, match="governing"):
        run(policies=(CLASSIFIED_WORST,))
    with pytest.raises(ValueError, match="ZERO_RECOVERY itself"):
        run(policies=(TerminationPolicy("zero_recovery", 1.0, None),))
    with pytest.raises(ValueError, match="series evidence"):
        run(evidence=None)
    with pytest.raises(ValueError, match="no series evidence"):
        run(evidence={k: v for k, v in evidence.items() if k != "DED"})
    clipped = dict(evidence)
    clipped["DED"] = SeriesEvidence(1, date(2022, 6, 30), date(2024, 1, 2), evidence["DED"].evidence)
    with pytest.raises(ValueError, match="do not match the stored"):
        run(evidence=clipped)
    with pytest.raises(ValueError, match="sorted"):
        run(schedules={"arm": tuple(reversed(REPLAY_SCHEDULE))})
    with pytest.raises(ValueError, match="window end"):
        run(schedules={"arm": ((datetime(2024, 9, 27, 16), frozenset({"LIV"})),)})


def test_binding_policy_is_the_minimum_and_needs_zero_recovery() -> None:
    assert binding_policy({"zero_recovery": 0.02, "classified_worst": -0.01, "classified_best": 0.03}) == (
        "classified_worst",
        -0.01,
    )
    # A classified policy never rescues a governing fail.
    assert binding_policy({"zero_recovery": -0.05, "classified_best": 0.10}) == ("zero_recovery", -0.05)
    with pytest.raises(ValueError, match="zero_recovery"):
        binding_policy({"classified_best": 0.1})


def test_census_and_identity() -> None:
    evidence = _evidence(REPLAY_PRICES, linked={"DED": "(a)(3)"})
    census = universe_census(REPLAY_PRICES, evidence)
    assert census == {
        "priced": 5,
        "live": 3,
        "alive_at_capture": 1,
        "terminated_by_class": {"operation_of_law": 1},
    }
    result = simulate_portfolio(
        schedule=REPLAY_SCHEDULE, prices=REPLAY_PRICES, policy=CLASSIFIED_WORST, half_spread=0.0, evidence=evidence
    )
    cells = realisation_census(result)
    assert set(cells) == {"alive_at_capture:-", "gap:-", "terminated:operation_of_law"}
    assert cells["terminated:operation_of_law"]["realised_value"] == pytest.approx(
        cells["terminated:operation_of_law"]["last_close_value"]
    )
    identity = termination_identity(PROGRAMME_POLICIES, evidence)
    assert identity["termination_rule_version"] == TERMINATION_RULE_VERSION
    assert [p["label"] for p in identity["policies"]] == ["zero_recovery", "classified_worst", "classified_best"]
    assert identity["evidence_sha256"] == evidence_sha256(evidence)
    assert evidence_sha256(evidence) != evidence_sha256(_evidence(REPLAY_PRICES))
    assert identity["linkage_provenance"]["artefact_sha256"].startswith("63bb68ee")
