"""Bounded decision report for the frozen Schedule 13D falsification.

This module is deliberately pure: callers must cross the separately reviewed
outcome gate before supplying price windows.  It performs no database reads and
persists no event-level observations.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any, Literal

from scripts.evaluate_2582_schedule13d_outcomes import (
    Initial13GSourceEvent,
    PriceWindow,
    SourceEvent,
    accepted_window_return_pct,
    window_match_features,
)
from scripts.schedule13d_challengers import RULE_13G, match_initial_13g_without_replacement
from scripts.schedule13d_statistics import (
    DifferenceTest,
    EventOutcome,
    OutcomeStatistics,
    PairedDifference,
    holm_adjust,
    paired_clustered_difference_test,
    summarise_outcomes,
)

Decision = Literal["pass", "fail", "inconclusive"]
GateState = Literal["pass", "fail", "inconclusive"]


@dataclass(frozen=True)
class PairedComparison:
    name: str
    challenger_eligible_count: int
    matched_count: int
    unmatched_treatment_count: int
    test: DifferenceTest | None
    holm_adjusted_one_sided_p_value: float | None = None


@dataclass(frozen=True)
class DecisionGate:
    name: str
    state: GateState
    observed: str


@dataclass(frozen=True)
class HistoricalFalsificationReport:
    decision: Decision
    primary_source_count: int
    primary_eligible_count: int
    primary_refusals: Mapping[str, int]
    initial_13g_source_count: int
    initial_13g_refusals: Mapping[str, int]
    primary: OutcomeStatistics
    unfiltered_eligible_count: int
    unfiltered: OutcomeStatistics | None
    comparisons: tuple[PairedComparison, ...]
    gates: tuple[DecisionGate, ...]

    def as_dict(self) -> dict[str, Any]:
        """Return the one bounded JSON-compatible aggregate artifact."""

        return _json_compatible(asdict(self))


def _json_compatible(value: Any) -> Any:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_compatible(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_compatible(item) for item in value]
    return value


def _refusal_counts(
    source_events: Sequence[SourceEvent],
    initial_13g_sources: Sequence[Initial13GSourceEvent],
    windows: Sequence[PriceWindow],
) -> tuple[dict[str, int], dict[str, int]]:
    primary: Counter[str] = Counter()
    for event in source_events:
        if (refusal := event.primary_source_refusal) is not None:
            primary[refusal] += 1
    challengers: Counter[str] = Counter()
    for event in initial_13g_sources:
        if (refusal := event.source_refusal) is not None:
            challengers[refusal] += 1
    for window in windows:
        refusal = window.outcome_refusal
        if refusal is None:
            continue
        if window.population == "13g":
            challengers[refusal] += 1
        elif window.population == "primary":
            primary[refusal] += 1
    return dict(sorted(primary.items())), dict(sorted(challengers.items()))


def _accepted(windows: Sequence[PriceWindow], population: str) -> tuple[PriceWindow, ...]:
    return tuple(window for window in windows if window.population == population and window.outcome_refusal is None)


def _unique_by_accession(windows: Sequence[PriceWindow]) -> dict[str, PriceWindow]:
    result: dict[str, PriceWindow] = {}
    for window in windows:
        accession = window.event.accession_number
        if accession in result:
            raise ValueError(f"duplicate accepted accession: {accession}")
        result[accession] = window
    return result


def _event_outcome(window: PriceWindow, sector_by_instrument: Mapping[int, str]) -> EventOutcome:
    event = window.event
    instrument_id = event.instrument_id
    maximum = event.maximum_percent_of_class if isinstance(event, SourceEvent) else None
    return EventOutcome(
        accession_number=event.accession_number,
        issuer_cik=event.issuer_cik,
        entry_date=window.entry_date,
        exit_date=window.exit_date,
        net_return_pct=float(accepted_window_return_pct(window)),
        maximum_percent_of_class=None if maximum is None else float(maximum),
        sector=None if instrument_id is None else sector_by_instrument.get(instrument_id),
    )


def _difference(treatment: PriceWindow, challenger: PriceWindow) -> PairedDifference:
    return PairedDifference(
        treatment_accession=treatment.event.accession_number,
        treatment_issuer_cik=treatment.event.issuer_cik,
        treatment_entry_date=treatment.entry_date,
        difference_pct=float(accepted_window_return_pct(treatment) - accepted_window_return_pct(challenger)),
    )


def _comparison(
    name: str,
    treatments: Mapping[str, PriceWindow],
    challengers: Sequence[PriceWindow],
    pairs: Sequence[tuple[str, str]],
) -> PairedComparison:
    challenger_by_accession = _unique_by_accession(challengers)
    differences = tuple(
        _difference(treatments[treatment_accession], challenger_by_accession[challenger_accession])
        for treatment_accession, challenger_accession in pairs
    )
    test = paired_clustered_difference_test(differences) if len(differences) >= 2 else None
    return PairedComparison(
        name=name,
        challenger_eligible_count=len(challengers),
        matched_count=len(differences),
        unmatched_treatment_count=len(treatments) - len(differences),
        test=test,
    )


def _random_comparison(
    treatments: Mapping[str, PriceWindow], random_windows: Sequence[PriceWindow]
) -> PairedComparison:
    random_by_accession = _unique_by_accession(random_windows)
    pairs = tuple((accession, accession) for accession in sorted(set(treatments) & set(random_by_accession)))
    return _comparison("random_time", treatments, random_windows, pairs)


def _13g_comparison(
    rule: RULE_13G,
    treatments: Mapping[str, PriceWindow],
    challenger_windows: Sequence[PriceWindow],
) -> PairedComparison:
    matches = match_initial_13g_without_replacement(
        tuple(window_match_features(window) for window in treatments.values()),
        tuple(window_match_features(window) for window in challenger_windows),
        rule=rule,
    )
    pairs = tuple((item.treatment_accession, item.challenger_accession) for item in matches)
    eligible = tuple(
        window
        for window in challenger_windows
        if isinstance(window.event, Initial13GSourceEvent) and window.event.rule == rule
    )
    return _comparison(f"initial_13g_{rule}", treatments, eligible, pairs)


def _with_holm(comparisons: Sequence[PairedComparison]) -> tuple[PairedComparison, ...]:
    gating_names = ("random_time", "initial_13g_1b", "initial_13g_1c")
    by_name = {comparison.name: comparison for comparison in comparisons}
    tests = [by_name[name].test for name in gating_names]
    if any(test is None for test in tests):
        return tuple(comparisons)
    adjusted = holm_adjust(tuple(test.one_sided_p_value for test in tests if test is not None))
    adjusted_by_name: dict[str, float] = dict(zip(gating_names, adjusted, strict=True))
    return tuple(
        PairedComparison(
            name=item.name,
            challenger_eligible_count=item.challenger_eligible_count,
            matched_count=item.matched_count,
            unmatched_treatment_count=item.unmatched_treatment_count,
            test=item.test,
            holm_adjusted_one_sided_p_value=adjusted_by_name.get(item.name),
        )
        for item in comparisons
    )


def _boolean_gate(name: str, passed: bool, observed: str) -> DecisionGate:
    return DecisionGate(name, "pass" if passed else "fail", observed)


def _decision_gates(primary: OutcomeStatistics, comparisons: Sequence[PairedComparison]) -> tuple[DecisionGate, ...]:
    effective_n = primary.clustered.effective_sample_size
    gates: list[DecisionGate] = [
        DecisionGate(
            "effective_sample_size_gte_785",
            "pass" if effective_n >= 785 else "inconclusive",
            f"{effective_n:.6g}",
        ),
        _boolean_gate(
            "adverse_cost_clustered_lower_bound_gt_zero",
            primary.clustered.lower_95_pct > 0,
            f"{primary.clustered.lower_95_pct:.6g}",
        ),
        _boolean_gate(
            "profit_factor_gt_one",
            primary.profit_factor is None
            and primary.hit_rate_pct == 100
            or primary.profit_factor is not None
            and primary.profit_factor > 1,
            "infinite_no_losers"
            if primary.profit_factor is None and primary.hit_rate_pct == 100
            else str(primary.profit_factor),
        ),
        _boolean_gate(
            "positive_result_excluding_best_1pct",
            primary.excluding_best_1pct_mean_pct > 0,
            f"{primary.excluding_best_1pct_mean_pct:.6g}",
        ),
        _boolean_gate(
            "maximum_issuer_positive_concentration_lt_20pct",
            primary.maximum_issuer_positive_concentration_pct < 20,
            f"{primary.maximum_issuer_positive_concentration_pct:.6g}",
        ),
        _boolean_gate(
            "maximum_entry_session_positive_concentration_lt_20pct",
            primary.maximum_entry_session_positive_concentration_pct < 20,
            f"{primary.maximum_entry_session_positive_concentration_pct:.6g}",
        ),
    ]
    stability = primary.stability[:3]
    latest = stability[-1].mean_net_return_pct
    gates.append(
        _boolean_gate(
            "latest_nonoverlapping_6_month_mean_positive",
            latest is not None and latest > 0,
            str(latest),
        )
    )
    positive_windows = sum(item.mean_net_return_pct is not None and item.mean_net_return_pct > 0 for item in stability)
    gates.append(_boolean_gate("two_of_three_6_month_means_positive", positive_windows >= 2, str(positive_windows)))
    for comparison in comparisons:
        if comparison.name not in ("random_time", "initial_13g_1b", "initial_13g_1c"):
            continue
        test = comparison.test
        adjusted = comparison.holm_adjusted_one_sided_p_value
        if test is None or adjusted is None:
            gates.append(DecisionGate(f"paired_{comparison.name}", "inconclusive", "insufficient matched pairs"))
        else:
            gates.append(
                _boolean_gate(
                    f"paired_{comparison.name}",
                    test.lower_95_pct > 0 and adjusted <= 0.05,
                    f"lower={test.lower_95_pct:.6g};holm_p={adjusted:.6g}",
                )
            )
    return tuple(gates)


def build_historical_falsification_report(
    *,
    source_events: Sequence[SourceEvent],
    initial_13g_sources: Sequence[Initial13GSourceEvent],
    primary_windows: Sequence[PriceWindow],
    unfiltered_windows: Sequence[PriceWindow],
    random_windows: Sequence[PriceWindow],
    initial_13g_windows: Sequence[PriceWindow],
    sector_by_instrument: Mapping[int, str] | None = None,
) -> HistoricalFalsificationReport:
    """Assemble all predeclared arms and decide without subgroup selection."""

    sectors = sector_by_instrument or {}
    accepted_primary = _accepted(primary_windows, "primary")
    if len(accepted_primary) < 2:
        raise ValueError("primary report requires at least two eligible events")
    treatments = _unique_by_accession(accepted_primary)
    primary = summarise_outcomes(tuple(_event_outcome(item, sectors) for item in accepted_primary))
    accepted_unfiltered = _accepted(unfiltered_windows, "unfiltered")
    unfiltered = (
        summarise_outcomes(tuple(_event_outcome(item, sectors) for item in accepted_unfiltered))
        if len(accepted_unfiltered) >= 2
        else None
    )
    accepted_random = _accepted(random_windows, "random")
    accepted_13g = _accepted(initial_13g_windows, "13g")
    comparisons = _with_holm(
        (
            _random_comparison(treatments, accepted_random),
            _13g_comparison("1b", treatments, accepted_13g),
            _13g_comparison("1c", treatments, accepted_13g),
            _13g_comparison("both", treatments, accepted_13g),
            _13g_comparison("unknown", treatments, accepted_13g),
        )
    )
    gates = _decision_gates(primary, comparisons)
    decision: Decision
    if any(gate.state == "fail" for gate in gates):
        decision = "fail"
    elif any(gate.state == "inconclusive" for gate in gates):
        decision = "inconclusive"
    else:
        decision = "pass"
    primary_refusals, initial_13g_refusals = _refusal_counts(
        source_events,
        initial_13g_sources,
        tuple(primary_windows) + tuple(initial_13g_windows),
    )
    return HistoricalFalsificationReport(
        decision=decision,
        primary_source_count=len(source_events),
        primary_eligible_count=len(accepted_primary),
        primary_refusals=primary_refusals,
        initial_13g_source_count=len(initial_13g_sources),
        initial_13g_refusals=initial_13g_refusals,
        primary=primary,
        unfiltered_eligible_count=len(accepted_unfiltered),
        unfiltered=unfiltered,
        comparisons=comparisons,
        gates=gates,
    )
