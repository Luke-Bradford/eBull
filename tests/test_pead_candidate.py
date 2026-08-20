from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from statistics import fmean, stdev

import pytest

from app.services.pead_candidate import (
    MIN_THRESHOLD_EVENTS,
    QuarterObservation,
    ReportedFact,
    SueEvent,
    _archive_sha256,
    calculate_sue_events,
    classify_causal_triggers,
    construct_quarters,
    expand_instrument_alternatives,
    nearest_rank,
)


def _fact(
    fiscal_year: int,
    fiscal_period: str,
    value: str,
    *,
    accession: str | None = None,
    filed_month: int | None = None,
) -> ReportedFact:
    quarter = 4 if fiscal_period == "FY" else int(fiscal_period[1])
    month = filed_month or quarter * 3
    return ReportedFact(
        instrument_id=7,
        accession_number=accession or f"{fiscal_year}-{fiscal_period}",
        form_type="10-K" if fiscal_period == "FY" else "10-Q",
        filed_date=date(fiscal_year + (quarter == 4), month if quarter < 4 else 2, 15),
        accepted_at=None,
        fiscal_year=fiscal_year,
        fiscal_period=fiscal_period,
        period_end=date(fiscal_year, quarter * 3, 1),
        values=(Decimal(value),),
    )


def test_q4_is_the_as_known_annual_residual() -> None:
    observations, refusals = construct_quarters(
        [_fact(2024, "Q1", "10"), _fact(2024, "Q2", "20"), _fact(2024, "Q3", "30"), _fact(2024, "FY", "100")]
    )
    q4 = next(item for item in observations if item.fiscal_quarter == 4)
    assert q4.value == Decimal("40")
    assert q4.derived_q4 is True
    assert q4.source_accessions == ("2024-FY", "2024-Q1", "2024-Q2", "2024-Q3")
    assert refusals == {}


def test_missing_or_ambiguous_source_facts_are_refused() -> None:
    ambiguous = _fact(2024, "Q1", "10")
    ambiguous = ReportedFact(**{**ambiguous.__dict__, "values": (Decimal("10"), Decimal("11"))})
    observations, refusals = construct_quarters([ambiguous, _fact(2024, "FY", "100")])
    assert observations == ()
    assert refusals["ambiguous_current_fact"] == 1
    assert refusals["q4_missing_quarter_leg"] == 1


def test_non_finite_source_fact_is_refused() -> None:
    invalid = _fact(2024, "Q1", "NaN")
    observations, refusals = construct_quarters([invalid])
    assert observations == ()
    assert refusals["non_finite_source_value"] == 1


def test_duplicate_original_fiscal_slot_is_not_ordered_into_a_truth() -> None:
    observations, refusals = construct_quarters(
        [_fact(2024, "Q1", "10", accession="first"), _fact(2024, "Q1", "10", accession="second")]
    )
    assert observations == ()
    assert refusals["duplicate_fiscal_slot"] == 1


def _observation(key: int, value: int, instrument_id: int = 7) -> QuarterObservation:
    fiscal_year, zero_quarter = divmod(key, 4)
    fiscal_quarter = zero_quarter + 1
    return QuarterObservation(
        instrument_id=instrument_id,
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
        value=Decimal(value),
        filed_date=date(2020 + key % 6, fiscal_quarter * 3, 1),
        accepted_at=None,
        accession_number=f"a-{key}",
        source_accessions=(f"a-{key}",),
        derived_q4=fiscal_quarter == 4,
    )


def test_sue_uses_exactly_21_prior_seasonal_differences() -> None:
    # Keys 100..125 provide the required current quarter plus 25-quarter
    # history. Quadratic values make the forecast-error dispersion non-zero.
    observations = [_observation(key, key * key) for key in range(100, 126)]
    events, refusals = calculate_sue_events(observations)
    assert len(events) == 1
    current_key = 125
    differences = [float(key * key - (key - 4) * (key - 4)) for key in range(current_key - 21, current_key)]
    drift = fmean(differences)
    dispersion = stdev(item - drift for item in differences)
    expected = (float(current_key * current_key - (current_key - 4) * (current_key - 4)) - drift) / dispersion
    assert events[0].sue == expected
    assert refusals["insufficient_consecutive_history"] == 25


def test_a_gap_in_fiscal_history_refuses_the_later_sue() -> None:
    observations = [_observation(key, key * key) for key in range(100, 126) if key != 110]
    events, refusals = calculate_sue_events(observations)
    assert events == ()
    assert refusals["insufficient_consecutive_history"] == 25


def _event(calendar_quarter: int, sue: float, instrument_id: int) -> SueEvent:
    year, zero_quarter = divmod(calendar_quarter - 1, 4)
    quarter = zero_quarter + 1
    observation = QuarterObservation(
        instrument_id=instrument_id,
        fiscal_year=2020,
        fiscal_quarter=1,
        value=Decimal("1"),
        filed_date=date(year, quarter * 3, 1),
        accepted_at=None,
        accession_number=f"event-{calendar_quarter}-{instrument_id}",
        source_accessions=(f"event-{calendar_quarter}-{instrument_id}",),
        derived_q4=False,
    )
    return SueEvent(observation=observation, sue=sue)


def test_trigger_threshold_uses_completed_quarters_only() -> None:
    target_quarter = 2025 * 4 + 1
    prior = [_event(target_quarter - 1, float(item), item + 1) for item in range(MIN_THRESHOLD_EVENTS)]
    target = _event(target_quarter, 1_000.0, 10_001)
    same_quarter_future_peer = _event(target_quarter, 1_000_000.0, 10_002)
    classified, refusals = classify_causal_triggers([*prior, target, same_quarter_future_peer])
    target_result = next(item for item in classified if item.event.observation.instrument_id == 10_001)
    assert target_result.upper_threshold == nearest_rank([float(item) for item in range(MIN_THRESHOLD_EVENTS)], 0.9)
    assert target_result.side == "long"
    assert target_result.threshold_population == MIN_THRESHOLD_EVENTS
    assert refusals["thin_prior_cross_section"] == MIN_THRESHOLD_EVENTS


def test_nearest_rank_is_not_interpolated() -> None:
    assert nearest_rank([1.0, 2.0, 100.0], 0.5) == 2.0


def test_share_classes_expand_only_after_one_issuer_signal_exists() -> None:
    event = _event(2025 * 4 + 1, 3.0, 10)
    triggered, _ = classify_causal_triggers(
        [*[_event(2025 * 4, float(item), item + 100) for item in range(MIN_THRESHOLD_EVENTS)], event]
    )
    signal = next(item for item in triggered if item.event.observation.instrument_id == 10)
    expanded = expand_instrument_alternatives((signal,), {10: (10, 11)})
    assert [item.event.observation.instrument_id for item in expanded] == [10, 11]
    assert all(item.event.sue == signal.event.sue for item in expanded)


def test_archive_digest_is_measured_not_merely_read_from_sidecar(tmp_path: Path) -> None:
    archive = tmp_path / "companyfacts.zip"
    archive.write_bytes(b"declared archive bytes")
    archive.with_name("companyfacts.zip.sha256").write_text("0" * 64)
    with pytest.raises(ValueError, match="archive SHA-256 mismatch"):
        _archive_sha256(archive)
