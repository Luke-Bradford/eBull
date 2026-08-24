"""Pure contract checks for #2833's prospective selection readout."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from scripts.verify_2833_core_selection import (
    DECLARATION_PATH,
    Eligibility,
    Observation,
    evaluate,
    load_declaration,
    percentile_cont,
)


def _eligibility(instrument_id: int) -> Eligibility:
    return Eligibility(
        instrument_id=instrument_id,
        observed_at=datetime(2026, 8, 24, tzinfo=UTC),
        verdict="underlying",
        settlement_type="real",
        direction="long",
        leverage_values=(1,),
        allow_open_position=True,
        response_digest=f"digest-{instrument_id}",
    )


def _population(*, days: int = 5, broken_id: int | None = None, non_usd_id: int | None = None) -> list[Observation]:
    rows: list[Observation] = []
    start = datetime(2026, 8, 25, 10, tzinfo=UTC)
    symbols = {3417: "SPY.RTH", 3434: "CSPX.L", 3075: "IUSA.L"}
    spreads = {3417: Decimal("4"), 3434: Decimal("2"), 3075: Decimal("1")}
    for day in range(days):
        for instrument_id, symbol in symbols.items():
            for hour in range(3):
                status = "invalid" if instrument_id == broken_id and day == 2 and hour == 1 else "observed"
                rows.append(
                    Observation(
                        instrument_id=instrument_id,
                        symbol=symbol,
                        sample_bucket=start + timedelta(days=day, hours=hour),
                        status=status,
                        spread_bps=spreads[instrument_id] if status == "observed" else None,
                        conversion_rate=(Decimal("0.013") if instrument_id == non_usd_id else Decimal(1))
                        if status == "observed"
                        else None,
                    )
                )
    return rows


def test_frozen_declaration_digest_is_intact() -> None:
    declaration = load_declaration(DECLARATION_PATH)
    assert declaration["schema_version"] == "core-selection-2833-v1"
    assert declaration["evidence_not_before"] == "2026-08-25T00:00:00Z"


def test_percentile_matches_continuous_n_minus_one_interpolation() -> None:
    assert percentile_cont([Decimal("0"), Decimal("10")], Decimal("0.75")) == Decimal("7.50")


def test_no_candidate_metrics_are_revealed_before_five_common_dates() -> None:
    result = evaluate(
        _population(days=4),
        {instrument_id: _eligibility(instrument_id) for instrument_id in (3417, 3434, 3075)},
        load_declaration(),
        now=datetime(2026, 9, 1, tzinfo=UTC),
    )
    assert result["outcome"] == "evidence_collecting"
    assert "candidates" not in result


def test_fifth_date_stays_sealed_until_the_following_utc_midnight() -> None:
    result = evaluate(
        _population(),
        {instrument_id: _eligibility(instrument_id) for instrument_id in (3417, 3434, 3075)},
        load_declaration(),
        now=datetime(2026, 8, 29, 23, 59, tzinfo=UTC),
    )
    assert result["outcome"] == "evidence_collecting"
    assert "candidates" not in result


def test_winner_is_lowest_p75_among_candidates_that_close_every_cost() -> None:
    result = evaluate(
        _population(non_usd_id=3075),
        {instrument_id: _eligibility(instrument_id) for instrument_id in (3417, 3434, 3075)},
        load_declaration(),
        now=datetime(2026, 8, 30, tzinfo=UTC),
    )
    assert result["outcome"] == "pass"
    assert result["selected_instrument_id"] == 3434
    iusa = next(row for row in result["candidates"] if row["instrument_id"] == 3075)
    assert iusa["refusals"] == ("fx_unmodelled",)


def test_an_internal_missing_bucket_fails_the_candidate() -> None:
    result = evaluate(
        _population(broken_id=3434),
        {instrument_id: _eligibility(instrument_id) for instrument_id in (3417, 3434, 3075)},
        load_declaration(),
        now=datetime(2026, 8, 30, tzinfo=UTC),
    )
    cspx = next(row for row in result["candidates"] if row["instrument_id"] == 3434)
    assert "incomplete_population" in cspx["refusals"]


def test_unproved_product_cannot_pass_on_a_tight_spread() -> None:
    eligibilities = {instrument_id: _eligibility(instrument_id) for instrument_id in (3417, 3434, 3075)}
    eligibilities.pop(3075)
    result = evaluate(
        _population(),
        eligibilities,
        load_declaration(),
        now=datetime(2026, 8, 30, tzinfo=UTC),
    )
    iusa = next(row for row in result["candidates"] if row["instrument_id"] == 3075)
    assert "not_proved_real_long_x1" in iusa["refusals"]
