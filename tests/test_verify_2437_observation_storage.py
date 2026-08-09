import pytest

from scripts.verify_2437_observation_storage import (
    TIERS,
    Tier,
    measured_bytes_per_row,
    projected_annual_signal_bytes,
    projected_annual_signal_rows,
    projected_bytes,
)


def test_declared_tier_row_caps_are_stable() -> None:
    assert {tier.name: tier.rows for tier in TIERS} == {
        "30-minute context": 6_552_000,
        "5-minute setup": 4_914_000,
        "1-minute execution": 585_000,
    }


def test_projection_uses_measured_reference_size() -> None:
    tier = Tier("test", instruments=2, minutes_per_bar=30, retained_days=3)

    assert tier.rows == 78
    assert projected_bytes(tier, bytes_per_row=100) == 7_800


def test_signal_projection_includes_every_daily_verdict_leg() -> None:
    assert projected_annual_signal_rows(34_698) == 8_743_896
    assert (
        projected_annual_signal_bytes(
            34_698,
            measured_rows=34_698,
            measured_total_bytes=32_000_000,
        )
        == 8_064_000_000
    )


def test_signal_projection_refuses_an_unmeasured_row_size() -> None:
    with pytest.raises(ValueError, match="measured_rows must be positive"):
        projected_annual_signal_bytes(100, measured_rows=0, measured_total_bytes=0)


def test_measured_bytes_per_row_handles_an_empty_relation() -> None:
    assert measured_bytes_per_row(0, 0) is None
    assert measured_bytes_per_row(4, 100) == 25.0
