from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from scripts.verify_2914_operational_rules import _build_evidence, render_markdown


def _row(*, declared: int = 2, observed: int = 2) -> dict[str, object]:
    return {
        "snapshot_id": 1,
        "source": "aqr",
        "dataset_key": "vme_monthly",
        "response_sha256": "a" * 64,
        "parser_version": "aqr-vme-monthly-xlsx-v2",
        "declared_row_count": declared,
        "series_key": "MOM",
        "unit": "decimal_return",
        "observation_count": observed,
        "first_observation": date(2020, 1, 31),
        "last_observation": date(2020, 2, 29),
    }


def test_census_records_returns_as_ineligible_for_factor_valuation() -> None:
    evidence = _build_evidence(
        [_row()],
        measured_at=datetime(2026, 8, 23, tzinfo=UTC),
        execution_commit="b" * 40,
    )
    assert evidence.observation_count == 2
    assert evidence.eligible_valuation_spread_series == 0
    assert evidence.operational_checks and all(evidence.operational_checks.values())
    assert evidence.factor_valuation_record["status"] == "unavailable"
    assert evidence.haircuts["15pct"].startswith("N/A")
    assert evidence.haircuts["58pct"].startswith("N/A")
    assert str(evidence.factor_valuation_record["reason"]) in render_markdown(evidence)


def test_census_refuses_snapshot_row_count_mismatch() -> None:
    with pytest.raises(RuntimeError, match="row conservation failed"):
        _build_evidence(
            [_row(declared=3, observed=2)],
            measured_at=datetime(2026, 8, 23, tzinfo=UTC),
            execution_commit="b" * 40,
        )
