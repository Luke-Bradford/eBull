"""Pure-logic tests for the #2902 power-first script.

No DB. The verdict rests on the power formula and on the parser never letting a test-window month, a
duplicate or a gap into the pre-window sample, so those are what is pinned.
"""

from __future__ import annotations

import hashlib
import io
import math
import zipfile
from pathlib import Path

import pytest

from scripts.measure_2902_power import active_returns, expected_months, power, read_section

TITLE = "Equal Weight Returns -- Monthly"


def _archive(tmp_path: Path, rows: list[str]) -> tuple[Path, str]:
    text = "\n".join(["preamble", "", f"  {TITLE}", ",A,B", *rows, "", "  Next Section"])
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("data.csv", text)
    path = tmp_path / "data.zip"
    path.write_bytes(buffer.getvalue())
    return path, hashlib.sha256(buffer.getvalue()).hexdigest()


def _rows(months: list[tuple[int, int]]) -> list[str]:
    return [f"{y}{m:02d},1.0,2.0" for y, m in months]


class TestPower:
    def test_half_power_exactly_at_the_bar(self) -> None:
        years, te, bar = 11.0, 0.05, 3.0
        edge = bar * te / math.sqrt(years)
        assert power(edge, te, years, bar) == pytest.approx(0.5)

    def test_depends_on_edge_only_through_the_information_ratio(self) -> None:
        assert power(0.015, 0.05, 11.0, 3.0) == pytest.approx(power(0.03, 0.10, 11.0, 3.0))

    def test_declared_edge_matches_the_recorded_quintile_row(self) -> None:
        assert power(0.015, 0.0556, 134 / 12, 3.0) == pytest.approx(0.018, abs=0.001)


class TestReadSection:
    def test_keeps_only_the_pre_window_months(self, tmp_path: Path) -> None:
        window = [(2013, 7), (2013, 8)]
        path, digest = _archive(tmp_path, _rows([(1963, 6), *expected_months(), *window]))
        rows = read_section(path, digest, TITLE)
        assert sorted(rows) == expected_months()

    def test_a_gap_refuses(self, tmp_path: Path) -> None:
        months = expected_months()
        path, digest = _archive(tmp_path, _rows(months[:10] + months[11:]))
        with pytest.raises(SystemExit, match="contiguous"):
            read_section(path, digest, TITLE)

    def test_a_duplicate_month_refuses(self, tmp_path: Path) -> None:
        months = expected_months()
        path, digest = _archive(tmp_path, _rows([months[0], *months]))
        with pytest.raises(SystemExit, match="duplicate"):
            read_section(path, digest, TITLE)

    def test_a_non_finite_value_refuses(self, tmp_path: Path) -> None:
        months = expected_months()
        rows = _rows(months)
        rows[5] = rows[5].replace("1.0", "nan")
        path, digest = _archive(tmp_path, rows)
        with pytest.raises(SystemExit, match="non-finite"):
            read_section(path, digest, TITLE)

    def test_a_moved_digest_refuses(self, tmp_path: Path) -> None:
        path, _ = _archive(tmp_path, _rows(expected_months()))
        with pytest.raises(SystemExit, match="digest moved"):
            read_section(path, "0" * 64, TITLE)


def test_control_is_the_count_weighted_universe() -> None:
    returns = {m: {"A": 1.0, "B": 4.0} for m in expected_months()}
    counts = {m: {"A": 3.0, "B": 1.0} for m in expected_months()}
    active = active_returns(returns, counts, "B", ("A", "B"))
    assert active[0] == pytest.approx((4.0 - (1.0 * 3 + 4.0 * 1) / 4) / 100)
