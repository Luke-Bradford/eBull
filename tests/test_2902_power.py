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

from scripts.measure_2902_power import (
    QUINTILES,
    SMALL_CAP_ROW,
    TERTILES,
    active_returns,
    expected_months,
    power,
    read_section,
)

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
    return [f"{y}{m:02d},1.5,2.5" for y, m in months]


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
        rows[5] = rows[5].replace("1.5", "nan")
        path, digest = _archive(tmp_path, rows)
        with pytest.raises(SystemExit, match="non-finite"):
            read_section(path, digest, TITLE)

    def test_a_moved_digest_refuses(self, tmp_path: Path) -> None:
        path, _ = _archive(tmp_path, _rows(expected_months()))
        with pytest.raises(SystemExit, match="digest moved"):
            read_section(path, "0" * 64, TITLE)


def _sections(columns: tuple[str, ...]) -> tuple[dict[tuple[int, int], dict[str, float]], ...]:
    returns = {m: {c: 1.5 + i for i, c in enumerate(columns)} for m in expected_months()}
    counts = {m: {c: 2.0 for c in columns} for m in expected_months()}
    return returns, counts


class TestActiveReturns:
    def test_control_is_the_count_weighted_universe(self) -> None:
        returns = {m: {"A": 1.5, "B": 4.0} for m in expected_months()}
        counts = {m: {"A": 3.0, "B": 1.0} for m in expected_months()}
        active = active_returns(returns, counts, "B", ("A", "B"))
        assert active[0] == pytest.approx((4.0 - (1.5 * 3 + 4.0 * 1) / 4) / 100)

    def test_small_cap_row_is_a_contiguous_run_of_the_5x5_header(self) -> None:
        returns, counts = _sections((*SMALL_CAP_ROW, "ME2 BM1"))
        assert len(active_returns(returns, counts, SMALL_CAP_ROW[-1], SMALL_CAP_ROW)) == len(expected_months())

    @pytest.mark.parametrize(
        "universe",
        [
            ("SMALL LoBM", "ME1 BM3", "ME1 BM3", "ME1 BM4", "SMALL HiBM"),  # wrong-but-existing key
            ("SMALL LoBM", "ME1 BM2", "ME1 BM3", "ME1 BM4", "ME2 BM1"),  # crosses into the next size row
            ("SMALL HiBM", "SMALL LoBM"),  # out of header order
        ],
    )
    def test_a_non_contiguous_universe_refuses(self, universe: tuple[str, ...]) -> None:
        returns, counts = _sections((*SMALL_CAP_ROW, "ME2 BM1"))
        with pytest.raises(SystemExit, match="contiguous"):
            active_returns(returns, counts, universe[-1], universe)

    def test_an_arm_that_is_not_the_top_member_refuses(self) -> None:
        returns, counts = _sections(QUINTILES)
        with pytest.raises(SystemExit, match="contiguous"):
            active_returns(returns, counts, "Lo 20", QUINTILES)

    def test_a_count_section_read_as_returns_refuses(self) -> None:
        _, counts = _sections(TERTILES)
        with pytest.raises(SystemExit, match="count section"):
            active_returns(counts, counts, TERTILES[-1], TERTILES)


def test_universes_match_the_french_headers() -> None:
    """Header lines copied from the pinned 202608 archives; the universes must be runs of them."""
    be_me = "<= 0,Lo 30,Med 40,Hi 30,Lo 20,Qnt 2,Qnt 3,Qnt 4,Hi 20,Lo 10".split(",")
    size_bm = "SMALL LoBM,ME1 BM2,ME1 BM3,ME1 BM4,SMALL HiBM,ME2 BM1".split(",")
    for universe, header in ((QUINTILES, be_me), (TERTILES, be_me), (SMALL_CAP_ROW, size_bm)):
        start = header.index(universe[0])
        assert tuple(header[start : start + len(universe)]) == universe
