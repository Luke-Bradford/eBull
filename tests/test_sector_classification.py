"""Pure tests for the SIC → GICS-sector → SPDR crosswalk (#1634).

No DB. The full 389-SIC dev population is verified out-of-band (recorded on the
PR); here we pin the panel, every GICS-driven carve-out, fail-closed handling,
the override-before-major-group ordering, and the SPDR/SPDR_SECTORS invariants.
"""

from __future__ import annotations

import pytest

from app.services.sector_classification import (
    _CROSSWALK,
    SPDR_SECTORS,
    resolve_sector_spdr,
)
from app.workers.scheduler import BENCHMARK_SYMBOLS


def _spdr(sic: str) -> str | None:
    out = resolve_sector_spdr(sic)
    return out.spdr_symbol if out is not None else None


class TestPanel:
    @pytest.mark.parametrize(
        ("sic", "spdr"),
        [
            ("3571", "XLK"),  # AAPL — electronic computers
            ("7372", "XLK"),  # MSFT — prepackaged software
            ("6021", "XLF"),  # JPM — national commercial banks
            ("6798", "XLRE"),  # a REIT (Realty Income)
            ("2834", "XLV"),  # PFE — pharmaceutical preparations
            ("1311", "XLE"),  # XOM-like — crude petroleum & natural gas
            ("4911", "XLU"),  # an electric utility
        ],
    )
    def test_panel(self, sic: str, spdr: str) -> None:
        assert _spdr(sic) == spdr


class TestCarveOuts:
    """Every GICS-driven carve-out must beat its enclosing major group."""

    @pytest.mark.parametrize(
        ("sic", "spdr"),
        [
            ("2834", "XLV"),  # pharma out of 28xx chemicals (chemicals→XLB)
            ("3571", "XLK"),  # computers out of 35xx machinery (→XLI)
            ("3674", "XLK"),  # semiconductors out of 36xx (base→XLI)
            ("3634", "XLY"),  # household appliances out of 36xx → cons. disc.
            ("3651", "XLY"),  # consumer audio/video out of 36xx
            ("3690", "XLI"),  # misc electrical machinery stays Industrials
            ("3711", "XLY"),  # autos out of 37xx (aerospace→XLI)
            ("3721", "XLI"),  # aircraft stays Industrials
            ("3812", "XLI"),  # defense electronics out of 38xx (base→XLK)
            ("3826", "XLV"),  # life-sciences tools out of 38xx
            ("3841", "XLV"),  # surgical/medical devices
            ("3873", "XLY"),  # watches out of 38xx
            ("4953", "XLI"),  # refuse/waste out of 49xx utilities (→XLU)
            ("6324", "XLV"),  # managed care out of 63xx insurance (→XLF)
            ("6321", "XLF"),  # accident/health insurance stays Financials
            ("6792", "XLE"),  # oil royalty out of 67xx (→XLF)
            ("6795", "XLB"),  # mineral royalty out of 67xx
            ("6770", "XLF"),  # blank-check/SPAC stays Financials
            ("5045", "XLK"),  # computer wholesale out of 50xx (→XLI)
            ("5122", "XLV"),  # drug wholesale out of 51xx (→XLP)
            ("5172", "XLE"),  # petroleum wholesale out of 51xx
            ("5411", "XLP"),  # grocery retail out of 52-59xx retail (→XLY)
            ("5961", "XLY"),  # catalog retail stays Consumer Discretionary
            ("7372", "XLK"),  # software out of 73xx services (→XLI)
            ("7311", "XLC"),  # advertising out of 73xx
            ("8731", "XLV"),  # commercial biological research out of 87xx (→XLI)
        ],
    )
    def test_carve_out(self, sic: str, spdr: str) -> None:
        assert _spdr(sic) == spdr


class TestFailClosed:
    @pytest.mark.parametrize("sic", [None, "", "  ", "abc", "0", "0000", "9999", "9100"])
    def test_unmappable_is_none(self, sic: str | None) -> None:
        # No SIC, blank, non-numeric, or out-of-mapped-range → None (never guessed).
        assert resolve_sector_spdr(sic) is None

    def test_int_input_accepted(self) -> None:
        assert _spdr("2834") == _spdr(2834) == "XLV"  # type: ignore[arg-type]


class TestInvariants:
    def test_every_spdr_in_benchmark_universe(self) -> None:
        # The 11 sector SPDRs we map to must be the candle-ingested benchmarks.
        assert set(SPDR_SECTORS) <= BENCHMARK_SYMBOLS

    def test_crosswalk_spdrs_are_known(self) -> None:
        for _lo, _hi, spdr in _CROSSWALK:
            assert spdr in SPDR_SECTORS

    def test_ranges_are_well_formed(self) -> None:
        for lo, hi, _spdr_ in _CROSSWALK:
            assert lo <= hi

    def test_resolved_gics_label_matches_spdr(self) -> None:
        out = resolve_sector_spdr("3841")
        assert out is not None
        assert out.gics_sector == SPDR_SECTORS[out.spdr_symbol] == "Health Care"
