"""The split-corrected ratio basis — #2834 §7 item 2, slice C.

Covers ``app/services/research_split_corrected_reader.py``: the pairing of an
as-traded ``BarSeries`` with its corrected counterpart, the per-series method
table, and the one property the whole design rests on — a corrected RATIO is
anchor-invariant while a corrected LEVEL is not.

Pure tier: no database, no fixtures, no IO. The DB-shaped half of the module
(``load_split_factors`` / ``load_ratio_basis``) is exercised by
``scripts/verify_2240_s2_cross_sectional.py --census`` against the real corpus,
per the repo's lean-test rule.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.indicator_series import BarSeries
from app.services.research_split_adjustment import StampsUnavailable, UncorrectableStamp
from app.services.research_split_corrected_reader import (
    CorrectedSeries,
    ratio_basis_method_for,
    ratio_basis_series,
)
from app.services.technical_analysis import OHLCVRow

MARKER = "vendor_supplied"


def _bars(closes: Sequence[float], *, volumes: Sequence[int | None] | None = None) -> BarSeries:
    rows: list[OHLCVRow] = [
        {
            "open": Decimal(str(close)),
            "high": Decimal(str(close + 1)),
            "low": Decimal(str(close - 1)),
            "close": Decimal(str(close)),
            "volume": None if volumes is None else volumes[i],
        }
        for i, close in enumerate(closes)
    ]
    return BarSeries(dates=tuple(date(2020, 1, 1) + timedelta(days=i) for i in range(len(closes))), rows=tuple(rows))


def _paired(closes: Sequence[float], factors: Sequence[str | None], **kwargs: object) -> CorrectedSeries:
    series = _bars(closes, **kwargs)  # type: ignore[arg-type]
    return ratio_basis_series(
        series,
        series_id=7,
        factor_dates=series.dates,
        factors=tuple(None if f is None else Decimal(f) for f in factors),
        stamps_marker=MARKER,
    )


class TestTheCorrection:
    def test_the_aapl_shape(self) -> None:
        """The stamp sits on the bar that FIRST PRINTS the post-split level.

        AAPL's 4:1 settled 2020-08-31: close runs 499.23 -> 129.04, and it is
        the PRE-split bar that needs the 4. A correction that divided the
        stamped bar by its own factor would adjust it twice — the shape this
        asserts is the one ``split_scales`` documents from those bars.
        """
        paired = _paired([499.23, 129.04], ["1", "4"])
        assert [row["close"] for row in paired.ratio_basis.rows] == [
            Decimal("499.23") / 4,
            Decimal("129.04"),
        ]
        # The last bar always scales to 1 — there is no later event.
        assert paired.ratio_basis.rows[-1]["close"] == paired.as_traded.rows[-1]["close"]

    def test_a_reverse_split_scales_the_other_way(self) -> None:
        """1-for-10: the price multiplies, so the stamped factor is below one.

        This is the direction that matters for the ``MIN_CLOSE`` floor: a name
        that traded at $0.20 reads $2.00 on the corrected basis.
        """
        paired = _paired([0.20, 2.00], ["1", "0.1"])
        assert paired.ratio_basis.rows[0]["close"] == Decimal("2.00")
        assert paired.as_traded.rows[0]["close"] == Decimal("0.20")

    def test_moved_bars_counts_a_reverse_split(self) -> None:
        """``!= 1``, not ``> 1``. A magnitude test would report half a corpus."""
        assert _paired([0.20, 2.00], ["1", "0.1"]).moved_bars == 1
        assert _paired([10.0, 10.0], ["1", "1"]).moved_bars == 0

    def test_a_series_with_no_event_is_byte_identical(self) -> None:
        paired = _paired([10.0, 11.0, 12.0], ["1", "1", "1"])
        assert [r["close"] for r in paired.ratio_basis.rows] == [r["close"] for r in paired.as_traded.rows]
        assert paired.moved_bars == 0


class TestAnchorInvariance:
    """The property the whole design rests on — see the module docstring.

    ``ratio(a)/ratio(b)`` must not depend on where the series ends, because the
    anchor's factors cancel. ``level`` must depend on it, because a level is
    denominated in the terminal unit. If the first ever failed, the corrected
    basis would carry look-ahead into every backtest; if the second ever held,
    the levels would not be on one unit at all.
    """

    CLOSES = (100.0, 100.0, 25.0, 25.0, 5.0)
    #: A 4:1 at index 2 and a 5:1 at index 4.
    FACTORS = ("1", "1", "4", "1", "5")

    def test_a_ratio_is_the_same_when_the_series_is_extended(self) -> None:
        short = _paired(self.CLOSES[:4], self.FACTORS[:4])
        full = _paired(self.CLOSES, self.FACTORS)

        def ratio(paired: CorrectedSeries, a: int, b: int) -> Decimal:
            return paired.ratio_basis.rows[a]["close"] / paired.ratio_basis.rows[b]["close"]  # type: ignore[operator]

        # Every pair inside the shorter window agrees, although the extra 5:1
        # moved every level in `full`.
        for a in range(4):
            for b in range(4):
                assert ratio(short, a, b) == ratio(full, a, b)

    def test_a_level_is_not(self) -> None:
        short = _paired(self.CLOSES[:4], self.FACTORS[:4])
        full = _paired(self.CLOSES, self.FACTORS)
        assert short.ratio_basis.rows[0]["close"] != full.ratio_basis.rows[0]["close"]
        # ...and the as-traded level is invariant, which is why the floor reads it.
        assert short.as_traded.rows[0]["close"] == full.as_traded.rows[0]["close"]

    def test_the_interval_is_half_open_and_includes_the_LATER_endpoints_stamp(self) -> None:
        """``(b, a]`` — ``a``'s own stamp is IN, ``b``'s is OUT.

        ⚠ This is the test that discriminates the identity from the "strictly
        between" reading an earlier draft of the docstring carried (Codex
        ckpt-1). It uses endpoints that LAND ON stamped bars, so the two
        readings give different answers; a window whose endpoints sit between
        events passes either way and proves nothing.

        Fixture: a 4:1 stamped at index 2 and a 5:1 stamped at index 4.
        """
        full = _paired(self.CLOSES, self.FACTORS)

        def quotient(rows: Sequence[object], a: int, b: int) -> Decimal:
            return rows[a]["close"] / rows[b]["close"]  # type: ignore[index,operator]

        # b = 2 (stamped 4:1), a = 4 (stamped 5:1). The half-open interval
        # (2, 4] contains ONLY index 4's 5:1 — index 2's own 4:1 is excluded
        # because it is the LOWER endpoint, and index 4's is included because
        # it is the upper one. "Strictly between" would give 1 (no event), and
        # "closed both ends" would give 20.
        assert quotient(full.ratio_basis.rows, 4, 2) == quotient(full.as_traded.rows, 4, 2) * 5

        # And the mirrored pair is the RECIPROCAL, not the same multiplier.
        assert quotient(full.ratio_basis.rows, 2, 4) == quotient(full.as_traded.rows, 2, 4) / 5

    def test_a_ratio_of_a_bar_with_itself_is_one(self) -> None:
        full = _paired(self.CLOSES, self.FACTORS)
        for i in range(len(self.CLOSES)):
            assert full.ratio_basis.rows[i]["close"] / full.ratio_basis.rows[i]["close"] == 1  # type: ignore[operator]


class TestVolume:
    def test_the_row_keeps_the_as_traded_count_and_the_pair_carries_the_corrected_one(self) -> None:
        """``OHLCVRow.volume`` is ``int | None`` and a restated count is fractional."""
        paired = _paired([499.23, 129.04], ["1", "4"], volumes=[46_907_479, 223_505_733])
        assert paired.ratio_basis.rows[0]["volume"] == 46_907_479
        # Multiplied, not divided — the pre-split bar is restated in NEW shares.
        assert paired.ratio_basis_shares[0] == Decimal(46_907_479) * 4
        assert paired.ratio_basis_shares[1] == Decimal(223_505_733)

    def test_a_reverse_split_gives_a_fractional_count(self) -> None:
        paired = _paired([0.20, 2.00], ["1", "0.1"], volumes=[5, 50])
        assert paired.ratio_basis_shares[0] == Decimal("0.5")

    def test_zero_volume_survives_as_a_decimal_zero(self) -> None:
        """A falsy test here would return a bare ``int`` 0 — prevention log, 2026-09-21."""
        paired = _paired([10.0, 10.0], ["1", "1"], volumes=[0, 3])
        assert paired.ratio_basis_shares[0] == Decimal(0)
        assert isinstance(paired.ratio_basis_shares[0], Decimal)

    def test_an_absent_volume_stays_absent(self) -> None:
        paired = _paired([10.0, 10.0], ["1", "1"], volumes=[None, 3])
        assert paired.ratio_basis_shares[0] is None


class TestTheMethodTable:
    @pytest.mark.parametrize(
        ("basis", "marker", "expected"),
        [
            ("unadjusted", "vendor_supplied", "split_corrected"),
            ("split_adjusted", "absent", "vendor_already_adjusted"),
        ],
    )
    def test_the_two_shapes_the_corpus_has(self, basis: str, marker: str, expected: str) -> None:
        assert ratio_basis_method_for(basis, marker) == expected

    @pytest.mark.parametrize(
        ("basis", "marker"),
        [
            # As-traded bars with UNKNOWN corporate actions: no ratio basis
            # exists, and both defaults are wrong.
            ("unadjusted", "absent"),
            # Already adjusted AND stamped: correcting would adjust twice.
            ("split_adjusted", "vendor_supplied"),
            (None, None),
            ("unadjusted", None),
            ("something_new", "vendor_supplied"),
        ],
    )
    def test_every_other_shape_refuses(self, basis: str | None, marker: str | None) -> None:
        with pytest.raises(StampsUnavailable):
            ratio_basis_method_for(basis, marker)


class TestItRefusesRatherThanGuesses:
    def test_an_absent_marker_is_refused(self) -> None:
        series = _bars([10.0, 10.0])
        with pytest.raises(StampsUnavailable):
            ratio_basis_series(
                series,
                series_id=7,
                factor_dates=series.dates,
                factors=(Decimal(1), Decimal(1)),
                stamps_marker="absent",
            )

    def test_a_factor_read_that_does_not_align_is_refused(self) -> None:
        """``split_scales`` obligation 2: a SLICE silently re-anchors the result."""
        series = _bars([10.0, 11.0, 12.0])
        with pytest.raises(ValueError, match="re-anchors"):
            ratio_basis_series(
                series,
                series_id=7,
                factor_dates=series.dates[:2],
                factors=(Decimal(1), Decimal(1)),
                stamps_marker=MARKER,
            )

    def test_reordered_factor_dates_are_refused(self) -> None:
        """Obligation 1: reversed factors produce a well-formed, wrong answer."""
        series = _bars([10.0, 11.0, 12.0])
        with pytest.raises(ValueError, match="re-anchors"):
            ratio_basis_series(
                series,
                series_id=7,
                factor_dates=tuple(reversed(series.dates)),
                factors=(Decimal(1), Decimal(1), Decimal(1)),
                stamps_marker=MARKER,
            )

    def test_a_missing_factor_inside_a_correctable_series_is_refused(self) -> None:
        with pytest.raises(UncorrectableStamp):
            _paired([10.0, 11.0], ["1", None])

    def test_a_non_positive_factor_is_refused(self) -> None:
        with pytest.raises(UncorrectableStamp):
            _paired([10.0, 11.0], ["1", "0"])


class TestThePairIsValidated:
    def test_mismatched_dates_are_refused(self) -> None:
        with pytest.raises(ValueError, match="not the same bars"):
            CorrectedSeries(
                series_id=1,
                as_traded=_bars([10.0, 11.0]),
                ratio_basis=_bars([10.0, 11.0, 12.0]),
                ratio_basis_shares=(None, None, None),
                moved_bars=0,
            )

    def test_misaligned_share_counts_are_refused(self) -> None:
        with pytest.raises(ValueError, match="do not align"):
            CorrectedSeries(
                series_id=1,
                as_traded=_bars([10.0, 11.0]),
                ratio_basis=_bars([10.0, 11.0]),
                ratio_basis_shares=(None,),
                moved_bars=0,
            )

    def test_an_impossible_moved_count_is_refused(self) -> None:
        with pytest.raises(ValueError, match="moved_bars"):
            CorrectedSeries(
                series_id=1,
                as_traded=_bars([10.0, 11.0]),
                ratio_basis=_bars([10.0, 11.0]),
                ratio_basis_shares=(None, None),
                moved_bars=3,
            )
