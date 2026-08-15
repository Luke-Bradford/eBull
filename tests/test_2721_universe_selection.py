"""#2721 step 3 — universe selection, the termination rule's wiring, and the
write boundary that keeps synthetic name keys out of storage.

Pure-logic tier: the admission QUERY is exercised on the dev DB by
``scripts/verify_2721_survivorship_universe.py`` (the acceptance script); what
lives here are the constructions no fixture DB is needed for.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from app.services.technical_analysis import OHLCVRow

import pytest

from app.services.backtest_run import (
    BACKTEST_UNIVERSE,
    STANDING_REFUSALS,
    _corpus_version_for,
    _terminate_open_positions,
    load_corpus,
)
from app.services.indicator_series import BarSeries
from app.services.position_builder import Position, Window
from app.services.research_corpus_ingest import (
    archive_symbol_candidates,
    vendor_symbol_has_bankruptcy_suffix,
)
from app.services.series_termination import (
    SHUMWAY_HAIRCUT,
    TerminationClass,
    TerminationEvidence,
)
from app.services.strategy_result import CORPUS_VERSION
from app.services.strategy_result_universe import (
    TERMINATION_CENSUS_STRATA,
    ResultUniverseRecord,
    store_termination_census,
)
from app.services.universe_selection import (
    INTRADER_CAPTURE_DATE,
    SURVIVORSHIP_FREE_VENDOR,
    vendor_for,
)


def _series(closes: list[float | None]) -> BarSeries:
    return BarSeries(
        dates=tuple(date(2024, 1, day + 1) for day in range(len(closes))),
        rows=cast(
            "tuple[OHLCVRow, ...]",
            tuple({"open": value, "high": value, "low": value, "close": value} for value in closes),
        ),
    )


def _position(
    *,
    fill_day: int,
    close_source: str | None = None,
    close_day: int | None = None,
    open_reason: str | None = "window_end",
) -> Position:
    closed = close_source is not None
    return Position(
        strategy_id="s-test",
        strategy_version="v-test",
        instrument_id=7,
        entry_signal_id=1,
        entry_signal_bar_date=date(2024, 1, fill_day),
        entry_fill_bar_date=date(2024, 1, fill_day + 1),
        entry_fill_price=Decimal("10"),
        close_source=close_source,  # type: ignore[arg-type]
        close_bar_date=date(2024, 1, close_day) if close_day is not None else None,
        close_price=Decimal("11") if closed and close_source != "ambiguous" else None,
        bars_held=(close_day - fill_day - 1) if close_day is not None else None,
        open_reason=None if closed else open_reason,  # type: ignore[arg-type]
        mark_price=None,
    )


_FAILURE = TerminationEvidence(linked=True, provision="(b)", q_suffix=False)
_UNKNOWN = TerminationEvidence(linked=False, provision=None, q_suffix=False)


class TestTerminateOpenPositions:
    """The precedence table, priced per arm, spec §termination-wiring."""

    def test_a_builder_close_outranks_termination(self) -> None:
        closed = _position(fill_day=1, close_source="signal_pair", close_day=3)
        (out,) = _terminate_open_positions(
            [closed], series=_series([10.0] * 5), evidence=_FAILURE, ambiguity_arm="worst_case"
        )
        assert out is closed

    def test_a_window_end_open_realises_the_haircut_at_the_last_close(self) -> None:
        (out,) = _terminate_open_positions(
            [_position(fill_day=1)],
            series=_series([10.0, 10.0, 10.0, 10.0, 8.0]),
            evidence=_FAILURE,
            ambiguity_arm="worst_case",
        )
        assert out.close_source == "series_termination"
        assert out.close_bar_date == date(2024, 1, 5)
        assert out.close_price == Decimal("8.0") * Decimal(str(1.0 - SHUMWAY_HAIRCUT))
        assert out.bars_held == 3
        assert out.open_reason is None

    @pytest.mark.parametrize("reason", ["series_break", "unresolved_outcome", "close_bar_unfillable"])
    def test_the_other_open_reasons_are_never_touched(self, reason: str) -> None:
        skipped = _position(fill_day=1, open_reason=reason)
        (out,) = _terminate_open_positions(
            [skipped], series=_series([10.0] * 5), evidence=_FAILURE, ambiguity_arm="worst_case"
        )
        assert out is skipped

    def test_no_admissible_close_relabels_rather_than_prices(self) -> None:
        # Every close after the fill is masked away: nothing to realise against.
        (out,) = _terminate_open_positions(
            [_position(fill_day=1)],
            series=_series([10.0, None, None, None, None]),
            evidence=_FAILURE,
            ambiguity_arm="worst_case",
        )
        assert out.close_source is None
        assert out.open_reason == "termination_price_unlocatable"

    def test_a_two_armed_class_prices_the_arms_apart_and_a_one_armed_class_does_not(self) -> None:
        series = _series([10.0] * 5)
        (best,) = _terminate_open_positions(
            [_position(fill_day=1)], series=series, evidence=_UNKNOWN, ambiguity_arm="best_case"
        )
        (worst,) = _terminate_open_positions(
            [_position(fill_day=1)], series=series, evidence=_UNKNOWN, ambiguity_arm="worst_case"
        )
        assert best.close_price == Decimal("10.0")
        assert worst.close_price == Decimal("10.0") * Decimal(str(1.0 - SHUMWAY_HAIRCUT))
        (b2,) = _terminate_open_positions(
            [_position(fill_day=1)], series=series, evidence=_FAILURE, ambiguity_arm="best_case"
        )
        (w2,) = _terminate_open_positions(
            [_position(fill_day=1)], series=series, evidence=_FAILURE, ambiguity_arm="worst_case"
        )
        assert b2.close_price == w2.close_price

    def test_every_open_position_terminates_not_just_the_first(self) -> None:
        out = _terminate_open_positions(
            [_position(fill_day=1), _position(fill_day=2)],
            series=_series([10.0] * 5),
            evidence=_FAILURE,
            ambiguity_arm="worst_case",
        )
        assert [p.close_source for p in out] == ["series_termination", "series_termination"]


class _RefusingConn:
    """A connection stub that fails the test if the code under test reads it —
    proving the refusal fires before any database work."""

    def execute(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover - the point is it never runs
        raise AssertionError("the refusal must fire before any database read")


class TestWindowBounds:
    def test_a_survivorship_free_window_past_capture_refuses_before_any_read(self) -> None:
        with pytest.raises(ValueError, match="frozen corpus window"):
            load_corpus(
                _RefusingConn(),  # type: ignore[arg-type]
                universe_basis="survivorship_free",
                evaluation_window=Window(start=date(2020, 1, 1), end=INTRADER_CAPTURE_DATE.replace(year=2025)),
            )

    def test_the_survivor_default_is_unchanged(self) -> None:
        assert BACKTEST_UNIVERSE == "survivor_only"
        assert _corpus_version_for("survivor_only") == CORPUS_VERSION
        assert _corpus_version_for("survivorship_free") == f"{SURVIVORSHIP_FREE_VENDOR}@2024-09-27"


class TestWriteBoundary:
    """No synthetic name key reaches storage; the census vocabulary is closed."""

    def test_the_universe_record_refuses_a_negative_or_zero_id_in_every_field(self) -> None:
        for field in ("evaluated_instrument_ids", "validated_universe_ids", "evaluated_series_ids"):
            kwargs: dict[str, Any] = {
                "evaluated_instrument_ids": frozenset({1}),
                "validated_universe_ids": frozenset({1}),
                "evaluated_series_ids": frozenset({2}),
            }
            kwargs[field] = frozenset({-42})
            with pytest.raises(ValueError, match="positive"):
                ResultUniverseRecord(universe_rule_version="v", **kwargs)

    def test_the_census_vocabulary_is_closed_and_refused_before_any_write(self) -> None:
        with pytest.raises(ValueError, match="closed vocabulary"):
            store_termination_census(_RefusingConn(), result_id=1, census={"free_text": 1})  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="negative"):
            store_termination_census(
                _RefusingConn(),  # type: ignore[arg-type]
                result_id=1,
                census={"terminated_exchange_failure": -1},
            )

    def test_the_stratum_vocabulary_mirrors_the_termination_classes(self) -> None:
        """A class added to ``series_termination`` without a migration must fail
        HERE, not silently produce an unstorable census."""
        assert {f"terminated_{member.value}" for member in TerminationClass} <= TERMINATION_CENSUS_STRATA

    def test_standing_refusals_no_longer_carry_the_universe_member(self) -> None:
        assert "universe_basis_not_survivorship_free" not in STANDING_REFUSALS


class TestQSuffixRule:
    """One rule, two directions — the strip and the read must agree."""

    @pytest.mark.parametrize(
        ("symbol", "expected"),
        [("ABCQ", True), ("Q", False), ("abcq", True), ("ABC", False), ("", False)],
    )
    def test_the_read_direction(self, symbol: str, expected: bool) -> None:
        assert vendor_symbol_has_bankruptcy_suffix(symbol) is expected

    def test_the_read_agrees_with_the_strip(self) -> None:
        # The candidate ladder appends the stripped spelling exactly when the
        # read direction says the suffix is present.
        for symbol in ("ABCQ", "NHIQ", "XQ", "Q", "ABC"):
            strips = symbol[:-1] in archive_symbol_candidates(symbol)
            assert vendor_symbol_has_bankruptcy_suffix(symbol) is strips

    def test_vendor_for_is_total_and_pinned(self) -> None:
        assert vendor_for("survivor_only") == "paperswithbacktest/Stocks-Daily-Price"
        assert vendor_for("survivorship_free") == "icyDenev/Intrader"
        with pytest.raises(ValueError):
            vendor_for("nonsense")  # type: ignore[arg-type]
