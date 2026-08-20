"""``spy_chain_v1`` — the backtest's chained SPY benchmark (#2437 walk-forward prereq).

Covers the PURE half (``_chain_closes``) plus the frozen-constants pin. The
fetch half (``load_research``) is two straight reads whose invariants
(single-series pin, adjustment-basis match) are exercised against the dev DB by
the full-population dry run recorded in the proposal doc, not by a DB-tier test
— per the lean-test rule.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from app.services.market_regime_provider import (
    CHAIN_FALLBACK,
    CHAIN_FALLBACK_BASIS,
    CHAIN_MAX_SEAM_GAP_DAYS,
    CHAIN_PRIMARY,
    CHAIN_PRIMARY_BASIS,
    CHAIN_SEAM,
    RULE_SET_VERSION,
    BenchmarkUnavailableError,
    MarketRegimeProvider,
    _chain_closes,
)

SEAM = date(2022, 5, 10)


def _bars(*days_closes: tuple[date, float]) -> list[tuple[date, float]]:
    return list(days_closes)


class TestTheFrozenConstantsCannotDriftSilently:
    """The declared version string and the constants it describes are one
    frozen block — an implementation edit that moves a constant without bumping
    the string is exactly the silent redefinition the spec forbids."""

    def test_the_frozen_block(self) -> None:
        assert RULE_SET_VERSION == "benchmark-source-v1:live=price_daily_spy;research=spy_chain_v1"
        assert CHAIN_PRIMARY == ("etoro/etoro-comparators-2026-07-08-v1", "SPY")
        assert CHAIN_PRIMARY_BASIS == "split_adjusted"
        assert CHAIN_FALLBACK == ("icyDenev/Intrader", "SPY")
        assert CHAIN_FALLBACK_BASIS == "unadjusted"
        assert CHAIN_SEAM == SEAM
        assert CHAIN_MAX_SEAM_GAP_DAYS == 7


class TestTheChainHasOneSeam:
    def test_fallback_contributes_strictly_before_the_seam(self) -> None:
        primary = _bars((SEAM, 400.0), (SEAM + timedelta(days=1), 401.0))
        fallback = _bars(
            (SEAM - timedelta(days=3), 300.0),
            # A fallback bar ON the seam and one after it — both must be dropped,
            # not merged: the primary owns the seam onward.
            (SEAM, 999.0),
            (SEAM + timedelta(days=1), 999.0),
        )
        chained = _chain_closes(primary, fallback, seam=SEAM)
        assert chained == [
            (SEAM - timedelta(days=3), 300.0),
            (SEAM, 400.0),
            (SEAM + timedelta(days=1), 401.0),
        ]

    def test_a_primary_hole_after_the_seam_stays_a_hole(self) -> None:
        """⚠ The central invariant: no per-date interleaving. A vendor hole must
        NOT flip the chain back to the fallback mid-window."""
        hole = SEAM + timedelta(days=1)
        primary = _bars((SEAM, 400.0), (SEAM + timedelta(days=2), 402.0))  # no bar on ``hole``
        fallback = _bars((SEAM - timedelta(days=1), 300.0), (hole, 999.0))
        chained = _chain_closes(primary, fallback, seam=SEAM)
        assert hole not in [day for day, _ in chained]

    def test_primary_bars_before_the_seam_are_dropped(self) -> None:
        primary = _bars((SEAM - timedelta(days=1), 999.0), (SEAM, 400.0))
        fallback = _bars((SEAM - timedelta(days=1), 300.0))
        chained = _chain_closes(primary, fallback, seam=SEAM)
        assert chained == [(SEAM - timedelta(days=1), 300.0), (SEAM, 400.0)]


def test_a_sealed_research_regime_query_never_reads_beyond_its_boundary() -> None:
    boundary = date(2021, 12, 31)
    fallback: list[tuple[object, ...]] = [
        (boundary - timedelta(days=399 - offset), 100.0 + offset * 0.1) for offset in range(400)
    ]

    class _Rows:
        def __init__(self, rows: list[tuple[object, ...]]) -> None:
            self.rows = rows

        def fetchall(self) -> list[tuple[object, ...]]:
            return self.rows

    class _Connection:
        def __init__(self) -> None:
            self.price_queries: list[tuple[str, tuple[object, ...]]] = []

        def execute(self, query: str, params: tuple[object, ...]) -> _Rows:
            if "SELECT series_id, adjustment_basis" in query:
                if params == CHAIN_PRIMARY:
                    return _Rows([(1, CHAIN_PRIMARY_BASIS)])
                if params == CHAIN_FALLBACK:
                    return _Rows([(2, CHAIN_FALLBACK_BASIS)])
                raise AssertionError(f"unexpected series pin {params!r}")
            self.price_queries.append((query, params))
            return _Rows([] if params[0] == 1 else fallback)

    conn = _Connection()
    provider = MarketRegimeProvider.load_research(conn, through_date=boundary)  # type: ignore[arg-type]

    assert len(conn.price_queries) == 2
    assert all("bar_date <= %s::date" in query for query, _ in conn.price_queries)
    assert all(params[1:] == (boundary, boundary) for _, params in conn.price_queries)
    assert provider.for_dates((boundary,)).values[0] is not None


class TestTheChainRefusesRatherThanDegrades:
    def test_no_primary_bar_on_the_seam(self) -> None:
        primary = _bars((SEAM + timedelta(days=1), 400.0))
        fallback = _bars((SEAM - timedelta(days=1), 300.0))
        with pytest.raises(BenchmarkUnavailableError, match="no bar on the frozen seam"):
            _chain_closes(primary, fallback, seam=SEAM)

    def test_empty_fallback(self) -> None:
        primary = _bars((SEAM, 400.0))
        with pytest.raises(BenchmarkUnavailableError, match="no bars before the seam"):
            _chain_closes(primary, [], seam=SEAM)

    def test_fallback_short_of_the_seam_window(self) -> None:
        primary = _bars((SEAM, 400.0))
        fallback = _bars((SEAM - timedelta(days=CHAIN_MAX_SEAM_GAP_DAYS + 1), 300.0))
        with pytest.raises(BenchmarkUnavailableError, match="eroded"):
            _chain_closes(primary, fallback, seam=SEAM)

    def test_a_holiday_week_gap_is_accepted(self) -> None:
        primary = _bars((SEAM, 400.0))
        fallback = _bars((SEAM - timedelta(days=CHAIN_MAX_SEAM_GAP_DAYS), 300.0))
        assert len(_chain_closes(primary, fallback, seam=SEAM)) == 2

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), 0.0, -5.0])
    def test_a_non_positive_or_non_finite_close_is_refused(self, bad: float) -> None:
        primary = _bars((SEAM, 400.0), (SEAM + timedelta(days=1), bad))
        fallback = _bars((SEAM - timedelta(days=1), 300.0))
        with pytest.raises(BenchmarkUnavailableError, match="not a positive finite price"):
            _chain_closes(primary, fallback, seam=SEAM)

    def test_a_duplicate_date_is_refused(self) -> None:
        primary = _bars((SEAM, 400.0), (SEAM, 400.5))
        fallback = _bars((SEAM - timedelta(days=1), 300.0))
        with pytest.raises(BenchmarkUnavailableError, match="strictly increasing"):
            _chain_closes(primary, fallback, seam=SEAM)
