"""Unit tests for daily_candle_refresh T3 bootstrap logic.

Verifies that the candle refresh includes a capped batch of T3
instruments with fundamentals data alongside the full T1/T2 set.

Fix for #253 — T3 instruments were excluded from candle refresh,
creating a bootstrap deadlock where T3 had no price data and could
not score high enough to promote.

No live database or network calls — all dependencies are mocked.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.workers.scheduler import _T3_BOOTSTRAP_BATCH_SIZE, daily_candle_refresh


def _make_mock_conn(
    tier12_rows: list[tuple[int, str]],
    t3_rows: list[tuple[int, str]],
    held_rows: list[tuple[int, str]] | None = None,
    benchmark_rows: list[tuple[int, str]] | None = None,
) -> MagicMock:
    """Mock connection that returns held_rows / tier12_rows / benchmark_rows /
    t3_rows in the order the handler executes them
    (held → tier12 → benchmark → t3)."""
    conn = MagicMock()
    result_held = MagicMock()
    result_held.fetchall.return_value = held_rows or []
    result_12 = MagicMock()
    result_12.fetchall.return_value = tier12_rows
    result_bm = MagicMock()
    result_bm.fetchall.return_value = benchmark_rows or []
    result_t3 = MagicMock()
    result_t3.fetchall.return_value = t3_rows
    conn.execute.side_effect = [result_held, result_12, result_bm, result_t3]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    return conn


_PATCHES = {
    "creds": "app.workers.scheduler._load_etoro_credentials",
    "tracked": "app.workers.scheduler._tracked_job",
    "provider_cls": "app.workers.scheduler.EtoroMarketDataProvider",
    "connect": "app.workers.scheduler.psycopg.connect",
    "refresh": "app.workers.scheduler.refresh_market_data",
}


class TestDailyCandleRefreshT3Bootstrap:
    """Verify T3 bootstrap instruments are included in candle refresh."""

    def _run(
        self,
        tier12_rows: list[tuple[int, str]],
        t3_rows: list[tuple[int, str]],
        held_rows: list[tuple[int, str]] | None = None,
    ) -> MagicMock:
        """Run daily_candle_refresh with mocked dependencies.

        Returns the mock for refresh_market_data so callers can inspect
        what instruments were passed.
        """
        mock_conn = _make_mock_conn(tier12_rows, t3_rows, held_rows)
        mock_provider = MagicMock()
        mock_provider.__enter__ = MagicMock(return_value=mock_provider)
        mock_provider.__exit__ = MagicMock(return_value=False)

        mock_tracker = MagicMock()
        mock_tracker.__enter__ = MagicMock(return_value=mock_tracker)
        mock_tracker.__exit__ = MagicMock(return_value=False)

        mock_summary = MagicMock()
        mock_summary.candle_rows_upserted = 10
        mock_summary.instruments_refreshed = len(tier12_rows) + len(t3_rows)
        mock_summary.features_computed = 5
        mock_summary.quotes_updated = 0
        mock_summary.quotes_skipped = 0
        mock_summary.spread_flags_set = 0
        # #1293 — must be real ints; the disambiguation branch compares them
        # (`> 0`), and a bare MagicMock attribute raises on comparison.
        mock_summary.candles_failed = 0
        mock_summary.candles_skipped = 0

        with (
            patch(_PATCHES["creds"], return_value=("key", "ukey")),
            patch(_PATCHES["tracked"], return_value=mock_tracker),
            patch(_PATCHES["provider_cls"], return_value=mock_provider),
            patch(_PATCHES["connect"], return_value=mock_conn),
            patch(_PATCHES["refresh"], return_value=mock_summary) as mock_refresh,
        ):
            daily_candle_refresh()

        return mock_refresh

    def test_t3_instruments_with_fundamentals_included(self) -> None:
        """T3 instruments with fundamentals data are passed to refresh."""
        tier12 = [(1, "AAPL"), (2, "MSFT")]
        t3_bootstrap = [(100, "XYZ"), (101, "ABC")]

        mock_refresh = self._run(tier12, t3_bootstrap)

        mock_refresh.assert_called_once()
        instruments = mock_refresh.call_args[0][2]
        assert instruments == [(1, "AAPL"), (2, "MSFT"), (100, "XYZ"), (101, "ABC")]

    def test_skip_quotes_true(self) -> None:
        """Candle refresh must pass skip_quotes=True per quote ownership rule."""
        mock_refresh = self._run([(1, "AAPL")], [])
        assert mock_refresh.call_args[1]["skip_quotes"] is True

    def test_empty_t3_batch_still_refreshes_tier12(self) -> None:
        """When no T3 instruments qualify, only T1/T2 are refreshed."""
        tier12 = [(1, "AAPL")]
        mock_refresh = self._run(tier12, [])

        instruments = mock_refresh.call_args[0][2]
        assert instruments == [(1, "AAPL")]

    def test_no_instruments_skips_refresh(self) -> None:
        """When both queries return empty, refresh is not called."""
        mock_refresh = self._run([], [])
        mock_refresh.assert_not_called()

    def test_held_positions_included(self) -> None:
        """Held positions must be refreshed regardless of coverage tier."""
        held = [(999, "LEGACY_HOLD")]
        tier12 = [(1, "AAPL")]
        t3_bootstrap = [(100, "XYZ")]
        mock_refresh = self._run(tier12, t3_bootstrap, held_rows=held)

        instruments = mock_refresh.call_args[0][2]
        assert (999, "LEGACY_HOLD") in instruments
        # Dedupe: ordering puts held first, T1/T2 next, T3 last.
        assert instruments[0] == (999, "LEGACY_HOLD")

    def test_duplicate_instrument_across_scopes_is_deduped(self) -> None:
        """An instrument that's both held and T1/T2 must not be fetched twice."""
        held = [(1, "AAPL")]
        tier12 = [(1, "AAPL"), (2, "MSFT")]
        mock_refresh = self._run(tier12, [], held_rows=held)

        instruments = mock_refresh.call_args[0][2]
        assert instruments == [(1, "AAPL"), (2, "MSFT")]

    def test_t3_query_uses_limit_param(self) -> None:
        """Verify the T3 query passes _T3_BOOTSTRAP_BATCH_SIZE as limit."""
        mock_conn = _make_mock_conn([(1, "AAPL")], [(100, "XYZ")])
        mock_provider = MagicMock()
        mock_provider.__enter__ = MagicMock(return_value=mock_provider)
        mock_provider.__exit__ = MagicMock(return_value=False)
        mock_tracker = MagicMock()
        mock_tracker.__enter__ = MagicMock(return_value=mock_tracker)
        mock_tracker.__exit__ = MagicMock(return_value=False)
        mock_summary = MagicMock()
        mock_summary.candle_rows_upserted = 1
        mock_summary.candles_failed = 0
        mock_summary.candles_skipped = 0

        with (
            patch(_PATCHES["creds"], return_value=("key", "ukey")),
            patch(_PATCHES["tracked"], return_value=mock_tracker),
            patch(_PATCHES["provider_cls"], return_value=mock_provider),
            patch(_PATCHES["connect"], return_value=mock_conn),
            patch(_PATCHES["refresh"], return_value=mock_summary),
        ):
            daily_candle_refresh()

        # Fourth execute call is the T3 query with limit + benchmark_symbols
        # (1st=held, 2nd=tier12, 3rd=benchmark, 4th=T3 bootstrap).
        t3_call = mock_conn.execute.call_args_list[3]
        sql_text = t3_call[0][0]
        params = t3_call[0][1]
        assert "LIMIT" in sql_text
        from app.workers.scheduler import BENCHMARK_SYMBOLS

        assert params == {"limit": _T3_BOOTSTRAP_BATCH_SIZE, "benchmark_symbols": sorted(BENCHMARK_SYMBOLS)}

    def test_bootstrap_batch_size_is_200(self) -> None:
        """Sanity check the constant value."""
        assert _T3_BOOTSTRAP_BATCH_SIZE == 200

    def test_daily_candle_refresh_includes_benchmark_before_t3(self) -> None:
        """Benchmark instruments appear in the refresh list before T3 rows."""
        benchmark = [(3000, "SPY")]
        t3 = [(900, "AAA")]
        mock_conn = _make_mock_conn([], t3, held_rows=[], benchmark_rows=benchmark)
        mock_provider = MagicMock()
        mock_provider.__enter__ = MagicMock(return_value=mock_provider)
        mock_provider.__exit__ = MagicMock(return_value=False)
        mock_tracker = MagicMock()
        mock_tracker.__enter__ = MagicMock(return_value=mock_tracker)
        mock_tracker.__exit__ = MagicMock(return_value=False)
        mock_summary = MagicMock()
        mock_summary.candle_rows_upserted = 1
        mock_summary.instruments_refreshed = 2
        mock_summary.features_computed = 0
        mock_summary.quotes_updated = 0
        mock_summary.quotes_skipped = 0
        mock_summary.spread_flags_set = 0
        mock_summary.candles_failed = 0
        mock_summary.candles_skipped = 0

        with (
            patch(_PATCHES["creds"], return_value=("key", "ukey")),
            patch(_PATCHES["tracked"], return_value=mock_tracker),
            patch(_PATCHES["provider_cls"], return_value=mock_provider),
            patch(_PATCHES["connect"], return_value=mock_conn),
            patch(_PATCHES["refresh"], return_value=mock_summary) as mock_refresh,
        ):
            daily_candle_refresh()

        instruments = mock_refresh.call_args[0][2]
        ids = [iid for iid, _ in instruments]
        assert 3000 in ids
        assert 900 in ids
        assert ids.index(3000) < ids.index(900)

    def test_benchmark_also_held_is_deduped(self) -> None:
        """SPY in both held and benchmark must appear EXACTLY ONCE in the refresh list."""
        held = [(3000, "SPY")]
        benchmark = [(3000, "SPY")]
        t3 = [(900, "AAA")]
        mock_conn = _make_mock_conn([], t3, held_rows=held, benchmark_rows=benchmark)
        mock_provider = MagicMock()
        mock_provider.__enter__ = MagicMock(return_value=mock_provider)
        mock_provider.__exit__ = MagicMock(return_value=False)
        mock_tracker = MagicMock()
        mock_tracker.__enter__ = MagicMock(return_value=mock_tracker)
        mock_tracker.__exit__ = MagicMock(return_value=False)
        mock_summary = MagicMock()
        mock_summary.candle_rows_upserted = 1
        mock_summary.instruments_refreshed = 2
        mock_summary.features_computed = 0
        mock_summary.quotes_updated = 0
        mock_summary.quotes_skipped = 0
        mock_summary.spread_flags_set = 0
        mock_summary.candles_failed = 0
        mock_summary.candles_skipped = 0

        with (
            patch(_PATCHES["creds"], return_value=("key", "ukey")),
            patch(_PATCHES["tracked"], return_value=mock_tracker),
            patch(_PATCHES["provider_cls"], return_value=mock_provider),
            patch(_PATCHES["connect"], return_value=mock_conn),
            patch(_PATCHES["refresh"], return_value=mock_summary) as mock_refresh,
        ):
            daily_candle_refresh()

        instruments = mock_refresh.call_args[0][2]
        ids = [iid for iid, _ in instruments]
        assert ids.count(3000) == 1
        assert 900 in ids

    def test_t3_select_excludes_benchmark_symbols(self) -> None:
        """_T3_BOOTSTRAP_SELECT must reference %(benchmark_symbols)s."""
        from app.workers.scheduler import _T3_BOOTSTRAP_SELECT

        assert "benchmark_symbols" in _T3_BOOTSTRAP_SELECT


# ---------------------------------------------------------------------------
# #1293 — empty-fetch / candles=0 disambiguation
# ---------------------------------------------------------------------------


class TestDailyCandleRefreshEmptyFetchDisambiguation:
    """S2 run #6 completed in 9s with rows_processed=0. Without structured
    logging a healthy 'all already fresh' run and a broken 'every fetch
    failed' run both look like a clean SUCCESS. These tests pin the three
    cases to distinct log levels (#1293)."""

    def _run_with_summary(
        self,
        *,
        tier12_rows: list[tuple[int, str]],
        candle_rows_upserted: int,
        candles_failed: int,
        candles_skipped: int,
    ) -> None:
        mock_conn = _make_mock_conn(tier12_rows, [], None)
        mock_provider = MagicMock()
        mock_provider.__enter__ = MagicMock(return_value=mock_provider)
        mock_provider.__exit__ = MagicMock(return_value=False)
        mock_tracker = MagicMock()
        mock_tracker.__enter__ = MagicMock(return_value=mock_tracker)
        mock_tracker.__exit__ = MagicMock(return_value=False)
        mock_summary = MagicMock()
        mock_summary.candle_rows_upserted = candle_rows_upserted
        mock_summary.instruments_refreshed = len(tier12_rows)
        mock_summary.features_computed = 0
        mock_summary.quotes_updated = 0
        mock_summary.quotes_skipped = 0
        mock_summary.spread_flags_set = 0
        mock_summary.candles_failed = candles_failed
        mock_summary.candles_skipped = candles_skipped
        with (
            patch(_PATCHES["creds"], return_value=("key", "ukey")),
            patch(_PATCHES["tracked"], return_value=mock_tracker),
            patch(_PATCHES["provider_cls"], return_value=mock_provider),
            patch(_PATCHES["connect"], return_value=mock_conn),
            patch(_PATCHES["refresh"], return_value=mock_summary),
        ):
            daily_candle_refresh()

    def test_empty_scope_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        # Empty held + T1/T2 + T3 → refresh never called; WARNING surfaced.
        mock_conn = _make_mock_conn([], [], None)
        mock_provider = MagicMock()
        mock_provider.__enter__ = MagicMock(return_value=mock_provider)
        mock_provider.__exit__ = MagicMock(return_value=False)
        mock_tracker = MagicMock()
        mock_tracker.__enter__ = MagicMock(return_value=mock_tracker)
        mock_tracker.__exit__ = MagicMock(return_value=False)
        with (
            patch(_PATCHES["creds"], return_value=("key", "ukey")),
            patch(_PATCHES["tracked"], return_value=mock_tracker),
            patch(_PATCHES["provider_cls"], return_value=mock_provider),
            patch(_PATCHES["connect"], return_value=mock_conn),
            caplog.at_level("WARNING", logger="app.workers.scheduler"),
        ):
            daily_candle_refresh()
        assert any("refresh scope is EMPTY" in r.message for r in caplog.records)

    def test_all_fetches_failed_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level("WARNING", logger="app.workers.scheduler"):
            self._run_with_summary(
                tier12_rows=[(1, "AAPL"), (2, "MSFT"), (3, "GME")],
                candle_rows_upserted=0,
                candles_failed=3,
                candles_skipped=0,
            )
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert any("refreshes FAILED" in r.message and "Investigate" in r.message for r in warnings)

    def test_all_fresh_logs_info_not_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level("INFO", logger="app.workers.scheduler"):
            self._run_with_summary(
                tier12_rows=[(1, "AAPL"), (2, "MSFT")],
                candle_rows_upserted=0,
                candles_failed=0,
                candles_skipped=2,
            )
        assert any("already fresh" in r.message for r in caplog.records if r.levelname == "INFO")
        # The healthy no-op must NOT raise a failure warning.
        assert not any(r.levelname == "WARNING" and "FAILED" in r.message for r in caplog.records)

    def test_partial_failure_with_some_writes_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        # #1293 (Codex): candles>0 AND failed>0 must still WARN — a partial
        # failure is not a clean success.
        with caplog.at_level("WARNING", logger="app.workers.scheduler"):
            self._run_with_summary(
                tier12_rows=[(1, "AAPL"), (2, "MSFT"), (3, "GME")],
                candle_rows_upserted=20,
                candles_failed=1,
                candles_skipped=0,
            )
        assert any("refreshes FAILED" in r.message for r in caplog.records if r.levelname == "WARNING")

    def test_zero_writes_no_failures_not_all_fresh_logs_no_new_bars(self, caplog: pytest.LogCaptureFixture) -> None:
        # #1293 (Codex): non-empty scope, 0 written, 0 failed, not-all-fresh →
        # instruments returned no new bars (benign), logged at INFO, no WARNING.
        with caplog.at_level("INFO", logger="app.workers.scheduler"):
            self._run_with_summary(
                tier12_rows=[(1, "AAPL"), (2, "MSFT")],
                candle_rows_upserted=0,
                candles_failed=0,
                candles_skipped=1,  # 1 fresh, 1 fetched-but-no-bars
            )
        assert any("no new bars" in r.message for r in caplog.records if r.levelname == "INFO")
        assert not any(r.levelname == "WARNING" and "FAILED" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# #591 — benchmark instruments constant
# ---------------------------------------------------------------------------


def test_benchmark_symbols_constant_is_the_expected_set() -> None:
    from app.workers.scheduler import BENCHMARK_SYMBOLS

    # SPX500 = the reporting benchmark (reporting.py::BENCHMARK_SYMBOL); it must
    # be in the always-fresh set or its closes freeze (#1818). SPY = the risk
    # layer's beta benchmark — distinct purpose, both kept fresh here.
    assert BENCHMARK_SYMBOLS == frozenset(
        {"SPX500", "SPY", "QQQ", "XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY"}
    )


def test_reporting_benchmark_symbol_is_always_refreshed() -> None:
    """The reporting benchmark must be in the always-fresh candle set (#1818).

    The report compares portfolio vs ``reporting.BENCHMARK_SYMBOL``. That symbol
    is tier-3 and never held, so the ONLY scope that keeps its closes current is
    ``BENCHMARK_SYMBOLS``. If the two diverge, the benchmark silently freezes and
    the report renders "benchmark unavailable" (#1817 null-guard) — pin the
    invariant so a future symbol change to either constant can't recur the freeze.
    """
    from app.services.reporting import BENCHMARK_SYMBOL
    from app.workers.scheduler import BENCHMARK_SYMBOLS

    assert BENCHMARK_SYMBOL in BENCHMARK_SYMBOLS
