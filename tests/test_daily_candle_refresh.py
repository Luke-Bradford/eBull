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

from app.workers.scheduler import _T3_BOOTSTRAP_BATCH_SIZE, daily_candle_refresh


def _make_mock_conn(
    tier12_rows: list[tuple[int, str]],
    t3_rows: list[tuple[int, str]],
    held_rows: list[tuple[int, str]] | None = None,
) -> MagicMock:
    """Mock connection that returns held_rows / tier12_rows / t3_rows in
    the order the handler executes them (held → tier12 → t3)."""
    conn = MagicMock()
    result_held = MagicMock()
    result_held.fetchall.return_value = held_rows or []
    result_12 = MagicMock()
    result_12.fetchall.return_value = tier12_rows
    result_t3 = MagicMock()
    result_t3.fetchall.return_value = t3_rows
    conn.execute.side_effect = [result_held, result_12, result_t3]
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

        with (
            patch(_PATCHES["creds"], return_value=("key", "ukey")),
            patch(_PATCHES["tracked"], return_value=mock_tracker),
            patch(_PATCHES["provider_cls"], return_value=mock_provider),
            patch(_PATCHES["connect"], return_value=mock_conn),
            patch(_PATCHES["refresh"], return_value=mock_summary),
        ):
            daily_candle_refresh()

        # Third execute call is the T3 query with limit param
        # (1st=held, 2nd=tier12, 3rd=T3 bootstrap).
        t3_call = mock_conn.execute.call_args_list[2]
        sql_text = t3_call[0][0]
        params = t3_call[0][1]
        assert "LIMIT" in sql_text
        assert params == {"limit": _T3_BOOTSTRAP_BATCH_SIZE}

    def test_bootstrap_batch_size_is_200(self) -> None:
        """Sanity check the constant value."""
        assert _T3_BOOTSTRAP_BATCH_SIZE == 200
