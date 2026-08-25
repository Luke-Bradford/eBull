"""Unit tests for the scheduled quote refresh (#2271).

The ``quotes`` table had no scheduled writer: the WS subscriber is
visibility-driven (#498 — writes only for instruments an SSE stream has on
screen) and ``market_data._upsert_quote`` was unreachable because every
caller of ``refresh_market_data`` passes ``skip_quotes=True``. Eight
headless services read the table regardless.

No live database or network calls — all dependencies are mocked.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from app.providers.market_data import Quote
from app.services.market_data import refresh_quotes
from app.workers.scheduler import JOB_QUOTES_REFRESH, quotes_refresh


def _quote(instrument_id: int, *, bid: str = "10", ask: str = "10.02", last: str = "10.01") -> Quote:
    return Quote(
        instrument_id=instrument_id,
        bid=Decimal(bid),
        ask=Decimal(ask),
        last=Decimal(last),
        timestamp=datetime(2026, 8, 4, 12, 0, tzinfo=UTC),
    )


def _passthrough_conn() -> MagicMock:
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=ctx)
    ctx.__exit__ = MagicMock(return_value=False)
    conn.transaction.return_value = ctx
    return conn


def _provider() -> MagicMock:
    """A provider mock carrying the real ``quote_batch_size`` contract.

    Without it the attribute is a MagicMock and the candidate cohort's
    chunking arithmetic silently misbehaves.
    """
    provider = MagicMock()
    provider.quote_batch_size = 50
    return provider


class TestRefreshQuotes:
    def test_empty_instruments_does_not_call_provider(self) -> None:
        provider = MagicMock()
        summary = refresh_quotes(provider, _passthrough_conn(), [])
        provider.get_quotes.assert_not_called()
        assert summary.instruments_requested == 0
        assert summary.batch_failed is False

    def test_instrument_without_a_returned_quote_counts_as_skipped(self) -> None:
        """A provider that answers for only some IDs must not be read as a
        failure — it is the normal shape for untraded instruments."""
        provider = _provider()
        provider.get_quotes.return_value = [_quote(1)]

        with patch("app.services.market_data._upsert_quote", return_value=False):
            summary = refresh_quotes(provider, _passthrough_conn(), [(1, "AAA"), (2, "BBB")])

        assert summary.quotes_updated == 1
        assert summary.quotes_skipped == 1
        assert summary.batch_failed is False

    def test_a_broken_observation_lane_is_counted_not_silently_swallowed(self) -> None:
        """#2833 — the lane is isolated from the quote refresh, so a bad
        column or bad SQL would otherwise log once an hour forever while the
        candidate sample never grows. The count is the detector."""
        provider = _provider()
        provider.get_quotes.side_effect = [[_quote(1)], [_quote(1)]]

        with (
            patch("app.services.market_data._upsert_quote", return_value=False),
            patch(
                "app.services.market_data.record_core_quote_observations",
                side_effect=RuntimeError("relation does not exist"),
            ),
        ):
            summary = refresh_quotes(
                provider,
                _passthrough_conn(),
                [(1, "AAA")],
                observe_instrument_ids=frozenset({1}),
            )

        assert summary.core_observation_failures == 1
        assert summary.core_observations_written == 0
        # The quote refresh eight headless services depend on still ran.
        assert summary.quotes_updated == 1
        assert summary.batch_failed is False

    def test_observations_are_only_written_for_named_candidates(self) -> None:
        provider = _provider()
        provider.get_quotes.side_effect = [[_quote(1), _quote(2)], [_quote(2)]]

        with (
            patch("app.services.market_data._upsert_quote", return_value=False),
            patch("app.services.market_data.record_core_quote_observations", return_value=1) as record,
        ):
            summary = refresh_quotes(
                provider,
                _passthrough_conn(),
                [(1, "AAA"), (2, "BBB")],
                observe_instrument_ids=frozenset({2}),
            )

        assert summary.core_observations_written == 1
        assert [o.instrument_id for o in record.call_args_list[0].args[1]] == [2]

    def test_a_missing_quote_still_records_coverage_for_a_candidate(self) -> None:
        """Absence is evidence: dropping it would shrink the sample silently."""
        provider = _provider()
        provider.get_quotes.side_effect = [[], []]

        with (
            patch("app.services.market_data._upsert_quote", return_value=False),
            patch("app.services.market_data.record_core_quote_observations", return_value=1) as record,
        ):
            summary = refresh_quotes(
                provider,
                _passthrough_conn(),
                [(1, "AAA")],
                observe_instrument_ids=frozenset({1}),
            )

        assert summary.quotes_skipped == 1
        assert summary.core_observations_written == 1
        assert record.call_args_list[0].args[1][0].observation_status == "missing"

    def test_a_failed_candidate_fetch_records_nothing_rather_than_absence(self) -> None:
        """#2833 — `get_quotes` swallows a failed CHUNK and returns the rest,
        so a candidate in that chunk looks identical to one eToro has no
        quote for. Writing it as `provider_omitted_quote` would store a
        transport failure as broker evidence."""
        provider = _provider()
        provider.get_quotes.side_effect = [[_quote(1)], RuntimeError("etoro chunk down")]

        with (
            patch("app.services.market_data._upsert_quote", return_value=False),
            patch("app.services.market_data.record_core_quote_observations", return_value=0) as record,
        ):
            summary = refresh_quotes(
                provider,
                _passthrough_conn(),
                [(1, "AAA")],
                observe_instrument_ids=frozenset({1}),
            )

        record.assert_not_called()
        assert summary.core_observations_written == 0
        assert summary.core_observation_failures == 1
        # The quote refresh itself is unaffected.
        assert summary.quotes_updated == 1
        assert summary.batch_failed is False

    def test_the_candidate_cohort_is_split_by_the_provider_batch_size(self) -> None:
        """#2833 — one call per upstream request, so no candidate can go
        missing to a chunk the provider swallowed. A cohort larger than the
        batch size must fan out, not ride in one ambiguous call."""
        provider = MagicMock()
        provider.quote_batch_size = 2
        provider.get_quotes.side_effect = [
            [_quote(1)],  # main scope
            [_quote(1), _quote(2)],  # cohort chunk 1
            [_quote(3)],  # cohort chunk 2
        ]

        with (
            patch("app.services.market_data._upsert_quote", return_value=False),
            patch("app.services.market_data.record_core_quote_observations", return_value=3) as record,
        ):
            refresh_quotes(
                provider,
                _passthrough_conn(),
                [(1, "AAA")],
                observe_instrument_ids=frozenset({1, 2, 3}),
            )

        cohort_calls = [c.args[0] for c in provider.get_quotes.call_args_list[1:]]
        assert cohort_calls == [[1, 2], [3]]
        assert [o.instrument_id for o in record.call_args_list[0].args[1]] == [1, 2, 3]

    def test_one_failed_cohort_chunk_voids_the_whole_tick(self) -> None:
        """Partial cohort coverage would still mislabel the failed chunk's
        candidates as `provider_omitted_quote`, so nothing is written."""
        provider = MagicMock()
        provider.quote_batch_size = 2
        provider.get_quotes.side_effect = [
            [_quote(1)],
            [_quote(1), _quote(2)],
            RuntimeError("chunk down"),
        ]

        with (
            patch("app.services.market_data._upsert_quote", return_value=False),
            patch("app.services.market_data.record_core_quote_observations", return_value=0) as record,
        ):
            summary = refresh_quotes(
                provider,
                _passthrough_conn(),
                [(1, "AAA")],
                observe_instrument_ids=frozenset({1, 2, 3}),
            )

        record.assert_not_called()
        assert summary.core_observation_failures == 3

    def test_batch_fetch_failure_is_distinguishable_from_no_quotes(self) -> None:
        """#2218 shape — a total upstream outage and a universe of untraded
        instruments both write zero quotes. The caller must be able to tell
        them apart, or an outage reports as a clean run."""
        provider = _provider()
        provider.get_quotes.side_effect = RuntimeError("etoro down")

        summary = refresh_quotes(provider, _passthrough_conn(), [(1, "AAA"), (2, "BBB")])

        assert summary.batch_failed is True
        assert summary.quotes_updated == 0
        assert summary.quotes_skipped == 2

    def test_one_bad_upsert_does_not_abort_the_rest(self) -> None:
        provider = _provider()
        provider.get_quotes.return_value = [_quote(1), _quote(2), _quote(3)]

        with patch("app.services.market_data._upsert_quote", side_effect=[RuntimeError("boom"), False, True]):
            summary = refresh_quotes(provider, _passthrough_conn(), [(1, "AAA"), (2, "BBB"), (3, "CCC")])

        assert summary.quotes_updated == 2
        assert summary.spread_flags_set == 1


class TestQuotesRefreshJob:
    """The job's wiring — scope query, connection shape, empty-scope signal."""

    @staticmethod
    def _run(rows: list[tuple[int, str, bool]]) -> tuple[MagicMock, MagicMock]:
        conn = _passthrough_conn()
        result = MagicMock()
        result.fetchall.return_value = rows
        conn.execute.return_value = result
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)

        provider = MagicMock()
        provider.__enter__ = MagicMock(return_value=provider)
        provider.__exit__ = MagicMock(return_value=False)

        tracker = MagicMock()
        tracker.__enter__ = MagicMock(return_value=tracker)
        tracker.__exit__ = MagicMock(return_value=False)

        summary = MagicMock()
        summary.instruments_requested = len(rows)
        summary.quotes_updated = len(rows)
        summary.quotes_skipped = 0
        summary.spread_flags_set = 0
        summary.batch_failed = False
        summary.batch_error = None

        with (
            patch("app.workers.scheduler._load_etoro_credentials", return_value=("key", "ukey")),
            patch("app.workers.scheduler._tracked_job", return_value=tracker),
            patch("app.workers.scheduler.EtoroMarketDataProvider", return_value=provider),
            patch("app.workers.scheduler.psycopg.connect", return_value=conn) as mock_connect,
            patch("app.workers.scheduler.refresh_quotes", return_value=summary) as mock_refresh,
        ):
            quotes_refresh()

        return mock_connect, mock_refresh

    def test_connection_is_autocommit(self) -> None:
        """#2269 — the scope SELECT opens an implicit transaction, which would
        turn refresh_quotes' per-instrument ``conn.transaction()`` into a
        savepoint and defer every write to connection close."""
        mock_connect, _ = self._run([(1, "AAPL", False)])
        mock_connect.assert_called_once()
        assert mock_connect.call_args.kwargs.get("autocommit") is True

    def test_scope_rows_are_passed_through(self) -> None:
        _, mock_refresh = self._run([(1, "AAPL", False), (2, "MSFT", True)])
        mock_refresh.assert_called_once()
        assert mock_refresh.call_args[0][2] == [(1, "AAPL"), (2, "MSFT")]

    def test_candidacy_comes_from_the_same_scope_row(self) -> None:
        """#2833 — one snapshot, so a proof landing mid-tick cannot make the
        scope and the candidate set disagree."""
        _, mock_refresh = self._run([(1, "AAPL", False), (2, "MSFT", True)])
        assert mock_refresh.call_args.kwargs["observe_instrument_ids"] == frozenset({2})

    def test_empty_scope_warns_rather_than_reading_as_a_clean_no_op(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level("WARNING"):
            _, mock_refresh = self._run([])
        mock_refresh.assert_not_called()
        assert any("scope is EMPTY" in r.message for r in caplog.records)

    def test_total_batch_failure_raises_inside_the_tracked_block(self) -> None:
        """#2218 shape (Codex round 2). The failure must propagate while
        ``_tracked_job`` is still open — once its context exits the job_runs
        row is already stamped success/row_count=0 and the job's success state
        has advanced, so a total eToro outage would read as a clean run."""
        import httpx

        request = httpx.Request("GET", "https://example.test/rates")
        boom = httpx.HTTPStatusError("err", request=request, response=httpx.Response(503, request=request, text="down"))

        conn = _passthrough_conn()
        result = MagicMock()
        result.fetchall.return_value = [(1, "AAPL", False)]
        conn.execute.return_value = result
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)

        provider = MagicMock()
        provider.__enter__ = MagicMock(return_value=provider)
        provider.__exit__ = MagicMock(return_value=False)

        tracker = MagicMock()
        tracker.__enter__ = MagicMock(return_value=tracker)
        # Records whether the tracker was still open when the error surfaced.
        exits: list[object] = []
        tracker.__exit__ = MagicMock(side_effect=lambda *a: exits.append(a[0]) or False)

        summary = MagicMock()
        summary.instruments_requested = 1
        summary.quotes_updated = 0
        summary.quotes_skipped = 1
        summary.spread_flags_set = 0
        summary.batch_failed = True
        summary.batch_error = boom

        with (
            patch("app.workers.scheduler._load_etoro_credentials", return_value=("key", "ukey")),
            patch("app.workers.scheduler._tracked_job", return_value=tracker),
            patch("app.workers.scheduler.EtoroMarketDataProvider", return_value=provider),
            patch("app.workers.scheduler.psycopg.connect", return_value=conn),
            patch("app.workers.scheduler.refresh_quotes", return_value=summary),
            pytest.raises(httpx.HTTPStatusError),
        ):
            quotes_refresh()

        # The tracker saw the exception type rather than a clean exit, which is
        # what makes it record a failure instead of success.
        assert exits and exits[0] is httpx.HTTPStatusError


def test_quotes_refresh_is_registered_and_invocable() -> None:
    """A job body with no registry entry never fires, and one with no invoker
    cannot be triggered from the admin UI — both silent."""
    from app.jobs.runtime import _INVOKERS
    from app.workers.scheduler import SCHEDULED_JOBS

    entry = next((j for j in SCHEDULED_JOBS if j.name == JOB_QUOTES_REFRESH), None)
    assert entry is not None, "quotes_refresh missing from SCHEDULED_JOBS"
    assert entry.source == "etoro_quotes"
    assert JOB_QUOTES_REFRESH in _INVOKERS


def test_quotes_refresh_slot_avoids_the_lane_tick_race() -> None:
    """#1526/#1527 — a 5-min-aligned slot loses its lane tick to a same-lane
    every_5min job. Pin the offset so a future cadence edit cannot silently
    reintroduce the skip."""
    from app.workers.scheduler import SCHEDULED_JOBS

    entry = next(j for j in SCHEDULED_JOBS if j.name == JOB_QUOTES_REFRESH)
    assert entry.cadence.kind == "hourly"
    assert entry.cadence.minute % 5 != 0


class TestGetQuotesTotalFailure:
    """#2271 (Codex) — ``get_quotes`` swallows per-chunk failures and returns
    partial results, so a TOTAL outage returned ``[]``, which is byte-identical
    to "none of these instruments has a quote". The scheduled job would then
    report a clean run while every headless reader sat on stale marks. Partial
    failure must still degrade gracefully; total failure must raise."""

    @staticmethod
    def _provider() -> tuple[object, MagicMock]:
        from app.providers.implementations.etoro import EtoroMarketDataProvider

        provider = EtoroMarketDataProvider(api_key="k", user_key="u", env="demo")
        http = MagicMock()
        provider._http = http  # type: ignore[attr-defined]
        return provider, http

    @staticmethod
    def _http_error(status: int) -> Exception:
        import httpx

        request = httpx.Request("GET", "https://example.test/rates")
        response = httpx.Response(status, request=request, text="boom")
        return httpx.HTTPStatusError("err", request=request, response=response)

    def test_all_chunks_failing_raises_rather_than_returning_empty(self) -> None:
        provider, http = self._provider()
        http.get.side_effect = self._http_error(503)

        import httpx

        with pytest.raises(httpx.HTTPStatusError):
            provider.get_quotes([1, 2, 3])  # type: ignore[attr-defined]

    def test_reraised_error_preserves_its_failure_category(self) -> None:
        """The original httpx type is re-raised, not a bespoke provider error,
        so classify_exception still yields AUTH_EXPIRED / RATE_LIMITED /
        SOURCE_DOWN instead of flattening to INTERNAL_ERROR."""
        from app.services.sync_orchestrator.exception_classifier import FailureCategory, classify_exception

        for status, expected in (
            (401, FailureCategory.AUTH_EXPIRED),
            (429, FailureCategory.RATE_LIMITED),
            (503, FailureCategory.SOURCE_DOWN),
        ):
            provider, http = self._provider()
            http.get.side_effect = self._http_error(status)
            try:
                provider.get_quotes([1])  # type: ignore[attr-defined]
            except Exception as exc:  # noqa: BLE001 - asserting on the classified category
                assert classify_exception(exc) is expected, f"status {status}"
            else:  # pragma: no cover - the call above must raise
                raise AssertionError(f"status {status} did not raise")

    def test_partial_failure_still_returns_what_succeeded(self) -> None:
        """Only TOTAL failure raises — a single bad chunk must not lose the
        quotes the other chunks did return."""

        provider, http = self._provider()
        ok = MagicMock()
        ok.raise_for_status.return_value = None
        ok.json.return_value = {
            "rates": [
                {"instrumentID": 1, "bid": 10, "ask": 10.02, "lastExecution": 10.01, "date": "2026-08-04T12:00:00Z"}
            ]
        }
        # 60 ids => 2 chunks at _RATES_BATCH_SIZE=50: first fails, second succeeds.
        http.get.side_effect = [self._http_error(500), ok]

        quotes = provider.get_quotes(list(range(1, 61)))  # type: ignore[attr-defined]

        assert http.get.call_count == 2
        assert [q.instrument_id for q in quotes] == [1]
