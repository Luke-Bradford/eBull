"""#3545 slice 1: the session rates capture's write path against a real database."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest

from app.providers.implementations.etoro_perishables import RATES_BATCH_SIZE, RawResponse
from app.services import session_rate_capture
from app.services.perishables_recorder import PerishableSnapshotPartial, PerishableSnapshotRefused
from app.services.session_rate_capture import capture_session_rates

T0 = datetime(2026, 10, 5, 14, 37, tzinfo=UTC)  # Monday, 10:37 ET
# Three batches: two full, one of five.
UNIVERSE = tuple(range(1001, 1001 + 2 * RATES_BATCH_SIZE + 5))


class _Source:
    """Answers every id; `errors` maps a batch's first id to the response that batch gets instead."""

    def __init__(self, errors: dict[int, RawResponse] | None = None) -> None:
        self.errors = errors or {}
        self.calls = 0

    def get_rates(self, instrument_ids: list[int]) -> RawResponse:
        self.calls += 1
        if instrument_ids[0] in self.errors:
            return self.errors[instrument_ids[0]]
        rates = [
            # One crossed quote (ask < bid) so the quoted census differs from the served count.
            {"instrumentID": i, "bid": 10, "ask": 9.9 if i == 1001 else 10.1, "date": "2026-10-05T14:36:59Z"}
            for i in instrument_ids
        ]
        return RawResponse(200, {"rates": rates}, T0)


@pytest.fixture
def universe(monkeypatch: pytest.MonkeyPatch) -> list[tuple[int, ...]]:
    holder = [UNIVERSE]
    monkeypatch.setattr(session_rate_capture, "load_validated_universe", lambda conn: holder[0])
    return holder


def _header(conn: psycopg.Connection[Any]) -> tuple[Any, ...]:
    row = conn.execute(
        "SELECT status, universe_size, requests_expected, requests_ok, requests_errored, instruments_served, "
        "instruments_quoted, error IS NOT NULL, universe_rule_version FROM etoro_session_rate_captures "
        "ORDER BY capture_id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    return tuple(row)


def test_a_complete_capture_writes_header_ledger_and_rows(
    ebull_test_conn: psycopg.Connection[Any], universe: list[tuple[int, ...]]
) -> None:
    result = capture_session_rates(ebull_test_conn, _Source(), clock=lambda: T0)

    assert result.status == "complete"
    assert (result.instruments_served, result.instruments_quoted) == (len(UNIVERSE), len(UNIVERSE) - 1)
    status, size, expected, ok, errored, served, quoted, has_error, rule = _header(ebull_test_conn)
    assert (status, size, expected, ok, errored, served, quoted, has_error) == (
        "complete",
        len(UNIVERSE),
        3,
        3,
        0,
        len(UNIVERSE),
        len(UNIVERSE) - 1,
        False,
    )
    assert rule == session_rate_capture.VALIDATED_UNIVERSE_RULE_VERSION
    # Membership is recoverable from the ledger: every requested id, exactly once, and no ok body kept.
    ledger = ebull_test_conn.execute(
        "SELECT array_agg(i ORDER BY i), bool_and(error_body IS NULL) FROM etoro_session_rate_requests r, "
        "unnest(r.instrument_ids) i WHERE capture_id = %s",
        (result.capture_id,),
    ).fetchone()
    assert ledger is not None
    assert tuple(ledger[0]) == UNIVERSE and ledger[1] is True
    row = ebull_test_conn.execute(
        "SELECT o.bid, o.ask, o.quote_at, r.seq FROM etoro_session_rate_observations o "
        "JOIN etoro_session_rate_requests r ON r.capture_id = o.capture_id AND r.seq = o.request_seq "
        "WHERE o.capture_id = %s AND o.instrument_id = %s",
        (result.capture_id, UNIVERSE[-1]),
    ).fetchone()
    assert row is not None
    assert (str(row[0]), str(row[1]), row[2], row[3]) == (
        "10",
        "10.1",
        datetime(2026, 10, 5, 14, 36, 59, tzinfo=UTC),
        2,
    )


def test_errored_batches_commit_partial_keep_their_bodies_then_raise(
    ebull_test_conn: psycopg.Connection[Any], universe: list[tuple[int, ...]]
) -> None:
    second, third = UNIVERSE[RATES_BATCH_SIZE], UNIVERSE[2 * RATES_BATCH_SIZE]
    source = _Source(
        {
            second: RawResponse(500, "upstream down", T0),
            # A 200 that answers an id it was not asked for breaks the envelope: the whole batch is an error.
            third: RawResponse(200, {"rates": [{"instrumentID": 1, "bid": 1, "ask": 1}]}, T0),
        }
    )

    with pytest.raises(PerishableSnapshotPartial):
        capture_session_rates(ebull_test_conn, source, clock=lambda: T0)

    assert _header(ebull_test_conn)[:6] == ("partial", len(UNIVERSE), 3, 1, 2, RATES_BATCH_SIZE)
    errors = ebull_test_conn.execute(
        "SELECT seq, http_status, error_body FROM etoro_session_rate_requests WHERE outcome = 'error' ORDER BY seq"
    ).fetchall()
    assert [(r[0], r[1]) for r in errors] == [(1, 500), (2, 200)]
    assert errors[0][2] == "upstream down"
    assert errors[1][2] == {"rates": [{"instrumentID": 1, "bid": 1, "ask": 1}]}


def test_a_credential_refusal_fails_the_capture_and_keeps_what_it_collected(
    ebull_test_conn: psycopg.Connection[Any], universe: list[tuple[int, ...]]
) -> None:
    source = _Source({UNIVERSE[RATES_BATCH_SIZE]: RawResponse(401, {"error": "unauthorized"}, T0)})

    with pytest.raises(PerishableSnapshotRefused):
        capture_session_rates(ebull_test_conn, source, clock=lambda: T0)

    status, size, expected, ok, errored, served, _quoted, has_error, _rule = _header(ebull_test_conn)
    assert (status, size, expected, ok, errored, served, has_error) == (
        "failed",
        len(UNIVERSE),
        3,
        1,
        1,
        RATES_BATCH_SIZE,
        True,
    )
    assert source.calls == 2  # stopped at the refusal, the third batch never asked


def test_an_empty_universe_is_refused_as_a_failed_capture(
    ebull_test_conn: psycopg.Connection[Any], universe: list[tuple[int, ...]]
) -> None:
    universe[0] = ()
    source = _Source()

    with pytest.raises(PerishableSnapshotRefused):
        capture_session_rates(ebull_test_conn, source, clock=lambda: T0)

    assert _header(ebull_test_conn)[:6] == ("failed", 0, None, 0, 0, 0)
    assert source.calls == 0


def test_rows_refuse_update(ebull_test_conn: psycopg.Connection[Any], universe: list[tuple[int, ...]]) -> None:
    capture_session_rates(ebull_test_conn, _Source(), clock=lambda: T0)

    with pytest.raises(psycopg.errors.RaiseException):
        ebull_test_conn.execute("UPDATE etoro_session_rate_observations SET bid = 0")
    ebull_test_conn.rollback()
