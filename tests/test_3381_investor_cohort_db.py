"""#3381 slice 2: the investor-cohort recorder's write path against a real database."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
import psycopg
import pytest

from app.providers.implementations.etoro_social import SocialResponse
from app.services.investor_cohort_recorder import (
    COHORT_RULE_VERSION,
    STATUS_COMPLETE,
    STATUS_FAILED,
    STATUS_PARTIAL,
    InvestorSnapshotPartial,
    InvestorSnapshotRefused,
    record_investor_snapshot,
)

T0 = datetime(2026, 9, 25, 22, 7, tzinfo=UTC)
PARAMS = {"rankings_path": "/api/v2/portfolios/rankings"}
POSITION = {"positionId": 11, "instrumentId": 1832, "isBuy": True, "leverage": 1, "openRate": 524.0}


class _Source:
    """Ranking of ``cids``; ``live`` maps username → response (default: one ok position)."""

    def __init__(self, cids: list[int], live: dict[str, SocialResponse | Exception] | None = None) -> None:
        self.cids = cids
        self.live = live or {}
        self.fetched: list[str] = []

    def get_rankings_page(self, params: dict[str, str | int | bool], page: int) -> SocialResponse:
        items = [{"cid": c, "username": f"u{c}", "copiers": 10, "riskScore": 3, "gain": 0.1} for c in self.cids]
        envelope = {"page": 1, "pageSize": 100, "totalItems": len(items), "hasNext": False}
        return SocialResponse(200, {"results": items, "pagination": envelope}, T0)

    def get_live_portfolio(self, username: str) -> SocialResponse:
        self.fetched.append(username)
        outcome = self.live.get(username, SocialResponse(200, {"positions": [POSITION], "socialTrades": []}, T0))
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def _header(conn: psycopg.Connection[Any], snapshot_id: int) -> tuple[Any, ...]:
    row = conn.execute(
        "SELECT status, ranking_recorded, investors_expected, investors_attempted, investors_fetched, "
        "investors_unavailable, investors_errored FROM etoro_investor_snapshots WHERE snapshot_id = %s",
        (snapshot_id,),
    ).fetchone()
    assert row is not None
    return tuple(row)


def _latest(conn: psycopg.Connection[Any]) -> int:
    row = conn.execute("SELECT max(snapshot_id) FROM etoro_investor_snapshots").fetchone()
    assert row is not None
    return int(row[0])


def test_complete_snapshot_then_a_dropped_member_stays_in_the_fetch_set(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    first = record_investor_snapshot(conn, _Source([1, 2]), request_params=PARAMS, clock=lambda: T0)
    assert _header(conn, first.snapshot_id) == (STATUS_COMPLETE, 2, 2, 2, 2, 0, 0)
    assert conn.execute("SELECT count(*) FROM etoro_investor_positions").fetchone() == (2,)
    assert conn.execute("SELECT cohort_rule_version, cid FROM etoro_investor_cohort ORDER BY cid").fetchall() == [
        (COHORT_RULE_VERSION, 1),
        (COHORT_RULE_VERSION, 2),
    ]

    # cid 2 leaves the ranking and its profile goes private: still attempted, recorded unavailable.
    gone = SocialResponse(404, {"message": "An error has occurred."}, T0)
    source = _Source([1], live={"u2": gone})
    second = record_investor_snapshot(conn, source, request_params=PARAMS, clock=lambda: T0)
    assert source.fetched == ["u1", "u2"]
    assert _header(conn, second.snapshot_id) == (STATUS_COMPLETE, 1, 2, 2, 1, 1, 0)
    assert conn.execute(
        "SELECT username, username_from_ranking, selected_today, outcome, http_status "
        "FROM etoro_investor_fetches WHERE snapshot_id = %s AND cid = 2",
        (second.snapshot_id,),
    ).fetchone() == ("u2", False, False, "unavailable", 404)

    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        with conn.transaction():
            conn.execute("UPDATE etoro_investor_cohort SET first_selected_at = now()")


def test_partial_commits_once_then_raises(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    busy = SocialResponse(500, "upstream", T0)
    with pytest.raises(InvestorSnapshotPartial):
        record_investor_snapshot(conn, _Source([1, 2], live={"u2": busy}), request_params=PARAMS, clock=lambda: T0)
    assert conn.execute("SELECT count(*) FROM etoro_investor_snapshots").fetchone() == (1,)
    assert _header(conn, _latest(conn)) == (STATUS_PARTIAL, 2, 2, 2, 1, 0, 1)


def test_a_late_401_keeps_the_ranking_membership_and_fetches_so_far(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    request = httpx.Request("GET", "https://example.invalid")
    unauthorised = httpx.HTTPStatusError("401", request=request, response=httpx.Response(401, request=request))
    with pytest.raises(httpx.HTTPStatusError):
        record_investor_snapshot(
            conn, _Source([1, 2, 3], live={"u2": unauthorised}), request_params=PARAMS, clock=lambda: T0
        )
    snapshot_id = _latest(conn)
    assert _header(conn, snapshot_id) == (STATUS_FAILED, 3, 3, 1, 1, 0, 0)
    assert conn.execute("SELECT count(*) FROM etoro_investor_cohort").fetchone() == (3,)
    assert conn.execute("SELECT count(*) FROM etoro_investor_rankings").fetchone() == (3,)


def test_a_member_selected_by_an_aborted_run_is_still_addressable_later(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Codex ckpt-2: an abort commits ledger rows for members never fetched; the next run must still find
    their username (from the ranking row) once they leave the ranking."""
    conn = ebull_test_conn
    request = httpx.Request("GET", "https://example.invalid")
    unauthorised = httpx.HTTPStatusError("401", request=request, response=httpx.Response(401, request=request))
    with pytest.raises(httpx.HTTPStatusError):
        record_investor_snapshot(
            conn, _Source([1, 2], live={"u1": unauthorised}), request_params=PARAMS, clock=lambda: T0
        )

    source = _Source([1])  # cid 2 was never fetched and has left the ranking
    record_investor_snapshot(conn, source, request_params=PARAMS, clock=lambda: T0)
    assert source.fetched == ["u1", "u2"]
    params = conn.execute("SELECT request_params FROM etoro_investor_snapshots ORDER BY snapshot_id").fetchone()
    assert params is not None and params[0]["ranking_params"]["sort"] == "username"


def test_nothing_fetched_is_a_failure_not_a_success(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    private = SocialResponse(403, "", T0)
    with pytest.raises(InvestorSnapshotRefused, match="no member fetched ok"):
        record_investor_snapshot(
            conn, _Source([1, 2], live={"u1": private, "u2": private}), request_params=PARAMS, clock=lambda: T0
        )
    assert _header(conn, _latest(conn)) == (STATUS_FAILED, 2, 2, 2, 0, 2, 0)


def test_a_failed_ranking_records_a_bare_failed_header(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn

    class _Broken(_Source):
        def get_rankings_page(self, params: dict[str, str | int | bool], page: int) -> SocialResponse:
            return SocialResponse(200, {"results": []}, T0)

    with pytest.raises(ValueError, match="lacks results/pagination"):
        record_investor_snapshot(conn, _Broken([]), request_params=PARAMS, clock=lambda: T0)
    assert _header(conn, _latest(conn)) == (STATUS_FAILED, None, None, None, None, None, None)
    assert conn.execute("SELECT count(*) FROM etoro_investor_cohort").fetchone() == (0,)
