"""#3381 slice 2: the investor-cohort recorder's pure rules — ranking walk, fetch set, response classes."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from app.providers.implementations.etoro_social import SocialResponse
from app.services.investor_cohort_recorder import (
    COHORT_TOP_N,
    OUTCOME_ERROR,
    OUTCOME_OK,
    OUTCOME_UNAVAILABLE,
    Member,
    build_fetch_set,
    classify_response,
    fetch_member,
    fetch_ranking,
)

T0 = datetime(2026, 9, 25, 22, 7, tzinfo=UTC)


def _rank_item(cid: int, username: str | None = None) -> dict[str, Any]:
    return {"cid": cid, "username": username or f"user{cid}", "copiers": 1000 - cid, "riskScore": 4, "gain": 0.12}


def _page(page: int, items: list[dict[str, Any]], total: int, has_next: bool) -> SocialResponse:
    envelope = {"page": page, "pageSize": 100, "totalItems": total, "hasNext": has_next}
    return SocialResponse(200, {"results": items, "pagination": envelope}, T0)


class _Pages:
    """Serves ``pages``; with ``walks``, serves each walk's pages in turn (a re-walk starts at page 1)."""

    def __init__(self, *pages: SocialResponse, walks: tuple[tuple[SocialResponse, ...], ...] = ()) -> None:
        self.walks = list(walks) or [pages]
        self.requested: list[int] = []

    def get_rankings_page(self, params: dict[str, str | int | bool], page: int) -> SocialResponse:
        assert params["sort"] == "username"
        if page == 1 and self.requested and len(self.walks) > 1:
            self.walks.pop(0)
        self.requested.append(page)
        return self.walks[0][page - 1]

    def get_live_portfolio(self, username: str) -> SocialResponse:  # pragma: no cover - unused here
        raise AssertionError


def test_ranking_walks_every_page_then_ranks_by_copiers_then_cid() -> None:
    tied = {**_rank_item(9), "copiers": 500}
    source = _Pages(
        _page(1, [_rank_item(3), {**_rank_item(1), "copiers": None}, tied], 5, True),
        _page(2, [{**_rank_item(4), "copiers": 500}, _rank_item(2)], 5, False),
    )
    ranking = fetch_ranking(source)
    assert source.requested == [1, 2]
    # copiers: cid2=998, cid3=997, cid4=500, cid9=500 (tie → cid), cid1=None (last)
    assert [(r.rank, r.cid) for r in ranking.rows] == [(1, 2), (2, 3), (3, 4), (4, 9), (5, 1)]
    assert (ranking.total_items, len(ranking.envelopes), ranking.attempts) == (5, 2, 1)


def test_an_inconsistent_walk_is_re_walked_and_the_attempts_recorded() -> None:
    bad = (_page(1, [_rank_item(1)], 2, True), _page(2, [_rank_item(1)], 2, False))
    good = (_page(1, [_rank_item(1)], 2, True), _page(2, [_rank_item(2)], 2, False))
    source = _Pages(walks=(bad, good))
    ranking = fetch_ranking(source)
    assert source.requested == [1, 2, 1, 2]
    assert ([r.cid for r in ranking.rows], ranking.attempts) == ([1, 2], 2)


def test_a_malformed_page_is_not_re_walked() -> None:
    source = _Pages(SocialResponse(200, {"results": []}, T0))
    with pytest.raises(ValueError, match="lacks results/pagination"):
        fetch_ranking(source)
    assert source.requested == [1]


@pytest.mark.parametrize(
    ("pages", "message"),
    [
        ((_page(1, [_rank_item(1)], 2, True), _page(2, [_rank_item(2)], 3, False)), "totalItems changed"),
        ((_page(1, [_rank_item(1)], 2, True), _page(2, [_rank_item(1)], 2, False)), "repeated cid"),  # every attempt
        ((_page(1, [_rank_item(1)], 2, False),), "incomplete"),
        ((_page(1, [], 0, False),), "zero popular investors"),
        ((_page(1, [], 5, True),), "empty but reports hasNext"),
        ((_page(1, [{"cid": 7}], 1, False),), "lacks cid/username"),
        ((SocialResponse(200, {"results": []}, T0),), "lacks results/pagination"),
    ],
)
def test_an_inconsistent_ranking_refuses(pages: tuple[SocialResponse, ...], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        fetch_ranking(_Pages(*pages))


def test_a_non_integer_optional_metric_is_dropped_not_fatal() -> None:
    item = {**_rank_item(1), "copiers": "many", "riskScore": 4.5}
    row = fetch_ranking(_Pages(_page(1, [item], 1, False))).rows[0]
    assert (row.copiers, row.risk_score, row.raw["copiers"]) == (None, None, "many")


def _ranking_of(n: int):  # noqa: ANN202 - test helper
    return fetch_ranking(_Pages(_page(1, [_rank_item(cid) for cid in range(1, n + 1)], n, False)))


def test_fetch_set_is_top_n_plus_every_ledger_member_in_rank_then_cid_order() -> None:
    ranking = _ranking_of(COHORT_TOP_N + 5)
    below_cut = COHORT_TOP_N + 3  # still ranked, fell out of the top N
    gone = 999_001  # no longer ranked at all
    members = build_fetch_set(ranking, frozenset({below_cut, gone, 1}), {gone: "renamed_or_private"})

    assert [m.cid for m in members] == [*range(1, COHORT_TOP_N + 1), below_cut, gone]
    by_cid = {m.cid: m for m in members}
    assert by_cid[1] == Member(1, "user1", True, True)
    assert by_cid[below_cut] == Member(below_cut, f"user{below_cut}", True, False)
    assert by_cid[gone] == Member(gone, "renamed_or_private", False, False)


def test_fewer_rows_than_n_selects_them_all() -> None:
    assert [m.cid for m in build_fetch_set(_ranking_of(3), frozenset(), {})] == [1, 2, 3]


def test_a_ledger_member_with_no_username_is_an_invariant_breach() -> None:
    with pytest.raises(RuntimeError, match="no recorded username"):
        build_fetch_set(_ranking_of(1), frozenset({42}), {})


MEMBER = Member(5, "user5", True, True)
POSITION = {
    "positionId": 11,
    "openTimestamp": "2026-06-23T13:53:46.72Z",
    "openRate": 524.043,
    "instrumentId": 1832,
    "isBuy": True,
    "leverage": 1,
    "takeProfitRate": 5760.81,
    "stopLossRate": 0.0001,
    "socialTradeId": 0,
    "parentPositionId": 0,
    "investmentPct": 0.565921,
    "netProfit": 20.23875,
    "trailingStopLoss": False,
}


def test_an_ok_body_parses_top_level_positions_and_counts_social_trades() -> None:
    body = {"realizedCreditPct": 6.7, "unrealizedCreditPct": 6.4, "positions": [POSITION], "socialTrades": [{}]}
    fetch = classify_response(MEMBER, SocialResponse(200, body, T0))
    assert (fetch.outcome, fetch.position_count, fetch.social_trade_count) == (OUTCOME_OK, 1, 1)
    assert fetch.positions[0][:3] == (11, 1832, True)
    assert fetch.positions[0][6] == datetime(2026, 6, 23, 13, 53, 46, 720000, tzinfo=UTC)


@pytest.mark.parametrize("status", [403, 404])
def test_a_private_or_missing_profile_is_unavailable(status: int) -> None:
    fetch = classify_response(MEMBER, SocialResponse(status, {"message": "An error has occurred."}, T0))
    assert (fetch.outcome, fetch.http_status, fetch.position_count) == (OUTCOME_UNAVAILABLE, status, None)


@pytest.mark.parametrize(
    "body",
    [
        {"positions": [POSITION, POSITION]},  # repeated positionId
        {"positions": [{**POSITION, "openRate": "x"}]},
        {"positions": [{**POSITION, "openTimestamp": "not a time"}]},
        {"positions": None},
        "plain text",
    ],
)
def test_a_malformed_ok_body_errors_this_member_and_keeps_the_raw(body: object) -> None:
    fetch = classify_response(MEMBER, SocialResponse(200, body, T0))
    assert (fetch.outcome, fetch.raw, fetch.positions) == (OUTCOME_ERROR, body, ())


class _Live:
    def __init__(self, outcome: SocialResponse | Exception) -> None:
        self.outcome = outcome

    def get_rankings_page(self, params: dict[str, str | int | bool], page: int) -> SocialResponse:  # pragma: no cover
        raise AssertionError

    def get_live_portfolio(self, username: str) -> SocialResponse:
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


def _status_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://example.invalid/live")
    return httpx.HTTPStatusError("x", request=request, response=httpx.Response(status, request=request, text="busy"))


def test_exhausted_retries_become_an_error_row() -> None:
    fetch = fetch_member(_Live(_status_error(429)), MEMBER, lambda: T0)
    assert (fetch.outcome, fetch.http_status, fetch.raw) == (OUTCOME_ERROR, 429, "busy")


def test_a_transport_error_becomes_an_error_row_with_no_status() -> None:
    fetch = fetch_member(_Live(httpx.ConnectError("down")), MEMBER, lambda: T0)
    assert (fetch.outcome, fetch.http_status, fetch.raw) == (OUTCOME_ERROR, None, "ConnectError: down")


@pytest.mark.parametrize("outcome", [SocialResponse(401, {}, T0), _status_error(401)])
def test_a_401_aborts_the_whole_run(outcome: SocialResponse | Exception) -> None:
    with pytest.raises(httpx.HTTPStatusError):
        fetch_member(_Live(outcome), MEMBER, lambda: T0)
