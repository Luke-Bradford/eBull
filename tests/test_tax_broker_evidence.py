"""Pure tests for the slice 3c lookup capture (#3616; spec Design 3): classification, candidates and the plan."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.providers.broker import RawOrderLookup
from app.services.tax_broker_evidence import (
    TAX_EVIDENCE_POLICY_ID,
    LedgerOrder,
    LookupAttempt,
    classify_lookup,
    lookup_candidates,
    plan_lookups,
)

NOW = datetime(2026, 10, 14, 12, 13, tzinfo=UTC)


def _raw(status: int | None, body: str | None = None) -> RawOrderLookup:
    return RawOrderLookup(http_status=status, body_text=body, error_detail=None if status else "ConnectError: x")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (_raw(None), ("transport_error", None)),
        (_raw(404, '{"errorMessage": "Order category for 7 not found"}'), ("not_found", None)),
        (_raw(429, ""), ("rate_limited", None)),
        (_raw(401, ""), ("auth_failed", None)),
        (_raw(403, ""), ("auth_failed", None)),
        (_raw(500, "oops"), ("http_error", None)),
        (_raw(200, "not json"), ("malformed", None)),
        (_raw(200, "[]"), ("malformed", None)),
        (_raw(200, "{}"), ("malformed", None)),
        (_raw(200, '{"orderId": 8}'), ("malformed", None)),
        (_raw(200, '{"orderId": "7"}'), ("malformed", None)),
        (_raw(200, '{"orderId": 7.0}'), ("malformed", None)),
        (_raw(200, '{"orderId": 7, "asset": {}, "positionExecutions": [{}]}'), ("found", None)),
        (_raw(200, '{"orderID": 7, "accountId": 99}'), ("found", 99)),
        # A second spelling that disagrees still binds; derivation reports field_conflict:orderId.
        (_raw(200, '{"orderId": 7, "orderID": 8, "accountId": 99}'), ("found", 99)),
        (_raw(200, '{"orderId": 7, "accountId": 99, "accountID": 98}'), ("found", None)),
        (_raw(200, '{"orderId": 7, "accountId": true}'), ("found", None)),
        # Out of BIGINT range: not an account id, so the attempt is still stored.
        (_raw(200, '{"orderId": 7, "accountId": 9223372036854775808}'), ("found", None)),
        (_raw(200, '{"orderId": 7, "accountId": 9223372036854775807}'), ("found", 9223372036854775807)),
        (_raw(200, "[" * 100_000 + "]" * 100_000), ("malformed", None)),
    ],
)
def test_classify_lookup(raw: RawOrderLookup, expected: tuple[str, int | None]) -> None:
    assert classify_lookup(7, raw) == expected


def test_classify_true_is_not_the_id_one() -> None:
    assert classify_lookup(1, _raw(200, '{"orderId": true}')) == ("malformed", None)


def _order(order_id: int, e: int = 10, opened_days_ago: int = 30, last_days_ago: int = 20, held: bool = False):
    return LedgerOrder(
        order_id=order_id,
        max_event_id=e,
        opened_at=NOW - timedelta(days=opened_days_ago),
        last_event_at=NOW - timedelta(days=last_days_ago),
        held=held,
    )


_IDS = iter(range(1, 10_000))


def _attempt(order_id: int, outcome: str, days_ago: float, e: int = 10, policy: str = TAX_EVIDENCE_POLICY_ID):
    return LookupAttempt(
        lookup_id=next(_IDS),
        order_id=order_id,
        requested_at=NOW - timedelta(days=days_ago),
        policy_id=policy,
        outcome=outcome,
        order_event_max_id=e,
    )


def _queues(order: LedgerOrder, *attempts: LookupAttempt) -> tuple[list[int], list[int]]:
    first, retry = lookup_candidates([order], attempts, NOW)
    return [o.order_id for o in first], [o.order_id for o in retry]


def test_no_attempt_is_a_first_attempt() -> None:
    assert _queues(_order(1)) == ([1], [])


@pytest.mark.parametrize("outcome", ["rate_limited", "transport_error", "http_error", "auth_failed"])
def test_a_failed_latest_attempt_is_retried(outcome: str) -> None:
    assert _queues(_order(1), _attempt(1, outcome, 1)) == ([], [1])


def test_malformed_is_retried_on_new_events_age_or_policy() -> None:
    assert _queues(_order(1), _attempt(1, "malformed", 1)) == ([], [])
    assert _queues(_order(1, e=11), _attempt(1, "malformed", 1)) == ([], [1])
    assert _queues(_order(1), _attempt(1, "malformed", 7)) == ([], [1])
    assert _queues(_order(1), _attempt(1, "malformed", 1, policy="broker_evidence_v0")) == ([], [1])


def test_not_found_is_retried_only_on_new_events() -> None:
    assert _queues(_order(1), _attempt(1, "not_found", 30)) == ([], [])
    assert _queues(_order(1, e=11), _attempt(1, "not_found", 30)) == ([], [1])


def test_found_is_retried_on_new_events() -> None:
    held = _order(1, held=True)
    assert _queues(held, _attempt(1, "found", 1)) == ([], [])
    assert _queues(_order(1, e=11, held=True), _attempt(1, "found", 1)) == ([], [1])


def test_a_later_failure_never_hides_the_found_body() -> None:
    # F exists and new events arrived: retried even though the latest attempt is not_found.
    assert _queues(_order(1, e=11, held=True), _attempt(1, "found", 3), _attempt(1, "not_found", 1)) == ([], [1])


def test_the_settling_lookup() -> None:
    # Closed 20 days ago, last asked 15 days ago (5 days after the last event): one settling lookup is due.
    assert _queues(_order(1, last_days_ago=20), _attempt(1, "found", 15)) == ([], [1])
    # Once an attempt 7+ days after the last event exists, it is settled.
    assert _queues(_order(1, last_days_ago=20), _attempt(1, "found", 15), _attempt(1, "found", 10)) == ([], [])
    # Not before 7 days have passed since the last event.
    assert _queues(_order(1, last_days_ago=3), _attempt(1, "found", 2)) == ([], [])
    # A held order is not settling.
    assert _queues(_order(1, last_days_ago=20, held=True), _attempt(1, "found", 15)) == ([], [])


def test_queue_order() -> None:
    orders = [_order(1, opened_days_ago=300), _order(2, opened_days_ago=10), _order(3), _order(4)]
    attempts = [_attempt(3, "http_error", 1), _attempt(4, "http_error", 5)]
    first, retry = lookup_candidates(orders, attempts, NOW)
    assert [o.order_id for o in first] == [2, 1]  # newest order first
    assert [o.order_id for o in retry] == [4, 3]  # oldest latest attempt first


def _ids(prefix: int, n: int) -> list[LedgerOrder]:
    return [_order(prefix + i) for i in range(n)]


def test_plan_caps_and_interleaves() -> None:
    first, retry = _ids(1000, 50), _ids(2000, 20)
    plan = [o.order_id for o in plan_lookups(first, retry)]
    assert len(plan) == 40
    assert sum(1 for o in plan if o >= 2000) == 10
    assert plan[:8] == [2000, 1000, 1001, 1002, 2001, 1003, 1004, 1005]


def test_plan_spills_unused_slots() -> None:
    assert len(plan_lookups(_ids(1000, 50), _ids(2000, 2))) == 40  # 38 first + 2 retries
    plan = plan_lookups(_ids(1000, 5), _ids(2000, 50))
    assert sum(1 for o in plan if o.order_id >= 2000) == 35
    assert len(plan) == 40


def test_a_retry_candidate_is_reached_after_the_attempts_ahead_of_it() -> None:
    # Twelve orders failing every run: each run serves the ten oldest latest attempts, and a served order goes to
    # the back, so no order waits more than two runs.
    orders = _ids(3000, 12)
    attempts = [_attempt(o.order_id, "http_error", 10 - i * 0.1) for i, o in enumerate(orders)]
    served: set[int] = set()
    now_attempts = list(attempts)
    for run in range(2):
        _, retry = lookup_candidates(orders, now_attempts, NOW)
        batch = plan_lookups([], retry)
        served |= {o.order_id for o in batch}
        now_attempts += [_attempt(o.order_id, "http_error", 1 - run * 0.1) for o in batch]
    assert served == {o.order_id for o in orders}
