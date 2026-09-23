"""#2942 attended recommendation window-B release — the act against a real database.

The broker, clock and sender-liveness probe are doubles: the broker answers only the
informational account read and counts every call, the clock's ``sleep`` advances time
instead of waiting ``WINDOW_B_VISIBILITY_WAIT``, and the probe says dead or alive as
each test needs. Everything else — candidacy, the key, the compare-and-set, the audit
rows — is the real SQL.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import psycopg
import pytest
from psycopg import sql

from app.services import recommendation_window_b_release as rwb
from app.services.recommendation_window_b_release import (
    ALREADY_RELEASED,
    INTERNAL_ERROR,
    OUTCOME_INDETERMINATE,
    RECOMMENDATION_RELEASED,
    RECOMMENDATION_WINDOW_B_RULE_VERSION,
    payload_digest,
    release_recommendation_window_b,
)
from app.services.strategy_core_window_b_release import WINDOW_B_VISIBILITY_WAIT, WindowBRefused
from tests.fixtures.ebull_test_db import test_database_url
from tests.fixtures.recommendation_window_b import (
    EXIT_LOT_ID,
    EXIT_UNITS,
    INSTRUMENT_ID,
    PARKED_AT,
    Credentials,
    key_held_elsewhere,
    order_status,
    recommendation_status,
    seed_stranded,
    seed_world,
)

_START = PARKED_AT + timedelta(hours=1)


class _Clock:
    def __init__(self, on_sleep: Callable[[], None] | None = None) -> None:
        self._mono = 1000.0
        self._wall = _START
        self._on_sleep = on_sleep

    def monotonic(self) -> float:
        return self._mono

    def now(self) -> datetime:
        return self._wall

    def sleep(self, seconds: float) -> None:
        if self._on_sleep is not None:
            self._on_sleep()
            self._on_sleep = None
        self._mono += seconds
        self._wall += timedelta(seconds=seconds)


class _Broker:
    """Answers ONLY the account read; any other attribute is a mutation attempt."""

    def __init__(self, payload: Any, clock: _Clock, on_read: Callable[[], None] | None) -> None:
        self._payload = payload
        self._clock = clock
        self._on_read = on_read
        self.reads = 0

    def get_account_risk_snapshot(self) -> Any:
        self.reads += 1
        if self._on_read is not None:
            self._on_read()
        return SimpleNamespace(raw_payload=self._payload, observed_at=self._clock.now())

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"broker mutation attempted: {name}")


def _clean_pnl(action: str) -> dict[str, Any]:
    positions: list[dict[str, Any]] = []
    if action == "EXIT":
        positions.append(
            {
                "positionID": EXIT_LOT_ID,
                "instrumentID": INSTRUMENT_ID,
                "isBuy": True,
                "mirrorID": 0,
                "units": str(EXIT_UNITS),
                "openDateTime": "2026-01-01T00:00:00Z",
            }
        )
    return {"clientPortfolio": {"positions": positions, "ordersForOpen": [], "orders": []}}


def _dead(_pid: int, _sig: int) -> None:
    raise ProcessLookupError


def _alive(_pid: int, _sig: int) -> None:
    return None


def _digest(conn: psycopg.Connection[Any], order_id: int) -> str:
    row = conn.execute(
        "SELECT raw_payload_json, recommendation_park_message FROM orders WHERE order_id=%s", (order_id,)
    ).fetchone()
    conn.commit()
    assert row is not None
    return payload_digest(row[0], row[1])


def _release(
    conn: psycopg.Connection[Any],
    order_id: int,
    *,
    action: str = "BUY",
    digest: str | None = None,
    probe: Callable[[int, int], None] = _dead,
    payload: Any = None,
    clock: _Clock | None = None,
    on_read: Callable[[], None] | None = None,
    factory: Any = None,
) -> tuple[Any, list[_Broker]]:
    clock = clock or _Clock()
    brokers: list[_Broker] = []

    @contextmanager
    def _factory(api: UUID, user: UUID) -> Iterator[Any]:
        broker = _Broker(payload if payload is not None else _clean_pnl(action), clock, on_read)
        brokers.append(broker)
        yield broker

    result = release_recommendation_window_b(
        conn,
        order_id=order_id,
        operator_id="attending-operator",
        attestation="checked the demo portal: no order for this instrument",
        payload_sha256=digest if digest is not None else _digest(conn, order_id),
        broker_factory=factory or _factory,
        environment="demo",
        clock=clock,
        attendance=lambda: None,
        this_host="sender-host",
        this_pid=1,
        liveness_probe=probe,
    )
    return result, brokers


def _audits(conn: psycopg.Connection[Any], order_id: int) -> list[tuple[str, str, int]]:
    rows = conn.execute(
        """
        SELECT pass_fail, model_version, decision_id FROM decision_audit
        WHERE stage = 'recommendation_window_b_release' AND evidence_json->>'order_id' = %s
        ORDER BY decision_id
        """,
        (str(order_id),),
    ).fetchall()
    conn.commit()
    return [(str(r[0]), str(r[1]), int(r[2])) for r in rows]


@pytest.fixture
def creds(ebull_test_conn: psycopg.Connection[Any]) -> Credentials:
    return seed_world(ebull_test_conn)


# ---------------------------------------------------------------------------
# PASS
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("action", ["BUY", "ADD", "EXIT"])
@pytest.mark.parametrize("state", ["W", "U"])
def test_the_pass_matrix(ebull_test_conn: psycopg.Connection[Any], creds: Credentials, state: str, action: str) -> None:
    order_id, rec = seed_stranded(ebull_test_conn, state=state, action=action, credentials=creds)

    result, brokers = _release(ebull_test_conn, order_id, action=action)

    assert (result.passed, result.slug) == (True, RECOMMENDATION_RELEASED), result.evidence
    assert order_status(ebull_test_conn, order_id) == "rejected"
    assert recommendation_status(ebull_test_conn, rec) == "execution_failed"
    assert [(pf, version) for pf, version, _ in _audits(ebull_test_conn, order_id)] == [
        ("PASS", RECOMMENDATION_WINDOW_B_RULE_VERSION)
    ]
    assert [b.reads for b in brokers] == [1]  # one read, zero mutations (_Broker raises on any)
    assert result.evidence["state"] == state
    assert not key_held_elsewhere(test_database_url(), rec)

    again, _ = _release(ebull_test_conn, order_id, action=action)
    assert (again.passed, again.slug, again.decision_id) == (True, ALREADY_RELEASED, result.decision_id)


def test_u_never_consults_sender_death(ebull_test_conn: psycopg.Connection[Any], creds: Credentials) -> None:
    """The executor is the long-lived daemon: a LIVE recorded sender, or none at all,
    must not refuse a U row — the park commit is its sends-ended proof."""
    live, _ = seed_stranded(ebull_test_conn, state="U", credentials=creds)
    unrecorded, _ = seed_stranded(ebull_test_conn, state="U", credentials=creds, sender=None)
    assert _release(ebull_test_conn, live, probe=_alive)[0].passed
    assert _release(ebull_test_conn, unrecorded, probe=_alive)[0].passed


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("state", "seed_kwargs", "probe", "slug"),
    [
        ("W", {}, _alive, "window_b_sender_alive"),
        ("W", {"sender": None}, _dead, "window_b_sender_identity_unrecorded"),
        ("U", {"parked": False}, _dead, "recommendation_release_park_unrecorded"),
        ("U", {"park_message": None}, _dead, "recommendation_release_park_unrecorded"),
        ("W", {"credentials": None}, _dead, "recommendation_release_not_a_candidate"),
        ("W", {"broker_environment": None}, _dead, "recommendation_release_environment_not_demo"),
        ("U", {"broker_environment": "real"}, _dead, "recommendation_release_environment_not_demo"),
        ("W", {"raw_payload": {"orderID": 99}}, _dead, "recommendation_release_payload_has_ref"),
        ("U", {"park_message": "504: {'positionId': 12}"}, _dead, "recommendation_release_payload_has_ref"),
        ("W", {"recommendation_status": "execution_pending"}, _dead, "recommendation_release_not_a_candidate"),
        ("U", {"recommendation_status": "approved"}, _dead, "recommendation_release_not_a_candidate"),
    ],
    ids=lambda v: v if isinstance(v, str) else None,
)
def test_a_missing_proof_refuses_audited_and_changes_nothing(
    ebull_test_conn: psycopg.Connection[Any],
    creds: Credentials,
    state: str,
    seed_kwargs: dict[str, Any],
    probe: Callable[[int, int], None],
    slug: str,
) -> None:
    kwargs: dict[str, Any] = {"credentials": creds, **seed_kwargs}
    order_id, rec = seed_stranded(ebull_test_conn, state=state, **kwargs)
    before = order_status(ebull_test_conn, order_id)

    result, brokers = _release(ebull_test_conn, order_id, probe=probe)

    assert (result.passed, result.slug) == (False, slug)
    assert [pf for pf, _, _ in _audits(ebull_test_conn, order_id)] == ["FAIL"]
    assert order_status(ebull_test_conn, order_id) == before
    assert brokers == []


@pytest.mark.parametrize(
    "statement",
    [
        # recommendation action differs from the order's (request_id is trigger-immutable)
        "UPDATE trade_recommendations SET action = 'EXIT' "
        "WHERE recommendation_id = (SELECT recommendation_id FROM orders WHERE order_id = %s)",
        "UPDATE orders SET action = 'ADD' WHERE order_id = %s",  # order/recommendation action mismatch
        "UPDATE orders SET broker_order_ref = 'X1' WHERE order_id = %s",
    ],
)
def test_candidacy_integrity(ebull_test_conn: psycopg.Connection[Any], creds: Credentials, statement: str) -> None:
    order_id, _ = seed_stranded(ebull_test_conn, state="W", credentials=creds)
    ebull_test_conn.execute(statement, (order_id,))  # type: ignore[arg-type]
    ebull_test_conn.commit()
    assert _release(ebull_test_conn, order_id)[0].slug == "recommendation_release_not_a_candidate"


def test_a_row_changed_between_the_unlocked_and_the_locked_read_refuses(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    order_id, _ = seed_stranded(ebull_test_conn, state="U", credentials=creds)
    real = rwb._holds_recommendation_key

    def _mutate_then_check(conn: psycopg.Connection[Any], rid: int) -> bool:
        with psycopg.connect(test_database_url()) as other:
            other.execute(
                "UPDATE orders SET recommendation_parked_at = recommendation_parked_at + interval '1 s' "
                "WHERE order_id = %s",
                (order_id,),
            )
        return real(conn, rid)

    monkeypatch.setattr(rwb, "_holds_recommendation_key", _mutate_then_check)
    assert _release(ebull_test_conn, order_id)[0].slug == "recommendation_release_candidate_changed"


def test_a_digest_mismatch_refuses_not_inspected(ebull_test_conn: psycopg.Connection[Any], creds: Credentials) -> None:
    order_id, _ = seed_stranded(ebull_test_conn, state="U", credentials=creds)
    shown = _digest(ebull_test_conn, order_id)
    assert (
        _release(ebull_test_conn, order_id, digest="0" * 64)[0].slug == "recommendation_release_payload_not_inspected"
    )
    # The park message changes after --show while the payload does not.
    ebull_test_conn.execute(
        "UPDATE orders SET recommendation_park_message = 'something else' WHERE order_id = %s", (order_id,)
    )
    ebull_test_conn.commit()
    result, _ = _release(ebull_test_conn, order_id, digest=shown)
    assert result.slug == "recommendation_release_payload_not_inspected"
    assert order_status(ebull_test_conn, order_id) == "uncertain"


@pytest.mark.parametrize(
    "mutation",
    [
        "recommendation_parked_at = recommendation_parked_at + interval '1 s'",
        "raw_payload_json = '{\"status\": 503}'::jsonb",
        "created_at = created_at - interval '1 s'",
        "recommendation_park_message = 'changed'",
    ],
)
def test_evidence_changed_during_the_wait_misses_the_cas(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials, mutation: str
) -> None:
    order_id, rec = seed_stranded(ebull_test_conn, state="U", credentials=creds)

    def _mutate() -> None:
        with psycopg.connect(test_database_url()) as other:
            other.execute(sql.SQL("UPDATE orders SET {} WHERE order_id = %s").format(sql.SQL(mutation)), (order_id,))  # type: ignore[arg-type]

    result, _ = _release(ebull_test_conn, order_id, clock=_Clock(on_sleep=_mutate))

    assert result.slug == "recommendation_release_terminal_write_mismatch"
    assert order_status(ebull_test_conn, order_id) == "uncertain"
    assert recommendation_status(ebull_test_conn, rec) == "execution_pending"
    assert [pf for pf, _, _ in _audits(ebull_test_conn, order_id)] == ["FAIL"]


def test_a_moved_recommendation_rolls_back_the_order_update_too(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials
) -> None:
    order_id, rec = seed_stranded(ebull_test_conn, state="W", credentials=creds)

    def _move() -> None:
        with psycopg.connect(test_database_url()) as other:
            other.execute("UPDATE trade_recommendations SET status='cancelled' WHERE recommendation_id=%s", (rec,))

    result, _ = _release(ebull_test_conn, order_id, on_read=_move)

    assert result.slug == "recommendation_release_terminal_write_mismatch"
    assert order_status(ebull_test_conn, order_id) == "submitted"


def test_a_busy_key_and_a_caller_holding_it_both_refuse(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials
) -> None:
    order_id, rec = seed_stranded(ebull_test_conn, state="W", credentials=creds)
    with psycopg.connect(test_database_url(), autocommit=True) as other:
        other.execute("SELECT pg_advisory_lock(2942, %s)", (rec,))
        assert _release(ebull_test_conn, order_id)[0].slug == "recommendation_release_lock_busy"
    ebull_test_conn.execute("SELECT pg_advisory_lock(2942, %s)", (rec,))
    ebull_test_conn.commit()
    try:
        assert _release(ebull_test_conn, order_id)[0].slug == "recommendation_release_caller_holds_key"
    finally:
        ebull_test_conn.execute("SELECT pg_advisory_unlock(2942, %s)", (rec,))
        ebull_test_conn.commit()
    assert order_status(ebull_test_conn, order_id) == "submitted"


def test_the_witness_is_read_after_both_deadlines_with_the_key_held_and_the_connection_idle(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials
) -> None:
    order_id, rec = seed_stranded(ebull_test_conn, state="U", credentials=creds)
    clock = _Clock()
    seen: dict[str, Any] = {}

    def _observe() -> None:
        seen["elapsed"] = clock.monotonic() - 1000.0
        seen["wall"] = clock.now()
        seen["key_held"] = key_held_elsewhere(test_database_url(), rec)
        seen["idle"] = ebull_test_conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE

    assert _release(ebull_test_conn, order_id, clock=clock, on_read=_observe)[0].passed
    assert seen["elapsed"] >= WINDOW_B_VISIBILITY_WAIT.total_seconds()
    assert seen["wall"] >= PARKED_AT + WINDOW_B_VISIBILITY_WAIT
    assert seen["key_held"] and seen["idle"]


def test_a_witness_finding_refuses(ebull_test_conn: psycopg.Connection[Any], creds: Credentials) -> None:
    order_id, _ = seed_stranded(ebull_test_conn, state="W", action="EXIT", credentials=creds)
    gone = {"clientPortfolio": {"positions": [], "ordersForOpen": [], "orders": []}}
    assert _release(ebull_test_conn, order_id, action="EXIT", payload=gone)[0].slug == (
        "recommendation_release_exit_lot_gone"
    )


def test_a_factory_refusal_is_reported_under_the_recommendation_slug(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials
) -> None:
    order_id, _ = seed_stranded(ebull_test_conn, state="W", credentials=creds)

    @contextmanager
    def _unresolved(api: UUID, user: UUID) -> Iterator[Any]:
        raise WindowBRefused("window_b_credentials_unresolved", "revoked")
        yield  # pragma: no cover

    result, _ = _release(ebull_test_conn, order_id, factory=_unresolved)
    assert result.slug == "recommendation_release_credentials_unresolved"


# ---------------------------------------------------------------------------
# Unexpected exceptions, the terminal COMMIT and cleanup
# ---------------------------------------------------------------------------


def test_an_evaluator_exception_is_an_audited_internal_error(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    order_id, _ = seed_stranded(ebull_test_conn, state="W", credentials=creds)

    def _boom(*_a: Any, **_k: Any) -> Any:
        raise KeyError("evaluator bug")

    monkeypatch.setattr(rwb, "evaluate_window_b_witness", _boom)
    result, _ = _release(ebull_test_conn, order_id)
    assert result.slug == INTERNAL_ERROR
    assert [pf for pf, _, _ in _audits(ebull_test_conn, order_id)] == ["FAIL"]
    assert order_status(ebull_test_conn, order_id) == "submitted"


def test_a_pass_insert_exception_rolls_back_both_updates(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    order_id, rec = seed_stranded(ebull_test_conn, state="U", credentials=creds)
    real = rwb._write_audit

    def _fail_pass(conn: psycopg.Connection[Any], **kwargs: Any) -> int:
        if kwargs["passed"]:
            raise RuntimeError("PASS insert failed")
        return real(conn, **kwargs)

    monkeypatch.setattr(rwb, "_write_audit", _fail_pass)
    result, _ = _release(ebull_test_conn, order_id)
    assert result.slug == INTERNAL_ERROR
    assert order_status(ebull_test_conn, order_id) == "uncertain"
    assert recommendation_status(ebull_test_conn, rec) == "execution_pending"
    assert [pf for pf, _, _ in _audits(ebull_test_conn, order_id)] == ["FAIL"]


def test_a_best_effort_fail_row_that_cannot_be_written_still_returns_the_slug(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    order_id, _ = seed_stranded(ebull_test_conn, state="W", credentials=creds)

    def _no_audit(*_a: Any, **_k: Any) -> int:
        raise RuntimeError("audit INSERT failed")

    monkeypatch.setattr(rwb, "_write_audit", _no_audit)
    result, _ = _release(ebull_test_conn, order_id, probe=_alive)
    assert (result.passed, result.slug, result.decision_id) == (False, "window_b_sender_alive", None)
    assert result.evidence["order_id"] == order_id


class _CommitProxy:
    """Wraps the real connection; the terminal COMMIT (the one after the PASS
    INSERT) raises, before or after the server commits."""

    def __init__(self, conn: psycopg.Connection[Any], *, after_server_commit: bool | None, unlock_raises: bool) -> None:
        self._conn = conn
        self._after = after_server_commit
        self._unlock_raises = unlock_raises
        self._pass_written = False
        self.fired = False

    def execute(self, query: Any, params: Any = None) -> Any:
        text = str(query)
        if "INSERT INTO decision_audit" in text and params is not None and "PASS" in params:
            self._pass_written = True
        if self._unlock_raises and self.fired and "pg_advisory_unlock" in text:
            raise psycopg.OperationalError("unlock failed")
        return self._conn.execute(query, params)

    def commit(self) -> None:
        if self._pass_written and not self.fired:
            self.fired = True
            if self._after is None:
                self._conn.commit()
                return
            if self._after:
                self._conn.commit()
            raise psycopg.OperationalError("connection lost during COMMIT")
        self._conn.commit()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


@pytest.mark.parametrize("after_server_commit", [True, False])
def test_a_terminal_commit_failure_is_indeterminate_and_a_rerun_learns_it(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials, after_server_commit: bool
) -> None:
    order_id, _ = seed_stranded(ebull_test_conn, state="U", credentials=creds)
    proxy = _CommitProxy(ebull_test_conn, after_server_commit=after_server_commit, unlock_raises=False)

    result, _ = _release(proxy, order_id)  # type: ignore[arg-type]

    assert (result.passed, result.slug) == (False, OUTCOME_INDETERMINATE)
    # No FAIL row for an indeterminate outcome: it may have committed.
    assert [pf for pf, _, _ in _audits(ebull_test_conn, order_id)] == (["PASS"] if after_server_commit else [])
    rerun, _ = _release(ebull_test_conn, order_id)
    assert rerun.passed
    assert rerun.slug == (ALREADY_RELEASED if after_server_commit else RECOMMENDATION_RELEASED)


def test_a_cleanup_failure_after_an_indeterminate_commit_stays_indeterminate(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials
) -> None:
    order_id, _ = seed_stranded(ebull_test_conn, state="U", credentials=creds)
    proxy = _CommitProxy(ebull_test_conn, after_server_commit=True, unlock_raises=True)
    result, _ = _release(proxy, order_id)  # type: ignore[arg-type]
    assert result.slug == OUTCOME_INDETERMINATE


def test_a_committed_release_whose_unlock_fails_is_still_a_pass(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials
) -> None:
    order_id, rec = seed_stranded(ebull_test_conn, state="U", credentials=creds)
    proxy = _CommitProxy(ebull_test_conn, after_server_commit=None, unlock_raises=True)

    result, _ = _release(proxy, order_id)  # type: ignore[arg-type]

    assert (result.passed, result.slug) == (True, RECOMMENDATION_RELEASED)
    assert ebull_test_conn.closed  # the helper closed it, so no key survives
    assert not key_held_elsewhere(test_database_url(), rec)
    with psycopg.connect(test_database_url()) as other:
        row = other.execute("SELECT status FROM orders WHERE order_id=%s", (order_id,)).fetchone()
    assert row == ("rejected",)


def test_the_real_environment_refuses_before_any_read(ebull_test_conn: psycopg.Connection[Any]) -> None:
    result = release_recommendation_window_b(
        ebull_test_conn,
        order_id=1,
        operator_id="x",
        attestation="y",
        payload_sha256="0" * 64,
        broker_factory=lambda *_: None,  # type: ignore[arg-type,return-value]
        environment="real",
        attendance=lambda: None,
    )
    assert result.slug == "recommendation_release_environment_not_demo"
    assert result.decision_id is None


@pytest.mark.parametrize("defect", ["revoked", "swapped_labels", "mixed_operators", "real_environment"])
def test_the_recommendation_factory_refuses_a_pair_that_is_not_one_live_demo_account(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials, monkeypatch: pytest.MonkeyPatch, defect: str
) -> None:
    from uuid import UUID as _UUID

    from app.config import settings
    from scripts.release_recommendation_window_b import _recommendation_account_broker

    monkeypatch.setattr(settings, "database_url", test_database_url())
    api, user = creds.api, creds.user
    if defect == "revoked":
        ebull_test_conn.execute("UPDATE broker_credentials SET revoked_at = now() WHERE id = %s", (user,))
    elif defect == "swapped_labels":
        api, user = user, api
    elif defect == "mixed_operators":
        other = seed_world(ebull_test_conn, operator_id=_UUID("2942a0b0-0000-4000-8000-00000000beef"))
        user = other.user
    else:
        ebull_test_conn.execute("UPDATE broker_credentials SET environment = 'real' WHERE id = %s", (api,))
    ebull_test_conn.commit()

    with pytest.raises(WindowBRefused) as raised, _recommendation_account_broker(api, user):
        pass
    assert raised.value.slug == "recommendation_release_credentials_unresolved"
