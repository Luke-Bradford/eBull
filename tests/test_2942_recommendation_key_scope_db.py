"""#2942 spec Deltas 3-4 on the executor side, against a real database.

What only real Postgres can show: that the per-recommendation key is held from
before the checks to the step-4/5 COMMIT (another backend sees it), that the
branches which take no claim refuse to overtake one, that the claim INSERT records
and gates on the account's credential ids, that rotation refuses while a claim needs
the account, and that a lock-path failure never leaves a session key behind.

``execute_order``'s environment (runtime config, kill switch, safety layers, cost
provenance, post-trade enqueue) is stubbed as ``tests/test_order_client.py`` does;
the SQL under test is real.
"""

from __future__ import annotations

import os
import socket
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.providers.broker import BrokerOrderResult, BrokerOrderSubmissionUncertain, OrderStatus
from app.services import order_client
from app.services.broker_credentials import CredentialInUse, revoke_credential
from app.services.order_client import (
    ConcurrentSubmissionInFlightError,
    PriorSubmissionUnresolvedError,
    RecommendationNoLongerApprovedError,
    SubmissionControlsRevokedError,
    _park_uncertain_submission,
    _persist_submitted_intent,
    _recommendation_submission_try_lock,
    execute_order,
    mark_recommendation_submission_entered,
)
from app.services.runtime_config import RuntimeConfig
from tests.fixtures.ebull_test_db import test_database_url
from tests.fixtures.recommendation_window_b import (
    INSTRUMENT_ID,
    OPERATOR_ID,
    Credentials,
    key_held_elsewhere,
    recommendation_status,
    seed_decision,
    seed_recommendation,
    seed_stranded,
    seed_world,
)

_NOW = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)


def _runtime(live: bool) -> RuntimeConfig:
    return RuntimeConfig(
        enable_auto_trading=True,
        enable_live_trading=live,
        display_currency="USD",
        llm_provider="openai_compatible",
        llm_base_url="http://localhost:11434/v1",
        llm_model_writer="qwen3:14b",
        llm_model_critic="qwen3:14b",
        updated_at=_NOW,
        updated_by="test",
        reason="test",
    )


@pytest.fixture
def creds(ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch) -> Credentials:
    monkeypatch.setattr(order_client, "load_kill_switch", lambda _c: {"is_active": False})
    monkeypatch.setattr(order_client, "_assert_safety_layers_enabled_for_buy_add", lambda _c, _a: None)
    monkeypatch.setattr(order_client, "_assert_transaction_cost_complete_for_buy_add", lambda _c, _a, _i: None)
    monkeypatch.setattr(order_client, "enqueue_post_trade_sync", lambda *_a, **_k: None)
    return seed_world(ebull_test_conn)


def _set_live(monkeypatch: pytest.MonkeyPatch, live: bool) -> None:
    monkeypatch.setattr(order_client, "get_runtime_config", lambda _c: _runtime(live))


class _Broker:
    def __init__(self, status: OrderStatus, ref: str | None = None) -> None:
        self.result = BrokerOrderResult(
            broker_order_ref=ref, status=status, filled_price=None, filled_units=None, fees=Decimal("0"), raw_payload={}
        )
        self.calls: list[str] = []
        self.raise_on_call: Exception | None = None

    def place_order(self, **_kwargs: Any) -> BrokerOrderResult:
        self.calls.append("place_order")
        if self.raise_on_call is not None:
            raise self.raise_on_call
        return self.result

    def close_position(self, *_a: Any, **_k: Any) -> BrokerOrderResult:
        self.calls.append("close_position")
        return self.result


def _execute(conn: Any, rec: int, *, broker: _Broker | None, creds: Credentials | None) -> Any:
    decision = seed_decision(conn)
    return execute_order(
        conn,
        recommendation_id=rec,
        decision_id=decision,
        broker=broker,  # type: ignore[arg-type]
        broker_env="demo" if broker is not None else None,
        broker_credential_ids=None if creds is None else (creds.api, creds.user),
    )


def _orders_for(rec: int) -> list[tuple[int, str]]:
    with psycopg.connect(test_database_url()) as other:
        rows = other.execute(
            "SELECT order_id, status FROM orders WHERE recommendation_id=%s ORDER BY order_id", (rec,)
        ).fetchall()
    return [(int(r[0]), str(r[1])) for r in rows]


def _refusal_audits(conn: psycopg.Connection[Any], rec: int) -> list[str]:
    rows = conn.execute(
        "SELECT evidence_json->>'refusal' FROM decision_audit WHERE recommendation_id=%s AND pass_fail='FAIL' "
        "ORDER BY decision_id",
        (rec,),
    ).fetchall()
    conn.commit()
    return [str(r[0]) for r in rows]


# ---------------------------------------------------------------------------
# The key spans acquisition -> step-4/5 COMMIT
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("a_status", "b_error"),
    [("rejected", RecommendationNoLongerApprovedError), ("pending", PriorSubmissionUnresolvedError)],
)
def test_the_key_spans_persistence_and_a_stale_executor_cannot_resubmit(
    ebull_test_conn: psycopg.Connection[Any],
    creds: Credentials,
    monkeypatch: pytest.MonkeyPatch,
    a_status: OrderStatus,
    b_error: type[Exception],
) -> None:
    _set_live(monkeypatch, True)
    rec = seed_recommendation(ebull_test_conn)
    stale_row = order_client._load_approved_recommendation(ebull_test_conn, rec)
    ebull_test_conn.commit()
    b_busy: list[Exception] = []
    real_update = order_client._update_order_with_broker_result

    def _during_a_persistence(conn: Any, **kwargs: Any) -> None:
        # A's provider call has returned; its step-4/5 has not committed. B, on
        # another backend, must refuse busy rather than claim after A's lift.
        with psycopg.connect(test_database_url()) as b_conn:
            try:
                _execute(b_conn, rec, broker=_Broker("filled"), creds=creds)
            except ConcurrentSubmissionInFlightError as exc:
                b_busy.append(exc)
        real_update(conn, **kwargs)

    monkeypatch.setattr(order_client, "_update_order_with_broker_result", _during_a_persistence)
    result = _execute(
        ebull_test_conn, rec, broker=_Broker(a_status, "REF-A" if a_status == "pending" else None), creds=creds
    )
    monkeypatch.setattr(order_client, "_update_order_with_broker_result", real_update)

    assert len(b_busy) == 1
    assert _orders_for(rec) == [(result.order_id, a_status)]  # visible from another backend
    assert not key_held_elsewhere(test_database_url(), rec)

    # B paused after reading 'approved' at step 1 and reaches the key only now.
    monkeypatch.setattr(order_client, "_load_approved_recommendation", lambda _c, _r: stale_row)
    b_broker = _Broker("filled")
    with psycopg.connect(test_database_url()) as b_conn, pytest.raises(b_error):
        _execute(b_conn, rec, broker=b_broker, creds=creds)
    assert b_broker.calls == []
    assert len(_orders_for(rec)) == 1


def test_a_synthetic_success_is_committed_before_the_key_is_released(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_live(monkeypatch, False)
    rec = seed_recommendation(ebull_test_conn)
    seen: list[list[tuple[int, str]]] = []
    real_execute = ebull_test_conn.execute

    def _spy(query: Any, params: Any = None, **kw: Any) -> Any:
        if "pg_advisory_unlock" in str(query):
            seen.append(_orders_for(rec))
        return real_execute(query, params, **kw)

    monkeypatch.setattr(ebull_test_conn, "execute", _spy)
    result = _execute(ebull_test_conn, rec, broker=None, creds=None)
    # The LAST unlock is the outer key (the first is the terminaliser's nested one).
    # No quote: the synthetic result persists with zero units, outcome `failed`.
    assert [order_id for order_id, _ in seen[-1]] == [result.order_id]
    assert result.outcome == "failed"
    assert recommendation_status(ebull_test_conn, rec) == "execution_failed"


def test_a_live_no_lot_exit_is_committed_before_the_key_is_released(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_live(monkeypatch, True)
    rec = seed_recommendation(ebull_test_conn, action="EXIT")
    seen: list[list[tuple[int, str]]] = []
    real_execute = ebull_test_conn.execute

    def _spy(query: Any, params: Any = None, **kw: Any) -> Any:
        if "pg_advisory_unlock" in str(query):
            seen.append(_orders_for(rec))
        return real_execute(query, params, **kw)

    monkeypatch.setattr(ebull_test_conn, "execute", _spy)
    broker = _Broker("filled")
    result = _execute(ebull_test_conn, rec, broker=broker, creds=creds)
    assert broker.calls == []
    assert seen[-1] == [(result.order_id, "failed")]


def test_a_no_lot_cas_miss_rolls_back_its_order_row(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_live(monkeypatch, True)
    rec = seed_recommendation(ebull_test_conn, action="EXIT")

    def _status_moves(_conn: Any, _iid: int) -> int:
        with psycopg.connect(test_database_url()) as other:
            other.execute("UPDATE trade_recommendations SET status='cancelled' WHERE recommendation_id=%s", (rec,))
        return 0

    monkeypatch.setattr(order_client, "_engine_owned_long_lot_count", _status_moves)
    with pytest.raises(RecommendationNoLongerApprovedError):
        _execute(ebull_test_conn, rec, broker=_Broker("filled"), creds=creds)
    assert _orders_for(rec) == []
    assert _refusal_audits(ebull_test_conn, rec) == ["recommendation_no_longer_approved"]


# ---------------------------------------------------------------------------
# Overtaking: the claim-less branches respect an outstanding claim
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("state", ["W", "U"])
@pytest.mark.parametrize("branch", ["synthetic", "live_no_lot_exit"])
def test_a_claimless_branch_never_overtakes_an_outstanding_attempt(
    ebull_test_conn: psycopg.Connection[Any],
    creds: Credentials,
    monkeypatch: pytest.MonkeyPatch,
    state: str,
    branch: str,
) -> None:
    live = branch == "live_no_lot_exit"
    _set_live(monkeypatch, live)
    action = "EXIT" if live else "BUY"
    order_id, rec = seed_stranded(ebull_test_conn, state=state, action=action, credentials=creds)
    if state == "U":
        # A U recommendation is execution_pending, so only an executor that read
        # 'approved' before the park can reach the check.
        stale = {
            "recommendation_id": rec,
            "instrument_id": INSTRUMENT_ID,
            "action": action,
            "target_entry": None,
            "suggested_size_pct": None,
            "model_version": None,
            "status": "approved",
            "stop_loss_rate": None,
            "take_profit_rate": None,
        }
        monkeypatch.setattr(order_client, "_load_approved_recommendation", lambda _c, _r: stale)

    with pytest.raises(PriorSubmissionUnresolvedError):
        _execute(ebull_test_conn, rec, broker=_Broker("filled") if live else None, creds=creds if live else None)

    assert _orders_for(rec) == [(order_id, "submitted" if state == "W" else "uncertain")]
    assert _refusal_audits(ebull_test_conn, rec)[-1] == "prior_submission_unresolved"


@pytest.mark.parametrize("live", [False, True])
def test_a_key_held_elsewhere_refuses_busy_on_every_branch(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials, monkeypatch: pytest.MonkeyPatch, live: bool
) -> None:
    _set_live(monkeypatch, live)
    rec = seed_recommendation(ebull_test_conn, action="EXIT")
    with psycopg.connect(test_database_url(), autocommit=True) as other:
        other.execute("SELECT pg_advisory_lock(2942, %s)", (rec,))
        with pytest.raises(ConcurrentSubmissionInFlightError):
            _execute(ebull_test_conn, rec, broker=_Broker("filled") if live else None, creds=creds if live else None)
    assert _orders_for(rec) == []


def test_synthetic_mode_releases_a_stranded_window_a_row_and_proceeds(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_live(monkeypatch, False)
    rec = seed_recommendation(ebull_test_conn)
    stranded, _ = _persist_submitted_intent(
        ebull_test_conn,
        instrument_id=INSTRUMENT_ID,
        recommendation_id=rec,
        decision_id=seed_decision(ebull_test_conn),
        action="BUY",
        requested_amount=None,
        requested_units=None,
        broker_env="demo",
        order_params=None,
        exit_lot=None,
        now=_NOW,
    )
    ebull_test_conn.commit()

    result = _execute(ebull_test_conn, rec, broker=None, creds=None)

    assert [row[0] for row in _orders_for(rec)] == [stranded, result.order_id]
    assert _orders_for(rec)[0] == (stranded, "refused")


# ---------------------------------------------------------------------------
# What the attempt records (sql/412)
# ---------------------------------------------------------------------------


def test_the_claim_records_the_account_and_the_marker_and_park_record_their_evidence(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials
) -> None:
    rec = seed_recommendation(ebull_test_conn)
    order_id, _ = _persist_submitted_intent(
        ebull_test_conn,
        instrument_id=INSTRUMENT_ID,
        recommendation_id=rec,
        decision_id=seed_decision(ebull_test_conn),
        action="BUY",
        requested_amount=None,
        requested_units=None,
        broker_env="demo",
        order_params=None,
        exit_lot=None,
        now=_NOW,
        broker_credential_ids=(creds.api, creds.user),
    )
    ebull_test_conn.commit()
    mark_recommendation_submission_entered(ebull_test_conn, order_id=order_id)
    _park_uncertain_submission(
        ebull_test_conn,
        order_id=order_id,
        instrument_id=INSTRUMENT_ID,
        recommendation_id=rec,
        payload={"status": 504},
        message="HTTP 504",
        now=_NOW,
    )
    row = ebull_test_conn.execute(
        """
        SELECT recommendation_api_key_credential_id, recommendation_user_key_credential_id,
               recommendation_submission_entered_at IS NOT NULL, recommendation_submission_entered_pid,
               recommendation_submission_entered_host, recommendation_parked_at IS NOT NULL,
               recommendation_park_message, status
        FROM orders WHERE order_id = %s
        """,
        (order_id,),
    ).fetchone()
    ebull_test_conn.commit()
    assert row == (creds.api, creds.user, True, os.getpid(), socket.gethostname(), True, "HTTP 504", "uncertain")


@pytest.mark.parametrize("site", ["place_order_uncertain", "place_order_unexpected"])
def test_both_park_arms_persist_the_exception_text(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials, monkeypatch: pytest.MonkeyPatch, site: str
) -> None:
    _set_live(monkeypatch, True)
    rec = seed_recommendation(ebull_test_conn)
    broker = _Broker("filled")
    if site == "place_order_uncertain":
        broker.raise_on_call = BrokerOrderSubmissionUncertain("HTTP 503", raw_payload={"status": 503})
    else:
        broker.raise_on_call = ValueError("decoder exploded")
    with pytest.raises(order_client.BrokerSubmissionUncertainError):
        _execute(ebull_test_conn, rec, broker=broker, creds=creds)
    row = ebull_test_conn.execute(
        "SELECT status, recommendation_park_message, recommendation_parked_at IS NOT NULL, raw_payload_json "
        "FROM orders WHERE recommendation_id=%s",
        (rec,),
    ).fetchone()
    ebull_test_conn.commit()
    assert row is not None and row[0] == "uncertain" and row[2] is True
    assert row[1] == str(broker.raise_on_call)
    if site == "place_order_unexpected":
        assert row[3]["exception"] == "ValueError" and row[3]["str"] == "decoder exploded"


def test_a_claim_against_a_revoked_credential_refuses_before_io(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_live(monkeypatch, True)
    rec = seed_recommendation(ebull_test_conn)
    ebull_test_conn.execute("UPDATE broker_credentials SET revoked_at = now() WHERE id = %s", (creds.user,))
    ebull_test_conn.commit()
    broker = _Broker("filled")
    with pytest.raises(SubmissionControlsRevokedError):
        _execute(ebull_test_conn, rec, broker=broker, creds=creds)
    assert broker.calls == []
    assert _orders_for(rec) == []
    assert _refusal_audits(ebull_test_conn, rec) == ["broker_credential_revoked"]


def test_rotation_refuses_while_a_recommendation_claim_needs_the_account(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials
) -> None:
    order_id, _ = seed_stranded(ebull_test_conn, state="U", credentials=creds)
    for credential_id in (creds.api, creds.user):
        with pytest.raises(CredentialInUse), ebull_test_conn.transaction():
            revoke_credential(ebull_test_conn, credential_id=credential_id, operator_id=OPERATOR_ID)
    ebull_test_conn.execute("UPDATE orders SET status='rejected' WHERE order_id=%s", (order_id,))
    ebull_test_conn.commit()
    with ebull_test_conn.transaction():
        revoke_credential(ebull_test_conn, credential_id=creds.api, operator_id=OPERATOR_ID)


# ---------------------------------------------------------------------------
# The key helper never leaks a session lock
# ---------------------------------------------------------------------------


class _Failing:
    """Proxy that raises at one named lock-path site."""

    def __init__(self, conn: psycopg.Connection[Any], site: str) -> None:
        self._conn = conn
        self._site = site

    def execute(self, query: Any, params: Any = None) -> Any:
        text = str(query)
        if "pg_try_advisory_lock" in text:
            if self._site == "acquire_execute":
                raise psycopg.OperationalError("acquire execute")
            cursor = self._conn.execute(query, params)
            if self._site == "acquire_fetchone":
                # The key is taken server-side before the result is consumed.
                raise psycopg.OperationalError("acquire fetchone")
            return cursor
        if "pg_advisory_unlock" in text and self._site == "unlock":
            raise psycopg.OperationalError("unlock")
        return self._conn.execute(query, params)

    def commit(self) -> None:
        if self._site == "acquire_commit" and not getattr(self, "_committed", False):
            self._committed = True
            raise psycopg.OperationalError("acquire commit")
        self._conn.commit()

    def rollback(self) -> None:
        if self._site == "cleanup_rollback":
            raise psycopg.OperationalError("cleanup rollback")
        self._conn.rollback()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


@pytest.mark.parametrize(
    "site", ["acquire_execute", "acquire_fetchone", "acquire_commit", "cleanup_rollback", "unlock"]
)
def test_a_lock_path_failure_closes_the_connection_and_leaves_no_key(
    ebull_test_conn: psycopg.Connection[Any], site: str
) -> None:
    rec = 29_420_001
    proxy = _Failing(ebull_test_conn, site)
    body_error = LookupError("the body's own failure")
    with pytest.raises(Exception) as raised:  # noqa: PT011 - which one is the assertion
        with _recommendation_submission_try_lock(proxy, rec) as acquired:  # type: ignore[arg-type]
            assert acquired
            ebull_test_conn.execute("SELECT 1")  # leave a read open so cleanup rolls back
            raise body_error
    if site.startswith("acquire"):
        assert isinstance(raised.value, psycopg.OperationalError)
    else:
        assert raised.value is body_error
    assert ebull_test_conn.closed
    assert not key_held_elsewhere(test_database_url(), rec)


def test_a_committed_execution_whose_unlock_fails_returns_its_result(
    ebull_test_conn: psycopg.Connection[Any], creds: Credentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_live(monkeypatch, False)
    rec = seed_recommendation(ebull_test_conn)
    decision = seed_decision(ebull_test_conn)
    real_execute = ebull_test_conn.execute
    unlocks = {"n": 0}

    def _unlock_fails_last(query: Any, params: Any = None, **kw: Any) -> Any:
        if "pg_advisory_unlock" in str(query):
            unlocks["n"] += 1
            if unlocks["n"] == 2:  # the outer key; the first is the terminaliser's nested one
                raise psycopg.OperationalError("unlock failed")
        return real_execute(query, params, **kw)

    monkeypatch.setattr(ebull_test_conn, "execute", _unlock_fails_last)
    result = execute_order(ebull_test_conn, recommendation_id=rec, decision_id=decision)

    assert result.outcome == "failed"  # no quote: a persisted synthetic failure, not an exception
    assert ebull_test_conn.closed
    assert [row[0] for row in _orders_for(rec)] == [result.order_id]  # committed, visible elsewhere
    assert not key_held_elsewhere(test_database_url(), rec)
