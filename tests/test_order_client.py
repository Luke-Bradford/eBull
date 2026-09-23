"""
Tests for app.services.order_client.

Structure:
  - TestSyntheticFill         — _synthetic_fill pure logic
  - TestLoadApprovedRec       — _load_approved_recommendation validation
  - TestExecuteOrderDemoMode  — full execute_order in demo mode (no broker)
  - TestExecuteOrderLiveMode  — execute_order with mocked broker provider
  - TestExecuteOrderFailures  — error paths: rejected, failed, missing broker

Mock DB approach mirrors test_execution_guard.py:
  - _make_cursor(rows) builds a context-manager cursor mock
  - _make_conn(cursors) builds a connection mock
  - conn.transaction() is a no-op context manager

Cursor call order inside execute_order (demo BUY with suggested_size_pct):
  1. _load_approved_recommendation  — fetchone
  2. _load_cash                     — fetchone
  3. _load_quote_for_execution      — fetchone (last/bid/ask/spread_pct)
  4. _persist_order                 — fetchone (INSERT RETURNING)
  5. get_transaction_cost_config    — fetchone
  6. load_instrument_cost           — fetchone
  7. record_estimated_cost          — cursor (INSERT, no fetchone)
  8. _persist_fill                  — fetchone (INSERT RETURNING)
  9. conn.execute x5                — position upsert, broker_positions, cash_ledger, rec status, audit

  When spread data is unavailable (no cost_model, no quote spread_pct),
  cursor 7 (record_estimated_cost) is skipped — s_bps is None.

Cursor call order inside execute_order (demo EXIT):
  1. _load_approved_recommendation  — fetchone
  2. _load_position_units           — fetchone
  3. _load_quote_for_execution      — fetchone (last/bid/ask/spread_pct)
  4. _persist_order                 — fetchone (INSERT RETURNING)
  5. _persist_fill                  — fetchone (INSERT RETURNING)
  6. conn.execute x4                — position update, cash_ledger, rec status, audit

  Cost recording (steps 5-7 in BUY sequence) is BUY/ADD only — skipped for EXIT.

Live mode BUY cursor sequences are similar but without step 3 in the main
path (no _load_quote_for_execution for the fill). Instead, when
cost_model_row is None, _load_quote_for_execution is called in the
cost-recording fallback path (between steps 6 and 7).
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch
from uuid import UUID

import psycopg
import pytest
from psycopg.pq import TransactionStatus

from app.providers.broker import BrokerOrderResult, BrokerOrderSubmissionUncertain, OrderParams
from app.security.unattended_guard import UnattendedExecutionRefused
from app.services.order_client import (
    _OUTSTANDING_CLAIM_SQL,
    _RECOMMENDATION_STATUS_SQL,
    BrokerSubmissionUncertainError,
    PriorSubmissionUnresolvedError,
    RecommendationNoLongerApprovedError,
    SubmissionControlsRevokedError,
    _load_approved_recommendation,
    _load_exit_lot,
    _load_latest_quote_price,
    _load_position_units,
    _persist_broker_position,
    _synthetic_fill,
    _update_position_buy,
    describe_exit_completion,
    execute_order,
    pending_order_verdict,
)
from app.services.runtime_config import RuntimeConfig, RuntimeConfigCorrupt

# ---------------------------------------------------------------------------
# Cost-related test constants
# ---------------------------------------------------------------------------

_DEFAULT_COST_CONFIG: dict[str, Any] = {
    "max_total_cost_bps": Decimal("150"),
    "min_return_vs_cost_ratio": Decimal("3.0"),
    "default_hold_days": 90,
}

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)

_RUNTIME_DEMO = RuntimeConfig(
    enable_auto_trading=True,
    enable_live_trading=False,
    display_currency="USD",
    llm_provider="openai_compatible",
    llm_base_url="http://localhost:11434/v1",
    llm_model_writer="qwen3:14b",
    llm_model_critic="qwen3:14b",
    updated_at=_NOW,
    updated_by="test",
    reason="test",
)

_RUNTIME_LIVE = RuntimeConfig(
    enable_auto_trading=True,
    enable_live_trading=True,
    display_currency="USD",
    llm_provider="openai_compatible",
    llm_base_url="http://localhost:11434/v1",
    llm_model_writer="qwen3:14b",
    llm_model_critic="qwen3:14b",
    updated_at=_NOW,
    updated_by="test",
    reason="test",
)


@pytest.fixture(autouse=True)
def _patch_runtime_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default every test in this file to demo mode.

    Tests that need live mode override via monkeypatch within the test body.
    Tests that need a corrupt-config failure raise RuntimeConfigCorrupt.
    """
    monkeypatch.setattr(
        "app.services.order_client.get_runtime_config",
        lambda _conn: _RUNTIME_DEMO,
    )
    monkeypatch.setattr(
        "app.services.order_client._assert_transaction_cost_complete_for_buy_add",
        lambda _conn, _action, _instrument_id: None,
    )


@pytest.fixture(autouse=True)
def _patch_kill_switch(monkeypatch: pytest.MonkeyPatch) -> None:
    """#2943: the submission-control re-check opens its own cursor for the
    kill_switch row, which would consume a mock conn from every sequenced
    cursor list in this file. Stub it inactive by default; the dedicated
    refusal tests below override it.
    """
    monkeypatch.setattr(
        "app.services.order_client.load_kill_switch",
        lambda _conn: {"is_active": False, "activated_at": None, "reason": None},
    )


@pytest.fixture(autouse=True)
def _stub_post_trade_enqueue() -> Iterator[MagicMock]:
    """#1593: enqueue_post_trade_sync opens its own cursor, which would
    exhaust the sequenced mock conns here. Patched at point of use;
    the enqueue contract is asserted in TestPostTradeEnqueue."""
    with patch("app.services.order_client.enqueue_post_trade_sync") as stub:
        yield stub


_BROKER_ENV = "demo"


_BROKER_CREDENTIAL_IDS = (UUID("00000000-0000-0000-0000-00000000a01d"), UUID("00000000-0000-0000-0000-00000000b01d"))


def _execute(conn: Any, **kwargs: Any) -> Any:
    """``execute_order`` with this suite's broker environment and account ids.

    The live path REFUSES a submission whose broker environment is not recorded
    (#3189 finding 4b): the poller fails closed on an unrecorded environment, so
    such an order could never be reconciled. Defaulting it once here keeps each
    test's own subject visible; the test that is ABOUT the refusal omits it.
    """
    kwargs.setdefault("broker_env", _BROKER_ENV)
    kwargs.setdefault("broker_credential_ids", _BROKER_CREDENTIAL_IDS)
    return execute_order(conn, **kwargs)


def _make_cursor(rows: list[dict[str, Any]]) -> MagicMock:
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cur.fetchone.return_value = rows[0] if rows else None
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    return cur


def _exit_lot_lock_cursor(units: float | Decimal = 1000.0) -> MagicMock:
    """#3020: the ``SELECT units ... FOR UPDATE`` on the lot an EXIT closed.

    A plain cursor with no ``row_factory``, so the row is a TUPLE — matching
    ``_deduct_closed_exit_lot``, which reads ``locked_row[0]``.
    """
    cur = MagicMock()
    cur.fetchone.return_value = (units,)
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    return cur


def _advisory_lock_aware_result(result: MagicMock, sql: object) -> MagicMock:
    """Make the shared `conn.execute` result answer advisory locks like Postgres.

    `pg_try_advisory_lock` / `pg_advisory_unlock` answer `(True,)` on a healthy
    connection nobody else is holding. Everything else answers `fetchone() ->
    None`, which for the #2942 release UPDATE means "no never-submitted claim to
    release" — the ordinary case in every test that does not set one up.

    ⚠ It MUTATES and returns the one shared `conn.execute.return_value` rather
    than building a fresh mock per call, so a test that sets
    `conn.execute.return_value.rowcount = 0` still steers the statement it means
    to (`_update_position_exit`'s one-row assertion, #3013).

    ⚠ Only those two statements are special-cased. Everything else keeps the
    pre-#2942 behaviour — `fetchone()` returns a bare mock — because several
    readers (`_engine_owned_long_lot_count`) assert the row is not None, and a
    blanket `None` default would fail them for a reason unrelated to what they
    test.
    """
    text = str(sql)
    if "pg_try_advisory_lock" in text or "pg_advisory_unlock" in text:
        result.fetchone.return_value = (True,)
    elif "recommendation_submission_phase = 'claim_committed'" in text:
        result.fetchone.return_value = None
    elif text == _OUTSTANDING_CLAIM_SQL:
        # #2942 Delta 4: no other attempt holds the claim, the ordinary case.
        result.fetchone.return_value = None
    elif text == _RECOMMENDATION_STATUS_SQL:
        # #2942 Delta 4: still 'approved' under the key, the ordinary case.
        result.fetchone.return_value = ("approved",)
    else:
        result.fetchone.return_value = MagicMock()
    return result


def _make_conn(cursor_sequence: list[MagicMock]) -> MagicMock:
    """
    Build a fake psycopg connection.
    conn.cursor() calls consume cursor_sequence in order.
    conn.execute() is a no-op mock.
    conn.transaction() is a no-op context manager.
    """
    conn = MagicMock()
    conn.cursor.side_effect = cursor_sequence
    conn.execute.return_value = MagicMock()
    # #2942 half 2: the live path takes a session-scoped advisory lock and then
    # runs one `UPDATE … RETURNING` that releases a never-submitted claim. Both
    # go through `conn.execute(...).fetchone()`, and they want DIFFERENT answers,
    # so the fake has to read the statement rather than return one constant:
    # a healthy Postgres answers the lock `(True,)`, and the release UPDATE
    # matches no row in the ordinary case (`None`). Returning a bare MagicMock
    # for both would make every live test refuse with
    # `ConcurrentSubmissionInFlightError`.
    conn.execute.side_effect = lambda sql, *_a, **_kw: _advisory_lock_aware_result(conn.execute.return_value, sql)
    # A bare MagicMock compares unequal to IDLE, which would make the lock's
    # release path roll back on every exit and make `assert conn.rollback.called`
    # trivially true wherever it is asserted. The fake reports the real state.
    conn.info.transaction_status = TransactionStatus.IDLE
    # #3013: _update_position_exit asserts its UPDATE matched exactly one row.
    # A bare MagicMock's rowcount is a MagicMock and compares unequal to 1, so
    # the default here has to be the HEALTHY value; the zero-row case is set
    # explicitly by the test that wants it.
    conn.execute.return_value.rowcount = 1
    conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
    conn.transaction.return_value.__exit__ = MagicMock(return_value=False)
    return conn


def _rec_cursor(
    action: str = "BUY",
    instrument_id: int = 1,
    recommendation_id: int = 42,
    target_entry: float | None = 100.0,
    suggested_size_pct: float | None = 0.05,
    model_version: str | None = "v1-balanced",
    status: str = "approved",
    stop_loss_rate: float | None = None,
    take_profit_rate: float | None = None,
) -> MagicMock:
    return _make_cursor(
        [
            {
                "recommendation_id": recommendation_id,
                "instrument_id": instrument_id,
                "action": action,
                "target_entry": target_entry,
                "suggested_size_pct": suggested_size_pct,
                "model_version": model_version,
                "status": status,
                "stop_loss_rate": stop_loss_rate,
                "take_profit_rate": take_profit_rate,
            }
        ]
    )


def _cash_cursor(balance: float | None = 10_000.0) -> MagicMock:
    return _make_cursor([{"balance": balance}])


def _quote_cursor(
    last: float | None = 150.0,
    bid: float | None = None,
    ask: float | None = None,
    spread_pct: float | None = None,
) -> MagicMock:
    if last is None and bid is None:
        return _make_cursor([])
    row: dict[str, Any] = {"last": last, "bid": bid, "ask": ask, "spread_pct": spread_pct}
    return _make_cursor([row])


def _position_cursor(current_units: float = 10.0) -> MagicMock:
    return _make_cursor([{"current_units": current_units}])


def _order_returning_cursor(order_id: int = 1) -> MagicMock:
    return _make_cursor([{"order_id": order_id}])


def _fill_returning_cursor(fill_id: int = 1) -> MagicMock:
    return _make_cursor([{"fill_id": fill_id}])


def _cost_config_cursor() -> MagicMock:
    return _make_cursor([_DEFAULT_COST_CONFIG])


def _cost_model_cursor(row: dict[str, Any] | None = None) -> MagicMock:
    return _make_cursor([row] if row is not None else [])


def _cost_record_write_cursor() -> MagicMock:
    """Cursor consumed by record_estimated_cost INSERT (no rows returned)."""
    return _make_cursor([])


def _update_cursor(rowcount: int = 1) -> MagicMock:
    """Cursor consumed by an UPDATE that asserts ``cur.rowcount == 1``.

    Used by the #243 ``_update_order_with_broker_result`` post-broker
    UPDATE on the pre-call intent row.
    """
    cur = _make_cursor([])
    cur.__enter__.return_value.rowcount = rowcount
    return cur


# ---------------------------------------------------------------------------
# TestSyntheticFill
# ---------------------------------------------------------------------------


class TestSyntheticFill:
    def test_buy_with_amount(self) -> None:
        result = _synthetic_fill(
            instrument_id=1,
            action="BUY",
            quote_price=Decimal("100"),
            requested_amount=Decimal("500"),
            requested_units=None,
        )
        assert result.status == "filled"
        assert result.filled_price == Decimal("100")
        assert result.filled_units == Decimal("5.000000")
        assert result.broker_order_ref == "DEMO-1-BUY"
        assert result.raw_payload["demo"] is True
        assert result.fees == Decimal("0")

    def test_buy_with_units(self) -> None:
        result = _synthetic_fill(
            instrument_id=1,
            action="BUY",
            quote_price=Decimal("50"),
            requested_amount=None,
            requested_units=Decimal("10"),
        )
        assert result.status == "filled"
        assert result.filled_price == Decimal("50")
        assert result.filled_units == Decimal("10")

    def test_exit_with_units(self) -> None:
        result = _synthetic_fill(
            instrument_id=1,
            action="EXIT",
            quote_price=Decimal("200"),
            requested_amount=None,
            requested_units=Decimal("5"),
        )
        assert result.status == "filled"
        assert result.filled_price == Decimal("200")
        assert result.filled_units == Decimal("5")

    def test_no_quote_price_uses_zero(self) -> None:
        result = _synthetic_fill(
            instrument_id=1,
            action="BUY",
            quote_price=None,
            requested_amount=Decimal("500"),
            requested_units=None,
        )
        assert result.filled_price == Decimal("0")
        assert result.filled_units == Decimal("0")
        assert "no quote available" in result.raw_payload["note"]

    def test_zero_amount_and_no_units(self) -> None:
        result = _synthetic_fill(
            instrument_id=1,
            action="BUY",
            quote_price=Decimal("100"),
            requested_amount=None,
            requested_units=None,
        )
        assert result.filled_units == Decimal("0")

    def test_exit_with_no_quote_fails_closed(self) -> None:
        """Regression for #241.

        Demo EXIT with no quote (no last, no bid) used to return
        ``status='filled'`` with ``filled_price=0`` and
        ``filled_units = position_units``. The outer guard ``fu > 0``
        in execute_order is satisfied (units came from the open
        position) so a fill at price=0 was persisted, the cash ledger
        credited 0, and a realised loss equal to the position's open
        basis was booked.

        Now: status='failed', filled_units=None, filled_price=None.
        execute_order's persistence guard skips the fill and the
        recommendation ends in a failed state.
        """
        result = _synthetic_fill(
            instrument_id=123,
            action="EXIT",
            quote_price=None,
            requested_amount=None,
            requested_units=Decimal("10"),  # position size
            bid=None,
            ask=None,
        )
        assert result.status == "failed"
        assert result.filled_price is None
        assert result.filled_units is None
        assert result.fees == Decimal("0")
        assert "no quote" in result.raw_payload["error"].lower()

    def test_buy_with_no_quote_still_zero_units(self) -> None:
        """The fail-closed branch is EXIT-only. BUY/ADD with
        amount-based sizing already correctly produced units=0 when
        price=0; that path must remain unchanged.
        """
        result = _synthetic_fill(
            instrument_id=123,
            action="BUY",
            quote_price=None,
            requested_amount=Decimal("1000"),
            requested_units=None,
            bid=None,
            ask=None,
        )
        assert result.status == "filled"
        assert result.filled_units == Decimal("0")

    def test_buy_zero_ask_does_not_override_valid_last(self) -> None:
        """#1439: a non-positive ask must not price the fill at 0.

        eToro persists ``bid=ask=0.00`` for an instrument with no
        recent two-sided book. The directional ``BUY at ask`` branch
        used ``ask is not None`` — a 0.00 ask is non-None, so it won
        over a perfectly valid ``last``, filling the BUY at price 0.
        A non-positive book side is treated as missing; the fill falls
        back to the valid ``last``.
        """
        result = _synthetic_fill(
            instrument_id=1,
            action="BUY",
            quote_price=Decimal("150"),
            requested_amount=Decimal("300"),
            requested_units=None,
            bid=Decimal("0"),
            ask=Decimal("0"),
        )
        assert result.status == "filled"
        assert result.filled_price == Decimal("150")
        assert result.filled_units == Decimal("2.000000")

    def test_exit_zero_bid_does_not_fail_when_last_valid(self) -> None:
        """#1439: a non-positive bid must not price the EXIT at 0.

        ``EXIT at bid`` used ``bid is not None`` — a 0.00 bid won over
        a valid ``last`` and tripped the price==0 EXIT fail-closed,
        wrongly refusing an exit that has a usable last price. The
        non-positive bid is ignored; the EXIT fills at the valid last.
        """
        result = _synthetic_fill(
            instrument_id=1,
            action="EXIT",
            quote_price=Decimal("200"),
            requested_amount=None,
            requested_units=Decimal("5"),
            bid=Decimal("0"),
            ask=Decimal("0"),
        )
        assert result.status == "filled"
        assert result.filled_price == Decimal("200")
        assert result.filled_units == Decimal("5")

    def test_zero_last_with_valid_ask_fills_at_ask(self) -> None:
        """#1439 canonical scenario: last=0 (no recent trade) but a
        valid two-sided book. BUY fills at ask, never at the 0 last.
        """
        result = _synthetic_fill(
            instrument_id=1,
            action="BUY",
            quote_price=Decimal("0"),
            requested_amount=Decimal("500"),
            requested_units=None,
            bid=Decimal("99.80"),
            ask=Decimal("100.20"),
        )
        assert result.filled_price == Decimal("100.20")


# ---------------------------------------------------------------------------
# TestSyntheticFillSpreadCost
# ---------------------------------------------------------------------------


class TestSyntheticFillSpreadCost:
    def test_buy_fills_at_ask_with_zero_fees(self) -> None:
        """BUY at ask already embeds half-spread vs mid in execution price.
        Fees must be 0 — see #255 — so the cash ledger does not subtract
        the spread twice (once via gross at ask, once via fees).
        """
        result = _synthetic_fill(
            instrument_id=123,
            action="BUY",
            quote_price=Decimal("100.00"),
            requested_amount=Decimal("1000"),
            requested_units=None,
            bid=Decimal("99.80"),
            ask=Decimal("100.20"),
        )
        # BUY fills at ask
        assert result.filled_price == Decimal("100.20")
        # units = 1000 / 100.20
        expected_units = (Decimal("1000") / Decimal("100.20")).quantize(Decimal("0.000001"))
        assert result.filled_units == expected_units
        # Spread already embedded in fill price; fees=0 to avoid double count.
        assert result.fees == Decimal("0")

    def test_exit_fills_at_bid_with_zero_fees(self) -> None:
        """EXIT at bid already embeds half-spread vs mid. Fees must be 0
        so cash credit on close is `bid * units` flat, not `bid * units
        - half_spread * units`.
        """
        result = _synthetic_fill(
            instrument_id=123,
            action="EXIT",
            quote_price=Decimal("100.00"),
            requested_amount=None,
            requested_units=Decimal("10"),
            bid=Decimal("99.80"),
            ask=Decimal("100.20"),
        )
        assert result.filled_price == Decimal("99.80")
        assert result.fees == Decimal("0")

    def test_demo_buy_cash_ledger_writes_gross_only_no_double_count(self) -> None:
        """Regression for #255 at the ledger boundary.

        Before the fix the cash ledger row for demo BUY was
        ``-(gross + half_spread*units)`` — spread subtracted twice
        (once via ask>mid in gross, once via fees). After the fix the
        ledger row must be exactly ``-gross``.
        """
        cursors = [
            _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
            _cash_cursor(balance=10_000.0),
            _quote_cursor(last=100.0, bid=99.80, ask=100.20, spread_pct=0.40),
            _order_returning_cursor(order_id=7),
            _cost_config_cursor(),
            _cost_model_cursor(),
            _cost_record_write_cursor(),
            _fill_returning_cursor(fill_id=3),
        ]
        conn = _make_conn(cursors)

        with patch("app.services.order_client._utcnow", return_value=_NOW):
            _execute(conn, recommendation_id=42, decision_id=10)

        # gross = filled_price (ask=100.20) * units (suggested_size=5% of 10_000 / 100.20)
        # Match cash_ledger inserts via positional OR keyword query
        # (#612 review). A positional-only filter would silently
        # pass with zero matches if the production call is ever
        # refactored to ``conn.execute(query=...)``, defeating the
        # regression guard.
        def _query_str(c: Any) -> str:
            if c.args:
                return str(c.args[0])
            return str(c.kwargs.get("query", ""))

        ledger_calls = [c for c in conn.execute.call_args_list if "cash_ledger" in _query_str(c)]
        # Belt-and-braces: assert match count is non-zero before
        # taking [0], so a future test-mock refactor that drops
        # all positional args trips this assertion loudly.
        assert len(ledger_calls) >= 1, list(conn.execute.call_args_list)
        assert len(ledger_calls) == 1, ledger_calls
        params = ledger_calls[0].args[1] if ledger_calls[0].args else ledger_calls[0].kwargs.get("params")
        # amount must equal -gross exactly (fees=0). Any negative drift
        # vs that == double-count regression.
        units = (Decimal("500") / Decimal("100.20")).quantize(Decimal("0.000001"))
        expected_gross = Decimal("100.20") * units
        assert params["amount"] == -expected_gross
        assert params["type"] == "order_buy"

    def test_no_bid_ask_falls_back_to_zero_fees(self) -> None:
        result = _synthetic_fill(
            instrument_id=123,
            action="BUY",
            quote_price=Decimal("100.00"),
            requested_amount=Decimal("1000"),
            requested_units=None,
            bid=None,
            ask=None,
        )
        assert result.fees == Decimal("0")
        assert result.filled_price == Decimal("100.00")


# ---------------------------------------------------------------------------
# TestLoadApprovedRec
# ---------------------------------------------------------------------------


class TestLoadApprovedRec:
    def test_not_found_raises(self) -> None:
        conn = _make_conn([_make_cursor([])])
        with pytest.raises(ValueError, match="not found"):
            _load_approved_recommendation(conn, 999)

    def test_not_approved_raises(self) -> None:
        """#2942: an existing recommendation that moved on is an AUDITED refusal
        (counted `refused`), not a programmer-error ValueError (counted `failed`)."""
        conn = _make_conn([_rec_cursor(status="proposed")])
        with pytest.raises(RecommendationNoLongerApprovedError, match="no longer 'approved'"):
            _load_approved_recommendation(conn, 42)
        assert conn.commit.called  # the FAIL audit row is committed before the raise

    def test_approved_returns_row(self) -> None:
        conn = _make_conn([_rec_cursor(status="approved")])
        row = _load_approved_recommendation(conn, 42)
        assert row["recommendation_id"] == 42
        assert row["action"] == "BUY"
        assert row["status"] == "approved"


# ---------------------------------------------------------------------------
# TestLoadHelpers
# ---------------------------------------------------------------------------


class TestLoadHelpers:
    def test_quote_price_returns_decimal(self) -> None:
        conn = _make_conn([_quote_cursor(last=150.50)])
        price = _load_latest_quote_price(conn, 1)
        assert price == Decimal("150.5")

    def test_quote_price_none_when_no_rows(self) -> None:
        conn = _make_conn([_make_cursor([])])
        price = _load_latest_quote_price(conn, 1)
        assert price is None

    def test_quote_price_none_when_last_is_null(self) -> None:
        conn = _make_conn([_make_cursor([{"last": None}])])
        price = _load_latest_quote_price(conn, 1)
        assert price is None

    def test_quote_price_none_when_last_is_zero(self) -> None:
        """#1439: last=0.00 is not a usable mark — treat as missing."""
        conn = _make_conn([_make_cursor([{"last": 0}])])
        price = _load_latest_quote_price(conn, 1)
        assert price is None

    def test_quote_price_none_when_last_is_negative(self) -> None:
        """#1439: a negative last is impossible/garbage — treat as missing."""
        conn = _make_conn([_make_cursor([{"last": -1.5}])])
        price = _load_latest_quote_price(conn, 1)
        assert price is None

    def test_position_units_returns_decimal(self) -> None:
        conn = _make_conn([_position_cursor(current_units=10.5)])
        units = _load_position_units(conn, 1)
        assert units == Decimal("10.5")

    def test_position_units_zero_when_no_position(self) -> None:
        conn = _make_conn([_make_cursor([])])
        units = _load_position_units(conn, 1)
        assert units == Decimal("0")


# ---------------------------------------------------------------------------
# TestExecuteOrderDemoMode
# ---------------------------------------------------------------------------


#: #2942 Delta 4: the synthetic branch now runs under the per-recommendation
#: key like the live one. Seven `conn.execute` calls it did not make before: the
#: key acquire, the terminaliser's nested acquire, its release UPDATE (matching
#: nothing), its nested unlock, the outstanding-claim check, the status re-check,
#: and the outer unlock. The step-5 status UPDATE is still one call (now a CAS).
_DEMO_KEY_SPAN_EXECUTES = 7


class TestExecuteOrderDemoMode:
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_demo_buy_produces_fill_and_order(self, _mock_now: MagicMock) -> None:
        """Demo BUY: synthetic fill, order row, fill row, position upsert, cash, audit."""
        cursors = [
            _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
            _cash_cursor(balance=10_000.0),
            _quote_cursor(last=100.0, spread_pct=0.20),
            # Inside transaction:
            _order_returning_cursor(order_id=7),
            _cost_config_cursor(),
            _cost_model_cursor(),  # no cost_model → falls back to quote spread
            _cost_record_write_cursor(),  # record_estimated_cost INSERT
            _fill_returning_cursor(fill_id=3),
        ]
        conn = _make_conn(cursors)
        result = _execute(
            conn,
            recommendation_id=42,
            decision_id=10,
        )
        assert result.outcome == "filled"
        assert result.order_id == 7
        assert result.fill_id == 3
        assert result.broker_order_ref == "DEMO-1-BUY"
        assert "order filled" in result.explanation

        # conn.execute: safety-layer checks (fx_rates + portfolio_sync = 2),
        # position upsert, broker_positions, cash_ledger, rec status, audit = 7
        assert conn.execute.call_count == 7 + _DEMO_KEY_SPAN_EXECUTES

    @patch("app.services.order_client._maybe_trigger_attribution")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_demo_exit_produces_fill(self, _mock_now: MagicMock, _mock_attr: MagicMock) -> None:
        """Demo EXIT: loads position units, synthetic fill at quote price."""
        cursors = [
            _rec_cursor(action="EXIT", target_entry=None, suggested_size_pct=None),
            _position_cursor(current_units=5.0),
            _quote_cursor(last=200.0, spread_pct=0.30),
            # Inside transaction (no cost recording for EXIT):
            _order_returning_cursor(order_id=8),
            _fill_returning_cursor(fill_id=4),
            # Post-fill: read current_units for attribution check
            _make_cursor([{"current_units": 0}]),
        ]
        conn = _make_conn(cursors)
        result = _execute(
            conn,
            recommendation_id=42,
            decision_id=10,
        )
        assert result.outcome == "filled"
        assert result.order_id == 8
        assert result.fill_id == 4

        # conn.execute: position update, cash_ledger, rec status, audit = 4
        assert conn.execute.call_count == 4 + _DEMO_KEY_SPAN_EXECUTES

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_demo_exit_no_quote_fails_closed(self, _mock_now: MagicMock) -> None:
        """Regression for #241. Demo EXIT with no quote must NOT
        persist a fill at price=0 / book a synthetic loss / drain
        the position. Synthetic_fill returns status=failed; the outer
        guard skips _persist_fill, _update_position_exit and the cash
        ledger entirely.
        """
        cursors = [
            _rec_cursor(action="EXIT", target_entry=None, suggested_size_pct=None),
            _position_cursor(current_units=5.0),
            _make_cursor([]),  # no quote
            # Inside transaction: order persisted (status=failed), no fill, no
            # position update, no cash ledger, no attribution.
            _order_returning_cursor(order_id=11),
        ]
        conn = _make_conn(cursors)
        result = _execute(
            conn,
            recommendation_id=42,
            decision_id=10,
        )
        assert result.outcome == "failed"
        assert result.fill_id is None
        assert result.order_id == 11

        # conn.execute: rec status update + audit = 2 (NO position deduct,
        # NO cash ledger, NO broker_positions). The fill guard rejected
        # everything because broker_result.status = 'failed'.
        assert conn.execute.call_count == 2 + _DEMO_KEY_SPAN_EXECUTES

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_demo_buy_no_quote_produces_failed_no_fill(self, _mock_now: MagicMock) -> None:
        """Demo BUY with no quote: zero-unit fill is not persisted."""
        cursors = [
            _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
            _cash_cursor(balance=10_000.0),
            _make_cursor([]),  # no quote → quote_data=None
            # Inside transaction: order persisted, no fill (zero units)
            _order_returning_cursor(order_id=9),
            _cost_config_cursor(),
            _cost_model_cursor(),  # no cost_model → falls back to quote
            _make_cursor([]),  # cost fallback: no quote either → s_bps=None, no record
        ]
        conn = _make_conn(cursors)
        result = _execute(
            conn,
            recommendation_id=42,
            decision_id=10,
        )
        assert result.outcome == "failed"
        assert result.fill_id is None
        assert result.order_id == 9
        assert "zero units" in result.explanation

        # conn.execute: safety-layer checks (fx_rates + portfolio_sync = 2),
        # rec status update, audit = 4 (no fill/position/cash)
        assert conn.execute.call_count == 4 + _DEMO_KEY_SPAN_EXECUTES

    @patch("app.services.order_client._synthetic_fill")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_demo_filled_zero_price_positive_units_not_persisted(
        self, _mock_now: MagicMock, mock_fill: MagicMock
    ) -> None:
        """#1439 defense-in-depth: a 'filled' broker result with price=0 but
        positive units must NOT persist a fill (free holdings, prevention-log
        #68). The persistence guard requires ``fp > 0`` for every action.
        """
        mock_fill.return_value = BrokerOrderResult(
            broker_order_ref="DEMO-1-BUY",
            status="filled",
            filled_price=Decimal("0"),
            filled_units=Decimal("5"),
            fees=Decimal("0"),
            raw_payload={"demo": True},
        )
        cursors = [
            _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
            _cash_cursor(balance=10_000.0),
            _quote_cursor(last=100.0, spread_pct=0.20),
            _order_returning_cursor(order_id=3),
            _cost_config_cursor(),
            _cost_model_cursor(),
            _cost_record_write_cursor(),
        ]
        conn = _make_conn(cursors)
        result = _execute(conn, recommendation_id=42, decision_id=10)
        assert result.fill_id is None
        assert result.outcome == "failed"
        sql_calls = [str(c.args[0]) for c in conn.execute.call_args_list]
        assert not any("INSERT INTO fills" in s for s in sql_calls)

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_demo_mode_never_calls_broker(self, _mock_now: MagicMock) -> None:
        """Demo mode must never invoke the broker provider."""
        broker = MagicMock()
        cursors = [
            _rec_cursor(action="BUY"),
            _cash_cursor(balance=10_000.0),
            _quote_cursor(last=100.0, spread_pct=0.20),
            _order_returning_cursor(order_id=1),
            _cost_config_cursor(),
            _cost_model_cursor(),
            _cost_record_write_cursor(),
            _fill_returning_cursor(fill_id=1),
        ]
        conn = _make_conn(cursors)
        _execute(
            conn,
            recommendation_id=42,
            decision_id=10,
            broker=broker,
        )
        broker.place_order.assert_not_called()
        broker.close_position.assert_not_called()
        broker.get_order_status.assert_not_called()

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_demo_buy_writes_broker_positions_row(self, _mock_now: MagicMock) -> None:
        """BUY/ADD fills must INSERT into broker_positions so EXIT can resolve."""
        cursors = [
            _rec_cursor(action="BUY", stop_loss_rate=90.0, take_profit_rate=120.0),
            _cash_cursor(balance=10_000.0),
            _quote_cursor(last=100.0, spread_pct=0.20),
            _order_returning_cursor(order_id=7),
            _cost_config_cursor(),
            _cost_model_cursor(),
            _cost_record_write_cursor(),
            _fill_returning_cursor(fill_id=3),
        ]
        conn = _make_conn(cursors)
        result = _execute(
            conn,
            recommendation_id=42,
            decision_id=10,
        )
        assert result.outcome == "filled"

        # Find the broker_positions INSERT among conn.execute calls
        bp_calls = [c for c in conn.execute.call_args_list if "broker_positions" in str(c)]
        assert len(bp_calls) == 1, f"Expected 1 broker_positions INSERT, got {len(bp_calls)}"

        # Verify params passed through execute_order (not just SQL shape)
        params = bp_calls[0].args[1]
        # #227: synthetic position_id is the negation of order_id so the
        # synthetic-id namespace can never collide with a real broker
        # position_id pulled in by portfolio sync.
        assert params["pid"] == -7
        assert params["pid"] < 0  # negative-namespace contract
        assert params["iid"] == 1  # instrument_id from rec
        assert params["sl"] == Decimal("90")
        assert params["tp"] == Decimal("120")
        assert params["no_sl"] is False
        assert params["no_tp"] is False

    @patch("app.services.order_client._maybe_trigger_attribution")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_demo_exit_does_not_write_broker_positions(self, _mock_now: MagicMock, _mock_attr: MagicMock) -> None:
        """EXIT fills must NOT insert into broker_positions (the row already exists)."""
        cursors = [
            _rec_cursor(action="EXIT", target_entry=None, suggested_size_pct=None),
            _position_cursor(current_units=5.0),
            _quote_cursor(last=200.0, spread_pct=0.30),
            # No cost recording for EXIT
            _order_returning_cursor(order_id=8),
            _fill_returning_cursor(fill_id=4),
            # Post-fill: read current_units for attribution check
            _make_cursor([{"current_units": 0}]),
        ]
        conn = _make_conn(cursors)
        result = _execute(
            conn,
            recommendation_id=42,
            decision_id=10,
        )
        assert result.outcome == "filled"
        bp_calls = [c for c in conn.execute.call_args_list if "broker_positions" in str(c)]
        assert len(bp_calls) == 0

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_demo_buy_writes_execution_audit(self, _mock_now: MagicMock) -> None:
        """Every code path must write a decision_audit row for execution outcome."""
        cursors = [
            _rec_cursor(action="BUY"),
            _cash_cursor(balance=10_000.0),
            _quote_cursor(last=100.0, spread_pct=0.20),
            _order_returning_cursor(order_id=1),
            _cost_config_cursor(),
            _cost_model_cursor(),
            _cost_record_write_cursor(),
            _fill_returning_cursor(fill_id=1),
        ]
        conn = _make_conn(cursors)
        _execute(
            conn,
            recommendation_id=42,
            decision_id=10,
        )
        # Find the decision_audit INSERT among conn.execute calls
        audit_calls = [c for c in conn.execute.call_args_list if "decision_audit" in str(c)]
        assert len(audit_calls) == 1


# ---------------------------------------------------------------------------
# TestExecuteOrderLiveMode
# ---------------------------------------------------------------------------


class TestPostTradeEnqueue:
    """#1593: a persisted fill queues an immediate daily_portfolio_sync
    so the broker-observed trade lands in trade_events within seconds."""

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_filled_demo_buy_enqueues_portfolio_sync(
        self, _mock_now: MagicMock, _stub_post_trade_enqueue: MagicMock
    ) -> None:
        cursors = [
            _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
            _cash_cursor(balance=10_000.0),
            _quote_cursor(last=100.0, spread_pct=0.20),
            _order_returning_cursor(order_id=7),
            _cost_config_cursor(),
            _cost_model_cursor(),
            _cost_record_write_cursor(),
            _fill_returning_cursor(fill_id=3),
        ]
        result = _execute(_make_conn(cursors), recommendation_id=42, decision_id=10)
        assert result.outcome == "filled"
        _stub_post_trade_enqueue.assert_called_once()
        assert _stub_post_trade_enqueue.call_args.kwargs["requested_by"] == "execute_order"

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_failed_order_does_not_enqueue(self, _mock_now: MagicMock, _stub_post_trade_enqueue: MagicMock) -> None:
        # Demo EXIT with no quote fails closed: no fill → no enqueue.
        cursors = [
            _rec_cursor(action="EXIT", target_entry=None, suggested_size_pct=None),
            _position_cursor(current_units=5.0),
            _make_cursor([]),  # no quote
            _order_returning_cursor(order_id=11),
        ]
        result = _execute(_make_conn(cursors), recommendation_id=42, decision_id=10)
        assert result.outcome == "failed"
        _stub_post_trade_enqueue.assert_not_called()


class TestExecuteOrderLiveMode:
    @pytest.fixture(autouse=True)
    def _force_live(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Override the file-level demo default for every test in this class.
        monkeypatch.setattr(
            "app.services.order_client.get_runtime_config",
            lambda _conn: _RUNTIME_LIVE,
        )

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_a_live_submission_without_a_broker_environment_is_refused(self, _mock_now: MagicMock) -> None:
        """#3189 finding 4b (Codex ckpt-2). The poller fails closed on an
        unrecorded environment, so a live order that omitted one could never be
        reconciled — `environment_mismatch` on every attempt, claim held for
        ever. The keyword default that keeps the demo call sites untouched is
        exactly what makes this assertion necessary.

        Refused BEFORE the claim and before any broker I/O: the cursor list
        stops at the recommendation and the cash read, and the broker is never
        touched.
        """
        broker = MagicMock()
        conn = _make_conn(
            [
                _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
                _cash_cursor(balance=10_000.0),
            ]
        )

        with pytest.raises(ValueError, match="no broker_env supplied"):
            execute_order(conn, recommendation_id=42, decision_id=10, broker=broker)

        broker.place_order.assert_not_called()

    @pytest.mark.parametrize("action", ["BUY", "EXIT"])
    @pytest.mark.parametrize(
        "ids",
        [None, (_BROKER_CREDENTIAL_IDS[0], None), (None, _BROKER_CREDENTIAL_IDS[1]), (_BROKER_CREDENTIAL_IDS[0],)],
        ids=["none", "no_user", "no_api", "one"],
    )
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_a_live_submission_without_its_account_ids_is_refused_before_the_claim(
        self, _mock_now: MagicMock, ids: Any, action: str
    ) -> None:
        """#2942 Delta 3: an attempt whose account is not recorded could never be
        released by the attended window-B act, so it must not start."""
        broker = MagicMock()
        cursors = [_rec_cursor(action=action, target_entry=100.0, suggested_size_pct=0.05)]
        cursors.append(_cash_cursor(balance=10_000.0) if action == "BUY" else _position_cursor(current_units=5.0))
        conn = _make_conn(cursors)

        with pytest.raises(ValueError, match="no broker_credential_ids supplied"):
            _execute(conn, recommendation_id=42, decision_id=10, broker=broker, broker_credential_ids=ids)

        broker.place_order.assert_not_called()
        broker.close_position.assert_not_called()
        assert not any("pg_try_advisory_lock" in str(c.args[0]) for c in conn.execute.call_args_list)

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_live_buy_calls_broker_place_order(self, _mock_now: MagicMock) -> None:
        broker = MagicMock()
        broker.place_order.return_value = BrokerOrderResult(
            broker_order_ref="ORD-123",
            status="filled",
            filled_price=Decimal("100"),
            filled_units=Decimal("5"),
            fees=Decimal("1.50"),
            raw_payload={"orderId": "ORD-123", "status": "filled"},
        )
        cursors = [
            _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
            _cash_cursor(balance=10_000.0),
            # #243 pre-broker durable intent INSERT
            _order_returning_cursor(order_id=10),
            # broker called (no cursor)
            # #243 post-broker UPDATE asserts rowcount == 1
            _update_cursor(rowcount=1),
            _cost_config_cursor(),
            _cost_model_cursor(),  # no cost_model → falls back to quote
            _quote_cursor(last=100.0, bid=99.5, ask=100.5, spread_pct=0.30),  # cost fallback
            _make_cursor([]),  # record_estimated_cost INSERT
            _fill_returning_cursor(fill_id=6),
        ]
        conn = _make_conn(cursors)
        result = _execute(
            conn,
            recommendation_id=42,
            decision_id=10,
            broker=broker,
        )
        assert result.outcome == "filled"
        assert result.broker_order_ref == "ORD-123"
        broker.place_order.assert_called_once()
        # #243: durable intent commit happens before broker call.
        assert conn.commit.called

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_live_buy_records_cost_from_quote_fallback(self, _mock_now: MagicMock) -> None:
        """Live mode with no cost_model falls back to quote spread_pct for cost recording."""
        broker = MagicMock()
        broker.place_order.return_value = BrokerOrderResult(
            broker_order_ref="ORD-789",
            status="filled",
            filled_price=Decimal("100"),
            filled_units=Decimal("5"),
            fees=Decimal("1.50"),
            raw_payload={"orderId": "ORD-789"},
        )
        cost_insert_cursor = _make_cursor([])
        cursors = [
            _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
            _cash_cursor(balance=10_000.0),
            _order_returning_cursor(order_id=10),  # #243 pre-broker intent
            _update_cursor(rowcount=1),  # #243 post-broker UPDATE
            _cost_config_cursor(),
            _cost_model_cursor(),  # no cost_model → falls back to quote
            _quote_cursor(last=100.0, bid=99.5, ask=100.5, spread_pct=0.30),
            cost_insert_cursor,  # record_estimated_cost INSERT
            _fill_returning_cursor(fill_id=6),
        ]
        conn = _make_conn(cursors)
        _execute(conn, recommendation_id=42, decision_id=10, broker=broker)
        # Verify the cost record INSERT was called
        cost_insert_cursor.__enter__.return_value.execute.assert_called_once()
        sql = cost_insert_cursor.__enter__.return_value.execute.call_args[0][0]
        assert "INSERT INTO trade_cost_record" in sql

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_cost_insert_failure_does_not_abort_fill(self, _mock_now: MagicMock) -> None:
        """A DB error during record_estimated_cost must not prevent the fill.

        The cost recording block runs inside a savepoint.  If the cost INSERT
        raises, the savepoint rolls back and the outer transaction stays
        intact — _persist_fill still executes.
        """
        broker = MagicMock()
        broker.place_order.return_value = BrokerOrderResult(
            broker_order_ref="ORD-COST-FAIL",
            status="filled",
            filled_price=Decimal("100"),
            filled_units=Decimal("5"),
            fees=Decimal("1.50"),
            raw_payload={"orderId": "ORD-COST-FAIL"},
        )
        # Cost INSERT cursor raises on execute — simulates a DB error.
        bad_cost_cursor = _make_cursor([])
        bad_cost_cursor.__enter__.return_value.execute.side_effect = Exception("FK violation")
        cursors = [
            _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
            _cash_cursor(balance=10_000.0),
            _order_returning_cursor(order_id=10),  # #243 pre-broker intent
            _update_cursor(rowcount=1),  # #243 post-broker UPDATE
            _cost_config_cursor(),
            _cost_model_cursor(),  # no cost_model → falls back to quote
            _quote_cursor(last=100.0, bid=99.5, ask=100.5, spread_pct=0.30),
            bad_cost_cursor,  # record_estimated_cost INSERT — will raise
            _fill_returning_cursor(fill_id=6),
        ]
        conn = _make_conn(cursors)
        result = _execute(conn, recommendation_id=42, decision_id=10, broker=broker)
        # Fill must succeed despite cost recording failure.
        assert result.outcome == "filled"
        assert result.fill_id == 6

    @patch("app.services.order_client._maybe_trigger_attribution")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_live_exit_calls_broker_close_position(self, _mock_now: MagicMock, _mock_attr: MagicMock) -> None:
        broker = MagicMock()
        broker.close_position.return_value = BrokerOrderResult(
            broker_order_ref="ORD-456",
            status="filled",
            filled_price=Decimal("200"),
            filled_units=Decimal("5"),
            fees=Decimal("0"),
            raw_payload={"orderId": "ORD-456", "status": "filled"},
        )
        cursors = [
            _rec_cursor(action="EXIT", target_entry=None, suggested_size_pct=None),
            _position_cursor(current_units=5.0),
            # _load_exit_lot resolves instrument_id → (position_id, units)
            _make_cursor([{"position_id": 98765, "units": 5.0}]),
            # #243 pre-broker durable intent INSERT
            intent_cursor := _order_returning_cursor(order_id=11),
            # broker called (no cursor)
            # #243 post-broker UPDATE asserts rowcount == 1
            _update_cursor(rowcount=1),
            # No cost recording for EXIT
            _fill_returning_cursor(fill_id=7),
            # #3020: lock the broker lot before deducting the close
            _exit_lot_lock_cursor(5.0),
            # Post-fill: read current_units for attribution check
            _make_cursor([{"current_units": 0}]),
        ]
        conn = _make_conn(cursors)
        result = _execute(
            conn,
            recommendation_id=42,
            decision_id=10,
            broker=broker,
        )
        assert result.outcome == "filled"
        broker.close_position.assert_called_once()
        call = broker.close_position.call_args
        assert call.args == (98765,)
        assert call.kwargs["instrument_id"] == 1
        # #2942: the committed identity, not a fresh one minted at the header.
        assert isinstance(call.kwargs["request_id"], UUID)
        # #3007: the claim INSERT records the exact lot handed to `close_position`,
        # and no OrderParams (`close_position` takes none).
        params = intent_cursor.__enter__.return_value.execute.call_args.args[1]
        assert params["exit_position_id"] == 98765
        assert params["exit_units"] == Decimal("5")
        assert params["context"].obj == {"order_params": None}

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_live_exit_no_broker_positions_row_fails(self, _mock_now: MagicMock) -> None:
        """EXIT with no broker_positions row returns failed (pre-024 positions)."""
        broker = MagicMock()
        cursors = [
            _rec_cursor(action="EXIT", target_entry=None, suggested_size_pct=None),
            _position_cursor(current_units=5.0),
            # _load_exit_lot returns None — no broker-closeable long lot
            _make_cursor([]),
            # broker NOT called — broker_result is constructed inline as failed
            # No cost recording for EXIT
            _order_returning_cursor(order_id=12),
        ]
        conn = _make_conn(cursors)
        result = _execute(
            conn,
            recommendation_id=42,
            decision_id=10,
            broker=broker,
        )
        assert result.outcome == "failed"
        broker.close_position.assert_not_called()

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_live_buy_persists_intent_before_broker_call(self, _mock_now: MagicMock) -> None:
        """#243 — durable order intent must be INSERTed and COMMITed
        BEFORE the broker call. A fake broker that raises after the
        commit must leave the intent row visible to a reconciler.

        Mock-level proof: track the order of cursor consumption, the
        commit call, and the broker call. Asserts:
          1. the intent INSERT cursor is consumed
          2. conn.commit() runs
          3. broker.place_order is invoked
          4. all in that exact order

        With this ordering, a real DB would have a committed
        ``status='submitted'`` row on disk before any external side
        effect — the reconciler can find it after a crash.
        """
        sequence: list[str] = []

        broker = MagicMock()

        def _broker_side_effect(*_args: object, **_kwargs: object) -> BrokerOrderResult:
            sequence.append("broker.place_order")
            raise RuntimeError("simulated broker crash")

        broker.place_order.side_effect = _broker_side_effect

        intent_cursor = _order_returning_cursor(order_id=99)

        def _intent_execute(*_args: object, **_kwargs: object) -> None:
            # Tag the cursor execute fired inside _persist_submitted_intent.
            # No real DB; fetchone is already wired by _make_cursor.
            sequence.append("intent_insert")

        intent_cursor.__enter__.return_value.execute.side_effect = _intent_execute

        cursors = [
            _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
            _cash_cursor(balance=10_000.0),
            intent_cursor,
        ]
        conn = _make_conn(cursors)

        def _commit_tag() -> None:
            sequence.append("commit")

        conn.commit.side_effect = _commit_tag

        # #2942 half 2: tag the statements the marker mechanism depends on, so
        # the assertion below pins WHERE the marker commits, not merely that it
        # does. Delegates to the shared fake for the actual result.
        def _execute_tag(sql: object, *_args: object, **_kwargs: object) -> MagicMock:
            text = str(sql)
            if "pg_try_advisory_lock" in text:
                sequence.append("lock")
            elif "pg_advisory_unlock" in text:
                sequence.append("unlock")
            elif "recommendation_submission_phase = 'broker_verb_entered'" in text:
                sequence.append("marker_update")
            elif "status = 'refused'" in text:
                sequence.append("release_probe")
            return _advisory_lock_aware_result(conn.execute.return_value, sql)

        conn.execute.side_effect = _execute_tag

        # #2942 Delta 1: an unexpected provider exception is parked `uncertain`
        # and surfaces as BrokerSubmissionUncertainError, chained from the cause.
        with pytest.raises(BrokerSubmissionUncertainError, match="simulated broker crash") as raised:
            _execute(
                conn,
                recommendation_id=42,
                decision_id=10,
                broker=broker,
            )
        assert isinstance(raised.value.__cause__, RuntimeError)

        # Order matters, and every step of it is load-bearing:
        #   lock            — the evidence key, taken before the claim exists
        #   release_probe   — a never-submitted claim from a previous crash goes
        #                     first; here it matches nothing
        #   intent_insert   — the claim
        #   commit          — the claim is on disk before anything external
        #   marker_update + commit — its OWN commit; folded into the claim's it
        #                     would prove nothing
        #   broker.place_order — the unprovable side starts here
        assert sequence == [
            "lock",
            "commit",
            "lock",
            "commit",
            "release_probe",
            "unlock",
            "commit",
            "intent_insert",
            "commit",
            "marker_update",
            "commit",
            "broker.place_order",
            "commit",  # #2942: the uncertain park's audit row
            "unlock",
            "commit",
        ], f"durable-intent ordering violated: {sequence}"
        # The two things the sequence has to get right, stated separately so a
        # future reordering fails with a readable message rather than a diff.
        assert sequence.index("intent_insert") < sequence.index("marker_update")
        assert sequence.index("marker_update") < sequence.index("broker.place_order")
        assert sequence[sequence.index("marker_update") + 1] == "commit"

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_live_buy_zero_row_update_raises(self, _mock_now: MagicMock) -> None:
        """#637 review BLOCKING — _update_order_with_broker_result MUST
        refuse to advance to fill/cost/position writes when the UPDATE
        matches zero rows. Without this guard, ``order_id`` would
        silently flow forward as a foreign key into fills, corrupting
        referential integrity. Simulate by returning rowcount=0 from
        the UPDATE cursor."""
        broker = MagicMock()
        broker.place_order.return_value = BrokerOrderResult(
            broker_order_ref="ORD-PHANTOM",
            status="filled",
            filled_price=Decimal("100"),
            filled_units=Decimal("5"),
            fees=Decimal("0"),
            raw_payload={},
        )
        cursors = [
            _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
            _cash_cursor(balance=10_000.0),
            _order_returning_cursor(order_id=99),  # pre-broker intent INSERT
            _update_cursor(rowcount=0),  # phantom UPDATE matches nothing
        ]
        conn = _make_conn(cursors)

        with pytest.raises(RuntimeError, match="expected to update exactly 1 orders row"):
            _execute(
                conn,
                recommendation_id=42,
                decision_id=10,
                broker=broker,
            )

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_live_mode_no_broker_raises(self, _mock_now: MagicMock) -> None:
        cursors = [
            _rec_cursor(action="BUY"),
            _cash_cursor(balance=10_000.0),
        ]
        conn = _make_conn(cursors)
        with pytest.raises(ValueError, match="no broker provider supplied"):
            _execute(
                conn,
                recommendation_id=42,
                decision_id=10,
                broker=None,
            )


# ---------------------------------------------------------------------------
# TestExecuteOrderFailures
# ---------------------------------------------------------------------------


class TestExecuteOrderFailures:
    @pytest.fixture(autouse=True)
    def _force_live(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # All tests in this class exercise broker error paths, which are
        # only reachable in live mode.  The two not-found / not-approved
        # tests fail before the runtime read, so this override is harmless
        # for them.
        monkeypatch.setattr(
            "app.services.order_client.get_runtime_config",
            lambda _conn: _RUNTIME_LIVE,
        )

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_broker_failed_persists_order_with_failed_status(self, _mock_now: MagicMock) -> None:
        """Failed broker call still persists an order row and audit row."""
        broker = MagicMock()
        broker.place_order.return_value = BrokerOrderResult(
            broker_order_ref=None,
            status="failed",
            filled_price=None,
            filled_units=None,
            fees=Decimal("0"),
            raw_payload={"error": "insufficient funds"},
        )
        cursors = [
            _rec_cursor(action="BUY"),
            _cash_cursor(balance=10_000.0),
            _order_returning_cursor(order_id=12),  # #243 pre-broker intent
            _update_cursor(rowcount=1),  # #243 post-broker UPDATE
            _cost_config_cursor(),
            _cost_model_cursor(),  # no cost_model → falls back to quote
            _quote_cursor(last=100.0, spread_pct=0.30),  # cost fallback
            _make_cursor([]),  # record_estimated_cost INSERT
        ]
        conn = _make_conn(cursors)
        result = _execute(
            conn,
            recommendation_id=42,
            decision_id=10,
            broker=broker,
        )
        assert result.outcome == "failed"
        assert result.fill_id is None
        assert result.order_id == 12
        assert "failed" in result.explanation

        # conn.execute: safety-layer checks (fx_rates + portfolio_sync = 2),
        # rec status update + audit = 2. No fill/position/cash on a failed
        # broker call. The #243 post-broker UPDATE goes through a cursor, not
        # conn.execute, so it does not bump this counter.
        #
        # #2942 half 2 adds six more, all on the live submission span:
        # the outer evidence-lock acquire, the terminaliser's own nested
        # acquire, its release UPDATE (matching nothing here), its nested
        # unlock, the marker UPDATE, and the outer unlock.
        #
        # #2942 Delta 4 adds two, under the key: the outstanding-claim check and
        # the status re-check.
        assert conn.execute.call_count == 4 + 6 + 2

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_broker_pending_persists_order_with_pending_status(self, _mock_now: MagicMock) -> None:
        """Pending broker response still persists an order row and audit row."""
        broker = MagicMock()
        broker.place_order.return_value = BrokerOrderResult(
            broker_order_ref="ORD-789",
            status="pending",
            filled_price=None,
            filled_units=None,
            fees=Decimal("0"),
            raw_payload={"orderId": "ORD-789", "status": "pending"},
        )
        cursors = [
            _rec_cursor(action="BUY"),
            _cash_cursor(balance=10_000.0),
            _order_returning_cursor(order_id=13),  # #243 pre-broker intent
            _update_cursor(rowcount=1),  # #243 post-broker UPDATE
            _cost_config_cursor(),
            _cost_model_cursor(),  # no cost_model → falls back to quote
            _quote_cursor(last=100.0, spread_pct=0.30),  # cost fallback
            _make_cursor([]),  # record_estimated_cost INSERT
        ]
        conn = _make_conn(cursors)
        result = _execute(
            conn,
            recommendation_id=42,
            decision_id=10,
            broker=broker,
        )
        assert result.outcome == "pending"
        assert result.fill_id is None
        assert result.broker_order_ref == "ORD-789"
        assert "pending" in result.explanation

    def test_recommendation_not_found_raises(self) -> None:
        conn = _make_conn([_make_cursor([])])
        with pytest.raises(ValueError, match="not found"):
            _execute(
                conn,
                recommendation_id=999,
                decision_id=10,
            )

    def test_recommendation_not_approved_raises(self) -> None:
        conn = _make_conn([_rec_cursor(status="proposed")])
        with pytest.raises(RecommendationNoLongerApprovedError):
            _execute(
                conn,
                recommendation_id=42,
                decision_id=10,
            )

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_broker_rejected_persists_order_row(self, _mock_now: MagicMock) -> None:
        """Rejected broker response persists order with rejected status."""
        broker = MagicMock()
        broker.place_order.return_value = BrokerOrderResult(
            broker_order_ref="ORD-REJ",
            status="rejected",
            filled_price=None,
            filled_units=None,
            fees=Decimal("0"),
            raw_payload={"orderId": "ORD-REJ", "status": "rejected", "reason": "market closed"},
        )
        cursors = [
            _rec_cursor(action="BUY"),
            _cash_cursor(balance=10_000.0),
            _order_returning_cursor(order_id=14),  # #243 pre-broker intent
            _update_cursor(rowcount=1),  # #243 post-broker UPDATE
            _cost_config_cursor(),
            _cost_model_cursor(),  # no cost_model → falls back to quote
            _quote_cursor(last=100.0, spread_pct=0.30),  # cost fallback
            _make_cursor([]),  # record_estimated_cost INSERT
        ]
        conn = _make_conn(cursors)
        result = _execute(
            conn,
            recommendation_id=42,
            decision_id=10,
            broker=broker,
        )
        assert result.outcome == "failed"
        assert result.fill_id is None
        assert result.order_id == 14

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_failed_order_still_writes_audit(self, _mock_now: MagicMock) -> None:
        """Even a failed order must produce a decision_audit row."""
        broker = MagicMock()
        broker.place_order.return_value = BrokerOrderResult(
            broker_order_ref=None,
            status="failed",
            filled_price=None,
            filled_units=None,
            fees=Decimal("0"),
            raw_payload={"error": "timeout"},
        )
        cursors = [
            _rec_cursor(action="BUY"),
            _cash_cursor(balance=10_000.0),
            _order_returning_cursor(order_id=15),  # #243 pre-broker intent
            _update_cursor(rowcount=1),  # #243 post-broker UPDATE
            _cost_config_cursor(),
            _cost_model_cursor(),  # no cost_model → falls back to quote
            _quote_cursor(last=100.0, spread_pct=0.30),  # cost fallback
            _make_cursor([]),  # record_estimated_cost INSERT
        ]
        conn = _make_conn(cursors)
        _execute(
            conn,
            recommendation_id=42,
            decision_id=10,
            broker=broker,
        )
        audit_calls = [c for c in conn.execute.call_args_list if "decision_audit" in str(c)]
        assert len(audit_calls) == 1


# ---------------------------------------------------------------------------
# TestExecuteOrderRuntimeConfigCorrupt
# ---------------------------------------------------------------------------


class TestExecuteOrderRuntimeConfigCorrupt:
    """RuntimeConfigCorrupt must propagate from execute_order — never silently
    fall through to demo or live mode.  The execution_guard fails closed on
    the same condition; the order client is the second line of defence.
    """

    def test_corrupt_runtime_config_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def _raise(_conn: object) -> RuntimeConfig:
            raise RuntimeConfigCorrupt("singleton missing")

        monkeypatch.setattr("app.services.order_client.get_runtime_config", _raise)

        cursors = [
            _rec_cursor(action="BUY"),
            _cash_cursor(balance=10_000.0),
        ]
        conn = _make_conn(cursors)
        with pytest.raises(RuntimeConfigCorrupt):
            _execute(conn, recommendation_id=42, decision_id=10)

        # No order should have been persisted, no audit row written.
        conn.transaction.assert_not_called()


# ---------------------------------------------------------------------------
# TestSubmissionControls (#2943)
# ---------------------------------------------------------------------------


def _audit_insert_calls(conn: MagicMock) -> list[Any]:
    """conn.execute calls that wrote a decision_audit row."""
    return [c for c in conn.execute.call_args_list if "INSERT INTO decision_audit" in c.args[0]]


class TestSubmissionControls:
    """#2943: an approved recommendation must not reach the broker after the
    operator closes a control. The guard checked these at approval time; these
    tests cover the window between approval and submission.
    """

    @pytest.fixture
    def _force_live(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.services.order_client.get_runtime_config",
            lambda _conn: _RUNTIME_LIVE,
        )

    @pytest.mark.usefixtures("_force_live")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_kill_switch_activated_after_approval_refuses(
        self, _mock_now: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Kill switch flipped on after approval: no broker call at all."""
        monkeypatch.setattr(
            "app.services.order_client.load_kill_switch",
            lambda _conn: {"is_active": True, "activated_at": _NOW, "reason": "operator halt"},
        )
        broker = MagicMock()
        conn = _make_conn([_rec_cursor(action="BUY"), _cash_cursor(balance=10_000.0)])

        with pytest.raises(SubmissionControlsRevokedError) as exc:
            _execute(conn, recommendation_id=42, decision_id=10, broker=broker)

        assert exc.value.failed_rules == ["kill_switch"]
        assert "operator halt" in str(exc.value)
        broker.place_order.assert_not_called()
        broker.close_position.assert_not_called()
        # Nothing was staged: the intent INSERT happens after this gate.
        conn.transaction.assert_not_called()

    @pytest.mark.usefixtures("_force_live")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_refusal_is_audited_and_committed(self, _mock_now: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        """The refusal row must be committed BEFORE the raise — the scheduler's
        ``with connect_job()`` rolls back on exception, so an uncommitted audit
        row would disappear exactly when the operator needs it.
        """
        monkeypatch.setattr(
            "app.services.order_client.load_kill_switch",
            lambda _conn: {"is_active": True, "activated_at": _NOW, "reason": None},
        )
        conn = _make_conn([_rec_cursor(action="BUY"), _cash_cursor(balance=10_000.0)])

        with pytest.raises(SubmissionControlsRevokedError):
            _execute(conn, recommendation_id=42, decision_id=10, broker=MagicMock())

        audits = _audit_insert_calls(conn)
        assert len(audits) == 1
        params = audits[0].args[1]
        assert params["stage"] == "order_client"
        assert params["pf"] == "FAIL"
        assert params["rid"] == 42
        conn.commit.assert_called_once()

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_auto_trading_disabled_after_approval_refuses(
        self, _mock_now: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """enable_auto_trading turned off after approval: refuse."""
        runtime = replace(_RUNTIME_LIVE, enable_auto_trading=False)
        monkeypatch.setattr("app.services.order_client.get_runtime_config", lambda _conn: runtime)
        broker = MagicMock()
        conn = _make_conn([_rec_cursor(action="BUY"), _cash_cursor(balance=10_000.0)])

        with pytest.raises(SubmissionControlsRevokedError) as exc:
            _execute(conn, recommendation_id=42, decision_id=10, broker=broker)

        assert exc.value.failed_rules == ["auto_trading"]
        broker.place_order.assert_not_called()

    @pytest.mark.usefixtures("_force_live")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_missing_kill_switch_row_refuses(self, _mock_now: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        """A missing kill_switch row is configuration corruption, not consent."""
        monkeypatch.setattr("app.services.order_client.load_kill_switch", lambda _conn: None)
        conn = _make_conn([_rec_cursor(action="BUY"), _cash_cursor(balance=10_000.0)])

        with pytest.raises(SubmissionControlsRevokedError) as exc:
            _execute(conn, recommendation_id=42, decision_id=10, broker=MagicMock())

        assert exc.value.failed_rules == ["kill_switch_config_corrupt"]

    @pytest.mark.usefixtures("_force_live")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_exit_is_refused_on_the_same_controls(self, _mock_now: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        """EXIT is gated identically — which PRESERVES the exit policy rather
        than tightening it. The guard already applies kill switch / config
        rules to every action; "EXIT is never blocked" governs thesis,
        coverage and spread only.
        """
        monkeypatch.setattr(
            "app.services.order_client.load_kill_switch",
            lambda _conn: {"is_active": True, "activated_at": _NOW, "reason": None},
        )
        broker = MagicMock()
        conn = _make_conn(
            [_rec_cursor(action="EXIT", target_entry=None, suggested_size_pct=None), _position_cursor(5.0)]
        )

        with pytest.raises(SubmissionControlsRevokedError):
            _execute(conn, recommendation_id=42, decision_id=10, broker=broker)

        broker.close_position.assert_not_called()

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_controls_are_read_per_order_not_per_batch(
        self, _mock_now: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Toggling a control between two orders of the same batch must stop
        the second one. A batch-level check would let it through — that shape
        is the defect, so the read has to happen per submission.
        """
        kill_switch_active = {"value": False}
        monkeypatch.setattr(
            "app.services.order_client.load_kill_switch",
            lambda _conn: {
                "is_active": kill_switch_active["value"],
                "activated_at": None,
                "reason": None,
            },
        )

        first = _make_conn(
            [
                _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
                _cash_cursor(balance=10_000.0),
                _quote_cursor(last=100.0, spread_pct=0.20),
                _order_returning_cursor(order_id=7),
                _cost_config_cursor(),
                _cost_model_cursor(),
                _cost_record_write_cursor(),
                _fill_returning_cursor(fill_id=3),
            ]
        )
        assert _execute(first, recommendation_id=42, decision_id=10).outcome == "filled"

        kill_switch_active["value"] = True

        second = _make_conn([_rec_cursor(action="BUY"), _cash_cursor(balance=10_000.0)])
        with pytest.raises(SubmissionControlsRevokedError):
            _execute(second, recommendation_id=43, decision_id=11)

        assert _audit_insert_calls(second)


# ---------------------------------------------------------------------------
# TestUpdatePositionBuySource
# ---------------------------------------------------------------------------


class TestUpdatePositionBuySource:
    """Verify _update_position_buy writes ``source='ebull'`` and resets on reopen.

    Issue #180 — the positions ``source`` column identifies who currently
    manages the open units.  Every eBull-originated BUY must insert
    ``'ebull'``.  On reopen (ON CONFLICT into a closed row), source must
    flip to ``'ebull'`` too; on ADD into an already-open position, the
    existing source must be preserved so an ebull ADD into a
    broker_sync-owned position doesn't claim ownership of the original
    external open.
    """

    def test_insert_emits_source_literal_and_reopen_reset_clause(self) -> None:
        """INSERT carries the 'ebull' literal AND the reset CASE WHEN.

        With a mocked connection, ``_update_position_buy`` captures a
        single SQL string per call regardless of whether Postgres would
        take the INSERT or the ON CONFLICT branch at runtime — the
        branch decision is made by the planner, not by us.  So the
        unit-level guarantee we can assert here is SQL *shape*: a
        single captured string must contain both the hard-coded VALUES
        literal and the reset CASE WHEN, evaluated together from one
        call.

        End-to-end verification that Postgres actually routes closed
        rows through the reset arm is tracked in the DB integration
        test backlog (#186) — unreachable from a mocked connection.
        """
        conn = _make_conn([])
        _update_position_buy(
            conn,
            instrument_id=42,
            filled_price=Decimal("100"),
            filled_units=Decimal("5"),
            now=_NOW,
        )

        assert conn.execute.call_count == 1
        sql = conn.execute.call_args_list[0].args[0]
        normalised = re.sub(r"\s+", " ", sql)

        # Hard-coded VALUES literal — no parameter placeholder.
        assert "INSERT INTO positions" in normalised
        assert "'ebull'" in normalised
        # Reset CASE WHEN: pre-update row fully closed → overwrite
        # source; otherwise preserve.  Postgres evaluates CASE against
        # the pre-update row, so SET-list ordering is irrelevant.
        assert "positions.current_units <= 0" in normalised
        assert "EXCLUDED.source" in normalised
        assert "ELSE positions.source" in normalised


# ---------------------------------------------------------------------------
# TestPersistBrokerPosition
# ---------------------------------------------------------------------------


class TestPersistBrokerPosition:
    """Verify _persist_broker_position emits the correct INSERT."""

    def test_inserts_with_source_ebull_and_sl_tp(self) -> None:
        conn = _make_conn([])
        _persist_broker_position(
            conn,
            order_id=7,
            instrument_id=42,
            filled_price=Decimal("100"),
            filled_units=Decimal("5"),
            fees=Decimal("1.50"),
            order_params=OrderParams(
                stop_loss_rate=Decimal("90"),
                take_profit_rate=Decimal("120"),
            ),
            raw_payload={"demo": True},
            now=_NOW,
        )
        assert conn.execute.call_count == 1
        sql = conn.execute.call_args_list[0].args[0]
        normalised = re.sub(r"\s+", " ", sql)
        assert "INSERT INTO broker_positions" in normalised
        assert "'ebull'" in normalised
        assert "ON CONFLICT (position_id) DO UPDATE" in normalised
        # ON CONFLICT must update raw_payload to prevent silent payload loss
        assert "raw_payload = EXCLUDED.raw_payload" in normalised

        params = conn.execute.call_args_list[0].args[1]
        # #227: synthetic position_id = -order_id (negative-namespace
        # convention to avoid colliding with real broker position_ids).
        assert params["pid"] == -7
        assert params["pid"] < 0
        assert params["iid"] == 42
        assert params["units"] == Decimal("5")
        # amount = price * units = 500
        assert params["amount"] == Decimal("500")
        assert params["sl"] == Decimal("90")
        assert params["tp"] == Decimal("120")
        assert params["no_sl"] is False
        assert params["no_tp"] is False

    def test_inserts_without_order_params(self) -> None:
        conn = _make_conn([])
        _persist_broker_position(
            conn,
            order_id=8,
            instrument_id=99,
            filled_price=Decimal("50"),
            filled_units=Decimal("10"),
            fees=Decimal("0"),
            order_params=None,
            raw_payload={"demo": True},
            now=_NOW,
        )
        params = conn.execute.call_args_list[0].args[1]
        assert params["sl"] is None
        assert params["tp"] is None
        assert params["no_sl"] is True
        assert params["no_tp"] is True
        assert params["leverage"] == 1
        assert params["tsl"] is False


# ---------------------------------------------------------------------------
# #2942 — durable request identity and the submission claim
# ---------------------------------------------------------------------------


class _ClaimViolation(psycopg.errors.UniqueViolation):
    """A UniqueViolation that reports the claim index by name.

    psycopg builds ``diag`` from a libpq result, which a unit test has no way
    to fabricate. Overriding the property is the smallest thing that exercises
    the constraint-name check rather than bypassing it.
    """

    @property
    def diag(self) -> Any:  # type: ignore[override]
        return SimpleNamespace(constraint_name="idx_orders_recommendation_open_attempt")


class _OtherViolation(psycopg.errors.UniqueViolation):
    @property
    def diag(self) -> Any:  # type: ignore[override]
        return SimpleNamespace(constraint_name="orders_pkey")


def _raising_cursor(exc: BaseException) -> MagicMock:
    cur = MagicMock()
    cur.execute.side_effect = exc
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    return cur


class TestSubmissionClaim:
    """An uncertain attempt must not be able to create a second economic order."""

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_second_attempt_is_refused_without_calling_the_broker(
        self,
        _mock_now: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            "app.services.order_client.get_runtime_config",
            lambda _conn: _RUNTIME_LIVE,
        )
        broker = MagicMock()
        cursors = [
            _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
            _cash_cursor(balance=10_000.0),
            # the claim INSERT collides with the outstanding attempt
            _raising_cursor(_ClaimViolation("duplicate key")),
            # after rollback: which attempt holds the claim?
            _make_cursor(
                [
                    {
                        "order_id": 11,
                        "status": "uncertain",
                        "recommendation_request_id": UUID(int=7),
                    }
                ]
            ),
        ]
        conn = _make_conn(cursors)

        with pytest.raises(PriorSubmissionUnresolvedError) as excinfo:
            _execute(conn, recommendation_id=42, decision_id=10, broker=broker)

        assert excinfo.value.order_id == 11
        # The whole point: no second submission.
        broker.place_order.assert_not_called()
        broker.close_position.assert_not_called()
        # The aborted transaction is rolled back BEFORE anything else is
        # written, and the refusal audit is committed so a raising caller
        # (connect_job rolls back) cannot lose it.
        assert conn.rollback.called
        assert conn.rollback.call_count >= 1
        assert conn.commit.called
        audit_sql = " ".join(str(call.args[0]) for call in conn.execute.call_args_list)
        assert "decision_audit" in audit_sql

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_an_unrelated_unique_violation_is_not_reinterpreted(
        self,
        _mock_now: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Catching every UniqueViolation would tell the operator a false story."""
        monkeypatch.setattr(
            "app.services.order_client.get_runtime_config",
            lambda _conn: _RUNTIME_LIVE,
        )
        broker = MagicMock()
        cursors = [
            _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
            _cash_cursor(balance=10_000.0),
            _raising_cursor(_OtherViolation("some other constraint")),
        ]
        conn = _make_conn(cursors)

        with pytest.raises(psycopg.errors.UniqueViolation):
            _execute(conn, recommendation_id=42, decision_id=10, broker=broker)

        broker.place_order.assert_not_called()

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_the_committed_request_id_is_sent_to_the_broker(
        self,
        _mock_now: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            "app.services.order_client.get_runtime_config",
            lambda _conn: _RUNTIME_LIVE,
        )
        broker = MagicMock()
        broker.place_order.return_value = BrokerOrderResult(
            broker_order_ref="ORD-1",
            status="pending",
            filled_price=None,
            filled_units=None,
            fees=Decimal("0"),
            raw_payload={"status": "pending"},
        )
        intent_cursor = _order_returning_cursor(order_id=11)
        cursors = [
            _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
            _cash_cursor(balance=10_000.0),
            intent_cursor,
            _update_cursor(rowcount=1),
        ]
        conn = _make_conn(cursors)

        _execute(conn, recommendation_id=42, decision_id=10, broker=broker)

        sent = broker.place_order.call_args.kwargs["request_id"]
        persisted = intent_cursor.execute.call_args.args[1]["request_id"]
        # Same UUID in the committed row and on the wire — never rotated.
        assert sent == persisted
        assert isinstance(sent, UUID)


class TestUncertainSubmission:
    """A broker call whose outcome is unknown is not a failure."""

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_uncertain_place_order_parks_the_attempt(
        self,
        _mock_now: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            "app.services.order_client.get_runtime_config",
            lambda _conn: _RUNTIME_LIVE,
        )
        broker = MagicMock()
        broker.place_order.side_effect = BrokerOrderSubmissionUncertain(
            "transport failure",
            raw_payload={"error": "Network error: connection refused"},
        )
        cursors = [
            _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
            _cash_cursor(balance=10_000.0),
            _order_returning_cursor(order_id=11),
        ]
        conn = _make_conn(cursors)

        with pytest.raises(BrokerSubmissionUncertainError) as excinfo:
            _execute(conn, recommendation_id=42, decision_id=10, broker=broker)

        assert excinfo.value.order_id == 11
        statements = [str(call.args[0]) for call in conn.execute.call_args_list]
        joined = " ".join(statements)
        # The intent is parked, not failed, and keeps the claim.
        assert "status = 'uncertain'" in joined
        assert "execution_pending" in joined
        # Nothing economic is booked: we do not know that anything executed.
        assert "INSERT INTO fills" not in joined
        assert "cash_ledger" not in joined
        assert conn.commit.called

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_a_pre_io_refusal_releases_the_claim(
        self,
        _mock_now: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The unattended guard raises BEFORE any request is built (#2942).

        No order can exist at the broker, so holding the claim would park the
        recommendation permanently — including after the operator does exactly
        what the refusal message asks and re-runs from the main checkout.
        """
        monkeypatch.setattr(
            "app.services.order_client.get_runtime_config",
            lambda _conn: _RUNTIME_LIVE,
        )
        broker = MagicMock()
        broker.place_order.side_effect = UnattendedExecutionRefused(
            "refusing 'place_order': this checkout is a linked git worktree"
        )
        cursors = [
            _rec_cursor(action="BUY", target_entry=100.0, suggested_size_pct=0.05),
            _cash_cursor(balance=10_000.0),
            _order_returning_cursor(order_id=11),
        ]
        conn = _make_conn(cursors)

        with pytest.raises(UnattendedExecutionRefused):
            _execute(conn, recommendation_id=42, decision_id=10, broker=broker)

        joined = " ".join(str(call.args[0]) for call in conn.execute.call_args_list)
        # 'refused' is outside the claim index predicate, so the claim lifts.
        assert "status = 'refused'" in joined
        assert "status = 'uncertain'" not in joined
        # The recommendation is untouched — it stays approved and retryable.
        # (#2942 Delta 4 READS its status under the key; nothing writes it.)
        assert "UPDATE trade_recommendations" not in joined
        assert conn.commit.called


# ---------------------------------------------------------------------------
# #3006 — an EXIT closes ONE broker lot, not "the position"
# ---------------------------------------------------------------------------


class TestDescribeExitCompletion:
    """Pure: how much of the POSITION did an EXIT actually close?"""

    @pytest.mark.parametrize(
        ("lot", "closed", "after", "fully_closed"),
        [
            # Single-lot instrument: the lot IS the position.
            ("5", "5", "0", True),
            # GME as measured on dev: 1000 of 1500 closed, 500 still open.
            ("1000", "1000", "500", False),
            # Dust left by rounding still counts as open exposure.
            ("1000", "1000", "0.000001", False),
            # A negative remainder is over-subtraction, not "extra closed" —
            # it must still read as closed rather than as open exposure.
            ("5", "5", "-0.5", True),
        ],
    )
    def test_completion_flag_tracks_remaining_exposure(
        self, lot: str, closed: str, after: str, fully_closed: bool
    ) -> None:
        result = describe_exit_completion(
            lot_units_selected=Decimal(lot),
            units_closed=Decimal(closed),
            units_open_after=Decimal(after),
        )
        assert result["position_fully_closed"] is fully_closed
        assert result["exit_scope"] == "lot"
        # Quantities are stringified for JSONB, never floated.
        assert result["lot_units_selected"] == lot
        assert result["units_closed"] == closed
        assert result["units_open_after"] == after


class TestExitLotSelection:
    """#3006: the live EXIT path must record the LOT, not the aggregate."""

    def _multi_lot_exit_cursors(self) -> list[MagicMock]:
        return [
            _rec_cursor(action="EXIT", target_entry=None, suggested_size_pct=None),
            # Aggregate ledger position across both GME lots.
            _position_cursor(current_units=1500.0),
            # _load_exit_lot picks the oldest broker-closeable long lot.
            _make_cursor([{"position_id": 3308442058, "units": 1000.0}]),
            _order_returning_cursor(order_id=11),
            _update_cursor(rowcount=1),
            _fill_returning_cursor(fill_id=7),
            # #3020: lock the broker lot before deducting the close
            _exit_lot_lock_cursor(1000.0),
            # Post-fill read of positions.current_units — 500 still open.
            _make_cursor([{"current_units": 500}]),
        ]

    @patch("app.services.order_client._maybe_trigger_attribution")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_requested_units_is_the_lot_not_the_aggregate(
        self, _mock_now: MagicMock, _mock_attr: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The durable intent must record what the broker was actually asked to close.

        Before #3006 this row carried 1500 — ``positions.current_units`` summed
        across every lot — while ``close_position`` closed the 1000-unit lot
        whole. Asserted on the BOUND PARAMETER, not on the SQL text.
        """
        monkeypatch.setattr(
            "app.services.order_client.get_runtime_config",
            lambda _conn: _RUNTIME_LIVE,
        )
        broker = MagicMock()
        broker.close_position.return_value = BrokerOrderResult(
            broker_order_ref="ORD-456",
            status="filled",
            filled_price=Decimal("20"),
            filled_units=Decimal("1000"),
            fees=Decimal("0"),
            raw_payload={},
        )
        cursors = self._multi_lot_exit_cursors()
        conn = _make_conn(cursors)
        _execute(conn, recommendation_id=42, decision_id=10, broker=broker)

        intent_params = cursors[3].execute.call_args.args[1]
        assert intent_params["units"] == Decimal("1000")
        assert intent_params["units"] != Decimal("1500")

    @patch("app.services.order_client._maybe_trigger_attribution")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_partial_exit_is_recorded_as_not_fully_exited(
        self, _mock_now: MagicMock, _mock_attr: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A one-lot close of a multi-lot position must not read as a complete exit."""
        monkeypatch.setattr(
            "app.services.order_client.get_runtime_config",
            lambda _conn: _RUNTIME_LIVE,
        )
        broker = MagicMock()
        broker.close_position.return_value = BrokerOrderResult(
            broker_order_ref="ORD-456",
            status="filled",
            filled_price=Decimal("20"),
            filled_units=Decimal("1000"),
            fees=Decimal("0"),
            raw_payload={},
        )
        conn = _make_conn(self._multi_lot_exit_cursors())
        result = _execute(conn, recommendation_id=42, decision_id=10, broker=broker)

        assert "POSITION NOT FULLY EXITED" in result.explanation
        assert "500" in result.explanation

        audit_calls = [c for c in conn.execute.call_args_list if "decision_audit" in str(c.args[0])]
        assert len(audit_calls) == 1
        completion = audit_calls[0].args[1]["ev"].obj["exit_completion"]
        assert completion["position_fully_closed"] is False
        assert completion["units_closed"] == "1000"
        assert completion["units_open_after"] == "500"

    @patch("app.services.order_client._maybe_trigger_attribution")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_demo_exit_carries_no_lot_completion_claim(self, _mock_now: MagicMock, _mock_attr: MagicMock) -> None:
        """Demo never resolves a lot, so it must make no completion claim.

        The synthetic path has no ``broker_positions`` handle to be partial
        about; asserting completion there would invent an observation.
        """
        cursors = [
            _rec_cursor(action="EXIT", target_entry=None, suggested_size_pct=None),
            _position_cursor(current_units=10.0),
            _quote_cursor(last=100.0, bid=99.0, ask=101.0),
            _order_returning_cursor(order_id=13),
            _fill_returning_cursor(fill_id=8),
            _make_cursor([{"current_units": 0}]),
        ]
        conn = _make_conn(cursors)
        result = _execute(conn, recommendation_id=42, decision_id=10)

        assert "POSITION NOT FULLY EXITED" not in result.explanation
        audit_calls = [c for c in conn.execute.call_args_list if "decision_audit" in str(c.args[0])]
        assert len(audit_calls) == 1
        assert "exit_completion" not in audit_calls[0].args[1]["ev"].obj


class TestLoadExitLot:
    """#3006: which broker_positions rows are eligible to be closed."""

    def test_selector_excludes_shorts_and_synthetic_ids(self) -> None:
        """The predicate is the fix, so the predicate is what is asserted.

        ``is_buy`` keeps a short lot out of a path whose accounting is a long
        sale; ``position_id > 0`` keeps out ``-order_id`` synthetic rows, which
        are our record of a fill rather than a broker-closeable handle; and
        #3025's anti-join keeps out a lot the strategy engine actively owns.

        ⚠ Columns are ``bp.``-qualified since #3025 added a correlated subquery —
        the alias is load-bearing, not cosmetic, because ``own`` and ``bp`` both
        carry a position id and an unqualified reference inside the ``EXISTS``
        would resolve to the wrong one.
        """
        cur = _make_cursor([])
        conn = _make_conn([cur])
        assert _load_exit_lot(conn, 1699) is None

        sql = " ".join(str(cur.execute.call_args.args[0]).split())
        assert "AND bp.is_buy" in sql
        assert "AND bp.position_id > 0" in sql
        # #3025: the engine-ownership anti-join. Asserted here only as text — what
        # it actually EXCLUDES is in tests/test_3025_exit_lot_engine_ownership_db.py,
        # against rows.
        assert "strategy_position_ownership" in sql
        assert "own.status = 'active'" in sql
        # Deterministic FIFO: age first, id as the tie-break.
        assert "ORDER BY bp.open_date_time ASC, bp.position_id ASC" in sql

    def test_selector_returns_the_lot_units(self) -> None:
        cur = _make_cursor([{"position_id": 3308442058, "units": Decimal("1000.00000000")}])
        conn = _make_conn([cur])
        lot = _load_exit_lot(conn, 1699)
        assert lot is not None
        assert lot.position_id == 3308442058
        assert lot.units == Decimal("1000.00000000")


class TestExitLotUnitsPrecision:
    """#3006 review NITPICK: the numeric(20,8) → numeric(18,6) copy must not be silent."""

    def _exit_cursors(self, lot_units: float | Decimal) -> list[MagicMock]:
        return [
            _rec_cursor(action="EXIT", target_entry=None, suggested_size_pct=None),
            _position_cursor(current_units=1500.0),
            _make_cursor([{"position_id": 3308442058, "units": lot_units}]),
            _order_returning_cursor(order_id=11),
            _update_cursor(rowcount=1),
            _fill_returning_cursor(fill_id=7),
            _exit_lot_lock_cursor(1000.0),
            _make_cursor([{"current_units": 500}]),
        ]

    def _run(self, lot_units: float | Decimal, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "app.services.order_client.get_runtime_config",
            lambda _conn: _RUNTIME_LIVE,
        )
        broker = MagicMock()
        broker.close_position.return_value = BrokerOrderResult(
            broker_order_ref="ORD-456",
            status="filled",
            filled_price=Decimal("20"),
            filled_units=Decimal("1000"),
            fees=Decimal("0"),
            raw_payload={},
        )
        conn = _make_conn(self._exit_cursors(lot_units))
        _execute(conn, recommendation_id=42, decision_id=10, broker=broker)

    @patch("app.services.order_client._maybe_trigger_attribution")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_genuine_seventh_decimal_is_logged(
        self,
        _mock_now: MagicMock,
        _mock_attr: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level("WARNING", logger="app.services.order_client"):
            self._run(Decimal("1000.12345678"), monkeypatch)
        assert "do not survive the numeric(18,6) orders column" in caplog.text
        assert "1000.123457" in caplog.text

    @patch("app.services.order_client._maybe_trigger_attribution")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_trailing_zeros_are_not_a_loss(
        self,
        _mock_now: MagicMock,
        _mock_attr: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Every lot currently held stores 8 decimals of which the last two are
        zero (e.g. 1305.05709600). Decimal compares by value, so those must not
        raise a precision warning — a warning that fires on every EXIT would be
        noise, not a signal."""
        with caplog.at_level("WARNING", logger="app.services.order_client"):
            self._run(Decimal("1305.05709600"), monkeypatch)
        assert "numeric(18,6)" not in caplog.text


class TestExitPositionRowcountGuard:
    """#3013: the EXIT ledger write must not proceed on a missing positions row."""

    def _live_exit(self, monkeypatch: pytest.MonkeyPatch) -> MagicMock:
        monkeypatch.setattr(
            "app.services.order_client.get_runtime_config",
            lambda _conn: _RUNTIME_LIVE,
        )
        broker = MagicMock()
        broker.close_position.return_value = BrokerOrderResult(
            broker_order_ref="ORD-456",
            status="filled",
            filled_price=Decimal("20"),
            filled_units=Decimal("1000"),
            fees=Decimal("0"),
            raw_payload={},
        )
        return broker

    def _cursors(self) -> list[MagicMock]:
        return [
            _rec_cursor(action="EXIT", target_entry=None, suggested_size_pct=None),
            _position_cursor(current_units=1500.0),
            _make_cursor([{"position_id": 3308442058, "units": 1000.0}]),
            _order_returning_cursor(order_id=11),
            _update_cursor(rowcount=1),
            _fill_returning_cursor(fill_id=7),
            _exit_lot_lock_cursor(1000.0),
            _make_cursor([{"current_units": 500}]),
        ]

    @patch("app.services.order_client._maybe_trigger_attribution")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_a_zero_row_position_update_raises(
        self, _mock_now: MagicMock, _mock_attr: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The broker closed a lot the ledger has no position for.

        The raise aborts the caller's transaction, so the fill, the cash credit
        and the `executed` status roll back together rather than booking a
        disposal of something never held.
        """
        conn = _make_conn(self._cursors())
        conn.execute.return_value.rowcount = 0

        with pytest.raises(RuntimeError, match="expected to update exactly 1 positions row"):
            _execute(conn, recommendation_id=42, decision_id=10, broker=self._live_exit(monkeypatch))

    @patch("app.services.order_client._maybe_trigger_attribution")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_the_healthy_single_row_case_still_completes(
        self, _mock_now: MagicMock, _mock_attr: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pins that the guard is a rowcount check and not a blanket refusal."""
        conn = _make_conn(self._cursors())
        result = _execute(conn, recommendation_id=42, decision_id=10, broker=self._live_exit(monkeypatch))
        assert result.outcome == "filled"


class TestExitLotMirrorDeduction:
    """#3020: a filled EXIT must deduct the broker lot it closed.

    Wiring only — that the deduction is reached on the live path with the right
    bound arguments, and NOT reached on demo. The behaviour of the statement
    itself (proration, the ``FOR UPDATE`` re-read, and that ``_load_exit_lot``
    stops returning the lot) is in ``tests/test_3020_exit_lot_deduction_db.py``,
    because a mocked cursor cannot evaluate any of it.
    """

    @staticmethod
    def _mirror_update_params(conn: MagicMock) -> dict[str, Any] | None:
        """The bound parameters of the broker_positions UPDATE, or None.

        Keyed on the parameter SHAPE rather than the SQL text (#3003): a
        substring match on ``"UPDATE broker_positions"`` passes identically for a
        statement that binds the wrong lot.
        """
        for call in conn.execute.call_args_list:
            if len(call.args) > 1 and isinstance(call.args[1], dict) and "pid" in call.args[1]:
                return call.args[1]
        return None

    def _live_exit_broker(self, monkeypatch: pytest.MonkeyPatch) -> MagicMock:
        monkeypatch.setattr(
            "app.services.order_client.get_runtime_config",
            lambda _conn: _RUNTIME_LIVE,
        )
        broker = MagicMock()
        broker.close_position.return_value = BrokerOrderResult(
            broker_order_ref="ORD-456",
            status="filled",
            filled_price=Decimal("20"),
            filled_units=Decimal("1000"),
            fees=Decimal("0"),
            raw_payload={},
        )
        return broker

    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_an_unexpected_close_position_exception_parks_uncertain(
        self, _mock_now: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """#2942 Delta 1, at the EXIT call site: the call returned by raising, so
        its sends are over and the outcome is unknown — park, never propagate raw."""
        broker = self._live_exit_broker(monkeypatch)
        broker.close_position.side_effect = KeyError("unparseable close ack")
        conn = _make_conn(
            [
                _rec_cursor(action="EXIT", target_entry=None, suggested_size_pct=None),
                _position_cursor(current_units=1500.0),
                _make_cursor([{"position_id": 3308442058, "units": 1000.0}]),
                _order_returning_cursor(order_id=11),
            ]
        )
        with pytest.raises(BrokerSubmissionUncertainError) as raised:
            _execute(conn, recommendation_id=42, decision_id=10, broker=broker)
        assert raised.value.order_id == 11
        park = next(c for c in conn.execute.call_args_list if "status = 'uncertain'" in str(c.args[0]))
        params = park.args[1]
        assert params["message"] == str(KeyError("unparseable close ack"))
        assert params["payload"].obj["exception"] == "KeyError"

    @patch("app.services.order_client._maybe_trigger_attribution")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_a_live_exit_deducts_the_lot_it_closed(
        self, _mock_now: MagicMock, _mock_attr: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        broker = self._live_exit_broker(monkeypatch)
        conn = _make_conn(
            [
                _rec_cursor(action="EXIT", target_entry=None, suggested_size_pct=None),
                _position_cursor(current_units=1500.0),
                _make_cursor([{"position_id": 3308442058, "units": 1000.0}]),
                _order_returning_cursor(order_id=11),
                _update_cursor(rowcount=1),
                _fill_returning_cursor(fill_id=7),
                _exit_lot_lock_cursor(1000.0),
                _make_cursor([{"current_units": 500}]),
            ]
        )
        _execute(conn, recommendation_id=42, decision_id=10, broker=broker)

        params = self._mirror_update_params(conn)
        assert params is not None, "the closed lot was never deducted from broker_positions"
        # The lot the broker was actually asked to close, and the units it filled.
        assert params["pid"] == 3308442058
        assert params["units"] == Decimal("1000.00000000")

    @patch("app.services.order_client._maybe_trigger_attribution")
    @patch("app.services.order_client._utcnow", return_value=_NOW)
    def test_a_demo_exit_writes_nothing_to_the_mirror(
        self, _mock_now: MagicMock, _mock_attr: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Demo resolves no lot, and its own ``broker_positions`` rows carry the
        synthetic negative ids ``_load_exit_lot`` already excludes — there is
        nothing the broker could have closed, so there is nothing to deduct."""
        monkeypatch.setattr(
            "app.services.order_client.get_runtime_config",
            lambda _conn: _RUNTIME_DEMO,
        )
        conn = _make_conn(
            [
                _rec_cursor(action="EXIT", target_entry=None, suggested_size_pct=None),
                _position_cursor(current_units=5.0),
                _quote_cursor(last=100.0, bid=99.5, ask=100.5, spread_pct=0.30),
                _order_returning_cursor(order_id=11),
                _fill_returning_cursor(fill_id=7),
                _make_cursor([{"current_units": 0}]),
            ]
        )
        _execute(conn, recommendation_id=42, decision_id=10, broker=None)

        assert self._mirror_update_params(conn) is None


class TestPendingOrderVerdict:
    """#2942 slice B — the verdict table, asserted without a DB or a broker.

    This mapping is the whole safety argument of the poller: exactly one
    reconciliation state may release a submission claim. Keeping it pure means
    the table can be read off the test rather than inferred from a fixture.
    """

    def test_only_a_broker_rejection_terminalises(self) -> None:
        assert pending_order_verdict("rejected") == "terminalised_rejected"

    def test_a_pending_state_leaves_the_order_alone(self) -> None:
        assert pending_order_verdict("pending") == "still_pending"

    def test_a_fill_is_recognised_but_not_booked(self) -> None:
        """``resolved`` means the broker filled it. The poller records the fact
        and keeps the claim: booking a late fill needs the submission-time exit
        lot and an attended observation to verify."""
        assert pending_order_verdict("resolved") == "filled_not_booked"

    @pytest.mark.parametrize("state", ["not_found", "ambiguous", "error", "unresolved", ""])
    def test_an_unmapped_state_raises_rather_than_guessing(self, state: str) -> None:
        """The advancing verdict releases a claim, so a state nobody mapped must
        stop the order rather than fall through to a default."""
        with pytest.raises(ValueError, match="unmapped reconciliation state"):
            pending_order_verdict(state)


class TestContainedPollErrorsAreNotSilent:
    """#3189 — per-row containment must not trade a loud failure for a silent success.

    Before containment, one unexpected raise failed the whole run. Containing it
    keeps every other order resolvable, but a contained error that reported
    ``success`` would be the #2218 shape: a job that did not do its work while
    status-based monitoring stays green.

    ⚠ What these pin is the CONTRACT the scheduler wires to, not the wiring
    itself — the job body needs credentials, a broker and a database. The
    plausible regression they catch is `poll_error` being moved onto the
    `outcomes` axis, where it would stop degrading the run.
    """

    def test_a_contained_poll_error_degrades_the_run(self) -> None:
        from app.services.job_progress import JobProgress, degradation_reason

        progress = JobProgress(
            candidates_seen=2,
            outcomes={"still_pending": 1},
            errors={"poll_error": 1},
        )
        assert degradation_reason(progress) is not None

    def test_a_clean_batch_does_not_degrade_the_run(self) -> None:
        from app.services.job_progress import JobProgress, degradation_reason

        progress = JobProgress(
            candidates_seen=2,
            outcomes={"still_pending": 2},
            errors={"poll_error": 0},
        )
        assert degradation_reason(progress) is None

    def test_poll_error_is_never_parked(self) -> None:
        """Parking stops the asking, and is only for a PERMANENT property of the
        row. An unpredicted exception is a statement about one attempt."""
        from app.services.order_client import _PARKING_POLL_VERDICTS

        assert "poll_error" not in _PARKING_POLL_VERDICTS
        assert "lock_busy" not in _PARKING_POLL_VERDICTS

    def test_an_identity_mismatch_is_an_error_not_an_outcome(self) -> None:
        """#3189 finding 4 (Codex checkpoint 2). A broker answering about a
        different order is work the job COULD NOT DO. On the outcome axis a run
        in which every lookup broke the identity contract and resolved nothing
        would still record ``success``.

        ⚠ This asserts the SCHEDULER's own tuple, not a restatement of it — the
        job body needs credentials, a broker and a database, so the constant is
        named at module scope precisely so the wiring is reachable from here.
        """
        from app.services.job_progress import JobProgress, degradation_reason
        from app.workers.scheduler import RECONCILE_ERROR_VERDICTS

        assert "identity_mismatch" in RECONCILE_ERROR_VERDICTS
        assert "poll_error" in RECONCILE_ERROR_VERDICTS
        # #3189 finding 4b: a row held back by the environment gate still holds
        # its submission claim and nothing will resolve it while the deployment
        # points elsewhere, so a correct refusal and a silent stall are the same
        # state.
        assert "environment_mismatch" in RECONCILE_ERROR_VERDICTS
        assert degradation_reason(JobProgress(candidates_seen=1, errors={"identity_mismatch": 1})) is not None

    def test_an_identity_mismatch_is_never_parked(self) -> None:
        """A wrong answer is a statement about the ANSWER. Parking it would stop
        the poller re-asking once the broker started answering correctly."""
        from app.services.order_client import _PARKING_POLL_VERDICTS

        assert "identity_mismatch" not in _PARKING_POLL_VERDICTS
