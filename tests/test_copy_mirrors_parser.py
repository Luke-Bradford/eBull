# tests/test_copy_mirrors_parser.py
"""§8.1 parser unit tests for copy-trading mirror ingestion.

Pure unit tests — no DB, no I/O, no broker HTTP. Exercises
_parse_mirror / _parse_mirror_position and the outer top-level
loop in etoro_broker.get_portfolio's mirrors[] branch.
"""

from __future__ import annotations

import decimal
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from app.providers.broker import BrokerMirror, BrokerMirrorPosition
from app.providers.implementations.etoro_broker import (
    PortfolioParseError,
    _parse_mirror,
    _parse_mirror_position,
    _parse_mirrors_payload,
)


def test_portfolio_parse_error_is_direct_exception_subclass() -> None:
    """Spec §2.2.1: PortfolioParseError MUST subclass Exception directly.

    If it subclassed ValueError / TypeError / KeyError /
    decimal.DecimalException, the outer parse loop's
    `except (KeyError, ValueError, TypeError, decimal.DecimalException)`
    block would silently swallow it, defeating the §2.3.3 strict-raise
    and enabling the §2.3.4 soft-close hole Codex v3 finding V flagged.
    """
    assert issubclass(PortfolioParseError, Exception) is True
    assert issubclass(PortfolioParseError, ValueError) is False
    assert issubclass(PortfolioParseError, TypeError) is False
    assert issubclass(PortfolioParseError, KeyError) is False
    assert issubclass(PortfolioParseError, decimal.DecimalException) is False


def test_portfolio_parse_error_is_raisable_with_cause() -> None:
    inner = ValueError("boom")
    with pytest.raises(PortfolioParseError) as excinfo:
        raise PortfolioParseError("wrap") from inner
    assert excinfo.value.__cause__ is inner


def _make_position_payload(**overrides: Any) -> dict[str, Any]:
    """Return a valid mirror-position payload; override any field."""
    base: dict[str, Any] = {
        "positionID": 1001,
        "parentPositionID": 5001,
        "instrumentID": 42,
        "isBuy": True,
        "units": "6.28927",
        "amount": "101.08",
        "initialAmountInDollars": "101.08",
        "openRate": "1207.4994",
        "openConversionRate": "0.01331",
        "openDateTime": "2026-04-10T00:00:00Z",
        "takeProfitRate": None,
        "stopLossRate": None,
        "totalFees": "0",
        "leverage": 1,
    }
    base.update(overrides)
    return base


def test_parse_mirror_position_happy_path_non_usd() -> None:
    payload = _make_position_payload()
    pos = _parse_mirror_position(payload)
    assert isinstance(pos, BrokerMirrorPosition)
    assert pos.position_id == 1001
    assert pos.instrument_id == 42
    assert pos.is_buy is True
    assert pos.units == Decimal("6.28927")
    assert pos.open_rate == Decimal("1207.4994")
    assert pos.open_conversion_rate == Decimal("0.01331")  # FX round-trip
    assert pos.open_date_time == datetime(2026, 4, 10, 0, 0, tzinfo=UTC)
    assert pos.take_profit_rate is None
    assert pos.stop_loss_rate is None
    assert pos.total_fees == Decimal("0")
    assert pos.leverage == 1
    assert pos.raw_payload is payload  # stored as-is


def test_parse_mirror_position_missing_open_conversion_rate_raises() -> None:
    """Spec §2.2.2: openConversionRate is a required field in prod
    — no silent default. A mirror-position without it raises."""
    payload = _make_position_payload()
    del payload["openConversionRate"]
    with pytest.raises(KeyError):
        _parse_mirror_position(payload)


def test_parse_mirror_position_non_numeric_units_raises_decimal_exc() -> None:
    """Spec §2.2.2 + §8.1: Decimal(str('bogus')) raises
    decimal.InvalidOperation, a subclass of DecimalException —
    NOT a ValueError. This test pins the exception type so the
    caller's `except DecimalException` clause catches correctly."""
    payload = _make_position_payload(units="bogus")
    with pytest.raises(decimal.DecimalException):
        _parse_mirror_position(payload)


def test_parse_mirror_position_optional_fields_none() -> None:
    payload = _make_position_payload(takeProfitRate=None, stopLossRate=None)
    pos = _parse_mirror_position(payload)
    assert pos.take_profit_rate is None
    assert pos.stop_loss_rate is None


def test_parse_mirror_position_optional_fields_present() -> None:
    payload = _make_position_payload(takeProfitRate="1500.0", stopLossRate="1000.0")
    pos = _parse_mirror_position(payload)
    assert pos.take_profit_rate == Decimal("1500.0")
    assert pos.stop_loss_rate == Decimal("1000.0")


def _make_mirror_payload(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "mirrorID": 15712187,
        "parentCID": 111,
        "parentUsername": "thomaspj",
        "initialInvestment": "20000",
        "depositSummary": "0",
        "withdrawalSummary": "0",
        "availableAmount": "2800.33",
        "closedPositionsNetProfit": "-110.34",
        "stopLossPercentage": None,
        "stopLossAmount": None,
        "mirrorStatusID": None,
        "mirrorCalculationType": None,
        "pendingForClosure": False,
        "startedCopyDate": "2025-01-01T00:00:00Z",
        "positions": [_make_position_payload(positionID=1001)],
    }
    base.update(overrides)
    return base


def test_parse_mirror_happy_path() -> None:
    payload = _make_mirror_payload()
    mirror = _parse_mirror(payload)
    assert isinstance(mirror, BrokerMirror)
    assert mirror.mirror_id == 15712187
    assert mirror.parent_cid == 111
    assert mirror.parent_username == "thomaspj"
    assert mirror.available_amount == Decimal("2800.33")
    assert mirror.closed_positions_net_profit == Decimal("-110.34")
    assert len(mirror.positions) == 1
    assert mirror.positions[0].position_id == 1001
    assert mirror.started_copy_date == datetime(2025, 1, 1, tzinfo=UTC)
    assert mirror.raw_payload is payload


def test_parse_mirror_empty_positions_is_valid() -> None:
    """A mirror with positions == [] is a valid state (holds only cash).

    §2.2.2: raw_positions == [] yields positions=(), which the §3.2
    AUM formula in Track 1b handles as mirror_equity = available_amount.
    Nothing raises.
    """
    payload = _make_mirror_payload(positions=[])
    mirror = _parse_mirror(payload)
    assert mirror.positions == ()


def test_parse_mirror_nested_failure_wraps_with_index() -> None:
    """Spec §2.2.2: inner loop catches (KeyError, ValueError, TypeError,
    DecimalException) and re-raises as PortfolioParseError with both
    the mirror_id AND the position index in the message."""
    bad_pos = _make_position_payload(positionID=9999, units="bogus")
    payload = _make_mirror_payload(
        positions=[
            _make_position_payload(positionID=1001),
            _make_position_payload(positionID=1002),
            bad_pos,  # idx 2 — this is the failing one
        ]
    )
    with pytest.raises(PortfolioParseError) as excinfo:
        _parse_mirror(payload)
    msg = str(excinfo.value)
    assert "15712187" in msg
    assert "position[2]" in msg
    assert isinstance(excinfo.value.__cause__, decimal.InvalidOperation)


def test_parse_mirror_nested_key_error_wraps() -> None:
    """Missing openConversionRate in a nested position raises
    KeyError from _parse_mirror_position, which _parse_mirror's
    inner wrap catches and re-raises as PortfolioParseError."""
    bad_pos = _make_position_payload(positionID=9999)
    del bad_pos["openConversionRate"]
    payload = _make_mirror_payload(positions=[bad_pos])
    with pytest.raises(PortfolioParseError) as excinfo:
        _parse_mirror(payload)
    assert "15712187" in str(excinfo.value)
    assert "position[0]" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, KeyError)


# ---------------------------------------------------------------------------
# Task 8: _parse_mirrors_payload — outer top-level loop
# ---------------------------------------------------------------------------


def test_parse_mirrors_payload_happy_path_two_mirrors() -> None:
    raw = [_make_mirror_payload(mirrorID=1), _make_mirror_payload(mirrorID=2)]
    result = _parse_mirrors_payload(raw)
    assert len(result) == 2
    assert result[0].mirror_id == 1
    assert result[1].mirror_id == 2


def test_parse_mirrors_payload_skips_unrecognisable_no_mirror_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Spec §2.2.2: the ONLY surviving log-and-skip path is a row
    with no usable mirrorID — it cannot collide with any known
    local row, so it is safe to skip."""
    raw = [
        {"not a mirror": True},  # no mirrorID → safe skip
        "not even a dict",  # not a dict → safe skip
        _make_mirror_payload(mirrorID=42),  # valid → parsed
    ]
    with caplog.at_level(logging.WARNING):
        result = _parse_mirrors_payload(raw)
    assert len(result) == 1
    assert result[0].mirror_id == 42
    assert any("unrecognisable" in rec.message.lower() for rec in caplog.records)


def test_parse_mirrors_payload_known_mirror_top_level_failure_raises() -> None:
    """Spec §2.2.2: a row with a recognisable mirrorID but a
    missing/malformed required top-level field raises
    PortfolioParseError — NOT log-and-skip. Otherwise the sync
    would then interpret this as a disappearance and soft-close
    the local row (Codex v3 finding V parse-and-soft-close hole)."""
    bad = _make_mirror_payload(mirrorID=15712187)
    del bad["availableAmount"]
    raw = [bad, _make_mirror_payload(mirrorID=42)]
    with pytest.raises(PortfolioParseError) as excinfo:
        _parse_mirrors_payload(raw)
    assert "15712187" in str(excinfo.value)
    # The underlying cause is a KeyError on the missing key.
    assert isinstance(excinfo.value.__cause__, KeyError)


def test_parse_mirrors_payload_known_mirror_decimal_failure_raises() -> None:
    """Non-numeric top-level availableAmount raises
    decimal.InvalidOperation, which the outer fallback catch wraps
    as PortfolioParseError with mirror_id attribution."""
    bad = _make_mirror_payload(mirrorID=15712187, availableAmount="bogus")
    with pytest.raises(PortfolioParseError) as excinfo:
        _parse_mirrors_payload([bad])
    assert "15712187" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, decimal.InvalidOperation)


def test_parse_mirrors_payload_nested_failure_propagates_unchanged() -> None:
    """Spec §2.2.2: the outer loop's `except PortfolioParseError: raise`
    preserves the inner-loop's position[idx] attribution."""
    bad_pos = _make_position_payload(positionID=9999, units="bogus")
    bad_mirror = _make_mirror_payload(mirrorID=15712187, positions=[bad_pos])
    with pytest.raises(PortfolioParseError) as excinfo:
        _parse_mirrors_payload([bad_mirror])
    assert "15712187" in str(excinfo.value)
    assert "position[0]" in str(excinfo.value)
