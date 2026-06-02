"""Unit tests for resolve_quote_price — the shared usable-live-quote rule.

#1428: eToro persists ``quotes.last = 0.00`` for instruments not freshly
traded (bid/ask present, no recent trade). A non-positive ``last`` must be
treated as missing, never as a valid 0 mark (which would value a position
at 0 → fake −100% P&L). Fall back to the live bid/ask mid when available.
"""

from __future__ import annotations

from app.api._helpers import resolve_quote_price


def test_positive_last_is_used() -> None:
    assert resolve_quote_price(190.0, None, None) == 190.0


def test_zero_last_falls_back_to_bid_ask_mid() -> None:
    assert resolve_quote_price(0.0, 697.16, 697.22) == 697.19


def test_negative_last_falls_back_to_bid_ask_mid() -> None:
    assert resolve_quote_price(-1.0, 10.0, 12.0) == 11.0


def test_none_last_falls_back_to_bid_ask_mid() -> None:
    assert resolve_quote_price(None, 10.0, 12.0) == 11.0


def test_zero_last_and_no_usable_bid_ask_returns_none() -> None:
    assert resolve_quote_price(0.0, None, None) is None
    assert resolve_quote_price(0.0, 0.0, 0.0) is None


def test_partial_bid_ask_not_used() -> None:
    # A one-sided book is not a usable mid.
    assert resolve_quote_price(0.0, 10.0, None) is None
    assert resolve_quote_price(0.0, None, 12.0) is None
    assert resolve_quote_price(0.0, 0.0, 12.0) is None
