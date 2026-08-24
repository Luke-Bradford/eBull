from __future__ import annotations

import pytest

from app.services.r6_pit_universe import common_equity_reason, is_common_equity


@pytest.mark.parametrize(
    "title",
    [
        "Class A Common Stock, par value $0.0001 per share",
        "Common Shares, no par value",
        "Class A ordinary shares, par value $0.0001 per share",
    ],
)
def test_common_equity_titles_are_included(title: str) -> None:
    assert is_common_equity(security_title=title, exchange="NASDAQ")


@pytest.mark.parametrize(
    "title,term",
    [
        ("Warrants to purchase common stock", "warrant"),
        ("Class A common stock included as part of the units", "unit"),
        ("American Depositary Shares", "depositary"),
        ("Preferred Stock Purchase Rights", "preferred"),
        ("Common Units of Beneficial Interest", "beneficial interest"),
    ],
)
def test_non_common_classes_cannot_enter_through_underlying_text(title: str, term: str) -> None:
    assert common_equity_reason(security_title=title, exchange="NYSE") == f"excluded_title:{term}"


def test_unlisted_or_unrecognised_class_is_excluded() -> None:
    assert common_equity_reason(security_title="Common Stock", exchange="NONE") == "unsupported_exchange"
    assert common_equity_reason(security_title="Shares", exchange="NYSE") == "unrecognised_security_title"
