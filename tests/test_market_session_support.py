"""The venue-support allow-list is shared by the core submission and selection
paths (#2312 / #2603); these pin the direction it fails in."""

import pytest

from app.services.market_session_support import (
    SESSION_SUPPORTED_ASSET_CLASSES,
    session_support_reason,
)


def test_the_only_supported_class_is_the_one_calendar_we_actually_have() -> None:
    """``app/services/market_calendar.py`` is NYSE-scoped by its own docstring, so
    a second entry here without a second calendar would be a false claim."""
    assert SESSION_SUPPORTED_ASSET_CLASSES == frozenset({"us_equity"})
    assert session_support_reason("us_equity") is None


@pytest.mark.parametrize(
    "asset_class",
    # `uk_equity` is the live one: two of three #2833 core candidates sit there.
    # The rest are real `exchanges.asset_class` values, plus a class added AFTER
    # this code was written — an allow-list must put that on the refuse side.
    ["uk_equity", "eu_equity", "asia_equity", "mena_equity", "crypto", "commodity", "fx", "index", "unknown", None],
)
def test_every_unsupported_or_unknown_venue_class_refuses(asset_class: str | None) -> None:
    reason = session_support_reason(asset_class)
    assert reason is not None
    assert reason.startswith(f"asset_class={asset_class!r}")


def test_the_detail_prefix_is_stable_for_the_refusal_that_records_it() -> None:
    """``core_unsupported_market_session`` records this string as its ``detail``.
    The prefix is the part that identifies WHICH class was seen, so it is pinned
    separately from the explanatory tail."""
    assert session_support_reason("uk_equity").startswith("asset_class='uk_equity'")  # type: ignore[union-attr]
