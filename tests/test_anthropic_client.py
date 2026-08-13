"""Bounded Anthropic client factory (#1479 PR2).

Pins the contract that every app-side Anthropic client is constructed
with an explicit bounded ``timeout=`` + ``max_retries=`` — never the
unbounded 600s SDK default that wedged the jobs boot ~43 min on
2026-06-04. The factory is the single permitted construction site
(``scripts/check_anthropic_timeout.sh`` enforces that structurally);
this test pins the values it applies.
"""

from __future__ import annotations

from unittest.mock import patch

import httpx

from app.services.anthropic_client import (
    ANTHROPIC_MAX_RETRIES,
    ANTHROPIC_REQUEST_TIMEOUT,
    make_anthropic_client,
)


def test_timeout_is_bounded_not_the_sdk_default() -> None:
    """The read window must be a bounded value well under the SDK's 600s
    default — that default is exactly what hung the boot thread."""
    assert isinstance(ANTHROPIC_REQUEST_TIMEOUT, httpx.Timeout)
    assert ANTHROPIC_REQUEST_TIMEOUT.read is not None
    assert ANTHROPIC_REQUEST_TIMEOUT.read < 600.0
    assert ANTHROPIC_REQUEST_TIMEOUT.connect is not None
    assert ANTHROPIC_REQUEST_TIMEOUT.connect <= 10.0
    # max_retries must be explicit and below the SDK default of 2 so a
    # wedged read cannot compound into ~3× the read window.
    assert ANTHROPIC_MAX_RETRIES < 2


def test_factory_passes_bounded_timeout_and_retries() -> None:
    """``make_anthropic_client`` must hand the bounded timeout + retry
    policy to the SDK constructor for any caller."""
    with patch("app.services.anthropic_client.anthropic.Anthropic") as ctor:
        make_anthropic_client("sk-test")
    ctor.assert_called_once_with(
        api_key="sk-test",
        timeout=ANTHROPIC_REQUEST_TIMEOUT,
        max_retries=ANTHROPIC_MAX_RETRIES,
    )


def test_factory_accepts_none_api_key() -> None:
    """``settings.anthropic_api_key`` is ``str | None``; ``None`` must be
    accepted (SDK then resolves from the ANTHROPIC_API_KEY env var)."""
    with patch("app.services.anthropic_client.anthropic.Anthropic") as ctor:
        make_anthropic_client(None)
    ctor.assert_called_once_with(
        api_key=None,
        timeout=ANTHROPIC_REQUEST_TIMEOUT,
        max_retries=ANTHROPIC_MAX_RETRIES,
    )
