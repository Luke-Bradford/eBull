"""Tests for ``FinraShortInterestProvider`` — G6/#915 Phase 6 PR 11.

Provider unit-level tests: URL builder shape, 404 → ``FinraNotFound``,
5xx → re-raise (with attached Request), shared rate-limit clock
identity, back-to-back throttle smoke.

Pattern reference: ``tests/test_sec_fundamentals_companyconcept.py``
(G10) — MockTransport-based isolation + ``max_retries=0`` test wrapper
to drop 5xx wall-clock from ~7s default backoff to ~0.1s.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import date

import httpx
import pytest

from app.providers.implementations import finra_short_interest as _fsi_module
from app.providers.implementations.finra_short_interest import (
    _FINRA_MIN_INTERVAL_S,
    _FINRA_RATE_LIMIT_CLOCK,
    _FINRA_RATE_LIMIT_LOCK,
    FinraNotFound,
    FinraShortInterestProvider,
)
from app.providers.resilient_client import ResilientClient

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _make_provider() -> FinraShortInterestProvider:
    return FinraShortInterestProvider()


def _rewire_transport(
    provider: FinraShortInterestProvider,
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    preserve_throttle: bool = False,
    max_retries: int = 0,
) -> None:
    """Swap the provider's underlying HTTP client for a MockTransport.

    ``preserve_throttle=False`` (default) — sets
    ``min_request_interval_s=0.0`` so unit tests skip the rate-limit
    gate; ``True`` re-uses ``_FINRA_RATE_LIMIT_CLOCK`` + LOCK to
    exercise the throttle path end-to-end.

    ``max_retries=0`` — disables ``ResilientClient`` retry/backoff so
    5xx fixtures fire once and propagate cleanly (Codex G10 r1 LOW-3
    pattern; without this, the 5xx test sleeps ~7s through the default
    exponential backoff schedule).
    """
    new_client = httpx.Client(
        headers={"User-Agent": "ebull-test/0.0", "Accept": "text/csv,*/*"},
        transport=httpx.MockTransport(handler),
    )
    if preserve_throttle:
        provider._http = ResilientClient(  # noqa: SLF001
            new_client,
            min_request_interval_s=_FINRA_MIN_INTERVAL_S,
            shared_last_request=_FINRA_RATE_LIMIT_CLOCK,
            shared_throttle_lock=_FINRA_RATE_LIMIT_LOCK,
            max_retries=max_retries,
        )
    else:
        provider._http = ResilientClient(  # noqa: SLF001
            new_client,
            min_request_interval_s=0.0,
            max_retries=max_retries,
        )


# ----------------------------------------------------------------------
# 1 — URL builder
# ----------------------------------------------------------------------


def test_settlement_file_url_iso_date() -> None:
    p = _make_provider()
    assert (
        p.settlement_file_url(date(2026, 4, 30)) == "https://cdn.finra.org/equity/otcmarket/biweekly/shrt20260430.csv"
    )


def test_settlement_file_url_leap_year() -> None:
    p = _make_provider()
    assert (
        p.settlement_file_url(date(2024, 2, 29)) == "https://cdn.finra.org/equity/otcmarket/biweekly/shrt20240229.csv"
    )


def test_settlement_file_url_january_one() -> None:
    p = _make_provider()
    assert p.settlement_file_url(date(2026, 1, 1)) == "https://cdn.finra.org/equity/otcmarket/biweekly/shrt20260101.csv"


# ----------------------------------------------------------------------
# 2 — 404 → FinraNotFound
# ----------------------------------------------------------------------


def test_404_raises_finra_not_found() -> None:
    p = _make_provider()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, request=request)

    _rewire_transport(p, handler)

    with pytest.raises(FinraNotFound, match="shrt20260430.csv"):
        p.fetch_settlement_file(date(2026, 4, 30))


# ----------------------------------------------------------------------
# 3 — 5xx → HTTPStatusError (with Request attached so raise_for_status fires)
# ----------------------------------------------------------------------


def test_5xx_raises_http_status_error() -> None:
    p = _make_provider()

    def handler(request: httpx.Request) -> httpx.Response:
        # Attach Request so raise_for_status fires cleanly (G10 r1 MED-3).
        return httpx.Response(500, request=request)

    _rewire_transport(p, handler, max_retries=0)

    with pytest.raises(httpx.HTTPStatusError):
        p.fetch_settlement_file(date(2026, 4, 30))


# ----------------------------------------------------------------------
# 4 — Happy path returns bytes
# ----------------------------------------------------------------------


def test_happy_path_returns_bytes() -> None:
    p = _make_provider()
    payload = b"accountingYearMonthNumber|symbolCode|...\n20260430|AAPL|..."

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=payload, request=request)

    _rewire_transport(p, handler)

    result = p.fetch_settlement_file(date(2026, 4, 30))
    assert result == payload


# ----------------------------------------------------------------------
# 5 — Rate-limit clock identity (shared across instances)
# ----------------------------------------------------------------------


def test_rate_limit_clock_identity_across_instances() -> None:
    """Two ``FinraShortInterestProvider`` instances share the same
    module-global throttle list (prevention-log #726)."""
    p1 = FinraShortInterestProvider()
    p2 = FinraShortInterestProvider()
    # The provider stores the ResilientClient which holds a reference
    # to the shared list. The list identity (not equality) is the
    # contract the prevention-log requires.
    assert p1._http._last_request_at is _FINRA_RATE_LIMIT_CLOCK  # noqa: SLF001
    assert p2._http._last_request_at is _FINRA_RATE_LIMIT_CLOCK  # noqa: SLF001
    assert p1._http._last_request_at is p2._http._last_request_at  # noqa: SLF001


# ----------------------------------------------------------------------
# 6 — Back-to-back throttle smoke
# ----------------------------------------------------------------------


def test_back_to_back_throttle_enforces_min_interval() -> None:
    """Two consecutive fetches against MockTransport should be spaced
    >= _FINRA_MIN_INTERVAL_S apart when ``preserve_throttle=True``.
    """
    p = _make_provider()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"ok", request=request)

    _rewire_transport(p, handler, preserve_throttle=True)

    # Reset the shared clock so the first fetch isn't throttled by
    # state leaked from earlier tests.
    _FINRA_RATE_LIMIT_CLOCK[0] = 0.0
    try:
        t0 = time.monotonic()
        p.fetch_settlement_file(date(2026, 4, 30))
        p.fetch_settlement_file(date(2026, 4, 15))
        elapsed = time.monotonic() - t0
        # Allow a small tolerance below the floor to absorb sleep
        # granularity / scheduler jitter on slow CI.
        assert elapsed >= _FINRA_MIN_INTERVAL_S - 0.05, (
            f"back-to-back fetch elapsed {elapsed:.3f}s, expected >= {_FINRA_MIN_INTERVAL_S - 0.05:.3f}s"
        )
    finally:
        # Teardown — reset shared clock so other tests aren't affected.
        _FINRA_RATE_LIMIT_CLOCK[0] = 0.0


# ----------------------------------------------------------------------
# 7 — Base URL constant + module-global pattern
# ----------------------------------------------------------------------


def test_base_url_constant() -> None:
    assert FinraShortInterestProvider.BASE_URL == "https://cdn.finra.org/equity/otcmarket/biweekly/"


def test_module_globals_exist() -> None:
    """Sanity: the shared-throttle module globals are at module top
    (not lazy-created per-instance), so multiple provider instances
    coordinate.
    """
    assert isinstance(_fsi_module._FINRA_RATE_LIMIT_CLOCK, list)
    assert _fsi_module._FINRA_RATE_LIMIT_LOCK is not None
    assert _fsi_module._FINRA_MIN_INTERVAL_S == 1.0
