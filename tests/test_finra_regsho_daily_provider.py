"""Tests for ``FinraRegShoProvider`` — G6/#916 Phase 6 PR 12.

Mirrors ``tests/test_finra_short_interest_provider.py``. The new daily
provider shares the same module-global throttle clock + lock as the
bimonthly sibling — covered by ``test_rate_limit_clock_identity_shared_with_bimonthly``.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import date

import httpx
import pytest

from app.providers.implementations import finra_regsho as _regsho_module
from app.providers.implementations.finra_regsho import (
    PREFIXES,
    FinraRegShoProvider,
)
from app.providers.implementations.finra_short_interest import (
    _FINRA_MIN_INTERVAL_S,
    _FINRA_RATE_LIMIT_CLOCK,
    _FINRA_RATE_LIMIT_LOCK,
    FinraNotFound,
    FinraShortInterestProvider,
)
from app.providers.resilient_client import ResilientClient


def _make_provider() -> FinraRegShoProvider:
    return FinraRegShoProvider()


def _rewire_transport(
    provider: FinraRegShoProvider,
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    preserve_throttle: bool = False,
    max_retries: int = 0,
) -> None:
    new_client = httpx.Client(
        headers={"User-Agent": "ebull-test/0.0", "Accept": "text/plain,*/*"},
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


def test_regsho_daily_url_builder_iso_date() -> None:
    p = _make_provider()
    assert (
        p.regsho_daily_url(date(2026, 5, 15), "CNMS")
        == "https://cdn.finra.org/equity/regsho/daily/CNMSshvol20260515.txt"
    )


def test_regsho_daily_url_builder_all_prefixes() -> None:
    p = _make_provider()
    for prefix in PREFIXES:
        url = p.regsho_daily_url(date(2026, 5, 15), prefix)
        assert url == f"https://cdn.finra.org/equity/regsho/daily/{prefix}shvol20260515.txt"


def test_regsho_daily_url_builder_unknown_prefix_raises() -> None:
    p = _make_provider()
    with pytest.raises(ValueError, match="unknown FINRA RegSHO prefix"):
        p.regsho_daily_url(date(2026, 5, 15), "ZZZZ")


def test_regsho_daily_url_builder_leap_year() -> None:
    p = _make_provider()
    assert (
        p.regsho_daily_url(date(2024, 2, 29), "CNMS")
        == "https://cdn.finra.org/equity/regsho/daily/CNMSshvol20240229.txt"
    )


# ----------------------------------------------------------------------
# 2 — 404 → FinraNotFound
# ----------------------------------------------------------------------


def test_404_raises_finra_not_found() -> None:
    p = _make_provider()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, request=request)

    _rewire_transport(p, handler)

    with pytest.raises(FinraNotFound, match="CNMSshvol20260515.txt"):
        p.fetch_regsho_daily_file(date(2026, 5, 15), "CNMS")


def test_403_raises_finra_not_found() -> None:
    """FINRA's RegSHO CDN returns 403 (NOT 404) for not-yet-published
    trade dates. Empirically verified 2026-05-18 live-smoke against
    the actual cdn.finra.org endpoint. Both statuses MUST map to
    FinraNotFound so the cron can safely run before EOD publication.
    """
    p = _make_provider()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, request=request)

    _rewire_transport(p, handler)

    with pytest.raises(FinraNotFound, match="CNMSshvol20260515.txt"):
        p.fetch_regsho_daily_file(date(2026, 5, 15), "CNMS")


# ----------------------------------------------------------------------
# 3 — 5xx → HTTPStatusError
# ----------------------------------------------------------------------


def test_5xx_raises_http_status_error() -> None:
    p = _make_provider()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, request=request)

    _rewire_transport(p, handler, max_retries=0)

    with pytest.raises(httpx.HTTPStatusError):
        p.fetch_regsho_daily_file(date(2026, 5, 15), "CNMS")


# ----------------------------------------------------------------------
# 4 — Happy path returns bytes
# ----------------------------------------------------------------------


def test_happy_path_returns_bytes() -> None:
    p = _make_provider()
    payload = b"Date|Symbol|...\n20260515|AAPL|..."

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=payload, request=request)

    _rewire_transport(p, handler)

    result = p.fetch_regsho_daily_file(date(2026, 5, 15), "CNMS")
    assert result == payload


# ----------------------------------------------------------------------
# 5 — Rate-limit clock IDENTITY-shared with bimonthly
# ----------------------------------------------------------------------


def test_rate_limit_clock_identity_shared_with_bimonthly() -> None:
    """The daily provider MUST share the FINRA throttle module-globals
    with the bimonthly sibling so combined fetch in one process never
    exceeds 1 req/s. The list IDENTITY (``is`` operator) is the contract
    the prevention-log #726 rule requires.
    """
    daily = FinraRegShoProvider()
    bimonthly = FinraShortInterestProvider()
    assert daily._http._last_request_at is _FINRA_RATE_LIMIT_CLOCK  # noqa: SLF001
    assert bimonthly._http._last_request_at is _FINRA_RATE_LIMIT_CLOCK  # noqa: SLF001
    assert daily._http._last_request_at is bimonthly._http._last_request_at  # noqa: SLF001


def test_rate_limit_clock_identity_across_daily_instances() -> None:
    """Two daily provider instances share the same throttle clock."""
    p1 = FinraRegShoProvider()
    p2 = FinraRegShoProvider()
    assert p1._http._last_request_at is p2._http._last_request_at  # noqa: SLF001


# ----------------------------------------------------------------------
# 6 — Back-to-back throttle smoke
# ----------------------------------------------------------------------


def test_back_to_back_throttle_enforces_min_interval() -> None:
    p = _make_provider()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"ok", request=request)

    _rewire_transport(p, handler, preserve_throttle=True)

    _FINRA_RATE_LIMIT_CLOCK[0] = 0.0
    try:
        t0 = time.monotonic()
        p.fetch_regsho_daily_file(date(2026, 5, 15), "CNMS")
        p.fetch_regsho_daily_file(date(2026, 5, 15), "FNQC")
        elapsed = time.monotonic() - t0
        assert elapsed >= _FINRA_MIN_INTERVAL_S - 0.05, (
            f"back-to-back fetch elapsed {elapsed:.3f}s, expected >= {_FINRA_MIN_INTERVAL_S - 0.05:.3f}s"
        )
    finally:
        _FINRA_RATE_LIMIT_CLOCK[0] = 0.0


# ----------------------------------------------------------------------
# 7 — Base URL + module-level constants
# ----------------------------------------------------------------------


def test_base_url_constant() -> None:
    assert FinraRegShoProvider.BASE_URL == "https://cdn.finra.org/equity/regsho/daily/"


def test_prefixes_tuple_membership() -> None:
    assert PREFIXES == ("CNMS", "FNQC", "FNRA", "FNSQ", "FNYX", "FORF")


def test_module_globals_imported_from_bimonthly() -> None:
    """The daily module MUST re-use the bimonthly's module-globals via
    import — NOT create its own copies (would double the FINRA budget).
    """
    # The provider module imports the throttle objects but doesn't
    # define them — sanity-check by reading them straight out of the
    # bimonthly module.
    from app.providers.implementations import finra_short_interest as _fsi

    assert _regsho_module._FINRA_RATE_LIMIT_CLOCK is _fsi._FINRA_RATE_LIMIT_CLOCK
    assert _regsho_module._FINRA_RATE_LIMIT_LOCK is _fsi._FINRA_RATE_LIMIT_LOCK
