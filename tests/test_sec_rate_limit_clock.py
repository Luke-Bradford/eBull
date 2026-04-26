"""Tests for process-wide SEC rate-limit clock (#537).

Pre-#537 each ``SecFilingsProvider`` / ``SecFundamentalsProvider``
instance carried its own ``shared_last_request`` list. Concurrent
ingest jobs (e.g. ``sec_8k_events_ingest`` + ``sec_insider_transactions_ingest``
both firing on the hour) collectively bursted past the SEC fair-use
10 req/s limit even though each individual provider stayed under.

Post-#537 every provider in this Python process funnels through
``_PROCESS_RATE_LIMIT_CLOCK`` so the global budget is respected.
"""

from __future__ import annotations

from app.providers.implementations.sec_edgar import (
    _PROCESS_RATE_LIMIT_CLOCK,
    SecFilingsProvider,
)
from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider


def test_filings_provider_instances_share_process_clock() -> None:
    p1 = SecFilingsProvider(user_agent="test-ua")
    p2 = SecFilingsProvider(user_agent="test-ua")
    try:
        # Both clients on a single instance + clients on a separate
        # instance must reference the SAME list object (identity, not
        # equality) so writes on one tick the other.
        assert p1._http._last_request_at is _PROCESS_RATE_LIMIT_CLOCK
        assert p1._http_tickers._last_request_at is _PROCESS_RATE_LIMIT_CLOCK
        assert p2._http._last_request_at is _PROCESS_RATE_LIMIT_CLOCK
        assert p2._http_tickers._last_request_at is _PROCESS_RATE_LIMIT_CLOCK
    finally:
        p1.__exit__(None, None, None)
        p2.__exit__(None, None, None)


def test_fundamentals_provider_shares_process_clock_with_filings_provider() -> None:
    """Cross-class sharing: SecFundamentalsProvider funnels through
    the same _PROCESS_RATE_LIMIT_CLOCK as SecFilingsProvider so a
    fundamentals_sync run + a filings ingest run cannot collectively
    exceed the SEC 10 req/s budget."""
    filings = SecFilingsProvider(user_agent="test-ua")
    fundamentals = SecFundamentalsProvider(user_agent="test-ua")
    try:
        assert filings._http._last_request_at is _PROCESS_RATE_LIMIT_CLOCK
        assert fundamentals._http._last_request_at is _PROCESS_RATE_LIMIT_CLOCK
        # And critically: both reference the same object.
        assert filings._http._last_request_at is fundamentals._http._last_request_at
    finally:
        filings.__exit__(None, None, None)
        fundamentals.__exit__(None, None, None)
