"""FINRA Equity Short Interest CDN provider (#915 — Phase 6 PR 11).

Spec: docs/superpowers/specs/2026-05-18-finra-bimonthly-short-interest.md.
Spike: docs/superpowers/spikes/2026-05-18-finra-bimonthly-short-interest-feasibility.md.

Anonymous CDN access at ``https://cdn.finra.org/equity/otcmarket/biweekly/``.
1 req/s polite floor (FINRA publishes no explicit rate limit on the
equity short interest catalog page; CDN robots.txt is 403). Independent
of the SEC rate-limit pool — different host, no shared per-IP budget.

The module-global throttle clock + lock pattern mirrors
``app/providers/implementations/sec_edgar.py:54-80, 237-253``. Multiple
``FinraShortInterestProvider`` instances coordinate via
``ResilientClient.shared_last_request`` + ``shared_throttle_lock`` so a
single FINRA rate budget is enforced cluster-wide — preserves the
prevention-log #726 rule at ``docs/review-prevention-log.md:510-513``.

Endpoint shape (empirically verified 2026-05-18 in spike §4):

  URL: https://cdn.finra.org/equity/otcmarket/biweekly/shrt{YYYYMMDD}.csv
  Format: pipe-delimited TEXT (despite ``.csv`` extension).
  Auth: none.
  Cadence: bimonthly — 15th + last business day of each month.
  Archive: 2014-→; post-June-2021 covers exchange-listed cohort.

Provider surface is intentionally minimal: URL builder + GET with
``FinraNotFound`` on 404 + ``raise_for_status()`` on 5xx. Symbol
normalisation, parsing, and ingest all live in the service layer
(``app/services/finra_short_interest_ingest.py``) per the
provider-thin discipline.
"""

from __future__ import annotations

import threading
from datetime import date
from typing import Final

import httpx

from app.providers.resilient_client import ResilientClient

# Module-global throttle state. Shared across multiple
# ``FinraShortInterestProvider`` instances via the ResilientClient
# ``shared_last_request`` + ``shared_throttle_lock`` parameters.
# Preserves the "Multiple ResilientClient instances sharing a rate
# limit must share throttle state" prevention-log rule (#726).
_FINRA_RATE_LIMIT_CLOCK: Final[list[float]] = [0.0]
_FINRA_RATE_LIMIT_LOCK: Final[threading.Lock] = threading.Lock()
_FINRA_MIN_INTERVAL_S: Final[float] = 1.0


class FinraNotFound(Exception):
    """404 from the FINRA CDN — file not yet published or archive purged.

    The ScheduledJob treats this as a benign skip (next-fire revisits
    on the cron cadence). Distinguishable from 5xx / network errors so
    that those propagate as job-failure signals.
    """


class FinraShortInterestProvider:
    """Anonymous CDN client for FINRA bimonthly equity short interest.

    Parameters
    ----------
    http_client:
        Optional ``ResilientClient`` injection — primarily for tests
        with ``httpx.MockTransport``. Default construction wires a
        fresh ``httpx.Client`` with the shared throttle list/lock so
        sibling providers in the same process never combine to
        exceed the 1 req/s ceiling.
    """

    BASE_URL: Final[str] = "https://cdn.finra.org/equity/otcmarket/biweekly/"

    def __init__(self, http_client: ResilientClient | None = None) -> None:
        if http_client is None:
            inner = httpx.Client(
                timeout=httpx.Timeout(30.0),
                headers={
                    "User-Agent": "eBull/0.1 (luke.bradford@hotmail.co.uk)",
                    "Accept": "text/csv,*/*",
                },
            )
            http_client = ResilientClient(
                inner,
                min_request_interval_s=_FINRA_MIN_INTERVAL_S,
                shared_last_request=_FINRA_RATE_LIMIT_CLOCK,
                shared_throttle_lock=_FINRA_RATE_LIMIT_LOCK,
            )
        self._http = http_client

    def settlement_file_url(self, settlement_date: date) -> str:
        """Return canonical URL for a settlement-date file.

        Format: ``BASE_URL + 'shrt{YYYYMMDD}.csv'``.
        """
        return f"{self.BASE_URL}shrt{settlement_date.strftime('%Y%m%d')}.csv"

    def fetch_settlement_file(self, settlement_date: date) -> bytes:
        """GET the settlement file bytes.

        Returns the raw response content as ``bytes`` (UTF-8 / ASCII
        pipe-delim text). Callers decode at the parse layer.

        Raises
        ------
        FinraNotFound:
            On 404 — file not yet published. The ScheduledJob treats
            this as a benign skip (next-fire revisits).
        httpx.HTTPStatusError:
            On 5xx after retries exhausted.
        httpx.TimeoutException / httpx.ConnectError:
            On network failure after retries exhausted.
        """
        url = self.settlement_file_url(settlement_date)
        response = self._http.get(url)
        # 404 = not yet published; 403 = also not-yet-published on the
        # FINRA CDN (empirical, confirmed at #916 RegSHO live smoke).
        # Both map to the same operator semantic ("next-fire revisits").
        if response.status_code in (403, 404):
            raise FinraNotFound(f"FINRA settlement file not found: {url}")
        response.raise_for_status()
        return response.content
