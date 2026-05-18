"""FINRA RegSHO Daily Short Volume CDN provider (#916 — Phase 6 PR 12).

Spec: docs/superpowers/specs/2026-05-18-finra-regsho-daily.md.
Spike: docs/superpowers/spikes/2026-05-18-finra-regsho-daily-feasibility.md.

Sibling to ``finra_short_interest`` (#915, G6 bimonthly). Same host
``cdn.finra.org``; reuses the module-global throttle clock + lock from
the bimonthly module so bimonthly + daily ingest never combine to
exceed the 1 req/s polite floor. Preserves the prevention-log #726
"multiple ResilientClient instances sharing a rate limit must share
throttle state" rule by import — both modules reach the same
``_FINRA_RATE_LIMIT_CLOCK`` / ``_FINRA_RATE_LIMIT_LOCK`` objects.

Endpoint shape (empirically verified 2026-05-18 in spike §3):

  URL: https://cdn.finra.org/equity/regsho/daily/{PREFIX}shvol{YYYYMMDD}.txt
  Format: pipe-delimited TEXT, CRLF line terminators.
  Auth: anonymous CDN.
  Cadence: daily EOD ~6 PM ET on trading days.
  Prefixes: CNMS (aggregate), FNQC, FNRA (often empty — legacy ADF),
            FNSQ, FNYX, FORF (one per FINRA reporting facility).

Provider surface is intentionally minimal: URL builder + GET with
``FinraNotFound`` on 404 + ``raise_for_status()`` on 5xx. Symbol
normalisation, parsing, and ingest all live in the service layer
(``app/services/finra_regsho_ingest.py``) per the provider-thin
discipline.
"""

from __future__ import annotations

from datetime import date
from typing import Final

import httpx

from app.providers.implementations.finra_short_interest import (
    _FINRA_MIN_INTERVAL_S,
    _FINRA_RATE_LIMIT_CLOCK,
    _FINRA_RATE_LIMIT_LOCK,
    FinraNotFound,
)
from app.providers.resilient_client import ResilientClient

# Six FINRA RegSHO daily reporting facilities. Tuple is fixed-length —
# the ScheduledJob iterates over this verbatim and tests pin the
# membership.
PREFIXES: Final[tuple[str, ...]] = (
    "CNMS",  # Consolidated NMS — aggregate across facilities; Market
    # value is comma-joined union (e.g. 'B,Q,N').
    "FNQC",  # FINRA/NASDAQ TRF Chicago.
    "FNRA",  # ADF — legacy alt display facility; often empty body.
    "FNSQ",  # FINRA/NASDAQ TRF Carteret.
    "FNYX",  # FINRA/NYSE TRF.
    "FORF",  # ORF — OTC reporting facility.
)


class FinraRegShoProvider:
    """Anonymous CDN client for FINRA RegSHO daily short volume.

    Parameters
    ----------
    http_client:
        Optional ``ResilientClient`` injection — primarily for tests
        with ``httpx.MockTransport``. Default construction wires a
        fresh ``httpx.Client`` with the shared throttle list/lock so
        sibling providers in the same process (bimonthly + daily) never
        combine to exceed the 1 req/s ceiling.
    """

    BASE_URL: Final[str] = "https://cdn.finra.org/equity/regsho/daily/"

    def __init__(self, http_client: ResilientClient | None = None) -> None:
        if http_client is None:
            inner = httpx.Client(
                timeout=httpx.Timeout(30.0),
                headers={
                    "User-Agent": "eBull/0.1 (luke.bradford@hotmail.co.uk)",
                    "Accept": "text/plain,*/*",
                },
            )
            http_client = ResilientClient(
                inner,
                min_request_interval_s=_FINRA_MIN_INTERVAL_S,
                shared_last_request=_FINRA_RATE_LIMIT_CLOCK,
                shared_throttle_lock=_FINRA_RATE_LIMIT_LOCK,
            )
        self._http = http_client

    def regsho_daily_url(self, trade_date: date, prefix: str) -> str:
        """Return canonical URL for a (trade_date, prefix) pair.

        Format: ``BASE_URL + '{PREFIX}shvol{YYYYMMDD}.txt'``.
        Raises ``ValueError`` if ``prefix`` is not one of the
        documented ``PREFIXES``.
        """
        if prefix not in PREFIXES:
            raise ValueError(f"unknown FINRA RegSHO prefix: {prefix!r} (allowed: {PREFIXES})")
        return f"{self.BASE_URL}{prefix}shvol{trade_date.strftime('%Y%m%d')}.txt"

    def fetch_regsho_daily_file(self, trade_date: date, prefix: str) -> bytes:
        """GET the daily file bytes for ``(trade_date, prefix)``.

        Returns the raw response content as ``bytes``. Callers decode
        at the parse layer.

        Raises
        ------
        FinraNotFound:
            On 404 OR 403 — file not yet published OR a non-trading-day
            (FINRA does not publish on US federal holidays). Empirically
            verified 2026-05-18 live-smoke: FINRA's RegSHO CDN returns
            **403 Forbidden** (not 404) for not-yet-published trade
            dates BEFORE the EOD ~6 PM ET publication window. Both
            statuses mean "no file at this URL" in the FINRA RegSHO
            taxonomy — the ScheduledJob treats both as benign skips so
            running the cron earlier in the trading day doesn't
            generate spurious failures.
        httpx.HTTPStatusError:
            On 5xx after retries exhausted. On 4xx other than 403/404
            (which would indicate a true rate-limit/auth defect, not a
            missing-file condition).
        httpx.TimeoutException / httpx.ConnectError:
            On network failure after retries exhausted.
        """
        url = self.regsho_daily_url(trade_date, prefix)
        response = self._http.get(url)
        if response.status_code in (403, 404):
            raise FinraNotFound(f"FINRA RegSHO daily file not found: {url}")
        response.raise_for_status()
        return response.content
