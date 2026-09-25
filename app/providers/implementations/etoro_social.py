"""Read-only eToro social endpoints for the #3381 investor-cohort recorder.

Two endpoints, two documented quotas (``etoro_quota_lanes``):

* ``GET /api/v2/portfolios/rankings`` — lane G, the default shared pool.
* ``GET /api/v1/user-info/people/{username}/portfolio/live`` — lane H, dedicated 60/min.

Each lane gets its own ``ResilientClient`` so neither spends the other's spacing. Both are
informational reads: nothing here mutates broker state, and the recorder's raw-body
persistence (``sql/425``) is the audit trail, so the provider writes nothing itself.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Final
from urllib.parse import quote
from uuid import uuid4

import httpx

from app.config import settings
from app.providers.implementations.etoro_request_log import attempt_observer
from app.providers.resilient_client import ResilientClient

#: Spacing for both clients. Each lane documents 60 per rolling 60 s, whose rolling-window
#: floor is ``min_interval_for_stamps(60, 60)`` ≈ 1.017 s (checked by
#: ``tests/test_etoro_quota_lanes.py``).
_ETORO_SOCIAL_INTERVAL_S: Final = 1.1

RANKINGS_PATH: Final = "/api/v2/portfolios/rankings"
LIVE_PORTFOLIO_PATH: Final = "/api/v1/user-info/people/{username}/portfolio/live"


@dataclass(frozen=True)
class SocialResponse:
    """One response as served: status, decoded body (JSON, else the text) and its fetch time."""

    status: int
    body: Any
    observed_at: datetime


def _decode(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return response.text


class EtoroSocialProvider:
    """Thin reader for rankings and public live portfolios. Use as a context manager."""

    def __init__(self, api_key: str, user_key: str, env: str = "demo") -> None:
        self._client = httpx.Client(
            base_url=settings.etoro_base_url,
            headers={"x-api-key": api_key, "x-user-key": user_key, "Content-Type": "application/json"},
            timeout=30.0,
        )
        self._http_rankings = ResilientClient(
            self._client,
            min_request_interval_s=_ETORO_SOCIAL_INTERVAL_S,
            shared_last_request=[0.0],
            shared_throttle_lock=threading.Lock(),
            on_attempt=attempt_observer("social", env),
        )
        self._http_live = ResilientClient(
            self._client,
            min_request_interval_s=_ETORO_SOCIAL_INTERVAL_S,
            shared_last_request=[0.0],
            shared_throttle_lock=threading.Lock(),
            on_attempt=attempt_observer("social", env),
        )

    def __enter__(self) -> EtoroSocialProvider:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._client.close()

    def get_rankings_page(self, params: dict[str, str | int | bool], page: int) -> SocialResponse:
        """One page of investor rankings. Raises on any non-2xx (after the client's retries)."""
        response = self._http_rankings.get(
            RANKINGS_PATH, params={**params, "page": page}, headers={"x-request-id": str(uuid4())}
        )
        response.raise_for_status()
        return SocialResponse(response.status_code, _decode(response), datetime.now(UTC))

    def get_live_portfolio(self, username: str) -> SocialResponse:
        """A public investor's live portfolio, AS SERVED — a non-2xx status is returned, not raised.

        Retryable statuses (429, 5xx) are retried by the client and raise once exhausted; the
        recorder decides what every other status means.
        """
        path = LIVE_PORTFOLIO_PATH.format(username=quote(username, safe=""))
        response = self._http_live.get(path, headers={"x-request-id": str(uuid4())})
        return SocialResponse(response.status_code, _decode(response), datetime.now(UTC))


__all__ = ["LIVE_PORTFOLIO_PATH", "RANKINGS_PATH", "EtoroSocialProvider", "SocialResponse"]
