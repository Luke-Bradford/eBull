"""
News provider interface.

No external news provider is wired in v1 — the news and sentiment service
will pull from eToro's feed and/or public RSS sources. This interface is
defined now so a dedicated provider (e.g. Benzinga, NewsAPI) can be dropped
in later without touching domain logic.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Literal


@dataclass(frozen=True)
class NewsItem:
    """A single news article or market event."""

    provider_id: str  # provider-native article ID or URL hash
    symbol: str | None  # None for macro/sector items not tied to one stock
    published_at: datetime
    source: str  # publication name
    headline: str
    snippet: str | None  # first paragraph or summary, if available
    url: str
    raw_payload: str | None  # serialised original response, for audit
    raw_payload_format: Literal["json", "xml", "rss"] | None  # format of raw_payload


class NewsProvider(ABC):
    """
    Interface for news and market event ingestion.

    v1: no dedicated provider — populated from eToro feed / public RSS.
    Stub is defined here so the interface contract is established.
    """

    @abstractmethod
    def get_news(
        self,
        symbol: str,
        from_dt: datetime,
        to_dt: datetime,
    ) -> list[NewsItem]:
        """
        Return news items for a symbol published within the datetime range,
        oldest first.
        """
