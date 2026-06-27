"""
Yahoo Finance RSS news provider (#1750).

Keyless, per-ticker news source. Yahoo publishes a per-symbol RSS feed at
``https://feeds.finance.yahoo.com/rss/2.0/headline?s=<SYM>&region=US&lang=en-US``
that returns ~20 recent headlines aggregated from across the financial press.

Why RSS (not a paid API): the project's free-source posture (#532) and the
``NewsProvider`` stub's own documented v1 plan ("populated from eToro feed /
public RSS"). The feed is keyless; it does require a browser-like User-Agent
(the default httpx UA is rejected) — see ``settings.news_rss_user_agent``.

Limits (documented, not gaps):
  * RSS is recent-only (~last few days). There is no deep historical bulk
    source for news; first-load is shallow by nature.
  * Yahoo's ``<item>`` has no publisher field, so ``source`` is the feed name
    ("Yahoo Finance"), not the originating publication.
  * Category is heuristic (keyword match on title/snippet) — Yahoo does not
    tag items.
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET  # noqa: S405 — only ParseError caught; size-capped untrusted parse (see get_news)
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import httpx

from app.config import settings
from app.providers.news import NewsCategory, NewsItem, NewsProvider

logger = logging.getLogger(__name__)

_FEED_URL = "https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
_HTTP_TIMEOUT_S = 15.0
# Real Yahoo feeds are ~15-125KB. Reject anything wildly larger before the XML
# parse so a malicious/MITM'd feed cannot mount a billion-laughs amplification
# (stdlib expat expands internal entities). 5MB is ~40x the largest real feed.
_MAX_FEED_BYTES = 5 * 1024 * 1024

# Category heuristics — first match wins, else "general". Word-boundary
# anchored so "rerating" does not trip "rating", etc.
_EARNINGS_RE = re.compile(r"\b(earnings|revenue|eps|quarterly results|guidance|profit warning)\b", re.IGNORECASE)
_ANALYST_RE = re.compile(
    r"\b(analyst|price target|upgrade[sd]?|downgrade[sd]?|rating|initiates? coverage|reiterates?)\b",
    re.IGNORECASE,
)

# An ``http_get`` returns the response body text for a URL. Injectable so
# tests pass a fake and never touch the network (mirrors
# ``sec_submissions.check_freshness(http_get=...)``).
HttpGet = Callable[[str], str]


def _default_http_get(url: str) -> str:
    # Stream and cap DECOMPRESSED bytes (Codex pre-push): a buffered
    # ``resp.text`` would download + gunzip the whole body before any size
    # guard, so a compression bomb could exhaust memory. ``iter_bytes()``
    # yields already-decompressed chunks; bail the moment we exceed the cap.
    with httpx.stream(
        "GET",
        url,
        timeout=_HTTP_TIMEOUT_S,
        headers={"User-Agent": settings.news_rss_user_agent, "Accept-Encoding": "gzip, deflate"},
        follow_redirects=True,
    ) as resp:
        resp.raise_for_status()
        chunks: list[bytes] = []
        total = 0
        for chunk in resp.iter_bytes():
            total += len(chunk)
            if total > _MAX_FEED_BYTES:
                raise ValueError(f"Yahoo RSS feed exceeds {_MAX_FEED_BYTES} bytes (decompressed)")
            chunks.append(chunk)
        return b"".join(chunks).decode(resp.encoding or "utf-8", "replace")


def _classify(headline: str, snippet: str | None) -> NewsCategory:
    text = f"{headline} {snippet or ''}"
    if _EARNINGS_RE.search(text):
        return "earnings"
    if _ANALYST_RE.search(text):
        return "analyst_note"
    return "general"


def _to_utc_aware(dt: datetime) -> datetime:
    """
    Normalise a datetime to UTC-aware.

    Naive → assume UTC; aware → CONVERT to UTC (Codex pre-push). Converting (not
    just tagging) matters because downstream ``_importance_score`` strips tzinfo
    — a Yahoo ``09:00 -0400`` left aware-but-not-converted would be mis-scored as
    09:00 UTC instead of 13:00 UTC, and persisted ``event_time`` would be off.
    """
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def _text(item: ET.Element, tag: str) -> str | None:
    el = item.find(tag)
    if el is None or el.text is None:
        return None
    stripped = el.text.strip()
    return stripped or None


class YahooRssNewsProvider(NewsProvider):
    """Keyless Yahoo Finance per-ticker RSS provider."""

    def __init__(self, http_get: HttpGet | None = None) -> None:
        self._http_get = http_get or _default_http_get

    def get_news(self, symbol: str, from_dt: datetime, to_dt: datetime) -> list[NewsItem]:
        url = _FEED_URL.format(symbol=quote(symbol))
        body = self._http_get(url)

        if len(body.encode("utf-8", "ignore")) > _MAX_FEED_BYTES:
            logger.warning("Yahoo RSS: feed for symbol=%s exceeds %d bytes, refusing to parse", symbol, _MAX_FEED_BYTES)
            return []

        try:
            root = ET.fromstring(body)  # noqa: S314 — size-capped above; only ParseError handled
        except ET.ParseError:
            logger.warning("Yahoo RSS: unparseable feed for symbol=%s", symbol)
            return []

        channel = root.find("channel")
        if channel is None:
            return []

        lo = _to_utc_aware(from_dt)
        hi = _to_utc_aware(to_dt)

        items: list[NewsItem] = []
        for el in channel.findall("item"):
            parsed = self._parse_item(el, symbol)
            if parsed is None:
                continue
            if lo <= parsed.published_at <= hi:
                items.append(parsed)

        # Contract: oldest first.
        items.sort(key=lambda it: it.published_at)
        return items

    def _parse_item(self, el: ET.Element, symbol: str) -> NewsItem | None:
        # Strict validity (Codex BLOCKING): a NewsItem requires non-null
        # headline/url/published_at, and refresh_news runs _url_hash(url)
        # immediately — a blank/bad field must drop the item, not the batch.
        headline = _text(el, "title")
        link = _text(el, "link")
        if not headline or not link or not link.startswith(("http://", "https://")):
            return None

        pub = _text(el, "pubDate")
        if pub is None:
            return None
        try:
            published = parsedate_to_datetime(pub)
        except TypeError, ValueError:
            return None
        if published is None:
            return None
        published = _to_utc_aware(published)

        snippet = _text(el, "description")
        # guid is the provider-native id; fall back to the link when absent.
        provider_id = _text(el, "guid") or link

        raw_payload = ET.tostring(el, encoding="unicode")

        return NewsItem(
            provider_id=provider_id,
            symbol=symbol,
            published_at=published,
            source="Yahoo Finance",
            headline=headline,
            snippet=snippet,
            url=link,
            category=_classify(headline, snippet),
            raw_payload=raw_payload,
            raw_payload_format="rss",
        )
