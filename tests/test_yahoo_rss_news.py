"""Pure-logic tests for the Yahoo RSS news provider (#1750). No network."""

from __future__ import annotations

from datetime import UTC, datetime

from app.providers.implementations.yahoo_rss_news import YahooRssNewsProvider

# A representative feed: one fully-valid item, plus one of each skip case.
# Dates span 24-26 Jun 2026 so a 25-26 window excludes the oldest.
_FEED = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <title>Yahoo! Finance: AAPL News</title>
  <item>
    <title>Apple beats earnings, revenue tops estimates</title>
    <description>Strong quarter with record profit &amp; raised guidance</description>
    <link>https://finance.yahoo.com/news/apple-beats-1</link>
    <pubDate>Fri, 26 Jun 2026 13:00:00 +0000</pubDate>
    <guid isPermaLink="false">guid-apple-1</guid>
  </item>
  <item>
    <title>Analyst upgrades Apple, raises price target</title>
    <description>Bullish note</description>
    <link>https://finance.yahoo.com/news/apple-upgrade-2</link>
    <pubDate>Thu, 25 Jun 2026 09:00:00 +0000</pubDate>
    <guid isPermaLink="false">guid-apple-2</guid>
  </item>
  <item>
    <title>Old news outside the window</title>
    <description>too old</description>
    <link>https://finance.yahoo.com/news/old-3</link>
    <pubDate>Wed, 24 Jun 2026 09:00:00 +0000</pubDate>
    <guid isPermaLink="false">guid-old-3</guid>
  </item>
  <item>
    <title></title>
    <description>blank title — skip</description>
    <link>https://finance.yahoo.com/news/blank-title</link>
    <pubDate>Fri, 26 Jun 2026 10:00:00 +0000</pubDate>
  </item>
  <item>
    <title>Missing link — skip</title>
    <description>no link element</description>
    <pubDate>Fri, 26 Jun 2026 10:00:00 +0000</pubDate>
  </item>
  <item>
    <title>Relative link — skip</title>
    <description>relative</description>
    <link>/news/relative-4</link>
    <pubDate>Fri, 26 Jun 2026 10:00:00 +0000</pubDate>
  </item>
  <item>
    <title>Bad pubDate — skip</title>
    <description>unparsable date</description>
    <link>https://finance.yahoo.com/news/baddate-5</link>
    <pubDate>not a date</pubDate>
  </item>
  <item>
    <title>No guid — provider_id falls back to link</title>
    <description>general market move</description>
    <link>https://finance.yahoo.com/news/noguid-6</link>
    <pubDate>Fri, 26 Jun 2026 11:00:00 +0000</pubDate>
  </item>
</channel></rss>
"""


def _provider(feed: str = _FEED) -> YahooRssNewsProvider:
    return YahooRssNewsProvider(http_get=lambda _url: feed)


def _window() -> tuple[datetime, datetime]:
    # Naive bounds on purpose — must not raise against tz-aware item dates.
    return datetime(2026, 6, 25, 0, 0, 0), datetime(2026, 6, 26, 23, 59, 59)


def test_valid_items_parsed_and_filtered() -> None:
    lo, hi = _window()
    items = _provider().get_news("AAPL", lo, hi)
    # Of 8 items: 3 valid in-window (earnings, analyst, no-guid), the 24 Jun
    # one is out of window, and 4 are malformed → 3 returned.
    urls = [i.url for i in items]
    assert urls == [
        "https://finance.yahoo.com/news/apple-upgrade-2",  # 25 Jun (oldest first)
        "https://finance.yahoo.com/news/noguid-6",  # 26 Jun 11:00
        "https://finance.yahoo.com/news/apple-beats-1",  # 26 Jun 13:00
    ]


def test_oldest_first_ordering() -> None:
    lo, hi = _window()
    items = _provider().get_news("AAPL", lo, hi)
    times = [i.published_at for i in items]
    assert times == sorted(times)
    assert all(t.tzinfo is not None for t in times)


def test_field_mapping_and_html_unescape() -> None:
    lo, hi = _window()
    items = _provider().get_news("AAPL", lo, hi)
    beat = next(i for i in items if i.url.endswith("apple-beats-1"))
    assert beat.headline == "Apple beats earnings, revenue tops estimates"
    assert beat.snippet == "Strong quarter with record profit & raised guidance"  # &amp; decoded
    assert beat.source == "Yahoo Finance"
    assert beat.provider_id == "guid-apple-1"
    assert beat.symbol == "AAPL"
    assert beat.raw_payload_format == "rss"
    assert beat.raw_payload is not None


def test_guid_fallback_to_link() -> None:
    lo, hi = _window()
    items = _provider().get_news("AAPL", lo, hi)
    noguid = next(i for i in items if i.url.endswith("noguid-6"))
    assert noguid.provider_id == noguid.url


def test_category_classification() -> None:
    lo, hi = _window()
    items = _provider().get_news("AAPL", lo, hi)
    by_url = {i.url.split("/")[-1]: i.category for i in items}
    assert by_url["apple-beats-1"] == "earnings"
    assert by_url["apple-upgrade-2"] == "analyst_note"
    assert by_url["noguid-6"] == "general"


def test_malformed_items_skipped_not_fatal() -> None:
    lo, hi = _window()
    items = _provider().get_news("AAPL", lo, hi)
    # None of the malformed items leak through.
    bad = {"blank-title", "relative-4", "baddate-5"}
    assert not any(i.url.split("/")[-1] in bad for i in items)


def test_unparseable_feed_returns_empty() -> None:
    items = _provider("<<<not xml>>>").get_news("AAPL", *_window())
    assert items == []


def test_oversized_feed_refused() -> None:
    huge = "<rss><channel>" + ("x" * (5 * 1024 * 1024 + 1)) + "</channel></rss>"
    assert _provider(huge).get_news("AAPL", *_window()) == []


def test_non_utc_offset_converted_to_utc() -> None:
    feed = """<?xml version="1.0"?><rss version="2.0"><channel>
      <item>
        <title>Offset item</title>
        <description>x</description>
        <link>https://finance.yahoo.com/news/offset-1</link>
        <pubDate>Fri, 26 Jun 2026 09:00:00 -0400</pubDate>
        <guid>g-offset</guid>
      </item>
    </channel></rss>"""
    items = _provider(feed).get_news("AAPL", datetime(2026, 6, 26), datetime(2026, 6, 26, 23, 59, 59))
    assert len(items) == 1
    pub = items[0].published_at
    offset = pub.utcoffset()
    assert offset is not None and offset.total_seconds() == 0  # converted to UTC
    assert (pub.hour, pub.minute) == (13, 0)  # 09:00 -0400 == 13:00 UTC


def test_aware_window_also_works() -> None:
    lo = datetime(2026, 6, 25, tzinfo=UTC)
    hi = datetime(2026, 6, 26, 23, 59, 59, tzinfo=UTC)
    items = _provider().get_news("AAPL", lo, hi)
    assert len(items) == 3
