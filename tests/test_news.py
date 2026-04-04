"""
Unit tests for the news and sentiment service.

No network calls, no database, no live Claude API.
All external dependencies are stubbed or mocked.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal
from unittest.mock import MagicMock

import pytest

from app.providers.news import NewsCategory, NewsItem, NewsProvider
from app.services.news import (
    NewsRefreshSummary,
    _filter_near_duplicates,
    _normalise_headline,
    _url_hash,
    refresh_news,
    score_importance,
)
from app.services.sentiment import SentimentResult, SentimentScorer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_DT = datetime(2026, 4, 4, 12, 0, 0, tzinfo=UTC)


def _make_item(
    headline: str,
    url: str = "https://example.com/article",
    source: str = "Reuters",
    category: NewsCategory = "general",
    published_at: datetime | None = None,
    snippet: str | None = None,
) -> NewsItem:
    return NewsItem(
        provider_id=_url_hash(url),
        symbol="AAPL",
        published_at=published_at or _BASE_DT,
        source=source,
        headline=headline,
        snippet=snippet,
        url=url,
        category=category,
        raw_payload=None,
        raw_payload_format=None,
    )


# ---------------------------------------------------------------------------
# Fake implementations
# ---------------------------------------------------------------------------


class FakeNewsProvider(NewsProvider):
    def __init__(self, items: list[NewsItem]) -> None:
        self._items = items

    def get_news(self, symbol: str, from_dt: datetime, to_dt: datetime) -> list[NewsItem]:
        return self._items


class FakeSentimentScorer(SentimentScorer):
    """Always returns a fixed positive result. No Claude calls."""

    def __init__(self, label: Literal["positive", "negative", "neutral"] = "positive", magnitude: float = 0.5) -> None:
        self._result = SentimentResult(label=label, magnitude=magnitude)

    def score(self, headline: str, snippet: str | None) -> SentimentResult:
        return self._result


# ---------------------------------------------------------------------------
# url_hash
# ---------------------------------------------------------------------------


def test_url_hash_is_deterministic() -> None:
    url = "https://example.com/story/123"
    assert _url_hash(url) == _url_hash(url)


def test_url_hash_differs_for_different_urls() -> None:
    assert _url_hash("https://example.com/a") != _url_hash("https://example.com/b")


def test_url_hash_is_64_chars() -> None:
    # SHA-256 hex = 64 characters
    assert len(_url_hash("https://example.com/")) == 64


# ---------------------------------------------------------------------------
# Headline normalisation
# ---------------------------------------------------------------------------


def test_normalise_headline_lowercases() -> None:
    assert _normalise_headline("APPLE BEATS EARNINGS") == "apple beats earnings"


def test_normalise_headline_strips_punctuation() -> None:
    result = _normalise_headline("Apple Inc. beats Q1 earnings: +12%")
    assert "." not in result
    assert ":" not in result
    assert "+" not in result


def test_normalise_headline_collapses_whitespace() -> None:
    result = _normalise_headline("Apple   beats   Q1")
    assert "  " not in result


# ---------------------------------------------------------------------------
# Near-duplicate filtering
# ---------------------------------------------------------------------------


def test_exact_headline_is_filtered() -> None:
    headline = "Apple beats Q1 earnings expectations"
    items = [
        (_make_item(headline, url="https://example.com/1"), "hash1"),
        (_make_item(headline, url="https://example.com/2"), "hash2"),
    ]
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = []

    deduped, skipped = _filter_near_duplicates(items, mock_conn, "42")

    assert len(deduped) == 1
    assert skipped == 1


def test_near_duplicate_above_threshold_is_filtered() -> None:
    # Two very similar headlines that should exceed 0.90 ratio
    h1 = "Apple Inc reports record Q1 earnings beating expectations"
    h2 = "Apple Inc reports record Q1 earnings, beating expectations"
    items = [
        (_make_item(h1, url="https://example.com/1"), "hash1"),
        (_make_item(h2, url="https://example.com/2"), "hash2"),
    ]
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = []

    deduped, skipped = _filter_near_duplicates(items, mock_conn, "42")

    assert len(deduped) == 1
    assert skipped == 1


def test_distinct_headlines_are_both_kept() -> None:
    h1 = "Apple beats earnings expectations"
    h2 = "Tesla misses revenue forecast for Q4"
    items = [
        (_make_item(h1, url="https://example.com/1"), "hash1"),
        (_make_item(h2, url="https://example.com/2"), "hash2"),
    ]
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = []

    deduped, skipped = _filter_near_duplicates(items, mock_conn, "42")

    assert len(deduped) == 2
    assert skipped == 0


def test_near_duplicate_against_db_headline_is_filtered() -> None:
    """Article that is a near-dup of a headline already in the DB is skipped."""
    db_headline = "Apple reports strong Q1 earnings ahead of expectations"
    candidate_headline = "Apple reports strong Q1 earnings, ahead of expectations"

    items = [(_make_item(candidate_headline, url="https://example.com/new"), "newhash")]
    mock_conn = MagicMock()
    # Simulate DB returning the existing headline
    mock_conn.execute.return_value.fetchall.return_value = [(db_headline,)]

    deduped, skipped = _filter_near_duplicates(items, mock_conn, "42")

    assert len(deduped) == 0
    assert skipped == 1


# ---------------------------------------------------------------------------
# Importance scoring
# ---------------------------------------------------------------------------


def test_earnings_scores_higher_than_general() -> None:
    earnings_item = _make_item("Apple Q1 earnings beat", category="earnings")
    general_item = _make_item("Apple joins sustainability index", category="general")

    score_e = score_importance(earnings_item, _BASE_DT)
    score_g = score_importance(general_item, _BASE_DT)

    assert score_e > score_g


def test_reuters_scores_higher_than_unknown_source() -> None:
    reuters_item = _make_item("Apple earnings beat", source="Reuters")
    unknown_item = _make_item("Apple earnings beat", source="Some Blog")

    score_r = score_importance(reuters_item, _BASE_DT)
    score_u = score_importance(unknown_item, _BASE_DT)

    assert score_r > score_u


def test_fresh_article_scores_higher_than_stale() -> None:
    fresh_dt = _BASE_DT
    stale_dt = datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC)  # 3 days old

    fresh_item = _make_item("Apple earnings beat", published_at=fresh_dt)
    stale_item = _make_item("Apple earnings beat", published_at=stale_dt)

    score_f = score_importance(fresh_item, _BASE_DT)
    score_s = score_importance(stale_item, _BASE_DT)

    assert score_f > score_s


def test_importance_score_clamped_to_unit_interval() -> None:
    item = _make_item("Apple Q1 earnings record beat", source="Bloomberg", category="earnings")
    score = score_importance(item, _BASE_DT)
    assert 0.0 <= score <= 1.0


def test_fully_stale_article_recency_is_zero() -> None:
    """Article older than 72h should have recency_w=0, reducing overall score."""
    very_old = datetime(2026, 3, 28, 12, 0, 0, tzinfo=UTC)  # 7 days old
    old_item = _make_item("Apple earnings beat", source="Reuters", category="earnings", published_at=very_old)
    fresh_item = _make_item("Apple earnings beat", source="Reuters", category="earnings", published_at=_BASE_DT)

    score_old = score_importance(old_item, _BASE_DT)
    score_fresh = score_importance(fresh_item, _BASE_DT)

    assert score_fresh > score_old


# ---------------------------------------------------------------------------
# SentimentResult.signed_score
# ---------------------------------------------------------------------------


def test_signed_score_positive() -> None:
    result = SentimentResult(label="positive", magnitude=0.75)
    assert result.signed_score == pytest.approx(0.75)


def test_signed_score_negative() -> None:
    result = SentimentResult(label="negative", magnitude=0.40)
    assert result.signed_score == pytest.approx(-0.40)


def test_signed_score_neutral() -> None:
    result = SentimentResult(label="neutral", magnitude=0.20)
    assert result.signed_score == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# refresh_news integration (no DB, mocked conn)
# ---------------------------------------------------------------------------


def _mock_conn_with_no_existing_data() -> MagicMock:
    """Return a mock psycopg connection that reports no existing hashes/headlines."""
    conn = MagicMock()

    def execute_side_effect(query: str, params: object = None) -> MagicMock:
        result = MagicMock()
        result.fetchall.return_value = []
        return result

    conn.execute.side_effect = execute_side_effect
    conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
    conn.transaction.return_value.__exit__ = MagicMock(return_value=False)
    return conn


def test_refresh_news_inserts_new_articles() -> None:
    items = [
        _make_item("Apple beats earnings", url="https://example.com/1"),
        _make_item("Tesla misses revenue", url="https://example.com/2"),
    ]
    provider = FakeNewsProvider(items)
    scorer = FakeSentimentScorer()
    conn = _mock_conn_with_no_existing_data()

    summary = refresh_news(
        provider=provider,
        scorer=scorer,
        conn=conn,
        instrument_symbols=[("AAPL", "1")],
        from_dt=_BASE_DT,
        to_dt=_BASE_DT,
    )

    assert isinstance(summary, NewsRefreshSummary)
    assert summary.instruments_attempted == 1
    assert summary.articles_fetched == 2
    assert summary.exact_duplicates_skipped == 0
    assert summary.articles_upserted == 2


def test_refresh_news_skips_exact_duplicate_urls() -> None:
    url = "https://example.com/same"
    items = [_make_item("Apple beats earnings", url=url)]
    provider = FakeNewsProvider(items)
    scorer = FakeSentimentScorer()

    conn = MagicMock()

    def execute_side_effect(query: str, params: object = None) -> MagicMock:
        result = MagicMock()
        # Return the hash as already known for the url_hash query
        if "url_hash" in query and "headline" not in query:
            result.fetchall.return_value = [(_url_hash(url),)]
        else:
            result.fetchall.return_value = []
        return result

    conn.execute.side_effect = execute_side_effect
    conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
    conn.transaction.return_value.__exit__ = MagicMock(return_value=False)

    summary = refresh_news(
        provider=provider,
        scorer=scorer,
        conn=conn,
        instrument_symbols=[("AAPL", "1")],
        from_dt=_BASE_DT,
        to_dt=_BASE_DT,
    )

    assert summary.exact_duplicates_skipped == 1
    assert summary.articles_upserted == 0


def test_refresh_news_provider_error_skips_instrument() -> None:
    class BrokenProvider(NewsProvider):
        def get_news(self, symbol: str, from_dt: datetime, to_dt: datetime) -> list[NewsItem]:
            raise RuntimeError("provider down")

    summary = refresh_news(
        provider=BrokenProvider(),
        scorer=FakeSentimentScorer(),
        conn=_mock_conn_with_no_existing_data(),
        instrument_symbols=[("AAPL", "1"), ("TSLA", "2")],
        from_dt=_BASE_DT,
        to_dt=_BASE_DT,
    )

    assert summary.instruments_skipped == 2
    assert summary.articles_upserted == 0


def test_refresh_news_empty_provider_returns_zero_upserted() -> None:
    summary = refresh_news(
        provider=FakeNewsProvider([]),
        scorer=FakeSentimentScorer(),
        conn=_mock_conn_with_no_existing_data(),
        instrument_symbols=[("AAPL", "1")],
        from_dt=_BASE_DT,
        to_dt=_BASE_DT,
    )

    assert summary.articles_fetched == 0
    assert summary.articles_upserted == 0
