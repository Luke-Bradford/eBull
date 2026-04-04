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
    _importance_score,
    _normalise_headline,
    _url_hash,
    refresh_news,
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
    """Always returns a fixed result. No Claude calls."""

    def __init__(self, label: Literal["positive", "negative", "neutral"] = "positive", magnitude: float = 0.5) -> None:
        self._result = SentimentResult(label=label, magnitude=magnitude)

    def score(self, headline: str, snippet: str | None) -> SentimentResult:
        return self._result


def _mock_conn(known_hashes: list[str] | None = None, recent_headlines: list[str] | None = None) -> MagicMock:
    """
    Build a mock psycopg connection with explicit return values per query type.

    Uses call order rather than query text inspection: the service calls
    execute() in a fixed order per instrument:
      call 0: load hashes (SELECT url_hash)
      call 1: load recent headlines (SELECT headline)
      call 2+: upsert INSERT statements — return value unused

    Note: when the provider returns no candidates, _process_instrument returns
    early and neither DB call is made — the call-order assumption is simply not
    exercised in that path.

    Passing a list to side_effect would exhaust on the upsert calls, so we use
    a closure that returns the right result by position and falls back to a
    plain MagicMock for any additional calls.
    """
    conn = MagicMock()

    hash_result = MagicMock()
    hash_result.fetchall.return_value = [(h,) for h in (known_hashes or [])]

    headline_result = MagicMock()
    headline_result.fetchall.return_value = [(h,) for h in (recent_headlines or [])]

    _call_count = [0]

    def _execute(*args: object, **kwargs: object) -> MagicMock:
        idx = _call_count[0]
        _call_count[0] += 1
        if idx == 0:
            return hash_result
        if idx == 1:
            return headline_result
        return MagicMock()

    conn.execute.side_effect = _execute
    conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
    conn.transaction.return_value.__exit__ = MagicMock(return_value=False)
    return conn


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


def _conn_no_db_data() -> MagicMock:
    """Mock conn returning empty sets for both DB queries."""
    conn = MagicMock()
    result = MagicMock()
    result.fetchall.return_value = []
    conn.execute.return_value = result
    return conn


def test_exact_headline_is_filtered() -> None:
    headline = "Apple beats Q1 earnings expectations"
    items = [
        (_make_item(headline, url="https://example.com/1"), "hash1"),
        (_make_item(headline, url="https://example.com/2"), "hash2"),
    ]
    deduped, skipped = _filter_near_duplicates(items, _conn_no_db_data(), "42")
    assert len(deduped) == 1
    assert skipped == 1


def test_near_duplicate_above_threshold_is_filtered() -> None:
    h1 = "Apple Inc reports record Q1 earnings beating expectations"
    h2 = "Apple Inc reports record Q1 earnings, beating expectations"
    items = [
        (_make_item(h1, url="https://example.com/1"), "hash1"),
        (_make_item(h2, url="https://example.com/2"), "hash2"),
    ]
    deduped, skipped = _filter_near_duplicates(items, _conn_no_db_data(), "42")
    assert len(deduped) == 1
    assert skipped == 1


def test_distinct_headlines_are_both_kept() -> None:
    h1 = "Apple beats earnings expectations"
    h2 = "Tesla misses revenue forecast for Q4"
    items = [
        (_make_item(h1, url="https://example.com/1"), "hash1"),
        (_make_item(h2, url="https://example.com/2"), "hash2"),
    ]
    deduped, skipped = _filter_near_duplicates(items, _conn_no_db_data(), "42")
    assert len(deduped) == 2
    assert skipped == 0


def test_near_duplicate_against_db_headline_is_filtered() -> None:
    """Article that is a near-dup of a headline already in the DB is skipped."""
    db_headline = "Apple reports strong Q1 earnings ahead of expectations"
    candidate_headline = "Apple reports strong Q1 earnings, ahead of expectations"

    conn = MagicMock()
    result = MagicMock()
    result.fetchall.return_value = [(db_headline,)]
    conn.execute.return_value = result

    items = [(_make_item(candidate_headline, url="https://example.com/new"), "newhash")]
    deduped, skipped = _filter_near_duplicates(items, conn, "42")
    assert len(deduped) == 0
    assert skipped == 1


# ---------------------------------------------------------------------------
# Importance scoring
# ---------------------------------------------------------------------------


def test_earnings_scores_higher_than_general() -> None:
    earnings_item = _make_item("Apple Q1 earnings beat", category="earnings")
    general_item = _make_item("Apple joins sustainability index", category="general")
    assert _importance_score(earnings_item, _BASE_DT) > _importance_score(general_item, _BASE_DT)


def test_reuters_scores_higher_than_unknown_source() -> None:
    reuters_item = _make_item("Apple earnings beat", source="Reuters")
    unknown_item = _make_item("Apple earnings beat", source="Some Blog")
    assert _importance_score(reuters_item, _BASE_DT) > _importance_score(unknown_item, _BASE_DT)


def test_fresh_article_scores_higher_than_stale() -> None:
    stale_dt = datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC)  # 3 days old
    fresh_item = _make_item("Apple earnings beat", published_at=_BASE_DT)
    stale_item = _make_item("Apple earnings beat", published_at=stale_dt)
    assert _importance_score(fresh_item, _BASE_DT) > _importance_score(stale_item, _BASE_DT)


def test_importance_score_clamped_to_unit_interval() -> None:
    item = _make_item("Apple Q1 earnings record beat", source="Bloomberg", category="earnings")
    score = _importance_score(item, _BASE_DT)
    assert 0.0 <= score <= 1.0


def test_fully_stale_article_recency_is_zero() -> None:
    """Article older than 72h gets recency_w=0, reducing overall score vs fresh."""
    very_old = datetime(2026, 3, 28, 12, 0, 0, tzinfo=UTC)  # 7 days old
    old_item = _make_item("Apple earnings beat", source="Reuters", category="earnings", published_at=very_old)
    fresh_item = _make_item("Apple earnings beat", source="Reuters", category="earnings", published_at=_BASE_DT)
    assert _importance_score(fresh_item, _BASE_DT) > _importance_score(old_item, _BASE_DT)


# ---------------------------------------------------------------------------
# SentimentResult.signed_score
# ---------------------------------------------------------------------------


def test_signed_score_positive() -> None:
    assert SentimentResult(label="positive", magnitude=0.75).signed_score == pytest.approx(0.75)


def test_signed_score_negative() -> None:
    assert SentimentResult(label="negative", magnitude=0.40).signed_score == pytest.approx(-0.40)


def test_signed_score_neutral() -> None:
    assert SentimentResult(label="neutral", magnitude=0.20).signed_score == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# refresh_news integration (no DB, mocked conn)
# ---------------------------------------------------------------------------


def test_refresh_news_inserts_new_articles() -> None:
    items = [
        _make_item("Apple beats earnings", url="https://example.com/1"),
        _make_item("Tesla misses revenue", url="https://example.com/2"),
    ]
    conn = _mock_conn()

    summary = refresh_news(
        provider=FakeNewsProvider(items),
        scorer=FakeSentimentScorer(),
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
    known = _url_hash(url)
    # known_hashes pre-populated; no recent_headlines needed (exact dup filtered before near-dup check)
    conn = _mock_conn(known_hashes=[known])

    summary = refresh_news(
        provider=FakeNewsProvider([_make_item("Apple beats earnings", url=url)]),
        scorer=FakeSentimentScorer(),
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
        conn=_mock_conn(),
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
        conn=_mock_conn(),
        instrument_symbols=[("AAPL", "1")],
        from_dt=_BASE_DT,
        to_dt=_BASE_DT,
    )

    assert summary.articles_fetched == 0
    assert summary.articles_upserted == 0


def test_scorer_not_called_for_exact_duplicates() -> None:
    """Scorer must never be called for articles already in DB (wasted API calls)."""
    url = "https://example.com/existing"
    conn = _mock_conn(known_hashes=[_url_hash(url)])

    scorer = MagicMock(spec=SentimentScorer)
    refresh_news(
        provider=FakeNewsProvider([_make_item("Some headline", url=url)]),
        scorer=scorer,
        conn=conn,
        instrument_symbols=[("AAPL", "1")],
        from_dt=_BASE_DT,
        to_dt=_BASE_DT,
    )

    scorer.score.assert_not_called()
