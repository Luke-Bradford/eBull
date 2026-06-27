"""Lexicon fallback scorer + scorer factory (#1750). Pure-logic, no network."""

from __future__ import annotations

from app.config import settings
from app.services.sentiment import (
    ClaudeSentimentScorer,
    LexiconSentimentScorer,
    make_sentiment_scorer,
)


def test_positive_headline() -> None:
    r = LexiconSentimentScorer().score("Apple beats earnings, shares surge", None)
    assert r.label == "positive"
    assert r.signed_score > 0


def test_negative_headline() -> None:
    r = LexiconSentimentScorer().score("Company misses guidance, stock plunges on layoffs", None)
    assert r.label == "negative"
    assert r.signed_score < 0


def test_neutral_headline() -> None:
    r = LexiconSentimentScorer().score("Company files routine 10-Q with the SEC", None)
    assert r.label == "neutral"
    assert r.signed_score == 0.0


def test_magnitude_saturates_at_one() -> None:
    r = LexiconSentimentScorer().score("beat surge rally gain record profit boost wins", None)
    assert 0.0 < r.magnitude <= 1.0


def test_snippet_contributes() -> None:
    r = LexiconSentimentScorer().score("Apple update", "Earnings beat with record profit and raised dividend")
    assert r.label == "positive"


def test_factory_uses_lexicon_without_key(monkeypatch) -> None:
    monkeypatch.setattr(settings, "anthropic_api_key", None, raising=False)
    assert isinstance(make_sentiment_scorer(), LexiconSentimentScorer)


def test_factory_uses_claude_with_key(monkeypatch) -> None:
    monkeypatch.setattr(settings, "anthropic_api_key", "sk-test", raising=False)
    # Avoid constructing a real Anthropic client in the factory path.
    monkeypatch.setattr(ClaudeSentimentScorer, "__init__", lambda self, api_key: None)
    assert isinstance(make_sentiment_scorer(), ClaudeSentimentScorer)
