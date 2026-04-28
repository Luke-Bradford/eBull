"""Regression tests for #29 — retry/backoff on ClaudeSentimentScorer.score().

Anthropic raises ``RateLimitError`` (429) and ``APIStatusError`` (5xx)
for transient faults. The scorer must retry those a small number of
times with exponential backoff, but propagate non-retryable 4xx
errors and exhaust-then-raise on persistent transient errors so the
caller's outer ``except Exception`` does not silently drop a whole
instrument's articles.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import anthropic
import httpx
import pytest

from app.services.sentiment import ClaudeSentimentScorer, SentimentResult


def _mk_message(label: str = "positive", magnitude: float = 0.7) -> MagicMock:
    block = MagicMock()
    block.text = f'{{"label": "{label}", "magnitude": {magnitude}}}'
    msg = MagicMock()
    msg.content = [block]
    return msg


def _mk_rate_limit() -> anthropic.RateLimitError:
    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    response = httpx.Response(429, request=request)
    return anthropic.RateLimitError("rate limited", response=response, body=None)


def _mk_api_status(status: int) -> anthropic.APIStatusError:
    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    response = httpx.Response(status, request=request)
    return anthropic.APIStatusError(f"status {status}", response=response, body=None)


@pytest.fixture(autouse=True)
def _no_sleep() -> object:
    """Patch time.sleep so retries don't actually wait."""
    with patch("app.services.sentiment.time.sleep") as p:
        yield p


class TestClaudeSentimentScorerRetry:
    def test_first_attempt_succeeds_no_retry(self) -> None:
        scorer = ClaudeSentimentScorer.__new__(ClaudeSentimentScorer)
        scorer._client = MagicMock()
        scorer._client.messages.create.return_value = _mk_message()

        result = scorer.score("good earnings", None)

        assert result == SentimentResult(label="positive", magnitude=0.7)
        assert scorer._client.messages.create.call_count == 1

    def test_one_429_then_success(self, _no_sleep: MagicMock) -> None:
        scorer = ClaudeSentimentScorer.__new__(ClaudeSentimentScorer)
        scorer._client = MagicMock()
        scorer._client.messages.create.side_effect = [_mk_rate_limit(), _mk_message()]

        result = scorer.score("good earnings", None)

        assert result.label == "positive"
        assert scorer._client.messages.create.call_count == 2
        # Backoff was applied once (1s before retry).
        assert _no_sleep.call_count == 1

    def test_three_429s_raises(self, _no_sleep: MagicMock) -> None:
        """All three attempts hit 429 → re-raise the last RateLimitError."""
        scorer = ClaudeSentimentScorer.__new__(ClaudeSentimentScorer)
        scorer._client = MagicMock()
        scorer._client.messages.create.side_effect = [
            _mk_rate_limit(),
            _mk_rate_limit(),
            _mk_rate_limit(),
        ]

        with pytest.raises(anthropic.RateLimitError):
            scorer.score("good earnings", None)

        assert scorer._client.messages.create.call_count == 3
        # Two backoff sleeps (between attempts 1→2 and 2→3).
        assert _no_sleep.call_count == 2

    def test_5xx_retried(self, _no_sleep: MagicMock) -> None:
        """5xx (incl. 529 overloaded) is treated as transient."""
        scorer = ClaudeSentimentScorer.__new__(ClaudeSentimentScorer)
        scorer._client = MagicMock()
        scorer._client.messages.create.side_effect = [_mk_api_status(529), _mk_message()]

        result = scorer.score("good earnings", None)

        assert result.label == "positive"
        assert scorer._client.messages.create.call_count == 2

    def test_4xx_not_retried(self, _no_sleep: MagicMock) -> None:
        """A non-429 4xx (e.g. 400 bad_request) is the caller's bug —
        retrying does not help. Raise immediately.
        """
        scorer = ClaudeSentimentScorer.__new__(ClaudeSentimentScorer)
        scorer._client = MagicMock()
        scorer._client.messages.create.side_effect = _mk_api_status(400)

        with pytest.raises(anthropic.APIStatusError):
            scorer.score("good earnings", None)

        assert scorer._client.messages.create.call_count == 1
        assert _no_sleep.call_count == 0

    def test_exponential_backoff_doubles(self, _no_sleep: MagicMock) -> None:
        scorer = ClaudeSentimentScorer.__new__(ClaudeSentimentScorer)
        scorer._client = MagicMock()
        scorer._client.messages.create.side_effect = [
            _mk_rate_limit(),
            _mk_rate_limit(),
            _mk_rate_limit(),
        ]
        with pytest.raises(anthropic.RateLimitError):
            scorer.score("good earnings", None)

        sleeps = [c.args[0] for c in _no_sleep.call_args_list]
        # 1.0s then 2.0s — exponential doubling.
        assert sleeps == [1.0, 2.0]

    def test_non_anthropic_exception_propagates_immediately(
        self,
        _no_sleep: MagicMock,
    ) -> None:
        """A bug-shaped exception (e.g. ValueError) must NOT be caught
        by the retry block — the caller's outer handler should see it.
        """
        scorer = ClaudeSentimentScorer.__new__(ClaudeSentimentScorer)
        scorer._client = MagicMock()
        scorer._client.messages.create.side_effect = ValueError("programmer error")

        with pytest.raises(ValueError):
            scorer.score("good earnings", None)

        assert scorer._client.messages.create.call_count == 1
        assert _no_sleep.call_count == 0
