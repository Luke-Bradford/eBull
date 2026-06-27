"""
Sentiment scorer interface and Claude Haiku implementation.

The ABC isolates the rest of the application from Anthropic-specific types.
Tests inject a fake implementation; production uses ClaudeSentimentScorer.

Sentiment is encoded as a signed float for persistence:
  positive => +magnitude  (e.g. +0.75)
  negative => -magnitude  (e.g. -0.40)
  neutral  =>  0.0
"""

from __future__ import annotations

import json
import logging
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal

logger = logging.getLogger(__name__)

# Retry budget for transient Anthropic errors (#29). Three attempts
# total: an initial call + two retries with exponential backoff
# (1s, 2s). Anything beyond raises the SDK's exception so the caller
# can decide whether to skip the article or fail the run.
_RETRY_MAX_ATTEMPTS = 3
_RETRY_INITIAL_BACKOFF_S = 1.0

SentimentLabel = Literal["positive", "negative", "neutral"]

_SYSTEM_PROMPT = """\
You are a financial news sentiment classifier.

Given a news headline and an optional snippet, return a JSON object with:
  "label":     one of "positive", "negative", or "neutral"
  "magnitude": a float between 0.0 and 1.0 indicating strength of sentiment
               (0.0 = weak / ambiguous, 1.0 = very strong)

Rules:
- Judge sentiment from the perspective of a long-only equity investor in the company mentioned.
- Earnings beats, dividend increases, buybacks, strong guidance => positive.
- Earnings misses, guidance cuts, layoffs, regulatory fines => negative.
- Routine filings, index inclusions with no clear valuation impact => neutral.
- When in doubt, return neutral with low magnitude.

Respond with ONLY valid JSON. Example:
{"label": "positive", "magnitude": 0.72}
"""


@dataclass(frozen=True)
class SentimentResult:
    label: SentimentLabel
    magnitude: float  # 0.0–1.0

    @property
    def signed_score(self) -> float:
        """
        Signed float suitable for persistence in news_events.sentiment_score.

        positive  =>  +magnitude  (e.g. 0.75)
        negative  =>  -magnitude  (e.g. -0.40)
        neutral   =>   0.0  (magnitude is discarded — use sentiment_raw_json
                             if you need the raw magnitude for neutral articles)
        """
        if self.label == "positive":
            return self.magnitude
        if self.label == "negative":
            return -self.magnitude
        return 0.0


class SentimentScorer(ABC):
    """Abstract sentiment scorer. Implementations must be stateless and thread-safe."""

    @abstractmethod
    def score(self, headline: str, snippet: str | None) -> SentimentResult:
        """
        Score a news headline + optional snippet.

        Returns a SentimentResult. Raises on unrecoverable scorer errors.
        """


class LexiconSentimentScorer(SentimentScorer):
    """
    Keyless deterministic fallback scorer (#1750).

    A small signed finance lexicon — no external calls, no API key, no new
    dependency. Used when ``ANTHROPIC_API_KEY`` is absent (e.g. dev) so the
    news pipeline degrades gracefully instead of hard-skipping. Quality is
    lower than Haiku (bag-of-words, no negation/context handling); the signed
    score still satisfies the settled-decision contract (signed numeric, no
    label column). Haiku is the primary scorer when a key is present.
    """

    # Net hits are normalised by this divisor → magnitude saturates at 3 net
    # signed terms. Keeps a single strong word from pinning magnitude to 1.0.
    _SATURATION = 3.0

    _POSITIVE: frozenset[str] = frozenset(
        {
            "beat",
            "beats",
            "surge",
            "surged",
            "soar",
            "soars",
            "soared",
            "rally",
            "rallies",
            "jump",
            "jumps",
            "jumped",
            "gain",
            "gains",
            "rise",
            "rises",
            "rose",
            "upgrade",
            "upgraded",
            "outperform",
            "strong",
            "growth",
            "record",
            "profit",
            "profitable",
            "dividend",
            "buyback",
            "raises",
            "raised",
            "bullish",
            "win",
            "wins",
            "approval",
            "approved",
            "boost",
            "boosts",
            "expands",
            "expansion",
            "tops",
            "beating",
        }
    )
    _NEGATIVE: frozenset[str] = frozenset(
        {
            "miss",
            "misses",
            "missed",
            "plunge",
            "plunges",
            "plunged",
            "drop",
            "drops",
            "dropped",
            "fall",
            "falls",
            "fell",
            "slump",
            "slumps",
            "decline",
            "declines",
            "downgrade",
            "downgraded",
            "underperform",
            "weak",
            "loss",
            "losses",
            "cut",
            "cuts",
            "warning",
            "warns",
            "lawsuit",
            "probe",
            "investigation",
            "fine",
            "fined",
            "layoff",
            "layoffs",
            "bearish",
            "bankruptcy",
            "default",
            "recall",
            "halt",
            "halted",
            "fraud",
            "slashes",
            "slashed",
            "tumble",
            "tumbles",
            "tumbled",
        }
    )

    _TOKEN_RE = re.compile(r"[a-z]+")

    def score(self, headline: str, snippet: str | None) -> SentimentResult:
        text = f"{headline} {snippet or ''}".lower()
        tokens = self._TOKEN_RE.findall(text)
        pos = sum(1 for t in tokens if t in self._POSITIVE)
        neg = sum(1 for t in tokens if t in self._NEGATIVE)
        net = pos - neg
        if net == 0:
            return SentimentResult(label="neutral", magnitude=0.0)
        magnitude = min(1.0, abs(net) / self._SATURATION)
        label: SentimentLabel = "positive" if net > 0 else "negative"
        return SentimentResult(label=label, magnitude=round(magnitude, 4))


class ClaudeSentimentScorer(SentimentScorer):
    """
    Sentiment scorer backed by Claude Haiku via the Anthropic SDK.

    The Anthropic client is imported lazily so the rest of the app does not
    depend on it at import time.
    """

    MODEL = "claude-haiku-4-5-20251001"
    MAX_TOKENS = 64

    def __init__(self, api_key: str) -> None:
        # Lazy import — keep the Anthropic factory (and its anthropic/httpx
        # imports) out of module scope; pulled in only when a classifier is
        # constructed. The factory applies the bounded #1479 timeout.
        from app.services.anthropic_client import make_anthropic_client

        self._client = make_anthropic_client(api_key)

    def _call_with_retry(self, user_content: str):
        """Send the messages.create call with retry on transient errors.

        Retries (#29) are limited to:
          * ``anthropic.RateLimitError`` — HTTP 429.
          * ``anthropic.APIStatusError`` with ``status_code >= 500`` —
            transient server-side faults (529 included).

        Non-retryable: 4xx other than 429 (auth, bad request) and any
        non-Anthropic exception. Those propagate immediately so the
        caller's outer ``except Exception`` does not silently swallow
        a programmer error.

        Backoff is 1s, then 2s — short enough to keep the news loop
        moving on a brief 429 spike, long enough to clear most
        token-bucket windows.
        """
        import anthropic

        backoff = _RETRY_INITIAL_BACKOFF_S
        last_exc: Exception | None = None
        for attempt in range(1, _RETRY_MAX_ATTEMPTS + 1):
            try:
                return self._client.messages.create(
                    model=self.MODEL,
                    max_tokens=self.MAX_TOKENS,
                    system=_SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": user_content}],
                )
            except anthropic.RateLimitError as exc:
                last_exc = exc
                if attempt == _RETRY_MAX_ATTEMPTS:
                    break
                logger.warning(
                    "Sentiment scorer: 429 rate limit (attempt %d/%d); sleeping %.1fs",
                    attempt,
                    _RETRY_MAX_ATTEMPTS,
                    backoff,
                )
            except anthropic.APIStatusError as exc:
                # Only retry server-side errors. 4xx other than 429
                # mean the request is malformed or unauthorised —
                # retrying does not help.
                if exc.status_code < 500:
                    raise
                last_exc = exc
                if attempt == _RETRY_MAX_ATTEMPTS:
                    break
                logger.warning(
                    "Sentiment scorer: %d %s (attempt %d/%d); sleeping %.1fs",
                    exc.status_code,
                    type(exc).__name__,
                    attempt,
                    _RETRY_MAX_ATTEMPTS,
                    backoff,
                )
            time.sleep(backoff)
            backoff *= 2

        # All retries exhausted — re-raise the last seen transient
        # error so the caller can decide between skip and fail.
        # Explicit raise (not ``assert``) so production runs under
        # ``python -O`` (which strips assertions) cannot silently
        # fall through and return ``None`` — review feedback #618.
        if last_exc is None:
            raise RuntimeError("ClaudeSentimentScorer retry loop exited with no exception captured")
        raise last_exc

    def score(self, headline: str, snippet: str | None) -> SentimentResult:
        user_content = f"Headline: {headline}"
        if snippet:
            user_content += f"\nSnippet: {snippet}"

        message = self._call_with_retry(user_content)

        block = message.content[0]
        if not hasattr(block, "text"):
            logger.warning("Sentiment scorer: unexpected content block type %r, defaulting to neutral", type(block))
            return SentimentResult(label="neutral", magnitude=0.0)
        raw = block.text.strip()  # type: ignore[union-attr]
        try:
            parsed = json.loads(raw)
            label: SentimentLabel = parsed["label"]
            magnitude = float(parsed["magnitude"])
        except (KeyError, ValueError, json.JSONDecodeError) as exc:
            logger.warning("Sentiment scorer: unparseable response %r (%s), defaulting to neutral", raw, exc)
            return SentimentResult(label="neutral", magnitude=0.0)

        if label not in ("positive", "negative", "neutral"):
            logger.warning("Sentiment scorer: unexpected label %r, defaulting to neutral", label)
            return SentimentResult(label="neutral", magnitude=0.0)

        magnitude = max(0.0, min(1.0, magnitude))
        return SentimentResult(label=label, magnitude=magnitude)


def make_sentiment_scorer() -> SentimentScorer:
    """
    Build the active sentiment scorer (#1750).

    Single source of truth for "which scorer": Claude Haiku when
    ``ANTHROPIC_API_KEY`` is configured, else the keyless lexicon fallback so
    the news pipeline never hard-blocks on Anthropic availability.
    """
    from app.config import settings

    if settings.anthropic_api_key:
        return ClaudeSentimentScorer(settings.anthropic_api_key)
    logger.info("Sentiment: no ANTHROPIC_API_KEY — using keyless lexicon fallback scorer")
    return LexiconSentimentScorer()
