"""
News and sentiment service.

Owns:
  - fetching candidate articles from a NewsProvider
  - url_hash computation (SHA-256 of the URL)
  - exact-duplicate filtering via (instrument_id, url_hash) in the DB
  - near-duplicate headline filtering (per-instrument, SequenceMatcher)
  - importance scoring (heuristic, no external calls)
  - sentiment scoring via a SentimentScorer (called only for new articles,
    outside any DB transaction)
  - DB upsert into news_events

Processing order per instrument:
  1. fetch candidates from provider
  2. compute url_hash for each
  3. remove exact duplicates already in DB
  4. run near-duplicate headline filtering on remaining candidates
  5. compute importance score (pure, no I/O)
  6. call sentiment scorer (outside DB transaction)
  7. persist all scored rows in a single transaction
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher

import psycopg

from app.providers.news import NewsCategory, NewsItem, NewsProvider
from app.services.sentiment import SentimentResult, SentimentScorer

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Near-duplicate detection config
# ---------------------------------------------------------------------------

SIMILARITY_THRESHOLD = 0.90  # SequenceMatcher ratio — tune upward to be stricter

# ---------------------------------------------------------------------------
# Importance scoring config
# ---------------------------------------------------------------------------

# Category weights — higher is more important
_CATEGORY_WEIGHT: dict[NewsCategory, float] = {
    "earnings": 1.0,
    "analyst_note": 0.6,
    "general": 0.3,
}

# Source-tier weights by publication name (lowercase).
# Tier 1: major wires / financial press. Default: 0.5.
_SOURCE_TIER: dict[str, float] = {
    "reuters": 1.0,
    "bloomberg": 1.0,
    "wall street journal": 1.0,
    "wsj": 1.0,
    "financial times": 1.0,
    "ft": 1.0,
    "cnbc": 0.8,
    "marketwatch": 0.7,
    "seeking alpha": 0.6,
    "benzinga": 0.6,
    "motley fool": 0.5,
}
_DEFAULT_SOURCE_WEIGHT = 0.5

# Recency decay: articles older than this many hours are penalised linearly.
_RECENCY_FULL_WEIGHT_HOURS = 4
_RECENCY_ZERO_WEIGHT_HOURS = 72  # fully stale after 3 days


# ---------------------------------------------------------------------------
# Internal scored article type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ScoredArticle:
    item: NewsItem
    url_hash: str
    importance: float
    sentiment: SentimentResult


# ---------------------------------------------------------------------------
# Public dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NewsRefreshSummary:
    instruments_attempted: int
    articles_fetched: int
    exact_duplicates_skipped: int
    near_duplicates_skipped: int
    articles_upserted: int
    instruments_skipped: int  # provider error


# ---------------------------------------------------------------------------
# Service entry point
# ---------------------------------------------------------------------------


def refresh_news(
    provider: NewsProvider,
    scorer: SentimentScorer,
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_symbols: list[tuple[str, str]],  # [(symbol, instrument_id), ...]
    from_dt: datetime,
    to_dt: datetime,
) -> NewsRefreshSummary:
    """
    Refresh news events for a list of instruments.

    instrument_symbols: list of (symbol, instrument_id) pairs — instrument_id
        is the BIGINT PK from the instruments table, passed as str for
        consistency with the rest of the service layer.
    """
    total_fetched = 0
    total_exact_skip = 0
    total_near_skip = 0
    total_upserted = 0
    total_skipped = 0

    for symbol, instrument_id in instrument_symbols:
        try:
            fetched, exact_skip, near_skip, upserted = _process_instrument(
                provider=provider,
                scorer=scorer,
                conn=conn,
                symbol=symbol,
                instrument_id=instrument_id,
                from_dt=from_dt,
                to_dt=to_dt,
            )
            total_fetched += fetched
            total_exact_skip += exact_skip
            total_near_skip += near_skip
            total_upserted += upserted
        except Exception:
            logger.warning(
                "News refresh: failed for symbol=%s instrument_id=%s, skipping",
                symbol,
                instrument_id,
                exc_info=True,
            )
            total_skipped += 1

    return NewsRefreshSummary(
        instruments_attempted=len(instrument_symbols),
        articles_fetched=total_fetched,
        exact_duplicates_skipped=total_exact_skip,
        near_duplicates_skipped=total_near_skip,
        articles_upserted=total_upserted,
        instruments_skipped=total_skipped,
    )


# ---------------------------------------------------------------------------
# Per-instrument processing
# ---------------------------------------------------------------------------


def _process_instrument(
    provider: NewsProvider,
    scorer: SentimentScorer,
    conn: psycopg.Connection,  # type: ignore[type-arg]
    symbol: str,
    instrument_id: str,
    from_dt: datetime,
    to_dt: datetime,
) -> tuple[int, int, int, int]:
    """
    Returns (fetched, exact_skipped, near_skipped, upserted).
    """
    candidates = provider.get_news(symbol=symbol, from_dt=from_dt, to_dt=to_dt)
    fetched = len(candidates)

    if not candidates:
        return fetched, 0, 0, 0

    # Step 2 — compute url_hash for each candidate
    hashed: list[tuple[NewsItem, str]] = [(item, _url_hash(item.url)) for item in candidates]

    # Step 3 — remove exact duplicates already in the DB
    known_hashes = _load_known_hashes(conn, instrument_id)
    new_items = [(item, h) for item, h in hashed if h not in known_hashes]
    exact_skipped = len(hashed) - len(new_items)

    # Step 4 — near-duplicate headline filtering (per-instrument)
    deduped, near_skipped = _filter_near_duplicates(new_items, conn, instrument_id)

    # Steps 5–6 — score outside any DB transaction (Claude calls must not hold
    # a DB connection open; a transient 429/529 from Anthropic would otherwise
    # abort the entire batch insert and hold the connection for the full round-trip)
    scored: list[_ScoredArticle] = []
    for item, url_hash in deduped:
        importance = _importance_score(item, to_dt)
        sentiment = scorer.score(item.headline, item.snippet)
        scored.append(_ScoredArticle(item=item, url_hash=url_hash, importance=importance, sentiment=sentiment))

    # Step 7 — persist all scored rows in a single transaction
    with conn.transaction():
        for article in scored:
            _upsert_news_event(conn, instrument_id, article)

    return fetched, exact_skipped, near_skipped, len(scored)


# ---------------------------------------------------------------------------
# Deduplication helpers
# ---------------------------------------------------------------------------


def _url_hash(url: str) -> str:
    """SHA-256 hex digest of the URL."""
    return hashlib.sha256(url.encode()).hexdigest()


def _load_known_hashes(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: str,
) -> set[str]:
    """Load all url_hash values already stored for this instrument."""
    rows = conn.execute(
        "SELECT url_hash FROM news_events WHERE instrument_id = %(id)s",
        {"id": instrument_id},
    ).fetchall()
    return {row[0] for row in rows}


def _normalise_headline(headline: str) -> str:
    """Lowercase, strip accents, collapse punctuation and whitespace."""
    text = unicodedata.normalize("NFKD", headline).encode("ascii", "ignore").decode()
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _filter_near_duplicates(
    candidates: list[tuple[NewsItem, str]],
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: str,
) -> tuple[list[tuple[NewsItem, str]], int]:
    """
    Remove near-duplicate headlines from candidates.

    Compares candidates against each other (within this batch) using
    SequenceMatcher on normalised headlines. The first article in each
    near-duplicate cluster is kept. Also guards against near-dupes of
    headlines already in the DB from the last 72 hours.

    Normalised forms are pre-computed and cached to avoid O(N²) re-normalisation.

    Returns (deduped_list, near_skipped_count).
    """
    # Load recent normalised headlines already in the DB for this instrument
    db_norms = _load_recent_headlines(conn, instrument_id)

    # Pre-compute normalised forms for all candidates
    candidate_norms: list[tuple[NewsItem, str, str]] = [
        (item, url_hash, _normalise_headline(item.headline)) for item, url_hash in candidates
    ]

    kept: list[tuple[NewsItem, str]] = []
    kept_norms: list[str] = []
    skipped = 0

    for item, url_hash, norm in candidate_norms:
        is_near_dup = any(
            SequenceMatcher(None, norm, kept_norm).ratio() >= SIMILARITY_THRESHOLD for kept_norm in kept_norms
        )
        if not is_near_dup:
            is_near_dup = any(
                SequenceMatcher(None, norm, db_norm).ratio() >= SIMILARITY_THRESHOLD for db_norm in db_norms
            )

        if is_near_dup:
            skipped += 1
        else:
            kept.append((item, url_hash))
            kept_norms.append(norm)

    return kept, skipped


def _load_recent_headlines(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: str,
) -> list[str]:
    """
    Load normalised headlines already in the DB for this instrument from the
    last 72 hours (the recency window used for importance scoring).
    """
    rows = conn.execute(
        """
        SELECT headline
        FROM news_events
        WHERE instrument_id = %(id)s
          AND event_time >= NOW() - INTERVAL '72 hours'
          AND headline IS NOT NULL
        """,
        {"id": instrument_id},
    ).fetchall()
    return [_normalise_headline(row[0]) for row in rows]


# ---------------------------------------------------------------------------
# Importance scoring
# ---------------------------------------------------------------------------


def score_importance(item: NewsItem, as_of: datetime) -> float:
    """
    Heuristic importance score in [0.0, 1.0].

    Weighted combination of:
      - category weight (earnings > analyst_note > general)
      - source tier weight
      - recency (linear decay from 4h to 72h)

    Exposed at module level so it can be tested directly.
    """
    return _importance_score(item, as_of)


def _importance_score(item: NewsItem, as_of: datetime) -> float:
    category_w = _CATEGORY_WEIGHT.get(item.category, _CATEGORY_WEIGHT["general"])
    source_w = _SOURCE_TIER.get(item.source.lower(), _DEFAULT_SOURCE_WEIGHT)

    # Normalise both to naive UTC for arithmetic — providers may supply tz-aware datetimes
    as_of_naive = as_of.replace(tzinfo=None)
    published_naive = item.published_at.replace(tzinfo=None)
    age_hours = max(0.0, (as_of_naive - published_naive).total_seconds() / 3600)
    if age_hours <= _RECENCY_FULL_WEIGHT_HOURS:
        recency_w = 1.0
    elif age_hours >= _RECENCY_ZERO_WEIGHT_HOURS:
        recency_w = 0.0
    else:
        span = _RECENCY_ZERO_WEIGHT_HOURS - _RECENCY_FULL_WEIGHT_HOURS
        recency_w = 1.0 - (age_hours - _RECENCY_FULL_WEIGHT_HOURS) / span

    # Equal weighting across the three factors
    raw = (category_w + source_w + recency_w) / 3.0
    return round(min(1.0, max(0.0, raw)), 6)


# ---------------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------------


def _upsert_news_event(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: str,
    article: _ScoredArticle,
) -> None:
    """
    Upsert a single news event.
    Idempotent — keyed on (instrument_id, url_hash).

    raw_payload_json: pristine provider payload, unmodified.
    sentiment_raw_json: scorer output (label + magnitude) stored separately
        so provider data and derived data are never conflated.
    """
    item = article.item

    raw_payload: dict[str, object] | None = None
    if item.raw_payload is not None:
        try:
            raw_payload = json.loads(item.raw_payload)
        except (ValueError, TypeError):
            raw_payload = {"raw": item.raw_payload}

    sentiment_raw = {
        "label": article.sentiment.label,
        "magnitude": article.sentiment.magnitude,
    }

    conn.execute(
        """
        INSERT INTO news_events (
            instrument_id, event_time, source, headline, category,
            sentiment_score, importance_score, url_hash, url, snippet,
            raw_payload_json, sentiment_raw_json
        )
        VALUES (
            %(instrument_id)s, %(event_time)s, %(source)s, %(headline)s, %(category)s,
            %(sentiment_score)s, %(importance_score)s, %(url_hash)s, %(url)s, %(snippet)s,
            %(raw_payload_json)s, %(sentiment_raw_json)s
        )
        ON CONFLICT (instrument_id, url_hash) DO NOTHING
        """,
        {
            "instrument_id": instrument_id,
            "event_time": item.published_at,
            "source": item.source,
            "headline": item.headline,
            "category": item.category,
            "sentiment_score": article.sentiment.signed_score,
            "importance_score": article.importance,
            "url_hash": article.url_hash,
            "url": item.url,
            "snippet": item.snippet,
            "raw_payload_json": json.dumps(raw_payload) if raw_payload is not None else None,
            "sentiment_raw_json": json.dumps(sentiment_raw),
        },
    )
