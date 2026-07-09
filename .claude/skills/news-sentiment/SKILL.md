---
name: news-sentiment
description: eBull news ingest + signed sentiment — the daily Yahoo-RSS fetch/dedup/score pipeline in app/services/news.py + app/services/sentiment.py, the news_events table, and how the 30d importance-weighted sentiment feeds the scoring sentiment family.
---

# news-sentiment

## When to use

Any change to `app/services/news.py` (fetch/dedup/importance/upsert), `app/services/sentiment.py`
(scorer ABC + Claude/lexicon impls), the `news_events` table (`sql/001_init.sql`, `sql/005_news_events_url_snippet.sql`),
the `NewsProvider` abstraction (`app/providers/news.py`) / `YahooRssNewsProvider`
(`app/providers/implementations/yahoo_rss_news.py`), the `GET /news/{instrument_id}` endpoint
(`app/api/news.py`), the `daily_news_refresh` job (`app/workers/scheduler.py`), or the sentiment
family / news-completeness inputs to scoring (`_sentiment_score`, `_NEWS_LOOKBACK_DAYS`,
`_C_WEIGHT_NEWS` in `app/services/scoring.py`).

## What it is

**Ingest job.** `daily_news_refresh()` (`scheduler.py`, `JOB_DAILY_NEWS_REFRESH = "daily_news_refresh"`,
registered in `app/jobs/runtime.py`). Scope = covered Tier 1/2 via `select_tier12_instruments`
(`is_tradable AND filings_status='analysable' AND coverage_tier IN (1,2)`). Source is keyless
per-ticker Yahoo Finance RSS (`YahooRssNewsProvider`, #1750); fetch window is the last 7 days
(RSS is recent-only — there is no deeper historical bulk source).

**Pipeline** (`_process_instrument`, per instrument, best-effort): fetch candidates → `_url_hash`
(SHA-256 of URL) → drop exact dupes on `(instrument_id, url_hash)` → near-dup filter
(`_filter_near_duplicates`, `SequenceMatcher` ratio ≥ `SIMILARITY_THRESHOLD = 0.90`, per-instrument,
vs the batch + headlines from the last 72h) → `_importance_score` (heuristic, no I/O: category
weight earnings 1.0 / analyst_note 0.6 / general 0.3 + source-tier weight, default 0.5 + linear
recency decay, full ≤4h → zero ≥72h) → sentiment score (**outside** the DB transaction) → upsert.
A per-instrument provider/scorer error is logged and skipped, never aborts the batch.

**Sentiment scorer.** `make_sentiment_scorer()` (`sentiment.py`) returns `ClaudeSentimentScorer`
(Claude Haiku `claude-haiku-4-5-20251001`, transient-error retry #29) when `ANTHROPIC_API_KEY` is
set, else the keyless `LexiconSentimentScorer` (signed finance bag-of-words, #1750) so the pipeline
never hard-blocks on Anthropic. `SentimentResult.signed_score`: positive → +magnitude, negative →
−magnitude, neutral → 0.0.

**Table.** `news_events` (`sql/001_init.sql`); `url`, `snippet`, `sentiment_raw_json` columns +
`url_hash NOT NULL` + `UNIQUE (instrument_id, url_hash)` added in `sql/005`. `raw_payload_json` =
pristine provider payload; `sentiment_raw_json` = scorer label+magnitude, stored **separately** so
provider data and derived data are never conflated.

**Endpoint.** `GET /news/{instrument_id}` (`app/api/news.py`, prefix `/news`, mounted in
`app/main.py`) — default 30-day window, `event_time DESC`, paginated. Renders in
`frontend/src/components/instrument/RecentNewsPane.tsx` + `NewsAnalysisPage.tsx`.

**Scoring consumption.** `_sentiment_score` (`scoring.py`): importance-weighted mean of signed
sentiment over the 30-day lookback (`_NEWS_LOOKBACK_DAYS = 30`; query filters `sentiment_score IS
NOT NULL`), mapped [-1,1] → [0,1] via `(raw+1)/2`. Sentiment family weight 0.05 (balanced) to 0.10
(speculative) in `_WEIGHT_MODES`. Separately, the data-completeness `C` news component
(`_C_WEIGHT_NEWS = 0.10`): ≥3 items/90d full, ≥1 half, else 0. Freshness: `ops_monitor` tracks the
`news` layer via `MAX(created_at) FROM news_events`, stale after 3 days.

## Invariants

- **settled-decisions "## News and sentiment" → News event storage / News dedupe.** `news_events`
  stores url, url_hash, snippet, sentiment + importance, raw payload. Exact dedupe is per
  `(instrument_id, url_hash)`; near-dup detection is per instrument, **not global**.
- **settled-decisions "## News and sentiment" → Sentiment storage.** Persist sentiment as a **signed
  numeric** score; **no separate label columns in v1** (the label lives in `sentiment_raw_json`).
- **settled-decisions "## News and sentiment" → News provider shape.** Production depends on the
  `NewsProvider` abstraction; tests use fakes; do not shape production APIs around test convenience.
- **settled-decisions "## Thesis prompt budget".** The thesis writer consumes the latest 10 news
  items from the last 30 days — keep the read path (recent, capped) compatible.
- **Sentiment is not a thesis.** The family weight is 0.05–0.10 by design: retail hype is not a
  thesis by itself and sentiment must never dominate fundamentals. Judged from a long-only equity
  investor's perspective (`sentiment.py` system prompt) — consistent with long-only v1.
- **Deterministic + auditable.** Sentiment scoring runs outside the DB transaction; the upsert is
  idempotent (`ON CONFLICT DO NOTHING`); scoring aggregation is heuristic and explicit. News is
  research signal only — it never itself gates a trade.

## Failure conditions

- **Signed 0.0 = genuine NEUTRAL, never "missing".** A neutral article scores 0.0 and renders as
  neutral; `sentiment_raw_json` retains its raw label/magnitude. Do not treat 0.0 as absent data.
- **Missing critical source data surfaces as missingness, not a neutral fill.** No rows / all
  `sentiment_score` NULL → `_sentiment_score` returns 0.5 with an **explicit note** ("no recent news
  events; defaulting to neutral 0.5") AND the `C` news component drops toward 0 (completeness carries
  the gap). Never silently equate "no news" with "neutral news".
- **Stale timestamps.** RSS is recent-only (7-day fetch, no historical backfill); the near-dup guard
  only looks back 72h; `ops_monitor` flags the `news` layer stale after 3 days. A dry layer is a
  signal to raise, not to paper over.
- **Contradictory / out-of-range evidence is surfaced, not absorbed.** Sentiment values outside
  [-1, 1] are logged as a warning and clipped on aggregation (`_sentiment_score`) rather than
  distorting the mean silently.
- **Anthropic unavailable** → keyless lexicon fallback (bag-of-words, no negation/context); lower
  quality than Haiku. This is a documented degradation, not a silent equivalence.
