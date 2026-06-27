# #1750 — RSS news provider + keyless sentiment fallback (unblocks #593)

## Problem

`news_events` is empty on dev (0 rows). The whole consume side is built —
`refresh_news` pipeline (`app/services/news.py`), `ClaudeSentimentScorer`
(`app/services/sentiment.py:87`), read endpoint (`app/api/news.py:89`),
schema + unique `(instrument_id, url_hash)` — but **no concrete
`NewsProvider` exists** (`app/providers/news.py:34` is an abstract ABC), and
`daily_news_refresh` (`app/workers/scheduler.py:2926`) is unregistered and
hard-skips with "no provider configured". #593 (news-analytics drill) needs
rows with `sentiment_score` + `source` + `event_time`.

## Source rule / posture

- **News provider shape** (settled-decisions "News and sentiment"): production
  depends on the `NewsProvider` abstraction; tests use fakes. No vendor named.
- **Free-source posture** (#532, fundamentals-scoped but project ethos): prefer
  free, keyless sources. The stub's own docstring names "public RSS sources" as
  the v1 plan.
- **Sentiment storage** (settled): signed numeric score, no label columns in v1.
  A lexicon scorer satisfies this contract identically to Haiku.

## Source verification (empirical, 2026-06-27)

Yahoo Finance per-ticker RSS — `https://feeds.finance.yahoo.com/rss/2.0/headline?s=<SYM>&region=US&lang=en-US`:
- HTTP 200, keyless, **20 items/ticker**, real per-ticker headlines (verified for AAPL).
- Requires a browser-like `User-Agent` (default httpx UA risks block; `Mozilla/5.0` worked).
- Item fields: `title`, `description`, `link`, `pubDate` (RFC822 `Sat, 27 Jun 2026 13:38:13 +0000`), `guid`.
- **No explicit publisher/source field, no category** → `source = "Yahoo Finance"`, category via keyword heuristic.
- RSS is **recent-only** (~last few days). There is no deep historical bulk source — first-load is shallow by nature (documented limit, not a gap).

## Dev constraints (verified)

- Tier 1/2 analysable, tradable = **7 instruments** on dev: BBBY, GME, IEP, ATEX, MOG.A, TPR, TRGP. (tier3-analysable=3899 — out of daily scope.)
- **No `ANTHROPIC_API_KEY`** on dev (.env or shell) → Haiku scorer cannot run here. Operator decision: add keyless lexicon fallback so the pipeline degrades gracefully and dev-verify works keyless.

## Design

### 1. `YahooRssNewsProvider` — `app/providers/implementations/yahoo_rss_news.py`

Implements `NewsProvider.get_news(symbol, from_dt, to_dt) -> list[NewsItem]`:
- Build feed URL with `urllib.parse.quote` on symbol (validated as data, not interpolated raw).
- HTTP GET via injected `http_get` callable (default = module httpx.Client with `timeout=15s`, `User-Agent` from `settings.news_rss_user_agent`). Mirrors `sec_submissions.check_freshness(http_get=...)` injectable-client pattern so tests pass a fake — no network in tests.
- Parse with `xml.etree.ElementTree` (stdlib, no new dep). Per `<item>`: `title`→headline, `description`→snippet, `link`→url, `guid`→provider_id (**fallback to `link` when guid absent**), `pubDate`→published_at via `email.utils.parsedate_to_datetime`.
- **Strict per-item validity (Codex BLOCKING)** — skip the item (keep the rest) when any required field is missing/blank/unparsable: empty/missing `title`, empty/missing `link`, or missing/unparsable `pubDate`. `NewsItem` requires non-null `headline`/`url`/`published_at`; `refresh_news` runs `_url_hash(item.url)` immediately, so a bad url must never reach it.
- **tz normalisation (Codex MAJOR)** — `refresh_news` passes `from_dt`/`to_dt` through unchanged; a naive bound vs a tz-aware `published_at` raises. Normalise all three to UTC-aware in the provider before comparing (`dt if dt.tzinfo else dt.replace(tzinfo=UTC)`; `parsedate_to_datetime` may itself return naive for some feeds).
- **Date filter**: keep items with `from_dt <= published_at <= to_dt`. Sort **oldest-first** (contract).
- **Category heuristic** (`general` default): `earnings` if title/snippet matches `\b(earnings|revenue|eps|quarterly results|guidance)\b`; `analyst_note` if `\b(analyst|price target|upgrade|downgrade|rating|initiates? coverage)\b`. Pure function, table-tested.
- `raw_payload` = the item's serialized XML; `raw_payload_format="rss"`.
- Network/parse errors: raise (caught per-instrument by `refresh_news`'s try/except → instrument skipped, batch continues). Malformed single item → skip that item, keep the rest.

### 2. Lexicon sentiment fallback — `app/services/sentiment.py`

- Add `LexiconSentimentScorer(SentimentScorer)`: small inline signed lexicon (~40 finance terms, positive/negative). Tokenise headline + snippet (lowercase, `\w+`), sum hits, normalise to `magnitude ∈ [0,1]`, `label` from net sign, `signed_score` via existing property. Pure, deterministic, no I/O, no new dep.
- Add factory `make_sentiment_scorer() -> SentimentScorer`: returns `ClaudeSentimentScorer(settings.anthropic_api_key)` if key set, else `LexiconSentimentScorer()`. Single source of truth for "which scorer".

### 3. Tier 1/2 selector — `app/services/news.py`

Add `select_tier12_instruments(conn) -> list[tuple[str, str]]`:
```sql
SELECT i.symbol, i.instrument_id::text
FROM coverage c JOIN instruments i USING (instrument_id)
WHERE i.is_tradable AND c.filings_status = 'analysable' AND c.coverage_tier IN (1, 2)
ORDER BY i.symbol
```
`instrument_id::text` (Codex MEDIUM) — `refresh_news` contract is `list[tuple[str, str]]`. Returns `[(symbol, instrument_id_text), ...]`. (Not `find_stale_instruments` — that has thesis-staleness semantics; news wants the full covered set.)

### 4. Wire `daily_news_refresh` — `app/workers/scheduler.py`

**Zero-arg job (Codex BLOCKING ×3 fold).** No `instrument_ids` param:
`ParamMetadata` has no list/int-array type (`param_metadata.py:119`), an
arbitrary-id param would bypass the Tier1/2/analysable scope, and a
param-consuming body cannot use `_adapt_zero_arg`. Default scope (7 Tier1/2
instruments on dev) is small enough to run wholesale for dev-verify — no
scoping param needed.

Replace the stub body:
- `scorer = make_sentiment_scorer()` (no more hard key-skip — lexicon covers no-key).
- `provider = YahooRssNewsProvider()`.
- `from_dt = now - 7d`, `to_dt = now` (RSS is recent-only).
- Inside `_tracked_job(JOB_DAILY_NEWS_REFRESH)`: open conn, `symbols = select_tier12_instruments(conn)`, call `refresh_news(...)`, set `tracker.row_count = summary.articles_upserted`, `tracker.note = f"fetched={...} upserted={...} skipped={...}"`.

### 5. Register the job

- `SCHEDULED_JOBS` (`scheduler.py:645`): add `ScheduledJob(name=JOB_DAILY_NEWS_REFRESH, display_name="Daily news refresh", source="db", description=..., cadence=Cadence.daily(hour=?, minute=?), catch_up_on_boot=True, role="steady_state")`. `source="db"` — no `news` lane exists (`app/jobs/sources.py:62`); `db` is the generic job-overlap bucket (a lane bounds overlap, not rate — Yahoo has no shared budget). `catch_up_on_boot=True` = the first-load run over Tier 1/2 (RSS has no deeper bulk).
- `app/jobs/runtime.py:_INVOKERS` (line 280): add `JOB_DAILY_NEWS_REFRESH: _adapt_zero_arg(daily_news_refresh)` (zero-arg invoker). Membership here makes it appear in `VALID_JOB_NAMES`.

### 6. Config — `app/config.py`

Add `news_rss_user_agent: str = "Mozilla/5.0 (compatible; eBull/1.0; +research)"` (env `NEWS_RSS_USER_AGENT`; no settings prefix). Document it in `.env.example`. Yahoo rejects the default httpx UA — a browser-like UA is required (verified).

### 7. Tests (pure-logic, no DB)

- RSS fixture XML → `get_news` field mapping, date filter, oldest-first. Inject fake `http_get` returning the fixture string.
- Per-item skip cases (Codex): missing/empty `title` skip, empty/relative `link` skip, missing/unparsable `pubDate` skip, missing `guid` → `link` fallback, HTML-escaped `description` decoded, namespaced + non-namespaced item fixtures both parse.
- tz: naive `from_dt`/`to_dt` bounds vs tz-aware item must not raise.
- Category heuristic table-test (earnings / analyst_note / general).
- `LexiconSentimentScorer`: positive/negative/neutral headlines → expected sign.
- `make_sentiment_scorer`: key present → Claude type; absent → Lexicon type (monkeypatch settings).
- Registration: `JOB_DAILY_NEWS_REFRESH in VALID_JOB_NAMES` and in `_INVOKERS`.

### 8. Dev-verify

- `POST /jobs/daily_news_refresh/run` (zero-arg) — confirm 202, drain via `/jobs` status, `news_events` count > 0 across the 7 Tier1/2 instruments.
- `GET /news/{instrument_id}` (actual route, `app/api/news.py:91`) for GME (1699) → rows render with `sentiment_score`, `source`, `event_time`.
- Record counts + a sample headline in PR. (No `ANTHROPIC_API_KEY` on dev → lexicon scorer path exercised; document that.)

## Out of scope

- Non-US ticker suffixes (Yahoo `.L` etc.) — v1 passes symbol as-is; empty result is handled gracefully.
- Deep historical news backfill — RSS is recent-only; no source exists.
- #593 drill UI — separate ticket, unblocked by this.

## Files

- NEW `app/providers/implementations/yahoo_rss_news.py`
- `app/services/sentiment.py` (+LexiconSentimentScorer, +make_sentiment_scorer)
- `app/services/news.py` (+select_tier12_instruments)
- `app/workers/scheduler.py` (daily_news_refresh body + SCHEDULED_JOBS entry)
- `app/jobs/runtime.py` (_INVOKERS entry)
- `app/config.py` (news_rss_user_agent)
- NEW `tests/test_yahoo_rss_news.py`, `tests/test_sentiment_lexicon.py`
