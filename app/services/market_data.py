"""
Market data service.

Ingests daily OHLCV candles and current quotes for covered instruments,
computes rolling return and volatility features, and flags wide spreads.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

import psycopg
from psycopg.rows import dict_row

from app.providers.market_data import MarketDataProvider, OHLCVBar, Quote
from app.services.sync_orchestrator.exception_classifier import classify_exception
from app.services.sync_orchestrator.layer_types import FailureCategory, UpstreamUnreachableError
from app.services.sync_orchestrator.progress import report_progress
from app.services.technical_analysis import OHLCVRow, compute_indicators

logger = logging.getLogger(__name__)

# Batch circuit-breaker (#1833). When the eToro market-data API is
# unreachable, every per-instrument candle fetch hits the provider's 30s
# timeout and raises — walking all ~775 scoped instruments would burn
# ≈6.5h grinding through dead requests for a job that normally finishes in
# ~1 minute. After this many CONSECUTIVE *systemic* failures the candle
# loop aborts the whole batch with a clear terminal status instead.
# "Consecutive" counts ATTEMPTED fetches: a freshness-skipped instrument
# is neutral (no fetch, no reachability evidence) and neither increments
# nor resets the counter. The counter resets on any fetch that proves the
# server is still reachable — a clean fetch OR a per-instrument fault that
# still got a response (e.g. a 404 for a delisted symbol) — so a few
# genuinely-delisted instruments never trip it.
_CANDLE_BATCH_ABORT_LIMIT = 10

# Failure categories that indicate a WHOLE-BATCH outage (the next
# instrument will fail the same way), as opposed to a per-instrument fault
# (404 delisted → INTERNAL_ERROR, a unique-constraint clash → DB_CONSTRAINT,
# a feature-compute bug → INTERNAL_ERROR). Sourced from the single failure
# taxonomy in ``classify_exception`` so this never drifts from it:
#   * SOURCE_DOWN   — httpx.TransportError (DNS / connect / read timeout),
#                     5xx after retries, or a psycopg.OperationalError raised
#                     mid-fetch (transient DB blip inside the transaction).
#                     A HARD DB outage trips the freshness probe on the first
#                     instrument (outside the per-item try) and fails the run
#                     fast on its own — no grind to break.
#   * AUTH_EXPIRED  — 401/403: the broker session is dead for every call
#   * RATE_LIMITED  — 429 after the retry budget is exhausted
_SYSTEMIC_FAILURE_CATEGORIES = frozenset(
    {
        FailureCategory.SOURCE_DOWN,
        FailureCategory.AUTH_EXPIRED,
        FailureCategory.RATE_LIMITED,
    }
)

# Default spread threshold from trading-policy.md.
# An instrument is flagged if (ask - bid) / mid > this value.
DEFAULT_MAX_SPREAD_PCT = Decimal("1.0")  # 1%

# Lookback windows in calendar days for rolling return computation.
# These are approximate (trading days vary); exact day counts are resolved
# from the available price history.
_RETURN_WINDOWS: dict[str, int] = {
    "return_1w": 7,
    "return_1m": 30,
    "return_3m": 91,
    "return_6m": 182,
    "return_1y": 365,
}
_VOLATILITY_WINDOW_DAYS = 30


@dataclass(frozen=True)
class DayChange:
    """Close-to-close day-change for one instrument, from ``price_daily``.

    Built from an instrument's two most recent **strictly-positive** closes.
    ``as_of`` is the latest close's ``price_date`` — the metric is stamped with
    it (settled-decisions.md:767 "latest closed session" as-of convention) so a
    stale close reads honestly rather than as "today". ``change_pct`` is a
    FRACTION (``-0.015`` = −1.5%), matching the ``formatPct`` frontend contract.
    """

    as_of: date
    last_close: Decimal
    prior_close: Decimal
    change_abs: Decimal
    change_pct: Decimal


def compute_day_change(last_close: Decimal, prior_close: Decimal) -> Decimal | None:
    """Fractional close-to-close change, or ``None`` when ``prior_close <= 0``.

    A non-positive prior close is a non-price sentinel (``price_daily`` holds
    real ``close = 0`` rows — the same cross-surface invariant prevention-log
    #1428 documents for ``quotes.last``), so no meaningful change exists.
    """
    if prior_close <= 0:
        return None
    return (last_close - prior_close) / prior_close


def load_day_changes(
    conn: psycopg.Connection[object],
    instrument_ids: Sequence[int],
) -> dict[int, DayChange]:
    """Batch day-change over an instrument's two most-recent positive closes.

    One window query ranks ``close > 0`` rows per instrument (strictly-positive
    skips ``price_daily``'s real zero-close sentinels) and keeps the top two.
    Instruments with fewer than two positive closes are omitted (caller renders
    "—"). Fan-out-safe: PK ``(instrument_id, price_date)`` guarantees one row
    per date.
    """
    ids = list({int(i) for i in instrument_ids})
    if not ids:
        return {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            WITH ranked AS (
                SELECT instrument_id, price_date, close,
                       row_number() OVER (
                           PARTITION BY instrument_id ORDER BY price_date DESC
                       ) AS rn
                FROM price_daily
                WHERE instrument_id = ANY(%(ids)s) AND close > 0
            )
            SELECT instrument_id,
                   max(close)      FILTER (WHERE rn = 1) AS last_close,
                   max(price_date) FILTER (WHERE rn = 1) AS as_of,
                   max(close)      FILTER (WHERE rn = 2) AS prior_close
            FROM ranked
            WHERE rn <= 2
            GROUP BY instrument_id
            HAVING count(*) = 2
            """,
            {"ids": ids},
        )
        rows = cur.fetchall()

    out: dict[int, DayChange] = {}
    for r in rows:
        last_close = r["last_close"]  # type: ignore[assignment]
        prior_close = r["prior_close"]  # type: ignore[assignment]
        # ``prior_close > 0`` is guaranteed by the ``WHERE close > 0`` filter, so
        # this is never None in practice — the guard narrows the type for the
        # checker and routes the formula through the single tested source
        # (``compute_day_change``) rather than duplicating it inline.
        pct = compute_day_change(last_close, prior_close)
        if pct is None:  # pragma: no cover — defensive; filter guarantees prior_close > 0
            continue
        out[int(r["instrument_id"])] = DayChange(  # type: ignore[arg-type]
            as_of=r["as_of"],  # type: ignore[arg-type]
            last_close=last_close,
            prior_close=prior_close,
            change_abs=last_close - prior_close,
            change_pct=pct,
        )
    return out


@dataclass(frozen=True)
class MarketRefreshSummary:
    instruments_refreshed: int
    candle_rows_upserted: int
    features_computed: int
    quotes_updated: int
    quotes_skipped: int
    spread_flags_set: int
    # #1293 — disambiguate a candle_rows_upserted=0 outcome. ``candles_skipped``
    # counts instruments skipped because their candles were already fresh
    # (legitimate no-op). ``candles_failed`` counts instruments whose refresh
    # raised — that wraps the WHOLE per-instrument transaction (provider
    # fetch + ``_upsert_candles`` + feature compute), so a DB/write error
    # counts too, not only an eToro fetch/session failure; callers must phrase
    # the cause accordingly. Without these the caller cannot tell a healthy
    # "everything already fresh" run from a broken "every fetch failed" run;
    # both report 0 candles written.
    candles_skipped: int = 0
    candles_failed: int = 0
    # #2066 — instruments whose incremental overlap showed a ratio-scale
    # close mismatch (split/adjustment event) and were healed with an
    # in-run full-history re-fetch.
    adjustment_refetches: int = 0


def refresh_market_data(
    provider: MarketDataProvider,
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instruments: list[tuple[int, str]],  # [(instrument_id, symbol), ...]
    lookback_days: int = 1000,
    max_spread_pct: Decimal = DEFAULT_MAX_SPREAD_PCT,
    *,
    skip_quotes: bool = False,
    force_backfill: bool = False,
    consecutive_failure_limit: int = _CANDLE_BATCH_ABORT_LIMIT,
) -> MarketRefreshSummary:
    """
    For each instrument: fetch candles, upsert to price_daily, compute
    features, then (unless skip_quotes=True) batch-fetch quotes and
    upsert with spread flag.

    When skip_quotes is True, quote fetching and upserting are skipped
    entirely. Use this when a separate hourly job owns quote freshness
    (e.g. fx_rates_refresh).

    When force_backfill is True, every instrument fetches the full
    ``lookback_days`` window regardless of whether incremental mode
    would otherwise apply. Used for the one-shot deepening invocation
    (#603) — the daily scheduled refresh leaves it False so steady-state
    eToro call weight stays at the incremental cadence.

    instruments is a list of (instrument_id, symbol) tuples — instrument_id
    must already exist in the instruments table. symbol is used for logging.

    ``consecutive_failure_limit`` (#1833) is the batch circuit-breaker
    threshold: after this many CONSECUTIVE systemic candle-fetch failures
    (provider unreachable, session dead, rate-limited) the loop raises
    ``UpstreamUnreachableError`` and aborts the rest of the batch instead
    of grinding through hundreds of per-instrument 30s timeouts. "Consecutive"
    counts attempted fetches — a freshness-skipped instrument is neutral; a
    reachable response (clean fetch or a 404) resets the counter. Pass
    ``<= 0`` to disable the breaker (walk every instrument regardless).

    Raw provider responses are persisted by the provider before being returned.
    """
    candle_rows_upserted = 0
    features_computed = 0
    quotes_updated = 0
    quotes_skipped = 0
    spread_flags_set = 0

    today = date.today()
    candles_skipped = 0
    candles_failed = 0

    # --- Candles: per-instrument (with freshness skip + two-mode fetch) ---
    # Two-mode fetch (#271):
    #   * Backfill mode — instrument has NO prior candles (new to the
    #     universe, or gap detected). Pull full `lookback_days` history.
    #     Default 1000 — eToro's hard ceiling per request (#603 raised
    #     from 400 → 1000). 1000 trading days ≈ 4 calendar years of
    #     price points, which is the most we can fit in a single fetch.
    #     The endpoint is count-based with no from_date pagination, so
    #     we cannot deepen further without re-fetching everything.
    #   * Incremental mode — instrument already has candle history.
    #     Pull only INCREMENTAL_FETCH_BARS bars (yesterday + today +
    #     correction buffer). The upsert dedupes on (instrument_id,
    #     price_date) so overlap with existing rows is harmless.
    # On a typical day, ~100% of Tier 1/2 instruments are in incremental
    # mode — eToro call weight stays at 3 × ~500 instruments (~1500
    # rows). The 1000-bar default only fires on initial seed,
    # gap-detect, or the one-shot ``force_backfill=True`` deepening.
    total = len(instruments)
    # #1833 batch circuit-breaker — consecutive systemic failures.
    consecutive_systemic_failures = 0
    adjustment_refetches = 0
    for idx, (instrument_id, symbol) in enumerate(instruments, start=1):
        if not force_backfill and _candles_are_fresh(conn, instrument_id, today):
            candles_skipped += 1
            report_progress(idx, total)
            continue
        if force_backfill:
            fetch_count = lookback_days
        else:
            fetch_count = _candles_fetch_count(conn, instrument_id, default=lookback_days, today=today)
        upserted = 0
        computed = 0
        try:
            with conn.transaction():
                bars = provider.get_daily_candles(instrument_id, fetch_count)
                # #2066 split-cliff guard: provider history is back-adjusted
                # at fetch time, so a future split re-bases every bar — but an
                # incremental fetch only rewrites the overlap window, leaving
                # all older rows on the old basis (a permanent cliff at the
                # buffer edge). The overlap re-fetch is the one place the two
                # bases meet: a ratio-scale close mismatch on an already-stored
                # date = adjustment event → heal same-day with an in-run
                # full-history re-fetch (idempotent upsert rewrites the series).
                if bars and not force_backfill and fetch_count == _INCREMENTAL_FETCH_BARS:
                    stored = _stored_overlap_closes(conn, instrument_id, [b.price_date for b in bars])
                    ratio = detect_adjustment_event(stored, bars)
                    if ratio is not None:
                        adjustment_refetches += 1
                        logger.warning(
                            "Adjustment event detected for %s (id=%d): overlap close ratio %s — "
                            "re-fetching full %d-bar history to heal the series",
                            symbol,
                            instrument_id,
                            ratio,
                            lookback_days,
                        )
                        bars = provider.get_daily_candles(instrument_id, lookback_days)
                if bars:
                    upserted = _upsert_candles(conn, instrument_id, bars)
                    computed = _compute_and_store_features(conn, instrument_id)
            # Accumulate the running totals ONLY after the transaction has
            # committed cleanly (#1293 / Codex): incrementing inside the
            # ``with`` block would over-report rows for an instrument whose
            # feature-compute or commit later raised and rolled the write back
            # — and that same instrument is also counted in ``candles_failed``.
            candle_rows_upserted += upserted
            features_computed += computed
            # A clean fetch proves the provider + DB are reachable → reset.
            consecutive_systemic_failures = 0
        except Exception as exc:
            candles_failed += 1
            logger.warning("Failed to refresh candles for %s (id=%d), skipping", symbol, instrument_id, exc_info=True)
            category = classify_exception(exc)
            if category in _SYSTEMIC_FAILURE_CATEGORIES:
                consecutive_systemic_failures += 1
                if 0 < consecutive_failure_limit <= consecutive_systemic_failures:
                    # Whole-batch outage — fail FAST with the triggering
                    # category instead of walking the remaining instruments
                    # through the same 30s-timeout grind (#1833). report_progress
                    # the partial position first so the run's last heartbeat
                    # reflects where it stopped.
                    report_progress(idx, total, force=True)
                    raise UpstreamUnreachableError(
                        category,
                        f"{consecutive_systemic_failures} consecutive systemic candle-fetch "
                        f"failures (aborted batch of {total} after {idx} instruments; "
                        f"last failure on {symbol} id={instrument_id})",
                    ) from exc
            else:
                # A per-instrument fault (404 delisted, DB-constraint clash,
                # feature-compute bug) proves the server still responds →
                # reset so a sprinkling of dead symbols never trips the breaker.
                consecutive_systemic_failures = 0
        report_progress(idx, total)

    # Final force-tick so items_done lands at the loop boundary even
    # if the last increment was below the throttle threshold.
    report_progress(total, total, force=True)

    if candles_skipped:
        logger.info("Candle freshness skip: %d/%d instruments already fresh", candles_skipped, len(instruments))

    # --- Quotes: batch fetch, then per-instrument upsert ---
    # When skip_quotes is True, quote freshness is owned by the hourly
    # fx_rates_refresh job — the daily candle job must not shadow those
    # fresher values with stale end-of-day data.
    if not skip_quotes:
        all_ids = [iid for iid, _ in instruments]
        batch_failed = False
        try:
            quotes = provider.get_quotes(all_ids)
        except Exception:
            logger.warning("Failed to batch-fetch quotes, skipping all quote updates", exc_info=True)
            quotes = []
            quotes_skipped = len(instruments)
            batch_failed = True

        if not batch_failed:
            quote_map: dict[int, Quote] = {q.instrument_id: q for q in quotes}

            for instrument_id, symbol in instruments:
                quote = quote_map.get(instrument_id)
                if quote is None:
                    logger.debug("No quote returned for %s (id=%d), skipping quote upsert", symbol, instrument_id)
                    quotes_skipped += 1
                    continue
                try:
                    with conn.transaction():
                        flagged = _upsert_quote(conn, instrument_id, quote, max_spread_pct)
                        quotes_updated += 1
                        if flagged:
                            spread_flags_set += 1
                except Exception:
                    logger.warning(
                        "Failed to upsert quote for %s (id=%d), skipping",
                        symbol,
                        instrument_id,
                        exc_info=True,
                    )

    return MarketRefreshSummary(
        instruments_refreshed=len(instruments),
        candle_rows_upserted=candle_rows_upserted,
        features_computed=features_computed,
        quotes_updated=quotes_updated,
        quotes_skipped=quotes_skipped,
        spread_flags_set=spread_flags_set,
        candles_skipped=candles_skipped,
        candles_failed=candles_failed,
        adjustment_refetches=adjustment_refetches,
    )


def _most_recent_trading_day(today: date) -> date:
    """Return the most recent weekday (Mon-Fri) on or before today.

    On weekdays (Mon-Fri), today's candle is available after market
    close — the daily candle job runs at 22:00 UTC, well after the
    US close (~21:00 UTC). So the freshness target is today itself.

    Weekends roll back to Friday (no candles for Sat/Sun).

    No holiday calendar — if a holiday causes a gap, the next fetch
    fills it. Holidays don't cause false staleness because the candle
    endpoint simply returns nothing new.
    """
    weekday = today.weekday()  # 0=Mon, 6=Sun
    if weekday == 5:  # Saturday → Friday
        return today - timedelta(days=1)
    if weekday == 6:  # Sunday → Friday
        return today - timedelta(days=2)
    # Mon-Fri: today's candle is the freshness target
    return today


def _candles_are_fresh(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    today: date,
) -> bool:
    """Return True if price_daily already has the most recent trading day's candle."""
    row = conn.execute(
        """
        SELECT MAX(price_date)
        FROM price_daily
        WHERE instrument_id = %(instrument_id)s
        """,
        {"instrument_id": instrument_id},
    ).fetchone()
    if row is None or row[0] is None:
        return False
    latest_date: date = row[0]
    return latest_date >= _most_recent_trading_day(today)


# Incremental fetch window in bars — yesterday + today + one
# correction-day buffer. eToro's /candles endpoint has no date-range
# filter, only `candlesCount`; this is the smallest count that still
# catches the latest bar plus a one-day retrospective correction.
_INCREMENTAL_FETCH_BARS = 3


def _candles_fetch_count(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    *,
    default: int,
    today: date | None = None,
) -> int:
    """Decide the candlesCount for an instrument's fetch (#271).

    Returns ``default`` (typically 1000 per #603) in two cases:
      * No prior candles at all — initial backfill mode.
      * Prior candles exist but the most recent is older than the
        incremental window (e.g. instrument was halted, re-added to
        the universe after a gap, or a multi-day market closure). A
        3-bar incremental fetch here would silently leave a history
        gap; falling back to ``default`` closes the gap.

    Returns ``_INCREMENTAL_FETCH_BARS`` when the most recent candle is
    within the incremental window — normal daily maintenance mode.
    The upsert dedupes on (instrument_id, price_date) so overlap is
    safe.

    Note: this function does NOT extend an instrument's lookback when
    ``default`` is bumped. An instrument that has 400 bars stays at
    400 in incremental mode; deepening to 5y requires the one-shot
    ``force_backfill=True`` invocation in ``refresh_market_data``.
    """
    row = conn.execute(
        """
        SELECT MAX(price_date) FROM price_daily
        WHERE instrument_id = %(instrument_id)s
        """,
        {"instrument_id": instrument_id},
    ).fetchone()
    if row is None or row[0] is None:
        return default  # no prior data — backfill
    latest: date = row[0]
    reference = today if today is not None else date.today()
    gap_days = (reference - latest).days
    if gap_days > _INCREMENTAL_FETCH_BARS:
        # Gap wider than the incremental window — backfill to close it.
        return default
    return _INCREMENTAL_FETCH_BARS


# #2066 — smallest overlap close ratio that reads as an adjustment event
# rather than a late correction. Splits re-base by the split ratio (2x,
# 3x, 10x; smallest common uneven split 5:4 = 1.25x); exchange corrections
# to a finalized close are single-digit percent. 1.2 sits between the two
# with margin, and a false positive only costs one idempotent full-history
# re-fetch, so the threshold errs low.
_ADJUSTMENT_RATIO_THRESHOLD = Decimal("1.2")


def detect_adjustment_event(
    stored_closes: dict[date, Decimal],
    bars: list[OHLCVBar],
) -> Decimal | None:
    """Ratio-scale mismatch between stored and re-fetched overlap closes (#2066).

    Provider candles are back-adjusted at fetch time, so after a split every
    re-fetched bar is on the new basis while stored rows outside the fetch
    window keep the old one. Comparing the re-fetched bars against what is
    already stored for the SAME dates exposes the re-basing: returns the
    largest direction-normalised close ratio (max(r, 1/r)) at or above
    ``_ADJUSTMENT_RATIO_THRESHOLD``, or None when the overlap is consistent.

    Non-positive closes on either side are skipped — ``price_daily`` holds
    zero-close sentinels and a garbage quote must not fake a split. Dates
    absent from ``stored_closes`` (new rows) carry no signal.
    """
    worst: Decimal | None = None
    for bar in bars:
        stored = stored_closes.get(bar.price_date)
        if stored is None or stored <= 0 or bar.close <= 0:
            continue
        ratio = bar.close / stored
        normalised = max(ratio, Decimal(1) / ratio)
        if normalised >= _ADJUSTMENT_RATIO_THRESHOLD and (worst is None or normalised > worst):
            worst = normalised
    return worst


def _stored_overlap_closes(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    dates: list[date],
) -> dict[date, Decimal]:
    """Stored ``price_daily`` closes for the given dates (#2066 overlap read)."""
    if not dates:
        return {}
    rows = conn.execute(
        """
        SELECT price_date, close FROM price_daily
        WHERE instrument_id = %(instrument_id)s AND price_date = ANY(%(dates)s)
        """,
        {"instrument_id": instrument_id, "dates": dates},
    ).fetchall()
    return {row[0]: row[1] for row in rows}


def _upsert_candles(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    bars: list[OHLCVBar],
) -> int:
    """
    Upsert OHLCV bars into price_daily. Idempotent — re-running with the same
    data produces no changes (ON CONFLICT DO UPDATE with WHERE clause).
    Returns the number of rows written (insert or update).
    """
    written = 0
    for bar in bars:
        result = conn.execute(
            """
            INSERT INTO price_daily (
                instrument_id, price_date, open, high, low, close, volume
            )
            VALUES (
                %(instrument_id)s, %(price_date)s,
                %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s
            )
            ON CONFLICT (instrument_id, price_date) DO UPDATE SET
                open   = EXCLUDED.open,
                high   = EXCLUDED.high,
                low    = EXCLUDED.low,
                close  = EXCLUDED.close,
                volume = EXCLUDED.volume
            WHERE (
                price_daily.open   IS DISTINCT FROM EXCLUDED.open   OR
                price_daily.high   IS DISTINCT FROM EXCLUDED.high   OR
                price_daily.low    IS DISTINCT FROM EXCLUDED.low    OR
                price_daily.close  IS DISTINCT FROM EXCLUDED.close  OR
                price_daily.volume IS DISTINCT FROM EXCLUDED.volume
            )
            """,
            {
                "instrument_id": instrument_id,
                "price_date": bar.price_date,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            },
        )
        written += result.rowcount
    return written


def _compute_and_store_features(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
) -> int:
    """
    Compute rolling returns and 30-day realised volatility for the most recent
    price_daily row of this instrument, then write back to the same row.

    Returns 1 if the most recent row was updated, 0 if no price data exists.

    Rolling return formula: (close_today / close_n_days_ago) - 1
    Volatility: annualised standard deviation of daily log returns over 30 days.
    """
    # Fetch enough history to compute all windows (up to 1y + buffer)
    rows = conn.execute(
        """
        SELECT price_date, close
        FROM price_daily
        WHERE instrument_id = %(instrument_id)s
          AND close IS NOT NULL
        ORDER BY price_date DESC
        LIMIT 400
        """,
        {"instrument_id": instrument_id},
    ).fetchall()

    if not rows:
        return 0

    # rows are newest-first; reverse to oldest-first for computation
    prices: list[tuple[date, Decimal]] = [(r[0], r[1]) for r in reversed(rows)]
    latest_date, _ = prices[-1]

    returns = _compute_rolling_returns(prices)
    volatility = _compute_volatility_30d(prices)

    # --- TA indicators (full OHLCV needed, not just close) ---
    # Require all four price columns non-null; the schema permits partial
    # rows (close-only) which would crash float() in stochastic/ATR.
    # Include price_date so we can verify the latest complete OHLCV bar
    # matches the row we're updating — avoids writing stale TA values
    # when the newest candle has close but incomplete OHLC.
    ohlcv_rows = conn.execute(
        """
        SELECT price_date, open, high, low, close, volume
        FROM price_daily
        WHERE instrument_id = %(instrument_id)s
          AND open IS NOT NULL
          AND high IS NOT NULL
          AND low IS NOT NULL
          AND close IS NOT NULL
        ORDER BY price_date DESC
        LIMIT 400
        """,
        {"instrument_id": instrument_id},
    ).fetchall()

    _TA_COLUMNS = [
        "sma_20",
        "sma_50",
        "sma_200",
        "ema_12",
        "ema_26",
        "macd_line",
        "macd_signal",
        "macd_histogram",
        "rsi_14",
        "stoch_k",
        "stoch_d",
        "bb_upper",
        "bb_lower",
        "atr_14",
    ]
    ta_params: dict[str, Decimal | None] = {k: None for k in _TA_COLUMNS}

    if ohlcv_rows:
        # Only compute TA if the latest complete OHLCV bar matches the row
        # we're updating; otherwise the indicators would be stale-by-one-day.
        ohlcv_latest_date: date = ohlcv_rows[0][0]  # newest first
        if ohlcv_latest_date == latest_date:
            bars: list[OHLCVRow] = [
                OHLCVRow(open=r[1], high=r[2], low=r[3], close=r[4], volume=r[5]) for r in reversed(ohlcv_rows)
            ]
            ta_result = compute_indicators(bars)
            if ta_result is not None:
                for k, v in ta_result.items():
                    if k in ta_params and isinstance(v, float) and math.isfinite(v):
                        ta_params[k] = Decimal(str(round(v, 6)))

    conn.execute(
        """
        UPDATE price_daily SET
            return_1w      = %(return_1w)s,
            return_1m      = %(return_1m)s,
            return_3m      = %(return_3m)s,
            return_6m      = %(return_6m)s,
            return_1y      = %(return_1y)s,
            volatility_30d = %(volatility_30d)s,
            sma_20         = %(sma_20)s,
            sma_50         = %(sma_50)s,
            sma_200        = %(sma_200)s,
            ema_12         = %(ema_12)s,
            ema_26         = %(ema_26)s,
            macd_line      = %(macd_line)s,
            macd_signal    = %(macd_signal)s,
            macd_histogram = %(macd_histogram)s,
            rsi_14         = %(rsi_14)s,
            stoch_k        = %(stoch_k)s,
            stoch_d        = %(stoch_d)s,
            bb_upper       = %(bb_upper)s,
            bb_lower       = %(bb_lower)s,
            atr_14         = %(atr_14)s
        WHERE instrument_id = %(instrument_id)s
          AND price_date = %(price_date)s
        """,
        {
            "instrument_id": instrument_id,
            "price_date": latest_date,
            "return_1w": returns.get("return_1w"),
            "return_1m": returns.get("return_1m"),
            "return_3m": returns.get("return_3m"),
            "return_6m": returns.get("return_6m"),
            "return_1y": returns.get("return_1y"),
            "volatility_30d": volatility,
            **ta_params,
        },
    )
    return 1


def _compute_rolling_returns(
    prices: list[tuple[date, Decimal]],
) -> dict[str, Decimal | None]:
    """
    Compute rolling returns for each window against the most recent close.

    prices must be sorted oldest-first. Returns a dict of column_name → return
    value (or None if insufficient history for that window).

    Return = (close_latest / close_at_window_start) - 1
    """
    if not prices:
        return {col: None for col in _RETURN_WINDOWS}

    latest_date, latest_close = prices[-1]
    results: dict[str, Decimal | None] = {}

    for col, days in _RETURN_WINDOWS.items():
        target_date = date.fromordinal(latest_date.toordinal() - days)
        # Find the closest available price on or before target_date.
        # The break on the first date after target_date is correct only because
        # prices is strictly sorted oldest-first. That ordering is guaranteed by
        # the DB query (ORDER BY price_date DESC, reversed in Python) and by the
        # UNIQUE (instrument_id, price_date) constraint preventing duplicate dates.
        anchor: Decimal | None = None
        for price_date, close in prices[:-1]:  # exclude the latest bar itself
            if price_date <= target_date:
                anchor = close  # keep iterating to find closest to target
            else:
                break
        if anchor is not None and anchor != 0:
            results[col] = (latest_close / anchor) - Decimal("1")
        else:
            results[col] = None

    return results


def _compute_volatility_30d(prices: list[tuple[date, Decimal]]) -> Decimal | None:
    """
    Compute 30-day annualised realised volatility from daily log returns.

    Uses the most recent 31 prices (30 daily returns).
    Returns None if fewer than 5 returns are available (too few to be meaningful).
    """
    if len(prices) < 2:
        return None

    recent = prices[-31:]  # up to 31 prices → up to 30 returns
    log_returns = []
    for i in range(1, len(recent)):
        prev = recent[i - 1][1]
        curr = recent[i][1]
        if prev > 0 and curr > 0:
            log_returns.append(math.log(float(curr) / float(prev)))

    if len(log_returns) < 5:
        return None

    n = len(log_returns)
    mean = sum(log_returns) / n
    variance = sum((r - mean) ** 2 for r in log_returns) / (n - 1)
    daily_std = math.sqrt(variance)
    annualised = daily_std * math.sqrt(252)  # trading days per year

    return Decimal(str(round(annualised, 6)))


def _upsert_quote(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    quote: Quote,
    max_spread_pct: Decimal,
) -> bool:
    """
    Upsert the current quote into the quotes table.
    Computes spread_pct and sets spread_flag if spread exceeds the threshold.
    Returns True if spread_flag was set (i.e. spread is wide).
    """
    spread_pct = compute_spread_pct(quote.bid, quote.ask)
    spread_flag = spread_pct is not None and spread_pct > max_spread_pct

    conn.execute(
        """
        INSERT INTO quotes (
            instrument_id, quoted_at, bid, ask, last, spread_pct, spread_flag
        )
        VALUES (
            %(instrument_id)s, %(quoted_at)s, %(bid)s, %(ask)s,
            %(last)s, %(spread_pct)s, %(spread_flag)s
        )
        ON CONFLICT (instrument_id) DO UPDATE SET
            quoted_at   = EXCLUDED.quoted_at,
            bid         = EXCLUDED.bid,
            ask         = EXCLUDED.ask,
            last        = EXCLUDED.last,
            spread_pct  = EXCLUDED.spread_pct,
            spread_flag = EXCLUDED.spread_flag
        """,
        {
            "instrument_id": instrument_id,
            "quoted_at": quote.timestamp,
            "bid": quote.bid,
            "ask": quote.ask,
            "last": quote.last,
            "spread_pct": spread_pct,
            "spread_flag": spread_flag,
        },
    )
    return spread_flag


def compute_spread_pct(bid: Decimal, ask: Decimal) -> Decimal | None:
    """Public helper for testing: compute spread % from bid/ask."""
    mid = (bid + ask) / 2
    if mid <= 0:
        return None
    return (ask - bid) / mid * 100
