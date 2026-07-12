"""
Thesis engine service.

Responsibilities:
  - Assemble a compact research context from filings, fundamentals, news,
    and the prior thesis for a given instrument.
  - Call the configured LLM writer to produce a structured investment memo.
  - Call the configured LLM critic to produce a counter-thesis / challenge.
  - Insert a new versioned row into the `theses` table.
  - Record every generation attempt in `thesis_runs` (all trigger paths).
  - Update coverage.last_reviewed_at on success.
  - Identify stale instruments (no thesis, or thesis older than review_frequency).

Context caps (v2 — settled-decisions "Thesis prompt budget", amended #1987):
  - prior thesis:         latest 1
  - filing events:        latest 3
  - fundamentals:         latest snapshot + up to 4 prior snapshots
  - earnings events:      latest 4 quarters (confirmed only)
  - analyst estimates:    latest 1 snapshot
  - news events:          latest 10 from last 30 days, importance desc → recency desc
  - risk metrics (#1632): instrument_risk_metrics_current scalars, statused
  - price anchor (#1987): latest price_daily close (native ccy) + 52w range + returns
  - valuation (#1987):    instrument_valuation row when present; statused absence
  - analytics (#1987):    latest scores.analytics_json, shaped compact, scored_at-stamped
  - ta_state (#1987):     latest price_daily indicators + derived regime signals

LLM provider: resolved from `runtime_config` via
`app.services.llm_client.make_llm_clients` (#1919 — local-first default;
Anthropic by configuration). Writer and critic may run DIFFERENT models
(#1995 split knobs) but share provider/base URL; both get one retry on
schema/parse failure, with `finish_reason` recorded so truncation is
distinguishable from malformed output.

Versioning contract:
  thesis_version is computed atomically inside the INSERT via a subquery:
    COALESCE(MAX(thesis_version), 0) + 1
  This eliminates TOCTOU races when two workers process the same instrument
  concurrently. The UNIQUE(instrument_id, thesis_version) constraint on the
  theses table is the final guard.
"""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal

import psycopg
from psycopg import sql as psql
from psycopg.types.json import Jsonb

# Safe at module scope: instrument_analytics has no app-module imports at
# top level (its insider_transactions read is function-lazy), so this does
# NOT re-enter the risk_metrics -> scheduler -> refresh_cascade cycle that
# forces the lazy import inside _assemble_context.
from app.services.instrument_analytics import SCHEMA_VERSION as _IAR_SCHEMA_VERSION
from app.services.llm_client import LLMClient, LLMClientPair, LLMCompletion
from app.services.technical_analysis import derive_trend_signals

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Domain literals
# ---------------------------------------------------------------------------

ThesisType = Literal["compounder", "value", "turnaround", "speculative"]
Stance = Literal["buy", "hold", "watch", "avoid"]
StaleReason = Literal[
    "no_thesis",
    "stale",
    "missing_frequency",
    "event_new_10k",
    "event_new_10q",
    "event_new_8k",
]

_VALID_THESIS_TYPES: frozenset[str] = frozenset({"compounder", "value", "turnaround", "speculative"})
_VALID_STANCES: frozenset[str] = frozenset({"buy", "hold", "watch", "avoid"})
_VALID_VERDICTS: frozenset[str] = frozenset({"Strong challenge", "Moderate challenge", "Weak challenge"})

_REVIEW_FREQUENCY_DAYS: dict[str, int] = {
    "daily": 1,
    "weekly": 7,
    "monthly": 30,
}

# ---------------------------------------------------------------------------
# LLM call budgets + prompt version
# ---------------------------------------------------------------------------

_MAX_TOKENS_WRITER = 2048
# 1024 length-failed live on IEP (2026-07-10, thesis stored without critic_json);
# the #1987 context growth makes recurrence more likely. Local-first default
# makes the cost delta negligible.
_MAX_TOKENS_CRITIC = 2048

# Stamped onto every stored thesis row (theses.prompt_version). Bump
# whenever _WRITER_SYSTEM / _CRITIC_SYSTEM or the _assemble_context shape
# changes — memos from different prompt versions are not comparable.
# v3 (#2007): _WRITER_SYSTEM gains the availability-claim mirror rule
# (never disclaim a block the context marks available, then cite its figures).
_PROMPT_VERSION = "v3"

# thesis_runs.trigger — matches the table CHECK in sql/218.
RunTrigger = Literal["manual", "cascade", "scheduled"]

# ---------------------------------------------------------------------------
# Context caps
# ---------------------------------------------------------------------------

_MAX_PRIOR_THESES = 1
_MAX_FILING_EVENTS = 3
_MAX_FUNDAMENTALS_SNAPSHOTS = 5  # latest + 4 prior
_MAX_NEWS_EVENTS = 10
_NEWS_LOOKBACK_DAYS = 30
# 52w range window for the price anchor (#1987), measured back from the
# LATEST close's price_date (not today) so a stale price series yields an
# honest historical range rather than a shrunken one.
_PRICE_ANCHOR_LOOKBACK_DAYS = 365

# ---------------------------------------------------------------------------
# Public result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ThesisResult:
    instrument_id: int
    thesis_version: int
    thesis_type: ThesisType
    confidence_score: float
    stance: Stance
    buy_zone_low: float | None
    buy_zone_high: float | None
    base_value: float | None
    bull_value: float | None
    bear_value: float | None
    break_conditions: list[str]
    memo_markdown: str
    critic_json: dict[str, object] | None


@dataclass(frozen=True)
class StaleInstrument:
    instrument_id: int
    symbol: str
    reason: StaleReason


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    """Return the current UTC datetime. Extracted for testability."""
    return datetime.now(tz=UTC)


def _to_float(val: object) -> float | None:
    """
    Convert a value to float, returning None on failure.

    Used to safely convert AI-sourced numeric fields from the writer
    output dict before persisting to the DB and returning in ThesisResult.
    Both sites must use the same conversion so the DB row and the returned
    struct are always consistent.

    Non-finite floats (NaN / ±inf) map to None: an LLM can emit ``"nan"`` /
    ``"inf"`` for a target field, and NaN silently defeats the #2007 ordering
    guard (every ``>`` comparison against NaN is False) while persisting as a
    non-numeric garbage target. Treat them as missing, consistently, at the one
    coercion chokepoint both the guard and the INSERT share.
    """
    if val is None:
        return None
    try:
        result = float(val)  # type: ignore[arg-type]
    except TypeError, ValueError:
        return None
    return result if math.isfinite(result) else None


# ---------------------------------------------------------------------------
# Stale detection
# ---------------------------------------------------------------------------


def find_stale_instruments(
    conn: psycopg.Connection[Any],
    tier: int | None = 1,
    *,
    instrument_ids: Sequence[int] | None = None,
) -> list[StaleInstrument]:
    """
    Return instruments whose most recent thesis is absent, older than
    their coverage.review_frequency allows, or superseded by a new
    10-K / 10-Q / 8-K filing (#273 event-driven trigger).

    Stale rules (evaluated in order per instrument):
      1. No thesis row exists → stale (reason: "no_thesis")
      2. review_frequency missing / unrecognised → stale (reason: "missing_frequency")
      3. filing_events row newer than latest thesis, filing_type in
         ('10-K', '10-K/A', '10-Q', '10-Q/A', '8-K', '8-K/A') → stale
         (reason: "event_new_{10k,10q,8k}")
      4. now >= latest_thesis.created_at + interval(review_frequency) → stale (reason: "stale")

    Every returned instrument must have ``coverage.filings_status =
    'analysable'`` (#268 Chunk J gate). Non-analysable instruments are
    silently excluded — thesis generation on them is wasted Claude
    spend.

    Parameters
    ----------
    tier
        Coverage tier filter. Pass ``None`` to bypass tier filtering
        entirely — typically used by the cascade (#276) in
        combination with ``instrument_ids`` to scope to a specific
        subset across any tier.
    instrument_ids
        When provided, restrict the scan to these instruments. Used by
        the cascade to check "did the CIKs that just had filings need
        a thesis refresh". Does not bypass the filings_status gate.
    """
    params: dict[str, Any] = {}
    where_clauses = [
        "i.is_tradable = TRUE",
        "c.filings_status = 'analysable'",
    ]
    if tier is not None:
        where_clauses.append("c.coverage_tier = %(tier)s")
        params["tier"] = tier
    if instrument_ids is not None:
        where_clauses.append("i.instrument_id = ANY(%(ids)s)")
        params["ids"] = list(instrument_ids)

    # Build WHERE via structural psql.SQL composition (each clause is
    # a literal fragment from the list above — no user input channel).
    # Avoids ad-hoc string concatenation so a future caller that adds
    # a user-derived clause cannot regress into injection.
    where_block = psql.SQL(" AND ").join(
        psql.SQL(clause)  # pyright: ignore[reportArgumentType]
        for clause in where_clauses
    )

    # Single LATERAL subquery drives both the timestamp AND the form
    # type from the SAME row so they can never disagree on same-second
    # ties. MAX-aggregate + correlated-subquery would tiebreak
    # independently and could report "new 10-K" while the actual
    # newest row is an 8-K (audit-trail lie). LATERAL scope + explicit
    # ORDER BY created_at DESC, filing_event_id DESC resolves ties
    # deterministically.
    query = (
        psql.SQL(
            """
        SELECT
            i.instrument_id,
            i.symbol,
            c.review_frequency,
            MAX(t.created_at)                        AS latest_thesis_at,
            le.created_at                            AS latest_event_created_at,
            le.filing_type                           AS latest_event_filing_type
        FROM instruments i
        JOIN coverage c ON c.instrument_id = i.instrument_id
        LEFT JOIN theses t ON t.instrument_id = i.instrument_id
        LEFT JOIN LATERAL (
            SELECT fe.created_at, fe.filing_type
            FROM filing_events fe
            WHERE fe.instrument_id = i.instrument_id
              AND fe.filing_type IN (
                  '10-K','10-K/A','10-Q','10-Q/A','8-K','8-K/A'
              )
            ORDER BY fe.created_at DESC, fe.filing_event_id DESC
            LIMIT 1
        ) le ON TRUE
        WHERE """
        )
        + where_block
        + psql.SQL(
            """
        GROUP BY i.instrument_id, i.symbol, c.review_frequency,
                 le.created_at, le.filing_type
        ORDER BY i.symbol
        """
        )
    )
    rows = conn.execute(query, params).fetchall()

    now = _utcnow()
    stale: list[StaleInstrument] = []

    for row in rows:
        instrument_id: int = row[0]
        symbol: str = row[1]
        review_frequency: str | None = row[2]
        latest_thesis_at: datetime | None = row[3]
        latest_event_created_at: datetime | None = row[4]
        latest_event_filing_type: str | None = row[5]

        if latest_thesis_at is None:
            stale.append(StaleInstrument(instrument_id=instrument_id, symbol=symbol, reason="no_thesis"))
            continue

        if review_frequency not in _REVIEW_FREQUENCY_DAYS:
            stale.append(StaleInstrument(instrument_id=instrument_id, symbol=symbol, reason="missing_frequency"))
            continue

        # Event-driven refresh: any qualifying filing INGESTED
        # (``filing_events.created_at``) after the thesis was generated
        # triggers a fresh run regardless of the time-based cadence
        # window. Timestamp comparison (not date) so same-day
        # post-thesis filings still fire. Using created_at instead of
        # filing_date also catches backfilled filings whose reported
        # filing_date predates the thesis — the thesis couldn't have
        # seen them, so the refresh is warranted.
        if (
            latest_event_created_at is not None
            and latest_event_filing_type is not None
            and latest_event_created_at > latest_thesis_at
        ):
            reason = _event_reason_for_form(latest_event_filing_type)
            stale.append(StaleInstrument(instrument_id=instrument_id, symbol=symbol, reason=reason))
            continue

        threshold = latest_thesis_at + timedelta(days=_REVIEW_FREQUENCY_DAYS[review_frequency])
        if now >= threshold:
            stale.append(StaleInstrument(instrument_id=instrument_id, symbol=symbol, reason="stale"))

    return stale


def _event_reason_for_form(form_type: str) -> StaleReason:
    """Map a filing_type to its corresponding event_* StaleReason."""
    base = form_type.split("/", 1)[0]  # strip /A suffix
    if base == "10-K":
        return "event_new_10k"
    if base == "10-Q":
        return "event_new_10q"
    return "event_new_8k"  # 8-K, 8-K/A


# ---------------------------------------------------------------------------
# Context assembly
# ---------------------------------------------------------------------------


_RISK_BASIS_NOTE = (
    "All returns/ratios are fractions (0.10 = 10%, not percent). "
    "Drawdown, var_5 and worst_day are signed losses (negative is a loss). "
    "Basis is price-return (no dividend reinvestment; total-return is future "
    "work), so do not over-read CAGR for high-yield names. A non-'ok' status "
    "means the metric is NOT a precise number: treat insufficient_history / "
    "partial_window as provisional, and benchmark_missing beta/excess as "
    "absent, not zero. Cite a risk figure as {window_key, as_of_date, "
    "metric_version}."
)


def _shape_risk_metrics(
    rows: Sequence[Sequence[object]],
    metric_version: str,
) -> dict[str, object] | None:
    """Shape the `instrument_risk_metrics_current` rows into the context block.

    Pure: no DB, no I/O — the row order/column order is fixed by the SELECT in
    :func:`_assemble_context`. Returns ``None`` when there are no rows (the
    instrument's metrics were never computed — distinct from a thin-history
    instrument, which HAS rows carrying flagged statuses that pass through
    verbatim). NULL scalars stay ``None`` via :func:`_to_float` — never a
    fabricated zero. ``as_of_date`` rides each window (no constraint enforces
    one shared snapshot date across windows).
    """
    if not rows:
        return None
    return {
        "metric_version": metric_version,
        "basis_note": _RISK_BASIS_NOTE,
        "windows": [
            {
                "window_key": r[0],
                "as_of_date": str(r[1]) if r[1] is not None else None,
                "benchmark_symbol": r[2],
                "cagr": _to_float(r[3]),
                "excess_cagr_vs_spy": _to_float(r[4]),
                "vol_annualized": _to_float(r[5]),
                "beta": _to_float(r[6]),
                "beta_r2": _to_float(r[7]),
                "calmar": _to_float(r[8]),
                "max_drawdown": _to_float(r[9]),
                "current_drawdown": _to_float(r[10]),
                "var_5": _to_float(r[11]),
                "worst_day": _to_float(r[12]),
                "cagr_status": r[13],
                "excess_cagr_status": r[14],
                "vol_status": r[15],
                "beta_status": r[16],
                "drawdown_status": r[17],
                "distribution_status": r[18],
                "calmar_status": r[19],
            }
            for r in rows
        ],
    }


# ---------------------------------------------------------------------------
# #1987 context blocks — pure row-shaping (spec:
# docs/specs/thesis/2026-07-10-thesis-context-enrichment.md). All four follow
# the #1632 evidence discipline: statuses verbatim, as-of stamps, missing data
# stays missing (None, never a fabricated zero).
# ---------------------------------------------------------------------------

# Column order of the shared price_daily latest-row SELECT (13 cols), consumed
# by _shape_price_anchor (0-6) and _shape_ta_state (0, 7-12):
#   0 close, 1 price_date, 2 return_1w, 3 return_1m, 4 return_3m,
#   5 return_6m, 6 return_1y, 7 sma_50, 8 sma_200, 9 rsi_14,
#   10 macd_histogram, 11 atr_14, 12 volatility_30d


def _shape_price_anchor(
    price_row: tuple[object, ...] | None,
    agg_row: tuple[object, ...] | None,
    currency: object,
) -> dict[str, object] | None:
    """Block A: latest native-currency close + 52w range + persisted returns.

    agg_row = (high_52w, low_52w, window_days) over the trailing
    _PRICE_ANCHOR_LOOKBACK_DAYS from the latest price_date. No price history
    -> None (never a fabricated anchor); NULL returns stay None.
    """
    if price_row is None:
        return None
    high_52w = low_52w = None
    window_days = 0
    if agg_row is not None:
        high_52w = _to_float(agg_row[0])
        low_52w = _to_float(agg_row[1])
        window_count = _to_float(agg_row[2])
        window_days = int(window_count) if window_count is not None else 0
    return {
        "close": _to_float(price_row[0]),
        "price_date": str(price_row[1]) if price_row[1] is not None else None,
        "currency": currency,
        "high_52w": high_52w,
        "low_52w": low_52w,
        "window_days_52w": window_days,
        "return_1w": _to_float(price_row[2]),
        "return_1m": _to_float(price_row[3]),
        "return_3m": _to_float(price_row[4]),
        "return_6m": _to_float(price_row[5]),
        "return_1y": _to_float(price_row[6]),
    }


# Column order of the instrument_valuation SELECT (18 cols):
#   0 current_price, 1 price_as_of, 2 market_cap_live, 3 enterprise_value,
#   4 pe_ratio, 5 pb_ratio, 6 p_fcf_ratio, 7 fcf_yield, 8 ev_revenue,
#   9 ev_ebitda, 10 debt_equity_ratio, 11 net_margin, 12 gross_margin,
#   13 operating_margin, 14 roa, 15 roe, 16 dividend_yield, 17 is_complete_ttm
_VALUATION_FIELDS: tuple[str, ...] = (
    "current_price",
    "price_as_of",
    "market_cap_live",
    "enterprise_value",
    "pe_ratio",
    "pb_ratio",
    "p_fcf_ratio",
    "fcf_yield",
    "ev_revenue",
    "ev_ebitda",
    "debt_equity_ratio",
    "net_margin",
    "gross_margin",
    "operating_margin",
    "roa",
    "roe",
    "dividend_yield",
    "is_complete_ttm",
)


def _shape_valuation(row: tuple[object, ...] | None) -> dict[str, object]:
    """Block B: instrument_valuation row, statused absence.

    The view is quotes-gated (sql/201 `priced` CTE reads FROM quotes; #1857
    class) so a missing row is STRUCTURAL for most of the universe — statused,
    not an error. #1664 dual-class NULLs pass through as None (honest
    suppression).
    """
    if row is None:
        return {"available": False, "reason": "no_live_quote"}
    shaped: dict[str, object] = {"available": True}
    for idx, field_name in enumerate(_VALUATION_FIELDS):
        val = row[idx]
        if field_name == "price_as_of":
            shaped[field_name] = val.isoformat() if isinstance(val, (datetime, date)) else None
        elif field_name == "is_complete_ttm":
            shaped[field_name] = bool(val) if val is not None else None
        else:
            shaped[field_name] = _to_float(val)
    return shaped


def _signal_entry_ok(entry: object) -> bool:
    """A positioning entry is forwardable iff it is a dict whose `signal` is
    None or a number in [0, 1] — grounded by the 2026-07-10 full-population
    shape scan (3,906/3,906 conforming). Anything else fails closed."""
    if not isinstance(entry, dict):
        return False
    sig = entry.get("signal")
    if sig is None:
        return True
    # bool is an int subclass — {"signal": true} is malformed, not 1.0.
    if isinstance(sig, bool):
        return False
    return isinstance(sig, (int, float)) and 0.0 <= float(sig) <= 1.0


_MALFORMED: dict[str, object] = {"reason": "malformed"}


def _shape_analytics_evidence(
    analytics: object,
    scored_at: datetime | None,
    model_version: object,
) -> dict[str, object] | None:
    """Block C: latest scores.analytics_json (#1823 iar_v1) shaped compact.

    None analytics (scores row exists, analytics_json NULL) -> None (absent
    evidence). Present-but-non-dict -> {"reason": "malformed"} — the
    absent-vs-malformed distinction is deliberate (spec §Block C). Fail-closed
    per sub-block: unexpected types are dropped to {"reason": "malformed"},
    never forwarded. Drops piotroski.components + peer families' `absolute`
    (token noise); positioning passes verbatim (`asof` optional upstream —
    818/3,906 undated insider signals on dev).
    """
    if analytics is None:
        return None
    if not isinstance(analytics, dict):
        return dict(_MALFORMED)
    schema = analytics.get("schema")
    if schema != _IAR_SCHEMA_VERSION:
        # A future iar_v2 must not be silently compacted under v1
        # assumptions — surface it as unsupported (absent evidence to the
        # writer) until this shaper is deliberately migrated (bot review
        # NITPICK, PR #1999).
        return {"reason": "unsupported_schema", "schema": schema}

    out: dict[str, object] = {
        "schema": analytics.get("schema"),
        "as_of": scored_at.isoformat() if scored_at is not None else None,
        "model_version": model_version,
    }

    piotroski = analytics.get("piotroski")
    if isinstance(piotroski, dict):
        out["piotroski"] = {
            k: piotroski.get(k) for k in ("score", "band", "components_available", "suppressed", "reason")
        }
    else:
        out["piotroski"] = dict(_MALFORMED)

    altman = analytics.get("altman_z")
    out["altman_z"] = dict(altman) if isinstance(altman, dict) else dict(_MALFORMED)

    positioning = analytics.get("positioning")
    if isinstance(positioning, dict):
        out["positioning"] = {
            key: (dict(entry) if _signal_entry_ok(entry) else dict(_MALFORMED)) for key, entry in positioning.items()
        }
    else:
        out["positioning"] = dict(_MALFORMED)

    peer = analytics.get("peer_grade")
    if isinstance(peer, dict):
        # `families` may be legitimately EMPTY ({} — absolute_only rows
        # persisted outside a run cohort) but must be a dict; a non-dict
        # families or family entry is corruption, not missing evidence —
        # mark it malformed rather than silently degrading to {} (Codex
        # ckpt-2, 2026-07-10).
        families = peer.get("families")
        if isinstance(families, dict):
            shaped_families: dict[str, object] = {
                fam: (
                    {"hybrid": grades.get("hybrid"), "percentile": grades.get("percentile")}
                    if isinstance(grades, dict)
                    else dict(_MALFORMED)
                )
                for fam, grades in families.items()
            }
            out["peer_grade"] = {
                "peer_key": peer.get("peer_key"),
                "peer_n": peer.get("peer_n"),
                "basis": peer.get("basis"),
                "families": shaped_families,
            }
        else:
            out["peer_grade"] = dict(_MALFORMED)
    else:
        out["peer_grade"] = dict(_MALFORMED)

    return out


def _shape_ta_state(price_row: tuple[object, ...] | None) -> dict[str, object] | None:
    """Block D: latest persisted TA indicators + derived-at-read signals.

    The trend signals (`price_vs_sma200`, `sma_50_200_regime`) come from
    ``technical_analysis.derive_trend_signals`` — the single source (#1989:
    read-derive from stored SMAs, no extra price_daily columns). The context
    keys are stable — the thesis prompt and eval fixtures depend on them.
    """
    if price_row is None:
        return None
    close = _to_float(price_row[0])
    sma_50 = _to_float(price_row[7])
    sma_200 = _to_float(price_row[8])

    return {
        "sma_50": sma_50,
        "sma_200": sma_200,
        "rsi_14": _to_float(price_row[9]),
        "macd_histogram": _to_float(price_row[10]),
        "atr_14": _to_float(price_row[11]),
        "volatility_30d": _to_float(price_row[12]),
        **derive_trend_signals(close, sma_50, sma_200),
    }


def _assemble_context(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> dict[str, object]:
    """
    Pull capped research inputs from the DB for a single instrument.
    Returns a plain dict used to build the writer prompt.
    """
    # Fundamentals: latest + up to 4 prior (5 total)
    fund_rows = conn.execute(
        """
        SELECT as_of_date, revenue_ttm, gross_margin, operating_margin,
               fcf, cash, debt, net_debt, eps, book_value
        FROM fundamentals_snapshot
        WHERE instrument_id = %(id)s
        ORDER BY as_of_date DESC
        LIMIT %(limit)s
        """,
        {"id": instrument_id, "limit": _MAX_FUNDAMENTALS_SNAPSHOTS},
    ).fetchall()
    fundamentals = [
        {
            "as_of_date": str(r[0]),
            "revenue_ttm": _to_float(r[1]),
            "gross_margin": _to_float(r[2]),
            "operating_margin": _to_float(r[3]),
            "fcf": _to_float(r[4]),
            "cash": _to_float(r[5]),
            "debt": _to_float(r[6]),
            "net_debt": _to_float(r[7]),
            "eps": _to_float(r[8]),
            "book_value": _to_float(r[9]),
        }
        for r in fund_rows
    ]

    # Filing events: latest N (summary text only — not raw payload)
    filing_rows = conn.execute(
        """
        SELECT filing_date, filing_type, extracted_summary, red_flag_score
        FROM filing_events
        WHERE instrument_id = %(id)s
          AND extracted_summary IS NOT NULL
        ORDER BY filing_date DESC
        LIMIT %(limit)s
        """,
        {"id": instrument_id, "limit": _MAX_FILING_EVENTS},
    ).fetchall()
    filings = [
        {
            "filing_date": str(r[0]),
            "filing_type": r[1],
            "summary": r[2],
            "red_flag_score": _to_float(r[3]),
        }
        for r in filing_rows
    ]

    # News events: latest N from last 30 days, importance desc then recency desc
    cutoff = _utcnow() - timedelta(days=_NEWS_LOOKBACK_DAYS)
    news_rows = conn.execute(
        """
        SELECT event_time, source, headline, category, sentiment_score, importance_score
        FROM news_events
        WHERE instrument_id = %(id)s
          AND event_time >= %(cutoff)s
        ORDER BY importance_score DESC NULLS LAST, event_time DESC
        LIMIT %(limit)s
        """,
        {"id": instrument_id, "cutoff": cutoff, "limit": _MAX_NEWS_EVENTS},
    ).fetchall()
    news = [
        {
            "event_time": r[0].isoformat() if r[0] else None,
            "source": r[1],
            "headline": r[2],
            "category": r[3],
            "sentiment_score": _to_float(r[4]),
            "importance_score": _to_float(r[5]),
        }
        for r in news_rows
    ]

    # Prior thesis: latest 1
    prior_row = conn.execute(
        """
        SELECT thesis_version, thesis_type, stance, confidence_score,
               buy_zone_low, buy_zone_high, base_value, bull_value, bear_value,
               break_conditions_json, memo_markdown, created_at
        FROM theses
        WHERE instrument_id = %(id)s
        ORDER BY thesis_version DESC
        LIMIT %(limit)s
        """,
        {"id": instrument_id, "limit": _MAX_PRIOR_THESES},
    ).fetchone()
    prior_thesis: dict[str, object] | None = None
    if prior_row is not None:
        prior_thesis = {
            "version": prior_row[0],
            "thesis_type": prior_row[1],
            "stance": prior_row[2],
            "confidence_score": _to_float(prior_row[3]),
            "buy_zone_low": _to_float(prior_row[4]),
            "buy_zone_high": _to_float(prior_row[5]),
            "base_value": _to_float(prior_row[6]),
            "bull_value": _to_float(prior_row[7]),
            "bear_value": _to_float(prior_row[8]),
            "break_conditions": prior_row[9],
            "memo_markdown": prior_row[10],
            "created_at": prior_row[11].isoformat() if prior_row[11] else None,
        }

    # Instrument metadata
    inst_row = conn.execute(
        "SELECT symbol, company_name, sector, industry, country, currency"
        " FROM instruments WHERE instrument_id = %(id)s",
        {"id": instrument_id},
    ).fetchone()
    instrument: dict[str, object] = {}
    if inst_row is not None:
        instrument = {
            "symbol": inst_row[0],
            "company_name": inst_row[1],
            "sector": inst_row[2],
            "industry": inst_row[3],
            "country": inst_row[4],
            "currency": inst_row[5],
        }

    # Realized-risk metrics (#1632): persisted, versioned, quality-flagged
    # risk_v1 scalars per window as structured evidence for the writer + critic.
    # Lazy import — risk_metrics pulls in app.workers.scheduler, which transitively
    # imports this module (refresh_cascade); a module-level import would be a cycle.
    # By call time thesis is fully initialized, so the function-level import is safe
    # and keeps the version/window constants single-sourced (no magic-string dup).
    from app.services.risk_metrics import RISK_METRICS_VERSION, WINDOW_KEYS

    # ONE statement (LEFT JOIN for the benchmark symbol) so context assembly does
    # not add a second snapshot-spanning read. No rows → None (never computed,
    # like analyst_estimates); a thin-history instrument DOES have rows carrying
    # flagged statuses, which pass through verbatim — honest-status discipline,
    # never a fabricated zero (NULL scalars stay None via _to_float).
    risk_rows = conn.execute(
        """
        SELECT c.window_key, c.as_of_date, b.symbol AS benchmark_symbol,
               c.cagr, c.excess_cagr_vs_spy, c.vol_annualized,
               c.beta, c.beta_r2, c.calmar,
               c.max_drawdown, c.current_drawdown, c.var_5, c.worst_day,
               c.cagr_status, c.excess_cagr_status, c.vol_status, c.beta_status,
               c.drawdown_status, c.distribution_status, c.calmar_status
        FROM instrument_risk_metrics_current c
        LEFT JOIN instruments b ON b.instrument_id = c.benchmark_instrument_id
        WHERE c.instrument_id = %(id)s
          AND c.metric_version = %(ver)s
        ORDER BY array_position(%(worder)s, c.window_key)
        """,
        {"id": instrument_id, "ver": RISK_METRICS_VERSION, "worder": list(WINDOW_KEYS)},
    ).fetchall()
    risk_metrics = _shape_risk_metrics(risk_rows, RISK_METRICS_VERSION)

    # #1987 Block A + D: one price_daily latest-row read serves both the
    # price anchor and the TA state. Native currency close — matches the
    # writer's targets-in-instrument-currency contract (#1845/#1906).
    # Quotes are deliberately NOT read here (85 rows on dev; a second
    # price source/currency would undermine the anchor contract).
    price_row = conn.execute(
        """
        SELECT close, price_date, return_1w, return_1m, return_3m,
               return_6m, return_1y, sma_50, sma_200, rsi_14,
               macd_histogram, atr_14, volatility_30d
        FROM price_daily
        WHERE instrument_id = %(id)s
          AND close IS NOT NULL
        ORDER BY price_date DESC
        LIMIT 1
        """,
        {"id": instrument_id},
    ).fetchone()
    agg_row: tuple[object, ...] | None = None
    if price_row is not None and price_row[1] is not None:
        # 52w window measured back from the LATEST price_date (not today)
        # so a stale series yields an honest historical range.
        agg_row = conn.execute(
            """
            SELECT MAX(COALESCE(high, close)) AS high_52w,
                   MIN(COALESCE(low, close)) AS low_52w,
                   COUNT(*) AS window_days
            FROM price_daily
            WHERE instrument_id = %(id)s
              AND close IS NOT NULL
              AND price_date >= %(cutoff)s
            """,
            {
                "id": instrument_id,
                "cutoff": price_row[1] - timedelta(days=_PRICE_ANCHOR_LOOKBACK_DAYS),
            },
        ).fetchone()
    price_anchor = _shape_price_anchor(price_row, agg_row, instrument.get("currency"))
    ta_state = _shape_ta_state(price_row)

    # #1987 Block B: quotes-gated view (#1857 class) — absence is
    # structural for most of the universe, statused not errored.
    val_row = conn.execute(
        """
        SELECT current_price, price_as_of, market_cap_live, enterprise_value,
               pe_ratio, pb_ratio, p_fcf_ratio, fcf_yield, ev_revenue,
               ev_ebitda, debt_equity_ratio, net_margin, gross_margin,
               operating_margin, roa, roe, dividend_yield, is_complete_ttm
        FROM instrument_valuation
        WHERE instrument_id = %(id)s
        """,
        {"id": instrument_id},
    ).fetchone()
    valuation = _shape_valuation(val_row)

    # #1987 Block C: latest persisted IAR evidence (#1823). Refreshes only
    # on compute_rankings — staleness is allowed and stamped (scored_at),
    # mirroring risk_v1's as_of_date discipline.
    score_row = conn.execute(
        """
        SELECT analytics_json, scored_at, model_version
        FROM scores
        WHERE instrument_id = %(id)s
        ORDER BY scored_at DESC
        LIMIT 1
        """,
        {"id": instrument_id},
    ).fetchone()
    analytics_evidence: dict[str, object] | None = None
    if score_row is not None:
        analytics_evidence = _shape_analytics_evidence(score_row[0], score_row[1], score_row[2])

    # #539: earnings_events + analyst_estimates retired with FMP. The
    # writer prompt's optional contextual fields fall back to None;
    # the writer system prompt already tolerates absent enrichment.
    return {
        "instrument": instrument,
        "fundamentals": fundamentals,
        "filings": filings,
        "news": news,
        "prior_thesis": prior_thesis,
        "risk_metrics": risk_metrics,
        "price_anchor": price_anchor,
        "valuation": valuation,
        "analytics_evidence": analytics_evidence,
        "ta_state": ta_state,
        "earnings_history": [],
        "analyst_estimates": None,
    }


# ---------------------------------------------------------------------------
# Writer prompt
# ---------------------------------------------------------------------------

_WRITER_SYSTEM = """\
You are a long-horizon equity analyst producing structured investment memos.

You will be given a research context including:
- company metadata
- recent fundamentals (up to 5 snapshots)
- recent filing summaries (up to 3)
- recent news events (up to 10, last 30 days)
- prior thesis if one exists
- realized-risk metrics (`risk_metrics`): persisted, versioned, quality-flagged
  scalars per window (1y/3y/full) — CAGR, annualized vol, beta + R² vs the
  benchmark, Calmar, max/current drawdown, var_5. All are FRACTIONS (0.10 = 10%,
  not percent); drawdown / var_5 / worst_day are SIGNED losses (negative). Basis
  is PRICE-RETURN (no dividends) — do not over-read CAGR for high-yield names.
  May be null when no metrics are computed.
- `price_anchor`: latest persisted daily close in the instrument's NATIVE
  currency (the same currency your per-share targets must use), its as-of
  date (`price_date` — may lag; treat a stale anchor as approximate), the
  trailing-52-week high/low with the observed window size
  (`window_days_52w` — a small window means a short history; treat the
  range as partial), and persisted simple returns (1w/1m/3m/6m/1y,
  fractions). Null when no price history exists.
- `valuation`: fundamentals-derived multiples (P/E, P/B, P/FCF, FCF yield,
  EV/revenue, EV/EBITDA, margins, ROA/ROE, dividend yield) with their own
  `price_as_of`. `available: false` with a reason means the surface is
  structurally absent for this instrument — not a data error. Null fields
  inside an available row are honest gaps (e.g. dual-class suppression) —
  never invent multiples.
- `analytics_evidence`: quality + positioning evidence — Piotroski F
  (score/band), Altman Z (z/band), positioning signals (insider net 90d,
  institutional 13F QoQ, short interest) and a sector peer grade — stamped
  `as_of` (the scoring-run date; may lag — treat stale stamps as
  approximate). `signal` fields are 0-1 normalized (higher = more
  supportive of a long). A positioning entry without `asof` is undated —
  cite it only as approximate. Any sub-block with `reason: "malformed"` is
  absent evidence.
- `ta_state`: latest persisted technical indicators (SMA 50/200, RSI-14,
  MACD histogram, ATR-14, 30d volatility) plus `price_vs_sma200` and
  `sma_50_200_regime`. The regime is the CURRENT 50d-vs-200d SMA relation
  ("golden" = 50d above 200d), NOT a recent crossover event. Null
  indicators mean insufficient history.

Produce a JSON object with EXACTLY these fields:

{
  "thesis_type": "<compounder|value|turnaround|speculative>",
  "confidence_score": <float 0.0-1.0>,
  "stance": "<buy|hold|watch|avoid>",
  "buy_zone_low": <float or null>,
  "buy_zone_high": <float or null>,
  "base_value": <float or null>,
  "bull_value": <float or null>,
  "bear_value": <float or null>,
  "break_conditions": ["<condition 1>", "<condition 2>", ...],
  "memo_markdown": "<full investment memo in markdown>"
}

Rules:
- thesis_type must be one of: compounder, value, turnaround, speculative
- stance must be one of: buy, hold, watch, avoid
- confidence_score in [0.0, 1.0] — higher means more conviction
- buy_zone_low/high: only populate when stance is "buy"; null otherwise
- base/bull/bear_value: per-share price targets in the instrument currency; null if insufficient data
- break_conditions: list of concrete, specific events that would invalidate the thesis
- memo_markdown: full structured memo covering: business quality, key financials, recent news
  impact, valuation, risks, stance rationale. Min 3 paragraphs.
- Use `risk_metrics` to ground the risk section: deep max drawdown, high beta,
  low Calmar, or fat-tail var_5 are downside context that can support or weaken
  a long. Respect the status flags — do NOT cite a non-`ok` metric as a precise
  number (a `benchmark_missing` beta is absent, not 0; a `partial_window` CAGR
  is provisional). When you cite a risk figure, name its {window_key,
  as_of_date, metric_version} so the claim stays reproducible.
- Sanity-check buy_zone_low/high and base/bull/bear_value against
  `price_anchor.close` and the 52-week range. State the implied
  upside/downside from the current price to base_value explicitly in the
  memo. A "buy" stance whose buy zone lies wholly above the current price,
  or targets far outside the 52-week range, must be corrected or explicitly
  justified in the memo.
- Do NOT mechanically anchor targets to the current price — the anchor
  grounds your numbers; valuation judgement produces them.
- When `price_anchor` is null: leave buy_zone_low/high null regardless of
  stance (an entry band is meaningless without a market price), and emit
  base/bull/bear_value only if fundamentals give a defensible per-share
  basis.
- Data-availability language MUST mirror the block status fields verbatim
  (#1632 evidence discipline). Never state a block is unavailable, missing, or
  absent when its `available`/status field marks it present. When a block IS
  unavailable or its status is non-`ok`, do not cite figures drawn from it —
  omit the number or name the gap explicitly. Cited figures and availability
  claims must both agree with the block statuses, never with each other.
- Separate facts from judgement. Be explicit about what must go right.
- Respond with ONLY valid JSON. No explanation outside the JSON object.
"""


def _build_writer_prompt(context: dict[str, object]) -> str:
    return json.dumps(context, indent=2, default=str)


# ---------------------------------------------------------------------------
# Critic prompt
# ---------------------------------------------------------------------------

_CRITIC_SYSTEM = """\
You are a contrarian equity analyst. Your job is to attack the current long thesis
and surface the strongest failure case.

You will be given the investment memo and the research context it was built on.

Produce a JSON object with EXACTLY these fields:

{
  "summary": "<short counter-thesis in 1-2 sentences>",
  "key_risks": ["<risk 1>", "<risk 2>", ...],
  "hidden_assumptions": ["<assumption 1>", "<assumption 2>", ...],
  "evidence_gaps": ["<gap 1>", "<gap 2>", ...],
  "thesis_breakers": ["<event 1>", "<event 2>", ...],
  "verdict": "<Strong|Moderate|Weak> challenge"
}

Rules:
- Fight confirmation bias. Do not restate the bull case.
- Prefer the strongest realistic objection over generic cautions.
- Be concrete — cite specific metrics, dates, or events where possible.
- Mine `risk_metrics` for realized-risk objections the memo glossed over — e.g.
  a deep peak-to-trough drawdown, a high beta, a weak Calmar, or fat-tail var_5
  that the bull case ignores. These are FRACTIONS, signed losses are negative,
  basis is price-return. Respect status flags (a non-`ok` metric is not precise;
  `benchmark_missing` beta is absent, not 0) and cite {window_key, as_of_date,
  metric_version}.
- Attack target-vs-price inconsistency: a buy zone away from the current
  `price_anchor.close`, an implied upside to base_value that is implausible
  against the 52-week range, or targets that ignore the anchor entirely.
- Flag adverse `analytics_evidence` (weak Piotroski or Altman band,
  distressed positioning signals, poor peer grade) and adverse `ta_state`
  (death regime, price below the 200d SMA) that the memo glossed over.
  Respect the as-of stamps — stale evidence is approximate, and a
  `reason: "malformed"` sub-block is absent, not adverse.
- verdict must be exactly one of: "Strong challenge", "Moderate challenge", "Weak challenge"
- Respond with ONLY valid JSON. No explanation outside the JSON object.
"""


def _build_critic_prompt(memo_markdown: str, context: dict[str, object]) -> str:
    payload = {
        "memo_to_challenge": memo_markdown,
        "research_context": context,
    }
    return json.dumps(payload, indent=2, default=str)


# ---------------------------------------------------------------------------
# LLM calls
# ---------------------------------------------------------------------------


def _complete_json_validated(
    client: LLMClient,
    *,
    label: str,
    system: str,
    user: str,
    max_tokens: int,
    validate: Callable[[dict[str, object]], None],
) -> tuple[dict[str, object], LLMCompletion]:
    """One LLM completion, JSON-parsed and schema-validated.

    Every ValueError raised here carries the completion's finish_reason
    so a truncated response (``length``) is distinguishable from a
    malformed one (``stop``) in logs and thesis_runs.error.
    """
    completion = client.complete(system=system, user=user, max_tokens=max_tokens)
    try:
        parsed: dict[str, object] = json.loads(completion.text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label}: unparseable JSON (finish_reason={completion.finish_reason}): {exc}") from exc

    try:
        validate(parsed)
    except ValueError as exc:
        raise ValueError(f"{exc} (finish_reason={completion.finish_reason})") from exc
    return parsed, completion


def _call_with_one_retry(
    client: LLMClient,
    *,
    label: str,
    system: str,
    user: str,
    max_tokens: int,
    validate: Callable[[dict[str, object]], None],
) -> tuple[dict[str, object], LLMCompletion]:
    """Retry ONCE on schema/parse ValueError (spec §1) — local models
    intermittently emit near-miss JSON; a single re-roll recovers most
    of them without masking a systematically broken model."""
    try:
        return _complete_json_validated(
            client, label=label, system=system, user=user, max_tokens=max_tokens, validate=validate
        )
    except ValueError as exc:
        logger.warning("%s attempt 1 failed (%s); retrying once", label, exc)
        return _complete_json_validated(
            client, label=label, system=system, user=user, max_tokens=max_tokens, validate=validate
        )


def _call_writer(client: LLMClient, context: dict[str, object]) -> tuple[dict[str, object], LLMCompletion]:
    """
    Call the LLM writer and parse the structured thesis JSON.
    Raises ValueError on unparseable or schema-invalid response
    (after one retry).
    """
    return _call_with_one_retry(
        client,
        label="Writer",
        system=_WRITER_SYSTEM,
        user=_build_writer_prompt(context),
        max_tokens=_MAX_TOKENS_WRITER,
        validate=_validate_writer_output,
    )


def _validate_writer_output(data: dict[str, object]) -> None:
    required = {
        "thesis_type",
        "confidence_score",
        "stance",
        "buy_zone_low",
        "buy_zone_high",
        "base_value",
        "bull_value",
        "bear_value",
        "break_conditions",
        "memo_markdown",
    }
    missing = required - data.keys()
    if missing:
        raise ValueError(f"Writer output missing fields: {missing}")

    thesis_type = data["thesis_type"]
    if thesis_type not in _VALID_THESIS_TYPES:
        raise ValueError(f"Writer output invalid thesis_type: {thesis_type!r}")

    stance = data["stance"]
    if stance not in _VALID_STANCES:
        raise ValueError(f"Writer output invalid stance: {stance!r}")

    try:
        score = float(data["confidence_score"])  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Writer output invalid confidence_score: {data['confidence_score']!r}") from exc
    if not (0.0 <= score <= 1.0):
        raise ValueError(f"Writer output confidence_score out of range: {score}")

    if not isinstance(data.get("break_conditions"), list):
        raise ValueError("Writer output break_conditions must be a list")

    memo = data.get("memo_markdown")
    if not isinstance(memo, str) or not memo.strip():
        raise ValueError("Writer output memo_markdown must be a non-empty string")

    # Valuation-band coherence (#2007): the stored band must satisfy
    # bear <= base <= bull, and the buy zone low <= high. Local writers
    # intermittently emit mechanical copies (AMSC v1: bear=52w-low,
    # bull=52w-high, base=book/share, so base < bear). Coerce through the
    # SAME _to_float used at INSERT so we validate the values that actually
    # persist; a null/garbage field coerces to None and drops out of its
    # comparison. Raising ValueError rides the existing retry-once machinery.
    bear = _to_float(data.get("bear_value"))
    base = _to_float(data.get("base_value"))
    bull = _to_float(data.get("bull_value"))
    if bear is not None and base is not None and bear > base:
        raise ValueError(f"Writer output incoherent targets: bear_value {bear} > base_value {base}")
    if base is not None and bull is not None and base > bull:
        raise ValueError(f"Writer output incoherent targets: base_value {base} > bull_value {bull}")
    if bear is not None and bull is not None and bear > bull:
        raise ValueError(f"Writer output incoherent targets: bear_value {bear} > bull_value {bull}")
    zone_low = _to_float(data.get("buy_zone_low"))
    zone_high = _to_float(data.get("buy_zone_high"))
    if zone_low is not None and zone_high is not None and zone_low > zone_high:
        raise ValueError(f"Writer output inverted buy zone: buy_zone_low {zone_low} > buy_zone_high {zone_high}")


def _call_critic(client: LLMClient, memo_markdown: str, context: dict[str, object]) -> dict[str, object]:
    """
    Call the LLM critic and parse the structured counter-thesis JSON.
    Returns an empty dict on any failure (after one schema/parse retry) —
    critic is best-effort and must never block the thesis insert.

    The as-reported critic model is stamped into the returned dict
    (``model`` key) — with split knobs (#1995) the critic may differ from
    the writer, and ``theses.model`` records the writer only.
    """
    try:
        parsed, completion = _call_with_one_retry(
            client,
            label="Critic",
            system=_CRITIC_SYSTEM,
            user=_build_critic_prompt(memo_markdown, context),
            max_tokens=_MAX_TOKENS_CRITIC,
            validate=_validate_critic_output,
        )
        parsed["model"] = completion.model
        return parsed
    except Exception:
        logger.warning("Critic call failed; thesis will be stored without critic_json", exc_info=True)
        return {}


def _validate_critic_output(data: dict[str, object]) -> None:
    required = {"summary", "key_risks", "hidden_assumptions", "evidence_gaps", "thesis_breakers", "verdict"}
    missing = required - data.keys()
    if missing:
        raise ValueError(f"Critic output missing fields: {missing}")

    verdict = data["verdict"]
    if verdict not in _VALID_VERDICTS:
        raise ValueError(f"Critic output invalid verdict: {verdict!r}")


# ---------------------------------------------------------------------------
# DB persistence
# ---------------------------------------------------------------------------


def _insert_thesis_atomic(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    writer: dict[str, object],
    critic: dict[str, object] | None,
    *,
    model: str,
    provider: str,
) -> tuple[int, int]:
    """
    Insert a new thesis row and return (thesis_id, thesis_version).

    thesis_version is computed atomically inside the INSERT via a subquery
    (COALESCE(MAX(thesis_version), 0) + 1) so two concurrent inserts for the
    same instrument cannot produce the same version number. The
    UNIQUE(instrument_id, thesis_version) constraint is the final guard.

    ``model`` is the model string AS REPORTED by the provider response
    (not the configured knob) and ``provider`` the resolved provider name —
    stored with ``_PROMPT_VERSION`` so every memo is attributable (#1919).

    Must be called inside an open transaction.
    """
    break_conditions = writer.get("break_conditions") or []

    row = conn.execute(
        """
        INSERT INTO theses (
            instrument_id, thesis_version,
            thesis_type, confidence_score, stance,
            buy_zone_low, buy_zone_high,
            base_value, bull_value, bear_value,
            break_conditions_json, memo_markdown, critic_json,
            model, provider, prompt_version
        )
        VALUES (
            %(instrument_id)s,
            (SELECT COALESCE(MAX(thesis_version), 0) + 1
             FROM theses WHERE instrument_id = %(instrument_id)s),
            %(thesis_type)s, %(confidence_score)s, %(stance)s,
            %(buy_zone_low)s, %(buy_zone_high)s,
            %(base_value)s, %(bull_value)s, %(bear_value)s,
            %(break_conditions_json)s, %(memo_markdown)s, %(critic_json)s,
            %(model)s, %(provider)s, %(prompt_version)s
        )
        RETURNING thesis_id, thesis_version
        """,
        {
            "instrument_id": instrument_id,
            "thesis_type": writer["thesis_type"],
            "confidence_score": float(writer["confidence_score"]),  # type: ignore[arg-type]
            "stance": writer["stance"],
            "buy_zone_low": _to_float(writer.get("buy_zone_low")),
            "buy_zone_high": _to_float(writer.get("buy_zone_high")),
            "base_value": _to_float(writer.get("base_value")),
            "bull_value": _to_float(writer.get("bull_value")),
            "bear_value": _to_float(writer.get("bear_value")),
            "break_conditions_json": Jsonb(break_conditions),
            "memo_markdown": writer["memo_markdown"],
            "critic_json": Jsonb(critic) if critic else None,
            "model": model,
            "provider": provider,
            "prompt_version": _PROMPT_VERSION,
        },
    ).fetchone()

    if row is None:
        raise RuntimeError(f"INSERT INTO theses did not RETURN a row for instrument_id={instrument_id}")
    return int(row[0]), int(row[1])


def _update_last_reviewed(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> None:
    conn.execute(
        "UPDATE coverage SET last_reviewed_at = NOW() WHERE instrument_id = %(id)s",
        {"id": instrument_id},
    )


# ---------------------------------------------------------------------------
# thesis_runs — one row per generation attempt (#1919, all trigger paths)
# ---------------------------------------------------------------------------


def _insert_thesis_run(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    trigger: RunTrigger,
    *,
    provider: str,
    model: str,
    critic_model: str,
) -> int:
    """Insert a 'running' thesis_runs row and return its run_id.

    ``model`` (writer) and ``critic_model`` are the CONFIGURED models
    (the run may fail before any provider response exists); the stored
    thesis row carries the writer model as reported by the response, and
    ``critic_json.model`` the critic's. Recording ``critic_model`` here
    is what keeps critic provenance auditable when the best-effort critic
    fails and no ``critic_json`` is stored (#1995).
    """
    row = conn.execute(
        """
        INSERT INTO thesis_runs (instrument_id, trigger, provider, model, critic_model)
        VALUES (%(instrument_id)s, %(trigger)s, %(provider)s, %(model)s, %(critic_model)s)
        RETURNING run_id
        """,
        {
            "instrument_id": instrument_id,
            "trigger": trigger,
            "provider": provider,
            "model": model,
            "critic_model": critic_model,
        },
    ).fetchone()
    if row is None:
        raise RuntimeError(f"INSERT INTO thesis_runs did not RETURN a row for instrument_id={instrument_id}")
    return int(row[0])


def _finish_thesis_run_ok(
    conn: psycopg.Connection[Any],
    run_id: int,
    thesis_id: int,
) -> None:
    """Mark a run ok, linking the inserted thesis row.

    Must be called inside the same transaction as the thesis INSERT so
    the run row can never claim success for a rolled-back thesis.
    """
    result = conn.execute(
        """
        UPDATE thesis_runs
        SET status = 'ok', finished_at = NOW(), thesis_id = %(thesis_id)s
        WHERE run_id = %(run_id)s
        """,
        {"thesis_id": thesis_id, "run_id": run_id},
    )
    # prevention-log: single-row UPDATE silent no-op on missing row.
    if result.rowcount == 0:
        raise RuntimeError(f"thesis_runs run_id={run_id} vanished before ok-finish")


def _record_thesis_run_failure(
    conn: psycopg.Connection[Any],
    run_id: int,
    exc: Exception,
) -> None:
    """Best-effort failure record — must never mask the original exception.

    Called from the except path of generate_thesis, OUTSIDE any open
    transaction (the pre-LLM commit closed it), so the UPDATE + commit
    here open and close their own short implicit transaction.
    """
    error_text = f"{type(exc).__name__}: {exc}"[:2000]
    try:
        result = conn.execute(
            """
            UPDATE thesis_runs
            SET status = 'failed', finished_at = NOW(), error = %(error)s
            WHERE run_id = %(run_id)s
            """,
            {"error": error_text, "run_id": run_id},
        )
        if result.rowcount == 0:
            logger.error("thesis_runs run_id=%d vanished while recording failure", run_id)
        conn.commit()
    except Exception:
        logger.exception("failed to record thesis_runs failure for run_id=%d", run_id)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_thesis(
    instrument_id: int,
    conn: psycopg.Connection[Any],
    clients: LLMClientPair,
    *,
    trigger: RunTrigger,
) -> ThesisResult:
    """
    Generate and persist a new versioned thesis for an instrument.

    Steps:
      1. Assemble context from DB (capped research inputs).
      2. Insert a 'running' thesis_runs row (in-flight indicator) and
         commit — this same commit closes the context-read transaction.
      3. Call the LLM writer → structured memo. Raises on failure (after
         one retry), recording the failure on the run row first.
      4. Call the LLM critic → counter-thesis (best-effort; failure is
         logged only).
      5. Open a transaction, INSERT a new thesis row with an
         atomically-computed thesis_version (+ model/provider/
         prompt_version), update coverage.last_reviewed_at, mark the run
         row ok, commit.

    Returns ThesisResult. LLM calls are made outside any DB transaction
    to avoid holding a connection open during network I/O.

    The explicit ``conn.commit()`` after ``_assemble_context`` is
    load-bearing (#293): on a non-autocommit connection the context
    SELECTs open an implicit transaction that would otherwise stay open
    through both LLM calls (seconds on cloud, minutes on a local 14B).
    Holding a DB tx across HTTP is the anti-pattern called out in
    CLAUDE.md Architecture invariants; the commit closes the read tx so
    the connection is ``idle`` (not ``idle in transaction``) while the
    LLM runs. It also makes the 'running' run row visible to readers.

    **Caller contract:** do NOT wrap this call in ``with conn.transaction():``.
    psycopg3 forbids explicit ``commit()`` inside an outer transaction
    block; this function commits mid-flow. Callers managing their own
    transaction must either split the call boundary around it or open a
    dedicated connection.
    """
    context = _assemble_context(conn, instrument_id)
    run_id = _insert_thesis_run(
        conn,
        instrument_id,
        trigger,
        provider=clients.writer.provider_name,
        model=clients.writer.model,
        critic_model=clients.critic.model,
    )
    # Close the implicit read tx opened by _assemble_context SELECTs
    # (and publish the 'running' run row) BEFORE the LLM calls below.
    # Without this, the connection stays ``idle in transaction`` for the
    # duration of the LLM round-trips.
    conn.commit()

    # LLM calls — outside any DB transaction; these can take seconds
    # (cloud) to minutes (local 14B).
    try:
        writer_output, writer_completion = _call_writer(clients.writer, context)
        critic_output = _call_critic(clients.critic, str(writer_output.get("memo_markdown", "")), context)
    except Exception as exc:
        _record_thesis_run_failure(conn, run_id, exc)
        raise

    # Validated by _validate_writer_output; cast once and reuse.
    confidence = float(writer_output["confidence_score"])  # type: ignore[arg-type]

    # Codex ckpt-2 HIGH: a failure INSIDE this write transaction (e.g. a
    # UniqueViolation when a concurrent generation raced the versioning
    # subquery — the UNIQUE(instrument_id, thesis_version) final guard)
    # must not strand the run row at 'running' forever. The transaction
    # CM rolls the writes back; record the failure in its own short tx,
    # then re-raise.
    try:
        with conn.transaction():
            # critic_output is {} on failure — treat empty dict as no critic data
            thesis_id, version = _insert_thesis_atomic(
                conn,
                instrument_id,
                writer_output,
                critic_output if critic_output else None,
                model=writer_completion.model,
                provider=clients.writer.provider_name,
            )
            _update_last_reviewed(conn, instrument_id)
            _finish_thesis_run_ok(conn, run_id, thesis_id)
    except Exception as exc:
        _record_thesis_run_failure(conn, run_id, exc)
        raise

    logger.info(
        "Thesis generated: instrument_id=%d version=%d stance=%s confidence=%.2f",
        instrument_id,
        version,
        writer_output["stance"],
        confidence,
    )

    return ThesisResult(
        instrument_id=instrument_id,
        thesis_version=version,
        thesis_type=writer_output["thesis_type"],  # type: ignore[arg-type]
        confidence_score=confidence,
        stance=writer_output["stance"],  # type: ignore[arg-type]
        buy_zone_low=_to_float(writer_output.get("buy_zone_low")),
        buy_zone_high=_to_float(writer_output.get("buy_zone_high")),
        base_value=_to_float(writer_output.get("base_value")),
        bull_value=_to_float(writer_output.get("bull_value")),
        bear_value=_to_float(writer_output.get("bear_value")),
        break_conditions=list(writer_output.get("break_conditions", [])),  # type: ignore[arg-type]
        memo_markdown=str(writer_output["memo_markdown"]),
        critic_json=critic_output if critic_output else None,
    )
