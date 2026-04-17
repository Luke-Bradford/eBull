"""
Thesis engine service.

Responsibilities:
  - Assemble a compact research context from filings, fundamentals, news,
    and the prior thesis for a given instrument.
  - Call Claude Sonnet (writer) to produce a structured investment memo.
  - Call Claude Sonnet (critic) to produce a counter-thesis / challenge.
  - Insert a new versioned row into the `theses` table.
  - Update coverage.last_reviewed_at on success.
  - Identify stale instruments (no thesis, or thesis older than review_frequency).

Context caps (v1 hard limits):
  - prior thesis:         latest 1
  - filing events:        latest 3
  - fundamentals:         latest snapshot + up to 4 prior snapshots
  - earnings events:      latest 4 quarters (confirmed only)
  - analyst estimates:    latest 1 snapshot
  - news events:          latest 10 from last 30 days, importance desc → recency desc

Claude model: claude-sonnet-4-6 for both writer and critic calls.

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
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import anthropic
import psycopg
from psycopg import sql as psql
from psycopg.types.json import Jsonb

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
# Claude model
# ---------------------------------------------------------------------------

_MODEL = "claude-sonnet-4-6"
_MAX_TOKENS_WRITER = 2048
_MAX_TOKENS_CRITIC = 1024

# ---------------------------------------------------------------------------
# Context caps
# ---------------------------------------------------------------------------

_MAX_PRIOR_THESES = 1
_MAX_FILING_EVENTS = 3
_MAX_FUNDAMENTALS_SNAPSHOTS = 5  # latest + 4 prior
_MAX_EARNINGS_EVENTS = 4  # 1 year of quarterly history (confirmed only)
_MAX_NEWS_EVENTS = 10
_NEWS_LOOKBACK_DAYS = 30

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
    """
    if val is None:
        return None
    try:
        return float(val)  # type: ignore[arg-type]
    except TypeError, ValueError:
        return None


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

    # Earnings history and analyst estimates from enrichment tables.
    # Each query gets its own savepoint so a missing analyst_estimates
    # table doesn't discard successfully-fetched earnings history.
    earnings_history: list[dict[str, object]] = []
    try:
        with conn.transaction():
            earnings_rows = conn.execute(
                """
                SELECT fiscal_date_ending, reporting_date,
                       eps_estimate, eps_actual, revenue_estimate, revenue_actual,
                       surprise_pct
                FROM earnings_events
                WHERE instrument_id = %(id)s
                  AND eps_actual IS NOT NULL
                ORDER BY fiscal_date_ending DESC
                LIMIT %(limit)s
                """,
                {"id": instrument_id, "limit": _MAX_EARNINGS_EVENTS},
            ).fetchall()
            earnings_history = [
                {
                    "fiscal_date": str(r[0]),
                    "eps_estimate": _to_float(r[2]),
                    "eps_actual": _to_float(r[3]),
                    "revenue_actual": _to_float(r[5]),
                    "surprise_pct": _to_float(r[6]),
                }
                for r in earnings_rows
            ]
    except psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn:
        pass  # pre-migration: degrade gracefully

    analyst_estimates: dict[str, object] | None = None
    try:
        with conn.transaction():
            estimates_row = conn.execute(
                """
                SELECT consensus_eps_fq, analyst_count, buy_count, hold_count,
                       sell_count, price_target_mean, price_target_high, price_target_low
                FROM analyst_estimates
                WHERE instrument_id = %(id)s
                ORDER BY as_of_date DESC
                LIMIT 1
                """,
                {"id": instrument_id},
            ).fetchone()
            if estimates_row is not None:
                analyst_estimates = {
                    "consensus_eps": _to_float(estimates_row[0]),
                    "analyst_count": estimates_row[1],
                    "buy_count": estimates_row[2],
                    "hold_count": estimates_row[3],
                    "sell_count": estimates_row[4],
                    "price_target_mean": _to_float(estimates_row[5]),
                    "price_target_high": _to_float(estimates_row[6]),
                    "price_target_low": _to_float(estimates_row[7]),
                }
    except psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn:
        pass  # pre-migration: degrade gracefully

    return {
        "instrument": instrument,
        "fundamentals": fundamentals,
        "filings": filings,
        "news": news,
        "prior_thesis": prior_thesis,
        "earnings_history": earnings_history,
        "analyst_estimates": analyst_estimates,
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
# Claude calls
# ---------------------------------------------------------------------------


def _call_writer(client: anthropic.Anthropic, context: dict[str, object]) -> dict[str, object]:
    """
    Call the Claude writer and parse the structured thesis JSON.
    Raises ValueError on unparseable or schema-invalid response.
    """
    message = client.messages.create(
        model=_MODEL,
        max_tokens=_MAX_TOKENS_WRITER,
        system=_WRITER_SYSTEM,
        messages=[{"role": "user", "content": _build_writer_prompt(context)}],
    )
    block = message.content[0]
    text: str | None = getattr(block, "text", None)
    if text is None:
        raise ValueError(f"Writer: unexpected content block type {type(block)!r}")

    try:
        parsed: dict[str, object] = json.loads(text.strip())
    except json.JSONDecodeError as exc:
        raise ValueError(f"Writer: unparseable JSON: {exc}") from exc

    _validate_writer_output(parsed)
    return parsed


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


def _call_critic(client: anthropic.Anthropic, memo_markdown: str, context: dict[str, object]) -> dict[str, object]:
    """
    Call the Claude critic and parse the structured counter-thesis JSON.
    Returns an empty dict on any failure — critic is best-effort and must
    never block the thesis insert.
    """
    try:
        message = client.messages.create(
            model=_MODEL,
            max_tokens=_MAX_TOKENS_CRITIC,
            system=_CRITIC_SYSTEM,
            messages=[{"role": "user", "content": _build_critic_prompt(memo_markdown, context)}],
        )
        block = message.content[0]
        text: str | None = getattr(block, "text", None)
        if text is None:
            logger.warning("Critic: unexpected content block type %r, storing without critic_json", type(block))
            return {}

        parsed: dict[str, object] = json.loads(text.strip())
        _validate_critic_output(parsed)
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
) -> int:
    """
    Insert a new thesis row and return the assigned thesis_version.

    thesis_version is computed atomically inside the INSERT via a subquery
    (COALESCE(MAX(thesis_version), 0) + 1) so two concurrent inserts for the
    same instrument cannot produce the same version number. The
    UNIQUE(instrument_id, thesis_version) constraint is the final guard.

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
            break_conditions_json, memo_markdown, critic_json
        )
        VALUES (
            %(instrument_id)s,
            (SELECT COALESCE(MAX(thesis_version), 0) + 1
             FROM theses WHERE instrument_id = %(instrument_id)s),
            %(thesis_type)s, %(confidence_score)s, %(stance)s,
            %(buy_zone_low)s, %(buy_zone_high)s,
            %(base_value)s, %(bull_value)s, %(bear_value)s,
            %(break_conditions_json)s, %(memo_markdown)s, %(critic_json)s
        )
        RETURNING thesis_version
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
        },
    ).fetchone()

    if row is None:
        raise RuntimeError(f"INSERT INTO theses did not RETURN a row for instrument_id={instrument_id}")
    return int(row[0])


def _update_last_reviewed(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> None:
    conn.execute(
        "UPDATE coverage SET last_reviewed_at = NOW() WHERE instrument_id = %(id)s",
        {"id": instrument_id},
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_thesis(
    instrument_id: int,
    conn: psycopg.Connection[Any],
    client: anthropic.Anthropic,
) -> ThesisResult:
    """
    Generate and persist a new versioned thesis for an instrument.

    Steps:
      1. Assemble context from DB (capped research inputs).
      2. Call Claude writer → structured memo. Raises on failure.
      3. Call Claude critic → counter-thesis (best-effort; failure is logged only).
      4. Open a transaction, INSERT a new thesis row with an atomically-computed
         thesis_version, update coverage.last_reviewed_at, commit.

    Returns ThesisResult. Claude calls are made outside any DB transaction
    to avoid holding a connection open during network I/O.

    The explicit ``conn.commit()`` after ``_assemble_context`` is
    load-bearing: on a non-autocommit connection the context SELECTs
    open an implicit transaction that would otherwise stay open through
    both Claude calls (2-5s each, sometimes 10s+). Holding a DB tx
    across HTTP is the anti-pattern called out in CLAUDE.md Architecture
    invariants; the commit closes the read tx so the connection is
    ``idle`` (not ``idle in transaction``) while Claude runs.

    **Caller contract:** do NOT wrap this call in ``with conn.transaction():``.
    psycopg3 forbids explicit ``commit()`` inside an outer transaction
    block; this function commits mid-flow. Callers managing their own
    transaction must either split the call boundary around it or open a
    dedicated connection.
    """
    context = _assemble_context(conn, instrument_id)
    # Close the implicit read tx opened by _assemble_context SELECTs
    # BEFORE the Claude calls below. Without this, the connection stays
    # ``idle in transaction`` for the duration of the Claude round-trips.
    conn.commit()

    # Claude calls — outside any DB transaction; these can take seconds
    writer_output = _call_writer(client, context)
    critic_output = _call_critic(client, str(writer_output.get("memo_markdown", "")), context)

    # Validated by _validate_writer_output; cast once and reuse.
    confidence = float(writer_output["confidence_score"])  # type: ignore[arg-type]

    with conn.transaction():
        # critic_output is {} on failure — treat empty dict as no critic data
        version = _insert_thesis_atomic(conn, instrument_id, writer_output, critic_output if critic_output else None)
        _update_last_reviewed(conn, instrument_id)

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
