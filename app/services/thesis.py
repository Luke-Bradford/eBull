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
  - news events:          latest 10 from last 30 days, importance desc → recency desc

Claude model: claude-sonnet-4-6 for both writer and critic calls.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

import anthropic
import psycopg
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Domain literals
# ---------------------------------------------------------------------------

ThesisType = Literal["compounder", "value", "turnaround", "speculative"]
Stance = Literal["buy", "hold", "watch", "avoid"]

_VALID_THESIS_TYPES: frozenset[str] = frozenset({"compounder", "value", "turnaround", "speculative"})
_VALID_STANCES: frozenset[str] = frozenset({"buy", "hold", "watch", "avoid"})

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
_MAX_NEWS_EVENTS = 10
_NEWS_LOOKBACK_DAYS = 30

# ---------------------------------------------------------------------------
# Public result type
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


# ---------------------------------------------------------------------------
# Stale detection
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StaleInstrument:
    instrument_id: int
    symbol: str
    reason: str  # "no_thesis" | "stale" | "missing_frequency"


def find_stale_instruments(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    tier: int = 1,
) -> list[StaleInstrument]:
    """
    Return instruments whose most recent thesis is absent or older than
    their coverage.review_frequency allows.

    Stale rules:
      1. No thesis row exists → stale (reason: "no_thesis")
      2. review_frequency missing / unrecognised → stale (reason: "missing_frequency")
      3. now >= latest_thesis.created_at + interval(review_frequency) → stale (reason: "stale")
    """
    rows = conn.execute(
        """
        SELECT
            i.instrument_id,
            i.symbol,
            c.review_frequency,
            MAX(t.created_at) AS latest_thesis_at
        FROM instruments i
        JOIN coverage c ON c.instrument_id = i.instrument_id
        LEFT JOIN theses t ON t.instrument_id = i.instrument_id
        WHERE i.is_tradable = TRUE
          AND c.coverage_tier = %(tier)s
        GROUP BY i.instrument_id, i.symbol, c.review_frequency
        ORDER BY i.symbol
        """,
        {"tier": tier},
    ).fetchall()

    now = datetime.now(tz=UTC)
    stale: list[StaleInstrument] = []

    for row in rows:
        instrument_id: int = row[0]
        symbol: str = row[1]
        review_frequency: str | None = row[2]
        latest_thesis_at: datetime | None = row[3]

        if latest_thesis_at is None:
            stale.append(StaleInstrument(instrument_id=instrument_id, symbol=symbol, reason="no_thesis"))
            continue

        if review_frequency not in _REVIEW_FREQUENCY_DAYS:
            stale.append(StaleInstrument(instrument_id=instrument_id, symbol=symbol, reason="missing_frequency"))
            continue

        threshold = latest_thesis_at + timedelta(days=_REVIEW_FREQUENCY_DAYS[review_frequency])
        if now >= threshold:
            stale.append(StaleInstrument(instrument_id=instrument_id, symbol=symbol, reason="stale"))

    return stale


# ---------------------------------------------------------------------------
# Context assembly
# ---------------------------------------------------------------------------


def _assemble_context(
    conn: psycopg.Connection,  # type: ignore[type-arg]
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
            "revenue_ttm": float(r[1]) if r[1] is not None else None,
            "gross_margin": float(r[2]) if r[2] is not None else None,
            "operating_margin": float(r[3]) if r[3] is not None else None,
            "fcf": float(r[4]) if r[4] is not None else None,
            "cash": float(r[5]) if r[5] is not None else None,
            "debt": float(r[6]) if r[6] is not None else None,
            "net_debt": float(r[7]) if r[7] is not None else None,
            "eps": float(r[8]) if r[8] is not None else None,
            "book_value": float(r[9]) if r[9] is not None else None,
        }
        for r in fund_rows
    ]

    # Filing events: latest 3 (summary text only — not raw payload)
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
            "red_flag_score": float(r[3]) if r[3] is not None else None,
        }
        for r in filing_rows
    ]

    # News events: latest 10 from last 30 days, importance desc then recency desc
    cutoff = datetime.now(tz=UTC) - timedelta(days=_NEWS_LOOKBACK_DAYS)
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
            "sentiment_score": float(r[4]) if r[4] is not None else None,
            "importance_score": float(r[5]) if r[5] is not None else None,
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
        LIMIT 1
        """,
        {"id": instrument_id},
    ).fetchone()
    prior_thesis: dict[str, object] | None = None
    if prior_row is not None:
        prior_thesis = {
            "version": prior_row[0],
            "thesis_type": prior_row[1],
            "stance": prior_row[2],
            "confidence_score": float(prior_row[3]) if prior_row[3] is not None else None,
            "buy_zone_low": float(prior_row[4]) if prior_row[4] is not None else None,
            "buy_zone_high": float(prior_row[5]) if prior_row[5] is not None else None,
            "base_value": float(prior_row[6]) if prior_row[6] is not None else None,
            "bull_value": float(prior_row[7]) if prior_row[7] is not None else None,
            "bear_value": float(prior_row[8]) if prior_row[8] is not None else None,
            "break_conditions": prior_row[9],
            "memo_markdown": prior_row[10],
            "created_at": prior_row[11].isoformat() if prior_row[11] else None,
        }

    # Instrument name
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

    return {
        "instrument": instrument,
        "fundamentals": fundamentals,
        "filings": filings,
        "news": news,
        "prior_thesis": prior_thesis,
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
  "confidence_score": <float 0.0–1.0>,
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
- memo_markdown: full structured memo covering: business quality, key financials, recent news impact,
  valuation, risks, stance rationale. Min 3 paragraphs.
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
  "summary": "<short counter-thesis in 1–2 sentences>",
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
    if not hasattr(block, "text"):
        raise ValueError(f"Writer: unexpected content block type {type(block)!r}")

    raw = block.text.strip()  # type: ignore[union-attr]
    try:
        parsed: dict[str, object] = json.loads(raw)
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
    Returns an empty dict on failure — critic is best-effort.
    """
    try:
        message = client.messages.create(
            model=_MODEL,
            max_tokens=_MAX_TOKENS_CRITIC,
            system=_CRITIC_SYSTEM,
            messages=[{"role": "user", "content": _build_critic_prompt(memo_markdown, context)}],
        )
        block = message.content[0]
        if not hasattr(block, "text"):
            logger.warning("Critic: unexpected content block type %r", type(block))
            return {}

        raw = block.text.strip()  # type: ignore[union-attr]
        parsed: dict[str, object] = json.loads(raw)
        _validate_critic_output(parsed)
        return parsed
    except Exception as exc:
        logger.warning("Critic call failed (%s): thesis will be stored without critic_json", exc)
        return {}


def _validate_critic_output(data: dict[str, object]) -> None:
    required = {"summary", "key_risks", "hidden_assumptions", "evidence_gaps", "thesis_breakers", "verdict"}
    missing = required - data.keys()
    if missing:
        raise ValueError(f"Critic output missing fields: {missing}")

    verdict = data["verdict"]
    valid_verdicts = {"Strong challenge", "Moderate challenge", "Weak challenge"}
    if verdict not in valid_verdicts:
        raise ValueError(f"Critic output invalid verdict: {verdict!r}")


# ---------------------------------------------------------------------------
# DB persistence
# ---------------------------------------------------------------------------


def _next_thesis_version(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(thesis_version), 0) FROM theses WHERE instrument_id = %(id)s",
        {"id": instrument_id},
    ).fetchone()
    return (row[0] if row else 0) + 1


def _insert_thesis(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    version: int,
    writer: dict[str, object],
    critic: dict[str, object] | None,
) -> None:
    break_conditions = writer.get("break_conditions") or []

    def _to_float(val: object) -> float | None:
        if val is None:
            return None
        try:
            return float(val)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None

    conn.execute(
        """
        INSERT INTO theses (
            instrument_id, thesis_version,
            thesis_type, confidence_score, stance,
            buy_zone_low, buy_zone_high,
            base_value, bull_value, bear_value,
            break_conditions_json, memo_markdown, critic_json
        )
        VALUES (
            %(instrument_id)s, %(thesis_version)s,
            %(thesis_type)s, %(confidence_score)s, %(stance)s,
            %(buy_zone_low)s, %(buy_zone_high)s,
            %(base_value)s, %(bull_value)s, %(bear_value)s,
            %(break_conditions_json)s, %(memo_markdown)s, %(critic_json)s
        )
        """,
        {
            "instrument_id": instrument_id,
            "thesis_version": version,
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
    )


def _update_last_reviewed(
    conn: psycopg.Connection,  # type: ignore[type-arg]
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
    conn: psycopg.Connection,  # type: ignore[type-arg]
    client: anthropic.Anthropic,
) -> ThesisResult:
    """
    Generate and persist a new versioned thesis for an instrument.

    Steps:
      1. Assemble context from DB (capped research inputs).
      2. Call Claude writer → structured memo.
      3. Call Claude critic → counter-thesis (best-effort; never blocks the insert).
      4. Insert new thesis row with incremented thesis_version.
      5. Update coverage.last_reviewed_at.

    Returns ThesisResult. Raises on writer failure (critic failure is logged only).
    """
    context = _assemble_context(conn, instrument_id)

    # Writer call — raises on failure
    writer_output = _call_writer(client, context)

    # Critic call — best-effort
    critic_output = _call_critic(client, str(writer_output.get("memo_markdown", "")), context)

    version = _next_thesis_version(conn, instrument_id)

    with conn.transaction():
        _insert_thesis(conn, instrument_id, version, writer_output, critic_output or None)
        _update_last_reviewed(conn, instrument_id)

    logger.info(
        "Thesis generated: instrument_id=%d version=%d stance=%s confidence=%.2f",
        instrument_id,
        version,
        writer_output["stance"],
        float(writer_output["confidence_score"]),  # type: ignore[arg-type]
    )

    def _to_float(val: object) -> float | None:
        if val is None:
            return None
        try:
            return float(val)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None

    return ThesisResult(
        instrument_id=instrument_id,
        thesis_version=version,
        thesis_type=writer_output["thesis_type"],  # type: ignore[arg-type]
        confidence_score=float(writer_output["confidence_score"]),  # type: ignore[arg-type]
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
