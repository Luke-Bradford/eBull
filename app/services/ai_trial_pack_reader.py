"""AI-discretionary-v1 shortlist + pack, DB half (#3471 slice 1b-iii).

Spec: ``docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md`` §3 steps 1-2, §3.1,
§3.2 and obligations O1-O3. The pure half is ``app/services/ai_trial_pack``; this module reads
the rows at ``as_of`` and hands them to it.

Read-only. Nothing here writes, calls the model or touches the broker: the intraday fetch is
an injected callable, so the job (slice 2) owns provider construction and credentials.

Knowledge time (O2), per source:

- ``price_daily``: bars dated ≤ the last completed NYSE session at ``as_of``, read through the
  quarantine-masked loader (``price_masked_bars``).
- ``quotes``: one current row per instrument; ``quoted_at ≤ as_of`` is ``is_eligible``'s check.
- ``scores``: the latest ``_DEFAULT_MODEL_VERSION`` run with ``scored_at ≤ as_of``. ``scores`` has
  no run id; every row of one run shares ``scored_at``, so ``(model_version, scored_at)`` IS the
  run key and the pack records it.
- crowd: the latest ``complete`` snapshot that started after that session's close and finished
  by ``as_of``.
- filings: ``filing_events.created_at`` (our ingestion time) ≤ ``as_of``; ``filing_date`` dates the
  30-day window.
- news: ``news_events.created_at`` ≤ ``as_of``; ``event_time`` dates the window.
- intraday: fetched after ``as_of``; only bars that CLOSED by ``as_of`` are kept, and the fetch
  time is recorded.

⚠ ``instrument_valuation`` is a live view with no as-of. The market cap only decides the
§3.1 small-cap slice, and the pack records the figure it used next to each name.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Final, Literal
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row

from app.providers.market_data import IntradayBar
from app.services.ai_trial_pack import (
    INDICATOR_BARS,
    PROMPT_BARS,
    Disclosure,
    Shortlist,
    ShortlistCandidate,
    build_bar_series,
    canonical_sha256,
    indicators,
    is_eligible,
    select_disclosures,
    select_shortlist,
)
from app.services.market_calendar import latest_completed_us_session, us_market_status
from app.services.price_masked_bars import MASKED_REASON, load_masked_bars
from app.services.scoring import _DEFAULT_MODEL_VERSION, _apply_market_cap_basis
from app.services.xbrl_derived_stats import MarketCapResolution, resolve_market_cap_basis

logger = logging.getLogger(__name__)

Conn = psycopg.Connection[Any]

_NY: Final = ZoneInfo("America/New_York")

#: §3 step 1 — frozen gate term.
SCORES_MAX_AGE: Final = timedelta(days=3)

#: §3.2 intraday — frozen terms. 400 FourHours bars reach ~3 months (1000 reach ~8,
#: ``etoro-api.md`` "WE HAVE INTRADAY HISTORY"), so a 30-day window fits in one request with
#: room; a response that fills the request and still starts inside the window is truncated.
INTRADAY_INTERVAL: Final = "FourHours"
INTRADAY_BAR_LENGTH: Final = timedelta(hours=4)
INTRADAY_WINDOW: Final = timedelta(days=30)
INTRADAY_REQUEST_COUNT: Final = 400

#: §3.2 disclosures: 8-K and Form 4. Amendments are their own rows (O3).
FILING_FORMS: Final = ("8-K", "8-K/A", "4", "4/A")

#: Ranking families carried into the pack (``scores`` columns).
SCORE_FAMILIES: Final = ("quality", "value", "turnaround", "momentum", "sentiment", "confidence")

Step1Refusal = Literal["scores_run_missing", "scores_run_stale", "price_daily_stale", "crowd_snapshot_missing"]
IntradayIncomplete = Literal[
    "intraday_fetch_failed",
    "intraday_empty",
    "intraday_truncated",
    "intraday_non_finite",
    "intraday_duplicate_bar",
    "intraday_too_few_bars",
]


# ---------------------------------------------------------------------------
# NYSE session arithmetic (market_calendar has status + latest-completed only)
# ---------------------------------------------------------------------------
def session_close_utc(session: date) -> datetime:
    """The official close of a NYSE session: 16:00 ET, or 13:00 ET on a half day."""
    status = us_market_status(session)
    if status == "closed":
        raise ValueError(f"{session} is not a NYSE session")
    close = time(13, 0) if status == "half_day" else time(16, 0)
    return datetime.combine(session, close, tzinfo=_NY).astimezone(UTC)


def next_us_session(after: date) -> date:
    candidate = after + timedelta(days=1)
    while us_market_status(candidate) == "closed":
        candidate += timedelta(days=1)
    return candidate


def us_sessions_between(first: date, last: date) -> int:
    """NYSE sessions with ``first ≤ date ≤ last``."""
    count, day = 0, first
    while day <= last:
        if us_market_status(day) != "closed":
            count += 1
        day += timedelta(days=1)
    return count


# ---------------------------------------------------------------------------
# §3 step 1 — gates
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ScoresRun:
    model_version: str
    scored_at: datetime


@dataclass(frozen=True)
class Step1:
    """The step-1 reads. ``refusal`` set → the run is refused and nothing else is read."""

    as_of: datetime
    #: The last completed NYSE session at ``as_of``.
    last_session: date
    #: §3 "session identity": the first session after ``last_session``.
    session_date: date
    scores_run: ScoresRun | None
    crowd_snapshot_id: int | None
    refusal: Step1Refusal | None


def read_scores_run(conn: Conn, *, as_of: datetime, model_version: str = _DEFAULT_MODEL_VERSION) -> ScoresRun | None:
    row = conn.execute(
        "SELECT max(scored_at) FROM scores WHERE model_version = %(mv)s AND scored_at <= %(as_of)s",
        {"mv": model_version, "as_of": as_of},
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return ScoresRun(model_version, row[0])


def read_step1(conn: Conn, *, as_of: datetime) -> Step1:
    """§3 step 1: the scores run is ≤ 3 days old, the latest ``price_daily`` session is the last
    completed NYSE session, and a complete crowd snapshot exists for that session."""
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
    last_session = latest_completed_us_session(as_of)
    session_date = next_us_session(last_session)

    def refused(reason: Step1Refusal, run: ScoresRun | None = None, snapshot: int | None = None) -> Step1:
        return Step1(as_of, last_session, session_date, run, snapshot, reason)

    run = read_scores_run(conn, as_of=as_of)
    if run is None:
        return refused("scores_run_missing")
    if as_of - run.scored_at > SCORES_MAX_AGE:
        return refused("scores_run_stale", run)

    row = conn.execute(
        "SELECT max(price_date) FROM price_daily WHERE price_date <= %(d)s", {"d": last_session}
    ).fetchone()
    if row is None or row[0] != last_session:
        return refused("price_daily_stale", run)

    row = conn.execute(
        """
        SELECT snapshot_id FROM etoro_crowd_snapshots
         WHERE status = 'complete' AND started_at >= %(close)s AND finished_at <= %(as_of)s
         ORDER BY started_at DESC, snapshot_id DESC
         LIMIT 1
        """,
        {"close": session_close_utc(last_session), "as_of": as_of},
    ).fetchone()
    if row is None:
        return refused("crowd_snapshot_missing", run)
    return Step1(as_of, last_session, session_date, run, int(row[0]), None)


# ---------------------------------------------------------------------------
# §3.1 shortlist candidates
# ---------------------------------------------------------------------------
def _decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _market_cap(conn: Conn, instrument_id: int, valuation_cap: object) -> Decimal | None:
    """``market_cap_live`` with the #1664 overlay, exactly as the scorer applies it."""
    try:
        with conn.transaction():
            resolution = resolve_market_cap_basis(conn, instrument_id=instrument_id)
    except psycopg.errors.UndefinedTable:
        resolution = MarketCapResolution(basis="not_multiclass")
    overlaid = _apply_market_cap_basis({"market_cap_live": valuation_cap, "fcf_ttm": None}, resolution)
    return _decimal(overlaid["market_cap_live"]) if overlaid is not None else None


def drop_symbol_collisions(candidates: Sequence[ShortlistCandidate]) -> list[ShortlistCandidate]:
    """Every candidate whose symbol another candidate shares, removed — both copies.

    ``instruments.symbol`` is not unique (sql/043), and the model answers with a symbol that
    §6 maps back to an instrument id. Picking one listing would be a guess; dropping the
    symbol fails closed. (None exist among tradable ``us_equity`` names on 2026-09-28.)"""
    counts: dict[str, int] = {}
    for c in candidates:
        counts[c.symbol] = counts.get(c.symbol, 0) + 1
    dropped = sorted(s for s, n in counts.items() if n > 1)
    if dropped:
        logger.warning("ai_trial shortlist: dropping colliding symbols %s", dropped)
    return [c for c in candidates if counts[c.symbol] == 1]


def read_shortlist(conn: Conn, *, step1: Step1) -> Shortlist:
    """The §3.1 shortlist at ``as_of``: tradable ``us_equity`` names scored in the recorded run.

    ⚠ ``instrument_valuation`` is read by id list for the ELIGIBLE names only, as the scorer
    does: a LEFT JOIN of the view onto the candidate query did not finish in 5 minutes on the
    dev DB (2026-09-28). The #1664 overlay then resolves lazily, only for the names the
    small-cap walk actually reaches.
    """
    run = step1.scores_run
    if run is None:
        raise ValueError("read_shortlist needs a step-1 scores run")
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT i.instrument_id, i.symbol, q.bid, q.ask, q.quoted_at, s.total_score
              FROM instruments i
              JOIN exchanges e ON e.exchange_id = i.exchange
              JOIN scores s ON s.instrument_id = i.instrument_id
                           AND s.model_version = %(mv)s AND s.scored_at = %(scored_at)s
              LEFT JOIN quotes q ON q.instrument_id = i.instrument_id
             WHERE i.is_tradable AND e.asset_class = 'us_equity'
            """,
            {"mv": run.model_version, "scored_at": run.scored_at},
        )
        candidates = [
            ShortlistCandidate(
                instrument_id=int(r["instrument_id"]),
                symbol=str(r["symbol"]),
                bid=_decimal(r["bid"]),
                ask=_decimal(r["ask"]),
                quoted_at=r["quoted_at"],
                # NUMERIC → float; a non-finite value fails `is_eligible` (O1).
                total_score=float(r["total_score"]) if r["total_score"] is not None else None,
                market_cap_usd=None,
            )
            for r in cur.fetchall()
        ]
        candidates = drop_symbol_collisions(candidates)
        eligible_ids = [c.instrument_id for c in candidates if is_eligible(c, as_of=step1.as_of)]
        cur.execute(
            "SELECT instrument_id, market_cap_live FROM instrument_valuation "
            "WHERE instrument_id = ANY(%(ids)s::bigint[])",
            {"ids": eligible_ids},
        )
        view_caps = {int(r["instrument_id"]): r["market_cap_live"] for r in cur.fetchall()}
    return select_shortlist(
        candidates,
        as_of=step1.as_of,
        # No view row → no cap, as in the scorer, which overlays only ids that have one.
        market_cap=lambda c: (
            _market_cap(conn, c.instrument_id, view_caps[c.instrument_id]) if c.instrument_id in view_caps else None
        ),
    )


# ---------------------------------------------------------------------------
# §3.2 per-name reads
# ---------------------------------------------------------------------------
def read_bars(
    conn: Conn, instrument_ids: Sequence[int], *, last_session: date
) -> dict[int, tuple[list[date], list[Mapping[str, Any]]]]:
    """The last ``INDICATOR_BARS`` bars dated ≤ ``last_session`` per name, ascending, through the
    house fail-closed reader ``price_masked_bars.load_masked_bars``.

    Its masking is the quarantine's: an unevaluated instrument returns no bars, and a
    quarantined field comes back ``None`` (``assemble_pack`` turns that into
    ``quarantined_bar``). A raw ``price_daily`` read here would feed quarantined bars into
    the indicators — it is the #3046 consumer-exposure class."""
    out: dict[int, tuple[list[date], list[Mapping[str, Any]]]] = {}
    for instrument_id in instrument_ids:
        series = load_masked_bars(conn, instrument_id).series
        n = sum(1 for d in series.dates if d <= last_session)
        dates = list(series.dates[:n])
        rows: list[Mapping[str, Any]] = [dict(r) for r in series.rows[:n]]
        out[instrument_id] = (dates[-INDICATOR_BARS:], rows[-INDICATOR_BARS:])
    return out


def read_crowd(conn: Conn, instrument_ids: Sequence[int], *, snapshot_id: int) -> dict[int, dict[str, Decimal | None]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, buy_holding_pct, sell_holding_pct, traders_change_7d
              FROM etoro_crowd_observations
             WHERE snapshot_id = %(s)s AND instrument_id = ANY(%(ids)s::bigint[])
            """,
            {"s": snapshot_id, "ids": list(instrument_ids)},
        )
        return {int(r.pop("instrument_id")): r for r in cur.fetchall()}


def filing_title(filing_type: str, filing_date: date, items: Sequence[str] | None, report_date: date | None) -> str:
    """A filing's title, built from the structured submission fields — ``filing_events`` has no
    title column. 8-K item codes come from the SEC submissions JSON (``sec-edgar.md``, 8-K row).
    The format is frozen with the pack."""
    title = f"{filing_type} filed {filing_date.isoformat()}"
    if items:
        title += f" (items {', '.join(items)})"
    if report_date is not None:
        title += f" (period {report_date.isoformat()})"
    return title


def read_disclosures(
    conn: Conn, instrument_ids: Sequence[int], *, as_of: datetime
) -> dict[int, dict[str, tuple[Disclosure, ...]]]:
    """Per name: at most 5 filings and 5 headlines via ``select_disclosures`` (one source per
    call). The SQL bounds knowledge time and the window; the pure selector re-applies both."""
    window_start = as_of - INTRADAY_WINDOW
    filings: dict[int, list[Disclosure]] = {i: [] for i in instrument_ids}
    news: dict[int, list[Disclosure]] = {i: [] for i in instrument_ids}
    params = {"ids": list(instrument_ids), "as_of": as_of, "start": window_start, "forms": list(FILING_FORMS)}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT filing_event_id, instrument_id, filing_type, filing_date, items, report_date, created_at
              FROM filing_events
             WHERE instrument_id = ANY(%(ids)s::bigint[])
               AND filing_type = ANY(%(forms)s::text[])
               AND created_at <= %(as_of)s
               AND filing_date >= (%(start)s::timestamptz AT TIME ZONE 'UTC')::date
            """,
            params,
        )
        for r in cur.fetchall():
            filings[int(r["instrument_id"])].append(
                Disclosure(
                    source="filing",
                    source_id=int(r["filing_event_id"]),
                    title=filing_title(r["filing_type"], r["filing_date"], r["items"], r["report_date"]),
                    event_at=datetime.combine(r["filing_date"], time(0), tzinfo=UTC),
                    known_at=r["created_at"],
                )
            )
        cur.execute(
            """
            SELECT news_event_id, instrument_id, headline, event_time, created_at
              FROM news_events
             WHERE instrument_id = ANY(%(ids)s::bigint[])
               AND created_at <= %(as_of)s
               AND event_time >= %(start)s AND event_time <= %(as_of)s
            """,
            params,
        )
        for r in cur.fetchall():
            news[int(r["instrument_id"])].append(
                Disclosure("news", int(r["news_event_id"]), r["headline"] or "", r["event_time"], r["created_at"])
            )
    return {
        i: {
            "filings": select_disclosures(filings[i], as_of=as_of),
            "news": select_disclosures(news[i], as_of=as_of),
        }
        for i in instrument_ids
    }


def read_ranking(conn: Conn, instrument_ids: Sequence[int], *, run: ScoresRun) -> dict[int, dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT score_id, instrument_id, rank, total_score,
                   quality_score, value_score, turnaround_score, momentum_score, sentiment_score,
                   confidence_score
              FROM scores
             WHERE model_version = %(mv)s AND scored_at = %(at)s AND instrument_id = ANY(%(ids)s::bigint[])
            """,
            {"mv": run.model_version, "at": run.scored_at, "ids": list(instrument_ids)},
        )
        return {
            int(r["instrument_id"]): {
                "score_id": int(r["score_id"]),
                "rank": r["rank"],
                "total_score": r["total_score"],
                "families": {f: r[f"{f}_score"] for f in SCORE_FAMILIES},
            }
            for r in cur.fetchall()
        }


# ---------------------------------------------------------------------------
# §3.2 intraday (pure)
# ---------------------------------------------------------------------------
def select_intraday(
    bars: Sequence[IntradayBar], *, as_of: datetime, requested: int, sessions_in_window: int
) -> tuple[IntradayBar, ...] | IntradayIncomplete:
    """Completed bars (bar end ≤ ``as_of``) opened within the 30 days before ``as_of``, ascending.

    Incomplete when the response is empty, fills the request yet starts inside the window
    (truncated), carries a non-finite or negative value or a repeated bar, or keeps fewer bars than the window
    holds NYSE sessions (O1: fewer than one bar per session)."""
    if not bars:
        return "intraday_empty"
    start = as_of - INTRADAY_WINDOW
    if len(bars) >= requested and min(b.timestamp for b in bars) > start:
        return "intraday_truncated"
    kept = sorted(
        (b for b in bars if b.timestamp >= start and b.timestamp + INTRADAY_BAR_LENGTH <= as_of),
        key=lambda b: b.timestamp,
    )
    for b in kept:
        prices = (b.open, b.high, b.low, b.close)
        if not all(p.is_finite() for p in prices) or (b.volume is not None and b.volume < 0):
            return "intraday_non_finite"
    if len({b.timestamp for b in kept}) != len(kept):
        return "intraday_duplicate_bar"
    if len(kept) < sessions_in_window:
        return "intraday_too_few_bars"
    return tuple(kept)


# ---------------------------------------------------------------------------
# §3.2 assembly
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class AccountContext:
    """The arm's book as the model sees it (§3.2). Built by the caller (slice 2)."""

    open_positions: tuple[Mapping[str, Any], ...]
    free_slots: int
    max_new_entries: int


@dataclass(frozen=True)
class Pack:
    pack: dict[str, Any]
    sha256: str
    #: Pack-complete shortlist, symbol → instrument id: the §6 validator's and §7 pool's universe.
    complete: dict[str, int]
    incomplete: dict[str, str]


IntradayFetch = Callable[[int], Sequence[IntradayBar]]


def _bar_json(d: date, row: Mapping[str, Any]) -> dict[str, Any]:
    return {"d": d, "o": row["open"], "h": row["high"], "l": row["low"], "c": row["close"], "v": row["volume"]}


def _finite_or_none(value: float | None) -> float | None:
    return value if value is not None and math.isfinite(value) else None


def assemble_pack(
    conn: Conn,
    *,
    step1: Step1,
    account: AccountContext,
    fetch_intraday: IntradayFetch,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Pack:
    """§3 step 2. A name failing any O1 check drops from BOTH legs' universe, with its reason
    recorded in the pack; nothing is replenished."""
    if step1.refusal is not None or step1.scores_run is None or step1.crowd_snapshot_id is None:
        raise ValueError(f"step 1 refused ({step1.refusal}); no pack")
    as_of, run = step1.as_of, step1.scores_run
    shortlist = read_shortlist(conn, step1=step1)
    ids = [n.instrument_id for n in shortlist.names]
    bars = read_bars(conn, ids, last_session=step1.last_session)
    crowd = read_crowd(conn, ids, snapshot_id=step1.crowd_snapshot_id)
    disclosures = read_disclosures(conn, ids, as_of=as_of)
    ranking = read_ranking(conn, ids, run=run)
    window_sessions = us_sessions_between((as_of - INTRADAY_WINDOW).astimezone(_NY).date(), step1.last_session)

    names: list[dict[str, Any]] = []
    complete: dict[str, int] = {}
    incomplete: dict[str, str] = {}
    for name in shortlist.names:
        dates, rows = bars[name.instrument_id]
        if any(row[k] is None for row in rows for k in ("open", "high", "low", "close")):
            incomplete[name.symbol] = MASKED_REASON
            continue
        series = build_bar_series(dates, rows, last_session=step1.last_session)
        if isinstance(series, str):
            incomplete[name.symbol] = series
            continue
        fetched_at = clock()
        try:
            raw = list(fetch_intraday(name.instrument_id))
        except Exception:
            logger.warning("ai_trial pack: intraday fetch failed for %s", name.symbol, exc_info=True)
            incomplete[name.symbol] = "intraday_fetch_failed"
            continue
        intraday = select_intraday(
            raw, as_of=as_of, requested=INTRADAY_REQUEST_COUNT, sessions_in_window=window_sessions
        )
        if isinstance(intraday, str):
            incomplete[name.symbol] = intraday
            continue
        ind = {k: _finite_or_none(v) for k, v in indicators(series).items()}
        rank = ranking[name.instrument_id]
        crowd_row = crowd.get(name.instrument_id, {})
        names.append(
            {
                "symbol": name.symbol,
                "instrument_id": name.instrument_id,
                "slice": name.slice,
                "bars": [_bar_json(d, r) for d, r in zip(series.dates, series.rows, strict=True)][-PROMPT_BARS:],
                "indicators": ind,
                "intraday": {
                    "interval": INTRADAY_INTERVAL,
                    "fetched_at": fetched_at,
                    "returned_count": len(raw),
                    "bars": [
                        {"t": b.timestamp, "o": b.open, "h": b.high, "l": b.low, "c": b.close, "v": b.volume}
                        for b in intraday
                    ],
                },
                "crowd": {k: crowd_row.get(k) for k in ("buy_holding_pct", "sell_holding_pct", "traders_change_7d")},
                "filings": [_disclosure_json(d) for d in disclosures[name.instrument_id]["filings"]],
                "news": [_disclosure_json(d) for d in disclosures[name.instrument_id]["news"]],
                "ranking": rank,
            }
        )
        complete[name.symbol] = name.instrument_id

    pack: dict[str, Any] = {
        "as_of": as_of,
        "session_date": step1.session_date,
        "last_session": step1.last_session,
        "scores_run": {"model_version": run.model_version, "scored_at": run.scored_at},
        "crowd_snapshot_id": step1.crowd_snapshot_id,
        "eligible_count": shortlist.eligible_count,
        "shortlist": [
            # The cap that admitted a small-cap name, kept here so an INCOMPLETE one stays auditable.
            {"symbol": n.symbol, "instrument_id": n.instrument_id, "slice": n.slice, "market_cap_usd": n.market_cap_usd}
            for n in shortlist.names
        ],
        "incomplete": incomplete,
        "account": {
            "open_positions": [dict(p) for p in account.open_positions],
            "free_slots": account.free_slots,
            "max_new_entries": account.max_new_entries,
        },
        "names": names,
    }
    return Pack(pack, canonical_sha256(pack), complete, incomplete)


def _disclosure_json(d: Disclosure) -> dict[str, Any]:
    return {"source_id": d.source_id, "title": d.title, "event_at": d.event_at, "known_at": d.known_at}
