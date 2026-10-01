"""Ranking-pot-v1 monthly rebalance, steps 0–3 (#2842 slice 4b-i).

Spec ``docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md`` §4: step 0 (due), step 2 (input checks) and
step 3 (the snapshot). Step 1 (the pot's own scoring run) and step 4 (every book's decisions, published with the
snapshot in one transaction) are slice 4b-ii; it calls ``prepare`` inside its REPEATABLE READ transaction, after its
scoring run commits, and writes the ``decided`` row from what ``prepare`` returns.

Writes here are ``ranking_pot_rebalance_attempts`` rows for ``refused`` and ``skipped`` only (``sql/446``).

Fixed here by construction (each closes a spec Appendix A item; the PR lists them):

- **Due (r3-71):** a month is resolved by exactly one ``decided`` or ``skipped`` row. Any fire whose target session
  lies past the 5th NYSE session of an unresolved month, or in a later month, writes that month's ``skipped`` with
  its last refusal (``not_attempted`` when no fire reached a verdict). Downtime cannot leave a month open.
- **First month:** the month of the first target session after the freeze if that session is among the first five
  of its month, else the next month. A mid-month freeze starts at the next month and skips nothing before it.
- **Coverage (r3-76):** the score numerator counts names with a FINITE score row in the run; the bar numerator
  counts names with a finite, positive, range-consistent ``price_daily`` bar on the last completed session.
  Thresholds are inclusive (≥ 95%). A run cannot hold two rows for one name (``uq_scores_instrument_model_scored``)
  and every joined table is keyed by ``instrument_id``, so one row per S₀ name is asserted, not counted (r3-135).
- **Per-name freshness (r3-77):** the aggregate gate is an outage guard only. A name's own stale or missing bars fail
  ITS entry rules (``max_unavailable`` / ``bars_incomplete``); holding needs no bar (§5.0), so no held name is exited
  because a bar is late.
- **Outage guard (r3-74, r3-75):** |R_t| ≥ 80% of the previous DECIDED rebalance's |R| (cardinality). The first
  rebalance has no previous and passes. After two consecutive months skipped with ``universe_collapse`` as their
  last refusal, the third proceeds and records the contraction.
- **SPY (r3-24):** instrument 3000, symbol ``SPY`` (verified at read). Its quote must satisfy the names' own §5.0
  quote rule (``ai_trial_pack.is_eligible``: fresh within 24 h, bid ≥ 3, ask ≥ bid, spread ≤ 1% of mid) and its
  bar on the last completed session must be finite, positive and range-consistent through the masked reader.
- **Breakpoint population (r3-85):** every scored, tradable name on the one ``exchanges`` row described ``NYSE`` in
  the run, INCLUDING names outside S₀; the snapshot stores each one's id and overlaid cap.
- **Overlay (r3-133, r3-134):** every name with a valuation row is overlaid exactly as the scorer does
  (``ai_trial_pack_reader._market_cap``); any failure to resolve or apply it is no cap (fail closed).
- **Snapshot (r3-9):** INPUTS only — own facts, the score row, the bars of R members, the NYSE caps, SPY. No rank, no
  universe, no control-derived field: ``decode_snapshot`` + ``ranking_pot.build_universes`` re-derive them. Decimals
  are stored as their ``str`` (``NaN`` included), so the canonical JSON never refuses a stored value and the decode
  is exact.
- **Snapshot transaction (Codex ckpt-2):** ``begin_rebalance`` opens it: REPEATABLE READ, then ``LOCK TABLE
  ranking_pot_state_events IN SHARE MODE`` before any query, so the snapshot sees every committed state event and no
  wind-down or halt commits until the rebalance does. ``sql/446``'s trigger refuses an attempt without the lock.
- **Thesis provenance (r3-95):** each name carries the thesis the scoring call consumed (id, created_at, model,
  prompt_version), passed in by the scoring step (slice 4b-ii). It is captured, never re-read from ``theses``, whose
  latest row can move after scoring.
- **Bars:** at most ``INDICATOR_BARS`` (260) of the latest price segment, for R members only. A name outside R fails
  a hold rule decided before any bar is read (``ranking_pot.hold_failure``), so omitting its bars changes no output.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from typing import Any, Final, Literal

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.services.ai_trial_pack import ShortlistCandidate, canonical_sha256, is_eligible
from app.services.ai_trial_pack_reader import _market_cap, next_us_session, read_bars
from app.services.market_calendar import latest_completed_us_session, us_market_status
from app.services.ranking_pot import (
    STRATEGY_ID,
    NameFacts,
    ScoreBreakdown,
    Universes,
    build_universes,
    cap_breakpoint,
    hold_failure,
)
from app.services.ranking_pot_policy import RANKING_POT_POLICY_HASH, policy_manifest_now
from app.services.scoring import _DEFAULT_MODEL_VERSION

Conn = psycopg.Connection[Any]

#: §4 step 0 — the target session must be among the first ``FIRST_SESSIONS`` NYSE sessions of its month.
FIRST_SESSIONS: Final = 5
#: §4 step 2 — inclusive thresholds.
SCORE_COVERAGE_MIN: Final = Fraction(95, 100)
BAR_COVERAGE_MIN: Final = Fraction(95, 100)
UNIVERSE_FLOOR: Final = Fraction(80, 100)
#: §4 step 2 — after this many consecutive ``universe_collapse`` months, the next proceeds.
COLLAPSE_OVERRIDE_AFTER: Final = 2
#: §4 step 2 / §9.3 — the benchmark (``SELECT instrument_id, symbol FROM instruments WHERE instrument_id = 3000``).
SPY_INSTRUMENT_ID: Final = 3000
SPY_SYMBOL: Final = "SPY"
NYSE_DESCRIPTION: Final = "NYSE"
SNAPSHOT_KIND: Final = "ranking-pot-snapshot-v1"
#: ``scores`` family columns (``<family>_score``), carried into the ticket (§6).
SCORE_FAMILIES: Final = ("quality", "value", "turnaround", "momentum", "sentiment", "confidence")

InputRefusal = Literal[
    "ranking_drift",
    "scores_run_incomplete",
    "price_daily_stale",
    "spy_unavailable",
    "breakpoint_unavailable",
    "max_cut_unavailable",
    "universe_collapse",
]
#: §4 step 2 refusals in their frozen evaluation order: the first failure is the one recorded.
INPUT_REFUSALS: Final[tuple[InputRefusal, ...]] = (
    "ranking_drift",
    "scores_run_incomplete",
    "price_daily_stale",
    "spy_unavailable",
    "breakpoint_unavailable",
    "max_cut_unavailable",
    "universe_collapse",
)
#: A skipped month that no fire reached a verdict for.
NOT_ATTEMPTED: Final = "not_attempted"


# ---------------------------------------------------------------------------
# Step 0 — due (pure)
# ---------------------------------------------------------------------------
def month_of(d: date) -> date:
    return d.replace(day=1)


def _next_month(m: date) -> date:
    return (m.replace(day=28) + timedelta(days=4)).replace(day=1)


def target_session(as_of: datetime) -> date:
    """§4: the next NYSE session after the last completed one at ``as_of``."""
    return next_us_session(latest_completed_us_session(as_of))


def session_ordinal(session: date) -> int:
    """1-based position of ``session`` among its month's NYSE sessions."""
    if us_market_status(session) == "closed":
        raise ValueError(f"{session} is not a NYSE session")
    d, k = month_of(session), 0
    while d <= session:
        if us_market_status(d) != "closed":
            k += 1
        d += timedelta(days=1)
    return k


def first_month(frozen_at: datetime) -> date:
    """The trial's first rebalance month (see the module docstring)."""
    t0 = target_session(frozen_at)
    return month_of(t0) if session_ordinal(t0) <= FIRST_SESSIONS else _next_month(month_of(t0))


@dataclass(frozen=True)
class DuePlan:
    target_session: date
    #: The target session's month.
    month: date
    #: This fire rebalances ``month``.
    due: bool
    #: Unresolved months this fire closes as ``skipped``, ascending.
    skips: tuple[date, ...]


def plan(as_of: datetime, *, first: date, resolved: frozenset[date]) -> DuePlan:
    """§4 step 0 for one fire. ``resolved`` = months with a ``decided`` or ``skipped`` row."""
    if first != month_of(first):
        raise ValueError("first must be the first day of a month")
    t = target_session(as_of)
    m, k = month_of(t), session_ordinal(t)
    skips: list[date] = []
    cursor = first
    while cursor <= m:
        if cursor not in resolved and (cursor < m or k > FIRST_SESSIONS):
            skips.append(cursor)
        cursor = _next_month(cursor)
    return DuePlan(
        target_session=t,
        month=m,
        due=m >= first and m not in resolved and k <= FIRST_SESSIONS,
        skips=tuple(skips),
    )


# ---------------------------------------------------------------------------
# Step 2 — input gates (pure)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Coverage:
    """§4 step 2 counts. ``s0_tradable``: S₀ names tradable now; ``scored``: of those, a finite score row in the
    run; ``barred``: of ``scored``, a valid bar on the last completed session."""

    s0_tradable: int
    scored: int
    barred: int


def _at_least(numerator: int, denominator: int, floor: Fraction) -> bool:
    return denominator > 0 and Fraction(numerator, denominator) >= floor


def gate_refusal(coverage: Coverage, *, policy_ok: bool, spy_ok: bool) -> InputRefusal | None:
    """The §4 step-2 checks that precede the universes, in ``INPUT_REFUSALS`` order."""
    if not policy_ok:
        return "ranking_drift"
    if not _at_least(coverage.scored, coverage.s0_tradable, SCORE_COVERAGE_MIN):
        return "scores_run_incomplete"
    if not _at_least(coverage.barred, coverage.scored, BAR_COVERAGE_MIN):
        return "price_daily_stale"
    if not spy_ok:
        return "spy_unavailable"
    return None


def universe_collapsed(r_count: int, *, previous_r_count: int | None, consecutive_collapses: int) -> bool:
    """r3-74/75: |R_t| < 80% of the previous decided |R|, unless none exists or the override has matured."""
    if previous_r_count is None or consecutive_collapses >= COLLAPSE_OVERRIDE_AFTER:
        return False
    return Fraction(r_count) < UNIVERSE_FLOOR * previous_r_count


# ---------------------------------------------------------------------------
# Snapshot encode / decode (pure)
# ---------------------------------------------------------------------------
def _s(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _d(value: str | None) -> Decimal | None:
    return None if value is None else Decimal(value)


def _ts(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def _bar(d: date, row: Mapping[str, Any]) -> list[Any]:
    def num(v: Any) -> str | None:
        if v is None:
            return None
        return str(v if isinstance(v, Decimal) else Decimal(repr(v)) if isinstance(v, float) else Decimal(v))

    vol = row.get("volume")
    return [
        d.isoformat(),
        num(row.get("open")),
        num(row.get("high")),
        num(row.get("low")),
        num(row.get("close")),
        None if vol is None else int(vol),
    ]


def _unbar(bar: Sequence[Any]) -> tuple[date, dict[str, Any]]:
    d, o, h, low, c, v = bar
    return date.fromisoformat(d), {"open": _d(o), "high": _d(h), "low": _d(low), "close": _d(c), "volume": v}


def _thesis_json(t: ThesisUsed | None) -> dict[str, Any] | None:
    if t is None:
        return None
    return {
        "thesis_id": t.thesis_id,
        "created_at": _ts(t.created_at),
        "model": t.model,
        "prompt_version": t.prompt_version,
    }


def _score_json(score: ScoreBreakdown) -> dict[str, Any]:
    return {
        "model_version": score.model_version,
        "total_score": str(score.total_score),
        "raw_total": _s(score.raw_total),
        "families": {k: _s(v) for k, v in score.families.items()},
        "penalties_json": [dict(p) for p in score.penalties_json],
    }


def _score_from(doc: Mapping[str, Any]) -> ScoreBreakdown:
    return ScoreBreakdown(
        model_version=doc["model_version"],
        total_score=Decimal(doc["total_score"]),
        raw_total=_d(doc["raw_total"]),
        families={k: _d(v) for k, v in doc["families"].items()},
        penalties_json=tuple(doc["penalties_json"]),
    )


@dataclass(frozen=True)
class SpyInputs:
    bid: Decimal | None
    ask: Decimal | None
    quoted_at: datetime | None
    #: The masked bar on the last completed session, or ``None``.
    bar: tuple[date, Mapping[str, Any]] | None


@dataclass(frozen=True)
class ThesisUsed:
    """The thesis a scoring call consumed for one name (§6, §9.4)."""

    thesis_id: int
    created_at: datetime
    model: str | None
    prompt_version: str | None


@dataclass(frozen=True)
class SnapshotInputs:
    """Everything §5 and §9 read at one rebalance. ``facts`` covers S₀, ascending by id; ``scores`` the S₀ names
    with a score row; ``nyse_caps`` the breakpoint population as ``(instrument_id, overlaid cap)``, ascending."""

    declaration_id: int
    declaration_sha256: str
    policy_hash: str
    as_of: datetime
    last_session: date
    target_session: date
    scored_at: datetime
    facts: tuple[NameFacts, ...]
    scores: Mapping[int, ScoreBreakdown]
    nyse_caps: tuple[tuple[int, Decimal | None], ...]
    spy: SpyInputs
    #: S₀ names whose score consumed a thesis; absent = none consumed.
    theses: Mapping[int, ThesisUsed]


def encode_snapshot(inputs: SnapshotInputs) -> dict[str, Any]:
    """The canonical snapshot document: str/int/bool/None/list/dict only, so its JSONB round trip keeps its sha."""
    if not set(inputs.theses) <= {f.instrument_id for f in inputs.facts}:
        raise ValueError("a thesis for a name outside S₀")
    names = []
    for f in inputs.facts:
        score = inputs.scores.get(f.instrument_id)
        if (score is None) != (f.total_score is None):
            raise ValueError(f"{f.instrument_id}: the facts and the score row disagree")
        names.append(
            {
                "instrument_id": f.instrument_id,
                "symbol": f.symbol,
                "is_tradable": f.is_tradable,
                "asset_class": f.asset_class,
                "completeness_tier": f.completeness_tier,
                "filings_status": f.filings_status,
                "market_cap_usd": _s(f.market_cap_usd),
                "bid": _s(f.bid),
                "ask": _s(f.ask),
                "quoted_at": _ts(f.quoted_at),
                "score": None if score is None else _score_json(score),
                "thesis": _thesis_json(inputs.theses.get(f.instrument_id)),
                "bars": [_bar(d, r) for d, r in zip(f.bar_dates, f.bar_rows, strict=True)],
            }
        )
    spy = inputs.spy
    return {
        "kind": SNAPSHOT_KIND,
        "strategy_id": STRATEGY_ID,
        "declaration_id": inputs.declaration_id,
        "declaration_sha256": inputs.declaration_sha256,
        "policy_hash": inputs.policy_hash,
        "as_of": _ts(inputs.as_of),
        "last_session": inputs.last_session.isoformat(),
        "target_session": inputs.target_session.isoformat(),
        "scores_run": {"model_version": _DEFAULT_MODEL_VERSION, "scored_at": _ts(inputs.scored_at)},
        "names": names,
        "nyse_caps": [[iid, _s(cap)] for iid, cap in inputs.nyse_caps],
        "spy": {
            "instrument_id": SPY_INSTRUMENT_ID,
            "bid": _s(spy.bid),
            "ask": _s(spy.ask),
            "quoted_at": _ts(spy.quoted_at),
            "bar": None if spy.bar is None else _bar(*spy.bar),
        },
    }


def decode_snapshot(doc: Mapping[str, Any]) -> SnapshotInputs:
    if doc.get("kind") != SNAPSHOT_KIND:
        raise ValueError(f"not a {SNAPSHOT_KIND} document")
    facts: list[NameFacts] = []
    scores: dict[int, ScoreBreakdown] = {}
    theses: dict[int, ThesisUsed] = {}
    for n in doc["names"]:
        iid = int(n["instrument_id"])
        if (t := n["thesis"]) is not None:
            theses[iid] = ThesisUsed(
                int(t["thesis_id"]), datetime.fromisoformat(t["created_at"]), t["model"], t["prompt_version"]
            )
        score = None if n["score"] is None else _score_from(n["score"])
        if score is not None:
            scores[iid] = score
        bars = [_unbar(b) for b in n["bars"]]
        facts.append(
            NameFacts(
                instrument_id=iid,
                symbol=n["symbol"],
                is_tradable=bool(n["is_tradable"]),
                asset_class=n["asset_class"],
                total_score=None if score is None else score.total_score,
                completeness_tier=n["completeness_tier"],
                filings_status=n["filings_status"],
                market_cap_usd=_d(n["market_cap_usd"]),
                bid=_d(n["bid"]),
                ask=_d(n["ask"]),
                quoted_at=None if n["quoted_at"] is None else datetime.fromisoformat(n["quoted_at"]),
                bar_dates=tuple(d for d, _ in bars),
                bar_rows=tuple(r for _, r in bars),
            )
        )
    spy = doc["spy"]
    return SnapshotInputs(
        declaration_id=int(doc["declaration_id"]),
        declaration_sha256=doc["declaration_sha256"],
        policy_hash=doc["policy_hash"],
        as_of=datetime.fromisoformat(doc["as_of"]),
        last_session=date.fromisoformat(doc["last_session"]),
        target_session=date.fromisoformat(doc["target_session"]),
        scored_at=datetime.fromisoformat(doc["scores_run"]["scored_at"]),
        facts=tuple(facts),
        scores=scores,
        nyse_caps=tuple((int(iid), _d(cap)) for iid, cap in doc["nyse_caps"]),
        spy=SpyInputs(
            bid=_d(spy["bid"]),
            ask=_d(spy["ask"]),
            quoted_at=None if spy["quoted_at"] is None else datetime.fromisoformat(spy["quoted_at"]),
            bar=None if spy["bar"] is None else _unbar(spy["bar"]),
        ),
        theses=theses,
    )


def universes_of(inputs: SnapshotInputs) -> Universes | Literal["breakpoint_unavailable", "max_cut_unavailable"]:
    """§5.0 from a snapshot: the one derivation every book and every replay uses."""
    return build_universes(
        inputs.facts,
        nyse_caps=[cap for _, cap in inputs.nyse_caps],
        as_of=inputs.as_of,
        last_session=inputs.last_session,
    )


def _valid_bar(row: Mapping[str, Any] | None) -> bool:
    if row is None:
        return False
    exact: list[Decimal] = []
    for k in ("open", "high", "low", "close"):
        v = row.get(k)
        if isinstance(v, bool) or not isinstance(v, Decimal | float | int):
            return False
        # A float by its shortest round-trip form, as ``ranking_pot._exact`` reads one.
        exact.append(Decimal(repr(v)) if isinstance(v, float) else Decimal(v))
    if not all(v.is_finite() and v > 0 for v in exact):
        return False
    o, h, low, c = exact
    return low <= min(o, c) and h >= max(o, c)


def spy_ok(spy: SpyInputs, *, as_of: datetime, last_session: date) -> bool:
    """r3-24: the names' own §5.0 quote rule, and a valid bar ON the last completed session."""
    quote = ShortlistCandidate(
        instrument_id=SPY_INSTRUMENT_ID,
        symbol=SPY_SYMBOL,
        bid=spy.bid,
        ask=spy.ask,
        quoted_at=spy.quoted_at,
        # `is_eligible` also requires a finite positive score; SPY has none, so the quote rule alone decides.
        total_score=1.0,
        market_cap_usd=None,
    )
    return (
        is_eligible(quote, as_of=as_of)
        and spy.bar is not None
        and spy.bar[0] == last_session
        and _valid_bar(spy.bar[1])
    )


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PotDeclaration:
    declaration_id: int
    doc: Mapping[str, Any]
    doc_sha256: str
    frozen_at: datetime
    #: Latest ``to_state``; ``None`` when no event exists (readers fail closed).
    state: str | None

    @property
    def s0_ids(self) -> tuple[int, ...]:
        return tuple(int(i) for i in self.doc["s0"]["instrument_ids"])


class SnapshotIntegrityError(RuntimeError):
    """A stored document does not hash to its stored sha256."""


def load_declaration(conn: Conn) -> PotDeclaration | None:
    """The pot's non-terminal declaration (§7.1: at most one), its document integrity checked."""
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(
            """
            SELECT d.declaration_id, d.doc, d.doc_sha256, d.frozen_at,
                   (SELECT e.to_state FROM ranking_pot_state_events e
                     WHERE e.declaration_id = d.declaration_id ORDER BY e.event_id DESC LIMIT 1) AS state
              FROM ranking_pot_declarations d
             WHERE d.strategy_id = %(id)s
             ORDER BY d.declaration_id
            """,
            {"id": STRATEGY_ID},
        ).fetchall()
    live = [r for r in rows if r["state"] != "completed"]
    if not live:
        return None
    if len(live) > 1:
        raise SnapshotIntegrityError(f"{len(live)} non-terminal {STRATEGY_ID} declarations")
    r = live[0]
    if canonical_sha256(r["doc"]) != r["doc_sha256"]:
        raise SnapshotIntegrityError(f"declaration {r['declaration_id']} does not hash to its doc_sha256")
    return PotDeclaration(int(r["declaration_id"]), r["doc"], str(r["doc_sha256"]), r["frozen_at"], r["state"])


@dataclass(frozen=True)
class AttemptHistory:
    resolved: frozenset[date]
    #: Each unresolved month's latest refusal.
    last_refusal: Mapping[date, str]
    #: |R| of the latest decided rebalance (``detail.r_count``).
    previous_r_count: int | None
    #: The streak of the NEWEST resolved months skipped with ``universe_collapse`` as their last refusal. Any other
    #: resolution ends it: a ``decided`` month, or a skip for another reason (``not_attempted`` included — a month
    #: with no verdict is no evidence of a collapse).
    consecutive_collapses: int


def read_history(conn: Conn, declaration_id: int) -> AttemptHistory:
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(
            "SELECT month, outcome, refusal, detail FROM ranking_pot_rebalance_attempts "
            "WHERE declaration_id = %(d)s ORDER BY attempt_id",
            {"d": declaration_id},
        ).fetchall()
    resolved: dict[date, dict[str, Any]] = {}
    last_refusal: dict[date, str] = {}
    previous_r: int | None = None
    for r in rows:
        if r["outcome"] == "refused":
            last_refusal[r["month"]] = r["refusal"]
        else:
            resolved[r["month"]] = r
            if r["outcome"] == "decided":
                previous_r = int(r["detail"]["r_count"])
    collapses = 0
    for m in sorted(resolved, reverse=True):
        row = resolved[m]
        if row["outcome"] == "skipped" and row["refusal"] == "universe_collapse":
            collapses += 1
        else:
            break
    return AttemptHistory(
        resolved=frozenset(resolved),
        last_refusal={m: v for m, v in last_refusal.items() if m not in resolved},
        previous_r_count=previous_r,
        consecutive_collapses=collapses,
    )


def begin_rebalance(conn: Conn) -> None:
    """Make the open transaction a rebalance transaction: REPEATABLE READ, holding SHARE on
    ``ranking_pot_state_events`` from before its snapshot (``sql/446`` SNAPSHOT RULE). Call first inside
    ``conn.transaction()``; ``SET TRANSACTION`` raises if any query already ran, so a late call cannot pass."""
    if conn.info.transaction_status != TransactionStatus.INTRANS:
        raise RuntimeError("begin_rebalance must run inside an open transaction")
    conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    conn.execute("LOCK TABLE ranking_pot_state_events IN SHARE MODE")


def _assert_rebalance_transaction(conn: Conn) -> None:
    row = conn.execute(
        """
        SELECT current_setting('transaction_isolation') = 'repeatable read'
           AND EXISTS (SELECT 1 FROM pg_locks
                        WHERE locktype = 'relation' AND relation = 'ranking_pot_state_events'::regclass
                          AND pid = pg_backend_pid() AND granted AND mode = 'ShareLock')
        """
    ).fetchone()
    if conn.info.transaction_status != TransactionStatus.INTRANS or row is None or not row[0]:
        raise RuntimeError("the rebalance reads run inside begin_rebalance's transaction")


def _overlaid_cap(conn: Conn, instrument_id: int, raw: object) -> Decimal | None:
    """The scorer's #1664 overlay; any failure to resolve or apply it is no cap (§2, r3-134). The savepoint keeps a
    caught database error from aborting the rebalance's transaction (``_market_cap`` also takes one around its own
    reads; this one does not rely on it)."""
    try:
        with conn.transaction():
            cap = _market_cap(conn, instrument_id, raw)
    except psycopg.Error, ArithmeticError, ValueError, TypeError:
        return None
    if cap is None or not cap.is_finite() or cap <= 0:
        return None
    return cap


def _nyse_exchange_id(conn: Conn) -> str | None:
    rows = conn.execute("SELECT exchange_id FROM exchanges WHERE description = %s", (NYSE_DESCRIPTION,)).fetchall()
    return str(rows[0][0]) if len(rows) == 1 else None


def _score_row(r: Mapping[str, Any]) -> ScoreBreakdown | None:
    if r["total_score"] is None:
        return None
    return ScoreBreakdown(
        model_version=_DEFAULT_MODEL_VERSION,
        total_score=r["total_score"],
        raw_total=r["raw_total"],
        families={f: r[f"{f}_score"] for f in SCORE_FAMILIES},
        penalties_json=tuple(r["penalties_json"] or ()),
    )


@dataclass(frozen=True)
class InputsRead:
    """Steps 2–3 read: the snapshot inputs, the counts behind the gates, and the NYSE id check."""

    inputs: SnapshotInputs
    coverage: Coverage
    nyse_found: bool


def read_snapshot_inputs(
    conn: Conn,
    decl: PotDeclaration,
    *,
    as_of: datetime,
    scored_at: datetime,
    theses: Mapping[int, ThesisUsed],
) -> InputsRead:
    """Read every step-2/3 input inside ``begin_rebalance``'s transaction, opened after the scoring run commits
    (§4). ``theses`` is what the scoring call consumed (slice 4b-ii). Only reads."""
    if conn.info.transaction_status != TransactionStatus.INTRANS:
        raise RuntimeError("read_snapshot_inputs must run inside the rebalance's transaction")
    _assert_rebalance_transaction(conn)
    last_session = latest_completed_us_session(as_of)
    s0 = decl.s0_ids
    mv = _DEFAULT_MODEL_VERSION
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(
            """
            SELECT u.instrument_id, i.symbol, coalesce(i.is_tradable, FALSE) AS is_tradable, e.asset_class,
                   s.total_score, s.raw_total, s.quality_score, s.value_score, s.turnaround_score,
                   s.momentum_score, s.sentiment_score, s.confidence_score, s.penalties_json, s.completeness_tier,
                   c.filings_status, q.bid, q.ask, q.quoted_at
              FROM unnest(%(ids)s::bigint[]) AS u(instrument_id)
              LEFT JOIN instruments i ON i.instrument_id = u.instrument_id
              LEFT JOIN exchanges e ON e.exchange_id = i.exchange
              LEFT JOIN scores s ON s.instrument_id = u.instrument_id
                                AND s.model_version = %(mv)s AND s.scored_at = %(t)s
              LEFT JOIN coverage c ON c.instrument_id = u.instrument_id
              LEFT JOIN quotes q ON q.instrument_id = u.instrument_id
             ORDER BY u.instrument_id
            """,
            {"ids": list(s0), "mv": mv, "t": scored_at},
        ).fetchall()
        if [int(r["instrument_id"]) for r in rows] != sorted(set(s0)) or len(s0) != len(set(s0)):
            raise SnapshotIntegrityError("the S₀ read is not one row per declared name")

        nyse_id = _nyse_exchange_id(conn)
        nyse_ids: list[int] = []
        if nyse_id is not None:
            nyse_ids = [
                int(r["instrument_id"])
                for r in cur.execute(
                    """
                    SELECT s.instrument_id FROM scores s JOIN instruments i ON i.instrument_id = s.instrument_id
                     WHERE s.model_version = %(mv)s AND s.scored_at = %(t)s AND i.is_tradable AND i.exchange = %(x)s
                     ORDER BY s.instrument_id
                    """,
                    {"mv": mv, "t": scored_at, "x": nyse_id},
                ).fetchall()
            ]
        cap_ids = sorted(set(s0) | set(nyse_ids))
        view_caps = {
            int(r["instrument_id"]): r["market_cap_live"]
            for r in cur.execute(
                "SELECT instrument_id, market_cap_live FROM instrument_valuation "
                "WHERE instrument_id = ANY(%(ids)s::bigint[])",
                {"ids": cap_ids},
            ).fetchall()
        }
    # No view row → no cap, as in the scorer, which overlays only ids that have one.
    caps = {iid: _overlaid_cap(conn, iid, raw) for iid, raw in view_caps.items()}

    scores: dict[int, ScoreBreakdown] = {}
    facts: list[NameFacts] = []
    for r in rows:
        iid = int(r["instrument_id"])
        score = _score_row(r)
        if score is not None:
            scores[iid] = score
        facts.append(
            NameFacts(
                instrument_id=iid,
                symbol=r["symbol"] or "",
                is_tradable=bool(r["is_tradable"]),
                asset_class=r["asset_class"],
                total_score=r["total_score"],
                completeness_tier=r["completeness_tier"],
                filings_status=r["filings_status"],
                market_cap_usd=caps.get(iid),
                bid=r["bid"],
                ask=r["ask"],
                quoted_at=r["quoted_at"],
            )
        )

    s0_tradable = [f.instrument_id for f in facts if f.is_tradable]
    scored = [iid for iid in s0_tradable if iid in scores and scores[iid].total_score.is_finite()]
    barred = _count_valid_bars(conn, scored, last_session)

    nyse_caps = tuple((iid, caps.get(iid)) for iid in nyse_ids)
    breakpoint = cap_breakpoint([cap for _, cap in nyse_caps])
    if isinstance(breakpoint, Decimal):
        members = {f.instrument_id for f in facts if hold_failure(f, breakpoint) is None}
    else:
        members = set()
    bars = read_bars(conn, sorted(members | {SPY_INSTRUMENT_ID}), last_session=last_session)
    facts = [
        replace(f, bar_dates=tuple(bars[f.instrument_id][0]), bar_rows=tuple(bars[f.instrument_id][1]))
        if f.instrument_id in members
        else f
        for f in facts
    ]
    spy = _read_spy(conn, bars.get(SPY_INSTRUMENT_ID))

    inputs = SnapshotInputs(
        declaration_id=decl.declaration_id,
        declaration_sha256=decl.doc_sha256,
        policy_hash=RANKING_POT_POLICY_HASH,
        as_of=as_of,
        last_session=last_session,
        target_session=next_us_session(last_session),
        scored_at=scored_at,
        facts=tuple(facts),
        scores=scores,
        nyse_caps=nyse_caps,
        spy=spy,
        theses=dict(theses),
    )
    return InputsRead(
        inputs=inputs,
        coverage=Coverage(s0_tradable=len(s0_tradable), scored=len(scored), barred=barred),
        nyse_found=nyse_id is not None,
    )


def _count_valid_bars(conn: Conn, instrument_ids: Sequence[int], session: date) -> int:
    rows = conn.execute(
        "SELECT instrument_id, open, high, low, close FROM price_daily "
        "WHERE instrument_id = ANY(%(ids)s::bigint[]) AND price_date = %(d)s",
        {"ids": list(instrument_ids), "d": session},
    ).fetchall()
    valid = {int(iid) for iid, o, h, low, c in rows if _valid_bar({"open": o, "high": h, "low": low, "close": c})}
    return len(valid)


def _read_spy(conn: Conn, spy_bars: tuple[list[date], list[Mapping[str, Any]]] | None) -> SpyInputs:
    row = conn.execute(
        "SELECT i.symbol, q.bid, q.ask, q.quoted_at FROM instruments i "
        "LEFT JOIN quotes q ON q.instrument_id = i.instrument_id WHERE i.instrument_id = %s",
        (SPY_INSTRUMENT_ID,),
    ).fetchone()
    if row is None or row[0] != SPY_SYMBOL:
        return SpyInputs(None, None, None, None)
    bar = (spy_bars[0][-1], spy_bars[1][-1]) if spy_bars and spy_bars[0] else None
    return SpyInputs(bid=row[1], ask=row[2], quoted_at=row[3], bar=bar)


# ---------------------------------------------------------------------------
# Steps 2–3 together
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Prepared:
    snapshot: dict[str, Any]
    snapshot_sha256: str
    universes: Universes
    detail: dict[str, Any]


@dataclass(frozen=True)
class Refused:
    refusal: InputRefusal
    detail: dict[str, Any]


def _policy_ok(decl: PotDeclaration) -> bool:
    """§8 drift: the declared hash, the import-time hash and the bytes on disk now all agree (r3-90 residual)."""
    return decl.doc.get("policy_hash") == RANKING_POT_POLICY_HASH == policy_manifest_now().digest()


def prepare(
    conn: Conn,
    decl: PotDeclaration,
    history: AttemptHistory,
    *,
    as_of: datetime,
    scored_at: datetime,
    theses: Mapping[int, ThesisUsed],
) -> Prepared | Refused:
    """§4 steps 2–3: read, gate, snapshot. Writes nothing."""
    read = read_snapshot_inputs(conn, decl, as_of=as_of, scored_at=scored_at, theses=theses)
    inputs, cov = read.inputs, read.coverage
    detail: dict[str, Any] = {
        "s0_tradable": cov.s0_tradable,
        "scored": cov.scored,
        "barred": cov.barred,
        "nyse_exchange_found": read.nyse_found,
        "nyse_caps": len(inputs.nyse_caps),
        "nyse_caps_valid": sum(1 for _, c in inputs.nyse_caps if c is not None),
    }
    refusal = gate_refusal(
        cov,
        policy_ok=_policy_ok(decl),
        spy_ok=spy_ok(inputs.spy, as_of=as_of, last_session=inputs.last_session),
    )
    if refusal is not None:
        return Refused(refusal, detail)
    universes = universes_of(inputs)
    if isinstance(universes, str):
        return Refused(universes, detail)
    r_count = len(universes.r_ids)
    detail |= {
        "r_count": r_count,
        "f_count": len(universes.f_ids),
        "max_population": universes.max_population,
        "previous_r_count": history.previous_r_count,
    }
    if universe_collapsed(
        r_count, previous_r_count=history.previous_r_count, consecutive_collapses=history.consecutive_collapses
    ):
        return Refused("universe_collapse", detail)
    if history.previous_r_count is not None and Fraction(r_count) < UNIVERSE_FLOOR * history.previous_r_count:
        detail["contraction_recorded"] = True  # the override matured (§4 step 2)
    snapshot = encode_snapshot(inputs)
    return Prepared(snapshot=snapshot, snapshot_sha256=canonical_sha256(snapshot), universes=universes, detail=detail)


# ---------------------------------------------------------------------------
# Writes: refused and skipped rows (the decided row is slice 4b-ii's)
# ---------------------------------------------------------------------------
def record_refused(
    conn: Conn,
    decl: PotDeclaration,
    due: DuePlan,
    refused: Refused | Literal["scoring_failed"],
    *,
    as_of: datetime,
    scored_at: datetime | None,
) -> int:
    code, detail = (refused, {}) if isinstance(refused, str) else (refused.refusal, refused.detail)
    row = conn.execute(
        "INSERT INTO ranking_pot_rebalance_attempts "
        "(declaration_id, fired_at, target_session, month, outcome, refusal, scored_at, policy_hash, detail) "
        "VALUES (%s, %s, %s, %s, 'refused', %s, %s, %s, %s) RETURNING attempt_id",
        (
            decl.declaration_id,
            as_of,
            due.target_session,
            due.month,
            code,
            scored_at,
            RANKING_POT_POLICY_HASH,
            Jsonb(detail),
        ),
    ).fetchone()
    assert row is not None
    return int(row[0])


def record_skips(conn: Conn, decl: PotDeclaration, due: DuePlan, history: AttemptHistory, *, as_of: datetime) -> int:
    """Close every month in ``due.skips`` as ``skipped`` with its last refusal (r3-71). Returns the count."""
    for m in due.skips:
        conn.execute(
            "INSERT INTO ranking_pot_rebalance_attempts "
            "(declaration_id, fired_at, target_session, month, outcome, refusal, policy_hash) "
            "VALUES (%s, %s, %s, %s, 'skipped', %s, %s)",
            (
                decl.declaration_id,
                as_of,
                due.target_session,
                m,
                history.last_refusal.get(m, NOT_ATTEMPTED),
                RANKING_POT_POLICY_HASH,
            ),
        )
    return len(due.skips)


__all__ = [
    "BAR_COVERAGE_MIN",
    "FIRST_SESSIONS",
    "INPUT_REFUSALS",
    "SCORE_COVERAGE_MIN",
    "SNAPSHOT_KIND",
    "SPY_INSTRUMENT_ID",
    "UNIVERSE_FLOOR",
    "AttemptHistory",
    "Coverage",
    "DuePlan",
    "PotDeclaration",
    "Prepared",
    "Refused",
    "SnapshotInputs",
    "SnapshotIntegrityError",
    "SpyInputs",
    "ThesisUsed",
    "begin_rebalance",
    "decode_snapshot",
    "encode_snapshot",
    "first_month",
    "gate_refusal",
    "load_declaration",
    "plan",
    "prepare",
    "read_history",
    "read_snapshot_inputs",
    "record_refused",
    "record_skips",
    "session_ordinal",
    "spy_ok",
    "target_session",
    "universe_collapsed",
    "universes_of",
]
