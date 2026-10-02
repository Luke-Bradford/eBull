"""Ranking-pot-v1 online step job (#2842 slice 6a).

Spec ``docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md`` §9.1 ("The step job" paragraph) and §4 (**): the
shadow (book 0), the K order-reassignment controls (books 1..K) and the no-SL/TP variant (book K + 1, §9.4 slice
6c-ii-a) are advanced one NYSE session at a time from the stored bars, and each published ``decided`` snapshot is
applied to every book before its target session's step. After each committed step (and at the start of each fire) the
§9.3 looks are computed and the state reconciled (``ranking_pot_look``, slice 6b).

Writes: one ``ranking_pot_steps`` row per stepped session and every book's ``ranking_pot_book_checkpoints`` row, in
one transaction (``sql/447``).

Fixed here by construction (each closes a spec Appendix A item or a §9.1 paragraph clause; the PR lists them):

- **When (r3-40).** Session S is due from ``ranking_pot_job.BARS_WAIT_UNTIL`` UTC on the calendar day after S. One
  REPEATABLE READ transaction reads everything once (bars through the masked reader, reference closes, SPY); the
  values it reads are the values stepped and stored, and a bar absent or masked then is missing for S for good.
- **Gate (r3-30..32).** SPY's bar for S is valid, and at least ``BAR_COVERAGE_MIN`` (inclusive) of the population has
  a valid bar (``ranking_pot_sim.valid_bar``; an empty population passes). The population is every name held or
  pending in the shadow or a control (the variant's own names are read and stored, never gated), plus F_t when a
  rebalance is applied at S and no wind-down falls on S (a superset of the names entering in some book). A failed
  gate records a ``bar_gate`` refusal and steps nothing. Once ``FORCE_AFTER`` later NYSE sessions have completed, S
  is stepped without the gate (``forced``), so an outage cannot stall the books.
- **Decision (§4 (**)).** Applied before S's step from the ``decided`` row targeting S, with every book's ledger at
  the snapshot's ``last_session`` (asserted); the first decided snapshot creates the books there. A decided row whose
  target session is already behind the ledger and was never applied raises (no month dropped).
- **Wind-down (§5.1 rule 3).** W = the first NYSE session after the first ``winding_down`` event's New York date; at
  W's step, after any decision targeting W, every book stamps each unstamped position for W and drops its pending
  entries (``ranking_pot_sim.wind_down``); the step row records the event id. A W already behind the ledger and not
  applied raises: stepping S needs 09:00 UTC on S + 1 day, so W is after every session steppable when the event commits.
- **Exactly once (r3-42).** ``ranking_pot_rebalance.begin_rebalance`` opens the step transaction (REPEATABLE READ,
  ``ranking_pot_state_events`` SHARE-locked before the first query), then the declaration row is locked: the
  rebalance's lock order. The ``(declaration_id, session)`` key fences a second step; every checkpoint update is
  conditional on the previous session; ``sql/447`` fences publication against stepping. The policy hash is checked
  before every step (r3-92); a mismatch records a ``policy_drift`` refusal.
- **Endpoint valuation (Codex ckpt-1).** Every session stores, per book, the record sum with still-open positions
  charged ``h_exit`` (``ranking_pot_sim.liquidation_charged``) and the charged NAV, so any session can be a look
  endpoint without the mutable checkpoint. Session sums add that session's records in the simulator's order under
  ``ranking_pot_sim.CTX``.
- **Checkpoint (r3-43).** Each book's state is canonical JSON; the step row stores the digest over every book's
  sha256 in book order, and the next step recomputes it from the stored rows before stepping.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Any, Final
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.services import ranking_pot as pot
from app.services import ranking_pot_exits as exits
from app.services import ranking_pot_look as look
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_sim as sim
from app.services.ai_trial_pack import canonical_sha256
from app.services.market_calendar import latest_completed_us_session
from app.services.price_masked_bars import load_masked_bars
from app.services.ranking_pot_job import BARS_WAIT_UNTIL
from app.services.ranking_pot_policy import RANKING_POT_POLICY_HASH

logger = logging.getLogger(__name__)

Conn = psycopg.Connection[Any]

SHADOW_BOOK: Final = 0
#: Spec §9.1: a session still unstepped once this many later NYSE sessions have completed is stepped without the gate.
FORCE_AFTER: Final = 10
#: Books decoded, stepped and written back at a time (memory bound: K + 2 books never sit in memory together).
CHUNK: Final = 500
#: Sessions of each name's series kept for the split check's reference closes. A held position's
#: ``last_close_session`` is at most ``MISSING_EXIT_RUN`` stepped sessions back, a pending entry's is the
#: snapshot's ``last_session``, so 20 sessions is ample; an older lookup raises rather than skipping the check.
REFERENCE_SESSIONS: Final = 20
_NEW_YORK: Final = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# Pure: timing
# ---------------------------------------------------------------------------
def book_terms(decl: rb.PotDeclaration) -> tuple[int, int]:
    """(N, K) as the declaration froze them (§8)."""
    terms = decl.doc["terms"]
    return int(terms["n"]), int(terms["k_controls"])


def step_ready_at(session: date) -> datetime:
    """The earliest instant ``session`` may be stepped: ``BARS_WAIT_UNTIL`` UTC on the next calendar day."""
    return datetime.combine(session + timedelta(days=1), BARS_WAIT_UNTIL, UTC)


def step_due(as_of: datetime, session: date) -> bool:
    if as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    return session <= latest_completed_us_session(as_of) and as_of >= step_ready_at(session)


def wind_down_session(event_at: datetime) -> date:
    """W: the first NYSE session after the wind-down event's New York date."""
    if event_at.tzinfo is None:
        raise ValueError("event_at must be timezone-aware")
    return sim.next_session(event_at.astimezone(_NEW_YORK).date())


# ---------------------------------------------------------------------------
# Pure: book state codec
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Book:
    state: sim.BookState
    #: Names whose lifecycle in this book closed at a session stepped since the previous applied rebalance.
    exited_since: frozenset[int] = frozenset()


def _s(v: Decimal) -> str:
    if not v.is_finite():
        raise ValueError("a book holds only finite values")
    return str(v)


def encode_book(book: Book) -> dict[str, Any]:
    st = book.state
    return {
        "n": st.n,
        "cash": [_s(c) for c in st.cash],
        "positions": [
            [
                p.instrument_id,
                p.lifecycle,
                p.slot,
                _s(p.units),
                p.entry_session.isoformat(),
                _s(p.entry_fill),
                _s(p.invested),
                _s(p.stop_loss),
                _s(p.take_profit),
                _s(p.last_close),
                p.last_close_session.isoformat(),
                _s(p.value),
                p.missing_run,
                _s(p.h_exit),
                None if p.exit_stamp is None else [p.exit_stamp[0].isoformat(), p.exit_stamp[1]],
            ]
            for p in st.positions
        ],
        "pending": [
            [
                e.instrument_id,
                e.session.isoformat(),
                _s(e.h),
                _s(e.atr14),
                _s(e.snapshot_close),
                e.snapshot_session.isoformat(),
            ]
            for e in st.pending
        ],
        "next_lifecycle": st.next_lifecycle,
        "last_session": None if st.last_session is None else st.last_session.isoformat(),
        "exited_since": sorted(book.exited_since),
    }


def decode_book(doc: Mapping[str, Any]) -> Book:
    D, d = Decimal, date.fromisoformat
    positions = tuple(
        sim.Position(
            instrument_id=int(r[0]),
            lifecycle=int(r[1]),
            slot=int(r[2]),
            units=D(r[3]),
            entry_session=d(r[4]),
            entry_fill=D(r[5]),
            invested=D(r[6]),
            stop_loss=D(r[7]),
            take_profit=D(r[8]),
            last_close=D(r[9]),
            last_close_session=d(r[10]),
            value=D(r[11]),
            missing_run=int(r[12]),
            h_exit=D(r[13]),
            exit_stamp=None if r[14] is None else (d(r[14][0]), str(r[14][1])),
        )
        for r in doc["positions"]
    )
    pending = tuple(sim.PendingEntry(int(r[0]), d(r[1]), D(r[2]), D(r[3]), D(r[4]), d(r[5])) for r in doc["pending"])
    state = sim.BookState(
        n=int(doc["n"]),
        cash=tuple(D(c) for c in doc["cash"]),
        positions=positions,
        pending=pending,
        next_lifecycle=int(doc["next_lifecycle"]),
        last_session=None if doc["last_session"] is None else d(doc["last_session"]),
    )
    return Book(state, frozenset(int(i) for i in doc["exited_since"]))


def book_names(state: sim.BookState) -> list[int]:
    return sorted({p.instrument_id for p in state.positions} | {e.instrument_id for e in state.pending})


def checkpoint_digest(book_shas: Sequence[str]) -> str:
    """sha256 over ``"<book>:<sha256 of its canonical state>\\n"`` in book order."""
    h = hashlib.sha256()
    for book, sha in enumerate(book_shas):
        h.update(f"{book}:{sha}\n".encode())
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Pure: one book's rebalance (§5.1 applied to the simulator)
# ---------------------------------------------------------------------------
def _dec(x: Fraction) -> Decimal:
    with localcontext(sim.CTX):
        return Decimal(x.numerator) / Decimal(x.denominator)


@dataclass(frozen=True)
class Rebalance:
    """One decided snapshot, decoded once for every book."""

    attempt_id: int
    target_session: date
    last_session: date
    universes: pot.Universes
    #: Every S₀ name's score in the run (``None`` = no score): the controls' donor scores.
    run_scores: Mapping[int, Decimal | None]
    #: Per S₀ name with a valid quote: its half-spread fraction (r3-45).
    half_spread: Mapping[int, Decimal]
    #: Per F_t name: its close on ``last_session`` (the split check's anchor for a pending entry).
    snapshot_close: Mapping[int, Decimal]

    @classmethod
    def of(cls, attempt_id: int, inputs: rb.SnapshotInputs) -> Rebalance:
        universes = rb.universes_of(inputs)
        if isinstance(universes, str):
            raise rb.SnapshotIntegrityError(f"attempt {attempt_id}: a decided snapshot re-derives {universes}")
        spreads = {}
        closes: dict[int, Decimal] = {}
        for f in inputs.facts:
            h = pot.half_spread(f.bid, f.ask)
            if h is not None:
                spreads[f.instrument_id] = _dec(h)
            if f.instrument_id in universes.f_ids:
                if not f.bar_dates or f.bar_dates[-1] != inputs.last_session:
                    raise rb.SnapshotIntegrityError(f"{f.instrument_id}: an F_t name without a last-session bar")
                closes[f.instrument_id] = f.bar_rows[-1]["close"]
        missing = universes.f_ids - spreads.keys()
        if missing:
            raise rb.SnapshotIntegrityError(f"F_t names without a valid quote: {sorted(missing)[:5]}")
        return cls(
            attempt_id=attempt_id,
            target_session=inputs.target_session,
            last_session=inputs.last_session,
            universes=universes,
            run_scores={f.instrument_id: f.total_score for f in inputs.facts},
            half_spread=spreads,
            snapshot_close=closes,
        )

    def order_for(self, donor_of: Mapping[int, int] | None) -> tuple[int, ...]:
        if donor_of is None:
            return pot.real_order(self.universes)
        return pot.control_order(self.universes, donor_of, self.run_scores)

    def missing_donors(self, donor_of: Mapping[int, int]) -> int:
        """§9.2: R members whose donor has no finite positive score this run."""
        count = 0
        for iid in self.universes.r_ids:
            s = self.run_scores.get(donor_of[iid])
            if s is None or not s.is_finite() or s <= 0:
                count += 1
        return count


def apply_decision(book: Book, rebalance: Rebalance, order: Sequence[int]) -> tuple[Book, pot.BookDecision]:
    """§5.1 for one book from its ledger at the snapshot's ``last_session`` (§4 (**)); queues the entries for the
    target session and stamps the exits. ``recently_exited`` is the book's ``exited_since``, which restarts here."""
    st = book.state
    if st.last_session != rebalance.last_session:
        raise ValueError(f"book ledger at {st.last_session}, snapshot at {rebalance.last_session}")
    holdings = [pot.Holding(p.instrument_id, "open", exit_stamped=p.exit_stamp is not None) for p in st.positions] + [
        pot.Holding(e.instrument_id, "entry_pending") for e in st.pending
    ]
    decision = pot.decide(
        rebalance.universes,
        order,
        holdings,
        recently_exited=book.exited_since,
        entries_allowed=True,
        n=st.n,
    )
    exits = {row.instrument_id: row.reason or "" for row in decision.rows if row.action == "exit"}
    entries = [
        sim.PendingEntry(
            instrument_id=iid,
            session=rebalance.target_session,
            h=rebalance.half_spread[iid],
            atr14=_dec(rebalance.universes.atr[iid]),
            snapshot_close=rebalance.snapshot_close[iid],
            snapshot_session=rebalance.last_session,
        )
        for iid in decision.entries
    ]
    state = sim.apply_rebalance(
        st, target_session=rebalance.target_session, exits=exits, entries=entries, h_exit=rebalance.half_spread
    )
    return Book(state, frozenset()), decision


# ---------------------------------------------------------------------------
# Pure: per-session bars and the gate
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SessionBars:
    session: date
    bars: Mapping[int, sim.Bar | None]
    #: name → {session: stored close} over its last ``REFERENCE_SESSIONS`` bars through ``session`` (masked = None).
    closes: Mapping[int, Mapping[date, Decimal | None]]
    spy: sim.Bar | None


def gate_passes(population: Sequence[int], bars: Mapping[int, sim.Bar | None], spy: sim.Bar | None) -> bool:
    if not sim.valid_bar(spy):
        return False
    if not population:
        return True
    valid = sum(1 for iid in population if sim.valid_bar(bars.get(iid)))
    return Fraction(valid, len(population)) >= rb.BAR_COVERAGE_MIN


def reference_closes(
    state: sim.BookState, closes: Mapping[int, Mapping[date, Decimal | None]], used: set[tuple[int, date]]
) -> dict[int, Decimal | None]:
    """The split check's anchors for one book: per held name its ``last_close_session`` close, per pending entry its
    ``snapshot_session`` close, as stored NOW. Records every (name, session) read into ``used``."""
    out: dict[int, Decimal | None] = {}
    for iid, d in [(p.instrument_id, p.last_close_session) for p in state.positions] + [
        (e.instrument_id, e.snapshot_session) for e in state.pending
    ]:
        series = closes.get(iid, {})
        if series and d < min(series):
            raise ValueError(f"{iid}: reference session {d} is older than the kept window")
        out[iid] = series.get(d)
        used.add((iid, d))
    return out


def advance(
    book: Book,
    *,
    session: date,
    bars: SessionBars,
    rebalance: Rebalance | None,
    donor_of: Mapping[int, int] | None,
    wind: bool,
    used: set[tuple[int, date]],
    protective: bool = True,
) -> tuple[Book, sim.StepResult, pot.BookDecision | None]:
    """One book through ``session`` in the spec's order: the decision targeting it (§4 (**)), the wind-down stamps
    (§5.1 rule 3), then the §9.1 step. ``donor_of`` is ``None`` for the shadow and the variant; ``protective`` is
    ``False`` only for the variant."""
    decision: pot.BookDecision | None = None
    if rebalance is not None:
        book, decision = apply_decision(book, rebalance, rebalance.order_for(donor_of))
    state = sim.wind_down(book.state, session=session) if wind else book.state
    result = sim.step(
        state,
        session=session,
        bars=bars.bars,
        reference_closes=reference_closes(state, bars.closes, used),
        protective=protective,
    )
    return Book(result.state, book.exited_since | {c.instrument_id for c in result.closed}), result, decision


# ---------------------------------------------------------------------------
# Pure: per-session outputs
# ---------------------------------------------------------------------------
_CONTROL_FIELDS: Final = (
    "records",
    "sum_return",
    "sum_return_charged",
    "nav",
    "nav_charged",
    "held",
    "closed",
    "missing_exits",
    "refusals",
    "rescales",
    "entered",
    "bought",
    "ineligible_exits",
)
_DECISION_FIELDS: Final = ("entries", "exits", "unfilled", "occupied", "missing_donors")


@dataclass(frozen=True)
class EntryStats:
    """Slice 6c-i's per-session readout inputs (§9.4 "The readout", Storage)."""

    #: Positions whose entry session is this session.
    entered: int
    #: Σ of their ``invested``.
    bought: Decimal
    #: Lifecycles closed this session with an ``ineligible:`` reason.
    ineligible_exits: int

    @classmethod
    def of(cls, result: sim.StepResult, session: date) -> EntryStats:
        new = [p for p in result.state.positions if p.entry_session == session]
        with localcontext(sim.CTX):
            bought = sum((p.invested for p in new), Decimal(0))
        return cls(len(new), bought, sum(1 for c in result.closed if c.reason.startswith("ineligible:")))


@dataclass
class ControlColumns:
    """Books 1..K's per-session aggregates, one list element per control in book order."""

    values: dict[str, list[Any]] = field(default_factory=lambda: {k: [] for k in _CONTROL_FIELDS})
    decisions: dict[str, list[int]] = field(default_factory=lambda: {k: [] for k in _DECISION_FIELDS})

    def add(self, result: sim.StepResult, session: date) -> None:
        charged = sim.liquidation_charged(result.state, result.position_sessions)
        v = self.values
        v["records"].append(len(result.position_sessions))
        v["sum_return"].append(str(session_sum(result.position_sessions)))
        v["sum_return_charged"].append(str(session_sum(charged)))
        v["nav"].append(str(result.nav))
        v["nav_charged"].append(str(sim.charged_nav(result.state)))
        v["held"].append(len(result.state.positions))
        v["closed"].append(len(result.closed))
        v["missing_exits"].append(sum(1 for c in result.closed if c.reason == "missing_bars"))
        v["refusals"].append(len(result.refusals))
        v["rescales"].append(len(result.rescales))
        stats = EntryStats.of(result, session)
        v["entered"].append(stats.entered)
        v["bought"].append(str(stats.bought))
        v["ineligible_exits"].append(stats.ineligible_exits)

    def add_decision(self, decision: pot.BookDecision, missing_donors: int) -> None:
        d = self.decisions
        d["entries"].append(len(decision.entries))
        d["exits"].append(len(decision.exits))
        d["unfilled"].append(decision.slots_unfilled)
        d["occupied"].append(decision.occupied)
        d["missing_donors"].append(missing_donors)

    def doc(self) -> dict[str, Any]:
        out: dict[str, Any] = dict(self.values)
        if self.decisions["entries"]:
            out["decision"] = dict(self.decisions)
        return out


session_sum = sim.session_sum


def _records_json(records: Sequence[sim.PositionSession]) -> list[list[Any]]:
    return [[r.instrument_id, r.lifecycle, str(r.start_value), str(r.end_value)] for r in records]


def shadow_doc(result: sim.StepResult, decision: pot.BookDecision | None, session: date) -> dict[str, Any]:
    stats = EntryStats.of(result, session)
    doc: dict[str, Any] = {
        "nav": str(result.nav),
        "nav_charged": str(sim.charged_nav(result.state)),
        "held": len(result.state.positions),
        "records": _records_json(result.position_sessions),
        "records_charged": _records_json(sim.liquidation_charged(result.state, result.position_sessions)),
        "closed": [
            [
                c.instrument_id,
                c.lifecycle,
                c.slot,
                c.entry_session.isoformat(),
                c.exit_session.isoformat(),
                str(c.entry_fill),
                str(c.exit_fill),
                str(c.invested),
                str(c.proceeds),
                c.reason,
            ]
            for c in result.closed
        ],
        "refusals": [[r.instrument_id, r.reason] for r in result.refusals],
        "rescales": [[iid, str(k)] for iid, k in result.rescales],
        "entered": stats.entered,
        "bought": str(stats.bought),
        "ineligible_exits": stats.ineligible_exits,
    }
    if decision is not None:
        doc["decision"] = {
            "rows": [[r.instrument_id, r.action, r.reason, r.r_rank, r.f_rank] for r in decision.rows],
            "entries": list(decision.entries),
            "exits": list(decision.exits),
            "slots_unfilled": decision.slots_unfilled,
            "occupied": decision.occupied,
        }
    return doc


def _bar_json(bar: sim.Bar | None) -> list[str] | None:
    return None if bar is None else [str(bar.open), str(bar.high), str(bar.low), str(bar.close)]


def inputs_doc(sb: SessionBars, population: Sequence[int], used: set[tuple[int, date]]) -> dict[str, Any]:
    return {
        "session": sb.session.isoformat(),
        "bars": [[iid, _bar_json(sb.bars.get(iid))] for iid in population],
        "reference_closes": [
            [iid, d.isoformat(), None if (c := sb.closes.get(iid, {}).get(d)) is None else str(c)]
            for iid, d in sorted(used)
        ],
        "spy": _bar_json(sb.spy),
    }


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------
def _bar_of(row: Mapping[str, Any]) -> sim.Bar | None:
    o, h, low, c = (row.get(k) for k in ("open", "high", "low", "close"))
    if not (isinstance(o, Decimal) and isinstance(h, Decimal) and isinstance(low, Decimal) and isinstance(c, Decimal)):
        return None
    return sim.Bar(o, h, low, c)


def read_session_bars(conn: Conn, instrument_ids: Sequence[int], session: date) -> SessionBars:
    """Each name's bar for ``session`` and its recent stored closes, through the fail-closed masked reader (the
    snapshot's basis: raw stored prices, quarantined fields masked to ``None`` → a missing bar)."""
    bars: dict[int, sim.Bar | None] = {}
    closes: dict[int, dict[date, Decimal | None]] = {}
    for iid in [*instrument_ids, rb.SPY_INSTRUMENT_ID]:
        series = load_masked_bars(conn, iid).series
        kept = [(d, r) for d, r in zip(series.dates, series.rows, strict=True) if d <= session][-REFERENCE_SESSIONS:]
        # Masked closes stay as None: the kept window is a range of dates, not of usable closes (Codex ckpt-2).
        closes[iid] = {d: r["close"] if isinstance(r["close"], Decimal) else None for d, r in kept}
        hit = next((r for d, r in kept if d == session), None)
        bars[iid] = None if hit is None else _bar_of(hit)
    spy = bars.pop(rb.SPY_INSTRUMENT_ID)
    closes.pop(rb.SPY_INSTRUMENT_ID)
    return SessionBars(session, bars, closes, spy)


def _first_decided(conn: Conn, declaration_id: int) -> tuple[int, str, date] | None:
    row = conn.execute(
        "SELECT attempt_id, snapshot_sha256, (snapshot ->> 'last_session')::date FROM ranking_pot_rebalance_attempts "
        "WHERE declaration_id = %s AND outcome = 'decided' ORDER BY attempt_id LIMIT 1",
        (declaration_id,),
    ).fetchone()
    return None if row is None else (int(row[0]), str(row[1]), row[2])


def _decided_for(conn: Conn, declaration_id: int, session: date) -> Rebalance | None:
    row = conn.execute(
        "SELECT attempt_id, snapshot, snapshot_sha256 FROM ranking_pot_rebalance_attempts "
        "WHERE declaration_id = %s AND outcome = 'decided' AND target_session = %s",
        (declaration_id, session),
    ).fetchone()
    if row is None:
        return None
    if canonical_sha256(row[1]) != row[2]:
        raise rb.SnapshotIntegrityError(f"attempt {row[0]}: the stored snapshot does not hash to its sha256")
    return Rebalance.of(int(row[0]), rb.decode_snapshot(row[1]))


def _unapplied_behind(conn: Conn, declaration_id: int, session: date) -> list[int]:
    rows = conn.execute(
        "SELECT a.attempt_id FROM ranking_pot_rebalance_attempts a "
        "WHERE a.declaration_id = %(d)s AND a.outcome = 'decided' AND a.target_session < %(s)s "
        "AND NOT EXISTS (SELECT 1 FROM ranking_pot_steps s WHERE s.applied_attempt_id = a.attempt_id)",
        {"d": declaration_id, "s": session},
    ).fetchall()
    return [int(r[0]) for r in rows]


def _wind_down_event(conn: Conn, declaration_id: int) -> tuple[int, datetime] | None:
    row = conn.execute(
        "SELECT event_id, at FROM ranking_pot_state_events WHERE declaration_id = %s AND to_state = 'winding_down' "
        "ORDER BY event_id LIMIT 1",
        (declaration_id,),
    ).fetchone()
    return None if row is None else (int(row[0]), row[1])


@dataclass(frozen=True)
class LastStep:
    session: date
    checkpoint_sha256: str
    wind_down_applied: bool


def _last_step(conn: Conn, declaration_id: int) -> LastStep | None:
    row = conn.execute(
        "SELECT session, checkpoint_sha256, "
        "EXISTS (SELECT 1 FROM ranking_pot_steps w "
        "        WHERE w.declaration_id = %(d)s AND w.wind_down_event_id IS NOT NULL) "
        "FROM ranking_pot_steps WHERE declaration_id = %(d)s ORDER BY session DESC LIMIT 1",
        {"d": declaration_id},
    ).fetchone()
    return None if row is None else LastStep(row[0], str(row[1]), bool(row[2]))


@dataclass(frozen=True)
class StoredBook:
    book: int
    value: Book
    #: sha256 of the stored canonical state, for the digest check.
    sha256: str


def _checkpoint_chunks(conn: Conn, declaration_id: int, books: int, at: date) -> Iterator[list[StoredBook]]:
    """Every book's stored checkpoint, ``CHUNK`` at a time, in book order; each must be at ``at`` and its
    ``instrument_ids`` must match its state (the column is the next step's bar population)."""
    for lo in range(0, books, CHUNK):
        hi = min(lo + CHUNK, books)
        with conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute(
                "SELECT book, last_session, state, instrument_ids FROM ranking_pot_book_checkpoints "
                "WHERE declaration_id = %s AND book >= %s AND book < %s ORDER BY book",
                (declaration_id, lo, hi),
            ).fetchall()
        if [r["book"] for r in rows] != list(range(lo, hi)):
            raise rb.SnapshotIntegrityError(f"declaration {declaration_id}: checkpoint rows {lo}..{hi} are incomplete")
        out = []
        for r in rows:
            value = decode_book(r["state"])
            if not (r["last_session"] == value.state.last_session == at):
                raise rb.SnapshotIntegrityError(f"book {r['book']}: checkpoint is not at the last stepped session")
            if sorted(r["instrument_ids"]) != book_names(value.state):
                raise rb.SnapshotIntegrityError(f"book {r['book']}: instrument_ids disagree with the state")
            out.append(StoredBook(int(r["book"]), value, canonical_sha256(r["state"])))
        yield out


def _population(conn: Conn, declaration_id: int, *, variant_book: int) -> tuple[list[int], list[int]]:
    """(the gate's population: every name held or pending in the shadow or a control; every name held or pending
    in the variant only). The variant's names are read and stored but never gated (§9.4, slice 6c-ii-a): a book that
    enters no look must not move when the others step."""
    rows = conn.execute(
        "SELECT DISTINCT unnest(instrument_ids), book = %s FROM ranking_pot_book_checkpoints WHERE declaration_id = %s",
        (variant_book, declaration_id),
    ).fetchall()
    gated = {int(r[0]) for r in rows if not r[1]}
    return sorted(gated), sorted({int(r[0]) for r in rows if r[1]} - gated)


# ---------------------------------------------------------------------------
# Draw
# ---------------------------------------------------------------------------
def control_drawer(
    s0_ids: Sequence[int], *, declaration_sha256: str, first_snapshot_sha256: str
) -> Callable[[int], dict[int, int]]:
    """π_k (§9.2) for one control at a time, recipient → donor. Drawn per book when a rebalance is applied and then
    dropped: K bijections of S₀ held together are ~39M dict entries (Codex ckpt-2)."""
    seed = sim.control_seed(declaration_sha256, first_snapshot_sha256)
    ids = sorted(s0_ids)
    return lambda k: sim.draw_bijection(ids, seed=seed, k=k)


# ---------------------------------------------------------------------------
# One session
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class StepOutcome:
    session: date
    stepped: bool
    note: str
    refusal: str | None = None


def _lock(conn: Conn, declaration_id: int) -> None:
    """The rebalance's lock order (``sql/446``): the state events table (SHARE, before the first query, inside
    REPEATABLE READ: one snapshot for every read of the step), then the declaration row, which serialises two step
    fires."""
    rb.begin_rebalance(conn)
    conn.execute(
        "SELECT 1 FROM ranking_pot_declarations WHERE declaration_id = %s FOR NO KEY UPDATE", (declaration_id,)
    )


def sessions_completed_after(session: date, as_of: datetime) -> int:
    count, d, last = 0, session, latest_completed_us_session(as_of)
    while (d := sim.next_session(d)) <= last:
        count += 1
    return count


def _refuse(
    conn: Conn, declaration_id: int, session: date, as_of: datetime, reason: str, detail: dict[str, Any]
) -> None:
    conn.execute(
        "INSERT INTO ranking_pot_step_refusals (declaration_id, session, fired_at, reason, detail) "
        "VALUES (%s, %s, %s, %s, %s)",
        (declaration_id, session, as_of, reason, Jsonb(detail)),
    )


def step_next_session(
    conn: Conn,
    decl: rb.PotDeclaration,
    *,
    as_of: datetime,
    first: tuple[int, str, date],
    donor: Callable[[int], dict[int, int]],
) -> StepOutcome | None:
    """Step the next session if it is due and its gate passes. Runs inside the caller's transaction. ``None`` = not
    due yet. ``donor(k)`` draws π_k, called per control book only when a rebalance is applied."""
    _lock(conn, decl.declaration_id)
    n, k = book_terms(decl)
    books = sim.book_count(k)
    variant_at = sim.variant_book(k)
    last = _last_step(conn, decl.declaration_id)
    ledger_at = first[2] if last is None else last.session
    session = sim.next_session(ledger_at)
    if not step_due(as_of, session):
        return None
    if not rb._policy_ok(decl):
        _refuse(conn, decl.declaration_id, session, as_of, "policy_drift", {"process": RANKING_POT_POLICY_HASH})
        return StepOutcome(
            session, False, f"{session}: refused policy_drift (the policy hash check failed)", "policy_drift"
        )
    if behind := _unapplied_behind(conn, decl.declaration_id, session):
        raise rb.SnapshotIntegrityError(f"decided attempts {behind} target sessions already stepped without them")

    rebalance = _decided_for(conn, decl.declaration_id, session)
    wd = _wind_down_event(conn, decl.declaration_id)
    wind_session = None if wd is None else wind_down_session(wd[1])
    wind_applied = last is not None and last.wind_down_applied
    if wind_session is not None and wind_session < session and not wind_applied:
        raise rb.SnapshotIntegrityError(f"wind-down session {wind_session} is behind the ledger and was never applied")
    wind = wind_session == session and not wind_applied
    if last is None:
        if rebalance is None or rebalance.attempt_id != first[0]:
            raise rb.SnapshotIntegrityError("the first step must apply the first decided snapshot")
        population, variant_only = [], []
    else:
        population, variant_only = _population(conn, decl.declaration_id, variant_book=variant_at)
    if rebalance is not None and not wind:
        population = sorted(set(population) | rebalance.universes.f_ids)
    read = sorted(set(population) | set(variant_only))

    sb = read_session_bars(conn, read, session)
    forced = False
    if not gate_passes(population, sb.bars, sb.spy):
        valid = sum(1 for iid in population if sim.valid_bar(sb.bars.get(iid)))
        later = sessions_completed_after(session, as_of)
        detail = {"valid": valid, "population": len(population), "spy_valid": sim.valid_bar(sb.spy), "later": later}
        if later < FORCE_AFTER:
            _refuse(conn, decl.declaration_id, session, as_of, "bar_gate", detail)
            return StepOutcome(
                session, False, f"{session}: refused bar_gate ({valid}/{len(population)}, {detail})", "bar_gate"
            )
        forced = True

    def initial() -> Iterator[list[StoredBook]]:
        empty = Book(replace(sim.new_book(n), last_session=ledger_at))
        for lo in range(0, books, CHUNK):
            yield [StoredBook(b, empty, "") for b in range(lo, min(lo + CHUNK, books))]

    chunks = initial() if last is None else _checkpoint_chunks(conn, decl.declaration_id, books, ledger_at)
    used: set[tuple[int, date]] = set()
    columns = ControlColumns()
    shadow: dict[str, Any] | None = None
    variant: dict[str, Any] | None = None
    before: list[str] = []
    shas: list[str] = []
    for chunk in chunks:
        writes: list[tuple[Any, ...]] = []
        for stored_book in chunk:
            b = stored_book.book
            before.append(stored_book.sha256)
            control = b not in (SHADOW_BOOK, variant_at)
            donor_of = donor(b) if rebalance is not None and control else None
            stepped, result, decision = advance(
                stored_book.value,
                session=session,
                bars=sb,
                rebalance=rebalance,
                donor_of=donor_of,
                wind=wind,
                used=used,
                protective=b != variant_at,
            )
            if b == SHADOW_BOOK:
                shadow = shadow_doc(result, decision, session)
            elif b == variant_at:
                variant = shadow_doc(result, decision, session)
            else:
                columns.add(result, session)
                if decision is not None and donor_of is not None and rebalance is not None:
                    columns.add_decision(decision, rebalance.missing_donors(donor_of))
            doc = encode_book(stepped)
            shas.append(canonical_sha256(doc))
            writes.append((decl.declaration_id, b, session, Jsonb(doc), book_names(stepped.state), ledger_at))
        _write_checkpoints(conn, writes, insert=last is None)
    # The digest check: a failure raises and the whole step rolls back.
    if last is not None and checkpoint_digest(before) != last.checkpoint_sha256:
        raise rb.SnapshotIntegrityError(f"declaration {decl.declaration_id}: checkpoints do not match the last step")
    assert shadow is not None and variant is not None
    inputs = inputs_doc(sb, read, used)
    inputs_sha = canonical_sha256(inputs)
    conn.execute(
        "INSERT INTO ranking_pot_steps (declaration_id, session, stepped_at, policy_hash, applied_attempt_id, "
        "wind_down_event_id, forced, inputs, inputs_sha256, shadow, controls, variant, checkpoint_sha256) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            decl.declaration_id,
            session,
            as_of,
            RANKING_POT_POLICY_HASH,
            None if rebalance is None else rebalance.attempt_id,
            wd[0] if wind and wd is not None else None,
            forced,
            Jsonb(inputs),
            inputs_sha,
            Jsonb(shadow),
            Jsonb(columns.doc()),
            Jsonb(variant),
            checkpoint_digest(shas),
        ),
    )
    stored = conn.execute(
        "SELECT inputs FROM ranking_pot_steps WHERE declaration_id = %s AND session = %s",
        (decl.declaration_id, session),
    ).fetchone()
    if stored is None or canonical_sha256(stored[0]) != inputs_sha:
        raise rb.SnapshotIntegrityError(f"{session}: the stored step inputs do not hash to their sha256")
    what = (f", applied attempt {rebalance.attempt_id}" if rebalance is not None else "") + (
        ", forced" if forced else ""
    )
    return StepOutcome(session, True, f"{session}: stepped {books} books, shadow NAV {shadow['nav'][:10]}{what}")


def _write_checkpoints(conn: Conn, writes: Sequence[tuple[Any, ...]], *, insert: bool) -> None:
    with conn.cursor() as cur:
        if insert:
            cur.executemany(
                "INSERT INTO ranking_pot_book_checkpoints (declaration_id, book, last_session, state, instrument_ids) "
                "VALUES (%s, %s, %s, %s, %s)",
                [w[:5] for w in writes],
            )
            return
        for w in writes:
            cur.execute(
                "UPDATE ranking_pot_book_checkpoints SET last_session = %s, state = %s, instrument_ids = %s, "
                "updated_at = now() WHERE declaration_id = %s AND book = %s AND last_session = %s",
                (w[2], w[3], w[4], w[0], w[1], w[5]),
            )
            if cur.rowcount != 1:
                raise rb.SnapshotIntegrityError(f"book {w[1]}: checkpoint moved during the step")


# ---------------------------------------------------------------------------
# The job
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class StepJobResult:
    note: str
    stepped: int = 0


def run_step_job(conn: Conn, *, now: Callable[[], datetime] = lambda: datetime.now(UTC)) -> StepJobResult:
    """One fire: step every due session in order, each in its own transaction, until one is not due or its gate
    fails. ``conn`` must be autocommit."""
    if not conn.autocommit:
        raise RuntimeError("run_step_job needs an autocommit connection")
    decl = rb.load_declaration(conn)
    if decl is None:
        return StepJobResult("no ranking-pot declaration")
    notes: list[str] = []
    # §7.4 wind-down stamps first, before anything below can raise: they are what closes the executed book.
    _stamps(conn, decl, notes)
    first = _first_decided(conn, decl.declaration_id)
    if first is None:
        # No books to step or look at: a trial winding down before its first decision completes at once.
        _complete(conn, decl, now(), notes)
        return StepJobResult("; ".join(notes or ["no decided rebalance yet"]))
    donor = control_drawer(decl.s0_ids, declaration_sha256=decl.doc_sha256, first_snapshot_sha256=first[1])

    stepped = 0
    # §9.3 looks: first repair a look committed without its state event or a step committed without its look.
    _looks(conn, decl, now(), notes)
    while True:
        with conn.transaction():
            outcome = step_next_session(conn, decl, as_of=now(), first=first, donor=donor)
        if outcome is None:
            notes.append("next session not due")
            break
        notes.append(outcome.note)
        logger.info("ranking pot step: %s", outcome.note)
        if outcome.refusal == "policy_drift":
            # Recorded (committed above), then raised: drift is a defect, not a wait, so the job run fails visibly.
            raise RuntimeError(f"ranking pot {decl.declaration_id}: {outcome.note}")
        if not outcome.stepped:
            break
        stepped += 1
        _looks(conn, decl, now(), notes)
    if not rb._policy_ok(decl):
        # No step was due to record the refusal, but a look may have been skipped: drift still fails the run.
        raise RuntimeError(f"ranking pot {decl.declaration_id}: policy drift ({'; '.join(notes)})")
    # §7.4 `completed`: after this fire's looks and steps (this job is their only writer, so none can become due
    # in between).
    _complete(conn, decl, now(), notes)
    return StepJobResult("; ".join(notes), stepped)


def _stamps(conn: Conn, decl: rb.PotDeclaration, notes: list[str]) -> None:
    if written := exits.stamp_wind_down(conn, decl.declaration_id):
        notes.append(f"wind-down stamps {written}")
        logger.info("ranking pot exits: %s wind-down stamps", written)


def _complete(conn: Conn, decl: rb.PotDeclaration, as_of: datetime, notes: list[str]) -> None:
    if (note := exits.complete_if_flat(conn, decl, now=as_of)) is not None:
        notes.append(note)
        logger.info("ranking pot exits: %s", note)


def _looks(conn: Conn, decl: rb.PotDeclaration, as_of: datetime, notes: list[str]) -> None:
    """Reconcile the state, compute every due look (each its own transaction), reconcile again (spec §9.3)."""
    for note in (look.reconcile_state(conn, decl.declaration_id), *look.compute_due_looks(conn, decl, as_of=as_of)):
        if note is not None:
            notes.append(note)
            logger.info("ranking pot look: %s", note)
    if (note := look.reconcile_state(conn, decl.declaration_id)) is not None:
        notes.append(note)
        logger.info("ranking pot look: %s", note)
    # A harm wind-down written just now is stamped in this fire.
    _stamps(conn, decl, notes)


__all__ = [
    "CHUNK",
    "FORCE_AFTER",
    "SHADOW_BOOK",
    "Book",
    "ControlColumns",
    "Rebalance",
    "SessionBars",
    "StepJobResult",
    "StepOutcome",
    "advance",
    "apply_decision",
    "book_names",
    "book_terms",
    "checkpoint_digest",
    "decode_book",
    "control_drawer",
    "encode_book",
    "gate_passes",
    "inputs_doc",
    "read_session_bars",
    "reference_closes",
    "run_step_job",
    "session_sum",
    "sessions_completed_after",
    "shadow_doc",
    "step_due",
    "step_next_session",
    "step_ready_at",
    "wind_down_session",
]
