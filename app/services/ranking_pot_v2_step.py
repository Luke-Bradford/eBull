"""Ranking-pot-v2 online step job (#3592 slice 4a; spec ``2026-10-03-3592-ranking-pot-v2.md`` §4 "Step job", §5, §6).

A copy of v1's step (``ranking_pot_step.py`` at ``2beb1bcf``, unchanged to ``1644ac72``), not an import: v1's step
imports ``ranking_pot_exits`` (an executed-book module) and ``ranking_pot_job`` (which imports ``ranking_pot_exec``),
which §8 bars from v2's hash. A later v1 fix reaches v2 only by an edit here, which drifts v2's hash.

What differs from v1, and nothing else:

- **Books (§4, sql/463).** 0 the shadow, 1..K the controls, K + 1 the no-SL/TP variant, **K + 2 the v1-reference
  book** (v1's order, ``ranking_pot.real_order``, on v2's own snapshots, with protective exits). The layout comes from
  ``terms.book_count``, validated as K + 3. The reference is gated like the shadow; its step is stored in
  ``ranking_pot_steps.reference`` (sql/464), never in the K-wide control arrays.
- **Orders (§5, §6; Appendix A 60).** The ``Rebalance`` adapter decodes v1's snapshot (``rb.decode_snapshot``, which
  ignores the ``strategy_id`` and ``v2`` keys) plus the ``v2`` block (``ranking_pot_v2.v2_block_of``), and re-derives
  the DTC and insider reads from the block alone at the snapshot's own ``as_of`` with the frozen ``history_floor``: the
  order is a function of the stored snapshot and the declaration, nothing read live. Shadow and variant take
  ``ranking_pot_v2.real_order``; control k takes ``control_order`` under π_k = ``draw_stratified`` over the frozen
  strata (seed = v1's ``control_seed`` of the declaration and first decided snapshot shas); the reference takes
  ``ranking_pot.real_order``.
- **Missing donors (Appendix A 61).** Per control, the R members whose donor has no DTC
  (``ranking_pot_v2.missing_donor_dtc``), stored as ``decision.missing_donor_dtc``: v1's missing-score count has no
  meaning here (each member keeps its own score).
- **Explainability (§5).** The shadow's decision carries, per entry, its three percentiles, the composite, its DTC
  row and settlement date (or the missing reason), and its qualifying purchases with each pair's class and cells.
- **Policy.** v2's hash (``ranking_pot_v2_job.policy_ok``), checked before the frozen terms are decoded, so a drifted
  declaration records ``policy_drift`` rather than raising on a block its drifted decoder reads differently.
- **Shadow-only lifecycle (Appendix A 62).** No executed book (sql/463), so no wind-down stamps and no executed-book
  flatness check: ``completed`` is written once the state is ``winding_down`` and the K + 3 books are flat with the
  wind-down applied, or at once when no rebalance was ever decided.
- **Looks.** Not here yet: slice 4b adds v2's looks (v1's pure evaluation plus conditions 6–7). Until then
  ``BUILD_COMPLETE`` is False and no v2 declaration can be frozen, so this job has nothing to step.

Writes: one ``ranking_pot_steps`` row per stepped session and every book's ``ranking_pot_book_checkpoints`` row, in
one transaction (``sql/447``).

v1's fixes, kept exactly (each closes a v1 spec Appendix A item or a §9.1 clause):

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
  conditional on the previous session; ``sql/447`` fences publication against stepping. v2's policy hash is checked
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
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Any, Final, Literal
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.services import ranking_pot as pot
from app.services import ranking_pot_exposure as ex
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_sim as sim
from app.services import ranking_pot_v2 as v2
from app.services.ai_trial_pack import canonical_sha256
from app.services.market_calendar import latest_completed_us_session
from app.services.price_masked_bars import load_masked_bars
from app.services.ranking_pot_v2_declaration import FrozenTerms, frozen_terms, load_declaration
from app.services.ranking_pot_v2_job import BARS_WAIT_UNTIL, policy_ok
from app.services.ranking_pot_v2_policy import RANKING_POT_V2_POLICY_HASH

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
    """(N, K) as the declaration froze them (§8); its ``book_count`` must be v2's K + 3 (sql/463 enforces it at
    insert; checked again here, so a layout this module does not keep is never stepped)."""
    terms = decl.doc["terms"]
    n, k = int(terms["n"]), int(terms["k_controls"])
    if terms.get("book_count") != book_count(k):
        raise rb.SnapshotIntegrityError(f"declaration {decl.declaration_id}: book_count is not K + 3")
    return n, k


def variant_book(k: int) -> int:
    """The no-SL/TP variant: v1's book number (``ranking_pot_sim.variant_book``)."""
    return sim.variant_book(k)


def reference_book(k: int) -> int:
    """The v1-reference book (§4): after the variant."""
    return k + 2


def book_count(k: int) -> int:
    """Every book v2 keeps: shadow, K controls, the variant and the reference (sql/463's ``terms.book_count``)."""
    return k + 3


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


Order = Literal["v2", "reference"]


@dataclass(frozen=True)
class Rebalance:
    """One decided v2 snapshot, decoded once for every book (Appendix A 60: the order takes the decoded block)."""

    attempt_id: int
    target_session: date
    last_session: date
    universes: pot.Universes
    #: Per S₀ name with a valid quote: its half-spread fraction (r3-45).
    half_spread: Mapping[int, Decimal]
    #: Per F_t name: its close on ``last_session`` (the split check's anchor for a pending entry).
    snapshot_close: Mapping[int, Decimal]
    #: §5, re-derived from the ``v2`` block: S₀ names with a usable DTC at S*, and those with ``ins = 1``.
    dtc: Mapping[int, Fraction]
    buyers: frozenset[int]
    #: The reads behind them, for the shadow's per-entry reasons (§5 "Explainability").
    dtc_read: v2.DtcRead | None = None
    insider: v2.InsiderRead | None = None
    purchases: tuple[v2.InsiderRow, ...] = ()
    #: The decoded snapshot: the exposures' characteristics are computed from it (§9.4, slice 6c-ii-c-1).
    inputs: rb.SnapshotInputs | None = None

    @classmethod
    def of(cls, attempt_id: int, doc: Mapping[str, Any], *, s0_ids: Sequence[int], history_floor: date) -> Rebalance:
        """Decode a stored v2 snapshot: v1's document (``rb.decode_snapshot``) plus the ``v2`` block, whose reads are
        re-derived here at the snapshot's own ``as_of`` — the order depends on the stored bytes and the declaration
        only."""
        if doc.get("strategy_id") != v2.STRATEGY_ID:
            raise rb.SnapshotIntegrityError(f"attempt {attempt_id}: not a {v2.STRATEGY_ID} snapshot")
        inputs = rb.decode_snapshot(doc)
        if inputs.policy_hash != RANKING_POT_V2_POLICY_HASH:
            raise rb.SnapshotIntegrityError(f"attempt {attempt_id}: the snapshot is not under v2's policy hash")
        block = v2.v2_block_of(doc)
        dtc = v2.dtc_read(block.dtc, s0_ids=s0_ids, as_of=inputs.as_of)
        insider = v2.insider_read(
            block.purchases,
            block.history,
            s0_ids=s0_ids,
            target_session=inputs.target_session,
            as_of=inputs.as_of,
            history_floor=history_floor,
        )
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
            half_spread=spreads,
            snapshot_close=closes,
            dtc=dtc.values,
            buyers=insider.buyers,
            dtc_read=dtc,
            insider=insider,
            purchases=block.purchases,
            inputs=inputs,
        )

    def order_for(self, donor_of: Mapping[int, int] | None, *, order: Order = "v2") -> tuple[int, ...]:
        """§5 for the shadow and the variant (``donor_of`` ``None``), §6 for a control, v1's order for the reference."""
        if order == "reference":
            if donor_of is not None:
                raise ValueError("the reference book takes no donor")
            return pot.real_order(self.universes)
        if donor_of is None:
            return v2.real_order(self.universes, self.dtc, self.buyers)
        return v2.control_order(self.universes, donor_of, self.dtc, self.buyers)

    def missing_donors(self, donor_of: Mapping[int, int]) -> int:
        """§6: R members whose donor has no DTC (Appendix A 61, in place of v1's missing-score count)."""
        return v2.missing_donor_dtc(self.universes, donor_of, self.dtc)

    def explain(self, entries: Sequence[int]) -> list[dict[str, Any]]:
        """§5 "Explainability": per shadow entry, its three percentiles and composite (exact, as ``p/q``), its DTC
        row (or the missing reason) and its qualifying purchases with each pair's class and cells."""
        if self.dtc_read is None or self.insider is None:
            raise rb.SnapshotIntegrityError(f"attempt {self.attempt_id}: a rebalance without its v2 reads")
        # Entries are F_t names, and F_t ⊆ R_t by construction (``ranking_pot.universes`` builds both from one loop
        # over the ranking population); the percentiles exist only over R_t, so a breach is refused, not indexed.
        if outside := sorted(set(entries) - self.universes.r_ids):
            raise rb.SnapshotIntegrityError(f"attempt {self.attempt_id}: entries outside R_t {outside[:5]}")
        u = components(self.universes, self.dtc, self.buyers)
        wanted = set(entries)
        by_name: dict[int, list[v2.InsiderRow]] = {}
        for r in self.purchases:
            if r.instrument_id in wanted:
                by_name.setdefault(r.instrument_id, []).append(r)
        out = []
        for iid in entries:
            row = self.dtc_read.chosen.get(iid)
            pairs = sorted(
                {
                    key
                    for r in by_name.get(iid, ())
                    if r.accession in self.insider.qualifying.get(iid, ())
                    and (key := (r.filer_cik or "", r.issuer_cik or "", r.txn_date.year)) in self.insider.pair_class
                    and self.insider.pair_class[key] == "opportunistic"
                }
            )
            out.append(
                {
                    "instrument_id": iid,
                    "u_score": str(u.score[iid]),
                    "u_dtc": str(u.dtc[iid]),
                    "u_ins": str(u.ins[iid]),
                    "composite": str(u.composite[iid]),
                    # The stored value when usable; a chosen but unusable row is ``None`` here, with its reason in
                    # ``dtc_missing`` and its date and source still given below.
                    "dtc": None if iid not in self.dtc or row is None else str(row.days_to_cover),
                    "dtc_missing": self.dtc_read.missing.get(iid),
                    "dtc_settlement_date": None if row is None else row.settlement_date.isoformat(),
                    "dtc_source_document_id": None if row is None else row.source_document_id,
                    "accessions": list(self.insider.qualifying.get(iid, ())),
                    "pairs": [
                        [*key, self.insider.pair_class[key], [list(c) for c in self.insider.pair_cells[key]]]
                        for key in pairs
                    ],
                }
            )
        return out


@dataclass(frozen=True)
class Components:
    score: Mapping[int, Fraction]
    dtc: Mapping[int, Fraction]
    ins: Mapping[int, Fraction]
    composite: Mapping[int, Fraction]


def components(universes: pot.Universes, dtc: Mapping[int, Fraction], buyers: frozenset[int]) -> Components:
    """§2's three percentiles within R_t, as ``ranking_pot_v2.composite_scores`` forms them, with its composite;
    raises if their mean is not that composite (the explanation must be the order's own arithmetic)."""
    members = universes.r_ids
    composite = v2.composite_scores(members, universes.own_score, dtc, buyers)
    u_score = v2.midrank_percentiles({iid: Fraction(universes.own_score[iid]) for iid in members}, members)
    u_dtc = v2.midrank_percentiles({iid: -v for iid, v in dtc.items() if iid in members}, members)
    u_ins = v2.midrank_percentiles({iid: Fraction(int(iid in buyers)) for iid in members}, members)
    if any((u_score[i] + u_dtc[i] + u_ins[i]) / 3 != composite[i] for i in members):
        raise AssertionError("the explained percentiles do not reproduce the composite")
    return Components(u_score, u_dtc, u_ins, composite)


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
    order: Order = "v2",
) -> tuple[Book, sim.StepResult, pot.BookDecision | None]:
    """One book through ``session`` in the spec's order: the decision targeting it (§4 (**)), the wind-down stamps
    (§5.1 rule 3), then the §9.1 step. ``donor_of`` is ``None`` for the shadow, the variant and the reference;
    ``protective`` is ``False`` only for the variant; ``order`` is ``"reference"`` only for the reference."""
    decision: pot.BookDecision | None = None
    if rebalance is not None:
        book, decision = apply_decision(book, rebalance, rebalance.order_for(donor_of, order=order))
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
    "exposure",
)
_DECISION_FIELDS: Final = ("entries", "exits", "unfilled", "occupied", "missing_donor_dtc")


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

    def add(self, result: sim.StepResult, session: date, table: Mapping[int, ex.Characteristic]) -> None:
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
        v["exposure"].append(ex.book_sums(result.state, table).doc())

    def add_decision(self, decision: pot.BookDecision, missing_donor_dtc: int) -> None:
        d = self.decisions
        d["entries"].append(len(decision.entries))
        d["exits"].append(len(decision.exits))
        d["unfilled"].append(decision.slots_unfilled)
        d["occupied"].append(decision.occupied)
        d["missing_donor_dtc"].append(missing_donor_dtc)

    def doc(self) -> dict[str, Any]:
        out: dict[str, Any] = dict(self.values)
        if self.decisions["entries"]:
            out["decision"] = dict(self.decisions)
        return out


session_sum = sim.session_sum


def _records_json(records: Sequence[sim.PositionSession]) -> list[list[Any]]:
    return [[r.instrument_id, r.lifecycle, str(r.start_value), str(r.end_value)] for r in records]


def shadow_doc(
    result: sim.StepResult,
    decision: pot.BookDecision | None,
    session: date,
    table: Mapping[int, ex.Characteristic],
    explain: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """v1's shadow document; for the shadow's own decision, ``explain`` is §5's per-entry reasons
    (``Rebalance.explain``), stored as ``decision.v2_entries``."""
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
        "exposure": ex.book_sums(result.state, table).doc(),
    }
    if decision is not None:
        doc["decision"] = {
            "rows": [[r.instrument_id, r.action, r.reason, r.r_rank, r.f_rank] for r in decision.rows],
            "entries": list(decision.entries),
            "exits": list(decision.exits),
            "slots_unfilled": decision.slots_unfilled,
            "occupied": decision.occupied,
        }
        if explain is not None:
            doc["decision"]["v2_entries"] = explain
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


def _decided_for(conn: Conn, decl: rb.PotDeclaration, terms: FrozenTerms, session: date) -> Rebalance | None:
    row = conn.execute(
        "SELECT attempt_id, snapshot, snapshot_sha256 FROM ranking_pot_rebalance_attempts "
        "WHERE declaration_id = %s AND outcome = 'decided' AND target_session = %s",
        (decl.declaration_id, session),
    ).fetchone()
    if row is None:
        return None
    if canonical_sha256(row[1]) != row[2]:
        raise rb.SnapshotIntegrityError(f"attempt {row[0]}: the stored snapshot does not hash to its sha256")
    return Rebalance.of(int(row[0]), row[1], s0_ids=decl.s0_ids, history_floor=terms.history_floor)


def _unapplied_behind(conn: Conn, declaration_id: int, session: date) -> list[int]:
    rows = conn.execute(
        "SELECT a.attempt_id FROM ranking_pot_rebalance_attempts a "
        "WHERE a.declaration_id = %(d)s AND a.outcome = 'decided' AND a.target_session < %(s)s "
        "AND NOT EXISTS (SELECT 1 FROM ranking_pot_steps s WHERE s.applied_attempt_id = a.attempt_id)",
        {"d": declaration_id, "s": session},
    ).fetchall()
    return [int(r[0]) for r in rows]


def table_for(rebalance: Rebalance, ids: Iterable[int]) -> dict[int, ex.Characteristic]:
    """The applied snapshot's characteristics for ``ids`` (§9.4 exposures)."""
    if rebalance.inputs is None:
        raise rb.SnapshotIntegrityError(f"attempt {rebalance.attempt_id}: a rebalance without its snapshot")
    return ex.characteristics(rebalance.inputs, ids)


def _latest_table(conn: Conn, declaration_id: int) -> dict[int, ex.Characteristic]:
    """The characteristics of the latest applied step row (``sql/455``: stored exactly when an attempt is applied)."""
    row = conn.execute(
        "SELECT characteristics FROM ranking_pot_steps WHERE declaration_id = %s AND applied_attempt_id IS NOT NULL "
        "ORDER BY session DESC LIMIT 1",
        (declaration_id,),
    ).fetchone()
    if row is None or row[0] is None:
        raise rb.SnapshotIntegrityError(f"declaration {declaration_id}: no applied step row carries characteristics")
    return ex.decode_table(row[0])


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
    strata: Mapping[int, int], *, declaration_sha256: str, first_snapshot_sha256: str
) -> Callable[[int], dict[int, int]]:
    """π_k (§6) for one control at a time, recipient → donor: v1's draw within each frozen score stratum
    (``ranking_pot_v2.draw_stratified``), base seed = v1's ``control_seed``. Drawn per book when a rebalance is applied
    and then dropped: K bijections of S₀ held together are ~39M dict entries (v1 Codex ckpt-2)."""
    seed = sim.control_seed(declaration_sha256, first_snapshot_sha256)
    return lambda k: v2.draw_stratified(strata, base_seed=seed, k=k)


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
    terms: FrozenTerms | None,
    donor: Callable[[int], dict[int, int]] | None,
) -> StepOutcome | None:
    """Step the next session if it is due and its gate passes. Runs inside the caller's transaction. ``None`` = not
    due yet. ``donor(k)`` draws π_k, called per control book only when a rebalance is applied. ``terms`` and
    ``donor`` are ``None`` only when the caller found the policy drifted (they come from the frozen block, which is
    decoded only under v2's own hash): a due session then records ``policy_drift``."""
    _lock(conn, decl.declaration_id)
    n, k = book_terms(decl)
    books = book_count(k)
    variant_at, reference_at = variant_book(k), reference_book(k)
    last = _last_step(conn, decl.declaration_id)
    ledger_at = first[2] if last is None else last.session
    session = sim.next_session(ledger_at)
    if not step_due(as_of, session):
        return None
    if not policy_ok(decl) or terms is None or donor is None:
        _refuse(conn, decl.declaration_id, session, as_of, "policy_drift", {"process": RANKING_POT_V2_POLICY_HASH})
        return StepOutcome(
            session, False, f"{session}: refused policy_drift (the policy hash check failed)", "policy_drift"
        )
    if behind := _unapplied_behind(conn, decl.declaration_id, session):
        raise rb.SnapshotIntegrityError(f"decided attempts {behind} target sessions already stepped without them")

    rebalance = _decided_for(conn, decl, terms, session)
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
        # The reference's names are gated as the shadow's are (§4); only the variant's are read without gating.
        population, variant_only = _population(conn, decl.declaration_id, variant_book=variant_at)
    if rebalance is not None and not wind:
        population = sorted(set(population) | rebalance.universes.f_ids)
    read = sorted(set(population) | set(variant_only))
    # §9.4 exposures: an applied rebalance fixes the table for every position held after this step until the next
    # applied target (entries happen only at a target session); any other session reads the latest stored one.
    if rebalance is not None:
        table = table_for(rebalance, rebalance.universes.r_ids | rebalance.universes.f_ids | set(read))
        table_doc: list[list[Any]] | None = ex.encode_table(table)
    else:
        table, table_doc = _latest_table(conn, decl.declaration_id), None

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
    docs: dict[int, dict[str, Any]] = {}
    before: list[str] = []
    shas: list[str] = []
    for chunk in chunks:
        writes: list[tuple[Any, ...]] = []
        for stored_book in chunk:
            b = stored_book.book
            before.append(stored_book.sha256)
            control = b not in (SHADOW_BOOK, variant_at, reference_at)
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
                order="reference" if b == reference_at else "v2",
            )
            if b == SHADOW_BOOK:
                explain = None if decision is None or rebalance is None else rebalance.explain(decision.entries)
                docs[b] = shadow_doc(result, decision, session, table, explain)
            elif not control:
                docs[b] = shadow_doc(result, decision, session, table)
            else:
                columns.add(result, session, table)
                if decision is not None and donor_of is not None and rebalance is not None:
                    columns.add_decision(decision, rebalance.missing_donors(donor_of))
            doc = encode_book(stepped)
            shas.append(canonical_sha256(doc))
            writes.append((decl.declaration_id, b, session, Jsonb(doc), book_names(stepped.state), ledger_at))
        _write_checkpoints(conn, writes, insert=last is None)
    # The digest check: a failure raises and the whole step rolls back.
    if last is not None and checkpoint_digest(before) != last.checkpoint_sha256:
        raise rb.SnapshotIntegrityError(f"declaration {decl.declaration_id}: checkpoints do not match the last step")
    shadow, variant, reference = docs[SHADOW_BOOK], docs[variant_at], docs[reference_at]
    inputs = inputs_doc(sb, read, used)
    inputs_sha = canonical_sha256(inputs)
    conn.execute(
        "INSERT INTO ranking_pot_steps (declaration_id, session, stepped_at, policy_hash, applied_attempt_id, "
        "wind_down_event_id, forced, inputs, inputs_sha256, shadow, controls, variant, reference, checkpoint_sha256, "
        "characteristics) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            decl.declaration_id,
            session,
            as_of,
            RANKING_POT_V2_POLICY_HASH,
            None if rebalance is None else rebalance.attempt_id,
            wd[0] if wind and wd is not None else None,
            forced,
            Jsonb(inputs),
            inputs_sha,
            Jsonb(shadow),
            Jsonb(columns.doc()),
            Jsonb(variant),
            Jsonb(reference),
            checkpoint_digest(shas),
            None if table_doc is None else Jsonb(table_doc),
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
    return StepOutcome(
        session,
        True,
        f"{session}: stepped {books} books, shadow NAV {shadow['nav'][:10]}, "
        f"reference NAV {reference['nav'][:10]}{what}",
    )


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
    decl = load_declaration(conn)
    if decl is None:
        return StepJobResult(f"no {v2.STRATEGY_ID} declaration")
    notes: list[str] = []
    first = _first_decided(conn, decl.declaration_id)
    if first is None:
        # No books to step: a seat winding down before its first decision completes at once.
        _complete(conn, decl, notes)
        return StepJobResult("; ".join(notes or ["no decided rebalance yet"]))
    # v2's hash before the frozen block is decoded (as the rebalance job does): a drifted declaration records
    # `policy_drift` on its next due session instead of raising on a block its drifted decoder may read differently.
    terms = frozen_terms(decl) if policy_ok(decl) else None
    donor = (
        None
        if terms is None
        else control_drawer(terms.strata, declaration_sha256=decl.doc_sha256, first_snapshot_sha256=first[1])
    )

    stepped = 0
    while True:
        with conn.transaction():
            outcome = step_next_session(conn, decl, as_of=now(), first=first, terms=terms, donor=donor)
        if outcome is None:
            notes.append("next session not due")
            break
        notes.append(outcome.note)
        logger.info("ranking pot v2 step: %s", outcome.note)
        if outcome.refusal == "policy_drift":
            # Recorded (committed above), then raised: drift is a defect, not a wait, so the job run fails visibly.
            # The sessions this fire already stepped (each committed under the hash it checked) are named in the
            # error, so the failed run still reports its progress.
            raise RuntimeError(f"ranking pot v2 {decl.declaration_id}: {outcome.note} (stepped {stepped} first)")
        if not outcome.stepped:
            break
        stepped += 1
    if terms is None or not policy_ok(decl):
        # No step was due to record the refusal: drift still fails the run, with this fire's progress in the error.
        # `completed` is deliberately not written under a drifted hash: it waits for the drift to be resolved.
        raise RuntimeError(
            f"ranking pot v2 {decl.declaration_id}: policy drift, stepped {stepped} ({'; '.join(notes)})"
        )
    _complete(conn, decl, notes)
    return StepJobResult("; ".join(notes), stepped)


def books_not_flat(conn: Conn, decl: rb.PotDeclaration) -> str | None:
    """sql/463's completion fence for v2's K + 3 books, read before the insert so an unmet fence is a quiet "not
    yet" (v1's ``ranking_pot_exits._books_not_flat`` with v2's layout). ``None`` = the fence would pass."""
    decided = conn.execute(
        "SELECT EXISTS (SELECT 1 FROM ranking_pot_rebalance_attempts "
        "WHERE declaration_id = %s AND outcome = 'decided')",
        (decl.declaration_id,),
    ).fetchone()
    if decided is None or not decided[0]:
        return None
    row = conn.execute(
        """
        SELECT count(*), max(book),
               count(*) FILTER (WHERE jsonb_array_length(state -> 'positions') > 0
                                   OR jsonb_array_length(state -> 'pending') > 0),
               EXISTS (SELECT 1 FROM ranking_pot_steps s
                        WHERE s.declaration_id = %(d)s
                          AND s.wind_down_event_id = (SELECT min(e.event_id) FROM ranking_pot_state_events e
                                                       WHERE e.declaration_id = %(d)s AND e.to_state = 'winding_down'))
          FROM ranking_pot_book_checkpoints WHERE declaration_id = %(d)s
        """,
        {"d": decl.declaration_id},
    ).fetchone()
    assert row is not None
    books, top, holding, applied = int(row[0]), row[1], int(row[2]), bool(row[3])
    _, k = book_terms(decl)
    # The count and the highest book: with the (declaration, book) key and ``book >= 0``, exactly books 0..K + 2.
    if books != book_count(k) or top != reference_book(k):
        return f"books_incomplete ({books} checkpoints)"
    if holding:
        return f"books_not_flat ({holding} books)"
    if not applied:
        return "wind_down_not_stepped"
    return None


def _complete(conn: Conn, decl: rb.PotDeclaration, notes: list[str]) -> None:
    """Appendix A 62: v2's engine ``completed`` event. No executed book exists (sql/463), so there are no wind-down
    stamps and no executed-book flatness to wait for: only the books' fence."""
    with conn.transaction():
        row = conn.execute(
            "SELECT to_state FROM ranking_pot_state_events WHERE declaration_id = %s ORDER BY event_id DESC LIMIT 1",
            (decl.declaration_id,),
        ).fetchone()
        if row is None or row[0] != "winding_down":
            return
        if (why := books_not_flat(conn, decl)) is not None:
            notes.append(f"not completed: {why}")
            return
        conn.execute(
            "INSERT INTO ranking_pot_state_events (declaration_id, from_state, to_state, reason, actor) "
            "VALUES (%s, 'winding_down', 'completed', %s, 'engine')",
            (decl.declaration_id, "v2 §4: shadow only, every book flat"),
        )
    notes.append("completed")
    logger.info("ranking pot v2: completed")


__all__ = [
    "CHUNK",
    "FORCE_AFTER",
    "SHADOW_BOOK",
    "Book",
    "Components",
    "ControlColumns",
    "Order",
    "Rebalance",
    "SessionBars",
    "StepJobResult",
    "StepOutcome",
    "advance",
    "apply_decision",
    "book_count",
    "book_names",
    "book_terms",
    "books_not_flat",
    "checkpoint_digest",
    "components",
    "decode_book",
    "control_drawer",
    "encode_book",
    "gate_passes",
    "inputs_doc",
    "read_session_bars",
    "reference_book",
    "reference_closes",
    "run_step_job",
    "session_sum",
    "sessions_completed_after",
    "shadow_doc",
    "step_due",
    "step_next_session",
    "step_ready_at",
    "variant_book",
    "wind_down_session",
]
