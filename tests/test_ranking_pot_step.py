"""#2842 slice 6a — the online step job's pure parts (spec §9.1 "The step job", §4 (**))."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from fractions import Fraction

import pytest

from app.services import ranking_pot as pot
from app.services import ranking_pot_exposure as ex
from app.services import ranking_pot_sim as sim
from app.services import ranking_pot_step as st
from app.services.ai_trial_pack import canonical_sha256

D = Decimal
THU, FRI, MON, TUE = date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 5), date(2026, 10, 6)
#: A characteristics table for names 1..3 (§9.4 exposures): 2 has no beta, 3 has no sector, size or ATR%.
TABLE = {
    1: ex.Characteristic("XLK", D(20), D("1.5"), None, D("0.02")),
    2: ex.Characteristic("XLF", D(22), None, "too_few_pairs", D("0.04")),
    3: ex.Characteristic(ex.NO_SECTOR, None, D("0.5"), None, None),
}


def table_of(ids: set[int]) -> dict[int, ex.Characteristic]:
    """A table covering any ids: ``TABLE``'s rows where it has them, else a name with only a sector."""
    return {i: TABLE.get(i, ex.Characteristic("XLE", None, None, "too_few_pairs", None)) for i in ids}


def _universes(r: dict[int, str], f: set[int]) -> pot.Universes:
    """R with each name's own score; every name in F has ATR 1."""
    return pot.Universes(
        breakpoint=D(1),
        max_cut=Fraction(1, 10),
        r_ids=frozenset(r),
        f_ids=frozenset(f),
        hold_failure={},
        entry_failure={iid: "quote_ineligible" for iid in r if iid not in f},
        own_score={iid: D(s) for iid, s in r.items()},
        max_return={},
        atr={iid: Fraction(1) for iid in f},
        max_population=0,
        nyse_cap_population=0,
    )


def _rebalance(target: date, last: date, r: dict[int, str], f: set[int], run: dict[int, str | None] | None = None):
    scores = run if run is not None else dict(r)
    return st.Rebalance(
        attempt_id=7,
        target_session=target,
        last_session=last,
        universes=_universes(r, f),
        run_scores={iid: None if s is None else D(s) for iid, s in scores.items()},
        half_spread={iid: D("0.001") for iid in r},
        snapshot_close={iid: D(100) for iid in f},
    )


def _bars(session: date, bars: dict[int, sim.Bar | None], closes: dict[int, dict[date, Decimal]] | None = None):
    return st.SessionBars(session, bars, closes or {}, sim.Bar(D(500), D(501), D(499), D(500)))


FLAT = sim.Bar(D(100), D(101), D(99), D(100))


def _book(n: int = 2, at: date = THU) -> st.Book:
    return st.Book(replace(sim.new_book(n), last_session=at))


def test_a_rebalance_is_applied_before_its_target_sessions_step_and_exits_block_reentry() -> None:
    r = {1: "0.9", 2: "0.8", 3: "0.7"}
    used: set[tuple[int, date]] = set()
    book, result, decision = st.advance(
        _book(),
        session=FRI,
        bars=_bars(FRI, {1: FLAT, 2: FLAT}, {1: {THU: D(100)}, 2: {THU: D(100)}}),
        rebalance=_rebalance(FRI, THU, r, {1, 2, 3}),
        donor_of=None,
        wind=False,
        used=used,
    )
    assert decision is not None and decision.entries == (1, 2)
    assert book.state.held() == {1, 2} and len(result.position_sessions) == 2
    assert used == {(1, THU), (2, THU)}  # the pending entries' split anchors were read

    # Monday: name 1 gaps below its 3-ATR stop and exits; it is recorded as exited since the rebalance.
    gap = sim.Bar(D(96), D(97), D(95), D(96))
    book, result, _ = st.advance(
        book,
        session=MON,
        bars=_bars(MON, {1: gap, 2: FLAT}, {1: {FRI: D(100)}, 2: {FRI: D(100)}}),
        rebalance=None,
        donor_of=None,
        wind=False,
        used=set(),
    )
    assert [c.reason for c in result.closed] == ["stop_loss_gap"] and book.exited_since == {1}

    # The next rebalance: 2 holds, 1 is not re-entered (r3-102); 3 is F-rank 3 > N, so the slot stays cash
    # (r3-103). exited_since restarts.
    book, _, decision = st.advance(
        book,
        session=TUE,
        bars=_bars(TUE, {2: FLAT, 3: FLAT}, {2: {MON: D(100)}, 3: {MON: D(100)}}),
        rebalance=_rebalance(TUE, MON, r, {1, 2, 3}),
        donor_of=None,
        wind=False,
        used=set(),
    )
    assert decision is not None
    rows = {row.instrument_id: (row.action, row.reason) for row in decision.rows}
    assert rows == {1: ("not_selected", "recently_exited"), 2: ("hold", None), 3: ("not_selected", "frank_out_of_band")}
    assert decision.slots_unfilled == 1
    assert book.state.held() == {2} and book.exited_since == frozenset()


def test_a_control_ranks_by_its_donors_scores_and_counts_missing_donors() -> None:
    r = {1: "0.9", 2: "0.8", 3: "0.7"}
    rebalance = _rebalance(FRI, THU, r, {1, 2, 3}, run={1: "0.9", 2: "0.8", 3: "0.7", 4: None})
    donor_of = {1: 4, 2: 1, 3: 2, 4: 3}  # 1 takes 4's (missing) score, so it sorts last
    assert rebalance.order_for(donor_of) == (2, 3, 1)
    assert rebalance.missing_donors(donor_of) == 1
    _, _, decision = st.advance(
        _book(),
        session=FRI,
        bars=_bars(FRI, {2: FLAT, 3: FLAT}, {2: {THU: D(100)}, 3: {THU: D(100)}}),
        rebalance=rebalance,
        donor_of=donor_of,
        wind=False,
        used=set(),
    )
    assert decision is not None and decision.entries == (2, 3)


def test_wind_down_after_a_same_session_decision_drops_its_entries_and_closes_everything() -> None:
    r = {1: "0.9", 2: "0.8", 3: "0.7"}
    book, _, _ = st.advance(
        _book(),
        session=FRI,
        bars=_bars(FRI, {1: FLAT, 2: FLAT}, {1: {THU: D(100)}, 2: {THU: D(100)}}),
        rebalance=_rebalance(FRI, THU, r, {1, 2, 3}),
        donor_of=None,
        wind=False,
        used=set(),
    )
    book, result, decision = st.advance(
        book,
        session=MON,
        bars=_bars(MON, {1: FLAT, 2: FLAT, 3: FLAT}, {1: {FRI: D(100)}, 2: {FRI: D(100)}, 3: {FRI: D(100)}}),
        rebalance=_rebalance(MON, FRI, {1: "0.9", 3: "0.8", 2: "0.1"}, {1, 2, 3}),
        donor_of=None,
        wind=True,
        used=set(),
    )
    assert decision is not None
    assert book.state.positions == () and book.state.pending == ()
    assert sorted(c.reason for c in result.closed) == ["wind_down", "wind_down"]


def test_apply_decision_needs_the_ledger_at_the_snapshots_last_session() -> None:
    with pytest.raises(ValueError, match="ledger at"):
        st.apply_decision(_book(at=MON), _rebalance(FRI, THU, {1: "0.9"}, {1}), (1,))


def test_book_codec_round_trips_exactly() -> None:
    r = {1: "0.9", 2: "0.8", 3: "0.7"}
    book, _, _ = st.advance(
        _book(),
        session=FRI,
        bars=_bars(FRI, {1: FLAT, 2: FLAT}, {1: {THU: D(100)}, 2: {THU: D(100)}}),
        rebalance=_rebalance(FRI, THU, r, {1, 2, 3}),
        donor_of=None,
        wind=False,
        used=set(),
    )
    stamped = st.Book(
        sim.apply_rebalance(
            book.state,
            target_session=MON,
            exits={1: "rerank_out_of_band"},
            entries=[sim.PendingEntry(3, MON, D("0.002"), D("1.5"), D(100), FRI)],
            h_exit={},
        ),
        frozenset({9}),
    )
    doc = st.encode_book(stamped)
    assert st.decode_book(doc) == stamped
    assert canonical_sha256(st.encode_book(st.decode_book(doc))) == canonical_sha256(doc)
    assert st.book_names(stamped.state) == [1, 2, 3]


def test_gate_is_inclusive_needs_spy_and_passes_an_empty_population() -> None:
    spy = sim.Bar(D(500), D(501), D(499), D(500))
    pop = list(range(20))
    bars: dict[int, sim.Bar | None] = {iid: FLAT for iid in range(19)}
    assert st.gate_passes(pop, bars, spy)  # 19/20 = 95%
    bars[18] = sim.Bar(D(0), D(101), D(99), D(100))  # a zero open is not a valid bar
    assert not st.gate_passes(pop, bars, spy)
    assert st.gate_passes([], {}, spy)
    assert not st.gate_passes([], {}, None)


def test_timing_wind_down_session_and_forcing_count() -> None:
    assert st.step_ready_at(FRI) == datetime(2026, 10, 3, 9, 0, tzinfo=UTC)
    assert not st.step_due(datetime(2026, 10, 3, 8, 59, tzinfo=UTC), FRI)
    assert st.step_due(datetime(2026, 10, 3, 9, 10, tzinfo=UTC), FRI)
    assert not st.step_due(datetime(2026, 10, 2, 21, 0, tzinfo=UTC), FRI)  # the session's own day
    # 03:00 UTC Tuesday is Monday in New York: W is Tuesday. 15:00 UTC Tuesday: W is Wednesday.
    assert st.wind_down_session(datetime(2026, 10, 6, 3, 0, tzinfo=UTC)) == TUE
    assert st.wind_down_session(datetime(2026, 10, 6, 15, 0, tzinfo=UTC)) == date(2026, 10, 7)
    assert st.sessions_completed_after(FRI, datetime(2026, 10, 7, 9, 0, tzinfo=UTC)) == 2  # Mon, Tue


def test_control_columns_store_the_endpoint_charged_sum_beside_the_plain_one() -> None:
    book, result, _ = st.advance(
        _book(),
        session=FRI,
        bars=_bars(FRI, {1: FLAT}, {1: {THU: D(100)}}),
        rebalance=_rebalance(FRI, THU, {1: "0.9"}, {1}),
        donor_of=None,
        wind=False,
        used=set(),
    )
    cols = st.ControlColumns()
    cols.add(result, FRI, TABLE)
    doc = cols.doc()
    assert doc["records"] == [1] and doc["held"] == [1] and "decision" not in doc
    plain, charged = D(doc["sum_return"][0]), D(doc["sum_return_charged"][0])
    assert charged < plain < 0  # entry cost, then the open position is charged its exit half-spread too
    assert D(doc["nav_charged"][0]) < D(doc["nav"][0])
    assert st.session_sum(result.position_sessions) == plain


def test_reference_closes_record_what_was_read_and_refuse_an_unkept_session() -> None:
    book, _, _ = st.advance(
        _book(),
        session=FRI,
        bars=_bars(FRI, {1: FLAT}, {1: {THU: D(100)}}),
        rebalance=_rebalance(FRI, THU, {1: "0.9"}, {1}),
        donor_of=None,
        wind=False,
        used=set(),
    )
    used: set[tuple[int, date]] = set()
    assert st.reference_closes(book.state, {1: {FRI: D(100)}}, used) == {1: D(100)}
    assert st.reference_closes(book.state, {}, used) == {1: None}  # masked or absent: no split check
    assert used == {(1, FRI)}
    with pytest.raises(ValueError, match="kept window"):
        st.reference_closes(book.state, {1: {MON: D(100)}}, set())
    # A masked reference close inside the kept window is None, not an expired window (Codex ckpt-2).
    assert st.reference_closes(book.state, {1: {FRI: None, MON: D(100)}}, set()) == {1: None}


def test_the_control_drawer_is_the_simulators_draw_one_control_at_a_time() -> None:
    donor = st.control_drawer([3, 1, 2], declaration_sha256="a" * 64, first_snapshot_sha256="b" * 64)
    seed = sim.control_seed("a" * 64, "b" * 64)
    assert donor(5) == sim.draw_bijection([1, 2, 3], seed=seed, k=5)


def test_checkpoint_digest_is_order_sensitive() -> None:
    assert st.checkpoint_digest(["a", "b"]) != st.checkpoint_digest(["b", "a"])
