"""#3592 slice 4a — ranking-pot-v2's step job against real Postgres (spec §4 "Step job"; ``sql/464``; Appendix A 62).

- The first step creates the K + 3 books; the step row stores the v1-reference book beside the shadow, the shadow's
  per-entry reasons, and the controls' missing-donor-DTC counts, under v2's policy hash.
- The reference book's names are gated like the shadow's; only the variant's are read without gating.
- Policy drift is recorded and fails the run WITHOUT decoding the frozen block.
- Shadow-only lifecycle: wind-down is applied by a step and ``completed`` follows with no executed book; a seat
  winding down before its first decision completes at once.
- ``sql/464``: a v2 step row needs its reference, and a v1 step row may not carry one.

Bars, the decided snapshot's universes and the frozen terms are synthetic (``tests/test_ranking_pot_v2_step.py`` covers
the orders and the snapshot replay); the database parts are real.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_sim as sim
from app.services import ranking_pot_v2 as v2
from app.services import ranking_pot_v2_step as st
from app.services.ai_trial_pack import canonical_sha256
from app.services.ranking_pot_freeze import prereg_declaration
from app.services.ranking_pot_v2_policy import RANKING_POT_V2_POLICY_HASH
from app.services.result_ledger import freeze_preregistration
from tests.test_ranking_pot_rebalance_db import _decided, _insert
from tests.test_ranking_pot_schema_db import _move
from tests.test_ranking_pot_step import FLAT, table_of
from tests.test_ranking_pot_v2_step import R5, _rebalance

Conn = psycopg.Connection[Any]
K = 2
THU, FRI, MON = date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 5)
SPY = sim.Bar(Decimal(500), Decimal(501), Decimal(499), Decimal(500))


def _declare(conn: Conn, strategy_id: str = v2.STRATEGY_ID, terms: dict[str, Any] | None = None) -> int:
    """A declaration row (#2599 row, document, genesis event) with S₀ = R5's names and K = 2."""
    if terms is None:
        terms = {"n": 2, "k_controls": K, "book_count": K + 3, "execution": "none"}
    row = conn.execute("SELECT coalesce(max(family_seq), 0) + 1 FROM ranking_pot_declarations").fetchone()
    assert row is not None
    seq = int(row[0])
    doc = {
        "strategy_id": strategy_id,
        "strategy_version": "v1",
        "family": "ranking-pot",
        "family_seq": seq,
        "terms": terms,
        "s0": {"instrument_ids": sorted(R5)},
    }
    sha = canonical_sha256(doc)
    prereg = replace(prereg_declaration(doc_sha256=sha, declared_by="t"), strategy_id=strategy_id)
    decl = freeze_preregistration(cast(psycopg.Connection[tuple], conn), prereg)
    conn.execute(
        "INSERT INTO ranking_pot_declarations "
        "(declaration_id, strategy_id, strategy_version, family, family_seq, doc_path, doc, doc_sha256) "
        "VALUES (%s, %s, 'v1', 'ranking-pot', %s, 'p', %s, %s)",
        (decl, strategy_id, seq, Jsonb(doc), sha),
    )
    conn.execute(
        "INSERT INTO ranking_pot_state_events (declaration_id, from_state, to_state, reason, actor) "
        "VALUES (%s, NULL, 'shadow_only', 'freeze', 'supervisor')",
        (decl,),
    )
    conn.commit()
    return decl


def _at(session: date) -> datetime:
    return datetime.combine(session + timedelta(days=1), time(9, 10), UTC)


def _patch(monkeypatch: pytest.MonkeyPatch, *, terms_calls: list[int]) -> None:
    """Synthetic frozen terms, decided snapshot, bars and characteristics; v2's policy check passes."""
    monkeypatch.setattr(st, "policy_ok", lambda _decl: True)

    def frozen(_decl: rb.PotDeclaration) -> Any:
        terms_calls.append(1)
        return SimpleNamespace(strata=dict.fromkeys(R5, 0), history_floor=date(2020, 1, 1))

    monkeypatch.setattr(st, "frozen_terms", frozen)
    # Name 5: the lowest score, the lowest DTC and a buyer — v2 lifts it, v1's order (the reference) does not.
    reb = _rebalance(R5, set(R5), {1: "9", 2: "8", 3: "7", 4: "6", 5: "1"}, {5})
    monkeypatch.setattr(
        st.Rebalance,
        "of",
        classmethod(
            lambda cls, aid, _doc, **_kw: replace(
                reb,
                attempt_id=aid,
                dtc_read=v2.dtc_read((), s0_ids=R5, as_of=_at(FRI)),
                insider=v2.insider_read((), (), s0_ids=R5, target_session=FRI, as_of=_at(FRI), history_floor=THU),
            )
        ),
    )
    monkeypatch.setattr(st, "table_for", lambda _reb, ids: table_of(set(ids)))

    def bars(_conn: Conn, ids: Any, session: date) -> st.SessionBars:
        closes = {iid: {s: Decimal(100) for s in (THU, FRI, session - timedelta(days=3))} for iid in R5}
        return st.SessionBars(session, {iid: FLAT for iid in ids}, closes, SPY)

    monkeypatch.setattr(st, "read_session_bars", bars)


def _decide(conn: Conn, decl: int) -> int:
    snapshot = {
        "kind": rb.SNAPSHOT_KIND,
        "strategy_id": v2.STRATEGY_ID,
        "target_session": FRI.isoformat(),
        "last_session": THU.isoformat(),
    }
    _insert(
        conn,
        decl,
        target_session=FRI,
        month=FRI.replace(day=1),
        **_decided(snapshot=Jsonb(snapshot), snapshot_sha256=canonical_sha256(snapshot)),
    )
    row = conn.execute("SELECT attempt_id FROM ranking_pot_rebalance_attempts WHERE declaration_id = %s", (decl,))
    attempt = row.fetchone()
    assert attempt is not None
    conn.commit()
    return int(attempt[0])


def test_the_v2_step_keeps_k_plus_3_books_and_completes_shadow_only(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    decl = _declare(conn)
    attempt = _decide(conn, decl)
    conn.autocommit = True
    terms_calls: list[int] = []
    _patch(monkeypatch, terms_calls=terms_calls)

    first = st.run_step_job(conn, now=lambda: _at(FRI))
    assert first.stepped == 1, first.note
    books = conn.execute(
        "SELECT book, instrument_ids FROM ranking_pot_book_checkpoints WHERE declaration_id = %s ORDER BY book",
        (decl,),
    ).fetchall()
    assert [b[0] for b in books] == [0, 1, 2, 3, 4]  # shadow, K = 2 controls, variant, reference
    held = {b[0]: sorted(b[1]) for b in books}
    assert held[0] == held[3] == [1, 5] and held[4] == [1, 2]
    row = conn.execute(
        "SELECT applied_attempt_id, policy_hash, shadow, controls, variant, reference FROM ranking_pot_steps "
        "WHERE declaration_id = %s",
        (decl,),
    ).fetchone()
    assert row is not None
    applied, policy_hash, shadow, controls, variant, reference = row
    assert (applied, policy_hash) == (attempt, RANKING_POT_V2_POLICY_HASH)
    assert shadow["decision"]["entries"] == variant["decision"]["entries"] == [5, 1]
    assert reference["decision"]["entries"] == [1, 2]  # v1's order: the two best scores
    assert [e["instrument_id"] for e in shadow["decision"]["v2_entries"]] == [5, 1]
    assert "v2_entries" not in variant["decision"] and "v2_entries" not in reference["decision"]
    assert len(controls["records"]) == K and len(controls["decision"]["missing_donor_dtc"]) == K
    # The reference is gated like the shadow; only the variant's own names are read without gating.
    gated, variant_only = st._population(conn, decl, variant_book=3)
    assert set(held[4]) <= set(gated) and variant_only == []

    # Drift: the next due session records `policy_drift` and the run fails, and the frozen block is never decoded.
    monkeypatch.setattr(st, "policy_ok", lambda _decl: False)
    calls_before = len(terms_calls)
    with pytest.raises(RuntimeError, match=r"policy_drift .*\(stepped 0 first\)"):
        st.run_step_job(conn, now=lambda: _at(MON))
    assert len(terms_calls) == calls_before
    refusals = conn.execute(
        "SELECT session, reason, detail FROM ranking_pot_step_refusals WHERE declaration_id = %s", (decl,)
    ).fetchall()
    assert refusals == [(MON, "policy_drift", {"process": RANKING_POT_V2_POLICY_HASH})]
    monkeypatch.setattr(st, "policy_ok", lambda _decl: True)

    # Wind-down on Saturday 2026-10-03: W = Monday. The step applies it, every book is flat, and `completed`
    # follows in the same fire — no wind-down stamps, no executed book.
    conn.execute(
        "INSERT INTO ranking_pot_state_events (declaration_id, from_state, to_state, wind_down_reason, reason, actor, "
        "at) VALUES (%s, 'shadow_only', 'winding_down', 'operator', 't', 'operator', %s)",
        (decl, datetime(2026, 10, 3, 12, tzinfo=UTC)),
    )
    done = st.run_step_job(conn, now=lambda: _at(MON))
    assert done.stepped == 1 and done.note.endswith("completed"), done.note
    states = [
        r[0]
        for r in conn.execute(
            "SELECT to_state FROM ranking_pot_state_events WHERE declaration_id = %s ORDER BY event_id", (decl,)
        )
    ]
    assert states == ["shadow_only", "winding_down", "completed"]
    assert st.run_step_job(conn, now=lambda: _at(MON)).note == "no ranking-pot-v2 declaration"


def test_a_seat_winding_down_before_its_first_decision_completes_at_once(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _declare(conn)
    conn.autocommit = True
    assert st.run_step_job(conn).note == "no decided rebalance yet"
    _move(conn, decl, "shadow_only", "winding_down", "operator", "operator")
    assert st.run_step_job(conn).note == "completed"


def test_v1_step_never_sees_v2_and_v2_never_sees_v1(ebull_test_conn: Conn) -> None:
    from app.services import ranking_pot_step as v1_step

    conn = ebull_test_conn
    _declare(conn)
    conn.autocommit = True
    assert v1_step.run_step_job(conn).note == "no ranking-pot declaration"


def _raw_step(conn: Conn, decl: int, reference: Any, session: date = FRI) -> None:
    conn.execute(
        "INSERT INTO ranking_pot_steps (declaration_id, session, stepped_at, policy_hash, forced, inputs, "
        "inputs_sha256, shadow, controls, variant, reference, checkpoint_sha256) "
        "VALUES (%s, %s, now(), %s, FALSE, '{}', %s, '{}', '{}', '{}', %s, %s)",
        (decl, session, "a" * 64, "b" * 64, None if reference is None else Jsonb(reference), "c" * 64),
    )


def test_sql_464_ties_the_reference_column_to_the_layout(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    v2_decl = _declare(conn)
    v1_decl = _declare(conn, "ranking-pot-v1", {"n": 2, "k_controls": K})
    for decl, reference, match in ((v2_decl, None, "reference_missing"), (v1_decl, {}, "reference_unexpected")):
        with pytest.raises(psycopg.errors.RaiseException, match=match):
            _raw_step(conn, decl, reference)
        conn.rollback()
    _raw_step(conn, v2_decl, {})
    _raw_step(conn, v1_decl, None)
    with pytest.raises(psycopg.errors.CheckViolation):
        _raw_step(conn, v2_decl, [], MON)
