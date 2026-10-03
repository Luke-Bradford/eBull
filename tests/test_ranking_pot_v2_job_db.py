"""#3592 slice 3c-ii — ranking-pot-v2's rebalance job against real Postgres (spec §4; Appendix A S2-a).

- Isolation: v1's job never acts on v2's declaration and v2's never on v1's.
- The pre-score factor gates refuse without scoring (``dtc_incomplete``, ``insider_history_floor_moved``), as gate
  verdicts (no ``pre_score`` flag), under v2's policy hash.
- Past them, the bar wait and the snapshot transaction run as v1's do, and the snapshot re-runs the factor gates at its
  own ``as_of`` before v1's gates.
- The factor read under its savepoint: a failing read refuses ``insider_read_failed`` and the transaction lives on.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, cast

import psycopg
import pytest

from app.services import ranking_pot_job as v1_job
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_v2 as v2
from app.services import ranking_pot_v2_job as job
from app.services import ranking_pot_v2_policy
from app.services.ai_trial_pack import canonical_sha256
from app.services.market_calendar import latest_completed_us_session, us_market_status
from app.services.ranking_pot import Universes
from app.services.ranking_pot_freeze_v2 import freeze_pot_v2
from app.services.ranking_pot_v2_declaration import frozen_terms, load_declaration
from tests.test_ranking_pot_freeze_v2_db import IDS, NOW, _seed_history
from tests.test_ranking_pot_schema_db import PROVENANCE, SCORED_AT, _frozen, _seed_scores
from tests.test_ranking_pot_v2_inputs_db import _dtc

Conn = psycopg.Connection[Any]


@pytest.fixture(autouse=True)
def _build_complete(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(ranking_pot_v2_policy, "BUILD_COMPLETE", True)
    yield


def _iso(ts: datetime) -> str:
    return ts.astimezone(UTC).replace(tzinfo=None).isoformat()


def _freeze_v2(conn: Conn) -> rb.PotDeclaration:
    """v2 frozen on three names. DTC rows at two settlements, both observed long before any time a test reads at: one
    fresh at the real clock (the freeze and the pre-score gate read at ``transaction_timestamp()``) and one 30 days
    on, so the snapshot's own ``as_of`` — the first window after the freeze, up to ~5 weeks away — also finds an S*
    within 31 days."""
    _seed_scores(conn, IDS)
    _seed_history(conn)
    known = _iso(NOW - timedelta(days=60))
    for iid in IDS:
        for k, days in enumerate((-10, 20)):
            _dtc(conn, iid, (NOW.date() + timedelta(days=days)).isoformat(), known, "2.50", 50_000, doc=f"s{k}")
    conn.commit()
    report = freeze_pot_v2(conn, provenance=PROVENANCE, apply=True, declared_by="supervisor")
    assert (report.refusals, report.applied) == ((), True)
    decl = load_declaration(conn)
    conn.commit()
    assert decl is not None
    return decl


def _first_window(decl: rb.PotDeclaration) -> date:
    """The completed session D whose window decides the declaration's first month (v1's test helper, for v2)."""
    s1 = rb.first_month(decl.frozen_at)
    while us_market_status(s1) == "closed":
        s1 += timedelta(days=1)
    return latest_completed_us_session(datetime.combine(s1, time(12), UTC))


def _attempts(conn: Conn) -> list[tuple[str, str | None, str, dict[str, Any] | None]]:
    rows = conn.execute(
        "SELECT outcome, refusal, policy_hash, detail FROM ranking_pot_rebalance_attempts ORDER BY attempt_id"
    ).fetchall()
    return [(r[0], r[1], r[2], r[3]) for r in rows]


def _never_scores() -> job.ScoringRun:
    raise AssertionError("must not score")


def test_each_job_acts_only_on_its_own_declaration(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _frozen(conn)  # v1 only
    conn.autocommit = True
    assert job.run_rebalance_job(conn, score=_never_scores).note == "no ranking-pot-v2 declaration"


def test_v1_job_never_sees_v2(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _freeze_v2(conn)
    conn.autocommit = True

    def never() -> v1_job.ScoringRun:
        raise AssertionError("must not score")

    assert v1_job.run_rebalance_job(conn, score=never).note == "no ranking-pot declaration"


def test_pre_score_gates_refuse_without_scoring(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _freeze_v2(conn)
    d = _first_window(decl)
    waiting = lambda: datetime.combine(d, time(23, 50), UTC)  # noqa: E731

    # FINRA's N/A on every name at the S* the real clock sees: fewer usable than 95% of the frozen baseline.
    conn.execute(
        "UPDATE finra_short_interest_observations SET average_daily_volume = 0 WHERE source_document_id = 's0'"
    )
    conn.commit()
    conn.autocommit = True
    first = job.run_rebalance_job(conn, score=_never_scores, now=waiting)
    assert first.note.endswith("refused dtc_incomplete")
    [(outcome, refusal, policy_hash, detail)] = _attempts(conn)
    assert (outcome, refusal, policy_hash) == (
        "refused",
        "dtc_incomplete",
        ranking_pot_v2_policy.RANKING_POT_V2_POLICY_HASH,
    )
    assert detail is not None and (detail["dtc_usable"], detail["dtc_baseline"]) == (0, len(IDS))
    assert "pre_score" not in detail  # a gate verdict, not a bar wait

    # Usable again, but the insider corpus wiped: every guarded month below 95% of its frozen count.
    conn.execute("UPDATE finra_short_interest_observations SET average_daily_volume = 50000")
    conn.execute("DELETE FROM insider_transactions")
    second = job.run_rebalance_job(conn, score=_never_scores, now=waiting)
    assert second.note.endswith("refused insider_history_floor_moved")
    detail = _attempts(conn)[-1][3]
    assert detail is not None and detail["floor_moved"] and all(live == 0 for _, live, _ in detail["floor_moved"])
    assert rb.read_history(conn, decl.declaration_id).last_refusal == {
        rb.plan(waiting(), first=rb.first_month(decl.frozen_at), resolved=frozenset()).month: (
            "insider_history_floor_moved"
        )
    }


def test_past_the_gates_the_fire_waits_then_the_snapshot_re_gates(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _freeze_v2(conn)
    d = _first_window(decl)
    conn.autocommit = True

    # Waiting phase, no bars for D: v1's recorded wait.
    waited = job.run_rebalance_job(conn, score=_never_scores, now=lambda: datetime.combine(d, time(23, 50), UTC))
    assert "0/3 S0 bars landed" in waited.note
    assert _attempts(conn)[-1][1:] == (
        "price_daily_stale",
        ranking_pot_v2_policy.RANKING_POT_V2_POLICY_HASH,
        {"pre_score": True, "barred": 0, "s0_tradable": 3},
    )

    # Final phase: scored, and the snapshot transaction runs the factor gates at its own as_of before v1's gates.
    final = lambda: datetime.combine(d + timedelta(days=1), time(11, 50), UTC)  # noqa: E731
    refused = job.run_rebalance_job(conn, score=lambda: job.ScoringRun(SCORED_AT, {}), now=final)
    assert refused.note.endswith("refused price_daily_stale")
    detail = _attempts(conn)[-1][3]
    assert detail is not None
    assert (detail["s0_tradable"], detail["scored"], detail["barred"]) == (3, 3, 0)
    assert (detail["dtc_usable"], detail["dtc_baseline"]) == (3, 3)

    # A scorer failure is recorded as v1 records it.
    def boom() -> job.ScoringRun:
        raise RuntimeError("scorer down")

    assert job.run_rebalance_job(conn, score=boom, now=final).note.endswith("refused scoring_failed")


def test_a_failing_factor_read_refuses_and_the_transaction_lives_on(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    decl = _freeze_v2(conn)
    terms = frozen_terms(decl)
    target = rb.target_session(NOW)
    with conn.transaction():
        ts = conn.execute("SELECT transaction_timestamp()").fetchone()
        assert ts is not None
        gate = job.factor_gates(conn, decl, terms, target_session=target, as_of=ts[0])
        assert gate.refusal is None and len(gate.dtc_rows) == len(IDS)
        ok = job.read_factors(conn, decl, terms, gate, target_session=target, as_of=ts[0])
        assert not isinstance(ok, job.Refused)
        block, ins = ok
        assert block.dtc == gate.dtc_rows and ins.buyers == frozenset()

        def broken(*_: Any, **__: Any) -> Any:
            conn.execute("SELECT 1/0")

        monkeypatch.setattr(job.reader, "read_purchases", broken)
        failed = job.read_factors(conn, decl, terms, gate, target_session=target, as_of=ts[0])
        assert isinstance(failed, job.Refused) and failed.refusal == "insider_read_failed"
        assert failed.detail["error"] == "DivisionByZero"
        assert conn.execute("SELECT 1").fetchone() == (1,)  # the savepoint kept the transaction usable


def _prepared(due: rb.DuePlan, **over: Any) -> rb.Prepared:
    snapshot = {
        "kind": rb.SNAPSHOT_KIND,
        "strategy_id": v2.STRATEGY_ID,
        "target_session": due.target_session.isoformat(),
        "names": [],
        "v2": {},
    }
    fields: dict[str, Any] = {
        "snapshot": snapshot,
        "snapshot_sha256": canonical_sha256(snapshot),
        "universes": cast(Universes, None),
        "detail": {"r_count": 1650, "f_count": 718},
    }
    return rb.Prepared(**(fields | over))


def test_record_decided_writes_v2_rows_only(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _freeze_v2(conn)
    d = _first_window(decl)
    conn.autocommit = True
    as_of = datetime.combine(d, time(23, 50), UTC)
    due = rb.plan(as_of, first=rb.first_month(decl.frozen_at), resolved=frozenset())
    assert due.due

    v1_doc = {k: v for k, v in _prepared(due).snapshot.items() if k not in ("strategy_id", "v2")}
    with pytest.raises(ValueError, match="ranking-pot-v2"), conn.transaction():
        rb.begin_rebalance(conn)
        job.record_decided(
            conn,
            decl,
            due,
            _prepared(due, snapshot=v1_doc, snapshot_sha256=canonical_sha256(v1_doc)),
            as_of=as_of,
            scored_at=SCORED_AT,
        )
    with pytest.raises(rb.SnapshotIntegrityError), conn.transaction():
        rb.begin_rebalance(conn)
        job.record_decided(conn, decl, due, _prepared(due, snapshot_sha256="c" * 64), as_of=as_of, scored_at=SCORED_AT)
    assert _attempts(conn) == []

    with conn.transaction():
        rb.begin_rebalance(conn)
        attempt = job.record_decided(conn, decl, due, _prepared(due), as_of=as_of, scored_at=SCORED_AT)
    assert attempt > 0
    assert [a[:3] for a in _attempts(conn)] == [("decided", None, ranking_pot_v2_policy.RANKING_POT_V2_POLICY_HASH)]
    again = job.run_rebalance_job(conn, score=_never_scores, now=lambda: as_of)
    assert again.note.endswith("not due")
