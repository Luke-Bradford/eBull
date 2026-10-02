"""#2842 slice 4b-ii — the rebalance job and the ``decided`` writer against real Postgres (spec §4)."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from typing import Any, cast

import psycopg
import pytest

from app.services import ranking_pot_job as job
from app.services import ranking_pot_rebalance as rb
from app.services.ai_trial_pack import canonical_sha256
from app.services.market_calendar import latest_completed_us_session, us_market_status
from app.services.ranking_pot import Universes
from tests.test_ranking_pot_schema_db import SCORED_AT, _frozen, _move

Conn = psycopg.Connection[Any]


def _first_window(conn: Conn) -> tuple[date, rb.PotDeclaration]:
    """The completed session D whose window decides the declaration's first month (its first session)."""
    decl = rb.load_declaration(conn)
    assert decl is not None
    first = rb.first_month(decl.frozen_at)
    s1 = first
    while us_market_status(s1) == "closed":
        s1 += timedelta(days=1)
    return latest_completed_us_session(datetime.combine(s1, time(12), UTC)), decl


def _attempts(conn: Conn) -> list[tuple[str, str | None]]:
    return [
        (r[0], r[1])
        for r in conn.execute(
            "SELECT outcome, refusal FROM ranking_pot_rebalance_attempts ORDER BY attempt_id"
        ).fetchall()
    ]


def _never_scores() -> job.ScoringRun:
    raise AssertionError("must not score")


def test_no_declaration_is_a_no_op(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    conn.autocommit = True
    assert job.run_rebalance_job(conn, score=_never_scores).note == "no ranking-pot declaration"


def test_waits_for_bars_then_records_the_final_fires_refusals(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _frozen(conn)
    conn.autocommit = True
    d, _ = _first_window(conn)
    nxt = d + timedelta(days=1)

    # Waiting phase, no bars for D: no score, but the fire's refusal is recorded (r3-88).
    waiting = job.run_rebalance_job(conn, score=_never_scores, now=lambda: datetime.combine(d, time(23, 40), UTC))
    assert "0/3 S0 bars landed" in waiting.note and _attempts(conn) == [("refused", "price_daily_stale")]
    detail = conn.execute(
        "SELECT detail FROM ranking_pot_rebalance_attempts WHERE attempt_id = %s", (waiting.attempt_id,)
    ).fetchone()
    assert detail is not None and detail[0] == {"pre_score": True, "barred": 0, "s0_tradable": 3}

    # Final phase: the fire proceeds. A failing scorer is recorded as scoring_failed.
    def boom() -> job.ScoringRun:
        raise RuntimeError("scorer down")

    final = lambda: datetime.combine(nxt, time(11, 40), UTC)  # noqa: E731
    failed = job.run_rebalance_job(conn, score=boom, now=final)
    assert failed.attempt_id is not None and _attempts(conn)[-1] == ("refused", "scoring_failed")

    # A scored run with no bars: the gate's own refusal, with its counts and the consumed run recorded.
    refused = job.run_rebalance_job(conn, score=lambda: job.ScoringRun(SCORED_AT, {}), now=final)
    assert refused.note.endswith("refused price_daily_stale")
    row = conn.execute(
        "SELECT refusal, scored_at, detail FROM ranking_pot_rebalance_attempts WHERE attempt_id = %s",
        (refused.attempt_id,),
    ).fetchone()
    assert row is not None
    assert (row[0], row[1]) == ("price_daily_stale", SCORED_AT)
    assert (row[2]["s0_tradable"], row[2]["scored"], row[2]["barred"]) == (3, 3, 0)

    # Outside the window: nothing.
    closed = job.run_rebalance_job(conn, score=_never_scores, now=lambda: datetime.combine(nxt, time(12, 0), UTC))
    assert "outside the decision window" in closed.note and len(_attempts(conn)) == 3


def test_downtime_skips_are_written_by_any_fire(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _frozen(conn)
    conn.autocommit = True
    d, decl = _first_window(conn)
    # Two months later, mid-month and outside the window: both earlier months close as not_attempted.
    later = datetime.combine(d + timedelta(days=75), time(15, 0), UTC)
    result = job.run_rebalance_job(conn, score=_never_scores, now=lambda: later)
    assert result.skipped_months >= 2
    assert set(_attempts(conn)) == {("skipped", "not_attempted")}
    assert rb.read_history(conn, decl.declaration_id).resolved


def test_refuses_to_decide_outside_shadow_only(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl_id = _frozen(conn)
    _move(conn, decl_id, "shadow_only", "executing", "supervisor")
    conn.autocommit = True
    d, _ = _first_window(conn)
    with pytest.raises(RuntimeError, match="executed-book decisions are slice 5"):
        job.run_rebalance_job(conn, score=_never_scores, now=lambda: datetime.combine(d, time(23, 40), UTC))
    assert _attempts(conn) == []


def _prepared(due: rb.DuePlan, **over: Any) -> rb.Prepared:
    snapshot = {"kind": rb.SNAPSHOT_KIND, "target_session": due.target_session.isoformat(), "names": []}
    fields: dict[str, Any] = {
        "snapshot": snapshot,
        "snapshot_sha256": canonical_sha256(snapshot),
        "universes": cast(Universes, None),
        "detail": {"r_count": 1650, "f_count": 718},
    }
    return rb.Prepared(**(fields | over))


def test_record_decided_publishes_the_snapshot_and_checks_its_hash(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _frozen(conn)
    d, decl = _first_window(conn)
    conn.commit()
    conn.autocommit = True  # each `conn.transaction()` below is a top-level transaction, as in the job
    as_of = datetime.combine(d, time(23, 40), UTC)
    due = rb.plan(as_of, first=rb.first_month(decl.frozen_at), resolved=frozenset())
    assert due.due

    with pytest.raises(rb.SnapshotIntegrityError), conn.transaction():
        rb.begin_rebalance(conn)
        rb.record_decided(conn, decl, due, _prepared(due, snapshot_sha256="c" * 64), as_of=as_of, scored_at=SCORED_AT)
    with pytest.raises(ValueError, match="r_count"), conn.transaction():
        rb.begin_rebalance(conn)
        rb.record_decided(conn, decl, due, _prepared(due, detail={}), as_of=as_of, scored_at=SCORED_AT)
    with pytest.raises(RuntimeError, match="begin_rebalance"), conn.transaction():
        rb.record_decided(conn, decl, due, _prepared(due), as_of=as_of, scored_at=SCORED_AT)
    assert _attempts(conn) == []

    with conn.transaction():
        rb.begin_rebalance(conn)
        attempt = rb.record_decided(conn, decl, due, _prepared(due), as_of=as_of, scored_at=SCORED_AT)
    history = rb.read_history(conn, decl.declaration_id)
    assert attempt > 0 and history.previous_r_count == 1650 and due.month in history.resolved

    again = job.run_rebalance_job(conn, score=_never_scores, now=lambda: as_of)
    assert again.note.endswith("not due") and _attempts(conn) == [("decided", None)]


def test_a_waiting_refusal_never_masks_a_gate_verdict(ebull_test_conn: Conn) -> None:
    """Bot WARNING on #3558: the month's last refusal (a skip's reason, the collapse streak) is the latest GATE
    verdict; a pre-score waiting row names it only when no gate ever reached one."""
    conn = ebull_test_conn
    _frozen(conn)
    conn.autocommit = True
    d, decl = _first_window(conn)
    as_of = datetime.combine(d, time(23, 40), UTC)
    due = rb.plan(as_of, first=rb.first_month(decl.frozen_at), resolved=frozenset())
    waiting = rb.Refused("price_daily_stale", {"pre_score": True})

    rb.record_refused(conn, decl, due, waiting, as_of=as_of, scored_at=None)
    assert rb.read_history(conn, decl.declaration_id).last_refusal == {due.month: "price_daily_stale"}
    rb.record_refused(conn, decl, due, rb.Refused("universe_collapse", {}), as_of=as_of, scored_at=SCORED_AT)
    rb.record_refused(conn, decl, due, waiting, as_of=as_of, scored_at=None)  # the next night's waiting fire
    assert rb.read_history(conn, decl.declaration_id).last_refusal == {due.month: "universe_collapse"}
