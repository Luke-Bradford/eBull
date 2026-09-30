"""#3515 slice 3c — a fund-v1 decision routes end to end with v1's rows present (fund-v1 spec §7, §8.3).

The run is driven through the real claim, control draw, publish and leg links, then the halts;
only the pack builder and the model are stubbed (``tests/test_ai_trial_run_db.py``'s harness).
Every v1 row is snapshotted before fund-v1 runs and compared after: none is read into fund-v1's
records, none is written.
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from app.services.ai_trial_fund_policy import FUND_POLICY_HASH
from app.services.ai_trial_halts import HaltCheck, enforce_trial_halts
from app.services.ai_trial_run import run_trial_decision
from app.services.ai_trial_version import FUND_V1, BuiltPack
from tests.test_ai_trial_run_db import ENV, PACK, RISK, _decisions, _invoke, _run, _seed, stubbed  # noqa: F401

Conn = psycopg.Connection[Any]

_V1_ROWS: dict[str, str] = {
    "runs": "SELECT * FROM ai_trial_runs WHERE declaration_id = %(d)s ORDER BY run_id",
    "decisions": (
        "SELECT d.* FROM ai_trial_decisions d JOIN ai_trial_runs r ON r.run_id = d.run_id "
        "WHERE r.declaration_id = %(d)s ORDER BY d.decision_id"
    ),
    "pairs": "SELECT * FROM ai_trial_pairs WHERE declaration_id = %(d)s ORDER BY pair_id",
    "links": (
        "SELECT l.* FROM ai_trial_leg_links l JOIN ai_trial_pairs p ON p.pair_id = l.pair_id "
        "WHERE p.declaration_id = %(d)s ORDER BY l.pair_id, l.leg"
    ),
    "signals": (
        "SELECT * FROM strategy_signals WHERE strategy_id IN ('ai-discretionary-v1', 'ai-discretionary-v1-control') "
        "ORDER BY signal_id"
    ),
    "states": "SELECT * FROM ai_trial_state_events WHERE declaration_id = %(d)s ORDER BY event_id",
}


def _v1_rows(conn: Conn, declaration_id: int) -> dict[str, list[tuple[Any, ...]]]:
    rows = {name: conn.execute(q, {"d": declaration_id}).fetchall() for name, q in _V1_ROWS.items()}  # type: ignore[call-overload]
    conn.commit()
    return rows


def test_a_fund_v1_decision_routes_without_reading_or_writing_v1_rows(
    ebull_test_conn: Conn,
    stubbed: dict[str, Any],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    v1_id = _seed(conn)
    v1 = _run(conn, _invoke(_decisions()))
    assert v1.status == "decided" and len(v1.pair_ids) == 1
    before = _v1_rows(conn, v1_id)
    assert all(before[name] for name in ("runs", "decisions", "pairs", "links", "signals"))

    fund_id = _seed(conn, policy_hash=FUND_POLICY_HASH, strategy_id=FUND_V1.arm_strategy_id)
    record = {"fund_blocks": Jsonb({"coverage": {"names": len(PACK.complete)}})}
    fund = dataclasses.replace(FUND_V1, build_pack=lambda _conn, **_kw: BuiltPack(PACK, record))
    outcome = run_trial_decision(
        conn, env=ENV, risk=RISK, fetch_intraday=lambda _iid: [], invoke=_invoke(_decisions()), version=fund
    )
    assert outcome.status == "decided" and len(outcome.pair_ids) == 1
    # Same target session as v1's, its own claim: the unique key is per declaration.
    assert outcome.session_date == v1.session_date and outcome.run_id != v1.run_id

    run = conn.execute(
        "SELECT declaration_id, policy_hash, system_prompt_sha256, fund_blocks FROM ai_trial_runs WHERE run_id = %s",
        (outcome.run_id,),
    ).fetchone()
    assert run == (fund_id, FUND_POLICY_HASH, FUND_V1.system_prompt_sha256, {"coverage": {"names": len(PACK.complete)}})
    # fund-v1's pair sequence is its own (v1 already holds pair_seq 0).
    pair = conn.execute(
        "SELECT declaration_id, pair_seq FROM ai_trial_pairs WHERE pair_id = %s", (outcome.pair_ids[0],)
    ).fetchone()
    assert pair == (fund_id, 0)
    links = conn.execute(
        """
        SELECT l.leg, s.strategy_id, s.strategy_version, s.input_rule_set_versions
        FROM ai_trial_leg_links l JOIN strategy_signals s ON s.signal_id = l.signal_id
        WHERE l.pair_id = %s ORDER BY l.leg
        """,
        (outcome.pair_ids[0],),
    ).fetchall()
    assert links == [
        ("arm", "ai-discretionary-fund-v1", "v1", {"ai_trial_policy": FUND_POLICY_HASH}),
        ("control", "ai-discretionary-fund-v1-control", "v1", {"ai_trial_policy": FUND_POLICY_HASH}),
    ]
    conn.commit()

    # The halts check both trials, each under its own descriptor; neither fails nor halts.
    checks = enforce_trial_halts(conn, now=datetime.now(UTC) + timedelta(days=1))
    conn.commit()
    assert sorted(checks, key=lambda c: c.declaration_id) == [HaltCheck(v1_id, None, 0), HaltCheck(fund_id, None, 0)]

    assert _v1_rows(conn, v1_id) == before
