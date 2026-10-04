"""#3515 slice 4 — fund-v1's freeze against real Postgres (fund-v1 spec §0, §6, §7).

While v1 is active the fund-v1 freeze refuses ``v1_not_wound_down`` and writes nothing. Once v1 is
completed, the apply writes fund-v1's own #2599 row, document, managers and genesis event; the run
publisher's runtime check accepts it under ``FUND_POLICY_HASH``, the per-run byte gate reads the
fixture's bytes back, and no v1 row moves."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services.ai_trial_freeze import Provenance, freeze_trial
from app.services.ai_trial_fund_freeze import FUND_TERMS
from app.services.ai_trial_fund_pack import fixture_bytes
from app.services.ai_trial_fund_policy import FUND_POLICY_HASH
from app.services.ai_trial_run import declaration_refusal, load_declaration
from app.services.ai_trial_version import FUND_V1
from app.services.strategy_control_plane import configure_deployment
from tests.test_ai_trial_fund_freeze import _fixture
from tests.test_ai_trial_intent_db import _deploy, _seed_instruments

# #3610: freezes a claim with no TrialDesign while testing something else (tests/conftest.py).
pytestmark = pytest.mark.usefixtures("assume_trial_powered")

Conn = psycopg.Connection[Any]

PROVENANCE = Provenance(code_git_sha="a" * 40, spec_sha256="b" * 64, python_version="3.12.0", refusals=())
V1_LEGS = ("ai-discretionary-v1", "ai-discretionary-v1-control")
OUTSIDE = {
    "budget_fixture": _fixture(),
    "measurements": {
        "coverage": {"command": "c", "output_lines": ["as_of: x"]},
        "prompt_budget": {"command": "p", "output_lines": ["fund-v1 rendered user prompt bytes: 1,185,479"]},
        "real_prompt_bytes": 1_185_479,
        "fixture_prompt_bytes": 1_531_589,
    },
}


def _setup(conn: Conn) -> int:
    """v1 frozen and active; both fund-v1 legs deployed at $3,000 with equal policies."""
    _seed_instruments(conn)
    for leg in V1_LEGS:
        _deploy(conn, leg, "v1", policy_stop="25")
    for leg in FUND_V1.leg_strategy_ids:
        _deploy(conn, leg, "v1", policy_stop="25")
        configure_deployment(
            conn,
            strategy_id=leg,
            strategy_version="v1",
            mode="paper",
            capital_limit=Decimal("3000"),
            enabled=True,
            changed_by="operator",
            reason="#3515 freeze fixture",
        )
    conn.commit()
    dry = freeze_trial(conn, provenance=PROVENANCE, apply=False)
    v1 = freeze_trial(
        conn,
        provenance=PROVENANCE,
        apply=True,
        declared_by="supervisor",
        wake_evidence="https://example.invalid/3471",
        expect_config_sha256=dry.config_sha256,
    )
    assert v1.applied and v1.declaration_id is not None
    return v1.declaration_id


def _fund_rows(conn: Conn) -> tuple[int, int, int]:
    row = conn.execute(
        """
        SELECT (SELECT count(*) FROM strategy_preregistration_declarations WHERE strategy_id = %(arm)s),
               (SELECT count(*) FROM ai_trial_declarations WHERE strategy_id = %(arm)s),
               (SELECT count(*) FROM strategy_position_manager_policies m
                JOIN strategy_deployments d ON d.deployment_id = m.deployment_id
                WHERE d.strategy_id IN (%(arm)s, %(control)s))
        """,
        {"arm": FUND_V1.arm_strategy_id, "control": FUND_V1.control_strategy_id},
    ).fetchone()
    conn.commit()
    assert row is not None
    return (int(row[0]), int(row[1]), int(row[2]))


def _v1_snapshot(conn: Conn) -> list[tuple[Any, ...]]:
    rows = conn.execute(
        """
        SELECT 'event', event_id::text, to_state FROM ai_trial_state_events
         WHERE declaration_id IN (SELECT declaration_id FROM ai_trial_declarations WHERE strategy_id = %s)
        UNION ALL SELECT 'manager', m.deployment_id::text, m.max_position_age_seconds::text
          FROM strategy_position_manager_policies m JOIN strategy_deployments d ON d.deployment_id = m.deployment_id
         WHERE d.strategy_id = ANY(%s)
        ORDER BY 1, 2
        """,
        (V1_LEGS[0], list(V1_LEGS)),
    ).fetchall()
    conn.commit()
    return [tuple(r) for r in rows]


def test_fund_v1_refuses_while_v1_runs_and_writes_nothing(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _setup(conn)
    before = _v1_snapshot(conn)
    report = freeze_trial(conn, provenance=PROVENANCE, apply=False, terms=FUND_TERMS, outside=OUTSIDE)
    assert report.refusals == ("v1_not_wound_down:trial_active",)
    assert sorted(report.config) == sorted(FUND_V1.leg_strategy_ids)
    assert all(report.doc["v1_hashed_module_parity"][key] for key in report.doc["v1_hashed_module_parity"])
    assert _fund_rows(conn) == (0, 0, 0)
    assert _v1_snapshot(conn) == before


def test_fund_v1_freezes_once_v1_is_completed(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    v1_id = _setup(conn)
    conn.execute(
        "INSERT INTO ai_trial_state_events (declaration_id, from_state, to_state, reason, actor) "
        "VALUES (%s, 'active', 'completed', 'test: v1 wound down', 'supervisor')",
        (v1_id,),
    )
    conn.commit()
    before = _v1_snapshot(conn)

    unmeasured = freeze_trial(conn, provenance=PROVENANCE, apply=False, terms=FUND_TERMS)
    assert set(unmeasured.refusals) == {"budget_fixture_not_run", "measurements_missing"}

    dry = freeze_trial(conn, provenance=PROVENANCE, apply=False, terms=FUND_TERMS, outside=OUTSIDE)
    assert dry.refusals == ()
    applied = freeze_trial(
        conn,
        provenance=PROVENANCE,
        apply=True,
        declared_by="supervisor",
        wake_evidence="https://example.invalid/3515",
        expect_config_sha256=dry.config_sha256,
        terms=FUND_TERMS,
        outside=OUTSIDE,
    )
    assert (applied.refusals, applied.applied, applied.doc_sha256) == ((), True, dry.doc_sha256)
    assert _fund_rows(conn) == (1, 1, 2)
    assert _v1_snapshot(conn) == before

    declaration = load_declaration(conn, version=FUND_V1)
    conn.commit()
    assert declaration is not None and declaration.declaration_id == applied.declaration_id
    assert declaration_refusal(declaration, policy_hash=FUND_POLICY_HASH) is None
    assert fixture_bytes(declaration.doc) == 1_531_589
    assert declaration.doc["v1_hashed_module_parity"][str(v1_id)]["ai_trial_guard.py"] is True
    doc_path = conn.execute(
        "SELECT doc_path FROM ai_trial_declarations WHERE declaration_id = %s", (applied.declaration_id,)
    ).fetchone()
    conn.commit()
    assert doc_path is not None and doc_path[0] == f"generated:ai_trial_fund_freeze.FUND_TERMS@{'a' * 40}"

    again = freeze_trial(conn, provenance=PROVENANCE, apply=False, terms=FUND_TERMS, outside=OUTSIDE)
    assert "already_frozen" in again.refusals
