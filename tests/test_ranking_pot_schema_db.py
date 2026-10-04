"""#2842 slice 4a — ranking-pot-v1 declaration, states and isolation against real Postgres (spec §7.1, §8).

- The freeze: a dry run writes nothing; the apply writes the #2599 row, the document and the genesis
  ``shadow_only`` event; a second freeze is ``already_frozen``.
- ``sql/445``: the §7.1 transition matrix, append-only, the family seq and the ``v1_active`` refusal.
- Isolation: the AI-trial loader and the standard paper loader refuse a pot signal; the one control-plane
  branch is a paper deployment, live authority is refused.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

import psycopg
import pytest

from app.services import ai_trial_freeze, ranking_pot_policy
from app.services.ai_trial_freeze import Provenance
from app.services.ai_trial_intent import load_trial_intent
from app.services.ai_trial_pack import canonical_sha256
from app.services.ranking_pot_freeze import freeze_pot, prereg_declaration
from app.services.result_ledger import freeze_preregistration
from app.services.scoring import _DEFAULT_MODEL_VERSION
from app.services.strategy_control_plane import StrategyControlError, configure_deployment
from app.services.strategy_paper_executor import execute_fired_paper_signal

# #3610: freezes a claim with no TrialDesign while testing something else (tests/conftest.py).
pytestmark = pytest.mark.usefixtures("assume_trial_powered")

Conn = psycopg.Connection[Any]

POT = "ranking-pot-v1"
PROVENANCE = Provenance(code_git_sha="a" * 40, spec_sha256="b" * 64, python_version="3.12.0", refusals=())
SCORED_AT = datetime(2026, 10, 1, 17, 7, 54, tzinfo=UTC)


def _seed_scores(conn: Conn, ids: tuple[int, ...] = (2842, 2843, 2844)) -> None:
    for iid in ids:
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, 'Pot', TRUE)",
            (iid, f"POT{iid}"),
        )
        conn.execute(
            "INSERT INTO scores (instrument_id, model_version, scored_at, rank, total_score) "
            "VALUES (%s, %s, %s, 1, 0.5)",
            (iid, _DEFAULT_MODEL_VERSION, SCORED_AT),
        )
    conn.commit()


def _counts(conn: Conn) -> tuple[int, int, int]:
    row = conn.execute(
        """
        SELECT (SELECT count(*) FROM strategy_preregistration_declarations WHERE strategy_id = %s),
               (SELECT count(*) FROM ranking_pot_declarations),
               (SELECT count(*) FROM ranking_pot_state_events)
        """,
        (POT,),
    ).fetchone()
    conn.commit()
    assert row is not None
    return (int(row[0]), int(row[1]), int(row[2]))


@pytest.fixture(autouse=True)
def _build_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    """These tests exercise the applied freeze, which refuses ``build_incomplete`` until slice 7."""
    monkeypatch.setattr(ranking_pot_policy, "BUILD_COMPLETE", True)


def _frozen(conn: Conn) -> int:
    _seed_scores(conn)
    with pytest.MonkeyPatch.context() as mp:  # imported by other modules, which lack the autouse fixture
        mp.setattr(ranking_pot_policy, "BUILD_COMPLETE", True)
        report = freeze_pot(conn, provenance=PROVENANCE, apply=True, declared_by="supervisor")
    assert (report.refusals, report.applied) == ((), True)
    assert report.declaration_id is not None
    return report.declaration_id


def _move(conn: Conn, decl: int, frm: str, to: str, actor: str, wind_down: str | None = None) -> None:
    try:
        if to == "executing":
            # sql/452: `→ executing` needs the activation row (the activation script writes it first).
            conn.execute(
                "INSERT INTO ranking_pot_activations (declaration_id, pot_capital) VALUES (%s, 1000) "
                "ON CONFLICT (declaration_id) DO NOTHING",
                (decl,),
            )
        conn.execute(
            "INSERT INTO ranking_pot_state_events "
            "(declaration_id, from_state, to_state, wind_down_reason, reason, actor) VALUES (%s, %s, %s, %s, 't', %s)",
            (decl, frm, to, wind_down, actor),
        )
        conn.commit()
    except psycopg.Error:
        conn.rollback()
        raise


# ---------------------------------------------------------------------------
# The freeze
# ---------------------------------------------------------------------------
def test_dry_run_writes_nothing_and_apply_lands_in_shadow_only(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed_scores(conn)
    dry = freeze_pot(conn, provenance=PROVENANCE, apply=False)
    assert (dry.refusals, dry.applied, dry.declaration_id) == ((), False, None)
    assert dry.doc is not None and dry.doc["s0"]["instrument_ids"] == [2842, 2843, 2844]
    assert _counts(conn) == (0, 0, 0)

    applied = freeze_pot(conn, provenance=PROVENANCE, apply=True, declared_by="supervisor")
    assert (applied.refusals, applied.applied) == ((), True)
    assert applied.doc_sha256 == dry.doc_sha256
    assert _counts(conn) == (1, 1, 1)
    row = conn.execute(
        """
        SELECT d.doc, d.doc_sha256, d.family_seq, p.contract_version, e.to_state, e.actor
        FROM ranking_pot_declarations d
        JOIN strategy_preregistration_declarations p USING (declaration_id)
        JOIN ranking_pot_state_events e USING (declaration_id)
        """
    ).fetchone()
    conn.commit()
    assert row is not None
    assert canonical_sha256(row[0]) == row[1] == applied.doc_sha256
    assert (row[2], row[3], row[4], row[5]) == (
        1,
        "ranking-pot-declaration-v1:" + str(applied.doc_sha256),
        "shadow_only",
        "supervisor",
    )

    again = freeze_pot(conn, provenance=PROVENANCE, apply=True, declared_by="supervisor")
    assert "already_frozen" in again.refusals and not again.applied
    assert again.existing_doc_sha256 == applied.doc_sha256
    assert _counts(conn) == (1, 1, 1)


def test_freeze_refuses_without_a_scores_run_or_a_declarer(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    assert freeze_pot(conn, provenance=PROVENANCE, apply=False).refusals == ("s0_no_scores_run",)
    _seed_scores(conn)
    assert freeze_pot(conn, provenance=PROVENANCE, apply=True).refusals == ("declared_by_missing",)


def test_apply_refuses_until_the_build_is_complete(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    """Slice 4b-ii (Codex ckpt-1): a declaration frozen on a partial build could never be measured."""
    conn = ebull_test_conn
    _seed_scores(conn)
    monkeypatch.setattr(ranking_pot_policy, "BUILD_COMPLETE", False)
    report = freeze_pot(conn, provenance=PROVENANCE, apply=True, declared_by="supervisor")
    assert (report.refusals, report.applied) == (("build_incomplete",), False)
    assert _counts(conn) == (0, 0, 0)
    assert freeze_pot(conn, provenance=PROVENANCE, apply=False).refusals == ()  # the dry run still runs
    assert _counts(conn) == (0, 0, 0)


# ---------------------------------------------------------------------------
# sql/445: the §7.1 transition matrix
# ---------------------------------------------------------------------------
def test_transition_matrix_walk(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _frozen(conn)

    refused = [
        ("shadow_only", "halted_loss", "engine", None),
        ("shadow_only", "halted_operator", "supervisor", None),
        ("shadow_only", "executing", "engine", None),
        ("shadow_only", "executing", "operator", None),
        ("shadow_only", "completed", "engine", None),
        ("executing", "executing", "supervisor", None),  # stale from_state
    ]
    for frm, to, actor, wd in refused:
        with pytest.raises(psycopg.errors.RaiseException):
            _move(conn, decl, frm, to, actor, wd)

    _move(conn, decl, "shadow_only", "executing", "supervisor")
    with pytest.raises(psycopg.errors.RaiseException):
        _move(conn, decl, "executing", "halted_loss", "supervisor")
    _move(conn, decl, "executing", "halted_loss", "engine")
    _move(conn, decl, "halted_loss", "halted_operator", "operator")
    with pytest.raises(psycopg.errors.RaiseException):
        _move(conn, decl, "halted_operator", "halted_loss", "engine")  # the operator halt dominates
    _move(conn, decl, "halted_operator", "executing", "supervisor")

    with pytest.raises(psycopg.errors.RaiseException):
        _move(conn, decl, "executing", "winding_down", "supervisor", "harm")  # harm is the engine's
    with pytest.raises(psycopg.errors.RaiseException):
        _move(conn, decl, "executing", "winding_down", "engine", "operator")
    with pytest.raises(psycopg.errors.CheckViolation):
        _move(conn, decl, "executing", "winding_down", "engine", None)
    _move(conn, decl, "executing", "winding_down", "engine", "harm")

    for to, actor in (("executing", "supervisor"), ("halted_operator", "operator"), ("completed", "supervisor")):
        with pytest.raises(psycopg.errors.RaiseException):
            _move(conn, decl, "winding_down", to, actor)
    _move(conn, decl, "winding_down", "completed", "engine")
    with pytest.raises(psycopg.errors.RaiseException):
        _move(conn, decl, "completed", "winding_down", "engine", "harm")


def test_a_never_executed_trial_winds_down_from_shadow_only(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _frozen(conn)
    _move(conn, decl, "shadow_only", "winding_down", "operator", "operator")
    _move(conn, decl, "winding_down", "completed", "engine")


def test_records_are_append_only(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _frozen(conn)
    for sql in (
        "UPDATE ranking_pot_declarations SET doc_path = 'x' WHERE declaration_id = %s",
        "DELETE FROM ranking_pot_state_events WHERE declaration_id = %s",
    ):
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
            conn.execute(sql, (decl,))
        conn.rollback()


def test_family_seq_must_be_the_next_one(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    sha = "d" * 64
    with conn.transaction():
        declaration_id = freeze_preregistration(
            cast(psycopg.Connection[tuple], conn), prereg_declaration(doc_sha256=sha, declared_by="t")
        )
        with pytest.raises(psycopg.errors.RaiseException, match="is not the next"):
            conn.execute(
                "INSERT INTO ranking_pot_declarations "
                "(declaration_id, strategy_id, strategy_version, family, family_seq, doc_path, doc, doc_sha256) "
                "VALUES (%s, %s, 'v1', 'ranking-pot', 2, 'p', %s::jsonb, %s)",
                (
                    declaration_id,
                    POT,
                    '{"strategy_id": "ranking-pot-v1", "strategy_version": "v1", "family": "ranking-pot", '
                    '"family_seq": 2}',
                    sha,
                ),
            )
    conn.rollback()


def test_executing_is_refused_while_an_ai_trial_is_active(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _frozen(conn)
    sha = "e" * 64
    v1 = freeze_preregistration(
        cast(psycopg.Connection[tuple], conn),
        ai_trial_freeze.prereg_declaration(doc_sha256=sha, declared_by="t"),
    )
    conn.execute(
        "INSERT INTO ai_trial_declarations (declaration_id, strategy_id, strategy_version, doc_path, doc, doc_sha256) "
        "VALUES (%s, 'ai-discretionary-v1', 'v1', 'p', "
        '\'{"strategy_id": "ai-discretionary-v1", "strategy_version": "v1"}\'::jsonb, %s)',
        (v1, sha),
    )
    conn.execute(
        "INSERT INTO ai_trial_state_events (declaration_id, from_state, to_state, reason, actor) "
        "VALUES (%s, NULL, 'active', 't', 'supervisor')",
        (v1,),
    )
    conn.commit()
    with pytest.raises(psycopg.errors.RaiseException, match="v1_active"):
        _move(conn, decl, "shadow_only", "executing", "supervisor")
    conn.execute(
        "INSERT INTO ai_trial_state_events (declaration_id, from_state, to_state, reason, actor) "
        "VALUES (%s, 'active', 'halted_operator', 't', 'supervisor')",
        (v1,),
    )
    conn.commit()
    _move(conn, decl, "shadow_only", "executing", "supervisor")


# ---------------------------------------------------------------------------
# §7.1 isolation
# ---------------------------------------------------------------------------
def _pot_signal(conn: Conn) -> int:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (2849, 'POTS', 'Pot', TRUE)"
    )
    row = conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id, strategy_version, instrument_id, signal_bar_date,
            signal_kind, verdict, fill_bar_date, fill_price, universe, input_rule_set_versions
        ) VALUES (%s, 'v1', 2849, DATE '2026-10-01', 'entry', 'fired',
                  DATE '2026-10-02', 10, 'survivor_only', '{"test":"v1"}'::jsonb)
        RETURNING signal_id
        """,
        (POT,),
    ).fetchone()
    assert row is not None
    conn.commit()
    return int(row[0])


def test_the_ai_trial_loader_refuses_a_pot_signal(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    signal_id = _pot_signal(conn)
    intent, reason, _ = load_trial_intent(conn, signal_id=signal_id, now=datetime(2026, 10, 2, 15, tzinfo=UTC))
    assert (intent, reason) == (None, "trial_link_missing")


def test_the_standard_paper_loader_refuses_a_pot_signal(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    signal_id = _pot_signal(conn)
    result = execute_fired_paper_signal(conn, broker=cast(Any, object()), signal_id=signal_id)
    assert (result.verdict, result.reason_code) == ("rejected", "strategy_not_capital_candidate")


def test_control_plane_admits_a_paper_deployment_and_refuses_live(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn

    def configure(mode: Any, limit: str, enabled: bool) -> Any:
        return configure_deployment(
            conn,
            strategy_id=POT,
            strategy_version="v1",
            mode=mode,
            capital_limit=Decimal(limit),
            enabled=enabled,
            changed_by="operator",
            reason="#2842 test",
        )

    assert configure("paper", "1000", True).deployment_id > 0
    with pytest.raises(StrategyControlError, match="demo-trial strategies cannot receive live capital"):
        configure("live", "100", True)
