"""#3471 slice 3d — the declaration freeze against real Postgres (spec §9 "Freeze").

The dry run writes nothing; the apply writes the #2599 row, the document, both position managers
and the genesis ``active`` event in one transaction, and the run publisher's own runtime check
accepts what it froze (digest intact after the JSONB round trip, ``policy_hash`` current)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import psycopg

from app.services.ai_trial_deadline import TRIAL_MAX_POSITION_AGE_SECONDS
from app.services.ai_trial_freeze import Provenance, freeze_trial
from app.services.ai_trial_intent import declaration_digest
from app.services.ai_trial_policy import AI_TRIAL_POLICY_HASH
from app.services.ai_trial_run import declaration_refusal, load_declaration
from app.services.strategy_control_plane import configure_deployment
from tests.test_ai_trial_intent_db import _deploy, _seed_instruments

Conn = psycopg.Connection[Any]

PROVENANCE = Provenance(code_git_sha="a" * 40, spec_sha256="b" * 64, python_version="3.12.0", refusals=())
ARM, CONTROL = "ai-discretionary-v1", "ai-discretionary-v1-control"


def _deployed(conn: Conn) -> None:
    _seed_instruments(conn)
    _deploy(conn, ARM, "v1", policy_stop="25")
    _deploy(conn, CONTROL, "v1", policy_stop="25")
    conn.commit()


def _counts(conn: Conn) -> tuple[int, int, int, int]:
    row = conn.execute(
        """
        SELECT (SELECT count(*) FROM strategy_preregistration_declarations WHERE strategy_id = %(arm)s),
               (SELECT count(*) FROM ai_trial_declarations),
               (SELECT count(*) FROM ai_trial_state_events),
               (SELECT count(*) FROM strategy_position_manager_policies m
                JOIN strategy_deployments d ON d.deployment_id = m.deployment_id
                WHERE d.strategy_id IN (%(arm)s, %(control)s))
        """,
        {"arm": ARM, "control": CONTROL},
    ).fetchone()
    conn.commit()
    assert row is not None
    return (int(row[0]), int(row[1]), int(row[2]), int(row[3]))


def test_the_dry_run_executes_the_freeze_and_writes_nothing(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _deployed(conn)
    report = freeze_trial(conn, provenance=PROVENANCE, apply=False)
    assert (report.refusals, report.applied, report.declaration_id) == ((), False, None)
    assert sorted(report.config) == [ARM, CONTROL]
    assert _counts(conn) == (0, 0, 0, 0)


def test_apply_starts_the_trial_and_the_runtime_accepts_the_frozen_document(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _deployed(conn)
    dry = freeze_trial(conn, provenance=PROVENANCE, apply=False)

    # Apply is bound to the reviewed config and to the §8 wake evidence.
    unbound = freeze_trial(conn, provenance=PROVENANCE, apply=True, declared_by="supervisor")
    assert set(unbound.refusals) == {"config_sha_unconfirmed", "wake_evidence_missing"}
    assert _counts(conn) == (0, 0, 0, 0)

    applied = freeze_trial(
        conn,
        provenance=PROVENANCE,
        apply=True,
        declared_by="supervisor",
        wake_evidence="https://example.invalid/3471#answer",
        expect_config_sha256=dry.config_sha256,
    )
    assert (applied.refusals, applied.applied) == ((), True)
    assert applied.doc_sha256 == dry.doc_sha256
    assert _counts(conn) == (1, 1, 1, 2)

    declaration = load_declaration(conn)
    conn.commit()
    assert declaration is not None and declaration.declaration_id == applied.declaration_id
    # The publisher's own §9 runtime check: digest intact after JSONB, policy hash current, active.
    assert declaration_digest(declaration.doc) == applied.doc_sha256
    assert declaration_refusal(declaration, policy_hash=AI_TRIAL_POLICY_HASH) is None
    event = conn.execute("SELECT from_state, to_state, actor, reason FROM ai_trial_state_events").fetchone()
    assert event is not None and event[:3] == (None, "active", "supervisor")
    assert "https://example.invalid/3471#answer" in event[3]
    managers = conn.execute(
        "SELECT max_position_age_seconds, ratchet_variant_id FROM strategy_position_manager_policies"
    ).fetchall()
    assert sorted(tuple(row) for row in managers) == [(TRIAL_MAX_POSITION_AGE_SECONDS, None)] * 2
    conn.commit()

    # A retry after a lost commit sees its own document; nothing more is written.
    again = freeze_trial(conn, provenance=PROVENANCE, apply=False)
    assert "already_frozen" in again.refusals
    assert again.existing_doc_sha256 == applied.doc_sha256
    assert _counts(conn) == (1, 1, 1, 2)


def test_a_config_change_after_review_and_unequal_legs_refuse(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _deployed(conn)
    reviewed = freeze_trial(conn, provenance=PROVENANCE, apply=False).config_sha256
    configure_deployment(
        conn,
        strategy_id=CONTROL,
        strategy_version="v1",
        mode="paper",
        capital_limit=Decimal("999"),
        enabled=True,
        changed_by="operator",
        reason="#3471 freeze fixture: changed after the dry run",
    )
    conn.commit()
    changed = freeze_trial(
        conn,
        provenance=PROVENANCE,
        apply=True,
        declared_by="supervisor",
        wake_evidence="https://example.invalid/3471#answer",
        expect_config_sha256=reviewed,
    )
    assert set(changed.refusals) == {"config_changed", "trial_capital_parity"}
    assert _counts(conn) == (0, 0, 0, 0)
    # Provenance refusals block the apply too.
    dirty = Provenance(
        code_git_sha="a" * 40, spec_sha256="b" * 64, python_version="3.12.0", refusals=("worktree_dirty",)
    )
    assert "worktree_dirty" in freeze_trial(conn, provenance=dirty, apply=False).refusals
