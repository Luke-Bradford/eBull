"""#2612 — the two single-entry barriers, exercised against a real database.

The pure half of this ticket lives in
`tests/test_2612_forward_window_single_entry.py`, which explains the invariant.

⚠ SPLIT DELIBERATELY, NOT FOR TIDINESS.  `tests/conftest.py` auto-applies the
`db` marker per MODULE, by matching the literal string ``ebull_test_conn``
against the file's source text (`_module_source_touches_db`).  One DB test in a
module therefore drags every pure test in it out of the fast pre-push gate.  The
stage-machine coupling guard is the one test here that must run on EVERY push —
its whole job is to fire on the day someone adds a re-entry edge — so it cannot
share a module with this file.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services.strategy_control_plane import StrategyControlError, promote_strategy
from app.services.strategy_live_gate import assess_live_gate

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("registered_strategy_test_candidates")]

_STRATEGY_ID = "S-GOV"
_VERSION = "forward-window-v1"


def _advance_to_forward_observation(conn: psycopg.Connection[Any]) -> None:
    """Seed the lifecycle directly.

    ⚠ Deliberately NOT via `promote_strategy`: reaching `forward_observation`
    through it requires pinned results carrying #2505 edge evidence and #2621
    frozen-universe records, none of which this invariant depends on.  The
    transition check runs before any of that.
    """
    conn.execute(
        """
        INSERT INTO strategy_promotions (
            strategy_id,strategy_version,from_stage,to_stage,gate_version,
            evidence_ref,promoted_by,reason,promoted_at
        ) VALUES
          (%s,%s,NULL,'research_candidate','test-v1',NULL,'test','registered',now()-interval '40 days'),
          (%s,%s,'research_candidate','historical_validated','test-v1',
           'hist','test','history',now()-interval '39 days'),
          (%s,%s,'historical_validated','forward_observation','test-v1',
           'forward','test','observe',now()-interval '38 days')
        """,
        (_STRATEGY_ID, _VERSION, _STRATEGY_ID, _VERSION, _STRATEGY_ID, _VERSION),
    )


@pytest.mark.parametrize("anchor", ["forward_observation", "paper_enabled"])
def test_promote_strategy_refuses_a_second_arrival_at_a_window_anchor(
    ebull_test_conn: psycopg.Connection[Any], anchor: str
) -> None:
    """Barrier 1, exercised through the sole production writer.

    `forward_observation` is refused because the version already sits there;
    `paper_enabled` is refused on the second attempt, after the first legitimately
    advances it.
    """
    _advance_to_forward_observation(ebull_test_conn)
    if anchor == "paper_enabled":
        promote_strategy(
            ebull_test_conn,
            strategy_id=_STRATEGY_ID,
            strategy_version=_VERSION,
            to_stage="paper_enabled",
            promoted_by="test",
            reason="first paper arrival",
            evidence_ref="paper-1",
        )

    with pytest.raises(StrategyControlError, match="invalid promotion transition"):
        promote_strategy(
            ebull_test_conn,
            strategy_id=_STRATEGY_ID,
            strategy_version=_VERSION,
            to_stage=anchor,  # type: ignore[arg-type]
            promoted_by="test",
            reason="second arrival",
            evidence_ref="second",
        )


def test_the_unique_index_refuses_a_second_arrival_the_service_check_never_saw(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Barrier 2, exercised by bypassing barrier 1 entirely.

    Five test modules already INSERT into `strategy_promotions` directly, so the
    service check is not the only path that reaches this table.  This is the
    barrier that holds when it is bypassed.

    ⚠ Asserts `diag.constraint_name`, not just the exception class: a bare
    `UniqueViolation` would also be raised by an unrelated index on this insert,
    and a probe that passes on a bystander proves nothing.
    """
    _advance_to_forward_observation(ebull_test_conn)

    with pytest.raises(psycopg.errors.UniqueViolation) as excinfo:
        ebull_test_conn.execute(
            """
            INSERT INTO strategy_promotions (
                strategy_id,strategy_version,from_stage,to_stage,gate_version,
                evidence_ref,promoted_by,reason
            ) VALUES (%s,%s,'historical_validated','forward_observation','test-v1',
                      'forward-again','test','re-observe')
            """,
            (_STRATEGY_ID, _VERSION),
        )
    assert excinfo.value.diag.constraint_name == "idx_strategy_promotions_one_successor"


def test_the_measured_window_reports_one_arrival_per_anchor(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The counts `assess_live_gate` now carries agree with the barriers above.

    Pins the wiring between the SQL `count(*) FILTER (...)` and the two
    `LiveGateFacts` fields — without this, both could read zero and every
    ambiguity test in the pure module would still pass.
    """
    _advance_to_forward_observation(ebull_test_conn)
    promote_strategy(
        ebull_test_conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_VERSION,
        to_stage="paper_enabled",
        promoted_by="test",
        reason="paper",
        evidence_ref="paper-1",
    )

    report = assess_live_gate(
        ebull_test_conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_VERSION,
        requested_capital=Decimal("100"),
    )
    assert report.facts.forward_observation_entries == 1
    assert report.facts.paper_enabled_entries == 1
    assert "forward_window_ambiguous" not in report.refusal_codes
    assert "paper_window_ambiguous" not in report.refusal_codes
