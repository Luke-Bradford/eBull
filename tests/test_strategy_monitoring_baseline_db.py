"""#2500 slice 1 — the baseline read against the real tables (DB tier)."""

from __future__ import annotations

from typing import Any

import psycopg
import pytest

from app.services.result_ledger import store_in_sample_result
from app.services.strategy_monitoring_baseline import (
    MISSING_ENVELOPE_COMPONENTS,
    NO_PROMOTION_FOR_VERSION,
    PROMOTION_EVIDENCE_MISSING,
    baseline_unavailable_reasons,
    resolve_monitoring_baseline,
)
from app.services.strategy_promotion_evidence_store import store_promotion_evidence
from tests.test_result_ledger import build_result
from tests.test_strategy_promotion_evidence_store import _evidence


def _pin(conn: psycopg.Connection[Any], *, stage: str, result_ids: tuple[int, ...]) -> int:
    row = conn.execute(
        """
        INSERT INTO strategy_promotions (
            strategy_id, strategy_version, from_stage, to_stage, gate_version,
            evidence_ref, promoted_by, reason
        ) VALUES ('S-MON', 'v1', NULL, %s, 'gate-v1', 'ref:test', 'test', 'baseline read fixture')
        RETURNING promotion_id
        """,
        (stage,),
    ).fetchone()
    assert row is not None
    promotion_id = int(row[0])
    for result_id in result_ids:
        conn.execute(
            "INSERT INTO strategy_promotion_results (promotion_id, result_id) VALUES (%s, %s)",
            (promotion_id, result_id),
        )
    return promotion_id


def test_baseline_reads_the_immutable_evidence_and_absence_is_explicit(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    assert resolve_monitoring_baseline(conn, strategy_id="S-MON", strategy_version="v1") is None
    assert baseline_unavailable_reasons(conn, strategy_id="S-MON", strategy_version="v1") == (NO_PROMOTION_FOR_VERSION,)

    result_id = store_in_sample_result(conn, build_result(namespace="in_sample"))
    promotion_id = _pin(conn, stage="historical_validated", result_ids=(result_id,))

    # Pinned, but the immutable evidence record does not exist yet.
    assert resolve_monitoring_baseline(conn, strategy_id="S-MON", strategy_version="v1") is None
    assert baseline_unavailable_reasons(conn, strategy_id="S-MON", strategy_version="v1") == (
        PROMOTION_EVIDENCE_MISSING,
    )

    evidence = _evidence()
    store_promotion_evidence(conn, result_id=result_id, evidence=evidence)

    baseline = resolve_monitoring_baseline(conn, strategy_id="S-MON", strategy_version="v1")
    assert baseline is not None
    assert baseline.promotion_id == promotion_id
    assert baseline.baseline_stage == "historical_validated"
    assert baseline.result_ids == (result_id,)
    assert baseline.evidence == (evidence,)
    assert len(baseline.evidence_payload_sha256) == 1
    assert len(baseline.evidence_payload_sha256[0]) == 64
    assert baseline.absent_components == MISSING_ENVELOPE_COMPONENTS
    assert baseline_unavailable_reasons(conn, strategy_id="S-MON", strategy_version="v1") == ()


def test_a_database_error_propagates_rather_than_reading_as_absent(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Fail-closed must not mean 'swallow'.

    Returning ``None`` on a raised error would leave the caller's transaction aborted and
    make *the database is down* indistinguishable from *nothing was approved*.
    """
    conn = ebull_test_conn
    conn.execute("ALTER TABLE strategy_promotions RENAME TO strategy_promotions_hidden")
    try:
        with pytest.raises(psycopg.errors.UndefinedTable):
            resolve_monitoring_baseline(conn, strategy_id="S-MON", strategy_version="v1")
    finally:
        conn.rollback()
