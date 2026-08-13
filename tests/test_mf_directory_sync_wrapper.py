"""Tests for the dedicated ``mf_directory_sync`` bootstrap-stage wrapper (#1174).

Spec §6 cases 12-13. The wrapper-effect tests verify:

* On success: rows land in ``cik_refresh_mf_directory``; tracker
  ``row_count`` records the directory write count; the orchestrator
  capability layer advertises ``class_id_mapping_ready`` from a
  ``success`` status (per ``_STAGE_PROVIDES``).
* On failure: the wrapper does NOT swallow the exception (no fail-soft
  on the bootstrap path — daily cron retains its own fail-soft); the
  capability is not advertised; ``_classify_dead_cap`` returns the
  discriminant that maps downstream stages to ``blocked``.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from app.services.bootstrap_orchestrator import (
    _classify_dead_cap,
    _satisfied_capabilities,
)
from app.workers.scheduler import (
    JOB_MF_DIRECTORY_SYNC,
    mf_directory_sync,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    symbol: str,
) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} fund"),
    )


def _fake_provider(payload: dict[str, Any]) -> Any:
    provider = MagicMock()
    provider.fetch_document_text.return_value = json.dumps(payload)
    provider.__enter__ = MagicMock(return_value=provider)
    provider.__exit__ = MagicMock(return_value=False)
    return provider


def _fake_raising_provider() -> Any:
    provider = MagicMock()
    provider.fetch_document_text.side_effect = RuntimeError("simulated SEC outage")
    provider.__enter__ = MagicMock(return_value=provider)
    provider.__exit__ = MagicMock(return_value=False)
    return provider


# ---------------------------------------------------------------------------
# Case 12 — success: writes rows + records tracker + advertises capability
# ---------------------------------------------------------------------------


def test_mf_directory_sync_success_writes_rows_and_advertises_cap(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    _seed_instrument(ebull_test_conn, iid=5001, symbol="VFIAX")
    ebull_test_conn.commit()
    payload = {
        "fields": ["cik", "seriesId", "classId", "symbol"],
        "data": [
            [36405, "S000002839", "C000010048", "VFIAX"],
            [36405, "S000002839", "C000010049", "VFINX"],
            [1100663, "S000004310", "C000012124", "IVV"],
            [1100663, "S000004311", "C000012125", "AGG"],
            [819118, "S000006027", "C000016700", "FXAIX"],
        ],
    }
    provider = _fake_provider(payload)

    # Stand-in for the connect/SecFilingsProvider context managers. We
    # let the wrapper call refresh_mf_directory against the actual test
    # DB connection but mock the SEC provider + the psycopg.connect call.
    captured_tracker: dict[str, Any] = {}

    class _CapturingTracker:
        def __init__(self) -> None:
            self.row_count = 0

        def __enter__(self) -> _CapturingTracker:
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            captured_tracker["row_count"] = self.row_count
            return False

    with (
        patch("app.workers.scheduler.SecFilingsProvider", return_value=provider),
        patch("app.workers.scheduler.psycopg.connect", return_value=_PassthroughConn(ebull_test_conn)),
        patch("app.workers.scheduler._tracked_job", return_value=_CapturingTracker()),
    ):
        mf_directory_sync({})

    # Directory + ext-id rows landed.
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM cik_refresh_mf_directory")
        row = cur.fetchone()
    assert row is not None
    assert int(row[0]) == 5

    # Tracker row_count records the directory write count.
    assert captured_tracker["row_count"] == 5

    # Capability layer advertises class_id_mapping_ready from a success status.
    caps = _satisfied_capabilities(
        statuses={"mf_directory_sync": "success"},
        rows_processed={"mf_directory_sync": 5},
    )
    assert "class_id_mapping_ready" in caps


# ---------------------------------------------------------------------------
# Case 13 — failure: NO fail-soft + cap not advertised + T8 blocks
# ---------------------------------------------------------------------------


def test_mf_directory_sync_failure_propagates_and_blocks_downstream(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    provider = _fake_raising_provider()

    class _Tracker:
        def __init__(self) -> None:
            self.row_count = 0

        def __enter__(self) -> _Tracker:
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

    # The bootstrap path MUST NOT swallow the exception.
    with (
        patch("app.workers.scheduler.SecFilingsProvider", return_value=provider),
        patch("app.workers.scheduler.psycopg.connect", return_value=_PassthroughConn(ebull_test_conn)),
        patch("app.workers.scheduler._tracked_job", return_value=_Tracker()),
        pytest.raises(RuntimeError, match="simulated SEC outage"),
    ):
        mf_directory_sync({})

    # Rollback after the raise so other tests see a clean conn.
    ebull_test_conn.rollback()

    # Capability NOT advertised when the providing stage errored.
    caps = _satisfied_capabilities(
        statuses={"mf_directory_sync": "error"},
        rows_processed={},
    )
    assert "class_id_mapping_ready" not in caps

    # Classify_dead_cap returns the 'error' discriminant — orchestrator
    # then transitions downstream T8 to ``blocked`` (NOT ``skipped``).
    classification = _classify_dead_cap(
        cap="class_id_mapping_ready",
        statuses={"mf_directory_sync": "error"},
        rows_processed={},
    )
    assert classification == "error"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _PassthroughConn:
    """Context-manager facade around an already-open conn so the wrapper
    can use ``with psycopg.connect(...) as conn:`` while we keep the
    real test DB connection alive across the call."""

    def __init__(self, conn: psycopg.Connection[Any]) -> None:
        self._conn = conn

    def __enter__(self) -> psycopg.Connection[Any]:
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        # Do NOT close — the fixture owns lifecycle.
        return False


# ---------------------------------------------------------------------------
# Smoke — job-name constant is what the registry expects
# ---------------------------------------------------------------------------


def test_job_name_constant() -> None:
    assert JOB_MF_DIRECTORY_SYNC == "mf_directory_sync"
