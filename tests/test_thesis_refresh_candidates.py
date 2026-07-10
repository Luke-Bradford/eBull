"""Integration test for _thesis_refresh_candidates (#1919 PR-B).

One DB-tier test for the genuinely-new SQL mechanism (house rule): the
held-first ∪ top-N-ranked candidate query, pinned against the real
``positions`` / ``scores`` tables. Verifies the Codex ckpt-2 finding
fix — top-N must come from the LATEST scoring run (the ``GET /scores``
cohort definition), not a per-instrument latest-row scan that
resurrects names dropped from the current ranked cohort.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from app.services.scoring import _DEFAULT_MODEL_VERSION
from app.workers.scheduler import _thesis_refresh_candidates
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable",
)


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
        (iid, symbol, symbol),
    )


def _seed_position(conn: psycopg.Connection[tuple], iid: int, units: float) -> None:
    conn.execute(
        "INSERT INTO positions (instrument_id, current_units, source) VALUES (%s, %s, 'broker_sync')",
        (iid, units),
    )


def _seed_score(
    conn: psycopg.Connection[tuple],
    iid: int,
    rank: int | None,
    scored_at: datetime,
    model_version: str = _DEFAULT_MODEL_VERSION,
) -> None:
    conn.execute(
        "INSERT INTO scores (instrument_id, scored_at, rank, model_version) VALUES (%s, %s, %s, %s)",
        (iid, scored_at, rank, model_version),
    )


def test_held_first_then_latest_run_cohort(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Held (units > 0) come first, symbol-ordered; then the latest
    scoring RUN's ranked rows in rank order, deduped against held.
    Older-run ranks, NULL ranks, other model_versions, and zero-unit
    positions are all excluded."""
    now = datetime.now(UTC)
    old = now - timedelta(hours=6)

    for iid, symbol in [(1, "BBB"), (2, "AAA"), (3, "CCC"), (4, "DDD"), (5, "EEE")]:
        _seed_instrument(ebull_test_conn, iid, symbol)

    _seed_position(ebull_test_conn, 1, 10.0)  # held — "BBB"
    _seed_position(ebull_test_conn, 2, 5.0)  # held — "AAA" (sorts first)
    _seed_position(ebull_test_conn, 5, 0.0)  # closed — excluded

    # Older run: iid=4 was rank 1 but is NOT in the latest run — must
    # not be resurrected into the candidate set.
    _seed_score(ebull_test_conn, 4, 1, old)
    # Latest run: iid=3 rank 2, iid=1 rank 1 (already held → deduped),
    # iid=5 rank NULL (unranked → excluded).
    _seed_score(ebull_test_conn, 1, 1, now)
    _seed_score(ebull_test_conn, 3, 2, now)
    _seed_score(ebull_test_conn, 5, None, now)
    # Foreign model_version in an even newer run — must not shift the
    # MAX(scored_at) cohort for the default model_version.
    _seed_score(ebull_test_conn, 4, 1, now + timedelta(minutes=5), model_version="other-model")
    ebull_test_conn.commit()

    candidates = _thesis_refresh_candidates(ebull_test_conn)

    # Held first (AAA=2, BBB=1), then latest-run ranked (rank1=1 deduped,
    # rank2=3). Old-run iid=4 and zero-unit iid=5 absent.
    assert candidates == [2, 1, 3]
