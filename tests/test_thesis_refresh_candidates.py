"""Integration test for _thesis_refresh_candidates (#1919 PR-B; #2065 leg 3).

One DB-tier test per genuinely-new SQL mechanism (house rule): the
held-first ∪ top-N-ranked candidate query, pinned against the real
``positions`` / ``scores`` tables. Verifies the Codex ckpt-2 finding
fix — top-N must come from the LATEST scoring run (the ``GET /scores``
cohort definition), not a per-instrument latest-row scan that
resurrects names dropped from the current ranked cohort. #2065 adds
the has-thesis third leg (tradable-only, symbol-ordered, deduped) that
replaces the removed fundamentals_sync cascade's refresh coverage.
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


def _seed_thesis(conn: psycopg.Connection[tuple], iid: int, version: int = 1) -> None:
    conn.execute(
        """
        INSERT INTO theses (instrument_id, thesis_version, thesis_type, stance, memo_markdown)
        VALUES (%s, %s, 'value', 'watch', 'memo')
        """,
        (iid, version),
    )


def test_has_thesis_third_leg_appended_tradable_only(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#2065: instruments with an existing thesis come AFTER held +
    ranked, symbol-ordered, deduped, tradable-only. No-thesis
    instruments never enter via this leg (the wide-backfill gate)."""
    now = datetime.now(UTC)

    for iid, symbol in [(11, "HLD"), (12, "RNK"), (13, "THB"), (14, "THA"), (16, "NOT")]:
        _seed_instrument(ebull_test_conn, iid, symbol)
    # Non-tradable instrument with a thesis — excluded from leg 3.
    ebull_test_conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (15, 'DEL', 'DEL', FALSE)"
    )

    _seed_position(ebull_test_conn, 11, 3.0)  # held
    _seed_score(ebull_test_conn, 12, 1, now)  # ranked
    # Theses: held (dedup), ranked (dedup), two outside-scope tradable
    # (symbol order THA < THB), one non-tradable (excluded). iid=16 has
    # no thesis → absent.
    for iid in (11, 12, 13, 14, 15):
        _seed_thesis(ebull_test_conn, iid)
    ebull_test_conn.commit()

    candidates = _thesis_refresh_candidates(ebull_test_conn)

    # held → ranked → has-thesis (THA=14 before THB=13); 15 (non-tradable)
    # and 16 (no thesis) absent; no duplicates for 11/12.
    assert candidates == [11, 12, 14, 13]
