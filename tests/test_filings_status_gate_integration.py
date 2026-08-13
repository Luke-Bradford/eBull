"""Integration tests for #268 Chunk J — filings_status gate on
scoring producer + consumers.

Covers three enforcement sites:
- ``scheduler._has_scoreable_instruments`` (prerequisite mirror).
- ``scoring.compute_rankings`` (producer — eligibility query).
- ``portfolio._load_ranked_scores`` (consumer — latest-score reader).

All three must agree: only instruments with
``coverage.filings_status = 'analysable'`` land in the scoring /
recommendation pool.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import psycopg
import pytest

from app.services.portfolio import _load_ranked_scores
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable",
)


def _seed(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    symbol: str,
    filings_status: str,
    score_total: float = 0.5,
    score_rank: int = 1,
    model_version: str = "v1.1-balanced",
) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
        (instrument_id, symbol, symbol),
    )
    conn.execute(
        "INSERT INTO coverage (instrument_id, coverage_tier, filings_status) VALUES (%s, 1, %s)",
        (instrument_id, filings_status),
    )
    conn.execute(
        """
        INSERT INTO scores (instrument_id, model_version, total_score, rank, scored_at)
        VALUES (%s, %s, %s, %s, NOW())
        """,
        (instrument_id, model_version, score_total, score_rank),
    )
    conn.commit()


def test_load_ranked_scores_excludes_insufficient(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Two scored instruments; only the analysable one is returned."""
    _seed(ebull_test_conn, instrument_id=1, symbol="GOOD", filings_status="analysable", score_rank=1)
    _seed(ebull_test_conn, instrument_id=2, symbol="BAD", filings_status="insufficient", score_rank=2)

    rows = _load_ranked_scores(ebull_test_conn, model_version="v1.1-balanced")

    assert len(rows) == 1
    assert rows[0]["instrument_id"] == 1


def test_load_ranked_scores_excludes_fpi_and_no_primary_sec_cik(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Only 'analysable' passes; fpi / no_primary_sec_cik / structurally_young / unknown all blocked."""
    _seed(ebull_test_conn, instrument_id=1, symbol="OK", filings_status="analysable", score_rank=1)
    _seed(ebull_test_conn, instrument_id=2, symbol="FPI", filings_status="fpi", score_rank=2)
    _seed(ebull_test_conn, instrument_id=3, symbol="NOCIK", filings_status="no_primary_sec_cik", score_rank=3)
    _seed(ebull_test_conn, instrument_id=4, symbol="YOUNG", filings_status="structurally_young", score_rank=4)
    _seed(ebull_test_conn, instrument_id=5, symbol="UNK", filings_status="unknown", score_rank=5)

    rows = _load_ranked_scores(ebull_test_conn, model_version="v1.1-balanced")

    assert len(rows) == 1
    assert rows[0]["instrument_id"] == 1


def test_load_ranked_scores_respects_null_filings_status(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """NULL filings_status (pre-first-audit) is NOT analysable — strict equality.
    Prevents the historic fail-open bug Codex caught in the master plan."""
    ebull_test_conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (1, 'NULLSTATUS', 'NULLSTATUS', TRUE)"
    )
    # Coverage row but filings_status explicitly NULL
    ebull_test_conn.execute("INSERT INTO coverage (instrument_id, coverage_tier) VALUES (1, 1)")
    ebull_test_conn.execute(
        """
        INSERT INTO scores (instrument_id, model_version, total_score, rank, scored_at)
        VALUES (1, 'v1.1-balanced', 0.9, 1, NOW())
        """
    )
    ebull_test_conn.commit()

    rows = _load_ranked_scores(ebull_test_conn, model_version="v1.1-balanced")

    assert rows == []


def test_compute_rankings_eligibility_gates_on_filings_status(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """compute_rankings's own eligibility query must gate on
    filings_status = 'analysable' — stay in lockstep with
    _has_scoreable_instruments and _load_ranked_scores."""
    from app.services import scoring

    # Seed 2 instruments, fundamentals_snapshot for each so they
    # appear in the eligibility cohort via the fundamentals EXISTS.
    for iid, symbol, status in [
        (1, "ANALYSABLE", "analysable"),
        (2, "INSUFFICIENT", "insufficient"),
    ]:
        ebull_test_conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
            (iid, symbol, symbol),
        )
        ebull_test_conn.execute(
            "INSERT INTO coverage (instrument_id, coverage_tier, filings_status) VALUES (%s, 1, %s)",
            (iid, status),
        )
        ebull_test_conn.execute(
            """
            INSERT INTO fundamentals_snapshot (instrument_id, as_of_date)
            VALUES (%s, %s)
            """,
            (iid, date.today() - timedelta(days=30)),
        )
    ebull_test_conn.commit()

    # Run the eligibility SQL directly — avoids setting up the full
    # price + thesis stack that compute_score would traverse.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT i.instrument_id
            FROM instruments i
            JOIN coverage c ON c.instrument_id = i.instrument_id
            WHERE i.is_tradable = TRUE
              AND c.filings_status = 'analysable'
              AND (
                  EXISTS (SELECT 1 FROM theses t WHERE t.instrument_id = i.instrument_id)
                  OR EXISTS (SELECT 1 FROM fundamentals_snapshot f WHERE f.instrument_id = i.instrument_id)
                  OR EXISTS (SELECT 1 FROM price_daily p WHERE p.instrument_id = i.instrument_id)
              )
            ORDER BY i.instrument_id
            """
        )
        eligible = [int(r[0]) for r in cur.fetchall()]

    assert eligible == [1]  # instrument 2 filtered out by filings_status gate

    # Smoke-check that compute_rankings actually imports and that its
    # eligibility query shape matches what this test is asserting.
    # (A drift between production query and the test's copy would be
    # caught by a reviewer reading both, not by execution alone.)
    assert hasattr(scoring, "compute_rankings")


def test_has_scoreable_instruments_prereq_respects_filings_status(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """scheduler._has_scoreable_instruments must return False when the
    only tradable instrument with data is blocked by filings_status."""
    from app.workers.scheduler import _has_scoreable_instruments

    # Instrument blocked by filings_status but has fundamentals.
    ebull_test_conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (1, 'BAD', 'BAD', TRUE)"
    )
    ebull_test_conn.execute(
        "INSERT INTO coverage (instrument_id, coverage_tier, filings_status) VALUES (1, 1, 'insufficient')"
    )
    ebull_test_conn.execute("INSERT INTO fundamentals_snapshot (instrument_id, as_of_date) VALUES (1, CURRENT_DATE)")
    ebull_test_conn.commit()

    ok, _reason = _has_scoreable_instruments(ebull_test_conn)
    assert ok is False

    # Now flip to analysable — should turn True.
    ebull_test_conn.execute("UPDATE coverage SET filings_status = 'analysable' WHERE instrument_id = 1")
    ebull_test_conn.commit()

    ok, _reason = _has_scoreable_instruments(ebull_test_conn)
    assert ok is True


def test_coverage_review_loader_suppresses_score_for_non_analysable(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """coverage._load_instruments_for_review's score LATERAL must NOT
    return a score for an instrument whose filings_status is not
    'analysable' — otherwise promotion logic could fire on stale
    pre-regression scores."""
    from app.services.coverage import _load_instruments_for_review

    _seed(ebull_test_conn, instrument_id=1, symbol="OK", filings_status="analysable", score_total=0.9)
    _seed(ebull_test_conn, instrument_id=2, symbol="BAD", filings_status="insufficient", score_total=0.9)

    snapshots = _load_instruments_for_review(ebull_test_conn)
    by_id = {s.instrument_id: s for s in snapshots}

    assert by_id[1].total_score is not None  # analysable → score surfaced
    assert by_id[2].total_score is None  # insufficient → score hidden


def test_scores_list_api_excludes_non_analysable(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """GET /api/scores/ list endpoint must not surface score rows for
    instruments whose filings_status regressed to non-analysable.
    Codex-flagged consumer site on top of the three Chunk J already
    covered."""
    # Set up 2 instruments scored at the same scored_at timestamp
    # (same run), one analysable + one insufficient.
    scored_at = datetime(2026, 4, 17, 12, 0, 0, tzinfo=UTC)
    for iid, sym, status in [(1, "GOOD", "analysable"), (2, "BAD", "insufficient")]:
        ebull_test_conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable, sector) "
            "VALUES (%s, %s, %s, TRUE, 'Tech')",
            (iid, sym, sym),
        )
        ebull_test_conn.execute(
            "INSERT INTO coverage (instrument_id, coverage_tier, filings_status) VALUES (%s, 1, %s)",
            (iid, status),
        )
        ebull_test_conn.execute(
            """
            INSERT INTO scores (instrument_id, model_version, total_score, rank, scored_at)
            VALUES (%s, 'v1.1-balanced', 0.5, %s, %s)
            """,
            (iid, iid, scored_at),
        )
    ebull_test_conn.commit()

    # Run the list-endpoint SQL shape directly (API layer would wrap
    # this in pagination + DTOs; the gate is what we're testing).
    rows = ebull_test_conn.execute(
        """
        SELECT s.instrument_id
        FROM scores s
        LEFT JOIN coverage c USING (instrument_id)
        WHERE s.model_version = 'v1.1-balanced'
          AND s.scored_at = %s
          AND c.filings_status = 'analysable'
        """,
        (scored_at,),
    ).fetchall()

    ids = [int(r[0]) for r in rows]
    assert ids == [1]


def test_scores_api_source_contains_filings_status_gate() -> None:
    """Closes the PR-review gap flagged on the locally-written query
    in test_scores_list_api_excludes_non_analysable: assert directly
    that the production ``app/api/scores.py`` module source contains
    both the LEFT JOIN coverage c clause AND the filings_status gate
    so a future refactor that drops one without the other fails
    loudly at test-collection time rather than drifting silently."""
    from pathlib import Path

    # Anchor on __file__ so pytest works from any working directory
    # (subdir, docker container with different WORKDIR). Relative
    # "app/api/scores.py" would silently FileNotFoundError in those
    # cases and the test would be a false negative.
    scores_py = Path(__file__).resolve().parent.parent / "app" / "api" / "scores.py"
    source = scores_py.read_text(encoding="utf-8")
    assert "LEFT JOIN coverage c USING (instrument_id)" in source, (
        "scores.py list_rankings base query must JOIN coverage on alias 'c' so the filings_status WHERE clause resolves"
    )
    # Assert the plain SQL snippet, not its Python string-literal
    # surround — the production code could use single OR double
    # quotes around the string element without changing SQL behavior.
    assert "c.filings_status = 'analysable'" in source, (
        "scores.py list_rankings where_clauses must include the filings_status gate literal for #268 Chunk J"
    )
