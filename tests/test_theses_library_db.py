"""DB-tier test for the #1902 theses-library SQL mechanism.

One integration test for the genuinely-new SQL shape (test-quality
skill): ``_LIBRARY_SQL`` — DISTINCT ON latest-per-instrument over the
versioned theses table, plus the held-EXISTS, latest-score LATERAL and
latest-thesis_runs LATERAL joins. Filters/pagination are pure Python
(``filter_and_page_library``) and are table-tested in
tests/test_api_theses.py without a DB.
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg.rows
import pytest

from app.api.theses import _HELD_NO_THESIS_SQL, _LIBRARY_SQL
from app.services.scoring import _DEFAULT_MODEL_VERSION

_T1 = datetime(2026, 6, 1, tzinfo=UTC)
_T2 = datetime(2026, 6, 20, tzinfo=UTC)


@pytest.fixture
def conn(ebull_test_conn):
    return ebull_test_conn


def _seed(conn) -> tuple[int, int]:
    a, b = 8101, 8102
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)"
        " VALUES (%s, 'LIBA', 'Library A Co', TRUE), (%s, 'LIBB', 'Library B Co', TRUE)",
        (a, b),
    )
    # A: two thesis versions — DISTINCT ON must pick the newer one.
    conn.execute(
        """
        INSERT INTO theses (instrument_id, thesis_version, thesis_type, stance,
                            confidence_score, memo_markdown, critic_json, created_at)
        VALUES (%(a)s, 1, 'value', 'watch', 0.4, 'old memo', NULL, %(t1)s),
               (%(a)s, 2, 'value', 'buy', 0.8, 'new memo',
                '{"verdict": "Weak challenge"}'::jsonb, %(t2)s),
               (%(b)s, 1, 'turnaround', 'avoid', 0.2, 'b memo', NULL, %(t1)s)
        """,
        {"a": a, "b": b, "t1": _T1, "t2": _T2},
    )
    # A is held; B is not.
    conn.execute(
        "INSERT INTO positions (instrument_id, current_units, cost_basis, source) VALUES (%s, 10, 100, 'broker_sync')",
        (a,),
    )
    # A: two runs — the LATERAL must surface the newer (failed) one.
    conn.execute(
        """
        INSERT INTO thesis_runs (instrument_id, trigger, started_at, status, error)
        VALUES (%(a)s, 'manual', %(t1)s, 'ok', NULL),
               (%(a)s, 'scheduled', %(t2)s, 'failed', 'writer schema error (finish_reason=stop)')
        """,
        {"a": a, "t1": _T1, "t2": _T2},
    )
    # A: two scores at the default model version — LATERAL picks the newer.
    conn.execute(
        """
        INSERT INTO scores (instrument_id, scored_at, total_score, model_version, rank)
        VALUES (%(a)s, %(t1)s, 0.40, %(mv)s, 50),
               (%(a)s, %(t2)s, 0.75, %(mv)s, 3)
        """,
        {"a": a, "t1": _T1, "t2": _T2, "mv": _DEFAULT_MODEL_VERSION},
    )
    conn.commit()
    return a, b


class TestLibrarySql:
    def test_latest_per_instrument_with_context(self, conn) -> None:
        a, b = _seed(conn)
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(_LIBRARY_SQL, {"mv": _DEFAULT_MODEL_VERSION})
            rows = {r["instrument_id"]: r for r in cur.fetchall() if r["instrument_id"] in (a, b)}

        assert set(rows) == {a, b}

        row_a = rows[a]
        # DISTINCT ON picked v2, not v1.
        assert row_a["thesis_version"] == 2
        assert row_a["stance"] == "buy"
        assert row_a["critic_json"] == {"verdict": "Weak challenge"}
        assert row_a["is_held"] is True
        # LATERALs picked the newest score and the newest (failed) run.
        assert float(row_a["latest_score"]) == 0.75
        assert row_a["latest_rank"] == 3
        assert row_a["run_status"] == "failed"
        assert row_a["run_trigger"] == "scheduled"

        row_b = rows[b]
        assert row_b["thesis_version"] == 1
        assert row_b["is_held"] is False
        assert row_b["latest_score"] is None
        assert row_b["run_status"] is None

        # Global ordering: newest thesis first — A (T2) before B (T1).
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(_LIBRARY_SQL, {"mv": _DEFAULT_MODEL_VERSION})
            ordered = [r["instrument_id"] for r in cur.fetchall() if r["instrument_id"] in (a, b)]
        assert ordered == [a, b]

    def test_held_without_thesis_surfaces_as_gap_row(self, conn) -> None:
        a, b = _seed(conn)
        # C: held, but no thesis row at all — must come back from the
        # gap query with typed-NULL thesis columns, and must NOT appear
        # in the library query.
        c = 8103
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)"
            " VALUES (%s, 'LIBC', 'Library C Co', TRUE)",
            (c,),
        )
        conn.execute(
            "INSERT INTO positions (instrument_id, current_units, cost_basis, source)"
            " VALUES (%s, 5, 50, 'broker_sync')",
            (c,),
        )
        conn.commit()

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(_HELD_NO_THESIS_SQL, {"mv": _DEFAULT_MODEL_VERSION})
            gap_rows = {r["instrument_id"]: r for r in cur.fetchall() if r["instrument_id"] in (a, b, c)}
            cur.execute(_LIBRARY_SQL, {"mv": _DEFAULT_MODEL_VERSION})
            lib_ids = {r["instrument_id"] for r in cur.fetchall() if r["instrument_id"] in (a, b, c)}

        # A is held WITH a thesis → library only. C is held WITHOUT → gap only.
        assert set(gap_rows) == {c}
        assert lib_ids == {a, b}
        row_c = gap_rows[c]
        assert row_c["thesis_id"] is None
        assert row_c["stance"] is None
        assert row_c["created_at"] is None
        assert row_c["is_held"] is True
        assert row_c["symbol"] == "LIBC"
