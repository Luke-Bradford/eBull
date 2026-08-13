"""DB-tier proof that the #2017 context-audit columns persist on the run row
and survive a failed run.

Drives the narrow insert/failure seams (`_insert_thesis_run`,
`_record_thesis_run_failure`) rather than the full `generate_thesis` path,
which needs live LLM clients unavailable in the test env — same rationale as
tests/test_thesis_valuation_audit.py. `generate_thesis` calls
`_insert_thesis_run` (with the context audit) then commits BEFORE the LLM, and
the failure path only UPDATEs status — so proving the columns are written at
insert and untouched by `_record_thesis_run_failure` proves the failed-run
capture end to end.
"""

from __future__ import annotations

import pytest

from app.services.thesis import _insert_thesis_run, _record_thesis_run_failure

pytestmark = pytest.mark.db

_SUMMARY = {
    "prompt_version": "v4",
    "blocks": {
        "fundamentals": {"available": True, "count": 5, "as_of": "2025-03-31"},
        "valuation": {"available": False, "status": "no_live_quote"},
    },
}


@pytest.fixture
def conn(ebull_test_conn):
    return ebull_test_conn


def _seed_instrument(conn, instrument_id: int) -> int:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
        (instrument_id, "TCA", "Thesis Context Audit Test Co"),
    )
    conn.commit()
    return instrument_id


def test_insert_persists_context_audit(conn) -> None:
    iid = _seed_instrument(conn, 9151)
    run_id = _insert_thesis_run(
        conn,
        iid,
        "manual",
        provider="openai_compatible",
        model="qwen3:14b",
        critic_model="qwen3:14b",
        context_sha256="a" * 64,
        context_summary=_SUMMARY,
    )
    conn.commit()

    row = conn.execute(
        "SELECT context_sha256, context_summary FROM thesis_runs WHERE run_id = %s", (run_id,)
    ).fetchone()
    assert row[0] == "a" * 64
    assert row[1] == _SUMMARY  # JSONB round-trips to the same dict


def test_missing_context_audit_leaves_nulls(conn) -> None:
    # Backward-compat: existing callers (and historical rows) leave both NULL.
    iid = _seed_instrument(conn, 9152)
    run_id = _insert_thesis_run(
        conn, iid, "manual", provider="openai_compatible", model="qwen3:14b", critic_model="qwen3:14b"
    )
    conn.commit()
    row = conn.execute(
        "SELECT context_sha256, context_summary FROM thesis_runs WHERE run_id = %s", (run_id,)
    ).fetchone()
    assert row == (None, None)


def test_failed_run_retains_context_audit(conn) -> None:
    # The #2017 core property: audit written at insert (before the LLM)
    # survives the failure path (status-only UPDATE) — the #2007 AMSC class.
    iid = _seed_instrument(conn, 9153)
    run_id = _insert_thesis_run(
        conn,
        iid,
        "scheduled",
        provider="openai_compatible",
        model="qwen3:14b",
        critic_model="qwen3:14b",
        context_sha256="b" * 64,
        context_summary=_SUMMARY,
    )
    conn.commit()

    _record_thesis_run_failure(conn, run_id, ValueError("Writer: incoherent targets bear>base"))

    row = conn.execute(
        "SELECT status, context_sha256, context_summary FROM thesis_runs WHERE run_id = %s", (run_id,)
    ).fetchone()
    assert row[0] == "failed"
    assert row[1] == "b" * 64
    assert row[2] == _SUMMARY
