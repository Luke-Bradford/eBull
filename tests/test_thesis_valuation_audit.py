"""DB-tier tests for the #2009 PR-B divergence audit-row invariant (sql/222).

Drives the narrower seam rather than the full ``generate_thesis`` path
(which needs live LLM clients unavailable in the test env): seed a
``theses`` row via ``_insert_thesis_atomic``, then call
``_insert_thesis_valuation_audit`` inside the SAME transaction (mirroring
how ``generate_thesis`` calls them back-to-back inside its
``with conn.transaction():`` block), and assert the resulting row.

Keeps ``tests/test_fair_value_band_policy.py`` (pure ``compute_divergence``
table tests) DB-free — this module is the one db-tier proof that the row
is actually inserted, inserted in-txn, and carries the right snapshot
fields (Codex ckpt-1 PR-B MED).
"""

from __future__ import annotations

import datetime

import pytest

from app.services.fair_value_band import DIVERGENCE_THRESHOLD, METHOD_VERSION, compute_divergence
from app.services.thesis import _insert_thesis_atomic, _insert_thesis_valuation_audit, _to_float

pytestmark = pytest.mark.db

_VALID_WRITER = {
    "thesis_type": "compounder",
    "confidence_score": 0.75,
    "stance": "buy",
    "buy_zone_low": 150.0,
    "buy_zone_high": 170.0,
    "base_value": 200.0,
    "bull_value": 250.0,
    "bear_value": 120.0,
    "break_conditions": ["Revenue growth falls below 10% for two consecutive quarters"],
    "memo_markdown": "## Test\n\nMemo body.",
}


@pytest.fixture
def conn(ebull_test_conn):
    return ebull_test_conn


def _seed_instrument(conn, instrument_id: int = 9101) -> int:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
        (instrument_id, "TVA", "Thesis Valuation Audit Test Co"),
    )
    conn.commit()
    return instrument_id


def test_audit_row_present_band(conn) -> None:
    """Band present (base non-null) -> exactly one audit row, band_base
    snapshotted, divergence_pct non-NULL."""
    iid = _seed_instrument(conn)

    band_base = 180.0
    llm_base = _to_float(_VALID_WRITER["base_value"])  # 200.0
    divergence_pct, divergence_flag = compute_divergence(llm_base, band_base, DIVERGENCE_THRESHOLD)

    with conn.transaction():
        thesis_id, _version = _insert_thesis_atomic(
            conn,
            iid,
            _VALID_WRITER,
            None,
            model="qwen3:14b",
            provider="openai_compatible",
        )
        _insert_thesis_valuation_audit(
            conn,
            thesis_id,
            band_base=band_base,
            band_quality_status="high",
            price_as_of="2026-07-10",
            llm_base=llm_base,
            divergence_pct=divergence_pct,
            divergence_flag=divergence_flag,
        )

    rows = conn.execute(
        """
        SELECT thesis_id, band_method_version, band_base, band_quality_status,
               price_as_of, llm_base, divergence_pct, divergence_flag
        FROM thesis_valuation_audit
        WHERE thesis_id = %s
        """,
        (thesis_id,),
    ).fetchall()

    assert len(rows) == 1
    row = rows[0]
    assert row[0] == thesis_id
    assert row[1] == METHOD_VERSION
    assert float(row[2]) == band_base
    assert row[3] == "high"
    assert row[4] == datetime.date(2026, 7, 10)
    assert row[5] is not None and float(row[5]) == llm_base
    assert row[6] is not None  # divergence_pct
    assert row[7] is not None  # divergence_flag
    # (200 - 180) / 180 ≈ 0.1111 <= threshold 0.30 -> not divergent
    assert float(row[6]) == pytest.approx(divergence_pct)
    assert row[7] is False


def test_audit_row_absent_band_null_divergence(conn) -> None:
    """No band (available:false) -> audit row still written, divergence_pct
    AND divergence_flag both NULL (never 0/false, #1632)."""
    iid = _seed_instrument(conn, 9102)

    llm_base = _to_float(_VALID_WRITER["base_value"])
    band_base = None
    divergence_pct, divergence_flag = compute_divergence(llm_base, band_base, DIVERGENCE_THRESHOLD)
    assert divergence_pct is None
    assert divergence_flag is None

    with conn.transaction():
        thesis_id, _version = _insert_thesis_atomic(
            conn,
            iid,
            _VALID_WRITER,
            None,
            model="qwen3:14b",
            provider="openai_compatible",
        )
        _insert_thesis_valuation_audit(
            conn,
            thesis_id,
            band_base=band_base,
            band_quality_status=None,
            price_as_of=None,
            llm_base=llm_base,
            divergence_pct=divergence_pct,
            divergence_flag=divergence_flag,
        )

    rows = conn.execute(
        """
        SELECT band_base, band_quality_status, price_as_of, divergence_pct, divergence_flag
        FROM thesis_valuation_audit
        WHERE thesis_id = %s
        """,
        (thesis_id,),
    ).fetchall()

    assert len(rows) == 1
    row = rows[0]
    assert row[0] is None  # band_base
    assert row[1] is None  # band_quality_status
    assert row[2] is None  # price_as_of
    assert row[3] is None  # divergence_pct
    assert row[4] is None  # divergence_flag
