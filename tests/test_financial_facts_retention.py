"""Retention sweep correctness tests for `financial_facts_raw` (#1208 Phase 3).

Tests target `app.services.financial_facts_retention`:

- per-form-family horizon enforcement (10-K = 3, 10-Q = 8; amendments share)
- DISTINCT-accession ranking (NOT per-fact — Codex 1a BLOCKING #3 regression)
- non-swept form_types untouched (8-K, DEF 14A, ...)
- service-no-commit invariant
- idempotency
"""

from __future__ import annotations

from datetime import date, timedelta

import psycopg

from app.services.financial_facts_retention import (
    KEEP_10K,
    KEEP_10Q,
    sweep_retention_all_instruments,
    sweep_retention_for_instrument,
)
from tests.fixtures.ebull_test_db import test_database_url


def _seed_instrument(conn: psycopg.Connection[tuple], *, instrument_id: int) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
        (instrument_id, f"T{instrument_id}", f"Test {instrument_id}"),
    )
    conn.commit()


def _seed_accession(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession_number: str,
    form_type: str,
    filed_date: date,
    n_facts: int,
) -> None:
    """Insert n_facts rows for one accession spanning n_facts distinct
    period_end values (one per quarter back from filed_date)."""
    with conn.cursor() as cur:
        for i in range(n_facts):
            cur.execute(
                """
                INSERT INTO financial_facts_raw (
                    instrument_id, taxonomy, concept, unit, period_end,
                    val, accession_number, form_type, filed_date
                ) VALUES (
                    %s, 'us-gaap', %s, 'USD', %s, %s, %s, %s, %s
                )
                """,
                (
                    instrument_id,
                    f"Concept_{i}",  # distinct concept per fact so identity is unique
                    filed_date - timedelta(days=90),
                    i + 1,
                    accession_number,
                    form_type,
                    filed_date,
                ),
            )
    conn.commit()


def _count_facts(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    form_type: str | None = None,
) -> int:
    with conn.cursor() as cur:
        if form_type is None:
            cur.execute(
                "SELECT count(*) FROM financial_facts_raw WHERE instrument_id = %s",
                (instrument_id,),
            )
        else:
            cur.execute(
                "SELECT count(*) FROM financial_facts_raw WHERE instrument_id = %s AND form_type = %s",
                (instrument_id, form_type),
            )
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def test_keeps_latest_3_10k_with_12_facts_each(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Codex 1a BLOCKING #3 regression: seed 5 distinct 10-K accessions,
    each with 12 facts → sweep MUST keep 3 accessions × 12 facts = 36
    facts (NOT 3 individual facts)."""
    iid = 20001
    _seed_instrument(ebull_test_conn, instrument_id=iid)
    for i in range(5):
        _seed_accession(
            ebull_test_conn,
            instrument_id=iid,
            accession_number=f"10K-{i:02d}",
            form_type="10-K",
            filed_date=date(2025, 1, 1) - timedelta(days=365 * i),
            n_facts=12,
        )
    assert _count_facts(ebull_test_conn, instrument_id=iid) == 60

    deleted = sweep_retention_for_instrument(ebull_test_conn, instrument_id=iid)
    ebull_test_conn.commit()
    assert deleted == 24, f"expected 24 deleted (2 oldest × 12 facts), got {deleted}"
    assert _count_facts(ebull_test_conn, instrument_id=iid) == 36


def test_keeps_latest_8_10q(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    iid = 20002
    _seed_instrument(ebull_test_conn, instrument_id=iid)
    for i in range(10):
        _seed_accession(
            ebull_test_conn,
            instrument_id=iid,
            accession_number=f"10Q-{i:02d}",
            form_type="10-Q",
            filed_date=date(2025, 1, 1) - timedelta(days=90 * i),
            n_facts=8,
        )
    assert _count_facts(ebull_test_conn, instrument_id=iid) == 80

    deleted = sweep_retention_for_instrument(ebull_test_conn, instrument_id=iid)
    ebull_test_conn.commit()
    assert deleted == 16, f"expected 16 deleted (2 oldest × 8 facts), got {deleted}"
    assert _count_facts(ebull_test_conn, instrument_id=iid) == 64


def test_10ka_amendments_share_annual_family_budget(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Codex 1a BLOCKING #4 regression: 2 × 10-K + 2 × 10-K/A = 4
    accessions in ANNUAL family → keep 3, evict 1."""
    iid = 20003
    _seed_instrument(ebull_test_conn, instrument_id=iid)
    accessions = [
        ("10K-oldest", "10-K", date(2022, 3, 1)),
        ("10KA-amend1", "10-K/A", date(2023, 3, 1)),
        ("10K-recent", "10-K", date(2024, 3, 1)),
        ("10KA-amend2", "10-K/A", date(2025, 3, 1)),
    ]
    for acc, ft, fd in accessions:
        _seed_accession(
            ebull_test_conn,
            instrument_id=iid,
            accession_number=acc,
            form_type=ft,
            filed_date=fd,
            n_facts=5,
        )
    assert _count_facts(ebull_test_conn, instrument_id=iid) == 20

    deleted = sweep_retention_for_instrument(ebull_test_conn, instrument_id=iid)
    ebull_test_conn.commit()
    assert deleted == 5, f"expected 5 deleted (oldest 10-K × 5 facts), got {deleted}"
    # Verify the kept accessions are the 3 most-recent
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT accession_number FROM financial_facts_raw "
            " WHERE instrument_id = %s ORDER BY accession_number",
            (iid,),
        )
        kept = sorted(r[0] for r in cur.fetchall())
    assert kept == ["10K-recent", "10KA-amend1", "10KA-amend2"]


def test_10qa_amendments_share_quarterly_family_budget(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    iid = 20004
    _seed_instrument(ebull_test_conn, instrument_id=iid)
    for i in range(5):
        _seed_accession(
            ebull_test_conn,
            instrument_id=iid,
            accession_number=f"10Q-{i:02d}",
            form_type="10-Q",
            filed_date=date(2025, 1, 1) - timedelta(days=90 * (i * 2)),
            n_facts=3,
        )
    for i in range(5):
        _seed_accession(
            ebull_test_conn,
            instrument_id=iid,
            accession_number=f"10QA-{i:02d}",
            form_type="10-Q/A",
            filed_date=date(2025, 1, 15) - timedelta(days=90 * (i * 2)),
            n_facts=3,
        )
    assert _count_facts(ebull_test_conn, instrument_id=iid) == 30

    deleted = sweep_retention_for_instrument(ebull_test_conn, instrument_id=iid)
    ebull_test_conn.commit()
    # 10 distinct accessions in QUARTERLY family → keep 8, evict 2
    assert deleted == 6, f"expected 6 deleted (2 × 3 facts), got {deleted}"


def test_non_swept_form_types_untouched(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """8-K, DEF 14A, 13F-HR rows are NOT swept."""
    iid = 20005
    _seed_instrument(ebull_test_conn, instrument_id=iid)
    for ft in ("8-K", "DEF 14A", "13F-HR"):
        _seed_accession(
            ebull_test_conn,
            instrument_id=iid,
            accession_number=f"acc-{ft}",
            form_type=ft,
            filed_date=date(2020, 1, 1),  # ancient
            n_facts=2,
        )
    assert _count_facts(ebull_test_conn, instrument_id=iid) == 6
    deleted = sweep_retention_for_instrument(ebull_test_conn, instrument_id=iid)
    ebull_test_conn.commit()
    assert deleted == 0
    assert _count_facts(ebull_test_conn, instrument_id=iid) == 6


def test_idempotent_second_run_deletes_zero(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    iid = 20006
    _seed_instrument(ebull_test_conn, instrument_id=iid)
    for i in range(5):
        _seed_accession(
            ebull_test_conn,
            instrument_id=iid,
            accession_number=f"10K-{i:02d}",
            form_type="10-K",
            filed_date=date(2025, 1, 1) - timedelta(days=365 * i),
            n_facts=4,
        )
    sweep_retention_for_instrument(ebull_test_conn, instrument_id=iid)
    ebull_test_conn.commit()
    second_deleted = sweep_retention_for_instrument(ebull_test_conn, instrument_id=iid)
    ebull_test_conn.commit()
    assert second_deleted == 0


def test_service_does_not_commit_or_open_own_transaction(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Service-no-commit invariant: `sweep_retention_for_instrument`
    must NOT call `conn.commit()` or enter `with conn.transaction()`.

    Wrap the conn with a recording proxy: any commit() or transaction()
    call inside the service body fails the test.
    """
    iid = 20007
    _seed_instrument(ebull_test_conn, instrument_id=iid)
    for i in range(4):
        _seed_accession(
            ebull_test_conn,
            instrument_id=iid,
            accession_number=f"10K-{i:02d}",
            form_type="10-K",
            filed_date=date(2025, 1, 1) - timedelta(days=365 * i),
            n_facts=2,
        )

    forbidden_calls: list[str] = []

    class _RecordingConn:
        def __init__(self, inner: psycopg.Connection[tuple]) -> None:
            self._inner = inner

        def commit(self) -> None:
            forbidden_calls.append("commit")
            self._inner.commit()

        def transaction(self, *args: object, **kwargs: object) -> object:
            forbidden_calls.append("transaction")
            return self._inner.transaction(*args, **kwargs)  # type: ignore[arg-type]

        def __getattr__(self, name: str) -> object:
            return getattr(self._inner, name)

    recording = _RecordingConn(ebull_test_conn)
    sweep_retention_for_instrument(recording, instrument_id=iid)  # type: ignore[arg-type]
    ebull_test_conn.commit()
    assert forbidden_calls == [], f"service violated no-commit/no-transaction contract: {forbidden_calls}"


def test_horizon_constants_match_skill_section_13(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Sanity guard against drift: the module-level constants are the
    skill §13 horizons. If anyone bumps the constants without updating
    the skill, this test fails."""
    assert KEEP_10K == 3
    assert KEEP_10Q == 8


def test_orchestrator_commits_per_instrument_autocommit_contract(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Codex 2 WARNING #4 regression: `sweep_retention_all_instruments`
    must open an autocommit conn AND commit per instrument.

    Strategy: seed two instruments with eviction-eligible accessions,
    fire the orchestrator with the test DB URL, then verify from a
    SEPARATE connection that the deletions are visible — proving the
    per-instrument transaction committed (a SAVEPOINT-only path inside
    one outer tx would leave the changes invisible to a fresh conn
    until the outer commit, which never happens because the orchestrator
    closes the conn on exit).
    """
    for iid in (50001, 50002):
        _seed_instrument(ebull_test_conn, instrument_id=iid)
        for i in range(5):
            _seed_accession(
                ebull_test_conn,
                instrument_id=iid,
                accession_number=f"10K-{iid}-{i:02d}",
                form_type="10-K",
                filed_date=date(2025, 1, 1) - timedelta(days=365 * i),
                n_facts=4,
            )
    # The orchestrator opens its OWN conn; the seed-side conn just
    # needs to commit so the rows are visible to the orchestrator.
    # Do NOT close — the fixture's teardown needs the conn alive for
    # the planner-table truncate.
    ebull_test_conn.commit()

    summary = sweep_retention_all_instruments(database_url=test_database_url())
    assert summary.instruments >= 2
    assert summary.rows_deleted == 16  # 2 instruments × 2 oldest × 4 facts

    # Verify from a fresh conn — if deletions weren't committed
    # per-instrument, the rows would still be present.
    import psycopg as _psycopg

    with _psycopg.connect(test_database_url()) as verify:
        with verify.cursor() as cur:
            cur.execute(
                "SELECT instrument_id, count(*) FROM financial_facts_raw "
                " WHERE instrument_id IN (50001, 50002) "
                " GROUP BY 1 ORDER BY 1"
            )
            counts = dict(cur.fetchall())
    assert counts == {50001: 12, 50002: 12}, f"expected each instrument at 3 accessions × 4 facts = 12, got {counts}"
