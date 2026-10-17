"""#3714: DB tests — the sole-writer trigger, cohort selection, and the per-row refill transaction."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from app.providers.implementations.sec_header import NEW_YORK, POLICY_VERSION, HeaderCheck, header_url
from app.services.sec_acceptance_refill import record_check, run_sec_acceptance_refill, select_ready
from app.services.sec_manifest import record_manifest_entry
from tests.fixtures.ebull_test_db import test_database_url

NOW = datetime(2026, 10, 17, 14, 0, tzinfo=UTC)
LEGACY = datetime(2026, 1, 29, 16, 30, 33, tzinfo=UTC)  # the ET wall time read as UTC: 5 h early
HEADER = datetime(2026, 1, 29, 21, 30, 33, tzinfo=UTC)
CIK = "0000320193"


def _acc(n: int) -> str:
    return f"0000320193-26-{n:06d}"


def _manifest(conn: psycopg.Connection[tuple], n: int, source: str, *, created_at: datetime | None = None) -> str:
    accession = _acc(n)
    conn.execute(
        """
        INSERT INTO sec_filing_manifest (
            accession_number, cik, form, source, subject_type, subject_id, filed_at, created_at
        ) VALUES (%s, %s, 'X', %s, 'blockholder_filer', %s, %s, COALESCE(%s, now()))
        """,
        (accession, CIK, source, CIK, LEGACY, created_at),
    )
    return accession


def _baseline(conn: psycopg.Connection[tuple], accession: str, source: str, value: datetime = LEGACY) -> None:
    """A pre-#3714 row: its value written under the GUC, as the migration's baseline copy found it."""
    with conn.transaction():
        conn.execute("SELECT set_config('ebull.acceptance_writer', 'header', true)")
        conn.execute("UPDATE sec_filing_manifest SET accepted_at = %s WHERE accession_number = %s", (value, accession))
        conn.execute("SELECT set_config('ebull.acceptance_writer', '', true)")
    conn.execute(
        "INSERT INTO sec_acceptance_legacy VALUES (%s, %s, %s, now() - interval '1 day')", (accession, value, source)
    )


def _accepted(conn: psycopg.Connection[tuple], accession: str) -> datetime | None:
    row = conn.execute(
        "SELECT accepted_at FROM sec_filing_manifest WHERE accession_number = %s", (accession,)
    ).fetchone()
    assert row is not None
    return row[0]


def _check(conn: psycopg.Connection[tuple], accession: str) -> tuple[object, ...] | None:
    return conn.execute(
        "SELECT outcome, attempts, next_retry_at, policy_version FROM sec_acceptance_checks"
        " WHERE accession_number = %s",
        (accession,),
    ).fetchone()


def _found(accession: str, utc: datetime = HEADER) -> HeaderCheck:
    raw = utc.astimezone(NEW_YORK).strftime("%Y%m%d%H%M%S")
    return HeaderCheck("found", header_url(CIK, accession), raw_value=raw, acceptance_utc=utc, http_status=200)


def _autocommit() -> psycopg.Connection[tuple]:
    return psycopg.connect(test_database_url(), autocommit=True)


# --- trigger -----------------------------------------------------------------------------------------------------


def test_trigger_holds_accepted_at_without_the_guc(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    conn.execute(
        """
        INSERT INTO sec_filing_manifest (
            accession_number, cik, form, source, subject_type, subject_id, filed_at, accepted_at
        ) VALUES (%s, %s, '8-K', 'sec_8k', 'blockholder_filer', %s, %s, %s)
        """,
        (_acc(1), CIK, CIK, LEGACY, LEGACY),
    )
    assert _accepted(conn, _acc(1)) is None  # INSERT: held at NULL
    _baseline(conn, _acc(1), "sec_8k")
    conn.execute("UPDATE sec_filing_manifest SET accepted_at = %s, form = '8-K/A' WHERE accession_number = %s",
                 (HEADER, _acc(1)))  # fmt: skip
    row = conn.execute("SELECT accepted_at, form FROM sec_filing_manifest WHERE accession_number = %s",
                       (_acc(1),)).fetchone()  # fmt: skip
    assert row == (LEGACY, "8-K/A")  # UPDATE: column held, the rest of the row lands
    conn.execute("SELECT set_config('ebull.acceptance_writer', 'header', true)")
    conn.execute("UPDATE sec_filing_manifest SET accepted_at = %s WHERE accession_number = %s", (HEADER, _acc(1)))
    assert _accepted(conn, _acc(1)) == HEADER


def test_discovery_upsert_no_longer_moves_accepted_at(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    accession = _manifest(conn, 2, "sec_8k")
    _baseline(conn, accession, "sec_8k")
    record_manifest_entry(
        conn, accession, cik=CIK, form="8-K", source="sec_8k", subject_type="blockholder_filer", subject_id=CIK,
        instrument_id=None, filed_at=NOW,
    )  # fmt: skip
    assert _accepted(conn, accession) == LEGACY


def test_guc_is_transaction_local(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    accession = _manifest(conn, 3, "sec_8k")
    conn.execute("SELECT set_config('ebull.acceptance_writer', 'header', true)")
    conn.commit()
    conn.execute("UPDATE sec_filing_manifest SET accepted_at = %s WHERE accession_number = %s", (HEADER, accession))
    assert _accepted(conn, accession) is None


# --- cohorts -----------------------------------------------------------------------------------------------------


def test_cohort_capacity_and_ready_predicate(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    epoch = conn.execute("SELECT epoch FROM sec_acceptance_epoch").fetchone()
    assert epoch is not None, "the migration seeds the epoch row"
    after = epoch[0] + timedelta(minutes=1)
    t1 = [_manifest(conn, 100 + i, "sec_form4", created_at=after) for i in range(6)]
    _manifest(conn, 120, "sec_13f_hr", created_at=after)  # new non-priority row: in no cohort
    _manifest(conn, 121, "sec_8k", created_at=epoch[0] - timedelta(days=1))  # old, never had a value: in no cohort
    t2 = [_manifest(conn, 200 + i, "sec_8k") for i in range(10)]
    t3 = [_manifest(conn, 300 + i, "sec_13f_hr") for i in range(10)]
    for accession in t2:
        _baseline(conn, accession, "sec_8k")
    for accession in t3:
        _baseline(conn, accession, "sec_13f_hr")

    picked = select_ready(conn, budget=10, now=NOW)
    # T1 <= 40 % (4 of 10); T3 >= 20 % (2); T2 takes the rest (4).
    assert [(row.cohort, row.accession_number) for row in picked] == (
        [("T1", a) for a in t1[:4]] + [("T2", a) for a in t2[:4]] + [("T3", a) for a in t3[:2]]
    )

    # Ready predicate: a final check is done; a retryable one waits for next_retry_at; an old-policy final is ready.
    conn.execute(
        """
        INSERT INTO sec_acceptance_checks VALUES
            (%(a)s, 'absent', NULL, NULL, 'u', 200, NULL, %(p)s, 1, %(now)s, NULL),
            (%(b)s, 'http_error', NULL, NULL, 'u', 503, NULL, %(p)s, 1, %(now)s, %(later)s),
            (%(c)s, 'http_error', NULL, NULL, 'u', 503, NULL, %(p)s, 1, %(now)s, %(now)s),
            (%(d)s, 'absent', NULL, NULL, 'u', 200, NULL, %(old)s, 1, %(now)s, NULL)
        """,
        {"a": t2[0], "b": t2[1], "c": t2[2], "d": t2[3], "p": POLICY_VERSION, "old": POLICY_VERSION - 1,
         "now": NOW, "later": NOW + timedelta(hours=1)},
    )  # fmt: skip
    ready_t2 = [row.accession_number for row in select_ready(conn, budget=100, now=NOW) if row.cohort == "T2"]
    assert ready_t2 == [t2[2], t2[3], *t2[4:]]

    # Unused T1 and T2 capacity passes on to T3.
    conn.execute("DELETE FROM sec_filing_manifest WHERE source IN ('sec_form4', 'sec_8k')")
    conn.execute("DELETE FROM sec_acceptance_legacy WHERE source = 'sec_8k'")
    assert [row.cohort for row in select_ready(conn, budget=10, now=NOW)] == ["T3"] * 10


def test_sparse_t3_hands_its_floor_to_t2(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    t2 = [_manifest(conn, 600 + i, "sec_form4") for i in range(12)]
    t3 = _manifest(conn, 650, "sec_13d")
    for accession in t2:
        _baseline(conn, accession, "sec_form4")
    _baseline(conn, t3, "sec_13d")
    # A CIK that cannot build a header URL is never selected (it would raise in ``header_url``).
    bad = _manifest(conn, 651, "sec_13d")
    conn.execute("UPDATE sec_filing_manifest SET cik = 'NOT-A-CIK' WHERE accession_number = %s", (bad,))
    _baseline(conn, bad, "sec_13d")
    picked = select_ready(conn, budget=10, now=NOW)
    assert [(row.cohort, row.accession_number) for row in picked] == [("T2", a) for a in t2[:9]] + [("T3", t3)]


# --- per-row transaction -----------------------------------------------------------------------------------------


@pytest.fixture
def committed() -> Iterator[psycopg.Connection[tuple]]:
    """Rows committed on an autocommit connection: ``record_check`` runs real transactions."""
    with _autocommit() as conn:
        yield conn


def test_found_writes_the_header_value_with_its_evidence(
    ebull_test_conn: psycopg.Connection[tuple], committed: psycopg.Connection[tuple]
) -> None:
    del ebull_test_conn  # wipes the test DB around this test
    accession = _manifest(committed, 400, "sec_8k")
    _baseline(committed, accession, "sec_8k")
    assert record_check(committed, accession, _found(accession), now=NOW) == "found"
    assert _accepted(committed, accession) == HEADER
    assert _check(committed, accession) == ("found", 1, None, POLICY_VERSION)
    assert record_check(committed, accession, _found(accession), now=NOW) == "already_final"


@pytest.mark.parametrize("outcome", ["absent", "invalid_value"])
def test_final_without_a_value_clears_the_legacy_value(
    ebull_test_conn: psycopg.Connection[tuple], committed: psycopg.Connection[tuple], outcome: str
) -> None:
    del ebull_test_conn
    accession = _manifest(committed, 410, "sec_8k")
    _baseline(committed, accession, "sec_8k")
    raw = "20261101013000" if outcome == "invalid_value" else None
    check = HeaderCheck(outcome, header_url(CIK, accession), raw_value=raw, http_status=200)  # type: ignore[arg-type]
    assert record_check(committed, accession, check, now=NOW) == outcome
    assert _accepted(committed, accession) is None
    assert _check(committed, accession) == (outcome, 1, None, POLICY_VERSION)


def test_retryable_keeps_the_legacy_value_and_backs_off_then_final_replaces_it(
    ebull_test_conn: psycopg.Connection[tuple], committed: psycopg.Connection[tuple]
) -> None:
    del ebull_test_conn
    accession = _manifest(committed, 420, "sec_8k")
    _baseline(committed, accession, "sec_8k")
    failed = HeaderCheck("http_error", header_url(CIK, accession), http_status=503)
    assert record_check(committed, accession, failed, now=NOW) == "http_error"
    assert record_check(committed, accession, failed, now=NOW) == "http_error"
    assert _accepted(committed, accession) == LEGACY
    assert _check(committed, accession) == ("http_error", 2, NOW + timedelta(hours=2), POLICY_VERSION)
    assert record_check(committed, accession, _found(accession), now=NOW) == "found"
    assert _accepted(committed, accession) == HEADER
    assert _check(committed, accession) == ("found", 3, None, POLICY_VERSION)


def test_retryable_never_overwrites_a_final_check(
    ebull_test_conn: psycopg.Connection[tuple], committed: psycopg.Connection[tuple]
) -> None:
    del ebull_test_conn
    accession = _manifest(committed, 430, "sec_8k")
    _baseline(committed, accession, "sec_8k")
    assert record_check(committed, accession, _found(accession), now=NOW) == "found"
    # An older policy makes the row ready again; a failed re-fetch keeps the final answer and its value.
    committed.execute("UPDATE sec_acceptance_checks SET policy_version = %s", (POLICY_VERSION - 1,))
    failed = HeaderCheck("transport_error", header_url(CIK, accession), diagnostic="ConnectError")
    assert record_check(committed, accession, failed, now=NOW) == "kept_final"
    assert _check(committed, accession) == ("found", 1, None, POLICY_VERSION - 1)
    assert _accepted(committed, accession) == HEADER
    # The upsert's WHERE enforces the same rule on its own.
    committed.execute(
        """
        INSERT INTO sec_acceptance_checks VALUES (%s, 'bad_response', NULL, NULL, 'u', 200, NULL, %s, 1, now(), now())
        ON CONFLICT (accession_number) DO UPDATE SET outcome = EXCLUDED.outcome, next_retry_at = EXCLUDED.next_retry_at
        WHERE sec_acceptance_checks.outcome NOT IN ('found', 'absent', 'invalid_value')
        """,
        (accession, POLICY_VERSION),
    )
    assert _check(committed, accession) == ("found", 1, None, POLICY_VERSION - 1)


def test_run_requires_autocommit_and_fetches_each_ready_row(
    ebull_test_conn: psycopg.Connection[tuple], committed: psycopg.Connection[tuple]
) -> None:
    with pytest.raises(ValueError, match="autocommit"):
        run_sec_acceptance_refill(ebull_test_conn, _never_called)
    ebull_test_conn.rollback()
    rows = [_manifest(committed, 500 + i, "sec_8k") for i in range(3)]
    for accession in rows:
        _baseline(committed, accession, "sec_8k")
    fetched: list[tuple[str, str]] = []

    def fetch(cik: str, accession: str) -> HeaderCheck:
        fetched.append((cik, accession))
        return _found(accession)

    report = run_sec_acceptance_refill(committed, fetch, clock=lambda: NOW)
    assert fetched == [(CIK, a) for a in rows]
    assert (report.selected, report.final, report.attempted) == ({"T2": 3}, 3, 3)
    assert [_accepted(committed, a) for a in rows] == [HEADER] * 3


def _never_called(cik: str, accession: str) -> HeaderCheck:
    raise AssertionError("no fetch expected")
