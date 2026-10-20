"""#2327 — the 13D/G CUSIP bridge's writer and window lists against ``ebull_test`` (spec "Construction" step 5).

Covers what the pure tests cannot: the insert and its provenance row, idempotence, the re-read under the CUSIP lock
(a stale snapshot is caught and counted as a race), the invalidated-row report, and the window-list classification
(duplicate payload, short list, unpublished quarter).
"""

from __future__ import annotations

import urllib.error
from datetime import date
from email.message import Message
from typing import Any

import psycopg
import pytest

from app.services import cusip_bridge_13dg
from app.services.cusip_bridge_13dg import CUSIP_BRIDGE_RULE_VERSION, BridgeInputs, bind_13dg_cusips
from app.services.cusip_check_digit import cusip_check_digit
from app.services.sec_13f_securities_list import WINDOW_LIST_MIN_ROWS, WindowList, ensure_window_lists
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration

IID_A, IID_B, IID_C = 232_701, 232_702, 232_703
CIK_A, CIK_B = "0002327001", "0002327002"


def _c(first_eight: str) -> str:
    return first_eight + str(cusip_check_digit(first_eight))


X_BIND = _c("23270110")  # instrument A, no row → bind
X_CONFIRM = _c("23270210")  # instrument B, openfigi row → confirm
LISTS = [
    WindowList("2026q2", "sha-q2", frozenset({X_BIND, X_CONFIRM}), None),
    WindowList("2026q3", "sha-q3", frozenset({X_BIND, X_CONFIRM}), None),
]


def _seed(conn: psycopg.Connection[tuple]) -> None:
    for iid, sym in ((IID_A, "BRA"), (IID_B, "BRB"), (IID_C, "BRC")):
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable) "
            "VALUES (%s, %s, %s, '4', 'USD', TRUE)",
            (iid, sym, f"{sym} Inc"),
        )
    for iid, cik in ((IID_A, CIK_A), (IID_B, CIK_B)):
        conn.execute(
            "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (%s, 'sec', 'cik', %s, TRUE)",
            (iid, cik),
        )
    conn.execute(
        "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary) "
        "VALUES (%s, 'openfigi', 'cusip', %s, FALSE)",
        (IID_B, X_CONFIRM),
    )
    conn.execute("INSERT INTO blockholder_filers (cik, name) VALUES ('0002327999', 'F')")
    filer_id = conn.execute("SELECT filer_id FROM blockholder_filers WHERE cik = '0002327999'").fetchone()
    assert filer_id is not None
    # CIK_A written unpadded and the CUSIP lower-case with a newline: the reader normalises both.
    for accession, cik, cusip in (
        ("a-1", CIK_A.lstrip("0"), X_BIND.lower() + "\n"),
        ("a-2", CIK_A, X_BIND),
        ("b-1", CIK_B, X_CONFIRM),
    ):
        conn.execute(
            """
            INSERT INTO blockholder_filings (
                filer_id, accession_number, submission_type, status, instrument_id, issuer_cik, issuer_cusip,
                reporter_no_cik, reporter_name, aggregate_amount_owned, percent_of_class
            ) VALUES (%s, %s, 'SCHEDULE 13G', 'passive', NULL, %s, %s, FALSE, 'R', 1000, 5.0)
            """,
            (filer_id[0], accession, cik, cusip),
        )
    conn.commit()


def _cusip_rows(conn: psycopg.Connection[tuple]) -> set[tuple[str, str, int]]:
    rows = conn.execute(
        "SELECT provider, identifier_value, instrument_id FROM external_identifiers WHERE identifier_type = 'cusip'"
    ).fetchall()
    conn.commit()
    return {(str(p), str(v), int(i)) for p, v, i in rows}


class TestBindWriter:
    def test_inserts_bind_and_confirm_with_provenance_and_is_idempotent(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed(conn)

        result = bind_13dg_cusips(conn, lists=LISTS, job_run_id=42)

        assert dict(result.inserted) == {"bind": 1, "confirm": 1}
        assert not result.raced and not result.invalidated
        assert _cusip_rows(conn) == {
            ("sec", X_BIND, IID_A),
            ("sec", X_CONFIRM, IID_B),
            ("openfigi", X_CONFIRM, IID_B),
        }
        prov = conn.execute(
            """
            SELECT b.cusip, b.instrument_id, b.issuer_cik, b.outcome, b.rule_version, b.accessions,
                   b.list_quarters, b.list_sha256, b.job_run_id, ei.provider
              FROM cusip_bridge_bindings b
              JOIN external_identifiers ei ON ei.external_identifier_id = b.external_identifier_id
             ORDER BY b.cusip
            """
        ).fetchall()
        conn.commit()
        assert prov == [
            (X_BIND, IID_A, CIK_A, "bind", CUSIP_BRIDGE_RULE_VERSION, ["a-1", "a-2"], ["2026q2", "2026q3"],
             ["sha-q2", "sha-q3"], 42, "sec"),
            (X_CONFIRM, IID_B, CIK_B, "confirm", CUSIP_BRIDGE_RULE_VERSION, ["b-1"], ["2026q2", "2026q3"],
             ["sha-q2", "sha-q3"], 42, "sec"),
        ]  # fmt: skip

        again = bind_13dg_cusips(conn, lists=LISTS, job_run_id=43)
        assert (sum(again.inserted.values()), again.held, again.outcomes["bind"]) == (0, 2, 1)
        assert conn.execute("SELECT count(*) FROM cusip_bridge_bindings").fetchone() == (2,)
        conn.commit()

    def test_stale_snapshot_is_caught_by_the_reread_under_the_lock(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        conn = ebull_test_conn
        _seed(conn)
        # After the snapshot: X_BIND gains an openfigi row on another instrument (R6), and CIK_B a second
        # instrument (R3). The writer must refuse both and count them.
        real_read = cusip_bridge_13dg.read_bridge_inputs

        def stale_read(c: psycopg.Connection[Any], lists: Any) -> tuple[BridgeInputs, Any, Any]:
            snapshot = real_read(c, lists)
            c.execute(
                "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, "
                "is_primary) VALUES (%s, 'openfigi', 'cusip', %s, FALSE), (%s, 'sec', 'cik', %s, FALSE)",
                (IID_C, X_BIND, IID_C, CIK_B),
            )
            return snapshot

        monkeypatch.setattr(cusip_bridge_13dg, "read_bridge_inputs", stale_read)

        result = bind_13dg_cusips(conn, lists=LISTS, job_run_id=1)

        assert dict(result.raced) == {"R6": 1, "R3": 1}
        assert sum(result.inserted.values()) == 0
        assert ("sec", X_BIND, IID_A) not in _cusip_rows(conn)
        assert conn.execute("SELECT count(*) FROM cusip_bridge_bindings").fetchone() == (0,)
        conn.commit()

    def test_bridge_row_that_would_now_refuse_is_reported_not_deleted(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed(conn)
        bind_13dg_cusips(conn, lists=LISTS, job_run_id=1)
        conn.execute(
            "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (%s, 'openfigi', 'cusip', %s, FALSE)",
            (IID_C, X_BIND),
        )
        conn.commit()

        result = bind_13dg_cusips(conn, lists=LISTS, job_run_id=2)

        assert result.invalidated == [(X_BIND, IID_A, "R6")]
        assert result.held == 1
        assert ("sec", X_BIND, IID_A) in _cusip_rows(conn)

    def test_latest_gap_refuses_without_writing(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed(conn)
        lists = [LISTS[0], WindowList("2026q3", "sha", frozenset(), "duplicate_payload")]

        result = bind_13dg_cusips(conn, lists=lists, job_run_id=1)

        assert result.refused is not None and "2026q3" in result.refused
        assert ("sec", X_BIND, IID_A) not in _cusip_rows(conn)


def _list_payload(cusips: list[str], extra: str = "") -> str:
    lines = [f"{c} {'CO ' + c:<30}{'COM':<28}{'':<10}E" for c in cusips]
    return "\n".join(lines) + "\n" + extra


class TestEnsureWindowLists:
    def test_classifies_duplicate_short_and_unpublished_quarters(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        base = [_c(f"{n:08d}") for n in range(WINDOW_LIST_MIN_ROWS)]
        deleted, call = _c("99999910"), _c("99999920")
        extra = f"{deleted} {'GONE CO':<30}{'COM':<28}{'*D*':<10}\n{call} {'OPT CO':<30}{'CALL':<28}{'':<10}E\n"
        payloads = {
            (2024, 4): _list_payload(base, extra),
            (2025, 2): _list_payload(base[:100]),  # too few rows
            (2025, 3): _list_payload(base[1:]) + _list_payload([X_BIND]),
            (2025, 4): _list_payload(base[1:]) + _list_payload([X_BIND]),  # byte-identical to 2025q3
            (2026, 1): _list_payload(base[2:] + [X_BIND, X_CONFIRM]),
        }

        def fetch(year: int, quarter: int) -> tuple[str, str]:
            if (year, quarter) not in payloads:
                raise urllib.error.HTTPError("u", 404, "Not Found", Message(), None)
            return payloads[(year, quarter)], f"test://{year}q{quarter}"

        lists = ensure_window_lists(conn, fetch=fetch, today=date(2026, 4, 20))
        conn.commit()

        assert [(wl.quarter, wl.gap_reason) for wl in lists] == [
            ("2024q4", None),
            ("2025q1", "not_published"),
            ("2025q2", "too_few_rows"),
            ("2025q3", "duplicate_payload"),
            ("2025q4", "duplicate_payload"),
            ("2026q1", None),
        ]
        assert lists[0].share_cusips == frozenset(base)  # the *D* row and the CALL class are not share rows
        assert X_CONFIRM in lists[-1].share_cusips
        assert lists[3].sha256 == lists[4].sha256 is not None
        stored = conn.execute(
            "SELECT count(*) FROM sec_reference_documents WHERE document_kind = '13f_securities_list'"
        ).fetchone()
        conn.commit()
        assert stored == (5,)  # raw bodies stored before parse, including the gaps
