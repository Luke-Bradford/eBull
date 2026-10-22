"""#3227 tier (IM) capture — DB tests (spec docs/specs/ownership/2026-10-22-3227-13f-included-managers.md § Change 1-3).

The bulk path aggregates segments in SQL and the per-filing paths in Python; these tests pin that both produce the same
payload, the upsert rule for writers that cannot capture (risk 18), replay orders, capture coherence and the
capture-only backfill.
"""

from __future__ import annotations

import csv
import io
import zipfile
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
import psycopg.rows
import pytest

from app.providers.implementations.sec_13f import ThirteenFHolding, ThirteenFOtherManager
from app.services.institutional_holdings import _record_13f_observations_for_filing
from app.services.ownership_observations import record_institution_observation
from app.services.sec_13f_dataset_ingest import ingest_13f_dataset_archive
from app.services.thirteen_f_capture import new_capture, recapture_from_stored, segments_payload, write_captures
from app.services.thirteen_f_normalise import merge_resolved_by_instrument, normalise_13f_holdings
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")

_ACC = "0000200406-26-000435"
_CIK = "0000200406"
_PERIOD = "2026-06-30"
_FILED = datetime(2026, 8, 14, tzinfo=UTC)


def _tsv(rows: list[dict[str, str]], fields: list[str]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, delimiter="\t")
    writer.writeheader()
    for row in rows:
        writer.writerow({f: row.get(f, "") for f in fields})
    return buf.getvalue()


_INFOTABLE_FIELDS = [
    "ACCESSION_NUMBER",
    "CUSIP",
    "VALUE",
    "SSHPRNAMT",
    "SSHPRNAMTTYPE",
    "PUTCALL",
    "INVESTMENTDISCRETION",
    "OTHERMANAGER",
    "VOTING_AUTH_SOLE",
    "VOTING_AUTH_SHARED",
    "VOTING_AUTH_NONE",
]


def _line(cusip: str, shares: int, discretion: str, column7: str, *, kind: str = "SH") -> dict[str, str]:
    return {
        "ACCESSION_NUMBER": _ACC,
        "CUSIP": cusip,
        "VALUE": str(shares * 10),
        "SSHPRNAMT": str(shares),
        "SSHPRNAMTTYPE": kind,
        "INVESTMENTDISCRETION": discretion,
        "OTHERMANAGER": column7,
        "VOTING_AUTH_SHARED": str(shares),
    }


def _archive(
    tmp_path: Path,
    lines: list[dict[str, str]],
    *,
    managers: list[tuple[str, str]] | None,
    entry_total: int | None = None,
    name: str = "a.zip",
    period: str = _PERIOD,
) -> Path:
    """A one-accession data set. ``managers`` None omits OTHERMANAGER2.tsv and SUMMARYPAGE.tsv."""
    out = tmp_path / name
    with zipfile.ZipFile(out, "w") as zf:
        zf.writestr(
            "SUBMISSION.tsv",
            _tsv(
                [{"ACCESSION_NUMBER": _ACC, "CIK": "200406", "FILING_DATE": "14-AUG-2026"}],
                ["ACCESSION_NUMBER", "CIK", "FILING_DATE"],
            ),
        )
        zf.writestr(
            "COVERPAGE.tsv",
            _tsv(
                [
                    {
                        "ACCESSION_NUMBER": _ACC,
                        "FILINGMANAGER_NAME": "JOHNSON & JOHNSON",
                        "REPORTCALENDARORQUARTER": period,
                    }
                ],
                ["ACCESSION_NUMBER", "FILINGMANAGER_NAME", "REPORTCALENDARORQUARTER"],
            ),
        )
        zf.writestr("INFOTABLE.tsv", _tsv(lines, _INFOTABLE_FIELDS))
        if managers is not None:
            zf.writestr(
                "SUMMARYPAGE.tsv",
                _tsv(
                    [
                        {
                            "ACCESSION_NUMBER": _ACC,
                            "OTHERINCLUDEDMANAGERSCOUNT": str(len(managers)),
                            "TABLEENTRYTOTAL": str(len(lines) if entry_total is None else entry_total),
                            "TABLEVALUETOTAL": str(sum(int(r["VALUE"]) for r in lines)),
                        }
                    ],
                    ["ACCESSION_NUMBER", "OTHERINCLUDEDMANAGERSCOUNT", "TABLEENTRYTOTAL", "TABLEVALUETOTAL"],
                ),
            )
            zf.writestr(
                "OTHERMANAGER2.tsv",
                _tsv(
                    [
                        {"ACCESSION_NUMBER": _ACC, "SEQUENCENUMBER": seq, "CIK": cik, "NAME": f"M{seq}"}
                        for seq, cik in managers
                    ],
                    [
                        "ACCESSION_NUMBER",
                        "SEQUENCENUMBER",
                        "CIK",
                        "FORM13FFILENUMBER",
                        "CRDNUMBER",
                        "SECFILENUMBER",
                        "NAME",
                    ],
                ),
            )
    return out


_NEXT_IID = [913000]


def _instrument(conn: psycopg.Connection[tuple], *cusips: str) -> int:
    _NEXT_IID[0] += 1
    iid = _NEXT_IID[0]
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
        "VALUES (%s, %s, 'X', 'USD', TRUE)",
        (iid, f"T{iid}"),
    )
    for cusip in cusips:
        conn.execute(
            "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (%s, 'sec', 'cusip', %s, FALSE)",
            (iid, cusip),
        )
    conn.commit()
    return iid


def _observation(conn: psycopg.Connection[tuple], iid: int) -> dict[str, Any]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT shares, thirteen_f_segments, ingest_run_id, ingested_at FROM ownership_institutions_observations "
            "WHERE instrument_id = %s AND source = '13f'",
            (iid,),
        )
        rows = cur.fetchall()
    assert len(rows) == 1
    return rows[0]


def _capture_record(conn: psycopg.Connection[tuple]) -> tuple[Any, ...] | None:
    return conn.execute(
        "SELECT capture_id::text, source, status, list_count, expected_list_count, line_count, expected_line_count "
        "FROM institutional_13f_captures WHERE accession_number = %s",
        (_ACC,),
    ).fetchone()


def _python_segments(capture_id: str, lines: list[dict[str, str]]) -> dict[str, Any]:
    """The per-filing path's payload for the same lines, through the shared normaliser."""
    holdings = [
        ThirteenFHolding(
            cusip=r["CUSIP"],
            name_of_issuer="X",
            title_of_class="COM",
            value_usd=Decimal(r["VALUE"]),
            shares_or_principal=Decimal(r["SSHPRNAMT"]),
            shares_or_principal_type=r.get("SSHPRNAMTTYPE", "SH"),
            put_call=None,
            investment_discretion=r["INVESTMENTDISCRETION"],
            voting_sole=Decimal(0),
            voting_shared=Decimal(0),
            voting_none=Decimal(0),
            other_managers_raw=r["OTHERMANAGER"] or None,
        )
        for r in lines
    ]
    merged = merge_resolved_by_instrument([(1, h) for h in normalise_13f_holdings(holdings, filed_at=_FILED)])
    assert len(merged) == 1
    holding = merged[0][1]
    return segments_payload(capture_id, holding.cusips, holding.segments)  # type: ignore[arg-type]


_MIXED_LINES = [
    _line("478160104", 1_000, "DFND", "1"),
    _line("478160104", 500, "DFND", "2, 1"),
    _line("478160104", 70, "SOLE", "0"),
    _line("47816A100", 30, "OTR", "1 2"),  # a second CUSIP of the same instrument
    _line("478160104", 9_999, "DFND", "1", kind="PRN"),  # bond principal: not a segment
]


def test_bulk_capture_matches_python_segments(ebull_test_conn: psycopg.Connection[tuple], tmp_path: Path) -> None:
    conn = ebull_test_conn
    iid = _instrument(conn, "478160104", "47816A100")
    archive = _archive(tmp_path, _MIXED_LINES, managers=[("1", "0000924020"), ("2", "")])
    result = ingest_13f_dataset_archive(conn=conn, archive_path=archive)
    conn.commit()

    assert result.captures_by_status == {"complete": 1}
    record = _capture_record(conn)
    assert record is not None
    assert record[1:] == ("dataset", "complete", 2, 2, 5, 5)
    obs = _observation(conn, iid)
    assert obs["shares"] == Decimal(1_600)
    assert obs["thirteen_f_segments"] == _python_segments(record[0], _MIXED_LINES)
    assert obs["thirteen_f_segments"]["lines"] == [
        {"discretion": "DFND", "other_managers": [1], "shares": "1000"},
        {"discretion": "DFND", "other_managers": [1, 2], "shares": "500"},
        {"discretion": "OTR", "other_managers": [1, 2], "shares": "30"},
        {"discretion": "SOLE", "other_managers": None, "shares": "70"},
    ]
    entries = conn.execute(
        "SELECT entry_index, sequence_number_raw, sequence_number, cik FROM institutional_13f_other_managers "
        "WHERE accession_number = %s ORDER BY entry_index",
        (_ACC,),
    ).fetchall()
    assert entries == [(0, "1", 1, "0000924020"), (1, "2", 2, None)]


def test_invalid_list_is_stored_as_filed(ebull_test_conn: psycopg.Connection[tuple], tmp_path: Path) -> None:
    conn = ebull_test_conn
    _instrument(conn, "478160104")
    archive = _archive(tmp_path, [_line("478160104", 10, "DFND", "1")], managers=[("1", "1"), ("01", "2")])
    ingest_13f_dataset_archive(conn=conn, archive_path=archive)
    conn.commit()
    record = _capture_record(conn)
    assert record is not None and record[2] == "invalid"
    raw = conn.execute(
        "SELECT sequence_number_raw, sequence_number FROM institutional_13f_other_managers ORDER BY entry_index"
    ).fetchall()
    assert raw == [("1", 1), ("01", 1)]


def test_line_count_mismatch_is_incomplete(ebull_test_conn: psycopg.Connection[tuple], tmp_path: Path) -> None:
    conn = ebull_test_conn
    _instrument(conn, "478160104")
    archive = _archive(tmp_path, [_line("478160104", 10, "DFND", "1")], managers=[("1", "1")], entry_total=2)
    ingest_13f_dataset_archive(conn=conn, archive_path=archive)
    conn.commit()
    record = _capture_record(conn)
    assert record is not None and record[2] == "incomplete"


def test_uncaptured_bulk_keeps_segments_only_on_equal_shares_and_cusips(
    ebull_test_conn: psycopg.Connection[tuple], tmp_path: Path
) -> None:
    conn = ebull_test_conn
    iid = _instrument(conn, "478160104", "47816A100")
    lines = [_line("478160104", 100, "DFND", "1")]
    ingest_13f_dataset_archive(conn=conn, archive_path=_archive(tmp_path, lines, managers=[("1", "1")], name="c.zip"))
    conn.commit()
    captured = _observation(conn, iid)["thirteen_f_segments"]
    assert captured is not None

    # Same shares, same CUSIP set, no OTHERMANAGER2.tsv: kept (risk 18, declared).
    ingest_13f_dataset_archive(conn=conn, archive_path=_archive(tmp_path, lines, managers=None, name="u1.zip"))
    conn.commit()
    assert _observation(conn, iid)["thirteen_f_segments"] == captured

    # Same total, different CUSIP set: cleared.
    moved = [_line("47816A100", 100, "DFND", "1")]
    ingest_13f_dataset_archive(conn=conn, archive_path=_archive(tmp_path, moved, managers=None, name="u2.zip"))
    conn.commit()
    assert _observation(conn, iid)["thirteen_f_segments"] is None

    # Recapture, then a different total without a list: cleared.
    ingest_13f_dataset_archive(conn=conn, archive_path=_archive(tmp_path, lines, managers=[("1", "1")], name="c2.zip"))
    conn.commit()
    assert _observation(conn, iid)["thirteen_f_segments"] is not None
    bigger = [_line("478160104", 101, "DFND", "1")]
    ingest_13f_dataset_archive(conn=conn, archive_path=_archive(tmp_path, bigger, managers=None, name="u3.zip"))
    conn.commit()
    assert _observation(conn, iid)["thirteen_f_segments"] is None


def _filer(conn: psycopg.Connection[tuple]) -> int:
    row = conn.execute(
        "INSERT INTO institutional_filers (cik, name, filer_type) VALUES (%s, 'JOHNSON & JOHNSON', 'INV') "
        "RETURNING filer_id",
        (_CIK,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _per_filing_write(conn: psycopg.Connection[tuple], filer_id: int, resolved: list[tuple[int, ThirteenFHolding]]):
    capture = new_capture(
        accession_number=_ACC,
        source="xml",
        entries=[ThirteenFOtherManager("1", "0000924020", "JJDC", None)],
        expected_list_count=1,
        line_count=len(resolved),
        expected_line_count=len(resolved),
        value_total=Decimal(0),
        expected_value_total=Decimal(0),
    )
    write_captures(conn, [capture])
    _record_13f_observations_for_filing(
        conn,
        filer_id=filer_id,
        accession_number=_ACC,
        period_of_report=date(2026, 6, 30),
        filed_at=_FILED,
        resolved_holdings=resolved,
        capture=capture,
    )
    conn.commit()
    return capture


def _normalised(cusip: str, shares: int, column7: str = "1") -> ThirteenFHolding:
    holding = ThirteenFHolding(
        cusip=cusip,
        name_of_issuer="X",
        title_of_class="COM",
        value_usd=Decimal(0),
        shares_or_principal=Decimal(shares),
        shares_or_principal_type="SH",
        put_call=None,
        investment_discretion="DFND",
        voting_sole=Decimal(0),
        voting_shared=Decimal(0),
        voting_none=Decimal(0),
        other_managers_raw=column7,
    )
    return normalise_13f_holdings([holding], filed_at=_FILED)[0]


def _sync_write(conn: psycopg.Connection[tuple], iid: int, shares: int) -> None:
    record_institution_observation(
        conn,
        instrument_id=iid,
        filer_cik=_CIK,
        filer_name="JOHNSON & JOHNSON",
        filer_type="INV",
        ownership_nature="economic",
        source="13f",
        source_document_id=_ACC,
        source_accession=_ACC,
        source_field=None,
        source_url=None,
        filed_at=_FILED,
        period_start=None,
        period_end=date(2026, 6, 30),
        ingest_run_id=uuid4(),
        shares=Decimal(shares),
        market_value_usd=None,
        voting_authority=None,
    )
    conn.commit()


def test_per_filing_then_sync(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    conn = ebull_test_conn
    iid = _instrument(conn, "478160104")
    filer_id = _filer(conn)
    capture = _per_filing_write(conn, filer_id, [(iid, _normalised("478160104", 1_979_173))])
    obs = _observation(conn, iid)
    assert obs["thirteen_f_segments"]["capture_id"] == str(capture.capture_id)
    assert _capture_record(conn)[0] == str(capture.capture_id)  # type: ignore[index]

    _sync_write(conn, iid, 1_979_173)  # the weekly sync replays the same population: kept
    assert _observation(conn, iid)["thirteen_f_segments"] == obs["thirteen_f_segments"]
    _sync_write(conn, iid, 1_979_174)  # changed shares: cleared
    assert _observation(conn, iid)["thirteen_f_segments"] is None


def test_replay_orders_end_on_the_last_capture(ebull_test_conn: psycopg.Connection[tuple], tmp_path: Path) -> None:
    conn = ebull_test_conn
    iid = _instrument(conn, "478160104")
    filer_id = _filer(conn)
    lines = [_line("478160104", 100, "DFND", "1")]

    # bulk, then per-filing: the per-filing capture wins, record and observation agree.
    ingest_13f_dataset_archive(conn=conn, archive_path=_archive(tmp_path, lines, managers=[("1", "1")], name="b.zip"))
    conn.commit()
    capture = _per_filing_write(conn, filer_id, [(iid, _normalised("478160104", 100))])
    assert _observation(conn, iid)["thirteen_f_segments"]["capture_id"] == str(capture.capture_id)
    assert _capture_record(conn)[:2] == (str(capture.capture_id), "xml")  # type: ignore[index]

    # per-filing, then bulk: the bulk capture wins.
    ingest_13f_dataset_archive(conn=conn, archive_path=_archive(tmp_path, lines, managers=[("1", "1")], name="b2.zip"))
    conn.commit()
    record = _capture_record(conn)
    assert record is not None and record[1] == "dataset"
    assert _observation(conn, iid)["thirteen_f_segments"]["capture_id"] == record[0]

    # sync last, equal shares: kept and still coherent.
    _sync_write(conn, iid, 100)
    assert _observation(conn, iid)["thirteen_f_segments"]["capture_id"] == record[0]


def test_recapture_not_rewriting_an_observation_leaves_it_incoherent(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    iid_a = _instrument(conn, "478160104")
    iid_b = _instrument(conn, "037833100")
    filer_id = _filer(conn)
    first = _per_filing_write(
        conn, filer_id, [(iid_a, _normalised("478160104", 10)), (iid_b, _normalised("037833100", 20))]
    )
    second = _per_filing_write(conn, filer_id, [(iid_a, _normalised("478160104", 10))])
    assert _capture_record(conn)[0] == str(second.capture_id)  # type: ignore[index]
    assert _observation(conn, iid_a)["thirteen_f_segments"]["capture_id"] == str(second.capture_id)
    # The read's coherence check (§ Rule 3) refuses this one.
    assert _observation(conn, iid_b)["thirteen_f_segments"]["capture_id"] == str(first.capture_id)


def test_recapture_from_stored_keeps_list_and_controls(
    ebull_test_conn: psycopg.Connection[tuple], tmp_path: Path
) -> None:
    conn = ebull_test_conn
    _instrument(conn, "478160104")
    lines = [_line("478160104", 100, "DFND", "1")]
    ingest_13f_dataset_archive(conn=conn, archive_path=_archive(tmp_path, lines, managers=[("1", "77")]))
    conn.commit()
    capture = recapture_from_stored(conn, accession_number=_ACC, line_count=1, value_total=Decimal(1000))
    assert capture is not None
    assert capture.status == "complete"
    assert [e.cik for e in capture.entries] == ["77"]
    assert (
        recapture_from_stored(conn, accession_number="0000000000-00-000000", line_count=0, value_total=Decimal(0))
        is None
    )


def test_capture_only_backfill_writes_segments_and_nothing_else(
    ebull_test_conn: psycopg.Connection[tuple], tmp_path: Path
) -> None:
    conn = ebull_test_conn
    iid = _instrument(conn, "478160104", "47816A100")
    unresolved = _line("999999999", 5, "SOLE", "")
    ingest_13f_dataset_archive(
        conn=conn, archive_path=_archive(tmp_path, [*_MIXED_LINES, unresolved], managers=None, name="old.zip")
    )
    conn.commit()
    before = _observation(conn, iid)
    assert before["thirteen_f_segments"] is None
    unresolved_before = conn.execute("SELECT count(*), sum(observation_count) FROM unresolved_13f_cusips").fetchone()

    result = ingest_13f_dataset_archive(
        conn=conn,
        archive_path=_archive(tmp_path, [*_MIXED_LINES, unresolved], managers=[("1", "1"), ("2", "2")], name="new.zip"),
        capture_only=True,
    )
    conn.commit()
    after = _observation(conn, iid)
    record = _capture_record(conn)
    assert record is not None and record[2] == "complete"
    assert result.rows_written == 1
    assert after["thirteen_f_segments"] == _python_segments(record[0], _MIXED_LINES)
    assert (after["shares"], after["ingest_run_id"], after["ingested_at"]) == (
        before["shares"],
        before["ingest_run_id"],
        before["ingested_at"],
    )
    assert (
        conn.execute("SELECT count(*), sum(observation_count) FROM unresolved_13f_cusips").fetchone()
        == unresolved_before
    )


def test_capture_only_refuses_an_archive_without_a_list(
    ebull_test_conn: psycopg.Connection[tuple], tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="OTHERMANAGER2"):
        ingest_13f_dataset_archive(
            conn=ebull_test_conn,
            archive_path=_archive(tmp_path, [_line("478160104", 1, "SOLE", "")], managers=None),
            capture_only=True,
        )


def test_capture_only_reaches_a_late_filing_behind_the_retention_cap(
    ebull_test_conn: psycopg.Connection[tuple], tmp_path: Path
) -> None:
    # A 2024Q2 amendment filed in 2026: its observation exists, and the capture describes it although the bulk
    # write path's retention gate would skip the line.
    conn = ebull_test_conn
    iid = _instrument(conn, "478160104")
    record_institution_observation(
        conn,
        instrument_id=iid,
        filer_cik=_CIK,
        filer_name="JOHNSON & JOHNSON",
        filer_type="INV",
        ownership_nature="economic",
        source="13f",
        source_document_id=_ACC,
        source_accession=_ACC,
        source_field=None,
        source_url=None,
        filed_at=_FILED,
        period_start=None,
        period_end=date(2024, 6, 30),
        ingest_run_id=uuid4(),
        shares=Decimal(100),
        market_value_usd=None,
        voting_authority=None,
    )
    conn.commit()
    lines = [_line("478160104", 100, "DFND", "1")]
    archive = _archive(tmp_path, lines, managers=[("1", "1")], period="2024-06-30")
    result = ingest_13f_dataset_archive(conn=conn, archive_path=archive, capture_only=True)
    conn.commit()
    assert result.rows_written == 1
    assert _observation(conn, iid)["thirteen_f_segments"]["lines"] == [
        {"discretion": "DFND", "other_managers": [1], "shares": "100"}
    ]
