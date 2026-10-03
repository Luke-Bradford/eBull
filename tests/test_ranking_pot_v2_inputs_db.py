"""#3592 slice 3b — the v2 reader's SQL against real Postgres (spec §5; ``app/services/ranking_pot_v2_inputs.py``).

One case per SQL mechanism: the purchase window and known-at cut, the pair-keyed history (issuer-wide, three years,
filed before the purchase year's 1 January UTC), S* and its revisions, the manifest gap counts and the usable-row
month counts. The pure rules on top are ``tests/test_ranking_pot_v2.py``'s.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from fractions import Fraction
from typing import Any

import psycopg

from app.services import ranking_pot_v2 as v2
from app.services import ranking_pot_v2_inputs as rd

Conn = psycopg.Connection[Any]

TARGET = date(2026, 10, 5)  # window [2026-04-01, 2026-10-01)
AS_OF = datetime(2026, 10, 3, 12, tzinfo=UTC)
S0 = (35921, 35922)
OTHER = 35923  # not in S₀; shares the issuer CIK of 35921
ISSUER, ISSUER_B = "0000000789", "0000000790"
F1, F2 = "0006666001", "0006666002"


def _at(d: str) -> datetime:
    return datetime.fromisoformat(d).replace(tzinfo=UTC)


def _instruments(conn: Conn) -> None:
    for iid in (*S0, OTHER):
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, 'V2', TRUE)",
            (iid, f"V2R{iid}"),
        )


_seq = iter(range(1, 10_000))


def _txn(
    conn: Conn,
    *,
    iid: int,
    issuer: str,
    filer: str,
    txn: str,
    filed: str,
    code: str = "P",
    ad: str | None = "A",
    derivative: bool = False,
    status: str = "parsed",
) -> int:
    n = next(_seq)
    acc = f"0009999999-26-{n:06d}"
    conn.execute(
        "INSERT INTO sec_filing_manifest (accession_number, cik, form, source, subject_type, subject_id, "
        "instrument_id, filed_at, ingest_status) VALUES (%s, %s, '4', 'sec_form4', 'issuer', %s, %s, %s, %s)",
        (acc, issuer, str(iid), iid, _at(filed), status),
    )
    conn.execute(
        "INSERT INTO insider_filings (accession_number, instrument_id, document_type, issuer_cik) "
        "VALUES (%s, %s, '4', %s)",
        (acc, iid, issuer),
    )
    row = conn.execute(
        "INSERT INTO insider_transactions (accession_number, txn_row_num, instrument_id, filer_cik, filer_name, "
        "txn_date, txn_code, acquired_disposed_code, is_derivative) VALUES (%s, 1, %s, %s, 'x', %s, %s, %s, %s) "
        "RETURNING id",
        (acc, iid, filer, date.fromisoformat(txn), code, ad, derivative),
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_purchases_and_their_pair_history(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _instruments(conn)
    s0 = S0[0]
    buy = _txn(conn, iid=s0, issuer=ISSUER, filer=F1, txn="2026-05-10", filed="2026-05-12")
    no_direction = _txn(conn, iid=s0, issuer=ISSUER, filer=F1, txn="2026-06-01", filed="2026-06-02", ad=None)
    _txn(conn, iid=s0, issuer=ISSUER, filer=F1, txn="2026-03-31", filed="2026-04-01")  # before the window
    _txn(conn, iid=s0, issuer=ISSUER, filer=F1, txn="2026-09-01", filed="2026-10-04")  # filed after as_of
    _txn(conn, iid=OTHER, issuer=ISSUER, filer=F1, txn="2026-05-11", filed="2026-05-12")  # not an S₀ name
    _txn(conn, iid=s0, issuer=ISSUER, filer=F1, txn="2026-05-12", filed="2026-05-13", code="S", ad="D")  # a sale

    expected_history = {
        _txn(conn, iid=s0, issuer=ISSUER, filer=F1, txn="2025-02-01", filed="2025-02-03", code="S", ad="D"),
        _txn(conn, iid=OTHER, issuer=ISSUER, filer=F1, txn="2024-05-01", filed="2024-05-02"),  # issuer-wide
        _txn(conn, iid=s0, issuer=ISSUER, filer=F1, txn="2023-12-01", filed="2023-12-02", code="S", ad="D"),
    }
    excluded_history = [
        dict(txn="2022-12-01", filed="2022-12-02"),  # older than three years
        dict(txn="2025-03-01", filed="2025-03-02", derivative=True),
        dict(txn="2025-12-20", filed="2026-01-05"),  # filed after the purchase year's 1 January
        dict(txn="2025-04-01", filed="2025-04-02", code="A", ad="A"),  # not open-market
        dict(txn="2025-05-01", filed="2025-05-02", filer=F2),  # another owner
        dict(txn="2025-06-01", filed="2025-06-02", issuer=ISSUER_B),  # another issuer
    ]
    for kw in excluded_history:
        _txn(conn, **({"iid": s0, "issuer": ISSUER, "filer": F1} | kw))  # type: ignore[arg-type]
    conn.commit()

    purchases = rd.read_purchases(conn, s0_ids=S0, target_session=TARGET, as_of=AS_OF)
    assert {r.txn_id for r in purchases} == {buy, no_direction}
    history = rd.read_history(conn, purchases, as_of=AS_OF)
    assert {r.txn_id for r in history} == expected_history
    # The reader's history is exactly the cells the core keys on.
    key: v2.PairKey = (F1, ISSUER, 2026)
    assert v2.pair_cells(history, key) == ((2023, 12), (2024, 5), (2025, 2))

    inputs = rd.read_v2_inputs(conn, s0_ids=S0, target_session=TARGET, as_of=AS_OF)
    assert (inputs.purchases, inputs.history) == (purchases, history)
    assert inputs.reader_counts == {"form4_manifest_unparsed": 0, "form4_manifest_tombstoned": 0}
    assert v2.decode_v2_block(v2.encode_v2_block(inputs)) == inputs
    conn.rollback()


def test_manifest_gaps_and_usable_counts(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _instruments(conn)
    s0 = S0[0]
    _txn(conn, iid=s0, issuer=ISSUER, filer=F1, txn="2026-05-01", filed="2026-05-02", status="pending")
    _txn(conn, iid=s0, issuer=ISSUER, filer=F1, txn="2026-05-02", filed="2026-05-03", status="tombstoned")
    _txn(conn, iid=s0, issuer=ISSUER, filer=F1, txn="2026-03-01", filed="2026-03-31", status="pending")  # early
    _txn(conn, iid=OTHER, issuer=ISSUER, filer=F1, txn="2026-05-01", filed="2026-05-02", status="pending")
    _txn(conn, iid=s0, issuer=ISSUER, filer=F1, txn="2026-05-03", filed="2026-05-04", derivative=True)
    conn.commit()
    gaps = rd.read_manifest_gaps(conn, s0_ids=S0, target_session=TARGET, as_of=AS_OF)
    assert gaps == {"form4_manifest_unparsed": 2, "form4_manifest_tombstoned": 1}
    counts = rd.usable_history_counts(conn, first_month=date(2026, 3, 1), end_month=date(2026, 6, 1))
    # The derivative row is not a history trade; manifest status does not matter to the corpus count.
    assert counts == {date(2026, 3, 1): 1, date(2026, 5, 1): 3}
    conn.rollback()


def _dtc(conn: Conn, iid: int, settle: str, known: str, dtc: str | None, adv: int | None, doc: str = "d") -> None:
    conn.execute(
        "INSERT INTO finra_short_interest_observations (instrument_id, settlement_date, source_document_id, "
        "current_short_interest, average_daily_volume, days_to_cover, source, source_url, filed_at, period_end, "
        "known_from, ingest_run_id) VALUES (%s, %s, %s, 1000, %s, %s, 'finra_si', 'u', %s, %s, %s, %s)",
        (
            iid,
            date.fromisoformat(settle),
            doc,
            adv,
            None if dtc is None else Decimal(dtc),
            _at(known),
            date.fromisoformat(settle),
            _at(known),
            uuid.uuid4(),
        ),
    )


def test_dtc_rows_at_s_star(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _instruments(conn)
    a, b = S0
    _dtc(conn, a, "2026-08-31", "2026-09-10", "2.00", 500)
    _dtc(conn, a, "2026-09-15", "2026-09-20", "3.00", 400, doc="r1")
    _dtc(conn, a, "2026-09-15", "2026-09-25", "3.50", 400, doc="r2")  # a later revision: both are stored
    _dtc(conn, b, "2026-09-15", "2026-09-26", "999.99", 0)
    _dtc(conn, a, "2026-09-30", "2026-10-04", "4.00", 400)  # not yet observed at as_of
    _dtc(conn, OTHER, "2026-09-30", "2026-10-02", "4.00", 400)  # observed, but not an S₀ name: S* stays
    _dtc(conn, b, "2026-10-15", "2026-10-02", "1.00", 400)  # a settlement after as_of's date
    conn.commit()
    rows = rd.read_dtc_rows(conn, s0_ids=S0, as_of=AS_OF)
    assert [(r.instrument_id, r.settlement_date, r.source_document_id, r.days_to_cover) for r in rows] == [
        (a, date(2026, 9, 15), "r1", Decimal("3.00")),
        (a, date(2026, 9, 15), "r2", Decimal("3.50")),
        (b, date(2026, 9, 15), "d", Decimal("999.99")),
    ]
    assert all(type(r.average_daily_volume) is int for r in rows)
    read = v2.dtc_read(rows, s0_ids=S0, as_of=AS_OF)
    assert (read.settlement_date, dict(read.values), dict(read.missing)) == (
        date(2026, 9, 15),
        {a: Fraction(7, 2)},
        {b: "not_available"},
    )
    assert rd.read_dtc_rows(conn, s0_ids=S0, as_of=_at("2026-09-01")) == ()
    conn.rollback()
