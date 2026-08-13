"""FSNDS notes loader (#844): read-rule policy, row routing, axis-scoped
convergence against the segment-family writers."""

from __future__ import annotations

import zipfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest

from app.services.dimensional_facts_store import replace_accession_rows
from app.services.fsnds_notes_facts import (
    _NULL_DIMH,
    DEFAULT_MEMBER,
    NonvestedMemo,
    _AccBuf,
    _accumulate,
    ingest_fsnds_notes_archive,
    read_award_dim_map,
    select_nonvested_memo,
)
from app.services.ownership_rollup import _read_nonvested_awards

# --- select_nonvested_memo: the no-sum read rule --------------------------------

_RSU = "RestrictedStockUnitsRSU"


def _row(qname: str, val: int, label: str | None = None) -> tuple[str, str, Decimal]:
    return (qname, label or qname, Decimal(val))


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        # Default total present → wins outright, even over members.
        (
            [_row(DEFAULT_MEMBER, 500), _row(_RSU, 300), _row("PerformanceShares", 100)],
            NonvestedMemo(shares=Decimal(500), label="unvested awards", member_qname=DEFAULT_MEMBER),
        ),
        # Exactly one standard member → renders with member label.
        (
            [_row(_RSU, 151_574_000)],
            NonvestedMemo(shares=Decimal(151_574_000), label="unvested RSUs", member_qname=_RSU),
        ),
        (
            [_row("PerformanceShares", 42, "Performance Shares")],
            NonvestedMemo(shares=Decimal(42), label="unvested performance shares", member_qname="PerformanceShares"),
        ),
        # Single NON-standard member (plan name — unknowable scope) → abstain.
        ([_row("TwoThousandTwentyPlan", 42)], None),
        # Multiple members incl. RSU → RSU member alone (never a Σ).
        (
            [_row(_RSU, 300), _row("PerformanceShares", 100)],
            NonvestedMemo(shares=Decimal(300), label="unvested RSUs", member_qname=_RSU),
        ),
        # Multiple members, no RSU, no default → abstain (non-additive axis).
        ([_row("RestrictedStock", 10), _row("PerformanceShares", 20)], None),
        ([], None),
    ],
)
def test_select_nonvested_memo(rows: list[tuple[str, str, Decimal]], expected: NonvestedMemo | None) -> None:
    assert select_nonvested_memo(rows) == expected


# --- _accumulate: row gating + iprx arbitration ---------------------------------


def _num(
    adsh: str = "acc-1",
    ddate: str = "20250930",
    qtrs: str = "0",
    uom: str = "shares",
    dimh: str = "0xaaa",
    dimn: str = "1",
    iprx: str = "0",
    value: str = "100",
    version: str = "us-gaap/2025",
    coreg: str = "",
    dcml: str = "0",
) -> dict[str, str]:
    return {
        "adsh": adsh,
        "ddate": ddate,
        "qtrs": qtrs,
        "uom": uom,
        "dimh": dimh,
        "dimn": dimn,
        "iprx": iprx,
        "value": value,
        "version": version,
        "coreg": coreg,
        "dcml": dcml,
    }


_AWARD_MAP = {"0xaaa": _RSU}


def _run(rows: list[dict[str, str]]) -> dict[str, _AccBuf]:
    per: dict[str, _AccBuf] = {}
    for r in rows:
        _accumulate(per, r, _AWARD_MAP)
    return per


@pytest.mark.parametrize(
    "row",
    [
        _num(uom="USD"),  # not a share count
        _num(qtrs="4"),  # duration — the count is an instant
        _num(version="acme/2025"),  # filer-extension homonym
        _num(coreg="SubCo"),  # co-registrant entity
        _num(dimh="0xzzz", dimn="2"),  # cross/non-award dims, unrouted
        _num(value="notanumber"),
        _num(ddate="2025093"),  # malformed date
    ],
)
def test_accumulate_rejects(row: dict[str, str]) -> None:
    assert _run([row]) == {}


def test_accumulate_iprx_lowest_wins() -> None:
    per = _run([_num(iprx="1", value="200"), _num(iprx="0", value="100")])
    ((iprx, value, _dcml),) = per["acc-1"].facts.values()
    assert (iprx, value) == (0, Decimal(100))


def test_accumulate_same_iprx_conflict_drops() -> None:
    per = _run([_num(value="100"), _num(value="200")])
    assert per["acc-1"].facts == {}
    assert per["acc-1"].conflicted


def test_accumulate_default_rows_normalise_to_one_key() -> None:
    # dimn=0 with an unexpected dimh and the null hash MUST collapse to one
    # key, or two default rows collide on the identity index at insert.
    per = _run(
        [
            _num(dimh=_NULL_DIMH, dimn="0", value="500"),
            _num(dimh="0xother", dimn="0", value="500"),
        ]
    )
    assert list(per["acc-1"].facts.keys()) == [(datetime(2025, 9, 30, tzinfo=UTC).date(), _NULL_DIMH)]


# --- read_award_dim_map ---------------------------------------------------------


def test_read_award_dim_map_single_axis_only(tmp_path: Path) -> None:
    dim = "\n".join(
        [
            "dimhash\tsegments\tsegt",
            "0x00000000\t\t0",
            f"0xaaa\tAwardType={_RSU};\t0",
            "0xbbb\tAwardType=PerformanceShares\t0",  # no trailing ';' — still routed
            "0xccc\tAwardType=X;Range=Minimum;\t0",  # cross-dimensional — excluded
            "0xddd\tBusinessSegments=Cloud;\t0",  # different axis — excluded
        ]
    )
    path = tmp_path / "fsnds_2026_03_notes.zip"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("dim.tsv", dim)
    with zipfile.ZipFile(path) as zf:
        got = read_award_dim_map(zf)
    assert got == {"0xaaa": _RSU, "0xbbb": "PerformanceShares"}


# --- DB integration: ingest + axis-scoped convergence + read rule ---------------

_NONVESTED = "ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsNonvestedNumber"

_NUM_HEADER = (
    "adsh\ttag\tversion\tddate\tqtrs\tuom\tdimh\tiprx\tvalue\tfootnote\tfootlen\tdimn\tcoreg\tdurp\tdatp\tdcml"
)
_SUB_HEADER = "adsh\tcik\tform\tperiod\tfiled"
_DIM_HEADER = "dimhash\tsegments\tsegt"


def _num_line(adsh: str, ddate: str, dimh: str, dimn: str, value: str, *, form_tag: str = _NONVESTED) -> str:
    return f"{adsh}\t{form_tag}\tus-gaap/2025\t{ddate}\t0\tshares\t{dimh}\t0\t{value}\t\t0\t{dimn}\t\t0\t0\t0"


def _build_zip(path: Path) -> None:
    sub = "\n".join(
        [
            _SUB_HEADER,
            "acc-tenk\t111111\t10-K\t20250930\t20251120",
            "acc-tenq\t111111\t10-Q\t20250630\t20250801",
        ]
    )
    dim = "\n".join(
        [
            _DIM_HEADER,
            "0x00000000\t\t0",
            f"0xaaa\tAwardType={_RSU};\t0",
        ]
    )
    num = "\n".join(
        [
            _NUM_HEADER,
            # 10-K: prior + current balances, member + default rows.
            _num_line("acc-tenk", "20240930", "0xaaa", "1", "163326000.0000"),
            _num_line("acc-tenk", "20250930", "0xaaa", "1", "151574000.0000"),
            _num_line("acc-tenk", "20250930", "0x00000000", "0", "155000000.0000"),
            # 10-Q accession — must be skipped (10-K grain).
            _num_line("acc-tenq", "20250630", "0xaaa", "1", "999.0000"),
        ]
    )
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("sub.tsv", sub)
        zf.writestr("num.tsv", num)
        zf.writestr("dim.tsv", dim)


_NEXT_IID = [917400]  # high base, isolated from other suites' seeds


def _seed_instrument(conn: psycopg.Connection[tuple], *, symbol: str) -> int:
    _NEXT_IID[0] += 1
    iid = _NEXT_IID[0]
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (%s, %s, %s, 'USD', TRUE)",
            (iid, symbol, f"{symbol} Inc."),
        )
        cur.execute(
            "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (%s, 'sec', 'cik', %s, TRUE)",
            (iid, "0000111111"),
        )
    conn.commit()
    return iid


@pytest.mark.db
class TestFsndsIngest:
    def test_ingest_convergence_and_read_rule(self, ebull_test_conn: psycopg.Connection[tuple], tmp_path: Path) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="NVAWD")
        zip_path = tmp_path / "fsnds_2025_11_notes.zip"
        _build_zip(zip_path)

        result = ingest_fsnds_notes_archive(ebull_test_conn, archive_path=zip_path, fsnds_month="2025_11")
        assert result.accessions_written == 1  # acc-tenq (10-Q) skipped
        assert result.rows_written == 3

        # Read rule: default-total row wins over the RSU member.
        info = _read_nonvested_awards(ebull_test_conn, iid, today=datetime(2026, 7, 1, tzinfo=UTC).date())
        assert info is not None
        assert info.shares == Decimal("155000000.0000")
        assert info.label == "unvested awards"
        assert info.source_accession == "acc-tenk"

        # Staleness bound: a far-future today suppresses the memo.
        assert _read_nonvested_awards(ebull_test_conn, iid, today=datetime(2028, 1, 1, tzinfo=UTC).date()) is None

        # Idempotence: second run skips (award rows exist).
        result2 = ingest_fsnds_notes_archive(ebull_test_conn, archive_path=zip_path, fsnds_month="2025_11")
        assert result2.rows_written == 0
        assert result2.accessions_skipped_existing == 1

    def test_axis_scoped_convergence(self, ebull_test_conn: psycopg.Connection[tuple], tmp_path: Path) -> None:
        # (1) A pre-existing per-filing SEGMENT row for the same accession must
        # NOT block the award write; (2) a segment-family rewash of that
        # accession must NOT delete the award rows.
        iid = _seed_instrument(ebull_test_conn, symbol="NVAXS")
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO instrument_dimensional_facts (
                    instrument_id, axis, member_qname, member_label, metric, unit,
                    is_subtotal, period_start, period_end, val, decimals, source_accession,
                    form_type, filed_at, parser_version
                ) VALUES (%s, 'geographic', 'country:US', 'United States', 'revenue', 'USD',
                    FALSE, '2024-10-01', '2025-09-30', 123, NULL, 'acc-tenk', '10-K',
                    '2025-11-20T00:00:00Z', 'sec_10k_dimensional_v1')
                """,
                (iid,),
            )
        ebull_test_conn.commit()
        zip_path = tmp_path / "fsnds_2025_11_notes.zip"
        _build_zip(zip_path)

        result = ingest_fsnds_notes_archive(ebull_test_conn, archive_path=zip_path, fsnds_month="2025_11")
        assert result.rows_written == 3  # segment row did not block the award write

        # Segment rewash (empty per-filing extraction) deletes ONLY its axes.
        with ebull_test_conn.transaction():
            replace_accession_rows(
                ebull_test_conn,
                instrument_id=iid,
                source_accession="acc-tenk",
                form_type="10-K",
                filed_at=datetime(2025, 11, 20, tzinfo=UTC),
                parser_version="sec_10k_dimensional_v1",
                facts=[],
            )
        ebull_test_conn.commit()
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT axis, count(*) FROM instrument_dimensional_facts "
                "WHERE instrument_id = %s AND source_accession = 'acc-tenk' GROUP BY axis",
                (iid,),
            )
            by_axis = dict(cur.fetchall())
        assert by_axis == {"award_type": 3}  # geographic row gone, award rows intact
