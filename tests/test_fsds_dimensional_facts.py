"""Tests for the #1590 DERA FSDS bulk dimensional-facts loader.

Pure-logic tests (classifier exact-set rule, period reconstruction, label
prettify, value-overage parity) carry the routing correctness with no DB. ONE
DB-backed integration test exercises the genuinely-new SQL mechanism — the
advisory-lock + NOT-EXISTS convergence-guard writer + CIK fan-out — end to end
through a tiny synthetic FSDS zip. The full-quarter real-data behaviour is the
DoD dev-verify (backfill on 2025q1), not a heavyweight fixture.
"""

from __future__ import annotations

import zipfile
from datetime import date
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest

from app.services.dimensional_facts import DimensionalFact, DimensionalMetric, mark_value_overage_subtotals
from app.services.fsds_dimensional_facts import (
    _AccBuf,
    _accumulate,
    _classify_fsds_segments,
    _prettify_member,
    _reconstruct_period,
    ingest_fsds_dimensional_archive,
)

# --- _classify_fsds_segments (exact-set routing) --------------------------------


@pytest.mark.parametrize(
    ("cell", "expected"),
    [
        ("BusinessSegments=Cloud;", ("business_segment", "Cloud")),
        ("ProductOrService=Widgets;", ("product_service", "Widgets")),
        ("Geographical=US;", ("geographic", "US")),
        # {BusinessSegments, ConsolidationItems=OperatingSegments} routes (the clean value).
        ("BusinessSegments=Cloud;ConsolidationItems=OperatingSegments;", ("business_segment", "Cloud")),
    ],
)
def test_classify_routes(cell: str, expected: tuple[str, str]) -> None:
    assert _classify_fsds_segments(cell) == expected


@pytest.mark.parametrize(
    "cell",
    [
        "",  # empty
        "BusinessSegments=Cloud;ProductOrService=Widgets;",  # cross-dimensional → double-count risk
        "BusinessSegments=Cloud;Geographical=US;",  # cross-dimensional
        "BusinessSegments=Cloud;ConsolidationItems=Eliminations;",  # not the operating-segment member
        "Geographical=US;Geographical=CA;",  # repeated axis token → ambiguous
        "GeographicalAreas=US;",  # different axis (exact token, not substring)
        "BusinessSegments=;",  # empty member
        "nodelimiter;",  # malformed fragment
        "ConsolidationItems=OperatingSegments;",  # ConsolidationItems alone is not a route
    ],
)
def test_classify_rejects(cell: str) -> None:
    assert _classify_fsds_segments(cell) is None


# --- _reconstruct_period (instant/duration contract) ----------------------------


def test_period_instant_assets() -> None:
    assert _reconstruct_period("20241231", "0", "assets") == (None, date(2024, 12, 31))


def test_period_duration_revenue() -> None:
    # qtrs=4 → start = end − 12 months (within the reader's 330-400d annual window).
    assert _reconstruct_period("20241231", "4", "revenue") == (date(2023, 12, 31), date(2024, 12, 31))


@pytest.mark.parametrize(
    ("ddate", "qtrs", "metric"),
    [
        ("20241231", "0", "revenue"),  # duration metric reported as an instant
        ("20241231", "4", "assets"),  # instant metric reported as a duration
        ("notadate", "4", "revenue"),  # bad ddate
        ("20241231", "x", "revenue"),  # bad qtrs
        ("20241231", "-1", "revenue"),  # negative qtrs
    ],
)
def test_period_rejects(ddate: str, qtrs: str, metric: str) -> None:
    assert _reconstruct_period(ddate, qtrs, metric) is None  # type: ignore[arg-type]


# --- _prettify_member -----------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "pretty"),
    [
        ("SpecialtyDiagnostics", "Specialty Diagnostics"),
        ("US", "US"),
        ("FoodAndBeverage", "Food And Beverage"),
        ("NorthAmerica", "North America"),
    ],
)
def test_prettify(raw: str, pretty: str) -> None:
    assert _prettify_member(raw) == pretty


# --- value-overage parity (the shared #554 helper, product axis) ----------------


def _fact(axis: str, member: str, val: str) -> DimensionalFact:
    return DimensionalFact(
        axis=axis,  # type: ignore[arg-type]
        member_qname=member,
        member_label=member,
        metric="revenue",
        unit="USD",
        is_subtotal=False,
        period_start=date(2023, 12, 31),
        period_end=date(2024, 12, 31),
        val=Decimal(val),
        decimals=None,
    )


def test_value_overage_marks_product_subtotal() -> None:
    # iPhone 200 + Mac 100 + Products 300; consolidated total 300 → overage 300 =
    # {Products} → Products is the subtotal.
    facts = [
        _fact("product_service", "iPhone", "200"),
        _fact("product_service", "Mac", "100"),
        _fact("product_service", "Products", "300"),
    ]
    totals: dict[tuple[DimensionalMetric, date | None, date], Decimal] = {
        ("revenue", date(2023, 12, 31), date(2024, 12, 31)): Decimal("300")
    }
    marked, _rej = mark_value_overage_subtotals(facts, totals, accession="acc-x")
    by_member = {f.member_qname: f.is_subtotal for f in marked}
    assert by_member == {"iPhone": False, "Mac": False, "Products": True}


def test_value_overage_business_segment_excluded() -> None:
    # business_segment is never value-overage-marked (member-sum legitimately differs
    # from consolidated via unallocated corporate items).
    facts = [
        _fact("business_segment", "A", "200"),
        _fact("business_segment", "B", "100"),
        _fact("business_segment", "Total", "300"),
    ]
    totals: dict[tuple[DimensionalMetric, date | None, date], Decimal] = {
        ("revenue", date(2023, 12, 31), date(2024, 12, 31)): Decimal("300")
    }
    marked, _rej = mark_value_overage_subtotals(facts, totals, accession="acc-x")
    assert all(not f.is_subtotal for f in marked)


# --- _accumulate (alias arbitration + coreg filter; the unique-index guard) -----


def _num_dict(tag: str, segments: str, value: str, *, coreg: str = "", version: str = "us-gaap/2024") -> dict[str, str]:
    return {
        "adsh": "acc",
        "tag": tag,
        "version": version,
        "ddate": "20241231",
        "qtrs": "4",
        "uom": "USD",
        "segments": segments,
        "coreg": coreg,
        "value": value,
        "footnote": "",
    }


_REV2 = "RevenueFromContractWithCustomerExcludingAssessedTax"  # a different revenue alias from Revenues


def test_accumulate_alias_collapses_to_one() -> None:
    # One member+period tagged under TWO revenue aliases (same value) must collapse to
    # ONE fact — else the uq_dimensional_facts_identity index rejects the insert batch.
    per: dict[str, _AccBuf] = {}
    _accumulate(per, _num_dict("Revenues", "BusinessSegments=Cloud;", "1000"))
    _accumulate(per, _num_dict(_REV2, "BusinessSegments=Cloud;", "1000"))
    assert len(per["acc"].members) == 1


def test_accumulate_same_alias_value_conflict_drops() -> None:
    # Same alias, same identity, DIFFERENT value, no precision to break the tie → drop.
    per: dict[str, _AccBuf] = {}
    _accumulate(per, _num_dict("Revenues", "BusinessSegments=Cloud;", "1000"))
    _accumulate(per, _num_dict("Revenues", "BusinessSegments=Cloud;", "2000"))
    assert len(per["acc"].members) == 0
    assert (
        "business_segment",
        "Cloud",
        "revenue",
        date(2023, 12, 31),
        date(2024, 12, 31),
    ) in per["acc"].conflicted


def test_accumulate_coreg_row_ignored() -> None:
    # A co-registrant fact (coreg set) belongs to a different entity → never buffered.
    per: dict[str, _AccBuf] = {}
    _accumulate(per, _num_dict("Revenues", "BusinessSegments=Cloud;", "1000", coreg="SUBCO"))
    assert "acc" not in per


def test_lock_templates_byte_identical() -> None:
    # The #1590 bulk writer and #554 replace_accession_rows MUST take the SAME advisory
    # lock key (identical SQL template) or they do not serialise against each other —
    # a desync silently reopens the bulk/per-filing double-count window. Pin byte-identity
    # so a future edit to one site can't drift (review PREVENTION on PR #1681).
    import re

    root = Path(__file__).resolve().parents[1]

    def _lock_sql(rel: str) -> set[str]:
        src = (root / rel).read_text()
        return set(re.findall(r'"([^"]*pg_advisory_xact_lock[^"]*)"', src))

    bulk = _lock_sql("app/services/fsds_dimensional_facts.py")
    store = _lock_sql("app/services/dimensional_facts_store.py")
    notes = _lock_sql("app/services/fsnds_notes_facts.py")
    assert bulk, "no pg_advisory_xact_lock template found in fsds_dimensional_facts.py"
    assert store, "no pg_advisory_xact_lock template found in dimensional_facts_store.py"
    assert notes, "no pg_advisory_xact_lock template found in fsnds_notes_facts.py"
    assert bulk == store == notes, f"advisory-lock templates desynced: bulk={bulk} store={store} notes={notes}"


# --- DB integration: convergence guard + CIK fan-out + 10-K filter --------------

_NUM_HEADER = "adsh\ttag\tversion\tddate\tqtrs\tuom\tsegments\tcoreg\tvalue\tfootnote"
_SUB_HEADER = "adsh\tcik\tform\tperiod\tfiled"


def _num_row(
    adsh: str, tag: str, ddate: str, qtrs: str, segments: str, value: str, version: str = "us-gaap/2024"
) -> str:
    return f"{adsh}\t{tag}\t{version}\t{ddate}\t{qtrs}\tUSD\t{segments}\t\t{value}\t"


def _build_zip(path: Path) -> None:
    rev = "RevenueFromContractWithCustomerExcludingAssessedTax"
    sub = "\n".join(
        [
            _SUB_HEADER,
            "acc-tenk\t111111\t10-K\t20241231\t20250215",
            "acc-tenq\t111111\t10-Q\t20240930\t20241105",  # not 10-K → fully skipped
        ]
    )
    num = "\n".join(
        [
            _NUM_HEADER,
            # 10-K business segments (two clean members).
            _num_row("acc-tenk", "Revenues", "20241231", "4", "BusinessSegments=Cloud;", "1000"),
            # Cloud revenue ALSO tagged under a second alias (same value) → must collapse,
            # not collide on the unique index (the bug the dev backfill caught).
            _num_row("acc-tenk", rev, "20241231", "4", "BusinessSegments=Cloud;", "1000"),
            _num_row("acc-tenk", "Revenues", "20241231", "4", "BusinessSegments=Devices;", "500"),
            # 10-K product disaggregation + consolidated total → value-overage marks Products.
            _num_row("acc-tenk", rev, "20241231", "4", "ProductOrService=Widgets;", "600"),
            _num_row("acc-tenk", rev, "20241231", "4", "ProductOrService=Gadgets;", "400"),
            _num_row("acc-tenk", rev, "20241231", "4", "ProductOrService=Products;", "1000"),
            _num_row("acc-tenk", rev, "20241231", "4", "", "1000"),  # dimensionless consolidated
            # Rejected rows: cross-dimensional, filer-extension namespace, and a 10-Q.
            _num_row("acc-tenk", "Revenues", "20241231", "4", "BusinessSegments=Cloud;Geographical=US;", "111"),
            _num_row("acc-tenk", "Revenues", "20241231", "4", "BusinessSegments=Cloud;", "222", version="acme/2024"),
            _num_row("acc-tenq", "Revenues", "20240930", "4", "BusinessSegments=Cloud;", "999"),
        ]
    )
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("sub.txt", sub)
        zf.writestr("num.txt", num)


_NEXT_IID = [915900]  # high base, isolated from other suites' seeds


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


def _members(conn: psycopg.Connection[tuple], instrument_id: int) -> dict[tuple[str, str], bool]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT axis, member_qname, is_subtotal FROM instrument_dimensional_facts "
            "WHERE instrument_id = %s AND metric = 'revenue' ORDER BY axis, member_qname",
            (instrument_id,),
        )
        return {(r[0], r[1]): r[2] for r in cur.fetchall()}


@pytest.mark.db
class TestBulkIngest:
    def test_fan_out_filter_and_convergence(self, ebull_test_conn: psycopg.Connection[tuple], tmp_path: Path) -> None:
        # Two siblings share CIK 0000111111 → CIK fan-out writes both.
        a = _seed_instrument(ebull_test_conn, symbol="ALPHA")
        b = _seed_instrument(ebull_test_conn, symbol="BETA")
        zip_path = tmp_path / "fsds_2024q4.zip"
        _build_zip(zip_path)

        result = ingest_fsds_dimensional_archive(ebull_test_conn, archive_path=zip_path, fsds_qtr="2024q4")
        assert result.accessions_written == 1  # only acc-tenk; acc-tenq (10-Q) skipped
        assert result.instruments_written == 2  # both siblings

        for iid in (a, b):
            got = _members(ebull_test_conn, iid)
            # Business segments routed; Products marked subtotal by value-overage;
            # cross-dim + filer-extension + 10-Q rows all rejected.
            assert got[("business_segment", "Cloud")] is False
            assert got[("business_segment", "Devices")] is False
            assert got[("product_service", "Widgets")] is False
            assert got[("product_service", "Gadgets")] is False
            assert got[("product_service", "Products")] is True  # value-overage subtotal
            assert ("geographic", "US") not in got  # cross-dim cell rejected
            # No 999 / 111 / 222 leaked in (10-Q + cross-dim + filer-ext).
            assert all(member != "Cloud" or sub is False for (axis, member), sub in got.items())

        # Convergence: a second bulk run writes nothing (rows already exist).
        result2 = ingest_fsds_dimensional_archive(ebull_test_conn, archive_path=zip_path, fsds_qtr="2024q4")
        assert result2.rows_written == 0
        assert result2.accessions_skipped_existing == 1

    def test_per_filing_rows_block_bulk(self, ebull_test_conn: psycopg.Connection[tuple], tmp_path: Path) -> None:
        # A per-filing row already owns acc-tenk on this instrument → bulk must NOT
        # augment it (would double-count: FSDS 'US' ≠ per-filing 'country:US').
        iid = _seed_instrument(ebull_test_conn, symbol="GAMMA")
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO instrument_dimensional_facts (
                    instrument_id, axis, member_qname, member_label, metric, unit,
                    is_subtotal, period_start, period_end, val, decimals, source_accession,
                    form_type, filed_at, parser_version
                ) VALUES (%s, 'geographic', 'country:US', 'United States', 'revenue', 'USD',
                    FALSE, '2024-01-01', '2024-12-31', 123, NULL, 'acc-tenk', '10-K',
                    '2025-02-15T00:00:00Z', 'sec_10k_dimensional_v1')
                """,
                (iid,),
            )
        ebull_test_conn.commit()
        zip_path = tmp_path / "fsds_2024q4.zip"
        _build_zip(zip_path)

        ingest_fsds_dimensional_archive(ebull_test_conn, archive_path=zip_path, fsds_qtr="2024q4")
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM instrument_dimensional_facts "
                "WHERE instrument_id = %s AND source_accession = 'acc-tenk'",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1  # only the pre-existing per-filing row; bulk added nothing
