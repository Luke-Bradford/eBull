"""Tests for the N-CSR / N-CSRS real manifest-worker parser adapter (#1171).

Replaces the #918 / PR #1170 synth no-op test. Real parser flow per
spec §8 with monkeypatched fetch + extractor stub so DB writes are
isolated to ``fund_metadata_observations`` + ``fund_metadata_current``.

Covered cases (per implementation plan §2.T10):

- Parser-version rewash: known_to supersession.
- Partial-success: 5 classes, 2 resolve, 3 miss with mixed reasons →
  outcome=parsed; 2 observations + 3 per-miss log entries.
- Zero-resolution unanimous deterministic reason → tombstoned.
- Zero-resolution mixed transient/deterministic → failed.
- Missing URL tombstone.
- Empty fetch tombstone.
- Fetch exception → failed.
- Parse exception (extractor raises ValueError) → tombstoned.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import pytest

from app.services.manifest_parsers import sec_n_csr as parser_mod
from app.services.manifest_parsers._fund_class_resolver import ResolverMissReason
from app.services.n_csr_extractor import FundMetadataFacts
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


@dataclass
class _ManifestRowStub:
    accession_number: str
    primary_document_url: str | None
    filed_at: datetime
    cik: str = "0000819118"
    form: str = "N-CSR"


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    symbol: str,
    class_id: str,
) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} fund"),
    )
    conn.execute(
        """
        INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value)
        VALUES (%s, 'sec', 'class_id', %s)
        ON CONFLICT DO NOTHING
        """,
        (iid, class_id),
    )


def _seed_directory(conn: psycopg.Connection[tuple], *, class_id: str, symbol: str | None) -> None:
    conn.execute(
        """
        INSERT INTO cik_refresh_mf_directory (class_id, series_id, symbol, trust_cik)
        VALUES (%s, 'S000000001', %s, '0000819118')
        ON CONFLICT (class_id) DO NOTHING
        """,
        (class_id, symbol),
    )


def _make_facts(
    *,
    class_id: str,
    expense_ratio: Decimal | None = Decimal("0.0004"),
    net_assets: Decimal | None = Decimal("1000000000"),
) -> FundMetadataFacts:
    return FundMetadataFacts(
        class_id=class_id,
        trust_cik="0000819118",
        series_id="S000000001",
        document_type="N-CSR",
        period_end=date(2025, 12, 31),
        expense_ratio_pct=expense_ratio,
        net_assets_amt=net_assets,
        sector_allocation={"Tech": Decimal("0.30"), "Finance": Decimal("0.20")},
    )


def _patch_fetch_and_extract(
    monkeypatch: pytest.MonkeyPatch,
    *,
    fetch_returns: bytes | None = b"<dummy/>",
    fetch_raises: Exception | None = None,
    extract_returns: list[FundMetadataFacts] | None = None,
    extract_raises: Exception | None = None,
) -> None:
    def _fake_fetch(_url: str) -> bytes | None:
        if fetch_raises:
            raise fetch_raises
        return fetch_returns

    monkeypatch.setattr(parser_mod, "_fetch_ixbrl", _fake_fetch)

    def _fake_extract(_bytes: bytes) -> list[FundMetadataFacts]:
        if extract_raises:
            raise extract_raises
        return extract_returns or []

    monkeypatch.setattr(parser_mod, "extract_fund_metadata_facts", _fake_extract)


def test_parsed_with_one_class(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_instrument(ebull_test_conn, iid=3001, symbol="VFIAX", class_id="C000000010")
    ebull_test_conn.commit()
    _patch_fetch_and_extract(
        monkeypatch,
        extract_returns=[_make_facts(class_id="C000000010")],
    )

    row = _ManifestRowStub(
        accession_number="0001-26-AAA",
        primary_document_url="https://www.sec.gov/Archives/edgar/data/819118/000087/primary_doc.htm",
        filed_at=datetime(2026, 2, 27, tzinfo=UTC),
    )
    outcome = parser_mod._parse_sec_n_csr(ebull_test_conn, row)
    assert outcome.status == "parsed"
    assert outcome.parser_version == "n-csr-fund-metadata-v1"

    cur = ebull_test_conn.execute(
        "SELECT COUNT(*) FROM fund_metadata_observations WHERE instrument_id = %s AND known_to IS NULL",
        (3001,),
    )
    _row = cur.fetchone()
    assert _row is not None
    assert _row[0] == 1

    cur = ebull_test_conn.execute(
        "SELECT expense_ratio_pct FROM fund_metadata_current WHERE instrument_id = %s",
        (3001,),
    )
    _row = cur.fetchone()
    assert _row is not None
    assert _row[0] == Decimal("0.00040000")


def test_parser_version_rewash_supersedes_prior(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_instrument(ebull_test_conn, iid=3002, symbol="VOO", class_id="C000000020")
    ebull_test_conn.commit()

    _patch_fetch_and_extract(
        monkeypatch,
        extract_returns=[_make_facts(class_id="C000000020", expense_ratio=Decimal("0.0003"))],
    )
    row = _ManifestRowStub(
        accession_number="0001-26-RWA",
        primary_document_url="https://www.sec.gov/Archives/edgar/data/819118/000088/primary_doc.htm",
        filed_at=datetime(2026, 2, 27, tzinfo=UTC),
    )
    parser_mod._parse_sec_n_csr(ebull_test_conn, row)

    _patch_fetch_and_extract(
        monkeypatch,
        extract_returns=[_make_facts(class_id="C000000020", expense_ratio=Decimal("0.0005"))],
    )
    outcome = parser_mod._parse_sec_n_csr(ebull_test_conn, row)
    assert outcome.status == "parsed"

    cur = ebull_test_conn.execute(
        "SELECT COUNT(*) FROM fund_metadata_observations WHERE instrument_id = %s",
        (3002,),
    )
    _row = cur.fetchone()
    assert _row is not None
    assert _row[0] == 2

    cur = ebull_test_conn.execute(
        "SELECT COUNT(*) FROM fund_metadata_observations WHERE instrument_id = %s AND known_to IS NULL",
        (3002,),
    )
    _row = cur.fetchone()
    assert _row is not None
    assert _row[0] == 1

    cur = ebull_test_conn.execute(
        "SELECT expense_ratio_pct FROM fund_metadata_current WHERE instrument_id = %s",
        (3002,),
    )
    _row = cur.fetchone()
    assert _row is not None
    assert _row[0] == Decimal("0.00050000")


def test_partial_success_mixed_misses(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_instrument(ebull_test_conn, iid=3010, symbol="V1", class_id="C000000031")
    _seed_instrument(ebull_test_conn, iid=3011, symbol="V2", class_id="C000000032")
    _seed_directory(ebull_test_conn, class_id="C000000034", symbol="UNKNOWN_SYMBOL_NOT_IN_INSTRUMENTS")
    _seed_instrument(ebull_test_conn, iid=3013, symbol="V3", class_id="C999999999")
    _seed_directory(ebull_test_conn, class_id="C000000035", symbol="V3")
    ebull_test_conn.commit()

    facts_list = [
        _make_facts(class_id="C000000031"),  # resolves
        _make_facts(class_id="C000000032"),  # resolves
        _make_facts(class_id="C000000033"),  # PENDING_CIK_REFRESH (no directory row)
        _make_facts(class_id="C000000034"),  # INSTRUMENT_NOT_IN_UNIVERSE
        _make_facts(class_id="C000000035"),  # EXT_ID_NOT_YET_WRITTEN
    ]
    _patch_fetch_and_extract(monkeypatch, extract_returns=facts_list)

    row = _ManifestRowStub(
        accession_number="0001-26-PART",
        primary_document_url="https://www.sec.gov/x/primary_doc.htm",
        filed_at=datetime(2026, 2, 27, tzinfo=UTC),
    )
    outcome = parser_mod._parse_sec_n_csr(ebull_test_conn, row)
    assert outcome.status == "parsed"

    cur = ebull_test_conn.execute(
        "SELECT COUNT(*) FROM fund_metadata_observations WHERE source_accession = %s AND known_to IS NULL",
        ("0001-26-PART",),
    )
    _row = cur.fetchone()
    assert _row is not None
    assert _row[0] == 2


def test_zero_resolution_unanimous_deterministic_tombstones(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_directory(ebull_test_conn, class_id="C000000041", symbol="NOT_IN_UNIVERSE_1")
    _seed_directory(ebull_test_conn, class_id="C000000042", symbol="NOT_IN_UNIVERSE_2")
    ebull_test_conn.commit()

    _patch_fetch_and_extract(
        monkeypatch,
        extract_returns=[
            _make_facts(class_id="C000000041"),
            _make_facts(class_id="C000000042"),
        ],
    )

    row = _ManifestRowStub(
        accession_number="0001-26-TOMB",
        primary_document_url="https://www.sec.gov/x/primary_doc.htm",
        filed_at=datetime(2026, 2, 27, tzinfo=UTC),
    )
    outcome = parser_mod._parse_sec_n_csr(ebull_test_conn, row)
    assert outcome.status == "tombstoned"
    assert outcome.error == ResolverMissReason.INSTRUMENT_NOT_IN_UNIVERSE.value


def test_zero_resolution_mixed_pending_failed(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_directory(ebull_test_conn, class_id="C000000051", symbol="NOT_IN_UNIVERSE")
    ebull_test_conn.commit()

    _patch_fetch_and_extract(
        monkeypatch,
        extract_returns=[
            _make_facts(class_id="C000000051"),
            _make_facts(class_id="C000000052"),
        ],
    )

    row = _ManifestRowStub(
        accession_number="0001-26-FAIL",
        primary_document_url="https://www.sec.gov/x/primary_doc.htm",
        filed_at=datetime(2026, 2, 27, tzinfo=UTC),
    )
    outcome = parser_mod._parse_sec_n_csr(ebull_test_conn, row)
    assert outcome.status == "failed"
    assert "pending" in outcome.error.lower()


def test_missing_url_tombstones(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    row = _ManifestRowStub(
        accession_number="0001-26-NOURL",
        primary_document_url=None,
        filed_at=datetime(2026, 2, 27, tzinfo=UTC),
    )
    outcome = parser_mod._parse_sec_n_csr(ebull_test_conn, row)
    assert outcome.status == "tombstoned"
    assert "primary_document_url" in outcome.error


def test_empty_fetch_tombstones(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_fetch_and_extract(monkeypatch, fetch_returns=None)
    row = _ManifestRowStub(
        accession_number="0001-26-EMPTY",
        primary_document_url="https://www.sec.gov/x/primary_doc.htm",
        filed_at=datetime(2026, 2, 27, tzinfo=UTC),
    )
    outcome = parser_mod._parse_sec_n_csr(ebull_test_conn, row)
    assert outcome.status == "tombstoned"


def test_fetch_exception_failed(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_fetch_and_extract(monkeypatch, fetch_raises=RuntimeError("network down"))
    row = _ManifestRowStub(
        accession_number="0001-26-NET",
        primary_document_url="https://www.sec.gov/x/primary_doc.htm",
        filed_at=datetime(2026, 2, 27, tzinfo=UTC),
    )
    outcome = parser_mod._parse_sec_n_csr(ebull_test_conn, row)
    assert outcome.status == "failed"
    assert "fetch error" in outcome.error


def test_parse_exception_tombstones(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_fetch_and_extract(monkeypatch, extract_raises=ValueError("iXBRL malformed"))
    row = _ManifestRowStub(
        accession_number="0001-26-PARSE",
        primary_document_url="https://www.sec.gov/x/primary_doc.htm",
        filed_at=datetime(2026, 2, 27, tzinfo=UTC),
    )
    outcome = parser_mod._parse_sec_n_csr(ebull_test_conn, row)
    assert outcome.status == "tombstoned"
    assert "parse error" in outcome.error


def test_ixbrl_companion_url_derivation() -> None:
    """Spike §3.3 convention: ``<basename>_htm.xml`` in same accession folder."""
    primary = "https://www.sec.gov/Archives/edgar/data/819118/000087119625020022/fcsxxxst.htm"
    assert (
        parser_mod._ixbrl_companion_url(primary)
        == "https://www.sec.gov/Archives/edgar/data/819118/000087119625020022/fcsxxxst_htm.xml"
    )


def test_register_idempotent() -> None:
    parser_mod.register()
    parser_mod.register()
