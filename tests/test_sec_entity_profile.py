"""Tests for app.services.sec_entity_profile against ebull_test."""

from __future__ import annotations

import psycopg
import pytest

from app.services.sec_entity_profile import (
    get_entity_profile,
    parse_entity_profile,
    upsert_entity_profile,
)
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]


# ---------------------------------------------------------------------------
# parse_entity_profile — unit (no DB)
# ---------------------------------------------------------------------------


class TestParseEntityProfile:
    def test_full_payload_extracts_every_field(self) -> None:
        payload = {
            "cik": "0000002969",
            "sic": "2810",
            "sicDescription": "Industrial Inorganic Chemicals",
            "ownerOrg": "08 Industrial Applications and Services",
            "description": "A chemical company.",
            "website": "https://example.com",
            "investorWebsite": "https://investors.example.com",
            "ein": "231274455",
            "lei": "LEI-XYZ",
            "stateOfIncorporation": "DE",
            "stateOfIncorporationDescription": "Delaware",
            "fiscalYearEnd": "0930",
            "category": "Large accelerated filer",
            "exchanges": ["NYSE"],
            "formerNames": [{"name": "OLD NAME INC", "from": "1994-01-01", "to": "2022-01-01"}],
            "insiderTransactionForIssuerExists": 1,
            "insiderTransactionForOwnerExists": 1,
        }
        out = parse_entity_profile(payload, instrument_id=42, cik="0000002969")
        assert out.instrument_id == 42
        assert out.sic == "2810"
        assert out.sic_description == "Industrial Inorganic Chemicals"
        assert out.exchanges == ["NYSE"]
        assert out.has_insider_issuer is True
        assert out.has_insider_owner is True
        assert len(out.former_names) == 1
        assert out.former_names[0]["name"] == "OLD NAME INC"

    def test_empty_strings_normalised_to_none(self) -> None:
        payload = {
            "description": "",
            "website": "",
            "ein": "   ",
        }
        out = parse_entity_profile(payload, instrument_id=1, cik="0000000001")
        assert out.description is None
        assert out.website is None
        assert out.ein is None

    def test_missing_fields_default_to_none_or_empty(self) -> None:
        out = parse_entity_profile({}, instrument_id=1, cik="0000000001")
        assert out.sic is None
        assert out.exchanges == []
        assert out.former_names == []
        assert out.has_insider_issuer is None

    def test_insider_flag_coerces_int_to_bool(self) -> None:
        zero = parse_entity_profile({"insiderTransactionForIssuerExists": 0}, instrument_id=1, cik="x")
        assert zero.has_insider_issuer is False

    def test_former_name_without_name_key_dropped(self) -> None:
        payload = {
            "formerNames": [
                {"from": "2020-01-01", "to": "2021-01-01"},  # missing name
                {"name": "VALID INC", "from": "2019-01-01", "to": "2020-01-01"},
            ]
        }
        out = parse_entity_profile(payload, instrument_id=1, cik="x")
        assert len(out.former_names) == 1
        assert out.former_names[0]["name"] == "VALID INC"

    def test_exchanges_non_list_yields_empty(self) -> None:
        out = parse_entity_profile({"exchanges": "NYSE"}, instrument_id=1, cik="x")
        assert out.exchanges == []


# ---------------------------------------------------------------------------
# Parse one real SEC submissions.json off disk
# ---------------------------------------------------------------------------


def _sample_submissions() -> dict:
    # Minimal faithful synthesis — real submissions.json is large and
    # live state would make the test flaky. Mirrors the audit sample at
    # data/raw/sec/sec_submissions_0000002969_20260417T002146Z.json.
    return {
        "cik": "0000002969",
        "sic": "2810",
        "sicDescription": "Industrial Inorganic Chemicals",
        "ownerOrg": "08 Industrial Applications and Services",
        "name": "Air Products & Chemicals, Inc.",
        "tickers": ["APD"],
        "exchanges": ["NYSE"],
        "ein": "231274455",
        "lei": None,
        "description": "",
        "website": "",
        "category": "Large accelerated filer",
        "fiscalYearEnd": "0930",
        "stateOfIncorporation": "DE",
        "stateOfIncorporationDescription": "DE",
        "formerNames": [
            {
                "name": "AIR PRODUCTS & CHEMICALS INC /DE/",
                "from": "1994-03-15T00:00:00.000Z",
                "to": "2022-04-07T00:00:00.000Z",
            }
        ],
        "insiderTransactionForIssuerExists": 1,
        "insiderTransactionForOwnerExists": 1,
    }


def test_parse_matches_real_sec_sample_shape() -> None:
    out = parse_entity_profile(_sample_submissions(), instrument_id=99, cik="0000002969")
    assert out.sic_description == "Industrial Inorganic Chemicals"
    assert out.exchanges == ["NYSE"]
    assert out.category == "Large accelerated filer"
    # Description comes back as None because source emits "" for APD.
    assert out.description is None
    assert out.has_insider_issuer is True
    assert out.former_names[0]["to"].startswith("2022-04-07")


# ---------------------------------------------------------------------------
# upsert + get — integration
# ---------------------------------------------------------------------------


pytestmark_int = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _test_db_available(),
        reason="ebull_test DB unavailable",
    ),
]

_NEXT_IID = [20_000]


def _seed_instrument(conn: psycopg.Connection[tuple], *, symbol: str) -> int:
    _NEXT_IID[0] += 1
    iid = _NEXT_IID[0]
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (%s, %s, %s, 'USD', TRUE)",
            (iid, symbol, f"{symbol} Inc."),
        )
    conn.commit()
    return iid


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestUpsertRoundTrip:
    def test_insert_then_read_returns_same_row(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="APD")
        profile = parse_entity_profile(_sample_submissions(), instrument_id=iid, cik="0000002969")
        upsert_entity_profile(ebull_test_conn, profile)
        ebull_test_conn.commit()

        got = get_entity_profile(ebull_test_conn, instrument_id=iid)
        assert got is not None
        assert got.sic == "2810"
        assert got.exchanges == ["NYSE"]
        assert got.has_insider_issuer is True
        assert len(got.former_names) == 1

    def test_upsert_overwrites_on_conflict(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="UPS")
        first = parse_entity_profile(
            {"sic": "1111", "sicDescription": "Old industry"},
            instrument_id=iid,
            cik="0000000001",
        )
        upsert_entity_profile(ebull_test_conn, first)
        ebull_test_conn.commit()

        second = parse_entity_profile(
            {"sic": "2222", "sicDescription": "New industry"},
            instrument_id=iid,
            cik="0000000001",
        )
        upsert_entity_profile(ebull_test_conn, second)
        ebull_test_conn.commit()

        got = get_entity_profile(ebull_test_conn, instrument_id=iid)
        assert got is not None
        assert got.sic == "2222"
        assert got.sic_description == "New industry"

    def test_get_returns_none_when_not_present(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # Seed instrument only; no profile upsert.
        iid = _seed_instrument(ebull_test_conn, symbol="NOPR")
        got = get_entity_profile(ebull_test_conn, instrument_id=iid)
        assert got is None
