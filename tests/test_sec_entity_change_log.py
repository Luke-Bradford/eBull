"""Tests for SEC entity change-log + long-tail field capture (#463)."""

from __future__ import annotations

import psycopg
import pytest

from app.services.sec_entity_profile import (
    SecEntityProfile,
    _address,
    detect_profile_changes,
    get_entity_profile,
    parse_entity_profile,
    upsert_entity_profile,
)

# ---------------------------------------------------------------------------
# Pure extraction — new long-tail fields
# ---------------------------------------------------------------------------


class TestLongTailFieldExtraction:
    def test_phone_entity_type_flags_captured(self) -> None:
        payload = {
            "cik": "0000002098",
            "phone": "203-254-6060",
            "entityType": "operating",
            "flags": "",
        }
        profile = parse_entity_profile(payload, instrument_id=1, cik="0000002098")
        assert profile.phone == "203-254-6060"
        assert profile.entity_type == "operating"
        # Empty flags normalises to None (consistent with other string
        # fields).
        assert profile.flags is None

    def test_addresses_normalised_to_snake_case(self) -> None:
        payload = {
            "cik": "0000002098",
            "addresses": {
                "business": {
                    "street1": "1 WATERVIEW DRIVE",
                    "street2": None,
                    "city": "SHELTON",
                    "stateOrCountry": "CT",
                    "stateOrCountryDescription": "CT",
                    "zipCode": "06484",
                    "country": None,
                    "countryCode": None,
                    "isForeignLocation": 0,
                    "foreignStateTerritory": None,
                },
                "mailing": {
                    "street1": "1 WATERVIEW DRIVE",
                    "city": "SHELTON",
                    "stateOrCountry": "CT",
                    "stateOrCountryDescription": "CT",
                    "zipCode": "06484",
                },
            },
        }
        profile = parse_entity_profile(payload, instrument_id=1, cik="0000002098")
        assert profile.address_business is not None
        assert profile.address_business["street1"] == "1 WATERVIEW DRIVE"
        assert profile.address_business["city"] == "SHELTON"
        assert profile.address_business["state_or_country"] == "CT"
        assert profile.address_business["zip_code"] == "06484"
        assert profile.address_mailing is not None
        assert profile.address_mailing["city"] == "SHELTON"

    def test_empty_address_block_returns_none(self) -> None:
        assert _address({}) is None
        assert _address({"street1": "", "city": None}) is None
        assert _address(None) is None

    def test_missing_addresses_key_leaves_none(self) -> None:
        payload = {"cik": "0000002098"}
        profile = parse_entity_profile(payload, instrument_id=1, cik="0000002098")
        assert profile.address_business is None
        assert profile.address_mailing is None


# ---------------------------------------------------------------------------
# detect_profile_changes — pure diff logic
# ---------------------------------------------------------------------------


def _profile(**overrides: object) -> SecEntityProfile:
    base: dict[str, object] = {
        "instrument_id": 1,
        "cik": "0000001",
        "sic": "2810",
        "sic_description": "Chemicals",
        "owner_org": "08 Industrial",
        "description": None,
        "website": None,
        "investor_website": None,
        "ein": None,
        "lei": None,
        "state_of_incorporation": "DE",
        "state_of_incorporation_desc": "DE",
        "fiscal_year_end": "1231",
        "category": "Large accelerated filer",
        "exchanges": ["NYSE"],
        "former_names": [],
        "has_insider_issuer": True,
        "has_insider_owner": False,
        "phone": "555-0100",
        "entity_type": "operating",
        "flags": None,
        "address_business": {"street1": "1 Main St", "city": "Town"},
        "address_mailing": {"street1": "1 Main St", "city": "Town"},
    }
    base.update(overrides)
    return SecEntityProfile(**base)  # type: ignore[arg-type]


class TestDetectProfileChanges:
    def test_initial_ingest_emits_no_changes(self) -> None:
        assert detect_profile_changes(None, _profile()) == []

    def test_identical_profiles_no_changes(self) -> None:
        assert detect_profile_changes(_profile(), _profile()) == []

    def test_sic_change_surfaces(self) -> None:
        changes = detect_profile_changes(
            _profile(sic="2810"),
            _profile(sic="2820"),
        )
        names = {name for name, _, _ in changes}
        assert "sic" in names

    def test_address_change_detected_via_dict_diff(self) -> None:
        old = _profile(address_business={"street1": "1 Main St", "city": "Town"})
        new = _profile(address_business={"street1": "2 Oak Rd", "city": "Town"})
        changes = detect_profile_changes(old, new)
        names = {name for name, _, _ in changes}
        assert "address_business" in names

    def test_exchange_list_reorder_detected(self) -> None:
        """Order matters — SEC publishes a stable listing order so
        a reorder IS a change."""
        changes = detect_profile_changes(
            _profile(exchanges=["NYSE", "NASDAQ"]),
            _profile(exchanges=["NASDAQ", "NYSE"]),
        )
        names = {name for name, _, _ in changes}
        assert "exchanges" in names


# ---------------------------------------------------------------------------
# Integration — upsert writes change log rows
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestUpsertAppendsChangeLog:
    def _seed_instrument(self, conn: psycopg.Connection[tuple], iid: int = 901) -> int:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO instruments (instrument_id, symbol, company_name) "
                "VALUES (%s, %s, %s) RETURNING instrument_id",
                (iid, "APEX", "Apex Inc."),
            )
            row = cur.fetchone()
            assert row is not None
        conn.commit()
        return int(row[0])

    def test_first_ingest_writes_no_change_log_entries(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = self._seed_instrument(ebull_test_conn)
        upsert_entity_profile(ebull_test_conn, _profile(instrument_id=iid))
        ebull_test_conn.commit()
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM sec_entity_change_log WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 0

    def test_second_ingest_writes_one_row_per_changed_field(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = self._seed_instrument(ebull_test_conn, iid=902)
        # First ingest.
        upsert_entity_profile(ebull_test_conn, _profile(instrument_id=iid))
        # Second ingest with sic + website changed.
        upsert_entity_profile(
            ebull_test_conn,
            _profile(instrument_id=iid, sic="9999", website="https://new.example.com"),
        )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT field_name, prev_value, new_value "
                "FROM sec_entity_change_log WHERE instrument_id = %s "
                "ORDER BY field_name",
                (iid,),
            )
            rows = cur.fetchall()
        field_names = {r[0] for r in rows}
        assert "sic" in field_names
        assert "website" in field_names
        # No spurious entries for unchanged fields.
        assert "fiscal_year_end" not in field_names

    def test_reader_returns_new_long_tail_fields(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = self._seed_instrument(ebull_test_conn, iid=903)
        upsert_entity_profile(
            ebull_test_conn,
            _profile(
                instrument_id=iid,
                phone="555-7777",
                entity_type="investment",
                address_business={"street1": "42 Elm St", "city": "Springfield"},
            ),
        )
        ebull_test_conn.commit()
        stored = get_entity_profile(ebull_test_conn, instrument_id=iid)
        assert stored is not None
        assert stored.phone == "555-7777"
        assert stored.entity_type == "investment"
        assert stored.address_business is not None
        assert stored.address_business["street1"] == "42 Elm St"
