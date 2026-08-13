"""Tests for the bulk submissions.zip ingester (#1022)."""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import psycopg
import pytest

from app.providers.implementations.sec_edgar import KNOWN_FILING_AGENT_CIKS
from app.providers.implementations.sec_submissions import parse_submissions_page
from app.services.sec_manifest import _FILER_COHORT_FORMS, map_form_to_source
from app.services.sec_submissions_ingest import (
    SubmissionsIngestResult,
    _cik_from_filename,
    ingest_submissions_archive,
)
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]


# ---------------------------------------------------------------------------
# CIK filename parser — unit
# ---------------------------------------------------------------------------


class TestCikFromFilename:
    def test_valid_cik_filename(self) -> None:
        assert _cik_from_filename("CIK0000320193.json") == "0000320193"

    def test_unpadded_cik_rejected(self) -> None:
        assert _cik_from_filename("CIK320193.json") is None

    def test_non_cik_filename_rejected(self) -> None:
        assert _cik_from_filename("README.txt") is None
        assert _cik_from_filename("CIK0000320193-submissions-001.json") is None


# ---------------------------------------------------------------------------
# #1337 P1 — filer-cohort form filter (pure unit, no DB)
# ---------------------------------------------------------------------------


def _filer_payload() -> dict:
    """A filer's submissions payload mixing cohort + off-cohort forms.

    An institutional manager that ALSO filed a stray 10-K and a 13F-NT
    notice — only the 13F-HR forms should survive the cohort filter.
    """
    return {
        "cik": "0001000001",
        "name": "Test Asset Manager LLC",
        "filings": {
            "recent": {
                "accessionNumber": [
                    "0001000001-25-000001",  # 13F-HR  -> kept (institutional)
                    "0001000001-25-000002",  # 13F-HR/A -> kept
                    "0001000001-25-000003",  # 13F-NT  -> dropped (unmapped)
                    "0001000001-25-000004",  # 10-K    -> dropped (off-cohort)
                    "0001000001-25-000005",  # SC 13G  -> dropped (wrong cohort)
                ],
                "filingDate": ["2025-11-14", "2025-11-20", "2025-08-14", "2025-03-01", "2025-02-14"],
                "form": ["13F-HR", "13F-HR/A", "13F-NT", "10-K", "SC 13G"],
                "primaryDocument": ["a.htm", "b.htm", "c.htm", "d.htm", "e.htm"],
            },
            "files": [],
        },
    }


class TestFilerCohortForms:
    """``_FILER_COHORT_FORMS`` must stay in sync with ``_FORM_TO_SOURCE``
    — a form code that maps to no source would silently never match."""

    def test_every_cohort_form_maps_to_a_source(self) -> None:
        for cohort, forms in _FILER_COHORT_FORMS.items():
            for form in forms:
                assert map_form_to_source(form) is not None, (
                    f"{cohort} cohort form {form!r} maps to no source — it would never be written"
                )

    def test_institutional_forms_map_to_13f_hr(self) -> None:
        sources = {map_form_to_source(f) for f in _FILER_COHORT_FORMS["institutional_filer"]}
        assert sources == {"sec_13f_hr"}

    def test_blockholder_forms_map_to_13d_or_13g(self) -> None:
        sources = {map_form_to_source(f) for f in _FILER_COHORT_FORMS["blockholder_filer"]}
        assert sources == {"sec_13d", "sec_13g"}


class TestFilerFormFilterPredicate:
    """Exercises the exact gate ``_ingest_one_filer`` applies — a row is
    written iff its source is mapped AND its form is in the cohort set —
    against the canonical ``parse_submissions_page`` output. No DB."""

    def _kept_forms(self, cohort: str) -> set[str]:
        allowed = _FILER_COHORT_FORMS[cohort]  # type: ignore[index]
        rows, _ = parse_submissions_page(_filer_payload(), cik="0001000001")
        return {r.form.strip() for r in rows if r.source is not None and r.form.strip() in allowed}

    def test_institutional_keeps_only_13f_hr(self) -> None:
        assert self._kept_forms("institutional_filer") == {"13F-HR", "13F-HR/A"}

    def test_blockholder_keeps_only_13dg(self) -> None:
        # The sample payload only has SC 13G for the blockholder set.
        assert self._kept_forms("blockholder_filer") == {"SC 13G"}


# ---------------------------------------------------------------------------
# Archive fixture builder
# ---------------------------------------------------------------------------


def _build_archive(entries: dict[str, dict]) -> bytes:
    """Build an in-memory submissions.zip with the given CIK->payload map."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for cik, payload in entries.items():
            zf.writestr(f"CIK{cik}.json", json.dumps(payload))
    return buf.getvalue()


def _aapl_payload() -> dict:
    return {
        "cik": "320193",
        "name": "Apple Inc.",
        "sic": "3571",
        "sicDescription": "Electronic Computers",
        "exchanges": ["Nasdaq"],
        "category": "Large accelerated filer",
        "fiscalYearEnd": "0930",
        "stateOfIncorporation": "CA",
        "stateOfIncorporationDescription": "California",
        "filings": {
            "recent": {
                "accessionNumber": ["0000320193-25-000001", "0000320193-25-000002"],
                "filingDate": ["2025-11-01", "2025-08-01"],
                "form": ["10-K", "10-Q"],
                "primaryDocument": ["aapl-10-k.htm", "aapl-10-q.htm"],
                "reportDate": ["2025-09-30", "2025-06-30"],
            },
            "files": [],
        },
    }


def _msft_payload() -> dict:
    return {
        "cik": "789019",
        "name": "Microsoft Corp",
        "sic": "7372",
        "sicDescription": "Prepackaged Software",
        "exchanges": ["Nasdaq"],
        "category": "Large accelerated filer",
        "filings": {
            "recent": {
                "accessionNumber": ["0000789019-25-000010"],
                "filingDate": ["2025-07-30"],
                "form": ["10-K"],
                "primaryDocument": ["msft-10-k.htm"],
                "reportDate": ["2025-06-30"],
            },
            "files": [],
        },
    }


# ---------------------------------------------------------------------------
# DB integration
# ---------------------------------------------------------------------------


_NEXT_IID: list[int] = [10000]


def _seed_universe(
    conn: psycopg.Connection[tuple],
    *,
    symbol: str,
    cik_padded: str,
) -> int:
    _NEXT_IID[0] += 1
    iid = _NEXT_IID[0]
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (%s, %s, %s, 'USD', TRUE)",
            (iid, symbol, f"{symbol} Inc."),
        )
        cur.execute(
            "INSERT INTO external_identifiers "
            "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (%s, 'sec', 'cik', %s, TRUE)",
            (iid, cik_padded),
        )
    conn.commit()
    return iid


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestIngestSubmissionsArchive:
    def test_universe_match_writes_filings_and_profile(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # Seed two universe instruments + one out-of-universe CIK.
        iid_aapl = _seed_universe(ebull_test_conn, symbol="AAPL", cik_padded="0000320193")
        iid_msft = _seed_universe(ebull_test_conn, symbol="MSFT", cik_padded="0000789019")

        archive_bytes = _build_archive(
            {
                "0000320193": _aapl_payload(),
                "0000789019": _msft_payload(),
                "9999999999": {
                    "cik": "9999999999",
                    "name": "Out of Universe",
                    "filings": {"recent": {}, "files": []},
                },
            }
        )
        archive_path = tmp_path / "submissions.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_submissions_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
        )
        ebull_test_conn.commit()

        assert isinstance(result, SubmissionsIngestResult)
        assert result.archive_entries_seen == 3
        assert result.instruments_matched == 2
        assert result.archive_entries_skipped == 1  # the out-of-universe CIK
        assert result.parse_errors == 0
        # 2 AAPL filings + 1 MSFT filing = 3 upserted.
        assert result.filings_upserted == 3
        assert result.profiles_upserted == 2

        # Verify the rows actually landed.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM filing_events WHERE instrument_id = %s AND provider = 'sec'",
                (iid_aapl,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 2

            cur.execute(
                "SELECT sic, sic_description FROM instrument_sec_profile WHERE instrument_id = %s",
                (iid_aapl,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "3571"
            assert row[1] == "Electronic Computers"

            # Codex review BLOCKING for PR #1030: ``raw_payload_json``
            # must carry the canonical ticker symbol (e.g. "AAPL"),
            # NOT a stringified instrument_id.
            cur.execute(
                "SELECT raw_payload_json->>'symbol' FROM filing_events "
                "WHERE instrument_id = %s ORDER BY filing_date DESC LIMIT 1",
                (iid_aapl,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "AAPL", f"expected ticker AAPL, got {row[0]!r}"

            # MSFT also landed.
            cur.execute(
                "SELECT COUNT(*) FROM filing_events WHERE instrument_id = %s AND provider = 'sec'",
                (iid_msft,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 1

    def test_corrupted_entry_increments_parse_errors_not_raise(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        iid = _seed_universe(ebull_test_conn, symbol="AAPL", cik_padded="0000320193")
        del iid

        # One bad JSON entry plus one good one.
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("CIK0000320193.json", "not valid json {")
        archive_path = tmp_path / "submissions.zip"
        archive_path.write_bytes(buf.getvalue())

        result = ingest_submissions_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
        )
        ebull_test_conn.commit()

        assert result.parse_errors == 1
        assert result.filings_upserted == 0

    def test_share_class_siblings_both_receive_filings_and_profile(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        """#1117 — GOOG + GOOGL co-bind one CIK; both must receive
        filings + entity profile.
        """
        iid_goog = _seed_universe(ebull_test_conn, symbol="GOOG", cik_padded="0001652044")
        iid_googl = _seed_universe(ebull_test_conn, symbol="GOOGL", cik_padded="0001652044")

        # Reuse AAPL submissions payload shape — _normalise_submissions_block
        # is content-agnostic; we care that the same archive entry
        # produces filings rows for both siblings.
        archive_bytes = _build_archive({"0001652044": _aapl_payload()})
        archive_path = tmp_path / "submissions.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_submissions_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
        )
        ebull_test_conn.commit()

        assert result.archive_entries_seen == 1
        assert result.instruments_matched == 2
        assert result.parse_errors == 0
        # 2 filings × 2 siblings = 4 filings; 1 profile × 2 siblings = 2.
        assert result.filings_upserted == 4
        assert result.profiles_upserted == 2

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM filing_events WHERE instrument_id = %s AND provider = 'sec'",
                (iid_goog,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 2, f"GOOG expected 2 filings, got {row[0]}"

            cur.execute(
                "SELECT COUNT(*) FROM filing_events WHERE instrument_id = %s AND provider = 'sec'",
                (iid_googl,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 2, f"GOOGL expected 2 filings, got {row[0]}"

            # Each sibling carries its own ticker on the filing_events
            # row — Codex review BLOCKING for PR #1030 (the canonical
            # symbol must NOT be a stringified instrument_id, and must
            # NOT cross-contaminate between siblings).
            cur.execute(
                "SELECT raw_payload_json->>'symbol' FROM filing_events WHERE instrument_id = %s LIMIT 1",
                (iid_goog,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "GOOG", f"GOOG row carrying wrong symbol {row[0]!r}"

            cur.execute(
                "SELECT raw_payload_json->>'symbol' FROM filing_events WHERE instrument_id = %s LIMIT 1",
                (iid_googl,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "GOOGL", f"GOOGL row carrying wrong symbol {row[0]!r}"

            cur.execute(
                "SELECT COUNT(*) FROM instrument_sec_profile WHERE instrument_id IN (%s, %s)",
                (iid_goog, iid_googl),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 2, f"expected 2 profiles, got {row[0]}"


# ---------------------------------------------------------------------------
# #1337 P1 — filer-cohort manifest seeding (DB integration)
# ---------------------------------------------------------------------------


def _seed_institutional_filer(conn: psycopg.Connection[tuple], *, cik_padded: str, name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO institutional_filers (cik, name) VALUES (%s, %s) ON CONFLICT (cik) DO NOTHING",
            (cik_padded, name),
        )
    conn.commit()


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestFilerCohortManifestSeeding:
    def test_institutional_filer_seeds_manifest_not_filing_events(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        cik = "0001000001"
        _seed_institutional_filer(ebull_test_conn, cik_padded=cik, name="Test Asset Manager LLC")

        archive_path = tmp_path / "submissions.zip"
        archive_path.write_bytes(_build_archive({cik: _filer_payload()}))

        result = ingest_submissions_archive(conn=ebull_test_conn, archive_path=archive_path)
        ebull_test_conn.commit()

        # Two cohort forms (13F-HR + 13F-HR/A) written; the 13F-NT / 10-K /
        # SC 13G rows are dropped by the cohort form-filter.
        assert result.filer_manifest_rows_upserted == 2
        assert result.instruments_matched == 0  # not a universe instrument
        assert result.filings_upserted == 0  # filer path never touches filing_events

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT form, subject_type, subject_id, instrument_id "
                "FROM sec_filing_manifest WHERE cik = %s ORDER BY form",
                (cik,),
            )
            rows = cur.fetchall()
            assert {r[0] for r in rows} == {"13F-HR", "13F-HR/A"}
            for _form, subject_type, subject_id, instrument_id in rows:
                assert subject_type == "institutional_filer"
                assert subject_id == cik
                assert instrument_id is None

            # No issuer-only sidecar rows for a pure-filer CIK (the
            # filing_events absence is already proven by filings_upserted == 0,
            # since filing_events is keyed by instrument_id which a pure
            # filer lacks).
            cur.execute(
                "SELECT COUNT(*) FROM sec_cik_submissions_files_index WHERE cik = %s",
                (cik,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 0, "pure-filer CIK must not seed the issuer-only sidecar"

    def test_agent_cik_in_filer_cohort_is_skipped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        agent_cik = next(iter(KNOWN_FILING_AGENT_CIKS))
        _seed_institutional_filer(ebull_test_conn, cik_padded=agent_cik, name="Filing Agent Co")

        payload = _filer_payload()
        payload["cik"] = agent_cik
        archive_path = tmp_path / "submissions.zip"
        archive_path.write_bytes(_build_archive({agent_cik: payload}))

        result = ingest_submissions_archive(conn=ebull_test_conn, archive_path=archive_path)
        ebull_test_conn.commit()

        assert result.filer_manifest_rows_upserted == 0, "agent CIKs must not seed filer manifest rows"
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM sec_filing_manifest WHERE cik = %s", (agent_cik,))
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 0

    def test_dual_role_cik_writes_issuer_filings_and_filer_manifest(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        """A CIK that is BOTH a tradable issuer AND a 13F filer: the
        issuer path writes filing_events + sidecar (for the 10-K), the
        filer path writes a 13F-HR manifest row. The multimap exposes
        both roles on the one CIK (#1337 P1 risk §10.1 + §10.5)."""
        cik = "0001000001"
        iid = _seed_universe(ebull_test_conn, symbol="DUAL1", cik_padded=cik)
        _seed_institutional_filer(ebull_test_conn, cik_padded=cik, name="Self-Managing Manager")

        archive_path = tmp_path / "submissions.zip"
        archive_path.write_bytes(_build_archive({cik: _filer_payload()}))

        result = ingest_submissions_archive(conn=ebull_test_conn, archive_path=archive_path)
        ebull_test_conn.commit()

        assert result.instruments_matched == 1  # the issuer role
        assert result.filer_manifest_rows_upserted == 2  # 13F-HR + 13F-HR/A

        with ebull_test_conn.cursor() as cur:
            # Issuer path wrote the 10-K to filing_events.
            cur.execute(
                "SELECT COUNT(*) FROM filing_events WHERE instrument_id = %s AND provider = 'sec'",
                (iid,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] >= 1, "issuer path must write the 10-K to filing_events"

            # Issuer path wrote a sidecar row (dual-role CIK still seeds it).
            cur.execute("SELECT COUNT(*) FROM sec_cik_submissions_files_index WHERE cik = %s", (cik,))
            row = cur.fetchone()
            assert row is not None
            assert row[0] >= 1, "dual-role CIK must still seed the issuer sidecar"

            # Filer path wrote the 13F-HR manifest rows (instrument_id NULL).
            cur.execute(
                "SELECT COUNT(*) FROM sec_filing_manifest "
                "WHERE cik = %s AND subject_type = 'institutional_filer' AND instrument_id IS NULL",
                (cik,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 2
