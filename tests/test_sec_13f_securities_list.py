"""Tests for the SEC Official List CUSIP universe backfill (#914).

Pins the contract:

  * Parser anchors on the leading 9-char CUSIP + trailing single-
    letter status code, splits the middle on 2+-space gaps to
    recover issuer_name + description.
  * Backfill walks unmapped tradable instruments, fuzzy-matches
    each company_name against pre-normalised Official-List entries
    (bucketed by first-token), INSERTs confident matches into
    ``external_identifiers``.
  * Idempotent — re-running on a populated install is a cheap
    read with zero writes.
  * Conflict (CUSIP already mapped to a different instrument) is
    surfaced as ``tombstoned_conflict`` and never silently
    overwrites the existing mapping.
  * Ambiguous (multiple distinct CUSIPs tie at the top score) is
    counted as ``tombstoned_ambiguous`` and skips the INSERT.
"""

from __future__ import annotations

from datetime import date

import psycopg
import psycopg.rows
import pytest

from app.services.sec_13f_securities_list import (
    _bucket_by_first_token,
    backfill_cusip_coverage,
    parse_13f_list,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export


def _line(
    cusip: str,
    name: str,
    description: str,
    status: str = "E",
    *,
    added: bool = False,
    per_row_flag: str = "",
) -> str:
    """Build one fixed-width SEC Official List row.

    Layout: cols 0-8 CUSIP, col 9 leading flag (' '/'*'),
    cols 10-39 issuer name (30 wide), cols 40-67 description
    (28 wide), cols 68+ trailing flag area + status.
    ``per_row_flag`` ('A'/'D' or '') populates the ``*A*``/``*D*``
    column.
    """
    leading = "*" if added else " "
    flag_field = f"*{per_row_flag}*" if per_row_flag else "   "
    return f"{cusip}{leading}{name:<30}{description:<28}{flag_field:<10}{status}\n"


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


class TestParse13fList:
    def test_parses_basic_us_equity_row(self) -> None:
        payload = _line("037833100", "APPLE INC", "COM")
        rows = list(parse_13f_list(payload))
        assert len(rows) == 1
        assert rows[0].cusip == "037833100"
        assert rows[0].issuer_name == "APPLE INC"
        assert rows[0].description == "COM"
        assert rows[0].status == "E"
        assert rows[0].is_added_since_last is False

    def test_added_flag_marks_new_securities(self) -> None:
        payload = _line("888889999", "NEW IPO CORP", "COM", status="N", added=True)
        row = next(parse_13f_list(payload))
        assert row.is_added_since_last is True
        assert row.status == "N"

    def test_cins_alpha_prefix_accepted(self) -> None:
        """CINS (foreign issuer ID) shares CUSIP shape but starts
        with an alpha char. Both flow into external_identifiers."""
        payload = _line("G0084W101", "ADIENT PLC", "ORD SHS")
        row = next(parse_13f_list(payload))
        assert row.cusip == "G0084W101"
        assert row.issuer_name == "ADIENT PLC"

    def test_skips_lines_without_valid_cusip(self) -> None:
        payload = (
            "garbage line no cusip here\n"
            + _line("037833100", "APPLE INC", "COM")
            + "another garbage row\n"
            + _line("594918104", "MICROSOFT CORP", "COM")
        )
        rows = list(parse_13f_list(payload))
        assert [r.cusip for r in rows] == ["037833100", "594918104"]

    def test_per_row_flag_A_marks_added(self) -> None:
        """Per-row ``*A*`` flag is the authoritative add/delete signal
        for this quarter's diff. Codex pre-push review #914."""
        line = _line("G0R78B106", "BAIN CAP GSS INVT CORP", "ORD CL A", status="N", per_row_flag="A")
        row = next(parse_13f_list(line))
        assert row.issuer_name == "BAIN CAP GSS INVT CORP"
        assert row.description == "ORD CL A"
        assert row.status == "N"
        assert row.is_added_since_last is True

    def test_per_row_flag_D_marks_deleted(self) -> None:
        """``*D*`` rows are returned with status='D' so the caller
        can decide whether to map them. The backfill skips D rows.
        Codex pre-push review #914."""
        line = _line("G00350101", "LOBO EV TECHNOLOGIES LTD", "SHS", status="E", per_row_flag="D")
        row = next(parse_13f_list(line))
        assert row.status == "D"

    def test_long_issuer_filling_field_keeps_description_clean(self) -> None:
        """An issuer that fills its 30-char column must not bleed
        into the description on a 1-space gap. Column-slicing
        catches this. Codex pre-push review #914."""
        # Issuer exactly 30 chars; description starts immediately.
        long_name = "AAA" + " " * 0 + ("X" * 27)  # 30 chars
        line = f"037833100*{long_name}{'COMMON STK':<28}   {'   ':<7}E\n"
        row = next(parse_13f_list(line))
        assert row.issuer_name == long_name.strip()
        assert row.description == "COMMON STK"

    def test_short_lines_skipped(self) -> None:
        assert list(parse_13f_list("\n  \n037833100\n")) == []


# ---------------------------------------------------------------------------
# Bucketing
# ---------------------------------------------------------------------------


class TestBucketing:
    def test_bucket_by_first_token_groups_normalized_first_word(self) -> None:
        from app.services.sec_13f_securities_list import ThirteenFSecurity

        secs = [
            ThirteenFSecurity(
                cusip="037833100", issuer_name="APPLE INC", description="COM", is_added_since_last=False, status="E"
            ),
            ThirteenFSecurity(
                cusip="037833200",
                issuer_name="APPLE HOSPITALITY REIT INC",
                description="COM",
                is_added_since_last=False,
                status="E",
            ),
            ThirteenFSecurity(
                cusip="594918104",
                issuer_name="MICROSOFT CORP",
                description="COM",
                is_added_since_last=False,
                status="E",
            ),
        ]
        buckets = _bucket_by_first_token(secs)
        assert set(buckets.keys()) == {"APPLE", "MICROSOFT"}
        assert len(buckets["APPLE"]) == 2
        assert len(buckets["MICROSOFT"]) == 1


# ---------------------------------------------------------------------------
# End-to-end backfill against ebull_test
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.integration


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    symbol: str,
    company_name: str,
) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, company_name),
    )


class TestBackfillCusipCoverage:
    def test_inserts_external_identifier_for_confident_match(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=914_001, symbol="AAPL", company_name="Apple Inc.")
        _seed_instrument(conn, iid=914_002, symbol="MSFT", company_name="Microsoft Corp")
        conn.commit()

        payload = _line("037833100", "APPLE INC", "COM") + _line("594918104", "MICROSOFT CORP", "COM")

        result = backfill_cusip_coverage(
            conn,
            year=2025,
            quarter=4,
            today=date(2026, 5, 5),
            fetch=lambda *_: payload,
        )

        assert result.list_rows == 2
        assert result.instruments_seen >= 2
        assert result.inserted == 2
        assert result.tombstoned_unresolvable == 0

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT i.symbol, ei.identifier_value
                FROM instruments i
                JOIN external_identifiers ei
                  ON ei.instrument_id = i.instrument_id
                 AND ei.provider = 'sec'
                 AND ei.identifier_type = 'cusip'
                WHERE i.instrument_id IN (914001, 914002)
                ORDER BY i.symbol
                """
            )
            rows = cur.fetchall()
        assert rows == [
            {"symbol": "AAPL", "identifier_value": "037833100"},
            {"symbol": "MSFT", "identifier_value": "594918104"},
        ]

    def test_idempotent_rerun_makes_no_new_inserts(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=914_010, symbol="AAPL", company_name="Apple Inc.")
        conn.commit()
        payload = _line("037833100", "APPLE INC", "COM")

        first = backfill_cusip_coverage(conn, year=2025, quarter=4, today=date(2026, 5, 5), fetch=lambda *_: payload)
        second = backfill_cusip_coverage(conn, year=2025, quarter=4, today=date(2026, 5, 5), fetch=lambda *_: payload)

        assert first.inserted == 1
        # Second run: the instrument is now mapped, so it's not in
        # the unmapped-instruments SELECT — instruments_seen drops.
        assert second.inserted == 0

    def test_conflict_when_cusip_already_mapped_to_different_instrument(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A pre-existing curated mapping must NEVER be silently
        overwritten. The conflicting CUSIP is counted as
        ``tombstoned_conflict`` and the existing mapping is preserved."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=914_020, symbol="AAPL", company_name="Apple Inc.")
        _seed_instrument(conn, iid=914_021, symbol="AAPL_DUP", company_name="Apple Hospitality REIT")
        # Pre-existing mapping: 037833100 -> 914_020 (the curated
        # AAPL mapping).
        conn.execute(
            "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (914020, 'sec', 'cusip', '037833100', TRUE)"
        )
        conn.commit()

        # Synthesise a backfill that matches Apple Hospitality REIT
        # to the same CUSIP (engineered ambiguity for test clarity).
        payload = _line("037833100", "APPLE HOSPITALITY REIT", "COM")
        result = backfill_cusip_coverage(conn, year=2025, quarter=4, today=date(2026, 5, 5), fetch=lambda *_: payload)

        assert result.tombstoned_conflict == 1
        assert result.inserted == 0

        # Existing mapping preserved — 037833100 still on instrument 914_020.
        with conn.cursor() as cur:
            cur.execute("SELECT instrument_id FROM external_identifiers WHERE identifier_value = '037833100'")
            rows = cur.fetchall()
        assert rows == [(914020,)]

    def test_unresolvable_when_no_candidate_meets_threshold(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=914_030, symbol="WIDGET", company_name="Acme Widget Manufacturing Co")
        conn.commit()
        payload = _line("999999999", "ENTIRELY DIFFERENT NAME LLC", "COM")
        result = backfill_cusip_coverage(conn, year=2025, quarter=4, today=date(2026, 5, 5), fetch=lambda *_: payload)
        assert result.inserted == 0
        assert result.tombstoned_unresolvable >= 1

    def test_ambiguous_when_two_distinct_cusips_tie_at_top(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Two distinct CUSIPs whose normalised issuer-names tie at
        the top similarity score → refuse to pick arbitrarily."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=914_040, symbol="ALPHA", company_name="Alphabet")
        conn.commit()
        # Two distinct CUSIPs, both normalise to "ALPHABET" after
        # share-class strip.
        payload = _line("02079K305", "ALPHABET INC", "CL A") + _line("02079K107", "ALPHABET INC", "CL C")
        result = backfill_cusip_coverage(conn, year=2025, quarter=4, today=date(2026, 5, 5), fetch=lambda *_: payload)
        assert result.tombstoned_ambiguous == 1
        assert result.inserted == 0

    def test_skips_deleted_securities(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A row marked ``*D*`` (deleted from the list this quarter)
        must not anchor a new mapping. Codex pre-push review #914."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=914_060, symbol="DEL", company_name="Deleted Issuer Corp")
        conn.commit()
        payload = _line("999999999", "DELETED ISSUER CORP", "COM", status="E", per_row_flag="D")
        result = backfill_cusip_coverage(conn, year=2025, quarter=4, today=date(2026, 5, 5), fetch=lambda *_: payload)
        # 'D' status is a deleted row; backfill skips it as
        # unresolvable to avoid mapping to a stale CUSIP.
        assert result.inserted == 0
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM external_identifiers WHERE instrument_id = 914060 "
                "AND provider = 'sec' AND identifier_type = 'cusip'"
            )
            assert cur.fetchone() is None

    def test_stale_snapshot_guard_does_not_double_map_instrument(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Race scenario: ``_select_unmapped_instruments`` runs at
        T0; another writer maps the instrument at T1; the backfill
        loop iterates to that instrument at T2. Without the
        stale-snapshot guard the loop would INSERT a second SEC
        CUSIP for the same instrument. Codex pre-push review #914."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=914_070, symbol="SNAP", company_name="Snap Inc.")
        conn.commit()

        # Simulate the stale-snapshot race by inserting an external_identifier
        # AFTER taking the unmapped-instruments snapshot. We can't easily
        # interleave the SELECT with the loop, so instead we pre-map then
        # call the backfill — the SELECT skips already-mapped instruments,
        # but if it ever got back to the inner _insert_external_identifier
        # for an already-mapped instrument the guard would catch it.
        # Direct test of the guard:
        from app.services.sec_13f_securities_list import _insert_external_identifier

        conn.execute(
            "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (914070, 'sec', 'cusip', '111111111', TRUE)"
        )
        conn.commit()

        # Try to insert a different CUSIP for the same instrument.
        # The guard must catch this and return 'already_mapped'.
        outcome = _insert_external_identifier(conn, instrument_id=914_070, cusip="222222222")
        assert outcome == "already_mapped"

        # Confirm the second CUSIP was NOT inserted.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT identifier_value FROM external_identifiers "
                "WHERE instrument_id = 914070 AND provider = 'sec' AND identifier_type = 'cusip'"
            )
            rows = cur.fetchall()
        assert rows == [("111111111",)]

    def test_raw_payload_persisted_before_parse(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """eBull non-negotiable: raw API payloads persisted before
        normalisation. Claude review BLOCKING #914."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=914_080, symbol="AAPL", company_name="Apple Inc.")
        conn.commit()
        payload = _line("037833100", "APPLE INC", "COM")

        backfill_cusip_coverage(conn, year=2025, quarter=4, today=date(2026, 5, 5), fetch=lambda *_: payload)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT period_year, period_quarter, payload, source_url "
                "FROM sec_reference_documents "
                "WHERE document_kind = '13f_securities_list'"
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 2025
        assert rows[0][1] == 4
        assert rows[0][2] == payload
        assert "13flist2025q4.txt" in rows[0][3]

    def test_raw_payload_upsert_idempotent(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Re-fetching the same quarter overwrites the body. The
        Official List is mutable across quarters; the latest fetch
        is authoritative."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=914_090, symbol="AAPL", company_name="Apple Inc.")
        conn.commit()
        first = _line("037833100", "APPLE INC", "COM")
        second = _line("037833100", "APPLE INC", "COM NEW")

        backfill_cusip_coverage(conn, year=2025, quarter=4, today=date(2026, 5, 5), fetch=lambda *_: first)
        backfill_cusip_coverage(conn, year=2025, quarter=4, today=date(2026, 5, 5), fetch=lambda *_: second)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload FROM sec_reference_documents "
                "WHERE document_kind = '13f_securities_list' AND period_year = 2025 AND period_quarter = 4"
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == second

    def test_list_rows_reports_raw_count_not_post_filter(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """``list_rows`` is the post-fetch raw count; the operator
        log line "X rows from the Official List" matches this. Claude
        review WARNING #914."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=914_100, symbol="AAPL", company_name="Apple Inc.")
        conn.commit()
        payload = _line("037833100", "APPLE INC", "COM") + _line("999999999", "DELETED CORP", "COM", per_row_flag="D")
        result = backfill_cusip_coverage(conn, year=2025, quarter=4, today=date(2026, 5, 5), fetch=lambda *_: payload)
        # 2 raw rows, 1 after deleted-status filter; list_rows is RAW.
        assert result.list_rows == 2

    def test_default_quarter_is_last_completed(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Caller-less invocation walks the most recent CLOSED quarter
        relative to ``today``."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=914_050, symbol="AAPL", company_name="Apple Inc.")
        conn.commit()
        captured: dict[str, tuple[int, int]] = {}

        def _fake(year: int, quarter: int) -> str:
            captured["q"] = (year, quarter)
            return _line("037833100", "APPLE INC", "COM")

        backfill_cusip_coverage(conn, today=date(2026, 5, 5), fetch=_fake)
        # 2026-05-05 → most recent closed quarter is 2026 Q1.
        assert captured["q"] == (2026, 1)


# ---------------------------------------------------------------------------
# CUSIP matcher COM/CALL/PUT triplet handling (#1054)
# ---------------------------------------------------------------------------


class TestBestMatchCommonSharePreference:
    """Pin the matcher's behaviour against the real-archive collision
    pattern: SEC 13F Official List ALWAYS lists each issuer 3 times
    (COM + CALL + PUT) sharing the same issuer name. Naive ambiguity
    detection collapses every equity issuer."""

    @staticmethod
    def _bucket(rows: list[tuple[str, str, str]]) -> list:
        from app.services.sec_13f_securities_list import ThirteenFSecurity

        return [
            (
                "APPLE",
                ThirteenFSecurity(
                    cusip=cusip, issuer_name=name, description=desc, is_added_since_last=False, status="E"
                ),
            )
            for cusip, name, desc in rows
        ]

    def test_com_call_put_collapses_to_com(self) -> None:
        from app.services.cusip_resolver import MATCH_THRESHOLD
        from app.services.sec_13f_securities_list import _best_match

        bucket = self._bucket(
            [
                ("037833100", "APPLE INC", "COM"),
                ("037833900", "APPLE INC", "CALL"),
                ("037833950", "APPLE INC", "PUT"),
            ]
        )
        best, ambig = _best_match("APPLE", bucket, threshold=MATCH_THRESHOLD)
        assert best is not None
        assert best.cusip == "037833100"
        assert ambig is False

    def test_unit_call_put_does_not_collapse_to_unit(self) -> None:
        # SPAC unit CUSIPs are distinct from common-stock CUSIPs;
        # without a COM row the matcher must NOT pick the UNIT row
        # silently. Codex pre-push MEDIUM for #1054.
        from app.services.cusip_resolver import MATCH_THRESHOLD
        from app.services.sec_13f_securities_list import _best_match

        bucket = self._bucket(
            [
                ("UNIT12345", "APPLE INC", "UNIT"),
                ("OPTC12345", "APPLE INC", "CALL"),
                ("OPTP12345", "APPLE INC", "PUT"),
            ]
        )
        best, ambig = _best_match("APPLE", bucket, threshold=MATCH_THRESHOLD)
        # No common-share row → no collapse; ambig stays True since
        # UNIT vs CALL/PUT remain distinct CUSIPs.
        assert ambig is True

    def test_distinct_issuers_at_top_score_stay_ambiguous(self) -> None:
        # True share-class collision: distinct issuers normalising to
        # same first-token name. Must remain ambiguous.
        from app.services.cusip_resolver import MATCH_THRESHOLD
        from app.services.sec_13f_securities_list import ThirteenFSecurity, _best_match

        bucket = [
            (
                "APPLE",
                ThirteenFSecurity(
                    cusip="037833100",
                    issuer_name="APPLE INC",
                    description="COM",
                    is_added_since_last=False,
                    status="E",
                ),
            ),
            (
                "APPLE",
                ThirteenFSecurity(
                    cusip="03784Y200",
                    issuer_name="APPLE HOSPITALITY REIT INC",
                    description="COM NEW",
                    is_added_since_last=False,
                    status="E",
                ),
            ),
        ]
        # With score-1.0 to "APPLE" the COM/COM-NEW tie is real.
        best, ambig = _best_match("APPLE", bucket, threshold=MATCH_THRESHOLD)
        assert ambig is True


class TestParseDateNPORT:
    def test_dd_mmm_yyyy_format_parses(self) -> None:
        # Regression for #1054: real N-PORT archive emits
        # `25-FEB-2026` for FILING_DATE and `31-DEC-2025` for
        # REPORT_ENDING_PERIOD. ISO-only parser silently dropped
        # every row.
        from app.services.sec_nport_dataset_ingest import _parse_filing_date, _parse_iso_date

        d = _parse_iso_date("31-DEC-2025")
        assert d is not None
        assert d.isoformat() == "2025-12-31"

        ts = _parse_filing_date("25-FEB-2026")
        assert ts is not None
        assert ts.date().isoformat() == "2026-02-25"
