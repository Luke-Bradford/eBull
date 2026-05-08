"""Tests for first-install drain (#871)."""

from __future__ import annotations

import json
from datetime import date

import psycopg
import pytest

from app.jobs.sec_first_install_drain import (
    run_first_install_drain,
    seed_manifest_from_filing_events,
)
from app.services.sec_manifest import get_manifest_row
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

pytestmark = pytest.mark.integration


_AAPL_RECENT = {
    "cik": "320193",
    "filings": {
        "recent": {
            "accessionNumber": ["0000320193-26-000001", "0000320193-26-000002"],
            "filingDate": ["2026-01-15", "2026-02-14"],
            "form": ["8-K", "DEF 14A"],
            "acceptanceDateTime": [
                "2026-01-15T16:30:00.000Z",
                "2026-02-14T08:00:00.000Z",
            ],
            "primaryDocument": ["item502.htm", "proxy.htm"],
        },
        "files": [],
    },
}


def _seed_aapl(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (1701, 'AAPL', 'Apple', '4', 'USD', TRUE)
        """
    )
    conn.execute("INSERT INTO instrument_sec_profile (instrument_id, cik) VALUES (1701, '0000320193')")
    conn.commit()


def _fake_get(payload: dict):
    body = json.dumps(payload).encode("utf-8")

    def _impl(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        return 200, body

    return _impl


class TestDrain:
    def test_drains_universe_in_order(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_aapl(ebull_test_conn)
        stats = run_first_install_drain(
            ebull_test_conn,
            http_get=_fake_get(_AAPL_RECENT),
            follow_pagination=False,
        )
        ebull_test_conn.commit()

        assert stats.ciks_processed == 1
        assert stats.manifest_rows_upserted == 2

        for accession in ("0000320193-26-000001", "0000320193-26-000002"):
            row = get_manifest_row(ebull_test_conn, accession)
            assert row is not None
            assert row.subject_type == "issuer"
            assert row.instrument_id == 1701

    def test_idempotent_on_rerun(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_aapl(ebull_test_conn)
        run_first_install_drain(
            ebull_test_conn,
            http_get=_fake_get(_AAPL_RECENT),
            follow_pagination=False,
        )
        ebull_test_conn.commit()
        run_first_install_drain(
            ebull_test_conn,
            http_get=_fake_get(_AAPL_RECENT),
            follow_pagination=False,
        )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM sec_filing_manifest")
            row = cur.fetchone()
            assert row is not None
            assert int(row[0]) == 2

    def test_bulk_zip_raises(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        with pytest.raises(NotImplementedError, match="bulk-zip drain not yet implemented"):
            run_first_install_drain(
                ebull_test_conn,
                http_get=_fake_get(_AAPL_RECENT),
                use_bulk_zip=True,
            )

    def test_drain_seeds_data_freshness_index(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # #937: drain MUST leave both manifest AND scheduler queryable
        # so the per-CIK poll (#870) finds work post-drain. Pre-fix
        # the scheduler stayed empty for the drained scope.
        _seed_aapl(ebull_test_conn)
        stats = run_first_install_drain(
            ebull_test_conn,
            http_get=_fake_get(_AAPL_RECENT),
            follow_pagination=False,
        )
        ebull_test_conn.commit()

        assert stats.manifest_rows_upserted == 2
        # AAPL recent has two distinct sources: sec_8k + sec_def14a.
        # Each (issuer, instrument_id, source) triple gets one
        # data_freshness_index row.
        assert stats.scheduler_rows_seeded >= 2

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT source FROM data_freshness_index
                WHERE subject_type = 'issuer' AND subject_id = '1701'
                ORDER BY source
                """
            )
            sources = [row[0] for row in cur.fetchall()]
        assert sources == ["sec_8k", "sec_def14a"]

    def test_max_subjects_caps(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_aapl(ebull_test_conn)
        # Add a second issuer
        ebull_test_conn.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
            VALUES (1702, 'X', 'X Inc', '4', 'USD', TRUE)
            """
        )
        ebull_test_conn.execute("INSERT INTO instrument_sec_profile (instrument_id, cik) VALUES (1702, '0000999999')")
        ebull_test_conn.commit()

        stats = run_first_install_drain(
            ebull_test_conn,
            http_get=_fake_get(_AAPL_RECENT),
            follow_pagination=False,
            max_subjects=1,
        )
        ebull_test_conn.commit()
        assert stats.ciks_processed == 1


class TestSeedFromFilingEvents:
    def test_seeds_manifest_from_filing_events_no_http(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # #1044: when filing_events has rows for the SEC provider,
        # the drain seeds sec_filing_manifest from that table without
        # any HTTP. The fake_get below would raise if called — proves
        # the fast path was taken.
        _seed_aapl(ebull_test_conn)
        ebull_test_conn.execute(
            """
            INSERT INTO external_identifiers
                (instrument_id, provider, identifier_type, identifier_value, is_primary)
            VALUES
                (1701, 'sec', 'cik', '0000320193', TRUE)
            ON CONFLICT DO NOTHING
            """
        )
        ebull_test_conn.execute(
            """
            INSERT INTO filing_events (
                instrument_id, filing_date, filing_type, provider,
                provider_filing_id, source_url, primary_document_url, raw_payload_json
            )
            VALUES
                (1701, %s, '8-K', 'sec', '0000320193-26-000001',
                 'https://www.sec.gov/...', 'https://www.sec.gov/.../item502.htm',
                 %s::jsonb),
                (1701, %s, 'DEF 14A', 'sec', '0000320193-26-000002',
                 'https://www.sec.gov/...', 'https://www.sec.gov/.../proxy.htm',
                 %s::jsonb)
            """,
            (
                date(2026, 1, 15),
                json.dumps({"provider_filing_id": "0000320193-26-000001"}),
                date(2026, 2, 14),
                json.dumps({"provider_filing_id": "0000320193-26-000002"}),
            ),
        )
        ebull_test_conn.commit()

        n = seed_manifest_from_filing_events(ebull_test_conn)
        ebull_test_conn.commit()

        assert n == 2
        for accession in ("0000320193-26-000001", "0000320193-26-000002"):
            row = get_manifest_row(ebull_test_conn, accession)
            assert row is not None
            assert row.subject_type == "issuer"
            assert row.instrument_id == 1701
            assert row.cik == "0000320193"

    def test_run_first_install_drain_uses_filing_events_fast_path(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # #1044: when filing_events seeds the issuer rows, the per-CIK
        # HTTP path is skipped for issuer subjects. Run with a fake
        # get that raises — proves no HTTP was issued for the issuer.
        _seed_aapl(ebull_test_conn)
        ebull_test_conn.execute(
            """
            INSERT INTO external_identifiers
                (instrument_id, provider, identifier_type, identifier_value, is_primary)
            VALUES
                (1701, 'sec', 'cik', '0000320193', TRUE)
            ON CONFLICT DO NOTHING
            """
        )
        ebull_test_conn.execute(
            """
            INSERT INTO filing_events (
                instrument_id, filing_date, filing_type, provider,
                provider_filing_id, source_url, primary_document_url, raw_payload_json
            )
            VALUES (1701, %s, '8-K', 'sec', '0000320193-26-000001',
                    'https://www.sec.gov/...', NULL, %s::jsonb)
            """,
            (
                date(2026, 1, 15),
                json.dumps({"provider_filing_id": "0000320193-26-000001"}),
            ),
        )
        ebull_test_conn.commit()

        def _raising_get(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
            raise AssertionError(f"HTTP fast-path bypass failed: {url}")

        stats = run_first_install_drain(
            ebull_test_conn,
            http_get=_raising_get,
            follow_pagination=False,
        )
        ebull_test_conn.commit()

        assert stats.rows_seeded_from_filing_events == 1
        # ciks_skipped picks up the issuer subject the loop short-circuited.
        assert stats.ciks_skipped >= 1
        assert stats.manifest_rows_upserted >= 1
