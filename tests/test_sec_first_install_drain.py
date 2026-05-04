"""Tests for first-install drain (#871)."""

from __future__ import annotations

import json

import psycopg
import pytest

from app.jobs.sec_first_install_drain import run_first_install_drain
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
