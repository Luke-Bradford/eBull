"""#3227 13G covers — the filer-of-record read against a real DB.

Spec: docs/specs/ownership/2026-10-22-3227-13f-13g-covers.md (Acceptance, Tests, Read). Auto-marked
``db`` (pulls ``ebull_test_conn``). ``_read_13g_filers`` returns only the current ``13g`` observations
of the requested accessions on this instrument.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import psycopg

from app.services.ownership_rollup import _CoverRow, _read_13g_filers

_IID, _OTHER_IID = 3_227_613, 3_227_614
_FILED = datetime(2026, 5, 15, tzinfo=UTC)
# Accessions must match the observations table's accession-format check (sql/134).
_G, _G2, _H = "0003227613-26-000001", "0003227613-26-000002", "0003227613-26-000003"


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, country, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', 'US', TRUE)
        """,
        (iid, f"T{iid}", f"T{iid} co"),
    )


def _observe(
    conn: psycopg.Connection[tuple], iid: int, accession: str, cik: str, *, source: str = "13g", closed: bool = False
) -> None:
    conn.execute(
        """
        INSERT INTO ownership_blockholders_observations (
            instrument_id, reporter_cik, reporter_name, ownership_nature, submission_type, status_flag,
            source, source_document_id, source_accession, source_url, filed_at, period_end,
            ingest_run_id, aggregate_amount_owned, known_to
        ) VALUES (
            %s, %s, 'FILER', 'beneficial', 'SCHEDULE 13G/A', 'passive',
            %s, %s, %s, NULL, %s, %s, gen_random_uuid(), 1, CASE WHEN %s THEN now() END
        )
        """,
        (iid, cik, source, f"{accession}:{cik}", accession, _FILED, _FILED.date(), closed),
    )


def _block(accession: str) -> _CoverRow:
    return _CoverRow(None, accession, _FILED, Decimal(0), Decimal(1), Decimal(1), "X", "SCHEDULE 13G/A")


def test_the_read_keeps_current_13g_observations_of_this_instrument_and_accession(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, _IID)
    _seed_instrument(conn, _OTHER_IID)
    _observe(conn, _IID, _G, "0001527166")
    _observe(conn, _IID, _G, "0000000999", closed=True)  # closed: may carry a pre-#1638 issuer CIK
    _observe(conn, _IID, _G, "0000000888", source="13d")
    _observe(conn, _OTHER_IID, _G, "0000000777")
    _observe(conn, _IID, _H, "0000000666")  # not requested
    _observe(conn, _IID, _G2, "0000000555")

    assert _read_13g_filers(conn, _IID, [_block(_G), _block(_G2)]) == {
        _G: ("0001527166",),
        _G2: ("0000000555",),
    }
