from __future__ import annotations

from datetime import UTC, datetime

import psycopg

from scripts.verify_2582_schedule13d_census import _CHAIN_SHAPE


def _seed(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    submission_type: str,
    reporter_cik: str,
    filed_at: datetime,
) -> int:
    filer_cik = "".join(character for character in accession if character.isdigit())[-10:]
    conn.execute(
        "INSERT INTO blockholder_filers (cik, name) VALUES (%s, %s)",
        (filer_cik, f"Filer {accession}"),
    )
    status = "active" if submission_type.startswith("SCHEDULE 13D") else "passive"
    conn.execute(
        """
        INSERT INTO blockholder_filings (
            filer_id, accession_number, submission_type, status,
            issuer_cik, issuer_cusip, reporter_cik, reporter_no_cik,
            reporter_name, filed_at
        )
        SELECT filer_id, %s, %s, %s, '0000000789', '999999999',
               %s, FALSE, %s, %s
        FROM blockholder_filers WHERE cik = %s
        """,
        (
            accession,
            submission_type,
            status,
            reporter_cik,
            f"Reporter {reporter_cik}",
            filed_at,
            filer_cik,
        ),
    )
    _seed_manifest(conn, accession=accession, cik=filer_cik, submission_type=submission_type, filed_at=filed_at)
    row = conn.execute("SELECT filer_id FROM blockholder_filers WHERE cik = %s", (filer_cik,)).fetchone()
    assert row is not None
    return int(row[0])


def _seed_manifest(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    cik: str,
    submission_type: str,
    filed_at: datetime,
) -> None:
    """Seed the manifest row ``_CHAIN_SHAPE`` reads the public clock from.

    ``deae7c03`` ("use SEC public filing clock") moved every chain-ordering
    date off ``blockholder_filings.filed_at`` and onto
    ``sec_filing_manifest.filed_at``, joined ``USING (accession_number)``. An
    inner join, so a blockholder filing with no manifest row contributes
    NOTHING — which is why this fixture must write both sides or the census
    counts every bucket as zero.

    ``subject_type = 'blockholder_filer'`` forces ``instrument_id IS NULL``
    (``chk_manifest_issuer_has_instrument``); the census tolerates that via
    ``max(...) FILTER (WHERE instrument_id IS NOT NULL)``.
    """
    conn.execute(
        """
        INSERT INTO sec_filing_manifest (
            accession_number, cik, form, source, subject_type, subject_id, filed_at
        ) VALUES (%s, %s, %s, %s, 'blockholder_filer', %s, %s)
        """,
        (
            accession,
            cik,
            submission_type,
            "sec_13d" if submission_type.startswith("SCHEDULE 13D") else "sec_13g",
            cik,
            filed_at,
        ),
    )


def test_joint_accession_uses_every_reporter_chain(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """A non-primary joint reporter's history must classify the accession."""

    _seed(
        ebull_test_conn,
        accession="0000000001-25-000001",
        submission_type="SCHEDULE 13G",
        reporter_cik="0000000002",
        filed_at=datetime(2025, 1, 2, tzinfo=UTC),
    )
    _seed(
        ebull_test_conn,
        accession="0000000003-25-000001",
        submission_type="SCHEDULE 13D",
        reporter_cik="0000000003",
        filed_at=datetime(2025, 1, 3, tzinfo=UTC),
    )
    _seed(
        ebull_test_conn,
        accession="0000000004-25-000001",
        submission_type="SCHEDULE 13D",
        reporter_cik="0000000004",
        filed_at=datetime(2025, 1, 4, tzinfo=UTC),
    )
    # Add two reporters to one initial 13D. Reporter 2 carries the prior 13G;
    # reporter 3 carries the prior 13D. Neither is the current primary filer.
    current_filer_id = _seed(
        ebull_test_conn,
        accession="0000000005-25-000001",
        submission_type="SCHEDULE 13D",
        reporter_cik="0000000002",
        filed_at=datetime(2025, 1, 5, tzinfo=UTC),
    )
    # The unique accession/reporter index permits this joint-reporter row, but
    # _seed would try to insert the same primary filer again. Insert only row 2.
    ebull_test_conn.execute(
        """
        INSERT INTO blockholder_filings (
            filer_id, accession_number, submission_type, status,
            issuer_cik, issuer_cusip, reporter_cik, reporter_no_cik,
            reporter_name, filed_at
        ) VALUES (%s, '0000000005-25-000001', 'SCHEDULE 13D', 'active',
                  '0000000789', '999999999', '0000000003', FALSE,
                  'Reporter 3', TIMESTAMPTZ '2025-01-05 00:00:00+00')
        """,
        (current_filer_id,),
    )

    row = ebull_test_conn.execute(_CHAIN_SHAPE).fetchone()
    assert row == (3, 2, 1, 1, 0, 0)


def test_same_timestamp_does_not_invent_chain_order(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    filed_at = datetime(2025, 2, 3, tzinfo=UTC)
    for accession in ("0000000006-25-000001", "0000000006-25-000002"):
        _seed(
            ebull_test_conn,
            accession=accession,
            submission_type="SCHEDULE 13D",
            reporter_cik="0000000006",
            filed_at=filed_at,
        )

    row = ebull_test_conn.execute(_CHAIN_SHAPE).fetchone()
    assert row == (2, 2, 0, 0, 2, 0)
