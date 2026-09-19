"""#3236 — the 13D/G link re-resolution sweep, against real PostgreSQL.

One integration test per genuinely-new SQL mechanism, plus the guards whose
whole job is to REFUSE. The refusals matter more than the happy path here:
#3236 rejected a rewash-shaped design partly because "no NULL rows remain" is
a success check that rows simply disappearing would satisfy. A guard that
silently stopped guarding would look exactly like a clean sweep.

Auto-marked ``db`` (pulls ``ebull_test_conn``).
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg

from app.providers.implementations.sec_13dg import parse_primary_doc
from app.services import raw_filings
from app.services.blockholders import (
    _repair_one_accession,
    _upsert_filer,
    _upsert_filing_row,
    sweep_unlinked_blockholder_filings,
)
from tests.test_sec_13dg_parser import _13d_xml

_FILER_CIK = "0002093607"
_ISSUER_CIK = "0001001250"
_ISSUER_CUSIP = "518439104"
_FILED_AT = datetime(2026, 6, 14, 12, 44, tzinfo=UTC)

_THREE_REPORTERS = "".join(
    f"""
    <reportingPersonInfo>
      <reportingPersonCIK>000000{n}111</reportingPersonCIK>
      <reportingPersonNoCIK>N</reportingPersonNoCIK>
      <reportingPersonName>REPORTER {n} LP</reportingPersonName>
      <citizenshipOrOrganization>DE</citizenshipOrOrganization>
      <soleVotingPower>{n}00</soleVotingPower>
      <sharedVotingPower>0</sharedVotingPower>
      <soleDispositivePower>{n}00</soleDispositivePower>
      <sharedDispositivePower>0</sharedDispositivePower>
      <aggregateAmountOwned>{n}00</aggregateAmountOwned>
      <percentOfClass>{n}.5</percentOfClass>
      <typeOfReportingPerson>CO</typeOfReportingPerson>
    </reportingPersonInfo>"""
    for n in (1, 2, 3)
)


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, country, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', 'US', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} co"),
    )


def _map_cusip(conn: psycopg.Connection[tuple], iid: int, cusip: str = _ISSUER_CUSIP) -> None:
    """The mapping whose LATE arrival is the whole defect."""
    conn.execute(
        """
        INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary)
        VALUES (%s, 'sec', 'cusip', %s, TRUE)
        ON CONFLICT DO NOTHING
        """,
        (iid, cusip),
    )


def _strand(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int | None = None,
    xml: str | None = None,
    store_body: bool = True,
) -> str:
    """Write an accession exactly as a race-losing ingest would: typed reporter
    rows with a NULL link, a stored raw body, and NO observation."""
    body = xml if xml is not None else _13d_xml()
    filing = parse_primary_doc(body)
    filer_id = _upsert_filer(conn, cik=_FILER_CIK, name="FILER CO")
    for person in filing.reporting_persons:
        _upsert_filing_row(
            conn,
            filer_id=filer_id,
            accession_number=accession,
            submission_type=filing.submission_type,
            status=filing.status,
            instrument_id=instrument_id,
            issuer_cik=filing.issuer_cik,
            issuer_cusip=filing.issuer_cusip,
            securities_class_title=filing.securities_class_title,
            date_of_event=None,  # NULL on 100% of the real corpus
            filed_at=_FILED_AT,
            person=person,
        )
    if store_body:
        raw_filings.store_raw(
            conn,
            accession_number=accession,
            document_kind="primary_doc_13dg",
            payload=body,
            parser_version="13dg-primary-v3",
            source_url=f"https://www.sec.gov/Archives/edgar/data/{int(_FILER_CIK)}/x/primary_doc.xml",
        )
    return body


def _links(conn: psycopg.Connection[tuple], accession: str) -> list[int | None]:
    return [
        r[0]
        for r in conn.execute(
            "SELECT instrument_id FROM blockholder_filings WHERE accession_number = %s",
            (accession,),
        ).fetchall()
    ]


def _observations(conn: psycopg.Connection[tuple], accession: str) -> list[tuple]:
    return conn.execute(
        """
        SELECT instrument_id, reporter_cik, aggregate_amount_owned, period_end, filed_at, known_to
        FROM ownership_blockholders_observations
        WHERE source_document_id = %s
        """,
        (accession,),
    ).fetchall()


def test_sweep_links_the_rows_and_writes_the_missing_observation(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The defect end to end: stranded on ingest, repaired once the mapping lands."""
    conn = ebull_test_conn
    accession = "9999999999-26-003236"
    _seed_instrument(conn, 932360, "SWEEP")
    _strand(conn, accession=accession)

    # Before the mapping exists the sweep must NOT invent a link. The
    # prefilter does not even select it — an issuer with no identifier row
    # anywhere cannot resolve, so selecting it would just burn a slot.
    first = sweep_unlinked_blockholder_filings(conn, limit=50)
    assert first.repaired == 0
    assert first.candidates_seen == 0
    assert _links(conn, accession) == [None, None]
    assert _observations(conn, accession) == []

    # The mapping arrives — which is the event nothing currently reacts to.
    _map_cusip(conn, 932360)

    report = sweep_unlinked_blockholder_filings(conn, limit=50)
    assert report.repaired == 1
    assert report.rows_linked == 2
    assert report.skipped == {}

    assert _links(conn, accession) == [932360, 932360]
    obs = _observations(conn, accession)
    assert len(obs) == 1
    # ``filed_at`` is pinned from the STORED row, not the re-parse: the body's
    # signatureInfo date is 2025-11-06 at UTC midnight, which would both move
    # period_end and lose the real timestamp.
    assert obs[0][0] == 932360
    assert obs[0][3] == _FILED_AT.date()
    assert obs[0][4] == _FILED_AT
    assert obs[0][5] is None  # live, not retired


def test_second_pass_is_a_no_op(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Idempotence — and specifically that the second pass recognises the
    accession as already linked rather than rewriting the observation."""
    conn = ebull_test_conn
    accession = "9999999999-26-003237"
    _seed_instrument(conn, 932370, "IDEM")
    _strand(conn, accession=accession)
    _map_cusip(conn, 932370)

    assert sweep_unlinked_blockholder_filings(conn, limit=50).repaired == 1
    before = _observations(conn, accession)

    second = sweep_unlinked_blockholder_filings(conn, limit=50)
    assert second.repaired == 0
    assert second.candidates_seen == 0  # no NULL-link rows left to even select
    assert _observations(conn, accession) == before


def test_sweep_refuses_an_accession_whose_body_disagrees_with_the_stored_rows(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The drift gate. The stored body is replaced with one carrying a
    different reporter, so the re-parse no longer describes the typed rows the
    drill-through renders; writing that observation would be a hybrid."""
    conn = ebull_test_conn
    accession = "9999999999-26-003238"
    _seed_instrument(conn, 932380, "DRIFT")
    _strand(conn, accession=accession)
    _map_cusip(conn, 932380)

    conn.execute(
        "UPDATE filing_raw_documents SET payload = %s WHERE accession_number = %s",
        (
            _13d_xml(
                reporters_xml="""
                <reportingPersonInfo>
                  <reportingPersonCIK>0000000777</reportingPersonCIK>
                  <reportingPersonNoCIK>N</reportingPersonNoCIK>
                  <reportingPersonName>SOMEONE ELSE ENTIRELY LLC</reportingPersonName>
                  <citizenshipOrOrganization>DE</citizenshipOrOrganization>
                  <soleVotingPower>42</soleVotingPower>
                  <sharedVotingPower>0</sharedVotingPower>
                  <soleDispositivePower>42</soleDispositivePower>
                  <sharedDispositivePower>0</sharedDispositivePower>
                  <aggregateAmountOwned>42</aggregateAmountOwned>
                  <percentOfClass>0.1</percentOfClass>
                  <typeOfReportingPerson>CO</typeOfReportingPerson>
                </reportingPersonInfo>
                """
            ),
            accession,
        ),
    )

    report = sweep_unlinked_blockholder_filings(conn, limit=50)
    assert report.repaired == 0
    assert report.skipped.get("drifted") == 1
    # Refused means UNTOUCHED — not half-repaired.
    assert _links(conn, accession) == [None, None]
    assert _observations(conn, accession) == []


def test_sweep_does_not_select_an_accession_with_no_stored_body(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """No stored body means nothing to gate against, so there is no safe
    repair — and the prefilter drops it before it costs a slot."""
    conn = ebull_test_conn
    accession = "9999999999-26-003239"
    _seed_instrument(conn, 932390, "NOBODY")
    _strand(conn, accession=accession, store_body=False)
    _map_cusip(conn, 932390)

    report = sweep_unlinked_blockholder_filings(conn, limit=50)
    assert report.repaired == 0
    assert report.candidates_seen == 0
    assert _links(conn, accession) == [None, None]


def test_missing_body_guard_still_refuses_if_selection_raced(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The ``no_raw_body`` guard is unreachable through the prefilter, so it is
    exercised directly. It exists for the TOCTOU window: selection commits its
    read transaction before the repair loop takes the accession lock, so the
    body can be retention-swept away in between. A defensive branch a test
    cannot reach is a branch that silently stops working."""
    conn = ebull_test_conn
    accession = "9999999999-26-003242"
    _seed_instrument(conn, 932420, "RACED")
    _strand(conn, accession=accession, store_body=False)
    _map_cusip(conn, 932420)

    reason, linked = _repair_one_accession(conn, accession=accession)
    assert reason == "no_raw_body"
    assert linked == 0
    assert _links(conn, accession) == [None, None]


def test_still_unresolved_guard_refuses_a_multi_sibling_cik(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The CIK tier resolves only a single-sibling issuer. Two instruments
    share the issuer CIK here, so the resolver must decline rather than guess
    a share class — and the sweep must record that, not repair."""
    conn = ebull_test_conn
    accession = "9999999999-26-003243"
    _seed_instrument(conn, 932430, "MULTIA")
    _seed_instrument(conn, 932431, "MULTIB")
    _strand(conn, accession=accession)
    # CIK mappings for BOTH siblings, and no CUSIP mapping at all.
    for iid in (932430, 932431):
        conn.execute(
            """
            INSERT INTO external_identifiers (instrument_id, provider, identifier_type,
                                              identifier_value, is_primary)
            VALUES (%s, 'sec', 'cik', %s, TRUE) ON CONFLICT DO NOTHING
            """,
            (iid, _ISSUER_CIK),
        )

    report = sweep_unlinked_blockholder_filings(conn, limit=50)
    assert report.repaired == 0
    assert report.skipped.get("still_unresolved") == 1
    assert _links(conn, accession) == [None, None]


def test_sweep_refuses_when_an_observation_already_exists(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``record_blockholder_observation`` is ON CONFLICT DO UPDATE and never
    clears ``known_to``. A RETIRED row at the key would be updated but stay
    invisible while the link went non-NULL — unreachable by any later pass.
    The guard refuses rather than writing into that state."""
    conn = ebull_test_conn
    accession = "9999999999-26-003240"
    _seed_instrument(conn, 932400, "RETIRED")
    _strand(conn, accession=accession)
    _map_cusip(conn, 932400)

    conn.execute(
        """
        INSERT INTO ownership_blockholders_observations (
            instrument_id, reporter_cik, reporter_name, ownership_nature,
            submission_type, status_flag, source, source_document_id,
            source_accession, source_url, filed_at, period_end,
            ingest_run_id, aggregate_amount_owned, known_to
        ) VALUES (
            %s, '0002093607', 'FILER CO', 'beneficial',
            'SCHEDULE 13D', 'active', '13d', %s,
            %s, NULL, %s, %s,
            gen_random_uuid(), 1, now()
        )
        """,
        (932400, accession, accession, _FILED_AT, _FILED_AT.date()),
    )

    report = sweep_unlinked_blockholder_filings(conn, limit=50)
    assert report.repaired == 0
    assert report.skipped.get("observation_exists") == 1
    assert _links(conn, accession) == [None, None]


def test_sweep_refuses_a_link_conflict(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """A partially-linked accession whose existing link disagrees with the one
    just resolved is a share-class mislink in progress, not a repair."""
    conn = ebull_test_conn
    accession = "9999999999-26-003241"
    _seed_instrument(conn, 932410, "CLASSA")
    _seed_instrument(conn, 932411, "CLASSB")
    _strand(conn, accession=accession)
    _map_cusip(conn, 932410)
    # One row already points at the OTHER share class.
    conn.execute(
        """
        UPDATE blockholder_filings SET instrument_id = %s
        WHERE filing_id = (
            SELECT min(filing_id) FROM blockholder_filings WHERE accession_number = %s
        )
        """,
        (932411, accession),
    )

    report = sweep_unlinked_blockholder_filings(conn, limit=50)
    assert report.repaired == 0
    assert report.skipped.get("link_conflict") == 1
    assert sorted(_links(conn, accession), key=lambda v: (v is None, v)) == [932411, None]


def test_sweep_refuses_an_accession_already_split_across_two_instruments(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``distinct_links > 1`` is a conflict whatever we resolved.

    The guard compares against ``min(instrument_id)``, so an accession already
    split across two instruments would pass a bare equality test whenever the
    resolved id happened to be the LOWER one — and the UPDATE would then fill
    the NULL rows while leaving the other link standing, committing a mixed
    instrument accession. Codex checkpoint-2 P2; the resolved id here is
    deliberately the lower of the two.
    """
    conn = ebull_test_conn
    accession = "9999999999-26-003244"
    _seed_instrument(conn, 932440, "LOWER")
    _seed_instrument(conn, 932441, "UPPER")
    # Three reporters so two can be pre-linked and one left NULL — with only
    # two the accession reads as ``already_linked`` and never reaches the
    # conflict branch at all.
    _strand(conn, accession=accession, xml=_13d_xml(reporters_xml=_THREE_REPORTERS))
    _map_cusip(conn, 932440)  # resolves to the LOWER id

    rows = conn.execute(
        "SELECT filing_id FROM blockholder_filings WHERE accession_number = %s ORDER BY filing_id",
        (accession,),
    ).fetchall()
    conn.execute(
        "UPDATE blockholder_filings SET instrument_id = %s WHERE filing_id = %s",
        (932440, rows[0][0]),
    )
    conn.execute(
        "UPDATE blockholder_filings SET instrument_id = %s WHERE filing_id = %s",
        (932441, rows[1][0]),
    )

    reason, linked = _repair_one_accession(conn, accession=accession)
    assert reason == "link_conflict"
    assert linked == 0
    assert sorted(v for v in _links(conn, accession) if v is not None) == [932440, 932441]
