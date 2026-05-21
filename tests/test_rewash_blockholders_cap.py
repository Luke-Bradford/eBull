"""Rewash gate F (chokepoint F) — branch-ordered retention guard on
``_apply_blockholders`` (PR11 #1233 §3.2 chokepoint F).

Three invariants pinned:

1. **Happy path uncapped** — accessions with existing
   ``blockholder_filings`` rows are NEVER capped on rewash. Parent
   spec §6.3: pre-wipe rows stay on the canvas until the operator
   explicitly wipes them. The rewash sweep refreshes them under the
   current parser_version regardless of ``filed_at``.

2. **Rescue path skips pre-cap** — accessions with zero existing rows
   (the row was tombstoned or never ingested) MUST short-circuit
   return ``False`` when ``manifest.filed_at`` falls strictly before
   ``blockholders_retention_cutoff()``. Without this gate, an
   operator-triggered rewash would silently re-introduce pre-cap
   observations through the back door.

3. **Rescue path writes post-cap** — when the rescue branch fires AND
   ``manifest.filed_at`` is inside the retention window, the rewash
   proceeds to its normal write (DELETE + re-INSERT semantics with
   the freshly-parsed body).

Branch order pinned by lint invariant H in
``scripts/check_13dg_retention.sh``: the
``blockholders_within_retention(`` call MUST precede any
``DELETE FROM blockholder_filings`` / ``_upsert_filing_row(`` call in
the same function body.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from app.services import raw_filings, rewash_filings
from app.services.blockholders import (
    SEC_SCHEDULE_13_XML_MANDATE_DATE,
    blockholders_retention_cutoff,
)
from app.services.rewash_filings import (
    ParserSpec,
    register_parser,
    registered_specs,
    run_rewash,
)
from app.services.sec_manifest import record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


_NS_13D = "http://www.sec.gov/edgar/schedule13D"


# Real post-mandate XML fixture mirroring
# ``tests/test_manifest_parser_sec_13dg.py::_FAKE_13D_XML``. Uses
# ``<coverPageHeader>`` (NOT ``<coverPage>``) per the Phase 5 lesson
# baked into ``.claude/skills/data-sources/edgartools.md`` G11. Verified
# to parse cleanly via ``Schedule13D.parse_xml`` (Phase 7 sanity-check).
_FAKE_13D_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="{_NS_13D}">
  <headerData>
    <submissionType>SCHEDULE 13D</submissionType>
    <filerInfo>
      <filer>
        <filerCredentials>
          <cik>0000111000</cik>
        </filerCredentials>
      </filer>
    </filerInfo>
  </headerData>
  <formData>
    <coverPageHeader>
      <securitiesClassTitle>Class A Common Stock, par value $.01 per share</securitiesClassTitle>
      <dateOfEvent>11/03/2025</dateOfEvent>
      <issuerInfo>
        <issuerCIK>0000999000</issuerCIK>
        <issuerCUSIP>BLKCSP01</issuerCUSIP>
        <issuerName>Rewash Cap Issuer Inc.</issuerName>
      </issuerInfo>
    </coverPageHeader>
    <reportingPersons>
      <reportingPersonInfo>
        <reportingPersonCIK>0000111000</reportingPersonCIK>
        <reportingPersonNoCIK>N</reportingPersonNoCIK>
        <reportingPersonName>Rewash Cap Filer LLC</reportingPersonName>
        <memberOfGroup>b</memberOfGroup>
        <citizenshipOrOrganization>DE</citizenshipOrOrganization>
        <soleVotingPower>1500000</soleVotingPower>
        <sharedVotingPower>0</sharedVotingPower>
        <soleDispositivePower>1500000</soleDispositivePower>
        <sharedDispositivePower>0</sharedDispositivePower>
        <aggregateAmountOwned>1500000</aggregateAmountOwned>
        <percentOfClass>5.5</percentOfClass>
        <typeOfReportingPerson>CO</typeOfReportingPerson>
      </reportingPersonInfo>
    </reportingPersons>
    <signatureInfo>
      <signaturePerson>
        <signatureDetails>
          <date>11/06/2025</date>
        </signatureDetails>
      </signaturePerson>
    </signatureInfo>
  </formData>
</edgarSubmission>
"""


@pytest.fixture
def isolated_registry() -> Iterator[None]:
    """Snapshot + restore the parser registry around each test."""
    saved = registered_specs()
    try:
        yield
    finally:
        rewash_filings._REGISTRY.clear()
        for spec in saved.values():
            register_parser(spec)


def _pre_cap_filed_at() -> datetime:
    """A ``filed_at`` strictly before the retention floor.

    Uses ``SEC_SCHEDULE_13_XML_MANDATE_DATE - 30d`` so the value is
    deterministic AND below the floor whether the floor is the XML
    mandate (early life of the cap) or the rolling 3y line (later
    life). The retention helper resolves to ``max(today-3y, mandate)``
    — both options reject anything before ``mandate - 30d``.
    """
    pre_cap_date = SEC_SCHEDULE_13_XML_MANDATE_DATE - timedelta(days=30)
    return datetime(pre_cap_date.year, pre_cap_date.month, pre_cap_date.day, tzinfo=UTC)


def _post_cap_filed_at() -> datetime:
    """A ``filed_at`` strictly inside the retention window.

    Uses ``cutoff + 30d`` (the cutoff is the inclusive lower bound, so
    one month past it is unambiguously inside).
    """
    cutoff = blockholders_retention_cutoff()
    inside = cutoff + timedelta(days=30)
    return datetime(inside.year, inside.month, inside.day, tzinfo=UTC)


def _seed_manifest_row(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    filer_cik: str,
    filed_at: datetime,
    form: str = "SC 13D",
    source: str = "sec_13d",
) -> None:
    """Seed a ``sec_filing_manifest`` row for the accession. Rewash
    rescue-path looks up ``filed_at`` + ``source`` + ``cik`` + ``form``
    via this row."""
    record_manifest_entry(
        conn,
        accession,
        cik=filer_cik,
        form=form,
        source=source,  # type: ignore[arg-type]
        subject_type="blockholder_filer",
        subject_id=filer_cik,
        instrument_id=None,
        filed_at=filed_at,
        primary_document_url=(
            f"https://www.sec.gov/Archives/edgar/data/111000/{accession.replace('-', '')}/primary_doc.xml"
        ),
    )


def _seed_instrument_and_cusip(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    symbol: str,
    cusip: str,
) -> None:
    """Seed an instrument + CUSIP mapping so the rewash's CUSIP→iid
    resolution succeeds (post-cap rescue path) or fails harmlessly
    (when not required)."""
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, country, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', 'US', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} co"),
    )
    conn.execute(
        """
        INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary)
        VALUES (%s, 'sec', 'cusip', %s, TRUE)
        ON CONFLICT DO NOTHING
        """,
        (iid, cusip),
    )


def _seed_raw_13dg(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    parser_version: str = "13dg-primary-v0",
) -> None:
    """Seed a raw 13D body so the rewash sweep finds it. Uses the
    pre-mandate-parseable post-mandate XML fixture above so the
    edgartools dispatch in ``_apply_blockholders`` finds typed data
    on the rescue-path write."""
    raw_filings.store_raw(
        conn,
        accession_number=accession,
        document_kind="primary_doc_13dg",
        payload=_FAKE_13D_XML,
        parser_version=parser_version,
    )
    conn.commit()


def test_happy_path_uncapped_for_existing_rows(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """Pre-cap accession WITH an existing ``blockholder_filings`` row →
    happy path: rewash proceeds (DELETE + re-INSERT), counted as
    ``rows_reparsed``. Parent spec §6.3 — pre-wipe rows stay on the
    canvas until explicit operator wipe."""
    conn = ebull_test_conn
    accession = "0001234567-26-100001"
    instrument_id = 950_200
    filer_cik = "0000111000"
    cusip = "BLKCSP01"

    _seed_instrument_and_cusip(conn, iid=instrument_id, symbol="BLKHAPPY", cusip=cusip)

    pre_cap = _pre_cap_filed_at()
    _seed_manifest_row(conn, accession=accession, filer_cik=filer_cik, filed_at=pre_cap)

    # Pre-seed the filer + an existing typed row (the happy-path
    # signal). The row's ``filed_at`` is pre-cap to prove the gate
    # does NOT fire on the happy path.
    conn.execute(
        "INSERT INTO blockholder_filers (cik, name) VALUES (%s, 'Existing Filer') ON CONFLICT (cik) DO NOTHING",
        (filer_cik,),
    )
    with conn.cursor() as cur:
        cur.execute("SELECT filer_id FROM blockholder_filers WHERE cik = %s", (filer_cik,))
        result = cur.fetchone()
    assert result is not None
    filer_id = result[0]
    conn.execute(
        """
        INSERT INTO blockholder_filings (
            filer_id, accession_number, submission_type, status,
            instrument_id, issuer_cik, issuer_cusip, securities_class_title,
            reporter_no_cik, reporter_name, aggregate_amount_owned,
            percent_of_class, filed_at
        ) VALUES (%s, %s, 'SCHEDULE 13D', 'active', %s,
                  '0000999000', %s, 'Class A Common Stock, par value $.01 per share',
                  FALSE, 'Existing Reporter', 1000, 5.5, %s)
        """,
        (filer_id, accession, instrument_id, cusip, pre_cap),
    )
    _seed_raw_13dg(conn, accession=accession, parser_version="13dg-primary-v0")

    rewash_filings._REGISTRY.clear()
    register_parser(
        ParserSpec(
            document_kind="primary_doc_13dg",
            current_version="13dg-primary-v1",
            apply_fn=rewash_filings._apply_blockholders,
        )
    )

    result = run_rewash(conn, document_kind="primary_doc_13dg")

    assert result.rows_scanned == 1
    assert result.rows_reparsed == 1, "happy path must rewash uncapped even with pre-cap filed_at"
    assert result.rows_skipped == 0
    assert result.rows_failed == 0

    # DELETE + re-INSERT happened — verify the per-reporter row reflects
    # the freshly-parsed body (new reporter name from the XML fixture
    # replaces the seeded 'Existing Reporter').
    with conn.cursor() as cur:
        cur.execute(
            "SELECT reporter_name FROM blockholder_filings WHERE accession_number = %s",
            (accession,),
        )
        rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "Rewash Cap Filer LLC"


def test_rescue_path_skips_pre_cap_accession(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """Pre-cap accession with ZERO existing rows → rescue path → returns
    ``False`` (skipped). No INSERT INTO blockholder_filings. Pins
    chokepoint F: the back-door rewash route MUST honour the retention
    cap on the rescue path. Lint invariant H pins the placement."""
    conn = ebull_test_conn
    accession = "0001234567-26-100002"
    filer_cik = "0000111001"

    pre_cap = _pre_cap_filed_at()
    _seed_manifest_row(conn, accession=accession, filer_cik=filer_cik, filed_at=pre_cap)
    _seed_raw_13dg(conn, accession=accession, parser_version="13dg-primary-v0")

    # No blockholder_filings row pre-seeded (rescue path).

    rewash_filings._REGISTRY.clear()
    register_parser(
        ParserSpec(
            document_kind="primary_doc_13dg",
            current_version="13dg-primary-v1",
            apply_fn=rewash_filings._apply_blockholders,
        )
    )

    result = run_rewash(conn, document_kind="primary_doc_13dg")

    assert result.rows_scanned == 1
    assert result.rows_skipped == 1, "rescue path must skip pre-cap accession"
    assert result.rows_reparsed == 0
    assert result.rows_failed == 0

    # Confirm no rows were inserted on the rescue-path skip — the gate
    # short-circuits BEFORE DELETE / _upsert_filing_row.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM blockholder_filings WHERE accession_number = %s",
            (accession,),
        )
        count = cur.fetchone()
    assert count is not None
    assert count[0] == 0


def test_rescue_path_writes_post_cap_accession(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """Post-cap accession with ZERO existing rows → rescue path past the
    gate → proceeds to normal write (DELETE no-op + re-INSERT). Pins
    that the rescue branch is NOT a blanket short-circuit — only the
    out-of-window subset is skipped."""
    conn = ebull_test_conn
    accession = "0001234567-26-100003"
    instrument_id = 950_201
    filer_cik = "0000111002"
    cusip = "BLKCSP01"

    _seed_instrument_and_cusip(conn, iid=instrument_id, symbol="BLKPOST", cusip=cusip)

    post_cap = _post_cap_filed_at()
    _seed_manifest_row(conn, accession=accession, filer_cik=filer_cik, filed_at=post_cap)
    _seed_raw_13dg(conn, accession=accession, parser_version="13dg-primary-v0")

    # No blockholder_filings row pre-seeded (rescue path).

    rewash_filings._REGISTRY.clear()
    register_parser(
        ParserSpec(
            document_kind="primary_doc_13dg",
            current_version="13dg-primary-v1",
            apply_fn=rewash_filings._apply_blockholders,
        )
    )

    result = run_rewash(conn, document_kind="primary_doc_13dg")

    assert result.rows_scanned == 1
    assert result.rows_reparsed == 1, "rescue path must write post-cap accession"
    assert result.rows_skipped == 0
    assert result.rows_failed == 0

    # Confirm the row was written under the new parser path.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT reporter_name, issuer_cusip, aggregate_amount_owned "
            "FROM blockholder_filings WHERE accession_number = %s",
            (accession,),
        )
        rows = cur.fetchall()
    assert len(rows) == 1
    reporter_name, issuer_cusip, aggregate = rows[0]
    assert reporter_name == "Rewash Cap Filer LLC"
    assert issuer_cusip == cusip
    assert aggregate == 1500000


# ---------------------------------------------------------------------------
# Bot PREVENTION (post-PR1253-iter-1): sec_13g happy-path mis-source guard
# ---------------------------------------------------------------------------

_NS_13G = "http://www.sec.gov/edgar/schedule13G"


_FAKE_13G_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="{_NS_13G}">
  <headerData>
    <submissionType>SCHEDULE 13G</submissionType>
    <filerInfo>
      <filer>
        <filerCredentials>
          <cik>0000222000</cik>
        </filerCredentials>
      </filer>
    </filerInfo>
  </headerData>
  <formData>
    <coverPageHeader>
      <securitiesClassTitle>Common Stock</securitiesClassTitle>
      <eventDateRequiresFilingThisStatement>11/03/2025</eventDateRequiresFilingThisStatement>
      <issuerInfo>
        <issuerCik>0000999000</issuerCik>
        <issuerCusip>BLKCSP01</issuerCusip>
        <issuerName>Rewash Cap Issuer Inc.</issuerName>
      </issuerInfo>
    </coverPageHeader>
    <coverPageHeaderReportingPersonDetails>
      <reportingPersonName>Passive Holder LLC</reportingPersonName>
      <reportingPersonNoCIK>N</reportingPersonNoCIK>
      <citizenshipOrOrganization>DE</citizenshipOrOrganization>
      <memberGroup>b</memberGroup>
      <reportingPersonBeneficiallyOwnedNumberOfShares>
        <soleVotingPower>1500000</soleVotingPower>
        <sharedVotingPower>0</sharedVotingPower>
        <soleDispositivePower>1500000</soleDispositivePower>
        <sharedDispositivePower>0</sharedDispositivePower>
      </reportingPersonBeneficiallyOwnedNumberOfShares>
      <reportingPersonBeneficiallyOwnedAggregateNumberOfShares>1500000</reportingPersonBeneficiallyOwnedAggregateNumberOfShares>
      <classPercent>5.5</classPercent>
      <typeOfReportingPerson>CO</typeOfReportingPerson>
    </coverPageHeaderReportingPersonDetails>
    <signatureInfo>
      <signaturePerson>
        <signatureDetails>
          <date>11/06/2025</date>
        </signatureDetails>
      </signaturePerson>
    </signatureInfo>
  </formData>
</edgarSubmission>
"""


def test_happy_path_sec_13g_writes_passive_status(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    isolated_registry: None,
) -> None:
    """Bot PREVENTION 2026-05-21: rewash happy-path must source-dispatch
    via the manifest row's source column. When manifest_source='sec_13g'
    AND existing typed rows exist, the re-written blockholder_filings row
    MUST have status='passive' (NOT silently defaulted to 'active' from
    a sec_13d fallback).

    Original bug: rewash code defaulted ``source_for_adapter`` to
    ``'sec_13d'`` when manifest_source was None on the happy path. Fix:
    require manifest_row in BOTH branches; return False if missing.
    """
    conn = ebull_test_conn
    instrument_id = 99500003
    accession = "0000222000-26-000003"
    filer_cik = "0000222000"
    cusip = "BLKCSP01"

    _seed_instrument_and_cusip(conn, iid=instrument_id, symbol="BLKPSV", cusip=cusip)

    post_cap = _post_cap_filed_at()
    _seed_manifest_row(
        conn,
        accession=accession,
        filer_cik=filer_cik,
        filed_at=post_cap,
        form="SCHEDULE 13G",
        source="sec_13g",
    )

    # Seed an existing row so we enter the HAPPY PATH (existing_rows > 0).
    # Pre-seed with the WRONG status ('active') so a silent default-to-13D
    # would leave it active; correct sec_13g dispatch overwrites with 'passive'.
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO blockholder_filers (cik, name)
            VALUES (%s, 'Passive Holder LLC')
            ON CONFLICT (cik) DO UPDATE SET name = EXCLUDED.name
            RETURNING filer_id
            """,
            (filer_cik,),
        )
        row = cur.fetchone()
        assert row is not None, "filer upsert returned no row"
        filer_id = row[0]
        cur.execute(
            """
            INSERT INTO blockholder_filings (
                filer_id, accession_number, submission_type, status,
                instrument_id, issuer_cik, issuer_cusip, reporter_name,
                aggregate_amount_owned, percent_of_class, filed_at
            ) VALUES (%s, %s, 'SCHEDULE 13G', 'passive', %s, '0000999000', %s, 'Stale Row', 1, 0.0, %s)
            """,
            (filer_id, accession, instrument_id, cusip, post_cap),
        )

    # Seed the raw 13G XML body so the rewash sweep can parse it.
    from app.services import raw_filings as _raw

    _raw.store_raw(
        conn,
        accession_number=accession,
        document_kind="primary_doc_13dg",
        payload=_FAKE_13G_XML,
        parser_version="13dg-primary-v0",  # older version → reparse trigger
    )
    conn.commit()

    rewash_filings._REGISTRY.clear()
    register_parser(
        ParserSpec(
            document_kind="primary_doc_13dg",
            current_version="13dg-primary-v1",
            apply_fn=rewash_filings._apply_blockholders,
        )
    )

    result = run_rewash(conn, document_kind="primary_doc_13dg")

    assert result.rows_reparsed == 1, f"happy-path sec_13g must rewash; got {result}"
    assert result.rows_failed == 0

    with conn.cursor() as cur:
        cur.execute(
            "SELECT submission_type, status, reporter_name FROM blockholder_filings WHERE accession_number = %s",
            (accession,),
        )
        rows = cur.fetchall()
    # Rewash performs DELETE + re-INSERT — should produce exactly one row.
    assert len(rows) == 1, f"expected exactly 1 row post-rewash, got {len(rows)}: {rows}"
    submission_type, status, reporter_name = rows[0]
    assert submission_type == "SCHEDULE 13G", f"expected SCHEDULE 13G, got {submission_type!r}"
    assert status == "passive", (
        f"sec_13g rewash must write status='passive', got {status!r} — "
        f"silent sec_13d default would write 'active' on a passive filing"
    )
    assert reporter_name == "Passive Holder LLC", (
        f"row was not re-written from the XML; pre-seeded 'Stale Row' remains. {reporter_name=!r}"
    )
