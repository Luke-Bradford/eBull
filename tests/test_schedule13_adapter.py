"""Tests for the edgartools → ``BlockholderFiling`` adapter
(``app/services/manifest_parsers/_schedule13_adapter.py``) used by
the SC 13D / 13G manifest worker (#1233 PR11).

Adapter contract (per spec §3.3 + plan Task 5.3):

* signature ``build_filing_from_edgartools_dict(parsed, *, source,
  manifest_form, manifest_filer_cik) -> BlockholderFiling``
* returns the repo-internal ``BlockholderFiling`` dataclass shape;
  downstream callers (``_upsert_filing_row`` +
  ``_record_13dg_observation_for_filing``) are unchanged consumers
* ``primary_filer_cik`` comes from ``manifest_filer_cik`` (the
  caller's ``row.cik`` — the archive-owner CIK) zero-padded; NOT
  from any reporting-person CIK (Codex 1c HIGH)
* ``submission_type`` is derived from ``manifest_form`` via the
  ``_SUBMISSION_TYPE_FOR_FORM`` mapping; unknown form raises
  KeyError (loud, not silent)
* ``status`` is derived from ``source`` via
  ``_STATUS_FOR_SOURCE`` (sec_13d → active; sec_13g → passive)
* share-power fields + ``aggregate_amount_owned`` +
  ``percent_of_class`` are ``Decimal`` (NUMERIC fidelity)
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.providers.implementations.sec_13dg import (
    BlockholderFiling,
    BlockholderReportingPerson,
)
from app.services.manifest_parsers._schedule13_adapter import (
    build_filing_from_edgartools_dict,
)
from tests.test_edgartools_schedule13_shape import (
    _FIXTURE_13D_XML,
    _FIXTURE_13G_XML,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parsed_13d() -> dict:
    from edgar.beneficial_ownership.schedule13 import Schedule13D

    return Schedule13D.parse_xml(_FIXTURE_13D_XML)


def _parsed_13g() -> dict:
    from edgar.beneficial_ownership.schedule13 import Schedule13G

    return Schedule13G.parse_xml(_FIXTURE_13G_XML)


# ---------------------------------------------------------------------------
# Happy-path 13D
# ---------------------------------------------------------------------------


def test_adapter_returns_blockholder_filing_for_13d_dict() -> None:
    """13D happy path — adapter yields a ``BlockholderFiling`` with
    the expected issuer / security / reporting-person mapping."""
    parsed = _parsed_13d()

    filing = build_filing_from_edgartools_dict(
        parsed,
        source="sec_13d",
        manifest_form="SC 13D",
        manifest_filer_cik="2093607",  # unpadded; adapter zero-pads
    )

    assert isinstance(filing, BlockholderFiling)
    assert filing.submission_type == "SCHEDULE 13D"
    assert filing.status == "active"

    # Codex 1c HIGH: primary_filer_cik MUST come from caller arg
    # (the archive-owner CIK), zero-padded — NOT from any reporting
    # person CIK. This is the canonical filer-of-record identity
    # downstream consumers (blockholder_filers PK, ingest_log) join on.
    assert filing.primary_filer_cik == "0002093607"

    # Issuer + share-class CUSIP (SecurityInfo.cusip, NOT IssuerInfo.cusip)
    assert filing.issuer_cik == "0001001250"
    assert filing.issuer_name == "The Estee Lauder Companies Inc."
    assert filing.issuer_cusip == "518439104"
    assert filing.securities_class_title == "Class A Common Stock, par value $.01 per share"

    # date_of_event: ISO-parsed when well-formed
    from datetime import date

    assert filing.date_of_event == date(2025, 11, 3)

    # filed_at: adapter leaves NULL; manifest layer fills from row.filed_at
    assert filing.filed_at is None


# Minimal primary_doc.xml carrying only <filerCredentials><cik> — the
# document filer of record the adapter reads for document_filer_cik (#1638).
_RAW_WITH_FILER_CREDENTIALS = (
    '<?xml version="1.0"?><edgarSubmission xmlns="http://www.sec.gov/edgar/schedule13D">'
    "<headerData><filerInfo><filer><filerCredentials><cik>0009999999</cik>"
    "</filerCredentials></filer></filerInfo></headerData></edgarSubmission>"
)


def test_adapter_extracts_document_filer_cik_from_raw_xml() -> None:
    filing = build_filing_from_edgartools_dict(
        _parsed_13d(),
        source="sec_13d",
        manifest_form="SC 13D",
        manifest_filer_cik="2093607",
        raw_xml=_RAW_WITH_FILER_CREDENTIALS,
    )
    # document_filer_cik comes from the raw doc's <filerCredentials>, NOT
    # the manifest CIK (which is the subject/issuer post-#1628).
    assert filing.document_filer_cik == "0009999999"
    assert filing.primary_filer_cik == "0002093607"  # still the manifest arg


def test_adapter_document_filer_cik_none_when_no_raw_xml() -> None:
    filing = build_filing_from_edgartools_dict(
        _parsed_13d(),
        source="sec_13d",
        manifest_form="SC 13D",
        manifest_filer_cik="2093607",
    )
    assert filing.document_filer_cik is None


def test_adapter_maps_reporting_persons_with_decimal_typing() -> None:
    """Per-reporter mapping preserves Decimal typing on share-power +
    aggregate + percent fields (matches NUMERIC schema)."""
    parsed = _parsed_13d()

    filing = build_filing_from_edgartools_dict(
        parsed,
        source="sec_13d",
        manifest_form="SC 13D",
        manifest_filer_cik="0002093607",
    )

    assert len(filing.reporting_persons) == 1
    person = filing.reporting_persons[0]

    assert isinstance(person, BlockholderReportingPerson)
    assert person.cik == "0002093607"  # passthrough since no_cik=False
    assert person.no_cik is False
    assert person.name == "Roaring Fork Trust Company, Inc."
    assert person.member_of_group == "b"
    assert person.type_of_reporting_person == "CO"
    assert person.citizenship == "SD"

    # Decimal typing — NOT int / float — for NUMERIC schema fidelity.
    assert person.sole_voting_power == Decimal("1500000")
    assert isinstance(person.sole_voting_power, Decimal)
    assert person.shared_voting_power == Decimal("0")
    assert person.sole_dispositive_power == Decimal("1500000")
    assert person.shared_dispositive_power == Decimal("0")
    # aggregate_amount_owned ← edgartools .aggregate_amount (NOT
    # .aggregate_amount_owned — Codex 1b HIGH)
    assert person.aggregate_amount_owned == Decimal("1500000")
    assert isinstance(person.aggregate_amount_owned, Decimal)
    # percent_of_class: float in edgartools → Decimal in repo
    assert person.percent_of_class == Decimal("5.5")
    assert isinstance(person.percent_of_class, Decimal)


# ---------------------------------------------------------------------------
# 13G dispatch
# ---------------------------------------------------------------------------


def test_adapter_returns_passive_status_for_sec_13g_source() -> None:
    """``status`` follows the ``_STATUS_FOR_SOURCE`` mapping:
    sec_13g → passive."""
    parsed = _parsed_13g()

    filing = build_filing_from_edgartools_dict(
        parsed,
        source="sec_13g",
        manifest_form="SC 13G",
        manifest_filer_cik="0002083532",
    )

    assert filing.status == "passive"
    assert filing.submission_type == "SCHEDULE 13G"
    # 13G primes ``event_date`` (NOT ``date_of_event``); adapter
    # dispatches on source to read the right key.
    from datetime import date

    assert filing.date_of_event == date(2025, 9, 30)


# ---------------------------------------------------------------------------
# Submission-type derivation table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("manifest_form", "expected"),
    [
        ("SC 13D", "SCHEDULE 13D"),
        ("SC 13D/A", "SCHEDULE 13D/A"),
        ("SC 13G", "SCHEDULE 13G"),
        ("SC 13G/A", "SCHEDULE 13G/A"),
    ],
)
def test_adapter_derives_submission_type_from_manifest_form_table(manifest_form: str, expected: str) -> None:
    """Submission type comes from a closed mapping table (KeyError
    raises loudly on an unknown form; Codex 1b HIGH said adapter must
    NOT silently fall back)."""
    parsed = _parsed_13d() if manifest_form.startswith("SC 13D") else _parsed_13g()
    # 13G dict source for the /A 13G case (mapping table dispatches
    # on the manifest form independent of dict source).
    if manifest_form == "SC 13G/A":
        source = "sec_13g"
    elif manifest_form == "SC 13G":
        source = "sec_13g"
    else:
        source = "sec_13d"

    filing = build_filing_from_edgartools_dict(
        parsed,
        source=source,  # type: ignore[arg-type]
        manifest_form=manifest_form,
        manifest_filer_cik="0001234567",
    )
    assert filing.submission_type == expected


def test_adapter_raises_on_unknown_manifest_form() -> None:
    parsed = _parsed_13d()
    with pytest.raises(KeyError):
        build_filing_from_edgartools_dict(
            parsed,
            source="sec_13d",
            manifest_form="FORM 999",  # not in the mapping
            manifest_filer_cik="0001234567",
        )


# ---------------------------------------------------------------------------
# MM/DD/YYYY date_of_event defensive handling
# ---------------------------------------------------------------------------


def test_adapter_yields_none_date_of_event_for_malformed_string() -> None:
    """``date.fromisoformat`` raises on ``MM/DD/YYYY`` — adapter
    catches ``ValueError`` and yields ``None`` rather than crashing.
    Existing legacy fixtures use MM/DD/YYYY; the in-house parser
    handled it, the edgartools-backed path falls back to ``None``."""
    parsed = _parsed_13d()
    # Replace the parsed date string with a non-ISO format.
    parsed["date_of_event"] = "11/03/2025"

    filing = build_filing_from_edgartools_dict(
        parsed,
        source="sec_13d",
        manifest_form="SC 13D",
        manifest_filer_cik="0002093607",
    )

    assert filing.date_of_event is None


def test_adapter_yields_none_date_of_event_for_empty_string() -> None:
    parsed = _parsed_13d()
    parsed["date_of_event"] = ""

    filing = build_filing_from_edgartools_dict(
        parsed,
        source="sec_13d",
        manifest_form="SC 13D",
        manifest_filer_cik="0002093607",
    )

    assert filing.date_of_event is None


def test_adapter_accepts_full_schedule_form_name() -> None:
    """Codex 1f gap + PR11 v8 empirical pivot: post-2024-12-18 manifest
    rows carry form='SCHEDULE 13D/A' style (not 'SC 13D/A'). Adapter MUST
    map both spellings to the same SubmissionType.

    Live smoke 2026-05-21 on accession 0001104659-26-064061 surfaced this
    as a KeyError before the alias was added — 0 post-mandate accessions
    would parse without it.
    """
    from app.services.manifest_parsers._schedule13_adapter import (
        _SUBMISSION_TYPE_FOR_FORM,
    )

    for short, full in [
        ("SC 13D", "SCHEDULE 13D"),
        ("SC 13D/A", "SCHEDULE 13D/A"),
        ("SC 13G", "SCHEDULE 13G"),
        ("SC 13G/A", "SCHEDULE 13G/A"),
    ]:
        assert _SUBMISSION_TYPE_FOR_FORM[short] == _SUBMISSION_TYPE_FOR_FORM[full] == full


def test_adapter_promotes_no_cik_when_raw_cik_empty() -> None:
    """Bot WARNING iter 2 — sql/095:103-105 semantic convention:
    reporter_cik NULL → reporter_no_cik TRUE. 13G ReportingPerson
    dataclasses carry cik='' + no_cik=False (the per-reporter CIK
    field is not on the 13G cover schema at all). Adapter must promote
    no_cik=True whenever it's writing NULL cik."""
    from types import SimpleNamespace

    fake_person = SimpleNamespace(
        cik="",
        no_cik=False,  # edgartools didn't flag it; we still NULL the CIK
        name="Empty CIK Holder LLC",
        citizenship="DE",
        member_of_group=None,
        type_of_reporting_person="CO",
        sole_voting_power=0,
        shared_voting_power=0,
        sole_dispositive_power=0,
        shared_dispositive_power=0,
        aggregate_amount=0,
        percent_of_class=0.0,
    )
    fake_parsed = {
        "issuer_info": SimpleNamespace(cik="0000999000", name="Issuer Inc.", cusip="X"),
        "security_info": SimpleNamespace(cusip="X", title="Common"),
        "reporting_persons": [fake_person],
        "event_date": "2025-01-01",
    }
    filing = build_filing_from_edgartools_dict(
        fake_parsed, source="sec_13g", manifest_form="SCHEDULE 13G", manifest_filer_cik="0000222000"
    )
    assert len(filing.reporting_persons) == 1
    p = filing.reporting_persons[0]
    assert p.cik is None, "empty raw_cik must coerce to None"
    assert p.no_cik is True, (
        "sql/095:103-105 convention requires reporter_no_cik=TRUE when "
        "reporter_cik IS NULL — adapter must promote even when edgartools "
        f"didn't flag it. Got no_cik={p.no_cik!r} cik={p.cik!r}"
    )
