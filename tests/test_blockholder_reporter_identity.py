"""Pure-logic tests for the 13D/G blockholder reporter-identity resolver
(#1638). No DB. The resolver picks the one observation-row identity for a
13D/G accession: reporter_cik = chosen (max-aggregate) reporter's own CIK,
else the document filer-of-record CIK; reporter_name = chosen reporter.
"""

from __future__ import annotations

from decimal import Decimal

from app.providers.implementations.sec_13dg import BlockholderReportingPerson
from app.services.blockholders import (
    BlockholderReporterIdentity,
    resolve_blockholder_reporter_identity,
)


def _person(
    name: str,
    cik: str | None,
    aggregate: str | None,
    percent: str | None = None,
) -> BlockholderReportingPerson:
    return BlockholderReportingPerson(
        cik=cik,
        no_cik=cik is None,
        name=name,
        member_of_group=None,
        type_of_reporting_person=None,
        citizenship=None,
        sole_voting_power=None,
        shared_voting_power=None,
        sole_dispositive_power=None,
        shared_dispositive_power=None,
        aggregate_amount_owned=Decimal(aggregate) if aggregate is not None else None,
        percent_of_class=Decimal(percent) if percent is not None else None,
    )


def test_13d_uses_chosen_per_reporter_cik() -> None:
    # GME 0000921895-25-000190: Cohen (largest aggregate) carries his own
    # CIK; RC Ventures is the filer of record but reports 0.
    persons = [
        _person("Cohen Ryan", "0001767470", "36847842", "8.2"),
        _person("RC Ventures LLC", "0001822844", "0"),
    ]
    out = resolve_blockholder_reporter_identity(persons, document_filer_cik="0001822844")
    assert out == BlockholderReporterIdentity(
        reporter_cik="0001767470",
        reporter_name="Cohen Ryan",
        aggregate_amount_owned=Decimal("36847842"),
        percent_of_class=Decimal("8.2"),
    )


def test_13g_null_reporter_cik_falls_back_to_document_filer() -> None:
    # Modern 13G omits per-reporter CIK; the filer of record IS the
    # beneficial owner (e.g. Vanguard).
    persons = [_person("The Vanguard Group", None, "100", "9.3")]
    out = resolve_blockholder_reporter_identity(persons, document_filer_cik="0000102909")
    assert out is not None
    assert out.reporter_cik == "0000102909"
    assert out.reporter_name == "The Vanguard Group"
    assert out.aggregate_amount_owned == Decimal("100")


def test_multiparty_13d_null_chosen_cik_uses_filer_of_record() -> None:
    # Group 13D where the largest-aggregate reporter is a natural person
    # with no CIK; a smaller member carries the filer-of-record CIK.
    persons = [
        _person("Mark Getty", None, "500"),
        _person("Getty Investments L.L.C.", "0001056213", "100"),
    ]
    out = resolve_blockholder_reporter_identity(persons, document_filer_cik="0001056213")
    assert out is not None
    assert out.reporter_cik == "0001056213"  # filer of record (joinable)
    assert out.reporter_name == "Mark Getty"  # largest disclosed position
    assert out.aggregate_amount_owned == Decimal("500")


def test_chosen_cik_present_ignores_document_filer() -> None:
    persons = [_person("Acme Fund", "0001234567", "100")]
    out = resolve_blockholder_reporter_identity(persons, document_filer_cik="0009999999")
    assert out is not None
    assert out.reporter_cik == "0001234567"


def test_tie_aggregate_picks_first_in_list() -> None:
    persons = [
        _person("First Filer", "0000000001", "100"),
        _person("Second Filer", "0000000002", "100"),
    ]
    out = resolve_blockholder_reporter_identity(persons, document_filer_cik=None)
    assert out is not None
    assert out.reporter_cik == "0000000001"
    assert out.reporter_name == "First Filer"


def test_empty_string_cik_treated_as_null() -> None:
    persons = [_person("Edge Filer", "", "100")]
    out = resolve_blockholder_reporter_identity(persons, document_filer_cik="0000102909")
    assert out is not None
    assert out.reporter_cik == "0000102909"


def test_no_persons_returns_none() -> None:
    assert resolve_blockholder_reporter_identity([], document_filer_cik="0000102909") is None


def test_chosen_aggregate_none_returns_none() -> None:
    # Defer-to-prior-cover amendment: no aggregate → skip (mirrors the
    # write-side aggregate_amount_owned IS NOT NULL guard).
    persons = [_person("No Aggregate", "0001767470", None)]
    assert resolve_blockholder_reporter_identity(persons, document_filer_cik="0001767470") is None


def test_both_ciks_null_returns_none() -> None:
    persons = [_person("Unjoinable", None, "100")]
    assert resolve_blockholder_reporter_identity(persons, document_filer_cik=None) is None
