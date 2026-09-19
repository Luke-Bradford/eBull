"""#3236 — the drift gate's identity sequence. Pure logic, no DB.

The sweep repairs a stranded 13D/G accession by re-parsing its STORED body and
writing the observation ingest skipped. That is only sound if the re-parse
still reads the body the way ingest did, so the gate compares the re-parse
against the already-stored typed rows.

Two properties of that comparison are load-bearing and neither is obvious:

  * **Order matters.** ``resolve_blockholder_reporter_identity`` picks with
    ``max()``, which returns the FIRST element among equals. A tie on the
    largest ``aggregate_amount_owned`` is the MAJORITY condition on the
    affected population — 13 of 19 accessions measured on dev 2026-09-19 —
    so a reordering silently changes the reporter CIK, name and percent while
    a sorted-multiset comparison sees nothing.
  * **Scale matters.** ``blockholder_filings`` stores these as
    ``numeric(_, 4)``, so Postgres rounds on store. Comparing an unrounded
    parse against a stored value would mark the accession ``drifted``
    forever — a permanent skip caused by the gate, not by the data.
"""

from __future__ import annotations

from decimal import Decimal

from app.providers.implementations.sec_13dg import BlockholderReportingPerson
from app.services.blockholders import (
    _identity_sequence,
    resolve_blockholder_reporter_identity,
)


def _person(name: str, cik: str | None, aggregate: str, percent: str = "5.0") -> BlockholderReportingPerson:
    return BlockholderReportingPerson(
        cik=cik,
        no_cik=cik is None,
        name=name,
        member_of_group="b",
        type_of_reporting_person="CO",
        citizenship="DE",
        sole_voting_power=Decimal(aggregate),
        shared_voting_power=None,
        sole_dispositive_power=Decimal(aggregate),
        shared_dispositive_power=None,
        aggregate_amount_owned=Decimal(aggregate),
        percent_of_class=Decimal(percent),
    )


def test_reordering_tied_reporters_changes_the_identity_but_not_the_multiset() -> None:
    """The reason the gate is order-sensitive, stated as a test.

    Both orderings hold the same reporters at the same amounts, so as SETS
    they are identical — yet they resolve to DIFFERENT observation identities.
    """
    first = _person("ALPHA LP", "0000000111", "1000")
    second = _person("BETA LP", "0000000222", "1000")

    ident_a = resolve_blockholder_reporter_identity([first, second], document_filer_cik="0000000999")
    ident_b = resolve_blockholder_reporter_identity([second, first], document_filer_cik="0000000999")

    assert ident_a is not None and ident_b is not None
    assert ident_a.reporter_cik == "0000000111"
    assert ident_b.reporter_cik == "0000000222"
    assert ident_a.reporter_cik != ident_b.reporter_cik

    # A set comparison cannot tell these apart...
    assert set(_identity_sequence([first, second])) == set(_identity_sequence([second, first]))
    # ...but the sequence the gate actually compares can.
    assert _identity_sequence([first, second]) != _identity_sequence([second, first])


def test_identity_sequence_quantises_to_the_stored_column_scale() -> None:
    """A parse carrying more precision than ``numeric(_, 4)`` must still
    compare equal to its stored form, or the accession is skipped forever."""
    parsed = _person("ALPHA LP", "0000000111", "1000", percent="5.123456")
    stored_equivalent = [("0000000111", "ALPHA LP", Decimal("1000.0000"), Decimal("5.1235"))]

    assert _identity_sequence([parsed]) == stored_equivalent


def test_identity_sequence_preserves_trailing_zero_equality() -> None:
    """``Decimal`` equality is numeric, so the stored ``1000.0000`` form and a
    parsed ``1000`` must not read as drift."""
    assert _identity_sequence([_person("A", "0000000111", "1000")])[0][2] == Decimal("1000.0000")


def test_identity_sequence_carries_a_no_cik_reporter_as_none() -> None:
    """Natural persons / trusts file without a CIK; the sequence must keep the
    ``None`` rather than coercing it, because the stored column is NULL."""
    assert _identity_sequence([_person("THE LAL 2015 ELF TRUST", None, "1500000")])[0][0] is None
