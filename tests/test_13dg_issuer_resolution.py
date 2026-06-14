"""13D/G issuer-identity extraction + adapter backfill (#1628) — pure.

The modern (post-2024-12-18 mandate) Schedule 13D/G XML nests the issuer
CUSIP as ``<issuerCusips>/<issuerCusipNumber>``. edgartools 5.30.2 and
the legacy in-house extractors read the stale flat ``<issuerCUSIP>``
element and returned an empty CUSIP for every modern filing, so
CUSIP-only resolution never resolved and blockholders never populated.

These tests pin the correct-tag extractor + CUSIP normalisation and the
adapter's helper-backfill / edgartools-preserve behaviour. They are pure
(no DB) so they run on the fast push gate. The DB-backed resolver matrix
lives in ``tests/test_13dg_resolver_db.py``.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.providers.implementations.sec_13dg import (
    IssuerIdentity,
    _normalise_cusip,
    extract_issuer_identity_from_primary_doc,
)
from app.services.manifest_parsers._schedule13_adapter import (
    build_filing_from_edgartools_dict,
)

# ---------------------------------------------------------------------------
# _normalise_cusip
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("15117f880", "15117F880"),  # upper-cased
        ("  15117F880  ", "15117F880"),  # stripped
        ("15117F880", "15117F880"),
        ("G0R21F121", "G0R21F121"),  # CINS (foreign) — leading letter is alnum
        (None, None),
        ("", None),
        ("123", None),  # too short
        ("1234567890", None),  # too long (10)
        ("15117F88!", None),  # non-alphanumeric
    ],
)
def test_normalise_cusip(raw: str | None, expected: str | None) -> None:
    assert _normalise_cusip(raw) == expected


# ---------------------------------------------------------------------------
# extract_issuer_identity_from_primary_doc
# ---------------------------------------------------------------------------


def _doc(issuer_inner: str, cover_inner: str = "") -> str:
    return (
        "<edgarSubmission><formData>"
        f"<issuerInfo>{issuer_inner}</issuerInfo>"
        f"<coverPageHeader>{cover_inner}</coverPageHeader>"
        "</formData></edgarSubmission>"
    )


def test_extract_reads_unified_mandate_schema() -> None:
    xml = _doc(
        "<issuerCik>0001279704</issuerCik>"
        "<issuerCusips><issuerCusipNumber>15117F880</issuerCusipNumber></issuerCusips>"
        "<issuerName>CELLECTAR BIOSCIENCES, INC.</issuerName>",
        "<securitiesClassTitle>Common Stock</securitiesClassTitle>",
    )
    assert extract_issuer_identity_from_primary_doc(xml) == IssuerIdentity(
        cik="0001279704",
        cusip="15117F880",
        name="CELLECTAR BIOSCIENCES, INC.",
        class_title="Common Stock",
    )


def test_extract_legacy_flat_tag_fallback_and_cik_zero_pad() -> None:
    # Pre-unified fixtures used the flat <issuerCIK>/<issuerCUSIP> tags.
    xml = _doc("<issuerCIK>37912</issuerCIK><issuerCUSIP>518439104</issuerCUSIP>")
    ident = extract_issuer_identity_from_primary_doc(xml)
    assert ident.cik == "0000037912"  # zero-padded to 10
    assert ident.cusip == "518439104"


def test_extract_missing_cusip_is_none_cik_preserved() -> None:
    ident = extract_issuer_identity_from_primary_doc(_doc("<issuerCik>0000037912</issuerCik>"))
    assert ident.cik == "0000037912"
    assert ident.cusip is None


def test_extract_malformed_cusip_normalised_to_none() -> None:
    xml = _doc(
        "<issuerCik>0000037912</issuerCik><issuerCusips><issuerCusipNumber>abc</issuerCusipNumber></issuerCusips>"
    )
    assert extract_issuer_identity_from_primary_doc(xml).cusip is None


def test_extract_non_numeric_cik_is_none() -> None:
    assert extract_issuer_identity_from_primary_doc(_doc("<issuerCik>x12</issuerCik>")).cik is None


def test_extract_unparseable_xml_returns_all_none() -> None:
    assert extract_issuer_identity_from_primary_doc("<not-closed") == IssuerIdentity(None, None, None, None)


# ---------------------------------------------------------------------------
# Adapter backfill (SimpleNamespace fakes the edgartools parse dict)
# ---------------------------------------------------------------------------

_UNIFIED_XML = _doc(
    "<issuerCik>0000037912</issuerCik>"
    "<issuerCusips><issuerCusipNumber>518439104</issuerCusipNumber></issuerCusips>"
    "<issuerName>ACME INC</issuerName>"
)


def _fake_parsed(*, edgartools_cusip: str = "") -> dict:
    person = SimpleNamespace(
        name="Holder LP",
        cik="0000123456",
        no_cik=False,
        member_of_group="b",
        type_of_reporting_person="IN",
        citizenship="DE",
        sole_voting_power=1000,
        shared_voting_power=0,
        sole_dispositive_power=1000,
        shared_dispositive_power=0,
        aggregate_amount=1000,
        percent_of_class=5,
    )
    return {
        "issuer_info": SimpleNamespace(cik="0000037912", name="ACME INC", cusip=""),
        "security_info": SimpleNamespace(cusip=edgartools_cusip, title="Common Stock"),
        "reporting_persons": [person],
        "date_of_event": "2026-01-15",
    }


def _build(parsed: dict, raw_xml: str | None):
    return build_filing_from_edgartools_dict(
        parsed,
        source="sec_13d",
        manifest_form="SC 13D",
        manifest_filer_cik="0000999999",
        raw_xml=raw_xml,
    )


def test_adapter_backfills_cusip_from_raw_xml() -> None:
    # edgartools returns an empty CUSIP (the bug); the adapter backfills
    # it from the unified mandate schema in the raw body.
    filing = _build(_fake_parsed(edgartools_cusip=""), _UNIFIED_XML)
    assert filing.issuer_cusip == "518439104"
    assert filing.issuer_cik == "0000037912"


def test_adapter_preserves_edgartools_cusip_when_no_raw_xml() -> None:
    filing = _build(_fake_parsed(edgartools_cusip="999999999"), None)
    assert filing.issuer_cusip == "999999999"  # helper not run — edgartools value kept


def test_adapter_helper_none_preserves_edgartools_cusip() -> None:
    # raw_xml present but carries no issuer CUSIP -> helper.cusip is None
    # -> the non-empty edgartools value is preserved (never nulled out).
    bare = _doc("<issuerCik>0000037912</issuerCik>")
    filing = _build(_fake_parsed(edgartools_cusip="888888888"), bare)
    assert filing.issuer_cusip == "888888888"
