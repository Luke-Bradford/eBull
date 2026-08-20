"""Pure-logic tests for the tender / going-private extractor (#1982).

No DB. Five real fixtures ground the observed schedule shapes: NUVL SC TO-T
(dual-party header, modern glyphs, duplicate-label filer typo, "$124.00 per
Share, net" price), DSX SC TO-T/A (legacy ``x``/``¨`` glyphs, Amendment No.
18, amends-13D box), IQ SC TO-I (issuer self-tender, Notes class, no priced
formula), LPRO SC 14D9 (self-filed both-blocks header, recommendation=accept,
"$3.15 per Share", offer expiration), BALY SC 13E3 (no Schedule TO
transaction boxes — all four NULL). Synthetic snippets cover the
spec-mandated edge cases (par-value-only body, conflicting prices, unmatched
recommendation prose, unusable header).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from app.services.tender_offers import (
    HeaderParty,
    parse_tender_offer,
    resolve_party_roles,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "tender"


def _fixture(name: str) -> str:
    return (_FIXTURES / name).read_text(encoding="utf-8", errors="replace")


_MINIMAL_HDR = """<SEC-HEADER>0000000000-26-000001.hdr.sgml : 20260101
<SUBJECT-COMPANY>
<COMPANY-DATA>
<CONFORMED-NAME>Target Corp Test
<CIK>0001000001
</COMPANY-DATA>
</SUBJECT-COMPANY>
<FILED-BY>
<COMPANY-DATA>
<CONFORMED-NAME>Acquirer LLC Test
<CIK>0001000002
</COMPANY-DATA>
</FILED-BY>
</SEC-HEADER>"""

_MINIMAL_BODY = "<html><body>SCHEDULE TO Tender Offer Statement {extra}</body></html>"


# --- Real fixtures ----------------------------------------------------------


def test_nuvl_sctot_dual_party_third_party_tender() -> None:
    """Dual-party TO-T: subject + offeror from the SGML header blocks; price
    from the "for $124.00 per Share, net" formula; duplicate third-party
    label (filer typo — one checked, one not) resolves checked-anywhere-wins;
    the issuer-tender line is ABSENT from this cover (the typo replaced it)
    so the boolean is honestly NULL (accession 0001193125-26-280246)."""
    p = parse_tender_offer(_fixture("nuvl_sctot.htm"), _fixture("nuvl_sctot.hdr.sgml"), "SC TO-T")
    assert p is not None
    assert p.subject == HeaderParty(cik="0001861560", name="Nuvalent, Inc.")
    assert p.filed_by == (HeaderParty(cik="0001131399", name="GSK plc"),)
    assert p.is_third_party_tender is True
    assert p.is_issuer_tender is None
    assert p.is_going_private is False
    assert p.amends_13d is False
    assert p.is_final_amendment is False
    assert p.amendment_no is None
    assert p.offer_price_per_unit == Decimal("124.00")
    assert p.unit_label == "Share"
    assert p.currency == "USD"
    # Price lives in the Offer to Purchase exhibit's expiration prose, not
    # this Schedule TO body — expiration honestly NULL.
    assert p.expiration_date is None
    assert p.board_recommendation is None
    assert p.cusip == "670703107"
    assert p.security_class_title is not None and "Class A Common Stock" in p.security_class_title


def test_dsx_sctota_legacy_glyphs_and_amendment() -> None:
    """Legacy filer-agent glyphs (``x`` checked / ``¨`` unchecked), Amendment
    No. 18, amends-Schedule-13D box checked; the "Debt Commitment Letter
    expires on September 30, 2026" sentence must NOT be read as the offer
    expiration (accession 0001104659-26-079410)."""
    p = parse_tender_offer(_fixture("dsx_sctota.htm"), _fixture("dsx_sctota.hdr.sgml"), "SC TO-T/A")
    assert p is not None
    assert p.subject.cik == "0001326200"  # Genco (subject)
    assert p.filed_by[0].cik == "0001318885"  # Diana Shipping (offeror)
    assert p.is_third_party_tender is True
    assert p.is_issuer_tender is False
    assert p.amends_13d is True
    assert p.amendment_no == 18
    assert p.offer_price_per_unit == Decimal("24.80")
    assert p.expiration_date is None
    assert p.cusip == "Y2685T131"


def test_iq_sctoi_self_filed_issuer_tender() -> None:
    """Issuer self-tender: SUBJECT-COMPANY and FILED-BY both = iQIYI; the
    issuer-tender box is checked; the note repurchase prices as % of
    principal so the per-unit price is honestly NULL (accession
    0001193125-26-043648)."""
    p = parse_tender_offer(_fixture("iq_sctoi.htm"), _fixture("iq_sctoi.hdr.sgml"), "SC TO-I")
    assert p is not None
    assert p.subject.cik == "0001722608"
    assert p.filed_by == (HeaderParty(cik="0001722608", name="iQIYI, Inc."),)
    assert resolve_party_roles(p) == {"0001722608": "subject"}  # both-blocks collapse
    assert p.is_issuer_tender is True
    assert p.is_third_party_tender is False
    assert p.offer_price_per_unit is None
    assert p.currency is None  # never defaulted without a matched price
    assert p.security_class_title is not None and "Convertible Senior Notes" in p.security_class_title


def test_lpro_sc14d9_recommendation_price_expiration() -> None:
    """14D-9: Item 1012(a) recommendation=accept; price via "purchase price
    of $3.15 per Share, net"; expiration through the "one minute after 11:59
    p.m. ... on July 27, 2026" formula (periods inside the gap); no Schedule
    TO transaction boxes ⇒ all four NULL (accession 0001193125-26-286952)."""
    p = parse_tender_offer(_fixture("lpro_sc14d9.htm"), _fixture("lpro_sc14d9.hdr.sgml"), "SC 14D9")
    assert p is not None
    assert p.subject.cik == "0001806201"
    assert resolve_party_roles(p) == {"0001806201": "subject"}
    assert p.board_recommendation == "accept"
    assert p.offer_price_per_unit == Decimal("3.15")
    assert p.unit_label == "Share"
    assert p.expiration_date == date(2026, 7, 27)
    assert p.is_third_party_tender is None
    assert p.is_issuer_tender is None
    assert p.is_going_private is None
    assert p.amends_13d is None


def test_bally_sc13e3_no_schedule_to_boxes() -> None:
    """13E-3 cover carries its own a-d context boxes, not the Schedule TO
    transaction-type labels ⇒ all four transaction booleans NULL (never
    inferred from the form type); the final-amendment sentence IS present and
    unchecked (accession 0001213900-24-073431)."""
    p = parse_tender_offer(_fixture("bally_sc13e3.htm"), _fixture("bally_sc13e3.hdr.sgml"), "SC 13E3")
    assert p is not None
    assert p.subject.name == "Bally's Corp"
    assert p.is_third_party_tender is None
    assert p.is_issuer_tender is None
    assert p.is_going_private is None
    assert p.is_final_amendment is False
    assert p.board_recommendation is None  # not a 14D-9
    assert p.cusip == "05875C"


# --- Synthetic edge cases ---------------------------------------------------


def test_par_value_only_body_yields_null_price() -> None:
    """Every cover carries "par value $0.01 per share" — it must never be
    read as the offer price."""
    body = _MINIMAL_BODY.format(extra="Common Stock, par value $0.01 per share, of Target Corp Test.")
    p = parse_tender_offer(body, _MINIMAL_HDR, "SC TO-T")
    assert p is not None
    assert p.offer_price_per_unit is None
    assert p.currency is None


def test_conflicting_prices_yield_null() -> None:
    body = _MINIMAL_BODY.format(
        extra=(
            "to purchase all Shares for $10.00 per Share, net to the seller in cash. "
            "Later restated: to purchase all Shares for $12.00 per Share, net to the seller in cash."
        )
    )
    p = parse_tender_offer(body, _MINIMAL_HDR, "SC TO-T")
    assert p is not None
    assert p.offer_price_per_unit is None


def test_price_match_carries_currency_and_unit() -> None:
    body = _MINIMAL_BODY.format(extra="offer to purchase all ADSs for $5.25 per ADS, net to the seller in cash.")
    p = parse_tender_offer(body, _MINIMAL_HDR, "SC TO-T")
    assert p is not None
    assert p.offer_price_per_unit == Decimal("5.25")
    assert p.unit_label == "ADS"
    assert p.currency == "USD"


def test_duplicate_checkbox_label_checked_anywhere_wins() -> None:
    body = _MINIMAL_BODY.format(
        extra=(
            "☐ Third-party tender offer subject to Rule 14d-1. "
            "☒ Third-party tender offer subject to Rule 14d-1. "
            "☐ Issuer tender offer subject to Rule 13e-4."
        )
    )
    p = parse_tender_offer(body, _MINIMAL_HDR, "SC TO-T")
    assert p is not None
    assert p.is_third_party_tender is True
    assert p.is_issuer_tender is False
    assert p.is_going_private is None  # label absent ⇒ NULL


def test_unmatched_recommendation_prose_stays_null() -> None:
    body = _MINIMAL_BODY.format(
        extra="SCHEDULE 14D-9 The board is carefully considering the offer and will respond in due course."
    )
    p = parse_tender_offer(body, _MINIMAL_HDR, "SC 14D9")
    assert p is not None
    assert p.board_recommendation is None


def test_recommendation_only_extracted_for_14d9() -> None:
    """Item 1012(a) is a 14D-9 item; the same words in a TO-T body (quoting
    the target's statement) must not populate the field."""
    body = _MINIMAL_BODY.format(extra="the target board recommends that stockholders accept the offer.")
    to_t = parse_tender_offer(body, _MINIMAL_HDR, "SC TO-T")
    d9 = parse_tender_offer(body.replace("SCHEDULE TO", "SCHEDULE 14D-9"), _MINIMAL_HDR, "SC 14D9")
    assert to_t is not None and to_t.board_recommendation is None
    assert d9 is not None and d9.board_recommendation == "accept"


def test_unusable_header_returns_none() -> None:
    body = _MINIMAL_BODY.format(extra="")
    assert parse_tender_offer(body, "<SEC-HEADER>no party blocks</SEC-HEADER>", "SC TO-T") is None


def test_unrecognizable_body_returns_none() -> None:
    assert parse_tender_offer("<html><body>hello world</body></html>", _MINIMAL_HDR, "SC TO-T") is None


def test_out_of_scope_form_raises() -> None:
    with pytest.raises(ValueError, match="form must be one of"):
        parse_tender_offer(_MINIMAL_BODY.format(extra=""), _MINIMAL_HDR, "PREM14C")


def test_resolve_party_roles_dual_party() -> None:
    p = parse_tender_offer(_MINIMAL_BODY.format(extra=""), _MINIMAL_HDR, "SC TO-T")
    assert p is not None
    assert resolve_party_roles(p) == {"0001000001": "subject", "0001000002": "offeror"}
