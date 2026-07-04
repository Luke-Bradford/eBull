"""Pure-logic tests for the PRE 14A / PRER14A proposal-signal extractor (#1892).

No DB. Six real fixtures ground the happy path across the numbering /
intro-phrasing renderings the full-population smoke check (dev DB, GME / HD /
FFAI / NRDY / SHAZ) surfaced:
  - Faraday Future (special meeting): period-terminated numbering.
  - Earth Science Tech (annual meeting): bare-number table rendering.
  - byNordic Acquisition Corp (SPAC): "Proposal No. N -- ..." numbering.
  - GameStop (annual meeting): "you will be asked to: (1) ... (6) ..."
    parenthesized numbering -- the first-cut extractor missed this real
    large-cap filing entirely (no "following proposals" intro phrase).
  - Home Depot (annual meeting): bare "ITEMS OF BUSINESS" heading (no
    "following" prefix, no colon) directly followed by a
    proposal/recommendation/page-number table collapsed to period-numbered
    text.
  - Nerdy Inc (special meeting, reverse split): singular "purpose ... is the
    following:" intro (not the plural "following purposes" pattern), single
    numbered item.
Synthetic snippets cover the false-positive-avoidance cases the
full-population check (#1892) surfaced: item-scoped classification vs a
whole-document keyword hit, and say-on-pay vs say-on-frequency.
"""

from __future__ import annotations

from pathlib import Path

from app.services.pre14a_proposals import parse_pre14a_proposals

_FIXTURES = Path(__file__).parent / "fixtures" / "pre14a"


def _fixture(name: str) -> str:
    return (_FIXTURES / name).read_text(encoding="utf-8", errors="replace")


# --- Real fixtures -------------------------------------------------------


def test_faraday_special_meeting_three_proposals() -> None:
    """Period-terminated numbering ('1. To approve...'); no reverse split /
    share increase / say-on-pay in this special-meeting agenda."""
    signal = parse_pre14a_proposals(_fixture("faraday_special_pre14a.htm"))
    assert signal is not None
    assert signal.proposal_count == 3
    assert signal.reverse_stock_split_proposal is False
    assert signal.authorized_share_increase_proposal is False
    assert signal.say_on_pay_advisory_vote is False
    assert "Private Placement Proposal" in signal.agenda_items[0]
    assert "Name Change Proposal" in signal.agenda_items[1]


def test_earthsci_annual_meeting_reverse_split_and_say_on_pay() -> None:
    """Bare-number table-rendered numbering ('1 A proposal to...'); six
    proposals including a reverse-split advisory vote (item 4) and a
    say-on-pay advisory vote (item 5). Item 6 is the DISTINCT say-on-
    FREQUENCY vote — must NOT also flag say_on_pay_advisory_vote true."""
    signal = parse_pre14a_proposals(_fixture("earthsci_annual_pre14a.htm"))
    assert signal is not None
    assert signal.proposal_count == 6
    assert signal.reverse_stock_split_proposal is True
    assert signal.authorized_share_increase_proposal is False
    assert signal.say_on_pay_advisory_vote is True
    assert "reverse stock split" in signal.agenda_items[3].lower()
    assert "frequency" in signal.agenda_items[5].lower()


def test_bynordic_spac_extension_and_adjournment() -> None:
    """'Proposal No. N — ...' numbering (SPAC extension proxy); neither
    proposal matches any of the three categories."""
    signal = parse_pre14a_proposals(_fixture("bynordic_spac_pre14a.htm"))
    assert signal is not None
    assert signal.proposal_count == 2
    assert signal.reverse_stock_split_proposal is False
    assert signal.authorized_share_increase_proposal is False
    assert signal.say_on_pay_advisory_vote is False
    assert "Extension Amendment Proposal" in signal.agenda_items[0]
    assert "Adjournment Proposal" in signal.agenda_items[1]


def test_gme_annual_meeting_parenthesized_numbering_and_share_increase() -> None:
    """ "you will be asked to: (1) ... (6) ..." intro + parenthesized item
    numbering (no "following proposals" phrase at all). Item 5 is a genuine
    authorized-share-increase proposal; item 2 is say-on-pay."""
    signal = parse_pre14a_proposals(_fixture("gme_annual_pre14a.htm"))
    assert signal is not None
    assert signal.proposal_count == 6
    assert signal.reverse_stock_split_proposal is False
    assert signal.authorized_share_increase_proposal is True
    assert signal.say_on_pay_advisory_vote is True
    assert "increase" in signal.agenda_items[4].lower()


def test_hd_annual_meeting_bare_items_of_business_heading() -> None:
    """Bare "ITEMS OF BUSINESS" heading (no "following" prefix, no colon)
    directly followed by a proposal/recommendation/page-number table
    collapsed to period-numbered text. Item 3 is say-on-pay."""
    signal = parse_pre14a_proposals(_fixture("hd_annual_pre14a.htm"))
    assert signal is not None
    assert signal.proposal_count == 8
    assert signal.reverse_stock_split_proposal is False
    assert signal.authorized_share_increase_proposal is False
    assert signal.say_on_pay_advisory_vote is True


def test_nrdy_singular_purpose_intro_single_item_reverse_split() -> None:
    """Singular "purpose ... is the following:" intro (not the plural
    "following purposes" pattern) with a single numbered agenda item."""
    signal = parse_pre14a_proposals(_fixture("nrdy_reverse_split_pre14a.htm"))
    assert signal is not None
    assert signal.proposal_count == 1
    assert signal.reverse_stock_split_proposal is True
    assert signal.authorized_share_increase_proposal is False
    assert signal.say_on_pay_advisory_vote is False


# --- Synthetic edge cases --------------------------------------------------


def test_no_intro_sentence_returns_none() -> None:
    """A body with no 'following proposals/items of business' intro sentence
    is not a recognizable Rule 14a-4(a)(3) notice — caller tombstones."""
    body = "<html><body>Some unrelated proxy boilerplate with no agenda list.</body></html>"
    assert parse_pre14a_proposals(body) is None


def test_whole_document_keyword_hit_outside_agenda_not_classified() -> None:
    """#1892 full-population finding: a risk-factor mention of 'authorized
    shares' OUTSIDE the numbered agenda list must not flag
    authorized_share_increase_proposal — classification is item-scoped."""
    body = """
    <html><body>
    RISK FACTORS: if the Company needed to increase its authorized shares
    of common stock, it would be required to seek stockholder approval.
    The Annual Meeting will be held for the purpose of considering and
    voting on the following proposals:
    1. To elect two directors.
    2. To ratify the appointment of the independent auditor.
    Each Proposal is more fully described in the accompanying proxy statement.
    </body></html>
    """
    signal = parse_pre14a_proposals(body)
    assert signal is not None
    assert signal.proposal_count == 2
    assert signal.authorized_share_increase_proposal is False


def test_authorized_share_increase_proposal_detected_in_item() -> None:
    body = """
    <html><body>
    The meeting will be held for the purpose of voting on the following
    proposals:
    1. To approve an amendment to the Certificate of Incorporation to
    increase the number of authorized shares of common stock from
    100,000,000 to 500,000,000.
    2. To approve one or more adjournments of the meeting.
    Each Proposal is more fully described in the accompanying proxy statement.
    </body></html>
    """
    signal = parse_pre14a_proposals(body)
    assert signal is not None
    assert signal.proposal_count == 2
    assert signal.authorized_share_increase_proposal is True
    assert signal.reverse_stock_split_proposal is False


def test_reverse_stock_split_proposal_detected_in_item() -> None:
    body = """
    <html><body>
    The meeting will be held for the purpose of voting on the following
    proposals:
    1. To approve an amendment to effect a reverse stock split of the
    Company's common stock at a ratio to be determined by the Board.
    2. To approve one or more adjournments of the meeting.
    Each Proposal is more fully described in the accompanying proxy statement.
    </body></html>
    """
    signal = parse_pre14a_proposals(body)
    assert signal is not None
    assert signal.reverse_stock_split_proposal is True
    assert signal.authorized_share_increase_proposal is False


def test_say_on_frequency_alone_not_classified_as_say_on_pay() -> None:
    """Rule 14a-21(b) say-on-frequency is a distinct proposal category
    #1892 does not ask for — must not be misclassified as say-on-pay."""
    body = """
    <html><body>
    The meeting will be held for the purpose of voting on the following
    proposals:
    1. A proposal to approve, on an advisory basis, the frequency of
    future advisory votes on executive compensation.
    2. To approve one or more adjournments of the meeting.
    Each Proposal is more fully described in the accompanying proxy statement.
    </body></html>
    """
    signal = parse_pre14a_proposals(body)
    assert signal is not None
    assert signal.say_on_pay_advisory_vote is False


def test_item_text_bounded_length() -> None:
    """Each retained agenda-item string is capped (operator/LLM context,
    not a document store — mirrors nt_notices._REASON_MAX discipline)."""
    long_clause = "extremely long boilerplate text " * 200
    body = f"""
    <html><body>
    The meeting will be held for the purpose of voting on the following
    proposals:
    1. To approve an amendment. {long_clause}
    Each Proposal is more fully described in the accompanying proxy statement.
    </body></html>
    """
    signal = parse_pre14a_proposals(body)
    assert signal is not None
    assert len(signal.agenda_items[0]) <= 2000
