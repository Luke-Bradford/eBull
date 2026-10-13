"""#3624 slice 2: the closed ``.hdr.sgml`` grammar fails closed on every checkpoint-1 counterexample (rounds 8-12)."""

from __future__ import annotations

import pytest
from edgar.sgml.sgml_parser import SubmissionFormatParser

from scripts.measure_3624_slice2_8k_items import field_outcomes, parse_header, row_class

ACCESSION = "0000003545-23-000037"
CIK = "0000003545"
#: A real cached header (ALICO, 8-K, 2023-12-14), trimmed to the elements the fixtures mutate.
VALID = """<SEC-HEADER>0000003545-23-000037.hdr.sgml : 20231214
<ACCEPTANCE-DATETIME>20231214171228
<ACCESSION-NUMBER>0000003545-23-000037
<TYPE>8-K
<PUBLIC-DOCUMENT-COUNT>14
<PERIOD>20231208
<ITEMS>4.01
<ITEMS>9.01
<FILING-DATE>20231214
<DATE-OF-FILING-DATE-CHANGE>20231214
<FILER>
<COMPANY-DATA>
<CONFORMED-NAME>ALICO, INC.
<CIK>0000003545
<ASSIGNED-SIC>0100
</COMPANY-DATA>
<FILING-VALUES>
<FORM-TYPE>8-K
<ACT>34
</FILING-VALUES>
<BUSINESS-ADDRESS>
<ADDRESS-IS-A-NON-US-LOCATION>YES
<CITY>FT. MYERS,
</BUSINESS-ADDRESS>
<FORMER-COMPANY>
<FORMER-CONFORMED-NAME>ALICO INC
<DATE-CHANGED>19920703
</FORMER-COMPANY>
</FILER>
</SEC-HEADER>
"""


def _replace(old: str, new: str) -> str:
    assert VALID.count(old) == 1, old
    return VALID.replace(old, new)


def test_valid_header_is_accepted_and_read_at_its_paths() -> None:
    header = parse_header(VALID)
    assert header["structure_defect"] is None
    assert header["items"] == ["4.01", "9.01"]
    assert header["filer_ciks"] == [CIK]
    assert header["acceptance_et"] == "20231214171228"
    assert row_class(field_outcomes(header, ACCESSION, CIK)) == "ok"


#: ``(case, text, cause)``: each round 8-12 counterexample and one case per cause.
CASES: list[tuple[str, str, str]] = [
    # 163: two CIKs in one company data; items placed inside company data.
    ("two_ciks", _replace("<CIK>0000003545\n", "<CIK>0000003545\n<CIK>0000099999\n"), "repeated"),
    (
        "items_in_company_data",
        _replace("<ITEMS>4.01\n<ITEMS>9.01\n", "").replace("<ASSIGNED-SIC>", "<ITEMS>4.02\n<ASSIGNED-SIC>"),
        "misplaced",
    ),
    # 164: continuation text after an item, an acceptance and a filing date.
    ("items_continuation", _replace("<ITEMS>4.01\n", "<ITEMS>1.01\n4.02\n"), "non_tag_line"),
    (
        "acceptance_continuation",
        _replace("<ACCEPTANCE-DATETIME>20231214171228\n", "<ACCEPTANCE-DATETIME>20231214171228\n20231215\n"),
        "non_tag_line",
    ),
    ("filing_date_continuation", _replace("<FILING-DATE>20231214\n", "<FILING-DATE>20231214\n1\n"), "non_tag_line"),
    # 165: an element outside the grammar, balanced or not, is never read as a leaf.
    (
        "unclosed_merger_series",
        _replace("<FILER>\n", "<MERGER-SERIES-AND-CLASSES-CONTRACTS>\n<FILER>\n"),
        "not_admitted",
    ),
    # Round 10: a space or lower case inside the brackets, a nested role, a missing role close.
    ("items_tag_with_space", _replace("<ITEMS>4.01", "<ITEMS >4.02"), "not_admitted"),
    ("lowercase_paper", _replace("<FILER>\n", "<paper >\n<FILER>\n"), "not_admitted"),
    ("lowercase_items", _replace("<ITEMS>4.01", "<items>4.02"), "not_admitted"),
    (
        "nested_subject_company",
        _replace(
            "</FILER>\n",
            "<SUBJECT-COMPANY>\n<COMPANY-DATA>\n<CIK>0000099999\n</COMPANY-DATA>\n</SUBJECT-COMPANY>\n</FILER>\n",
        ),
        "not_admitted",
    ),
    ("missing_filer_close", _replace("</FILER>\n", ""), "unbalanced"),
    # Envelope, values and identity.
    ("text_before_envelope", "preamble\n" + VALID, "envelope"),
    ("text_after_envelope", VALID + "<ITEMS>4.02\n", "outside_envelope"),
    ("envelope_without_file_line", _replace(f"{ACCESSION}.hdr.sgml : 20231214", ""), "envelope"),
    ("value_on_empty_element", _replace("<ACCESSION-NUMBER>", "<PAPER>NO\n<ACCESSION-NUMBER>"), "value_not_permitted"),
    ("markup_in_value", _replace("<CITY>FT. MYERS,", "<CITY>FT. <MYERS"), "value_not_permitted"),
    ("leading_blank_before_tag", _replace("<ITEMS>4.01", " <ITEMS>4.01"), "non_tag_line"),
    (
        "repeated_acceptance",
        _replace("<ACCEPTANCE-DATETIME>20231214171228\n", "<ACCEPTANCE-DATETIME>20231214171228\n" * 2),
        "repeated",
    ),
    (
        "two_company_data",
        _replace("</COMPANY-DATA>\n", "</COMPANY-DATA>\n<COMPANY-DATA>\n<CIK>0000099999\n</COMPANY-DATA>\n"),
        "repeated",
    ),
    (
        "no_company_data",
        _replace(
            "<COMPANY-DATA>\n<CONFORMED-NAME>ALICO, INC.\n<CIK>0000003545\n<ASSIGNED-SIC>0100\n</COMPANY-DATA>\n",
            "",
        ),
        "missing",
    ),
    ("company_data_without_cik", _replace("<CIK>0000003545\n", ""), "missing"),
    # Round 12 (168): order, required elements and DTD groups; (170) an empty body.
    (
        "items_after_filer",
        _replace("<ITEMS>4.01\n<ITEMS>9.01\n", "").replace("</FILER>\n", "</FILER>\n<ITEMS>4.01\n"),
        "order",
    ),
    ("missing_conformed_name", _replace("<CONFORMED-NAME>ALICO, INC.\n", ""), "missing"),
    ("missing_filing_date", _replace("<FILING-DATE>20231214\n", ""), "missing"),
    ("file_number_without_film_number", _replace("<ACT>34\n", "<ACT>34\n<FILE-NUMBER>000-00261\n"), "group"),
    ("correction_without_timestamp", _replace("<ACCESSION-NUMBER>", "<CORRECTION>\n<ACCESSION-NUMBER>"), "group"),
    (
        "deletion_in_two_places",
        _replace("<TYPE>8-K\n", "<DELETION>\n<TYPE>8-K\n").replace("<COMPANY-DATA>\n", "<DELETION>\n<COMPANY-DATA>\n"),
        "deletion_places",
    ),
    ("empty_body", "", "envelope"),
    ("blank_body", " \n\n\t\n", "envelope"),
    ("unclosed_envelope", _replace("</SEC-HEADER>\n", ""), "unclosed"),
]


@pytest.mark.parametrize(("case", "text", "cause"), CASES)
def test_counterexample_is_malformed_and_items_unestablished(case: str, text: str, cause: str) -> None:
    header = parse_header(text)
    assert str(header["structure_defect"]).split("@")[0] == cause, case
    assert row_class(field_outcomes(header, ACCESSION, CIK)) == "items_unestablished", case


def test_correction_evidence_is_detected_inside_a_valid_header() -> None:
    header = parse_header(_replace("<ACCESSION-NUMBER>", "<CORRECTION>\n<TIMESTAMP>20231215\n<ACCESSION-NUMBER>"))
    assert header["structure_defect"] is None
    assert header["correction_tags"] == ["CORRECTION", "TIMESTAMP"]
    assert row_class(field_outcomes(header, ACCESSION, CIK)) == "items_unestablished"


@pytest.mark.parametrize(
    ("year", "dated"),
    [
        ("2022", ("ACCEPTANCE-DATETIME", "FILING-DATE", "DATE-OF-FILING-DATE-CHANGE")),
        ("1923", ("ACCEPTANCE-DATETIME", "DATE-OF-FILING-DATE-CHANGE")),
    ],
)
def test_acceptance_outside_the_accession_year_is_unusable(year: str, dated: tuple[str, ...]) -> None:
    """167: a well-formed acceptance outside the accession's year (``20YY``) contradicts it and bounds nothing."""
    text = VALID
    for tag in dated:
        text = text.replace(f"<{tag}>2023", f"<{tag}>{year}")
    header = parse_header(text)
    assert header["structure_defect"] is None
    outcomes = field_outcomes(header, ACCESSION, CIK)
    assert outcomes["acceptance"] == "acceptance_year_mismatch"
    assert outcomes["correction"] == "not_evaluated"
    assert row_class(outcomes) == "items_unestablished"


#: The edgartools comparison behind "reuse first": its ``SubmissionFormatParser`` on the round 8-11 counterexamples
#: raises only on the two unbalanced ones and returns a result for the rest, so it cannot fail closed.
EDGARTOOLS_RAISES: dict[str, bool] = {
    "two_ciks": False,
    "items_in_company_data": False,
    "items_continuation": False,
    "acceptance_continuation": False,
    "unclosed_merger_series": True,
    "items_tag_with_space": False,
    "lowercase_paper": False,
    "missing_filer_close": True,
}


def test_edgartools_submission_parser_fails_closed_only_on_unbalanced_containers() -> None:
    texts = {case: text for case, text, _ in CASES if case in EDGARTOOLS_RAISES}
    observed: dict[str, bool] = {}
    for case, text in texts.items():
        try:
            SubmissionFormatParser().parse(text)
            observed[case] = False
        except ValueError:
            observed[case] = True
    assert observed == EDGARTOOLS_RAISES
    # The continuation line's code 4.02 is dropped without an error.
    assert SubmissionFormatParser().parse(texts["items_continuation"])["ITEMS"] == ["1.01", "9.01"]


SECOND_FILER = "<FILER>\n<COMPANY-DATA>\n<CONFORMED-NAME>SECOND FILER\n<CIK>{cik}\n</COMPANY-DATA>\n</FILER>\n"


def test_cik_tag_in_another_form_leaves_discovery_unresolved() -> None:
    """172: a character reference hides a CIK from the grammar's filer set; discovery counts it as unresolved."""
    header = parse_header(_replace("</SEC-HEADER>\n", SECOND_FILER.format(cik="00000999&#57;9") + "</SEC-HEADER>\n"))
    assert header["filer_ciks"] == [CIK, "00000999&#57;9"]
    assert header["discovered_ciks"] == [CIK]
    assert header["unresolved_cik_tags"] == 1


def test_a_rejected_header_still_discovers_every_cik_line() -> None:
    """173: discovery does not depend on the grammar accepting the header."""
    text = _replace(
        "</FILER>\n", "</FILER>\n<SUBJECT-COMPANY>\n</SUBJECT-COMPANY>\n" + SECOND_FILER.format(cik="0000099999")
    )
    header = parse_header(text)
    assert str(header["structure_defect"]).startswith("not_admitted")
    assert header["filer_ciks"] == [CIK]  # read before the defect; the second filer is never reached
    assert header["discovered_ciks"] == [CIK, "0000099999"]
    assert header["unresolved_cik_tags"] == 0


def test_values_are_not_trimmed() -> None:
    """174: a non-breaking space around an item code leaves the code invalid, never silently valid."""
    outcomes = field_outcomes(parse_header(_replace("<ITEMS>4.01\n", "<ITEMS>\u00a04.01\u00a0\n")), ACCESSION, CIK)
    assert outcomes["items"] == "items_invalid"
    assert row_class(outcomes) == "label_in_doubt"


def test_abs_front_matter_is_admitted() -> None:
    """175: the PDS depositor and sponsor CIKs (an ABS 8-K, 0001193125-26-394511) sit between PERIOD and ITEMS."""
    header = parse_header(
        _replace("<ITEMS>4.01\n", "<DEPOSITOR-CIK>0001133438\n<SPONSOR-CIK>0000047288\n<ITEMS>4.01\n")
    )
    assert header["structure_defect"] is None
    assert header["filer_ciks"] == [CIK]


def test_crlf_line_ends_read_as_lf_for_grammar_and_discovery() -> None:
    """177: one line-terminator rule, so a CRLF body parses and discovers exactly as its LF form does."""
    lf, crlf = parse_header(VALID), parse_header(VALID.replace("\n", "\r\n"))
    assert crlf["structure_defect"] is None
    assert (crlf["filer_ciks"], crlf["discovered_ciks"], crlf["unresolved_cik_tags"]) == (
        lf["filer_ciks"],
        lf["discovered_ciks"],
        0,
    )


@pytest.mark.parametrize(
    ("old", "new", "outcome"),
    [
        # A lone CR used as a line break joins two tags into one line, which is malformed.
        ("<ITEMS>4.01\n", "<ITEMS>4.01\r<ITEMS>2.02\n", ("parse", "malformed")),
        # CR before a CRLF: one CR is the terminator's, the other stays in the value.
        ("<ITEMS>4.01\n", "<ITEMS>4.01\r\r\n", ("items", "items_invalid")),
        ("<CIK>0000003545\n", "<CIK>0000003545\r\r\n", ("filer", "not_a_filer")),
    ],
)
def test_a_cr_that_is_not_a_crlf_terminator_stays_in_its_line(old: str, new: str, outcome: tuple[str, str]) -> None:
    """178: the terminator rule runs once, so an extra CR never turns an invalid value valid."""
    header = parse_header(_replace(old, new))
    outcomes = field_outcomes(header, ACCESSION, CIK)
    assert outcomes[outcome[0]] == outcome[1]
    assert row_class(outcomes) != "ok"
    if old.startswith("<CIK>"):
        assert (header["discovered_ciks"], header["unresolved_cik_tags"]) == ([], 1)


def test_a_terminal_lone_cr_stays_in_the_last_line() -> None:
    """179: only a CR that an LF follows is a terminator's, so a body ending in a lone CR keeps it, and the closing
    tag then carries a value."""
    assert VALID.endswith("</SEC-HEADER>\n")
    header = parse_header(VALID.removesuffix("\n") + "\r")
    assert header["structure_defect"] is not None
    assert row_class(field_outcomes(header, ACCESSION, CIK)) != "ok"
    assert parse_header(VALID.removesuffix("\n"))["structure_defect"] is None
