"""#3624 slice 2: the closed ``.hdr.sgml`` grammar fails closed on every checkpoint-1 counterexample (rounds 8-11)."""

from __future__ import annotations

import pytest

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


@pytest.mark.parametrize(
    ("case", "text", "cause"),
    [
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
            "unknown_element",
        ),
        # Round 10: a space or lower case inside the brackets, a nested role, a missing role close.
        ("items_tag_with_space", _replace("<ITEMS>4.01", "<ITEMS >4.02"), "unknown_element"),
        ("lowercase_paper", _replace("<FILER>\n", "<paper >\n<FILER>\n"), "unknown_element"),
        ("lowercase_items", _replace("<ITEMS>4.01", "<items>4.02"), "unknown_element"),
        (
            "nested_subject_company",
            _replace(
                "</FILER>\n",
                "<SUBJECT-COMPANY>\n<COMPANY-DATA>\n<CIK>0000099999\n</COMPANY-DATA>\n</SUBJECT-COMPANY>\n</FILER>\n",
            ),
            "unknown_element",
        ),
        ("missing_filer_close", _replace("</FILER>\n", ""), "unbalanced"),
        # Envelope, values and identity.
        ("text_before_envelope", "preamble\n" + VALID, "envelope"),
        ("text_after_envelope", VALID + "<ITEMS>4.02\n", "outside_envelope"),
        ("envelope_without_file_line", _replace(f"{ACCESSION}.hdr.sgml : 20231214", ""), "envelope"),
        ("value_on_empty_element", _replace("<FILER>\n", "<PAPER>NO\n<FILER>\n"), "value_not_permitted"),
        ("markup_in_value", _replace("<CITY>FT. MYERS,", "<CITY>FT. <MYERS"), "value_not_permitted"),
        ("leading_blank_before_tag", _replace("<ITEMS>4.01", " <ITEMS>4.01"), "non_tag_line"),
        (
            "repeated_acceptance",
            _replace("<TYPE>8-K\n", "<TYPE>8-K\n<ACCEPTANCE-DATETIME>20231215090000\n"),
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
            "registrant_data_count",
        ),
        ("company_data_without_cik", _replace("<CIK>0000003545\n", ""), "cik_count"),
        ("unclosed_envelope", _replace("</SEC-HEADER>\n", ""), "unclosed"),
    ],
)
def test_counterexample_is_malformed_and_items_unestablished(case: str, text: str, cause: str) -> None:
    header = parse_header(text)
    assert str(header["structure_defect"]).split("@")[0] == cause, case
    assert row_class(field_outcomes(header, ACCESSION, CIK)) == "items_unestablished", case


def test_correction_evidence_is_detected_inside_a_valid_header() -> None:
    header = parse_header(_replace("<ACCESSION-NUMBER>", "<CORRECTION>\n<TIMESTAMP>20231215\n<ACCESSION-NUMBER>"))
    assert header["structure_defect"] is None
    assert header["correction_tags"] == ["CORRECTION", "TIMESTAMP"]
    assert row_class(field_outcomes(header, ACCESSION, CIK)) == "items_unestablished"
