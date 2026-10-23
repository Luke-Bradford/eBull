"""#3947 parser contract (spec §Parser contract; acceptance 3, parser fixtures): one case per clause; and Amendment 2
B1.2's classification (B4.6, classification fixtures)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from app.services.inline_xbrl_dps import (
    DocumentSetError,
    classify,
    parse_document_set,
    rendered_report_files,
)

_DEFAULT_CONTEXTS = """
<xbrli:context id="c1"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">1411342</xbrli:identifier>
</xbrli:entity><xbrli:period><xbrli:startDate>2025-07-01</xbrli:startDate><xbrli:endDate>2025-09-30</xbrli:endDate>
</xbrli:period></xbrli:context>
<xbrli:context id="c2"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">0001411342</xbrli:identifier>
<xbrli:segment><xbrldi:explicitMember dimension="us-gaap:StatementClassOfStockAxis">us-gaap:CommonStockMember
</xbrldi:explicitMember></xbrli:segment></xbrli:entity><xbrli:period><xbrli:instant>2025-09-30</xbrli:instant>
</xbrli:period></xbrli:context>
<xbrli:context id="c3"><xbrli:entity><xbrli:identifier scheme="http://example.com/other">X1</xbrli:identifier>
</xbrli:entity><xbrli:period><xbrli:instant>2025-09-30</xbrli:instant></xbrli:period></xbrli:context>
"""
_DEFAULT_UNITS = """
<xbrli:unit id="usdPerShare"><xbrli:divide><xbrli:unitNumerator><xbrli:measure>iso4217:USD</xbrli:measure>
</xbrli:unitNumerator><xbrli:unitDenominator><xbrli:measure>xbrli:shares</xbrli:measure></xbrli:unitDenominator>
</xbrli:divide></xbrli:unit>
<xbrli:unit id="usd"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>
<xbrli:unit id="eurPerShare"><xbrli:divide><xbrli:unitNumerator><xbrli:measure>iso4217:EUR</xbrli:measure>
</xbrli:unitNumerator><xbrli:unitDenominator><xbrli:measure>xbrli:shares</xbrli:measure></xbrli:unitDenominator>
</xbrli:divide></xbrli:unit>
"""


def _doc(body: str, *, header: bool = True, contexts: str = _DEFAULT_CONTEXTS, units: str = _DEFAULT_UNITS) -> str:
    resources = f"<ix:header><ix:resources>{contexts}{units}</ix:resources></ix:header>" if header else ""
    return (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        '<html xmlns="http://www.w3.org/1999/xhtml" xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"'
        ' xmlns:xbrli="http://www.xbrl.org/2003/instance" xmlns:xbrldi="http://xbrl.org/2006/xbrldi"'
        ' xmlns:iso4217="http://www.xbrl.org/2003/iso4217" xmlns:us-gaap="http://fasb.org/us-gaap/2025"'
        ' xmlns:ixt="http://www.xbrl.org/inlineXBRL/transformation/2020-02-12"'
        ' xmlns:ixt3="http://www.xbrl.org/inlineXBRL/transformation/2015-02-26"'
        ' xmlns:ixt-sec="http://www.sec.gov/inlineXBRL/transformation/2015-08-31"'
        ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
        f'<body><div style="display:none">{resources}</div>{body}</body></html>'
    )


def _fact(attrs: str = "", text: str = "0.39", name: str = "us-gaap:CommonStockDividendsPerShareDeclared") -> str:
    base = f'name="{name}" contextRef="c1" unitRef="usdPerShare" decimals="2"'
    return f"<ix:nonFraction {base} {attrs}>{text}</ix:nonFraction>"


def _set(*documents: tuple[str, str]) -> list[Any]:
    """The occurrences of the documents' members, each classified from its UTF-8 bytes."""
    roots = [(name, classify(name, text.encode())) for name, text in documents]
    return parse_document_set([(name, root) for name, root in roots if root is not None])


def _one(body: str, **kwargs: Any) -> Any:
    occurrences = _set(("a.htm", _doc(body, **kwargs)))
    assert len(occurrences) == 1
    return occurrences[0]


def test_absent_scale_and_sign_take_their_defaults() -> None:
    o = _one(_fact())
    assert (o.scale, o.instance_value, o.unresolved) == (0, Decimal("0.39"), None)
    assert (o.entity_cik, o.period_start, o.period_end) == (
        "0001411342",
        date(2025, 7, 1),
        date(2025, 9, 30),
    )
    assert (o.dimensional, o.unit, o.display_text) == (False, "USD/shares", "0.39")


def test_present_scale_and_sign_form_the_instance_value() -> None:
    o = _one(_fact('scale="3" sign="-"'))
    assert (o.scale, o.instance_value) == (3, Decimal("-390"))
    assert (o.scale_raw, o.sign_raw) == ("3", "-")
    assert _one(_fact('scale="6"', text="1.75")).instance_value == Decimal("1750000")
    assert _one(_fact('scale="-2"', text="39")).instance_value == Decimal("0.39")


@pytest.mark.parametrize("raw", ["+3", " 3 ", "03", "0" * 5000 + "3"])
def test_scale_lexical_space_is_xs_integer_after_collapse(raw: str) -> None:
    o = _one(_fact(f'scale="{raw}"'))
    assert (o.scale, o.scale_raw, o.unresolved) == (3, raw, None)


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        ("3.0", "scale_lexical"),
        ("x", "scale_lexical"),
        ("31", "scale_out_of_bound"),
        ("-31", "scale_out_of_bound"),
        ("9" * 5000, "scale_out_of_bound"),
    ],
)
def test_scale_outside_lexical_space_or_bound_is_unresolved(raw: str, reason: str) -> None:
    o = _one(_fact(f'scale="{raw}"'))
    assert o.unresolved == reason
    assert (o.scale, o.instance_value, o.entity_cik) == (None, None, None)


def test_invalid_sign_is_unresolved() -> None:
    assert _one(_fact('sign="+"')).unresolved == "sign"


@pytest.mark.parametrize(
    ("fmt", "text", "value"),
    [
        ("ixt:num-dot-decimal", "1,234.50", Decimal("1234.50")),
        ("ixt:num-dot-decimal", ".39", Decimal("0.39")),
        ("ixt:num-dot-decimal", "1 234", Decimal("1234")),
        ("ixt:fixed-zero", "—", Decimal("0")),
        ("ixt3:numdotdecimal", "1,234.5", Decimal("1234.5")),
        ("ixt3:zerodash", "–", Decimal("0")),
    ],
)
def test_supported_formats_transform_by_registry_rule(fmt: str, text: str, value: Decimal) -> None:
    o = _one(_fact(f'format="{fmt}"', text=text))
    assert (o.instance_value, o.unresolved, o.format_raw) == (value, None, fmt)


@pytest.mark.parametrize(
    ("fmt", "text", "reason"),
    [
        ("ixt-sec:numwordsen", "no", "unsupported_format"),
        ("ixt:num-comma-decimal", "0,39", "unsupported_format"),
        ("undeclared:numdotdecimal", "0.39", "unsupported_format"),
        ("ixt:num-dot-decimal", "1.2.3", "format_lexical"),
        ("ixt:num-dot-decimal", "-", "format_lexical"),
        ("ixt3:numdotdecimal", "12,34", "format_lexical"),
        ("ixt3:zerodash", "0", "format_lexical"),
    ],
)
def test_unsupported_format_or_lexical_mismatch_is_unresolved(fmt: str, text: str, reason: str) -> None:
    assert _one(_fact(f'format="{fmt}"', text=text)).unresolved == reason


def test_no_format_needs_an_unsigned_decimal() -> None:
    assert _one(_fact(text="1,234")).unresolved == "format_lexical"


def test_nested_markup_is_part_of_the_display() -> None:
    o = _one(_fact('format="ixt:num-dot-decimal" scale="3"', text="<span>0.</span>39"))
    assert (o.display_text, o.instance_value) == ("0.39", Decimal("390"))


def test_nil_is_unresolved() -> None:
    assert _one(_fact('xsi:nil="true"', text="")).unresolved == "nil"


def test_unresolved_context_and_unit_refs() -> None:
    body = _fact().replace('contextRef="c1"', 'contextRef="missing"').replace('"usdPerShare"', '"nope"')
    assert _one(body).unresolved == "context_ref,unit_ref"


def test_unit_compared_as_expanded_qname_and_others_unsupported() -> None:
    # A different prefix bound to the ISO 4217 namespace is the same unit.
    units = _DEFAULT_UNITS.replace("iso4217:USD", "cur:USD").replace(
        '<xbrli:unit id="usdPerShare">', '<xbrli:unit id="usdPerShare" xmlns:cur="http://www.xbrl.org/2003/iso4217">'
    )
    assert _one(_fact(), units=units).unit == "USD/shares"
    for ref in ("usd", "eurPerShare"):
        o = _one(_fact().replace('"usdPerShare"', f'"{ref}"'))
        assert (o.unit, o.unresolved, o.instance_value) == ("unsupported_unit", None, Decimal("0.39"))
    undeclared = _DEFAULT_UNITS.replace(
        "iso4217:USD</xbrli:measure>\n</xbrli:unitNumerator>", "zz:USD</xbrli:measure>\n</xbrli:unitNumerator>", 1
    )
    assert _one(_fact(), units=undeclared).unresolved == "unit_qname"


def test_cik_scheme_dimension_and_instant() -> None:
    assert _one(_fact().replace('"c1"', '"c3"')).unresolved == "entity_scheme"
    o = _one(_fact().replace('"c1"', '"c2"'))
    assert (o.dimensional, o.period_start, o.period_end, o.entity_cik) == (
        True,
        None,
        date(2025, 9, 30),
        "0001411342",
    )


def test_concept_resolved_by_namespace_not_prefix() -> None:
    other_prefix = _fact(name="gaap:CommonStockDividendsPerShareDeclared").replace(
        "<ix:nonFraction ", '<ix:nonFraction xmlns:gaap="http://fasb.org/us-gaap/2024" '
    )
    assert _one(other_prefix).unresolved is None
    wrong_namespace = _fact(name="x:CommonStockDividendsPerShareDeclared").replace(
        "<ix:nonFraction ", '<ix:nonFraction xmlns:x="http://example.com/ext" '
    )
    assert _set(("a.htm", _doc(wrong_namespace))) == []
    assert _one(_fact(name="nope:CommonStockDividendsPerShareDeclared")).unresolved == "name_qname"
    assert _set(("a.htm", _doc(_fact(name="us-gaap:EarningsPerShareBasic")))) == []


def test_hidden_facts_and_ordinals() -> None:
    body = _fact('id="f1"') + _fact('id="f2" scale="3"')
    occurrences = _set(("a.htm", _doc(body)))
    assert [(o.element_ordinal, o.fact_id, o.scale) for o in occurrences] == [(0, "f1", 0), (1, "f2", 3)]


def test_duplicate_context_or_unit_id_fails_the_attempt() -> None:
    with pytest.raises(DocumentSetError, match="duplicate_context:c1"):
        _set(
            ("a.htm", _doc(_fact())),
            ("b.htm", _doc("", contexts=_DEFAULT_CONTEXTS.split('<xbrli:context id="c2"')[0], units="")),
        )
    with pytest.raises(DocumentSetError, match="duplicate_unit:usd"):
        _set(("a.htm", _doc(_fact(), units=_DEFAULT_UNITS + _DEFAULT_UNITS)))


def test_ix_target_fails_the_attempt_but_html_target_does_not() -> None:
    with pytest.raises(DocumentSetError, match="target"):
        _set(("a.htm", _doc(_fact('target="other"'))))
    assert len(_set(("a.htm", _doc('<a href="x" target="_blank">x</a>' + _fact())))) == 1


def test_multi_document_set_resolves_contexts_across_documents() -> None:
    # Each member of the set carries its own ix:header; the contexts and units sit in the main document only.
    exhibit = _doc(_fact('scale="3"'), contexts="", units="")
    occurrences = _set(("main.htm", _doc("")), ("ex13.htm", exhibit), ("r1.htm", "<p>rendered</p>"))
    assert [(o.document, o.instance_value, o.unresolved) for o in occurrences] == [("ex13.htm", Decimal("390"), None)]


def test_a_header_less_member_is_read_with_its_own_ordinals() -> None:
    """B1.4/B1.5: a member need not hold an ``ix:header``; its facts resolve against the set's resources."""
    exhibit = _doc(_fact('id="x1"') + _fact('id="x2" scale="3"', text="1.75"), header=False)
    occurrences = _set(("main.htm", _doc(_fact('id="m1"'))), ("ex99.htm", exhibit))
    assert [(o.document, o.element_ordinal, o.fact_id, o.instance_value) for o in occurrences] == [
        ("main.htm", 0, "m1", Decimal("0.39")),
        ("ex99.htm", 0, "x1", Decimal("0.39")),
        ("ex99.htm", 1, "x2", Decimal("1750")),
    ]


def test_a_set_with_no_ix_header_fails_the_attempt() -> None:
    with pytest.raises(DocumentSetError, match="no_ix_header"):
        _set(("a.htm", _doc(_fact(), header=False)))


_IX_DOC = (
    '<html xmlns="http://www.w3.org/1999/xhtml" xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"><body>'
    "<ix:header/></body></html>"
)


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(_IX_DOC.encode(), id="utf-8"),
        pytest.param(_IX_DOC.encode("utf-16"), id="utf-16-bom"),
        pytest.param(_IX_DOC.replace("inlineXBRL", "inline&#x58;BRL").encode(), id="namespace-by-character-reference"),
        pytest.param(_IX_DOC.replace("ix:", "inl:").replace("xmlns:ix", "xmlns:inl").encode(), id="other-prefix"),
        pytest.param(_IX_DOC.replace("<ix:header/>", "<ix:nonFraction/>").encode(), id="any-ix-element-no-header"),
        pytest.param(
            ('<?xml version="1.0" encoding="iso-8859-1"?>' + _IX_DOC.replace("<body>", "<body>\xe9")).encode("latin-1"),
            id="declared-encoding",
        ),
    ],
)
def test_classify_members(body: bytes) -> None:
    root = classify("a.htm", body)
    assert root is not None and root.tag == "{http://www.w3.org/1999/xhtml}html"


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b"<html><body><p>Exhibit 31 &nbsp; <br></body></html>", id="malformed-plain-html"),
        pytest.param(
            _IX_DOC.replace(
                '<html xmlns="http://www.w3.org/1999/xhtml"', '<h:html xmlns:h="http://www.w3.org/1999/xhtml"'
            )
            .replace("</html>", "</h:html>")
            .encode(),
            id="prefixed-root",
        ),
        pytest.param(_IX_DOC.replace(' xmlns="http://www.w3.org/1999/xhtml"', "").encode(), id="non-xhtml-root"),
        pytest.param(
            _IX_DOC.replace("<ix:header/>", "<p>http://www.xbrl.org/2013/inlineXBRL</p>").encode(),
            id="namespace-name-in-text-no-ix-element",
        ),
        pytest.param(b"", id="empty"),
    ],
)
def test_classify_non_members(body: bytes) -> None:
    assert classify("a.htm", body) is None


@pytest.mark.parametrize(
    "body",
    [
        pytest.param((_IX_DOC + "<unclosed>").encode(), id="malformed-declaring-the-namespace"),
        pytest.param((_IX_DOC + "<unclosed>").encode("utf-32"), id="malformed-utf-32-bom"),
        pytest.param(
            b'<html><body><div data-ns="http://www.xbrl.org/2013/inline&#88;BRL">&nbsp;</div></body></html>',
            id="namespace-name-in-an-attribute-only",
        ),
    ],
)
def test_classify_refuses_a_damaged_file_that_looks_inline(body: bytes) -> None:
    with pytest.raises(DocumentSetError, match="undecidable:a.htm"):
        classify("a.htm", body)


def test_rendered_report_files_reads_html_file_names_only() -> None:
    summary = (
        '<?xml version="1.0" encoding="utf-8"?><FilingSummary><InputFiles><File>a.htm</File></InputFiles>'
        "<MyReports><Report><HtmlFileName>R1.htm</HtmlFileName></Report>"
        "<Report><HtmlFileName> R2.htm </HtmlFileName></Report></MyReports></FilingSummary>"
    )
    assert rendered_report_files(summary) == frozenset({"R1.htm", "R2.htm"})
    assert rendered_report_files("not xml <") == frozenset()


def test_negative_scale_with_leading_zeros() -> None:
    o = _one(_fact('scale="-' + "0" * 5000 + '2"', text="39"))
    assert (o.scale, o.instance_value) == (-2, Decimal("0.39"))
