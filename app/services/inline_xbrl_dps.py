"""Inline XBRL reader for ``us-gaap:CommonStockDividendsPerShareDeclared`` occurrences (#3947).

Spec: ``docs/specs/fundamentals/2026-10-21-3947-dps-inline-scale.md`` §Parser contract, with Amendment 2 B1's
document set: :func:`classify` decides each candidate's membership, and :func:`parse_document_set` reads the set's
members, ``ix:header`` or not. Pure: the caller fetches the documents and the attachment types; this module never
touches the network or the DB.

Every inline element of the concept is returned, scale 0 included, with its raw attributes and, when they resolve,
its canonical key (entity CIK, period, dimensionality, unit) and instance value. The source rules:

* **DQC.US.0179.10141** flags an occurrence of this concept whose inline ``scale`` is greater than 0.
* **Inline XBRL 1.1 Part 1 §10.2.3**: the instance value is the transformed display × 10^scale, negated when
  ``sign="-"``; an absent ``scale`` is 0 and an absent ``sign`` is positive (each applies "if present").
* **Transformation Registry** formats are applied by their registry XSD's whitespace facet and pattern: TR3
  (2015-02-26) ``numdotdecimal`` and ``zerodash``; TR4 (2020-02-12) and TR5 (2022-02-16) ``num-dot-decimal`` and
  ``fixed-zero``. Any other format — the SEC registry's ``numwordsen`` included — is published as
  ``unsupported_format``, never guessed. (The format census over stored 10-K primary documents is in the PR.)

Two failure classes: :class:`DocumentSetError` fails the whole attempt (the scan's active generation stays in
force); an occurrence-level problem is published on the occurrence as ``unresolved``.

**Reuse first (spec §Parser contract).** edgartools 5.30.2's ``edgar.documents.strategies.xbrl_extraction.
XBRLExtractor`` was compared against this contract and not adopted. It detects ix elements by the literal prefix
``ix:`` as well as by namespace; reads only ``element.text``, so a value inside a nested ``<span>`` is lost;
passes an unknown ``format`` through untransformed; converts through ``float``; ignores ``xsi:nil``, a duplicate
context or unit id, ``target`` and an invalid ``scale`` (swallowed by ``except ValueError``); and normalises units
by local name, not expanded QName.
"""

from __future__ import annotations

import codecs
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Final

import lxml.etree as ET
import lxml.html

CONCEPT: Final = "CommonStockDividendsPerShareDeclared"

IX_NS: Final = "http://www.xbrl.org/2013/inlineXBRL"
XHTML_NS: Final = "http://www.w3.org/1999/xhtml"
XBRLI_NS: Final = "http://www.xbrl.org/2003/instance"
ISO4217_NS: Final = "http://www.xbrl.org/2003/iso4217"
XSI_NIL: Final = "{http://www.w3.org/2001/XMLSchema-instance}nil"
SEC_CIK_SCHEME: Final = "http://www.sec.gov/CIK"
_US_GAAP_NS = re.compile(r"http://fasb\.org/us-gaap/[0-9]{4}(-[0-9]{2}-[0-9]{2})?")

USD_PER_SHARE: Final = "USD/shares"
UNSUPPORTED_UNIT: Final = "unsupported_unit"

# xs:integer lexical space, after whitespace collapse; the bound is ours, not the schema's.
_SCALE_LEXICAL = re.compile(r"[-+]?[0-9]+")
SCALE_BOUND: Final = 30

_TR3: Final = "http://www.xbrl.org/inlineXBRL/transformation/2015-02-26"
_TR4: Final = "http://www.xbrl.org/inlineXBRL/transformation/2020-02-12"
_TR5: Final = "http://www.xbrl.org/inlineXBRL/transformation/2022-02-16"
# Registry XSD patterns, verbatim: TR3 numdotdecimalType; TR4/TR5 numDotDecimalType (identical in both).
_TR3_NUMDOTDECIMAL = re.compile("[0-9]{1,3}((,| | )?[0-9]{3})*(\\.[0-9]+)?")
_TR4_NUM_DOT_DECIMAL = re.compile("[,  0-9]*(\\.[  0-9]+)?")
# TR3 zerodashType enumeration (hyphen-minus, Armenian hyphen, maqaf, U+2010-U+2015, U+FE58, U+FE63, U+FF0D).
_TR3_ZERODASH: Final = frozenset("-֊־‐‑‒–—―﹘﹣－")
# Content with no ``format``: an unsigned decimal (sign is carried by the ``sign`` attribute, §10.2.3).
_PLAIN_DECIMAL = re.compile(r"[0-9]+(\.[0-9]*)?|\.[0-9]+")

_XML_WS = re.compile(r"[ \t\r\n]+")
_XML_DECLARATION = re.compile(r"^\s*<\?xml[^>]*\?>")
_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
INLINE_SUFFIXES: Final = (".htm", ".html", ".xhtml")


class DocumentSetError(Exception):
    """The attempt fails; the caller keeps the accession's active generation."""


@dataclass(frozen=True)
class Occurrence:
    document: str
    element_ordinal: int
    fact_id: str | None
    context_ref_raw: str | None
    unit_ref_raw: str | None
    scale_raw: str | None
    sign_raw: str | None
    format_raw: str | None
    decimals_raw: str | None
    display_text: str
    entity_cik: str | None
    period_start: date | None
    period_end: date | None
    dimensional: bool | None
    unit: str | None
    scale: int | None
    instance_value: Decimal | None
    unresolved: str | None


@dataclass(frozen=True)
class _Context:
    entity_cik: str | None
    period_start: date | None
    period_end: date | None
    dimensional: bool
    problem: str | None


def _collapse(text: str) -> str:
    return _XML_WS.sub(" ", text).strip(" ")


def _qname(text: str | None, nsmap: Mapping[str | None, str]) -> tuple[str, str] | None:
    """Expand a QName against the element's in-scope namespaces; ``None`` when its prefix is not declared."""
    if text is None:
        return None
    prefix, sep, local = _collapse(text).rpartition(":")
    namespace = nsmap.get(prefix if sep else None)
    if namespace is None or not local:
        return None
    return namespace, local


def classify(name: str, body: bytes) -> ET._Element | None:
    """Amendment 2 B1.2: the parsed root when the candidate is an Inline XBRL document, else ``None``.

    The bytes go intact to the XML parser, which detects the encoding (BOM, declaration, else UTF-8). A document whose
    root is an unprefixed ``html`` in the XHTML namespace and which holds any element in the Inline XBRL namespace is
    one (EDGAR XBRL Guide §11). A file that is not well-formed XML has no root, so it is not one, unless it looks
    Inline (:func:`_looks_inline`): then the attempt fails ``undecidable:<name>``.
    """
    parser = ET.XMLParser(huge_tree=True, resolve_entities=False, no_network=True, recover=False)
    try:
        root = ET.fromstring(body, parser)
    except ET.XMLSyntaxError, ValueError:
        if _looks_inline(body):
            raise DocumentSetError(f"undecidable:{name}") from None
        return None
    if root.tag != f"{{{XHTML_NS}}}html" or root.prefix is not None:
        return None
    return root if next(root.iter(f"{{{IX_NS}}}*"), None) is not None else None


# UTF-32 is tested before UTF-16, whose little-endian mark is a prefix of UTF-32's.
_BOMS: Final = (
    (codecs.BOM_UTF32_LE, "utf-32"),
    (codecs.BOM_UTF32_BE, "utf-32"),
    (codecs.BOM_UTF8, "utf-8-sig"),
    (codecs.BOM_UTF16_LE, "utf-16"),
    (codecs.BOM_UTF16_BE, "utf-16"),
)
# XML 1.0 Appendix F: "<?" in each byte order, for a UTF-16 or UTF-32 document without a byte-order mark.
_NO_BOM_ORDER: Final = (
    (b"\x00\x00\x00<\x00\x00\x00?", "utf-32-be"),
    (b"<\x00\x00\x00?\x00\x00\x00", "utf-32-le"),
    (b"\x00<\x00?", "utf-16-be"),
    (b"<\x00?\x00", "utf-16-le"),
)
# IANA names XML allows that Python has no codec for: UCS-2 is the BMP subset of UTF-16, and UCS-4 has UTF-32's
# code units, so each decodes as its UTF counterpart (byte order still from the BOM or Appendix F).
_UCS: Final = {
    "ISO-10646-UCS-2": "utf-16",
    "UCS-2": "utf-16",
    "CSUNICODE": "utf-16",
    "ISO-10646-UCS-4": "utf-32",
    "UCS-4": "utf-32",
    "CSUCS4": "utf-32",
}
_DECLARED_ENCODING = re.compile(rb"""\s*<\?xml[^>]*\bencoding\s*=\s*["']([A-Za-z0-9._-]+)["']""")


def _canonical(encoding: str) -> str:
    """Python's canonical codec name, so every spelling (``UTF_16``, ``utf16``, ``UTF-16``) compares as one; an
    unknown name raises ``ValueError``."""
    try:
        return _UCS.get(encoding.upper().replace("_", "-")) or codecs.lookup(encoding).name
    except LookupError as exc:
        raise ValueError(f"unknown declared encoding {encoding!r}") from exc


def _codec(body: bytes, declared: str | None) -> str:
    """The BOM; else the byte order of ``<?`` in the first bytes, for a UTF-16 or UTF-32 file without one (XML 1.0
    Appendix F), whatever it declares; else the declared encoding by canonical name; else UTF-8. A UTF-16/32
    declaration on a file that starts neither way raises, so it is never mis-decoded."""
    codec = next((c for bom, c in _BOMS if body.startswith(bom)), None) or next(
        (c for prefix, c in _NO_BOM_ORDER if body.startswith(prefix)), None
    )
    if codec:
        return codec
    if declared:
        canonical = _canonical(declared)
        if canonical in ("utf-16", "utf-32"):
            raise ValueError(f"cannot read the byte order of a {declared} document without a BOM")
        return canonical
    return "utf-8"


def _looks_inline(body: bytes) -> bool:
    """B1.2's guard for a file that does not parse as XML: its text, decoded by :func:`_codec` from the declaration
    (UTF-8 with replacement when that rule fails), contains ``inlineXBRL``, or a lenient HTML parse finds the namespace
    name in an attribute value."""
    declared = _DECLARED_ENCODING.match(body)
    try:
        text = body.decode(_codec(body, declared.group(1).decode("ascii") if declared else None), "replace")
    except ValueError, LookupError:
        text = body.decode("utf-8", "replace")
    if "inlineXBRL" in text:
        return True
    try:
        tree = lxml.html.fromstring(body)
    except ET.LxmlError, ValueError:  # LxmlError is the base of every lxml parser error
        return False
    return any(IX_NS in value for el in tree.iter() for value in el.attrib.values())


def _date(element: ET._Element | None) -> date | None:
    if element is None or element.text is None:
        return None
    text = _collapse(element.text)
    if not _DATE.fullmatch(text):
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _context(element: ET._Element) -> _Context:
    identifier = element.find(f"{{{XBRLI_NS}}}entity/{{{XBRLI_NS}}}identifier")
    cik: str | None = None
    problem: str | None = None
    if identifier is None or _collapse(identifier.get("scheme", "")) != SEC_CIK_SCHEME:
        problem = "entity_scheme"
    else:
        digits = _collapse(identifier.text or "")
        if digits.isascii() and digits.isdigit() and len(digits) <= 10:
            cik = digits.zfill(10)
        else:
            problem = "entity_identifier"
    dimensional = (
        element.find(f"{{{XBRLI_NS}}}entity/{{{XBRLI_NS}}}segment") is not None
        or element.find(f"{{{XBRLI_NS}}}scenario") is not None
    )
    period = element.find(f"{{{XBRLI_NS}}}period")
    start: date | None = None
    end: date | None = None
    if period is not None:
        instant = period.find(f"{{{XBRLI_NS}}}instant")
        if instant is not None:
            end = _date(instant)
        else:
            start = _date(period.find(f"{{{XBRLI_NS}}}startDate"))
            end = _date(period.find(f"{{{XBRLI_NS}}}endDate"))
            if start is None:
                end = None
    if end is None and problem is None:
        problem = "period"
    return _Context(cik, start, end, dimensional, problem)


def _unit(element: ET._Element) -> str | None:
    """``USD/shares``, ``unsupported_unit``, or ``None`` when a measure's QName does not resolve."""

    def measures(parent: ET._Element | None) -> list[tuple[str, str] | None]:
        if parent is None:
            return []
        return [_qname(m.text, m.nsmap) for m in parent.findall(f"{{{XBRLI_NS}}}measure")]

    divide = element.find(f"{{{XBRLI_NS}}}divide")
    if divide is None:
        found = measures(element)
        return None if any(m is None for m in found) else UNSUPPORTED_UNIT
    numerator = measures(divide.find(f"{{{XBRLI_NS}}}unitNumerator"))
    denominator = measures(divide.find(f"{{{XBRLI_NS}}}unitDenominator"))
    if any(m is None for m in numerator + denominator):
        return None
    if numerator == [(ISO4217_NS, "USD")] and denominator == [(XBRLI_NS, "shares")]:
        return USD_PER_SHARE
    return UNSUPPORTED_UNIT


def _transform(display: str, format_raw: str | None, nsmap: Mapping[str | None, str]) -> Decimal | str:
    """The transformed numeric value, or the unresolved reason."""
    text = _collapse(display)
    if format_raw is None:
        return Decimal(text) if _PLAIN_DECIMAL.fullmatch(text) else "format_lexical"
    qname = _qname(format_raw, nsmap)
    if qname is None:
        return "unsupported_format"
    namespace, local = qname
    if namespace == _TR3 and local == "numdotdecimal":
        if not _TR3_NUMDOTDECIMAL.fullmatch(text):
            return "format_lexical"
        return Decimal(re.sub("[,  ]", "", text))
    if namespace in (_TR4, _TR5) and local == "num-dot-decimal":
        digits = re.sub("[,  ]", "", text)
        if not _TR4_NUM_DOT_DECIMAL.fullmatch(text) or not re.search("[0-9]", digits):
            return "format_lexical"
        return Decimal(digits)
    if namespace == _TR3 and local == "zerodash":
        return Decimal(0) if text in _TR3_ZERODASH else "format_lexical"
    if namespace in (_TR4, _TR5) and local == "fixed-zero":
        return Decimal(0)
    return "unsupported_format"


def _is_concept(element: ET._Element) -> bool | None:
    """True for the concept, False for another, None when the name's prefix is undeclared but its local name matches."""
    raw = element.get("name")
    qname = _qname(raw, element.nsmap)
    if qname is None:
        return None if raw is not None and _collapse(raw).rpartition(":")[2] == CONCEPT else False
    namespace, local = qname
    return local == CONCEPT and bool(_US_GAAP_NS.fullmatch(namespace))


def _occurrence(
    document: str,
    ordinal: int,
    element: ET._Element,
    contexts: Mapping[str, _Context],
    units: Mapping[str, str | None],
    name_resolved: bool,
) -> Occurrence:
    display = "".join(element.itertext())
    raw = {k: element.get(k) for k in ("contextRef", "unitRef", "scale", "sign", "format", "decimals", "id")}
    problems: list[str] = []
    if not name_resolved:
        problems.append("name_qname")
    if element.tag == f"{{{IX_NS}}}fraction":
        problems.append("fraction")
    if _collapse(element.get(XSI_NIL, "false")) in ("true", "1"):
        problems.append("nil")

    context = contexts.get(_collapse(raw["contextRef"] or ""))
    if context is None:
        problems.append("context_ref")
    elif context.problem:
        problems.append(context.problem)
    unit_ref = _collapse(raw["unitRef"] or "")
    unit = units.get(unit_ref) if unit_ref in units else None
    if unit_ref not in units:
        problems.append("unit_ref")
    elif unit is None:
        problems.append("unit_qname")

    scale: int | None = 0
    if raw["scale"] is not None:
        collapsed = _collapse(raw["scale"])
        # Leading zeros are stripped before ``int()``, which refuses a string past Python's 4,300-digit limit.
        digits = collapsed.lstrip("+-").lstrip("0") or "0"
        if not _SCALE_LEXICAL.fullmatch(collapsed):
            scale = None
            problems.append("scale_lexical")
        elif len(digits) > 2 or int(digits) > SCALE_BOUND:
            scale = None
            problems.append("scale_out_of_bound")
        else:
            scale = -int(digits) if collapsed.startswith("-") else int(digits)
    negative = False
    if raw["sign"] is not None:
        if raw["sign"] == "-":
            negative = True
        else:
            problems.append("sign")

    value: Decimal | None = None
    if element.tag == f"{{{IX_NS}}}nonFraction" and "nil" not in problems:
        transformed = _transform(display, raw["format"], element.nsmap)
        if isinstance(transformed, str):
            problems.append(transformed)
        elif scale is not None:
            try:
                value = transformed.scaleb(scale)
            except InvalidOperation:
                problems.append("value")
            else:
                value = -value if negative else value
    unresolved = ",".join(problems) or None
    return Occurrence(
        document=document,
        element_ordinal=ordinal,
        fact_id=raw["id"],
        context_ref_raw=raw["contextRef"],
        unit_ref_raw=raw["unitRef"],
        scale_raw=raw["scale"],
        sign_raw=raw["sign"],
        format_raw=raw["format"],
        decimals_raw=raw["decimals"],
        display_text=display,
        entity_cik=context.entity_cik if context and not unresolved else None,
        period_start=context.period_start if context and not unresolved else None,
        period_end=context.period_end if context and not unresolved else None,
        dimensional=context.dimensional if context and not unresolved else None,
        unit=unit if not unresolved else None,
        scale=scale if not unresolved else None,
        instance_value=value if not unresolved else None,
        unresolved=unresolved,
    )


def rendered_report_files(filing_summary: str) -> frozenset[str]:
    """The EDGAR Renderer's own output pages (``R1.htm`` …) named by ``FilingSummary.xml``'s ``<HtmlFileName>``.

    Those pages are written by SEC from the instance, so none can be a filer's inline document; the scan skips
    fetching them (spec §Document set, build note). The summary's ``<InputFiles>`` are not read (spec §Document
    set). An unparseable summary excludes nothing: it costs requests, never coverage.
    """
    try:
        root = ET.fromstring(
            _XML_DECLARATION.sub("", filing_summary, count=1),
            ET.XMLParser(resolve_entities=False, no_network=True),
        )
    except ET.XMLSyntaxError, ValueError:
        return frozenset()
    return frozenset(
        _collapse(e.text) for e in root.iterfind("MyReports/Report/HtmlFileName") if e.text and _collapse(e.text)
    )


def parse_document_set(roots: Sequence[tuple[str, ET._Element]]) -> list[Occurrence]:
    """Read the members of an Inline XBRL Document Set as one set (Amendment 2 B1.4, B1.5).

    ``roots`` is ``(name, root)`` of every member, each classified by :func:`classify`, in source order; a member
    need not hold an ``ix:header``. Returns every occurrence of the concept. Raises :class:`DocumentSetError` when no
    member holds an ``ix:header`` (``no_ix_header``; Inline XBRL 1.1 Part 1 §8.1.3), a context or unit id is defined
    twice in the set, or an ix element carries ``target``.
    """
    if not any(next(root.iter(f"{{{IX_NS}}}header"), None) is not None for _, root in roots):
        raise DocumentSetError("no_ix_header")

    contexts: dict[str, _Context] = {}
    units: dict[str, str | None] = {}
    for name, root in roots:
        for element in root.iter(ET.Element):
            tag = element.tag
            if isinstance(tag, str) and tag.startswith(f"{{{IX_NS}}}") and element.get("target") is not None:
                # Only ix elements: an HTML ``<a target=…>`` is not a target document.
                raise DocumentSetError(f"target:{name}")
        for element in root.iter(f"{{{XBRLI_NS}}}context"):
            key = _collapse(element.get("id", ""))
            if key in contexts:
                raise DocumentSetError(f"duplicate_context:{key}")
            contexts[key] = _context(element)
        for element in root.iter(f"{{{XBRLI_NS}}}unit"):
            key = _collapse(element.get("id", ""))
            if key in units:
                raise DocumentSetError(f"duplicate_unit:{key}")
            units[key] = _unit(element)

    occurrences: list[Occurrence] = []
    for name, root in roots:
        ordinal = 0
        for element in root.iter(f"{{{IX_NS}}}nonFraction", f"{{{IX_NS}}}fraction"):
            matched = _is_concept(element)
            if matched is False:
                continue
            occurrences.append(_occurrence(name, ordinal, element, contexts, units, matched is True))
            ordinal += 1
    return occurrences
