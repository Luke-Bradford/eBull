"""#3947 Amendment 2 B1: the document set an acquired source yields (B4.6 fixtures: the header-less case,
``not_inline``, types and sets). Stub provider and in-memory zips from the acquisition tests: no network, no DB."""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.dps_inline_scale_scan import acquire_document_source, read_document_set
from app.services.inline_xbrl_dps import DocumentSetError
from tests.test_3947_dps_archive_acquisition import _Provider, _record, _target, _zip

_NS = (
    'xmlns="http://www.w3.org/1999/xhtml" xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"'
    ' xmlns:xbrli="http://www.xbrl.org/2003/instance" xmlns:iso4217="http://www.xbrl.org/2003/iso4217"'
    ' xmlns:us-gaap="http://fasb.org/us-gaap/2025"'
)
_RESOURCES = (
    "<ix:header><ix:resources>"
    '<xbrli:context id="c1"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">1411342'
    "</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:startDate>2025-07-01</xbrli:startDate>"
    "<xbrli:endDate>2025-09-30</xbrli:endDate></xbrli:period></xbrli:context>"
    '<xbrli:unit id="u"><xbrli:divide><xbrli:unitNumerator><xbrli:measure>iso4217:USD</xbrli:measure>'
    "</xbrli:unitNumerator><xbrli:unitDenominator><xbrli:measure>xbrli:shares</xbrli:measure>"
    "</xbrli:unitDenominator></xbrli:divide></xbrli:unit></ix:resources></ix:header>"
)


def _fact(fact_id: str, text: str, scale: str = "0") -> str:
    return (
        f'<ix:nonFraction name="us-gaap:CommonStockDividendsPerShareDeclared" id="{fact_id}" contextRef="c1"'
        f' unitRef="u" decimals="2" scale="{scale}">{text}</ix:nonFraction>'
    )


def _inline(body: str, *, header: bool = False) -> bytes:
    return f"<html {_NS}><body>{_RESOURCES if header else ''}{body}</body></html>".encode()


_PRIMARY = ("main.htm", _inline(_fact("m1", "0.39"), header=True))
# The [Z3] shape: an EX-99 recast holding the concept with no ix:header of its own.
_RECAST = ("ex99.htm", _inline("<p>Dividends</p>" + _fact("x1", "0.39") + _fact("x2", "1.75", scale="3")))
_PLAIN = ("ex31.htm", b"<html><body><p>Certification &nbsp;</p></body></html>")


def _page(types: dict[str, str]) -> str:
    return "".join(_record(t, i, name) for i, (name, t) in enumerate(types.items(), start=1))


def _read(members: list[tuple[str, bytes]], types: dict[str, str], *, archive: bool = True) -> list[tuple[object, ...]]:
    """The occurrences, as (document, ordinal, id, value), of the source acquired from the archive or the listing."""
    provider = _Provider(
        _zip(members) if archive else None,
        listing=[name for name, _ in members],
        files=dict(members),
        header=_page(types),
    )
    source = acquire_document_source(provider, _target())
    assert source.path == ("archive" if archive else "listing")
    occurrences = read_document_set(source)
    assert occurrences is not None
    return [(o.document, o.element_ordinal, o.fact_id, o.instance_value) for o in occurrences]


@pytest.mark.parametrize("archive", [True, False], ids=["archive", "listing"])
def test_the_header_less_member_is_read_on_either_path(archive: bool) -> None:
    occurrences = _read(
        [_PRIMARY, _RECAST, _PLAIN], {"main.htm": "8-K", "ex99.htm": "EX-99.1", "ex31.htm": "EX-31.1"}, archive=archive
    )
    assert occurrences == [
        ("main.htm", 0, "m1", Decimal("0.39")),
        ("ex99.htm", 0, "x1", Decimal("0.39")),
        ("ex99.htm", 1, "x2", Decimal("1750")),
    ]


def test_a_separate_set_member_is_dropped() -> None:
    fees = ("fees.htm", _inline(_fact("f1", "9.99"), header=True))
    occurrences = _read([_PRIMARY, fees], {"main.htm": "10-K", "fees.htm": "EX-FILING FEES"})
    assert occurrences == [("main.htm", 0, "m1", Decimal("0.39"))]


@pytest.mark.parametrize("excluded", ["EX-98", "EX-FILING FEES"])
def test_only_separate_set_members_is_not_inline(excluded: str) -> None:
    provider = _Provider(_zip([_PRIMARY, _PLAIN]), header=_page({"main.htm": excluded, "ex31.htm": "EX-31.1"}))
    assert read_document_set(acquire_document_source(provider, _target())) is None


def test_an_archive_with_no_inline_document_is_not_inline() -> None:
    provider = _Provider(_zip([_PLAIN, ("efc.xml", b"<xbrl/>")]), header=_page({"ex31.htm": "EX-31.1"}))
    source = acquire_document_source(provider, _target())
    assert (source.path, len(source.candidates), read_document_set(source)) == ("archive", 1, None)


def test_members_with_no_ix_header_fail_the_attempt() -> None:
    provider = _Provider(_zip([_RECAST]), header=_page({"ex99.htm": "EX-99.1"}))
    with pytest.raises(DocumentSetError, match="no_ix_header"):
        read_document_set(acquire_document_source(provider, _target()))


@pytest.mark.parametrize(
    ("types", "reason"),
    [
        ({"main.htm": "8-K"}, "header_missing:ex99.htm"),
        ({"main.htm": "8-K", "ex99.htm": ""}, "header_type:ex99.htm"),
    ],
)
def test_an_inline_document_without_one_type_fails_the_attempt(types: dict[str, str], reason: str) -> None:
    provider = _Provider(_zip([_PRIMARY, _RECAST]), header=_page(types))
    with pytest.raises(DocumentSetError, match=reason):
        read_document_set(acquire_document_source(provider, _target()))


def test_an_undecidable_candidate_fails_the_attempt_whatever_its_type() -> None:
    damaged = ("fees.htm", _inline(_fact("f1", "9.99"), header=True) + b"<unclosed>")
    provider = _Provider(_zip([_PRIMARY, damaged]), header=_page({"main.htm": "10-K", "fees.htm": "EX-FILING FEES"}))
    with pytest.raises(DocumentSetError, match="undecidable:fees.htm"):
        read_document_set(acquire_document_source(provider, _target()))
