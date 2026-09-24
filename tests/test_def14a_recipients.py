"""#2351 slices 2 / 2b — pure tests for the DEF 14A recipient-suppression rule."""

from __future__ import annotations

from datetime import date

import pytest

from app.services.def14a_recipients import (
    REASON_OTHER_COVER,
    Cover,
    CoverUnresolved,
    Sibling,
    decide,
    extend_by_class,
    instance_url,
    parse_cover_instance,
    symbol_key,
    title_kind,
)


def cover_xml(pairs: list[tuple[str, str]], *, cik: str = "0001657853", extra_context: str = "") -> str:
    """A minimal cover instance: one context per (title, symbol) 12(b) pair."""
    contexts = "".join(
        f'<xbrli:context id="c{i}"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">{cik}'
        f"</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:startDate>2025-01-01</xbrli:startDate>"
        f"<xbrli:endDate>2025-12-31</xbrli:endDate></xbrli:period></xbrli:context>"
        for i in range(len(pairs))
    )
    facts = "".join(
        f'<dei:Security12bTitle contextRef="c{i}">{t}</dei:Security12bTitle>'
        f'<dei:TradingSymbol contextRef="c{i}">{s}</dei:TradingSymbol>'
        f'<dei:SecurityExchangeName contextRef="c{i}">NASDAQ</dei:SecurityExchangeName>'
        for i, (t, s) in enumerate(pairs)
    )
    return (
        '<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance" '
        'xmlns:dei="http://xbrl.sec.gov/dei/2024">'
        '<xbrli:context id="d"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">'
        f"{cik}</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:startDate>2025-01-01</xbrli:startDate>"
        "<xbrli:endDate>2025-12-31</xbrli:endDate></xbrli:period></xbrli:context>"
        f"{contexts}{extra_context}"
        f'<dei:EntityCentralIndexKey contextRef="d">{cik}</dei:EntityCentralIndexKey>'
        '<dei:DocumentPeriodEndDate contextRef="d">2025-12-31</dei:DocumentPeriodEndDate>'
        f"{facts}</xbrli:xbrl>"
    )


@pytest.mark.parametrize(
    ("title", "kind"),
    [
        ("Common Stock, $0.01 par value", "common"),
        ("Class A Ordinary Shares", "common"),
        ("Class C Capital Stock", "common"),
        ("Series N Non-Voting Common Stock, $0.001 par value", "common"),
        ("Warrants to purchase Common Stock", "non_common"),
        ("Common Stock Purchase Warrants", "non_common"),
        ("Series K Warrants, each whole warrant exercisable to purchase one share of common stock", "non_common"),
        ("8.00% Series A Perpetual Strike Preferred Stock, $0.001 par value per share", "non_common"),
        ("Series A Cumulative Redeemable preferred stock", "non_common"),
        ("Common Stock and associated preferred stock purchase rights", "other"),
        ("Preferred Stock Purchase Rights", "other"),
        ("Units, each consisting of one share of Class A common stock and one-half of one warrant", "other"),
        ("Capital Securities", "other"),
        ("8.375% Senior Notes due 2029", "other"),
    ],
)
def test_title_kind(title: str, kind: str) -> None:
    assert title_kind(title) == kind


def test_symbol_key() -> None:
    assert symbol_key(" strk.us ") == "STRK"
    assert symbol_key("ABC.D") == "ABC.D"
    assert symbol_key("ABCD") == "ABCD"


def test_instance_url() -> None:
    assert instance_url("https://www.sec.gov/Archives/edgar/data/1/0001/x10k.htm") == (
        "https://www.sec.gov/Archives/edgar/data/1/0001/x10k_htm.xml"
    )
    assert instance_url("https://www.sec.gov/Archives/edgar/data/1/0001/x10k.txt") is None


def _decide(siblings: list[tuple[int, str]], pairs: list[tuple[str, str]]) -> dict[int, int]:
    """suppressed instrument_id -> witness instrument_id."""
    out = decide(
        accession_number="0000000000-26-000001",
        issuer_cik="0000000001",
        siblings=[Sibling(instrument_id=i, symbol=s) for i, s in siblings],
        cover=Cover(accession="0000000000-26-000000", pairs=frozenset(pairs)),
    )
    return {s.instrument_id: s.witness_instrument_id for s in out}


def test_warrant_sibling_suppressed_common_kept() -> None:
    assert _decide(
        [(1, "HTZ"), (2, "HTZWW")],
        [("Common Stock", "HTZ"), ("Warrants to purchase Common Stock", "HTZWW")],
    ) == {2: 1}


def test_preferred_siblings_including_us_duplicate() -> None:
    pairs = [
        ("Class A common stock", "MSTR"),
        ("8.00% Series A Perpetual Strike Preferred Stock", "STRK"),
        ("10.00% Series A Perpetual Strife Preferred Stock", "STRF"),
    ]
    assert _decide([(1, "MSTR"), (2, "STRK"), (3, "STRK.US"), (4, "STRF")], pairs) == {2: 1, 3: 1, 4: 1}


def test_dual_class_common_untouched() -> None:
    pairs = [("Class A Common Stock", "GOOGL"), ("Class C Capital Stock", "GOOG")]
    assert _decide([(1, "GOOG"), (2, "GOOGL")], pairs) == {}


def test_us_duplicate_never_witnesses_itself() -> None:
    # A preferred with only its own .US duplicate on the cover has no common witness.
    assert _decide([(1, "STRK"), (2, "STRK.US")], [("Series A Preferred Stock", "STRK")]) == {}


def test_no_symbol_match_means_no_suppression() -> None:
    # Wrong-issuer cover: nobody matches.
    assert _decide([(1, "OSG"), (2, "OSG.US")], [("Common Stock", "AMBC")]) == {}
    # Warrant not on the cover: absence never suppresses.
    assert _decide([(1, "HTZ"), (2, "HTZWW")], [("Common Stock", "HTZ")]) == {}


def test_other_titles_neither_suppress_nor_witness() -> None:
    assert (
        _decide(
            [(1, "ABC"), (2, "ABCW")],
            [("8.375% Senior Notes due 2029", "ABC"), ("Warrants", "ABCW")],
        )
        == {}
    )


def test_punctuation_does_not_collide() -> None:
    assert _decide([(1, "ABCD"), (2, "ABC.D")], [("Common Stock", "ABCD"), ("Warrants", "ABC-D")]) == {}


def test_sibling_matching_two_titles_is_ambiguous() -> None:
    pairs = [("Common Stock", "XYZ"), ("Warrants", "XYZW"), ("Series A Preferred Stock", "XYZW")]
    assert _decide([(1, "XYZ"), (2, "XYZW")], pairs) == {}


def test_parse_cover_instance_pairs_and_cik() -> None:
    parsed = parse_cover_instance(cover_xml([("Common Stock", "htz"), ("Warrants to purchase  Common Stock", "HTZWW")]))
    assert parsed.outcome == "pairs"
    assert parsed.entity_ciks == frozenset({"0001657853"})
    assert parsed.pairs == frozenset({("Common Stock", "HTZ"), ("Warrants to purchase Common Stock", "HTZWW")})


def test_parse_cover_instance_drops_ambiguous_symbol() -> None:
    multi = (
        '<dei:Security12bTitle contextRef="m">Common Stock</dei:Security12bTitle>'
        '<dei:Security12bTitle contextRef="m">Preferred Stock</dei:Security12bTitle>'
        '<dei:TradingSymbol contextRef="m">HTZ</dei:TradingSymbol>'
        '<dei:SecurityExchangeName contextRef="m">NASDAQ</dei:SecurityExchangeName>'
        '<xbrli:context id="m"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">0001657853'
        "</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:instant>2025-12-31</xbrli:instant>"
        "</xbrli:period></xbrli:context>"
    )
    parsed = parse_cover_instance(cover_xml([("Common Stock", "HTZ"), ("Warrants", "HTZWW")], extra_context=multi))
    assert parsed.pairs == frozenset({("Warrants", "HTZWW")})


def test_parse_cover_instance_ignores_non_dei_facts() -> None:
    body = cover_xml([("Common Stock", "HTZ")]).replace("xbrl.sec.gov/dei/2024", "example.com/ext")
    parsed = parse_cover_instance(body)
    assert parsed.outcome == "no_pairs"
    assert parsed.entity_ciks == frozenset()


def test_parse_cover_instance_unresolved_on_non_xbrl() -> None:
    with pytest.raises(CoverUnresolved):
        parse_cover_instance("<html><body>Request Rate Threshold Exceeded</body></html>")
    with pytest.raises(CoverUnresolved):
        parse_cover_instance("not xml at all")


def test_parse_cover_instance_keeps_every_co_registrant_cik() -> None:
    # Hertz Global Holdings and The Hertz Corporation file one 10-K; the cover carries both.
    body = cover_xml([("Common Stock", "HTZ")]).replace(
        "</xbrli:xbrl>",
        '<dei:EntityCentralIndexKey contextRef="d">0000047129</dei:EntityCentralIndexKey></xbrli:xbrl>',
    )
    assert parse_cover_instance(body).entity_ciks == frozenset({"0001657853", "0000047129"})


# --- slice 2b: extend_by_class ------------------------------------------------

OPEN_SIBS = [Sibling(1, "OPEN"), Sibling(2, "OPENW")]
OLD_PROXY, NEW_PROXY = "0001140361-25-022644", "0001140361-26-017460"
OLD_COVER = Cover("0001801169-25-000038", frozenset({("Common stock, $0.0001 par value", "OPEN")}))
NEW_COVER = Cover(
    "0001801169-26-000010",
    frozenset({("Common stock, $0.0001 par value", "OPEN"), ("Series K Warrants", "OPENW")}),
)
DATES = {OLD_PROXY: date(2025, 6, 16), NEW_PROXY: date(2026, 4, 28)}
BOTH = {1: {OLD_PROXY, NEW_PROXY}, 2: {OLD_PROXY, NEW_PROXY}}
SYMBOLS = {1: "OPEN", 2: "OPENW"}


def _pit(*covers: tuple[str, Cover]) -> list:
    return [
        s for acc, c in covers for s in decide(accession_number=acc, issuer_cik="1801169", siblings=OPEN_SIBS, cover=c)
    ]


def test_older_proxy_whose_cover_lacks_the_warrant_is_suppressed() -> None:
    pit = _pit((OLD_PROXY, OLD_COVER), (NEW_PROXY, NEW_COVER))
    assert [(s.instrument_id, s.accession_number) for s in pit] == [(2, NEW_PROXY)]
    rows, vetoed = extend_by_class(
        point_in_time=pit,
        proxy_dates=DATES,
        accessions_by_instrument=BOTH,
        symbols=SYMBOLS,
        covers=[OLD_COVER, NEW_COVER],
        blocked=set(),
    )
    assert vetoed == set()
    assert [(s.instrument_id, s.accession_number, s.reason, s.cover_accession) for s in rows] == [
        (2, OLD_PROXY, REASON_OTHER_COVER, NEW_COVER.accession)
    ]


def test_a_cover_calling_the_symbol_common_vetoes() -> None:
    old = Cover(OLD_COVER.accession, OLD_COVER.pairs | {("Common stock", "OPENW")})
    pit = _pit((OLD_PROXY, old), (NEW_PROXY, NEW_COVER))
    rows, vetoed = extend_by_class(
        point_in_time=pit,
        proxy_dates=DATES,
        accessions_by_instrument=BOTH,
        symbols=SYMBOLS,
        covers=[old, NEW_COVER],
        blocked=set(),
    )
    assert rows == [] and vetoed == {2}


def test_blocked_instrument_is_not_extended() -> None:
    pit = _pit((NEW_PROXY, NEW_COVER))
    rows, vetoed = extend_by_class(
        point_in_time=pit,
        proxy_dates=DATES,
        accessions_by_instrument=BOTH,
        symbols=SYMBOLS,
        covers=[NEW_COVER],
        blocked={2},
    )
    assert rows == [] and vetoed == set()


def test_no_point_in_time_decision_means_nothing() -> None:
    rows, _ = extend_by_class(
        point_in_time=_pit((OLD_PROXY, OLD_COVER)),
        proxy_dates=DATES,
        accessions_by_instrument=BOTH,
        symbols=SYMBOLS,
        covers=[OLD_COVER],
        blocked=set(),
    )
    assert rows == []


def test_targets_only_accessions_the_instrument_holds_and_evidence_is_latest_proxy() -> None:
    mid = "0001140361-25-099999"
    dates = {**DATES, mid: date(2025, 12, 1)}
    pit = _pit((mid, NEW_COVER), (NEW_PROXY, NEW_COVER))
    held = {1: {OLD_PROXY, mid, NEW_PROXY}, 2: {mid, NEW_PROXY}}  # OPENW holds no OLD_PROXY rows
    rows, _ = extend_by_class(
        point_in_time=pit,
        proxy_dates=dates,
        accessions_by_instrument=held,
        symbols=SYMBOLS,
        covers=[NEW_COVER],
        blocked=set(),
    )
    assert rows == []
    held[2].add(OLD_PROXY)
    rows, _ = extend_by_class(
        point_in_time=pit,
        proxy_dates=dates,
        accessions_by_instrument=held,
        symbols=SYMBOLS,
        covers=[NEW_COVER],
        blocked=set(),
    )
    assert [s.accession_number for s in rows] == [OLD_PROXY]
