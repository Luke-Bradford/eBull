"""Tax character, pure (#3616 slice 3d-3a; spec docs/specs/metrics/2026-10-14-3616-slice3d3a-tax-character.md,
"Tests" 1-5). Instances are built here with the facts the spec names; the Energy Transfer ("Common Units", ET) class
is the spec's fixture."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import httpx
import pytest

from app.config import settings
from app.services.tax_character import (
    INSTANCE_MAX_BYTES,
    Cover,
    CoverFiling,
    FetchedInstance,
    Profile,
    SecurityEvidence,
    canonical_cik,
    classify,
    common_title,
    consolidate,
    document_entry,
    fetch_instance,
    read_cover,
    structurally_sound,
)
from tests.fixtures.xbrl_cover import AAPL_CIK
from tests.fixtures.xbrl_cover import cover as _cover
from tests.fixtures.xbrl_cover import ctx as _ctx
from tests.fixtures.xbrl_cover import fact as _fact
from tests.fixtures.xbrl_cover import instance as _instance

AS_OF = datetime(2026, 10, 14, 12, tzinfo=UTC)
NXH_CIK = "0001130713"
AAPL_10Q = _cover(
    AAPL_CIK,
    [
        ("Common Stock, $0.00001 par value per share", "AAPL"),
        ("1.375% Notes due 2029", None),
        ("0.500% Notes due 2031", None),
    ],
)
NXH_TODAY = _cover(NXH_CIK, [("Common stock, $0.0001 par value per share", "NXH"), ("Warrants", "NXHWW")])
NXH_START = _cover(NXH_CIK, [("Common Stock, $0.0001 par value per share", "BBBY")])
ET_10K = _cover("0001276187", [("Common Units", "ET")])

LATEST = CoverFiling("0000320193-26-000020", date(2026, 7, 31), "https://www.sec.gov/Archives/aapl-20260627.htm")
START = CoverFiling("0000320193-26-000010", date(2026, 5, 1), "https://www.sec.gov/Archives/aapl-20260328.htm")


def _aapl(**changes: object) -> SecurityEvidence:
    """A share: both covers stored and read, the holding started after the start cover."""
    ev = SecurityEvidence(
        types=frozenset({5}),
        ciks=frozenset({AAPL_CIK}),
        symbol="AAPL",
        symbols_ever=frozenset({"AAPL"}),
        profiles=(Profile("operating", "3571", AS_OF - timedelta(days=5)),),
        filings=(LATEST, START),
        first_acquisition=date(2026, 6, 1),
        covers={LATEST.accession: read_cover(AAPL_10Q), START.accession: read_cover(AAPL_10Q)},
    )
    return replace(ev, **changes)  # type: ignore[arg-type]


def _covers(latest: bytes | None, start: bytes | None = AAPL_10Q) -> dict[str, Cover | None]:
    out: dict[str, Cover | None] = {}
    if latest is not None:
        out[LATEST.accession] = read_cover(latest)
    if start is not None:
        out[START.accession] = read_cover(start)
    return out


# --- 1. each line of the table alone, in order ------------------------------------------------------------------------


def _profile(days: float = 5, entity_type: str = "operating", sic: str | None = "3571") -> Profile:
    return Profile(entity_type, sic, AS_OF - timedelta(days=days))


@pytest.mark.parametrize(
    ("changes", "detail"),
    [
        ({"types": frozenset({5, 6})}, "member_conflict"),
        ({"types": frozenset({None, 5})}, "member_conflict"),
        ({"ciks": frozenset({AAPL_CIK, NXH_CIK})}, "member_conflict"),
        ({"types": frozenset({6})}, "exchange_traded_fund"),
        ({"types": frozenset({4})}, "instrument_type"),
        ({"ciks": frozenset()}, "cik_unestablished"),
        ({"ciks": frozenset({"CIK320193"})}, "cik_unestablished"),  # a malformed stored value is no CIK
        ({"profiles": ()}, "sec_profile_unavailable"),
        ({"profiles": (None,)}, "sec_profile_unavailable"),
        (
            {"profiles": (Profile("operating", "3571", AS_OF - timedelta(days=30, microseconds=1)),)},
            "sec_profile_unavailable",
        ),
        ({"profiles": (Profile("operating", "3571", AS_OF + timedelta(microseconds=1)),)}, "sec_profile_unavailable"),
        ({"profiles": (_profile(), _profile(31))}, "sec_profile_unavailable"),
        ({"profiles": (_profile(), _profile(sic="3572"))}, "sec_profile_conflict"),
        ({"profiles": (_profile(entity_type="other"),)}, "entity_type"),
        *[
            ({"profiles": (_profile(sic=sic),)}, "sic")
            for sic in ("6770", "6722", "6726", "6799", "", " 3571", "6722\n", None)
        ],
        ({"filings": (LATEST, replace(LATEST, filed_on=date(2026, 8, 1)))}, "cover_metadata_conflict"),
        ({"filings": ()}, "cover_unavailable"),
        ({"covers": _covers(None)}, "cover_unavailable"),
        ({"covers": {LATEST.accession: None, START.accession: read_cover(AAPL_10Q)}}, "cover_unusable"),
        # AAPL's only EntityCentralIndexKey, its class reported in a context identifying NXH: the CIKs are two.
        (
            {
                "covers": _covers(
                    _instance(
                        [_ctx("c0"), _ctx("c1", NXH_CIK)],
                        [
                            _fact("EntityCentralIndexKey", "c0", AAPL_CIK),
                            _fact("Security12bTitle", "c1", "Common Stock"),
                            _fact("TradingSymbol", "c1", "AAPL"),
                        ],
                    )
                )
            },
            "cover_unusable",
        ),
        # Two CIKs, one ours, our symbol on a common class.
        ({"covers": _covers(_cover(AAPL_CIK, [("Common Stock", "AAPL")], ecik=NXH_CIK))}, "cover_unusable"),
        # A dei namespace not SEC's is not read: no CIK, so not usable for ours.
        (
            {
                "covers": _covers(
                    _instance(
                        [_ctx("c0")],
                        [_fact("Security12bTitle", "c0", "Common Stock"), _fact("TradingSymbol", "c0", "AAPL")],
                        dei="https://example.invalid/xbrl.sec.gov/dei/2026",
                    )
                )
            },
            "cover_unusable",
        ),
        # Our symbol in two contexts: "Common Stock" complete, "Common Units" without its exchange tag.
        (
            {
                "covers": _covers(
                    _instance(
                        [_ctx("c0"), _ctx("c1"), _ctx("c2")],
                        [
                            _fact("EntityCentralIndexKey", "c0", AAPL_CIK),
                            _fact("Security12bTitle", "c1", "Common Stock"),
                            _fact("TradingSymbol", "c1", "AAPL"),
                            _fact("SecurityExchangeName", "c1", "NASDAQ"),
                            _fact("Security12bTitle", "c2", "Common Units"),
                            _fact("TradingSymbol", "c2", "AAPL"),
                        ],
                    )
                )
            },
            "listed_class_not_common_stock",
        ),
        # One context with two titles and our symbol.
        (
            {
                "covers": _covers(
                    _instance(
                        [_ctx("c0")],
                        [
                            _fact("EntityCentralIndexKey", "c0", AAPL_CIK),
                            _fact("Security12bTitle", "c0", "Common Stock"),
                            _fact("Security12bTitle", "c0", "Preferred Stock"),
                            _fact("TradingSymbol", "c0", "AAPL"),
                        ],
                    )
                )
            },
            "listed_class_not_common_stock",
        ),
        # The Energy Transfer fixture: the listed class is partnership units.
        (
            {
                "ciks": frozenset({"0001276187"}),
                "symbol": "ET",
                "symbols_ever": frozenset({"ET"}),
                "covers": {LATEST.accession: read_cover(ET_10K), START.accession: read_cover(ET_10K)},
            },
            "listed_class_not_common_stock",
        ),
        ({"first_acquisition": date(2026, 4, 30)}, "holding_start_cover_unavailable"),
        ({"covers": _covers(AAPL_10Q, None)}, "holding_start_cover_unavailable"),
        ({"covers": {LATEST.accession: read_cover(AAPL_10Q), START.accession: None}}, "holding_start_cover_unusable"),
        (
            {"covers": _covers(AAPL_10Q, _cover(AAPL_CIK, [("Common Stock", "UNRELATED")]))},
            "holding_start_class_not_common_stock",
        ),
        (
            {
                "symbols_ever": frozenset({"AAPL", "BBBY"}),
                "covers": _covers(AAPL_10Q, _cover(AAPL_CIK, [("Common Units", "BBBY")])),
            },
            "holding_start_class_not_common_stock",
        ),
    ],
)
def test_each_line_of_the_table_alone(changes: dict[str, object], detail: str) -> None:
    assert classify(_aapl(**changes), AS_OF) == ("unestablished", detail)


def test_a_profile_exactly_30_days_old_is_current() -> None:
    ev = _aapl(profiles=(Profile("operating", "3571", AS_OF - timedelta(days=30)),))
    assert classify(ev, AS_OF) == ("share", None)


def _ok(cid: str = "c0") -> str:
    return _fact("EntityCentralIndexKey", cid, AAPL_CIK)


@pytest.mark.parametrize(
    "body",
    [
        b"<html><body>Not found</body></html>",
        b"not xml at all",
        _instance([_ctx("c0")], [_ok()], root="http://example.invalid/instance"),
        _instance([_ctx("c0")], [_ok("c9")]),  # a dei fact on an undefined context
        _instance([_ctx("c0").replace(' id="c0"', "")], [_ok()]),  # a context without an id
        _instance([_ctx("c0"), _ctx("c0")], [_ok()]),  # two contexts with one id
        _instance([_ctx("c0")], [_fact("EntityCentralIndexKey", None, AAPL_CIK)]),  # a fact without contextRef
        _instance([_ctx("c0")], [_fact("EntityCentralIndexKey", "c0", "CIK320193")]),  # a non-digit CIK
        _instance([_ctx("c0", where="none")], [_ok()]),  # a context without an entity identifier
        _instance([_ctx("c0", scheme="http://example.invalid/id")], [_ok()]),  # a non-SEC scheme
        _instance([_ctx("c0", where="context")], [_ok()]),  # an identifier directly under the context
        _instance([_ctx("c0", where="scenario")], [_ok()]),  # an identifier under xbrli:scenario
    ],
)
def test_unusable_instances(body: bytes) -> None:
    assert read_cover(body) is None


def test_an_instance_over_16_mib_is_unusable_and_not_sound() -> None:
    padding = b"<!--" + b"x" * (INSTANCE_MAX_BYTES - len(AAPL_10Q)) + b"-->"
    body = AAPL_10Q.replace(b"</xbrli:xbrl>", padding + b"</xbrli:xbrl>")
    assert len(body) > INSTANCE_MAX_BYTES
    assert read_cover(body) is None and not structurally_sound(body)
    assert structurally_sound(AAPL_10Q)


# --- 2. titles --------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "title",
    [
        "Common Stock, $0.00001 par value per share",
        "Common stock, $0.0001 par value per share",
        "Class A Common Stock",
        "Common Stock, $0.01 par value",
        "Common Stock, par value $0.001 per share",
        "Ordinary Shares, US$0.0001 par value per share",
        "Common Stock, no par value",
        "Common Stock\n",
    ],
)
def test_common_titles(title: str) -> None:
    assert common_title(title)


@pytest.mark.parametrize(
    "title",
    [
        "Common Stock Warrants",
        "Common Stock Units",
        "Ordinary Shares of Beneficial Interest",
        "Common Shares",
        "Common Units",
        "Warrants to Purchase Common Stock",
        "Class A Common Stock and one-third of one Redeemable Warrant",
    ],
)
def test_titles_that_are_not_common(title: str) -> None:
    assert not common_title(title)


# --- 3. shares --------------------------------------------------------------------------------------------------------


def test_aapl_is_a_share_its_untraded_notes_ignored() -> None:
    cover = read_cover(AAPL_10Q)
    assert cover == Cover(
        frozenset({AAPL_CIK}), frozenset({("Common Stock, $0.00001 par value per share", "AAPL")}), frozenset()
    )
    assert classify(_aapl(), AS_OF) == ("share", None)


def _nxh(symbols_ever: frozenset[str]) -> SecurityEvidence:
    today = CoverFiling("0001140361-26-038800", date(2026, 10, 6), "https://www.sec.gov/Archives/nxh-8k.htm")
    start = CoverFiling("0001140361-25-042343", date(2025, 11, 14), "https://www.sec.gov/Archives/bbby-8k.htm")
    return SecurityEvidence(
        types=frozenset({5}),
        ciks=frozenset({NXH_CIK}),
        symbol="NXH",
        symbols_ever=symbols_ever,
        profiles=(Profile("operating", "5961", AS_OF - timedelta(hours=3)),),
        filings=(today, start),
        first_acquisition=date(2025, 12, 1),
        covers={today.accession: read_cover(NXH_TODAY), start.accession: read_cover(NXH_START)},
    )


def test_nxh_is_a_share_through_its_symbol_history() -> None:
    assert classify(_nxh(frozenset({"NXH", "BBBY"})), AS_OF) == ("share", None)
    assert classify(_nxh(frozenset({"NXH"})), AS_OF) == ("unestablished", "holding_start_class_not_common_stock")


# --- 4. keying (the pure half) ----------------------------------------------------------------------------------------


def test_one_accession_on_two_members_counts_once_and_two_dates_conflict() -> None:
    base = CoverFiling("0000320193-26-000020", date(2026, 7, 31), "https://www.sec.gov/b.htm")
    variant = replace(base, primary_document_url="https://www.sec.gov/a.htm")
    assert consolidate([base, variant]) == [variant]  # the smallest primary-document URL represents it
    assert consolidate([base, replace(base, filed_on=date(2026, 8, 1))]) is None
    assert classify(_aapl(filings=(LATEST, START, LATEST)), AS_OF) == ("share", None)


# --- 5. the document entry --------------------------------------------------------------------------------------------


def test_the_document_entry_carries_inputs_not_dates() -> None:
    base = document_entry(1001, _aapl(), AS_OF)
    assert base == [
        1001,
        [5],
        [AAPL_CIK],
        "AAPL",
        ["AAPL"],
        ["operating", "3571"],
        [LATEST.accession, [[AAPL_CIK], [["Common Stock, $0.00001 par value per share", "AAPL"]], []]],
        [START.accession, [[AAPL_CIK], [["Common Stock, $0.00001 par value per share", "AAPL"]], []]],
    ]
    # The same profile refreshed: no change.
    assert document_entry(1001, _aapl(profiles=(_profile(1),)), AS_OF) == base
    newer = CoverFiling("0000320193-26-000030", date(2026, 10, 1), "https://www.sec.gov/c.htm")
    changed = [
        _aapl(filings=(newer, LATEST, START)),  # a new cover
        _aapl(covers=_covers(None)),  # the newest cover's instance absent
        _aapl(covers=_covers(_cover(AAPL_CIK, [("Common Units", "AAPL")]))),  # a changed class
        _aapl(profiles=(_profile(sic="3572"),)),
        _aapl(profiles=(_profile(entity_type="other"),)),
        _aapl(profiles=(_profile(31),)),  # gone stale
    ]
    for ev in changed:
        assert document_entry(1001, ev, AS_OF) != base
    # An absent instance A replaced by an absent instance B.
    absent_a = document_entry(1001, _aapl(filings=(LATEST,), covers={}, first_acquisition=None), AS_OF)
    absent_b = document_entry(1001, _aapl(filings=(newer,), covers={}, first_acquisition=None), AS_OF)
    assert absent_a[6] == [LATEST.accession, "absent"] and absent_a != absent_b
    assert document_entry(1001, _aapl(covers={LATEST.accession: None}), AS_OF)[6] == [LATEST.accession, "unusable"]
    conflict = _aapl(filings=(LATEST, replace(LATEST, filed_on=date(2026, 8, 1))))
    assert document_entry(1001, conflict, AS_OF)[6:] == ["metadata_conflict", "metadata_conflict"]
    assert document_entry(1001, _aapl(first_acquisition=None), AS_OF)[7] is None
    assert document_entry(1001, _aapl(profiles=()), AS_OF)[5] == "unavailable"
    assert document_entry(1001, _aapl(profiles=(_profile(), _profile(sic="1"))), AS_OF)[5] == "conflict"


# --- 6. the bounded fetch (pure half) ---------------------------------------------------------------------------------


def test_fetch_instance_is_bounded_and_reports_every_failure() -> None:
    big = b"x" * (INSTANCE_MAX_BYTES + 1)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/down":
            raise httpx.ConnectError("down", request=request)
        assert request.headers["User-Agent"] == settings.sec_user_agent
        bodies = {"/ok": (200, AAPL_10Q), "/big": (200, big), "/gone": (404, b"")}
        status, body = bodies[request.url.path]
        return httpx.Response(status, content=body)

    with httpx.Client(transport=httpx.MockTransport(handler), base_url="https://www.sec.gov") as client:
        assert fetch_instance(client, "https://www.sec.gov/ok") == FetchedInstance(AAPL_10Q)
        assert fetch_instance(client, "https://www.sec.gov/big") == FetchedInstance(
            None, f"refused: over {INSTANCE_MAX_BYTES} bytes"
        )
        assert fetch_instance(client, "https://www.sec.gov/gone") == FetchedInstance(None, "http 404")
        assert fetch_instance(client, "https://www.sec.gov/down") == FetchedInstance(
            None, "transport ConnectError: down"
        )


def test_stored_ciks_are_padded_and_a_malformed_one_kept_to_fail_closed() -> None:
    assert canonical_cik("320193") == AAPL_CIK
    assert canonical_cik(" 0000320193 ") == AAPL_CIK
    assert canonical_cik("CIK320193") == "CIK320193"
    assert canonical_cik("12345678901") == "12345678901"
