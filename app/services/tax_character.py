"""Tax character from listed-class evidence (#3616 slice 3d-3a).

Spec: docs/specs/metrics/2026-10-14-3616-slice3d3a-tax-character.md.

- ``read_cover`` is the strict reader of a cover's extracted inline-XBRL instance (Design 2).
- ``classify`` is the ``tax_character_v1`` table (Design 2): ``share``, or ``unestablished`` with its detail.
  ``tax_character_v2`` (slice 3d-3c-3 Design 1) keeps it and replaces only its ``exchange_traded_fund`` line, in
  ``tax_matching``: a fund whose reporting status is ``non_reporting_assumed`` is ``non_reporting_fund``.
- ``read_evidence`` reads each tax security's character inputs in the caller's snapshot, the stored instances of its
  two selected covers included (Design 5).
- ``fetch_instance`` is the bounded fetch ``tax_matching.refresh_cover_instances`` stores from (Design 1).

An exchange-traded fund is never a share: interests in a non-reporting fund are relevant securities outside the s104
pool (TCGA s104(3), s106A), which slice 3d-3c-3 identifies.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Literal

import httpx
import lxml.etree as ET
import psycopg

from app.config import settings
from app.providers.sec_rate_gate_holder import get_sec_rate_gate
from app.services.def14a_recipients import symbol_key
from app.services.xbrl_instance import SAFE_XML_PARSER

#: The frozen local policy (spec "Local policy"); changing any part of it is a new id.
TAX_CHARACTER_POLICY_ID = "tax_character_v2"

INSTANCE_MAX_BYTES = 16 * 1024 * 1024
PROFILE_MAX_AGE = timedelta(days=30)
#: eToro's ``instrument_type_id`` values.
STOCKS_TYPE_ID = 5
ETF_TYPE_ID = 6
#: SEC's investment-vehicle codes: open-end funds, unit trusts and closed-end funds, blank checks, investors n.e.c.
INVESTMENT_SIC = frozenset({"6722", "6726", "6770", "6799"})
COVER_FORMS = ("10-K", "10-Q", "8-K")

XBRLI = "http://www.xbrl.org/2003/instance"
SEC_CIK_SCHEME = "http://www.sec.gov/CIK"
_DEI = re.compile(r"http://xbrl\.sec\.gov/dei/[0-9]{4}")
_SIC = re.compile(r"[0-9]{4}")
_CIK = re.compile(r"[0-9]{10}")
#: The accession format ``tax_cover_instances`` admits (its CHECK).
ACCESSION = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")
_COMMON = re.compile(
    r"(CLASS [A-Z] )?(COMMON STOCK|ORDINARY SHARES)"
    r"(,? ((US)?\$?[0-9]*\.?[0-9]+ PAR VALUE|PAR VALUE (US)?\$?[0-9]*\.?[0-9]+|NO PAR VALUE)( PER SHARE)?)?"
)

Character = Literal["share", "unestablished"]

# Reasons every disposal and pool of a share carries (Design 3): assumptions A6 and A7.
OFFSHORE_FUND_EXCLUSION_ASSUMED = "offshore_fund_exclusion_assumed"
ISSUER_BINDING_ASSUMED = "issuer_binding_assumed"
#: The detail of a member of an unestablished identity component, decided without evaluation.
IDENTITY_UNESTABLISHED = "identity_unestablished"
METADATA_CONFLICT = "metadata_conflict"


@dataclass(frozen=True)
class Profile:
    entity_type: str | None
    sic: str | None
    fetched_at: datetime


@dataclass(frozen=True)
class CoverFiling:
    accession: str
    filed_on: date
    primary_document_url: str | None


@dataclass(frozen=True)
class Cover:
    """A strictly read cover instance: its CIKs, unambiguous (title, symbol) classes and ambiguous symbols."""

    entity_ciks: frozenset[str]
    pairs: frozenset[tuple[str, str]]
    ambiguous: frozenset[str]


@dataclass(frozen=True)
class SecurityEvidence:
    """One tax security's character inputs (Design 2), from every member of its component."""

    #: Each member's ``instrument_type_id`` (None for NULL).
    types: frozenset[int | None]
    #: Every member's primary SEC CIK.
    ciks: frozenset[str]
    #: The security's own row's symbol under ``symbol_key``; None when it has no row.
    symbol: str | None
    symbols_ever: frozenset[str]
    #: One per member carrying a CIK; None where it has no ``instrument_sec_profile`` row.
    profiles: tuple[Profile | None, ...]
    filings: tuple[CoverFiling, ...]
    first_acquisition: date | None
    #: Accession → strictly read instance (None: unusable), for the selected covers; absent when none is stored.
    covers: Mapping[str, Cover | None]


# --- the strict reader -------------------------------------------------------------------------------------------


def _xbrl_root(body: bytes) -> Any | None:
    """The parsed root when the body is at most ``INSTANCE_MAX_BYTES``, parses with ``SAFE_XML_PARSER`` and its root
    is ``xbrli:xbrl``; else None."""
    if len(body) > INSTANCE_MAX_BYTES:
        return None
    try:
        root = ET.fromstring(body, parser=SAFE_XML_PARSER)
    except ET.XMLSyntaxError:
        return None
    return root if isinstance(root.tag, str) and ET.QName(root).text == f"{{{XBRLI}}}xbrl" else None


def structurally_sound(body: bytes) -> bool:
    """The test a fetched body passes before it is stored (Design 1)."""
    return _xbrl_root(body) is not None


def read_cover(body: bytes) -> Cover | None:
    """Strict tax reader of SEC's extracted instance; None means unusable (Design 2).

    Unusable: over ``INSTANCE_MAX_BYTES``, not XBRL, a context without a unique non-empty id, a ``dei`` fact of the
    three read without a ``contextRef`` naming a context whose single direct entity holds a single direct SEC-scheme
    all-digit identifier, or a non-digit ``EntityCentralIndexKey``. The CIKs are those facts' values together with the
    identifiers of every context they use. A symbol whose context has several titles or symbols, or that two contexts
    give different titles, is ambiguous; a context with a title and no symbol is ignored (untraded notes).
    """
    root = _xbrl_root(body)
    if root is None:
        return None
    ids = [el.get("id") or "" for el in root.iter(f"{{{XBRLI}}}context")]
    if "" in ids or len(set(ids)) != len(ids):
        return None
    issuer: dict[str, str | None] = {}
    for ctx in root.iter(f"{{{XBRLI}}}context"):
        entities = ctx.findall(f"{{{XBRLI}}}entity")
        idents = entities[0].findall(f"{{{XBRLI}}}identifier") if len(entities) == 1 else []
        text = (idents[0].text or "").strip() if len(idents) == 1 else ""
        sec = len(idents) == 1 and idents[0].get("scheme") == SEC_CIK_SCHEME and text.isdigit()
        issuer[str(ctx.get("id"))] = text.zfill(10) if sec else None
    ciks: set[str] = set()
    by_ctx: dict[str, dict[str, set[str]]] = {}
    for el in root.iter():
        if not isinstance(el.tag, str):
            continue
        q = ET.QName(el)
        if not (q.namespace and _DEI.fullmatch(q.namespace)):
            continue
        if q.localname not in ("EntityCentralIndexKey", "Security12bTitle", "TradingSymbol"):
            continue
        ref = el.get("contextRef") or ""
        bound = issuer.get(ref)
        if bound is None:
            return None
        ciks.add(bound)
        text = " ".join("".join(el.itertext()).split())
        if q.localname == "EntityCentralIndexKey":
            if not text.isdigit():
                return None
            ciks.add(text.zfill(10))
        else:
            by_ctx.setdefault(ref, {}).setdefault(q.localname, set()).add(text)
    titles_of: dict[str, set[str]] = {}
    ambiguous: set[str] = set()
    for facts in by_ctx.values():
        symbols = {symbol_key(x) for x in facts.get("TradingSymbol", set()) if x}
        titles = facts.get("Security12bTitle", set())
        if len(symbols) == 1 and len(titles) == 1:
            titles_of.setdefault(next(iter(symbols)), set()).update(titles)
        else:
            ambiguous |= symbols
    ambiguous |= {sym for sym, ts in titles_of.items() if len(ts) > 1}
    pairs = frozenset((next(iter(ts)), sym) for sym, ts in titles_of.items() if sym not in ambiguous)
    return Cover(frozenset(ciks), pairs, frozenset(ambiguous))


def canonical_cik(value: str) -> str:
    """A stored CIK zero-padded to ten digits when it is one to ten digits; any other value unchanged, so ``classify``
    finds it malformed (``cik_unestablished``)."""
    text = value.strip()
    return text.zfill(10) if text.isdigit() and len(text) <= 10 else value


def is_cik(value: str) -> bool:
    return _CIK.fullmatch(value) is not None


def common_title(title: str) -> bool:
    """The title grammar of Design 2, whitespace collapsed and upper-cased, matched in full."""
    return _COMMON.fullmatch(" ".join(title.split()).upper()) is not None


# --- cover selection and the table -------------------------------------------------------------------------------


def consolidate(filings: Iterable[CoverFiling]) -> list[CoverFiling] | None:
    """One filing per accession (the smallest primary-document URL represents it), newest first by (date,
    accession); None when one accession carries two dates."""
    by_acc: dict[str, CoverFiling] = {}
    for f in filings:
        seen = by_acc.get(f.accession)
        if seen is not None and seen.filed_on != f.filed_on:
            return None
        if seen is None or (f.primary_document_url or "") < (seen.primary_document_url or ""):
            by_acc[f.accession] = f
    return sorted(by_acc.values(), key=lambda f: (f.filed_on, f.accession), reverse=True)


@dataclass(frozen=True)
class Selection:
    """The covers Design 2 reads; ``conflict`` when an accession carries two dates (then neither is selected)."""

    conflict: bool
    latest: CoverFiling | None
    #: The newest filed on or before the first acquisition; None with no acquisition or no such cover.
    start: CoverFiling | None


def select_covers(filings: Iterable[CoverFiling], first_acquisition: date | None) -> Selection:
    ordered = consolidate(filings)
    if ordered is None:
        return Selection(True, None, None)
    start = None
    if first_acquisition is not None:
        start = next((f for f in ordered if f.filed_on <= first_acquisition), None)
    return Selection(False, ordered[0] if ordered else None, start)


def _current(p: Profile | None, as_of: datetime) -> bool:
    return p is not None and as_of - PROFILE_MAX_AGE <= p.fetched_at <= as_of


def classify(ev: SecurityEvidence, as_of: datetime) -> tuple[Character, str | None]:
    """``tax_character_v1``: the first line of the table that applies decides; detail None for a share."""
    if len(ev.types) != 1 or len(ev.ciks) > 1:
        return "unestablished", "member_conflict"
    (kind,) = ev.types
    if kind == ETF_TYPE_ID:
        return "unestablished", "exchange_traded_fund"
    if kind != STOCKS_TYPE_ID:
        return "unestablished", "instrument_type"
    if not ev.ciks or ev.symbol is None:
        return "unestablished", "cik_unestablished"
    (cik,) = ev.ciks
    if not _CIK.fullmatch(cik):  # ``external_identifiers`` constrains no format: a malformed value is no CIK
        return "unestablished", "cik_unestablished"
    if not ev.profiles or not all(_current(p, as_of) for p in ev.profiles):
        return "unestablished", "sec_profile_unavailable"
    facts = {(p.entity_type, p.sic) for p in ev.profiles if p is not None}
    if len(facts) != 1:
        return "unestablished", "sec_profile_conflict"
    ((entity_type, sic),) = facts
    if entity_type != "operating":
        return "unestablished", "entity_type"
    if sic is None or not _SIC.fullmatch(sic) or sic in INVESTMENT_SIC:
        return "unestablished", "sic"
    selection = select_covers(ev.filings, ev.first_acquisition)
    if selection.conflict:
        return "unestablished", "cover_metadata_conflict"

    def read(filing: CoverFiling, prefix: str) -> Cover | str:
        if filing.accession not in ev.covers:
            return f"{prefix}cover_unavailable"
        got = ev.covers[filing.accession]
        if got is None or got.entity_ciks != {cik}:
            return f"{prefix}cover_unusable"
        return got

    if selection.latest is None:
        return "unestablished", "cover_unavailable"
    latest = read(selection.latest, "")
    if isinstance(latest, str):
        return "unestablished", latest
    listed = [t for t, s in latest.pairs if s == ev.symbol]
    if ev.symbol in latest.ambiguous or len(listed) != 1 or not common_title(listed[0]):
        return "unestablished", "listed_class_not_common_stock"
    if ev.first_acquisition is not None:
        if selection.start is None:
            return "unestablished", "holding_start_cover_unavailable"
        then = read(selection.start, "holding_start_")
        if isinstance(then, str):
            return "unestablished", then
        bound = [t for t, s in then.pairs if s in ev.symbols_ever]
        if then.ambiguous & ev.symbols_ever or len(bound) != 1 or not common_title(bound[0]):
            return "unestablished", "holding_start_class_not_common_stock"
    return "share", None


# --- the inputs document entry (Design 4) ------------------------------------------------------------------------


def document_entry(tax_security_id: int, ev: SecurityEvidence, as_of: datetime) -> list[Any]:
    """``[tax_security_id, types, ciks, symbol, symbols_ever, profile, latest, start]``: inputs, not dates, so a
    profile refreshed with the same values changes nothing while one going stale does."""
    if not ev.profiles or not all(_current(p, as_of) for p in ev.profiles):
        profile: Any = "unavailable"
    else:
        facts = {(p.entity_type, p.sic) for p in ev.profiles if p is not None}
        profile = "conflict" if len(facts) != 1 else list(next(iter(facts)))
    selection = select_covers(ev.filings, ev.first_acquisition)

    def state(filing: CoverFiling | None) -> Any:
        if selection.conflict:
            return METADATA_CONFLICT
        if filing is None:
            return None
        if filing.accession not in ev.covers:
            cover: Any = "absent"
        else:
            got = ev.covers[filing.accession]
            cover = (
                "unusable"
                if got is None
                else [sorted(got.entity_ciks), sorted([t, s] for t, s in got.pairs), sorted(got.ambiguous)]
            )
        return [filing.accession, cover]

    return [
        tax_security_id,
        sorted(ev.types, key=lambda t: (t is None, t or 0)),
        sorted(ev.ciks),
        ev.symbol,
        sorted(ev.symbols_ever),
        profile,
        state(selection.latest),
        None if ev.first_acquisition is None else state(selection.start),
    ]


# --- reading (Design 5) -------------------------------------------------------------------------------------------


def read_evidence(
    conn: psycopg.Connection[Any],
    securities: Mapping[int, tuple[int, ...]],
    first_acquisitions: Mapping[int, date],
    *,
    bodies: bool = True,
) -> dict[int, SecurityEvidence]:
    """Each security's character inputs, in the caller's snapshot. ``securities``: tax security → its members.

    With ``bodies``, the stored instances of the two selected covers of each security are read by ``read_cover``,
    each after its size is checked from ``octet_length(body)``; without, ``covers`` is empty.
    """
    members = sorted({m for ms in securities.values() for m in ms})
    rows = {
        r[0]: (r[1], r[2])
        for r in conn.execute(
            "SELECT instrument_id, symbol, instrument_type_id FROM instruments WHERE instrument_id = ANY(%s)",
            (members,),
        ).fetchall()
    }
    ciks: dict[int, set[str]] = {}
    for iid, value in conn.execute(
        "SELECT instrument_id, identifier_value FROM external_identifiers WHERE instrument_id = ANY(%s) "
        "AND provider = 'sec' AND identifier_type = 'cik' AND is_primary",
        (members,),
    ).fetchall():
        ciks.setdefault(iid, set()).add(canonical_cik(str(value)))
    history: dict[int, set[str]] = {}
    for iid, symbol in conn.execute(
        "SELECT instrument_id, symbol FROM instrument_symbol_history WHERE instrument_id = ANY(%s)", (members,)
    ).fetchall():
        history.setdefault(iid, set()).add(symbol_key(symbol))
    profiles = {
        r[0]: Profile(r[1], r[2], r[3])
        for r in conn.execute(
            "SELECT instrument_id, entity_type, sic, fetched_at FROM instrument_sec_profile "
            "WHERE instrument_id = ANY(%s)",
            (members,),
        ).fetchall()
    }
    filings: dict[int, list[CoverFiling]] = {}
    for iid, accession, filed_on, url in conn.execute(
        "SELECT instrument_id, provider_filing_id, filing_date, primary_document_url FROM filing_events "
        "WHERE provider = 'sec' AND instrument_id = ANY(%s) AND filing_type = ANY(%s)",
        (members, list(COVER_FORMS)),
    ).fetchall():
        filings.setdefault(iid, []).append(CoverFiling(accession, filed_on, url))

    partial: dict[int, SecurityEvidence] = {}
    for key, ms in securities.items():
        carriers = [m for m in ms if m in ciks]
        partial[key] = SecurityEvidence(
            types=frozenset(rows[m][1] if m in rows else None for m in ms),
            ciks=frozenset(c for m in carriers for c in ciks[m]),
            symbol=symbol_key(rows[key][0]) if key in rows else None,
            symbols_ever=frozenset(
                {symbol_key(rows[m][0]) for m in ms if m in rows} | {s for m in ms for s in history.get(m, ())}
            ),
            profiles=tuple(profiles.get(m) for m in carriers),
            filings=tuple(f for m in ms for f in filings.get(m, ())),
            first_acquisition=first_acquisitions.get(key),
            covers={},
        )
    if not bodies:
        return partial

    selected: set[str] = set()
    for ev in partial.values():
        selection = select_covers(ev.filings, ev.first_acquisition)
        selected |= {f.accession for f in (selection.latest, selection.start) if f is not None}
    read: dict[str, Cover | None] = {}
    sizes = conn.execute(
        "SELECT accession_number, octet_length(body) FROM tax_cover_instances WHERE accession_number = ANY(%s)",
        (sorted(selected),),
    ).fetchall()
    loadable = [acc for acc, size in sizes if size <= INSTANCE_MAX_BYTES]
    read.update({acc: None for acc, size in sizes if size > INSTANCE_MAX_BYTES})
    for acc, body in conn.execute(
        "SELECT accession_number, body FROM tax_cover_instances WHERE accession_number = ANY(%s)", (loadable,)
    ).fetchall():
        read[acc] = read_cover(bytes(body))
    out: dict[int, SecurityEvidence] = {}
    for key, ev in partial.items():
        selection = select_covers(ev.filings, ev.first_acquisition)
        accessions = {f.accession for f in (selection.latest, selection.start) if f is not None}
        out[key] = SecurityEvidence(
            types=ev.types,
            ciks=ev.ciks,
            symbol=ev.symbol,
            symbols_ever=ev.symbols_ever,
            profiles=ev.profiles,
            filings=ev.filings,
            first_acquisition=ev.first_acquisition,
            covers={acc: read[acc] for acc in accessions if acc in read},
        )
    return out


# --- fetching (Design 1) ------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class FetchedInstance:
    #: None: a failed cover (refused over ``INSTANCE_MAX_BYTES``, non-200 or transport error), with ``error``.
    body: bytes | None
    error: str | None = None


def fetch_instance(client: httpx.Client, url: str) -> FetchedInstance:
    """GET ``url`` on the shared SEC rate gate, reading the body as a stream and abandoning it past
    ``INSTANCE_MAX_BYTES``."""
    get_sec_rate_gate().acquire()
    try:
        with client.stream("GET", url, headers={"User-Agent": settings.sec_user_agent}) as response:
            if response.status_code != 200:
                return FetchedInstance(None, f"http {response.status_code}")
            body = bytearray()
            for chunk in response.iter_bytes():
                body += chunk
                if len(body) > INSTANCE_MAX_BYTES:
                    return FetchedInstance(None, f"refused: over {INSTANCE_MAX_BYTES} bytes")
    except httpx.HTTPError as exc:
        return FetchedInstance(None, f"transport {type(exc).__name__}: {exc}")
    return FetchedInstance(bytes(body))
