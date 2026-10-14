"""Fund evidence and candidate identity (#3616 slice 3d-3c-1): the pure readers and decisions.

Spec: docs/specs/metrics/2026-10-14-3616-slice3d3c1-fund-identity.md.

- ``consolidate``, ``newest_period`` and ``ncen_xml_url`` read a fund security's SEC N-CEN filings (Design 1).
- ``read_ncen``, ``read_gleif_record``, ``read_gleif_isins``, ``read_figi`` and ``read_content_item`` are the strict
  readers of each source's body (Designs 1 and 2).
- ``identity_outcome`` is the ``fund_identity_v1`` candidate-identity table (Design 2).
- ``read_reporting_list`` assesses one version of HMRC's reporting-funds list; ``search_version`` applies the match
  tests to it; ``search_outcome`` decides the list search over the stored versions (Design 2).

Nothing here touches the network or the DB, and nothing here decides a fund's tax character: every fund stays
withheld as ``exchange_traded_fund`` (slice 3d-3a). The capture, storage and job steps are the build's second part.
"""

from __future__ import annotations

import io
import json
import re
import zipfile
import zlib
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import IO, Any, Literal

import lxml.etree as ET

from app.services.def14a_recipients import symbol_key
from app.services.xbrl_instance import SAFE_XML_PARSER

#: The frozen local policy (spec "Local policy"); changing any part of it is a new id.
FUND_IDENTITY_POLICY_ID = "fund_identity_v1"

NCEN = "http://www.sec.gov/edgar/ncen"
LIST_HEADERS = (
    "Letter Group",
    "Reporting Fund Ref",
    "Parent Fund",
    "HMRC Share Class Ref No.’s",
    "Sub Fund Name",
    "ISIN No",
    "CUSIP No",
    "Reporting Fund, with effect from",
    "Ceased to be an RF on",
)
GENERIC_NAME_WORDS = frozenset({"THE", "TRUST", "FUND", "FUNDS", "ETF", "INC", "PLC", "LLC", "SERIES"})
GLEIF_REGISTRATION_OK = frozenset({"ISSUED", "PENDING_TRANSFER", "PENDING_ARCHIVAL"})
MAX_ISIN_SET = 10
BOOTSTRAP_MIN_ROWS = 100_000
BASELINE_FLOOR = 0.9
CONTENT_XML_MAX = 256 * 1024 * 1024
MAX_DATA_ROWS = 1_000_000
MAX_ROW_REPEAT = 1_048_576
MAX_CELL_REPEAT = 16_384
MAX_CELL_TEXT = 4_096
MAX_CELL_RAW = 65_536
MAX_VERSION_TEXT = 64 * 1024 * 1024
#: Data rows between two deadline checks while a list is read or searched (spec Design 2).
TICK_ROWS = 1_000
METHODS = ("identifier", "identifier_in_text", "near_identifier", "name")
EXAMPLES_PER_METHOD = 25

_ISIN = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]")
_LEI = re.compile(r"[A-Z0-9]{18}[0-9]{2}")
_CIK = re.compile(r"[0-9]{1,10}")
_ACCESSION = re.compile(r"([0-9]{10})-([0-9]{2})-([0-9]{6})")
_SEC_ARCHIVE = re.compile(
    r"https://www\.sec\.gov/Archives/edgar/data/([0-9]{1,10})/([0-9]{18})/(xslFormN-CEN_X01/)?primary_doc\.xml"
)
_FUND_REF = re.compile(r"[A-Z][0-9]{4}")
_MIC = re.compile(r"[A-Z0-9]{4}")
_DAY = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_RFC3339 = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])"
)
_ASSET_URL = re.compile(r"https://assets\.publishing\.service\.gov\.uk/[^?#@:]+\.ods")
_T = "{urn:oasis:names:tc:opendocument:xmlns:table:1.0}"
_X = "{urn:oasis:names:tc:opendocument:xmlns:text:1.0}"
_OFFICE = "{urn:oasis:names:tc:opendocument:xmlns:office:1.0}"

IdentityOutcome = Literal[
    "candidate",
    "cik_unestablished",
    "registration_metadata_conflict",
    "registration_unavailable",
    "registration_amended",
    "registration_unusable",
    "registration_final",
    "fund_listing_unmatched",
    "lei_record_unavailable",
    "lei_record_unsupported",
    "isin_unavailable",
    "isin_set_unsupported",
    "isin_set_unusable",
    "isin_mapping_unavailable",
    "isin_unresolved",
    "isin_unestablished",
]
SearchOutcome = Literal["listed", "unresolved", "unavailable", "not_found"]
CoverageState = Literal["pending", "unusable", "usable_unsearched", "searched"]


class DeadlineExceeded(Exception):
    """Raised by a caller's ``tick`` when the step deadline has passed; never caught here, so interrupted work is
    stored as nothing (spec Design 1)."""


Tick = Callable[[], None]


def _no_tick() -> None:
    return None


# --- identifiers -------------------------------------------------------------------------------------------------


def isin_check_digit(body11: str) -> str:
    """ISO 6166: letters to numbers (A=10 … Z=35), then Luhn over the digit string, doubling from the right."""
    digits = "".join(str(int(ch, 36)) for ch in body11)
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = int(ch) * (2 if i % 2 == 0 else 1)
        total += d // 10 + d % 10
    return str((10 - total % 10) % 10)


def valid_isin(value: str) -> bool:
    return bool(_ISIN.fullmatch(value)) and isin_check_digit(value[:11]) == value[11]


def valid_lei(value: str) -> bool:
    """ISO 17442: the whole string, letters converted, mod 97 = 1."""
    return bool(_LEI.fullmatch(value)) and int("".join(str(int(ch, 36)) for ch in value)) % 97 == 1


def padded_cik(value: str) -> str | None:
    return value.zfill(10) if _CIK.fullmatch(value) else None


def nsin(isin: str) -> str | None:
    """For a US or CA ISIN the NSIN is the CUSIP; None otherwise."""
    return isin[2:11] if isin[:2] in ("US", "CA") else None


def near_miss(a: str, b: str) -> bool:
    """One substituted character, or one adjacent transposition (spec Design 2)."""
    if len(a) != len(b) or a == b:
        return False
    diff = [i for i in range(len(a)) if a[i] != b[i]]
    if len(diff) == 1:
        return True
    return len(diff) == 2 and diff[1] == diff[0] + 1 and a[diff[0]] == b[diff[1]] and a[diff[1]] == b[diff[0]]


def name_key(name: str) -> str:
    """Unicode upper-casing, then the ASCII ``[A-Z0-9]+`` words (any other character separates words), minus the
    generic words, concatenated (spec Design 2)."""
    return "".join(w for w in re.findall(r"[A-Z0-9]+", name.upper()) if w not in GENERIC_NAME_WORDS)


def _squash(cell: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", cell.upper())


# --- N-CEN filings (Design 1) ------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Filing:
    """One SEC ``filing_events`` row of a fund security's members, ``N-CEN`` or ``N-CEN/A``."""

    accession: str
    form: str
    filing_date: date
    report_date: date | None
    url: str | None


def consolidate(rows: Iterable[Filing], observed_on: date) -> list[Filing] | None:
    """By accession, the representative URL the smallest; None (``registration_metadata_conflict``) for two forms,
    filing dates or report dates, a NULL report date, a report date after its filing date, or a filing date after
    the observation day."""
    by: dict[str, list[Filing]] = {}
    for f in rows:
        by.setdefault(f.accession, []).append(f)
    out: list[Filing] = []
    for acc, fs in sorted(by.items()):
        if len({(f.form, f.filing_date, f.report_date) for f in fs}) != 1:
            return None
        f = fs[0]
        if f.report_date is None or f.report_date > f.filing_date or f.filing_date > observed_on:
            return None
        urls = sorted(x.url for x in fs if x.url is not None)
        out.append(Filing(acc, f.form, f.filing_date, f.report_date, urls[0] if urls else None))
    return out


def newest_period(filings: Sequence[Filing]) -> tuple[date | None, Filing | None, list[str]]:
    """The newest report period across both forms; its newest original ``N-CEN`` by ``(filing_date, accession)``
    (None if absent); and the accessions of the ``N-CEN/A`` filings for that period."""
    dated = [f for f in filings if f.report_date is not None]
    if not dated:
        return None, None, []
    period = max(f.report_date for f in dated if f.report_date is not None)
    originals = sorted(
        (f for f in dated if f.form == "N-CEN" and f.report_date == period), key=lambda f: (f.filing_date, f.accession)
    )
    amendments = sorted(f.accession for f in dated if f.form == "N-CEN/A" and f.report_date == period)
    return period, (originals[-1] if originals else None), amendments


def ncen_xml_url(filing: Filing, cik: str) -> str | None:
    """The raw XML in the accession's own archive directory under the CIK, without the rendering segment; None for
    any other URL (spec Design 1)."""
    if filing.url is None:
        return None
    m = _SEC_ARCHIVE.fullmatch(filing.url)
    acc = _ACCESSION.fullmatch(filing.accession)
    if m is None or acc is None or m.group(1).zfill(10) != cik or m.group(2) != "".join(acc.groups()):
        return None
    return filing.url.replace("xslFormN-CEN_X01/", "")


# --- readers (Designs 1 and 2) -----------------------------------------------------------------------------------


def _ncen_root(body: bytes) -> Any | None:
    try:
        root = ET.fromstring(body, parser=SAFE_XML_PARSER)
    except ET.XMLSyntaxError:
        return None
    if not isinstance(root.tag, str) or root.tag != f"{{{NCEN}}}edgarSubmission":
        return None
    return root


def ncen_sound(body: bytes) -> bool:
    """Capture soundness (Design 1): parses, root ``edgarSubmission`` in the N-CEN namespace, and exactly one
    ``headerData/submissionType`` reading ``N-CEN``."""
    root = _ncen_root(body)
    if root is None:
        return False
    found = root.findall("n:headerData/n:submissionType", {"n": NCEN})
    return len(found) == 1 and not len(found[0]) and found[0].text == "N-CEN"


@dataclass(frozen=True)
class NcenDoc:
    report_period: str
    short_period: str
    lei: str
    #: ``(mic, ticker)`` per listing, the ticker as filed.
    listings: tuple[tuple[str, str], ...]
    last_filing: str
    trustee_countries: tuple[str, ...]
    registrant_country: str | None


def read_ncen(body: bytes, cik: str) -> NcenDoc | None:
    """The strict reader: exact paths, cardinalities and scalars (spec Design 2); None means unusable."""
    root = _ncen_root(body)
    if root is None:
        return None
    try:
        return _read_ncen(root, cik)
    except ValueError:
        return None


def _read_ncen(root: Any, cik: str) -> NcenDoc | None:
    ns = {"n": NCEN}

    def els(path: str) -> list[Any]:
        return root.findall(path, ns)

    def texts(path: str) -> list[str]:
        found = els(path)
        if any(len(e) for e in found):
            raise ValueError("scalar with child elements")
        return [e.text or "" for e in found]

    singletons = (
        "n:headerData",
        "n:formData",
        "n:formData/n:generalInfo",
        "n:formData/n:registrantInfo",
        "n:formData/n:unitInvestmentTrust",
        "n:formData/n:unitInvestmentTrust/n:notRegistrantSeparateInsuranceAccount",
        "n:formData/n:unitInvestmentTrust/n:notRegistrantSeparateInsuranceAccount/n:seriesCIKs",
        "n:formData/n:exchangeSeriesInfo",
        "n:formData/n:exchangeSeriesInfo/n:exchangeTradedFund",
        "n:formData/n:exchangeSeriesInfo/n:exchangeTradedFund/n:securityExchanges",
    )
    if any(len(els(path)) != 1 for path in singletons):
        return None
    general = els("n:formData/n:generalInfo")[0]
    period, short = general.get("reportEndingPeriod") or "", general.get("isReportPeriodLt12") or ""
    if not _DAY.fullmatch(period) or short not in ("Y", "N"):
        return None
    reg = "n:formData/n:registrantInfo/"
    uit = "n:formData/n:unitInvestmentTrust/n:notRegistrantSeparateInsuranceAccount/"
    etf = "n:formData/n:exchangeSeriesInfo/n:exchangeTradedFund/"
    ciks, leis = texts(reg + "n:registrantCik"), texts(reg + "n:registrantLei")
    if texts("n:headerData/n:submissionType") != ["N-CEN"] or len(ciks) != 1 or padded_cik(ciks[0]) != cik:
        return None
    last = texts(reg + "n:isRegistrantLastFiling")
    if len(leis) != 1 or not valid_lei(leis[0]) or texts(reg + "n:registrantClassificationType") != ["S-6"]:
        return None
    if last not in (["Y"], ["N"]) or texts(uit + "n:numOfExistingSeries") != ["1"]:
        return None
    series_ciks = [e.get("cik") or "" for e in els(uit + "n:seriesCIKs/n:seriesCIK")]
    if len(series_ciks) != 1 or padded_cik(series_ciks[0]) != cik:
        return None
    listings = tuple(
        (e.get("fundExchange") or "", e.get("fundsTickerSymbol") or "")
        for e in els(etf + "n:securityExchanges/n:securityExchange")
    )
    if not listings or any(not _MIC.fullmatch(mic) or not ticker.strip() for mic, ticker in listings):
        return None
    rc = texts(reg + "n:registrantcountry")
    return NcenDoc(
        report_period=period,
        short_period=short,
        lei=leis[0],
        listings=listings,
        last_filing=last[0],
        trustee_countries=tuple(
            e.get("trusteeCountry") or "" for e in els(uit + "n:trustees/n:trustee/n:trusteeStateCountry")
        ),
        registrant_country=rc[0] if len(rc) == 1 else None,
    )


@dataclass(frozen=True)
class GleifRecord:
    registration_status: str
    entity_status: str
    category: str
    #: The legal name first, then every ``otherNames`` entry.
    names: tuple[str, ...]
    successor: bool


def read_gleif_record(body: bytes, lei: str) -> GleifRecord | None:
    """None means unsound: every field Design 2 judges must have its stated type; an absent key means none."""
    try:
        doc = json.loads(body)
    except ValueError:
        return None
    data = doc.get("data") if isinstance(doc, dict) else None
    attrs = data.get("attributes") if isinstance(data, dict) and data.get("id") == lei else None
    entity = attrs.get("entity") if isinstance(attrs, dict) else None
    registration = attrs.get("registration") if isinstance(attrs, dict) else None
    if not isinstance(entity, dict) or not isinstance(registration, dict):
        return None
    legal = entity.get("legalName")
    others = entity.get("otherNames", [])
    succ = entity.get("successorEntity", {"lei": None, "name": None})
    succs = entity.get("successorEntities", [])
    fields = (registration.get("status"), entity.get("status"), entity.get("category"))
    if not all(isinstance(v, str) for v in fields):
        return None
    if not isinstance(legal, dict) or not isinstance(legal.get("name"), str):
        return None
    if not isinstance(others, list) or not all(isinstance(o, dict) and isinstance(o.get("name"), str) for o in others):
        return None
    if not isinstance(succ, dict) or not all(
        v is None or isinstance(v, str) for v in (succ.get("lei"), succ.get("name"))
    ):
        return None
    if not isinstance(succs, list):
        return None
    return GleifRecord(
        registration_status=str(fields[0]),
        entity_status=str(fields[1]),
        category=str(fields[2]),
        names=(legal["name"], *(o["name"] for o in others)),
        successor=succ.get("lei") is not None or succ.get("name") is not None or len(succs) > 0,
    )


@dataclass(frozen=True)
class GleifIsins:
    entries: tuple[str, ...]
    total: int
    last_page: int


def read_gleif_isins(body: bytes) -> GleifIsins | None:
    """The first page of the ISIN relationship; None when unsound (JSON integers only, ``currentPage`` 1)."""
    try:
        doc = json.loads(body)
    except ValueError:
        return None
    if not isinstance(doc, dict) or not isinstance(doc.get("data"), list) or not isinstance(doc.get("meta"), dict):
        return None
    pag = doc["meta"].get("pagination")
    if not isinstance(pag, dict) or not all(type(pag.get(k)) is int for k in ("currentPage", "lastPage", "total")):
        return None
    if pag["currentPage"] != 1:
        return None
    entries: list[str] = []
    for d in doc["data"]:
        attrs = d.get("attributes") if isinstance(d, dict) else None
        isin = attrs.get("isin") if isinstance(attrs, dict) else None
        if not isinstance(isin, str):
            return None
        entries.append(isin)
    return GleifIsins(tuple(entries), pag["total"], pag["lastPage"])


def figi_jobs(isins: Sequence[str], mic: str) -> list[dict[str, str]]:
    """The frozen job shape: one ``ID_ISIN`` job per ISIN, in the set's order, filtered to the listing's MIC."""
    return [{"idType": "ID_ISIN", "idValue": i, "micCode": mic} for i in isins]


def figi_request_key(isins: Sequence[str], mic: str) -> str:
    """The OpenFIGI request key: the job list as canonical JSON, in job order (spec Design 1)."""
    return json.dumps(figi_jobs(isins, mic), separators=(",", ":"), sort_keys=True)


Unresolved = Literal["unresolved"]


def read_figi(body: bytes, jobs: int) -> list[tuple[str, ...] | Unresolved] | None:
    """Per job, in job order: its tickers as received, or ``"unresolved"`` for a warning, an empty ``data`` or a row
    without a string ticker holding a non-white-space character. None when unsound: not a list of ``jobs`` objects,
    each exactly ``data`` (a list) or exactly ``warning`` (a string)."""
    try:
        doc = json.loads(body)
    except ValueError:
        return None
    if not isinstance(doc, list) or len(doc) != jobs:
        return None
    out: list[tuple[str, ...] | Unresolved] = []
    for item in doc:
        if not isinstance(item, dict):
            return None
        if set(item) == {"warning"} and isinstance(item["warning"], str):
            out.append("unresolved")
            continue
        if set(item) != {"data"} or not isinstance(item["data"], list):
            return None
        tickers: list[str] = []
        for d in item["data"]:
            t = d.get("ticker") if isinstance(d, dict) else None
            if not isinstance(t, str) or not t.strip():
                tickers = []
                break
            tickers.append(t)
        out.append(tuple(tickers) if tickers else "unresolved")
    return out


@dataclass(frozen=True)
class ContentItem:
    public_updated_at: datetime
    attachment_url: str
    file_size: int


def read_content_item(body: bytes, fetched_at: datetime) -> ContentItem | None:
    """GOV.UK's content item, sound per Design 1's table, or None: a JSON object; ``public_updated_at`` RFC 3339 with
    a zone and not after the fetch; exactly one ``.ods`` attachment object, on the asset host over HTTPS with no
    credentials, port, query or fragment, and a positive JSON integer ``file_size``."""
    try:
        item = json.loads(body)
    except ValueError:
        return None
    if not isinstance(item, dict):
        return None
    published = item.get("public_updated_at")
    if not isinstance(published, str) or _RFC3339.fullmatch(published) is None:
        return None
    try:
        when = datetime.fromisoformat(published.replace("Z", "+00:00"))
    except ValueError:
        return None
    if when > fetched_at:
        return None
    details = item.get("details")
    attachments = details.get("attachments") if isinstance(details, dict) else None
    if not isinstance(attachments, list):
        return None
    ods = [
        a for a in attachments if isinstance(a, dict) and isinstance(a.get("url"), str) and a["url"].endswith(".ods")
    ]
    if len(ods) != 1 or _ASSET_URL.fullmatch(ods[0]["url"]) is None:
        return None
    size = ods[0].get("file_size")
    if type(size) is not int or size <= 0:
        return None
    return ContentItem(when, ods[0]["url"], size)


# --- the identity decision (Design 2) ----------------------------------------------------------------------------


def identity_outcome(
    *,
    ciks: Iterable[str],
    symbol: str | None,
    filings: list[Filing] | None,
    ncen: Mapping[str, bytes],
    records: Mapping[str, bytes],
    isin_sets: Mapping[str, bytes],
    mappings: Mapping[str, bytes],
) -> tuple[IdentityOutcome, dict[str, Any]]:
    """The ``fund_identity_v1`` table, first applicable line wins; with the evaluated inputs it reached, keyed by
    their observation column (spec Design 3).

    ``symbol`` is already under ``symbol_key``. ``filings`` is ``consolidate``'s result. The mappings hold the
    stored bodies the caller may use, by request key: ``ncen`` every stored N-CEN by accession; ``records``,
    ``isin_sets`` and ``mappings`` the *current* newest fetch per LEI and per FIGI request key."""
    got: dict[str, Any] = {}
    padded = {padded_cik(c) for c in ciks}
    if len(padded) != 1 or None in padded:
        return "cik_unestablished", got
    cik = str(next(iter(padded)))
    got["cik"] = cik
    if filings is None:
        return "registration_metadata_conflict", got
    period, newest, amendments = newest_period(filings)
    got.update(newest_period=period, amendment_accessions=amendments)
    if newest is None:
        got["refusal"] = "no N-CEN for the newest report period"
        return "registration_unavailable", got
    url = ncen_xml_url(newest, cik)
    got.update(ncen_accession=newest.accession, ncen_report_date=newest.report_date, ncen_source_url=newest.url)
    if url is None or newest.accession not in ncen:
        got["refusal"] = "unsupported URL" if url is None else "no stored document"
        return "registration_unavailable", got
    if amendments:
        return "registration_amended", got
    doc = read_ncen(ncen[newest.accession], cik)
    if doc is None or doc.report_period != str(newest.report_date):
        return "registration_unusable", got
    got.update(
        lei=doc.lei,
        listings=[list(pair) for pair in doc.listings],
        short_period=doc.short_period,
        trustee_countries=list(doc.trustee_countries),
        registrant_country=doc.registrant_country,
    )
    if doc.last_filing == "Y":
        return "registration_final", got
    keyed = [symbol_key(ticker) for _mic, ticker in doc.listings]
    if symbol is None or keyed.count(symbol) != 1:
        return "fund_listing_unmatched", got
    mic = doc.listings[keyed.index(symbol)][0]
    got["mic"] = mic
    rec = read_gleif_record(records[doc.lei], doc.lei) if doc.lei in records else None
    if rec is None:
        return "lei_record_unavailable", got
    if (
        rec.registration_status not in GLEIF_REGISTRATION_OK
        or rec.entity_status != "ACTIVE"
        or rec.category != "FUND"
        or rec.successor
        or not name_key(rec.names[0])
    ):
        return "lei_record_unsupported", got
    parsed = read_gleif_isins(isin_sets[doc.lei]) if doc.lei in isin_sets else None
    if parsed is None:
        return "isin_unavailable", got
    entries = list(parsed.entries)
    got["isin_set"] = entries
    if (
        parsed.last_page != 1
        or parsed.total != len(entries)
        or not entries
        or len(entries) > MAX_ISIN_SET
        or len(set(entries)) != len(entries)
    ):
        return "isin_set_unsupported", got
    if not all(valid_isin(i) for i in entries):
        return "isin_set_unusable", got
    key = figi_request_key(entries, mic)
    maps = read_figi(mappings[key], len(entries)) if key in mappings else None
    if maps is None:
        return "isin_mapping_unavailable", got
    if any(m == "unresolved" for m in maps):
        return "isin_unresolved", got
    hits = [i for i, m in zip(entries, maps, strict=True) if any(symbol_key(t) == symbol for t in m)]
    if len(hits) != 1:
        return "isin_unestablished", got
    got["isin"] = hits[0]
    got["name_keys"] = sorted({name_key(n) for n in rec.names} - {""})
    return "candidate", got


# --- the reporting list (Design 2) -------------------------------------------------------------------------------


@dataclass(frozen=True)
class ListAssessment:
    """One version's assessment: ``rows`` (nine cells each) when usable, else ``refusal``."""

    rows: tuple[tuple[str, ...], ...] | None
    refusal: str | None
    #: The archive's ``(member, declared size)`` pairs, and the version's logical text in characters with repeats.
    members: tuple[tuple[str, int], ...] = ()
    logical_chars: int = 0


class _Capped(io.RawIOBase):
    """Counts decompressed bytes, refusing past the limit, and checks the deadline per read."""

    def __init__(self, inner: IO[bytes], limit: int, tick: Tick) -> None:
        self.inner, self.limit, self.seen, self.tick = inner, limit, 0, tick

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        self.tick()
        chunk = self.inner.read(len(buffer))
        self.seen += len(chunk)
        if self.seen > self.limit:
            raise ValueError("content.xml past its decompressed limit")
        buffer[: len(chunk)] = chunk
        return len(chunk)


class _Unusable(Exception):
    pass


def _paragraph_text(p: Any, budget: dict[str, int]) -> str:
    """ODF 1.3 part 3 §6.1.2 on one ``text:p``: ``text:span`` / ``text:a`` replaced by their content; tab, CR and LF
    in character data become spaces; spaces at the very start and end of the paragraph are removed; runs of spaces
    collapse, an explicit element ending a run; then ``text:s`` (``text:c`` spaces), ``text:tab`` and
    ``text:line-break`` are read as white space. Explicit elements are typed tokens, never characters, so any
    character data is preserved. Any other element, or a non-empty explicit element, makes the list unusable.
    ``budget`` holds the cell's remaining raw character data and logical text, charged before anything expands."""
    tokens: list[str | int] = []  # str = character data; int = explicit white space (n spaces, -1 tab, -2 break)

    def data(text: str | None) -> None:
        if text:
            budget["raw"] -= len(text)
            if budget["raw"] < 0:
                raise _Unusable("cell character data past its limit")
            tokens.append(text)

    def walk(el: Any) -> None:
        data(el.text)
        for ch in el:
            if ch.tag in (_X + "s", _X + "tab", _X + "line-break"):
                if len(ch) or (ch.text or ""):
                    raise _Unusable("inline element with content")
                if ch.tag == _X + "s":
                    c = ch.get(_X + "c", "1")
                    if not c.isdigit() or not 1 <= int(c) <= MAX_CELL_TEXT:
                        raise _Unusable("bad text:c")
                    tokens.append(int(c))
                else:
                    tokens.append(-1 if ch.tag == _X + "tab" else -2)
            elif ch.tag in (_X + "span", _X + "a"):
                walk(ch)
            else:
                raise _Unusable(f"unsupported text element {ch.tag}")
            data(ch.tail)

    walk(p)
    runs: list[str | int] = []
    for t in tokens:  # merge adjacent character data: inline elements are gone (step 1)
        if isinstance(t, str) and runs and isinstance(runs[-1], str):
            runs[-1] += t
        else:
            runs.append(t)
    runs = [re.sub(" +", " ", re.sub("[\t\r\n]", " ", t)) if isinstance(t, str) else t for t in runs]
    if runs and isinstance(runs[0], str):
        runs[0] = runs[0].lstrip(" ")
    if runs and isinstance(runs[-1], str):
        runs[-1] = runs[-1].rstrip(" ")
    budget["logical"] -= sum(len(t) if isinstance(t, str) else (t if t > 0 else 1) for t in runs)
    if budget["logical"] < 0:
        raise _Unusable("cell text past its limit")
    return "".join(t if isinstance(t, str) else (" " * t if t > 0 else "\t" if t == -1 else "\n") for t in runs)


def _repeat(el: Any, attr: str, cap: int) -> int:
    value = el.get(_T + attr, "1")
    if not value.isdigit() or not 1 <= int(value) <= cap:
        raise _Unusable(f"bad {attr}")
    return int(value)


def read_reporting_list(body: bytes, baseline: int | None, tick: Tick = _no_tick) -> ListAssessment:
    """Assess one list version (spec Design 2). ``baseline`` is the largest earlier usable version's data-row count,
    None when there is none. Any archive, decompression or structure failure is an unusable assessment; only
    ``tick``'s ``DeadlineExceeded`` escapes."""
    members: tuple[tuple[str, int], ...] = ()
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as z:
            members = tuple((i.filename, i.file_size) for i in z.infolist())
            if [name for name, _size in members].count("content.xml") != 1:
                return ListAssessment(None, "content_xml_count", members)
            info = z.getinfo("content.xml")
            if info.file_size > CONTENT_XML_MAX:
                return ListAssessment(None, "content_xml_declared_size", members)
            if info.flag_bits & 0x1 or info.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
                return ListAssessment(None, "content_xml_unsupported_compression", members)
            with z.open("content.xml") as raw:
                rows, logical = _read_rows(io.BufferedReader(_Capped(raw, CONTENT_XML_MAX, tick)), tick)
    except _Unusable as exc:
        return ListAssessment(None, str(exc), members)
    except (
        zipfile.BadZipFile,
        ValueError,
        ET.XMLSyntaxError,
        NotImplementedError,
        RuntimeError,
        EOFError,
        OSError,
        zlib.error,
    ) as exc:
        return ListAssessment(None, f"unusable: {type(exc).__name__}: {exc}", members)
    if not rows or rows[0] != LIST_HEADERS:
        return ListAssessment(None, "header", members, logical)
    data = rows[1:]
    if any(not _FUND_REF.fullmatch(r[1]) or not (r[2].strip() or r[4].strip()) for r in data):
        return ListAssessment(None, "row_structure", members, logical)
    floor = BOOTSTRAP_MIN_ROWS if baseline is None else BASELINE_FLOOR * baseline
    if len(data) < floor:
        return ListAssessment(None, f"rows {len(data)} below {floor}", members, logical)
    return ListAssessment(tuple(data), None, members, logical)


def _read_rows(stream: IO[bytes], tick: Tick) -> tuple[list[tuple[str, ...]], int]:
    tables = 0
    total_logical = 0
    seen_rows = 0
    rows: list[tuple[str, ...]] = []
    for _event, el in ET.iterparse(stream, events=("end",), resolve_entities=False, load_dtd=False, no_network=True):
        parent = el.getparent()
        if el.tag == _T + "table":
            tables += 1
            if parent is None or parent.tag != _OFFICE + "spreadsheet":
                raise _Unusable("table_outside_spreadsheet")
            if any(ch.tag not in (_T + "table-row", _T + "table-column") for ch in el):
                raise _Unusable("table_child")
        elif el.tag == _T + "covered-table-cell":
            raise _Unusable("covered_cell")
        elif el.tag == _T + "table-row":
            seen_rows += 1
            if seen_rows % TICK_ROWS == 0:
                tick()
            if parent is None or parent.tag != _T + "table":
                raise _Unusable("row_outside_table")
            repeat = _repeat(el, "number-rows-repeated", MAX_ROW_REPEAT)
            cells: list[str] = []
            col = 0
            nonempty = False
            for cell in el:
                if cell.tag != _T + "table-cell":
                    raise _Unusable(f"row child {cell.tag}")
                n = _repeat(cell, "number-columns-repeated", MAX_CELL_REPEAT)
                if any(cell.get(_T + a, "1") != "1" for a in ("number-rows-spanned", "number-columns-spanned")):
                    raise _Unusable("spanned_cell")
                if cell.get(_T + "formula") is not None or cell.get(_OFFICE + "value-type") not in (None, "string"):
                    raise _Unusable("unsupported_cell_value")
                if any(k.startswith(_OFFICE) and k != _OFFICE + "value-type" for k in cell.attrib):
                    raise _Unusable("unsupported_cell_value")
                budget = {"raw": MAX_CELL_RAW, "logical": MAX_CELL_TEXT}
                parts: list[str] = []
                for p in cell:
                    if p.tag != _X + "p":
                        raise _Unusable(f"cell child {p.tag}")
                    if parts:
                        budget["logical"] -= 1
                    parts.append(_paragraph_text(p, budget))
                text = "\n".join(parts)
                total_logical += len(text) * min(n, 9)
                if total_logical > MAX_VERSION_TEXT:
                    raise _Unusable("version text past its limit")
                if text:
                    nonempty = True
                    if col + n > 9:
                        raise _Unusable("content_past_column_nine")
                cells.extend([text] * max(0, min(n, 9 - col)))
                col += n
            el.clear()
            if nonempty:
                if repeat > 1:
                    raise _Unusable("repeated_row_with_content")
                rows.append(tuple(cells + [""] * (9 - len(cells))))
                if len(rows) > MAX_DATA_ROWS + 1:
                    raise _Unusable("too_many_rows")
    if tables != 1:
        raise _Unusable(f"tables={tables}")
    return rows, total_logical


# --- the list search (Design 2) ----------------------------------------------------------------------------------


@dataclass(frozen=True)
class VersionSearch:
    """One version searched for one key: complete per-method counts, and per method the first
    ``EXAMPLES_PER_METHOD`` matching rows in row order, as ``{"method", "key", "cells"}``."""

    counts: Mapping[str, int]
    examples: tuple[Mapping[str, Any], ...]


def match_row(row: Sequence[str], isin: str, keys: Sequence[str]) -> tuple[str, str] | None:
    """The first match test that applies to one row, with the target or name key it matched (spec Design 2)."""
    target_nsin = nsin(isin)
    targets = [isin] + ([target_nsin] if target_nsin else [])
    squashed = (_squash(row[5]), _squash(row[6]))
    for t in targets:
        if t in squashed:
            return "identifier", t
    for t in targets:
        if any(t in sq for sq in squashed):
            return "identifier_in_text", t
    if near_miss(row[5].strip().upper(), isin):
        return "near_identifier", isin
    if target_nsin is not None and near_miss(row[6].strip().upper(), target_nsin):
        return "near_identifier", target_nsin
    if valid_isin(row[5].strip()) and row[5].strip()[:2] != isin[:2]:
        return None
    row_key = name_key(" ".join(row[2:5]))
    key = next((k for k in keys if k in row_key), None)
    return ("name", key) if key else None


def search_version(
    isin: str, keys: Sequence[str], rows: Iterable[Sequence[str]], tick: Tick = _no_tick
) -> VersionSearch:
    """Test every row of one usable version; ``tick`` is checked every ``TICK_ROWS`` rows."""
    counts: dict[str, int] = dict.fromkeys(METHODS, 0)
    examples: list[Mapping[str, Any]] = []
    for n, row in enumerate(rows, start=1):
        if n % TICK_ROWS == 0:
            tick()
        hit = match_row(row, isin, keys)
        if hit is None:
            continue
        method, key = hit
        counts[method] += 1
        if counts[method] <= EXAMPLES_PER_METHOD:
            examples.append({"method": method, "key": key, "cells": list(row)})
    return VersionSearch(counts, tuple(examples))


def search_outcome(
    searches: Iterable[Mapping[str, int]],
    coverage: Iterable[CoverageState],
    *,
    content_current: bool,
    child_list_usable: bool,
) -> SearchOutcome:
    """The precedence over the stored versions (spec Design 2): ``listed`` on any completed ``identifier`` count;
    ``unresolved`` on any other completed count; ``unavailable`` when the newest content fetch is not current, its
    child list is missing or unusable, or any version is pending or not yet searched for this key; else
    ``not_found``. ``searches`` are this key's completed per-method counts; ``coverage`` one state per stored list
    fetch."""
    completed = list(searches)
    if any(c["identifier"] > 0 for c in completed):
        return "listed"
    if any(sum(c.values()) > 0 for c in completed):
        return "unresolved"
    if not content_current or not child_list_usable:
        return "unavailable"
    if any(state in ("pending", "usable_unsearched") for state in coverage):
        return "unavailable"
    return "not_found"
