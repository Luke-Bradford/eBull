"""#3616 slice 3d-3c-1 fact-finding: the public evidence about each held fund, and the observations the spec makes.

Read-only on the dev DB. Reads the effective evidence generation the way ``refresh_cover_instances`` does
(``tax_matching._read_inputs`` without bodies, then ``security_members``) in one REPEATABLE READ snapshot, and takes
every fund security (all members ``instrument_type_id`` 6). For each, from the snapshot: its members, symbol, primary
CIK and SEC ``N-CEN`` / ``N-CEN/A`` rows of ``filing_events``. Then it fetches, without storing anything in the DB:

- the raw XML of the newest N-CEN (spec Design 1), whose URL must be the accession's own SEC archive directory;
- GLEIF's LEI record and the first page (200) of its ISIN relationship;
- OpenFIGI's mapping of the ISIN set, one ``ID_ISIN`` job per ISIN on the N-CEN listing's ``micCode`` (keyless);
- once, HMRC's "Approved offshore reporting funds" content item and its ``.ods`` attachment.

It applies the spec's pure readers (``read_ncen``, ``read_gleif_record``, ``read_gleif_isins``, ``read_figi``,
``read_reporting_list``) and its two decisions (``identity_outcome``, ``list_observation``), printing every input and
outcome, and the population counts of the list the spec cites. It measures one fresh capture: the capture
preconditions it checks are printed, and stored-history behaviour (search reuse, outcomes over several versions) is
the build's. Bodies go gzipped to a new directory under
``var/research/3616/slice3d3c1/`` with their SHA-256. The printout carries dates, symbols, CIKs, LEIs, ISINs and
accession numbers, never an account identifier.

``--universe`` counts, DB-only, every ETF instrument (type 6, tradable or not, variants included): with a primary SEC
CIK, with at least one N-CEN in ``filing_events``, by country. Coverage, not correctness.

    uv run python -m scripts.measure_3616_slice3d3c1_fund_identity
    uv run python -m scripts.measure_3616_slice3d3c1_fund_identity --universe
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import re
import sys
import time
import zipfile
import zlib
from collections import Counter
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import IO, Any

import httpx
import lxml.etree as ET
import psycopg

from app.config import settings
from app.services.def14a_recipients import symbol_key
from app.services.tax_matching import (  # pyright: ignore[reportPrivateUsage]
    _read_inputs,
    effective_tax_matching,
    security_members,
)
from app.services.xbrl_instance import SAFE_XML_PARSER

ETF_TYPE_ID = 6
NCEN = "http://www.sec.gov/edgar/ncen"
HMRC_CONTENT_URL = "https://www.gov.uk/api/content/government/publications/approved-offshore-reporting-funds"
GLEIF = "https://api.gleif.org/api/v1/lei-records"
OPENFIGI = "https://api.openfigi.com/v3/mapping"
OPENFIGI_SPACING_S = 2.4
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
BOOTSTRAP_MIN_ROWS = 100_000
CONTENT_XML_MAX = 256 * 1024 * 1024
MAX_DATA_ROWS = 1_000_000
MAX_ROW_REPEAT = 1_048_576
MAX_CELL_REPEAT = 16_384
MAX_CELL_TEXT = 4_096
MAX_VERSION_TEXT = 64 * 1024 * 1024
_ISIN = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]")
_LEI = re.compile(r"[A-Z0-9]{18}[0-9]{2}")
_CIK = re.compile(r"[0-9]{1,10}")
_ACCESSION = re.compile(r"([0-9]{10})-([0-9]{2})-([0-9]{6})")
_SEC_ARCHIVE = re.compile(
    r"https://www\.sec\.gov/Archives/edgar/data/([0-9]{1,10})/([0-9]{18})/(xslFormN-CEN_X01/)?primary_doc\.xml"
)
_FUND_REF = re.compile(r"[A-Z][0-9]{4}")
_MIC = re.compile(r"[A-Z0-9]{4}")
T = "{urn:oasis:names:tc:opendocument:xmlns:table:1.0}"
X = "{urn:oasis:names:tc:opendocument:xmlns:text:1.0}"
OFFICE = "{urn:oasis:names:tc:opendocument:xmlns:office:1.0}"


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
    return bool(_LEI.fullmatch(value)) and int("".join(str(int(ch, 36)) for ch in value)) % 97 == 1


def padded_cik(value: str) -> str | None:
    return value.zfill(10) if _CIK.fullmatch(value) else None


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


def squash(cell: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", cell.upper())


# --- N-CEN filings and reader ------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Filing:
    accession: str
    form: str
    filing_date: date
    report_date: date | None
    url: str | None


def consolidate(rows: Iterable[Filing], observed_on: date) -> list[Filing] | None:
    """By accession; None (``registration_metadata_conflict``) for two forms, filing dates or report dates, a NULL
    report date, a report date after its filing date, or a filing date after the observation day."""
    by: dict[str, list[Filing]] = {}
    for f in rows:
        by.setdefault(f.accession, []).append(f)
    out: list[Filing] = []
    for acc, fs in by.items():
        if len({(f.form, f.filing_date, f.report_date) for f in fs}) != 1:
            return None
        f = fs[0]
        if f.report_date is None or f.report_date > f.filing_date or f.filing_date > observed_on:
            return None
        urls = sorted(x.url for x in fs if x.url is not None)
        out.append(Filing(acc, f.form, f.filing_date, f.report_date, urls[0] if urls else None))
    return out


def newest_period(filings: Sequence[Filing]) -> tuple[Filing | None, bool]:
    """The newest report period across both forms: its newest original N-CEN (None if absent), and whether an
    ``N-CEN/A`` has that period."""
    dated = [f for f in filings if f.report_date is not None]
    if not dated:
        return None, False
    period = max(f.report_date for f in dated if f.report_date is not None)
    originals = sorted(
        (f for f in dated if f.form == "N-CEN" and f.report_date == period), key=lambda f: (f.filing_date, f.accession)
    )
    amended = any(f.form == "N-CEN/A" and f.report_date == period for f in dated)
    return (originals[-1] if originals else None), amended


def ncen_xml_url(filing: Filing, cik: str) -> str | None:
    """The raw XML in the accession's own archive directory under the CIK; None for any other URL (spec Design 1)."""
    if filing.url is None:
        return None
    m = _SEC_ARCHIVE.fullmatch(filing.url)
    acc = _ACCESSION.fullmatch(filing.accession)
    if m is None or acc is None or m.group(1).zfill(10) != cik or m.group(2) != "".join(acc.groups()):
        return None
    return filing.url.replace("xslFormN-CEN_X01/", "")


@dataclass(frozen=True)
class NcenDoc:
    report_period: str
    short_period: str
    lei: str
    tickers: tuple[str, ...]
    exchanges: tuple[str, ...]
    last_filing: str
    trustee_countries: tuple[str, ...]
    registrant_country: str | None


def read_ncen(body: bytes, cik: str) -> NcenDoc | None:
    """The strict reader: exact paths and cardinalities (spec Design 2); None means unusable."""
    try:
        root = ET.fromstring(body, parser=SAFE_XML_PARSER)
    except ET.XMLSyntaxError:
        return None
    if not isinstance(root.tag, str) or root.tag != f"{{{NCEN}}}edgarSubmission":
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
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", period) or short not in ("Y", "N"):
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
    pairs = [
        (e.get("fundExchange") or "", e.get("fundsTickerSymbol") or "")
        for e in els(etf + "n:securityExchanges/n:securityExchange")
    ]
    if not pairs or any(not _MIC.fullmatch(a) or not b.strip() for a, b in pairs):
        return None
    rc = texts(reg + "n:registrantcountry")
    return NcenDoc(
        report_period=period,
        short_period=short,
        lei=leis[0],
        tickers=tuple(b for _a, b in pairs),
        exchanges=tuple(a for a, _b in pairs),
        last_filing=last[0],
        trustee_countries=tuple(
            e.get("trusteeCountry") or "" for e in els(uit + "n:trustees/n:trustee/n:trusteeStateCountry")
        ),
        registrant_country=rc[0] if len(rc) == 1 else None,
    )


# --- GLEIF and OpenFIGI readers ----------------------------------------------------------------------------------


@dataclass(frozen=True)
class GleifRecord:
    registration_status: str
    entity_status: str
    category: str
    names: tuple[str, ...]
    successor: bool


def read_gleif_record(body: bytes, lei: str) -> GleifRecord | None:
    """None means unsound or unreadable: every field Design 2 judges must have its stated type."""
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


def read_gleif_isins(body: bytes) -> tuple[list[str], int, int] | None:
    """(entries, total, lastPage); None when unsound."""
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
    return entries, pag["total"], pag["lastPage"]


def figi_jobs(isins: Sequence[str], mic: str) -> list[dict[str, str]]:
    """The frozen job shape: one ``ID_ISIN`` job per ISIN, filtered to the N-CEN listing's MIC."""
    return [{"idType": "ID_ISIN", "idValue": i, "micCode": mic} for i in isins]


def read_figi(body: bytes, jobs: int) -> list[list[str] | str] | None:
    """Per job, in order: the tickers of its rows, or ``"unresolved"`` for a warning, an empty ``data`` or a row
    without a non-empty string ticker. None when unsound (not a list of ``jobs`` objects, each exactly ``data`` or
    exactly ``warning``)."""
    try:
        doc = json.loads(body)
    except ValueError:
        return None
    if not isinstance(doc, list) or len(doc) != jobs:
        return None
    out: list[list[str] | str] = []
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
        out.append(tickers or "unresolved")
    return out


# --- the identity decision ---------------------------------------------------------------------------------------


def identity_outcome(
    *,
    ciks: frozenset[str],
    symbol: str | None,
    filings: list[Filing] | None,
    ncen: dict[str, bytes],
    record: bytes | None,
    isins: bytes | None,
    figi: bytes | None,
) -> tuple[str, dict[str, Any]]:
    """Spec Design 2's identity table: (outcome, the evaluated fields). Every input here was fetched in this run, so
    is current; the service also applies the 30-day currency."""
    got: dict[str, Any] = {}
    padded = {padded_cik(c) for c in ciks}
    if len(padded) != 1 or None in padded:
        return "cik_unestablished", got
    cik = str(next(iter(padded)))
    got["cik"] = cik
    if filings is None:
        return "registration_metadata_conflict", got
    newest, amended = newest_period(filings)
    dated = [f.report_date for f in filings if f.report_date is not None]
    period = max(dated) if dated else None
    got.update(
        newest_period=str(period) if period is not None else None,
        amendments=sorted(f.accession for f in filings if f.form == "N-CEN/A" and f.report_date == period),
    )
    if newest is None:
        got["refusal"] = "no N-CEN for the newest report period"
        return "registration_unavailable", got
    url = ncen_xml_url(newest, cik)
    got.update(accession=newest.accession, report_date=str(newest.report_date), source_url=newest.url, ncen_url=url)
    if url is None or newest.accession not in ncen:
        got["refusal"] = "unsupported URL" if url is None else "no stored document"
        return "registration_unavailable", got
    if amended:
        return "registration_amended", got
    doc = read_ncen(ncen[newest.accession], cik)
    if doc is None or doc.report_period != str(newest.report_date):
        return "registration_unusable", got
    got.update(
        lei=doc.lei,
        tickers=doc.tickers,
        exchanges=doc.exchanges,
        short_period=doc.short_period,
        trustee_countries=doc.trustee_countries,
        registrant_country=doc.registrant_country,
    )
    if doc.last_filing == "Y":
        return "registration_final", got
    keyed = [symbol_key(t) for t in doc.tickers]
    if symbol is None or keyed.count(symbol) != 1:
        return "fund_listing_unmatched", got
    mic = doc.exchanges[keyed.index(symbol)]
    got["mic"] = mic
    rec = read_gleif_record(record, doc.lei) if record is not None else None
    if rec is None:
        return "lei_record_unavailable", got
    keys = sorted({name_key(n) for n in rec.names} - {""})
    if (
        rec.registration_status not in GLEIF_REGISTRATION_OK
        or rec.entity_status != "ACTIVE"
        or rec.category != "FUND"
        or rec.successor
        or not name_key(rec.names[0])
    ):
        return "lei_record_unsupported", got
    parsed = read_gleif_isins(isins) if isins is not None else None
    if parsed is None:
        return "isin_unavailable", got
    entries, total, last_page = parsed
    got["isins"] = entries
    if last_page != 1 or total != len(entries) or not entries or len(set(entries)) != len(entries) or len(entries) > 10:
        return "isin_set_unsupported", got
    if not all(valid_isin(i) for i in entries):
        return "isin_set_unusable", got
    maps = read_figi(figi, len(entries)) if figi is not None else None
    if maps is None:
        return "isin_mapping_unavailable", got
    got["figi"] = dict(zip(entries, maps, strict=True))
    if any(isinstance(m, str) for m in maps):
        return "isin_unresolved", got
    hits = [i for i, m in zip(entries, maps, strict=True) if any(symbol_key(t) == symbol for t in m)]
    if len(hits) != 1:
        return "isin_unestablished", got
    got["isin"] = hits[0]
    got["name_keys"] = keys  # a search key exists only for a candidate (spec Design 3 state rules)
    return "candidate", got


# --- the reporting list ------------------------------------------------------------------------------------------


class _Capped(io.RawIOBase):
    """Counts decompressed bytes and refuses past the limit (spec Design 2)."""

    def __init__(self, inner: IO[bytes], limit: int) -> None:
        self.inner, self.limit, self.seen = inner, limit, 0

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        chunk = self.inner.read(len(buffer))
        self.seen += len(chunk)
        if self.seen > self.limit:
            raise ValueError("content.xml past its decompressed limit")
        buffer[: len(chunk)] = chunk
        return len(chunk)


def _paragraph_text(p: Any, budget: dict[str, int]) -> str:
    """ODF 1.3 part 3 §6.1.2 on one ``text:p``: ``text:span`` / ``text:a`` replaced by their content; tab, CR and LF
    in character data become spaces; spaces at the very start and end of the paragraph are removed; runs of spaces
    collapse, an explicit element ending a run; then ``text:s`` (``text:c`` spaces), ``text:tab`` and
    ``text:line-break`` are read as white space. Explicit elements are typed tokens, never characters, so any
    character data is preserved. Any other element raises, and so does a non-empty explicit element. ``budget``
    holds the cell's remaining raw character data and logical text, charged before anything is expanded."""
    tokens: list[str | int] = []  # str = character data; int = explicit white space (n spaces, -1 tab, -2 break)

    def data(text: str | None) -> None:
        if text:
            budget["raw"] -= len(text)
            if budget["raw"] < 0:
                raise ValueError("cell character data past its limit")
            tokens.append(text)

    def walk(el: Any) -> None:
        data(el.text)
        for ch in el:
            if ch.tag in (X + "s", X + "tab", X + "line-break"):
                if len(ch) or (ch.text or ""):
                    raise ValueError("inline element with content")
                if ch.tag == X + "s":
                    c = ch.get(X + "c", "1")
                    if not c.isdigit() or not 1 <= int(c) <= MAX_CELL_TEXT:
                        raise ValueError("bad text:c")
                    tokens.append(int(c))
                else:
                    tokens.append(-1 if ch.tag == X + "tab" else -2)
            elif ch.tag in (X + "span", X + "a"):
                walk(ch)
            else:
                raise ValueError(f"unsupported text element {ch.tag}")
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
    logical = sum(len(t) if isinstance(t, str) else (t if t > 0 else 1) for t in runs)
    budget["logical"] -= logical
    if budget["logical"] < 0:
        raise ValueError("cell text past its limit")
    return "".join(t if isinstance(t, str) else (" " * t if t > 0 else "\t" if t == -1 else "\n") for t in runs)


def _repeat(el: Any, attr: str, cap: int) -> int:
    value = el.get(T + attr, "1")
    if not value.isdigit() or not 1 <= int(value) <= cap:
        raise ValueError(f"bad {attr}")
    return int(value)


def read_reporting_list(body: bytes, baseline: int | None) -> list[tuple[str, ...]] | str:
    """The data rows (nine cells each), or why the list is unusable (spec Design 2). ``baseline`` is the largest
    earlier usable version's data-row count, None when there is none."""
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as z:
            if [i.filename for i in z.infolist()].count("content.xml") != 1:
                return "content_xml_count"
            print(f"  [reader] zip members={[(i.filename, i.file_size) for i in z.infolist()]}")
            info = z.getinfo("content.xml")
            if info.file_size > CONTENT_XML_MAX:
                return "content_xml_declared_size"
            if info.flag_bits & 0x1 or info.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
                return "content_xml_unsupported_compression"
            with z.open("content.xml") as raw:
                return _read_rows(io.BufferedReader(_Capped(raw, CONTENT_XML_MAX)), baseline)
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
        return f"unusable: {type(exc).__name__}: {exc}"


def _read_rows(stream: IO[bytes], baseline: int | None) -> list[tuple[str, ...]] | str:
    tables = 0
    total_logical = 0
    rows: list[tuple[str, ...]] = []
    for _event, el in ET.iterparse(stream, events=("end",), resolve_entities=False, load_dtd=False, no_network=True):
        parent = el.getparent()
        if el.tag == T + "table":
            tables += 1
            if parent is None or parent.tag != OFFICE + "spreadsheet":
                return "table_outside_spreadsheet"
            if any(ch.tag not in (T + "table-row", T + "table-column") for ch in el):
                return "table_child"
        elif el.tag == T + "covered-table-cell":
            return "covered_cell"
        elif el.tag == T + "table-row":
            if parent is None or parent.tag != T + "table":
                return "row_outside_table"
            repeat = _repeat(el, "number-rows-repeated", MAX_ROW_REPEAT)
            cells: list[str] = []
            col = 0
            nonempty = False
            for cell in el:
                if cell.tag != T + "table-cell":
                    return f"row child {cell.tag}"
                n = _repeat(cell, "number-columns-repeated", MAX_CELL_REPEAT)
                if any(cell.get(T + a, "1") != "1" for a in ("number-rows-spanned", "number-columns-spanned")):
                    return "spanned_cell"
                if cell.get(T + "formula") is not None or cell.get(OFFICE + "value-type") not in (None, "string"):
                    return "unsupported_cell_value"
                if any(k.startswith(OFFICE) and k != OFFICE + "value-type" for k in cell.attrib):
                    return "unsupported_cell_value"
                budget = {"raw": 16 * MAX_CELL_TEXT, "logical": MAX_CELL_TEXT}
                parts: list[str] = []
                for p in cell:
                    if p.tag != X + "p":
                        return f"cell child {p.tag}"
                    if parts:
                        budget["logical"] -= 1
                    parts.append(_paragraph_text(p, budget))
                text = "\n".join(parts)
                total_logical += len(text) * min(n, 9)
                if total_logical > MAX_VERSION_TEXT:
                    return "version text past its limit"
                if text:
                    nonempty = True
                    if col + n > 9:
                        return "content_past_column_nine"
                cells.extend([text] * max(0, min(n, 9 - col)))
                col += n
            el.clear()
            if nonempty:
                if repeat > 1:
                    return "repeated_row_with_content"
                rows.append(tuple(cells + [""] * (9 - len(cells))))
                if len(rows) > MAX_DATA_ROWS + 1:
                    return "too_many_rows"
    if tables != 1:
        return f"tables={tables}"
    if not rows or rows[0] != LIST_HEADERS:
        return "header"
    data = rows[1:]
    if any(not _FUND_REF.fullmatch(r[1]) or not (r[2].strip() or r[4].strip()) for r in data):
        return "row_structure"
    floor = BOOTSTRAP_MIN_ROWS if baseline is None else 0.9 * baseline
    if len(data) < floor:
        return f"rows {len(data)} below {floor}"
    print(f"  [reader] logical text of the version: {total_logical} characters")
    return data


METHODS = ("identifier", "identifier_in_text", "near_identifier", "name")
EXAMPLES_PER_METHOD = 25


def list_observation(
    isin: str, keys: Sequence[str], versions: Sequence[tuple[int, list[tuple[str, ...]]]]
) -> tuple[str, dict[int, dict[str, Any]]]:
    """Per version: complete per-method counts and the first ``EXAMPLES_PER_METHOD`` rows of each method, in row
    order (spec Design 2); the outcome over all versions: ``listed`` (any ``identifier``), ``unresolved`` (any other
    match) or ``not_found``. Availability is the service's."""
    nsin = isin[2:11] if isin[:2] in ("US", "CA") else None
    targets = [isin] + ([nsin] if nsin else [])
    results: dict[int, dict[str, Any]] = {}
    for fetch_id, rows in versions:
        counts: dict[str, int] = dict.fromkeys(METHODS, 0)
        examples: list[dict[str, Any]] = []
        for row in rows:
            method, key = match_row(row, isin, nsin, targets, keys)
            if method is None:
                continue
            counts[method] += 1
            if counts[method] <= EXAMPLES_PER_METHOD:
                examples.append({"method": method, "key": key, "cells": list(row)})
        results[fetch_id] = {"counts": counts, "examples": examples}
    if any(r["counts"]["identifier"] for r in results.values()):
        return "listed", results
    if any(sum(r["counts"].values()) for r in results.values()):
        return "unresolved", results
    return "not_found", results


def match_row(
    row: tuple[str, ...], isin: str, nsin: str | None, targets: Sequence[str], keys: Sequence[str]
) -> tuple[str | None, str | None]:
    """The first match test that applies to one row (spec Design 2), with the target or name key it matched."""
    isin_cell, cusip_cell = row[5].strip().upper(), row[6].strip().upper()
    squashed = (squash(row[5]), squash(row[6]))
    for t in targets:
        if t in squashed:
            return "identifier", t
    for t in targets:
        if any(t in sq for sq in squashed):
            return "identifier_in_text", t
    if near_miss(isin_cell, isin):
        return "near_identifier", isin
    if nsin is not None and near_miss(cusip_cell, nsin):
        return "near_identifier", nsin
    if valid_isin(row[5].strip()) and row[5].strip()[:2] != isin[:2]:
        return None, None
    row_key = name_key(" ".join(row[2:5]))
    key = next((k for k in keys if k in row_key), None)
    return ("name", key) if key else (None, None)


# --- measurement -------------------------------------------------------------------------------------------------


def _save(out: Path, name: str, body: bytes) -> str:
    sha = hashlib.sha256(body).hexdigest()
    (out / f"{name}.gz").write_bytes(gzip.compress(body))
    return sha


def cell_class(cell: str) -> str:
    if cell == cell.strip() and valid_isin(cell):
        return "valid"
    if valid_isin(cell.strip()):
        return "valid_after_trim"
    if _ISIN.fullmatch(cell.strip()):
        return "isin_shape_bad_check"
    if re.fullmatch(r"[A-Za-z0-9]{12}", cell.strip()):
        return "other_12_alnum"
    return "no_data" if cell == "no data" else "other"


def universe() -> None:
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(
            "SELECT coalesce(i.country, '?'), "
            "EXISTS (SELECT 1 FROM external_identifiers e WHERE e.instrument_id = i.instrument_id "
            "AND e.provider = 'sec' AND e.identifier_type = 'cik' AND e.is_primary), "
            "EXISTS (SELECT 1 FROM filing_events f WHERE f.instrument_id = i.instrument_id AND f.provider = 'sec' "
            "AND f.filing_type = 'N-CEN') FROM instruments i WHERE i.instrument_type_id = %s",
            (ETF_TYPE_ID,),
        ).fetchall()
    print(f"[universe] ETF instruments={len(rows)}")
    for (country, cik, ncen), n in Counter(rows).most_common():
        print(f"  country={country} primary_cik={cik} has_ncen={ncen}: {n}")


def _list_counts(rows: list[tuple[str, ...]]) -> Iterator[str]:
    isin_cells = [r[5] for r in rows]
    yield f"isin_cells={dict(Counter(cell_class(c) for c in isin_cells))}"
    shape = Counter(
        "no_data" if r[6] == "no data" else ("cusip_shape" if re.fullmatch(r"[A-Z0-9]{9}", r[6].strip()) else "other")
        for r in rows
    )
    yield f"cusip_cells={dict(shape)}"
    neither = [r for r in rows if r[5] == "no data" and r[6] == "no data"]
    odd_refs = [r[3] for r in rows if not re.fullmatch(r"[A-Z][0-9]{4}-[0-9]{4}", r[3])]
    yield (
        f"both identifiers 'no data'={len(neither)} "
        f"(no cessation date {sum(1 for r in neither if r[8] == 'no data')}); "
        f"US-prefixed ISIN cells={sum(1 for c in isin_cells if c.startswith('US'))}; "
        f"share-class refs not [A-Z]dddd-dddd={odd_refs}"
    )
    for col, label in ((7, "with effect from"), (8, "ceased")):
        kinds = Counter("date" if re.fullmatch(r"\d\d/\d\d/\d{4}", r[col]) else r[col] for r in rows)
        yield f"{label}: {kinds.most_common(4)}"
    named = [r for r in rows if "SPDR S&P 500" in " ".join(r[2:5]).upper()]
    yield f"rows naming 'SPDR S&P 500': {len(named)} {sorted((r[2], r[5]) for r in named)}"
    yield f"rows without a valid ISIN after trimming: {sum(1 for c in isin_cells if not valid_isin(c.strip()))}"


def _fetch_chain(
    client: httpx.Client, out: Path, ua: dict[str, str], newest: Filing, cik: str, symbol: str | None
) -> tuple[dict[str, bytes], bytes | None, bytes | None, bytes | None]:
    ncen: dict[str, bytes] = {}
    record = isins = figi = None
    url = ncen_xml_url(newest, cik)
    if url is None:
        return ncen, record, isins, figi
    r = client.get(url, headers=ua)
    sha = _save(out, "ncen_" + newest.accession, r.content)
    print(f"  [n-cen {newest.accession}] {url} http={r.status_code} sha256={sha}")
    try:
        root = ET.fromstring(r.content, parser=SAFE_XML_PARSER)
        sound = root.tag == f"{{{NCEN}}}edgarSubmission" and [
            e.text for e in root.findall("n:headerData/n:submissionType", {"n": NCEN})
        ] == ["N-CEN"]
    except ET.XMLSyntaxError:
        sound = False
    print(f"    capture sound (2xx, N-CEN root and submissionType)={200 <= r.status_code < 300 and sound}")
    if not (200 <= r.status_code < 300 and sound):
        return ncen, record, isins, figi
    ncen[newest.accession] = r.content
    doc = read_ncen(r.content, cik)
    print(f"    read_ncen={doc}")
    if doc is None:
        return ncen, record, isins, figi
    rec = client.get(f"{GLEIF}/{doc.lei}")
    print(f"  [gleif record] http={rec.status_code} sha256={_save(out, 'gleif_record_' + doc.lei, rec.content)}")
    record = rec.content if rec.status_code == 200 else None
    print(f"    read_gleif_record={read_gleif_record(rec.content, doc.lei) if record else None}")
    if record and read_gleif_record(record, doc.lei) is not None:
        entity = json.loads(record)["data"]["attributes"]["entity"]
        print(
            f"    legalForm={entity.get('legalForm')} jurisdiction={entity.get('jurisdiction')} "
            f"legalAddress={entity.get('legalAddress') if isinstance(entity.get('legalAddress'), dict) else None} "
            f"successorEntity={entity.get('successorEntity')} successorEntities={entity.get('successorEntities')}"
        )
    ir = client.get(f"{GLEIF}/{doc.lei}/isins", params={"page[size]": "200", "page[number]": "1"})
    print(f"  [gleif isins] http={ir.status_code} sha256={_save(out, 'gleif_isins_' + doc.lei, ir.content)}")
    isins = ir.content if ir.status_code == 200 else None
    parsed = read_gleif_isins(ir.content) if isins else None
    print(f"    read_gleif_isins={parsed}")
    keyed = [symbol_key(t) for t in doc.tickers]
    if parsed and symbol is not None and keyed.count(symbol) == 1 and 0 < len(parsed[0]) <= 10:
        jobs = figi_jobs(parsed[0], doc.exchanges[keyed.index(symbol)])
        time.sleep(OPENFIGI_SPACING_S)
        m = client.post(OPENFIGI, json=jobs)
        print(f"  [openfigi {jobs}] http={m.status_code} sha256={_save(out, 'openfigi_' + doc.lei, m.content)}")
        print(f"    body={m.text[:600]}")
        if m.status_code == 200:
            figi = m.content
            print(f"    read_figi={read_figi(m.content, len(jobs))}")
    return ncen, record, isins, figi


def main() -> None:
    if "--universe" in sys.argv[1:]:
        universe()
        return
    out = Path("var/research/3616/slice3d3c1") / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out.mkdir(parents=True, exist_ok=False)
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.read_only = True
        inputs = _read_inputs(conn, bodies=False)
        if inputs is None:
            print("no effective evidence generation")
            return
        members = security_members(inputs)
        funds = {s: m for s, m in members.items() if inputs.characters[s].types == frozenset({ETF_TYPE_ID})}
        all_ids = sorted({i for m in funds.values() for i in m})
        rows = conn.execute(
            "SELECT instrument_id, filing_type, provider_filing_id, filing_date, report_date, primary_document_url "
            "FROM filing_events WHERE provider = 'sec' AND instrument_id = ANY(%s) "
            "AND filing_type IN ('N-CEN', 'N-CEN/A')",
            (all_ids,),
        ).fetchall()
        shapes = conn.execute(
            "SELECT filing_type, regexp_replace(primary_document_url, "
            "'^https://www[.]sec[.]gov/Archives/edgar/data/[0-9]+/[0-9]{18}/', '<base>/'), count(*) "
            "FROM filing_events WHERE provider = 'sec' AND filing_type IN ('N-CEN', 'N-CEN/A') GROUP BY 1, 2"
        ).fetchall()
        country = dict(
            conn.execute(
                "SELECT instrument_id, country FROM instruments WHERE instrument_id = ANY(%s)", (all_ids,)
            ).fetchall()
        )
        nsin_rows = conn.execute(
            "SELECT coalesce(resolution_status, ''), coalesce(source, ''), coalesce(name_of_issuer, ''), "
            "observation_count FROM unresolved_13f_cusips WHERE cusip = '78462F103' ORDER BY 1, 2"
        ).fetchall()
        conn.commit()
    with psycopg.connect(settings.database_url) as conn:
        effective = effective_tax_matching(conn)
        characters = (
            conn.execute(
                "SELECT tax_security_id, character, detail FROM tax_matching_characters WHERE matching_run_id = %s "
                "AND tax_security_id = ANY(%s) ORDER BY 1",
                (effective.matching_run_id, sorted(funds)),
            ).fetchall()
            if effective is not None
            else []
        )
    print(f"[generation] evidence_run_id={inputs.evidence_run_id} as_of={inputs.as_of} securities={len(members)}")
    print(f"[effective matching] {effective} fund characters={characters}")
    print(f"[unresolved_13f_cusips 78462F103] {nsin_rows}")
    print(f"[raw N-CEN/N-CEN/A filing_events rows for fund members] {Counter((r[0], r[1]) for r in rows)}")
    print(f"[fund securities] {sorted(funds)}")
    print(f"[n-cen url shapes, all of dev] {shapes}")
    filings_of: dict[int, list[Filing]] = {}
    for iid, form, acc, filed, report, url in rows:
        filings_of.setdefault(iid, []).append(Filing(acc, form, filed, report, url))

    ua = {"User-Agent": settings.sec_user_agent}
    client = httpx.Client(timeout=60, follow_redirects=False)
    observed_on = datetime.now(UTC).date()
    candidates: dict[int, tuple[str, list[str]]] = {}
    for sec_id, member_ids in sorted(funds.items()):
        ev = inputs.characters[sec_id]
        filings = consolidate([f for i in member_ids for f in filings_of.get(i, [])], observed_on)
        print(f"\n== security {sec_id} members={list(member_ids)} symbol={ev.symbol} ciks={sorted(ev.ciks)}")
        print(f"  countries={sorted({str(country.get(i)) for i in member_ids})}")
        print(f"  filings={[(f.accession, f.form, str(f.filing_date), str(f.report_date)) for f in filings or []]}")
        padded = {padded_cik(c) for c in ev.ciks}
        newest, _amended = newest_period(filings or [])
        chain: tuple[dict[str, bytes], bytes | None, bytes | None, bytes | None] = ({}, None, None, None)
        if newest is not None and len(padded) == 1 and None not in padded:
            chain = _fetch_chain(client, out, ua, newest, str(next(iter(padded))), ev.symbol)
        ncen, record, isins, figi = chain
        outcome, got = identity_outcome(
            ciks=ev.ciks, symbol=ev.symbol, filings=filings, ncen=ncen, record=record, isins=isins, figi=figi
        )
        print(f"  IDENTITY outcome={outcome} fields={json.dumps(got, default=str)}")
        if outcome == "candidate":
            candidates[sec_id] = (got["isin"], got["name_keys"])

    content = client.get(HMRC_CONTENT_URL)
    print(f"\n[hmrc content] http={content.status_code} sha256={_save(out, 'hmrc_content_item', content.content)}")
    try:
        item = content.json() if content.status_code == 200 else {}
    except ValueError:
        item = {}
    if not isinstance(item, dict):
        print("  precondition failed: content item is not a JSON object")
        return
    details = item.get("details")
    attachments = details.get("attachments") if isinstance(details, dict) else None
    attachments = attachments if isinstance(attachments, list) else []
    ods = [a for a in attachments if isinstance(a, dict) and str(a.get("url", "")).lower().endswith(".ods")]
    print(
        f"  public_updated_at={item.get('public_updated_at')} ods={[(a.get('url'), a.get('file_size')) for a in ods]}"
    )
    published = item.get("public_updated_at") if isinstance(item, dict) else None
    rfc3339 = r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])"
    published_ok = isinstance(published, str) and re.fullmatch(rfc3339, published) is not None
    if published_ok:
        try:
            published_ok = datetime.fromisoformat(str(published).replace("Z", "+00:00")) <= datetime.now(UTC)
        except ValueError:
            published_ok = False
    url_ok = len(ods) == 1 and re.fullmatch(
        r"https://assets\.publishing\.service\.gov\.uk/[^?#@:]+\.ods", str(ods[0].get("url"))
    )
    print(f"  preconditions: attachment url on the GOV.UK asset host={bool(url_ok)}")
    size = ods[0].get("file_size") if len(ods) == 1 else None
    print(
        f"  preconditions: one .ods={len(ods) == 1} public_updated_at valid and past={published_ok} "
        f"file_size positive int={type(size) is int and size > 0}"
    )
    if len(ods) != 1 or not published_ok or not url_ok or not (type(size) is int and size > 0):
        return
    r = client.get(ods[0]["url"])
    print(
        f"[hmrc list] http={r.status_code} received={len(r.content)} sha256={_save(out, 'hmrc_weblist_ods', r.content)}"
    )
    print(f"  preconditions: http 200={r.status_code == 200} size equals file_size={len(r.content) == size}")
    if r.status_code != 200 or len(r.content) != size:
        return
    started = time.monotonic()
    parsed_list = read_reporting_list(r.content, None)
    print(f"  read_reporting_list (no earlier version) in {time.monotonic() - started:.1f}s")
    if isinstance(parsed_list, str):
        print(f"  UNUSABLE: {parsed_list}")
        return
    print(f"  usable; data rows={len(parsed_list)}")
    for line in _list_counts(parsed_list):
        print(f"  {line}")
    for sec_id, (isin, keys) in candidates.items():
        obs, results = list_observation(isin, keys, [(0, parsed_list)])
        print(f"\n  LIST security={sec_id} isin={isin} name_keys={keys} outcome={obs} per_version={results}")


if __name__ == "__main__":
    main()
