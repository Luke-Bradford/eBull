"""Reviewed fund determination (#3616 slice 3d-3c-2): the pure format, rules and outcome order.

Spec: docs/specs/metrics/2026-10-14-3616-slice3d3c2-fund-determination.md.

- ``parse_determination`` reads a committed determination file and ``validate`` checks its format (Design 1);
  ``authority_refusal`` checks one authority snapshot against it.
- ``document_text`` and ``content_sha256`` reduce a stored SEC document to its quotable text and bound hash.
- ``offshore_fund`` applies s355-s357 and the exclusions to the findings (Design 1).
- ``select_document_fetch``, ``newest_fetch``, ``submissions_coverage``, ``unreviewed_accessions`` and ``decide`` give
  Design 2's ordered outcome for one fund security.
- ``reporting_status`` is Design 3: a fund security's reporting status at a disposal date, over the newest run.

Nothing here touches the network or the DB, and nothing here changes a fund's tax character: every fund stays
withheld as ``exchange_traded_fund`` (slice 3d-3a). The storage and the job step are the build's second part.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from html.parser import HTMLParser
from typing import Any, Literal, cast
from urllib.parse import urlsplit, urlunsplit

from app.services.def14a_recipients import symbol_key

#: The frozen local policy (spec "Local policy"); changing any part of it is a new id.
FUND_DETERMINATION_POLICY_ID = "fund_determination_v1"
#: The one identity policy whose fund observations v1 accepts (spec Design 4).
ACCEPTED_IDENTITY_POLICY_ID = "fund_identity_v1"

GOVERNING_FORMS = frozenset(
    {"485APOS", "485BPOS", "485BXT", "497", "497J", "497K", "N-CEN", "N-CEN/A", "N-8F", "N-8F/A"}
)
#: Assumptions every determination must carry, because Design 3 adds them by policy.
POLICY_ASSUMPTIONS = (
    "A5_part3_not_applying",
    "A6_law_current",
    "A7_identity_continuity",
    "A8_inventory_complete",
    "A9_terms_continuity",
)
NON_REPORTING_ASSUMPTION = "A5_part3_not_applying"
QUESTIONS: dict[str, frozenset[str]] = {
    "arrangement": frozenset({"single_arrangement_single_class", "unresolved"}),
    "s355_limb": frozenset({"b", "none", "unresolved"}),
    "s356_condition_a": frozenset({"met", "not_met", "unresolved"}),
    "s356_condition_b": frozenset({"met", "not_met", "unresolved"}),
    "s356_condition_c": frozenset({"met", "not_met", "unresolved"}),
    "s357_condition_d": frozenset({"met", "not_met", "unresolved"}),
    "other_exclusions": frozenset({"no_applicable_exclusion", "applies", "unresolved"}),
    "transparent_fund": frozenset({"transparent", "opaque", "unresolved"}),
    #: Holding-dependent; policy v1 does not model holdings, so these are unresolved only.
    "reg29_sufficient_information": frozenset({"unresolved"}),
    "reg29_nonreporting_holdings": frozenset({"unresolved"}),
}
BASES = frozenset({"established", "assumed", "unresolved"})
#: ``offshore_fund``'s inputs in the table's question order: (answers that admit true, answers that make it false).
OFFSHORE_INPUTS: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "arrangement": (("single_arrangement_single_class",), ()),
    "s355_limb": (("b",), ("none",)),
    "s356_condition_a": (("met",), ("not_met",)),
    "s356_condition_b": (("met",), ("not_met",)),
    "s356_condition_c": (("met",), ("not_met",)),
    "s357_condition_d": (("not_met",), ()),
    "other_exclusions": (("no_applicable_exclusion",), ("applies",)),
}
#: The filing list is refetched when its newest fetch is over this old (elapsed time).
SUBMISSIONS_REFRESH = timedelta(days=7)
#: The filing list admits a determination only while its newest fetch is at most this old at the cutoff.
SUBMISSIONS_CURRENT = timedelta(days=30)
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
#: Tags whose boundary separates words; any other tag (``b``, ``i``, ``span``, ``font``, ``a``, ``page`` ...) joins.
BLOCK_TAGS = frozenset(
    {"p", "div", "br", "li", "ul", "ol", "tr", "td", "th", "table", "thead", "tbody", "tfoot", "caption", "h1", "h2",
     "h3", "h4", "h5", "h6", "blockquote", "dt", "dd", "dl", "section", "article", "center", "hr", "title", "pre",
     "html", "body", "head", "document", "text", "type", "sequence", "filename", "description"}
)  # fmt: skip

_CIK = re.compile(r"[0-9]{10}")
_ACCESSION = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")
_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_SHA = re.compile(r"[0-9a-f]{64}")
_LEI = re.compile(r"[A-Z0-9]{18}[0-9]{2}")
_ISIN = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]")
_MIC = re.compile(r"[A-Z0-9]{4}")
_ID = re.compile(r"[a-z0-9_]+")
_ASSUMPTION_ID = re.compile(r"A[0-9]+_[a-z0-9_]+")

Outcome = Literal[
    "fund_observation_unavailable",
    "not_candidate",
    "no_determination",
    "binding_mismatch",
    "documents_unavailable",
    "document_changed",
    "quote_unverified",
    "review_out_of_date",
    "coverage_unavailable",
    "stale",
    "determined",
]
#: Design 2's outcomes in table order; the first four carry no determination.
OUTCOMES: tuple[Outcome, ...] = (
    "fund_observation_unavailable",
    "not_candidate",
    "no_determination",
    "binding_mismatch",
    "documents_unavailable",
    "document_changed",
    "quote_unverified",
    "review_out_of_date",
    "coverage_unavailable",
    "stale",
    "determined",
)
UNDETERMINED_OUTCOMES: frozenset[Outcome] = frozenset(OUTCOMES[:4])
OffshoreAnswer = Literal["true", "false", "unresolved"]
Basis = Literal["established", "assumed", "unresolved"]
ReportingStatus = Literal[
    "no_run",
    "run_failed",
    "not_observed",
    "fund_observation_unavailable",
    "not_candidate",
    "no_determination",
    "binding_mismatch",
    "review_out_of_date",
    "documents_unavailable",
    "document_changed",
    "quote_unverified",
    "coverage_unavailable",
    "stale",
    "outside_interval",
    "not_offshore_fund",
    "offshore_fund_unresolved",
    "reporting_status_unestablished",
    "non_reporting_assumed",
]

Determination = dict[str, Any]


# --- text and hashes ---------------------------------------------------------------------------------------------


class _Text(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("script", "style"):
            self._skip += 1
        if tag in BLOCK_TAGS:
            self.parts.append(" ")

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in BLOCK_TAGS:
            self.parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style") and self._skip:
            self._skip -= 1
        if tag in BLOCK_TAGS:
            self.parts.append(" ")

    def handle_data(self, data: str) -> None:
        if not self._skip:
            self.parts.append(data)


def collapse(text: str) -> str:
    """Every run of white space (Unicode, so NBSP too) to one space, trimmed."""
    return re.sub(r"\s+", " ", text).strip()


def document_text(body: bytes) -> str:
    """Strict UTF-8 (raises ``UnicodeDecodeError``); tags dropped, a block tag becoming a space and any other joining
    its neighbours; ``script`` and ``style`` content dropped; character references decoded once; white space
    collapsed (spec Design 2)."""
    parser = _Text()
    parser.feed(body.decode("utf-8", errors="strict"))
    parser.close()
    return collapse("".join(parser.parts))


def _is_xml(url: str) -> bool:
    return urlsplit(url).path.endswith(".xml")


def content_sha256(url: str, body: bytes) -> str:
    """The bound hash: of the bytes for a path ending ``.xml`` (whose attributes carry its facts), else of
    ``document_text(body)`` encoded UTF-8 (raises ``UnicodeDecodeError`` on invalid UTF-8)."""
    if _is_xml(url):
        return hashlib.sha256(body).hexdigest()
    return hashlib.sha256(document_text(body).encode()).hexdigest()


# --- format (Design 1) -------------------------------------------------------------------------------------------


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    keys = [k for k, _ in pairs]
    if len(keys) != len(set(keys)):
        raise ValueError(f"duplicate JSON key in {sorted(keys)}")
    return dict(pairs)


def parse_determination(raw: bytes) -> tuple[Determination | None, list[str]]:
    """Read a determination file as UTF-8 bytes with duplicate keys refused, then ``validate`` it. Returns the body
    and no errors, or ``None`` and the format errors; never raises on bad content."""
    try:
        det = json.loads(raw.decode("utf-8"), object_pairs_hook=_no_duplicates)
    except (ValueError, RecursionError) as exc:  # UnicodeDecodeError and JSONDecodeError are ValueErrors
        return None, [f"unreadable: {type(exc).__name__}"]
    errs = validate(det)
    return (det, []) if not errs else (None, errs)


def validate(det: Any) -> list[str]:
    """Design 1's format errors; empty when valid. Total: a wrong type is an error, never an exception."""
    try:
        return _validate(det)
    except (TypeError, AttributeError, KeyError, ValueError, RecursionError) as exc:
        return [f"malformed: {type(exc).__name__}"]


def _is_date(v: Any) -> bool:
    if not isinstance(v, str) or not _DATE.fullmatch(v):
        return False
    try:
        date.fromisoformat(v)
    except ValueError:
        return False
    return True


def _text(v: Any) -> bool:
    return isinstance(v, str) and bool(collapse(v))


def _strs(v: Any) -> bool:
    return isinstance(v, list) and all(isinstance(x, str) for x in v)


def _keys(obj: Any, keys: set[str], where: str, errs: list[str]) -> bool:
    if not isinstance(obj, dict) or set(obj) != keys:
        errs.append(f"{where}: keys")
        return False
    return True


def sec_document_url_ok(url: str, cik: str, accession: str) -> bool:
    """A document URL: printable ASCII only, equal to its canonical parsed form, ``https://www.sec.gov`` with no
    query, fragment or parameters, and path ``/Archives/edgar/data/<cik as integer>/<accession digits>/<segment>``
    whose last segment is not ``.`` or ``..`` and holds no percent-encoding."""
    if not all(33 <= ord(c) <= 126 for c in url) or urlunsplit(urlsplit(url)) != url:
        return False
    parts = urlsplit(url)
    if parts.scheme != "https" or parts.netloc != "www.sec.gov" or parts.query or parts.fragment or ";" in parts.path:
        return False
    segs = parts.path.split("/")
    return (
        len(segs) == 7
        and segs[:4] == ["", "Archives", "edgar", "data"]
        and segs[4] == str(int(cik))
        and segs[5] == accession.replace("-", "")
        and bool(segs[6])
        and segs[6] not in (".", "..")
        and "%" not in segs[6]
    )


def review_by_for(reviewed_at: date) -> date:
    """``reviewed_at`` one year on; 29 February maps to 28 February."""
    try:
        return reviewed_at.replace(year=reviewed_at.year + 1)
    except ValueError:
        return reviewed_at.replace(year=reviewed_at.year + 1, day=28)


def _validate(det: Any) -> list[str]:
    errs: list[str] = []
    top = {"determination_id", "policy_id", "cik", "version", "binding", "valid_from", "valid_from_document",
           "reviewed_at", "review_by", "monitor_from", "refresh_forms", "reviewed_filings", "documents", "authorities",
           "assumptions", "findings"}  # fmt: skip
    if not _keys(det, top, "determination", errs):
        return errs
    cik, version = det["cik"], det["version"]
    if det["policy_id"] != FUND_DETERMINATION_POLICY_ID or not (isinstance(cik, str) and _CIK.fullmatch(cik)):
        return [*errs, "policy or cik"]
    if not (isinstance(version, int) and not isinstance(version, bool) and version == 1):
        errs.append("version (policy v1 supports version 1 only)")
    if det["determination_id"] != f"{FUND_DETERMINATION_POLICY_ID}:{cik}:{version}":
        errs.append("determination_id")
    b = det["binding"]
    if not (
        _keys(b, {"cik", "lei", "isin", "mic", "ticker"}, "binding", errs)
        and all(isinstance(b[k], str) for k in b)
        and b["cik"] == cik
        and _LEI.fullmatch(b["lei"])
        and _ISIN.fullmatch(b["isin"])
        and _MIC.fullmatch(b["mic"])
        and _text(b["ticker"])
    ):
        errs.append("binding")
    if not all(_is_date(det[k]) for k in ("valid_from", "reviewed_at", "review_by", "monitor_from")):
        return [*errs, "dates"]
    if not (det["valid_from"] <= det["reviewed_at"] and det["monitor_from"] == det["valid_from"]):
        errs.append("date order")
    if det["review_by"] != review_by_for(date.fromisoformat(det["reviewed_at"])).isoformat():
        errs.append("review_by must be one year after reviewed_at")
    if not (_strs(det["refresh_forms"]) and GOVERNING_FORMS <= set(det["refresh_forms"])):
        errs.append("refresh_forms must be strings including every governing form")
    for k in ("documents", "authorities", "assumptions", "findings", "reviewed_filings"):
        if not isinstance(det[k], list):
            return [*errs, f"{k}: not a list"]
    sources: dict[str, str] = {}
    docs: dict[str, dict[str, Any]] = {}
    dkeys = {"doc_id", "url", "accession", "form", "exhibit", "filed", "role", "content_sha256"}
    for d in det["documents"]:
        if not (
            _keys(d, dkeys, "document", errs)
            and all(isinstance(d[k], str) for k in dkeys - {"exhibit"})
            and (d["exhibit"] is None or _text(d["exhibit"]))
        ):
            errs.append("document types")
            continue
        if d["doc_id"] in sources or not _ID.fullmatch(d["doc_id"]):
            errs.append(f"document id {d['doc_id']}")
        sources[d["doc_id"]] = "document"
        docs[d["doc_id"]] = d
        if not (_ACCESSION.fullmatch(d["accession"]) and sec_document_url_ok(d["url"], cik, d["accession"])):
            errs.append(f"document {d['doc_id']}: accession or url")
        if not (_is_date(d["filed"]) and d["filed"] <= det["reviewed_at"] and _text(d["form"]) and _text(d["role"])):
            errs.append(f"document {d['doc_id']}: form, role or date")
        if not _SHA.fullmatch(d["content_sha256"]):
            errs.append(f"document {d['doc_id']}: hash")
    prospectuses = [d for d in docs.values() if d["form"] == "485BPOS"]
    anchor = docs.get(det["valid_from_document"]) if isinstance(det["valid_from_document"], str) else None
    if (
        anchor is None
        or anchor["form"] != "485BPOS"
        or anchor["filed"] != det["valid_from"]
        or anchor["filed"] != max(d["filed"] for d in prospectuses)
    ):
        errs.append("valid_from_document must be the newest reviewed 485BPOS, and valid_from its date")
    akeys = {"authority_id", "url", "version", "content_sha256"}
    for a in det["authorities"]:
        if not (_keys(a, akeys, "authority", errs) and all(isinstance(a[k], str) for k in akeys)):
            errs.append("authority types")
            continue
        if a["authority_id"] in sources or not _ID.fullmatch(a["authority_id"]):
            errs.append(f"authority id {a['authority_id']}")
        sources[a["authority_id"]] = "authority"
        if urlsplit(a["url"]).scheme != "https" or not urlsplit(a["url"]).hostname or not _text(a["version"]):
            errs.append(f"authority {a['authority_id']}")
        if not _SHA.fullmatch(a["content_sha256"]):
            errs.append(f"authority {a['authority_id']}: hash")
    manifest: dict[str, dict[str, Any]] = {}
    members: list[str] = []
    for f in det["reviewed_filings"]:
        if not _keys(f, {"accession", "form", "filed", "documents"}, "reviewed filing", errs) or not _strs(
            f["documents"]
        ):
            errs.append("reviewed filing types")
            continue
        if f["accession"] in manifest or not _is_date(f["filed"]) or not f["documents"] or not _text(f["form"]):
            errs.append(f"reviewed filing {f['accession']}")
        manifest[f["accession"]] = f
        members += f["documents"]
        for doc_id in f["documents"]:
            d = docs.get(doc_id)
            if d is None or d["accession"] != f["accession"] or d["filed"] != f["filed"] or d["form"] != f["form"]:
                errs.append(f"reviewed filing {f['accession']}: document {doc_id}")
    if sorted(members) != sorted(docs):
        errs.append("every document must belong to exactly one reviewed filing")
    assumption_ids: set[str] = set()

    def quotes_ok(quotes: Any, where: str) -> None:
        if not isinstance(quotes, list):
            errs.append(f"{where}: quotes")
            return
        for q in quotes:
            if not (
                _keys(q, {"source_id", "locator", "text"}, where, errs)
                and q["source_id"] in sources
                and _text(q["locator"])
                and _text(q["text"])
            ):
                errs.append(f"{where}: quote")

    for a in det["assumptions"]:
        if not _keys(a, {"assumption_id", "statement", "evidence", "residual"}, "assumption", errs):
            continue
        aid = a["assumption_id"]
        if aid in assumption_ids or not (
            isinstance(aid, str) and _ASSUMPTION_ID.fullmatch(aid) and _text(a["statement"]) and _text(a["residual"])
        ):
            errs.append(f"assumption {aid}")
        assumption_ids.add(aid)
        quotes_ok(a["evidence"], f"assumption {aid}")
    for required in POLICY_ASSUMPTIONS:
        if required not in assumption_ids:
            errs.append(f"policy assumption {required} missing")
    fkeys = {"question", "answer", "basis", "assumptions", "authorities", "quotes", "contrary", "reasoning",
             "would_resolve"}  # fmt: skip
    seen: list[str] = []
    for f in det["findings"]:
        if not (_keys(f, fkeys, "finding", errs) and _strs(f["assumptions"]) and _strs(f["authorities"])):
            errs.append("finding types")
            continue
        q, ans, basis = f["question"], f["answer"], f["basis"]
        seen.append(q)
        if ans not in QUESTIONS.get(q, ()) or basis not in BASES:
            errs.append(f"{q}: answer or basis")
        if (ans == "unresolved") != (basis == "unresolved"):
            errs.append(f"{q}: unresolved answer needs unresolved basis")
        if basis == "assumed" and not f["assumptions"]:
            errs.append(f"{q}: assumed without an assumption")
        if basis != "assumed" and f["assumptions"]:
            errs.append(f"{q}: assumptions on a basis other than assumed")
        if basis == "unresolved" and not _text(f["would_resolve"]):
            errs.append(f"{q}: unresolved without would_resolve")
        if f["would_resolve"] is not None and not _text(f["would_resolve"]):
            errs.append(f"{q}: blank would_resolve")
        if ans != "unresolved" and not f["quotes"]:
            errs.append(f"{q}: no quote")
        if not _text(f["reasoning"]):
            errs.append(f"{q}: reasoning")
        if not set(f["assumptions"]) <= assumption_ids or not f["authorities"]:
            errs.append(f"{q}: assumptions or authorities")
        if any(sources.get(x) != "authority" for x in f["authorities"]):
            errs.append(f"{q}: unknown authority")
        quotes_ok(f["quotes"], q)
        quotes_ok(f["contrary"], q)
    if sorted(seen) != sorted(QUESTIONS):
        errs.append(f"questions {sorted(set(seen) ^ set(QUESTIONS))}")
    return errs


def authority_refusal(authority: Mapping[str, Any], raw: bytes) -> str | None:
    """None if a snapshot's bytes have the authority's hash and its first line names the authority's URL (or, for an
    HMRC manual page, its content-API URL); otherwise why not."""
    if hashlib.sha256(raw).hexdigest() != authority["content_sha256"]:
        return "hash"
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeDecodeError:
        return "undecodable"
    url = authority["url"]
    api = url.replace("www.gov.uk/hmrc-internal-manuals", "www.gov.uk/api/content/hmrc-internal-manuals")
    if not lines or lines[0] not in (f"URL: {url}", f"URL: {api}"):
        return "url line"
    return None


def authority_text(raw: bytes) -> str:
    """A validated snapshot's quotable text: decoded, white space collapsed."""
    return collapse(raw.decode("utf-8"))


# --- offshore fund (Design 1) ------------------------------------------------------------------------------------


@dataclass(frozen=True)
class OffshoreFund:
    answer: OffshoreAnswer
    basis: Basis
    assumptions: tuple[str, ...]


def offshore_fund(det: Determination) -> OffshoreFund:
    """False on a decisive negative (established if any is, else assumed with the first one's assumptions in the
    table's order); true when every input admits it (assumed with their union if any is assumed); else unresolved."""
    f = {x["question"]: x for x in det["findings"]}
    negatives = [q for q, (_, neg) in OFFSHORE_INPUTS.items() if f[q]["answer"] in neg]
    if negatives:
        if any(f[q]["basis"] == "established" for q in negatives):
            return OffshoreFund("false", "established", ())
        return OffshoreFund("false", "assumed", tuple(f[negatives[0]]["assumptions"]))
    if any(f[q]["answer"] not in pos for q, (pos, _) in OFFSHORE_INPUTS.items()):
        return OffshoreFund("unresolved", "unresolved", ())
    if any(f[q]["basis"] == "assumed" for q in OFFSHORE_INPUTS):
        return OffshoreFund("true", "assumed", tuple(sorted({a for q in OFFSHORE_INPUTS for a in f[q]["assumptions"]})))
    return OffshoreFund("true", "established", ())


# --- applicability (Design 2) ------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Fetch:
    """One stored fetch of a document or the filing list."""

    fetch_id: int
    fetched_at: datetime
    body: bytes


@dataclass(frozen=True)
class FundObservation:
    """The 3d-3c-1 fund observation of one security, as Design 2 reads it."""

    fund_observation_id: int
    outcome: str
    cik: str | None
    lei: str | None
    isin: str | None
    mic: str | None
    symbol: str | None


@dataclass(frozen=True)
class Discovery:
    """An unreviewed governing filing listed in a filing-list fetch."""

    accession: str
    form: str
    filed: date


@dataclass(frozen=True)
class Decision:
    """Design 2's outcome for one fund security, with what its observation row records."""

    outcome: Outcome
    refusal: str | None = None
    determination_id: str | None = None
    submissions_fetch_id: int | None = None
    #: doc_id → the fetch used, for a ``determined`` outcome.
    document_fetch_ids: Mapping[str, int] | None = None
    offshore: OffshoreFund | None = None


def _utc(at: datetime) -> datetime:
    if at.tzinfo is None:
        raise ValueError("a naive datetime: every time is a timezone-aware instant")
    return at.astimezone(UTC)


def newest_fetch(fetches: Iterable[Fetch], cutoff: datetime) -> Fetch | None:
    """The newest fetch by ``(fetched_at, fetch_id)`` with ``fetched_at`` ≤ cutoff."""
    eligible = [f for f in fetches if _utc(f.fetched_at) <= _utc(cutoff)]
    return max(eligible, key=lambda f: (_utc(f.fetched_at), f.fetch_id), default=None)


def select_document_fetch(
    url: str, expected_sha256: str, fetches: Iterable[Fetch], cutoff: datetime
) -> tuple[Fetch | None, Literal["documents_unavailable", "document_changed"] | None]:
    """A cited document's evidence: its newest fetch with ``fetched_at`` ≤ cutoff whose content hash is the version's.
    ``documents_unavailable`` when no fetch is at or before the cutoff; ``document_changed`` when none matches."""
    eligible = [f for f in fetches if _utc(f.fetched_at) <= _utc(cutoff)]
    if not eligible:
        return None, "documents_unavailable"
    matching = [f for f in eligible if _hash_or_none(url, f.body) == expected_sha256]
    if not matching:
        return None, "document_changed"
    return max(matching, key=lambda f: (_utc(f.fetched_at), f.fetch_id)), None


def _hash_or_none(url: str, body: bytes) -> str | None:
    """The bound hash of a fetch that is usable as evidence, or None: its text must also be readable, because quotes
    are checked against ``document_text`` even where the hash binds an XML document's bytes."""
    try:
        document_text(body)
        return content_sha256(url, body)
    except UnicodeDecodeError:
        return None


def _parse_json(body: bytes) -> Any:
    try:
        return json.loads(body.decode("utf-8"), object_pairs_hook=_no_duplicates)
    except ValueError, RecursionError:
        return None


def _recent_rows(body: Any, cik: str) -> list[tuple[Any, Any, Any]] | None:
    """The filing list's ``filings.recent`` rows (form, filingDate, accessionNumber), when the body is an object for the
    CIK whose three lists are lists of equal length; else None."""
    if not isinstance(body, dict) or body.get("cik") != cik:
        return None
    filings = body.get("filings")
    recent = filings.get("recent") if isinstance(filings, dict) else None
    if not isinstance(recent, dict):
        return None
    cols = (recent.get("form"), recent.get("filingDate"), recent.get("accessionNumber"))
    if not all(isinstance(c, list) for c in cols) or len({len(c) for c in cols if isinstance(c, list)}) != 1:
        return None
    return list(zip(*cols, strict=True))  # type: ignore[arg-type]


def _well_formed(form: Any, filed: Any, acc: Any, fetched_on: date) -> bool:
    return (
        _text(form)
        and _is_date(filed)
        and isinstance(acc, str)
        and bool(_ACCESSION.fullmatch(acc))
        and date.fromisoformat(filed) <= fetched_on
    )


def unreviewed_accessions(det: Determination, submissions: Fetch) -> list[Discovery]:
    """The discoveries a filing-list fetch records (spec "Unreviewed accessions"): each listed filing of a
    ``refresh_forms`` form filed on or after ``monitor_from`` and not reviewed, from any body that is an object for the
    CIK with equal-length ``filings.recent`` lists, whose form, date and accession are well formed and whose date is
    no later than the fetch's UTC date. Currency, overflow ``files`` and the reviewed anchors are not required."""
    rows = _recent_rows(_parse_json(submissions.body), det["cik"])
    if rows is None:
        return []
    fetched_on = _utc(submissions.fetched_at).date()
    reviewed = {f["accession"] for f in det["reviewed_filings"]}
    monitor_from = date.fromisoformat(det["monitor_from"])
    found: dict[str, Discovery] = {}
    for form, filed, acc in rows:
        if not _well_formed(form, filed, acc, fetched_on):
            continue
        filed_on = date.fromisoformat(filed)
        if form in det["refresh_forms"] and filed_on >= monitor_from and acc not in reviewed and acc not in found:
            found[acc] = Discovery(acc, form, filed_on)
    return list(found.values())


def submissions_coverage(det: Determination, submissions: Fetch | None, cutoff: datetime) -> str | None:
    """None if the newest filing-list fetch is a current, sound inventory for the CIK at the cutoff; otherwise the
    ``coverage_unavailable`` refusal (spec Design 2)."""
    if submissions is None:
        return "submissions missing"
    fetched_at, at = _utc(submissions.fetched_at), _utc(cutoff)
    if fetched_at > at:
        return "submissions dated after the cutoff"
    if at - fetched_at > SUBMISSIONS_CURRENT:
        return "submissions not current"
    body = _parse_json(submissions.body)
    rows = _recent_rows(body, det["cik"])
    if rows is None or not isinstance(body["filings"].get("files"), list):
        return "submissions unsound"
    seen: dict[str, tuple[str, str]] = {}
    for form, filed, acc in rows:
        if not _well_formed(form, filed, acc, fetched_at.date()) or acc in seen:
            return "submissions unsound"
        seen[acc] = (form, filed)
    if body["filings"]["files"]:
        return "submissions overflow files"
    for f in det["reviewed_filings"]:
        if f["filed"] >= det["monitor_from"] and seen.get(f["accession"]) != (f["form"], f["filed"]):
            return f"submissions disagree with reviewed filing {f['accession']}"
    return None


def _binding_mismatch(det: Determination, obs: FundObservation) -> str | None:
    b = det["binding"]
    for name, mine, theirs in (("cik", obs.cik, b["cik"]), ("lei", obs.lei, b["lei"]), ("isin", obs.isin, b["isin"]),
                               ("mic", obs.mic, b["mic"])):  # fmt: skip
        if mine != theirs:
            return name
    if obs.symbol is None or symbol_key(obs.symbol) != symbol_key(b["ticker"]):
        return "ticker"
    return None


def _quotes(det: Determination) -> list[tuple[str, Mapping[str, Any]]]:
    out = [(f["question"], q) for f in det["findings"] for q in (*f["quotes"], *f["contrary"])]
    out += [(a["assumption_id"], q) for a in det["assumptions"] for q in a["evidence"]]
    return out


def decide(
    obs: FundObservation | None,
    determinations: Mapping[str, Determination],
    document_fetches: Mapping[str, Sequence[Fetch]],
    authority_texts: Mapping[str, str],
    submissions: Fetch | None,
    unreviewed: Collection[str],
    cutoff: datetime,
) -> Decision:
    """Design 2's outcome for one fund security, first row that applies.

    ``determinations`` are the run's active ones by CIK; ``document_fetches`` the stored fetches by document URL;
    ``authority_texts`` the validated snapshots' text by authority id; ``submissions`` the candidate CIK's newest
    filing-list fetch with ``fetched_at`` ≤ cutoff; ``unreviewed`` the accessions recorded for the determination
    (this run's discoveries included, since they are committed first)."""
    if obs is None:
        return Decision("fund_observation_unavailable")
    if obs.outcome != "candidate":
        return Decision("not_candidate")
    det = determinations.get(obs.cik or "")
    if det is None:
        return Decision("no_determination")
    did = det["determination_id"]
    mismatch = _binding_mismatch(det, obs)
    if mismatch:
        return Decision("binding_mismatch", mismatch)
    chosen: dict[str, Fetch] = {}
    changed: list[str] = []
    for d in det["documents"]:
        fetch, refusal = select_document_fetch(
            d["url"], d["content_sha256"], document_fetches.get(d["url"], ()), cutoff
        )
        if refusal == "documents_unavailable":
            return Decision("documents_unavailable", d["doc_id"], did)
        if fetch is None:
            changed.append(d["doc_id"])
        else:
            chosen[d["doc_id"]] = fetch
    if changed:
        return Decision("document_changed", changed[0], did)
    texts = {doc_id: document_text(f.body) for doc_id, f in chosen.items()}
    texts.update(authority_texts)
    for where, q in _quotes(det):
        if collapse(q["text"]) not in texts.get(q["source_id"], ""):
            return Decision("quote_unverified", f"{where}:{q['source_id']}", did)
    on = _utc(cutoff).date()
    if not date.fromisoformat(det["reviewed_at"]) <= on <= date.fromisoformat(det["review_by"]):
        return Decision("review_out_of_date", f"cutoff {on} outside {det['reviewed_at']}..{det['review_by']}", did)
    sub_id = submissions.fetch_id if submissions is not None else None
    refusal = submissions_coverage(det, submissions, cutoff)
    if refusal:
        return Decision("coverage_unavailable", refusal, did, sub_id)
    if unreviewed:
        return Decision("stale", sorted(unreviewed)[0], did, sub_id)
    return Decision("determined", None, did, sub_id, {k: f.fetch_id for k, f in chosen.items()}, offshore_fund(det))


# --- reporting status (Design 3) ---------------------------------------------------------------------------------


@dataclass(frozen=True)
class RunState:
    """The newest determination run: its status and, once it succeeded, its cutoff."""

    status: Literal["running", "succeeded", "failed"]
    cutoff: datetime | None


@dataclass(frozen=True)
class ReportingResult:
    status: ReportingStatus
    reason: str | None
    assumptions: tuple[str, ...]


def reporting_status(
    run: RunState | None,
    observed: Decision | None,
    det: Determination | None,
    search_outcome: str | None,
    d: date,
    evaluated_at: datetime,
) -> ReportingResult:
    """Design 3, pure, over the newest run: ``observed`` is that run's row for the security (``outcome``, ``refusal``),
    ``det`` its determination, ``search_outcome`` its fund observation's list search, ``d`` the disposal date."""
    if run is None:
        return ReportingResult("no_run", None, ())
    if run.status != "succeeded" or run.cutoff is None:
        return ReportingResult("run_failed", None, ())
    if observed is None:
        return ReportingResult("not_observed", None, ())
    if observed.outcome in UNDETERMINED_OUTCOMES:
        return ReportingResult(cast(ReportingStatus, observed.outcome), observed.refusal, ())
    if det is None:
        raise ValueError(f"a {observed.outcome} observation without its determination")
    if _utc(evaluated_at).date() > date.fromisoformat(det["review_by"]):
        return ReportingResult("review_out_of_date", f"evaluated after {det['review_by']}", ())
    if observed.outcome != "determined":
        return ReportingResult(cast(ReportingStatus, observed.outcome), observed.refusal, ())
    cutoff_on = _utc(run.cutoff).date()
    if not date.fromisoformat(det["valid_from"]) <= d <= cutoff_on:
        return ReportingResult("outside_interval", f"{d} outside {det['valid_from']}..{cutoff_on}", ())
    offshore = offshore_fund(det)
    rests_on = sorted({*offshore.assumptions, *POLICY_ASSUMPTIONS} - {NON_REPORTING_ASSUMPTION})
    if offshore.answer == "false":
        return ReportingResult("not_offshore_fund", None, tuple(rests_on))
    if offshore.answer == "unresolved":
        return ReportingResult("offshore_fund_unresolved", None, tuple(rests_on))
    if search_outcome in ("listed", "unresolved"):
        return ReportingResult("reporting_status_unestablished", "list_match", tuple(rests_on))
    if search_outcome != "not_found":
        return ReportingResult("reporting_status_unestablished", "search_unavailable", tuple(rests_on))
    return ReportingResult("non_reporting_assumed", None, tuple(sorted({*rests_on, NON_REPORTING_ASSUMPTION})))
