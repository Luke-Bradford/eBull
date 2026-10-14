"""#3616 slice 3d-3c-2 fact-finding: check a draft fund determination against its sources and the stored evidence.

Read-only. Fetches nothing: it reads saved copies. It prints an audit header, then three parts. A format or authority
failure stops it; a document or quote failure is carried into part 3 as Design 2's refusal and never reaches Design 3.
The exit status is non-zero on any failure, and when a candidate security with a determination is not ``determined``.

1. **Format** (spec Design 1): ``validate``.
2. **Sources.** Each SEC document's saved copy under ``--docs`` (``index.txt`` maps path to URL) is reduced by
   ``document_text`` and hashed (an ``.xml`` document's bytes are hashed instead); each authority snapshot
   ``--authorities/<authority_id>.txt`` is hashed as bytes; every quote (finding, contrary, assumption evidence),
   white space collapsed, must occur in its source's text. Failures are kept per source so part 3 maps them to
   Design 2's outcomes.
3. **Applicability and status on the dev DB** (spec Designs 2 and 3), in one REPEATABLE READ snapshot: the fund
   observations of the newest fund run (the greatest ``observed_at``; a narrower probe than the step, which takes its
   own job run's), Design 2's ordered outcome (the filing list is the saved
   ``submissions_CIK<cik>.json``, dated by ``index.txt``), and for a ``determined`` security Design 3's result at each
   disposal date of its lots in the observation's evidence run.

``--fill`` validates, checks sources, and prints to stdout only the draft with every ``PENDING`` hash filled; it exits
non-zero, printing nothing to stdout, if any check other than a pending hash fails. Diagnostics go to stderr.
``--fund-observed-at`` pins the fund run to evaluate (acceptance passes the step's own); one cutoff, the run time,
is used throughout. The Script holds no history of runs, so it neither latches staleness nor exercises
``fund_observation_unavailable``; it fails when the fund run has no observations.

The printout carries dates, symbols, CIKs, LEIs, ISINs and accession numbers, never an account identifier.

    uv run python -m scripts.measure_3616_slice3d3c2_determination \\
        --determination docs/specs/metrics/2026-10-14-3616-slice3d3c2-spy-determination-v1.json \\
        --docs var/research/3616/slice3d3c2/spy --authorities docs/specs/metrics/sources/3616-slice3d3c2
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import psycopg

from app.config import settings
from app.services.def14a_recipients import symbol_key

POLICY_ID = "fund_determination_v1"
GOVERNING_FORMS = frozenset(
    {"485APOS", "485BPOS", "485BXT", "497", "497J", "497K", "N-CEN", "N-CEN/A", "N-8F", "N-8F/A"}
)
#: Assumptions every determination must carry, because Design 3 adds them by policy.
POLICY_ASSUMPTIONS = ("A5_part3_not_applying", "A6_law_current", "A7_identity_continuity", "A8_inventory_complete")
QUESTIONS: dict[str, frozenset[str]] = {
    "arrangement": frozenset({"single_arrangement_single_class", "unresolved"}),
    "s355_limb": frozenset({"b", "none", "unresolved"}),
    "s356_condition_a": frozenset({"met", "not_met", "unresolved"}),
    "s356_condition_b": frozenset({"met", "not_met", "unresolved"}),
    "s356_condition_c": frozenset({"met", "not_met", "unresolved"}),
    "s357_condition_d": frozenset({"met", "not_met", "unresolved"}),
    "other_exclusions": frozenset({"no_applicable_exclusion", "applies", "unresolved"}),
    "transparent_fund": frozenset({"transparent", "opaque", "unresolved"}),
    "reg29_sufficient_information": frozenset({"made_available", "not_made_available", "unresolved"}),
    "reg29_nonreporting_holdings": frozenset({"within_limit", "exceeded", "unresolved"}),
}
#: Findings whose answer depends on the holding period; policy v1 does not model holdings, so they must be unresolved.
HOLDING_DEPENDENT = frozenset({"reg29_sufficient_information", "reg29_nonreporting_holdings"})
BASES = frozenset({"established", "assumed", "unresolved"})
OFFSHORE_INPUTS = {
    "s355_limb": (("b",), ("none",)),
    "s356_condition_a": (("met",), ("not_met",)),
    "s356_condition_b": (("met",), ("not_met",)),
    "s356_condition_c": (("met",), ("not_met",)),
    "s357_condition_d": (("not_met",), ()),
    "arrangement": (("single_arrangement_single_class",), ()),
    "other_exclusions": (("no_applicable_exclusion",), ("applies",)),
}
SUBMISSIONS_CURRENT = timedelta(days=30)
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
_ID = re.compile(r"[a-z0-9_]+")
_ASSUMPTION_ID = re.compile(r"A[0-9]+_[a-z0-9_]+")


def err(*a: object) -> None:
    print(*a, file=sys.stderr)


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
    return re.sub(r"\s+", " ", text).strip()


def document_text(body: bytes) -> str:
    """Strict UTF-8; tags dropped (block tags become a space, others join); script/style content dropped; character
    references decoded once by the parser; white space collapsed."""
    parser = _Text()
    parser.feed(body.decode("utf-8", errors="strict"))
    parser.close()
    return collapse("".join(parser.parts))


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


def _keys(obj: Any, keys: set[str], where: str, errs: list[str]) -> bool:
    if not isinstance(obj, dict) or set(obj) != keys:
        errs.append(f"{where}: keys")
        return False
    return True


def _sec_url_ok(url: str, cik: str, accession: str) -> bool:
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
    )


def _review_by(reviewed_at: date) -> date:
    try:
        return reviewed_at.replace(year=reviewed_at.year + 1)
    except ValueError:  # 29 February
        return reviewed_at.replace(year=reviewed_at.year + 1, day=28)


def validate(det: Any, allow_pending: bool = False) -> list[str]:
    try:
        return _validate(det, allow_pending)
    except (TypeError, AttributeError, KeyError, ValueError) as exc:
        return [f"malformed: {type(exc).__name__}: {exc}"]


def _strs(v: Any) -> bool:
    return isinstance(v, list) and all(isinstance(x, str) for x in v)


def _validate(det: Any, allow_pending: bool) -> list[str]:
    errs: list[str] = []
    top = {"determination_id", "policy_id", "cik", "version", "binding", "valid_from", "valid_from_document",
           "reviewed_at", "review_by", "monitor_from", "refresh_forms", "reviewed_filings", "documents", "authorities",
           "assumptions", "findings"}  # fmt: skip
    if not _keys(det, top, "determination", errs):
        return errs
    cik, version = det["cik"], det["version"]
    if det["policy_id"] != POLICY_ID or not (isinstance(cik, str) and _CIK.fullmatch(cik)):
        return [*errs, "policy or cik"]
    if not (isinstance(version, int) and not isinstance(version, bool) and version == 1):
        errs.append("version (policy v1 supports version 1 only)")
    if det["determination_id"] != f"{POLICY_ID}:{cik}:{version}":
        errs.append("determination_id")
    b = det["binding"]
    if not (
        _keys(b, {"cik", "lei", "isin", "mic", "ticker"}, "binding", errs)
        and all(isinstance(b[k], str) for k in b)
        and b["cik"] == cik
        and _LEI.fullmatch(b["lei"])
        and _ISIN.fullmatch(b["isin"])
        and re.fullmatch(r"[A-Z0-9]{4}", b["mic"])
        and _text(b["ticker"])
    ):
        errs.append("binding")
    if not all(_is_date(det[k]) for k in ("valid_from", "reviewed_at", "review_by", "monitor_from")):
        return [*errs, "dates"]
    reviewed_at = date.fromisoformat(det["reviewed_at"])
    if not (det["valid_from"] <= det["reviewed_at"] and det["monitor_from"] == det["valid_from"]):
        errs.append("date order")
    if det["review_by"] != _review_by(reviewed_at).isoformat():
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
        if not (_ACCESSION.fullmatch(d["accession"]) and _sec_url_ok(d["url"], cik, d["accession"])):
            errs.append(f"document {d['doc_id']}: accession or url")
        if not (_is_date(d["filed"]) and d["filed"] <= det["reviewed_at"] and _text(d["form"]) and _text(d["role"])):
            errs.append(f"document {d['doc_id']}: form, role or date")
        if not (_SHA.fullmatch(d["content_sha256"]) or (allow_pending and d["content_sha256"] == "PENDING")):
            errs.append(f"document {d['doc_id']}: hash")
    prospectuses = [d for d in docs.values() if d["form"] == "485BPOS"]
    anchor = docs.get(det["valid_from_document"])
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
        if not (_SHA.fullmatch(a["content_sha256"]) or (allow_pending and a["content_sha256"] == "PENDING")):
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
        if q in HOLDING_DEPENDENT and ans != "unresolved":
            errs.append(f"{q}: holding-dependent findings must be unresolved under policy v1")
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


@dataclass
class Sources:
    texts: dict[str, str] = field(default_factory=dict)
    hashes: dict[str, str] = field(default_factory=dict)
    unavailable: list[str] = field(default_factory=list)
    changed: list[str] = field(default_factory=list)
    bad_authorities: list[str] = field(default_factory=list)
    unverified: list[str] = field(default_factory=list)


def _local_paths(docs: Path) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for line in (docs / "index.txt").read_text().splitlines():
        cols = [c.strip() for c in line.split("|")]
        if len(cols) < 2 or not cols[1].startswith("https://"):
            continue
        if not cols[0].endswith(".txt") or cols[0].endswith("_full_submission.txt"):
            out[cols[1]] = docs / cols[0]
    return out


def content_hash(url: str, body: bytes) -> tuple[str, str]:
    """(content SHA-256, quotable text): an XML document is bound by its bytes, anything else by ``document_text``."""
    if urlsplit(url).path.endswith(".xml"):
        return hashlib.sha256(body).hexdigest(), document_text(body)
    text = document_text(body)
    return hashlib.sha256(text.encode()).hexdigest(), text


def check_sources(det: dict[str, Any], docs: Path, authorities: Path) -> Sources:
    s = Sources()
    paths = _local_paths(docs)
    for d in det["documents"]:
        path = paths.get(d["url"])
        if path is None or not path.exists():
            err(f"document {d['doc_id']}: UNAVAILABLE ({d['url']})")
            s.unavailable.append(d["doc_id"])
            continue
        body = path.read_bytes()
        h, text = content_hash(d["url"], body)
        s.texts[d["doc_id"]] = text
        s.hashes[d["doc_id"]] = h
        if d["content_sha256"] not in ("PENDING", h):
            s.changed.append(d["doc_id"])
        state = "CHANGED" if d["doc_id"] in s.changed else "ok"
        err(f"document {d['doc_id']} {d['accession']}: bytes {len(body)}; content sha256 {h}; {state}")
    for a in det["authorities"]:
        path = authorities / f"{a['authority_id']}.txt"
        if not path.exists():
            s.bad_authorities.append(a["authority_id"])
            err(f"authority {a['authority_id']}: UNAVAILABLE")
            continue
        raw = path.read_bytes()
        h = s.hashes[a["authority_id"]] = hashlib.sha256(raw).hexdigest()
        text = raw.decode()
        s.texts[a["authority_id"]] = collapse(text)
        api = a["url"].replace("www.gov.uk/hmrc-internal-manuals", "www.gov.uk/api/content/hmrc-internal-manuals")
        if a["content_sha256"] not in ("PENDING", h) or text.splitlines()[0] not in (f"URL: {a['url']}", f"URL: {api}"):
            s.bad_authorities.append(a["authority_id"])
        err(f"authority {a['authority_id']}: sha256 {h}; {'BAD' if a['authority_id'] in s.bad_authorities else 'ok'}")
    quotes = [(f["question"], q) for f in det["findings"] for q in f["quotes"] + f["contrary"]]
    quotes += [(a["assumption_id"], q) for a in det["assumptions"] for q in a["evidence"]]
    for where, q in quotes:
        ok = q["source_id"] in s.texts and collapse(q["text"]) in s.texts[q["source_id"]]
        if not ok:
            s.unverified.append(f"{where}:{q['source_id']}")
        err(f"  {where} {'ok' if ok else 'MISSING'} [{q['source_id']} {q['locator']}] {q['text'][:70]}")
    return s


def offshore_fund(det: dict[str, Any]) -> tuple[str, str, list[str]]:
    """(answer, basis, assumption ids): ``true``/``false`` with ``established`` or ``assumed``, or ``unresolved``
    with basis ``unresolved`` (spec Design 1). An established decisive negative is preferred to an assumed one."""
    f = {x["question"]: x for x in det["findings"]}
    negatives = [q for q in QUESTIONS if q in OFFSHORE_INPUTS and f[q]["answer"] in OFFSHORE_INPUTS[q][1]]
    if negatives:
        established = [q for q in negatives if f[q]["basis"] == "established"]
        if established:
            return "false", "established", []
        return "false", "assumed", list(f[negatives[0]]["assumptions"])
    if any(f[q]["answer"] not in pos for q, (pos, _) in OFFSHORE_INPUTS.items()):
        return "unresolved", "unresolved", []
    basis = "assumed" if any(f[q]["basis"] == "assumed" for q in OFFSHORE_INPUTS) else "established"
    return "true", basis, sorted({a for q in OFFSHORE_INPUTS for a in f[q]["assumptions"]})


def submissions_coverage(det: dict[str, Any], body: Any, fetched: date | None, cutoff: date) -> str | None:
    """None if the filing list is a current, sound inventory for the CIK; otherwise the refusal."""
    if body is None or fetched is None:
        return "submissions unavailable"
    if fetched > cutoff or cutoff - fetched > SUBMISSIONS_CURRENT:
        return "submissions not current"
    try:
        recent = body["filings"]["recent"]
        cols = (recent["form"], recent["filingDate"], recent["accessionNumber"])
        files = body["filings"]["files"]
    except KeyError, TypeError:
        return "submissions unsound"
    if body.get("cik") != det["cik"] or not all(isinstance(c, list) for c in cols) or len(set(map(len, cols))) != 1:
        return "submissions unsound"
    if not isinstance(files, list):
        return "submissions unsound"
    seen: dict[str, tuple[str, str]] = {}
    for form, filed, acc in zip(*cols, strict=True):
        if not (_text(form) and _is_date(filed) and isinstance(acc, str) and _ACCESSION.fullmatch(acc)):
            return "submissions unsound"
        if acc in seen or filed > cutoff.isoformat():
            return "submissions unsound"
        seen[acc] = (form, filed)
    if files:
        return "submissions overflow files"
    for f in det["reviewed_filings"]:
        if f["filed"] >= det["monitor_from"] and seen.get(f["accession"]) != (f["form"], f["filed"]):
            return f"submissions disagree with reviewed filing {f['accession']}"
    return None


def applicability(
    det: dict[str, Any], src: Sources, obs: tuple[Any, ...], sub: tuple[Any, date | None], cutoff: date
) -> tuple[str, str | None]:
    """Design 2's ordered outcome for one fund observation (staleness is not latched here: the Script has no
    history of runs)."""
    _, _, _, symbol, outcome, cik, lei, isin, mic, *_rest = obs
    if outcome != "candidate":
        return "not_candidate", None
    if cik != det["cik"]:
        return "no_determination", None
    b = det["binding"]
    for name, mine, theirs in (("lei", lei, b["lei"]), ("isin", isin, b["isin"]), ("mic", mic, b["mic"])):
        if mine != theirs:
            return "binding_mismatch", name
    if symbol_key(symbol or "") != symbol_key(b["ticker"]):
        return "binding_mismatch", "ticker"
    if src.unavailable:
        return "documents_unavailable", src.unavailable[0]
    if src.changed:
        return "document_changed", src.changed[0]
    if src.unverified:
        return "quote_unverified", src.unverified[0]
    if not det["reviewed_at"] <= cutoff.isoformat() <= det["review_by"]:
        return "review_out_of_date", f"cutoff {cutoff} outside {det['reviewed_at']}..{det['review_by']}"
    body, fetched = sub
    refusal = submissions_coverage(det, body, fetched, cutoff)
    if refusal:
        return "coverage_unavailable", refusal
    recent = body["filings"]["recent"]
    reviewed = {f["accession"] for f in det["reviewed_filings"]}
    for form, filed, acc in zip(recent["form"], recent["filingDate"], recent["accessionNumber"], strict=True):
        if form in det["refresh_forms"] and filed >= det["monitor_from"] and acc not in reviewed:
            return "stale", f"{form} {acc} {filed}"
    return "determined", None


def reporting_status(
    det: dict[str, Any], search_outcome: str | None, d: date, cutoff: date
) -> tuple[str, str | None, list[str]]:
    """Design 3 from step 3 on, for a ``determined`` security: (status, reason, assumption ids)."""
    if not date.fromisoformat(det["valid_from"]) <= d <= cutoff:
        return "outside_interval", f"{d} outside {det['valid_from']}..{cutoff}", []
    answer, _, rests_on = offshore_fund(det)
    rests_on = sorted({*rests_on, "A6_law_current", "A7_identity_continuity", "A8_inventory_complete"})
    if answer == "false":
        return "not_offshore_fund", None, rests_on
    if answer == "unresolved":
        return "offshore_fund_unresolved", None, rests_on
    if search_outcome in ("listed", "unresolved"):
        return "reporting_status_unestablished", "list_match", rests_on
    if search_outcome != "not_found":
        return "reporting_status_unestablished", "search_unavailable", rests_on
    return "non_reporting_assumed", None, sorted({*rests_on, "A5_part3_not_applying"})


def gate(det: dict[str, Any], src: Sources, sub: tuple[Any, date | None], fund_observed_at: str | None) -> bool:
    """Apply Designs 2 and 3 with one cutoff; return True on a failed verification: no fund observations, or a
    candidate security of the CIK that is not ``determined``."""
    cutoff = datetime.now(UTC)
    print(f"cutoff {cutoff.isoformat()} (dates compared as {cutoff.date()}; local copies carry fetch dates only)")
    failed = False
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with conn.transaction():
            at_row = conn.execute(
                "SELECT COALESCE(%s::timestamptz, (SELECT max(observed_at) FROM tax_fund_observations))",
                (fund_observed_at,),
            ).fetchone()
            run_at = at_row[0] if at_row else None
            rows = conn.execute(
                """
                SELECT observation_id, tax_security_id, member_ids, symbol, outcome,
                       cik, lei, isin, mic, search_outcome, evidence_run_id, observed_at
                FROM tax_fund_observations WHERE observed_at = %s ORDER BY tax_security_id
                """,
                (run_at,),
            ).fetchall()
            print(f"fund run {run_at.isoformat() if run_at else None}: {len(rows)} fund securities observed")
            if not rows:
                return True
            for obs in rows:
                oid, sid, members, symbol, outcome, cik, *_, search, run, _at = obs
                print(f"\nsecurity {sid} {symbol} members {members}: fund observation {oid}")
                result, refusal = applicability(det, src, obs, sub, cutoff.date())
                print(f"  fund observation {outcome}; search {search}; applicability {result} {refusal or ''}")
                if result != "determined":
                    failed |= outcome == "candidate" and cik == det["cik"]
                    continue
                answer, basis, rests_on = offshore_fund(det)
                print(f"  offshore_fund: {answer} ({basis}; {rests_on})")
                lots = conn.execute(
                    "SELECT direction, uk_date FROM tax_evidence_lots WHERE run_id = %s AND instrument_id = ANY(%s)",
                    (run, list(members)),
                ).fetchall()
                disposals = sorted({u for dirn, u in lots if dirn == "disposal"})
                print(f"  lots in run {run}: {len(lots)}; disposal dates {[str(x) for x in disposals]}")
                for d in disposals:
                    status, reason, assumptions = reporting_status(det, search, d, cutoff.date())
                    print(f"  disposal {d}: {status} {reason or ''}; rests on {assumptions}")
    return failed


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    keys = [k for k, _ in pairs]
    if len(keys) != len(set(keys)):
        raise ValueError(f"duplicate JSON key in {keys}")
    return dict(pairs)


def _submissions(docs: Path, cik: str) -> tuple[Any, date | None, str | None]:
    """The saved filing list and its fetch date; (None, None, None) when missing or not JSON, which part 3 reports
    as ``coverage_unavailable``."""
    name = f"submissions_CIK{cik}.json"
    try:
        raw = (docs / name).read_bytes()
        body = json.loads(raw)
    except OSError, ValueError:
        return None, None, None
    fetched = None
    for line in (docs / "index.txt").read_text().splitlines():
        cols = [c.strip() for c in line.split("|")]
        if cols[0] == name and len(cols) > 4 and _is_date(cols[4].removeprefix("fetched ")):
            fetched = date.fromisoformat(cols[4].removeprefix("fetched "))
    return body, fetched, hashlib.sha256(raw).hexdigest()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--determination", type=Path, required=True)
    p.add_argument("--docs", type=Path, required=True)
    p.add_argument("--authorities", type=Path, required=True)
    p.add_argument("--fund-observed-at", help="the fund run to evaluate (default: the newest)")
    p.add_argument("--fill", action="store_true")
    a = p.parse_args()
    out = sys.stderr if a.fill else sys.stdout
    raw = a.determination.read_bytes()
    script = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    print(f"run at {datetime.now(UTC).isoformat()}; python {sys.version.split()[0]}; script sha256 {script}", file=out)
    print(f"module {__spec__.name if __spec__ else __file__}; argv {json.dumps(sys.argv[1:])}", file=out)
    print(f"determination file sha256 {hashlib.sha256(raw).hexdigest()}", file=out)
    try:
        det = json.loads(raw.decode("utf-8"), object_pairs_hook=_no_duplicates)
    except ValueError as exc:
        print(f"format errors: ['unreadable: {exc}']", file=out)
        sys.exit(1)
    errs = validate(det, allow_pending=a.fill)
    print(f"format errors: {errs}", file=out)
    if errs:
        sys.exit(1)
    print(f"determination {det['determination_id']}", file=out)
    src = check_sources(det, a.docs, a.authorities)
    failures = src.unavailable + src.changed + src.bad_authorities + src.unverified
    items = det["documents"] + det["authorities"]
    pending = [x.get("doc_id") or x["authority_id"] for x in items if x["content_sha256"] == "PENDING"]
    print(f"source failures: {failures}; pending hashes: {len(pending)}", file=out)
    if a.fill:
        if failures:
            sys.exit(1)
        for item in items:
            if item["content_sha256"] == "PENDING":
                item["content_sha256"] = src.hashes[item.get("doc_id") or item["authority_id"]]
        print(json.dumps(det, indent=2, ensure_ascii=False))
        return
    if src.bad_authorities:
        sys.exit(1)
    body, fetched, sub_sha = _submissions(a.docs, det["cik"])
    print(f"submissions sha256 {sub_sha} fetched {fetched}")
    refused = gate(det, src, (body, fetched), a.fund_observed_at)
    sys.exit(1 if failures or refused else 0)


if __name__ == "__main__":
    main()
