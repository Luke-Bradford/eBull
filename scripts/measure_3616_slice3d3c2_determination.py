"""#3616 slice 3d-3c-2 fact-finding: check a draft fund determination against its sources and the stored evidence.

Read-only. Fetches nothing: it reads saved copies. Three parts, each printing every input and outcome; the exit status
is non-zero if any check fails.

1. **Format** (spec Design 1): ``validate`` checks the draft's keys, grammars, enums and cross-field invariants.
2. **Sources.** For each cited SEC document, the copy saved under ``--docs`` (``index.txt`` maps path to URL) is
   reduced by ``document_text`` and hashed; each quote, white space collapsed, must occur in it. Each authority is
   read from ``--authorities/<authority_id>.txt`` and hashed. ``--fill`` prints the draft with every ``PENDING``
   hash replaced, and changes nothing else.
3. **Applicability and status on the dev DB** (spec Designs 2 and 3), in one REPEATABLE READ snapshot: the newest
   ``tax_fund_observations`` row of each fund security, the ordered outcome of Design 2 (with the CIK's filing list
   read from the saved ``submissions_CIK<cik>.json`` instead of a stored fetch), and, for a ``determined`` security,
   Design 3's status at each disposal date of its lots in the observation's evidence run.

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
from datetime import UTC, date
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.services.def14a_recipients import symbol_key

POLICY_ID = "fund_determination_v1"
GOVERNING_FORMS = frozenset(
    {"485APOS", "485BPOS", "485BXT", "497", "497J", "497K", "N-CEN", "N-CEN/A", "N-8F", "N-8F/A"}
)
QUESTIONS: dict[str, frozenset[str]] = {
    "arrangement": frozenset({"single_arrangement_single_class", "unresolved"}),
    "s355_limb": frozenset({"a", "b", "c", "none", "unresolved"}),
    "s356_condition_a": frozenset({"met", "not_met", "unresolved"}),
    "s356_condition_b": frozenset({"met", "not_met", "unresolved"}),
    "s356_condition_c": frozenset({"met", "not_met", "unresolved"}),
    "s357_condition_d": frozenset({"met", "not_met", "unresolved"}),
    "s359_exclusion": frozenset({"none_found", "applies", "unresolved"}),
    "transparent_fund": frozenset({"transparent", "opaque", "unresolved"}),
    "reg29_sufficient_information": frozenset({"made_available", "not_made_available", "unresolved"}),
    "reg29_nonreporting_holdings": frozenset({"within_limit", "exceeded", "unresolved"}),
}
BASES = ("established", "assumed", "unresolved")
#: Tags whose boundary separates words; any other tag (``b``, ``i``, ``span``, ``font``, ``a`` ...) joins its text.
BLOCK_TAGS = frozenset(
    {
        "p",
        "div",
        "br",
        "li",
        "ul",
        "ol",
        "tr",
        "td",
        "th",
        "table",
        "thead",
        "tbody",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "blockquote",
        "dt",
        "dd",
        "dl",
        "section",
        "article",
        "center",
        "hr",
        "title",
        "pre",
        "document",
        "text",
        "type",
        "sequence",
        "filename",
        "description",
        "html",
        "body",
        "head",
    }
)
_CIK = re.compile(r"[0-9]{10}")
_ACCESSION = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")
_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_SHA = re.compile(r"[0-9a-f]{64}")
_LEI = re.compile(r"[A-Z0-9]{18}[0-9]{2}")
_ISIN = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]")


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
    """Strict UTF-8; tags dropped (block tags become a space); script/style content dropped; character references
    decoded once by the parser; white space collapsed."""
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


def validate(det: dict[str, Any]) -> list[str]:
    errs: list[str] = []
    keys = {
        "determination_id",
        "policy_id",
        "cik",
        "version",
        "binding",
        "valid_from",
        "reviewed_at",
        "refresh_forms",
        "reviewed_filings",
        "documents",
        "authorities",
        "assumptions",
        "findings",
    }
    if set(det) != keys:
        errs.append(f"keys {sorted(set(det) ^ keys)}")
        return errs
    cik, version = det["cik"], det["version"]
    if det["policy_id"] != POLICY_ID or not (isinstance(cik, str) and _CIK.fullmatch(cik)):
        errs.append("policy or cik")
    if not (isinstance(version, int) and not isinstance(version, bool) and version >= 1):
        errs.append("version")
    if det["determination_id"] != f"{POLICY_ID}:{cik}:{version}":
        errs.append("determination_id")
    b = det["binding"]
    if not (
        isinstance(b, dict)
        and set(b) == {"cik", "lei", "isin", "mic", "ticker"}
        and b["cik"] == cik
        and _LEI.fullmatch(b["lei"])
        and _ISIN.fullmatch(b["isin"])
        and re.fullmatch(r"[A-Z0-9]{4}", b["mic"])
        and isinstance(b["ticker"], str)
        and b["ticker"].strip()
    ):
        errs.append("binding")
    if not (_is_date(det["valid_from"]) and _is_date(det["reviewed_at"]) and det["valid_from"] <= det["reviewed_at"]):
        errs.append("dates")
    if not GOVERNING_FORMS <= set(det["refresh_forms"]):
        errs.append("refresh_forms must include every governing form")
    docs = {d["doc_id"]: d for d in det["documents"]}
    if len(docs) != len(det["documents"]):
        errs.append("duplicate doc_id")
    prefix = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
    for d in det["documents"]:
        if set(d) != {"doc_id", "url", "accession", "form", "filed", "role", "text_sha256"}:
            errs.append(f"document keys {d.get('doc_id')}")
            continue
        ok_url = (
            d["url"].startswith(prefix + d["accession"].replace("-", "") + "/")
            and "/" not in d["url"][len(prefix) + 19 :]
        )
        if not (_ACCESSION.fullmatch(d["accession"]) and ok_url and _is_date(d["filed"])):
            errs.append(f"document {d['doc_id']}: accession, url or date")
        if d["text_sha256"] != "PENDING" and not _SHA.fullmatch(d["text_sha256"]):
            errs.append(f"document {d['doc_id']}: hash")
    accessions = [f["accession"] for f in det["reviewed_filings"]]
    if len(set(accessions)) != len(accessions):
        errs.append("duplicate reviewed filing")
    for f in det["reviewed_filings"]:
        if f["form"] not in GOVERNING_FORMS or not _is_date(f["filed"]) or not f["documents"]:
            errs.append(f"reviewed filing {f['accession']}")
        for doc_id in f["documents"]:
            if doc_id not in docs or docs[doc_id]["accession"] != f["accession"]:
                errs.append(f"reviewed filing {f['accession']}: document {doc_id}")
    if max(f["filed"] for f in det["reviewed_filings"]) > det["reviewed_at"]:
        errs.append("reviewed filing after reviewed_at")
    auth = {a["authority_id"] for a in det["authorities"]}
    assumptions = {a["assumption_id"]: a for a in det["assumptions"]}
    for a in det["assumptions"]:
        if not (a["statement"].strip() and a["residual"].strip()):
            errs.append(f"assumption {a['assumption_id']}")
    questions = [f["question"] for f in det["findings"]]
    if sorted(questions) != sorted(QUESTIONS):
        errs.append(f"questions {sorted(set(questions) ^ set(QUESTIONS))}")
    for f in det["findings"]:
        q, ans, basis = f["question"], f["answer"], f["basis"]
        if ans not in QUESTIONS.get(q, ()) or basis not in BASES:
            errs.append(f"{q}: answer or basis")
        if (ans == "unresolved") != (basis == "unresolved"):
            errs.append(f"{q}: unresolved answer needs unresolved basis")
        if basis == "assumed" and not f["assumptions"]:
            errs.append(f"{q}: assumed without an assumption")
        if basis == "established" and f["assumptions"]:
            errs.append(f"{q}: established with assumptions")
        if basis == "unresolved" and not (f["would_resolve"] or "").strip():
            errs.append(f"{q}: unresolved without would_resolve")
        if ans != "unresolved" and not f["quotes"] and q != "s359_exclusion":
            errs.append(f"{q}: no quote")
        if not set(f["assumptions"]) <= set(assumptions) or not set(f["authorities"]) <= auth or not f["authorities"]:
            errs.append(f"{q}: unknown assumption or authority")
        for quote in f["quotes"] + f["contrary"]:
            if quote["doc_id"] not in docs or not collapse(quote["text"]) or not quote["locator"].strip():
                errs.append(f"{q}: quote reference")
    return errs


def _local_paths(docs: Path) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for line in (docs / "index.txt").read_text().splitlines():
        cols = [c.strip() for c in line.split("|")]
        if len(cols) >= 2 and cols[1].startswith("https://") and not cols[0].endswith(".txt"):
            out[cols[1]] = docs / cols[0]
    return out


def check_sources(det: dict[str, Any], docs: Path, authorities: Path) -> tuple[int, dict[str, str]]:
    failures = 0
    paths = _local_paths(docs)
    texts: dict[str, str] = {}
    hashes: dict[str, str] = {}
    for d in det["documents"]:
        path = paths.get(d["url"])
        if path is None:
            print(f"document {d['doc_id']}: UNAVAILABLE ({d['url']})")
            failures += 1
            continue
        body = path.read_bytes()
        text = document_text(body)
        texts[d["doc_id"]] = text
        h = hashes[d["doc_id"]] = hashlib.sha256(text.encode()).hexdigest()
        match = d["text_sha256"] in ("PENDING", h)
        failures += not match
        print(
            f"document {d['doc_id']} {d['accession']} {d['form']}: body sha256 {hashlib.sha256(body).hexdigest()} "
            f"bytes {len(body)}; text sha256 {h} chars {len(text)}; hash {'ok' if match else 'MISMATCH'}"
        )
    for a in det["authorities"]:
        path = authorities / f"{a['authority_id']}.txt"
        if not path.exists():
            print(f"authority {a['authority_id']}: UNAVAILABLE")
            failures += 1
            continue
        raw = path.read_bytes()
        h = hashes[a["authority_id"]] = hashlib.sha256(raw).hexdigest()
        url_line = raw.decode().splitlines()[0]
        match = (
            a["text_sha256"] in ("PENDING", h)
            and url_line == f"URL: {a['url'].replace('/hmrc-internal-manuals', '/api/content/hmrc-internal-manuals')}"
        )
        failures += not match
        print(f"authority {a['authority_id']}: sha256 {h} bytes {len(raw)}; {'ok' if match else 'MISMATCH'}")
    for f in det["findings"]:
        print(f"finding {f['question']} = {f['answer']} ({f['basis']}; assumptions {f['assumptions']})")
        for kind, quotes in (("quote", f["quotes"]), ("contrary", f["contrary"])):
            for q in quotes:
                ok = q["doc_id"] in texts and collapse(q["text"]) in texts[q["doc_id"]]
                failures += not ok
                print(f"  {kind} {'ok' if ok else 'MISSING'} [{q['doc_id']} {q['locator']}] {q['text'][:80]}")
    for a in det["assumptions"]:
        for q in a["evidence"]:
            ok = q["doc_id"] in texts and collapse(q["text"]) in texts[q["doc_id"]]
            failures += not ok
            print(f"assumption {a['assumption_id']} evidence {'ok' if ok else 'MISSING'} [{q['doc_id']}]")
    print(f"source failures: {failures}")
    return failures, hashes


def offshore_fund(det: dict[str, Any]) -> str:
    """``established``, ``assumed``, ``unresolved`` or ``false`` (spec Design 1)."""
    f = {x["question"]: x for x in det["findings"]}
    needed = {
        "s355_limb": ("a", "b", "c"),
        "s356_condition_a": ("met",),
        "s356_condition_b": ("met",),
        "s356_condition_c": ("met",),
        "s357_condition_d": ("not_met",),
        "arrangement": ("single_arrangement_single_class",),
        "s359_exclusion": ("none_found",),
    }
    if (
        f["s355_limb"]["answer"] == "none"
        or any(f[q]["answer"] == "not_met" for q in ("s356_condition_a", "s356_condition_b", "s356_condition_c"))
        or f["s359_exclusion"]["answer"] == "applies"
    ):
        return "false"
    if any(f[q]["answer"] not in ok for q, ok in needed.items()):
        return "unresolved"
    return "assumed" if any(f[q]["basis"] == "assumed" for q in needed) else "established"


def applicability(det: dict[str, Any], obs: tuple[Any, ...], submissions: dict) -> tuple[str, str | None]:
    """Design 2's ordered outcome for one fund observation (the stored-document rows are the Script's saved copies,
    checked by ``check_sources``)."""
    (_, _, members, symbol, outcome, cik, lei, isin, mic, *_rest) = obs
    if outcome != "candidate":
        return "not_candidate", None
    if cik != det["cik"]:
        return "no_determination", None
    b = det["binding"]
    for field, mine, theirs in (
        ("cik", cik, b["cik"]),
        ("lei", lei, b["lei"]),
        ("isin", isin, b["isin"]),
        ("mic", mic, b["mic"]),
    ):
        if mine != theirs:
            return "binding_mismatch", field
    if symbol_key(symbol or "") != symbol_key(b["ticker"]):
        return "binding_mismatch", "ticker"
    if submissions["filings"].get("files"):
        return "coverage_unavailable", "submissions overflow files not read"
    recent = submissions["filings"]["recent"]
    earliest = min(f["filed"] for f in det["reviewed_filings"])
    reviewed = {f["accession"] for f in det["reviewed_filings"]}
    for form, filed, acc in zip(recent["form"], recent["filingDate"], recent["accessionNumber"], strict=True):
        if form in det["refresh_forms"] and filed >= earliest and acc not in reviewed:
            return "stale", f"{form} {acc} {filed}"
    return "determined", None


def reporting_status(det: dict[str, Any], search_outcome: str | None, d: date) -> tuple[str, list[str]]:
    """Design 3, for a ``determined`` security."""
    if d < date.fromisoformat(det["valid_from"]):
        return "before_valid_from", []
    off = offshore_fund(det)
    if off not in ("established", "assumed"):
        return f"offshore_fund_{off}", []
    if search_outcome in ("listed", "unresolved"):
        return "reporting_status_unestablished", [f"list_match:{search_outcome}"]
    if search_outcome != "not_found":
        return "reporting_status_unestablished", [f"search:{search_outcome}"]
    used = sorted({a for x in det["findings"] if x["basis"] == "assumed" for a in x["assumptions"]})
    return "non_reporting_assumed", [*used, "A5_no_acceptance"]


def gate(det: dict[str, Any], submissions: dict) -> None:
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with conn.transaction():
            rows = conn.execute(
                """
                SELECT DISTINCT ON (tax_security_id) observation_id, tax_security_id, member_ids, symbol, outcome,
                       cik, lei, isin, mic, search_outcome, evidence_run_id, observed_at
                FROM tax_fund_observations
                ORDER BY tax_security_id, observed_at DESC, observation_id DESC
                """
            ).fetchall()
            print(f"\noffshore_fund: {offshore_fund(det)}")
            for obs in rows:
                oid, sid, members, symbol, outcome, *_, search, run, at = obs
                stamp = at.astimezone(UTC).isoformat()
                print(f"\nsecurity {sid} {symbol} members {members}: observation {oid} at {stamp}")
                result, refusal = applicability(det, obs, submissions)
                print(f"  fund observation {outcome}; search {search}; applicability {result} {refusal or ''}")
                if result != "determined":
                    continue
                lots = conn.execute(
                    "SELECT direction, uk_date FROM tax_evidence_lots WHERE run_id = %s AND instrument_id = ANY(%s)",
                    (run, list(members)),
                ).fetchall()
                disposals = sorted({u for dirn, u in lots if dirn == "disposal"})
                print(f"  lots in run {run}: {len(lots)}; disposal dates {[str(x) for x in disposals]}")
                for d in disposals:
                    status, rests_on = reporting_status(det, search, d)
                    print(f"  disposal {d}: {status}; rests on {rests_on}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--determination", type=Path, required=True)
    p.add_argument("--docs", type=Path, required=True)
    p.add_argument("--authorities", type=Path, required=True)
    p.add_argument("--fill", action="store_true", help="print the draft with PENDING hashes filled, then exit")
    a = p.parse_args()
    raw = a.determination.read_text()
    det = json.loads(raw, object_pairs_hook=_no_duplicates)
    if a.fill:
        _, hashes = check_sources(det, a.docs, a.authorities)
        for item in det["documents"] + det["authorities"]:
            key = item.get("doc_id") or item["authority_id"]
            if item["text_sha256"] == "PENDING":
                item["text_sha256"] = hashes[key]
        print(json.dumps(det, indent=2, ensure_ascii=False))
        return
    print(f"determination {det['determination_id']} file sha256 {hashlib.sha256(raw.encode()).hexdigest()}")
    errs = validate(det)
    print(f"format errors: {errs}")
    failures, _ = check_sources(det, a.docs, a.authorities)
    pending = sum(x["text_sha256"] == "PENDING" for x in det["documents"] + det["authorities"])
    print(f"pending hashes: {pending}")
    submissions = json.loads((a.docs / f"submissions_CIK{det['cik']}.json").read_text())
    gate(det, submissions)
    sys.exit(1 if errs or failures or pending else 0)


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    keys = [k for k, _ in pairs]
    if len(keys) != len(set(keys)):
        raise ValueError(f"duplicate JSON key in {keys}")
    return dict(pairs)


if __name__ == "__main__":
    main()
