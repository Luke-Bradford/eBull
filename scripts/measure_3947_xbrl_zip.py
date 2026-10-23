"""#3947 Amendment 2 measurements: the scan's document set from ``{accession}-xbrl.zip``.

Read-only. Reproduces the ``[Z…]`` figures in Amendment 2 of
``docs/specs/fundamentals/2026-10-21-3947-dps-inline-scale.md``.

The population is the scan's own (``_POPULATION_SQL``: in-scope, dated accessions of
``sec_inline_scale_population``), split into disjoint arms: form 8-K (manifest), else scanned ``inline``, else
scanned ``not_inline``, else unscanned with the concept in raw (``unscanned_raw``), else ``history``. For a seeded
sample per arm it runs two paths, in alternating order, and compares them:

* **listing:** ``index.json``, ``FilingSummary.xml``, then every ``.htm``/``.html``/``.xhtml`` file of the listing
  except the rendered pages (v1's acquisition).
* **archive:** one GET of ``{accession}-xbrl.zip``, its HTML members read from memory, then the SGML header
  (``-index-headers.html``) for every member's ``<TYPE>``, then occurrence parsing: Amendment 2's attempt.

Every HTML file on both paths is classified by Amendment 2's B1 rule: the bytes go intact to the XML parser; a file
that parses is an Inline XBRL document when its root is an unprefixed ``html`` in the XHTML namespace and it holds
any element in the Inline XBRL namespace; a file that does not parse is ``undecidable`` when a lenient HTML parse
finds the Inline XBRL namespace in an attribute value, else not a member.

* **[Z1]** per accession, the two paths' Inline XBRL documents: the same names; the same occurrences of the concept
  from the v1 extraction (``parse_document_set``) over B1-classified, XML-decoded inputs (the gate); and, for
  information, raw-byte equality and equality after ``_normalised`` (the script ``www.sec.gov`` appends before
  ``</body>`` removed, CRLF read as LF, blanks before a line end or at the end removed). Files carrying that script
  are counted per path.
* **[Z2]** per path, requests and end-to-end seconds per accession (requests, the 0.15 s spacing, unzip,
  classification, the SGML header and occurrence parsing included); the script does not share the jobs process's
  SEC limiter.
* **[Z3]** Inline XBRL documents with no ``ix:header``, with their ``<TYPE>`` and an independent regex census of the
  concept's ``nonFraction`` elements in each (``_regex_census``: neither lxml nor the app parser).
* **[Z4]** accessions with two or more header-bearing documents, with each one's ``<TYPE>``.
* **[Z5]** archive contents: non-Inline HTML members, an ``.xml`` file other than the linkbases, and the largest
  unpacked size.

Run it outside a heavy SEC ingest window (e.g. while ``sec_dps_inline_scale_scan`` is not running). ``--out`` writes
one JSON line per accession.

    PYTHONPATH=. uv run python -m scripts.measure_3947_xbrl_zip [--per-arm 30] [--out FILE]
"""

from __future__ import annotations

import argparse
import collections
import contextlib
import hashlib
import html
import io
import json
import random
import re
import statistics
import time
import zipfile
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import IO

import httpx
import lxml.etree as ET
import psycopg

from app.config import settings
from app.services.dps_inline_scale_scan import _POPULATION_SQL, FILING_SUMMARY, PARSER_VERSION, SCOPE_START
from app.services.inline_xbrl_dps import (
    _XML_DECLARATION,
    CONCEPT,
    INLINE_SUFFIXES,
    IX_NS,
    XHTML_NS,
    Occurrence,
    _codec,
    _looks_inline,
    parse_document_set,
    rendered_report_files,
)

_LINKBASES = ("_cal.xml", "_def.xml", "_lab.xml", "_pre.xml")
# www.sec.gov's web front end appends this bot-detection script to served HTML; the archive holds the filer's bytes.
_INJECTED = re.compile(rb'<script type="text/javascript"\s+src="/[^"<>]*"></script>(?=\s*</body>\s*</html>\s*$)')
_TRAILING_BLANKS = re.compile(rb"[ \t]+\n")
# Not lxml and not the app parser: the [Z3] census is an independent oracle for the header-less members.
_CENSUS = re.compile(
    r'<([\w.-]+):nonFraction\b([^>]*\bname=["\']([\w.-]+):' + CONCEPT + r'["\'][^>]*)>(.*?)</\1:nonFraction>', re.S
)
_ATTR = re.compile(r'([\w:-]+)=(?:"([^"]*)"|\'([^\']*)\')')
# EFC Q2 2026: v1 stored its 4 occurrences; the census and the instance must find them too (the control).
_CONTROL = "0001628280-26-055148"
_XSI_NIL = "{http://www.w3.org/2001/XMLSchema-instance}nil"
_SGML_DOCUMENT = re.compile(r"<DOCUMENT>(.*?)</DOCUMENT>", re.S | re.I)
_SGML_TAG = {k: re.compile(rf"<{k}>([^\n<]*)", re.I) for k in ("TYPE", "FILENAME")}
_SGML_SEQUENCE = re.compile(r"<SEQUENCE>([^\n<]*)", re.I)

_ARMS_SQL = (
    _POPULATION_SQL
    + """
SELECT p.accession_number,
       COALESCE(m.cik, (SELECT ei.identifier_value FROM external_identifiers ei
                        WHERE ei.instrument_id = ANY (p.instrument_ids) AND ei.provider = 'sec'
                          AND ei.identifier_type = 'cik' AND ei.is_primary
                        ORDER BY ei.instrument_id LIMIT 1)),
       CASE WHEN m.form IN ('8-K', '8-K/A') THEN '8-K'
            WHEN s.parser_version = %(version)s THEN s.outcome
            WHEN p.in_raw THEN 'unscanned_raw'
            ELSE 'history' END
FROM pop p
LEFT JOIN sec_filing_manifest m USING (accession_number)
LEFT JOIN sec_inline_scale_scan s USING (accession_number)
ORDER BY 1
"""
)


@dataclass(frozen=True)
class _Doc:
    state: str  # 'member', 'non_member', 'undecidable'
    header: bool
    sha256: str  # of the bytes normalised by ``_normalised`` (informational: [Z1] gates on occurrences)
    injected: bool
    raw_sha256: str
    encoding: str | None = None  # the XML parser's detected encoding, for members


def _normalised(body: bytes) -> tuple[bytes, bool]:
    """The bytes without the front end's script, with LF line ends and no blanks before a line end or at the end."""
    stripped, injected = _INJECTED.subn(b"", body, count=1)
    return _TRAILING_BLANKS.sub(b"\n", stripped.replace(b"\r\n", b"\n")).rstrip(), bool(injected)


def _text(body: bytes, encoding: str | None) -> str:
    """Decoded for the v1 parser's ``str`` input by :func:`_codec`, with the encoding the XML parser reported in
    ``classify`` as the declaration (lxml reports UTF-8 for a UTF-16 file with a BOM and no declaration, which the BOM
    overrides). No second parse; a decode error raises, and the accession is reported failed."""
    return body.decode(_codec(body, encoding))


def _v1_root(text: str) -> ET._Element:
    """v1's parse of a decoded document: lxml refuses a ``str`` carrying an encoding declaration."""
    parser = ET.XMLParser(huge_tree=True, resolve_entities=False, no_network=True, recover=False)
    return ET.fromstring(_XML_DECLARATION.sub("", text, count=1), parser)


def classify(body: bytes) -> _Doc:
    normalised, injected = _normalised(body)
    digest, raw = hashlib.sha256(normalised).hexdigest(), hashlib.sha256(body).hexdigest()
    try:
        root = ET.fromstring(body, ET.XMLParser(huge_tree=True, resolve_entities=False, no_network=True))
    except ET.XMLSyntaxError, ValueError:
        return _Doc("undecidable" if _looks_inline(body) else "non_member", False, digest, injected, raw)
    member = (
        root.tag == f"{{{XHTML_NS}}}html" and root.prefix is None and next(root.iter(f"{{{IX_NS}}}*"), None) is not None
    )
    header = member and next(root.iter(f"{{{IX_NS}}}header"), None) is not None
    encoding = root.getroottree().docinfo.encoding
    return _Doc("member" if member else "non_member", header, digest, injected, raw, encoding)


@dataclass
class _Path:
    secs: float = 0.0
    requests: int = 0
    status: int = 200
    unpacked: int = 0
    other_members: list[str] = field(default_factory=list)
    docs: dict[str, _Doc] = field(default_factory=dict)
    bodies: dict[str, bytes] = field(default_factory=dict)  # Inline XBRL members only
    types: dict[str, list[str]] = field(default_factory=dict)
    primary: list[str] = field(default_factory=list)
    parsed: list[Occurrence] | None = None
    listing: list[str] = field(default_factory=list)

    def occurrences(self) -> list[Occurrence] | None:
        """v1's extraction over this path's Inline XBRL documents, in listing order: v1's set rule (only the documents
        holding an ``ix:header``), each parsed from its decoded text as v1 parsed it. ``None`` when none holds one."""
        roots = [
            (name, _v1_root(_text(body, self.docs[name].encoding)))
            for name, body in self.bodies.items()
            if self.docs[name].header
        ]
        return parse_document_set(roots) if roots else None

    def inline(self) -> dict[str, str]:
        return {name: d.sha256 for name, d in self.docs.items() if d.state == "member"}


class _Client:
    def __init__(self) -> None:
        self._http = httpx.Client(headers={"User-Agent": settings.sec_user_agent}, timeout=120, follow_redirects=True)

    def get(self, url: str, path: _Path) -> httpx.Response:
        started = time.monotonic()
        response = self._http.get(url)
        time.sleep(max(0.0, 0.15 - (time.monotonic() - started)))
        path.requests += 1
        return response


def _listing(client: _Client, base: str) -> _Path:
    path, started = _Path(), time.monotonic()
    response = client.get(f"{base}/index.json", path)
    response.raise_for_status()
    names = [item["name"] for item in response.json()["directory"]["item"]]
    rendered = frozenset[str]()
    path.listing = names
    if FILING_SUMMARY in names:
        summary = client.get(f"{base}/{FILING_SUMMARY}", path)
        if summary.status_code != 404:
            summary.raise_for_status()
            rendered = rendered_report_files(summary.text)
    for name in names:
        if name.lower().endswith(INLINE_SUFFIXES) and name not in rendered:
            body = client.get(f"{base}/{name}", path)
            body.raise_for_status()
            path.docs[name] = classify(body.content)
            if path.docs[name].state == "member":
                path.bodies[name] = body.content
    path.parsed = path.occurrences()
    path.secs = time.monotonic() - started
    return path


def _archive(client: _Client, base: str, accession: str) -> _Path:
    """Acquisition and classification as Amendment 2 proposes (the archive, then the SGML header typing every member),
    with the v1 extraction over the classified members, timed. Not the complete v2 attempt: v2's set rules and
    extraction are not built yet."""
    path, started = _Path(), time.monotonic()
    response = client.get(f"{base}/{accession}-xbrl.zip", path)
    path.status = response.status_code
    if response.status_code == 200:
        archive = zipfile.ZipFile(io.BytesIO(response.content))
        names = archive.namelist()
        if len(names) != len(set(names)):
            raise ValueError(f"duplicate archive member names in {accession}")
        path.unpacked = sum(info.file_size for info in archive.infolist())
        for info in archive.infolist():
            if info.filename.lower().endswith(INLINE_SUFFIXES):
                body = archive.read(info)
                path.docs[info.filename] = classify(body)
                if path.docs[info.filename].state == "member":
                    path.bodies[info.filename] = body
            else:
                path.other_members.append(info.filename)
        path.types, path.primary = _header_types(client, base, accession, path)
        unmapped = sorted(
            n
            for n, d in path.docs.items()
            if d.state == "member" and (len(path.types.get(n, [])) != 1 or not path.types[n][0])
        )
        if unmapped:
            raise ValueError(f"Inline XBRL members without exactly one SGML <TYPE> in {accession}: {unmapped}")
        path.parsed = path.occurrences()
    path.secs = time.monotonic() - started
    return path


def _header_types(client: _Client, base: str, accession: str, path: _Path) -> tuple[dict[str, list[str]], list[str]]:
    """``<FILENAME>`` → every ``<TYPE>`` the SGML header gives it (one entry per ``<TYPE>`` tag of each
    ``<DOCUMENT>``, empty ones included), and the sequence-1 (primary) filenames. B1.3's rule, exactly one non-empty
    type, is checked for Inline XBRL members only, by the caller."""
    response = client.get(f"{base}/{accession}-index-headers.html", path)
    response.raise_for_status()
    types: dict[str, list[str]] = collections.defaultdict(list)
    primary: list[str] = []
    for block in _SGML_DOCUMENT.findall(html.unescape(response.text)):
        name = m.group(1).strip() if (m := _SGML_TAG["FILENAME"].search(block)) else ""
        types[name].extend(t.strip() for t in _SGML_TAG["TYPE"].findall(block))
        if (m := _SGML_SEQUENCE.search(block)) and m.group(1).strip() == "1":
            primary.append(name)
    return types, primary


def _instance_facts(client: _Client, base: str, names: list[str], path: _Path) -> set[tuple[str, str, Decimal]]:
    """(contextRef, unitRef, value) of each non-nil fact of the concept in EDGAR's extracted instance, named after the
    primary document (``xyz.htm`` → ``xyz_htm.xml``, PDS §2.3.7): SEC's own reading of the whole document set. A
    value that is not a decimal raises, so the accession is reported failed rather than silently short."""
    if len(names) != 1 or not names[0].lower().endswith(".htm"):
        raise ValueError(f"expected one .htm primary document, found {names}")
    response = client.get(f"{base}/{names[0][: -len('.htm')]}_htm.xml", path)
    response.raise_for_status()
    root = ET.fromstring(response.content, ET.XMLParser(huge_tree=True, resolve_entities=False, no_network=True))
    return {
        (e.get("contextRef", ""), e.get("unitRef", ""), _decimal(e.text))
        for e in root.iter()
        if isinstance(e.tag, str) and e.tag.endswith("}" + CONCEPT) and e.get(_XSI_NIL) != "true" and e.text
    }


def _decimal(text: str) -> Decimal:
    try:
        return Decimal(text.strip())
    except InvalidOperation as exc:
        raise ValueError(f"instance value is not a decimal: {text!r}") from exc


def _occurrence_facts(occurrences: list[Occurrence] | None) -> set[tuple[str, str, Decimal]]:
    return {
        ((o.context_ref_raw or "").strip(), (o.unit_ref_raw or "").strip(), o.instance_value)
        for o in occurrences or []
        if o.instance_value is not None and o.unresolved is None
    }


def _regex_census(body: bytes) -> list[tuple[str | None, str | None, str | None, str]]:
    """Independent of lxml and of the app parser: (contextRef, unitRef, scale, display) of each DPS element."""
    text = body.decode("utf-8", "replace")
    found = []
    for match in _CENSUS.finditer(text):
        attrs = {k: dq or sq for k, dq, sq in _ATTR.findall(match.group(2))}
        display = re.sub(r"<[^>]+>", "", match.group(4)).strip()
        found.append((attrs.get("contextRef"), attrs.get("unitRef"), attrs.get("scale"), display))
    return found


def _measure(
    client: _Client,
    base: str,
    accession: str,
    archive_first: bool,
    stats: collections.Counter[str],
    timing: dict[str, list[float]],
    requests: dict[str, list[int]],
    notes: list[str],
) -> dict[str, object]:
    if archive_first:
        new = _archive(client, base, accession)
        old = _listing(client, base)
    else:
        old = _listing(client, base)
        new = _archive(client, base, accession)
    undecidable = False
    for name, p in (("listing", old), ("archive", new)):
        timing[name].append(p.secs)
        requests[name].append(p.requests)
        stats[f"{name}_undecidable"] += sum(d.state == "undecidable" for d in p.docs.values())
        undecidable |= any(d.state == "undecidable" for d in p.docs.values())
        members = [n for n, d in p.docs.items() if d.state == "member"]
        stats[f"{name}_inline_members_with_injected_script"] += sum(p.docs[n].injected for n in members)
        stats[f"{name}_inline_members_without_injected_script"] += sum(not p.docs[n].injected for n in members)
    record: dict[str, object] = {
        "accession": accession,
        "listing": {n: [d.state, d.header, d.raw_sha256, d.sha256] for n, d in old.docs.items()},
        "archive_status": new.status,
    }
    if new.status != 200:
        stats[f"archive_{new.status}"] += 1
        notes.append(f"archive {new.status} {accession}")
        return record
    record["archive"] = {n: [d.state, d.header, d.raw_sha256, d.sha256] for n, d in new.docs.items()}
    record["archive_other"] = new.other_members
    if undecidable:
        stats["Z1_excluded_undecidable"] += 1
        notes.append(f"Z1 excluded (undecidable candidate) {accession}")
        return record
    raw_old = {n: d.raw_sha256 for n, d in old.docs.items() if d.state == "member"}
    raw_new = {n: d.raw_sha256 for n, d in new.docs.items() if d.state == "member"}
    stats["Z1_names_differ"] += set(raw_old) != set(raw_new)
    if raw_new:  # bytes are compared only where there are Inline XBRL members
        stats["Z1_nonempty"] += 1
        stats["Z1_nonempty_raw_bytes_equal"] += raw_old == raw_new
        stats["Z1_nonempty_normalised_bytes_equal"] += old.inline() == new.inline()
        if old.inline() != new.inline():
            notes.append(f"Z1 normalised bytes differ {accession} {sorted(old.inline())}")
    # None (not inline) and [] (inline, no occurrence) are different outcomes. Element ordinals are per document,
    # so listing order versus archive order does not move them.
    same = (old.parsed is None) == (new.parsed is None) and sorted(map(repr, old.parsed or [])) == sorted(
        map(repr, new.parsed or [])
    )
    stats["Z1_occurrences_equal"] += same
    if not same:
        notes.append(f"Z1 occurrences differ {accession}")
    stats["occurrences"] += len(new.parsed or [])
    members = {n: d for n, d in new.docs.items() if d.state == "member"}
    headers = sorted(n for n, d in members.items() if d.header)
    stats["inline_accessions"] += bool(members)
    stats["inline_documents"] += len(members)
    stats["accessions_with_non_inline_html_member"] += len(new.docs) > len(members)
    stats["accessions_with_other_xml"] += any(
        m.endswith(".xml") and not m.endswith(_LINKBASES) for m in new.other_members
    )
    stats["unpacked_max"] = max(stats["unpacked_max"], new.unpacked)
    headerless = [n for n, doc in members.items() if not doc.header]
    for name in headerless:
        census = _regex_census(new.bodies[name])
        stats["Z3_headerless_documents"] += 1
        stats["Z3_headerless_documents_with_dps"] += bool(census)
        stats["Z3_headerless_dps_occurrences"] += len(census)
        notes.append(f"Z3 header-less {accession}/{name} type {(new.types.get(name) or ['?'])[0]} dps {len(census)}")
    if headerless or accession == _CONTROL:
        # SEC's extracted instance against v1's occurrences: what v1 leaves out of the set, as SEC reads it.
        # Measurement-only request, deliberately outside the timed attempt and its request count ([Z2]).
        instance = _instance_facts(client, base, new.primary, _Path())
        v1 = _occurrence_facts(new.parsed)
        stats["Z3_instance_facts"] += len(instance)
        stats["Z3_instance_facts_missing_from_v1"] += len(instance - v1)
        if accession == _CONTROL:
            census = sum(len(_regex_census(new.bodies[n])) for n in members)
            stored = len(new.parsed or [])
            notes.append(f"Z3 control {accession}: census {census}, v1 extraction {stored}, instance {len(instance)}")
        notes.append(
            f"Z3 instance {accession}: {len(instance)} facts, v1 has {len(instance & v1)},"
            f" missing {len(instance - v1)}, v1-only {len(v1 - instance)}"
        )
    if len(headers) >= 2:
        stats["Z4_multi_header_accessions"] += 1
        notes.append(f"Z4 multi-header {accession} {[(n, (new.types.get(n) or ['?'])[0]) for n in headers]}")
    stats["compared"] += 1
    return record


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--per-arm", type=int, default=30)
    parser.add_argument("--out")
    args = parser.parse_args()
    params = {"concept": CONCEPT, "scope_start": SCOPE_START, "version": PARSER_VERSION}
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(_ARMS_SQL, params).fetchall()
        stored = conn.execute(
            "SELECT count(*) FROM sec_inline_dps_occurrence WHERE accession_number = %s", (_CONTROL,)
        ).fetchone()
    print(f"[Z3] control {_CONTROL}: stored v1 occurrences before any fetch: {stored[0] if stored else 0}")
    arms: dict[str, list[tuple[str, str]]] = collections.defaultdict(list)
    for accession, cik, arm in rows:
        if cik is not None:
            arms[arm].append((accession, cik))
    sizes = dict(sorted((a, len(m)) for a, m in arms.items()))
    print(f"[Z0] {PARSER_VERSION} in-scope population by disjoint arm (with a CIK): {sizes}")
    with contextlib.ExitStack() as stack:
        out = stack.enter_context(open(args.out, "w")) if args.out else None
        _run(arms, args.per_arm, out)


def _run(arms: dict[str, list[tuple[str, str]]], per_arm: int, out: IO[str] | None) -> None:
    rng = random.Random(3947)
    client = _Client()
    unique = 0
    control = [(a, c) for rows_ in arms.values() for a, c in rows_ if a == _CONTROL]
    for arm in ("inline", "not_inline", "unscanned_raw", "history", "8-K", "control"):
        sample = control if arm == "control" else rng.sample(arms[arm], min(per_arm, len(arms[arm])))
        unique += len(sample) if arm != "control" else 0
        stats: collections.Counter[str] = collections.Counter()
        timing: dict[str, list[float]] = {"listing": [], "archive": []}
        requests: dict[str, list[int]] = {"listing": [], "archive": []}
        notes: list[str] = []
        for i, (accession, cik) in enumerate(sample):
            base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}"
            try:
                record = _measure(client, base, accession, bool(i % 2), stats, timing, requests, notes)
            except Exception as exc:  # noqa: BLE001 - every selected accession gets a terminal record
                stats["failed"] += 1
                notes.append(f"failed {accession} {exc!r}")
                record = {"accession": accession, "failed": repr(exc)}
            if out:
                out.write(json.dumps({"arm": arm, **record}) + "\n")
        print(f"[Z] arm {arm}: sample {len(sample)}, {dict(sorted(stats.items()))}")
        for name in ("listing", "archive"):
            if timing[name]:
                print(
                    f"    [Z2] {name}: requests/accession mean {statistics.mean(requests[name]):.2f};"
                    f" seconds/accession mean {statistics.mean(timing[name]):.3f}"
                    f" median {statistics.median(timing[name]):.3f}"
                )
        for line in notes:
            print("    ", line)
    print(f"[Z0] accessions sampled (arms are disjoint): {unique}")


if __name__ == "__main__":
    main()
