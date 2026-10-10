"""#3739 slice 3b: the §3.3 deterministic checker and the §3.4 replica's comparison set.

Spec: ``docs/research/2026-10-09-3739-stage-c-panel.md`` §3.1, §3.3, §3.4.

``comparison`` pins every Intrader ``split_factor`` stamp (≠ 1) dated in the replica's event window, each with the
#3361 linkage's answer as of its stamp date and whether that CIK is in the replica's U. §3.4's comparison set is the
rows with ``in_u``. It is pinned before any replica adjudication, so the comparison cannot shape the protocol.

``check`` runs §3.3's checker over an event-file record CSV. Every evidence item's document and the filing's
index headers are fetched by accession (or read from the mirror when already there) and mirrored gzipped
(``mtime`` 0), the sha256 of the bytes as served is recorded, and the quote must occur in the document's text
(whitespace-normalised). Then the per-type field requirements are checked on the row's quotes. The checker proves the
quotes exist and state the row's fields; the adjudication log records why they describe one completed action.

    PYTHONPATH=. uv run python scripts/build_3739_slice3b.py comparison --replica 2022 \\
        --u replica-2022/u-pass1.csv --u-sha256 <d> --out replica-2022/comparison-intrader-stamps.csv
    PYTHONPATH=. uv run python scripts/build_3739_slice3b.py check --type split --records <records.csv> \\
        --mirror <dir> --out <check.jsonl>
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import gzip
import hashlib
import html
import io
import json
import re
import unicodedata
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from fractions import Fraction
from pathlib import Path
from typing import Final

import httpx
import psycopg

from app.config import settings
from app.services import market_calendar
from app.services.sec_pipelined_fetcher import FetchTask, PipelinedSecFetcher
from app.services.security_linkage import load_security_linkage
from scripts.build_3609_factor_panel import LINKAGE
from scripts.build_3739_slice2 import checked, sha256_of
from scripts.build_3739_slice2b import csv_bytes, publish
from scripts.build_3739_slice3a import REPLICAS, Replica

# --------------------------------------------------------------------------- §3.4 comparison set

COMPARISON_COLUMNS: Final = (
    "series_id",
    "vendor_symbol",
    "bar_date",
    "split_factor",
    "link_reason",
    "cik",
    "basis",
    "in_u",
)

_STAMPS_SQL: Final = """
SELECT d.series_id, s.vendor_symbol, d.bar_date, d.split_factor
FROM research_price_daily d
JOIN research_price_series s USING (series_id)
WHERE s.vendor = %(vendor)s AND d.split_factor <> 1 AND d.bar_date BETWEEN %(start)s AND %(through)s
ORDER BY d.series_id, d.bar_date
"""


def read_u(path: Path) -> frozenset[str]:
    with path.open(newline="") as handle:
        return frozenset(row["cik"] for row in csv.DictReader(handle))


def comparison(replica: Replica, u_path: Path, out: Path) -> None:
    u = read_u(u_path)
    linkage = load_security_linkage(LINKAGE[0], expected_manifest_sha256=LINKAGE[1])
    if linkage.supported_through < replica.through:
        raise ValueError(f"the linkage is supported through {linkage.supported_through}, before {replica.through}")
    with psycopg.connect(settings.database_url) as conn:
        stamps = conn.execute(
            _STAMPS_SQL,
            {"vendor": "icyDenev/Intrader", "start": replica.event_start, "through": replica.through},
        ).fetchall()
    rows: list[list[str]] = []
    ledger: Counter[str] = Counter()
    for series_id, symbol, bar_date, factor in stamps:
        link = linkage.link_as_of(int(series_id), bar_date)
        in_u = link.cik is not None and link.cik in u
        ledger[f"{link.label}{':in_u' if in_u else ''}"] += 1
        rows.append(
            [
                str(series_id),
                symbol,
                bar_date.isoformat(),
                str(factor),
                link.label,
                link.cik or "",
                link.basis or "",
                "1" if in_u else "0",
            ]  # fmt: skip
        )
    publish(out, csv_bytes(COMPARISON_COLUMNS, rows))
    in_u_rows = [r for r in rows if r[-1] == "1"]
    print(
        json.dumps(
            {
                "window": [replica.event_start.isoformat(), replica.through.isoformat()],
                "linkage_manifest_sha256": LINKAGE[1],
                "stamps": len(rows),
                "series": len({r[0] for r in rows}),
                "comparison_set": len(in_u_rows),
                "comparison_issuers": len({r[5] for r in in_u_rows}),
                "comparison_reverse": sum(1 for r in in_u_rows if Fraction(r[3]) < 1),
                "ledger": dict(sorted(ledger.items())),
                "sha256": sha256_of(out),
            },
            indent=1,
        )
    )


# --------------------------------------------------------------------------- §3.3 checker: text

#: Tag stripping is by construction (no source rule fixes how an EDGAR HTML document becomes text): comments,
#: scripts and styles are dropped, every tag becomes a space, entities are unescaped, the text is NFKC-normalised
#: (a no-break space becomes a space) and whitespace runs collapse to one space. A quote matches when it occurs
#: with ALL whitespace removed from both sides, so a tag boundary inside a word or number cannot fail a true quote.
_DROPPED: Final = re.compile(r"<!--.*?-->|<(script|style)\b.*?</\1\s*>", re.IGNORECASE | re.DOTALL)
_TAG: Final = re.compile(r"<[^>]*>")
_SPACE: Final = re.compile(r"\s+")
_HTML_NAME: Final = re.compile(r"\.(htm|html|xml)$", re.IGNORECASE)
MAX_QUOTE: Final = 300


def normalise(text: str) -> str:
    return _SPACE.sub(" ", unicodedata.normalize("NFKC", text)).strip()


def compact(text: str) -> str:
    return _SPACE.sub("", unicodedata.normalize("NFKC", text))


def document_text(raw: bytes, document: str) -> str:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("cp1252", errors="replace")  # EDGAR's legacy documents
    if _HTML_NAME.search(document) or "<html" in text[:4096].lower():
        text = html.unescape(_TAG.sub(" ", _DROPPED.sub(" ", text)))
    return normalise(text)


# --------------------------------------------------------------------------- §3.3 checker: stated values

_UNITS: Final = {
    w: i
    for i, w in enumerate(
        "zero one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen "
        "seventeen eighteen nineteen".split()
    )
}
_TENS: Final = {w: 10 * i for i, w in enumerate("twenty thirty forty fifty sixty seventy eighty ninety".split(), 2)}
_SCALES: Final = {"hundred": 100, "thousand": 1000}
_WORD: Final = "|".join(sorted([*_UNITS, *_TENS, *_SCALES], key=len, reverse=True))
_NUMERAL: Final = r"\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?"
_NUMBER: Final = rf"(?:{_NUMERAL}|(?:{_WORD})(?:[\s-]+(?:and[\s-]+)?(?:{_WORD}))*)(?:\s*\(\s*(?:{_NUMERAL})\s*\))?"
_DASH: Final = r"[-‐‑‒–—]"
#: "three-for-one", "1-for-10", "ten (10) for one (1)": a new shares for b old (§3.1's ratio = a / b).
_FOR_RATIO: Final = re.compile(
    rf"(?<![\w.])(?P<a>{_NUMBER})\s*{_DASH}?\s*for\s*{_DASH}?\s*(?P<b>{_NUMBER})(?!\w)", re.IGNORECASE
)
#: "10:1", "1:20": numerals only, a new shares for b old.
_COLON_RATIO: Final = re.compile(rf"(?<![\w.:])(?P<a>{_NUMERAL})\s*:\s*(?P<b>{_NUMERAL})(?![\w:])")
_MONTHS: Final = {
    name: number
    for number, names in enumerate(
        (
            ("january", "jan"),
            ("february", "feb"),
            ("march", "mar"),
            ("april", "apr"),
            ("may",),
            ("june", "jun"),
            ("july", "jul"),
            ("august", "aug"),
            ("september", "sept", "sep"),
            ("october", "oct"),
            ("november", "nov"),
            ("december", "dec"),
        ),
        1,
    )
    for name in names
}
_MONTH: Final = "|".join(sorted(_MONTHS, key=len, reverse=True))
_ORDINAL: Final = r"(?:st|nd|rd|th)?"
_DATES: Final = (
    re.compile(rf"\b(?P<m>{_MONTH})\.?\s+(?P<d>\d{{1,2}}){_ORDINAL}\s*,?\s*(?P<y>\d{{4}})\b", re.IGNORECASE),
    re.compile(rf"\b(?P<d>\d{{1,2}}){_ORDINAL}\s+(?:of\s+)?(?P<m>{_MONTH})\.?\s*,?\s*(?P<y>\d{{4}})\b", re.IGNORECASE),
    re.compile(r"\b(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})\b"),
    re.compile(r"\b(?P<m>\d{1,2})/(?P<d>\d{1,2})/(?P<y>\d{4})\b"),  # EDGAR filers are US: month first
)


def number_value(text: str) -> Fraction | None:
    """A numeral, number words up to the thousands, or words with a numeral in brackets (which must agree)."""
    head, _, bracket = text.partition("(")
    head = head.strip()
    if re.fullmatch(_NUMERAL, head):
        value: Fraction | None = Fraction(head.replace(",", ""))
    else:
        total = current = 0
        for word in re.split(r"[\s-]+", head.lower()):
            if word == "and":
                continue
            if word in _UNITS:
                current += _UNITS[word]
            elif word in _TENS:
                current += _TENS[word]
            elif word == "hundred":
                current = max(current, 1) * 100
            elif word == "thousand":
                total, current = total + max(current, 1) * 1000, 0
            else:
                return None
        value = Fraction(total + current)
    if bracket:
        stated = Fraction(bracket.rstrip(") ").strip().replace(",", ""))
        if value != stated:
            return None
    return value


def stated_ratios(text: str) -> set[Fraction]:
    out: set[Fraction] = set()
    for pattern in (_FOR_RATIO, _COLON_RATIO):
        for match in pattern.finditer(text):
            a, b = number_value(match["a"]), number_value(match["b"])
            if a and b:
                out.add(a / b)
    return out


def dated_spans(text: str) -> list[tuple[date, int, int]]:
    out: list[tuple[date, int, int]] = []
    for pattern in _DATES:
        for match in pattern.finditer(text):
            month = match["m"]
            number = _MONTHS[month.lower()] if month.isalpha() else int(month)
            try:
                out.append((date(int(match["y"]), number, int(match["d"])), match.start(), match.end()))
            except ValueError:
                continue
    return out


def stated_dates(text: str) -> set[date]:
    return {value for value, _, _ in dated_spans(text)}


#: How close (characters, either side) a date must sit to the words that give it its role. By construction, and
#: deliberately loose: in "record on June 6, 2024, payable on June 7, 2024" both dates are near both words, so a
#: swapped pair passes this test and is then caught only if the FINRA date it derives differs. The adjudication log
#: carries the rest.
ROLE_REACH: Final = 40


def dated_near(text: str, value: date, words: re.Pattern[str]) -> bool:
    """``value`` is stated within ``ROLE_REACH`` characters of a match of ``words``."""
    spans = [(start, end) for v, start, end in dated_spans(text) if v == value]
    return any(
        start - ROLE_REACH <= w.end() and w.start() <= end + ROLE_REACH
        for w in words.finditer(text)
        for start, end in spans
    )


#: §1's split effective date is "the first session ... the class trades on the adjusted basis"; FINRA's term for
#: the session is the ex-date. The words a quote must carry beside that date (by construction).
EFFECTIVE_WORDS: Final = re.compile(
    r"\b(split[- ]adjusted|post[- ](reverse[- ])?split|adjusted) basis|\bex[- ](dividend|distribution|date|split)\b",
    re.I,
)
RECORD_WORDS: Final = re.compile(r"\brecord\b", re.I)
PAYABLE_WORDS: Final = re.compile(r"\b(payable|paid|distributed|distribution date|issued)\b", re.I)


def names_symbol(text: str, symbol: str) -> bool:
    return bool(symbol) and re.search(rf"(?<![A-Za-z0-9]){re.escape(symbol)}(?![A-Za-z0-9])", text) is not None


_IX_FACT: Final = re.compile(r"<ix:nonNumeric\b([^>]*)>(.*?)</ix:nonNumeric\s*>", re.I | re.S)
_IX_NAME: Final = re.compile(r"\bname\s*=\s*[\"'][^\"':]*:(Security12bTitle|TradingSymbol)[\"']", re.I)
_IX_CONTEXT: Final = re.compile(r"\bcontextRef\s*=\s*[\"']([^\"']+)[\"']", re.I)


def cover_classes(raw: bytes) -> list[tuple[frozenset[str], frozenset[str]]]:
    """The 12(b) classes an inline-XBRL cover tags: one context per class carrying ``dei:Security12bTitle`` and
    ``dei:TradingSymbol`` (Reg S-K Item 601(b)(104), Reg S-T Rule 406; ``app/services/sec_cover_identity.py``
    reads the same rule from an XBRL instance). Each class is (titles, symbols), normalised."""
    facts: dict[str, dict[str, set[str]]] = {}
    for match in _IX_FACT.finditer(raw.decode("utf-8", errors="replace")):
        name, context = _IX_NAME.search(match[1]), _IX_CONTEXT.search(match[1])
        text = normalise(html.unescape(_TAG.sub(" ", match[2])))
        if name and context and text:
            facts.setdefault(context[1], {}).setdefault(name[1], set()).add(text)
    return [
        (frozenset(f["Security12bTitle"]), frozenset(f["TradingSymbol"]))
        for _, f in sorted(facts.items())
        if {"Security12bTitle", "TradingSymbol"} <= set(f)
    ]


def single_class(raw: bytes) -> tuple[str, str] | None:
    """The cover's only 12(b) class as (title, symbol), or None when it tags none, several, or an ambiguous one."""
    classes = cover_classes(raw)
    if len(classes) != 1 or len(classes[0][0]) != 1 or len(classes[0][1]) != 1:
        return None
    return next(iter(classes[0][0])), next(iter(classes[0][1]))


#: The words §3.3 requires beside a termination or first-trade date, by construction: no source rule lists them.
#: They show the quote is about the session, not that it describes a completed action (the adjudication log does).
LAST_SESSION_WORDS: Final = {
    "last_trading_day": re.compile(r"\b(last|final) (day of trading|trading day|day on which .{0,40}trade)", re.I),
    "suspended": re.compile(r"\bsuspend", re.I),
    "merger_closing": re.compile(r"\b(complet|consummat|clos(ed|ing) of the (merger|transaction|acquisition))", re.I),
}
FIRST_TRADE_WORDS: Final = re.compile(
    r"\b(began|begin|begins|commenced?|commences|start(ed|s)?) trading|\bfirst (day of trading|trading day)", re.I
)


# --------------------------------------------------------------------------- §3.3 checker: records

RECORD_TYPES: Final = ("split", "symbol_change", "first_trade", "termination_end")
STATUSES: Final = frozenset({"confirmed", "contested", "cancelled"})
MAX_EVIDENCE: Final = 3
_ACCESSION: Final = re.compile(r"\d{10}-\d{2}-\d{6}")
_DOCUMENT: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
_ACCEPTANCE: Final = re.compile(r"\d{14}")
_HEADER_ACCEPTANCE: Final = re.compile(rb"<ACCEPTANCE-DATETIME>\s*(\d{14})")


@dataclass(frozen=True)
class Evidence:
    cik: str
    accession: str
    document: str
    quote: str
    #: The filing's ``ACCEPTANCE-DATETIME`` as its index headers state it (14 digits, EDGAR's Eastern clock).
    acceptance: str

    @property
    def folder(self) -> str:
        return f"https://www.sec.gov/Archives/edgar/data/{int(self.cik)}/{self.accession.replace('-', '')}"

    @property
    def document_url(self) -> str:
        return f"{self.folder}/{self.document}"

    @property
    def header_url(self) -> str:
        return f"{self.folder}/{self.accession}-index-headers.html"


@dataclass(frozen=True)
class Row:
    line: int
    values: Mapping[str, str]
    evidence: tuple[Evidence, ...]
    problems: tuple[str, ...] = ()


def read_rows(path: Path) -> list[Row]:
    """Every row of a record CSV, with its evidence items. A malformed item is a problem on the row, never a skip."""
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for line, values in enumerate(reader, 2):
            problems: list[str] = []
            allowed = {values["cik"], values.get("counterparty_cik") or values["cik"]}
            items = []
            for n in range(1, MAX_EVIDENCE + 1):
                accession = values.get(f"evidence_{n}_accession") or ""
                if not accession:
                    continue
                item = Evidence(
                    values.get(f"evidence_{n}_cik") or values["cik"],
                    accession,
                    values.get(f"evidence_{n}_document") or "",
                    values.get(f"evidence_{n}_quote") or "",
                    values.get(f"evidence_{n}_acceptance") or "",
                )
                if item.cik not in allowed:
                    problems.append(f"evidence_{n}: CIK {item.cik} is neither the row's nor its counterparty's")
                if not _ACCESSION.fullmatch(item.accession) or not _DOCUMENT.fullmatch(item.document):
                    problems.append(f"evidence_{n}: malformed accession or document name")
                if not _ACCEPTANCE.fullmatch(item.acceptance):
                    problems.append(f"evidence_{n}: acceptance is not 14 digits")
                if not item.quote.strip() or len(item.quote) > MAX_QUOTE:
                    problems.append(f"evidence_{n}: quote is empty or longer than {MAX_QUOTE} characters")
                items.append(item)
            if not items:
                problems.append("no evidence item")
            if values.get("status") not in STATUSES:
                problems.append(f"status {values.get('status')!r} is not one of {sorted(STATUSES)}")
            rows.append(Row(line, values, tuple(items), tuple(problems)))
        return rows


def _date(values: Mapping[str, str], name: str) -> date | None:
    text = values.get(name) or ""
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


#: FINRA Rule 11140(b)(1)-(2), the spec's source rule for a split's effective date when filings state only record and
#: payable dates: a distribution of 25% or more goes ex the first business day after the payable date, a smaller one
#: on the record date.
LARGE_DISTRIBUTION: Final = Fraction(1, 4)


def next_session(day: date) -> date:
    day += timedelta(days=1)
    while market_calendar.us_market_status(day) == "closed":
        day += timedelta(days=1)
    return day


def _fallback_failures(effective: date, ratio: Fraction, values: Mapping[str, str], texts: Sequence[str]) -> list[str]:
    """No quote states the effective date in its role: the spec's record-and-payable fallback, which a reverse split
    lacks. Each of the two dates must be quoted beside the words of its own role."""
    if ratio < 1:
        return [
            f"no quote states effective_date {effective} as the adjusted-basis session; a reverse split has no fallback"
        ]
    record, payable = _date(values, "record_date"), _date(values, "payable_date")
    if (
        record is None
        or payable is None
        or not any(dated_near(t, record, RECORD_WORDS) for t in texts)
        or not any(dated_near(t, payable, PAYABLE_WORDS) for t in texts)
    ):
        return [
            f"no quote states effective_date {effective} as the adjusted-basis session, "
            "nor record_date and payable_date in their roles"
        ]
    derived = next_session(payable) if ratio - 1 >= LARGE_DISTRIBUTION else record
    if derived != effective:
        return [f"effective_date {effective} is not {derived}, the FINRA 11140 date from record and payable dates"]
    return []


def _class_failures(
    values: Mapping[str, str],
    texts: Sequence[str],
    ratio: Fraction | None,
    effective: date | None,
    single_classes: Sequence[tuple[str, str]],
) -> list[str]:
    """§3.3: the class's title or symbol in the action's own terms (a quote that also states the ratio or the
    effective date), or an evidence cover whose only 12(b) class is the row's. A cover row quoted from a multi-class
    cover names one class among several, so it does not identify the class the action is in."""
    title = compact(values.get("class_title") or "").casefold()
    symbol = values.get("class_symbol") or ""

    def names_class(text: str) -> bool:
        return bool(title and title in compact(text).casefold()) or names_symbol(text, symbol)

    for text in texts:
        if names_class(text) and (
            (ratio is not None and ratio in stated_ratios(text)) or (effective and effective in stated_dates(text))
        ):
            return []
    if any(compact(t).casefold() == title or s == symbol for t, s in single_classes):
        return []
    return ["neither an action quote nor a single-class cover names the class title or symbol"]


def field_failures(
    record_type: str,
    values: Mapping[str, str],
    quotes: Sequence[str],
    single_classes: Sequence[tuple[str, str]] = (),
) -> list[str]:
    """§3.3's per-type requirements, read off the row's quotes (normalised). A cancelled row states no fields.

    ``single_classes`` are the (title, symbol) of every evidence document whose cover tags exactly one 12(b) class
    (:func:`single_class`): the spec's "or the cover lists a single class"."""
    if values["status"] == "cancelled":
        return []
    texts = [normalise(q) for q in quotes]
    dates = set().union(*(stated_dates(t) for t in texts))
    failures: list[str] = []

    def needs_date(name: str, words: re.Pattern[str] | None = None) -> None:
        value = _date(values, name)
        if value is None:
            failures.append(f"{name} is not an ISO date")
        elif words is None and value not in dates:
            failures.append(f"no quote states {name} {value}")
        elif words is not None and not any(dated_near(t, value, words) for t in texts):
            failures.append(f"no quote states {name} {value} beside the words of its role")

    def needs_symbol(name: str) -> None:
        if not any(names_symbol(t, values.get(name) or "") for t in texts):
            failures.append(f"no quote names {name} {values.get(name)!r}")

    if record_type == "split":
        try:
            ratio: Fraction | None = Fraction(values["ratio"])
        except ValueError, ZeroDivisionError:
            ratio = None
            failures.append(f"ratio {values['ratio']!r} is not a number")
        else:
            if ratio not in set().union(*(stated_ratios(t) for t in texts)):
                failures.append(f"no quote states the ratio {ratio}")
        effective = _date(values, "effective_date")
        if effective is None:
            failures.append("effective_date is not an ISO date")
        elif ratio is not None and not any(dated_near(t, effective, EFFECTIVE_WORDS) for t in texts):
            failures += _fallback_failures(effective, ratio, values, texts)
        failures += _class_failures(values, texts, ratio, effective, single_classes)
    elif record_type == "symbol_change":
        needs_symbol("old_symbol")
        needs_symbol("new_symbol")
        needs_date("effective_date")
    elif record_type == "first_trade":
        needs_date("first_session", FIRST_TRADE_WORDS)
    elif record_type == "termination_end":
        basis = values.get("last_session_basis") or ""
        if values.get("last_session") != "not_stated":
            words = LAST_SESSION_WORDS.get(basis)
            if words is None:
                failures.append(f"last_session_basis {basis!r} is not one of {sorted(LAST_SESSION_WORDS)}")
            else:
                needs_date("last_session", words)
    else:
        raise ValueError(f"unknown record type {record_type}")
    return failures


# --------------------------------------------------------------------------- §3.3 checker: mirror and verdict


def mirror_path(mirror: Path, accession: str, name: str) -> Path:
    return mirror / accession / f"{name}.gz"


def fetch_into_mirror(urls: Mapping[str, Path]) -> Counter[str]:
    """Fetch every URL whose mirror file is absent; write the served bytes gzipped (mtime 0). Non-200 writes nothing."""
    pending = {url: path for url, path in urls.items() if not path.exists()}
    status: Counter[str] = Counter(mirrored=len(urls) - len(pending))
    if not pending:
        return status

    async def run() -> None:
        headers = {"User-Agent": settings.sec_user_agent, "Accept-Encoding": "gzip, deflate"}
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=False) as client:
            fetcher = PipelinedSecFetcher(client=client, target_rps=7.0, concurrency=8)
            async for got in fetcher.fetch_many(FetchTask(key=u, url=u, headers=headers) for u in pending):
                if got.response is None or got.response.status_code != 200:
                    status[f"http_{got.response.status_code}" if got.response else "transport_error"] += 1
                    continue
                path = pending[str(got.key)]
                path.parent.mkdir(parents=True, exist_ok=True)
                publish(path, gzip.compress(got.response.content, mtime=0))
                status["fetched"] += 1

    asyncio.run(run())
    return status


@dataclass
class Verdict:
    line: int
    passed: bool
    failures: list[str] = field(default_factory=list)
    evidence: list[dict[str, object]] = field(default_factory=list)


def check_row(record_type: str, row: Row, mirror: Path) -> Verdict:
    verdict = Verdict(row.line, False, list(row.problems))
    single_classes: list[tuple[str, str]] = []
    for n, item in enumerate(row.evidence, 1):
        entry: dict[str, object] = {"accession": item.accession, "document": item.document, "url": item.document_url}
        doc_path = mirror_path(mirror, item.accession, item.document)
        head_path = mirror_path(mirror, item.accession, f"{item.accession}-index-headers.html")
        if not doc_path.exists() or not head_path.exists():
            verdict.failures.append(f"evidence_{n}: not served by EDGAR")
            verdict.evidence.append(entry)
            continue
        raw = gzip.decompress(doc_path.read_bytes())
        header = _HEADER_ACCEPTANCE.search(gzip.decompress(head_path.read_bytes()))
        entry |= {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
        entry["acceptance"] = header.group(1).decode() if header else None
        if entry["acceptance"] != item.acceptance:
            verdict.failures.append(
                f"evidence_{n}: acceptance {item.acceptance} != index headers {entry['acceptance']}"
            )
        entry["quote_found"] = compact(item.quote) in compact(document_text(raw, item.document))
        if not entry["quote_found"]:
            verdict.failures.append(f"evidence_{n}: quote does not occur in {item.document}")
        # Only the issuer's own cover can say which of its classes is the only one; a counterparty's cannot.
        cover = single_class(raw) if item.cik == row.values["cik"] else None
        entry["single_class"] = list(cover) if cover else None
        if cover:
            single_classes.append(cover)
        verdict.evidence.append(entry)
    verdict.failures += field_failures(record_type, row.values, [e.quote for e in row.evidence], single_classes)
    verdict.passed = not verdict.failures
    return verdict


def check(record_type: str, records: Path, mirror: Path, out: Path) -> None:
    rows = read_rows(records)
    urls: dict[str, Path] = {}
    for row in rows:
        for item in row.evidence:
            if _ACCESSION.fullmatch(item.accession) and _DOCUMENT.fullmatch(item.document):
                urls[item.document_url] = mirror_path(mirror, item.accession, item.document)
                urls[item.header_url] = mirror_path(mirror, item.accession, f"{item.accession}-index-headers.html")
    fetched = fetch_into_mirror(urls)
    verdicts = [check_row(record_type, row, mirror) for row in rows]
    buffer = io.StringIO()
    for verdict in verdicts:
        buffer.write(json.dumps(verdict.__dict__, sort_keys=True) + "\n")
    publish(out, buffer.getvalue().encode())
    print(
        json.dumps(
            {
                "type": record_type,
                "records_sha256": sha256_of(records),
                "rows": len(verdicts),
                "passed": sum(v.passed for v in verdicts),
                "fetch": dict(sorted(fetched.items())),
                "check_sha256": sha256_of(out),
            },
            indent=1,
        )
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="#3739 slice 3b: the §3.3 checker and the §3.4 comparison set")
    sub = parser.add_subparsers(dest="command", required=True)
    cmp_ = sub.add_parser("comparison")
    cmp_.add_argument("--replica", choices=sorted(REPLICAS), required=True)
    cmp_.add_argument("--u", type=Path, required=True)
    cmp_.add_argument("--u-sha256", required=True)
    cmp_.add_argument("--out", type=Path, required=True)
    chk = sub.add_parser("check")
    chk.add_argument("--type", choices=RECORD_TYPES, required=True)
    chk.add_argument("--records", type=Path, required=True)
    chk.add_argument("--mirror", type=Path, required=True)
    chk.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "comparison":
        comparison(REPLICAS[args.replica], checked(args.u, args.u_sha256), args.out)
    else:
        check(args.type, args.records, args.mirror, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
