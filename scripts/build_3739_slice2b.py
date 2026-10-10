"""#3739 slice 2b: §3.2's candidate screens, run over every issuer in the source data, and their pinned lists.

Spec: ``docs/research/2026-10-09-3739-stage-c-panel.md`` §3.2 (screens), §3.1 (the event window), §2 (screens run
over every issuer, not only U). Nothing here reads a price: the inputs are SEC structured data sets, the slice-2a
filing extract and the slice-2a register extract.

``notes-extract`` reads Financial Statement and Notes archives and keeps the two XBRL facts the split screen reads,
every issuer and every form, each with its filing's acceptance from EDGAR ``submissions.zip`` (the clock step 1
uses, ``acceptance_ny_date``). companyfacts strips the per-class cover counts (``sec-edgar.md`` §7.17) and plain
FSDS carries no notes (§7.18), so FSNDS is the only structured source of both.

``insider-extract`` runs the #3361 linkage's admission (rule 2, ``_admit_all`` unchanged) over the insider data
sets and keeps every stored observation accepted from the symbol look-back before the event window on.

``screens`` writes the three candidate lists (split, symbol change, termination) from those extracts.

    PYTHONPATH=. uv run python scripts/build_3739_slice2b.py notes-extract --submissions <submissions.zip> \\
        --out <notes.jsonl.gz> <fsnds_*_notes.zip ...>
    PYTHONPATH=. uv run python scripts/build_3739_slice2b.py insider-extract --submissions <submissions.zip> \\
        --out <observations.jsonl.gz> <insider_YYYYqN.zip ...>
    PYTHONPATH=. uv run python scripts/build_3739_slice2b.py screens --through <date> \\
        --notes <f> --notes-sha256 <d> --observations <f> --observations-sha256 <d> \\
        --extract <f> --extract-sha256 <d> --register <f> --register-sha256 <d> --out-dir <dir>
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import os
import re
import zipfile
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final

from app.services.factor_panel_artefact import read_gz_lines
from app.services.pit_fundamentals import SubmissionsIndex, acceptance_ny_date, parse_submissions
from app.services.security_linkage import WINDOW_DAYS, Admitted, admit_row
from scripts.build_3361_security_linkage import _admit_all, _submission_text
from scripts.build_3739_slice2 import EXTRACT_FROM, Filing, checked, read_extract, sha256_of

#: §3.1: splits and symbol changes effective from this date are recorded. A screen keeps a candidate whose interval
#: reaches into [EVENT_START, through].
EVENT_START: Final = date(2024, 7, 1)

#: §3.2's two XBRL facts, each prefix-matched on its taxonomy version (``sec-edgar.md`` §7.18: ``version`` is
#: per-row, ``us-gaap/2025``; a company-extension tag of the same name carries the accession as its version).
RATIO_TAG: Final = "StockholdersEquityNoteStockSplitConversionRatio1"
COVER_TAG: Final = "EntityCommonStockSharesOutstanding"
NOTE_TAGS: Final = {RATIO_TAG: "us-gaap/", COVER_TAG: "dei/"}
#: §3.2: covers of consecutive 10-Q and 10-K filings, an amendment replacing its original.
COVER_FORMS: Final = frozenset({"10-Q", "10-K", "10-Q/A", "10-K/A"})
COUNT_BAND: Final = (0.8, 1.25)
#: The class axis as the data sets spell it: "Statement" and "Axis" are truncated (FSNDS readme, DIM), so
#: us-gaap ``StatementClassOfStockAxis`` is ``ClassOfStock``.
CLASS_AXIS: Final = "ClassOfStock"
#: §3.2: an 8-K whose items include 5.03 (amendments of articles; a split is often effected by one).
ITEM_503: Final = "5.03"
ITEM_503_FORMS: Final = frozenset({"8-K", "8-K/A"})
ITEM_503_SPAN: Final = timedelta(days=92)
#: §3.2: the adjudicator reads the issuer's 8-Ks within 120 days before the Form 25.
TERMINATION_LOOKBACK: Final = timedelta(days=120)
#: The linkage reads evidence accepted within ``WINDOW_DAYS`` before a date (#3361 rule 5), so the symbol in force
#: at EVENT_START is the latest evidence in that span.
SYMBOL_LOOKBACK: Final = timedelta(days=WINDOW_DAYS)

_NOTES_ARCHIVE: Final = re.compile(r"fsnds_(\d{4}(?:q[1-4]|_\d{2}))_notes\.zip")


# --------------------------------------------------------------------------- io helpers


def publish(out: Path, payload: bytes) -> None:
    """Write a fully serialised payload to a new file: exclusive open, fsync, and no partial file on failure."""
    handle = out.open("xb")  # refuses an existing file before anything is written
    try:
        with handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        out.unlink(missing_ok=True)
        raise


def csv_bytes(header: Sequence[str], rows: Iterable[Sequence[Any]]) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buffer.getvalue().encode()


def tsv_rows(archive: zipfile.ZipFile, member: str, malformed: Counter[str]) -> Iterator[dict[str, str]]:
    """A data-set TSV (flat, no quoting, UTF-8) as header-keyed rows. A row whose field count differs from the
    header's is counted under ``member``, never read: a partial row could misplace a value into another column."""
    with archive.open(member) as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8", newline="")
        header = text.readline().rstrip("\r\n").split("\t")
        for line in text:
            fields = line.rstrip("\r\n").split("\t")
            if len(fields) != len(header):
                malformed[member] += 1
                continue
            yield dict(zip(header, fields, strict=True))


def ny_date(acceptance: str) -> date:
    return acceptance_ny_date(acceptance)


def year_before(day: date) -> date:
    return day.replace(year=day.year - 1) if (day.month, day.day) != (2, 29) else date(day.year - 1, 2, 28)


def overlaps(start: date, end: date, through: date) -> bool:
    """The interval reaches into the event window [EVENT_START, through]."""
    return start <= through and end >= EVENT_START


# --------------------------------------------------------------------------- notes extract


@dataclass(frozen=True)
class NoteFact:
    """One distinct fact (``iprx`` duplicates collapsed) of a §3.2 tag, with its filing's identity."""

    archive: str
    adsh: str
    cik: str
    form: str
    #: ``sub.period``: the filing's fiscal period end (yyyymmdd).
    period: str
    #: EDGAR ``acceptanceDateTime`` from ``submissions.zip`` (UTC ISO), or None when the issuer's index lacks it.
    accepted: str | None
    tag: str
    #: ``num.ddate``: the fact's end date ROUNDED TO THE NEAREST MONTH END (FSNDS readme), kept for audit.
    ddate: str
    #: The end date as reported (ISO): ``ddate`` minus ``datp`` days, ``datp`` being "the date proximity in number of
    #: days between end date reported and month-end rounded date" (FSNDS readme). Sign checked on covers whose dates
    #: the filings state: Apple 10-Q 2024-08-01 "as of July 19, 2024" is ddate 20240731, datp 12.
    reported: str
    qtrs: str
    uom: str
    #: ``dim.tsv`` segments, "" for the default (no dimension).
    segments: str
    #: "" for the registrant; else a co-registrant, parent or other entity (FSNDS readme, NUM ``coreg``).
    coreg: str
    value: str


def archive_label(path: Path) -> str:
    match = _NOTES_ARCHIVE.fullmatch(path.name)
    if match is None:
        raise ValueError(f"not a Financial Statement and Notes archive name: {path.name}")
    return match.group(1)


def reported_date(ddate: str, datp: str) -> date | None:
    """``ddate`` less ``datp`` whole days, or None when either is unreadable (counted, never guessed)."""
    try:
        rounded = datetime.strptime(ddate, "%Y%m%d").date()
        offset = Decimal(datp)
    except ValueError, InvalidOperation:
        return None
    if not offset.is_finite() or offset != offset.to_integral_value() or abs(offset) > 16:
        return None
    return rounded - timedelta(days=int(offset))


def read_notes_archive(path: Path, ledger: Counter[str]) -> list[NoteFact]:
    """The §3.2 facts of one archive. A fact whose dimension hash ``dim.tsv`` does not resolve is counted and
    dropped: an unresolved hash is not the default dimension (``sec-edgar.md`` §7.18)."""
    label = archive_label(path)
    malformed: Counter[str] = Counter()
    facts: set[NoteFact] = set()
    with zipfile.ZipFile(path) as archive:
        subs = {
            row["adsh"]: (f"{int(row['cik']):010d}", row["form"], row["period"])
            for row in tsv_rows(archive, "sub.tsv", malformed)
        }
        dims = {row["dimhash"]: row["segments"] for row in tsv_rows(archive, "dim.tsv", malformed)}
        for row in tsv_rows(archive, "num.tsv", malformed):
            prefix = NOTE_TAGS.get(row["tag"])
            if prefix is None or not row["version"].startswith(prefix):
                continue
            ledger[f"{row['tag']}:rows"] += 1
            segments = dims.get(row["dimh"])
            filing = subs.get(row["adsh"])
            if segments is None or filing is None:
                ledger[f"{row['tag']}:{'unresolved_dimension' if segments is None else 'no_sub_row'}"] += 1
                continue
            reported = reported_date(row["ddate"], row["datp"])
            if reported is None:
                ledger[f"{row['tag']}:unreadable_date"] += 1
                continue
            cik, form, period = filing
            facts.add(
                NoteFact(
                    label, row["adsh"], cik, form, period, None, row["tag"], row["ddate"], reported.isoformat(),
                    row["qtrs"], row["uom"], segments, row["coreg"], row["value"],
                )
            )  # fmt: skip
    for member, count in malformed.items():
        ledger[f"malformed:{label}:{member}"] += count
    return sorted(facts, key=lambda f: (f.adsh, f.tag, f.ddate, f.qtrs, f.segments, f.coreg, f.value, f.uom))


def submissions_reader(archive: zipfile.ZipFile) -> Callable[[str], SubmissionsIndex | str]:
    """A filer's parsed submissions (main file and overflow pages), or #3360's integrity-failure reason."""
    names = set(archive.namelist())

    def index(cik: str) -> SubmissionsIndex | str:
        member = f"CIK{cik}.json"
        if member not in names:
            return "no_submissions_entry"

        def read_page(page: str) -> object | None:
            return json.loads(archive.read(page)) if page in names else None

        return parse_submissions(cik, json.loads(archive.read(member)), read_page)

    return index


def with_acceptance(facts: Sequence[NoteFact], submissions: zipfile.ZipFile, ledger: Counter[str]) -> list[NoteFact]:
    """Each fact with its filing's EDGAR acceptance (step 1's clock), read from the filer's submissions."""
    index_of = submissions_reader(submissions)
    by_cik: dict[str, SubmissionsIndex | str] = {}
    out: list[NoteFact] = []
    for fact in facts:
        if fact.cik not in by_cik:
            by_cik[fact.cik] = index_of(fact.cik)
        index = by_cik[fact.cik]
        filing = None if isinstance(index, str) else index.filings.get(fact.adsh)
        accepted = None if filing is None else filing.acceptance
        if accepted is None:
            ledger["no_acceptance"] += 1
        out.append(NoteFact(**{**asdict(fact), "accepted": accepted}))
    return out


def notes_extract(archives: Sequence[Path], submissions: Path, out: Path) -> None:
    labels = [archive_label(path) for path in archives]
    if len(set(labels)) != len(labels):
        raise ValueError("an archive is named twice")
    ledger: Counter[str] = Counter()
    facts: list[NoteFact] = []
    seen: dict[str, str] = {}
    for path in archives:
        archive_facts = read_notes_archive(path, ledger)
        for adsh in {f.adsh for f in archive_facts}:
            if adsh in seen:
                raise ValueError(f"{adsh} is in both {seen[adsh]} and {path.name}: the archives overlap")
            seen[adsh] = path.name
        facts.extend(archive_facts)
    with zipfile.ZipFile(submissions) as zf:
        facts = with_acceptance(facts, zf, ledger)
    lines = "".join(json.dumps(asdict(f), sort_keys=True) + "\n" for f in facts)
    publish(out, gzip.compress(lines.encode(), mtime=0))
    print(
        json.dumps(
            {
                "archives": {path.name: sha256_of(path) for path in archives},
                "submissions_sha256": sha256_of(submissions),
                "facts": len(facts),
                "filings": len(seen),
                "ledger": dict(sorted(ledger.items())),
                "notes_sha256": sha256_of(out),
            },
            indent=1,
        )
    )


def read_notes(path: Path) -> list[NoteFact]:
    return [NoteFact(**row) for row in read_gz_lines(path)]


# --------------------------------------------------------------------------- insider extract


@dataclass(frozen=True)
class SymbolEvidence:
    """The linkage's stored observations for one (issuer CIK, evidence symbol), summarised around the window."""

    cik: str
    symbol: str
    #: The latest acceptance in [EVENT_START - SYMBOL_LOOKBACK, EVENT_START), "" if none.
    last_before: str
    #: The first and last acceptance in [EVENT_START, through], "" if none, and how many observations.
    first_in: str
    last_in: str
    count_in: int


@dataclass(frozen=True)
class Observation:
    """One stored #3361 rule-2 observation: an insider filing's issuer CIK, unified symbol and EDGAR acceptance."""

    accession: str
    cik: str
    symbol: str
    acceptance: str


def summarise_symbols(stored: Iterable[Observation], through: date) -> list[SymbolEvidence]:
    before: dict[tuple[str, str], str] = {}
    inside: dict[tuple[str, str], list[str]] = defaultdict(list)
    for observation in stored:
        key = (observation.cik, observation.symbol)
        accepted = observation.acceptance
        day = ny_date(accepted)
        if EVENT_START - SYMBOL_LOOKBACK <= day < EVENT_START:
            before[key] = max(before.get(key, accepted), accepted, key=datetime.fromisoformat)
        elif EVENT_START <= day <= through:
            inside[key].append(accepted)
    out = []
    for key in sorted(set(before) | set(inside)):
        times = sorted(inside.get(key, []), key=datetime.fromisoformat)
        out.append(
            SymbolEvidence(
                key[0], key[1], before.get(key, ""), times[0] if times else "", times[-1] if times else "", len(times)
            )
        )
    return out


def window_observations(stored: Iterable[Mapping[str, Any]]) -> list[Observation]:
    """What ``summarise_symbols`` can read: every observation accepted from EVENT_START on, and per (CIK, symbol) the
    latest accepted in the look-back before it. The window's end is the screens' ``--through``, so one extract
    serves any cutoff its quarters cover."""
    inside: list[Observation] = []
    before: dict[tuple[str, str], Observation] = {}
    for o in stored:
        observation = Observation(o["accession"], o["cik"], o["symbol"], o["acceptance"])
        day = ny_date(observation.acceptance)
        if day >= EVENT_START:
            inside.append(observation)
        elif day >= EVENT_START - SYMBOL_LOOKBACK:
            key = (observation.cik, observation.symbol)
            held = before.get(key)
            if held is None or datetime.fromisoformat(observation.acceptance) > datetime.fromisoformat(held.acceptance):
                before[key] = observation
    return [*before.values(), *inside]


def insider_extract(quarters: Sequence[Path], submissions: Path, out: Path) -> None:
    """Rule 2 of the #3361 linkage over the given quarters (unchanged code: ``_admit_all``), every symbol kept,
    reduced to ``window_observations``."""
    named = sorted(quarters, key=lambda p: p.name)
    wanted: set[str] = set()
    for path in named:
        with zipfile.ZipFile(path) as archive:
            lines, columns = _submission_text(archive, path.name)
        for line in lines[1:]:
            admitted = admit_row(line.split("\t"), columns)
            if isinstance(admitted, Admitted):
                wanted.add(admitted.symbol)
    with zipfile.ZipFile(submissions) as zf:
        ledger, stored = _admit_all([(p.name, p) for p in named], zf, frozenset(wanted))
    kept = sorted(
        window_observations(stored), key=lambda o: (o.cik, o.symbol, datetime.fromisoformat(o.acceptance), o.accession)
    )
    lines = "".join(json.dumps(asdict(o), sort_keys=True) + "\n" for o in kept)
    publish(out, gzip.compress(lines.encode(), mtime=0))
    print(
        json.dumps(
            {
                "quarters": {p.name: sha256_of(p) for p in named},
                "submissions_sha256": sha256_of(submissions),
                "observation_outcomes": ledger["observation_outcomes"],
                "stored": len(stored),
                "kept": len(kept),
                "latest_acceptance": max((o.acceptance for o in kept), key=datetime.fromisoformat, default=None),
                "observations_sha256": sha256_of(out),
            },
            indent=1,
        )
    )


def read_observations(path: Path) -> list[Observation]:
    return [Observation(**row) for row in read_gz_lines(path)]


# --------------------------------------------------------------------------- screens


@dataclass(frozen=True)
class SplitCandidate:
    screen: str  # "xbrl_ratio" | "cover_count" | "item_503"
    cik: str
    #: The class axis member, "" for none (the default dimension).
    class_member: str
    start: date
    end: date
    #: The acceptance (UTC ISO) of the filing that raises the candidate: the later cover, the fact's or the 8-K's.
    accepted: str
    #: The indicated ratio (§2's q), "" when none; an indication, never a record field (§3.2).
    ratio: str
    evidence: tuple[str, ...]


def class_member(segments: str) -> str | None:
    """ "" for the default dimension; the member for exactly one class-axis pair; None for any other dimension."""
    if segments == "":
        return ""
    axis, _, member = segments.removesuffix(";").partition("=")
    if axis != CLASS_AXIS or not member or ";" in member or "=" in member:
        return None
    return member


def class_in(segments: str) -> str:
    """The class-axis member among a fact's dimensions, "" when it has none."""
    for pair in segments.split(";"):
        axis, _, member = pair.partition("=")
        if axis == CLASS_AXIS and member:
            return member
    return ""


def positive(value: str) -> Decimal | None:
    try:
        number = Decimal(value)
    except InvalidOperation:
        return None
    return number if number.is_finite() and number > 0 else None


def xbrl_candidates(facts: Iterable[NoteFact], through: date) -> list[SplitCandidate]:
    """§3.2: each ratio fact accepted in the window, whatever its context date, with interval [context end - 1 year,
    acceptance]. One candidate per (filing, context end, class): its repeated presentations are one indication."""
    groups: dict[tuple[str, str, str, str], list[NoteFact]] = defaultdict(list)
    for fact in facts:
        if fact.tag == RATIO_TAG and fact.accepted is not None:
            groups[(fact.cik, fact.adsh, fact.reported, class_in(fact.segments))].append(fact)
    out = []
    for (cik, adsh, reported, member), group in sorted(groups.items()):
        accepted = group[0].accepted
        assert accepted is not None
        day = ny_date(accepted)
        if not EVENT_START <= day <= through:
            continue
        values = {(f.uom, f.value) for f in group}
        (uom, value), *rest = sorted(values)
        ratio = str(positive(value)) if not rest and uom == "pure" and positive(value) else ""
        start = year_before(date.fromisoformat(reported))
        out.append(SplitCandidate("xbrl_ratio", cik, member, start, day, accepted, ratio, (adsh,)))
    return out


@dataclass(frozen=True)
class Cover:
    adsh: str
    accepted: str
    period: str
    #: The cover count's date as reported (``NoteFact.reported``).
    reported: date
    shares: Decimal


def covers(facts: Iterable[NoteFact], through: date, ledger: Counter[str]) -> dict[tuple[str, str], list[Cover]]:
    """Per (CIK, class), the cover count of each 10-Q and 10-K, in period order, an amendment replacing its original.

    Read from the registrant's facts only (``coreg`` empty: a co-registrant's count is another entity's) in unit
    ``shares``, under the default dimension or exactly one class-axis member. Within a filing a class takes its fact
    with the latest context date; two different values there are counted as ambiguous and the filing gives that
    class no cover. A filing slot is (CIK, class, form without ``/A``, period); the latest-accepted filing holds it.
    """
    per_filing: dict[tuple[str, str, str], list[NoteFact]] = defaultdict(list)
    for fact in facts:
        if fact.tag != COVER_TAG or fact.form not in COVER_FORMS:
            continue
        member = class_member(fact.segments)
        if fact.coreg or fact.uom != "shares" or member is None or fact.accepted is None:
            ledger["cover:not_read"] += 1
            continue
        if ny_date(fact.accepted) > through:
            # Before slots resolve, so an amendment accepted after the cutoff cannot replace its original.
            ledger["cover:after_cutoff"] += 1
            continue
        if positive(fact.value) is None:
            ledger["cover:non_positive"] += 1
            continue
        per_filing[(fact.cik, member, fact.adsh)].append(fact)
    slots: dict[tuple[str, str, str, str], Cover] = {}
    for (cik, member, adsh), group in per_filing.items():
        latest = max(f.reported for f in group)
        values = {positive(f.value) for f in group if f.reported == latest}
        if len(values) != 1:
            ledger["cover:ambiguous_filing"] += 1
            continue
        (shares,) = values
        assert shares is not None
        first = group[0]
        assert first.accepted is not None
        cover = Cover(adsh, first.accepted, first.period, date.fromisoformat(latest), shares)
        slot = (cik, member, first.form.removesuffix("/A"), first.period)
        incumbent = slots.get(slot)
        if incumbent is None or datetime.fromisoformat(cover.accepted) > datetime.fromisoformat(incumbent.accepted):
            if incumbent is not None:
                ledger["cover:replaced"] += 1
            slots[slot] = cover
        else:
            ledger["cover:replaced"] += 1
    out: dict[tuple[str, str], list[Cover]] = defaultdict(list)
    for (cik, member, _, _), cover in slots.items():
        out[(cik, member)].append(cover)
    ordered = {
        key: sorted(series, key=lambda c: (c.period, datetime.fromisoformat(c.accepted)))
        for key, series in sorted(out.items())
    }
    # A series whose first cover is dated in the window has no earlier cover in the archives read, so a jump from
    # its pre-window count cannot be seen: a new registrant, or an archive the build did not read. Published.
    ledger["cover:series_first_in_window"] = sum(series[0].reported >= EVENT_START for series in ordered.values())
    return ordered


def cover_candidates(series: Mapping[tuple[str, str], Sequence[Cover]], through: date) -> list[SplitCandidate]:
    """§3.2: consecutive covers whose count ratio lies outside [0.8, 1.25]; the interval is the two covers' dates."""
    low, high = COUNT_BAND
    out = []
    for (cik, member), ordered in series.items():
        for earlier, later in zip(ordered, ordered[1:], strict=False):
            ratio = later.shares / earlier.shares
            if Decimal(str(low)) <= ratio <= Decimal(str(high)):
                continue
            start, end = earlier.reported, later.reported
            if ny_date(later.accepted) > through or not overlaps(min(start, end), max(start, end), through):
                continue
            out.append(
                SplitCandidate(
                    "cover_count", cik, member, min(start, end), max(start, end), later.accepted,
                    f"{ratio:.6g}", (earlier.adsh, later.adsh),
                )
            )  # fmt: skip
    return out


def unread_business_days(start: date, extract_from: date) -> list[date]:
    """Weekdays in [start, extract_from): days a filing could be accepted (EDGAR accepts on business days) that the
    filing extract does not cover."""
    return [
        start + timedelta(days=i)
        for i in range((extract_from - start).days)
        if (start + timedelta(days=i)).weekday() < 5
    ]


def item_503_candidates(filings: Iterable[Filing], through: date) -> list[SplitCandidate]:
    """§3.2: any 8-K whose items include 5.03, with interval [acceptance, + 92 days]. Refuses unless the extract
    reaches back to the first acceptance whose interval meets the window (EVENT_START - 92 days, 2024-03-31, a Sunday
    before the extract's 2024-04-01: the only day it misses is one EDGAR accepts nothing on)."""
    missing = unread_business_days(EVENT_START - ITEM_503_SPAN, EXTRACT_FROM)
    if missing:
        raise ValueError(f"the filing extract starts {EXTRACT_FROM}; Item 5.03 acceptances on {missing} are unread")
    out = []
    for filing in filings:
        if filing.form not in ITEM_503_FORMS or ITEM_503 not in filing.items:
            continue
        day = filing.accepted_date
        if day > through or not overlaps(day, day + ITEM_503_SPAN, through):
            continue
        out.append(
            SplitCandidate(
                "item_503", filing.cik, "", day, day + ITEM_503_SPAN, filing.accepted, "", (filing.accession,)
            )
        )
    return out


@dataclass(frozen=True)
class SymbolCandidate:
    cik: str
    #: symbol -> (last before, first in, last in, count in), every symbol the window's evidence shows.
    symbols: tuple[SymbolEvidence, ...]


def symbol_candidates(evidence: Iterable[SymbolEvidence]) -> list[SymbolCandidate]:
    """§3.2: a CIK whose linkage evidence shows two symbols over the window. The window's evidence is every
    observation accepted in it plus the latest before it (within the linkage's look-back), which is the symbol in
    force at its start; without it a change between the last pre-window filing and the first in-window one would
    show one symbol."""
    by_cik: dict[str, list[SymbolEvidence]] = defaultdict(list)
    for row in evidence:
        by_cik[row.cik].append(row)
    out = []
    for cik, rows in sorted(by_cik.items()):
        befores = [r.last_before for r in rows if r.last_before]
        in_force = max(befores, key=datetime.fromisoformat) if befores else None
        shown = [
            r
            for r in rows
            if r.count_in > 0
            or (
                in_force is not None
                and r.last_before
                and datetime.fromisoformat(r.last_before) == datetime.fromisoformat(in_force)
            )
        ]
        if len({r.symbol for r in shown}) >= 2:
            out.append(SymbolCandidate(cik, tuple(sorted(shown, key=lambda r: r.symbol))))
    return out


def termination_candidates(register: Sequence[Mapping[str, str]]) -> list[dict[str, str]]:
    """§3.2: every register event; the adjudicator reads the issuer's 8-Ks from 120 days before the Form 25."""
    return [
        {**event, "read_from": (date.fromisoformat(event["filed_date"]) - TERMINATION_LOOKBACK).isoformat()}
        for event in sorted(register, key=lambda e: (e["issuer_cik"], e["accession_number"]))
    ]


SPLIT_COLUMNS: Final = ("candidate", "screen", "cik", "class_member", "start", "end", "accepted", "ratio", "evidence")


def split_rows(candidates: Iterable[SplitCandidate]) -> list[list[str]]:
    ordered = sorted(candidates, key=lambda c: (c.cik, c.start, c.end, c.screen, c.class_member, c.evidence))
    return [
        [f"S{i:06d}", c.screen, c.cik, c.class_member, c.start.isoformat(), c.end.isoformat(), c.accepted, c.ratio,
         ";".join(c.evidence)]
        for i, c in enumerate(ordered, start=1)
    ]  # fmt: skip


def read_register_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def screens(through: date, notes: Path, observations: Path, extract: Path, register: Path, out_dir: Path) -> None:
    ledger: Counter[str] = Counter()
    facts = read_notes(notes)
    split = [
        *xbrl_candidates(facts, through),
        *cover_candidates(covers(facts, through, ledger), through),
        *item_503_candidates(read_extract(extract), through),
    ]
    symbol = symbol_candidates(summarise_symbols(read_observations(observations), through))
    termination = termination_candidates(read_register_rows(register))
    payloads = {
        # Gzipped (mtime 0, so the bytes replay): at ~1.8 MB the plain CSV overflows the review bot's prompt.
        "candidates-split.csv.gz": gzip.compress(csv_bytes(SPLIT_COLUMNS, split_rows(split)), mtime=0),
        "candidates-symbol-change.csv": csv_bytes(
            ("candidate", "cik", "symbol", "last_before", "first_in", "last_in", "count_in"),
            (
                [f"Y{i:06d}", c.cik, r.symbol, r.last_before, r.first_in, r.last_in, r.count_in]
                for i, c in enumerate(symbol, start=1)
                for r in c.symbols
            ),
        ),
        "candidates-termination.csv": csv_bytes(
            ("candidate", "issuer_cik", "accession_number", "form", "filed_date", "rule_provision", "read_from"),
            (
                [
                    f"T{i:06d}",
                    *(
                        e[k]
                        for k in ("issuer_cik", "accession_number", "form", "filed_date", "rule_provision", "read_from")
                    ),
                ]
                for i, e in enumerate(termination, start=1)
            ),
        ),
    }
    targets = {name: out_dir / name for name in payloads}
    existing = [str(p) for p in targets.values() if p.exists()]
    if existing:
        raise FileExistsError(f"refusing to replace pinned candidate lists: {existing}")
    for name, payload in payloads.items():
        publish(targets[name], payload)
    print(
        json.dumps(
            {
                "through": through.isoformat(),
                "split_by_screen": dict(Counter(c.screen for c in split)),
                "split_issuers": len({c.cik for c in split}),
                "symbol_change_issuers": len(symbol),
                "termination_events": len(termination),
                "ledger": dict(sorted(ledger.items())),
                "sha256": {name: sha256_of(path) for name, path in targets.items()},
            },
            indent=1,
        )
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="#3739 slice 2b: §3.2 candidate screens")
    sub = parser.add_subparsers(dest="command", required=True)
    notes = sub.add_parser("notes-extract")
    notes.add_argument("--submissions", type=Path, required=True)
    notes.add_argument("--out", type=Path, required=True)
    notes.add_argument("archives", type=Path, nargs="+")
    insider = sub.add_parser("insider-extract")
    insider.add_argument("--submissions", type=Path, required=True)
    insider.add_argument("--out", type=Path, required=True)
    insider.add_argument("quarters", type=Path, nargs="+")
    scr = sub.add_parser("screens")
    scr.add_argument("--through", type=date.fromisoformat, required=True)
    for name in ("notes", "observations", "extract", "register"):
        scr.add_argument(f"--{name}", type=Path, required=True)
        scr.add_argument(f"--{name}-sha256", required=True)
    scr.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "notes-extract":
        notes_extract(args.archives, args.submissions, args.out)
    elif args.command == "insider-extract":
        insider_extract(args.quarters, args.submissions, args.out)
    else:
        screens(
            args.through,
            checked(args.notes, args.notes_sha256),
            checked(args.observations, args.observations_sha256),
            checked(args.extract, args.extract_sha256),
            checked(args.register, args.register_sha256),
            args.out_dir,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
