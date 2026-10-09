"""#3621 slice 5 premise: where a FINRA short-interest flag can be computed on the stage-B panel, and where it bites.

Counts only. It reads the stage-B artefact bound by step 2's capture (the one ``scripts.run_3621_avoidance`` reads):
its admitted rows' identity and ME fields (``M``, ``s_M``, ``name_key``, ``series_id``, ``symbol``, ``me.shares``,
``me.value``) and the retrospective ``terminating`` field (a printed coverage diagnostic only; it enters no state,
flag or calibration), the frozen daily bars (volume only), split stamps, SPY sessions and the JKP NYSE size cutoffs.
No holding return or month-(t+1) price enters any figure.

FINRA's bimonthly files are re-parsed from the stored raw payloads (``filing_raw_documents``, accession
``FINRA_SI_<YYYYMMDD>``), never from ``finra_short_interest_observations``: that table keeps only rows whose symbol
resolved to a currently tradable instrument at ingest, so a delisted name has no row there. A selected file with no
stored payload is fetched from the CDN into memory. Every body row's ``settlementDate`` must equal the file's date.
The calendar is ``docs/research/3621-finra-si-calendar.csv`` (FINRA's designated settlement and publication dates,
``scripts/build_3621_finra_si_calendar.py``); it must start at ``CALENDAR_FIRST``, hold exactly two settlements in
every month from the next month through ``CALENDAR_LAST_MONTH``, and none after, unique and with publication dates
strictly increasing.

Per formation M, the settlement used is the latest calendar settlement whose publication date is strictly before
s(M). Unless it is on or after ``COVERAGE_START`` the formation is ``no_settlement``. A covered formation whose
settlement is not the first calendar settlement of M's month refuses. ``revisionFlag`` is not read for a state: FINRA
defines it as "the previous short interest position in the security was revised since the prior reporting cycle"
(Regulatory Notice 21-19, note 10), so it marks a revision of the PRIOR settlement's figure, published after s(M);
the revision check below measures whether the stored prior file holds the original or the revised figure. Per
admitted name, in precedence order:

- ``ambiguous``: the normalised symbol (``normalise_symbol``) is empty, shared by two admitted names at M, or carried
  by two rows of the file used;
- ``unmatched``: no row of the settlement's file carries the normalised symbol;
- ``unverifiable``: a session of the row's ADV window has no usable bar (``usable`` true, volume finite and >= 0);
- ``identity_fail``: |ln(FINRA ``averageDailyVolumeQuantity`` / our ADV)| > ln ``ADV_TOLERANCE``, or exactly one of
  the two is zero;
- ``valid``: SIR = ``currentShortPositionQuantity`` x ``split_product(splits, settlement, s(M))`` / ``me.shares``.

Our ADV follows FINRA's glossary ("Total Volume or Adjusted Volume in case of splits / Total trade days between
(previous settlement date + 1) to (current settlement date)"): over the SPY sessions after the previous calendar
settlement through the row's settlement, the sum of each session's volume x ``split_product(splits, session,
settlement)`` (the Intrader series is unadjusted), divided by the number of those sessions.

``flagged``: SIR >= q, q the ``ceil(0.9 N)``-th smallest of the N valid values at M over all admitted names. A
covered formation with N = 0, or q <= 0, refuses. Diagnostic only: the flags under Asquith, Pathak & Ritter's
unreported-is-zero rule (``unmatched`` names enter N at 0).

Outputs: the printed summary; ``docs/research/3621-si-premise-counts.csv`` (per covered formation and population,
every state count, ``flagged`` and ``split_carried``: the exact reproduction reference); and a per-name file, one
line per admitted name at a covered formation, ordered by M ascending, then ME descending, then ``name_key``
ascending: ``json.dumps([M, name_key, state, settlement ISO date or null, SIR or null, flagged], separators=(",",
":"))`` plus a newline, SIR as ``float(short * split_product) / float(shares)`` with ``short`` an int and the product
a ``Decimal``. Its uncompressed sha256 is printed.

Usage: ``PYTHONPATH=. uv run python -m scripts.measure_3621_short_interest_premise [names_out.jsonl.gz]``
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import math
import re
import statistics
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.finra_short_interest import FinraShortInterestProvider
from app.services.factor_panel import SplitStamp, split_product
from app.services.finra_short_interest_ingest import normalise_symbol, parse_body_settlement_date
from app.services.raw_filings import stored_body
from scripts.build_3609_factor_panel import Frozen
from scripts.capture_3609_step2 import capture_path, verify_capture
from scripts.report_3609_step2_universe import NYSE_CUTOFFS
from scripts.run_3621_avoidance import DAILY, SESSIONS, STAGE_B_CAPTURE_RUN, STAGE_B_CAPTURE_SHA256

_ROOT = Path(__file__).resolve().parents[1]
CALENDAR = _ROOT / "docs" / "research" / "3621-finra-si-calendar.csv"
COUNTS_OUT = _ROOT / "docs" / "research" / "3621-si-premise-counts.csv"
NAMES_OUT = _ROOT / "var" / "research" / "3621" / "si-premise-names.jsonl.gz"
POPULATIONS: tuple[str, ...] = ("micro", "small", "large", "mega", "top1000", "rest", "all")
STATES: tuple[str, ...] = ("ambiguous", "unmatched", "unverifiable", "identity_fail", "valid")
COUNTED: tuple[str, ...] = ("admitted", *STATES, "flagged", "split_carried")
SPLITS = f"inputs/{Frozen.SPLITS}"
#: FINRA's file catalog: before June 2021 the files "do not reflect short interest data in exchange-listed securities".
COVERAGE_START = date(2021, 6, 15)
#: The ADV-window predecessor of ``COVERAGE_START``; the calendar's fixed span.
CALENDAR_FIRST = date(2021, 5, 28)
CALENDAR_LAST_MONTH = (2024, 8)
SI_DECILE = 0.9
ADV_TOLERANCE = 1.2
TOLERANCE_GRID = (1.1, 1.15, 1.2, 1.25, 1.5, 2.0, 3.0)
WINDOW_SHIFTS = (0, 1, 2)
TOP = 1000


class MeasureError(RuntimeError):
    pass


@dataclass(frozen=True)
class FinraRow:
    short: int
    previous: int
    adv: int
    revised: bool
    issue_name: str


@dataclass(frozen=True)
class FinraFile:
    rows: dict[str, FinraRow]
    twice: set[str]
    physical_rows: int
    physical_revised: int
    zero_short: int


@dataclass(frozen=True)
class Reading:
    state: str
    used: date | None = None
    sir: float | None = None
    split_carried: bool = False
    issue_name: str | None = None
    log_ratio: float | None = None


def _lines(data: bytes) -> list[Any]:
    with gzip.open(io.BytesIO(data), "rt") as f:
        return [json.loads(line) for line in f]


def read_calendar(path: Path = CALENDAR) -> list[tuple[date, date]]:
    """(settlement, publication) pairs, ascending, validated."""
    with path.open() as f:
        rows = [
            (date.fromisoformat(r["settlement_date"]), date.fromisoformat(r["publication_date"]))
            for r in csv.DictReader(f)
        ]
    days = [s for s, _p in rows]
    if not days or days[0] != CALENDAR_FIRST:
        raise MeasureError(f"calendar must start at {CALENDAR_FIRST}")
    if days != sorted(set(days)) or any(p1 <= p0 for (_s0, p0), (_s1, p1) in zip(rows, rows[1:], strict=False)):
        raise MeasureError("calendar settlements not unique and ascending, or publications not strictly increasing")
    if any(p <= s for s, p in rows):
        raise MeasureError("a publication date not after its settlement")
    per_month = Counter((s.year, s.month) for s in days[1:])
    month = (CALENDAR_FIRST.year + CALENDAR_FIRST.month // 12, CALENDAR_FIRST.month % 12 + 1)
    required: set[tuple[int, int]] = set()
    while month <= CALENDAR_LAST_MONTH:
        required.add(month)
        if per_month[month] != 2:
            raise MeasureError(f"calendar month {month} holds {per_month[month]} settlements, not 2")
        month = (month[0] + month[1] // 12, month[1] % 12 + 1)
    if set(per_month) != required:
        raise MeasureError(f"calendar settlements outside the fixed span: {sorted(set(per_month) - required)}")
    return rows


def settlement_for(calendar: Sequence[tuple[date, date]], s_m: date) -> date | None:
    """The latest settlement whose publication is strictly before s(M); ``None`` unless it is on or after
    ``COVERAGE_START``."""
    published = [s for s, p in calendar if p < s_m]
    if not published or published[-1] < COVERAGE_START:
        return None
    used = published[-1]
    first_of_month = min(s for s, _p in calendar if (s.year, s.month) == (s_m.year, s_m.month))
    if used != first_of_month:
        raise MeasureError(f"s(M) {s_m}: settlement {used} is not the month's mid-month settlement {first_of_month}")
    return used


def adv_window(
    calendar: Sequence[tuple[date, date]], settlement: date, sessions: Sequence[date], shift: int = 0
) -> list[date]:
    """SPY sessions after the previous calendar settlement, through ``settlement`` (FINRA's ADV denominator); ``shift``
    moves both ends back that many calendar days (a sensitivity diagnostic only)."""
    days = [s for s, _p in calendar]
    i = days.index(settlement)
    if i == 0:
        raise MeasureError(f"{settlement}: no previous calendar settlement for the ADV window")
    return [d for d in sessions if days[i - 1] - timedelta(shift) < d <= settlement - timedelta(shift)]


def our_adv(
    volume: Mapping[date, float], window: Sequence[date], splits: Sequence[SplitStamp], settlement: date
) -> float | None:
    """FINRA's ADV on our bars: the window's split-carried volume over its session count; ``None`` when a window
    session has no usable bar."""
    if not window or any(d not in volume for d in window):
        return None
    return sum(volume[d] * float(split_product(splits, d, settlement)) for d in window) / len(window)


def identity_ok(finra_adv: int, ours: float, tolerance: float) -> bool:
    if finra_adv == 0 or ours == 0:
        return finra_adv == 0 and ours == 0
    return abs(math.log(finra_adv / ours)) <= math.log(tolerance)


def parse_file(payload: bytes, day: date) -> FinraFile:
    """Every physical row is counted; the symbol map keeps the last row of a duplicated symbol, which is ambiguous."""
    rows: dict[str, FinraRow] = {}
    twice: set[str] = set()
    physical = revised = zero = 0
    for row in csv.DictReader(io.StringIO(payload.decode("utf-8")), delimiter="|"):
        if parse_body_settlement_date(row.get("settlementDate")) != day:
            raise MeasureError(f"{day}: a body row carries settlementDate {row.get('settlementDate')!r}")
        try:
            short, adv = int(row["currentShortPositionQuantity"]), int(row["averageDailyVolumeQuantity"] or 0)
            previous = int(row["previousShortPositionQuantity"] or 0)
        except ValueError as exc:
            raise MeasureError(f"{day}: non-integer short count or ADV for {row['symbolCode']!r}") from exc
        if short < 0 or adv < 0 or previous < 0:
            raise MeasureError(f"{day}: negative short count or ADV for {row['symbolCode']!r}")
        flagged = bool((row["revisionFlag"] or "").strip())
        physical += 1
        revised += flagged
        zero += short == 0
        key = normalise_symbol(row["symbolCode"] or "")
        if key in rows:
            twice.add(key)
        rows[key] = FinraRow(short, previous, adv, flagged, row["issueName"] or "")
    return FinraFile(rows, twice, physical, revised, zero)


def read_name(
    key: str,
    files: Mapping[date, FinraFile],
    used: date,
    windows: Mapping[tuple[date, int], Sequence[date]],
    volume: Mapping[date, float],
    splits: Sequence[SplitStamp],
    shares: float,
    s_m: date,
) -> Reading:
    """One admitted name's state at a covered formation (the docstring's precedence, after ``ambiguous`` among
    admitted names)."""
    current = files[used]
    if key in current.twice:
        return Reading("ambiguous")
    if key not in current.rows:
        return Reading("unmatched")
    hit = current.rows[key]
    ours = our_adv(volume, windows[(used, 0)], splits, used)
    if ours is None:
        return Reading("unverifiable", used, issue_name=hit.issue_name)
    ratio = math.log(hit.adv / ours) if hit.adv > 0 and ours > 0 else None
    if not identity_ok(hit.adv, ours, ADV_TOLERANCE):
        return Reading("identity_fail", used, issue_name=hit.issue_name, log_ratio=ratio)
    product = split_product(splits, used, s_m)
    carried = any(used < stamp.day <= s_m for stamp in splits)
    sir = float(hit.short * product) / shares
    return Reading("valid", used, sir, carried, hit.issue_name, ratio)


def main(names_out: Path) -> None:
    calendar = read_calendar()
    keep = (NYSE_CUTOFFS, DAILY, SESSIONS, SPLITS)
    stage_b = verify_capture(capture_path(STAGE_B_CAPTURE_RUN), STAGE_B_CAPTURE_SHA256, keep).stage_b
    sessions = [date.fromisoformat(d) for d in _lines(stage_b.files[SESSIONS])]
    cutoffs = {(k, m): float(v) * 1e6 for k, m, v, *_ in _lines(stage_b.files[NYSE_CUTOFFS])}

    # Per M: (ME, name_key, series_id, symbol, shares, s(M), terminating).
    rows: dict[str, list[tuple[float, int, int, str, float, date, bool]]] = defaultdict(list)
    with gzip.open(io.BytesIO(stage_b.rows), "rt") as f:
        for line in f:
            row = json.loads(line)
            if row["exclusion"] is None:
                me = row["me"]
                shares = float(me["shares"])
                if not (math.isfinite(shares) and shares > 0):
                    raise MeasureError(f"{row['M']} {row['name_key']}: admitted with shares {me['shares']!r}")
                rows[row["M"]].append(
                    (
                        float(me["value"]),
                        row["name_key"],
                        row["series_id"],
                        row["symbol"] or "",
                        shares,
                        date.fromisoformat(row["s_M"]),
                        bool(row["terminating"]),
                    )
                )
    wanted = {r[2] for month in rows.values() for r in month}
    splits: dict[int, list[SplitStamp]] = defaultdict(list)
    for sid, day, factor in _lines(stage_b.files[SPLITS]):
        splits[sid].append(SplitStamp(date.fromisoformat(day), Decimal(factor)))
    volume: dict[int, dict[date, float]] = {}
    with gzip.open(io.BytesIO(stage_b.files[DAILY]), "rt") as f:
        for line in f:
            sid, bars = json.loads(line)
            if sid in wanted:
                volume[sid] = {
                    date.fromisoformat(d): float(v)
                    for d, _c, _a, v, _st, ok in bars
                    if ok and v is not None and math.isfinite(float(v)) and float(v) >= 0
                }

    used_by = {month: settlement_for(calendar, rows[month][0][5]) for month in rows}
    covered = {d for d in used_by.values() if d is not None}
    # Every in-coverage calendar file is read: the covered formations use ``covered``; all feed the revision check.
    needed = [s for s, _p in calendar if s >= COVERAGE_START]
    windows = {(d, shift): adv_window(calendar, d, sessions, shift) for d in needed for shift in WINDOW_SHIFTS}
    provider = FinraShortInterestProvider()
    files: dict[date, FinraFile] = {}
    digests: dict[date, str] = {}
    with psycopg.connect(settings.database_url) as conn:
        for day in needed:
            body = stored_body(
                conn, accession_number=f"FINRA_SI_{day:%Y%m%d}", document_kind="finra_short_interest_csv"
            )
            origin = "stored" if body is not None else "cdn"
            payload = body.encode("utf-8") if body is not None else provider.fetch_settlement_file(day)
            files[day] = parse_file(payload, day)
            digests[day] = hashlib.sha256(payload).hexdigest()
            print(
                f"file {day} {origin} sha256={digests[day]} rows={files[day].physical_rows} "
                f"revised={files[day].physical_revised} zero_short={files[day].zero_short} "
                f"duplicated_symbols={len(files[day].twice)}" + ("" if day in covered else " (revision check only)")
            )

    # FINRA's revisionFlag marks a revision of the PRIOR settlement's figure. If the stored prior file had been
    # revised in place, a flagged row's previousShortPositionQuantity would equal that file's current figure.
    revision = Counter[tuple[bool, bool]]()
    for prior, day in zip(needed, needed[1:], strict=False):
        before, now = files[prior], files[day]
        for key, row in now.rows.items():
            if key in before.rows and key not in now.twice and key not in before.twice:
                revision[(row.revised, row.previous == before.rows[key].short)] += 1

    counts: list[tuple[str, str, dict[str, int]]] = []
    log_ratios: list[float] = []
    tolerance_fail = Counter[float]()
    shift_fail: dict[str, Counter[int]] = {}
    by_link = Counter[tuple[str, str]]()
    audit: dict[int, list[tuple[date, str, str, float | None]]] = defaultdict(list)
    names_lines: list[str] = []
    cut_rows: list[tuple[str, date, float, int, int, float, int]] = []
    for month in sorted(rows):
        names = sorted(rows[month], key=lambda r: (-r[0], r[1]))
        s_m = names[0][5]
        used = used_by[month]
        top = {r[1] for r in names[:TOP]}
        readings: dict[int, Reading] = {}
        if used is not None:
            keys = Counter(normalise_symbol(r[3]) for r in names)
            shift_fail[month] = Counter()
            for _me, key, sid, symbol, shares, _s, _t in names:
                k = normalise_symbol(symbol)
                if not k or keys[k] > 1:
                    readings[key] = Reading("ambiguous")
                    continue
                reading = read_name(k, files, used, windows, volume.get(sid, {}), splits[sid], shares, s_m)
                readings[key] = reading
                if reading.issue_name is not None and reading.used is not None:
                    audit[sid].append((reading.used, reading.issue_name, reading.state, reading.log_ratio))
                if reading.state in ("identity_fail", "valid") and reading.used is not None:
                    hit = (files[reading.used].rows[k]).adv
                    ours = our_adv(volume.get(sid, {}), windows[(reading.used, 0)], splits[sid], reading.used)
                    assert ours is not None
                    if reading.log_ratio is not None:
                        log_ratios.append(reading.log_ratio)
                    for t in TOLERANCE_GRID:
                        tolerance_fail[t] += not identity_ok(hit, ours, t)
                    for shift in WINDOW_SHIFTS:
                        moved = our_adv(volume.get(sid, {}), windows[(reading.used, shift)], splits[sid], reading.used)
                        shift_fail[month][shift] += moved is None or not identity_ok(hit, moved, ADV_TOLERANCE)
            values = sorted(r.sir for r in readings.values() if r.sir is not None)
            if not values:
                raise MeasureError(f"{month}: covered formation with no valid SIR")
            q = values[math.ceil(SI_DECILE * len(values)) - 1]
            if q <= 0:
                raise MeasureError(f"{month}: SIR decile cutoff {q} is not positive")
            # Diagnostic: Asquith, Pathak & Ritter's unreported-is-zero rule puts unmatched names in N at 0.
            zeros = sum(r.state == "unmatched" for r in readings.values())
            padded = [0.0] * zeros + values
            q_zero = padded[math.ceil(SI_DECILE * len(padded)) - 1]
            moved = sum((v >= q) != (v >= q_zero) for v in values)
            cut_rows.append((month, used, q, sum(v > 1 for v in values), len(values), q_zero, moved))
        else:
            q = math.inf
        p20, p50, p80 = (cutoffs[(k, month)] for k in ("nyse_p20", "nyse_p50", "nyse_p80"))
        tallies = {p: dict.fromkeys(COUNTED, 0) for p in POPULATIONS}
        for me, key, _sid, _sym, _sh, _s, terminating in names:
            reading = readings.get(key, Reading("no_settlement"))
            flagged = reading.sir is not None and reading.sir >= q
            by_link[("linked" if key > 0 else "unlinked", reading.state)] += 1
            if terminating:
                by_link[("terminating", reading.state)] += 1
            if used is None:
                continue
            names_lines.append(
                json.dumps(
                    [month, key, reading.state, reading.used and reading.used.isoformat(), reading.sir, flagged],
                    separators=(",", ":"),
                )
            )
            size = "micro" if me < p20 else "small" if me < p50 else "large" if me < p80 else "mega"
            for population in (size, "top1000" if key in top else "rest", "all"):
                t = tallies[population]
                t["admitted"] += 1
                t[reading.state] += 1
                t["flagged"] += flagged
                t["split_carried"] += reading.split_carried
        if used is not None:
            counts.extend((month, p, tallies[p]) for p in POPULATIONS)

    with COUNTS_OUT.open("w", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["M", "population", *COUNTED])
        for month, population, tally in counts:
            writer.writerow([month, population, *(tally[c] for c in COUNTED)])
    names_bytes = ("\n".join(names_lines) + "\n").encode()
    names_out.parent.mkdir(parents=True, exist_ok=True)
    names_out.write_bytes(gzip.compress(names_bytes, mtime=0))
    print()
    print(
        f"per-name states: {len(names_lines)} lines -> {names_out}, "
        f"sha256(uncompressed)={hashlib.sha256(names_bytes).hexdigest()}"
    )
    print(f"counts -> {COUNTS_OUT}, sha256={hashlib.sha256(COUNTS_OUT.read_bytes()).hexdigest()}")
    print(f"payloads: {len(covered)} used by covered formations, {len(digests)} read")
    print("revision check (flagged, previous == prior file's current):", dict(sorted(revision.items())))

    print()
    print("formations", len(rows), "covered", len(cut_rows))
    print("population", "count", "min", "median", "max", sep="\t")
    for population in POPULATIONS:
        per = [t for _m, p, t in counts if p == population]
        for name in COUNTED:
            series = [t[name] for t in per]
            print(population, name, min(series), statistics.median(series), max(series), sep="\t")
        share = [t["valid"] / t["admitted"] for t in per]
        low, mid, high = min(share), statistics.median(share), max(share)
        print(population, "valid_share", f"{low:.4f}", f"{mid:.4f}", f"{high:.4f}", sep="\t")
    print()
    print("identity: ln(FINRA ADV / our ADV) over checked matches with both positive:", len(log_ratios))
    qs = statistics.quantiles(log_ratios, n=100)
    for p in (1, 5, 25, 50, 75, 95, 99):
        print(f"  p{p}\t{qs[p - 1]:+.4f}\tratio {math.exp(qs[p - 1]):.4f}")
    for t in TOLERANCE_GRID:
        print(f"  identity_fail at tolerance {t}: {tolerance_fail[t]}")
    print("window shift (days back) -> identity failures at the frozen tolerance, every covered formation")
    total = Counter[int]()
    for month, fails in sorted(shift_fail.items()):
        total.update(fails)
        print(f"  {month}", *(f"shift{s}={fails[s]}" for s in WINDOW_SHIFTS), sep="\t")
    print("  total", *(f"shift{s}={total[s]}" for s in WINDOW_SHIFTS), sep="\t")
    print()
    print("name-months by link status and state (all formations)")
    for (group, st), n in sorted(by_link.items()):
        print(group, st, n, sep="\t")
    print()

    def norm(name: str) -> str:
        return re.sub(r"[^A-Z0-9]", "", name.upper())

    changed = {sid: seen for sid, seen in audit.items() if len({norm(n) for _d, n, _s, _r in seen}) > 1}
    accepted = {sid for sid, seen in changed.items() if len({norm(n) for _d, n, s, _r in seen if s == "valid"}) > 1}
    print(
        f"audit: series whose matched FINRA issueName changes: {len(changed)} of {len(audit)}; "
        f"with the change among accepted matches: {len(accepted)}"
    )
    for sid, seen in sorted(changed.items()):
        first: dict[str, tuple[date, str, float | None]] = {}
        for day, name, state, ratio in seen:
            first.setdefault(norm(name), (day, state, ratio))
        print(
            sid,
            *(f"{d}:{n[:28]}:{s}:{'-' if r is None else f'{math.exp(r):.3f}'}" for n, (d, s, r) in first.items()),
            sep="\t",
        )
    print()
    print("M", "settlement", "q", "SIR > 1", "valid", "q unreported-as-zero", "flags moved", sep="\t")
    for month, day, q, over, n, q_zero, moved in cut_rows:
        print(month, day, f"{q:.4f}", over, n, f"{q_zero:.4f}", moved, sep="\t")


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else NAMES_OUT)
