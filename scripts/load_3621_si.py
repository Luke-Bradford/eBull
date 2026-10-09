"""#3621 slice 5b-2a: the SI readings of a stage-B artefact, and slice 5c's reproduction of premise 2.

Spec: ``docs/research/2026-10-09-3621-slice5-short-interest.md`` §"Source rules", §"Slices" item 5c (PR #3734).
``app.services.short_interest_flag`` (slice 5a) reads each name; this module feeds it from a verified stage-B
artefact and the stored FINRA payloads, and checks the result against the premise run:

- :func:`read_payloads`: every payload the manifest ``docs/research/3621-si-premise-payloads.csv`` lists (78, the
  revision-check-only files included), each through ``fetch`` (the run's ``stored_body``), refused when missing,
  when its sha256 or physical row count differs from the manifest, or when the manifest's settlements are not the
  calendar's in-coverage settlements;
- :func:`si_names`: per formation, each admitted name's ``SiName``: the stage-B row's own ``symbol`` (as the premise
  reads it, not the ADMITTED file's) and ``me.shares``, the usable volume of its frozen daily bars and its split
  stamps;
- :func:`read_si`: per formation, the settlement used, every name's reading, q and the flagged names;
- :func:`reproduce_si`: the counts table must equal ``docs/research/3621-si-premise-counts.csv`` cell for cell and
  the per-name file's uncompressed sha256 must equal the premise run's, or the run stops.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
from collections import defaultdict
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

from app.services.factor_panel import SplitStamp
from app.services.factor_panel_prices import DailyBar
from app.services.short_interest_flag import (
    COVERAGE_START,
    Calendar,
    FinraFile,
    Reading,
    SiName,
    accession_for,
    adv_window,
    flag_formation,
    parse_file,
    read_formation,
    settlement_for,
    usable_volume,
)
from scripts.report_3621_books import Population
from scripts.report_3621_si import COUNTED, counts_rows, si_counts, si_name_lines

_ROOT: Final = Path(__file__).resolve().parents[1]
CALENDAR_PATH: Final = _ROOT / "docs" / "research" / "3621-finra-si-calendar.csv"
PAYLOADS_PATH: Final = _ROOT / "docs" / "research" / "3621-si-premise-payloads.csv"
COUNTS_PATH: Final = _ROOT / "docs" / "research" / "3621-si-premise-counts.csv"
#: The premise run's per-name file, uncompressed (addendum §"Premises", provenance).
PREMISE_NAMES_SHA256: Final = "c005ca8a83b3c8e2cb7c38afd17a7db55e778ced8711ef636b0fcc8af0d91b6d"
_PAYLOAD_COLUMNS: Final = ["settlement_date", "origin", "sha256", "physical_rows", "used_by_covered_formation"]


class SiLoadError(RuntimeError):
    """The payloads, the stage-B inputs or the reproduction do not hold; the run stops."""


def _lines(payload: bytes) -> Iterator[Any]:
    with gzip.GzipFile(fileobj=io.BytesIO(payload)) as handle:
        for line in handle:
            yield json.loads(line)


@dataclass(frozen=True)
class PayloadPin:
    settlement: date
    sha256: str
    physical_rows: int


def read_payload_manifest(text: str, calendar: Calendar) -> list[PayloadPin]:
    """The manifest's rows, ascending; refused unless its settlements are exactly the calendar's settlements on or
    after ``COVERAGE_START``."""
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames != _PAYLOAD_COLUMNS:
        raise SiLoadError(f"payload manifest columns {reader.fieldnames}, expected {_PAYLOAD_COLUMNS}")
    pins = [PayloadPin(date.fromisoformat(r["settlement_date"]), r["sha256"], int(r["physical_rows"])) for r in reader]
    wanted = [s for s, _p in calendar if s >= COVERAGE_START]
    if [p.settlement for p in pins] != wanted:
        raise SiLoadError("the payload manifest's settlements are not the calendar's in-coverage settlements")
    return pins


def read_payloads(pins: Sequence[PayloadPin], fetch: Callable[[str], str | None]) -> dict[date, FinraFile]:
    """Every pinned payload, parsed. ``fetch`` returns the stored body for an accession, ``None`` when absent."""
    files: dict[date, FinraFile] = {}
    for pin in pins:
        body = fetch(accession_for(pin.settlement))
        if body is None:
            raise SiLoadError(f"{accession_for(pin.settlement)} is not stored")
        payload = body.encode("utf-8")
        if hashlib.sha256(payload).hexdigest() != pin.sha256:
            raise SiLoadError(f"{accession_for(pin.settlement)} sha256 differs from the payload manifest")
        parsed = parse_file(payload, pin.settlement)
        if parsed.physical_rows != pin.physical_rows:
            raise SiLoadError(
                f"{accession_for(pin.settlement)} holds {parsed.physical_rows} rows, the manifest {pin.physical_rows}"
            )
        files[pin.settlement] = parsed
    return files


def si_names(rows: bytes, daily: bytes, splits: bytes) -> dict[date, dict[int, SiName]]:
    """Each formation's admitted names (``exclusion`` null), keyed by ``name_key``. Refuses an admitted series with no
    frozen daily bars."""
    admitted: dict[date, list[tuple[int, int, str, float]]] = defaultdict(list)
    for row in _lines(rows):
        if row["exclusion"] is None:
            admitted[date.fromisoformat(row["M"])].append(
                (row["name_key"], row["series_id"], row["symbol"] or "", float(row["me"]["shares"]))
            )
    wanted = {sid for names in admitted.values() for _k, sid, _s, _sh in names}
    stamps: dict[int, list[SplitStamp]] = defaultdict(list)
    for sid, day, factor in _lines(splits):
        stamps[sid].append(SplitStamp(date.fromisoformat(day), Decimal(factor)))
    volume: dict[int, dict[date, float]] = {}
    for sid, raw in _lines(daily):
        if sid in wanted:
            volume[sid] = usable_volume(DailyBar(date.fromisoformat(d), c, a, v, st, ok) for d, c, a, v, st, ok in raw)
    if absent := sorted(wanted - volume.keys()):
        raise SiLoadError(f"{len(absent)} admitted series have no frozen daily bars: {absent[:5]}")
    return {
        m: {key: SiName(symbol, shares, volume[sid], stamps[sid]) for key, sid, symbol, shares in names}
        for m, names in admitted.items()
    }


@dataclass(frozen=True)
class FormationSi:
    """One formation's SI: the settlement used (``None`` when uncovered), every admitted name's reading, q and the
    flagged names."""

    used: date | None
    readings: dict[int, Reading]
    q: float | None
    flagged: frozenset[int]


def read_si(
    calendar: Calendar,
    files: Mapping[date, FinraFile],
    sessions: Sequence[date],
    s_m: date,
    names: Mapping[int, SiName],
) -> FormationSi:
    used = settlement_for(calendar, s_m)
    window = [] if used is None else adv_window(calendar, used, sessions)
    readings = read_formation(names, None if used is None else files.get(used), used, window, s_m)
    q, flagged = flag_formation(readings, used)
    return FormationSi(used, readings, q, flagged)


@dataclass(frozen=True)
class SiFormation:
    """What the reproduction reads at one formation: its populations, each admitted name's ME and its SI."""

    formation: date
    pops: Mapping[Population, frozenset[int]]
    me: Mapping[int, float]
    si: FormationSi


def reproduce_si(
    formations: Sequence[SiFormation], reference_csv: str, names_sha256: str = PREMISE_NAMES_SHA256
) -> tuple[int, str]:
    """Refuses unless the covered formations' counts equal ``reference_csv`` cell for cell and the per-name file's
    uncompressed sha256 equals ``names_sha256``; returns (covered formations, that sha256)."""
    covered = sorted((f for f in formations if f.si.used is not None), key=lambda f: f.formation)
    got: list[list[str]] = []
    names: list[str] = []
    for f in covered:
        got.extend(counts_rows(f.formation, si_counts(f.pops, f.si.readings, f.si.flagged)))
        names.extend(si_name_lines(f.formation, f.me, f.si.readings, f.si.flagged))
    reader = csv.reader(io.StringIO(reference_csv))
    if (header := next(reader, None)) != ["M", "population", *COUNTED]:
        raise SiLoadError(f"REPRODUCTION: premise 2's header is {header}, not M, population and {list(COUNTED)}")
    expected = list(reader)
    if got != expected:
        differ = [g[:2] for g, e in zip(got, expected, strict=False) if g != e]
        raise SiLoadError(
            f"REPRODUCTION: {len(got)} count rows against premise 2's {len(expected)}; first differing: {differ[:3]}"
        )
    digest = hashlib.sha256(("\n".join(names) + "\n").encode()).hexdigest()
    if digest != names_sha256:
        raise SiLoadError(f"REPRODUCTION: per-name sha256 {digest}, premise 2's {names_sha256}")
    return len(covered), digest
