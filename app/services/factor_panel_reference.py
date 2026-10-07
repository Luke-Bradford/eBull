"""Reference inputs for the #3609 step 1 factor panel that are not monthly series (slice 1).

Spec: ``docs/research/2026-10-04-3609-step1-factor-panel.md`` §"Slices" item 1.

* **JKP Table 9 signs.** The factor sign the panel uses is JKP's Table 9 "Sign" column
  (Documentation.pdf, pinned below), transcribed into ``docs/research/3609-jkp-table9-signs.csv``.
  :func:`table9_direction_conflicts` checks the transcription against the ``direction`` column of the
  loaded JKP returns file.
* **FSDS SUB.** SEC DERA Financial Statement Data Sets ``sub.txt``: one row per submission, keyed by
  accession (``adsh``), carrying the SIC "assigned by the Commission as of the filing date" (DERA
  readme). The panel reads ``sic``, ``form`` and ``accepted`` by exact accession.
"""

from __future__ import annotations

import csv
import io
import re
import zipfile
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Final

JKP_DOCUMENTATION_URL: Final = "https://jkpfactors-data.s3.amazonaws.com/documents/Documentation.pdf"
#: Downloaded 2026-10-04 (``Last-Modified: Mon, 17 Nov 2025 05:27:00 GMT``).
JKP_DOCUMENTATION_SHA256: Final = "141c9571f00445937356328e22fd030e69dea4b68dea986f85dda6859ca3cc7d"
TABLE9_SIGNS_PATH: Final = Path(__file__).resolve().parents[2] / "docs" / "research" / "3609-jkp-table9-signs.csv"
#: Table 9 prints Heston & Sadka's years 16-20 annual lag with sign -1 and the non-annual lag with +1;
#: the published returns file signs them the other way round. Neither is a step-1 characteristic. A
#: conflict outside this set refuses publication.
KNOWN_TABLE9_DIRECTION_CONFLICTS: Final = frozenset({"seas_16_20an", "seas_16_20na"})

FSDS_SUB_URL: Final = "https://www.sec.gov/files/dera/data/financial-statement-data-sets/{quarter}.zip"
#: Stage A's SIC reads: formations 2014-09 .. 2021-04, with filer look-backs to 2012Q1 (spec §"Slices").
FSDS_SUB_FIRST_QUARTER: Final = (2012, 1)
FSDS_SUB_LAST_QUARTER: Final = (2021, 2)
_SUB_COLUMNS: Final = ("adsh", "cik", "sic", "form", "accepted")
_ADSH: Final = re.compile(r"\d{10}-\d{2}-\d{6}")


class ReferenceInputError(ValueError):
    """A reference input does not satisfy its source contract; publication is refused."""


def load_table9_signs(path: Path = TABLE9_SIGNS_PATH) -> dict[str, int]:
    return parse_table9_signs(path.read_bytes())


def parse_table9_signs(payload: bytes) -> dict[str, int]:
    reader = csv.DictReader(io.StringIO(payload.decode("utf-8"), newline=""))
    if tuple(reader.fieldnames or ()) != ("name", "sign"):
        raise ReferenceInputError(f"Table 9 CSV header {reader.fieldnames!r}; expected ('name', 'sign')")
    signs: dict[str, int] = {}
    for row in reader:
        if row["sign"] not in ("1", "-1") or not row["name"] or row["name"] in signs:
            raise ReferenceInputError(f"Table 9 CSV row is invalid or repeated: {row!r}")
        signs[row["name"]] = int(row["sign"])
    return signs


def jkp_directions(payload: bytes) -> dict[str, int]:
    """Each factor's ``direction`` in a JKP ``[all_factors]`` returns ZIP; one value per factor."""
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        (name,) = [member for member in archive.namelist() if not member.endswith("/")]
        text = archive.read(name).decode("utf-8-sig")
    seen: dict[str, set[str]] = {}
    for row in csv.DictReader(io.StringIO(text)):
        seen.setdefault(row["name"], set()).add(row["direction"])
    directions: dict[str, int] = {}
    for factor, values in seen.items():
        if len(values) != 1 or next(iter(values)) not in ("1", "-1"):
            raise ReferenceInputError(f"JKP factor {factor} has direction values {sorted(values)}")
        directions[factor] = int(next(iter(values)))
    return directions


def table9_direction_conflicts(signs: Mapping[str, int], directions: Mapping[str, int]) -> frozenset[str]:
    """Factors whose Table 9 sign differs from the returns file's direction; the factor sets must match."""
    if set(signs) != set(directions):
        only_table = sorted(set(signs) - set(directions))
        only_file = sorted(set(directions) - set(signs))
        raise ReferenceInputError(f"factor sets differ: Table 9 only {only_table}; returns file only {only_file}")
    return frozenset(name for name, sign in signs.items() if directions[name] != sign)


def fsds_sub_quarters() -> tuple[str, ...]:
    year, quarter = FSDS_SUB_FIRST_QUARTER
    quarters: list[str] = []
    while (year, quarter) <= FSDS_SUB_LAST_QUARTER:
        quarters.append(f"{year}q{quarter}")
        year, quarter = (year + 1, 1) if quarter == 4 else (year, quarter + 1)
    return tuple(quarters)


@dataclass(frozen=True)
class SubRecord:
    adsh: str
    cik: int
    #: ``None`` when the filed value is NULL; the readme marks ``sic`` nullable.
    sic: int | None
    form: str
    #: As printed (``yyyy-mm-dd hh:mm:ss``). The readme states no time zone.
    accepted: datetime


@dataclass(frozen=True)
class SubFile:
    quarter: str
    records: tuple[SubRecord, ...]
    sic_null: int
    #: Rows whose acceptance date falls outside the calendar quarter the file is named for.
    accepted_outside_quarter: int
    forms: Mapping[str, int]


def _quarter_bounds(quarter: str) -> tuple[date, date]:
    match = re.fullmatch(r"(\d{4})q([1-4])", quarter)
    if match is None:
        raise ValueError(f"not an FSDS quarter: {quarter!r}")
    year, number = int(match.group(1)), int(match.group(2))
    start = date(year, 3 * number - 2, 1)
    end = date(year + 1, 1, 1) if number == 4 else date(year, 3 * number + 1, 1)
    return start, end


def parse_fsds_sub(payload: bytes, *, quarter: str) -> SubFile:
    """Parse and validate one quarter's ``sub.txt`` (tab-separated, no quoting, Latin-1)."""
    start, end = _quarter_bounds(quarter)
    lines = payload.decode("latin-1").split("\n")
    header = lines[0].rstrip("\r").split("\t")
    missing = [column for column in _SUB_COLUMNS if column not in header]
    if missing:
        raise ReferenceInputError(f"{quarter} sub.txt lacks columns {missing}")
    index = {column: header.index(column) for column in _SUB_COLUMNS}
    records: list[SubRecord] = []
    seen: set[str] = set()
    for line_number, line in enumerate(lines[1:], start=2):
        if not line.strip():
            continue
        row = line.rstrip("\r").split("\t")
        if len(row) != len(header):
            raise ReferenceInputError(
                f"{quarter} sub.txt line {line_number}: {len(row)} fields, header has {len(header)}"
            )
        adsh, cik, sic, form, accepted = (row[index[column]].strip() for column in _SUB_COLUMNS)
        if not _ADSH.fullmatch(adsh) or adsh in seen:
            raise ReferenceInputError(f"{quarter} sub.txt line {line_number}: invalid or repeated adsh {adsh!r}")
        seen.add(adsh)
        if not cik.isdigit() or not form:
            raise ReferenceInputError(f"{quarter} sub.txt line {line_number}: invalid cik {cik!r} or empty form")
        if sic and (not sic.isdigit() or not 100 <= int(sic) <= 9999):
            raise ReferenceInputError(f"{quarter} sub.txt line {line_number}: sic {sic!r} is not a 3-4 digit code")
        try:
            when = datetime.strptime(accepted.removesuffix(".0"), "%Y-%m-%d %H:%M:%S")
        except ValueError as exc:
            raise ReferenceInputError(f"{quarter} sub.txt line {line_number}: accepted {accepted!r}") from exc
        records.append(SubRecord(adsh, int(cik), int(sic) if sic else None, form, when))
    if not records:
        raise ReferenceInputError(f"{quarter} sub.txt has no rows")
    return SubFile(
        quarter=quarter,
        records=tuple(records),
        sic_null=sum(1 for record in records if record.sic is None),
        accepted_outside_quarter=sum(1 for record in records if not start <= record.accepted.date() < end),
        forms=dict(Counter(record.form for record in records)),
    )


def read_sub_member(archive_path: Path) -> bytes:
    """The ``sub.txt`` bytes of one FSDS quarter ZIP."""
    with zipfile.ZipFile(archive_path) as archive:
        if "sub.txt" not in archive.namelist():
            raise ReferenceInputError(f"{archive_path.name} has no sub.txt")
        return archive.read("sub.txt")


__all__ = [
    "FSDS_SUB_URL",
    "JKP_DOCUMENTATION_SHA256",
    "JKP_DOCUMENTATION_URL",
    "KNOWN_TABLE9_DIRECTION_CONFLICTS",
    "TABLE9_SIGNS_PATH",
    "ReferenceInputError",
    "SubFile",
    "SubRecord",
    "fsds_sub_quarters",
    "jkp_directions",
    "load_table9_signs",
    "parse_fsds_sub",
    "read_sub_member",
    "table9_direction_conflicts",
]
