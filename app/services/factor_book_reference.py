"""Reference inputs for the #3609 step 2 factor book (slice 1).

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"Slices" item 1 (PR #3666) and §"Source rules".

* **Fama-French 12 industries.** French's data library ``Siccodes12.zip`` lists, per industry, the inclusive SIC
  ranges it covers; industry 12 ("Other") lists none and takes every code outside the other eleven. The ZIP is
  committed byte-for-byte at :data:`SICCODES12_PATH` and refused on any sha256 change. A NULL or unloaded SIC is
  not "Other": the step 2 report buckets it as unclassified, so :meth:`Ff12Map.industry` takes an ``int`` only.
* **QMJ.** The z-score and composite rules cite Asness, Frazzini & Pedersen's AQR working paper (the draft of
  2013-10-09 that AQR serves), pinned here by hash.

Kept apart from ``factor_panel_reference``: that module is inside step 1's hashed construction closure, so an edit
there would change stage A's construction versions.
"""

from __future__ import annotations

import bisect
import hashlib
import io
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from app.services.factor_panel_reference import ReferenceInputError
from app.services.reference_data import FRENCH_FTP

SICCODES12_URL: Final = f"{FRENCH_FTP}/Siccodes12.zip"
#: Downloaded 2026-10-06 (``Last-Modified: Wed, 08 Jan 2020 22:59:24 GMT``, 881 bytes).
SICCODES12_SHA256: Final = "d801141acf039f2e06e6d4d9ba2b3992e9747a1d82fabd53ef21da4a3af79fff"
SICCODES12_PATH: Final = Path(__file__).resolve().parents[2] / "docs" / "research" / "3609-ff12-Siccodes12.zip"
FF12_OTHER: Final = "Other"

QMJ_PDF_URL: Final = "https://www.aqr.com/-/media/AQR/Documents/Insights/Working-Papers/Quality-Minus-Junk.pdf"
#: Downloaded 2026-10-06 (``Last-Modified: Thu, 21 Feb 2019 22:26:48 GMT``); title page "This draft: October 9, 2013".
QMJ_PDF_SHA256: Final = "761c42f91d5f00fa75c8fb2b3722530562096de9491c80badf9e5b4091fbb2a9"

_HEADER: Final = re.compile(r" ?(\d{1,2}) (\S+) +(\S.*)")
_RANGE: Final = re.compile(r" +(\d{4})-(\d{4})")


@dataclass(frozen=True)
class Ff12Industry:
    number: int
    short: str
    description: str
    #: Inclusive ``(low, high)`` SIC ranges, in file order; empty for "Other".
    ranges: tuple[tuple[int, int], ...]


@dataclass(frozen=True)
class Ff12Map:
    industries: tuple[Ff12Industry, ...]
    #: Every listed range as ``(low, high, short)``, sorted by ``low``; checked non-overlapping by the parser.
    bounds: tuple[tuple[int, int, str], ...]

    def industry(self, sic: int) -> str:
        """The FF-12 short name for one SIC code; codes in no listed range are "Other"."""
        if not 0 <= sic <= 9999:
            raise ValueError(f"SIC {sic} is not a 4-digit code")
        position = bisect.bisect_right(self.bounds, sic, key=lambda bound: bound[0]) - 1
        if position >= 0 and sic <= self.bounds[position][1]:
            return self.bounds[position][2]
        return FF12_OTHER


def parse_siccodes12(payload: bytes) -> Ff12Map:
    """Parse and validate French's ``Siccodes12.zip``: twelve numbered industries, the last "Other" with no
    ranges, every other with at least one ``low-high`` range, and no SIC code in two ranges."""
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        if archive.namelist() != ["Siccodes12.txt"]:
            raise ReferenceInputError(f"Siccodes12.zip members {archive.namelist()}; expected ['Siccodes12.txt']")
        text = archive.read("Siccodes12.txt").decode("ascii")
    industries: list[Ff12Industry] = []
    for block in re.split(r"\r?\n\s*\r?\n", text.strip("\r\n")):
        header, *lines = block.splitlines()
        match = _HEADER.fullmatch(header)
        if match is None:
            raise ReferenceInputError(f"Siccodes12 industry header {header!r}")
        ranges: list[tuple[int, int]] = []
        for line in lines:
            found = _RANGE.fullmatch(line)
            if found is None or int(found.group(1)) > int(found.group(2)):
                raise ReferenceInputError(f"Siccodes12 range line {line!r}")
            ranges.append((int(found.group(1)), int(found.group(2))))
        industries.append(Ff12Industry(int(match.group(1)), match.group(2), match.group(3), tuple(ranges)))
    if [item.number for item in industries] != list(range(1, 13)):
        raise ReferenceInputError(f"Siccodes12 industry numbers {[item.number for item in industries]}")
    if industries[-1].short != FF12_OTHER or industries[-1].ranges:
        raise ReferenceInputError("Siccodes12 industry 12 must be 'Other' with no ranges")
    if any(not item.ranges for item in industries[:-1]):
        raise ReferenceInputError("Siccodes12 industries 1-11 must each list at least one range")
    bounds = sorted((low, high, item.short) for item in industries for low, high in item.ranges)
    for (_, high, short), (low, _, other) in zip(bounds, bounds[1:], strict=False):
        if low <= high:
            raise ReferenceInputError(f"Siccodes12 ranges overlap at SIC {low}: {short} and {other}")
    return Ff12Map(tuple(industries), tuple(bounds))


def load_ff12(path: Path = SICCODES12_PATH) -> Ff12Map:
    """The committed ``Siccodes12.zip``, refused unless its sha256 is :data:`SICCODES12_SHA256`."""
    payload = path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    if digest != SICCODES12_SHA256:
        raise ReferenceInputError(f"{path.name} sha256 {digest} does not match the pinned {SICCODES12_SHA256}")
    return parse_siccodes12(payload)


__all__ = [
    "FF12_OTHER",
    "QMJ_PDF_SHA256",
    "QMJ_PDF_URL",
    "SICCODES12_PATH",
    "SICCODES12_SHA256",
    "SICCODES12_URL",
    "Ff12Industry",
    "Ff12Map",
    "load_ff12",
    "parse_siccodes12",
]
