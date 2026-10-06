"""#3609 step 2 slice 1: the FF-12 SIC map and the QMJ pin."""

from __future__ import annotations

import hashlib
import io
import zipfile
from collections.abc import Callable
from pathlib import Path

import pytest

from app.services.factor_book_reference import (
    FF12_OTHER,
    SICCODES12_PATH,
    SICCODES12_SHA256,
    load_ff12,
    parse_siccodes12,
)
from app.services.factor_panel_reference import ReferenceInputError

#: Transcribed by hand from French's Siccodes12.txt (Last-Modified 2020-01-08), independently of the parser.
EXPECTED: dict[str, tuple[tuple[int, int], ...]] = {
    "NoDur": ((100, 999), (2000, 2399), (2700, 2749), (2770, 2799), (3100, 3199), (3940, 3989)),
    "Durbl": (
        (2500, 2519),
        (2590, 2599),
        (3630, 3659),
        (3710, 3711),
        (3714, 3714),
        (3716, 3716),
        (3750, 3751),
        (3792, 3792),
        (3900, 3939),
        (3990, 3999),
    ),
    "Manuf": (
        (2520, 2589),
        (2600, 2699),
        (2750, 2769),
        (3000, 3099),
        (3200, 3569),
        (3580, 3629),
        (3700, 3709),
        (3712, 3713),
        (3715, 3715),
        (3717, 3749),
        (3752, 3791),
        (3793, 3799),
        (3830, 3839),
        (3860, 3899),
    ),
    "Enrgy": ((1200, 1399), (2900, 2999)),
    "Chems": ((2800, 2829), (2840, 2899)),
    "BusEq": ((3570, 3579), (3660, 3692), (3694, 3699), (3810, 3829), (7370, 7379)),
    "Telcm": ((4800, 4899),),
    "Utils": ((4900, 4949),),
    "Shops": ((5000, 5999), (7200, 7299), (7600, 7699)),
    "Hlth": ((2830, 2839), (3693, 3693), (3840, 3859), (8000, 8099)),
    "Money": ((6000, 6999),),
    "Other": (),
}


def _expected_industry(sic: int) -> str:
    hits = [name for name, ranges in EXPECTED.items() if any(low <= sic <= high for low, high in ranges)]
    assert len(hits) <= 1
    return hits[0] if hits else FF12_OTHER


def _zip(text: str, member: str = "Siccodes12.txt") -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(member, text)
    return output.getvalue()


Industry = tuple[int, str, list[str]]


def _text(industries: list[Industry]) -> str:
    blocks = [
        f"{number:2d} {short}  Description\r\n" + "".join(f"          {r}\r\n" for r in ranges)
        for number, short, ranges in industries
    ]
    return "\r\n".join(blocks)


def _valid() -> list[Industry]:
    return [(n, f"Ind{n}", [f"{n * 100:04d}-{n * 100 + 49:04d}"]) for n in range(1, 12)] + [(12, "Other", [])]


def test_committed_zip_is_the_pinned_file_and_parses_to_the_transcription() -> None:
    assert hashlib.sha256(SICCODES12_PATH.read_bytes()).hexdigest() == SICCODES12_SHA256
    ff12 = load_ff12()
    assert [item.number for item in ff12.industries] == list(range(1, 13))
    assert {item.short: item.ranges for item in ff12.industries} == EXPECTED


def test_every_sic_code_maps_as_transcribed() -> None:
    """All 10,000 codes, so every range boundary and its neighbours on both sides are covered."""
    ff12 = load_ff12()
    mismatches = [sic for sic in range(10_000) if ff12.industry(sic) != _expected_industry(sic)]
    assert mismatches == []


@pytest.mark.parametrize(
    ("sic", "industry"),
    [
        (99, FF12_OTHER),  # below NoDur's first range
        (100, "NoDur"),
        (999, "NoDur"),
        (1000, FF12_OTHER),  # metal mining
        (1311, "Enrgy"),
        (3692, "BusEq"),
        (3693, "Hlth"),  # one-code range between two BusEq ranges
        (3694, "BusEq"),
        (3713, "Manuf"),
        (3714, "Durbl"),
        (3715, "Manuf"),
        (6221, "Money"),  # commodity pools' SIC (spec premise 3)
        (7000, FF12_OTHER),  # hotels, just past Money
        (7372, "BusEq"),
        (9999, FF12_OTHER),
    ],
)
def test_named_codes(sic: int, industry: str) -> None:
    assert load_ff12().industry(sic) == industry


@pytest.mark.parametrize("sic", [-1, 10_000])
def test_out_of_range_sic_is_refused(sic: int) -> None:
    with pytest.raises(ValueError, match="4-digit"):
        load_ff12().industry(sic)


def test_synthetic_file_parses() -> None:
    ff12 = parse_siccodes12(_zip(_text(_valid())))
    assert ff12.industry(149) == "Ind1"
    assert ff12.industry(150) == FF12_OTHER


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda rows: [*rows[:10], (11, "Ind11", ["0120-0130"]), rows[11]], "overlap"),
        (lambda rows: [*rows[:11], (12, "Other", ["9000-9001"])], "no ranges"),
        (lambda rows: [*rows[:11], (12, "Misc", [])], "no ranges"),
        (lambda rows: [*rows[:10], (11, "Ind11", []), rows[11]], "at least one range"),
        (lambda rows: rows[:11], "industry numbers"),
        (lambda rows: [*rows[:10], (11, "Ind11", ["1150-1100"]), rows[11]], "range line"),
        (lambda rows: [*rows[:10], (11, "Ind11", ["115-1199"]), rows[11]], "range line"),
    ],
)
def test_malformed_files_are_refused(mutate: Callable[[list[Industry]], list[Industry]], message: str) -> None:
    with pytest.raises(ReferenceInputError, match=message):
        parse_siccodes12(_zip(_text(mutate(_valid()))))


def test_unexpected_zip_member_is_refused() -> None:
    with pytest.raises(ReferenceInputError, match="members"):
        parse_siccodes12(_zip(_text(_valid()), member="other.txt"))


def test_changed_file_is_refused_by_the_pin(tmp_path: Path) -> None:
    changed = tmp_path / "Siccodes12.zip"
    changed.write_bytes(_zip(_text(_valid())))
    with pytest.raises(ReferenceInputError, match="does not match the pinned"):
        load_ff12(changed)
