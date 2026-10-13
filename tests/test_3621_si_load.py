"""#3621 slice 5b-2a: SI readings from a stage-B artefact and 5c's reproduction (``scripts/load_3621_si.py``)."""

from __future__ import annotations

import gzip
import hashlib
import json
from datetime import date, timedelta
from typing import Any

import pytest

from app.services.short_interest_flag import SiState, accession_for, read_calendar_csv
from scripts.load_3621_si import (
    CALENDAR_PATH,
    COUNTS_PATH,
    PAYLOADS_PATH,
    PayloadPin,
    SiFormation,
    SiLoadError,
    read_payload_manifest,
    read_payloads,
    read_si,
    reproduce_si,
    si_names,
)
from scripts.report_3621_books import Population

CALENDAR = read_calendar_csv(CALENDAR_PATH.read_text())
JUNE_15 = date(2021, 6, 15)
S_JUNE = date(2021, 6, 30)
S_MAY = date(2021, 5, 28)
HEADER = (
    "accountingYearMonthNumber|symbolCode|issueName|issuerServicesGroupExchangeCode|marketClassCode|"
    "currentShortPositionQuantity|previousShortPositionQuantity|stockSplitFlag|averageDailyVolumeQuantity|"
    "daysToCoverQuantity|revisionFlag|changePercent|changePreviousNumber|settlementDate"
)


def _sessions(start: date, end: date) -> list[date]:
    days = (start + timedelta(i) for i in range((end - start).days + 1))
    return [d for d in days if d.weekday() < 5]


SESSIONS = _sessions(date(2021, 5, 3), date(2021, 7, 30))


def _body(day: date, *rows: tuple[str, int, int]) -> str:
    """Rows of (symbol, current short, FINRA ADV)."""
    lines = [HEADER]
    for symbol, short, adv in rows:
        lines.append(f"{day:%Y%m%d}|{symbol}|{symbol} Inc|A|NYSE|{short}|{short}||{adv}|1.0||0|0|{day:%Y-%m-%d}")
    return "\n".join(lines) + "\n"


def _gz(lines: list[Any]) -> bytes:
    return gzip.compress("".join(json.dumps(line) + "\n" for line in lines).encode(), mtime=0)


def _row(m: str, key: int, sid: int, symbol: str | None, shares: float, *, admitted: bool = True) -> dict[str, Any]:
    exclusion = None if admitted else "veto"
    return {
        "M": m,
        "name_key": key,
        "series_id": sid,
        "symbol": symbol,
        "exclusion": exclusion,
        "me": {"shares": shares},
    }


def _bars(volume: float, *, skip: date | None = None) -> list[list[Any]]:
    return [[d.isoformat(), 10.0, 10.0, volume, False, True] for d in SESSIONS if d != skip]


# --------------------------------------------------------------------------- payloads


def test_the_committed_payload_manifest_matches_the_calendar() -> None:
    pins = read_payload_manifest(PAYLOADS_PATH.read_text(), CALENDAR)
    assert len(pins) == 78
    assert pins[0] == PayloadPin(JUNE_15, pins[0].sha256, 20251)


def test_a_manifest_short_of_the_calendar_refuses() -> None:
    text = PAYLOADS_PATH.read_text()
    with pytest.raises(SiLoadError, match="in-coverage settlements"):
        read_payload_manifest("\n".join(text.splitlines()[:-1]) + "\n", CALENDAR)


def test_read_payloads_checks_presence_sha256_and_row_count() -> None:
    body = _body(JUNE_15, ("AAA", 100, 1000))
    digest = hashlib.sha256(body.encode()).hexdigest()
    store = {accession_for(JUNE_15): body}
    files = read_payloads([PayloadPin(JUNE_15, digest, 1)], store.get)
    assert files[JUNE_15].rows["AAA"].short == 100
    with pytest.raises(SiLoadError, match="not stored"):
        read_payloads([PayloadPin(JUNE_15, digest, 1)], {}.get)
    with pytest.raises(SiLoadError, match="sha256"):
        read_payloads([PayloadPin(JUNE_15, "0" * 64, 1)], store.get)
    with pytest.raises(SiLoadError, match="rows"):
        read_payloads([PayloadPin(JUNE_15, digest, 2)], store.get)


# --------------------------------------------------------------------------- stage-B inputs


def test_si_names_reads_the_row_symbol_shares_volume_and_splits() -> None:
    rows = _gz(
        [
            _row("2021-06-30", 1, 11, "AAA", 2e6),
            _row("2021-06-30", 2, 12, None, 3e6),
            _row("2021-06-30", 3, 13, "CCC", 4e6, admitted=False),
        ]
    )
    daily = _gz([[11, _bars(1000.0)], [12, _bars(500.0)], [13, _bars(1.0)]])
    splits = _gz([[11, "2021-06-20", "2"]])
    names = si_names(rows, daily, splits)[S_JUNE]
    assert set(names) == {1, 2}
    assert (names[1].symbol, names[1].shares, names[2].symbol) == ("AAA", 2e6, "")
    assert names[1].volume[JUNE_15] == 1000.0 and [s.factor for s in names[1].splits] == [2]


def test_an_admitted_series_without_daily_bars_refuses() -> None:
    with pytest.raises(SiLoadError, match="no frozen daily bars"):
        si_names(_gz([_row("2021-06-30", 1, 11, "AAA", 2e6)]), _gz([]), _gz([]))


# --------------------------------------------------------------------------- readings and the reproduction


def _formation(*, skip: date | None = None) -> SiFormation:
    """Formation 2021-06: AAA valid (SIR 0.5), BBB valid (SIR 0.01), CCC unmatched, DDD unverifiable."""
    rows = _gz(
        [
            _row("2021-06-30", 1, 11, "AAA", 1e6),
            _row("2021-06-30", 2, 12, "BBB", 1e6),
            _row("2021-06-30", 3, 13, "CCC", 1e6),
            _row("2021-06-30", 4, 14, "DDD", 1e6),
        ]
    )
    daily = _gz([[11, _bars(1000.0)], [12, _bars(1000.0)], [13, _bars(1000.0)], [14, _bars(1000.0, skip=skip)]])
    names = si_names(rows, daily, _gz([]))[S_JUNE]
    body = _body(JUNE_15, ("AAA", 500_000, 1000), ("BBB", 10_000, 1000), ("DDD", 1, 1000))
    files = {
        JUNE_15: read_payloads(
            [PayloadPin(JUNE_15, hashlib.sha256(body.encode()).hexdigest(), 3)], {accession_for(JUNE_15): body}.get
        )[JUNE_15]
    }
    si = read_si(CALENDAR, files, SESSIONS, S_JUNE, names)
    me = {1: 4.0, 2: 3.0, 3: 2.0, 4: 1.0}
    pops = {p: frozenset[int]() for p in Population} | {
        Population.MICRO: frozenset(me),
        Population.REST: frozenset(me),
        Population.ALL: frozenset(me),
    }
    return SiFormation(date(2021, 6, 30), pops, me, si)


def test_read_si_resolves_the_settlement_and_flags_the_top_decile() -> None:
    f = _formation(skip=date(2021, 6, 1))
    assert f.si.used == JUNE_15
    assert {k: r.state for k, r in f.si.readings.items()} == {
        1: SiState.VALID,
        2: SiState.VALID,
        3: SiState.UNMATCHED,
        4: SiState.UNVERIFIABLE,
    }
    assert f.si.q == 0.5 and f.si.flagged == {1}


def test_an_uncovered_formation_reads_no_settlement() -> None:
    names = si_names(_gz([_row("2021-05-28", 1, 11, "AAA", 1e6)]), _gz([[11, _bars(1000.0)]]), _gz([]))
    si = read_si(CALENDAR, {}, SESSIONS, S_MAY, names[S_MAY])
    assert (si.used, si.q, si.flagged, si.readings[1].state) == (None, None, frozenset(), SiState.NO_SETTLEMENT)


def _reference() -> tuple[str, str]:
    """Premise 2's counts CSV and per-name sha256 for ``_formation``, written out by hand."""
    lines = ["M,population,admitted,ambiguous,unmatched,unverifiable,identity_fail,valid,flagged,split_carried"]
    full, empty = "4,0,1,1,0,2,1,0", "0,0,0,0,0,0,0,0"
    for p in Population:
        lines.append(f"2021-06-30,{p},{full if p in (Population.MICRO, Population.REST, Population.ALL) else empty}")
    names = [
        '["2021-06-30",1,"valid","2021-06-15",0.5,true]',
        '["2021-06-30",2,"valid","2021-06-15",0.01,false]',
        '["2021-06-30",3,"unmatched",null,null,false]',
        '["2021-06-30",4,"unverifiable","2021-06-15",null,false]',
    ]
    return "\n".join(lines) + "\n", hashlib.sha256(("\n".join(names) + "\n").encode()).hexdigest()


def test_reproduction_passes_on_equal_counts_and_names() -> None:
    f = _formation(skip=date(2021, 6, 1))
    counts, names_sha = _reference()
    assert reproduce_si([f], counts, names_sha) == (1, names_sha)


def test_reproduction_refuses_a_count_or_a_name_difference() -> None:
    f = _formation(skip=date(2021, 6, 1))
    counts, names_sha = _reference()
    with pytest.raises(SiLoadError, match="count rows"):
        reproduce_si([f], counts.replace("2021-06-30,micro,4", "2021-06-30,micro,5"), names_sha)
    with pytest.raises(SiLoadError, match="per-name sha256"):
        reproduce_si([f], counts, "0" * 64)
    with pytest.raises(SiLoadError, match="header"):
        reproduce_si([f], counts.replace("split_carried", "carried", 1), names_sha)
    # A window session without a usable bar before the window (2021-05-28 is not in it) leaves DDD valid: the
    # reading, so the counts, differ.
    with pytest.raises(SiLoadError, match="count rows"):
        reproduce_si([_formation(skip=date(2021, 5, 27))], counts, names_sha)


def test_the_committed_counts_reference_has_premise_twos_shape() -> None:
    lines = COUNTS_PATH.read_text().splitlines()
    assert len(lines) == 1 + 38 * len(Population)
