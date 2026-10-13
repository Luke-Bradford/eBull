"""#3621 slice 5a: the SI flag's pure functions (spec ``docs/research/2026-10-09-3621-slice5-short-interest.md``)."""

from __future__ import annotations

import math
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from app.services.factor_panel import SplitStamp
from app.services.factor_panel_prices import DailyBar
from app.services.short_interest_flag import (
    CALENDAR_FIRST,
    FinraFile,
    Reading,
    ShortInterestError,
    SiName,
    SiState,
    accession_for,
    adv_window,
    flag_formation,
    identity_ok,
    next_month,
    our_adv,
    parse_file,
    read_calendar_csv,
    read_formation,
    revision_check,
    settlement_for,
    si_cutoff,
    usable_volume,
    validate_calendar,
)
from scripts import measure_3621_short_interest_premise as premise

CALENDAR_CSV = Path(__file__).resolve().parents[1] / "docs" / "research" / "3621-finra-si-calendar.csv"
CALENDAR = read_calendar_csv(CALENDAR_CSV.read_text())

HEADER = (
    "accountingYearMonthNumber|symbolCode|issueName|issuerServicesGroupExchangeCode|marketClassCode|"
    "currentShortPositionQuantity|previousShortPositionQuantity|stockSplitFlag|averageDailyVolumeQuantity|"
    "daysToCoverQuantity|revisionFlag|changePercent|changePreviousNumber|settlementDate"
)


def _payload(day: date, *rows: tuple[str, int | str, int | str, int | str, str]) -> bytes:
    """Rows of (symbol, current short, previous short, ADV, revisionFlag)."""
    lines = [HEADER]
    for symbol, short, previous, adv, flag in rows:
        lines.append(
            f"{day:%Y%m%d}|{symbol}|{symbol} Inc|A|NYSE|{short}|{previous}||{adv}|1.0|{flag}|0|0|{day:%Y-%m-%d}"
        )
    return ("\n".join(lines) + "\n").encode()


def _sessions(start: date, end: date) -> list[date]:
    return [start + timedelta(i) for i in range((end - start).days + 1) if (start + timedelta(i)).weekday() < 5]


SESSIONS = _sessions(date(2021, 5, 3), date(2021, 7, 30))
JUNE_15 = date(2021, 6, 15)
S_JUNE = date(2021, 6, 30)  # formation 2021-06's s(M): June's last session
WINDOW = adv_window(CALENDAR, JUNE_15, SESSIONS)


def _volume(per_day: float = 1000.0, *, missing: date | None = None) -> dict[date, float]:
    return {d: per_day for d in SESSIONS if d != missing}


def _name(symbol: str, *, shares: float = 1e6, volume: dict[date, float] | None = None, splits=()) -> SiName:
    return SiName(symbol, shares, _volume() if volume is None else volume, list(splits))


def _file(*rows: tuple[str, int | str, int | str, int | str, str], day: date = JUNE_15) -> FinraFile:
    return parse_file(_payload(day, *rows), day)


# --------------------------------------------------------------------------- calendar


def test_real_calendar_matches_premise_reader() -> None:
    assert CALENDAR == premise.read_calendar(CALENDAR_CSV)
    assert CALENDAR[0][0] == CALENDAR_FIRST


def test_formation_2021_05_has_no_settlement_and_2021_06_uses_shrt20210615() -> None:
    assert settlement_for(CALENDAR, date(2021, 5, 28)) is None
    assert settlement_for(CALENDAR, S_JUNE) == JUNE_15
    assert accession_for(JUNE_15) == "FINRA_SI_20210615"


def test_every_covered_formation_resolves_to_its_mid_month_settlement() -> None:
    for year, month in [(2021, 7), (2022, 12), (2024, 7)]:
        s_m = max(s for s, _p in CALENDAR if (s.year, s.month) == (year, month))
        used = settlement_for(CALENDAR, s_m)
        assert used == min(s for s, _p in CALENDAR if (s.year, s.month) == (year, month))


def test_next_month_rolls_december() -> None:
    assert next_month((2021, 5)) == (2021, 6)
    assert next_month((2021, 12)) == (2022, 1)


def _drop_month(year: int, month: int) -> list[tuple[date, date]]:
    return [(s, p) for s, p in CALENDAR if (s.year, s.month) != (year, month)]


@pytest.mark.parametrize(
    "rows",
    [
        pytest.param(CALENDAR[1:], id="missing-first-row"),
        pytest.param(_drop_month(2021, 6), id="missing-first-month"),
        pytest.param([*CALENDAR[:3], CALENDAR[2], *CALENDAR[3:]], id="duplicate"),
        pytest.param([*CALENDAR, (date(2024, 9, 13), date(2024, 9, 24))], id="outside-span"),
        pytest.param([CALENDAR[0], (CALENDAR[1][0], CALENDAR[0][1]), *CALENDAR[2:]], id="publication-not-increasing"),
        pytest.param([CALENDAR[0], (CALENDAR[1][0], CALENDAR[1][0]), *CALENDAR[2:]], id="publication-on-settlement"),
        pytest.param([], id="empty"),
    ],
)
def test_calendar_refusals(rows: list[tuple[date, date]]) -> None:
    with pytest.raises(ShortInterestError):
        validate_calendar(rows)


def test_a_covered_settlement_that_is_not_mid_month_refuses() -> None:
    # Publish June's month-end settlement before s(M) so it, not 2021-06-15, is the latest published.
    rows = [(s, date(2021, 7, 1) if s == date(2021, 6, 30) else p) for s, p in CALENDAR]
    with pytest.raises(ShortInterestError, match="mid-month"):
        settlement_for(rows, date(2021, 7, 2))


# --------------------------------------------------------------------------- ADV


def test_adv_window_is_sessions_after_previous_settlement_through_settlement() -> None:
    # The fixture's sessions are every weekday, so 2021-05-31 is one; the previous settlement 2021-05-28 is excluded.
    assert WINDOW == [d for d in SESSIONS if date(2021, 5, 28) < d <= JUNE_15]
    assert WINDOW[0] == date(2021, 5, 31) and WINDOW[-1] == JUNE_15
    with pytest.raises(ShortInterestError, match="no previous"):
        adv_window(CALENDAR, CALENDAR_FIRST, SESSIONS)


def test_usable_volume_drops_unusable_null_and_negative() -> None:
    d = date(2021, 6, 1)
    bars = [
        DailyBar(d, 1.0, 1.0, 10, stamped=False, usable=True),
        DailyBar(d + timedelta(1), 1.0, 1.0, 10, stamped=False, usable=False),
        DailyBar(d + timedelta(2), 1.0, 1.0, None, stamped=False, usable=True),
        DailyBar(d + timedelta(3), 1.0, 1.0, -1, stamped=False, usable=True),
        DailyBar(d + timedelta(4), 1.0, 1.0, 0, stamped=False, usable=True),
    ]
    assert usable_volume(bars) == {d: 10.0, d + timedelta(4): 0.0}


def test_our_adv_carries_a_split_inside_the_window_to_the_settlement_basis() -> None:
    split_day = date(2021, 6, 8)
    splits = [SplitStamp(split_day, Decimal(2))]
    # Pre-split sessions trade 500 old shares = 1000 new; post-split 1000.
    volume = {d: (500.0 if d < split_day else 1000.0) for d in SESSIONS}
    assert our_adv(volume, WINDOW, splits, JUNE_15) == pytest.approx(1000.0)


def test_our_adv_is_none_when_a_window_session_has_no_bar() -> None:
    assert our_adv(_volume(missing=date(2021, 6, 9)), WINDOW, [], JUNE_15) is None


def test_identity_tolerance_and_zeros() -> None:
    assert identity_ok(1200, 1000.0)
    assert not identity_ok(1201, 1000.0)
    assert identity_ok(0, 0.0)
    assert not identity_ok(0, 5.0)
    assert not identity_ok(5, 0.0)


# --------------------------------------------------------------------------- file parsing


def test_parse_file_counts_and_keeps_flagged_rows_as_stored() -> None:
    f = _file(("AAA", 100, 90, 1000, "R"), ("B.B", 0, "", 0, ""), ("BB", 7, 7, 1000, ""))
    assert f.physical_rows == 3 and f.physical_revised == 1 and f.zero_short == 1 and f.blank_previous == 1
    assert f.rows["AAA"].short == 100 and f.rows["AAA"].revised
    assert f.twice == {"BB"}  # "B.B" and "BB" normalise to the same key


def test_blank_adv_reads_as_finra_null_zero() -> None:
    assert _file(("AAA", 100, 90, "", "")).rows["AAA"].adv == 0


@pytest.mark.parametrize(
    "row",
    [
        pytest.param(("AAA", -1, 0, 1000, ""), id="negative-short"),
        pytest.param(("AAA", 1, -1, 1000, ""), id="negative-previous"),
        pytest.param(("AAA", 1, 0, -5, ""), id="negative-adv"),
        pytest.param(("AAA", "1.5", 0, 1000, ""), id="non-integer"),
    ],
)
def test_parse_file_refusals(row: tuple[str, int | str, int | str, int | str, str]) -> None:
    with pytest.raises(ShortInterestError):
        _file(row)


def test_a_missing_header_column_refuses() -> None:
    payload = _payload(JUNE_15, ("AAA", 1, 0, 1000, "")).replace(b"|revisionFlag|", b"|revisionFlagX|")
    with pytest.raises(ShortInterestError, match="revisionFlag"):
        parse_file(payload, JUNE_15)


def test_a_header_only_payload_refuses() -> None:
    with pytest.raises(ShortInterestError, match="no rows"):
        parse_file(_payload(JUNE_15), JUNE_15)


def test_body_settlement_date_must_equal_the_file_date() -> None:
    with pytest.raises(ShortInterestError, match="settlementDate"):
        parse_file(_payload(JUNE_15, ("AAA", 1, 0, 1000, "")), date(2021, 6, 30))


# --------------------------------------------------------------------------- readings


def _read(names: dict[str, SiName], f: FinraFile) -> dict[str, Reading]:
    return read_formation(names, f, JUNE_15, WINDOW, S_JUNE)


def test_each_missing_state_in_precedence_order() -> None:
    f = _file(
        ("DUP", 1, 0, 1000, ""),
        ("DUP", 2, 0, 1000, ""),
        ("GAP", 5, 0, 1000, ""),
        ("FAR", 5, 0, 5000, ""),
        ("ZERO", 5, 0, 0, ""),
        ("OK", 5, 0, 1000, ""),
    )
    names = {
        "empty": _name("..."),
        "shared1": _name("SH.R"),
        "shared2": _name("SHR"),
        "dup": _name("DUP"),
        "unmatched": _name("NONE"),
        "gap": _name("GAP", volume=_volume(missing=date(2021, 6, 10))),
        "far": _name("FAR"),
        "zero": _name("ZERO"),
        "ok": _name("OK"),
    }
    states = {k: r.state for k, r in _read(names, f).items()}
    assert states == {
        "empty": SiState.AMBIGUOUS,
        "shared1": SiState.AMBIGUOUS,
        "shared2": SiState.AMBIGUOUS,
        "dup": SiState.AMBIGUOUS,
        "unmatched": SiState.UNMATCHED,
        "gap": SiState.UNVERIFIABLE,
        "far": SiState.IDENTITY_FAIL,
        "zero": SiState.IDENTITY_FAIL,
        "ok": SiState.VALID,
    }


def test_a_flagged_row_is_used_as_stored() -> None:
    r = _read({"a": _name("AAA")}, _file(("AAA", 250_000, 1, 1000, "R")))["a"]
    assert r.state is SiState.VALID and r.sir == pytest.approx(0.25)


def test_split_between_settlement_and_s_m_carries_the_count() -> None:
    splits = [SplitStamp(date(2021, 6, 21), Decimal("0.125"))]  # 1-for-8 reverse split after the settlement
    r = _read({"a": _name("AAA", splits=splits)}, _file(("AAA", 800_000, 0, 1000, "")))["a"]
    assert r.split_carried and r.sir == pytest.approx(0.1)


def test_split_after_s_m_neither_carries_nor_marks() -> None:
    splits = [SplitStamp(date(2021, 7, 6), Decimal(2))]
    r = _read({"a": _name("AAA", splits=splits)}, _file(("AAA", 100_000, 0, 1000, "")))["a"]
    assert not r.split_carried and r.sir == pytest.approx(0.1)


def test_ticker_reuse_fails_or_passes_on_the_volume_check() -> None:
    # The panel series trades ~1000/day; FINRA's row under the same ticker belongs to an issuer trading 40x that.
    reused = _read({"a": _name("TKR")}, _file(("TKR", 5, 0, 40_000, "")))["a"]
    assert reused.state is SiState.IDENTITY_FAIL and reused.log_ratio == pytest.approx(math.log(40))
    # A reused ticker whose volume happens to agree passes: the mapping is an unverified exception (spec).
    lookalike = _read({"a": _name("TKR")}, _file(("TKR", 5, 0, 1100, "")))["a"]
    assert lookalike.state is SiState.VALID


def test_non_positive_or_non_finite_shares_refuse() -> None:
    for shares in (0.0, -1.0, math.nan, math.inf):
        with pytest.raises(ShortInterestError, match="shares"):
            _read({"a": _name("AAA", shares=shares)}, _file(("AAA", 1, 0, 1000, "")))


def test_uncovered_formation_reads_no_settlement_and_flags_nothing() -> None:
    readings = read_formation({"a": _name("AAA")}, None, None, [], date(2021, 5, 28))
    assert readings == {"a": Reading(SiState.NO_SETTLEMENT)}
    assert flag_formation(readings, None) == (None, frozenset())


def test_covered_formation_without_its_payload_refuses() -> None:
    with pytest.raises(ShortInterestError, match="no payload"):
        read_formation({"a": _name("AAA")}, None, JUNE_15, WINDOW, S_JUNE)


# --------------------------------------------------------------------------- decile


def test_cutoff_is_the_ceil_ninety_percent_order_statistic_and_ties_flag() -> None:
    values = [i / 100 for i in range(1, 11)]  # N = 10: q is the 9th smallest
    assert si_cutoff(values) == 0.09
    readings = {i: Reading(SiState.VALID, JUNE_15, v) for i, v in enumerate([*values, 0.09])}
    readings[99] = Reading(SiState.UNMATCHED)
    q, flagged = flag_formation(readings, JUNE_15)
    # N = 11: ceil(9.9) = 10th smallest is 0.09 (tied); both 0.09s and 0.10 flag, the unmatched name never does.
    assert q == 0.09 and flagged == {8, 9, 10}


def test_cutoff_refusals() -> None:
    with pytest.raises(ShortInterestError, match="no valid"):
        si_cutoff([])
    with pytest.raises(ShortInterestError, match="not positive"):
        si_cutoff([0.0] * 10)
    with pytest.raises(ShortInterestError, match="no valid"):
        flag_formation({"a": Reading(SiState.UNMATCHED)}, JUNE_15)
    with pytest.raises(ShortInterestError, match="no valid"):
        flag_formation({}, JUNE_15)  # a covered formation with no admitted names


# --------------------------------------------------------------------------- revision check


def test_revision_check_compares_the_flag_against_the_stored_prior_figure() -> None:
    june_30 = date(2021, 6, 30)
    before = _file(("AAA", 100, 0, 1, ""), ("BBB", 50, 0, 1, ""), ("CCC", 10, 0, 1, ""), ("DD", 1, 0, 1, ""))
    after = _file(
        ("AAA", 110, 95, 1, "R"),  # flagged, prior differs: the stored prior file is first-published
        ("BBB", 60, 50, 1, ""),  # unflagged, agrees
        ("CCC", 12, 10, 1, "R"),  # flagged yet agrees: residue
        ("DD", 2, "", 1, ""),  # blank prior
        ("EEE", 3, 1, 1, ""),  # absent from the prior file
        day=june_30,
    )
    tally = revision_check([(JUNE_15, before), (june_30, after)])
    assert tally.compared == {(True, False): 1, (False, True): 1, (True, True): 1}
    assert tally.blank_previous == {False: 1}
    assert tally.uncompared == {False: 1}
    assert tally.residue == [(june_30, "CCC", True, 10, 10)]
