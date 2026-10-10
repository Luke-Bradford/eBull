"""#3739 slice 2b: §3.2's candidate screens (``scripts/build_3739_slice2b.py``)."""

from __future__ import annotations

import zipfile
from collections import Counter
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from scripts import build_3739_slice2b as s2b
from scripts.build_3739_slice2 import Filing

THROUGH = date(2026, 9, 30)


def fact(
    tag: str = s2b.COVER_TAG,
    *,
    adsh: str = "0000000001-24-000001",
    cik: str = "0000000001",
    form: str = "10-Q",
    period: str = "20240630",
    accepted: str | None = "2024-08-01T20:00:00.000Z",
    ddate: str = "20240801",
    qtrs: str = "0",
    uom: str = "shares",
    segments: str = "",
    coreg: str = "",
    value: str = "1000",
) -> s2b.NoteFact:
    reported = s2b.reported_date(ddate, "0")
    assert reported is not None
    return s2b.NoteFact(
        "2024q3", adsh, cik, form, period, accepted, tag, ddate, reported.isoformat(), qtrs, uom, segments, coreg, value
    )


def write_archive(path: Path, num_rows: list[str]) -> None:
    sub = "adsh\tcik\tform\tperiod\n0000000001-24-000001\t1\t10-Q\t20240630\n"
    dim = "dimhash\tsegments\n0x00000000\t\n0xaaaa\tClassOfStock=CommonClassAMember;\n"
    header = "adsh\ttag\tversion\tddate\tqtrs\tuom\tdimh\tiprx\tvalue\tcoreg\tdatp\n"
    num = header + "".join(r + "\n" for r in num_rows)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("sub.tsv", sub)
        archive.writestr("dim.tsv", dim)
        archive.writestr("num.tsv", num)


def test_notes_archive_keeps_the_screen_tags_collapses_presentations_and_refuses_unresolved_dimensions(
    tmp_path: Path,
) -> None:
    path = tmp_path / "fsnds_2024q3_notes.zip"
    a = "0000000001-24-000001"
    write_archive(
        path,
        [
            # Reported 2024-07-19, rounded to 2024-07-31 (datp 12).
            f"{a}\tEntityCommonStockSharesOutstanding\tdei/2024\t20240731\t0\tshares\t0xaaaa\t0\t500\t\t12.0",
            # The same fact in a second presentation (iprx 1) is one fact.
            f"{a}\tEntityCommonStockSharesOutstanding\tdei/2024\t20240731\t0\tshares\t0xaaaa\t1\t500\t\t12.0",
            # An unresolved dimension hash is not the default dimension: dropped and counted.
            f"{a}\tEntityCommonStockSharesOutstanding\tdei/2024\t20240731\t0\tshares\t0xbbbb\t0\t700\t\t0.0",
            # An unreadable date proximity: dropped and counted.
            f"{a}\tEntityCommonStockSharesOutstanding\tdei/2024\t20240731\t0\tshares\t0xaaaa\t0\t600\t\t",
            # A company-extension tag of the same name carries the accession as its version.
            f"{a}\tEntityCommonStockSharesOutstanding\t{a}\t20240731\t0\tshares\t0x00000000\t0\t900\t\t0.0",
            # Reported 2024-07-02, rounded back to 2024-06-30 (datp -2).
            f"{a}\tStockholdersEquityNoteStockSplitConversionRatio1\tus-gaap/2024\t20240630\t0\tpure\t0x00000000\t0\t4\t\t-2",
            f"{a}\tCommonStockSharesOutstanding\tus-gaap/2024\t20240630\t0\tshares\t0x00000000\t0\t1\t\t0.0",
            # Amendment 1: a stock-dividend element, under any dimension.
            f"{a}\tDividendsCommonStockStock\tus-gaap/2024\t20240630\t2\tUSD\t0xaaaa\t0\t1274000\t\t0.0",
            "short\trow",
        ],
    )
    ledger: Counter[str] = Counter()
    facts = s2b.read_notes_archive(path, ledger)
    assert [(f.tag, f.segments, f.value, f.reported) for f in facts] == [
        ("DividendsCommonStockStock", "ClassOfStock=CommonClassAMember;", "1274000", "2024-06-30"),
        (s2b.COVER_TAG, "ClassOfStock=CommonClassAMember;", "500", "2024-07-19"),
        (s2b.RATIO_TAG, "", "4", "2024-07-02"),
    ]
    assert ledger[f"{s2b.COVER_TAG}:unreadable_date"] == 1
    assert facts[1].cik == "0000000001" and facts[1].form == "10-Q" and facts[1].accepted is None
    assert ledger[f"{s2b.COVER_TAG}:unresolved_dimension"] == 1
    assert ledger["malformed:2024q3:num.tsv"] == 1


def test_archive_label_refuses_other_names() -> None:
    assert s2b.archive_label(Path("fsnds_2025_08_notes.zip")) == "2025_08"
    assert s2b.archive_label(Path("fsnds_2024q2_notes.zip")) == "2024q2"
    with pytest.raises(ValueError):
        s2b.archive_label(Path("fsds_2024q2.zip"))


@pytest.mark.parametrize(
    ("segments", "expected"),
    [
        ("", ""),
        ("ClassOfStock=CommonClassAMember;", "CommonClassAMember"),
        ("ClassOfStock=CommonClassAMember;LegalEntity=X;", None),
        ("LegalEntity=X;", None),
    ],
)
def test_class_member_reads_only_the_default_or_one_class(segments: str, expected: str | None) -> None:
    assert s2b.class_member(segments) == expected


def cover(adsh: str, period: str, accepted: str, ddate: str, value: str, **kw: str) -> s2b.NoteFact:
    return fact(adsh=adsh, period=period, accepted=accepted, ddate=ddate, value=value, **kw)


def test_covers_take_the_latest_context_an_amendment_and_skip_co_registrants() -> None:
    ledger: Counter[str] = Counter()
    series = s2b.covers(
        [
            cover("A1", "20240331", "2024-05-01T20:00:00.000Z", "20240425", "100"),
            # Within a filing the latest context date wins.
            cover("A2", "20240630", "2024-08-01T20:00:00.000Z", "20240601", "999"),
            cover("A2", "20240630", "2024-08-01T20:00:00.000Z", "20240725", "110"),
            # A later amendment of the same period replaces its original.
            cover("A3", "20240630", "2024-09-01T20:00:00.000Z", "20240825", "120", form="10-Q/A"),
            # A co-registrant's count is another entity's.
            cover("A4", "20240930", "2024-11-01T20:00:00.000Z", "20241025", "5", coreg="Sub"),
            # Two values at one latest context: no cover from that filing for the class.
            cover("A5", "20240930", "2024-11-01T20:00:00.000Z", "20241025", "130"),
            cover("A5", "20240930", "2024-11-01T20:00:00.000Z", "20241025", "131"),
            # An 8-K's cover is not a periodic cover.
            cover("A6", "20240930", "2024-11-02T20:00:00.000Z", "20241025", "1", form="8-K"),
            # An amendment accepted after the cutoff does not replace its original.
            cover("A7", "20240331", "2026-10-02T20:00:00.000Z", "20260925", "1", form="10-Q/A"),
        ],
        THROUGH,
        ledger,
    )
    assert [(c.adsh, c.shares) for c in series[("0000000001", "")]] == [("A1", Decimal(100)), ("A3", Decimal(120))]
    assert ledger["cover:after_cutoff"] == 1
    assert ledger["cover:replaced"] == 1
    assert ledger["cover:not_read"] == 1
    assert ledger["cover:ambiguous_filing"] == 1
    assert ledger["cover:series_first_in_window"] == 0


def test_cover_jumps_outside_the_band_become_candidates_with_the_covers_dates() -> None:
    def series(*values: tuple[str, str, str]) -> dict[tuple[str, str], list[s2b.Cover]]:
        return {
            ("0000000001", "CommonClassAMember"): [
                s2b.Cover(f"A{i}", accepted, ddate, date.fromisoformat(ddate), Decimal(v))
                for i, (ddate, accepted, v) in enumerate(values)
            ]
        }

    jumps = s2b.cover_candidates(
        series(
            ("20240425", "2024-05-01T20:00:00.000Z", "100"),
            ("20240725", "2024-08-01T20:00:00.000Z", "124"),  # 1.24: inside the band
            ("20241025", "2024-11-01T20:00:00.000Z", "155"),  # 1.25: the band's end raises (Amendment 1)
            ("20250125", "2025-02-01T20:00:00.000Z", "124"),  # 0.8: the band's end raises
            ("20250425", "2025-05-01T20:00:00.000Z", "496"),  # 4.0
            ("20250725", "2025-08-01T20:00:00.000Z", "99.2"),  # 0.2
        ),
        THROUGH,
    )
    assert [(c.start, c.end, c.ratio, c.class_member, c.evidence) for c in jumps] == [
        (date(2024, 7, 25), date(2024, 10, 25), "1.25", "CommonClassAMember", ("A1", "A2")),
        (date(2024, 10, 25), date(2025, 1, 25), "0.8", "CommonClassAMember", ("A2", "A3")),
        (date(2025, 1, 25), date(2025, 4, 25), "4", "CommonClassAMember", ("A3", "A4")),
        (date(2025, 4, 25), date(2025, 7, 25), "0.2", "CommonClassAMember", ("A4", "A5")),
    ]
    # A jump wholly before the window, or raised by a cover accepted after the cutoff, is not a candidate.
    assert s2b.cover_candidates(series(("20240125", "2024-02-01T20:00:00.000Z", "100"),
                                       ("20240425", "2024-05-01T20:00:00.000Z", "400")), THROUGH) == []  # fmt: skip
    assert s2b.cover_candidates(series(("20260825", "2026-09-01T20:00:00.000Z", "100"),
                                       ("20261025", "2026-11-01T20:00:00.000Z", "400")), THROUGH) == []  # fmt: skip
    # A jump straddling the window's start is a candidate.
    assert len(s2b.cover_candidates(series(("20240425", "2024-05-01T20:00:00.000Z", "100"),
                                           ("20240725", "2024-08-01T20:00:00.000Z", "400")), THROUGH)) == 1  # fmt: skip


def test_xbrl_ratio_candidates_are_by_acceptance_whatever_the_context_date() -> None:
    old_context = fact(s2b.RATIO_TAG, ddate="20211231", uom="pure", value="2", accepted="2024-08-01T20:00:00.000Z")
    candidates = s2b.xbrl_candidates(
        [
            old_context,
            fact(s2b.RATIO_TAG, adsh="B", ddate="20240630", uom="pure", value="0.1",
                 segments="ClassOfStock=CommonClassAMember;SubsequentEventType=SubsequentEvent;"),
            # Two values for one (filing, context, class): an indication with no ratio.
            fact(s2b.RATIO_TAG, adsh="C", ddate="20240630", uom="pure", value="2"),
            fact(s2b.RATIO_TAG, adsh="C", ddate="20240630", uom="pure", value="3"),
            # Accepted before the window, or with no acceptance: not a candidate.
            fact(s2b.RATIO_TAG, adsh="D", accepted="2024-06-28T20:00:00.000Z", uom="pure", value="2"),
            fact(s2b.RATIO_TAG, adsh="E", accepted=None, uom="pure", value="2"),
        ],
        THROUGH,
    )  # fmt: skip
    assert [(c.evidence, c.class_member, c.start, c.end, c.ratio) for c in candidates] == [
        (("0000000001-24-000001",), "", date(2020, 12, 31), date(2024, 8, 1), "2"),
        (("B",), "CommonClassAMember", date(2023, 6, 30), date(2024, 8, 1), "0.1"),
        (("C",), "", date(2023, 6, 30), date(2024, 8, 1), ""),
    ]


def test_the_evidence_horizon_lets_a_late_filing_close_an_interval_that_meets_the_window() -> None:
    through, horizon = date(2024, 7, 31), date(2025, 2, 19)
    late = {
        ("0000000001", ""): [
            s2b.Cover("A0", "2024-05-01T20:00:00.000Z", "20240331", date(2024, 4, 29), Decimal(100)),
            # WRB's shape: the first post-split cover is accepted after the window's end.
            s2b.Cover("A1", "2024-08-02T20:00:00.000Z", "20240630", date(2024, 7, 29), Decimal(150)),
        ]
    }
    assert s2b.cover_candidates(late, through) == []
    (candidate,) = s2b.cover_candidates(late, through, evidence_through=horizon)
    assert (candidate.start, candidate.end, candidate.evidence) == (date(2024, 4, 29), date(2024, 7, 29), ("A0", "A1"))
    # A pair wholly after the window is not a candidate, however early its evidence.
    after = {("0000000001", ""): [
        s2b.Cover("A0", "2024-08-02T20:00:00.000Z", "20240630", date(2024, 8, 1), Decimal(100)),
        s2b.Cover("A1", "2024-11-02T20:00:00.000Z", "20240930", date(2024, 10, 29), Decimal(150)),
    ]}  # fmt: skip
    assert s2b.cover_candidates(after, through, evidence_through=horizon) == []
    ratio = fact(s2b.RATIO_TAG, ddate="20240630", uom="pure", value="1.5", accepted="2024-08-02T20:00:00.000Z")
    assert s2b.xbrl_candidates([ratio], through) == []
    assert len(s2b.xbrl_candidates([ratio], through, evidence_through=horizon)) == 1
    # A ratio fact accepted in the horizon whose interval lies after the window is not a candidate.
    later = fact(s2b.RATIO_TAG, ddate="20240930", uom="pure", value="1.5", accepted="2024-11-02T20:00:00.000Z")
    assert len(s2b.xbrl_candidates([later], through, evidence_through=horizon)) == 1  # [2023-09-30, 2024-11-01]
    assert s2b.xbrl_candidates([later], date(2023, 9, 1), date(2023, 1, 1), horizon) == []


@pytest.mark.parametrize(
    ("qtrs", "start"),
    [
        ("0", date(2022, 11, 15)),  # 46 days: half a quarter, for a short duration rounded to zero
        ("1", date(2022, 8, 15)),  # 138 days
        ("4", date(2021, 11, 12)),  # 414 days: a 371-day 53-week year still starts after it
        ("x", None),
    ],
)
def test_the_context_start_bound_never_opens_after_the_context_start(qtrs: str, start: date | None) -> None:
    assert s2b.context_start_bound(date(2022, 12, 31), qtrs) == start


def test_stock_dividend_facts_raise_candidates_on_presence() -> None:
    def dividend(tag: str, accepted: str | None = "2023-02-23T20:00:00.000Z", **kw: str) -> s2b.NoteFact:
        values = {"form": "10-K", "ddate": "20221231", "qtrs": "4", "uom": "USD", "value": "1274000"} | kw
        return fact(tag, accepted=accepted, **values)

    through, event_start = date(2024, 7, 31), date(2022, 8, 1)
    candidates = s2b.stock_dividend_candidates(
        [
            # CBSH's shape: FY2022 10-K, accepted 2023-02-23.
            dividend("DividendsCommonStockStock", adsh="CBSH", accepted="2023-02-23T20:00:00.000Z"),
            # SCCO's shape: two equity components of one dividend in one 10-Q are one indication.
            *(
                dividend("StockIssuedDuringPeriodValueStockDividend", adsh="SCCO", form="10-Q", ddate="20240630",
                         qtrs="1", accepted="2024-08-02T20:00:00.000Z", segments=f"EquityComponents={m};", value=v)
                for m, v in (("AdditionalPaidInCapital", "726100000"), ("TreasuryStockCommon", "199500000"))
            ),
            # Not raised: a zero value, an 8-K, no acceptance, a tag outside the set.
            dividend("DividendsStock", adsh="ZERO", accepted="2023-02-23T20:00:00.000Z", value="0"),
            dividend("DividendsStock", adsh="8K", accepted="2023-02-23T20:00:00.000Z", form="8-K"),
            dividend("DividendsStock", adsh="NONE", accepted=None),
            dividend("DividendsCash", adsh="CASH", accepted="2023-02-23T20:00:00.000Z"),
        ],
        through,
        event_start,
        through + s2b.EVIDENCE_HORIZON,
    )  # fmt: skip
    assert [(c.screen, c.evidence, c.start, c.end, c.ratio) for c in candidates] == [
        ("stock_dividend", ("CBSH",), date(2021, 11, 12), date(2023, 5, 26), ""),
        ("stock_dividend", ("SCCO",), date(2024, 2, 13), date(2024, 11, 2), ""),
    ]
    # The interval must meet the window: a fact accepted 92+ days before it, or whose context starts after it.
    early = dividend("DividendsStock", adsh="E", ddate="20220331", qtrs="1", accepted="2022-04-29T20:00:00.000Z")
    assert s2b.stock_dividend_candidates([early], through, event_start) == []
    after = dividend("DividendsStock", adsh="L", ddate="20241231", qtrs="0", accepted="2025-01-30T20:00:00.000Z")
    assert s2b.stock_dividend_candidates([after], through, event_start, date(2025, 2, 19)) == []
    # Accepted after the evidence cutoff.
    assert (
        s2b.stock_dividend_candidates(
            [dividend("DividendsStock", accepted="2025-02-20T20:00:00.000Z")], through, event_start, date(2025, 2, 19)
        )
        == []
    )


def test_split_screens_refuse_an_evidence_cutoff_short_of_the_horizon() -> None:
    with pytest.raises(ValueError, match="under 203 days"):
        s2b.split_screens([], [], s2b.EVENT_START, THROUGH, THROUGH, s2b.EXTRACT_FROM, Counter())
    short = THROUGH + s2b.EVIDENCE_HORIZON - timedelta(days=1)
    with pytest.raises(ValueError, match="under 203 days"):
        s2b.split_screens([], [], s2b.EVENT_START, THROUGH, short, s2b.EXTRACT_FROM, Counter())


def test_split_screens_refuse_notes_that_miss_a_filing_month() -> None:
    # The 2020 replica: notes from 2020-05 (window start - 92 days) through 2023-02 (the horizon's end).
    start, through, horizon = date(2020, 8, 1), date(2022, 7, 31), date(2023, 2, 19)
    quarters = [f"{y}q{q}" for y in range(2020, 2024) for q in range(1, 5) if "2020q2" <= f"{y}q{q}" <= "2023q1"]

    def notes(labels: list[str]) -> list[s2b.NoteFact]:
        return [s2b.NoteFact(**{**fact().__dict__, "archive": label}) for label in labels]

    assert s2b.split_screens(notes(quarters), [], start, through, horizon, date(2020, 4, 1), Counter()) == []
    with pytest.raises(ValueError, match=r"\(2023, 1\), \(2023, 2\)"):
        s2b.split_screens(notes(quarters[:-1]), [], start, through, horizon, date(2020, 4, 1), Counter())
    # Monthly archives count month by month.
    monthly = [*quarters[:-1], "2023_01", "2023_02"]
    assert s2b.split_screens(notes(monthly), [], start, through, horizon, date(2020, 4, 1), Counter()) == []


@pytest.mark.parametrize(
    ("start", "end", "months"),
    [
        (date(2020, 5, 1), date(2020, 7, 31), [(2020, 5), (2020, 6), (2020, 7)]),
        (date(2020, 5, 31), date(2020, 6, 1), [(2020, 5), (2020, 6)]),
        (date(2022, 12, 15), date(2023, 1, 2), [(2022, 12), (2023, 1)]),
    ],
)
def test_months_between_is_inclusive_by_month(start: date, end: date, months: list[tuple[int, int]]) -> None:
    assert s2b.months_between(start, end) == months
    assert s2b.archive_months("2020q2") == [(2020, 4), (2020, 5), (2020, 6)]
    assert s2b.archive_months("2025_08") == [(2025, 8)]


def test_year_before_handles_a_leap_day() -> None:
    assert s2b.year_before(date(2024, 2, 29)) == date(2023, 2, 28)


def filing(form: str, accepted: str, items: tuple[str, ...]) -> Filing:
    return Filing("0000000001", f"acc-{accepted}", form, accepted, items)


@pytest.mark.parametrize(
    ("form", "accepted", "items", "expected"),
    [
        ("8-K", "2024-08-01T20:00:00.000Z", ("5.03", "9.01"), True),
        ("8-K/A", "2024-08-01T20:00:00.000Z", ("5.03",), True),
        ("8-K", "2024-08-01T20:00:00.000Z", ("5.02",), False),
        ("8-A12B", "2024-08-01T20:00:00.000Z", ("5.03",), False),
        # Its 92-day interval reaches the window's first day.
        ("8-K", "2024-04-01T14:00:00.000Z", ("5.03",), True),
        ("8-K", "2024-03-29T14:00:00.000Z", ("5.03",), False),
        ("8-K", "2026-10-01T14:00:00.000Z", ("5.03",), False),
    ],
)
def test_item_503_candidates(form: str, accepted: str, items: tuple[str, ...], expected: bool) -> None:
    candidates = s2b.item_503_candidates([filing(form, accepted, items)], THROUGH)
    assert bool(candidates) is expected
    if candidates:
        (c,) = candidates
        assert c.end - c.start == s2b.ITEM_503_SPAN and c.ratio == ""


def test_the_filing_extract_misses_no_business_day_of_the_item_503_look_back() -> None:
    assert s2b.unread_business_days(s2b.EVENT_START - s2b.ITEM_503_SPAN, s2b.EXTRACT_FROM) == []
    assert s2b.unread_business_days(date(2024, 3, 29), date(2024, 4, 1)) == [date(2024, 3, 29)]


def obs(cik: str, symbol: str, acceptance: str) -> s2b.Observation:
    return s2b.Observation(f"{cik}-{symbol}-{acceptance}", cik, symbol, acceptance)


def test_symbol_screen_sees_a_change_across_the_window_start_and_inside_it() -> None:
    evidence = s2b.summarise_symbols(
        [
            # 1: OLD in force at the start (latest before it), NEW inside: a candidate.
            obs("1", "OLDA", "2024-03-01T15:00:00.000Z"),
            obs("1", "NEWA", "2024-09-01T15:00:00.000Z"),
            # 2: one symbol throughout: not a candidate.
            obs("2", "BBB", "2024-03-01T15:00:00.000Z"),
            obs("2", "BBB", "2025-03-01T15:00:00.000Z"),
            # 3: a symbol superseded before the start, then one symbol: not a candidate.
            obs("3", "ANCIENT", "2023-01-01T15:00:00.000Z"),
            obs("3", "CCC", "2024-02-01T15:00:00.000Z"),
            obs("3", "CCC", "2024-10-01T15:00:00.000Z"),
            # 4: a second symbol only after the cutoff: not a candidate.
            obs("4", "DDD", "2025-01-01T15:00:00.000Z"),
            obs("4", "EEE", "2026-10-05T15:00:00.000Z"),
        ],
        THROUGH,
    )
    candidates = s2b.symbol_candidates(evidence)
    assert [(c.cik, [r.symbol for r in c.symbols]) for c in candidates] == [("1", ["NEWA", "OLDA"])]
    (c,) = candidates
    assert {r.symbol: (r.last_before, r.count_in) for r in c.symbols} == {
        "NEWA": ("", 1),
        "OLDA": ("2024-03-01T15:00:00.000Z", 0),
    }


def test_window_observations_keep_the_window_and_the_latest_before_it() -> None:
    def raw(symbol: str, acceptance: str) -> dict[str, str]:
        return {"accession": f"{symbol}-{acceptance}", "cik": "1", "symbol": symbol, "acceptance": acceptance}

    kept = s2b.window_observations(
        [
            raw("AAA", "2022-06-01T15:00:00.000Z"),  # before the look-back
            raw("AAA", "2023-01-01T15:00:00.000Z"),
            raw("AAA", "2024-03-01T15:00:00.000Z"),  # the latest before the window
            raw("AAA", "2024-02-01T15:00:00.000Z"),
            raw("AAA", "2024-07-01T15:00:00.000Z"),
            raw("AAA", "2026-10-05T15:00:00.000Z"),  # after any cutoff: kept, the screens apply --through
        ]
    )
    assert sorted(o.acceptance[:10] for o in kept) == ["2024-03-01", "2024-07-01", "2026-10-05"]


def test_termination_candidates_read_from_120_days_before_the_form_25() -> None:
    (row,) = s2b.termination_candidates(
        [{"issuer_cik": "0000000002", "accession_number": "x", "form": "25-NSE", "filed_date": "2025-05-01",
          "rule_provision": "(b)"}]
    )  # fmt: skip
    assert row["read_from"] == "2025-01-01"


def test_split_rows_are_numbered_in_a_stable_order() -> None:
    a = s2b.SplitCandidate("item_503", "2", "", date(2024, 8, 1), date(2024, 11, 1), "t", "", ("x",))
    b = s2b.SplitCandidate("cover_count", "1", "", date(2024, 8, 1), date(2024, 11, 1), "t", "4", ("y", "z"))
    assert s2b.split_rows([a, b]) == [
        ["S000001", "cover_count", "1", "", "2024-08-01", "2024-11-01", "t", "4", "y;z"],
        ["S000002", "item_503", "2", "", "2024-08-01", "2024-11-01", "t", "", "x"],
    ]


def test_publish_refuses_an_existing_file(tmp_path: Path) -> None:
    out = tmp_path / "x.csv"
    s2b.publish(out, b"a")
    with pytest.raises(FileExistsError):
        s2b.publish(out, b"b")
    assert out.read_bytes() == b"a"


def test_publish_all_writes_every_file_or_none(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    a, b = tmp_path / "a.csv", tmp_path / "b.csv"
    s2b.publish_all({a: b"1", b: b"2"})
    assert (a.read_bytes(), b.read_bytes()) == (b"1", b"2")
    assert sorted(p.name for p in tmp_path.iterdir()) == ["a.csv", "b.csv"]

    # A staging file left by a killed run does not block the next one.
    e = tmp_path / "e.csv"
    (tmp_path / ".e.csv.staging").write_bytes(b"stale")
    s2b.publish_all({e: b"5"})
    assert e.read_bytes() == b"5" and not (tmp_path / ".e.csv.staging").exists()
    e.unlink()

    c, d = tmp_path / "c.csv", tmp_path / "d.csv"
    real_link = s2b.os.link

    def failing_link(src: Path, dst: Path) -> None:
        if Path(dst) == d:
            raise OSError("disk full")
        real_link(src, dst)

    monkeypatch.setattr(s2b.os, "link", failing_link)
    with pytest.raises(OSError, match="disk full"):
        s2b.publish_all({c: b"3", d: b"4"})
    assert sorted(p.name for p in tmp_path.iterdir()) == ["a.csv", "b.csv"]


def test_publish_all_never_removes_a_name_it_did_not_create(tmp_path: Path) -> None:
    pinned, new = tmp_path / "pinned.csv", tmp_path / "new.csv"
    pinned.write_bytes(b"frozen")
    with pytest.raises(FileExistsError):
        s2b.publish_all({new: b"1", pinned: b"2"})
    assert sorted(p.name for p in tmp_path.iterdir()) == ["pinned.csv"]
    assert pinned.read_bytes() == b"frozen"
    with pytest.raises(ValueError, match="staging"):
        s2b.publish_all({tmp_path / "x": b"1", tmp_path / ".x.staging": b"2"})
    assert sorted(p.name for p in tmp_path.iterdir()) == ["pinned.csv"]
