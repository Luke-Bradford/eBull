"""Pure-logic tests for the total-return layer in app.services.risk_metrics.

NO DB. Covers the dividend-stream selection (#1635), the total-return index, and
the tr_status coverage gate. Each test encodes a contract from
docs/specs/ranking/2026-06-19-sec-total-return-calmar-v1.3.md.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from app.services import risk_metrics as rm


def _D(x) -> Decimal:
    return Decimal(str(x))


def _q(start: date, val, *, declared: bool = True) -> rm.DivFact:
    """A ~quarterly fact (90 d duration)."""
    return rm.DivFact(start, start + timedelta(days=90), _D(val), declared)


def _annual(start: date, val, *, declared: bool = True) -> rm.DivFact:
    return rm.DivFact(start, start + timedelta(days=364), _D(val), declared)


def _month(start: date, val, *, declared: bool = True) -> rm.DivFact:
    return rm.DivFact(start, start + timedelta(days=30), _D(val), declared)


# ---------------------------------------------------------------------------
# select_dividend_stream
# ---------------------------------------------------------------------------


def test_empty_facts_empty_stream():
    assert rm.select_dividend_stream([]) == []


def test_modal_dedup_picks_most_common_value():
    # Same period reported 3× as 0.25 and once as 0.99 (a stray) -> modal 0.25.
    s = date(2024, 1, 1)
    facts = [_q(s, "0.25"), _q(s, "0.25"), _q(s, "0.25"), _q(s, "0.99")]
    stream = rm.select_dividend_stream(facts)
    assert len(stream) == 1
    assert stream[0].dps == _D("0.25")


def test_cumulative_durations_excluded():
    # A 181 d (H1) and 272 d (9-mo) cumulative are dropped; only the 90 d survives.
    s = date(2024, 1, 1)
    facts = [
        _q(s, "0.25"),
        rm.DivFact(s, s + timedelta(days=181), _D("0.50"), True),
        rm.DivFact(s, s + timedelta(days=272), _D("0.75"), True),
    ]
    stream = rm.select_dividend_stream(facts)
    assert len(stream) == 1
    assert stream[0].dps == _D("0.25")


def test_quarterly_plus_annual_residual_is_q4():
    # 3 quarters (0.25 each) + annual 1.00 -> residual 0.25 = the implied Q4 the
    # issuer never tags separately (the dominant US 3×10-Q + 10-K pattern).
    y = date(2023, 10, 1)
    facts = [
        rm.DivFact(date(2023, 10, 1), date(2023, 12, 30), _D("0.25"), True),
        rm.DivFact(date(2024, 1, 1), date(2024, 4, 1), _D("0.25"), True),
        rm.DivFact(date(2024, 4, 2), date(2024, 7, 1), _D("0.25"), True),
        rm.DivFact(y, y + timedelta(days=364), _D("1.00"), True),  # FY = 1.00
    ]
    stream = rm.select_dividend_stream(facts)
    # 3 quarters + 1 residual.
    assert len(stream) == 4
    total = sum((p.dps for p in stream), Decimal(0))
    assert total == _D("1.00")  # no double-count: 3×0.25 + residual 0.25
    # the residual rides the annual's tier_days.
    residual = max(stream, key=lambda p: p.ex_date)
    assert residual.dps == _D("0.25")


def test_annual_fully_covered_by_quarters_no_residual():
    # 4 quarters summing exactly to the annual -> annual contributes nothing.
    facts = [
        rm.DivFact(date(2023, 10, 1), date(2023, 12, 30), _D("0.25"), True),
        rm.DivFact(date(2024, 1, 1), date(2024, 4, 1), _D("0.25"), True),
        rm.DivFact(date(2024, 4, 2), date(2024, 7, 1), _D("0.25"), True),
        rm.DivFact(date(2024, 7, 2), date(2024, 9, 30), _D("0.25"), True),
        rm.DivFact(date(2023, 10, 1), date(2024, 9, 30), _D("1.00"), True),  # FY
    ]
    stream = rm.select_dividend_stream(facts)
    assert len(stream) == 4  # annual dropped (residual ~0)
    assert sum((p.dps for p in stream), Decimal(0)) == _D("1.00")


def test_monthly_payer_uses_monthly_tier():
    # 12 monthly facts -> 12 periods, monthly tier_days.
    facts = [_month(date(2024, 1, 1) + timedelta(days=30 * i), "0.10") for i in range(12)]
    stream = rm.select_dividend_stream(facts)
    assert len(stream) == 12
    assert all(p.tier_days == _D("30.44") for p in stream)


def test_high_outlier_dropped_real_cut_kept():
    # A 10× stray (mis-tagged cumulative) dropped; a genuine low value kept.
    base = [_q(date(2020, 1, 1) + timedelta(days=91 * i), "1.00") for i in range(8)]
    cut = _q(date(2022, 6, 1), "0.05")  # real post-crisis cut — KEEP
    stray = _q(date(2022, 9, 1), "50.0")  # 50× median — DROP
    stream = rm.select_dividend_stream([*base, cut, stray])
    vals = [p.dps for p in stream]
    assert _D("0.05") in vals
    assert _D("50.0") not in vals


def test_dominant_concept_group_no_mixing():
    # declared has more periods than cash_paid -> only declared used (no double).
    s = date(2024, 1, 1)
    facts = [
        _q(s, "0.25", declared=True),
        _q(s + timedelta(days=91), "0.25", declared=True),
        _q(s, "0.24", declared=False),  # cash_paid same period — must not add
    ]
    stream = rm.select_dividend_stream(facts)
    assert len(stream) == 2
    assert all(p.dps == _D("0.25") for p in stream)


# ---------------------------------------------------------------------------
# total_return_index
# ---------------------------------------------------------------------------


def _closes(values, start=date(2024, 1, 1)):
    return [(start + timedelta(days=i), v) for i, v in enumerate(values)]


def test_tri_non_payer_equals_price_series():
    closes = _closes([100, 110, 121])
    tri = rm.total_return_index(closes, [])
    assert [v for _, v in tri] == [_D("100"), _D("110"), _D("121")]


def test_tri_single_dividend_reinvest():
    # Closes flat at 100; one 10.0 dividend on day 1 -> shares 1.1 thereafter.
    closes = _closes([100, 100, 100])
    div_date = closes[1][0]
    tri = rm.total_return_index(closes, [(div_date, _D("10"))])
    # day0 = 100 (div skipped at/before first close? div is on day1 > first day0).
    assert tri[0][1] == _D("100")
    # day1: shares = 1 + 10/100 = 1.1 -> 1.1 * 100 = 110.
    assert tri[1][1] == _D("110")
    assert tri[2][1] == _D("110")


def test_tri_dividend_before_first_close_skipped():
    closes = _closes([100, 100])
    before = closes[0][0] - timedelta(days=5)
    tri = rm.total_return_index(closes, [(before, _D("10"))])
    assert [v for _, v in tri] == [_D("100"), _D("100")]  # predates window — ignored


# ---------------------------------------------------------------------------
# tr_status
# ---------------------------------------------------------------------------


def _dp(ex: date, tier_days="91.25") -> rm.DivPeriod:
    return rm.DivPeriod(ex, _D("0.25"), _D(tier_days))


def test_status_no_dividends_for_non_payer():
    st = rm.tr_status([], None, date(2026, 1, 1), None, has_dividend=False, stream_nonempty=False)
    assert st == "no_dividends"


def test_status_known_payer_but_no_facts_is_incomplete():
    # Summary view says current TTM dividend (True) but the fact stream is empty
    # -> a real payer whose facts we failed to ingest -> not "exact".
    st = rm.tr_status([], None, date(2026, 1, 1), None, has_dividend=True, stream_nonempty=False)
    assert st == "tr_incomplete"


def test_status_no_summary_row_is_no_dividends():
    # No summary row (None): the view only rows EVER-payers, so absence = confirmed
    # never-payer; an empty fact stream confirms TR == price (exact).
    st = rm.tr_status([], None, date(2026, 1, 1), None, has_dividend=None, stream_nonempty=False)
    assert st == "no_dividends"


def test_status_long_time_payer_single_window_period_is_gap():
    # first_ever far before the window but only ONE recent in-window period -> a
    # data gap, not an initiation -> tr_incomplete (Codex ckpt-2 HIGH).
    as_of = date(2026, 6, 1)
    ws = as_of - timedelta(days=1095)
    one = [_dp(as_of - timedelta(days=30))]
    st = rm.tr_status(one, ws, as_of, date(2005, 1, 1), has_dividend=True, stream_nonempty=True)
    assert st == "tr_incomplete"


def test_tri_same_ex_date_dividends_summed_not_compounded():
    # Two dividends on the same date: applied once on pre-dividend shares (no
    # cross-product). closes flat 100; 5 + 5 on day1 -> shares 1.10 -> 110.
    closes = _closes([100, 100])
    d1 = closes[1][0]
    tri = rm.total_return_index(closes, [(d1, _D("5")), (d1, _D("5"))])
    assert tri[1][1] == _D("110")  # 1*(1+10/100)*100, NOT 1*(1.05)*(1.05)*100=110.25


def test_status_ok_full_quarterly_coverage():
    as_of = date(2026, 6, 1)
    ws = as_of - timedelta(days=1095)
    ends = [ws + timedelta(days=91 * i) for i in range(1, 13)]  # 12 quarters, recent last
    periods = [_dp(e) for e in ends]
    st = rm.tr_status(periods, ws, as_of, date(2010, 1, 1), has_dividend=True, stream_nonempty=True)
    assert st == "ok"


def test_status_internal_gap_is_incomplete():
    as_of = date(2026, 6, 1)
    ws = as_of - timedelta(days=1095)
    # quarters with a 1-year hole in the middle.
    ends = [ws + timedelta(days=91 * i) for i in range(1, 5)]
    ends += [as_of - timedelta(days=91 * i) for i in range(3, 0, -1)]
    periods = [_dp(e) for e in sorted(ends)]
    st = rm.tr_status(periods, ws, as_of, date(2010, 1, 1), has_dividend=True, stream_nonempty=True)
    assert st == "tr_incomplete"


def test_status_terminal_staleness_is_incomplete():
    as_of = date(2026, 6, 1)
    ws = as_of - timedelta(days=1095)
    # dense early quarters but nothing in the last ~2 years (dividend stopped / unfiled).
    ends = [ws + timedelta(days=91 * i) for i in range(1, 6)]
    periods = [_dp(e) for e in ends]
    st = rm.tr_status(periods, ws, as_of, date(2010, 1, 1), has_dividend=True, stream_nonempty=True)
    assert st == "tr_incomplete"


def test_status_mid_window_initiation_is_ok():
    # A payer that initiated 1y into a 3y window: only ~4 quarters, but contiguous
    # and recent, anchored on first-ever inside the window -> ok (not penalised for
    # not having paid before it existed).
    as_of = date(2026, 6, 1)
    ws = as_of - timedelta(days=1095)
    first = as_of - timedelta(days=365)
    ends = [first + timedelta(days=91 * i) for i in range(0, 5)]
    periods = [_dp(e) for e in ends]
    st = rm.tr_status(periods, ws, as_of, first, has_dividend=True, stream_nonempty=True)
    assert st == "ok"


def test_status_monthly_terminal_floored_to_filing_cadence():
    # Monthly payer whose latest month is ~2.5 months stale (data arrives in the
    # next 10-Q). The filing-cadence floor keeps it ok rather than flagging stale.
    as_of = date(2026, 6, 11)
    ws = as_of - timedelta(days=1095)
    ends = [ws + timedelta(days=30 * i) for i in range(1, 35)]  # last ~Mar, 72d stale
    periods = [_dp(e, tier_days="30.44") for e in ends]
    st = rm.tr_status(periods, ws, as_of, date(2010, 1, 1), has_dividend=True, stream_nonempty=True)
    assert st == "ok"
