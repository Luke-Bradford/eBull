"""#3609 step 1 slice 3b: the panel builder's universe funnel (steps 3-6) and census, on in-memory shards."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from app.services.factor_panel import SplitStamp
from scripts.build_3609_factor_panel import (
    Candidate,
    Census,
    Step,
    build_cik_rows,
    holding_last_sessions,
    holding_month,
    me_reconciliation,
    multiple_security_ciks,
)
from tests.test_3609_factor_panel import CIK, Shard, _balance, _cover

M = date(2017, 4, 30)
S = date(2017, 4, 28)
PRICES: dict[str, Any] = {
    "adj_close": 2.0,
    "ret_12_1": {"value": 0.1, "observations": 11, "missing": None},
    "rvol_21d": {"value": None, "observations": 9, "missing": "insufficient_returns"},
    "liquidity_tercile": None,
    "holding": {"status": "terminal"},
    "daily_monthly": "unexplained",
    "month_end_after_decision": False,
}


def _filer() -> Shard:
    shard = _balance(Shard().filing("k", "2017-02-20", "10-K"), "k", "2016-12-31")
    return _cover(shard, "k", "2017-02-10", 50)


def _rows(shard: Shard, *, sic: int | None = 3571, multi: bool = False, cik: str = CIK) -> list[dict[str, Any]]:
    candidate = Candidate(M, S, 7, 7, False, Decimal(2))
    base = {"M": M.isoformat(), "series_id": 7, "prices": PRICES}
    return list(
        build_cik_rows(
            shard.bundle(),
            cik,
            [(candidate, base)],
            {M: frozenset({cik}) if multi else frozenset()},
            {"k": sic},
            {},
        )
    )


def test_admitted_row_carries_me_and_every_characteristic() -> None:
    (row,) = _rows(_filer())
    assert row["exclusion"] is None
    assert (row["me"]["value"], row["me"]["shares_scope"], row["sic_status"]) == ("100", "cover", "sic")
    assert row["characteristics"]["be_me"]["value"] == 0.4
    assert set(row["characteristics"]) == {"gp_at", "be_me", "ope_be", "ni_me", "ocf_me", "at_gr1"}


def test_funnel_steps_in_order() -> None:
    assert _rows(_filer(), cik="0000000002")[0]["exclusion"] == "no_fundamentals"
    stale = _balance(Shard().filing("k", "2015-02-20", "10-K"), "k", "2014-12-31")
    assert _rows(stale)[0]["exclusion"] == Step.NOT_FILER
    assert _rows(_filer(), sic=6798)[0]["exclusion"] == Step.REIT
    assert _rows(_filer(), sic=6798, multi=True)[0]["exclusion"] == Step.REIT  # REIT is counted first
    assert _rows(_filer(), multi=True)[0]["exclusion"] == Step.MULTIPLE_SECURITIES
    no_shares = _balance(Shard().filing("k", "2017-02-20", "10-K"), "k", "2016-12-31")
    assert _rows(no_shares)[0]["exclusion"] == "no_shares"


def test_null_and_unloaded_sic_stay_in() -> None:
    assert _rows(_filer(), sic=None)[0]["sic_status"] == "sic_null"
    shard = _filer()
    (row,) = list(
        build_cik_rows(shard.bundle(), CIK, [(Candidate(M, S, 7, 7, False, Decimal(2)), {})], {M: frozenset()}, {}, {})
    )
    assert (row["sic_status"], row["exclusion"]) == ("sic_unloaded", None)


def test_multiple_securities_and_holding_month() -> None:
    assert multiple_security_ciks({1: "a", 2: "b", 3: "a"}) == frozenset({"a"})
    assert holding_month(date(2014, 9, 30)) == "2014-10"
    assert holding_month(date(2020, 12, 31)) == "2021-01"


def test_census_is_per_formation_and_weights_exclusions_with_known_me() -> None:
    (row,) = _rows(_filer())
    (reit,) = _rows(_filer(), sic=6798)
    tally = Census()
    reit = {**reit, "series_id": 8}
    for r in (row, reit, {**row, "M": "2017-05-31"}, {"M": "2017-04-30", "series_id": 9, "exclusion": Step.NOT_PRICED}):
        tally.add(r)
    summary = tally.to_json()
    april = summary["funnel_by_formation"]["2017-04-30"]
    assert april["admitted"] == {"count": 1, "me_share": 0.5}
    assert april["reit"] == {"count": 1, "me_share": 0.5}
    assert april["not_priced"] == {"count": 1, "me_share": None}
    assert summary["characteristics_by_formation"]["2017-05-31"]["be_me"]["value"] == {"count": 1, "me_share": 1.0}
    assert summary["characteristics_total_counts"]["be_me"] == {"value": 2}
    assert summary["largest_me_by_formation"]["2017-04-30"] == [{"symbol": "series:7", "me": 100.0}]
    assert summary["characteristics_by_formation"]["2017-04-30"]["rvol_21d"] == {
        "insufficient_returns": {"count": 1, "me_share": 1.0}
    }
    assert summary["diagnostics_total_counts"]["holding_status"] == {"terminal": 2}
    assert summary["diagnostics_total_counts"]["liquidity_tercile"] == {"unclassified": 2}
    assert summary["daily_monthly_unexplained"][0] == {"M": "2017-04-30", "series_id": 7, "symbol": "series:7"}
    # The REIT's ME still feeds the discontinuity pairs; only admitted rows are panel name-months.
    assert tally.me_points[7]["2017-04-30"] == (100.0, 2.0, True, "series:7")


def test_excluded_names_past_the_bundle_gate_carry_their_me() -> None:
    (reit,) = _rows(_filer(), sic=6798)
    assert (reit["exclusion"], reit["me"]["value"]) == (Step.REIT, "100")


def test_holding_last_session_is_the_following_months_last_spy_session() -> None:
    sessions = [date(2017, 4, 28), date(2017, 5, 30), date(2017, 5, 31), date(2017, 6, 1)]
    assert holding_last_sessions([M], sessions) == {M: date(2017, 5, 31)}


def _point(me: float, adj: float, admitted: bool = True) -> tuple[float, float, bool, str]:
    return (me, adj, admitted, "X")


def test_me_reconciliation_flags_moves_against_adj_close_and_checks_splits() -> None:
    m1, m2, m3 = date(2017, 4, 30), date(2017, 5, 31), date(2017, 6, 30)
    decisions = {m1: S, m2: date(2017, 5, 31), m3: date(2017, 6, 30)}
    points = {
        # ME tracks price into May; in June a 2:1 split stamp is missed by the count, so ME halves.
        1: {"2017-04-30": _point(100, 10), "2017-05-31": _point(110, 11), "2017-06-30": _point(55, 11)},
        # A gap at May: April-June is not a consecutive pair.
        2: {"2017-04-30": _point(100, 10), "2017-06-30": _point(500, 10)},
        # Excluded at June: not a panel name-month, but its split pair still counts (April admitted side).
        3: {"2017-05-31": _point(100, 10), "2017-06-30": _point(100, 10, admitted=False)},
    }
    splits = {1: [SplitStamp(date(2017, 6, 15), Decimal(2))], 3: [SplitStamp(date(2017, 6, 1), Decimal(2))]}
    got = me_reconciliation(points, decisions, splits)
    assert got["discontinuity"]["admitted_pairs"] == 2
    assert got["discontinuity"]["flagged"] == 1
    assert got["discontinuity"]["flagged_name_months"] == [
        {"M": "2017-06-30", "series_id": 1, "symbol": "X", "relative": 0.5}
    ]
    assert got["split_reconciliation"]["pairs_with_a_stamp"] == 2
    assert [f["series_id"] for f in got["split_reconciliation"]["failures"]] == [1]


def test_me_reconciliation_skips_calendar_gaps_in_a_formation_subset() -> None:
    april, june = date(2017, 4, 30), date(2017, 6, 30)
    points = {1: {"2017-04-30": _point(100, 10), "2017-06-30": _point(500, 10)}}
    got = me_reconciliation(points, {april: S, june: date(2017, 6, 30)}, {})
    assert got["discontinuity"]["admitted_pairs"] == 0
