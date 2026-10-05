"""#3609 step 1 slice 3b: the panel builder's universe funnel (steps 3-6) and census, on in-memory shards."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from scripts.build_3609_factor_panel import (
    Candidate,
    Census,
    Step,
    build_cik_rows,
    holding_month,
    multiple_security_ciks,
)
from tests.test_3609_factor_panel import CIK, Shard, _balance, _cover

M = date(2017, 4, 30)
S = date(2017, 4, 28)


def _filer() -> Shard:
    shard = _balance(Shard().filing("k", "2017-02-20", "10-K"), "k", "2016-12-31")
    return _cover(shard, "k", "2017-02-10", 50)


def _rows(shard: Shard, *, sic: int | None = 3571, multi: bool = False, cik: str = CIK) -> list[dict[str, Any]]:
    candidate = Candidate(M, S, 7, 7, False, Decimal(2))
    base = {"M": M.isoformat(), "series_id": 7}
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
    for r in (row, reit, {**row, "M": "2017-05-31"}, {"M": "2017-04-30", "exclusion": Step.NOT_PRICED}):
        tally.add(r)
    summary = tally.to_json()
    april = summary["funnel_by_formation"]["2017-04-30"]
    assert april["admitted"] == {"count": 1, "me_share": 0.5}
    assert april["reit"] == {"count": 1, "me_share": 0.5}
    assert april["not_priced"] == {"count": 1, "me_share": None}
    assert summary["characteristics_by_formation"]["2017-05-31"]["be_me"]["value"] == {"count": 1, "me_share": 1.0}
    assert summary["characteristics_total_counts"]["be_me"] == {"value": 2}


def test_excluded_names_past_the_bundle_gate_carry_their_me() -> None:
    (reit,) = _rows(_filer(), sic=6798)
    assert (reit["exclusion"], reit["me"]["value"]) == (Step.REIT, "100")
