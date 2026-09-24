"""#3361 acceptance item 4: the census's pure pieces (spec census section + precision rules)."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from app.services import security_linkage as sl
from scripts import census_3361_security_linkage as census

D = date(2020, 6, 30)


def _bars(days: list[date], close: str = "10", volume: int = 100) -> list[census.Bar]:
    return [(day, Decimal(close), volume) for day in days]


def _sessions(n: int, end: date = D - timedelta(days=1)) -> list[date]:
    return [end - timedelta(days=i) for i in range(n)][::-1]


def test_population_groups() -> None:
    first, last = date(2019, 1, 2), date(2021, 1, 4)
    assert census.population_group(first, last, frozenset({D}), D) == census.BAR_ON_D
    assert census.population_group(first, last, frozenset(), D) == census.NO_BAR_ON_D
    ended = D - timedelta(days=730)
    assert census.population_group(first, ended, frozenset(), D) == census.ENDED_IN_WINDOW
    assert census.population_group(first, ended - timedelta(days=1), frozenset(), D) is None
    assert census.population_group(D + timedelta(days=1), last, frozenset(), D) is None


def test_liquidity_is_the_median_of_the_latest_21_valid_bars_before_d() -> None:
    days = _sessions(25)
    bars = [(day, Decimal(i + 1), 1) for i, day in enumerate(days)]  # close x volume = 1..25
    bars.append((D, Decimal("999"), 1))  # ON D: never used
    assert census.liquidity(bars, D) == Decimal(15)  # latest 21 = 5..25, median 15


def test_liquidity_drops_invalid_bars_and_needs_21_within_42_days() -> None:
    days = _sessions(21)
    assert census.liquidity(_bars(days), D) == Decimal(1000)
    invalid = [(days[0], None, 100), *_bars(days[1:])]
    assert census.liquidity(invalid, D) is None  # only 20 valid
    assert census.liquidity([(days[0], Decimal(0), 100), *_bars(days[1:])], D) is None
    stale = _sessions(21, end=D - timedelta(days=30))  # oldest is D - 50
    assert census.liquidity(_bars(stale), D) is None
    edge = [D - timedelta(days=42), *_sessions(20)]  # oldest exactly D - 42
    assert census.liquidity(_bars(sorted(edge)), D) == Decimal(1000)


def test_deciles_rank_on_value_then_series_id() -> None:
    values = {sid: Decimal(1) for sid in range(1, 21)}
    ranks = census.deciles(values)
    assert ranks[1] == 0 and ranks[2] == 0 and ranks[3] == 1 and ranks[20] == 9
    assert census.deciles({}) == {}


def test_capture_status() -> None:
    end = date(2024, 9, 27)
    assert census.capture_status(end - timedelta(days=7), end) == "runs_to_capture"
    assert census.capture_status(end - timedelta(days=8), end) == "ends_before_capture"


def test_form25_outcome_window_and_observed_span() -> None:
    span = (date(2013, 1, 2), date(2024, 12, 31))
    assert census.form25_outcome([D], D, span) == ("form25_observed", 1)
    assert census.form25_outcome([D + timedelta(days=730), D - timedelta(days=1)], D, span) == ("form25_observed", 1)
    assert census.form25_outcome([], D, span) == ("no_form25_observed", 0)
    assert census.form25_outcome([], date(2023, 6, 30), span) == ("unobserved_horizon", 0)
    assert census.form25_outcome([], date(2012, 6, 29), span) == ("unobserved_horizon", 0)
    assert census.form25_outcome([], D, None) == ("unobserved_horizon", 0)


def test_reversion_precedence() -> None:
    switch, covered = date(2016, 6, 30), date(2024, 6, 28)
    later = [(date(2017, 6, 30), "0000000002"), (date(2018, 6, 29), "0000000001")]
    assert census.reversion_status(switch, "0000000001", later, covered) == "reverted"
    assert census.reversion_status(switch, "0000000009", later, covered) == "not_reverted"
    assert census.reversion_status(date(2023, 6, 30), "0000000009", [], covered) == "unobservable"
    # reverted wins over unobservable
    assert census.reversion_status(date(2023, 6, 30), "1", [(date(2024, 6, 28), "1")], covered) == "reverted"
    # beyond 730 days is not a reversion
    assert census.reversion_status(switch, "1", [(date(2019, 6, 28), "1")], covered) == "not_reverted"


def test_immediate_predecessor_orders_by_first_acceptance() -> None:
    def obs(cik: str, acceptance: str) -> sl.Observation:
        return sl.Observation("a" + acceptance, cik, acceptance, 1, False)

    result = sl.LinkResult(
        sl.Reason.LINKED,
        cik="3",
        basis="succession",
        observations=(obs("1", "2019-01"), obs("2", "2019-02"), obs("1", "2019-02b"), obs("3", "2019-03")),
    )
    assert census.immediate_predecessor(result) == "2"


def test_collision_groups_are_connected_components() -> None:
    groups = census.collision_groups(
        {1: frozenset({"ABCD", "ABCDQ"}), 2: frozenset({"ABCDQ"}), 3: frozenset({"XYZ"}), 4: frozenset({"XYZ"})}
    )
    assert groups == {1: 1, 2: 1, 3: 3, 4: 3}


def _write(path: Path, lines: list[str]) -> Path:
    path.write_text("\n".join(["series_id\tbar_date\tclose\tvolume", *lines]) + "\n")
    return path


def test_read_bars_accepts_a_clean_snapshot(tmp_path: Path) -> None:
    bounds = {1: (date(2020, 1, 2), date(2020, 1, 6))}
    bars = census.read_bars(_write(tmp_path / "b.tsv", ["1\t2020-01-02\t1.5\t10", "1\t2020-01-06\t\t"]), bounds)
    assert bars[1] == [(date(2020, 1, 2), Decimal("1.5"), 10), (date(2020, 1, 6), None, None)]


@pytest.mark.parametrize(
    ("lines", "message"),
    [
        (["1\t2020-01-02\t1\t1", "1\t2020-01-02\t1\t1", "1\t2020-01-06\t1\t1"], "duplicate"),
        (["1\t2020-01-02\tNaN\t1", "1\t2020-01-06\t1\t1"], "non-finite"),
        (["1\t2020-01-01\t1\t1", "1\t2020-01-02\t1\t1", "1\t2020-01-06\t1\t1"], "outside"),
        (["1\t2020-01-02\t1\t1"], "endpoint"),
    ],
)
def test_read_bars_refuses_a_bad_snapshot(tmp_path: Path, lines: list[str], message: str) -> None:
    bounds = {1: (date(2020, 1, 2), date(2020, 1, 6))}
    with pytest.raises(census.CensusError, match=message):
        census.read_bars(_write(tmp_path / "b.tsv", lines), bounds)


def test_tabulate_counts_ended_diagnostics_but_keeps_them_out_of_at_d_links(tmp_path: Path) -> None:
    from tests.test_3361_security_linkage import C1, Corpus, _build, _ny4pm, _series

    corpus = Corpus()
    corpus.file(C1, "ABC", _ny4pm("2019-03-01"))
    corpus.file(C1, "XYZ", _ny4pm("2019-03-01"))
    bundle, _, _ = _build(tmp_path, corpus, [_series(1, "ABC"), _series(2, "XYZ", last="2020-01-02")])
    inventory = {1: ("ABC", date(2008, 1, 1), date(2021, 12, 31)), 2: ("XYZ", date(2008, 1, 1), date(2020, 1, 2))}
    bars = {1: _bars([D]), 2: _bars([date(2020, 1, 2)])}
    formation = census.tabulate(bundle, D, inventory, bars, date(2021, 12, 31), {}, {}, None, {1: 1, 2: 2})
    assert formation.linked == {1: C1}  # the ended series is a diagnostic, not an at-D link
    assert formation.counts["distinct_linked_ciks:bar_on_d"] == 1
    assert formation.counts["distinct_linked_ciks:ended_in_window"] == 1
    assert formation.cross_tab == {
        "bar_on_d|linked:single_cik|liquidity_unavailable|runs_to_capture": 1,
        "ended_in_window|linked:single_cik|not_applicable|ends_before_capture": 1,
    }


def test_failed_run_leaves_no_evidence_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from app.services.r6_pit_bundle import R6PitBundleError

    out = tmp_path / "evidence"
    argv = ["census", "--bundle", str(tmp_path / "missing"), "--manifest-sha256", "0" * 64, "--out-dir", str(out)]
    monkeypatch.setattr("sys.argv", argv)
    with pytest.raises(R6PitBundleError):
        census.main()
    assert not out.exists()  # a retry with the same path is not blocked
