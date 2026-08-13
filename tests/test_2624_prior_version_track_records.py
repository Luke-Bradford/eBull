"""Prior-version track records and the rotated scan state (#2624 scopes 1 + 2).

Pure (no Postgres): ``build_prior_versions`` takes the two readers' rows and
returns the payload records, so the grouping, the comparability rule and the
ordering are all table-testable.

Spec: ``docs/proposals/ta/2026-08-13-prior-version-track-records.md``.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

from app.api.strategies import _current_identity_pins, build_prior_versions

_PINS = {"namespace": "hold_out", "cost_model_id": "cost-v2", "return_basis": "total-return-v1"}
_S = "s1-time-series-momentum"


def _group(version: str, count: int, **overrides: str) -> dict[str, object]:
    return {"strategy_id": _S, "strategy_version": version, "count": count, **_PINS, **overrides}


def _scan(version: str, frontier: date, at: datetime) -> dict[str, object]:
    return {"strategy_id": _S, "strategy_version": version, "frontier_date": frontier, "updated_at": at}


def test_a_version_on_the_current_basis_is_comparable() -> None:
    records = build_prior_versions(result_groups=[_group("v-old", 32)], scan_rows=[], pins=_PINS)

    assert [r.strategy_version for r in records[_S]] == ["v-old"]
    assert records[_S][0].comparable
    assert records[_S][0].incomparable_reasons == []
    assert records[_S][0].result_count == 32


def test_differing_pins_are_named_not_swallowed() -> None:
    """The refusal has to say WHICH basis differs — that is the whole payload.

    Measured on dev: every version replaced before today differs on at least
    ``cost_model_id`` and ``return_basis`` (cost model v1 -> v2+split-adjusted-max,
    raw-close -> split-dividend-adjusted wealth). Reporting "not comparable" with
    no reason would be the same blank the ticket exists to remove, one level down.
    """
    records = build_prior_versions(
        result_groups=[_group("v-old", 32, cost_model_id="cost-v1", return_basis="raw-close-v1")],
        scan_rows=[],
        pins=_PINS,
    )

    assert not records[_S][0].comparable
    assert records[_S][0].incomparable_reasons == ["cost_model_id", "return_basis"]


def test_a_partly_matching_version_is_not_comparable() -> None:
    """ "Partly on the current basis" is not a state an operator can act on."""
    records = build_prior_versions(
        result_groups=[_group("v-old", 16), _group("v-old", 4, cost_model_id="cost-v1")],
        scan_rows=[],
        pins=_PINS,
    )

    assert records[_S][0].result_count == 20  # every row counted, comparable or not
    assert not records[_S][0].comparable
    assert records[_S][0].incomparable_reasons == ["cost_model_id"]


def test_a_watermark_only_version_is_not_a_track_record() -> None:
    """Stored results are the qualifier; a scan is not — and that is the BOUND.

    Measured on dev: ``+6c7cff76dcde`` scanned 2026-08-07 and stored zero rows.
    ``strategy_scan_watermark`` gains a row per scan-day per version, so admitting
    watermark-only versions would grow this list with every registry version ever
    scanned — the unbounded growth the results-bearing rule exists to avoid.
    The scan itself is not lost: it is what ``ScanHealth.rotation`` carries.
    """
    records = build_prior_versions(
        result_groups=[],
        scan_rows=[_scan("v-scanned-only", date(2026, 8, 7), datetime(2026, 8, 9, 6, 46, tzinfo=UTC))],
        pins=_PINS,
    )

    assert records == {}, "a version that scanned and stored nothing has no track record"


def test_a_watermark_enriches_a_result_bearing_version_only() -> None:
    records = build_prior_versions(
        result_groups=[_group("v-results", 8)],
        scan_rows=[
            _scan("v-results", date(2026, 8, 7), datetime(2026, 8, 9, tzinfo=UTC)),
            _scan("v-scan-only", date(2026, 8, 11), datetime(2026, 8, 12, tzinfo=UTC)),
        ],
        pins=_PINS,
    )

    assert [r.strategy_version for r in records[_S]] == ["v-results"]
    assert records[_S][0].last_scan_frontier_date == date(2026, 8, 7)


def test_ordering_is_deterministic_when_scan_times_tie() -> None:
    """A bare date sort is not deterministic; the version breaks the tie."""
    same = datetime(2026, 8, 12, 18, 59, tzinfo=UTC)
    records = build_prior_versions(
        result_groups=[_group("v-aaa", 1), _group("v-zzz", 1)],
        scan_rows=[_scan("v-aaa", date(2026, 8, 11), same), _scan("v-zzz", date(2026, 8, 11), same)],
        pins=_PINS,
    )

    assert [r.strategy_version for r in records[_S]] == ["v-zzz", "v-aaa"]


def test_never_scanned_versions_sort_after_scanned_ones() -> None:
    records = build_prior_versions(
        result_groups=[_group("v-never-scanned", 4), _group("v-scanned", 4)],
        scan_rows=[_scan("v-scanned", date(2026, 8, 7), datetime(2026, 8, 9, tzinfo=UTC))],
        pins=_PINS,
    )

    assert [r.strategy_version for r in records[_S]] == ["v-scanned", "v-never-scanned"]


def test_records_never_cross_strategies() -> None:
    """The identity hash is over registry bytes; nothing stops two strategies
    sharing one, so the grouping key is the PAIR and never the version alone."""
    shared = "strategy-registry-v1+collide"
    records = build_prior_versions(
        result_groups=[
            {"strategy_id": "s1", "strategy_version": shared, "count": 3, **_PINS},
            {"strategy_id": "s2", "strategy_version": shared, "count": 5, **_PINS},
        ],
        scan_rows=[],
        pins=_PINS,
    )

    assert records["s1"][0].result_count == 3
    assert records["s2"][0].result_count == 5


def test_identity_pins_cover_every_equality_the_current_reader_applies() -> None:
    """If ``_RESULTS_SQL`` gains a pin and this does not, a prior version starts
    reporting "comparable" on a basis the current reader would have rejected."""
    from app.api.strategies import _RESULTS_SQL

    for pin in _current_identity_pins():
        assert f"r.{pin} = " in _RESULTS_SQL, f"{pin} is pinned by the prior-version reader but not by _RESULTS_SQL"
