"""#2624 scope 3 — the strategy-scan freshness verdict, as pure policy.

No DB: the rule is arithmetic over three measured inputs, and the reader is one
query. Per the repo's lean-test guidance, the decision is extracted into a pure
function and table-tested rather than staged in Postgres.

⚠ The premise these tests exist to hold is that the observable is the WATERMARK,
not the signal. Measured on dev 2026-08-13, ``s2-cross-sectional-momentum`` has
zero ``strategy_signals`` rows under any version while carrying a current-version
watermark — so a signal-aged check would alert on it forever and never clear.
``test_a_strategy_that_emits_no_signals_is_still_healthy`` is that case.
"""

from datetime import date

import pytest

from app.services.strategy_scan_freshness import (
    StrategyScanFreshness,
    assess_scan_freshness,
    check_scan_freshness,
)

# 10 consecutive weekday "trading dates", ascending, ending 2026-08-12 — the live
# corpus frontier on the day this was written.
_TRADING_DATES = [
    date(2026, 7, 30),
    date(2026, 7, 31),
    date(2026, 8, 3),
    date(2026, 8, 4),
    date(2026, 8, 5),
    date(2026, 8, 6),
    date(2026, 8, 7),
    date(2026, 8, 10),
    date(2026, 8, 11),
    date(2026, 8, 12),
]

_S1 = "s1-time-series-momentum"
_CURRENT = "strategy-registry-v1+67dbf07c9d72"
_PRIOR = "strategy-registry-v1+2307ee566d7b"


def _assess(
    watermarks: dict[tuple[str, str], date],
    *,
    trading_dates: list[date] | None = None,
    versions: dict[str, str] | None = None,
) -> StrategyScanFreshness:
    results = assess_scan_freshness(
        current_versions=versions if versions is not None else {_S1: _CURRENT},
        watermarks=watermarks,
        trading_dates=_TRADING_DATES if trading_dates is None else trading_dates,
    )
    assert len(results) == 1
    return results[0]


def test_the_healthy_steady_state_is_one_bar_of_lag() -> None:
    """The by-design arrears IS the healthy value, not a tolerance spent.

    ``strategy_signal_scan`` runs one bar in arrears because a signal on the last
    bar of a series has no t+1, so a frontier level with the corpus would be the
    anomaly. Pinned against the live reading: corpus 2026-08-12, frontier
    2026-08-11, lag 1.
    """
    verdict = _assess({(_S1, _CURRENT): date(2026, 8, 11)})
    assert (verdict.status, verdict.basis, verdict.lag_bars) == ("ok", "current", 1)
    assert verdict.corpus_date == date(2026, 8, 12)
    assert not verdict.is_alerting


def test_a_strategy_that_emits_no_signals_is_still_healthy() -> None:
    """The s2 case, and the whole reason the watermark is the observable.

    Nothing in this input mentions signals — which is the point: a strategy that
    scans and fires nothing is indistinguishable here from one that fires
    thousands, because both advance the watermark. #2624's proposed signal-aged
    check would have alerted on s2 permanently and could never have cleared.
    """
    verdict = _assess(
        {("s2-cross-sectional-momentum", _CURRENT): date(2026, 8, 11)},
        versions={"s2-cross-sectional-momentum": _CURRENT},
    )
    assert verdict.status == "ok"


@pytest.mark.parametrize(
    ("frontier", "expected_lag", "expected_status"),
    [
        # The arrears bar.
        (date(2026, 8, 11), 1, "ok"),
        # One missed daily tick — still inside tolerance, by construction.
        (date(2026, 8, 10), 2, "ok"),
        # The second missed bar becoming visible is what turns it red. This is
        # the detection latency the spec states, distinct from the tolerance.
        (date(2026, 8, 7), 3, "stale"),
        (date(2026, 7, 30), 9, "stale"),
    ],
)
def test_the_threshold_sits_exactly_one_missed_run_above_the_arrears(
    frontier: date, expected_lag: int, expected_status: str
) -> None:
    verdict = _assess({(_S1, _CURRENT): frontier})
    assert (verdict.lag_bars, verdict.status) == (expected_lag, expected_status)
    assert verdict.max_lag_bars == 2


def test_the_lag_is_trading_days_so_a_weekend_does_not_move_it() -> None:
    """What makes the threshold weekend-immune rather than weekend-padded.

    ``price_daily`` does not advance over a weekend either, so Friday's frontier
    read against Monday's corpus is the same 1 it read on Friday. A calendar
    threshold would have to absorb a three-day weekend and would then be too
    loose to catch the two-day outage it exists for.
    """
    friday_to_monday = _assess({(_S1, _CURRENT): date(2026, 8, 7)}, trading_dates=_TRADING_DATES[:8])
    assert (friday_to_monday.corpus_date, friday_to_monday.lag_bars) == (date(2026, 8, 10), 1)
    assert friday_to_monday.status == "ok"


def test_a_rotation_with_a_recent_prior_scan_is_not_an_alert() -> None:
    """The state between a registry-touching merge and the next 06:45 tick.

    A rotation does NOT trigger a scan — the job is a scheduled daily one with no
    rotation hook — so the live version has no track record for up to ~24h.
    Without this state the check fires on every registry-touching merge, which is
    the false positive #2624's own text rules out.
    """
    verdict = _assess({(_S1, _PRIOR): date(2026, 8, 11)})
    assert (verdict.status, verdict.basis, verdict.lag_bars) == ("rotated_awaiting_scan", "fallback", 1)
    assert not verdict.is_alerting


def test_a_rotation_whose_prior_scan_is_also_behind_alerts() -> None:
    """#2624's measured 2026-08-12 symptom, which is what this ticket is for.

    Four versions rotated, 0 rows under them, max signal bar days behind. The
    verdict does not need a rotation timestamp — ``strategy_scan_watermark``
    stores none — because the operator-visible truth is the same either way: no
    scan under this strategy has reached within reach of the corpus.
    """
    verdict = _assess({(_S1, _PRIOR): date(2026, 8, 6)})
    assert (verdict.status, verdict.basis, verdict.lag_bars) == ("rotated_scan_overdue", "fallback", 4)
    assert verdict.is_alerting


def test_the_newest_prior_version_wins_the_fallback() -> None:
    """Two stale versions and one recent one must not read as stale."""
    verdict = _assess(
        {
            (_S1, _PRIOR): date(2026, 8, 11),
            (_S1, "strategy-registry-v1+000000000000"): date(2026, 7, 30),
        }
    )
    assert (verdict.status, verdict.frontier_date) == ("rotated_awaiting_scan", date(2026, 8, 11))


def test_a_watermark_ahead_of_the_corpus_is_a_regression_not_health() -> None:
    """A rewash, a restore, or a rule-set bump that emptied the coverage table.

    ``run_signal_scan`` has its own branch for this (``declining to write``). It
    MUST be tested before the lag comparison: a regressed corpus makes the lag
    read 0, which is the healthiest number there is. Caught by Codex at
    checkpoint 1, where the first draft's ``ok`` swallowed it silently.
    """
    verdict = _assess({(_S1, _CURRENT): date(2026, 8, 20)})
    assert verdict.status == "frontier_regressed"
    assert verdict.is_alerting


def test_a_strategy_with_no_watermark_under_any_version_is_never_scanned() -> None:
    """Reported, but NOT alerting — a decision this repo already settled.

    ``_derive_overall_status``' docstring refuses to degrade the headline for a
    job with no runs ever recorded, because a fresh deploy would then always read
    degraded for having not started yet, and the empty data layers are the more
    meaningful signal. ``never_scanned`` is the strategy-scan analogue.

    Found by ``test_api_system``'s healthy-system fixtures, which have no
    watermarks at all and began reading ``degraded`` when this status alerted.
    The 2026-08-12 symptom the ticket exists for is unaffected: that is
    ``rotated_scan_overdue``, which by definition has prior watermarks.
    """
    verdict = _assess({})
    assert (verdict.status, verdict.basis, verdict.frontier_date) == ("never_scanned", None, None)
    assert not verdict.is_alerting


def test_an_empty_corpus_is_contained_rather_than_raising() -> None:
    """Reachable only pre-bootstrap, and an unassessable strategy still gets a row."""
    verdict = _assess({(_S1, _CURRENT): date(2026, 8, 11)}, trading_dates=[])
    assert verdict.status == "error"
    assert verdict.detail is not None


def test_a_lag_beyond_the_loaded_window_is_reported_as_inexact() -> None:
    """A number that looks precise and is not would be the worse failure.

    The reader loads a bounded window, so a watermark older than it cannot yield
    a true lag. The status is unaffected — it is over the threshold either way —
    but ``lag_exact`` marks the figure as a lower bound rather than letting the
    operator read the window size as the answer.
    """
    verdict = _assess({(_S1, _CURRENT): date(2026, 1, 5)})
    assert (verdict.status, verdict.lag_bars, verdict.lag_exact) == ("stale", len(_TRADING_DATES), False)


def test_every_manifest_strategy_gets_exactly_one_verdict() -> None:
    """A strategy with no verdict is a strategy nothing reports on."""
    versions = {"a": "v1", "b": "v1", "c": "v1"}
    results = assess_scan_freshness(
        current_versions=versions,
        watermarks={("a", "v1"): date(2026, 8, 11)},
        trading_dates=_TRADING_DATES,
    )
    assert [entry.strategy_id for entry in results] == ["a", "b", "c"]
    assert [entry.status for entry in results] == ["ok", "never_scanned", "never_scanned"]


def test_a_failed_probe_leaves_the_connection_usable(monkeypatch: pytest.MonkeyPatch) -> None:
    """The containment claim, tested as CONTAINMENT and not just as a caught error.

    ``get_conn`` hands out a NON-autocommit connection, so a failed query leaves
    Postgres' transaction aborted and catching the exception does not clear it —
    the next statement raises ``InFailedSqlTransaction``. In
    ``get_system_status`` that next statement is
    ``_build_credential_health_summary``, which sits outside the handler's guard,
    so a bare try/except would turn the intended ``error`` row into an HTTP 500.

    The fake connection models exactly that rule: statements raise while
    ``aborted`` is set, and only ``rollback`` (what ``conn.transaction()`` issues
    on the savepoint) clears it. Asserting ``status == "error"`` alone would pass
    against the broken version, which is why the post-condition is a SUBSEQUENT
    query rather than the returned row.
    """

    class _FakeTransaction:
        def __init__(self, conn: _FakeConn) -> None:
            self._conn = conn

        def __enter__(self) -> _FakeTransaction:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
            if exc_type is not None:
                self._conn.aborted = False  # the savepoint rollback
            return False

    class _FakeConn:
        def __init__(self) -> None:
            self.aborted = False

        def transaction(self) -> _FakeTransaction:
            return _FakeTransaction(self)

        def execute(self, *_args: object, **_kwargs: object) -> object:
            if self.aborted:
                raise RuntimeError("InFailedSqlTransaction")
            self.aborted = True
            raise RuntimeError("probe blew up")

    conn = _FakeConn()
    monkeypatch.setattr(
        "app.services.strategy_scan_freshness.read_scan_freshness_inputs",
        lambda c: c.execute("SELECT 1"),
    )

    verdicts = check_scan_freshness(conn)  # type: ignore[arg-type]

    assert [v.status for v in verdicts] == ["error"]
    assert verdicts[0].is_alerting
    # The post-condition that actually distinguishes contained from poisoned.
    assert not conn.aborted, "a failed probe left the connection in an aborted transaction"
