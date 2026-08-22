"""Is each strategy's shadow track record keeping up with the corpus? (#2624 scope 3)

The signal nothing else carries: ``strategy_signal_scan`` **succeeded** and the
strategy's CURRENT identity version still has no track record within reach of the
price frontier.  ``check_job_health`` sees a failed or stalled run; the layer
checks see a stale corpus; neither sees a green job that left the live version
dark, which is the #2218 invisible-no-op class #2624 is about.

⚠ Deliberately NOT a ``_STALENESS_THRESHOLDS`` row.  ``check_layer_staleness``
ages one table against one constant, and a plain ``strategy_signals`` layer on
that framework would fire on every registry-touching merge — a fresh version
legitimately has no rows yet.

⚠ The observable is the WATERMARK, not the signal.  Measured on dev 2026-08-13:
``s2-cross-sectional-momentum`` has **zero** ``strategy_signals`` rows under any
version while carrying a watermark at ``2026-08-11``.  A signal is an output whose
emptiness is a legitimate outcome (``scheduler.py::strategy_signal_scan``: *"zero
is a legitimate success"*), so ageing signals would fire permanently for s2 and
could never clear.

⚠ There is NO "while prices are fresh" conjunct, and #2624's text asking for one
cannot be honoured as written: ``_LAYER_QUERIES["prices"]`` ages
``MAX(price_date)::timestamptz`` — midnight of the last trading date — against a
4-hour threshold, so the prices layer reads ``stale`` from ~04:00 UTC daily and all
weekend, on a healthy system.  Gating on it would ship a check that never
evaluates.  It is also unnecessary: the lag is measured in TRADING DAYS against
``price_daily`` itself, so a corpus that stops advancing cannot grow the lag.  The
suppression is structural rather than conditional.

Spec: ``docs/proposals/ops/2026-08-13-strategy-scan-freshness.md``
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence, Set
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal

import psycopg

logger = logging.getLogger(__name__)

ScanFreshnessStatus = Literal[
    "ok",
    #: #2845 — the manifest declares this strategy retired, so it is EXPECTED not
    #: to scan. ⚠⚠ Non-alerting, and that is the whole point of the status. A
    #: retired strategy's watermark freezes the day it retires, so without this
    #: every poll of /system/status would report it `stale` for ever — the
    #: prevention-log defect of an alarm with a documented "ignore this" attached,
    #: which is strictly worse than no alarm because it still costs attention and
    #: occupies the slot a working detector would have.
    "retired",
    "stale",
    "rotated_awaiting_scan",
    "rotated_scan_overdue",
    "frontier_regressed",
    "never_scanned",
    "error",
]

#: Which watermark the lag was measured from.  ``current`` is the live identity
#: version; ``fallback`` is the newest watermark on ANY version of that strategy,
#: used when the live version has none yet.
ScanFreshnessBasis = Literal["current", "fallback"]

# Trading bars of lag a healthy scan may show.  Derived, not chosen (spec "Source
# rule"):
#   1  the by-design arrears -- SCHEDULED_JOBS[strategy_signal_scan] runs "one bar
#      in ARREARS", so a healthy frontier is ALWAYS one bar behind the corpus;
#   1  one missed daily 06:45 UTC tick, matching _STALENESS_THRESHOLDS' own stated
#      convention ("2 days allows for a missed night").
# Alert strictly above.  ⚠ Detection latency is not the same number: baseline 1 +
# one missed session reads 2 and stays healthy, so the check turns red only once a
# SECOND missed bar appears in price_daily.
_MAX_SCAN_LAG_BARS = 2

# How many recent trading dates the reader loads.  Bounds the query and the lag we
# can report EXACTLY; a basis older than the window reports the window size with
# `lag_exact=False` rather than a number that looks precise and is not.
_TRADING_DATE_WINDOW = 30

# ⚠ ``never_scanned`` is deliberately NOT here, and the reason is a decision this
# repo already settled one component over.  ``_derive_overall_status``' own
# docstring: *"Jobs with `last_status is None` (no runs ever recorded) are
# deliberately NOT treated as degraded on their own — a fresh deploy would
# otherwise always report 'degraded' purely because no jobs have fired yet …
# A fresh deploy will still report 'degraded' via the empty data layers, which is
# the more meaningful signal anyway."*  ``never_scanned`` is the strategy-scan
# analogue of exactly that, so it reports the state without claiming a fault.
# Caught by `test_api_system`'s healthy-system fixtures reading ``degraded``.
#
# The 2026-08-12 symptom this ticket exists for is NOT affected: that is
# ``rotated_scan_overdue``, which by definition has prior watermarks.
_ALERTING_STATUSES: frozenset[str] = frozenset({"stale", "rotated_scan_overdue", "frontier_regressed", "error"})

# A LOOSE INDEX SCAN, not a `DISTINCT ... LIMIT`, and the difference is the point.
# `/system/status` is polled, so this runs constantly; the cost must not scale with
# the corpus.  Measured on dev (6,755,721 rows, 1,995 distinct dates, 30-date
# window), each form after `ANALYZE`:
#
#   SELECT DISTINCT price_date … ORDER BY … LIMIT 30
#     without idx_price_daily_price_date : Parallel Seq Scan, 70,183 buffers, 279 ms
#     with it                            : Limit->Unique->Index Scan Backward,
#                                          93,734 buffers, 57 ms warm / 523 ms cold
#   this recursive form                  : 130 buffers, 1.0 ms
#
# The index alone is not enough because `Unique` over an index scan still walks
# EVERY ROW of each of the 30 dates -- ~3,400 instruments each -- so its cost grows
# with instruments-per-date, which is the growth Codex flagged at checkpoint 2.
# The recursion instead does exactly ``window`` ``max()`` probes, each an
# index-only backward scan, so the cost is a function of the WINDOW and nothing
# else.  ⚠ The ``n`` counter is load-bearing: without it the recursion runs to
# corpus exhaustion (1,995 iterations) because an outer LIMIT cannot stop a
# recursive CTE.  Requires sql/345.
_RECENT_TRADING_DATES = """
WITH RECURSIVE d(price_date, n) AS (
    SELECT max(price_date), 1 FROM price_daily
    UNION ALL
    SELECT (SELECT max(p.price_date) FROM price_daily p WHERE p.price_date < d.price_date), d.n + 1
    FROM d WHERE d.price_date IS NOT NULL AND d.n < %(window)s
)
SELECT price_date FROM d WHERE price_date IS NOT NULL ORDER BY price_date
"""


@dataclass(frozen=True)
class StrategyScanFreshness:
    """One strategy's verdict, carrying the arithmetic that produced it."""

    strategy_id: str
    strategy_version: str
    status: ScanFreshnessStatus
    basis: ScanFreshnessBasis | None
    frontier_date: date | None
    corpus_date: date | None
    lag_bars: int | None
    lag_exact: bool
    max_lag_bars: int
    detail: str | None = None

    @property
    def is_alerting(self) -> bool:
        return self.status in _ALERTING_STATUSES


def _lag_bars(basis: date, trading_dates: Sequence[date]) -> tuple[int, bool]:
    """Trading dates strictly after ``basis``, and whether that count is exact.

    Inexact when ``basis`` predates the loaded window: the true lag is at least
    the window size, which already exceeds any threshold, and reporting the window
    size as if it were the real number would be a derived statistic that is wrong
    in the place an operator trusts most.
    """
    lag = sum(1 for day in trading_dates if day > basis)
    return lag, basis >= trading_dates[0]


def assess_scan_freshness(
    *,
    current_versions: Mapping[str, str],
    watermarks: Mapping[tuple[str, str], date],
    trading_dates: Sequence[date],
    retired_ids: Set[str] = frozenset(),
) -> list[StrategyScanFreshness]:
    """Pure verdict for every strategy in ``current_versions``.

    ``trading_dates`` is the recent distinct ``price_daily`` dates, ASCENDING.
    ``watermarks`` is ``read_watermarks``' shape, keyed ``(strategy_id, version)``.

    Returns one entry per strategy, always — a strategy with no verdict is a
    strategy nothing reports on. That includes retired ones (#2845): they get a
    `retired` verdict rather than disappearing, for the same reason.
    """
    corpus_date = trading_dates[-1] if trading_dates else None
    newest_by_strategy: dict[str, date] = {}
    for (strategy_id, _version), frontier in watermarks.items():
        seen = newest_by_strategy.get(strategy_id)
        if seen is None or frontier > seen:
            newest_by_strategy[strategy_id] = frontier

    results: list[StrategyScanFreshness] = []
    for strategy_id in sorted(current_versions):
        version = current_versions[strategy_id]
        current = watermarks.get((strategy_id, version))
        fallback = newest_by_strategy.get(strategy_id)
        basis_date = current if current is not None else fallback
        basis: ScanFreshnessBasis | None = (
            None if basis_date is None else ("current" if current is not None else "fallback")
        )

        def _verdict(
            status: ScanFreshnessStatus,
            *,
            lag: int | None = None,
            exact: bool = True,
            detail: str | None = None,
        ) -> StrategyScanFreshness:
            return StrategyScanFreshness(
                strategy_id=strategy_id,
                strategy_version=version,
                status=status,
                basis=basis,
                frontier_date=basis_date,
                corpus_date=corpus_date,
                lag_bars=lag,
                lag_exact=exact,
                max_lag_bars=_MAX_SCAN_LAG_BARS,
                detail=detail,
            )

        if strategy_id in retired_ids:
            # #2845. FIRST, ahead of every measurement: a retired strategy is
            # expected not to scan, so its lag is not a defect and reporting one
            # would be a permanent false alarm. The verdict still carries the
            # watermark it froze at, so "when did it stop" stays answerable.
            results.append(_verdict("retired", detail="retired in the strategy manifest"))
            continue
        if basis_date is None:
            # No watermark under ANY version. One state, one meaning -- "no track
            # record at all" -- which also absorbs a renamed strategy_id and a
            # purged history, and does not claim to say which.
            results.append(_verdict("never_scanned"))
            continue
        if corpus_date is None:
            # Reachable only before the corpus exists (pre-bootstrap). Contained
            # rather than raising: an unassessable strategy must still get a row.
            results.append(_verdict("error", detail="no price corpus to measure against"))
            continue
        if basis_date > corpus_date:
            # The corpus went BACKWARDS under a completed scan -- a rewash, a
            # restore, a rule-set bump that emptied the coverage table.
            # `run_signal_scan` has its own branch for this
            # (strategy_signal_scan.py:492, "declining to write"). It must be
            # tested BEFORE the lag, which a regressed corpus makes read 0 and
            # therefore healthy.
            results.append(_verdict("frontier_regressed", lag=0, detail="watermark is ahead of the price corpus"))
            continue

        lag, exact = _lag_bars(basis_date, trading_dates)
        breached = lag > _MAX_SCAN_LAG_BARS
        if basis == "current":
            results.append(_verdict("stale" if breached else "ok", lag=lag, exact=exact))
        else:
            # The live version has no track record yet. Ageing the newest
            # watermark on any version answers the question that matters --
            # "has this strategy scanned recently" -- without a rotation
            # timestamp, which strategy_scan_watermark does not store.
            results.append(
                _verdict("rotated_scan_overdue" if breached else "rotated_awaiting_scan", lag=lag, exact=exact)
            )
    return results


def read_scan_freshness_inputs(
    conn: psycopg.Connection[Any],
) -> tuple[dict[str, str], dict[tuple[str, str], date], list[date], frozenset[str]]:
    """The measured inputs, so the verdict itself stays pure and testable.

    ``retired_ids`` is read from the manifest here rather than in the pure verdict,
    for the same reason as the other three: the verdict must be table-testable
    without importing the live catalogue (#2845).
    """
    from app.services.cost_model import COST_MODEL_ID
    from app.services.strategy_manifest import STRATEGY_MANIFEST
    from app.services.strategy_signal_scan import SCAN_UNIVERSE, read_watermarks

    current_versions = {
        strategy_id: entry.identity(universe=SCAN_UNIVERSE, cost_model_id=COST_MODEL_ID).version
        for strategy_id, entry in STRATEGY_MANIFEST.items()
    }
    retired_ids = frozenset(
        strategy_id for strategy_id, entry in STRATEGY_MANIFEST.items() if entry.retired_reason is not None
    )
    watermarks = read_watermarks(conn)
    trading_dates = [row[0] for row in conn.execute(_RECENT_TRADING_DATES, {"window": _TRADING_DATE_WINDOW}).fetchall()]
    return current_versions, watermarks, trading_dates, retired_ids


def check_scan_freshness(conn: psycopg.Connection[Any]) -> list[StrategyScanFreshness]:
    """Read the inputs and return one verdict per strategy.

    Never raises: a reader failure yields one ``error`` row, so a probe failure
    cannot 503 the whole status endpoint.

    ⚠ ``conn.transaction()`` is the containment, and a bare ``try/except`` is NOT.
    ``get_conn`` hands out a NON-autocommit pooled connection, so a failed query
    leaves Postgres' transaction ABORTED; catching the exception does not clear
    that, and the next statement on the same connection raises
    ``InFailedSqlTransaction``. In ``get_system_status`` the next statement is
    ``_build_credential_health_summary(conn)``, which sits OUTSIDE the handler's
    try — so the "contained" error row would have become an HTTP 500. Verified
    both ways on dev: bare catch → the following ``SELECT 1`` raises; inside a
    savepoint → it succeeds. Raised by Codex at checkpoint 2.
    """
    try:
        with conn.transaction():
            current_versions, watermarks, trading_dates, retired_ids = read_scan_freshness_inputs(conn)
    except Exception:
        logger.exception("check_scan_freshness: failed to read inputs")
        return [
            StrategyScanFreshness(
                strategy_id="*",
                strategy_version="",
                status="error",
                basis=None,
                frontier_date=None,
                corpus_date=None,
                lag_bars=None,
                lag_exact=False,
                max_lag_bars=_MAX_SCAN_LAG_BARS,
                detail="scan freshness query failed (see server logs)",
            )
        ]
    return assess_scan_freshness(
        current_versions=current_versions,
        watermarks=watermarks,
        trading_dates=trading_dates,
        retired_ids=retired_ids,
    )


__all__ = [
    "ScanFreshnessBasis",
    "ScanFreshnessStatus",
    "StrategyScanFreshness",
    "assess_scan_freshness",
    "check_scan_freshness",
    "read_scan_freshness_inputs",
]
