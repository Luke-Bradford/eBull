"""Resolve mature forward strategy signals into a bounded durable track record.

The daily signal scan records a fired decision and its causal next-open fill.
This service revisits only level-based entries whose current version has no
outcome at the current resolver/input-rule pair. An incomplete holding window is
not an outcome: it is left pending and retried after more bars arrive.

Storage is deliberately one terminal row per fired signal and version pair. No
polling snapshots or immature rows are appended, so growth is bounded by the
number of decisions the strategies actually fire.
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Final

import psycopg

from app.services.cost_model import COST_MODEL_ID
from app.services.indicator_series import BarSeries
from app.services.outcome_ledger import (
    OutcomeRow,
    PendingFill,
    bar_should_exist,
    fill_price_is_superseded,
    locate_fill_index,
    select_pending_fills,
    store_outcomes,
)
from app.services.outcome_resolver import RULE_SET_VERSION as OUTCOME_RULE_SET_VERSION
from app.services.outcome_resolver import Outcome, UnresolvedReason, resolve_outcome
from app.services.price_masked_bars import MASKED_REASON, QUARANTINE_RULE_SET_VERSION, load_masked_bars
from app.services.price_segments import load_unresolved_breaks, segment_end_index, segment_for_index
from app.services.strategy_manifest import STRATEGY_MANIFEST, StrategyEntry
from app.services.strategy_signal_scan import SCAN_UNIVERSE

logger = logging.getLogger(__name__)

DEFAULT_BATCH_LIMIT: Final = 1_000

_READ_CURSOR_SQL = """
    SELECT last_signal_id
    FROM strategy_outcome_cursor
    WHERE strategy_id = %(strategy_id)s
      AND strategy_version = %(strategy_version)s
      AND rule_set_version = %(rule_set_version)s
      AND input_rule_set_version = %(input_rule_set_version)s
"""

_WRITE_CURSOR_SQL = """
    INSERT INTO strategy_outcome_cursor (
        strategy_id, strategy_version, rule_set_version,
        input_rule_set_version, last_signal_id, updated_at
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(rule_set_version)s,
        %(input_rule_set_version)s, %(last_signal_id)s, now()
    )
    ON CONFLICT (strategy_id, strategy_version, rule_set_version, input_rule_set_version)
    DO UPDATE SET last_signal_id = EXCLUDED.last_signal_id, updated_at = now()
"""


@dataclass(frozen=True)
class StrategyOutcomeResult:
    strategy_id: str
    strategy_version: str
    selected: int = 0
    written: int = 0
    immature: int = 0
    ambiguous: int = 0
    outcomes: Mapping[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class OutcomeResolutionReport:
    selected: int = 0
    written: int = 0
    immature: int = 0
    ambiguous: int = 0
    skipped_non_level_strategies: int = 0
    per_strategy: tuple[StrategyOutcomeResult, ...] = ()


def _masked_reasons(series_rows: Sequence[Mapping[str, object]]) -> dict[int, UnresolvedReason]:
    """Name unavailable resolver inputs from the fail-closed live loader."""
    return {
        index: MASKED_REASON
        for index, row in enumerate(series_rows)
        if any(row.get(field) is None for field in ("open", "high", "low", "close"))
    }


def _locate_signal_index(series_dates: Sequence[date], signal_bar_date: date) -> int:
    try:
        return series_dates.index(signal_bar_date)
    except ValueError:
        raise ValueError(
            f"signal_bar_date {signal_bar_date} is absent from the current live series; "
            "the durable signal and its input corpus no longer agree"
        ) from None


def _unresolved_row(signal_id: int, reason: UnresolvedReason) -> OutcomeRow:
    """One recorded refusal, so a corpus disagreement cannot abort the batch.

    Every caller is a condition that RAISED before #3189 finding 10, so this
    can only change rows that previously crashed — no row that resolves today is
    relabelled.
    """
    return OutcomeRow.from_outcome(
        signal_id,
        Outcome(
            outcome="unresolved",
            resolution_method="daily_bar",
            rule_set_version=OUTCOME_RULE_SET_VERSION,
            reason=reason,
        ),
        input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
    )


def _resolve_fill(
    entry: StrategyEntry,
    fill: PendingFill,
    *,
    series: BarSeries,
    unresolved_breaks: Sequence[date],
    coverage: tuple[date, date] | None = None,
    masked_bar_reasons: Mapping[int, UnresolvedReason] | None = None,
) -> OutcomeRow | None:
    """Return a terminal row, or ``None`` while the forward window is immature."""
    if entry.exit_levels is None:
        raise ValueError(f"{entry.strategy_id} is level-based but has no exit-level factory")
    if fill.universe != SCAN_UNIVERSE:
        raise ValueError(
            f"signal {fill.signal_id} carries universe {fill.universe!r}, expected current scan universe "
            f"{SCAN_UNIVERSE!r}"
        )
    # ⚠⚠ A STORED DATE THE CORPUS NO LONGER HOLDS IS RECORDED, NOT RAISED
    # (#3189 finding 10). Both locators are guards against silently re-reading
    # "whatever bar sits at that position now" after a rebuild, and that intent
    # is kept — an `unresolved` row NAMES the disagreement rather than
    # reinterpreting it. What changes is only the blast radius: a raise here
    # escapes three nested loops and aborts the strategy BEFORE its cursor is
    # written, so the next tick re-selects the same fills and raises again,
    # while every alphabetically later strategy in the tick never runs. Same
    # treatment sql/396 gave the sibling case, and the same wedge shape as
    # #3189 finding 7 in the order poller.
    #
    # ⚠ `except ValueError` is safe to keep this narrow because each locator
    # raises exactly one ValueError and does nothing else; it is deliberately
    # NOT wrapped around the resolver below, whose ValueErrors are genuine
    # contract breaches (an unorderable bracket, a non-positive entry) that must
    # stay loud.
    try:
        signal_index = _locate_signal_index(series.dates, fill.signal_bar_date)
    except ValueError:
        if not bar_should_exist(coverage, fill.signal_bar_date):
            return None
        return _unresolved_row(fill.signal_id, "signal_bar_absent")
    try:
        fill_index = locate_fill_index(series, fill.fill_bar_date)
    except ValueError:
        if not bar_should_exist(coverage, fill.fill_bar_date):
            return None
        return _unresolved_row(fill.signal_id, "fill_bar_absent")
    signal_scale_segment_end = segment_end_index(
        series,
        fill_index=signal_index,
        unresolved_breaks=unresolved_breaks,
    )
    if signal_scale_segment_end is not None and fill_index > signal_scale_segment_end:
        return OutcomeRow.from_outcome(
            fill.signal_id,
            Outcome(
                outcome="unresolved",
                resolution_method="daily_bar",
                rule_set_version=OUTCOME_RULE_SET_VERSION,
                reason="series_break",
            ),
            input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
        )
    signal_segment, local_signal_index = segment_for_index(
        series,
        index=signal_index,
        unresolved_breaks=unresolved_breaks,
    )
    # ⚠⚠ BEFORE the levels factory, not after. This is the first point at which
    # the STORED entry price meets a FRESHLY LOADED series, and the factory is
    # its first consumer — it derives a bracket from both, so on a rescaled
    # series it can return levels that are unorderable against the stored entry,
    # and `resolve_outcome` then raises. Either way the raise escapes the batch:
    # `_resolve_fill` is called inside three nested loops with no handler, so one
    # unresolvable row aborts its strategy before the cursor advances AND every
    # alphabetically later strategy in the same tick. Same treatment as #2489's
    # unorderable bracket (sql/296): a closed refusal code, recorded.
    #
    # ⚠ AFTER the series-break test, so no row that resolves today is relabelled.
    # A break between signal and fill keeps `series_break`.
    if fill_price_is_superseded(series, fill_index=fill_index, fill_price=fill.fill_price):
        return OutcomeRow.from_outcome(
            fill.signal_id,
            Outcome(
                outcome="unresolved",
                resolution_method="daily_bar",
                rule_set_version=OUTCOME_RULE_SET_VERSION,
                reason="fill_price_superseded",
            ),
            input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
        )
    levels = entry.exit_levels(
        signal_segment,
        signal_index=local_signal_index,
        entry_price=fill.fill_price,
        universe=fill.universe,
    )
    if isinstance(levels, str):
        return OutcomeRow.from_outcome(
            fill.signal_id,
            Outcome(
                outcome="unresolved",
                resolution_method="daily_bar",
                rule_set_version=OUTCOME_RULE_SET_VERSION,
                reason=levels,
            ),
            input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
        )
    # ⚠ The fill bar survives but its open does not load — `load_masked_bars`
    # nulls an open that is NULL or <= 0 (`price_masked_bars.py:220`).
    # `fill_price_is_superseded` deliberately returns False here ("None is not
    # evidence the bar moved"), so without this the flow reaches
    # `resolve_outcome`, which raises "bar N has no open, so it cannot be a fill
    # bar" — correct, and a batch-aborting way to say it (#3189 finding 10).
    #
    # ⚠⚠ IMMEDIATELY BEFORE `resolve_outcome`, and after BOTH the supersession
    # test and the levels branch (Codex checkpoint 2). Placed any earlier it
    # relabels a row that resolves today: a fill whose open is masked AND whose
    # bracket is unreconstructible records `unorderable_exit_levels` now, and
    # must keep doing so. The invariant this whole change rests on is that only
    # rows that previously CRASHED can move.
    if series.rows[fill_index].get("open") is None:
        return _unresolved_row(fill.signal_id, "fill_bar_open_absent")
    outcome = resolve_outcome(
        series=series,
        fill_index=fill_index,
        entry_price=fill.fill_price,
        levels=levels,
        masked_bar_reasons=masked_bar_reasons if masked_bar_reasons is not None else _masked_reasons(series.rows),
        segment_end_index=segment_end_index(
            series,
            fill_index=fill_index,
            unresolved_breaks=unresolved_breaks,
        ),
    )
    if outcome.outcome == "unresolved" and outcome.reason == "window_truncated":
        return None
    return OutcomeRow.from_outcome(
        fill.signal_id,
        outcome,
        input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
    )


def _cursor_params(strategy_id: str, strategy_version: str) -> dict[str, str]:
    return {
        "strategy_id": strategy_id,
        "strategy_version": strategy_version,
        "rule_set_version": OUTCOME_RULE_SET_VERSION,
        "input_rule_set_version": QUARANTINE_RULE_SET_VERSION,
    }


def _read_cursor(conn: psycopg.Connection[Any], *, strategy_id: str, strategy_version: str) -> int:
    row = conn.execute(_READ_CURSOR_SQL, _cursor_params(strategy_id, strategy_version)).fetchone()
    return 0 if row is None else int(row[0])


def _write_cursor(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    last_signal_id: int,
) -> None:
    conn.execute(
        _WRITE_CURSOR_SQL,
        {**_cursor_params(strategy_id, strategy_version), "last_signal_id": last_signal_id},
    )


def _select_round_robin(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    cursor: int,
    limit: int,
) -> list[PendingFill]:
    """Select after ``cursor``, then wrap once without repeating a row."""
    fills = select_pending_fills(
        conn,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        rule_set_version=OUTCOME_RULE_SET_VERSION,
        input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
        limit=limit,
        after_signal_id=cursor,
    )
    if len(fills) < limit and cursor > 0:
        fills.extend(
            select_pending_fills(
                conn,
                strategy_id=strategy_id,
                strategy_version=strategy_version,
                rule_set_version=OUTCOME_RULE_SET_VERSION,
                input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
                limit=limit - len(fills),
                after_signal_id=0,
                at_or_before_signal_id=cursor,
            )
        )
    return fills


def run_outcome_resolution(
    conn: psycopg.Connection[Any],
    *,
    manifest: Mapping[str, StrategyEntry] = STRATEGY_MANIFEST,
    batch_limit: int = DEFAULT_BATCH_LIMIT,
) -> OutcomeResolutionReport:
    """Resolve a round-robin batch of current-version forward fills.

    The connection is autocommit for the same reason as the signal scan: each
    strategy owns a real top-level transaction, so one faulty strategy cannot
    roll back healthy strategies or leave a partial batch committed.
    """
    if not conn.autocommit:
        raise ValueError("run_outcome_resolution needs an autocommit connection for per-strategy commits")
    if batch_limit < 1:
        raise ValueError(f"batch_limit must be positive, got {batch_limit}")

    # ⚠⚠ RETIRED STRATEGIES ARE **NOT** FILTERED OUT HERE (#2845), and that is the
    # decision rather than an oversight — an unexamined omission and a deliberate
    # one look identical in a diff, so it is written down.
    #
    # Retirement stops a strategy producing NEW evidence; it never stops the drain
    # of old. s5/s6/s7/s9 are retired and level-based, and they hold already-fired
    # signals whose outcomes are unresolved. Filtering them would strand those
    # permanently — corrupting the very evidence record retirement exists to
    # preserve, and doing it silently, because an unresolved fill has no alarm.
    level_entries = [(strategy_id, entry) for strategy_id, entry in sorted(manifest.items()) if entry.exit_levels]
    if len(level_entries) > batch_limit:
        raise ValueError(
            f"batch_limit {batch_limit} is smaller than the {len(level_entries)} level strategies; "
            "at least one slot per strategy is required to prevent starvation"
        )
    remaining = batch_limit
    results: list[StrategyOutcomeResult] = []
    skipped = len(manifest) - len(level_entries)
    for index, (strategy_id, entry) in enumerate(level_entries):
        regime = entry.exit_regime(entry.decision_calendar(()))
        if not regime.level_based:
            raise ValueError(f"{strategy_id} declares exit levels but its exit regime is not level-based")
        strategies_left = len(level_entries) - index
        allowance = max(1, remaining // strategies_left)
        identity = entry.identity(universe=SCAN_UNIVERSE, cost_model_id=COST_MODEL_ID)
        cursor = _read_cursor(conn, strategy_id=strategy_id, strategy_version=identity.version)
        fills = _select_round_robin(
            conn,
            strategy_id=strategy_id,
            strategy_version=identity.version,
            cursor=cursor,
            limit=allowance,
        )
        remaining -= len(fills)
        by_instrument: dict[int, list[PendingFill]] = {}
        for fill in fills:
            by_instrument.setdefault(fill.instrument_id, []).append(fill)
        breaks = load_unresolved_breaks(conn, sorted(by_instrument))
        rows: list[OutcomeRow] = []
        immature = 0
        for instrument_id, instrument_fills in sorted(by_instrument.items()):
            masked = load_masked_bars(conn, instrument_id)
            series = masked.series
            masked_bar_reasons = _masked_reasons(series.rows)
            for fill in instrument_fills:
                row = _resolve_fill(
                    entry,
                    fill,
                    series=series,
                    unresolved_breaks=breaks.get(instrument_id, ()),
                    coverage=masked.coverage,
                    masked_bar_reasons=masked_bar_reasons,
                )
                if row is None:
                    immature += 1
                else:
                    rows.append(row)
        with conn.transaction():
            written = store_outcomes(conn, rows)
            if fills:
                _write_cursor(
                    conn,
                    strategy_id=strategy_id,
                    strategy_version=identity.version,
                    last_signal_id=fills[-1].signal_id,
                )
        outcomes = Counter(row.outcome for row in rows)
        results.append(
            StrategyOutcomeResult(
                strategy_id=strategy_id,
                strategy_version=identity.version,
                selected=len(fills),
                written=written,
                immature=immature,
                ambiguous=outcomes["ambiguous"],
                outcomes=dict(sorted(outcomes.items())),
            )
        )

    report = OutcomeResolutionReport(
        selected=sum(item.selected for item in results),
        written=sum(item.written for item in results),
        immature=sum(item.immature for item in results),
        ambiguous=sum(item.ambiguous for item in results),
        skipped_non_level_strategies=skipped,
        per_strategy=tuple(results),
    )
    logger.info(
        "strategy_outcome_resolution: selected=%d written=%d immature=%d ambiguous=%d skipped_non_level=%d limit=%d",
        report.selected,
        report.written,
        report.immature,
        report.ambiguous,
        report.skipped_non_level_strategies,
        batch_limit,
    )
    return report


__all__ = [
    "DEFAULT_BATCH_LIMIT",
    "OutcomeResolutionReport",
    "StrategyOutcomeResult",
    "run_outcome_resolution",
]
