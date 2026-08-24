"""Evidence-approved instrument for the deterministic core/cash sleeve (#2833)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Final, Literal

import psycopg

CORE_SELECTION_CANDIDATE_IDS: Final = (3417, 3434, 3075)
CORE_SELECTION_REQUIRED_TRADING_DAYS: Final = 5
CORE_SELECTION_MAX_COST_BPS: Final = 60

# Populated only by the reviewed #2833 verdict. Choosing here before the
# prospective five-day gate completes would be adoption before measurement.
SELECTED_CORE_INSTRUMENT_ID: Final[int | None] = None
SELECTED_CORE_EVIDENCE_REF: Final[str | None] = None

CoreSelectionState = Literal["evidence_collecting", "ready", "unavailable"]


class CoreSelectionError(RuntimeError):
    """An enabled mandate does not match an evidence-approved core sleeve."""


@dataclass(frozen=True)
class CoreCandidateCoverage:
    instrument_id: int
    symbol: str
    observed_trading_days: int
    first_observed_date: date | None
    last_observed_date: date | None


@dataclass(frozen=True)
class CoreSelection:
    state: CoreSelectionState
    selected_instrument_id: int | None
    selected_symbol: str | None
    evidence_ref: str | None
    required_trading_days: int
    observed_trading_days: int
    max_cost_bps: int
    candidates: tuple[CoreCandidateCoverage, ...]
    missing_candidate_ids: tuple[int, ...]
    configuration_error: str | None

    @property
    def ready(self) -> bool:
        return self.state == "ready"


_COVERAGE_SQL: Final = """
SELECT i.instrument_id, i.symbol,
       count(DISTINCT (o.observed_at AT TIME ZONE 'UTC')::date)
           FILTER (WHERE o.observation_status = 'observed') AS observed_days,
       min((o.observed_at AT TIME ZONE 'UTC')::date)
           FILTER (WHERE o.observation_status = 'observed') AS first_observed_date,
       max((o.observed_at AT TIME ZONE 'UTC')::date)
           FILTER (WHERE o.observation_status = 'observed') AS last_observed_date
FROM instruments i
LEFT JOIN strategy_core_quote_observations o ON o.instrument_id = i.instrument_id
WHERE i.instrument_id = ANY(%s)
GROUP BY i.instrument_id, i.symbol
ORDER BY array_position(%s::bigint[], i.instrument_id)
"""


def load_core_selection(conn: psycopg.Connection[Any]) -> CoreSelection:
    """Return reviewed selection and descriptive coverage, never infer a verdict."""
    rows = conn.execute(
        _COVERAGE_SQL,
        (list(CORE_SELECTION_CANDIDATE_IDS), list(CORE_SELECTION_CANDIDATE_IDS)),
    ).fetchall()
    candidates = tuple(
        CoreCandidateCoverage(
            instrument_id=int(row[0]),
            symbol=str(row[1]),
            observed_trading_days=int(row[2]),
            first_observed_date=row[3],
            last_observed_date=row[4],
        )
        for row in rows
    )
    coverage_by_id = {candidate.instrument_id: candidate for candidate in candidates}
    missing_candidate_ids = tuple(
        instrument_id for instrument_id in CORE_SELECTION_CANDIDATE_IDS if instrument_id not in coverage_by_id
    )
    observed_days = min(
        coverage_by_id[instrument_id].observed_trading_days if instrument_id in coverage_by_id else 0
        for instrument_id in CORE_SELECTION_CANDIDATE_IDS
    )
    selected = SELECTED_CORE_INSTRUMENT_ID
    selected_candidate = None if selected is None else coverage_by_id.get(selected)
    evidence_ref = SELECTED_CORE_EVIDENCE_REF
    selection_declared = selected is not None or evidence_ref is not None
    selection_complete = (
        selected in CORE_SELECTION_CANDIDATE_IDS
        and selected_candidate is not None
        and evidence_ref is not None
        and bool(evidence_ref.strip())
    )
    configuration_error = None
    if selection_declared and not selection_complete:
        configuration_error = "the reviewed core selection must name a declared candidate and a non-blank evidence ref"
    return CoreSelection(
        state=(
            "unavailable"
            if missing_candidate_ids or configuration_error is not None
            else "ready"
            if selection_complete
            else "evidence_collecting"
        ),
        selected_instrument_id=selected if selection_complete else None,
        selected_symbol=selected_candidate.symbol if selection_complete and selected_candidate is not None else None,
        evidence_ref=evidence_ref if selection_complete else None,
        required_trading_days=CORE_SELECTION_REQUIRED_TRADING_DAYS,
        observed_trading_days=observed_days,
        max_cost_bps=CORE_SELECTION_MAX_COST_BPS,
        candidates=candidates,
        missing_candidate_ids=missing_candidate_ids,
        configuration_error=configuration_error,
    )


def require_selected_core_instrument(conn: psycopg.Connection[Any], *, instrument_id: int) -> CoreSelection:
    """Require the reviewed sleeve selection below every mandate writer."""
    selection = load_core_selection(conn)
    if not selection.ready or selection.selected_instrument_id is None:
        raise CoreSelectionError(
            "the core sleeve cannot be enabled until #2833 completes its five-trading-day cost verdict"
        )
    if instrument_id != selection.selected_instrument_id:
        raise CoreSelectionError(
            f"instrument {instrument_id} is not the evidence-approved core sleeve ({selection.selected_instrument_id})"
        )
    return selection


__all__ = [
    "CORE_SELECTION_CANDIDATE_IDS",
    "CORE_SELECTION_MAX_COST_BPS",
    "CORE_SELECTION_REQUIRED_TRADING_DAYS",
    "CoreCandidateCoverage",
    "CoreSelection",
    "CoreSelectionError",
    "load_core_selection",
    "require_selected_core_instrument",
]
