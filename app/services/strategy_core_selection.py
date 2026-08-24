"""Evidence-approved instrument for the deterministic core/cash sleeve (#2833)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Final, Literal

import psycopg

CORE_SELECTION_CANDIDATE_IDS: Final = (3417, 3434, 3075)
CORE_SELECTION_REQUIRED_TRADING_DAYS: Final = 5
CORE_SELECTION_MAX_COST_BPS: Final = 60
# #2833's corrected prospective declaration excludes the already-seen
# 2026-08-24 observations.  The operator surface must count the same population
# the sealed verifier will open, or it reports progress that is not evidence.
CORE_SELECTION_EVIDENCE_NOT_BEFORE: Final = datetime(2026, 8, 25, tzinfo=UTC)

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
WITH candidate_dates AS (
    SELECT o.instrument_id,
           (o.sample_bucket AT TIME ZONE 'UTC')::date AS observation_date
    FROM strategy_core_quote_observations o
    WHERE o.instrument_id = ANY(%s)
      AND o.sample_bucket >= %s
      AND o.observation_status = 'observed'
    GROUP BY o.instrument_id, (o.sample_bucket AT TIME ZONE 'UTC')::date
), common_dates AS (
    SELECT observation_date
    FROM candidate_dates
    GROUP BY observation_date
    HAVING count(*) = cardinality(%s::bigint[])
)
SELECT i.instrument_id, i.symbol,
       count(d.observation_date) AS observed_days,
       min(d.observation_date) AS first_observed_date,
       max(d.observation_date) AS last_observed_date,
       (SELECT count(*) FROM common_dates) AS common_observed_days
FROM instruments i
LEFT JOIN candidate_dates d ON d.instrument_id = i.instrument_id
WHERE i.instrument_id = ANY(%s)
GROUP BY i.instrument_id, i.symbol
ORDER BY array_position(%s::bigint[], i.instrument_id)
"""


def load_core_selection(conn: psycopg.Connection[Any]) -> CoreSelection:
    """Return reviewed selection and descriptive coverage, never infer a verdict."""
    rows = conn.execute(
        _COVERAGE_SQL,
        (
            list(CORE_SELECTION_CANDIDATE_IDS),
            CORE_SELECTION_EVIDENCE_NOT_BEFORE,
            list(CORE_SELECTION_CANDIDATE_IDS),
            list(CORE_SELECTION_CANDIDATE_IDS),
            list(CORE_SELECTION_CANDIDATE_IDS),
        ),
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
    # The verdict window is the first five dates observed by EVERY candidate,
    # not the minimum of three independent date counts.  The latter can reach
    # 5/5 while the common-date intersection is still only four days.
    observed_days = int(rows[0][5]) if rows and not missing_candidate_ids else 0
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
    "CORE_SELECTION_EVIDENCE_NOT_BEFORE",
    "CORE_SELECTION_MAX_COST_BPS",
    "CORE_SELECTION_REQUIRED_TRADING_DAYS",
    "CoreCandidateCoverage",
    "CoreSelection",
    "CoreSelectionError",
    "load_core_selection",
    "require_selected_core_instrument",
]
