"""Pinned recent-regime windows for strategy evidence (#2447).

These windows are deliberately code, not request parameters.  A result window
is part of ``ResultIdentity.version``; accepting arbitrary dates would let an
operator search until a favourable interval appeared and then present that row
as if it were the declared test.  The registry also gives the monitoring API a
complete denominator: a missing row is visible as missing evidence.

Only compact aggregate result rows are stored.  Daily bars remain in the
existing research corpus and indicators/positions are recomputed while a run is
active, so adding all eight windows costs 128 rows at the current four runnable
controls (8 windows x 4 strategies x 2 ambiguity x 2 quarantine), not another
time-series store.  Only S-4 independently resolves best/worst ambiguity; the
shared non-level measurement for S-1..S-3 is deliberately carried under both
arm identities to keep the immutable denominator complete.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from types import MappingProxyType
from typing import Final, Literal

from app.services.position_builder import Window
from app.services.strategy_result import EVALUATION_WINDOW_END, HOLDOUT_BOUNDARY

RecentEvidenceWindowId = Literal[
    "primary-2022-plus",
    "rolling-36m",
    "rolling-24m",
    "year-2022",
    "year-2023",
    "year-2024",
    "year-2025",
    "year-2026-ytd",
]


@dataclass(frozen=True)
class RecentEvidenceWindow:
    window_id: RecentEvidenceWindowId
    label: str
    window: Window
    required_for_allocation: bool = True

    def __post_init__(self) -> None:
        if self.window.start < HOLDOUT_BOUNDARY:
            raise ValueError(
                f"recent evidence {self.window_id} starts {self.window.start}, before hold-out {HOLDOUT_BOUNDARY}"
            )
        if self.window.end > EVALUATION_WINDOW_END:
            raise ValueError(
                f"recent evidence {self.window_id} ends {self.window.end}, after corpus {EVALUATION_WINDOW_END}"
            )


_WINDOWS = (
    RecentEvidenceWindow("primary-2022-plus", "Primary: 2022 onward", Window(date(2022, 1, 1), EVALUATION_WINDOW_END)),
    RecentEvidenceWindow("rolling-36m", "Rolling 36 months", Window(date(2023, 7, 9), EVALUATION_WINDOW_END)),
    RecentEvidenceWindow("rolling-24m", "Rolling 24 months", Window(date(2024, 7, 9), EVALUATION_WINDOW_END)),
    RecentEvidenceWindow("year-2022", "Calendar 2022", Window(date(2022, 1, 1), date(2022, 12, 31))),
    RecentEvidenceWindow("year-2023", "Calendar 2023", Window(date(2023, 1, 1), date(2023, 12, 31))),
    RecentEvidenceWindow("year-2024", "Calendar 2024", Window(date(2024, 1, 1), date(2024, 12, 31))),
    RecentEvidenceWindow("year-2025", "Calendar 2025", Window(date(2025, 1, 1), date(2025, 12, 31))),
    RecentEvidenceWindow("year-2026-ytd", "2026 year to corpus date", Window(date(2026, 1, 1), EVALUATION_WINDOW_END)),
)

RECENT_EVIDENCE_WINDOWS: Final = MappingProxyType({item.window_id: item for item in _WINDOWS})


def recent_evidence_window(window_id: str) -> RecentEvidenceWindow:
    """Resolve only a declared window; raw dates never cross this boundary."""
    try:
        return RECENT_EVIDENCE_WINDOWS[window_id]  # type: ignore[index]
    except KeyError as exc:
        raise ValueError(
            f"unknown recent evidence window {window_id!r}; must be one of {list(RECENT_EVIDENCE_WINDOWS)}"
        ) from exc


__all__ = [
    "RECENT_EVIDENCE_WINDOWS",
    "RecentEvidenceWindow",
    "RecentEvidenceWindowId",
    "recent_evidence_window",
]
