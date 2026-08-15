"""Pinned recent-regime windows for strategy evidence (#2447).

These windows are deliberately code, not request parameters.  A result window
is part of ``ResultIdentity.version``; accepting arbitrary dates would let an
operator search until a favourable interval appeared and then present that row
as if it were the declared test.  The registry also gives the monitoring API a
complete denominator: a missing row is visible as missing evidence.

The survivorship-free archive is frozen at 2024-09-27. A window beyond that
date cannot earn the label, so post-capture calendar years are prospective
shadow evidence, not historical backtest windows. The six windows below end at
or before that hard bound; keeping the old 2025/2026 windows in this denominator
would make allocation permanently impossible while describing unavailable data
as an unfinished job.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from types import MappingProxyType
from typing import Final, Literal

from app.services.position_builder import Window
from app.services.strategy_result import EVALUATION_WINDOW_END, HOLDOUT_BOUNDARY
from app.services.universe_selection import INTRADER_CAPTURE_DATE

RecentEvidenceWindowId = Literal[
    "primary-2022-plus",
    "rolling-36m",
    "rolling-24m",
    "year-2022",
    "year-2023",
    "year-2024",
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
        if self.window.end > INTRADER_CAPTURE_DATE:
            raise ValueError(
                f"recent evidence {self.window_id} ends {self.window.end}, after the survivorship-free "
                f"archive capture {INTRADER_CAPTURE_DATE}"
            )


_WINDOWS = (
    RecentEvidenceWindow(
        "primary-2022-plus",
        "Primary: 2022 through archive capture",
        Window(date(2022, 1, 1), INTRADER_CAPTURE_DATE),
    ),
    RecentEvidenceWindow(
        "rolling-36m",
        "36 months through archive capture",
        Window(date(2021, 9, 28), INTRADER_CAPTURE_DATE),
    ),
    RecentEvidenceWindow(
        "rolling-24m",
        "24 months through archive capture",
        Window(date(2022, 9, 28), INTRADER_CAPTURE_DATE),
    ),
    RecentEvidenceWindow("year-2022", "Calendar 2022", Window(date(2022, 1, 1), date(2022, 12, 31))),
    RecentEvidenceWindow("year-2023", "Calendar 2023", Window(date(2023, 1, 1), date(2023, 12, 31))),
    RecentEvidenceWindow(
        "year-2024",
        "2024 through archive capture",
        Window(date(2024, 1, 1), INTRADER_CAPTURE_DATE),
    ),
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
