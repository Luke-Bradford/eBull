"""Ranking-pot-v1's exit due rule (#2842 slice 5c-i; spec §7.4, "Close").

A leaf module (stdlib only) because the shared position manager imports it, and the manager is itself imported by
``ai_trial_policy``, which the pot's policy module imports: anything heavier here closes that cycle.

Policy-hashed (``ranking_pot_policy.POLICY_MODULES``).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from typing import Final

#: §7.4: the manager closes from the first cycle at or after 15:00 UTC on ``exit_session`` (in session in both EDT
#: and EST, including a 13:00 ET half day; the entries' time, ``ranking_pot_executor.POT_ENTRY_TIME_UTC``).
POT_EXIT_TIME_UTC: Final = time(15, 0)


def pot_exit_due(exit_session: date, observed_at: datetime) -> bool:
    """True from 15:00 UTC on ``exit_session`` onwards (stays due, so every later cycle retries)."""
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    return observed_at >= datetime.combine(exit_session, POT_EXIT_TIME_UTC, tzinfo=UTC)


__all__ = ["POT_EXIT_TIME_UTC", "pot_exit_due"]
