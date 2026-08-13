"""Process counter of SEC 429 / UA-throttle responses (#1484 §4.4).

SEC-scoped on purpose: ResilientClient is shared by non-SEC providers, so
only the SEC clients wire incr_sec_429 as their on_429 callback.
"""

from __future__ import annotations

import threading

_lock = threading.Lock()
_sec_429_total = 0


def incr_sec_429() -> None:
    global _sec_429_total
    with _lock:
        _sec_429_total += 1


def sec_throttle_429_total() -> int:
    with _lock:
        return _sec_429_total
