# app/providers/sec_rate_gate_holder.py
"""Authoritative process-global SEC rate gate (#1484, §3b).

Accessed via get_sec_rate_gate() at construction/acquire time — NEVER
value-imported — so set_sec_rate_gate() at the composition root propagates
to every consumer module. Default is the in-process floor (correct, just
single-process) so tests / CLI / pool-less callers work without wiring.
"""

from __future__ import annotations

from app.providers.rate_gate import SEC_MIN_REQUEST_INTERVAL_S, InProcessFloorGate, RateGate

_sec_rate_gate: RateGate = InProcessFloorGate(floor=SEC_MIN_REQUEST_INTERVAL_S)


def get_sec_rate_gate() -> RateGate:
    return _sec_rate_gate


def set_sec_rate_gate(gate: RateGate) -> None:
    global _sec_rate_gate
    _sec_rate_gate = gate


def _reset_sec_rate_gate_for_tests() -> None:
    global _sec_rate_gate
    _sec_rate_gate = InProcessFloorGate(floor=SEC_MIN_REQUEST_INTERVAL_S)
