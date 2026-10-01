"""#3540 — print the engine-book NAV/P&L bridge for the last N snapshot intervals. Read-only.

    PYTHONPATH=. uv run python -m scripts.engine_nav_bridge --sessions 5

Spec: ``docs/proposals/execution/2026-10-01-3540-engine-nav-bridge.md``. One ``REPEATABLE READ``
transaction, rolled back. Exit status 0 when every interval is closed and validated, 1 otherwise
(the output names every residual and state either way).
"""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal

import psycopg

from app.config import settings
from app.services.engine_nav_bridge import BridgeCensus, BridgeInterval, load_bridge


def _fmt(value: Decimal | None) -> str:
    return "None" if value is None else f"{value:,.4f}"


def render(intervals: list[BridgeInterval], census: BridgeCensus) -> list[str]:
    lines = [
        f"owned positions: {census.owned_positions}  non-core owned: {census.non_core_owned_positions}  "
        f"trades without ownership: {census.trades_without_ownership}"
    ]
    for interval in intervals:
        lines.append("")
        lines.append(
            f"{interval.opened_at:%Y-%m-%d %H:%MZ} -> {interval.closed_at:%Y-%m-%d %H:%MZ} ({interval.days}d)  "
            f"closed={interval.closed} validated={interval.validated}"
        )
        for label, value in (
            ("opening_nav", interval.opening_nav),
            ("flows", interval.flows),
            ("realised", interval.realised),
            ("unrealised_released", interval.unrealised_released),
            ("unrealised_opened", interval.unrealised_opened),
            ("unrealised_continuing", interval.unrealised_continuing),
            ("  of which fx_effect", interval.fx_effect),
            ("  of which price_effect", interval.price_effect),
            ("closing_nav", interval.closing_nav),
        ):
            lines.append(f"  {label:<24}{_fmt(value):>16}")
        lines.append(f"  fees memo: {interval.fees_memo}  ({len(interval.fees_detail)} observations)")
        if interval.fees_memo != "fees_zero":
            lines.extend(f"    position {pid}: {_fmt(value)}" for pid, value in interval.fees_detail if value != 0)
        for residual in interval.residuals:
            flag = "EXCEEDS" if residual.exceeds else "ok"
            lines.append(
                f"  {residual.code:<18} pos {residual.position_id}  broker {_fmt(residual.broker)}  "
                f"recomputed {_fmt(residual.recomputed)}  diff {_fmt(residual.amount)}  "
                f"bound {_fmt(residual.bound)}  {flag}"
            )
        for state in interval.states:
            lines.append(f"  state {state.code}" + (f" pos {state.position_id}" if state.position_id else ""))
    return lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sessions", type=int, default=5)
    args = parser.parse_args(argv)
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.read_only = True
        intervals, census = load_bridge(conn, sessions=args.sessions)
        conn.rollback()
    print("\n".join(render(intervals, census)))
    return 0 if intervals and all(interval.validated for interval in intervals) else 1


if __name__ == "__main__":
    sys.exit(main())
