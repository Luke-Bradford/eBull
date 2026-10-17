"""#3611 transaction-cost readout of strategy entries (shortfall vs the decision price, split at the arrival quote) and
exits (engine closes against the arrival quote and the close-arm what-if; broker stop and target closes against their
level, with the session-open gap flagged).

The arithmetic lives in ``app/services/execution_tca.py``; this prints it. Read-only.

Usage::

    PYTHONPATH=. uv run python -m scripts.tca_readout --since 2026-10-15

Columns are basis points (positive = cost: paid above on a buy, received below on a sell) and seconds.
``decision_age_s`` is how old the decision price was when the order was created; ``fill_after_s`` is fill time minus
order creation. ``gapped`` says the session opened beyond the stop or target, so the level could not be the fill.
"""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Sequence
from datetime import date
from decimal import Decimal
from statistics import median

import psycopg

from app.config import settings
from app.services.execution_tca import ARRIVAL_MAX_AGE_SECONDS, EntryTca, ExitTca, load_entry_tca, load_exit_tca

_COLUMNS = (
    "order",
    "symbol",
    "ref",
    "decision_age_s",
    "fill_after_s",
    "shortfall_bps",
    "delay_bps",
    "execution_bps",
    "eff_spread_bps",
    "quoted_bps",
    "what_if_bps",
    "arrival_age_s",
    "unmeasured",
)


def _bps(x: Decimal | None) -> str:
    return "" if x is None else f"{x * 10000:.1f}"


def _row(e: EntryTca) -> tuple[str, ...]:
    c = e.costs
    decision_age = None if e.decision_at is None else (e.submitted_at - e.decision_at).total_seconds()
    fill_after = None if e.filled_at is None else (e.filled_at - e.submitted_at).total_seconds()
    return (
        str(e.order_id),
        e.symbol or str(e.instrument_id),
        e.price_reference or "",
        "" if decision_age is None else f"{decision_age:.0f}",
        "" if fill_after is None else f"{fill_after:.0f}",
        _bps(None if c is None else c.shortfall),
        _bps(None if c is None else c.delay),
        _bps(None if c is None else c.execution),
        _bps(None if c is None else c.effective_spread),
        _bps(None if c is None else c.quoted_spread),
        _bps(e.what_if_cost),
        "" if c is None or c.arrival_age_seconds is None else f"{c.arrival_age_seconds:.0f}",
        e.unmeasured or (c.arrival_unmeasured if c is not None else None) or "",
    )


def _median_bps(values: Sequence[Decimal | None]) -> str:
    present = [v for v in values if v is not None]
    return f"{median(present) * 10000:.1f} (n={len(present)})" if present else "n=0"


def render(entries: Sequence[EntryTca]) -> str:
    rows = [_COLUMNS, *(_row(e) for e in entries)]
    widths = [max(len(r[i]) for r in rows) for i in range(len(_COLUMNS))]
    lines = ["  ".join(cell.rjust(w) for cell, w in zip(r, widths, strict=True)) for r in rows]
    costs = [e.costs for e in entries if e.costs is not None]
    reasons = Counter(e.unmeasured or (e.costs.arrival_unmeasured if e.costs else None) for e in entries)
    lines += [
        "",
        f"entries: {len(entries)}  unmeasured: {dict((k, v) for k, v in reasons.items() if k is not None)}",
        f"arrival quotes older than {ARRIVAL_MAX_AGE_SECONDS}s at receipt are not used",
        f"median shortfall bps: {_median_bps([c.shortfall for c in costs])}",
        f"median delay bps: {_median_bps([c.delay for c in costs])}",
        f"median execution bps: {_median_bps([c.execution for c in costs])}",
        f"median effective spread bps: {_median_bps([c.effective_spread for c in costs])}",
        f"median what-if open cost bps: {_median_bps([e.what_if_cost for e in entries])}",
    ]
    return "\n".join(lines)


_EXIT_COLUMNS = (
    "position",
    "symbol",
    "kind",
    "closed_at",
    "execution_bps",
    "eff_spread_bps",
    "quoted_bps",
    "what_if_bps",
    "arrival_age_s",
    "level",
    "level_cost_bps",
    "gapped",
    "unmeasured",
)


def _exit_row(e: ExitTca) -> tuple[str, ...]:
    return (
        str(e.broker_position_id),
        e.symbol or str(e.instrument_id),
        e.kind or "",
        e.closed_at.strftime("%Y-%m-%d %H:%M"),
        _bps(e.execution),
        _bps(e.effective_spread),
        _bps(e.quoted_spread),
        _bps(e.what_if_cost),
        "" if e.arrival_age_seconds is None else f"{e.arrival_age_seconds:.0f}",
        "" if e.level is None else str(e.level),
        _bps(e.level_cost),
        "" if e.gapped is None else ("yes" if e.gapped else "no"),
        ",".join(r for r in (e.unmeasured, e.arrival_unmeasured, e.gap_unmeasured) if r),
    )


def render_exits(exits: Sequence[ExitTca]) -> str:
    rows = [_EXIT_COLUMNS, *(_exit_row(e) for e in exits)]
    widths = [max(len(r[i]) for r in rows) for i in range(len(_EXIT_COLUMNS))]
    lines = ["  ".join(cell.rjust(w) for cell, w in zip(r, widths, strict=True)) for r in rows]
    engine = [e for e in exits if e.kind == "engine"]
    levels = [e for e in exits if e.kind in ("stop_loss", "take_profit")]
    reasons = Counter(r for e in exits for r in (e.unmeasured, e.arrival_unmeasured, e.gap_unmeasured) if r)
    lines += [
        "",
        f"exits: {len(exits)}  by kind: {dict(Counter(e.kind for e in exits))}"
        f"  unmeasured: {dict((k, v) for k, v in reasons.items() if k is not None)}",
        f"median engine execution bps: {_median_bps([e.execution for e in engine])}",
        f"median engine effective spread bps: {_median_bps([e.effective_spread for e in engine])}",
        f"median close what-if cost bps: {_median_bps([e.what_if_cost for e in engine])}",
        f"median level cost bps (stop/target): {_median_bps([e.level_cost for e in levels])}"
        f"  gapped: {sum(1 for e in levels if e.gapped)}",
    ]
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="#3611 transaction-cost readout of strategy entries and exits")
    parser.add_argument("--since", type=date.fromisoformat, required=True, help="first order date (UTC)")
    args = parser.parse_args(argv)
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        print(render(load_entry_tca(conn, since=args.since)))
        print()
        print(render_exits(load_exit_tca(conn, since=args.since)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
