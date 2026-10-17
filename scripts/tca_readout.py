"""#3611 transaction-cost readout of strategy entries: shortfall vs the decision price, split at the arrival quote.

The arithmetic lives in ``app/services/execution_tca.py``; this prints it. Read-only.

Usage::

    PYTHONPATH=. uv run python -m scripts.tca_readout --since 2026-10-15

Columns are basis points (positive = cost on a buy) and seconds. ``decision_age_s`` is how old the decision price was
when the order was created; ``fill_after_s`` is fill time minus order creation.
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
from app.services.execution_tca import ARRIVAL_MAX_AGE_SECONDS, EntryTca, load_entry_tca

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


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="#3611 transaction-cost readout of strategy entries")
    parser.add_argument("--since", type=date.fromisoformat, required=True, help="first order date (UTC)")
    args = parser.parse_args(argv)
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        print(render(load_entry_tca(conn, since=args.since)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
