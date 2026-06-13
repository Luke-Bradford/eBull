"""Pure reconstruction logic for the value-history chart (#1594 PR-B).

The ``GET /portfolio/value-history`` endpoint rebuilds daily portfolio
equity from the ``trade_events`` ledger (exact open/close timeline)
instead of the pre-#1593 hybrid (current-units back-dated + fills replay,
which dropped closed positions). These pure helpers hold the decision
logic so it is table-tested without a DB and is the SINGLE source of the
formula — the endpoint calls them directly, the SQL only fetches rows.

Equity basis (HARD CONSTRAINT, see spec §1.A): the recompute uses the
**same** mark-to-market formula the EOD snapshot persists —
``amount + units*(close - open_rate)`` — with ``open_rate`` from
``trade_events.price`` and the per-unit native cost basis reconstructed from
``trade_events.investment_usd`` converted to the instrument's native currency
at the OPEN-day FX, divided by opened units. That is both leverage-correct
(``investment_usd`` carries the margin, so a 2x position prices to equity, not
notional) and currency-correct (the amount term is native, never account-ccy
mixed with a native price delta). On USD-native rows it equals
``broker_positions.amount`` exactly, so persisted and recomputed days do not
step at the boundary. When ``investment_usd`` or its open-day FX is missing it
falls back to ``units * open_rate`` (unleveraged-long native cost) — covering
both Codex ckpt-2 P2 horns (currency-mix and leverage-step).

Spec: docs/proposals/etl/2026-06-13-portfolio-value-v2-pr-b-units.md
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal

from app.services.fx import FxRateNotFound, convert


def reconstruct_units_at_day(
    open_units: Decimal,
    closes: list[tuple[date, Decimal]],
    day: date,
) -> Decimal:
    """Units still held on ``day`` = original opened units minus close slices.

    Units contract (sql/194): ``open.units`` is the ORIGINAL opened units;
    each ``close.units`` is that slice's delta. A position may have one open
    and N partial closes (eToro reduces the same ``position_id``). Closes
    executed AFTER ``day`` do not count. Returns the remaining units (≤ 0
    means fully closed → caller drops it from the series for ``day``).
    """
    closed = sum((units for close_day, units in closes if close_day <= day), Decimal("0"))
    return open_units - closed


def native_cost_basis(
    investment_usd: Decimal | None,
    open_units: Decimal,
    native_ccy: str | None,
    open_rate: Decimal,
    open_fx_rates: dict[tuple[str, str], Decimal],
) -> Decimal:
    """Per-unit invested capital in the instrument's NATIVE ccy.

    ``investment_usd`` is account currency (USD); convert it to native at the
    open-day FX and divide by opened units → a per-unit cost that is both
    leverage-correct (margin is in the investment) and currency-correct (no
    USD-vs-native mixing in the MTM amount term). Falls back to ``open_rate``
    (the unleveraged-long native cost) when investment is absent, the native
    currency is unknown, or its open-day FX pair is unavailable. ``open_units``
    is > 0 by the sql/194 CHECK.
    """
    if investment_usd is None or native_ccy is None:
        return open_rate
    try:
        amount_native = (
            investment_usd if native_ccy == "USD" else convert(investment_usd, "USD", native_ccy, open_fx_rates)
        )
    except FxRateNotFound:
        return open_rate
    return amount_native / open_units


def position_equity(
    amount_at_day: Decimal,
    units_at_day: Decimal,
    open_rate: Decimal,
    close: Decimal,
) -> Decimal:
    """Mark-to-market equity in native ccy — the canonical PR-A formula.

    ``amount + units*(close - open_rate)`` (long; the v1 universe is
    long-only / unleveraged). Equals ``close * units`` ONLY when
    ``amount == units * open_rate`` (the unleveraged identity); it does not
    collapse to that in general, which is what keeps it consistent with the
    EOD snapshot for any future leveraged/short row.
    """
    return amount_at_day + units_at_day * (close - open_rate)


def carry_forward_rate_map(
    fx_rows: list[tuple[date, str, str, Decimal]],
    days: list[date],
) -> dict[date, dict[tuple[str, str], Decimal]]:
    """Per-day FX rates by carry-forward (most-recent ``rate_date <= day``).

    ``fx_rows`` is ``(rate_date, base, quote, rate)`` covering the whole
    history up to today (the seed row before ``days[0]`` is required so a
    range that opens on a weekend/holiday still resolves — spec §1.C / Codex
    M1). ``days`` must be ascending. A day earlier than a pair's first row
    has no entry for that pair → the caller FX-skips it.
    """
    by_pair: dict[tuple[str, str], list[tuple[date, Decimal]]] = defaultdict(list)
    for rate_date, base, quote, rate in fx_rows:
        by_pair[(base, quote)].append((rate_date, rate))

    result: dict[date, dict[tuple[str, str], Decimal]] = {day: {} for day in days}
    for pair, series in by_pair.items():
        series.sort()
        idx = 0
        current: Decimal | None = None
        for day in days:  # ascending
            while idx < len(series) and series[idx][0] <= day:
                current = series[idx][1]
                idx += 1
            if current is not None:
                result[day][pair] = current
    return result


def overlay_persisted(
    recomputed: dict[date, Decimal],
    snapshots: list[tuple[date, Decimal, str]],
    display_ccy: str,
) -> dict[date, Decimal]:
    """Override recomputed days with the persisted snapshot total where one exists.

    The recompute is the always-present floor; persisted ``portfolio_eod_snapshots``
    rows are authoritative for the days they cover (auditable — what the
    dashboard recorded). A snapshot in a DIFFERENT display currency is left to
    the recompute rather than mislabelled (spec §1.B). Snapshots outside the
    recomputed range are ignored.
    """
    out = dict(recomputed)
    for snap_date, total, ccy in snapshots:
        if ccy == display_ccy and snap_date in out:
            out[snap_date] = total
    return out
