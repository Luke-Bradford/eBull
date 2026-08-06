"""Phase 3c — the signal-ledger writer.

Spec: ``docs/proposals/ta/2026-08-05-strategy-registry-and-signal-ledger.md``
§4. Registry contract: ``app/services/strategy_registry.py`` (3a). Table:
``sql/255_strategy_signals.sql`` (3b). Refs #2240, #2288.

⚠⚠ THIS MODULE IS THE ONLY CODE THAT MAY TOUCH BAR ``t+1``.

Parent §3.5: *"Signal on the close of bar t → fill at the OPEN of bar t+1. No
exceptions… structurally impossible rather than merely discouraged."* A
``StrategySignal`` carries a bar INDEX and no fill field, so a strategy cannot
express a fill at all. The fill is resolved HERE, from the series, by index
arithmetic the strategy never sees.

That split is the guarantee. The table's ``fill_bar_date > signal_bar_date``
CHECK is a backstop against a bug in *this file* and proves only that two
stored dates are ordered — not that the right bar was read, and not that the
fill was the NEXT bar. Only the arithmetic below guarantees those.

⚠ ``fill_bar_date`` is the next bar **in that instrument's series**, never
``signal_bar_date + 1 day``. Calendar gaps are normal (S4 measured 1,204
tradable instruments whose latest bar is over a month old), and date arithmetic
would invent a fill on a day the instrument did not trade.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import psycopg

from app.services.indicator_series import BarSeries, Universe
from app.services.strategy_registry import (
    NOT_EVALUABLE_REASONS,
    SIGNAL_KINDS,
    VERDICTS,
    NotEvaluableReason,
    SignalKind,
    StrategyIdentity,
    StrategySignal,
    Verdict,
)


@dataclass(frozen=True)
class LedgerRow:
    """One resolved signal, ready to store. The fill is already decided.

    ⚠ ``universe`` has NO DEFAULT and sits before every optional field, so
    constructing a row without it is a ``TypeError`` (#2288: a metric computed
    on a survivor-only universe must be marked as such, and a field with a
    default is a field a writer can forget). ``resolve_fills`` takes it from
    ``StrategyIdentity`` rather than as a separate argument — the identity hash
    already covers the universe (criterion 11), so a separate parameter is a
    second source of truth that can disagree with the version it is stored
    beside.

    The validation below MIRRORS ``sql/255``'s CHECK constraints. That is
    deliberate duplication, not redundancy: a bad row fails at construction
    with a message naming the field, while the constraints stay as the backstop
    for any writer that bypasses this class.
    """

    strategy_id: str
    strategy_version: str
    instrument_id: int
    signal_bar_date: date
    signal_kind: SignalKind
    verdict: Verdict
    universe: Universe
    not_evaluable_reason: NotEvaluableReason | None = None
    fill_bar_date: date | None = None
    fill_price: Decimal | None = None

    def __post_init__(self) -> None:
        if self.verdict not in VERDICTS:
            raise ValueError(f"unknown verdict {self.verdict!r}; must be one of {sorted(VERDICTS)}")
        if self.signal_kind not in SIGNAL_KINDS:
            raise ValueError(f"unknown signal kind {self.signal_kind!r}; must be one of {sorted(SIGNAL_KINDS)}")
        if self.not_evaluable_reason is not None and self.not_evaluable_reason not in NOT_EVALUABLE_REASONS:
            raise ValueError(
                f"unknown reason code {self.not_evaluable_reason!r}; must be one of {sorted(NOT_EVALUABLE_REASONS)}"
            )
        # strategy_signals_reason_matches_verdict
        if (self.verdict == "not_evaluable") != (self.not_evaluable_reason is not None):
            raise ValueError(
                f"verdict {self.verdict!r} and reason {self.not_evaluable_reason!r} disagree: "
                "a reason is required exactly when the verdict is not_evaluable"
            )
        # strategy_signals_fill_matches_verdict
        #
        # ⚠ Counted, not ANDed. `fill_bar_date is not None and fill_price is not
        # None` reads as "has a fill" and silently admits a HALF one: a
        # not_fired row carrying a fill_bar_date with no price scores False on
        # that expression, matches `verdict != "fired"`, and passes — while the
        # SQL CHECK requires BOTH columns NULL and rejects it. Caught by Codex
        # at checkpoint 2. The whole value of mirroring a constraint is that
        # the two agree, so the mirror has to be exact.
        fill_fields_set = (self.fill_bar_date is not None) + (self.fill_price is not None)
        if fill_fields_set != (2 if self.verdict == "fired" else 0):
            raise ValueError(
                f"verdict {self.verdict!r} carries fill {(self.fill_bar_date, self.fill_price)!r}: "
                "a fill exists exactly when the signal fired, and both fields move together"
            )
        # strategy_signals_fill_after_signal
        if self.fill_bar_date is not None and self.fill_bar_date <= self.signal_bar_date:
            raise ValueError(
                f"fill_bar_date {self.fill_bar_date} is not after signal_bar_date {self.signal_bar_date} "
                "— a same-bar or backwards fill is the look-ahead this ledger exists to prevent"
            )


def resolve_fills(
    signals: Sequence[StrategySignal],
    *,
    series: BarSeries,
    identity: StrategyIdentity,
    instrument_id: int,
) -> list[LedgerRow]:
    """Turn per-bar verdicts into storable rows, resolving each fill from ``series``.

    ``fill_index = signal_index + 1``, always, and the fill price is that bar's
    OPEN. There is no other path: a signal names an index, and this function
    owns everything downstream of it.

    Refusals, in the order they are applied:

    1. ⚠ **The LAST bar of the series is always** ``not_evaluable("no_fill_bar")``,
       whatever verdict it arrived with. There is no ``t+1``, so no decision on
       that bar can be acted on. ``strategy_registry.evaluate`` already stamps
       it, which makes this a no-op on the normal path — it is repeated here
       because the writer must not depend on which producer fed it, and a
       hand-built signal would otherwise slip a final-bar decision into the
       ledger.
    2. ⚠ **A** ``t+1`` **with no OPEN price is also** ``no_fill_bar``. Measured
       2026-08-06 on the full population: **zero NULL opens in either**
       ``price_daily`` or ``research_price_daily`` — so this branch is
       currently unreachable in practice. Reproduce with::

           select count(*) - count(open) from price_daily;
           select count(*) - count(open) from research_price_daily;

       ⚠ The row COUNTS are deliberately not written down here: the corpus is
       live (``daily_candle_refresh`` moved ``price_daily`` by 7 rows during
       this ticket alone), so a figure in a docstring goes stale in the place a
       reader trusts most. The zero is the claim; the query reproduces it.

       It is not unreachable structurally: both ``open`` columns are nullable, and
       ``price_structure`` builds ``OHLCVRow`` by passing ``bar.open`` through
       with no None check. Failing closed is what the rest of this codebase
       does with a masked bar.

       ⚠ Reusing ``no_fill_bar`` for it is a deliberate widening of a code
       whose stated meaning is "the series ended", and it is flagged rather
       than smuggled: criterion 8 exists precisely to stop a data gap and a
       real absence being collapsed. It is accepted here because the
       alternative — a ninth reason code — needs the parent vocabulary
       reopened and ``sql/255``'s CHECK widened for a case that has never
       occurred. **If the measured count ever leaves zero, split it.**

    A duplicate ``(signal_bar_date, signal_kind)`` inside one batch raises
    rather than reaching the database: the uniqueness key would reject it
    anyway, but a ``UniqueViolation`` after a partial insert is a worse
    diagnostic than a named collision before any write.
    """
    n_bars = len(series)
    version = identity.version  # hashes a file; resolve once, not per signal
    rows: list[LedgerRow] = []
    seen: set[tuple[date, SignalKind]] = set()

    for signal in signals:
        if signal.signal_index >= n_bars:
            raise ValueError(
                f"signal_index {signal.signal_index} is outside the {n_bars}-bar series it was resolved against "
                "— the signals and the series must come from the same run"
            )
        signal_bar_date = series.dates[signal.signal_index]
        key = (signal_bar_date, signal.kind)
        if key in seen:
            raise ValueError(
                f"duplicate signal for {signal_bar_date} / {signal.kind} on instrument {instrument_id}: "
                "the ledger is keyed on (strategy, version, instrument, signal bar, kind)"
            )
        seen.add(key)

        verdict = signal.verdict
        reason = signal.reason
        fill_bar_date: date | None = None
        fill_price: Decimal | None = None

        fill_index = signal.signal_index + 1
        fill_open = series.rows[fill_index].get("open") if fill_index < n_bars else None
        if fill_open is None:
            verdict, reason = "not_evaluable", "no_fill_bar"
        elif verdict == "fired":
            fill_bar_date = series.dates[fill_index]
            fill_price = fill_open

        rows.append(
            LedgerRow(
                strategy_id=identity.strategy_id,
                strategy_version=version,
                instrument_id=instrument_id,
                signal_bar_date=signal_bar_date,
                signal_kind=signal.kind,
                verdict=verdict,
                universe=identity.universe,
                not_evaluable_reason=reason,
                fill_bar_date=fill_bar_date,
                fill_price=fill_price,
            )
        )
    return rows


_INSERT = """
    INSERT INTO strategy_signals (
        strategy_id, strategy_version, instrument_id, signal_bar_date,
        signal_kind, verdict, not_evaluable_reason, fill_bar_date,
        fill_price, universe
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(instrument_id)s, %(signal_bar_date)s,
        %(signal_kind)s, %(verdict)s, %(not_evaluable_reason)s, %(fill_bar_date)s,
        %(fill_price)s, %(universe)s
    )
"""


def store_signals(conn: psycopg.Connection[tuple], rows: Sequence[LedgerRow]) -> int:
    """Insert ``rows``, returning the number written.

    ⚠⚠ **NO** ``ON CONFLICT``**, deliberately.** A colliding key raises
    ``UniqueViolation`` and aborts the batch. Both alternatives are worse:

    - ``DO UPDATE`` would let a re-run overwrite a recorded decision, which is
      the exact failure ``strategy_version`` is in the key to prevent (spec
      §2.1: *"the ledger stops being a record of what was actually decided"*).
    - ``DO NOTHING`` would silently keep the old row when the new one
      DISAGREES. Given a fixed ``strategy_version``, a verdict is a pure
      function of the bars, so a disagreement means the corpus moved under us
      (a rebuild, a re-adjustment) — the one case worth hearing about, and the
      one ``DO NOTHING`` hides.

    A deliberate re-run bumps the version, which is a different key and inserts
    cleanly. That is the intended path.
    """
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            _INSERT,
            [
                {
                    "strategy_id": row.strategy_id,
                    "strategy_version": row.strategy_version,
                    "instrument_id": row.instrument_id,
                    "signal_bar_date": row.signal_bar_date,
                    "signal_kind": row.signal_kind,
                    "verdict": row.verdict,
                    "not_evaluable_reason": row.not_evaluable_reason,
                    "fill_bar_date": row.fill_bar_date,
                    "fill_price": row.fill_price,
                    "universe": row.universe,
                }
                for row in rows
            ],
        )
        # psycopg3 executemany rowcount is cumulative across the batch. ⚠ -1 is
        # psycopg's "server reported nothing" sentinel and must not be returned
        # as a count (prevention log: "psycopg v3 rowcount sentinel (-1)
        # treated as valid count").
        written = cur.rowcount
    if written < 0:
        raise RuntimeError(f"strategy_signals INSERT reported rowcount {written} for {len(rows)} rows")
    return written
