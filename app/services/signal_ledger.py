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

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import psycopg
from psycopg.types.json import Jsonb

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
    #: ⚠ Also from ``StrategyIdentity``, and for the same reason as
    #: ``universe`` (#2333). It is hashed INTO ``strategy_version``, so taking
    #: it as a separate argument would create a second source of truth that can
    #: disagree with the version it is stored beside.
    input_rule_set_versions: Mapping[str, str]
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
        # strategy_signals_input_rule_sets_shape
        #
        # ⚠ An EXACT mirror of sql/257's CHECK — non-empty object, every value
        # a non-empty string — and nothing more. A stricter rule here (say,
        # rejecting a blank KEY, which the CHECK cannot express without a
        # subquery) would break the property that makes mirroring worth
        # anything: that a row this class accepts is a row the constraint
        # accepts, and vice versa.
        #
        # The blank-VALUE half is the one that is easy to skip. `NOT NULL`
        # passes `{"indicator_series": ""}` — present, correctly typed, and
        # recording nothing, which is the #2286 shape.
        if not isinstance(self.input_rule_set_versions, Mapping) or not self.input_rule_set_versions:
            raise ValueError(
                f"input_rule_set_versions must be a non-empty mapping, got {self.input_rule_set_versions!r} "
                "— a signal whose indicator rule set is unrecorded cannot be told apart from one produced "
                "under different indicator code (#2333)"
            )
        for module, version in self.input_rule_set_versions.items():
            if not isinstance(version, str) or not version.strip():
                raise ValueError(
                    f"input_rule_set_versions[{module!r}] is {version!r}: every rule set must carry a "
                    "non-empty version string"
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
    2. ⚠ **A** ``t+1`` **that EXISTS but cannot be priced is**
       ``unusable_fill_price`` — its open is NULL, or is present and
       non-positive. Reproduce the population with::

           select count(*) - count(open), count(*) filter (where open <= 0) from price_daily;
           select count(*) - count(open), count(*) filter (where open <= 0) from research_price_daily;

       ⚠ The row COUNTS are deliberately not written down here: the corpus is
       live (``daily_candle_refresh`` moved ``price_daily`` by 7 rows during
       one ticket alone), so a figure in a docstring goes stale in the place a
       reader trusts most. ``sql/270`` records the run that motivated the split,
       beside the queries that reproduce it.

       ⚠⚠ **This used to be** ``no_fill_bar`` **and the split is #2354.** The
       old branch tested ``fill_open is None`` alone, so a stored ``open = 0``
       — which is not NULL and is not a price — became ``fill_price = 0`` on a
       ``fired`` row. Every reader refuses that (``outcome_resolver``:
       *"entry_price must be > 0 … gross_return_pct divides by it"*;
       ``position_builder.EntryFill``/``ExitFill`` the same), so the writer and
       the readers disagreed, and only a ledger with 0 rows kept it academic.

       The reason code is a SPLIT, not a widening. This docstring previously
       accepted the widening on the stated condition *"if the measured count
       ever leaves zero, split it"*, and it has: both corpora carry
       non-positive opens today. ``no_fill_bar`` now means only what it says —
       the series ended — and criterion 8's distinction between a real absence
       and a data gap is countable again through
       ``idx_strategy_signals_reason``.

       ⚠ It stamps ``unusable_fill_price`` rather than ``quarantined_bar``,
       even though every such bar in both corpora is `B1`-quarantined on both
       axes today (measured; see ``sql/270``). This function receives a
       ``BarSeries`` and no verdicts — the quarantine is the CALLER's gate,
       which is why every strategy module takes its ``close_reason`` as an
       argument — so a ``quarantined_bar`` stamp here would assert a cause the
       writer has no input for, and would keep asserting it against a raw
       loader that never ran the quarantine.

       Neither branch is unreachable structurally: both ``open`` columns are
       nullable, ``price_structure`` builds ``OHLCVRow`` by passing
       ``bar.open`` through with no None check, and
       ``research_price_structure_store.load_masked_series`` masks the
       non-positive case only for callers that go through it — a raw
       ``price_daily`` read does not.

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
        # ⚠ TWO-SIDED, and the lower half is load-bearing (#2317). A negative
        # index passes `>= n_bars` and then WRAPS under Python list indexing —
        # at `series.dates[...]` below and again at `signal_index + 1` — so
        # rather than raising, the writer would resolve the signal against a bar
        # near the END of the series and store the result as an ordinary fill. A
        # ledger row that is wrong-but-plausible is worse than one that fails,
        # because nothing downstream can tell.
        #
        # ⚠ `StrategySignal.__post_init__` already refuses a negative index, so
        # this cannot fire on any path that goes through the contract, and it is
        # not claimed to fix a live wrong-row bug. It is here for the reason
        # refusal 1 above is repeated: the writer must not depend on which
        # producer fed it, and `Sequence[StrategySignal]` is an annotation, not a
        # runtime gate. `outcome_resolver.resolve_outcome` bounds its own
        # `fill_index` two-sided already — this brings the two layers in line
        # instead of leaving one of them trusting its caller.
        if not 0 <= signal.signal_index < n_bars:
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
        # ⚠ TWO refusals, not one, and the order is the order of the questions:
        # does the bar exist, and can it be priced. Collapsing them was #2354 —
        # `fill_open is None` alone let a stored `open = 0` through as a fill.
        if fill_index >= n_bars:
            verdict, reason = "not_evaluable", "no_fill_bar"
        else:
            fill_open = series.rows[fill_index].get("open")
            # ⚠ `<= 0`, not `== 0`. Measured on both corpora today there are no
            # NEGATIVE opens, only zeros — but "no negatives were stored" is a
            # fact about an ingest run, and a bound that holds by measurement
            # rather than by construction is one re-harvest from being wrong.
            if fill_open is None or fill_open <= 0:
                verdict, reason = "not_evaluable", "unusable_fill_price"
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
                input_rule_set_versions=identity.input_rule_set_versions,
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
        fill_price, universe, input_rule_set_versions
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(instrument_id)s, %(signal_bar_date)s,
        %(signal_kind)s, %(verdict)s, %(not_evaluable_reason)s, %(fill_bar_date)s,
        %(fill_price)s, %(universe)s, %(input_rule_set_versions)s
    )
"""


def store_signals(conn: psycopg.Connection[tuple], rows: Sequence[LedgerRow]) -> int:
    """Insert durable FIRED ``rows``, returning the number written.

    Routine negative decisions go through
    ``strategy_observation_storage.store_strategy_observations`` so they reach
    the 90-day partition and durable daily census rather than this heavily
    indexed, outcome-referenced ledger. Keeping this low-level function public
    for outcome fixtures does not permit bypassing that #2448 boundary.

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
    non_fired = [row.verdict for row in rows if row.verdict != "fired"]
    if non_fired:
        raise ValueError(
            f"strategy_signals is fired-only; got {len(non_fired)} routine verdict row(s) — "
            "use store_strategy_observations so daily counts and retention stay complete"
        )
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
                    # ⚠ `Jsonb`, not `Json`: the column is JSONB, and psycopg's
                    # `Json` adapts to the `json` type, which Postgres then has
                    # to cast — and a `MappingProxyType` is not JSON-adaptable
                    # without the explicit wrapper either way.
                    "input_rule_set_versions": Jsonb(dict(row.input_rule_set_versions)),
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
