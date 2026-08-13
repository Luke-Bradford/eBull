"""Phase 4b — acceptance harness for the outcome ledger.

Run from repo root:

    uv run python -m scripts.verify_2240_outcome_ledger --roundtrip
    uv run python -m scripts.verify_2240_outcome_ledger --cleanup

⚠ **Do NOT pipe this into `head`/`tail`.** A pipe buffers, so the flushed
progress lines go nowhere and the output file sits empty while the run is
perfectly healthy — that cost 7 minutes on 2026-08-05. Redirect to a file and
read the file. Same rule as `.claude/CLAUDE.md`'s "never pipe a gate command".

WHAT THIS ARM PROVES, AND WHAT IT DOES NOT
-------------------------------------------
Spec acceptance 7. It proves the ROUND TRIP over the full eligible population:
3c's writer → `select_pending_fills` → 4a's resolver → `store_outcomes` → read
back and compare field for field on the PERSISTED fields (`exit_index` is
deliberately not stored, so it is not compared).

It does **not** re-prove the resolver — that is 4a's equivalence arm, which
re-derives first-touch indices in SQL. Nothing here is an independent
derivation of an outcome CLASS. What it independently derives is the
`gross_return_pct`, in SQL from the stored exit price and the ledger's own fill
price, which is the one stored number that is a pure function of two other
stored numbers.

⚠ THE POPULATION IS EVERY RESEARCH SERIES THAT CAN CARRY A LEDGER ROW —
`research_price_series.instrument_id IS NOT NULL`. The rest are excluded
STRUCTURALLY, not sampled: `strategy_signals.instrument_id` is a FK to
`instruments`, so a series with no mapping cannot have a signal at all. The
excluded count is reported.

⚠ BARS COME FROM `load_masked_series`, the fail-closed loader 4a's contract
requires, and its `price_quarantine` rule-set version is what is stored as
`input_rule_set_version`. Resolving over raw unmasked bars would make this
harness a caller that violates the resolver's own documented obligation.

⚠ THIS HARNESS WRITES TO THE DEV DATABASE and deletes its own rows at the end,
asserting the WHOLE-TABLE counts of both tables return to their pre-run values
— not merely the count for its own strategy id, which would miss residue under
a different version. `--cleanup` removes leftovers from an interrupted run.

Sister to `scripts/verify_2240_outcome_resolver.py` (4a) and
`scripts/verify_2240_signal_ledger_fills.py` (3c).
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter, defaultdict
from decimal import Decimal

import psycopg

from app.config import settings
from app.services.outcome_ledger import (
    OutcomeRow,
    locate_fill_index,
    select_pending_fills,
    store_outcomes,
)
from app.services.outcome_resolver import RULE_SET_VERSION, resolve_outcome
from app.services.research_price_structure_store import QUARANTINE_RULE_SET_VERSION
from app.services.signal_ledger import resolve_fills, store_signals
from app.services.strategy_registry import StrategyIdentity, StrategySignal

# ⚠ REUSED from the 4a harness rather than re-derived. `_levels` carries the
# stop rule (`entry_timing._compute_stop_loss`, which S5 swept against), the
# target multiple and the `levels_do_not_bracket` flat-run case; a second copy
# would drift and make the two arms' distributions incomparable for a reason
# that has nothing to do with either.
from scripts.verify_2240_outcome_resolver import _levels, _load, _load_quarantined

#: The harness's own strategy identity. `source_hash` is fixed rather than a
#: file hash: this identity must be stable across runs so `--cleanup` can find
#: an interrupted run's rows.
_IDENTITY = StrategyIdentity(
    strategy_id="S-VERIFY-4B",
    params={"tp_mult": "2.0", "max_hold": 20},
    universe="survivor_only",
    cost_model_id="static-v1",
    source_hash="outcome-ledger-roundtrip",
)

#: 4a's equivalence cell. ⚠ The census below is NOT directly comparable to
#: 4a's: the cell is the same, the SIGNAL SET is not — 4a takes every bar as a
#: hypothetical entry, this arm takes a strided sample plus the tail, and the
#: tail is deliberately over-represented so `window_truncated` is exercised.
#: The shares differ for that reason and not because either is wrong.
_TP_MULT = Decimal("2.0")
_MAX_HOLD = 20

#: Signal placement. ⚠ NOT every bar: this arm writes rows, and 25.8M ledger
#: rows would be a corpus rebuild rather than a verification. Every 250th bar is
#: roughly annual, and the tail indices are included deliberately — they are
#: where `no_fill_bar` (3c) and `window_truncated` (4a) live, which are exactly
#: the refusals a round trip must carry through unchanged.
_STRIDE = 250
_WARMUP = 20
_TAIL = 3


def _series_by_instrument(conn: psycopg.Connection[tuple]) -> tuple[dict[int, int], int]:
    """Instrument-mapped series, and how many were structurally excluded."""
    rows = conn.execute(
        "SELECT series_id, instrument_id, "
        "       count(*) OVER () AS mapped, "
        "       (SELECT count(*) FROM research_price_series) AS total "
        "FROM research_price_series WHERE instrument_id IS NOT NULL ORDER BY series_id"
    ).fetchall()
    if not rows:
        return {}, 0
    mapped = {int(r[1]): int(r[0]) for r in rows}
    excluded = int(rows[0][3]) - int(rows[0][2])
    return mapped, excluded


def _signal_indices(n_bars: int) -> list[int]:
    if n_bars <= _WARMUP:
        return []
    strided = list(range(_WARMUP, n_bars, _STRIDE))
    tail = list(range(max(_WARMUP, n_bars - _TAIL), n_bars))
    return sorted(set(strided) | set(tail))


def _table_counts(conn: psycopg.Connection[tuple]) -> tuple[int, int]:
    row = conn.execute(
        "SELECT (SELECT count(*) FROM strategy_signals), (SELECT count(*) FROM strategy_outcomes)"
    ).fetchone()
    assert row is not None
    return int(row[0]), int(row[1])


def cleanup(conn: psycopg.Connection[tuple]) -> int:
    """Delete this harness's rows. Outcomes go via CASCADE."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM strategy_signals WHERE strategy_id = %s", (_IDENTITY.strategy_id,))
        deleted = cur.rowcount
    conn.commit()
    return max(deleted, 0)


def _write_signals(conn: psycopg.Connection[tuple], series_by_instrument: dict[int, int]) -> tuple[int, int]:
    """Pass 1 — a deterministic signal set for every eligible series."""
    quarantined = _load_quarantined(conn)
    written = 0
    evaluated = 0
    started = time.perf_counter()
    for k, (instrument_id, series_id) in enumerate(sorted(series_by_instrument.items())):
        series = _load(conn, series_id, quarantined.get(series_id, set()))
        n = len(series.bars)
        if n == 0:
            # ⚠ The loader is FAIL-CLOSED: a series with no coverage row at the
            # current quarantine version returns ZERO bars rather than raw ones.
            # Counting these is the point — an unevaluated series is unchecked,
            # not clean.
            continue
        evaluated += 1
        indices = _signal_indices(n)
        if not indices:
            continue
        rows = resolve_fills(
            [StrategySignal(verdict="fired", signal_index=i) for i in indices],
            series=series.bars,
            identity=_IDENTITY,
            instrument_id=instrument_id,
        )
        written += store_signals(conn, rows)
        if (k + 1) % 500 == 0:
            conn.commit()
            print(
                f"  signals {k + 1:,}/{len(series_by_instrument):,} series | rows={written:,} "
                f"| {time.perf_counter() - started:.0f}s",
                flush=True,
            )
    conn.commit()
    return written, evaluated


def _resolve_pending(
    conn: psycopg.Connection[tuple], series_by_instrument: dict[int, int]
) -> tuple[dict[int, OutcomeRow], Counter[str]]:
    """Pass 2 — read the pending fills back and resolve each one."""
    pending = select_pending_fills(
        conn,
        strategy_id=_IDENTITY.strategy_id,
        strategy_version=_IDENTITY.version,
        rule_set_version=RULE_SET_VERSION,
        input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
    )
    by_instrument: dict[int, list] = defaultdict(list)
    for fill in pending:
        by_instrument[fill.instrument_id].append(fill)

    quarantined = _load_quarantined(conn)
    rows: dict[int, OutcomeRow] = {}
    skipped: Counter[str] = Counter()
    started = time.perf_counter()
    for k, (instrument_id, fills) in enumerate(sorted(by_instrument.items())):
        series_id = series_by_instrument[instrument_id]
        series = _load(conn, series_id, quarantined.get(series_id, set()))
        for fill in fills:
            # ⚠ The DATE is what was stored; the index is re-derived. A date the
            # corpus no longer holds raises rather than silently landing on
            # whatever bar now sits at that position.
            fill_index = locate_fill_index(series.bars, fill.fill_bar_date)
            levels, skip = _levels(fill.fill_price, series.atr[fill_index - 1], _TP_MULT, _MAX_HOLD)
            if levels is None:
                assert skip is not None
                skipped[skip] += 1
                continue
            outcome = resolve_outcome(
                series=series.bars,
                fill_index=fill_index,
                entry_price=fill.fill_price,
                levels=levels,
                masked_bar_reasons=series.unusable,  # type: ignore[arg-type]
                segment_end_index=None,
            )
            rows[fill.signal_id] = OutcomeRow.from_outcome(
                fill.signal_id, outcome, input_rule_set_version=QUARANTINE_RULE_SET_VERSION
            )
        if (k + 1) % 500 == 0:
            print(
                f"  outcomes {k + 1:,}/{len(by_instrument):,} instruments | rows={len(rows):,} "
                f"| {time.perf_counter() - started:.0f}s",
                flush=True,
            )
    return rows, skipped


_READBACK = """
    SELECT o.signal_id, o.rule_set_version, o.input_rule_set_version, o.outcome,
           o.resolution_method, o.reason, o.exit_bar_date, o.exit_price,
           o.bars_held, o.gross_return_pct
    FROM strategy_outcomes o
    JOIN strategy_signals s ON s.signal_id = o.signal_id
    WHERE s.strategy_id = %(strategy_id)s
"""

# The one stored number that is a pure function of two others. ⚠ Postgres
# NUMERIC division and Python `Decimal` division do not agree to the last digit
# — Decimal's default context is 28 significant digits — so the comparison is a
# reported MAXIMUM, not an equality assertion, and the threshold below is
# stated rather than assumed.
_RETURN_CHECK = """
    SELECT count(*),
           max(abs(o.gross_return_pct - (o.exit_price - s.fill_price) / s.fill_price)),
           min(o.exit_price),
           min(o.gross_return_pct)
    FROM strategy_outcomes o
    JOIN strategy_signals s ON s.signal_id = o.signal_id
    WHERE s.strategy_id = %(strategy_id)s AND o.exit_price IS NOT NULL
"""

_RETURN_TOLERANCE = Decimal("1e-12")


def _compare(conn: psycopg.Connection[tuple], expected: dict[int, OutcomeRow]) -> list[str]:
    stored: dict[int, tuple] = {}
    with conn.cursor() as cur:
        cur.execute(_READBACK, {"strategy_id": _IDENTITY.strategy_id})
        for row in cur.fetchall():
            stored[int(row[0])] = row

    mismatches: list[str] = []
    if set(stored) != set(expected):
        missing = sorted(set(expected) - set(stored))[:5]
        extra = sorted(set(stored) - set(expected))[:5]
        mismatches.append(f"signal_id sets differ: {len(expected)} expected, {len(stored)} stored; {missing=} {extra=}")

    for signal_id, want in expected.items():
        got = stored.get(signal_id)
        if got is None:
            continue
        fields: tuple[tuple[str, object, object], ...] = (
            ("rule_set_version", want.rule_set_version, got[1]),
            ("input_rule_set_version", want.input_rule_set_version, got[2]),
            ("outcome", want.outcome, got[3]),
            ("resolution_method", want.resolution_method, got[4]),
            ("reason", want.reason, got[5]),
            ("exit_bar_date", want.exit_bar_date, got[6]),
            ("exit_price", want.exit_price, got[7]),
            ("bars_held", want.bars_held, got[8]),
            ("gross_return_pct", want.gross_return_pct, got[9]),
        )
        for name, wanted, actual in fields:
            if wanted != actual:
                mismatches.append(f"signal {signal_id}: {name} stored {actual!r}, resolved {wanted!r}")
    return mismatches


def roundtrip(conn: psycopg.Connection[tuple]) -> int:
    """Acceptance 7 — full-population round trip through the real tables."""
    before = _table_counts(conn)
    existing = conn.execute(
        "SELECT count(*) FROM strategy_signals WHERE strategy_id = %s", (_IDENTITY.strategy_id,)
    ).fetchone()
    assert existing is not None
    if existing[0]:
        print(
            f"*** REFUSING TO START: {existing[0]:,} rows already exist for {_IDENTITY.strategy_id}. "
            "Run --cleanup first (an interrupted run leaves them).",
            flush=True,
        )
        return 1

    series_by_instrument, excluded = _series_by_instrument(conn)
    print(f"=== phase 4b round trip ({RULE_SET_VERSION} / {QUARANTINE_RULE_SET_VERSION}) ===", flush=True)
    print(f"  strategy_version : {_IDENTITY.version}", flush=True)
    print(
        f"  population       : {len(series_by_instrument):,} instrument-mapped research series "
        f"({excluded:,} excluded — no instrument_id, so no ledger row is possible)",
        flush=True,
    )
    print(f"  table counts before: strategy_signals={before[0]:,} strategy_outcomes={before[1]:,}\n", flush=True)

    started = time.perf_counter()
    signals_written, evaluated = _write_signals(conn, series_by_instrument)
    print(
        f"  pass 1: {signals_written:,} ledger rows over {evaluated:,} series with quarantine coverage "
        f"({len(series_by_instrument) - evaluated:,} returned zero bars — the loader is fail-closed) "
        f"| {time.perf_counter() - started:.0f}s\n",
        flush=True,
    )

    expected, skipped = _resolve_pending(conn, series_by_instrument)
    written = 0
    batch: list[OutcomeRow] = []
    for row in expected.values():
        batch.append(row)
        if len(batch) >= 5_000:
            written += store_outcomes(conn, batch)
            batch = []
    if batch:
        written += store_outcomes(conn, batch)
    conn.commit()
    print(f"  pass 2: {written:,} outcomes written of {len(expected):,} resolved\n", flush=True)

    mismatches = _compare(conn, expected)

    census: Counter[str] = Counter(row.outcome for row in expected.values())
    reasons: Counter[str] = Counter(row.reason for row in expected.values() if row.reason is not None)
    total = sum(census.values())
    print(
        "  --- outcome census (masked bars, 2.0xATR / 20-bar hold; strided signals, NOT 4a's every-bar set) ---",
        flush=True,
    )
    for name in ("tp_hit", "sl_hit", "expired", "ambiguous", "unresolved"):
        share = f"{census[name] / total:.4%}" if total else "-"
        print(f"    {name:>12} {census[name]:>10,}  {share:>9}", flush=True)
    print("\n  --- criterion 9: refusals by reason ---", flush=True)
    for name in ("window_truncated", "series_break", "quarantined_bar", "missing_bar_data"):
        print(f"    {name:>18} {reasons[name]:>10,}", flush=True)
    print("\n  --- fills with no bracket (not stored; 4a's flat-run case) ---", flush=True)
    for name, count in sorted(skipped.items()):
        print(f"    {name:>22} {count:>10,}", flush=True)

    row = conn.execute(_RETURN_CHECK, {"strategy_id": _IDENTITY.strategy_id}).fetchone()
    assert row is not None
    booked, max_return_delta, min_exit_price, min_return = row
    print("\n  --- independent SQL re-derivation of the stored return ---", flush=True)
    print(f"    booked rows checked      : {booked:,}", flush=True)
    print(f"    max |stored - rederived| : {max_return_delta}  (tolerance {_RETURN_TOLERANCE})", flush=True)
    print(f"    min exit_price           : {min_exit_price}", flush=True)
    print(f"    min gross_return_pct     : {min_return}", flush=True)

    deleted = cleanup(conn)
    after = _table_counts(conn)
    print(f"\n  cleanup: deleted {deleted:,} ledger rows; outcomes followed by CASCADE", flush=True)
    print(f"  table counts after : strategy_signals={after[0]:,} strategy_outcomes={after[1]:,}", flush=True)

    failures: list[str] = []
    if mismatches:
        failures.append(f"{len(mismatches)} field mismatches, first 5: {mismatches[:5]}")
    if written != len(expected):
        failures.append(f"wrote {written} of {len(expected)} outcomes")
    if max_return_delta is not None and max_return_delta > _RETURN_TOLERANCE:
        failures.append(f"stored return differs from the SQL re-derivation by {max_return_delta}")
    if after != before:
        failures.append(f"table counts did not return to {before}, they are {after}")

    print(f"\n  elapsed: {time.perf_counter() - started:.1f}s", flush=True)
    if failures:
        for line in failures:
            print(f"*** FAIL: {line}", flush=True)
        return 1
    print("  *** PASS — round trip clean on the full eligible population ***", flush=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--roundtrip", action="store_true", help="acceptance 7, full population")
    parser.add_argument("--cleanup", action="store_true", help="delete an interrupted run's rows")
    args = parser.parse_args()
    if not (args.roundtrip or args.cleanup):
        parser.error("choose --roundtrip or --cleanup")

    with psycopg.connect(settings.database_url) as conn:
        if args.cleanup:
            print(f"deleted {cleanup(conn):,} ledger rows for {_IDENTITY.strategy_id}", flush=True)
            if not args.roundtrip:
                return 0
        return roundtrip(conn)


if __name__ == "__main__":
    sys.exit(main())
