"""#2414 — reproduce every figure the corpus-generation spec addendum states.

    PYTHONPATH=. uv run python -m scripts.verify_2414_corpus_generation

Read-only. Exits non-zero if an arm fails its own check.

⚠⚠ THE ENCODER A/B IS HERE BECAUSE A 400-INSTRUMENT SAMPLE GOT THE COST WRONG BY
4x. The first draft of the addendum extrapolated 2.9s from 400 instruments; the
full population says 11.5s for the `as_tuple()` encoder and 2.8s for the `str`
one. The sample was not wrong about scaling — it benchmarked a cheaper encoding
than the one specified, which is the more useful way for a sample to be wrong,
and it is exactly the case `.claude/CLAUDE.md`'s full-population rule exists for.
So the A/B ships as a script rather than as a table in prose.

⚠ Arms 2 and 3 load the same 3.4M bars once and hash them three ways, so the
comparison is between ENCODERS and not between runs. Timing one encoder per
process would fold cold-cache and corpus drift into the difference.
"""

from __future__ import annotations

import hashlib
import sys
import time
from collections.abc import Callable, Sequence
from decimal import Decimal

import psycopg

from app.config import settings
from app.services.corpus_generation import (
    CORPUS_GENERATION_RULE_VERSION,
    CorpusGenerationBuilder,
    encode_value,
)
from app.services.indicator_series import BarSeries
from app.services.market_regime_provider import MarketRegimeProvider
from app.services.price_masked_bars import (
    QUARANTINE_RULE_SET_VERSION,
    load_bar_spans,
    load_masked_bars,
    load_union_calendar,
)
from app.services.price_segments import load_unresolved_breaks
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_signal_scan import choose_frontier

_FIELDS = ("open", "high", "low", "close", "volume")


def _as_tuple_encoder(value: Decimal | None) -> str:
    """The PARENT spec's canonical form, for the A/B only — not used in production."""
    if value is None:
        return "~"
    sign, digits, exponent = value.as_tuple()
    return f"{sign}:{''.join(map(str, digits))}:{exponent}"


def _time_encoder(
    cache: Sequence[tuple[int, BarSeries]],
    encoder: Callable[[Decimal | None], str],
) -> tuple[float, str]:
    hasher = hashlib.blake2b(digest_size=8)
    started = time.perf_counter()
    for instrument_id, series in cache:
        parts = [str(instrument_id)]
        for day, row in zip(series.dates, series.rows, strict=True):
            parts.append(day.isoformat())
            parts.append("|".join(encoder(row.get(field)) for field in _FIELDS))
        hasher.update("\x1f".join(parts).encode("utf-8"))
        hasher.update(b"\x1e")
    return time.perf_counter() - started, hasher.hexdigest()


def _repr_encoder_time(cache: Sequence[tuple[int, BarSeries]]) -> float:
    hasher = hashlib.blake2b(digest_size=8)
    started = time.perf_counter()
    for instrument_id, series in cache:
        parts = [str(instrument_id)]
        for day, row in zip(series.dates, series.rows, strict=True):
            parts.append(day.isoformat())
            parts.append(repr(row))
        hasher.update("\x1f".join(parts).encode("utf-8"))
        hasher.update(b"\x1e")
    return time.perf_counter() - started


def main() -> int:
    failures: list[str] = []
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")

        print("=== arm 1: the pass's read set ===")
        universe = load_validated_universe(conn)
        spans = load_bar_spans(conn, universe)
        frontier = choose_frontier({instrument_id: span.last_bar for instrument_id, span in spans.items()})
        if frontier is None:
            print("  no loadable instruments — cannot reproduce")
            return 1
        eligible = sorted(
            instrument_id
            for instrument_id, span in spans.items()
            if span.last_bar == frontier.bar_date and span.bars >= 2
        )
        print(f"  universe={len(universe):,}  loadable={len(spans):,}  frontier={frontier.bar_date}")
        print(f"  eligible={len(eligible):,}")

        started = time.perf_counter()
        cache: list[tuple[int, BarSeries]] = []
        for instrument_id in eligible:
            series = load_masked_bars(conn, instrument_id).series
            if len(series) >= 2:
                cache.append((instrument_id, series))
        load_seconds = time.perf_counter() - started
        bars = sum(len(series) for _, series in cache)
        print(f"  instruments folded in={len(cache):,}  bars={bars:,}  load={load_seconds:.2f}s")

        print("\n=== arm 2: the production stamp ===")
        builder = CorpusGenerationBuilder(
            frontier_date=frontier.bar_date,
            quarantine_rule_set_version=QUARANTINE_RULE_SET_VERSION,
        )
        builder.add_spans(spans)
        # ⚠ The script reproduces the PASS's components, so it must reproduce the
        # calendar one too — the scan feeds `panel_dates_by_plan` here. Rebuilding
        # the published calendars needs the manifest walk `run_signal_scan` does;
        # this arm uses the union calendar over every loadable instrument, which
        # is that walk's input, and says so rather than silently omitting it.
        builder.add_panel_calendars({"__union__": frozenset(load_union_calendar(conn, sorted(spans)))})
        builder.add_unresolved_breaks(load_unresolved_breaks(conn, eligible))
        builder.add_regime(MarketRegimeProvider.load(conn).classification_items())
        started = time.perf_counter()
        for instrument_id, series in cache:
            builder.add_series(instrument_id, series)
        generation = builder.finish()
        production_seconds = time.perf_counter() - started
        print(f"  rule_version={CORPUS_GENERATION_RULE_VERSION}")
        print(f"  corpus_generation={generation}  ({production_seconds:.2f}s over {builder.bar_count:,} bars)")

        print("\n=== arm 3: encoder A/B, one corpus, three encodings ===")
        as_tuple_seconds, as_tuple_digest = _time_encoder(cache, _as_tuple_encoder)
        str_seconds, str_digest = _time_encoder(cache, encode_value)
        repr_seconds = _repr_encoder_time(cache)
        print(f"  {'encoder':<28}{'seconds':>9}")
        print(f"  {'as_tuple() (parent spec)':<28}{as_tuple_seconds:>9.2f}")
        print(f"  {'repr(row)':<28}{repr_seconds:>9.2f}")
        print(f"  {'str() per field (shipped)':<28}{str_seconds:>9.2f}")
        if str_seconds > 0:
            print(f"  as_tuple() costs {as_tuple_seconds / str_seconds:.1f}x the shipped encoder")
        # ⚠ The two digests MUST differ: they are different encodings of one
        # corpus, and equality would mean one of the arms is not hashing what it
        # claims to. This is the arm that would catch a benchmark measuring the
        # same code twice — which is how the 400-instrument sample went wrong.
        if as_tuple_digest == str_digest:
            failures.append("the two encoders produced the same digest — one arm is not hashing what it claims")

        print("\n=== arm 4: stamped rows in the ledger ===")
        for table in ("strategy_signals", "strategy_signal_observations", "strategy_signal_daily_counts"):
            counts = conn.execute(
                f"""
                SELECT count(*), count(corpus_generation), count(DISTINCT corpus_generation)
                FROM {table}
                """  # noqa: S608 — table names are a fixed literal tuple above, never input
            ).fetchone()
            if counts is None:
                failures.append(f"{table} returned no aggregate row")
                continue
            total, stamped, distinct = counts
            print(f"  {table:<34} rows={total:>9,}  stamped={stamped:>9,}  distinct={distinct}")
        watermarks = conn.execute(
            """
            SELECT strategy_id, frontier_date, corpus_generation
            FROM strategy_scan_watermark
            ORDER BY strategy_id
            """
        ).fetchall()
        print(f"  watermarks: {len(watermarks)}")
        for strategy_id, frontier_date, stored in watermarks:
            moved = "unknown" if stored is None else ("SAME" if stored == generation else "MOVED")
            print(f"    {strategy_id:<38} {frontier_date}  {stored}  corpus={moved}")

        print("\n=== arm 5: the baseline this is a share of ===")
        runs = conn.execute(
            """
            SELECT started_at::date, status,
                   extract(epoch FROM (finished_at - started_at))::int AS secs,
                   left(coalesce(error_msg, ''), 60)
            FROM job_runs
            WHERE job_name = 'strategy_signal_scan'
            ORDER BY started_at DESC
            LIMIT 8
            """
        ).fetchall()
        for row in runs:
            print(f"    {row[0]}  {row[1]:<8} {row[2]:>5}s  {row[3]}")
        # ⚠ A SHARE OF WHAT, NAMED. The denominator is the newest run that
        # actually scanned; an `up_to_date` run returns before the loop, so
        # dividing by it would report an overhead the pass never pays.
        scanned = [row for row in runs if "status=scanned" in (row[3] or "")]
        if scanned:
            print(f"  newest scanned pass: {scanned[0][2]}s → stamp is {production_seconds / scanned[0][2] * 100:.1f}%")
        else:
            print("  ⚠ no `scanned` run in the last 8 — the share cannot be computed from this window")

    for failure in failures:
        print(f"\nFAIL: {failure}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
