"""#2282 stage 2b — load the Hugging Face daily-price archive into the research corpus.

``paperswithbacktest/Stocks-Daily-Price``: 4 Parquet shards, ~525 MB,
25.8M rows, 7,693 symbols, 1962→2026, ~30 s to download.

WHY THIS ARCHIVE AND NOT yfinance
---------------------------------
We never run a scraper. Yahoo's ToS §2.4(i) binds the *collector*; downloading
a third party's already-published file does not put us in that role. The
archive is nonetheless a Yahoo derivative (29/29 identical first-bar dates
against Yahoo, including Yahoo's own artefacts — ``ATCX`` starting 2026-01-09
is in this file too), so ``upstream_source`` records ``yahoo_derivative`` and
never ``other``. Two vendors that both resolve to Yahoo are ONE observation;
agreement between them is circular, not corroborating. See
``.claude/skills/data-sources/research-price-corpus.md``.

WHAT THIS IS NOT
----------------
Not ``price_daily``. That is the eToro-sourced EXECUTION view the order path
reads. This is the RESEARCH corpus — third-party provenance, unspecified
licence, deep history. sql/249's header states the separation at length.

TWO PHASES, DELIBERATELY
------------------------
1. ``load_archive`` — series upsert, then bars via COPY, then the denormalised
   census columns from a Python-side accumulator.
2. ``run_quarantine`` — reads each series back from Postgres in date order and
   runs the #2261 rule set over it.

They are separate because the quarantine rules need a *complete* series in
ascending date order (B4 and T1-T3 all read neighbours), and a symbol can
straddle a Parquet row-group or shard boundary. Evaluating per-shard would
produce a wrong verdict at every seam. Reading back from Postgres is a few
minutes and is correct by construction. Both phases are independently
idempotent.

NOT IN scripts/check_bulk_ingest_copy_pattern.sh's whitelist
------------------------------------------------------------
That lint is scoped to the *ownership* dataset ingesters — its rule D.1
requires ``INSERT INTO ownership_*`` as the drain, which this file will never
have. The COPY-into-``_stg_``-then-``INSERT…SELECT…ON CONFLICT`` pattern it
enforces is followed here regardless; only the registration is inapplicable.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg

from app.services.price_quarantine import (
    RULE_SET_VERSION,
    Bar,
    evaluate_series,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Provenance — recorded verbatim, per the #2282 handoff.
# ---------------------------------------------------------------------------

VENDOR = "paperswithbacktest/Stocks-Daily-Price"

#: A Yahoo scrape, established by fingerprint rather than by the dataset card.
UPSTREAM_SOURCE = "yahoo_derivative"

#: The card's licence field literally reads "other". Not laundered into
#: "public domain" or "MIT" — an unspecified licence is a real state.
LICENCE = "other/unspecified"

#: ⚠ VERIFIED, not assumed. The handoff said to record 'unknown' and to
#: "verify it before claiming it"; sql/251's header carries the evidence:
#: AAPL 2020-08-27 close = 125.01 against an unadjusted ~$500 and a 4:1 split
#: settling 2020-08-31, so OHLC are split-adjusted. `adj_close` additionally
#: carries the dividend adjustment (it differs from `close` for AAPL/CSCO and
#: is identical for AMZN/AAL/BRK-A) and is stored in its own column, because
#: for a dividend payer it sits OUTSIDE [low, high].
ADJUSTMENT_BASIS = "split_adjusted"

#: Asset class handed to the quarantine rule set. Every symbol in this archive
#: is a US listing; ``params_for`` reads this to pick the 5-day-week
#: continuity parameters.
_ASSET_CLASS = "us_equity"

#: eToro publishes venue variants of the same company as separate instrument
#: rows — ``AAPL``, ``AAPL.RTH`` (regular trading hours) and ``AAPL.24-7``.
#: They are the same issuer on a different session, and 536 of the 558 ``.RTH``
#: rows say so themselves via ``canonical_instrument_id``. Resolving a research
#: series onto a venue variant would attach deep history to the wrong row and
#: consume the ``uq_research_price_series_vendor_instrument`` slot the real
#: instrument needs.
_VENUE_VARIANT_SUFFIXES = (".RTH", ".24-7")

#: eToro suffixes some US primary listings ``.US`` where no bare symbol exists
#: (``ABT.US`` is Abbott; there is no ``ABT`` row). Stripped for matching.
_PRIMARY_LISTING_SUFFIX = ".US"


# ---------------------------------------------------------------------------
# Census of the load itself
# ---------------------------------------------------------------------------


@dataclass
class LoadCensus:
    """What the load did, including everything it could not do.

    Reported in full — a coverage figure that omits its own failures is the
    thing #2282 exists to prevent.
    """

    symbols_seen: int = 0
    series_upserted: int = 0
    bars_copied: int = 0

    #: Rows the archive supplies with no close at all (NaN). A bar without a
    #: close is not a price observation, and ``research_price_daily.close`` is
    #: NOT NULL. This is absence of data, NOT a price judgement — no floor, no
    #: threshold. Counted so the drop is never silent.
    rows_without_close: int = 0

    #: (symbol, date) pairs the archive repeats. Drained via ON CONFLICT, so
    #: the last one wins; counted so a vendor-side duplication is visible.
    duplicate_bar_rows: int = 0

    resolved_series: int = 0
    unresolved_series: int = 0
    #: Vendor symbols that matched more than one instrument after
    #: normalisation. Left UNRESOLVED rather than guessed — an ambiguous join
    #: recorded as ``symbol_exact`` would be a lie about its own evidence.
    ambiguous_symbols: list[str] = field(default_factory=list)

    #: Ticker-reuse guard (handoff obligation 3). See ``reuse_guard_note``.
    reuse_guard_checked: int = 0
    reuse_guard_uncheckable: int = 0

    @property
    def reuse_guard_note(self) -> str:
        return (
            f"ticker-reuse guard: {self.reuse_guard_checked} series checked at "
            f"ingest, {self.reuse_guard_uncheckable} UNCHECKABLE HERE — the "
            "schema holds no listing date. instruments.first_seen_at is the "
            "eToro DETECTION date (#2290), not a listing date, and using it "
            "would reject every series with real pre-2025 history; #2290's "
            "forward record is what eventually supplies one. The delisting "
            "half needs stage 2c's Form 25 suspension dates. ⚠ A DIFFERENT "
            "discriminator does work and is run by `--verify`: return "
            "correlation against price_daily over the shared window. It "
            "reaches only the series with eToro overlap, and separating "
            "reuse from split-epoch from eToro-side defects is #2293."
        )


@dataclass
class QuarantineCensus:
    series_evaluated: int = 0
    bars_evaluated: int = 0
    transitions_evaluated: int = 0
    bar_verdicts_written: int = 0
    transition_verdicts_written: int = 0


# ---------------------------------------------------------------------------
# Symbol resolution
# ---------------------------------------------------------------------------


def normalise_vendor_symbol(symbol: str) -> str:
    """Vendor symbol → our symbol spelling.

    Yahoo (and therefore this archive) spells a share class with a hyphen —
    ``BRK-A``, ``BF-B``. We spell it with a dot, matching the exchange's own
    convention: ``BRK.B``. US tickers carry no native hyphen, so the
    substitution is unambiguous.
    """
    return symbol.strip().upper().replace("-", ".")


def index_instruments(
    rows: Sequence[tuple[int, str]],
) -> tuple[dict[str, int], set[str]]:
    """``(instrument_id, symbol)`` rows → lookup key → instrument_id, plus ambiguous keys.

    Pure, so the resolution policy is table-testable without a database — which
    is where the interesting cases are (venue variants, ``.US`` collisions,
    two instruments claiming one key).

    Venue variants are excluded outright rather than deduplicated afterwards,
    so a collision that survives into ``ambiguous`` is a genuine
    two-instruments-one-symbol case and not eToro session bookkeeping.
    """
    index: dict[str, int] = {}
    ambiguous: set[str] = set()
    for instrument_id, symbol in rows:
        sym = symbol.strip().upper()
        if sym.endswith(_VENUE_VARIANT_SUFFIXES):
            continue
        if sym.endswith(_PRIMARY_LISTING_SUFFIX):
            sym = sym[: -len(_PRIMARY_LISTING_SUFFIX)]
        if sym in index and index[sym] != instrument_id:
            ambiguous.add(sym)
            continue
        index[sym] = instrument_id

    for sym in ambiguous:
        index.pop(sym, None)
    return index, ambiguous


def build_symbol_index(conn: psycopg.Connection[Any]) -> tuple[dict[str, int], set[str]]:
    """``index_instruments`` over #2289's validated universe.

    Scoped to ``asset_class = 'us_equity'`` AND ``instrument_type_id = 5`` (US
    stocks ex-ETF). ``us_equity`` alone is an EXCHANGE class and mixes in
    several hundred ETFs — #2289 §4.0.
    """
    rows = conn.execute(
        """
        SELECT i.instrument_id, i.symbol
        FROM instruments i
        JOIN exchanges e ON e.exchange_id = i.exchange
        WHERE e.asset_class = 'us_equity'
          AND i.instrument_type_id = 5
        """
    ).fetchall()
    return index_instruments([(int(r[0]), str(r[1])) for r in rows])


# ---------------------------------------------------------------------------
# Parquet reading
# ---------------------------------------------------------------------------


def _decimal(value: object) -> Decimal | None:
    """Parquet double → Decimal, with NaN and infinity read as absent.

    The archive uses NaN for a missing field rather than null. ``Decimal(str(nan))``
    raises ``InvalidOperation``, so this is not optional defensiveness.
    """
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return Decimal(str(value))
    if isinstance(value, (int, Decimal)):
        return Decimal(value)
    return None


@dataclass(frozen=True)
class _Row:
    symbol: str
    bar_date: date
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume: int | None
    adj_close: Decimal | None


def iter_archive_symbols(paths: Sequence[Path]) -> Iterator[str]:
    """Distinct symbols, reading ONLY the symbol column.

    Parquet is columnar, so this touches a few MB rather than decoding 25.8M
    rows of OHLCV to learn 7,693 strings.
    """
    import pyarrow.parquet as pq  # local import — pyarrow is heavy and only needed here

    for path in paths:
        reader = pq.ParquetFile(path)
        for group in range(reader.metadata.num_row_groups):
            table = reader.read_row_group(group, columns=["symbol"])
            yield from table.column("symbol").unique().to_pylist()


def iter_archive_rows(paths: Sequence[Path]) -> Iterator[_Row]:
    """Stream the archive one Parquet row group at a time.

    Row-group at a time rather than whole-file: a shard is ~6.5M rows and
    materialising one as Python objects is several GB.
    """
    import pyarrow.parquet as pq  # local import — pyarrow is heavy and only needed here

    for path in paths:
        reader = pq.ParquetFile(path)
        for group in range(reader.metadata.num_row_groups):
            cols = reader.read_row_group(group).to_pydict()
            for sym, bar_date, o, h, low, c, v, ac in zip(
                cols["symbol"],
                cols["date"],
                cols["open"],
                cols["high"],
                cols["low"],
                cols["close"],
                cols["volume"],
                cols["adj_close"],
                strict=True,
            ):
                yield _Row(
                    symbol=sym,
                    bar_date=date.fromisoformat(bar_date),
                    open=_decimal(o),
                    high=_decimal(h),
                    low=_decimal(low),
                    close=_decimal(c),
                    volume=int(v) if v is not None else None,
                    adj_close=_decimal(ac),
                )


# ---------------------------------------------------------------------------
# Phase 1 — load
# ---------------------------------------------------------------------------

_STAGE_DDL = """
CREATE TEMP TABLE _stg_research_bars (
    -- Surrogate ordinal so the de-duplication below has a deterministic
    -- "last one" to keep. COPY preserves insertion order into the heap, but
    -- SELECT order is not guaranteed, so relying on it would be a bug that
    -- only shows up under a parallel plan.
    row_ordinal   BIGSERIAL,
    vendor_symbol TEXT NOT NULL,
    bar_date      DATE NOT NULL,
    open          NUMERIC,
    high          NUMERIC,
    low           NUMERIC,
    close         NUMERIC NOT NULL,
    volume        BIGINT,
    adj_close     NUMERIC
) ON COMMIT DROP
"""

# ⚠ DISTINCT ON is load-bearing, not tidiness. Postgres raises
# `cardinality_violation: ON CONFLICT DO UPDATE command cannot affect row a
# second time` when the SELECT feeding an upsert contains the conflict key
# twice — so a repeated (symbol, date) in one 500k-row batch would abort the
# whole flush rather than resolving to "last one wins". The same trap is
# documented in app/services/sec_13f_dataset_ingest.py.
#
# The 2026-08-05 load found zero duplicates in this archive, which is exactly
# why this has to be right by construction: the failure mode is invisible until
# the vendor republishes with one, and then it takes out a 25.8M-row load.
_DRAIN_SQL = """
INSERT INTO research_price_daily
    (series_id, bar_date, open, high, low, close, volume, adj_close)
SELECT DISTINCT ON (s.series_id, g.bar_date)
       s.series_id, g.bar_date, g.open, g.high, g.low, g.close, g.volume, g.adj_close
FROM _stg_research_bars g
JOIN research_price_series s
  ON s.vendor = %(vendor)s AND s.vendor_symbol = g.vendor_symbol
ORDER BY s.series_id, g.bar_date, g.row_ordinal DESC
ON CONFLICT (series_id, bar_date) DO UPDATE SET
    open      = EXCLUDED.open,
    high      = EXCLUDED.high,
    low       = EXCLUDED.low,
    close     = EXCLUDED.close,
    volume    = EXCLUDED.volume,
    adj_close = EXCLUDED.adj_close
"""


def upsert_series(
    conn: psycopg.Connection[Any],
    symbols: Sequence[str],
    index: dict[str, int],
    ambiguous: set[str],
    census: LoadCensus,
) -> None:
    """Create one series row per vendor symbol, resolved where we can resolve it.

    ⚠ An unresolved series is NOT an error and is NOT skipped. It is a company
    that was listed and is not on eToro's book, i.e. the only measurement we
    have of eToro-listing bias — see sql/249's header. Dropping it here would
    delete the evidence the corpus exists to produce.
    """
    taken: set[int] = set()
    payload: list[tuple[str, str, str, str, str, int | None, str | None]] = []
    for symbol in symbols:
        key = normalise_vendor_symbol(symbol)
        instrument_id = index.get(key)
        if key in ambiguous:
            census.ambiguous_symbols.append(symbol)
        if instrument_id is not None and instrument_id in taken:
            # Two vendor symbols normalising onto one instrument within a
            # vendor is the ticker-reuse pair the partial unique index rejects.
            # Leave the second unresolved and counted rather than letting the
            # insert fail the whole batch.
            #
            # ⚠ WHICH of the pair keeps the resolution is decided by
            # ``symbols`` order, which ``load_archive`` sorts — so it is
            # lexicographic, deterministic and ARBITRARY, not a policy. That is
            # deliberate: there is no evidence at ingest for preferring one
            # spelling of a reused ticker over the other, and inventing a
            # tie-break (shorter symbol, more bars, earlier first bar) would
            # dress a coin flip up as a rule. Both symbols land in
            # ``ambiguous_symbols`` and are reported, so the pair is visible
            # rather than silently halved.
            census.ambiguous_symbols.append(symbol)
            instrument_id = None
        if instrument_id is not None:
            taken.add(instrument_id)
        payload.append(
            (
                VENDOR,
                symbol,
                UPSTREAM_SOURCE,
                LICENCE,
                ADJUSTMENT_BASIS,
                instrument_id,
                "symbol_exact" if instrument_id is not None else None,
            )
        )

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO research_price_series
                (vendor, vendor_symbol, upstream_source, licence,
                 adjustment_basis, instrument_id, resolution_method)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (vendor, vendor_symbol) DO UPDATE SET
                upstream_source   = EXCLUDED.upstream_source,
                licence           = EXCLUDED.licence,
                adjustment_basis  = EXCLUDED.adjustment_basis,
                instrument_id     = EXCLUDED.instrument_id,
                resolution_method = EXCLUDED.resolution_method,
                updated_at        = now()
            """,
            payload,
        )

    census.series_upserted = len(payload)
    census.resolved_series = sum(1 for row in payload if row[5] is not None)
    census.unresolved_series = census.series_upserted - census.resolved_series
    # Obligation 3: say what the guard could NOT check.
    census.reuse_guard_uncheckable = census.resolved_series


def load_archive(
    conn: psycopg.Connection[Any],
    paths: Sequence[Path],
    *,
    batch_rows: int = 500_000,
) -> LoadCensus:
    """Two passes over the archive: symbols, then bars.

    The first pass is needed because a bar cannot be staged without a
    ``series_id``, and the archive is not guaranteed sorted by symbol. It reads
    one column, so it is cheap.
    """
    census = LoadCensus()

    logger.info("pass 1/2: collecting symbols from %d shard(s)", len(paths))
    symbols = sorted(set(iter_archive_symbols(paths)))
    census.symbols_seen = len(symbols)

    index, ambiguous = build_symbol_index(conn)
    logger.info(
        "resolving %d vendor symbols against %d us_equity type-5 instruments (%d ambiguous keys excluded)",
        len(symbols),
        len(index),
        len(ambiguous),
    )
    upsert_series(conn, symbols, index, ambiguous, census)
    conn.commit()

    logger.info("pass 2/2: loading bars")
    # (first_bar, last_bar, rows_staged) accumulated per symbol — ~7,693 narrow
    # entries, so O(symbols) memory rather than O(bars).
    #
    # ⚠ This is deliberately the count of rows STAGED, not of distinct bars. A
    # per-(symbol, date) dedup set over 25.8M rows would be ~2.5 GB, so vendor
    # duplication is detected AFTER the load instead: the drain collapses
    # duplicates via ON CONFLICT, so a symbol the archive repeats ends up with
    # a stored bar_count higher than its actual bar rows, and
    # `research_series_census_drift` reports it by name. That makes the drift
    # assert a real check rather than a tautology — writing the census from the
    # same aggregate the drift view reads would reconcile by construction and
    # prove nothing.
    stats: dict[str, tuple[date, date, int]] = {}

    batch: list[tuple[Any, ...]] = []

    def flush() -> None:
        if not batch:
            return
        with conn.cursor() as cur:
            cur.execute(_STAGE_DDL)
            copy_sql = (
                "COPY _stg_research_bars "
                "(vendor_symbol, bar_date, open, high, low, close, volume, adj_close) "
                "FROM STDIN"
            )
            with cur.copy(copy_sql) as copy:
                for record in batch:
                    copy.write_row(record)
            cur.execute(_DRAIN_SQL, {"vendor": VENDOR})
        conn.commit()  # ON COMMIT DROP releases the TEMP table
        batch.clear()

    for row in iter_archive_rows(paths):
        if row.close is None:
            census.rows_without_close += 1
            continue
        prior = stats.get(row.symbol)
        if prior is None:
            stats[row.symbol] = (row.bar_date, row.bar_date, 1)
        else:
            stats[row.symbol] = (
                min(prior[0], row.bar_date),
                max(prior[1], row.bar_date),
                prior[2] + 1,
            )

        batch.append(
            (
                row.symbol,
                row.bar_date,
                row.open,
                row.high,
                row.low,
                row.close,
                row.volume,
                row.adj_close,
            )
        )
        census.bars_copied += 1
        if len(batch) >= batch_rows:
            flush()
            logger.info("  %s bars loaded", f"{census.bars_copied:,}")
    flush()

    _write_census(conn, stats)
    census.duplicate_bar_rows = reconcile_census(conn, VENDOR)
    return census


def _write_census(
    conn: psycopg.Connection[Any],
    stats: dict[str, tuple[date, date, int]],
) -> None:
    """Set the denormalised census columns from the load accumulator.

    ⚠ ``bar_count = 0`` is unrepresentable by CHECK constraint (sql/249): "no
    bars" has exactly one spelling, all three NULL. A symbol whose every row
    lacked a close therefore never appears in ``stats`` and keeps its NULLs,
    which is the correct state and not a gap.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE _stg_research_census (
                vendor_symbol TEXT NOT NULL,
                first_bar     DATE NOT NULL,
                last_bar      DATE NOT NULL,
                bar_count     INTEGER NOT NULL
            ) ON COMMIT DROP
            """
        )
        with cur.copy("COPY _stg_research_census (vendor_symbol, first_bar, last_bar, bar_count) FROM STDIN") as copy:
            for symbol, (first, last, count) in stats.items():
                copy.write_row((symbol, first, last, count))
        cur.execute(
            """
            UPDATE research_price_series s
               SET first_bar = g.first_bar,
                   last_bar  = g.last_bar,
                   bar_count = g.bar_count,
                   updated_at = now()
              FROM _stg_research_census g
             WHERE s.vendor = %(vendor)s
               AND s.vendor_symbol = g.vendor_symbol
            """,
            {"vendor": VENDOR},
        )
    conn.commit()
    logger.info("census written for %d series", len(stats))


def reconcile_census(conn: psycopg.Connection[Any], vendor: str) -> int:
    """Correct any series the drift view disagrees with, and report the delta.

    Runs ONCE, immediately after a bulk load, and only over series the drift
    view actually names — so it is a targeted repair, not a re-derivation that
    would make the subsequent drift assert meaningless.

    The realistic cause is vendor duplication: the drain collapses repeated
    (symbol, date) rows via ON CONFLICT while the accumulator counted each one.
    Returns the total number of staged rows that turned out to be duplicates,
    which is the figure ``LoadCensus.duplicate_bar_rows`` reports.
    """
    rows = conn.execute(
        """
        SELECT d.series_id, d.vendor_symbol,
               d.stored_first_bar, d.actual_first_bar,
               d.stored_last_bar,  d.actual_last_bar,
               d.stored_bar_count, d.actual_bar_count
        FROM research_series_census_drift d
        WHERE d.vendor = %s
        """,
        (vendor,),
    ).fetchall()

    duplicates = 0
    for (
        series_id,
        symbol,
        stored_first,
        actual_first,
        stored_last,
        actual_last,
        stored_count,
        actual_count,
    ) in rows:
        logger.warning(
            "census drift on %s: first %s->%s last %s->%s count %s->%s",
            symbol,
            stored_first,
            actual_first,
            stored_last,
            actual_last,
            stored_count,
            actual_count,
        )
        if stored_count is not None and actual_count is not None:
            duplicates += max(0, int(stored_count) - int(actual_count))
        conn.execute(
            """
            UPDATE research_price_series
               SET first_bar = %s, last_bar = %s, bar_count = %s, updated_at = now()
             WHERE series_id = %s
            """,
            (actual_first, actual_last, actual_count, series_id),
        )
    conn.commit()
    if rows:
        logger.warning("reconciled %d drifting series (%d duplicate rows)", len(rows), duplicates)
    return duplicates


def census_drift(conn: psycopg.Connection[Any]) -> int:
    """Rows in ``research_series_census_drift``. MUST be 0 after a load.

    The denormalised census columns are derived state with no trigger behind
    them (a per-row trigger on a 25.8M-row COPY is the wrong trade), so this is
    the only thing standing between "the ingest maintained them" and "the
    ingest was believed to have maintained them".
    """
    row = conn.execute("SELECT count(*) FROM research_series_census_drift").fetchone()
    assert row is not None
    return int(row[0])


# ---------------------------------------------------------------------------
# Phase 2 — quarantine
# ---------------------------------------------------------------------------


def run_quarantine(
    conn: psycopg.Connection[Any],
    *,
    vendor: str = VENDOR,
    as_of: date,
) -> QuarantineCensus:
    """Run the #2261 rule set over every loaded series.

    Rules are imported from ``app.services.price_quarantine`` and NOT
    re-expressed here. One rule set, one implementation — a second copy in SQL
    is how a closed vocabulary ends up with three spellings and a live 500.

    Nothing is deleted and nothing is filtered. A failed company's last bar at
    $0.0004 is the signal; a price floor here would be a survivorship filter
    wearing a data-quality hat.
    """
    census = QuarantineCensus()
    series = conn.execute(
        """
        SELECT series_id FROM research_price_series
        WHERE vendor = %s AND bar_count IS NOT NULL
        ORDER BY series_id
        """,
        (vendor,),
    ).fetchall()

    for (series_id,) in series:
        rows = conn.execute(
            """
            SELECT bar_date, open, high, low, close, volume
            FROM research_price_daily
            WHERE series_id = %s
            ORDER BY bar_date
            """,
            (series_id,),
        ).fetchall()
        if not rows:
            continue
        bars = [
            Bar(
                price_date=r[0],
                open=r[1],
                high=r[2],
                low=r[3],
                close=r[4],
                volume=Decimal(r[5]) if r[5] is not None else None,
            )
            for r in rows
        ]
        verdicts = evaluate_series(bars, _ASSET_CLASS, as_of=as_of)

        bar_rows = [
            (
                series_id,
                v.price_date,
                v.return_usable,
                v.range_usable,
                v.provisional,
                list(v.rules),
                RULE_SET_VERSION,
            )
            for v in verdicts.bars
            if v.notable
        ]
        trans_rows = [
            (
                series_id,
                v.price_date,
                v.prior_date,
                v.observed_ratio,
                v.provisional,
                list(v.rules),
                v.turnover_ratio,
                v.corroboration,
                RULE_SET_VERSION,
            )
            for v in verdicts.transitions
            if v.notable
        ]

        with conn.cursor() as cur:
            # Replace this series' verdicts wholesale: a rule-set change must
            # not leave a stale verdict from an older version behind, and the
            # coverage row is what says which version a series was judged at.
            cur.execute("DELETE FROM research_bar_quarantine WHERE series_id = %s", (series_id,))
            cur.execute("DELETE FROM research_transition_quarantine WHERE series_id = %s", (series_id,))
            if bar_rows:
                cur.executemany(
                    """
                    INSERT INTO research_bar_quarantine
                        (series_id, bar_date, return_usable, range_usable,
                         provisional, rules, rule_set_version)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    bar_rows,
                )
            if trans_rows:
                cur.executemany(
                    """
                    INSERT INTO research_transition_quarantine
                        (series_id, bar_date, prior_date, observed_ratio,
                         provisional, rules, turnover_ratio, corroboration,
                         rule_set_version)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    trans_rows,
                )
            cur.execute(
                """
                INSERT INTO research_price_quarantine_coverage
                    (series_id, rule_set_version, first_bar, last_bar,
                     bars_evaluated, transitions_evaluated)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (series_id) DO UPDATE SET
                    rule_set_version      = EXCLUDED.rule_set_version,
                    first_bar             = EXCLUDED.first_bar,
                    last_bar              = EXCLUDED.last_bar,
                    bars_evaluated        = EXCLUDED.bars_evaluated,
                    transitions_evaluated = EXCLUDED.transitions_evaluated,
                    evaluated_at          = now()
                """,
                (
                    series_id,
                    RULE_SET_VERSION,
                    bars[0].price_date,
                    bars[-1].price_date,
                    len(bars),
                    len(verdicts.transitions),
                ),
            )
        conn.commit()

        census.series_evaluated += 1
        census.bars_evaluated += len(bars)
        census.transitions_evaluated += len(verdicts.transitions)
        census.bar_verdicts_written += len(bar_rows)
        census.transition_verdicts_written += len(trans_rows)
        if census.series_evaluated % 500 == 0:
            logger.info(
                "  quarantine: %d series / %s bars",
                census.series_evaluated,
                f"{census.bars_evaluated:,}",
            )

    return census
