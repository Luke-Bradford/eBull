"""Load a frozen third-party daily-price archive into the research corpus.

#2282 stage 2b built this for ONE archive; #2597 generalised it to any archive
that can yield ``(symbol, bar)`` rows, because the survivorship question turns
on having more than one capture date.

``paperswithbacktest/Stocks-Daily-Price``: 4 Parquet shards, ~525 MB,
25.8M rows, 7,693 symbols, 1962→2026, ~30 s to download.

``icyDenev/Intrader`` (#2597): a mirrored GitHub repo of headerless daily CSVs,
one per symbol. Its OHLC are **unadjusted** and its ``adjclose`` carries the
split AND dividend adjustment — the opposite split from the HF archive, whose
OHLC are already split-adjusted (sql/251). That is why ``adjustment_basis``
travels with the archive rather than living as a module constant.

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

import csv
import logging
import math
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol

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


@dataclass(frozen=True)
class ArchiveProvenance:
    """Who published this archive and what its stored prices actually mean.

    Travels with the load rather than living as module constants, because the
    two archives we hold disagree on the one field a reader cannot afford to
    get wrong. ``adjustment_basis`` describes the OHLC columns ONLY; a separate
    ``adj_close`` is stored where the archive supplies one (sql/251).
    """

    vendor: str
    upstream_source: str
    licence: str
    adjustment_basis: str


#: ⚠ ``split_adjusted`` is VERIFIED for this archive, not assumed. sql/251's
#: header carries the evidence: AAPL 2020-08-27 close = 125.01 against an
#: unadjusted ~$500 and a 4:1 split settling 2020-08-31.
HF_ARCHIVE = ArchiveProvenance(
    vendor=VENDOR,
    upstream_source=UPSTREAM_SOURCE,
    licence=LICENCE,
    adjustment_basis=ADJUSTMENT_BASIS,
)

#: ⚠ ``unadjusted`` is MEASURED, and it is the opposite of what #2398 recorded
#: for the same vendor. The same AAPL bar that reads 125.01 in the HF archive
#: reads **500.04** here (2020-08-27, pre-split), and the 1980-12-12 IPO bar
#: reads 28.75 against Yahoo's split-adjusted 0.1283. So this archive's OHLC
#: carry NEITHER the split nor the dividend adjustment, and its ninth CSV
#: column — stored as ``adj_close`` — carries both. Consumers computing returns
#: must read ``adj_close`` (#2400); ``close`` here is the raw traded level.
INTRADER_ARCHIVE = ArchiveProvenance(
    vendor="icyDenev/Intrader",
    # A Yahoo redistribution like the HF archive, so the two are ONE
    # observation and agreement between them is circular, never corroborating.
    upstream_source="yahoo_derivative",
    licence="other/unspecified",
    adjustment_basis="unadjusted",
)


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
            "forward record is what eventually supplies one. ⚠ The delisting "
            "half is now WIRED (#2297, `--link-delistings`) and it reaches "
            "almost nothing: `(b)` delistings state no suspension date at all, "
            "so the truncation set is empty by construction — see "
            "`link_form25_delistings`. ⚠ A DIFFERENT "
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


def archive_symbol_candidates(filing_symbol: str) -> list[str]:
    """A Form 25 cover-page symbol → the archive spellings to try, in priority order.

    SOURCE RULE, not convenience. A Chapter 11 filing moves the issuer to OTC
    under a ``Q``-suffixed ticker, and the Form 25 that removes the security
    from the exchange carries that POST-bankruptcy symbol. Price archives are
    keyed on whatever the scraper saw, which for most of the series' life was
    the pre-bankruptcy symbol. Resolving without the strip loses precisely the
    **bankruptcies** and keeps the acquisitions — a bias along the exact axis a
    survivorship-free corpus exists to protect
    (``.claude/skills/data-sources/research-price-corpus.md``).

    Exact match takes priority over the strip, and that ordering is
    load-bearing rather than tidy: ``NHIQ`` is present in both archives under
    its own name, so a blind strip would silently rebind it to a different
    security. Measured on the 15 ``Q``-suffixed symbols in
    ``sec_form25_register``: the Intrader mirror carries 11 under both
    spellings, 2 under the ``Q`` form only and 2 under the stripped form only,
    so the strip is what takes that archive from 13/15 to 15/15.

    Separator variants are tried too — the archives spell a share class with a
    hyphen where our register carries a dot.
    """
    base = filing_symbol.strip().upper()
    if not base:
        return []
    spellings = [base]
    if base.endswith("Q") and len(base) > 1:
        spellings.append(base[:-1])

    out: list[str] = []
    for spelling in spellings:
        for variant in (spelling, spelling.replace(".", "-"), spelling.replace("-", ".")):
            if variant not in out:
                out.append(variant)
    return out


def vendor_symbol_has_bankruptcy_suffix(vendor_symbol: str) -> bool:
    """The INVERSE read of ``archive_symbol_candidates``' ``Q``-strip: does this
    ARCHIVE spelling carry the post-bankruptcy OTC suffix?

    One rule, two directions — ``series_termination.TerminationEvidence`` names
    this function as the only permitted derivation of ``q_suffix``, because a
    second spelling of "trailing Q with more than one letter" would drift from
    the candidate ladder's.
    """
    base = vendor_symbol.strip().upper()
    return base.endswith("Q") and len(base) > 1


def resolve_archive_symbol(filing_symbol: str, available: set[str]) -> str | None:
    """First candidate spelling present in ``available``, else ``None``.

    Pure so the ``Q``-strip precedence is table-testable without a corpus.
    """
    for candidate in archive_symbol_candidates(filing_symbol):
        if candidate in available:
            return candidate
    return None


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


class ArchiveReader(Protocol):
    """Two passes over one archive: symbols first, then bars.

    Two rather than one because a bar cannot be staged without a ``series_id``,
    and no archive we hold guarantees symbol-sorted rows.
    """

    def symbols(self) -> Iterator[str]: ...

    def rows(self) -> Iterator[_Row]: ...


@dataclass(frozen=True)
class ParquetArchive:
    """The HF archive: 4 Parquet shards with a ``symbol`` column."""

    paths: Sequence[Path]

    def symbols(self) -> Iterator[str]:
        return iter_archive_symbols(self.paths)

    def rows(self) -> Iterator[_Row]:
        return iter_archive_rows(self.paths)


#: Column order of an ``icyDenev/Intrader`` daily CSV. Headerless, so the order
#: is the only contract there is — it was verified in #2398 against the
#: Deamoner mirror on a shared bar (SPY 1993-01-29) and against the last bar,
#: where ``adjclose == close`` because the back-adjustment factor is 1.0.
_INTRADER_COLUMNS = ("date", "open", "high", "low", "close", "volume", "split_factor", "dividend", "adjclose")


def _csv_decimal(value: str) -> Decimal | None:
    """CSV field → Decimal, with unparseable and non-finite read as absent.

    ``InvalidOperation`` alone is the right catch: every malformed string tried
    (``''``, ``'abc'``, ``'  '``, ``'1.2.3'``, an embedded NUL) raises it and
    none raises ``ValueError``. ``'nan'`` and ``'inf'`` parse SUCCESSFULLY and
    are caught by the finiteness test instead, which is why that test is not
    redundant with the except clause.
    """
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        return None
    return parsed if parsed.is_finite() else None


def _share_volume(value: str) -> int | None:
    """CSV field → a share COUNT, or absent.

    ⚠ A non-integral volume is read as ABSENT rather than truncated, and that
    is a provenance check rather than fussiness. This archive stores raw share
    counts — measured across all 22,879 mirror files, **zero** volume fields
    contain a decimal point — whereas the `Stonks/tickers` archive stores
    volume scaled to MILLIONS with three decimals (AAPL's 469,033,600 reads
    ``469.034``). So a fractional value here is evidence the reader is looking
    at the wrong archive, and ``int()`` would silently record a share count
    about a million times too small, understating turnover on every bar it
    touched. Absent is nullable and honest; a truncated 469 is neither.
    """
    parsed = _csv_decimal(value)
    if parsed is None or parsed != parsed.to_integral_value():
        return None
    return int(parsed)


def parse_intrader_rows(symbol: str, lines: Iterator[str]) -> Iterator[_Row]:
    """Parse one Intrader CSV into bars.

    ⚠ NO price filter, deliberately. #2398's reader drops any row with
    ``close <= 0`` because a benchmark has no business at zero; this one must
    not, because a failed company's last bar at $0.0004 is the exact signal the
    survivorship corpus is being built to keep. A floor here would be a
    survivorship filter wearing a data-quality hat — the same sentence
    ``run_quarantine`` carries. Only an unusable ``close`` (absent, unparseable
    or non-finite) drops a row, and ``LoadCensus.rows_without_close`` counts it.
    """
    for line in lines:
        fields = next(csv.reader([line]), None)
        if fields is None or len(fields) < len(_INTRADER_COLUMNS):
            continue
        try:
            bar_date = date.fromisoformat(fields[0].strip())
        except ValueError:
            continue
        yield _Row(
            symbol=symbol,
            bar_date=bar_date,
            open=_csv_decimal(fields[1]),
            high=_csv_decimal(fields[2]),
            low=_csv_decimal(fields[3]),
            close=_csv_decimal(fields[4]),
            volume=_share_volume(fields[5]),
            adj_close=_csv_decimal(fields[8]),
        )


@dataclass(frozen=True)
class IntraderCsvArchive:
    """The mirrored ``icyDenev/Intrader`` daily directory, one CSV per symbol.

    ⚠ Consumes the LOCAL MIRROR and never fetches. None of the three archives
    carries a licence and none owes us uptime, so the skill's rule is to mirror
    once and read the copy; re-fetching at ingest would also make the capture
    date — the single property that makes an archive survivorship-free — drift
    silently under us.
    """

    directory: Path

    def _files(self) -> list[Path]:
        return sorted(self.directory.glob("*.csv"))

    def symbols(self) -> Iterator[str]:
        for path in self._files():
            yield path.stem.strip().upper()

    def rows(self) -> Iterator[_Row]:
        for path in self._files():
            with path.open(newline="") as handle:
                yield from parse_intrader_rows(path.stem.strip().upper(), handle)


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
    provenance: ArchiveProvenance = HF_ARCHIVE,
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
                provenance.vendor,
                symbol,
                provenance.upstream_source,
                provenance.licence,
                provenance.adjustment_basis,
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
    archive: ArchiveReader,
    *,
    provenance: ArchiveProvenance = HF_ARCHIVE,
    batch_rows: int = 500_000,
) -> LoadCensus:
    """Two passes over the archive: symbols, then bars.

    The first pass is needed because a bar cannot be staged without a
    ``series_id``, and no archive is guaranteed sorted by symbol. On Parquet it
    reads one column; on a CSV directory it reads only filenames. Either way it
    is cheap relative to the bar pass.
    """
    census = LoadCensus()

    logger.info("pass 1/2: collecting symbols from %s", provenance.vendor)
    symbols = sorted(set(archive.symbols()))
    census.symbols_seen = len(symbols)

    index, ambiguous = build_symbol_index(conn)
    logger.info(
        "resolving %d vendor symbols against %d us_equity type-5 instruments (%d ambiguous keys excluded)",
        len(symbols),
        len(index),
        len(ambiguous),
    )
    upsert_series(conn, symbols, index, ambiguous, census, provenance)
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
            cur.execute(_DRAIN_SQL, {"vendor": provenance.vendor})
        conn.commit()  # ON COMMIT DROP releases the TEMP table
        batch.clear()

    for row in archive.rows():
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

    _write_census(conn, stats, provenance.vendor)
    census.duplicate_bar_rows = reconcile_census(conn, provenance.vendor)
    return census


def _write_census(
    conn: psycopg.Connection[Any],
    stats: dict[str, tuple[date, date, int]],
    vendor: str = VENDOR,
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
            {"vendor": vendor},
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


def census_drift(conn: psycopg.Connection[Any], vendor: str | None = None) -> int:
    """Rows in ``research_series_census_drift``. MUST be 0 after a load.

    The denormalised census columns are derived state with no trigger behind
    them (a per-row trigger on a 25.8M-row COPY is the wrong trade), so this is
    the only thing standing between "the ingest maintained them" and "the
    ingest was believed to have maintained them".

    ``vendor`` scopes it to one archive. Unscoped is the stricter reading and
    stays the default, but a per-vendor load wants a gate it can actually own:
    the corpus now holds four vendors, and failing a fresh load because a
    DIFFERENT vendor drifted points the operator at the wrong archive.
    """
    if vendor is None:
        row = conn.execute("SELECT count(*) FROM research_series_census_drift").fetchone()
    else:
        row = conn.execute(
            "SELECT count(*) FROM research_series_census_drift WHERE vendor = %s",
            (vendor,),
        ).fetchone()
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


# ---------------------------------------------------------------------------
# Phase 3 — Form 25 delisting linkage (#2297)
# ---------------------------------------------------------------------------
#
# THE GUARD AS SPECIFIED DOES NOT WORK, AND THE MEASUREMENT SAYS SO
# ---------------------------------------------------------------------------
# `research-price-corpus.md` names two mandatory guards, the second being
# "require the series' first bar to precede the known listing date, and
# truncate at any Form 25 suspension date". sql/249 shipped the columns and
# #2282 stage 2c shipped the register; nothing joined them, which is what #2297
# was filed for.
#
# Joining them produced a falsified premise rather than a fix. Measured on the
# full 2023 register, not a sample:
#
#     provision   cohort rows   with a suspension_date
#     (a)(3)          212              62
#     (b)             105               0
#
# `(b)` — exchange-initiated delisting for non-compliance — is the provision
# where truncation is unambiguously right, and it never states a suspension
# date, because that sentence lives in the EX-99 rule-provision exhibit and
# exchanges attach a stub (sec-edgar.md §2.6 trap 5). Every date the cohort
# does supply is `(a)(3)`: merger, holdco reorganisation, redomiciliation,
# where the same economic entity commonly keeps trading under the same ticker.
#
# So the truncation set is EMPTY by construction, and the two series that carry
# a date are `LIN` (Linde plc, 8,572 bars) and `AMRX` (Amneal, 2,051) — both
# `instruments.is_tradable = true` today. Truncating either would delete ~3
# years of correct history from a live name to satisfy a rule aimed at dead
# ones.
#
# ⚠ THEREFORE THIS FUNCTION DOES NOT TRUNCATE, AT INGEST OR AT READ TIME.
# It records the evidence (date + provision, CHECK-tied by sql/253) and
# publishes what is NOT covered. Reader-side truncation was the right call
# between the two options on the table; it is simply not the question, because
# a provision-blind truncation has nothing correct to act on and a
# provision-aware one has nothing to act on at all.
#
# WHAT IS ACTUALLY DETECTABLE
# ---------------------------------------------------------------------------
# The other half of the guard — "first bar precedes the known listing date" —
# was thought unimplementable because the schema holds no listing date. It has
# a usable proxy that needs no suspension date and no threshold: a series whose
# FIRST bar postdates a Form 25 filing on the same symbol cannot be the series
# of the security that filing removed. That is reported, not acted on, because
# `DBD` proves post-dating is not sufficient evidence of reuse — Diebold
# Nixdorf's 2023-08-14 restart is the same company relisting after Chapter 11.
# The honest output is "identity unverified", which is the FIRST mandatory
# guard (label it), not the second.


@dataclass
class DelistingLinkCensus:
    """What the linkage covered, and — the load-bearing half — what it did not."""

    #: Corpus series whose vendor symbol appears in the common-equity cohort.
    overlap_series: int = 0
    #: Series given a ``delisting_date``. Requires a stated suspension date.
    suspension_dates_written: int = 0
    #: Series given source + provision + filed date but NO ``delisting_date``,
    #: because the filing states no suspension date. The date is NOT
    #: back-filled from ``filed_date``: that is a different event and
    #: mistruncates every series it touches (sec-edgar.md §2.6 trap 5). Before
    #: #2721 these wrote nothing at all, which held the exchange-failure
    #: class ``(b)`` — 0 stated dates on 105 cohort rows — at zero coverage.
    undated_evidence_written: int = 0
    #: Symbols whose cohort filings disagree on provision or suspension date.
    #: Left NULL rather than tie-broken — two different delisting events on one
    #: ticker is precisely the ambiguity the guard exists to surface.
    conflicting: list[str] = field(default_factory=list)
    #: Overlap by rule provision. `(b)` here with a zero write count is the
    #: measurement that kills provision-blind truncation.
    by_provision: dict[str, int] = field(default_factory=dict)
    #: (symbol, latest filing, first bar) where the series starts AFTER every
    #: Form 25 on that symbol (the classifier's ``identity_unverified``
    #: verdict — nothing is written). May be a later occupant or the same
    #: issuer relisting. A series starting BETWEEN two filings is a conflict,
    #: not an identity refusal, and appears under ``conflicting`` only.
    identity_unverified: list[tuple[str, date, date]] = field(default_factory=list)
    #: (symbol, earliest filing, last bar) where the series ends BEFORE the
    #: filing — a genuine termination, the shape the acceptance test expects
    #: and has never yet observed on this archive.
    terminating: list[tuple[str, date, date]] = field(default_factory=list)
    #: Measured register span at link time — (filings, first filed, last
    #: filed). Measured rather than written into the note below because the
    #: 2013-2024 expansion (#2721) moves it, and a hardcoded "2023 only"
    #: would go stale in the sentence a reader trusts most.
    register_filings: int = 0
    register_first_filed: date | None = None
    register_last_filed: date | None = None

    @property
    def coverage_note(self) -> str:
        span = (
            f"{self.register_filings:,} filings, {self.register_first_filed} to {self.register_last_filed}"
            if self.register_filings
            else "EMPTY register"
        )
        return (
            f"Form 25 register coverage: {span} — measured at link time, "
            "against a corpus spanning 1962-2026. Form 25 is US-only — there "
            "is no free authoritative equivalent for EU/UK/Asia/MENA, where "
            "#2290's forward record is the only future source. The cohort "
            "also excludes issuer-filed paragraph (c) filings, which are "
            "delistings but carry no descriptionClassSecurity and so cannot "
            "be verified as COMMON EQUITY (sql/252's view comment). A series "
            "absent from this linkage is therefore UNCHECKED, never 'checked "
            "and clean'."
        )


@dataclass(frozen=True)
class Form25Match:
    """One corpus series' aggregated Form 25 evidence, as the cohort CTE returns it."""

    symbol: str
    first_bar: date | None
    last_bar: date | None
    earliest_filed: date
    latest_filed: date
    provision_variants: int
    provision: str | None
    suspension_variants: int
    suspension_date: date | None


def classify_form25_match(match: Form25Match) -> str:
    """One match's verdict: ``write`` / ``conflict`` / ``identity_unverified``
    / ``no_suspension``.

    Pure so the policy is table-testable without a corpus. Four rules, and
    the two refusals matter more than either write:

    ``conflict`` — the cohort's filings for this symbol disagree on the
    provision or on the suspension date. Two different delisting events on one
    ticker is exactly the ambiguity the guard exists to surface, so it is
    counted and left NULL rather than tie-broken. There is no source rule
    saying which of two filings wins, and inventing a precedence order
    (latest-filed? the /A amendment?) would be a fabricated citation. The only
    verdict that writes nothing at all.

    ``identity_unverified`` — the series STARTS after every Form 25 on its
    symbol (``first_bar > latest_filed``), so it may be a later occupant of
    the ticker (ALPS: filed 2023-07, first bar 2025-10) or the same issuer
    relisting post-Chapter-11 (DBD). Either way the filings removed a security
    whose price history this is demonstrably NOT, and writing the evidence
    would mark a live, running series as delisted. Refused and censused. This
    gate predates nothing: the dated writer always had the hole (it wrote
    unconditionally) and it simply never fired — none of the four 2023
    identity-unverified overlaps carries a stated suspension date. The
    evidence write #2721 added would have fired on all four.

    A series that STRADDLES the filings — starts after the earliest but not
    after the latest — is a ``conflict``, not an identity refusal: the later
    filing may well describe this series' security (a relist-then-delist
    cycle), but the aggregate collapses the events, so the evidence cannot be
    attributed to one of them without inventing a precedence order. Refusing
    beats attributing: an unlinked terminating series degrades to the
    termination rule's honest two-armed bounds, while a mis-attributed write
    stamps a filed date that PRECEDES the series' own first bar. Zero cases
    in the 2023 register (no resolved symbol carries two distinct filed
    dates); the 2013-2024 expansion is what makes the branch reachable, and
    it must exist BEFORE the data that exercises it (Codex ckpt-2 on #2721).

    ``no_suspension`` — the filing states no suspension date. Since #2721 this
    WRITES the evidence (source, provision, filed date) and leaves only
    ``delisting_date`` NULL: ``filed_date`` is a DIFFERENT event (a Form 25
    carries up to three dates, sec-edgar.md §2.6 trap 5), and substituting it
    mistruncates every series it touches. This is the common case, not the
    exception — ``(b)`` supplies a suspension date on 0 of 105 cohort rows,
    which is why refusing to store the undated link held the exchange-failure
    class at zero coverage.
    """
    if match.provision_variants > 1 or match.suspension_variants > 1:
        return "conflict"
    if match.first_bar is not None:
        if match.first_bar > match.latest_filed:
            return "identity_unverified"
        if match.first_bar > match.earliest_filed:
            return "conflict"
    if match.suspension_date is None:
        return "no_suspension"
    return "write"


def link_form25_delistings(
    conn: psycopg.Connection[Any],
    *,
    vendor: str = VENDOR,
) -> DelistingLinkCensus:
    """Write Form 25 suspension dates onto the corpus. Truncates nothing.

    Idempotent: clears every previously ``sec_form25``-sourced delisting for
    this vendor before writing, so a rebuilt register cannot leave a stale date
    behind. Other ``delisting_source`` values (``universe_membership``,
    ``vendor``) are untouched — they come from #2290's forward record and this
    function has no evidence about them.
    """
    census = DelistingLinkCensus()

    span = conn.execute("SELECT count(*), min(filed_date), max(filed_date) FROM sec_form25_register").fetchone()
    assert span is not None  # an aggregate always returns one row; typing guard only
    census.register_filings = int(span[0])
    census.register_first_filed = span[1]
    census.register_last_filed = span[2]

    # Scoped to the common-equity VIEW, never the raw register: a Form 25 is
    # per-SECURITY, so `sec_form25_register` contains bond and warrant
    # delistings whose issuer is very much alive (§2.6 traps 2, 3 and 6 —
    # Berkshire filed two in 2023 and both were notes).
    # ⚠ Resolution is NOT a SQL join on ``c.symbol = s.vendor_symbol``, which
    # is what this did before #2597. A Form 25 cover page carries the
    # POST-bankruptcy ``Q``-suffixed ticker while the archive is keyed on the
    # pre-bankruptcy one, so an exact join drops precisely the bankruptcies
    # and keeps the acquisitions — see ``archive_symbol_candidates``. The
    # candidate ladder needs exact-before-strip precedence, which as SQL is a
    # correlated NOT EXISTS nobody can test; in Python it is a pure function.
    series_by_symbol = {
        str(sym): (int(series_id), first_bar, last_bar)
        for series_id, sym, first_bar, last_bar in conn.execute(
            "SELECT series_id, vendor_symbol, first_bar, last_bar FROM research_price_series WHERE vendor = %s",
            (vendor,),
        ).fetchall()
    }
    available = set(series_by_symbol)

    # One filing per row, NOT pre-aggregated by ``resolved_symbol``. The
    # aggregation has to happen AFTER resolution, because the ``Q`` strip can
    # land two filing symbols (``RVLP`` and ``RVLPQ``) on ONE archive series —
    # and those are two delisting events against one price history, which is
    # the ambiguity ``classify_form25_match`` refuses to write. Aggregating in
    # SQL first would hide the collision and let the second filing silently
    # overwrite the first's date.
    filings = conn.execute(
        """
        SELECT d.resolved_symbol, d.filed_date, d.rule_provision, d.suspension_date
          FROM sec_form25_common_equity_delistings d
         WHERE d.resolved_symbol IS NOT NULL
         ORDER BY d.resolved_symbol, d.filed_date
        """
    ).fetchall()

    by_series: dict[str, list[tuple[date, str | None, date | None]]] = {}
    for filing_symbol, filed_date, provision, suspension_date in filings:
        matched = resolve_archive_symbol(str(filing_symbol), available)
        if matched is None:
            continue
        by_series.setdefault(matched, []).append((filed_date, provision, suspension_date))

    rows = []
    for matched in sorted(by_series):
        events = by_series[matched]
        series_id, first_bar, last_bar = series_by_symbol[matched]
        # NULL-excluding, matching the `count(DISTINCT ...)` this replaced —
        # SQL's DISTINCT count ignores NULLs and a Python set does not, and the
        # difference would turn "one provision plus an unparsed one" into a
        # fabricated conflict.
        provisions = {p for _f, p, _s in events if p is not None}
        suspensions = {s for _f, _p, s in events if s is not None}
        filed_dates = [f for f, _p, _s in events]
        rows.append(
            (
                series_id,
                matched,
                first_bar,
                last_bar,
                min(filed_dates),
                max(filed_dates),
                len(provisions),
                min(provisions, default=None),
                len(suspensions),
                min(suspensions, default=None),
            )
        )

    conn.execute(
        """
        UPDATE research_price_series
           SET delisting_date = NULL, delisting_source = NULL,
               delisting_provision = NULL, delisting_filed_date = NULL,
               updated_at = now()
         WHERE vendor = %s AND delisting_source = 'sec_form25'
        """,
        (vendor,),
    )

    for (
        series_id,
        symbol,
        first_bar,
        last_bar,
        earliest_filed,
        latest_filed,
        provision_variants,
        provision,
        suspension_variants,
        suspension_date,
    ) in rows:
        match = Form25Match(
            symbol=symbol,
            first_bar=first_bar,
            last_bar=last_bar,
            earliest_filed=earliest_filed,
            latest_filed=latest_filed,
            provision_variants=provision_variants,
            provision=provision,
            suspension_variants=suspension_variants,
            suspension_date=suspension_date,
        )
        action = classify_form25_match(match)
        census.overlap_series += 1

        # Bucket by the CLASSIFIER's verdict, never by re-deriving the
        # condition here. Keying on `provision_variants` alone undercounts:
        # a symbol whose filings agree on the provision but disagree on the
        # suspension date is a conflict the classifier refuses to write, and
        # it would still have been filed under its provision — a census that
        # disagrees with the writer about what happened.
        # `provision` is None where the filing carries no <ruleProvision>
        # (the 25-NSE form omits it by design); label it rather than key on
        # None, which would TypeError the census consumers' sorted() calls.
        key = "conflicting" if action == "conflict" else (provision or "(unparsed)")
        census.by_provision[key] = census.by_provision.get(key, 0) + 1

        # The identity census keys on the CLASSIFIER's verdict — a straddling
        # series (starts after the earliest filing, not after the latest) is a
        # `conflict`, and reporting it under identity_unverified as well would
        # file one series in two categories (Codex ckpt-2 on #2721). The
        # terminating test keeps min() as its bound: "the series ends before
        # any Form 25 on this symbol" is one-sided, and the earliest filing is
        # the conservative side of it.
        if action == "identity_unverified":
            census.identity_unverified.append((symbol, latest_filed, first_bar))
        if last_bar is not None and last_bar < earliest_filed:
            census.terminating.append((symbol, earliest_filed, last_bar))

        if action == "conflict":
            census.conflicting.append(symbol)
            continue
        if action == "identity_unverified":
            # Recorded above. Nothing is written: the series starts after
            # every filing, so the removed security's history is not this.
            continue

        # Both remaining verdicts persist the EVIDENCE (source, provision,
        # earliest filed date); they differ only in whether a suspension date
        # exists to write. ``delisting_date`` is never fabricated from
        # ``filed_date`` — for ``no_suspension`` it stays NULL, which sql/353
        # made representable (source without date) precisely so the undated
        # ``(b)`` links stop being dropped on the floor.
        conn.execute(
            """
            UPDATE research_price_series
               SET delisting_date = %s,
                   delisting_source = 'sec_form25',
                   delisting_provision = %s,
                   delisting_filed_date = %s,
                   updated_at = now()
             WHERE series_id = %s
            """,
            (
                suspension_date if action == "write" else None,
                provision,
                earliest_filed,
                series_id,
            ),
        )
        if action == "write":
            census.suspension_dates_written += 1
        else:
            census.undated_evidence_written += 1

    conn.commit()
    logger.info(
        "form25 linkage: %d overlapping series, %d dated, %d undated evidence "
        "(no stated suspension date), %d conflicting, %d identity-unverified",
        census.overlap_series,
        census.suspension_dates_written,
        census.undated_evidence_written,
        len(census.conflicting),
        len(census.identity_unverified),
    )
    return census
