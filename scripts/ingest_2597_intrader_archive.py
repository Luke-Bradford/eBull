"""#2597 — load the mirrored ``icyDenev/Intrader`` daily archive into the research corpus.

Run from repo root::

    uv run python -m scripts.ingest_2597_intrader_archive --load
    uv run python -m scripts.ingest_2597_intrader_archive --link-delistings
    uv run python -m scripts.ingest_2597_intrader_archive --quarantine

⚠ Long-running (~48.6M rows across 22,879 symbols). Launch it with the tool's
own background mode — a ``nohup … &`` started inside an ordinary tool call is
killed when that call's process group is cleaned up, and a load that dies
part-way shows up only as a wrong bar count.

WHY THIS ARCHIVE AND NOT THE OTHER TWO
--------------------------------------
#2597 named three free GitHub archives (#2346) and asked for two of them. The
measurement says one. Over the FULL ``sec_form25_register`` — 1,282 filings, of
which 261 rows carry a resolved cover-page symbol (260 distinct) — the mirrors
on disk serve:

    archive                     served      unique contribution
    icyDenev/Intrader           259/260     258 (everything but ASAP)
    Stonks/tickers              247/260     1   (ASAP)

So ``Stonks/tickers`` adds ONE delisted name that Intrader lacks, and it costs
a second price semantics to get it: its OHLC are back-adjusted with no raw
column (AAPL 2020-08-27 reads 122.79 where the traded close was 500.04) and its
volume is scaled to millions. Both archives are Yahoo redistributions, so they
are ONE observation and their agreement is circular — the second one buys no
corroboration either. It is not ingested; ``--acceptance`` reports the residual
by name so the decision stays visible rather than becoming an absence.

⚠ WHAT THIS DOES NOT ESTABLISH
------------------------------
Not that the corpus is survivorship-free. Form 25 is US-only and the register
loaded here is 2023-only, so coverage across 2020-2026 is unmeasured; the
archive is keyed on the live ticker and structurally cannot separate ``X`` from
``X-DELISTED``; and its capture date bounds it — a name delisted after the
freeze cannot be served at all. ``--acceptance`` prints the capture date it
measured rather than one written down here.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

import psycopg

from app.config import settings
from app.services import research_corpus_ingest as ingest

logger = logging.getLogger("ingest_2597")

#: The local mirror. NEVER fetched at ingest — none of these archives carries a
#: licence or owes us uptime, and re-fetching would let the capture date (the
#: single property that makes an archive survivorship-free) drift silently.
DEFAULT_MIRROR = Path("var/research_corpus/mirrors/icyDenev_Intrader/Data/Day")


def load(conn: psycopg.Connection[tuple], mirror: Path) -> int:
    if not mirror.is_dir():
        logger.error("mirror directory not found: %s", mirror)
        return 1

    provenance = ingest.INTRADER_ARCHIVE
    started = time.time()
    census = ingest.load_archive(
        conn,
        ingest.IntraderCsvArchive(mirror),
        provenance=provenance,
    )
    drift = ingest.census_drift(conn, provenance.vendor)

    print("\n=== #2597 Intrader load census ===")
    print(f"  mirror                : {mirror}")
    print(f"  vendor                : {provenance.vendor}")
    print(f"  upstream_source       : {provenance.upstream_source}   <- Yahoo redistribution, NOT independent")
    print(f"  licence               : {provenance.licence}")
    print(f"  adjustment_basis      : {provenance.adjustment_basis}  (OHLC raw; adj_close is split+dividend)")
    print(f"  symbols seen          : {census.symbols_seen:,}")
    print(f"  series upserted       : {census.series_upserted:,}")
    print(f"    resolved            : {census.resolved_series:,}")
    print(f"    unresolved          : {census.unresolved_series:,}   <- eToro-listing-bias measure")
    print(f"    ambiguous symbols   : {len(census.ambiguous_symbols):,}")
    print(f"  bars loaded           : {census.bars_copied:,}")
    print(f"  rows without a close  : {census.rows_without_close:,}  (dropped, counted, no floor)")
    print(f"  duplicate vendor rows : {census.duplicate_bar_rows:,}")
    print(f"  {census.reuse_guard_note}")
    print(f"  census drift rows     : {drift}   <- MUST be 0")
    print(f"  elapsed               : {time.time() - started:.0f}s")

    if drift:
        logger.error("census drift is %d, expected 0 — the load is NOT done", drift)
        return 1
    return 0


def link_delistings(conn: psycopg.Connection[tuple]) -> int:
    census = ingest.link_form25_delistings(conn, vendor=ingest.INTRADER_ARCHIVE.vendor)
    print("\n=== #2597 Form 25 delisting linkage (Intrader) ===")
    print(f"  overlapping series      : {census.overlap_series:,}")
    print(f"  suspension dates written: {census.suspension_dates_written:,}")
    print(f"  no suspension date (NULL, not back-filled): {census.no_suspension_date:,}")
    print(f"  conflicting symbols     : {census.conflicting or 'none'}")
    print("  overlap by rule provision:")
    for provision, count in sorted(census.by_provision.items()):
        print(f"    {provision:<14}{count:>6}")
    print(f"  identity-unverified     : {len(census.identity_unverified)}")
    for symbol, filed, first_bar in census.identity_unverified:
        print(f"    {symbol:<8} filed {filed}  first bar {first_bar}")
    print(f"  terminating             : {len(census.terminating)}")
    print(f"  {census.coverage_note}")
    return 0


def quarantine(conn: psycopg.Connection[tuple], as_of: date) -> int:
    started = time.time()
    census = ingest.run_quarantine(conn, vendor=ingest.INTRADER_ARCHIVE.vendor, as_of=as_of)
    print("\n=== #2597 quarantine census (Intrader) ===")
    print(f"  series evaluated      : {census.series_evaluated:,}")
    print(f"  bars evaluated        : {census.bars_evaluated:,}")
    print(f"  transitions evaluated : {census.transitions_evaluated:,}")
    print(f"  bar verdicts stored   : {census.bar_verdicts_written:,}")
    print(f"  trans verdicts stored : {census.transition_verdicts_written:,}")
    print(f"  elapsed               : {time.time() - started:.0f}s")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--load", action="store_true")
    parser.add_argument("--link-delistings", action="store_true")
    parser.add_argument("--quarantine", action="store_true")
    parser.add_argument(
        "--mirror",
        type=Path,
        default=DEFAULT_MIRROR,
        help=f"local mirror directory of per-symbol daily CSVs (default: {DEFAULT_MIRROR})",
    )
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        default=None,
        help="quarantine as-of date (default: the archive's own last bar, i.e. its capture date)",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not (args.load or args.link_delistings or args.quarantine):
        parser.error("pass at least one of --load / --link-delistings / --quarantine")

    with psycopg.connect(settings.database_url) as conn:
        if args.load:
            rc = load(conn, args.mirror)
            if rc:
                return rc
        if args.link_delistings:
            rc = link_delistings(conn)
            if rc:
                return rc
        if args.quarantine:
            as_of = args.as_of
            if as_of is None:
                # The capture date, MEASURED. Passing today's date would have
                # every series in the archive read as ~2 years stale and turn
                # the staleness rule into a blanket verdict.
                row = conn.execute(
                    "SELECT max(last_bar) FROM research_price_series WHERE vendor = %s",
                    (ingest.INTRADER_ARCHIVE.vendor,),
                ).fetchone()
                if row is None or row[0] is None:
                    logger.error("no bars loaded for %s — run --load first", ingest.INTRADER_ARCHIVE.vendor)
                    return 1
                as_of = row[0]
                logger.info("quarantine as-of = %s (measured archive capture date)", as_of)
            return quarantine(conn, as_of)
    return 0


if __name__ == "__main__":
    sys.exit(main())
