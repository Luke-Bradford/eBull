"""#2834 §7 item 2, slice A — full-population A/B for storing the Intrader split stamps.

Run from repo root::

    PYTHONPATH=. uv run python -m scripts.measure_2834_split_stamp_load --fingerprint var/2834_fingerprint_before.json
    # ... re-load the vendor ...
    PYTHONPATH=. uv run python -m scripts.measure_2834_split_stamp_load --fingerprint var/2834_fingerprint_after.json
    PYTHONPATH=. uv run python -m scripts.measure_2834_split_stamp_load \
        --compare var/2834_fingerprint_before.json var/2834_fingerprint_after.json
    PYTHONPATH=. uv run python -m scripts.measure_2834_split_stamp_load --verify
    PYTHONPATH=. uv run python -m scripts.measure_2834_split_stamp_load --census

WHY A FINGERPRINT AND NOT A TWO-CHECKOUT A/B
--------------------------------------------
The skill's arm 1 is ``main``'s parser against the branch's. Here that arm is
degenerate by construction — the branch adds two fields and touches no other —
so running it would compare a parser against itself on seven of nine columns
and prove nothing about the thing that can actually go wrong: **the re-load is
an ``ON CONFLICT DO UPDATE`` over 50.1M existing rows, so it rewrites every
pre-existing column.**

``--fingerprint`` is the real control the skill asks for and a two-checkout A/B
is not: the BEFORE file is what ``main`` actually stored, read off the table,
not a reconstruction of what it would have stored. Per-series exact ``NUMERIC``
sums, so a single changed cent in one bar of one series moves its row.

⚠ The BEFORE file can only be taken once, before the load. There is no way to
recompute it afterwards.

WHAT THE ENTITY IS
------------------
``--verify`` is the skill's arm 2 (parse vs STORED) and its distinct entity is
the **stamped event** — a ``(series, bar_date)`` whose split factor is not 1 or
whose dividend is not 0. Comparing all 50.1M bars would be comparing 1 to 1 and
0 to 0 fifty million times; the aggregate row counts in ``--compare`` already
cover wholesale loss. The symmetric difference of the two event sets is
enumerated by name, both directions, because a stamp the loader DROPPED and a
stamp it INVENTED are different defects.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

import psycopg

from app.config import settings
from app.services.research_corpus_ingest import INTRADER_ARCHIVE, parse_intrader_rows

#: The mirror. ⚠ NEVER fetched — see ``IntraderCsvArchive``'s docstring: a
#: re-fetch would let the capture date drift, and the capture date is the one
#: property that makes the archive survivorship-free.
MIRROR: Final[Path] = Path.home() / "Dev/eBull/var/research_corpus/mirrors/icyDenev_Intrader/Data/Day"

VENDOR: Final[str] = INTRADER_ARCHIVE.vendor

#: Columns the re-load rewrites and must not change. ``adj_close`` is included
#: even though nothing on the ARM B path reads it — the upsert rewrites it, so
#: it is in the blast radius whether or not anything consumes it.
_CARRIED_COLUMNS: Final[tuple[str, ...]] = ("open", "high", "low", "close", "volume", "adj_close")


# ---------------------------------------------------------------------------
# Fingerprint — the control arm
# ---------------------------------------------------------------------------

_FINGERPRINT_SQL = f"""
SELECT s.vendor_symbol,
       count(*)                            AS bars,
       min(d.bar_date)::text               AS first_bar,
       max(d.bar_date)::text               AS last_bar,
       {", ".join(f"sum(d.{c})::text AS sum_{c}" for c in _CARRIED_COLUMNS)},
       {", ".join(f"count(*) FILTER (WHERE d.{c} IS NULL) AS null_{c}" for c in _CARRIED_COLUMNS)}
FROM research_price_series s
JOIN research_price_daily d ON d.series_id = s.series_id
WHERE s.vendor = %(vendor)s
GROUP BY s.vendor_symbol
"""


def fingerprint(conn: psycopg.Connection[Any], out: Path) -> int:
    """Per-series exact aggregate of every column the re-load rewrites."""
    with conn.cursor() as cur:
        cur.execute(_FINGERPRINT_SQL, {"vendor": VENDOR})
        columns = [c.name for c in cur.description or ()]
        rows = {r[0]: dict(zip(columns[1:], r[1:], strict=True)) for r in cur}
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"vendor": VENDOR, "series": rows}, indent=1, sort_keys=True, default=str))
    print(f"fingerprint written: {out}  series={len(rows):,}")
    return 0


def compare(before_path: Path, after_path: Path) -> int:
    """Assert the re-load moved nothing it was not supposed to move."""
    before = json.loads(before_path.read_text())["series"]
    after = json.loads(after_path.read_text())["series"]

    lost = sorted(set(before) - set(after))
    gained = sorted(set(after) - set(before))
    changed: list[tuple[str, str, Any, Any]] = []
    for symbol in sorted(set(before) & set(after)):
        for key, old in before[symbol].items():
            new = after[symbol].get(key)
            if new != old:
                changed.append((symbol, key, old, new))

    print("\n=== control arm: pre-existing columns across the re-load ===")
    print(f"  series before        : {len(before):,}")
    print(f"  series after         : {len(after):,}")
    print(f"  series LOST          : {len(lost):,}   <- MUST be 0")
    print(f"  series GAINED        : {len(gained):,}")
    print(f"  series with a moved aggregate : {len({c[0] for c in changed}):,}   <- MUST be 0")
    for symbol in lost[:25]:
        print(f"    lost   {symbol}")
    for symbol in gained[:25]:
        print(f"    gained {symbol}  (a symbol the mirror now serves and did not before)")
    for symbol, key, old, new in changed[:50]:
        print(f"    moved  {symbol:<10} {key:<16} {old} -> {new}")
    if len(changed) > 50:
        print(f"    ... and {len(changed) - 50:,} more moved aggregates")
    return 1 if lost or changed else 0


# ---------------------------------------------------------------------------
# Arm 2 — parse vs STORED, over the stamped-event set
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Stamp:
    symbol: str
    bar_date: str
    split_factor: str
    dividend: str


def _parsed_events() -> Iterator[Stamp]:
    """Every stamped event the BRANCH parser reads off the mirror.

    ⚠ Reads the mirror through ``parse_intrader_rows`` rather than re-splitting
    the CSV here. A private re-parse would agree with the loader about a field
    index it also got wrong — the same circularity ``b65abd9c`` found in the
    vendor's own ``adj_close``.
    """
    for path in sorted(MIRROR.glob("*.csv")):
        symbol = path.stem.strip().upper()
        with path.open(newline="") as handle:
            for row in parse_intrader_rows(symbol, handle):
                if row.split_factor is None or row.dividend is None:
                    # Unparseable stamps are their own class and are counted by
                    # --census off the stored side; they cannot be compared.
                    continue
                if row.split_factor == 1 and row.dividend == 0:
                    continue
                yield Stamp(symbol, row.bar_date.isoformat(), _norm(row.split_factor), _norm(row.dividend))


def _norm(value: Decimal) -> str:
    """Trailing-zero-insensitive text form, so ``4`` and ``4.0`` compare equal.

    Postgres ``NUMERIC`` preserves the scale it was given, and the CSV writes
    ``4`` where a round-trip can produce ``4.0``. Comparing the raw text would
    report a difference that is not one.
    """
    return format(value.normalize(), "f")


_STORED_EVENTS_SQL = """
SELECT s.vendor_symbol, d.bar_date::text, d.split_factor::text, d.dividend::text
FROM research_price_series s
JOIN research_price_daily d ON d.series_id = s.series_id
WHERE s.vendor = %(vendor)s
  AND (d.split_factor IS DISTINCT FROM 1 OR d.dividend IS DISTINCT FROM 0)
"""


def verify(conn: psycopg.Connection[Any]) -> int:
    parsed = {(s.symbol, s.bar_date): (s.split_factor, s.dividend) for s in _parsed_events()}
    stored: dict[tuple[str, str], tuple[str, str]] = {}
    with conn.cursor(name="stored_events") as cur:
        cur.itersize = 100_000
        cur.execute(_STORED_EVENTS_SQL, {"vendor": VENDOR})
        for symbol, bar_date, factor, dividend in cur:
            stored[(symbol, bar_date)] = (
                _norm(Decimal(factor)) if factor is not None else "∅",
                _norm(Decimal(dividend)) if dividend is not None else "∅",
            )

    dropped = sorted(set(parsed) - set(stored))
    invented = sorted(set(stored) - set(parsed))
    disagreed = sorted(k for k in set(parsed) & set(stored) if parsed[k] != stored[k])

    print("\n=== arm 2: parse vs STORED, distinct entity = stamped event ===")
    print(f"  events parsed off the mirror : {len(parsed):,}")
    print(f"  events stored in the corpus  : {len(stored):,}")
    print(f"  DROPPED (parsed, not stored) : {len(dropped):,}   <- MUST be 0")
    print(f"  INVENTED (stored, not parsed): {len(invented):,}   <- MUST be 0")
    print(f"  DISAGREED on value           : {len(disagreed):,}   <- MUST be 0")
    for key in dropped[:25]:
        print(f"    dropped  {key[0]:<10} {key[1]}  parsed={parsed[key]}")
    for key in invented[:25]:
        print(f"    invented {key[0]:<10} {key[1]}  stored={stored[key]}")
    for key in disagreed[:25]:
        print(f"    differs  {key[0]:<10} {key[1]}  parsed={parsed[key]} stored={stored[key]}")
    return 1 if dropped or invented or disagreed else 0


# ---------------------------------------------------------------------------
# Arm 3 — mechanism audit: what the change newly ADMITS
# ---------------------------------------------------------------------------

_CENSUS_SQL = """
WITH stamped AS (
    SELECT s.series_id, s.vendor_symbol, d.bar_date, d.split_factor, d.dividend, d.close
    FROM research_price_series s
    JOIN research_price_daily d ON d.series_id = s.series_id
    WHERE s.vendor = %(vendor)s
)
SELECT
    count(*)                                                        AS bars,
    count(*) FILTER (WHERE split_factor IS NULL)                    AS split_absent,
    count(*) FILTER (WHERE dividend IS NULL)                        AS dividend_absent,
    count(*) FILTER (WHERE split_factor = 1)                        AS split_unit,
    count(*) FILTER (WHERE split_factor <> 1)                       AS split_event,
    count(*) FILTER (WHERE split_factor > 1)                        AS split_forward,
    count(*) FILTER (WHERE split_factor < 1 AND split_factor > 0)   AS split_reverse,
    count(*) FILTER (WHERE split_factor <= 0)                       AS split_nonpositive,
    count(*) FILTER (WHERE dividend <> 0)                           AS dividend_paid,
    count(*) FILTER (WHERE dividend < 0)                            AS dividend_negative,
    count(DISTINCT series_id) FILTER (WHERE split_factor <> 1)      AS series_with_event,
    count(DISTINCT series_id)                                       AS series
FROM stamped
"""

_EXTREME_SQL = """
SELECT s.vendor_symbol, d.bar_date, d.split_factor, d.close
FROM research_price_series s
JOIN research_price_daily d ON d.series_id = s.series_id
WHERE s.vendor = %(vendor)s AND d.split_factor <> 1
ORDER BY abs(ln(d.split_factor)) DESC
LIMIT 20
"""


def census(conn: psycopg.Connection[Any]) -> int:
    """What the corpus now carries, and specifically what it newly ADMITS.

    The change widens what a downstream derivation can see: two columns it will
    DIVIDE by. So the counts that matter are not "how many stamps landed" but
    the ones a uniform ``close / scale`` rule would choke on — a non-positive
    factor, an absent factor read as 1 by ``COALESCE``, a stamp on a bar the
    loader dropped.
    """
    with conn.cursor() as cur:
        cur.execute(_CENSUS_SQL, {"vendor": VENDOR})
        columns = [c.name for c in cur.description or ()]
        row = cur.fetchone()
        assert row is not None
        stats = dict(zip(columns, row, strict=True))
        cur.execute(_EXTREME_SQL, {"vendor": VENDOR})
        extremes = cur.fetchall()

    print("\n=== arm 3: stamp census as STORED ===")
    for key in columns:
        print(f"  {key:<22}: {stats[key]:>12,}")
    print("\n  20 largest stamped factors by |ln(factor)| — the gain side, by name:")
    for symbol, bar_date, factor, close in extremes:
        print(f"    {symbol:<10} {bar_date}  factor={factor}  close={close}")
    # The COALESCE trap named in the verdict's §6 item 8: an ABSENT stamp reads
    # exactly like "no split here". It is a failure only if it can happen at
    # all, so it is asserted rather than printed.
    return 1 if stats["split_absent"] or stats["split_nonpositive"] else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fingerprint", type=Path, metavar="OUT")
    parser.add_argument("--compare", type=Path, nargs=2, metavar=("BEFORE", "AFTER"))
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--census", action="store_true")
    args = parser.parse_args(argv)

    if args.compare:
        return compare(*args.compare)
    if not (args.fingerprint or args.verify or args.census):
        parser.error("pass one of --fingerprint / --compare / --verify / --census")

    rc = 0
    with psycopg.connect(settings.database_url) as conn:
        if args.fingerprint:
            rc |= fingerprint(conn, args.fingerprint)
        if args.verify:
            rc |= verify(conn)
        if args.census:
            rc |= census(conn)
    return rc


if __name__ == "__main__":
    sys.exit(main())
