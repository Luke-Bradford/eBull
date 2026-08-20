"""#2337 — read-only census of the FINRA bimonthly match rate, per skip class.

``finra_short_interest_refresh`` warned when ``rows_resolved / rows_parsed``
fell below an absolute 0.50 floor, described as a "universe drift / FINRA
column-shape regression sentinel". #2337 observed that the warning fires on
every normal fire. This script is what says whether that is true, and on what
population — every figure is computed at run time from the stored raw payloads,
so a later ingest moves the numbers instead of leaving a stale copy in prose.

Why the ratio cannot have an absolute floor: its two sides are governed by
different populations. The numerator is bounded by OUR universe
(``build_preloaded_symbol_resolver`` selects ``instruments WHERE is_tradable``),
the denominator is FINRA's — every US-reported symbol, including OTC,
preferreds, ETFs and share-class siblings. A ratio between two unrelated
populations has no healthy value that can be written down in advance, which is
exactly the shape ``docs/review-prevention-log.md`` warns about under "measure
the parse rate against the wire, not against itself".

So the census reports the skip classes SEPARATELY, because they answer
different questions:

``invalid_row``
    Required field missing or non-integer. This is the class a FINRA
    column-shape regression lands in, and it is the one whose healthy value is
    knowable in advance rather than measured: a well-formed pipe-delimited file
    has every required field on every row.

``ambiguous_symbol``
    The normalised symbol collides inside OUR universe (``BRK.A`` and ``BRKA``
    both collapse to ``BRKA``). A property of our instrument table, not of the
    file.

``no_instrument_match``
    The symbol is not in our tradable universe. Structural and dominant — this
    is the class that makes the aggregate ratio ~25%, and nothing about it is a
    defect.

``resolved``
    Stored.

The row-shape gate is ``finra_short_interest_ingest.required_row_fields``,
called directly rather than re-implemented, so ``invalid_row`` here is the same
count ingest would produce.

⚠ Body-date mismatches are COUNTED here, not raised. In ingest they are a
file-level fatal (``HeaderCorruptionError``), which is correct there; a census
that aborted on the first one would report nothing about the other 33 files.

Usage:

    PYTHONPATH=. uv run python -m scripts.audit_2337_finra_match_rate
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass

import psycopg

from app.config import settings
from app.services.finra_short_interest_ingest import (
    build_preloaded_symbol_resolver,
    normalise_symbol,
    parse_body_settlement_date,
    required_row_fields,
)

_RAW_SQL = """
    SELECT accession_number, payload
      FROM filing_raw_documents
     WHERE accession_number LIKE 'FINRA_SI_%'
       AND payload IS NOT NULL
     ORDER BY accession_number
"""


@dataclass(frozen=True)
class FileCensus:
    settlement_date: str
    parsed: int
    invalid: int
    ambiguous: int
    no_match: int
    resolved: int
    body_date_mismatch: int

    @property
    def match_rate(self) -> float:
        return self.resolved / self.parsed if self.parsed else 0.0

    @property
    def universe_share(self) -> float:
        """Resolved as a share of the rows whose symbol our universe COULD hold.

        Denominator excludes ``no_instrument_match``, i.e. it is the FINRA rows
        that name a symbol we track. Reported so the ``ambiguous`` cost is
        visible against a denominator that is ours rather than FINRA's.
        """
        resolvable = self.resolved + self.ambiguous
        return self.resolved / resolvable if resolvable else 0.0


def census_one(payload: str, settlement_date: str, resolver, ambiguous_keys) -> FileCensus:
    reader = csv.DictReader(io.StringIO(payload), delimiter="|")
    parsed = invalid = ambiguous = no_match = resolved = body_mismatch = 0
    for row in reader:
        parsed += 1
        required = required_row_fields(row)
        if required is None:
            invalid += 1
            continue
        symbol, _current_short, settlement_raw = required
        body = parse_body_settlement_date(settlement_raw)
        if body is None or body.strftime("%Y%m%d") != settlement_date:
            body_mismatch += 1
        if normalise_symbol(symbol) in ambiguous_keys:
            ambiguous += 1
            continue
        if resolver(symbol) is None:
            no_match += 1
            continue
        resolved += 1
    return FileCensus(
        settlement_date=settlement_date,
        parsed=parsed,
        invalid=invalid,
        ambiguous=ambiguous,
        no_match=no_match,
        resolved=resolved,
        body_date_mismatch=body_mismatch,
    )


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        resolver = build_preloaded_symbol_resolver(conn)
        ambiguous_keys = getattr(resolver, "ambiguous_keys", frozenset())
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM instruments WHERE is_tradable = TRUE")
            universe = cur.fetchone()[0]  # type: ignore[index]
            cur.execute(_RAW_SQL)
            raws = cur.fetchall()

    print(f"resolver universe (instruments WHERE is_tradable): {universe}")
    print(f"ambiguous normalised keys in that universe:        {len(ambiguous_keys)}")
    print(f"stored FINRA raw payloads:                         {len(raws)}\n")

    header = (
        f"{'settlement':<12}{'parsed':>8}{'invalid':>9}{'ambig':>7}{'no_match':>10}"
        f"{'resolved':>10}{'match%':>9}{'ours%':>8}{'bodybad':>9}"
    )
    print(header)
    print("-" * len(header))

    results: list[FileCensus] = []
    for accession, payload in raws:
        settlement_date = accession.removeprefix("FINRA_SI_")
        c = census_one(payload, settlement_date, resolver, ambiguous_keys)
        results.append(c)
        print(
            f"{c.settlement_date:<12}{c.parsed:>8}{c.invalid:>9}{c.ambiguous:>7}"
            f"{c.no_match:>10}{c.resolved:>10}{100 * c.match_rate:>8.2f}%"
            f"{100 * c.universe_share:>7.2f}%{c.body_date_mismatch:>9}"
        )

    if not results:
        print("\nNO STORED PAYLOADS — census measured nothing.")
        raise SystemExit(1)

    tot_parsed = sum(c.parsed for c in results)
    tot_resolved = sum(c.resolved for c in results)
    rates = sorted(c.match_rate for c in results)
    ours = sorted(c.universe_share for c in results)
    print("-" * len(header))
    print(
        f"{'TOTAL':<12}{tot_parsed:>8}{sum(c.invalid for c in results):>9}"
        f"{sum(c.ambiguous for c in results):>7}{sum(c.no_match for c in results):>10}"
        f"{tot_resolved:>10}{100 * tot_resolved / tot_parsed:>8.2f}%"
        f"{'':>8}{sum(c.body_date_mismatch for c in results):>9}"
    )
    print()
    print(f"match% (resolved/parsed)      min {100 * rates[0]:.2f}  max {100 * rates[-1]:.2f}")
    print(f"ours%  (resolved/resolvable)  min {100 * ours[0]:.2f}  max {100 * ours[-1]:.2f}")
    print(f"files below the old 0.50 floor: {sum(1 for c in results if c.match_rate < 0.50)} of {len(results)}")

    # Consecutive-file retention: resolved[i] / resolved[i-1]. This is the arm
    # whose threshold has to be DERIVED rather than being a boundary at zero,
    # so the derivation lives here and the job's constant cites this output.
    # Files are ordered by settlement date (accession sorts lexicographically
    # on YYYYMMDD), so pair i with i-1 directly.
    print()
    ratios = [
        (results[i].settlement_date, results[i].resolved / results[i - 1].resolved)
        for i in range(1, len(results))
        if results[i - 1].resolved
    ]
    worst = sorted(ratios, key=lambda p: p[1])[:5]
    print(f"consecutive retention resolved[i]/resolved[i-1] over {len(ratios)} pairs:")
    print(f"  min {min(r for _, r in ratios):.5f}   max {max(r for _, r in ratios):.5f}")
    print("  five lowest: " + ", ".join(f"{d}={r:.5f}" for d, r in worst))


if __name__ == "__main__":
    main()
