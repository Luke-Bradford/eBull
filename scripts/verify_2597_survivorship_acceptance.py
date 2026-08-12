"""#2597 — the survivorship acceptance test, run over the full Form 25 equity population.

Run from repo root::

    uv run python -m scripts.verify_2597_survivorship_acceptance
    uv run python -m scripts.verify_2597_survivorship_acceptance --vendor 'icyDenev/Intrader'

WHAT THIS MEASURES, AND WHY IT IS NOT "IS THE SYMBOL PRESENT"
-------------------------------------------------------------
``.claude/skills/data-sources/research-price-corpus.md`` states the test as
three questions, and only the first is about presence:

1. is the name served at all?
2. does the series terminate at the delisting?
3. is it the right company?

…and its own correction says question 2 must be **stratified by provision**,
because applying it flat produces a false negative on the most common one:

* ``(b)`` — exchange-initiated delisting for non-compliance. The ticker died,
  so the series SHOULD terminate. This is the arm that decides the test.
* ``(a)(3)`` — merger, holdco reorganisation, redomiciliation, i.e. "now
  evidence OTHER securities by operation of law". The same economic entity
  commonly keeps trading under the same ticker, so a CONTINUOUS series is the
  correct answer and continuation is not evidence of a defect.

So this reports both arms and passes judgement only on ``(b)``.

THE POPULATION
--------------
``sec_form25_common_equity_delistings`` — the FULL population of the class
under test, never the 259-name cohort fixture and never the raw per-security
register. #2597 asked for "the full register, not the 259-name cohort"; the
raw register is the wrong correction, because a Form 25 is filed per SECURITY
and its bond, note and warrant rows belong to issuers whose common stock is
very much alive. Both denominators are printed so the gap between them stays
visible rather than becoming an unexplained absence.

CONTROL ARM
-----------
Every figure is printed against the archive already in the corpus
(``paperswithbacktest/Stocks-Daily-Price``), read from the SAME stored columns
rather than reasoned about — a control that is described instead of measured is
the failure ``.claude/skills/engineering/full-population-ab.md`` exists to stop.

⚠ Bucketing uses NO invented threshold. A series either ends on or before the
delisting anchor or it does not; the anchor is the stated suspension date where
the filing gives one and the earliest filing date where it does not, which is
the conservative bound (a Form 25 carries three distinct dates and the filing
is the latest of them — sec-edgar.md §2.6 trap 5).
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date

import psycopg

from app.config import settings
from app.services.research_corpus_ingest import (
    HF_ARCHIVE,
    INTRADER_ARCHIVE,
    resolve_archive_symbol,
)

#: The provision that decides the test. Everything else is reported, not judged.
DECIDING_PROVISION = "(b)"


@dataclass
class ProvisionResult:
    symbols: int = 0
    served: int = 0
    terminates: int = 0
    spans: int = 0
    spans_to_capture: int = 0
    starts_after_event: int = 0
    unserved: list[str] = field(default_factory=list)


def _capture_date(conn: psycopg.Connection[tuple], vendor: str) -> date | None:
    """The archive's freeze date, MEASURED as its own last bar.

    Not stored as a column: it is exactly ``max(last_bar)`` over the vendor, so
    a column would be derived state that can drift from the bars — the trap
    sql/249's header already names for the census columns. It is the bound that
    matters most when reading a residual, because a name delisted after the
    freeze cannot be served by this archive at any coverage level.
    """
    row = conn.execute(
        "SELECT max(last_bar) FROM research_price_series WHERE vendor = %s",
        (vendor,),
    ).fetchone()
    return None if row is None else row[0]


def measure(conn: psycopg.Connection[tuple], vendor: str) -> tuple[dict[str, ProvisionResult], date | None]:
    """Full register, one row per provision. Never a cohort, never a sample."""
    series = {
        str(symbol): (first_bar, last_bar)
        for symbol, first_bar, last_bar in conn.execute(
            "SELECT vendor_symbol, first_bar, last_bar FROM research_price_series "
            "WHERE vendor = %s AND bar_count IS NOT NULL",
            (vendor,),
        ).fetchall()
    }
    available = set(series)
    capture = _capture_date(conn, vendor)

    # ⚠ The COMMON-EQUITY view, not the raw register — and this is the full
    # population of the class under test, not a sample of it. A Form 25 is
    # filed per SECURITY, so the raw register carries bond, note, warrant and
    # preferred removals whose issuer's common stock legitimately keeps
    # trading; Berkshire filed two in 2023 and both were notes (sec-edgar.md
    # §2.6 traps 2, 3 and 6). Counting those would land them in `spans` and
    # depress the deciding `(b)` rate with rows that were never about equity.
    #
    # Measured delta on this corpus: the two populations differ by exactly ONE
    # resolvable symbol (SPKY), because a non-common-equity Form 25 carries no
    # cover-page `dei:TradingSymbol` and so has nothing to join on in the first
    # place. `report` prints both denominators rather than only the one used,
    # so that gap stays visible instead of becoming an unexplained absence.
    filings = conn.execute(
        """
        SELECT resolved_symbol,
               min(filed_date)                AS earliest_filed,
               count(DISTINCT rule_provision)  AS provision_variants,
               min(rule_provision)             AS provision,
               min(suspension_date)            AS suspension_date
          FROM sec_form25_common_equity_delistings
         WHERE resolved_symbol IS NOT NULL
         GROUP BY resolved_symbol
         ORDER BY resolved_symbol
        """
    ).fetchall()

    out: dict[str, ProvisionResult] = defaultdict(ProvisionResult)
    for symbol, earliest_filed, provision_variants, provision, suspension_date in filings:
        if provision_variants > 1:
            key = "conflicting"
        else:
            key = provision or "(c)/absent"
        result = out[key]
        result.symbols += 1

        matched = resolve_archive_symbol(str(symbol), available)
        if matched is None:
            result.unserved.append(str(symbol))
            continue
        result.served += 1

        first_bar, last_bar = series[matched]
        anchor = suspension_date or earliest_filed
        if last_bar is not None:
            if last_bar <= anchor:
                result.terminates += 1
            else:
                result.spans += 1
                if capture is not None and last_bar >= capture:
                    result.spans_to_capture += 1
        if first_bar is not None and first_bar > anchor:
            result.starts_after_event += 1

    return dict(out), capture


def report(conn: psycopg.Connection[tuple], vendor: str) -> ProvisionResult | None:
    results, capture = measure(conn, vendor)
    total_rows, resolved_symbols = conn.execute(
        "SELECT count(*), count(DISTINCT resolved_symbol) FROM sec_form25_register"
    ).fetchone() or (0, 0)
    equity_rows, equity_symbols = conn.execute(
        "SELECT count(*), count(DISTINCT resolved_symbol) FROM sec_form25_common_equity_delistings"
    ).fetchone() or (0, 0)
    series_count, bar_total = conn.execute(
        "SELECT count(*), coalesce(sum(bar_count), 0) FROM research_price_series WHERE vendor = %s",
        (vendor,),
    ).fetchone() or (0, 0)

    print(f"\n=== {vendor} ===")
    print(f"  series loaded         : {series_count:,}  ({bar_total:,} bars)")
    print(f"  archive capture date  : {capture}   <- measured, = max(last_bar)")
    print(f"  register (all classes): {total_rows:,} filings, {resolved_symbols:,} distinct resolvable symbols")
    print(f"  common equity (tested): {equity_rows:,} filings, {equity_symbols:,} distinct resolvable symbols")
    header = f"  {'provision':<14}{'syms':>6}{'served':>8}{'term':>7}{'spans':>7}{'to_capture':>12}{'start>evt':>11}"
    print(header)
    for key in sorted(results):
        r = results[key]
        print(
            f"  {key:<14}{r.symbols:>6}{r.served:>8}{r.terminates:>7}{r.spans:>7}"
            f"{r.spans_to_capture:>12}{r.starts_after_event:>11}"
        )
    for key in sorted(results):
        if results[key].unserved:
            print(f"  residual {key}: {sorted(results[key].unserved)}")
    return results.get(DECIDING_PROVISION)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vendor", default=INTRADER_ARCHIVE.vendor, help="archive under test")
    parser.add_argument("--control", default=HF_ARCHIVE.vendor, help="archive to compare against")
    args = parser.parse_args(argv)

    with psycopg.connect(settings.database_url) as conn:
        treatment = report(conn, args.vendor)
        control = report(conn, args.control)

    print(f"\n=== acceptance on {DECIDING_PROVISION} (exchange-initiated failure) ===")
    for label, result in (("control ", control), ("treatment", treatment)):
        if result is None or result.served == 0:
            print(f"  {label}: no {DECIDING_PROVISION} filing is served at all")
            continue
        print(
            f"  {label}: {result.terminates}/{result.served} served series terminate at or before "
            f"the delisting ({result.terminates / result.served:.1%})"
        )
    print(
        "\n⚠ This does NOT close `universe_basis_not_survivorship_free`. Form 25 is US-only,\n"
        "  the register loaded here is 2023-only, the archive is keyed on the live ticker and\n"
        "  cannot separate X from X-DELISTED, and coverage past the capture date is zero by\n"
        "  construction. Point-in-time membership and corporate-action consideration remain\n"
        "  separate blockers this ingest EXPOSES rather than fixes."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
