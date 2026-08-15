"""#2282 stage 2c — build the SEC Form 25 delisting register for a calendar year.

Run from repo root:

    uv run python -m scripts.build_2282_form25_register --year 2023 --harvest
    uv run python -m scripts.build_2282_form25_register --year 2023 --resolve-symbols
    uv run python -m scripts.build_2282_form25_register --year 2023 --emit-fixture
    uv run python -m scripts.build_2282_form25_register --year 2023 --census

Multi-year expansion (#2721) runs strictly sequentially, one year at a time,
gating each year on its harvest failure count before touching the next:

    uv run python -m scripts.build_2282_form25_register --years 2013-2024 \
        --harvest --resolve-symbols --census

⚠ Rate limited against the SHARED SEC budget: SEC's published limit is
10 req/s and the jobs daemon draws from the same allowance. This script does
NOT use the app's Postgres GCRA gate — that gate lives behind the app's
connection pool and this is a one-shot script outside the app lifespan.
Instead ``_Fetcher`` enforces a local 0.2 s floor (5 req/s), a deliberate
PARTITION of the budget: this run takes half and leaves the other half to the
daemon, so the two clocks interleave safely without sharing state. (An
earlier version of this header claimed the GCRA gate was used — it never was;
ckpt-1 on #2721 caught the contradiction with ``_Fetcher``'s own docstring.)

Expected shape for 2023, from #2284's spike and reproduced here:

    2,437 index rows -> 1,282 filings          (trap 1: dual CIK indexing)
      440 debt lifecycle, (a)(1)+(a)(2)         (trap 3: 34.3%, NOT delistings)
      578 delisting-meaning equity filings
      443 distinct issuers
      382 issuers resolved to a ticker (86.2%)  (trap 4: cover-page XBRL)

``--emit-fixture`` writes ``tests/fixtures/form25_2023_cohort.csv``. That file
is not scratch: it is the **vendor acceptance test** for any future price
source, and the spike paid for it twice already by leaving it in a session
scratchpad.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

import httpx
import psycopg

from app.config import settings
from app.services.sec_form25_register import (
    COVER_PAGE_FORMS,
    TRADING_SYMBOL_RE,
    Form25IndexRow,
    classify_provision,
    classify_security,
    clean_trading_symbol,
    parse_index_line,
    parse_submission,
)

logger = logging.getLogger("form25_register")

_ARCHIVES = "https://www.sec.gov/Archives"
_FIXTURE = Path("tests/fixtures/form25_2023_cohort.csv")

#: SEC's published limit is 10 req/s across all data hosts. The jobs daemon
#: shares it, so this run takes half and leaves the other half to the daemon —
#: being blocked would cost far more than the extra minutes.
_MIN_INTERVAL_S = 0.2


class _Fetcher:
    """Sequential SEC fetcher with a floor between requests.

    Deliberately not the Postgres GCRA gate: that gate lives behind the app's
    connection pool and this is a one-shot script running outside the app
    lifespan. The floor here is twice SEC's minimum, so the two clocks
    interleave safely without needing to share state.
    """

    def __init__(self) -> None:
        self._client = httpx.Client(
            headers={
                "User-Agent": settings.sec_user_agent,
                "Accept-Encoding": "gzip, deflate",
            },
            timeout=60.0,
            follow_redirects=True,
        )
        self._next_free = 0.0

    def get(self, url: str) -> tuple[int, str]:
        wait = self._next_free - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        self._next_free = time.monotonic() + _MIN_INTERVAL_S
        response = self._client.get(url)
        return response.status_code, response.text

    def close(self) -> None:
        self._client.close()


def _index_rows(fetcher: _Fetcher, year: int) -> dict[str, Form25IndexRow]:
    """De-duplicated Form 25 filings for a year, keyed on accession.

    Trap 1 in one dict: a 25-NSE is indexed under both the exchange CIK and the
    issuer CIK, so ~90% of accessions arrive twice.
    """
    by_accession: dict[str, Form25IndexRow] = {}
    total_rows = 0
    for quarter in (1, 2, 3, 4):
        url = f"{_ARCHIVES}/edgar/full-index/{year}/QTR{quarter}/form.idx"
        status, body = fetcher.get(url)
        if status != 200:
            raise RuntimeError(f"form.idx fetch failed: status={status} url={url}")
        for line in body.splitlines():
            row = parse_index_line(line)
            if row is None:
                continue
            total_rows += 1
            by_accession.setdefault(row.accession_number, row)
        logger.info("QTR%d: %d Form 25 rows cumulative", quarter, total_rows)
    logger.info("%d index rows -> %d distinct filings", total_rows, len(by_accession))
    return by_accession


def _submission_url(row: Form25IndexRow) -> str:
    return f"{_ARCHIVES}/{row.path}"


_UPSERT = """
INSERT INTO sec_form25_register
    (accession_number, form, filed_date, exchange_cik, exchange_name,
     issuer_cik, issuer_name, file_number, description_class_security,
     rule_provision, provision_class, security_class, signature_date, suspension_date)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (accession_number) DO UPDATE SET
    form                       = EXCLUDED.form,
    filed_date                 = EXCLUDED.filed_date,
    exchange_cik               = EXCLUDED.exchange_cik,
    exchange_name              = EXCLUDED.exchange_name,
    issuer_cik                 = EXCLUDED.issuer_cik,
    issuer_name                = EXCLUDED.issuer_name,
    file_number                = EXCLUDED.file_number,
    description_class_security = EXCLUDED.description_class_security,
    rule_provision             = EXCLUDED.rule_provision,
    provision_class            = EXCLUDED.provision_class,
    security_class             = EXCLUDED.security_class,
    signature_date             = EXCLUDED.signature_date,
    suspension_date            = EXCLUDED.suspension_date,
    ingested_at                = now()
"""


def harvest(conn: psycopg.Connection[tuple], fetcher: _Fetcher, year: int) -> int:
    """Fetch and parse every Form 25 filing of a year into the register.

    One request per filing: the complete ``{accession}.txt`` submission carries
    primary_doc.xml AND every exhibit, so the suspension-date scan is free.
    """
    rows = _index_rows(fetcher, year)
    started = time.time()

    def _fetch_and_store(batch: dict[str, Form25IndexRow]) -> list[Form25IndexRow]:
        failed: list[Form25IndexRow] = []
        for i, row in enumerate(batch.values(), start=1):
            status, body = fetcher.get(_submission_url(row))
            if status != 200:
                logger.warning("%s: status %d", row.accession_number, status)
                failed.append(row)
                continue
            filing = parse_submission(row, body)
            conn.execute(
                _UPSERT,
                (
                    filing.accession_number,
                    filing.form,
                    filing.filed_date,
                    filing.exchange_cik,
                    filing.exchange_name,
                    filing.issuer_cik,
                    filing.issuer_name,
                    filing.file_number,
                    filing.description_class_security,
                    filing.rule_provision,
                    filing.provision_class,
                    filing.security_class,
                    filing.signature_date,
                    filing.suspension_date,
                ),
            )
            if i % 100 == 0:
                conn.commit()
                logger.info("  %d/%d filings (%.0fs)", i, len(batch), time.time() - started)
        conn.commit()
        return failed

    failed = _fetch_and_store(rows)
    if failed:
        # One retry pass after a pause — transient 429/5xx are the common
        # case at this volume. Anything that fails twice is a real failure
        # and the YEAR is incomplete: the caller must gate on the return
        # value, not shrug (ckpt-1 on #2721 — `main` used to discard it, so
        # a partial year read as complete).
        logger.warning("%d filing fetch(es) failed; retrying once in 30s", len(failed))
        time.sleep(30)
        failed = _fetch_and_store({r.accession_number: r for r in failed})
    if failed:
        logger.error(
            "%d filing(s) failed twice — year %d is INCOMPLETE: %s",
            len(failed),
            year,
            ", ".join(r.accession_number for r in failed[:10]),
        )
    return len(failed)


_SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik}.json"


def _cover_page_candidates(columnar: dict[str, list[str]], before: date) -> list[tuple[str, str, str]]:
    """(filing_date, accession, primary_doc) for cover-page forms at/before the delisting."""
    candidates: list[tuple[str, str, str]] = []
    for form, filing_date, accession, primary in zip(
        columnar.get("form", []),
        columnar.get("filingDate", []),
        columnar.get("accessionNumber", []),
        columnar.get("primaryDocument", []),
        strict=False,
    ):
        if form not in COVER_PAGE_FORMS or not primary:
            continue
        if filing_date > before.isoformat():
            continue
        candidates.append((filing_date, accession, primary))
    return candidates


def _resolve_symbol(fetcher: _Fetcher, cik: str, before: date) -> tuple[str, str] | None:
    """Cover-page inline XBRL → ``(symbol, source_accession)``.

    §2.6 trap 4: ``submissions`` JSON drops ``tickers`` to ``[]`` once a company
    delists and ``companyconcept/…/dei/TradingSymbol.json`` 404s (the XBRL
    company APIs serve numeric facts only). So walk the filing history for the
    newest cover-page form filed at or before the delisting and read
    ``dei:TradingSymbol`` off its primary document.

    ``filings.recent`` is capped at ~1000 filings / ≥1 year; older history
    lives in the ``filings.files[]`` pages (sec-edgar.md: "Always check
    ``files`` and recurse"). For old delisting years the miss is BIASED, not
    random: an issuer that failed stops filing (its cover pages stay in
    ``recent`` forever), while an ``(a)(3)`` survivor keeps filing and pushes
    its pre-delisting cover pages out — resolution would skew along exactly
    the failure-vs-acquisition axis the census z-tests (Codex ckpt-2, #2721).
    """
    status, body = fetcher.get(_SUBMISSIONS.format(cik=cik))
    if status != 200:
        return None
    filings = json.loads(body).get("filings", {})

    tried: set[str] = set()

    def _scan(candidates: list[tuple[str, str, str]]) -> tuple[str, str] | None:
        """Try the newest five UNTRIED candidates; None if all fail."""
        untried = [c for c in sorted(candidates, reverse=True) if c[1] not in tried]
        for _, accession, primary in untried[:5]:
            tried.add(accession)
            doc_status, document = fetcher.get(
                f"{_ARCHIVES}/edgar/data/{int(cik)}/{accession.replace('-', '')}/{primary}"
            )
            if doc_status != 200:
                continue
            match = TRADING_SYMBOL_RE.search(document)
            if match:
                symbol = clean_trading_symbol(match.group(1))
                if symbol:
                    return symbol, accession
        return None

    # ``recent`` holds the NEWEST filings, so any candidate it yields outranks
    # everything in the (strictly older) ``files[]`` pages for the
    # newest-at-or-before-the-delisting objective. Scan those first; the
    # pages are fetched only if that scan RESOLVES NOTHING — whether because
    # ``recent`` had no candidate at or before the cutoff, or because every
    # attempted one failed to fetch/parse (a pre-loop count cannot see the
    # second case — review round 3).
    candidates = _cover_page_candidates(filings.get("recent", {}), before)
    resolved = _scan(candidates)
    if resolved is not None:
        return resolved

    for page in filings.get("files", []):
        name = page.get("name")
        if not name:
            continue
        # Pages carry their span; skip any that BEGINS after the delisting.
        # A page missing ``filingFrom`` is unknown-but-possibly-relevant and
        # is fetched, not skipped.
        if page.get("filingFrom", "") > before.isoformat():
            continue
        page_status, page_body = fetcher.get(f"https://data.sec.gov/submissions/{name}")
        if page_status != 200:
            continue
        candidates.extend(_cover_page_candidates(json.loads(page_body), before))
    return _scan(candidates)


def resolve_symbols(conn: psycopg.Connection[tuple], fetcher: _Fetcher, year: int) -> int:
    """Resolve a ticker for every delisting-meaning issuer of the year.

    Per ISSUER, not per filing — an issuer with two delisting filings gets one
    lookup and both rows get the answer.
    """
    issuers = conn.execute(
        """
        SELECT issuer_cik, min(filed_date)
        FROM sec_form25_common_equity_delistings
        WHERE issuer_cik IS NOT NULL
          AND resolved_symbol IS NULL
          AND extract(year FROM filed_date) = %s
        GROUP BY issuer_cik
        ORDER BY issuer_cik
        """,
        (year,),
    ).fetchall()
    logger.info("resolving tickers for %d issuers", len(issuers))

    resolved = 0
    for i, (cik, first_filed) in enumerate(issuers, start=1):
        found = _resolve_symbol(fetcher, cik, first_filed)
        if found is None:
            continue
        symbol, source_accession = found
        conn.execute(
            """
            UPDATE sec_form25_register
               SET resolved_symbol = %s,
                   symbol_source = 'cover_page_xbrl',
                   symbol_source_accession = %s
             WHERE issuer_cik = %s
               AND provision_class = 'equity_delisting'
               AND security_class = 'common_equity'
               AND extract(year FROM filed_date) = %s
            """,
            (symbol, source_accession, cik, year),
        )
        resolved += 1
        if i % 50 == 0:
            conn.commit()
            logger.info("  %d/%d issuers, %d resolved", i, len(issuers), resolved)
    conn.commit()
    logger.info("resolved %d/%d issuers (%.1f%%)", resolved, len(issuers), 100 * resolved / max(1, len(issuers)))
    return resolved


def reclassify(conn: psycopg.Connection[tuple], year: int) -> int:
    """Recompute both class axes from already-stored columns. No network.

    A classification rule changing is not a reason to re-fetch 1,282 filings
    from SEC — the raw `form`, `rule_provision` and
    `description_class_security` are stored precisely so the derived classes
    can be re-derived. Keeps the shared rate budget for work that actually
    needs it, and makes a rule change cheap enough to iterate on.
    """
    rows = conn.execute(
        """
        SELECT accession_number, form, rule_provision, description_class_security,
               provision_class, security_class
        FROM sec_form25_register
        WHERE extract(year FROM filed_date) = %s
        """,
        (year,),
    ).fetchall()

    # Stored symbols are re-cleaned in the same pass: a filer's footnote marker
    # inside dei:TradingSymbol (IVC*, NHIQ*) is a derived-value defect, and
    # re-fetching 315 issuers to fix a string is the wrong trade.
    symbol_fixed = 0
    for accession, symbol in conn.execute(
        "SELECT accession_number, resolved_symbol FROM sec_form25_register "
        "WHERE resolved_symbol IS NOT NULL AND extract(year FROM filed_date) = %s",
        (year,),
    ).fetchall():
        cleaned = clean_trading_symbol(symbol)
        if cleaned and cleaned != symbol:
            conn.execute(
                "UPDATE sec_form25_register SET resolved_symbol = %s WHERE accession_number = %s",
                (cleaned, accession),
            )
            symbol_fixed += 1
    if symbol_fixed:
        logger.info("cleaned %d trading symbols", symbol_fixed)

    changed = 0
    for accession, form, provision, description, old_provision, old_security in rows:
        provision_class = classify_provision(form, provision)
        security_class = classify_security(description)
        if provision_class == old_provision and security_class == old_security:
            continue
        conn.execute(
            "UPDATE sec_form25_register SET provision_class = %s, security_class = %s WHERE accession_number = %s",
            (provision_class, security_class, accession),
        )
        changed += 1
    conn.commit()
    logger.info("reclassified %d of %d filings", changed, len(rows))
    return changed


def emit_fixture(conn: psycopg.Connection[tuple], year: int, path: Path) -> None:
    """Write the resolved cohort as a committed test fixture.

    ⚠ This is the vendor acceptance test, not scratch output. Any candidate
    price source claiming survivorship-bias-free coverage gets run against it
    BEFORE purchase: is the name served at all, does the series terminate at
    the delisting, and is it the right company rather than a later occupant of
    the ticker. The #2284 spike built this cohort, left it in a session
    scratchpad and lost it — committing it is what stops it being paid for a
    third time.
    """
    rows = conn.execute(
        """
        SELECT resolved_symbol, issuer_cik, issuer_name, rule_provision,
               min(filed_date) AS first_filed,
               max(suspension_date) AS suspension_date,
               min(description_class_security) AS security
        FROM sec_form25_common_equity_delistings
        WHERE resolved_symbol IS NOT NULL
          AND extract(year FROM filed_date) = %s
        -- NOT grouped by form: a 25-NSE/A amends the 25-NSE for the SAME
        -- delisting, so including form emits the name twice and inflates the
        -- acceptance-test denominator with an amendment.
        GROUP BY resolved_symbol, issuer_cik, issuer_name, rule_provision
        ORDER BY resolved_symbol
        """,
        (year,),
    ).fetchall()

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "symbol",
                "cik",
                "issuer_name",
                "rule_provision",
                "first_filed",
                "suspension_date",
                "security",
            ]
        )
        writer.writerows(rows)
    logger.info("wrote %d cohort rows to %s", len(rows), path)


def census(conn: psycopg.Connection[tuple], year: int) -> None:
    print(f"\n=== Form 25 register census, {year} ===")
    total = conn.execute(
        "SELECT count(*), count(DISTINCT issuer_cik) FROM sec_form25_register WHERE extract(year FROM filed_date) = %s",
        (year,),
    ).fetchone()
    assert total is not None
    print(f"  filings: {total[0]:,}   distinct issuers: {total[1]:,}")
    print(f"  {'class/security':<34} {'provision':<10} {'filings':>8} {'issuers':>8} {'w/sym':>7} {'w/susp':>7}")
    for row in conn.execute(
        """
        SELECT provision_class || '/' || security_class,
               coalesce(rule_provision, '(c) issuer'),
               filings, issuers, filings_with_symbol, filings_with_suspension_date
        FROM sec_form25_register_census
        WHERE filed_year = make_date(%s, 1, 1)
        ORDER BY provision_class, security_class, 2
        """,
        (year,),
    ).fetchall():
        print(f"  {row[0]:<34} {row[1]:<10} {row[2]:>8,} {row[3]:>8,} {row[4]:>7,} {row[5]:>7,}")

    delisting = conn.execute(
        """
        SELECT count(*), count(DISTINCT issuer_cik),
               count(DISTINCT issuer_cik) FILTER (WHERE resolved_symbol IS NOT NULL)
        FROM sec_form25_common_equity_delistings
        WHERE extract(year FROM filed_date) = %s
        """,
        (year,),
    ).fetchone()
    assert delisting is not None
    filings, issuers, resolved = delisting
    print(
        f"\n  COHORT (equity_delisting AND common_equity): {filings:,} filings / {issuers:,} issuers / "
        f"{resolved:,} resolved to a ticker "
        f"({100 * resolved / max(1, issuers):.1f}%)"
    )
    bias = conn.execute(
        """
        SELECT resolved_symbol IS NOT NULL AS resolved,
               count(DISTINCT issuer_cik) FILTER (WHERE rule_provision = '(a)(3)') AS acquisitions,
               count(DISTINCT issuer_cik) FILTER (WHERE rule_provision = '(b)')    AS failures
        FROM sec_form25_common_equity_delistings
        WHERE extract(year FROM filed_date) = %s
        GROUP BY 1 ORDER BY 1
        """,
        (year,),
    ).fetchall()
    print("\n  Resolution bias on the failure-vs-acquisition axis — the axis")
    print("  survivorship actually turns on:")
    for resolved, acquisitions, failures in bias:
        total = acquisitions + failures
        label = "resolved  " if resolved else "unresolved"
        print(
            f"    {label}: (a)(3) acquisitions {acquisitions:>3} / (b) failures "
            f"{failures:>3}  = {100 * acquisitions / max(1, total):.0f}pct acquisitions"
        )
    # ⚠ Keyed on the boolean, NEVER indexed positionally. A GROUP BY emits a
    # row only where one exists, so a year with (say) nothing unresolved
    # returns ONE row and `bias[1]` is an IndexError — the census crashes on
    # exactly the clean-data case it is least expected to.
    by_resolved = {bool(row[0]): (row[1], row[2]) for row in bias}
    ur_a, ur_b = by_resolved.get(False, (0, 0))
    r_a, r_b = by_resolved.get(True, (0, 0))
    if not (ur_a + ur_b + r_a + r_b):
        print(f"    no delisting-meaning common-equity filings for {year}")
        return
    p_unresolved = ur_a / max(1, ur_a + ur_b)
    p_resolved = r_a / max(1, r_a + r_b)
    n_unresolved, n_resolved = ur_a + ur_b, r_a + r_b
    standard_error = (
        p_resolved * (1 - p_resolved) / max(1, n_resolved) + p_unresolved * (1 - p_unresolved) / max(1, n_unresolved)
    ) ** 0.5
    z = abs(p_unresolved - p_resolved) / standard_error if standard_error else 0.0
    print(f"    skew {100 * (p_unresolved - p_resolved):+.1f} points, n={n_unresolved} unresolved, z={z:.2f}")
    print(
        "  ⚠ Computed, never asserted. Do NOT restate an earlier write-up's\n"
        "    'unbiased on the failure-vs-acquisition axis' — that was measured on\n"
        "    the pre-security-filter denominator. Read the z above: below ~1.96 it\n"
        "    is NEITHER established as biased NOR demonstrated unbiased, and the\n"
        "    honest statement is that the cohort cannot rule the skew out. This is\n"
        "    the axis survivorship turns on, so state the uncertainty.\n"
        "    Resolution drops closed-end funds (they file N-CSR, not a\n"
        "    cover-page-XBRL 10-K) and some foreign private issuers."
    )


def parse_years(spec: str) -> list[int]:
    """``2013-2024`` -> [2013, ..., 2024]. Pure so the CLI contract is testable."""
    start_s, _, end_s = spec.partition("-")
    start, end = int(start_s), int(end_s or start_s)
    if end < start:
        raise ValueError(f"years range is backwards: {spec}")
    return list(range(start, end + 1))


#: The year whose cohort the committed acceptance fixture was frozen from.
#: `--emit-fixture` for any other year against the DEFAULT path would silently
#: overwrite the vendor acceptance test (ckpt-1 on #2721) — a different year's
#: cohort is a different test, and it gets an explicit `--fixture-path`.
_FIXTURE_YEAR = 2023


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    # Default applied after parsing (not here) so that passing BOTH --year and
    # --years is detectable as the contradiction it is.
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        help="Inclusive range, e.g. 2013-2024. Runs the selected actions per "
        "year, strictly sequentially, aborting if a year's harvest is "
        "incomplete after retry. Excludes --emit-fixture.",
    )
    parser.add_argument("--harvest", action="store_true")
    parser.add_argument("--reclassify", action="store_true")
    parser.add_argument("--resolve-symbols", action="store_true")
    parser.add_argument("--emit-fixture", action="store_true")
    parser.add_argument("--census", action="store_true")
    parser.add_argument("--fixture-path", type=Path, default=_FIXTURE)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if not any((args.harvest, args.reclassify, args.resolve_symbols, args.emit_fixture, args.census)):
        parser.error("pick at least one action")

    if args.year is not None and args.years:
        parser.error("--year and --years contradict; pass one")
    if args.years:
        try:
            years = parse_years(args.years)
        except ValueError as exc:
            parser.error(f"--years: {exc}")
    else:
        years = [args.year if args.year is not None else _FIXTURE_YEAR]
    if args.emit_fixture:
        if args.years:
            parser.error("--emit-fixture is single-year; it is an acceptance fixture, not a log")
        if years != [_FIXTURE_YEAR] and args.fixture_path == _FIXTURE:
            parser.error(
                f"--emit-fixture --year {years[0]} would overwrite the frozen "
                f"{_FIXTURE_YEAR} acceptance fixture at {_FIXTURE}; pass an "
                "explicit --fixture-path"
            )

    fetcher = _Fetcher()
    try:
        with psycopg.connect(settings.database_url) as conn:
            for year in years:
                if args.harvest:
                    failures = harvest(conn, fetcher, year)
                    if failures:
                        # The year is incomplete; its census would understate
                        # and its resolution would run against a partial
                        # cohort. Stop rather than continue on bad data.
                        logger.error("aborting at year %d: %d unfetched filings", year, failures)
                        return 1
                if args.reclassify:
                    reclassify(conn, year)
                if args.resolve_symbols:
                    resolve_symbols(conn, fetcher, year)
                if args.emit_fixture:
                    emit_fixture(conn, year, args.fixture_path)
                if args.census:
                    census(conn, year)
    finally:
        fetcher.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
