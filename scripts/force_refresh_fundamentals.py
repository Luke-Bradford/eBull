"""Targeted SEC fundamentals refresh for a hand-picked set of symbols
(#674 follow-up, #677 precursor).

Bypasses the staleness gate in
:func:`app.services.fundamentals.plan_refresh` so the operator can
re-fetch SEC company-facts + re-derive ``financial_periods`` for a
specific cohort without waiting for SEC to ship them a new filing
or for the next universe-wide sweep.

Concrete trigger: PR #676 (#674) extended ``TRACKED_CONCEPTS`` with
LP/LLC partnership-distribution tags. The dividend chart for IEP /
ET / EPD / MPLX / KMI etc. won't fill until each of those CIKs is
re-fetched with the new allowlist applied. ``plan_refresh``'s
master-index selector only schedules CIKs SEC has filed something
new for in the last 7 days — schema changes don't qualify, so
without this script those instruments wait days/weeks.

Run from the repo root via the module form so the ``app`` package
imports resolve:

    uv run python -m scripts.force_refresh_fundamentals IEP ET EPD MPLX KMI

(The bare ``uv run python scripts/foo.py`` form does NOT add the
repo root to ``sys.path`` on this layout — every script under
``scripts/`` has to be invoked as a module.)

Defaults to dry-run (logs the resolved (symbol, CIK) cohort, no
fetches). Pass ``--apply`` to actually call SEC + write to the DB.
The script is idempotent: re-fetching the same companyfacts payload
and re-running normalisation is a no-op for unchanged rows
(``upsert_facts_for_instrument`` ON CONFLICT DO UPDATE WHERE
``IS DISTINCT FROM``; ``_canonical_merge_instrument`` likewise).

Rate limit: SEC public-key tier is 10 req/sec. The provider's
internal throttle handles spacing; this script does not need to
manage rate limits itself.

Out of scope (deliberate): no new HTTP API surface, no UI, no
expected-filings poller. Those live in #677 — this is the
operational quick-win that unblocks the dividend chart for the
known-affected MLP cohort while #677 is designed.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass

import psycopg

from app.config import settings
from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider
from app.services.fundamentals import (
    normalize_financial_periods,
    refresh_financial_facts,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResolvedSymbol:
    symbol: str
    instrument_id: int
    cik: str


def resolve_symbols(
    conn: psycopg.Connection[tuple],
    symbols: list[str],
) -> tuple[list[ResolvedSymbol], list[str]]:
    """Map each symbol to its (instrument_id, primary SEC CIK).

    Returns ``(resolved, missing)``: ``resolved`` contains every symbol
    with a primary SEC CIK, in caller order; ``missing`` is the list of
    symbols that either don't exist in ``instruments`` or have no
    primary SEC CIK row in ``external_identifiers``. The caller logs
    the missing list rather than aborting — partial coverage is
    expected (operator may include a non-US symbol by mistake).
    """
    if not symbols:
        return [], []
    upper = [s.upper() for s in symbols]
    rows = conn.execute(
        """
        SELECT i.symbol, i.instrument_id, ei.identifier_value
        FROM instruments i
        JOIN external_identifiers ei
          ON ei.instrument_id = i.instrument_id
         AND ei.provider = 'sec'
         AND ei.identifier_type = 'cik'
         AND ei.is_primary = TRUE
        WHERE UPPER(i.symbol) = ANY(%(symbols)s)
        ORDER BY i.is_primary_listing DESC NULLS LAST, i.instrument_id ASC
        """,
        {"symbols": upper},
    ).fetchall()

    by_symbol: dict[str, ResolvedSymbol] = {}
    for row in rows:
        sym = str(row[0]).upper()
        # Caller may pass duplicates — keep the first match (highest
        # is_primary_listing) per symbol.
        if sym not in by_symbol:
            by_symbol[sym] = ResolvedSymbol(
                symbol=str(row[0]),
                instrument_id=int(row[1]),
                cik=str(row[2]),
            )

    resolved = [by_symbol[s] for s in upper if s in by_symbol]
    missing = [s for s in upper if s not in by_symbol]
    return resolved, missing


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "symbols",
        nargs="+",
        help="Stock symbols to force-refresh (e.g. IEP ET EPD MPLX KMI).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually call SEC + write to the DB. Default is dry-run.",
    )
    args = parser.parse_args(argv)

    with psycopg.connect(settings.database_url) as conn:
        resolved, missing = resolve_symbols(conn, args.symbols)
        # Close the implicit read transaction opened by the resolver
        # SELECT before any HTTP-backed work below. Without this, the
        # ``with conn.transaction()`` blocks inside
        # ``refresh_financial_facts`` / ``normalize_financial_periods``
        # would degrade to savepoints under one giant outer
        # transaction, leaving the session idle-in-transaction
        # across multi-minute SEC fetches and rolling back the
        # whole cohort on a late failure. Same pattern the per-CIK
        # path in app/services/fundamentals.py uses.
        conn.commit()

        if missing:
            logger.warning(
                "force_refresh: %d/%d symbols have no primary SEC CIK and will be skipped: %s",
                len(missing),
                len(args.symbols),
                ", ".join(missing),
            )

        if not resolved:
            logger.error("force_refresh: no resolvable symbols; nothing to do")
            return 1

        # Dedupe by instrument_id before the expensive work — the
        # operator's CLI is positional, so a typo like
        # ``IEP IEP MPLX`` shouldn't triple-fetch IEP. Caller-order
        # of first occurrence is preserved so log output stays
        # predictable.
        seen_ids: set[int] = set()
        unique: list[ResolvedSymbol] = []
        for r in resolved:
            if r.instrument_id in seen_ids:
                continue
            seen_ids.add(r.instrument_id)
            unique.append(r)
        if len(unique) < len(resolved):
            logger.info(
                "force_refresh: deduped %d duplicate input(s)",
                len(resolved) - len(unique),
            )

        logger.info(
            "force_refresh: %d symbols resolved: %s",
            len(unique),
            ", ".join(f"{r.symbol}({r.cik})" for r in unique),
        )

        if not args.apply:
            logger.info("force_refresh: DRY-RUN — pass --apply to actually fetch")
            return 0

        # Re-fetch companyfacts for every resolved CIK and write to
        # ``financial_facts_raw``. ``refresh_financial_facts`` owns the
        # ingestion-run ledger + per-symbol error isolation, so a
        # single SEC 5xx for one CIK doesn't abort the rest of the
        # cohort.
        instrument_ids = [r.instrument_id for r in unique]
        symbols_for_refresh: list[tuple[str, int, str]] = [(r.symbol, r.instrument_id, r.cik) for r in unique]
        with SecFundamentalsProvider(user_agent=settings.sec_user_agent) as provider:
            facts_summary = refresh_financial_facts(provider, conn, symbols_for_refresh)
        # `refresh_financial_facts` opens an `ingestion_runs` row at
        # entry and closes it at exit; the per-CIK `with
        # conn.transaction()` blocks inside its loop work correctly,
        # but the surrounding ledger writes can leave the session in
        # a transaction once the function returns. Explicit commit
        # here separates the fetch phase from the normalisation
        # phase so the per-instrument transactions inside
        # `normalize_financial_periods` are top-level (not savepoints
        # under a multi-minute outer tx). Codex review #2 finding.
        conn.commit()
        logger.info(
            "force_refresh: facts upserted=%d skipped=%d failed=%d",
            facts_summary.facts_upserted,
            facts_summary.facts_skipped,
            facts_summary.symbols_failed,
        )

        # Re-derive ``financial_periods`` from the now-current raw
        # store. Scoped to the resolved cohort so unrelated rows
        # don't get touched.
        norm_summary = normalize_financial_periods(conn, instrument_ids=instrument_ids)
        conn.commit()
        logger.info(
            "force_refresh: normalisation processed=%d raw_upserted=%d canonical_upserted=%d",
            norm_summary.instruments_processed,
            norm_summary.periods_raw_upserted,
            norm_summary.periods_canonical_upserted,
        )

    # Surface any per-symbol SEC failure as a non-zero exit so an
    # automation wrapper / shell pipeline can detect it. A partial
    # failure (some succeeded, some failed) returns 2 so the caller
    # can distinguish "nothing landed" (1) from "review the log"
    # (2). Resolver-only failures (no resolvable symbols) already
    # returned 1 earlier.
    if facts_summary.symbols_failed == len(unique):
        return 1
    if facts_summary.symbols_failed > 0:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
