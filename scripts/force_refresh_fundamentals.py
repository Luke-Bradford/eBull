"""Targeted SEC fundamentals refresh for a hand-picked set of symbols
(#674 follow-up, #677 precursor) — CLI wrapper.

Bypasses the staleness gate in
:func:`app.services.fundamentals.plan_refresh` so the operator can
re-fetch SEC company-facts + re-derive ``financial_periods`` for a
specific cohort without waiting for SEC to ship them a new filing
or for the next universe-wide sweep.

Concrete trigger: PR #676 (#674) extended ``TRACKED_CONCEPTS`` with
LP/LLC partnership-distribution tags. The dividend chart for IEP /
ET / EPD / MPLX / KMI etc. won't fill until each of those CIKs is
re-fetched with the new allowlist applied.

Run from the repo root via the module form so the ``app`` package
imports resolve:

    uv run python -m scripts.force_refresh_fundamentals IEP ET EPD MPLX KMI

Defaults to dry-run (logs the resolved (symbol, CIK) cohort, no
fetches). Pass ``--apply`` to actually call SEC + write to the DB.

The resolve -> fetch -> normalize core lives in
``app.services.fundamentals.force_refresh`` so the CLI and the HTTP
endpoint (#677 Part A, ``POST /admin/fundamentals/refresh``) share one
reviewed implementation. ``resolve_symbols`` / ``ResolvedSymbol`` are
re-exported here for backward compatibility with existing callers/tests.
"""

from __future__ import annotations

import argparse
import logging
import sys

import psycopg

from app.config import settings
from app.services.fundamentals.force_refresh import (  # re-export for back-compat
    ResolvedSymbol,
    dedupe_resolved,
    resolve_symbols,
    run_force_refresh,
)

__all__ = ["ResolvedSymbol", "resolve_symbols", "main"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


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
        if not args.apply:
            resolved, missing = resolve_symbols(conn, args.symbols)
            conn.commit()
            _log_missing(missing, len(args.symbols))
            fetched = dedupe_resolved(resolved)
            if not fetched:
                logger.error("force_refresh: no resolvable symbols; nothing to do")
                return 1
            _log_deduped(resolved, fetched)
            logger.info(
                "force_refresh: %d symbols resolved: %s",
                len(fetched),
                ", ".join(f"{r.symbol}({r.cik})" for r in fetched),
            )
            logger.info("force_refresh: DRY-RUN — pass --apply to actually fetch")
            return 0

        result = run_force_refresh(conn, args.symbols)

    _log_missing(result.missing, len(args.symbols))
    if not result.fetched:
        logger.error("force_refresh: no resolvable symbols; nothing to do")
        return 1

    _log_deduped(result.resolved, result.fetched)
    logger.info(
        "force_refresh: %d symbols resolved: %s",
        len(result.fetched),
        ", ".join(f"{r.symbol}({r.cik})" for r in result.fetched),
    )
    logger.info(
        "force_refresh: facts upserted=%d skipped=%d failed=%d",
        result.facts.facts_upserted,
        result.facts.facts_skipped,
        result.facts.symbols_failed,
    )
    logger.info(
        "force_refresh: normalisation processed=%d raw_upserted=%d canonical_upserted=%d",
        result.periods.instruments_processed,
        result.periods.periods_raw_upserted,
        result.periods.periods_canonical_upserted,
    )

    # Exit codes: distinguish "nothing landed" (1) from "review the log"
    # (2). Resolver-only failure (no resolvable symbols) already returned
    # 1 above. ``symbols_failed`` is counted over the deduped fetch set.
    if result.facts.symbols_failed == len(result.fetched):
        return 1
    if result.facts.symbols_failed > 0:
        return 2
    return 0


def _log_deduped(resolved: list[ResolvedSymbol], fetched: list[ResolvedSymbol]) -> None:
    dropped = len(resolved) - len(fetched)
    if dropped > 0:
        logger.info("force_refresh: deduped %d duplicate input(s)", dropped)


def _log_missing(missing: list[str], requested: int) -> None:
    if missing:
        logger.warning(
            "force_refresh: %d/%d symbols have no primary SEC CIK and will be skipped: %s",
            len(missing),
            requested,
            ", ".join(missing),
        )


if __name__ == "__main__":
    sys.exit(main())
