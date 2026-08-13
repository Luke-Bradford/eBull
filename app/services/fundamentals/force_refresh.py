"""Shared targeted force-refresh core (#677).

Single source of truth for the resolve -> fetch -> normalize sequence,
consumed by BOTH:

  * the CLI ``scripts/force_refresh_fundamentals.py`` (#674 quick-win), and
  * the HTTP endpoint ``app/api/fundamentals_admin.py`` (#677 Part A).

Both bypass the ``plan_refresh`` watermark gate so an operator can
re-fetch SEC companyfacts + re-derive ``financial_periods`` for a
hand-picked cohort without waiting for SEC to ship those CIKs a new
filing. Data treatment is unchanged from the daily path (settled
"Fundamentals provider posture", #532) — only the trigger is new.

Commit discipline (psycopg3 savepoint != commit): the resolver SELECT
opens an implicit read transaction; we ``commit()`` it before any
HTTP-backed work so the ``with conn.transaction()`` blocks inside
``refresh_financial_facts`` / ``normalize_financial_periods`` run as
top-level transactions, not savepoints under one multi-minute outer tx
that would leave the session idle-in-transaction and roll back the whole
cohort on a late failure. Same pattern the per-CIK daily path uses.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import psycopg

from app.config import settings
from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider
from app.services.fundamentals import (
    FactsRefreshSummary,
    NormalizationSummary,
    normalize_financial_periods,
    refresh_financial_facts,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResolvedSymbol:
    symbol: str
    instrument_id: int
    cik: str


@dataclass(frozen=True)
class ForceRefreshResult:
    """Outcome of one force-refresh run.

    ``resolved`` has one entry per matched symbol (request order) — the
    per-symbol response contract. ``fetched`` is ``resolved`` deduped by
    ``instrument_id`` (two tickers can share one instrument) — the work
    actually sent to SEC; counts/exit-codes key off this, not
    ``resolved``.
    """

    resolved: list[ResolvedSymbol]  # one per matched symbol, request order
    fetched: list[ResolvedSymbol]  # resolved deduped by instrument_id
    missing: list[str]  # UPPER-cased symbols with no primary SEC CIK
    facts: FactsRefreshSummary
    periods: NormalizationSummary


def resolve_symbols(
    conn: psycopg.Connection[tuple],
    symbols: list[str],
) -> tuple[list[ResolvedSymbol], list[str]]:
    """Map each symbol to its (instrument_id, primary SEC CIK).

    Returns ``(resolved, missing)``: ``resolved`` contains every symbol
    with a primary SEC CIK, in caller order; ``missing`` is the list of
    UPPER-cased symbols that either don't exist in ``instruments`` or
    have no primary SEC CIK row in ``external_identifiers``. The caller
    surfaces the missing list rather than aborting — partial coverage is
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
        # is_primary_listing) per symbol so a duplicate/retired listing
        # can't shadow the canonical one.
        if sym not in by_symbol:
            by_symbol[sym] = ResolvedSymbol(
                symbol=str(row[0]),
                instrument_id=int(row[1]),
                cik=str(row[2]),
            )

    resolved = [by_symbol[s] for s in upper if s in by_symbol]
    missing = [s for s in upper if s not in by_symbol]
    return resolved, missing


def dedupe_resolved(resolved: list[ResolvedSymbol]) -> list[ResolvedSymbol]:
    """Drop duplicate instrument_ids, keeping first occurrence.

    Operator input is symbol-keyed, but two symbols (or a typo like
    ``IEP IEP``) can map to the same instrument — re-fetching the same
    CIK twice is wasted SEC budget. First-occurrence order is preserved
    so log/response output stays predictable.
    """
    seen: set[int] = set()
    unique: list[ResolvedSymbol] = []
    for r in resolved:
        if r.instrument_id in seen:
            continue
        seen.add(r.instrument_id)
        unique.append(r)
    return unique


def run_force_refresh(
    conn: psycopg.Connection[tuple],
    symbols: list[str],
) -> ForceRefreshResult:
    """Resolve -> fetch companyfacts -> re-normalize for ``symbols``.

    Commits between phases (see module docstring). When no symbol
    resolves, returns zero summaries without touching SEC. Idempotent:
    re-fetching unchanged companyfacts + re-running normalization is a
    no-op for unchanged rows.
    """
    resolved, missing = resolve_symbols(conn, symbols)
    # Close the implicit read tx before any HTTP-backed work below.
    conn.commit()

    fetched = dedupe_resolved(resolved)
    facts = FactsRefreshSummary(symbols_attempted=0, facts_upserted=0, facts_skipped=0, symbols_failed=0)
    periods = NormalizationSummary(instruments_processed=0, periods_raw_upserted=0, periods_canonical_upserted=0)

    if fetched:
        triples = [(r.symbol, r.instrument_id, r.cik) for r in fetched]
        with SecFundamentalsProvider(user_agent=settings.sec_user_agent) as provider:
            facts = refresh_financial_facts(provider, conn, triples)
        # Separate the fetch phase from normalization so the per-
        # instrument transactions inside normalize are top-level.
        conn.commit()
        # Normalize all fetched ids regardless of per-fetch outcome:
        # idempotent — a fetch-failed instrument re-derives from its
        # existing raw store (no corruption, no new data).
        periods = normalize_financial_periods(conn, [r.instrument_id for r in fetched])
        conn.commit()

    return ForceRefreshResult(resolved=resolved, fetched=fetched, missing=missing, facts=facts, periods=periods)
