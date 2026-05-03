"""CIK discovery from SEC's curated ticker→CIK map.

Operator audit 2026-05-03 found 7,281 of 12,379 instruments (59%)
have no SEC CIK row in ``external_identifiers``. Without a CIK
they're invisible to every SEC ingester (13F, 13D/G, Form 4, Form
3, DEF 14A, fundamentals). The pie chart can never populate for
those instruments.

This module's contract: walk every no-CIK instrument, look up the
ticker in SEC's ``company_tickers.json`` (a curated map maintained
by SEC, ~10k entries), write the ``external_identifiers`` row when a
match is found.

Source: ``https://www.sec.gov/files/company_tickers.json``. Updated
roughly daily by SEC. Single fetch is sufficient — a periodic
re-fetch (weekly) catches new IPOs and issuer renames.

Misses (no SEC ticker entry for the instrument's symbol) are
expected for:

  * Foreign issuers without ADRs.
  * Defunct / delisted tickers.
  * Synthetic / duplicate listings (e.g. ``.RTH`` suffixes used as
    operational duplicates of an underlying ticker).
  * Bonds / preferreds / warrants (separate ticker from common
    stock).

Misses are logged but never raise — the discovery sweep is
best-effort and operators triage the long tail manually.

Idempotent: ``ON CONFLICT DO NOTHING`` on the
``external_identifiers`` upsert means re-running won't duplicate
rows or stomp on operator-curated overrides. A re-fetch after a
ticker change would not over-write the prior CIK row — it just
no-ops because the prior row is still on file. Removing the prior
CIK after a ticker reassignment is operator territory (manual
DELETE + audit trail).
"""

from __future__ import annotations

import json
import logging
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import psycopg
import psycopg.rows

from app.config import settings

logger = logging.getLogger(__name__)

_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


@dataclass(frozen=True)
class TickerMapEntry:
    cik_padded: str  # 10-digit zero-padded
    ticker: str  # uppercase
    title: str  # SEC entity name


@dataclass(frozen=True)
class DiscoveryResult:
    instruments_scanned: int
    matches_found: int
    rows_inserted: int
    misses: int


def fetch_ticker_map() -> dict[str, TickerMapEntry]:
    """Fetch SEC's curated ticker→CIK map. Returns dict keyed on
    UPPERCASE ticker so callers can ``.get(symbol.upper())``.

    Raises ``urllib.error.URLError`` on network failure — caller
    decides whether that's transient (retry) or terminal (skip
    this run).
    """
    req = urllib.request.Request(
        _TICKERS_URL,
        headers={"User-Agent": settings.sec_user_agent},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 — fixed SEC URL
        payload = json.load(resp)

    out: dict[str, TickerMapEntry] = {}
    if not isinstance(payload, dict):
        return out
    for entry in payload.values():
        if not isinstance(entry, dict):
            continue
        cik_raw = entry.get("cik_str")
        ticker = entry.get("ticker")
        title = entry.get("title", "")
        if cik_raw is None or not ticker:
            continue
        try:
            cik_int = int(cik_raw)
        except TypeError, ValueError:
            continue
        cik_padded = f"{cik_int:010d}"
        ticker_upper = str(ticker).upper().strip()
        if not ticker_upper:
            continue
        # SEC's map can have multiple entries for the same ticker
        # (rare; share class amendments). Keep the first match —
        # callers can override manually if needed.
        if ticker_upper not in out:
            out[ticker_upper] = TickerMapEntry(
                cik_padded=cik_padded,
                ticker=ticker_upper,
                title=str(title),
            )
    return out


def iter_no_cik_instruments(
    conn: psycopg.Connection[Any],
) -> Iterator[tuple[int, str]]:
    """Yield ``(instrument_id, symbol)`` for every instrument with no
    primary SEC CIK.

    Eager-fetched — the cohort is bounded (~7k rows × ~50 bytes each
    = ~350 KB) and a server-side cursor would close on every
    per-instrument ``conn.commit()`` in the discovery loop. Loading
    once up front avoids cursor-lifetime headaches.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT i.instrument_id, i.symbol
            FROM instruments i
            LEFT JOIN external_identifiers ei
                ON ei.instrument_id = i.instrument_id
               AND ei.provider = 'sec'
               AND ei.identifier_type = 'cik'
               AND ei.is_primary = TRUE
            WHERE ei.identifier_value IS NULL
              AND i.symbol IS NOT NULL
            ORDER BY i.instrument_id
            """,
        )
        rows = cur.fetchall()
    for row in rows:
        yield int(row["instrument_id"]), str(row["symbol"])


def upsert_cik(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    cik_padded: str,
    ticker: str,
) -> bool:
    """Idempotent insert of one ``external_identifiers`` row. Returns
    ``True`` when a new row was inserted, ``False`` on no-op /
    conflict.

    Two unique constraints can fire here:

      1. ``uq_external_identifiers_primary`` partial unique on
         ``(instrument_id, provider, identifier_type) WHERE
         is_primary`` — same instrument already has a primary CIK
         row (with any value). Operator-curated CIK takes precedence
         over the discovery match; we no-op.
      2. ``uq_external_identifiers_provider_value`` on
         ``(provider, identifier_type, identifier_value)`` — same
         CIK already mapped to another instrument. Discovery match
         conflicts with an existing CIK→instrument mapping; we
         no-op (the prior mapping wins).

    Pre-check on (1) keeps the SQL straightforward; ON CONFLICT on
    (2) handles the cross-instrument case at insert time.
    """
    with conn.cursor() as cur:
        # Pre-check: does this instrument already have a primary CIK?
        cur.execute(
            """
            SELECT 1 FROM external_identifiers
            WHERE instrument_id = %s
              AND provider = 'sec'
              AND identifier_type = 'cik'
              AND is_primary = TRUE
            """,
            (instrument_id,),
        )
        if cur.fetchone() is not None:
            return False  # operator-curated row wins

        cur.execute(
            """
            INSERT INTO external_identifiers (
                instrument_id, provider, identifier_type, identifier_value,
                is_primary
            ) VALUES (%s, 'sec', 'cik', %s, TRUE)
            ON CONFLICT (provider, identifier_type, identifier_value) DO NOTHING
            """,
            (instrument_id, cik_padded),
        )
        affected = cur.rowcount or 0
    if affected > 0:
        logger.info(
            "cik_discovery: matched %s -> CIK %s (instrument_id=%s)",
            ticker,
            cik_padded,
            instrument_id,
        )
    return affected > 0


def discover_ciks(
    conn: psycopg.Connection[Any],
    *,
    ticker_map: dict[str, TickerMapEntry] | None = None,
) -> DiscoveryResult:
    """Walk every no-CIK instrument and attempt SEC ticker→CIK
    resolution. Idempotent.

    ``ticker_map`` is injectable for tests; production callers pass
    ``None`` and the function fetches from SEC.
    """
    if ticker_map is None:
        ticker_map = fetch_ticker_map()
    scanned = 0
    matches = 0
    inserts = 0
    misses = 0
    for instrument_id, symbol in iter_no_cik_instruments(conn):
        scanned += 1
        entry = ticker_map.get(symbol.upper())
        if entry is None:
            misses += 1
            continue
        matches += 1
        if upsert_cik(
            conn,
            instrument_id=instrument_id,
            cik_padded=entry.cik_padded,
            ticker=entry.ticker,
        ):
            inserts += 1
        # Commit per-instrument so a downstream failure doesn't
        # discard the entire batch's discoveries.
        conn.commit()
    return DiscoveryResult(
        instruments_scanned=scanned,
        matches_found=matches,
        rows_inserted=inserts,
        misses=misses,
    )
