"""CIK discovery from SEC's curated ticker→CIK maps.

Operator audit 2026-05-03 found 7,281 of 12,379 instruments (59%)
have no SEC CIK row in ``external_identifiers``. Without a CIK
they're invisible to every SEC ingester (13F, 13D/G, Form 4, Form
3, DEF 14A, fundamentals). The pie chart can never populate for
those instruments.

This module's contract: walk every no-CIK instrument, look up the
ticker in SEC's published ticker→CIK maps, write the
``external_identifiers`` row when a match is found.

Sources (SEC-published bulk exports — no inventive crawlers; the
operator audit framing is "data in forms and exports"):

  * ``https://www.sec.gov/files/company_tickers.json`` — ~10k
    entries, common stocks. **CANONICAL** — required for any
    sweep to proceed. If this fetch fails the sweep aborts (a
    healthy partial mapping is preferable to a divergent
    persisted-bad-mapping that the next sweep can't repair).
  * ``https://www.sec.gov/files/company_tickers_exchange.json`` —
    ~10k entries with ``cik / name / ticker / exchange``. ETFs
    and NYSE Arca listings live here that don't appear in the
    bare ``company_tickers.json``. Best-effort supplement; a
    fetch failure here only reduces coverage on subsequent runs.

Merge priority on collision: common-stocks > exchange. First
source wins.

``company_tickers_mf.json`` (~28k mutual-fund share-class rows) is
DELIBERATELY NOT consumed in this module today: SEC publishes one
TRUST CIK across many share-class symbols (LACAX / LIACX / ACRNX
all share CIK 2110), but ``external_identifiers`` enforces a
one-instrument-per-CIK unique constraint, so binding the trust
CIK to a single share class would silently no-op every other
sibling. The fund map needs the canonical-instrument-redirect
mechanism tracked in #819 before it can land cleanly.

Misses (no SEC ticker entry for the instrument's symbol) are
expected for:

  * Foreign issuers without ADRs (covered by a future ADR resolver).
  * Defunct / delisted tickers.
  * Synthetic / duplicate listings (e.g. ``.RTH`` suffixes used as
    operational duplicates of an underlying ticker — handled via
    suffix-stripping fallback).
  * Bonds / preferreds / warrants (separate ticker from common
    stock).

Misses are logged but never raise — the discovery sweep is
best-effort and operators triage the long tail manually.

Idempotent: ``ON CONFLICT DO NOTHING`` on the
``external_identifiers`` upsert means re-running won't duplicate
rows or stomp on operator-curated overrides.
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
_TICKERS_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"


@dataclass(frozen=True)
class TickerMapEntry:
    cik_padded: str  # 10-digit zero-padded
    ticker: str  # uppercase
    title: str  # SEC entity name
    # Which SEC export this entry came from. Useful for operator
    # triage when a ticker resolves to an unexpected entity ("why
    # did MFGAX map to a Vanguard CIK? — because mf.json").
    source: str = "company_tickers"


@dataclass(frozen=True)
class DiscoveryResult:
    instruments_scanned: int
    matches_found: int
    rows_inserted: int
    misses: int


def _fetch_sec_json(url: str) -> Any:
    """Fetch + decode a SEC bulk export. Raises on network /
    decode failure — callers wrap or let it bubble."""
    req = urllib.request.Request(
        url,
        headers={"User-Agent": settings.sec_user_agent},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 — fixed SEC URL
        return json.load(resp)


def _parse_company_tickers(payload: Any) -> Iterator[TickerMapEntry]:
    """Parse ``company_tickers.json``. Shape: dict keyed by
    arbitrary numeric string with each value carrying
    ``{cik_str, ticker, title}``."""
    if not isinstance(payload, dict):
        return
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
        ticker_upper = str(ticker).upper().strip()
        if not ticker_upper:
            continue
        yield TickerMapEntry(
            cik_padded=f"{cik_int:010d}",
            ticker=ticker_upper,
            title=str(title),
            source="company_tickers",
        )


def _parse_fields_data(
    payload: Any,
    *,
    cik_field: str,
    ticker_field: str,
    title_field: str | None,
    source: str,
) -> Iterator[TickerMapEntry]:
    """Parse the ``{fields: [...], data: [[...], ...]}`` shape used
    by ``company_tickers_exchange.json`` and
    ``company_tickers_mf.json``. Generic over which column holds
    the ticker / CIK / title because the two files use different
    column names."""
    if not isinstance(payload, dict):
        return
    fields = payload.get("fields")
    data = payload.get("data")
    if not isinstance(fields, list) or not isinstance(data, list):
        return
    try:
        cik_idx = fields.index(cik_field)
        ticker_idx = fields.index(ticker_field)
    except ValueError:
        return
    title_idx = fields.index(title_field) if title_field and title_field in fields else None
    for row in data:
        if not isinstance(row, list) or len(row) <= max(cik_idx, ticker_idx):
            continue
        cik_raw = row[cik_idx]
        ticker = row[ticker_idx]
        try:
            cik_int = int(cik_raw)
        except TypeError, ValueError:
            continue
        if not ticker:
            continue
        ticker_upper = str(ticker).upper().strip()
        if not ticker_upper:
            continue
        title = ""
        if title_idx is not None and len(row) > title_idx:
            title = str(row[title_idx] or "")
        yield TickerMapEntry(
            cik_padded=f"{cik_int:010d}",
            ticker=ticker_upper,
            title=title,
            source=source,
        )


def _parse_company_tickers_exchange(payload: Any) -> Iterator[TickerMapEntry]:
    """Parse ``company_tickers_exchange.json``. Fields:
    ``cik / name / ticker / exchange``. ETFs and NYSE Arca-listed
    securities live here that don't appear in the bare
    ``company_tickers.json``."""
    yield from _parse_fields_data(
        payload,
        cik_field="cik",
        ticker_field="ticker",
        title_field="name",
        source="company_tickers_exchange",
    )


def fetch_ticker_map() -> dict[str, TickerMapEntry]:
    """Fetch + merge SEC's published ticker→CIK exports into a
    single dict keyed on UPPERCASE ticker.

    Priority on collision: ``company_tickers.json`` >
    ``company_tickers_exchange.json``. The first source seen for a
    given ticker wins; later sources skip the existing key.

    Failure semantics — fail-CLOSED on the canonical source:

      * If ``company_tickers.json`` (canonical) fetch fails, the
        ENTIRE sweep aborts (raises). Rationale: a partial sweep
        with only ``exchange.json`` data could persist a wrong
        CIK for a ticker whose canonical entry exists but is
        currently unfetchable; later healthy sweeps can't repair
        the row because ``upsert_cik`` no-ops on existing primary.
        Better to skip the run than to lock in bad data.
      * If ``company_tickers_exchange.json`` (supplement) fetch
        fails, the sweep proceeds with canonical-only coverage —
        a missed ETF resolution today is recoverable; a wrong
        persisted CIK is not.
    """
    out: dict[str, TickerMapEntry] = {}

    # Canonical — required.
    canonical_payload = _fetch_sec_json(_TICKERS_URL)
    for entry in _parse_company_tickers(canonical_payload):
        if entry.ticker not in out:
            out[entry.ticker] = entry

    # Supplement — best-effort. A failure here only reduces
    # coverage on the current sweep; the next sweep retries.
    try:
        exchange_payload = _fetch_sec_json(_TICKERS_EXCHANGE_URL)
    except Exception:  # noqa: BLE001 — supplement failure is non-fatal
        logger.exception(
            "cik_discovery: failed to fetch %s; proceeding with canonical-only",
            _TICKERS_EXCHANGE_URL,
        )
        return out
    for entry in _parse_company_tickers_exchange(exchange_payload):
        if entry.ticker not in out:
            out[entry.ticker] = entry
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


# eToro-side operational suffixes that wrap the underlying common
# stock ticker (no separate SEC entry — the RTH listing is the same
# issuer as the underlying). Stripping the suffix and re-trying the
# lookup catches ~560 no-CIK instruments on the dev cohort without
# any false positives, because each suffix maps deterministically to
# a single underlying.
#
# Add new suffixes ONLY when:
#   1. The suffix is a deterministic operational duplicate of the
#      underlying (not a separate security like a warrant or pref).
#   2. The match rate is non-trivial against the operator audit
#      cohort.
#
# Warrants (``-W``, ``-WT``) and preferreds (``-PA``, ``-PB``) are
# DELIBERATELY not in this list — they are separate securities with
# their own filings, and folding them onto the common-stock CIK
# would mis-attribute filings on the pie chart.
_OPERATIONAL_SUFFIXES: tuple[str, ...] = (
    ".RTH",  # eToro "regular trading hours" duplicate
)


def _normalise_for_lookup(symbol: str) -> tuple[str, ...]:
    """Return candidate forms of ``symbol`` to try against the SEC
    map, in priority order. The original form always comes first so
    a genuine ``.RTH``-like SEC ticker (none known today, but the
    map is operator-curated and could change) wins over the
    stripped fallback."""
    upper = symbol.upper().strip()
    candidates: list[str] = [upper]
    for suffix in _OPERATIONAL_SUFFIXES:
        if upper.endswith(suffix):
            stripped = upper[: -len(suffix)]
            if stripped and stripped not in candidates:
                candidates.append(stripped)
    return tuple(candidates)


def discover_ciks(
    conn: psycopg.Connection[Any],
    *,
    ticker_map: dict[str, TickerMapEntry] | None = None,
) -> DiscoveryResult:
    """Walk every no-CIK instrument and attempt SEC ticker→CIK
    resolution. Idempotent.

    Two-pass strategy:

      1. **Direct match** — try the original symbol against the SEC
         map. The canonical underlying (e.g. ``AAPL``) gets the CIK
         row.
      2. **Suffix-stripped fallback** — try the suffix-stripped form
         (e.g. ``AAPL.RTH`` → ``AAPL``) only after pass 1 has
         finished. ``upsert_cik``'s ON CONFLICT means the underlying
         mapping (already inserted in pass 1) wins; the
         operational duplicate's row no-ops.

    Pass-ordering matters: a single-pass loop ordered by
    ``instrument_id`` would let an early-sorting ``.RTH`` row claim
    the only SEC CIK row, leaving the underlying unmapped. Two
    passes guarantee the underlying always wins. Codex pre-push
    review caught the single-pass version of this bug.

    ``ticker_map`` is injectable for tests; production callers pass
    ``None`` and the function fetches from SEC.
    """
    if ticker_map is None:
        ticker_map = fetch_ticker_map()

    cohort = list(iter_no_cik_instruments(conn))
    scanned = len(cohort)
    matches = 0
    inserts = 0

    # Pass 1: direct symbol match. Underlying tickers get first crack
    # at any shared CIK row.
    deferred: list[tuple[int, str]] = []
    for instrument_id, symbol in cohort:
        entry = ticker_map.get(symbol.upper().strip())
        if entry is not None:
            matches += 1
            if upsert_cik(
                conn,
                instrument_id=instrument_id,
                cik_padded=entry.cik_padded,
                ticker=entry.ticker,
            ):
                inserts += 1
            conn.commit()
        else:
            deferred.append((instrument_id, symbol))

    # Pass 2: suffix-stripped fallback. Pass-1 inserts already
    # committed, so ON CONFLICT here correctly leaves the underlying
    # as the canonical owner of the CIK.
    misses = 0
    for instrument_id, symbol in deferred:
        entry = None
        for candidate in _normalise_for_lookup(symbol):
            if candidate == symbol.upper().strip():
                continue  # already tried in pass 1
            entry = ticker_map.get(candidate)
            if entry is not None:
                break
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
        conn.commit()

    return DiscoveryResult(
        instruments_scanned=scanned,
        matches_found=matches,
        rows_inserted=inserts,
        misses=misses,
    )
