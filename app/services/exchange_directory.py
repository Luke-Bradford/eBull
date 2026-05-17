"""Bundled ``company_tickers_exchange.json`` ingest (G8, Phase 2 PR 4
of the US-ETL completion plan).

Fetches the SEC ticker-with-exchange directory + populates a single
snapshot table:

* ``cik_refresh_exchange_directory`` — keyed by ``(cik, ticker)``, one
  row per (CIK, ticker) pair the payload emits. Ticker-grain because
  a single CIK can produce multiple rows for share-class siblings
  (GOOG / GOOGL), preferred-series tickers (BAC has 17 variants), and
  ADR + OTC siblings (BABA / BABAF / BBAAY).

Called from ``daily_cik_refresh`` (Stage 7 sibling enrichment) so it
runs alongside the existing ``company_tickers.json`` ingest and the
Stage 6 ``cik_refresh_mf_directory`` ingest.

URL: ``https://www.sec.gov/files/company_tickers_exchange.json``.
Payload shape (empirical 2026-05-17):

.. code-block:: json

   {
     "fields": ["cik", "name", "ticker", "exchange"],
     "data": [[1045810, "NVIDIA CORP", "NVDA", "Nasdaq"], ...]
   }

CIKs in the payload arrive as integers; we zero-pad to 10-digit TEXT
to match the identity-resolution convention (data-engineer I10).

Snapshot semantics: "observed-ever". UPSERT advances ``last_seen`` on
every observed row; rows SEC drops from the payload remain in the
table with an older ``last_seen``. Consumers needing a freshness gate
filter on ``last_seen >= cutoff``. No DELETE / mark-stale in v1 —
matches the MF directory precedent.

Conditional GET: not implemented in v1 — the file is ~1 MB and daily
fetch cost is acceptable. ETag / Last-Modified plumbing can land in a
follow-up if SEC adds bandwidth pressure.

This is a **parsed snapshot**, not a raw-payload sink. The raw bytes
are not retained; the raw-payload prevention rule
(``docs/review-prevention-log.md`` line 1171) targets per-filing
ingest writers, not reference-directory aggregates. If exact-bytes
retention becomes a future requirement, expand
``cik_raw_documents.document_kind`` enum.

Spec: ``docs/superpowers/specs/2026-05-17-g8-company-tickers-exchange-directory.md``.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import SecFilingsProvider

logger = logging.getLogger(__name__)


_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"


def _fetch_directory(provider: SecFilingsProvider) -> dict[str, Any]:
    """Fetch + parse the exchange directory payload via the shared SEC pool."""
    body = provider.fetch_document_text(_EXCHANGE_URL)
    if not body:
        raise RuntimeError(f"Empty body fetching {_EXCHANGE_URL}")
    parsed: dict[str, Any] = json.loads(body)
    return parsed


def _coerce_text(value: object) -> str | None:
    """Normalise a payload cell to ``str | None``.

    Returns ``None`` for non-string inputs (defends against SEC ever
    emitting a numeric in a TEXT-typed column) and for empty strings
    after strip. Without the ``isinstance`` guard, a numeric ticker
    would raise ``AttributeError: 'int' object has no attribute 'strip'``.
    """
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def refresh_exchange_directory(
    conn: psycopg.Connection[Any],
    *,
    provider: SecFilingsProvider | None = None,
) -> dict[str, int]:
    """Refresh ``cik_refresh_exchange_directory`` from
    ``company_tickers_exchange.json``.

    Returns counts: ``{fetched, directory_rows}`` where ``fetched`` is
    the total row count in the payload and ``directory_rows`` is the
    count successfully upserted (rows skipped due to malformed shape
    are not counted).
    """
    owns_provider = provider is None
    if owns_provider:
        provider = SecFilingsProvider(user_agent=settings.sec_user_agent)
        provider.__enter__()

    try:
        payload = _fetch_directory(provider)  # type: ignore[arg-type]
    finally:
        if owns_provider:
            provider.__exit__(None, None, None)  # type: ignore[union-attr]

    fields = payload.get("fields", [])
    rows = payload.get("data", [])
    if not isinstance(fields, list) or not isinstance(rows, list) or not fields or not rows:
        return {"fetched": 0, "directory_rows": 0}

    # Per-field tolerance — if any required field is absent, no-op
    # safely. Without this guard ``fields.index(...)`` would raise
    # ``ValueError`` and surface as an uncaught exception.
    required_fields = ("cik", "name", "ticker", "exchange")
    field_idx: dict[str, int] = {}
    for fld in required_fields:
        try:
            field_idx[fld] = fields.index(fld)
        except ValueError:
            logger.warning(
                "exchange_directory: SEC payload missing required field %r; skipping refresh",
                fld,
            )
            return {"fetched": 0, "directory_rows": 0}

    idx_cik = field_idx["cik"]
    idx_name = field_idx["name"]
    idx_ticker = field_idx["ticker"]
    idx_exchange = field_idx["exchange"]
    max_idx = max(field_idx.values())

    directory_rows = 0
    with conn.transaction(), conn.cursor() as cur:
        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) <= max_idx:
                logger.warning("exchange_directory: skipping malformed row: %r", row)
                continue

            raw_cik = row[idx_cik]
            if raw_cik is None:
                logger.warning("exchange_directory: skipping row with null cik: %r", row)
                continue
            try:
                cik_padded = str(int(raw_cik)).zfill(10)
            except TypeError, ValueError:
                logger.warning(
                    "exchange_directory: skipping row with non-numeric cik %r: %r",
                    raw_cik,
                    row,
                )
                continue

            ticker = _coerce_text(row[idx_ticker])
            if ticker is None:
                logger.warning(
                    "exchange_directory: skipping row with empty/non-string ticker (cik=%s): %r",
                    cik_padded,
                    row,
                )
                continue

            name = _coerce_text(row[idx_name])
            exchange = _coerce_text(row[idx_exchange])

            cur.execute(
                """
                INSERT INTO cik_refresh_exchange_directory
                    (cik, ticker, name, exchange, last_seen)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (cik, ticker) DO UPDATE SET
                    name = EXCLUDED.name,
                    exchange = EXCLUDED.exchange,
                    last_seen = NOW()
                """,
                (cik_padded, ticker, name, exchange),
            )
            # UPSERT always advances last_seen (no DO NOTHING branch),
            # so unconditional increment matches actual write count.
            directory_rows += 1

    logger.info(
        "refresh_exchange_directory: fetched=%s directory_rows=%s",
        len(rows),
        directory_rows,
    )
    return {
        "fetched": len(rows),
        "directory_rows": directory_rows,
    }
