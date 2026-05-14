"""Bundled ``company_tickers_mf.json`` ingest (#1171, T4 in the plan).

Fetches the SEC mutual-fund / ETF directory + populates two tables:

1. ``cik_refresh_mf_directory`` — snapshot keyed by classId (all rows).
2. ``external_identifiers`` (``provider='sec', identifier_type='class_id'``)
   for classes whose ``symbol`` matches an existing instrument.

Called from ``daily_cik_refresh`` (Stage 6) so it runs alongside the
existing ``company_tickers.json`` ingest.

URL: ``https://www.sec.gov/files/company_tickers_mf.json`` — payload
shape:

.. code-block:: json

   {
     "fields": ["cik", "seriesId", "classId", "symbol"],
     "data": [[36405, "S000002839", "C000010048", "VFINX"], ...]
   }

CIKs in the payload arrive as integers; we zero-pad to 10-digit TEXT
to match the identity-resolution convention (data-engineer I10).

Conditional GET: not implemented in v1 — the file is ~1 MB and daily
fetch cost is acceptable. ETag/Last-Modified plumbing can land in a
follow-up if SEC adds bandwidth pressure.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import SecFilingsProvider

logger = logging.getLogger(__name__)


_MF_DIRECTORY_URL = "https://www.sec.gov/files/company_tickers_mf.json"


def _fetch_directory(provider: SecFilingsProvider) -> dict[str, Any]:
    """Fetch + parse the MF directory payload via the shared SEC pool."""
    body = provider.fetch_document_text(_MF_DIRECTORY_URL)
    if not body:
        raise RuntimeError(f"Empty body fetching {_MF_DIRECTORY_URL}")
    return json.loads(body)


def refresh_mf_directory(
    conn: psycopg.Connection[Any],
    *,
    provider: SecFilingsProvider | None = None,
) -> dict[str, int]:
    """Refresh ``cik_refresh_mf_directory`` + populate ``external_identifiers``
    for in-universe symbols.

    Returns counts: ``{fetched, directory_rows, external_identifier_rows}``.
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
    if not fields or not rows:
        return {"fetched": 0, "directory_rows": 0, "external_identifier_rows": 0}

    idx_cik = fields.index("cik")
    idx_series = fields.index("seriesId")
    idx_class = fields.index("classId")
    idx_symbol = fields.index("symbol")

    directory_rows = 0
    ext_id_rows = 0
    with conn.transaction(), conn.cursor() as cur:
        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) <= max(idx_cik, idx_series, idx_class, idx_symbol):
                logger.warning("mf_directory skipping malformed row: %r", row)
                continue
            raw_cik = row[idx_cik]
            series_id = row[idx_series]
            class_id = row[idx_class]
            symbol = row[idx_symbol]

            if not class_id:
                continue

            trust_cik = str(raw_cik).zfill(10) if raw_cik is not None else None
            symbol_stripped = (symbol or "").strip() or None

            cur.execute(
                """
                INSERT INTO cik_refresh_mf_directory (class_id, series_id, symbol, trust_cik, last_seen)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (class_id) DO UPDATE SET
                    series_id = EXCLUDED.series_id,
                    symbol = EXCLUDED.symbol,
                    trust_cik = EXCLUDED.trust_cik,
                    last_seen = NOW()
                """,
                (class_id, series_id, symbol_stripped, trust_cik),
            )
            directory_rows += 1

            if symbol_stripped is None:
                continue

            # Look up the matching instrument by symbol. Skip if not in universe.
            cur.execute(
                "SELECT instrument_id FROM instruments WHERE symbol = %s",
                (symbol_stripped,),
            )
            inst_row = cur.fetchone()
            if inst_row is None:
                continue
            instrument_id = inst_row[0]

            cur.execute(
                """
                INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value)
                VALUES (%s, 'sec', 'class_id', %s)
                ON CONFLICT DO NOTHING
                """,
                (instrument_id, class_id),
            )
            if cur.rowcount > 0:
                ext_id_rows += 1

    logger.info(
        "refresh_mf_directory: directory_rows=%s external_identifier_rows=%s",
        directory_rows,
        ext_id_rows,
    )
    return {
        "fetched": len(rows),
        "directory_rows": directory_rows,
        "external_identifier_rows": ext_id_rows,
    }
