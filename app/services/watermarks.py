"""External data watermark store (#269).

Every incremental-fetch adapter uses the same helper to remember
"what was the newest thing we saw last time from this provider."
Replaces the pattern of each job rolling its own bespoke storage.

Usage pattern:

    from app.services.watermarks import get_watermark, set_watermark

    wm = get_watermark(conn, "sec.tickers", "global")
    headers = {}
    if wm and wm.response_hash:
        headers["If-Modified-Since"] = wm.watermark  # Last-Modified value

    resp = http.get(url, headers=headers)
    if resp.status_code == 304:
        return  # nothing changed

    body_hash = sha256(resp.content).hexdigest()
    if wm and wm.response_hash == body_hash:
        return  # identical body, provider didn't serve 304 (e.g. FMP)

    # Do the work — upsert / ingest
    ...

    set_watermark(
        conn,
        source="sec.tickers",
        key="global",
        watermark=resp.headers.get("Last-Modified", ""),
        watermark_at=parse_http_date(resp.headers.get("Last-Modified")),
        response_hash=body_hash,
    )

The ``source`` identifier is a stable short string — one per
"data source under a specific fetch contract." Document new sources
in docs/superpowers/plans/2026-04-17-lightweight-etl-audit.md so
future adapters reuse names rather than inventing variants.

The ``watermark`` value is opaque to this module. Callers interpret
it (ETag string, accession_number, ISO date) in their own domain.
This module is pure key-value storage.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import psycopg


@dataclass(frozen=True)
class Watermark:
    """Snapshot of a stored watermark row."""

    source: str
    key: str
    watermark: str
    watermark_at: datetime | None
    fetched_at: datetime
    response_hash: str | None


def get_watermark(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    source: str,
    key: str,
) -> Watermark | None:
    """Return the stored watermark row for (source, key), or None if
    no watermark has ever been recorded.

    None return is a hot-path signal to the caller: "no prior state,
    do full backfill instead of incremental fetch." Do not treat
    a missing row as an error.
    """
    row = conn.execute(
        """
        SELECT source, key, watermark, watermark_at, fetched_at, response_hash
        FROM external_data_watermarks
        WHERE source = %s AND key = %s
        """,
        (source, key),
    ).fetchone()
    if row is None:
        return None
    return Watermark(
        source=row[0],
        key=row[1],
        watermark=row[2],
        watermark_at=row[3],
        fetched_at=row[4],
        response_hash=row[5],
    )


def set_watermark(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    *,
    source: str,
    key: str,
    watermark: str,
    watermark_at: datetime | None = None,
    response_hash: str | None = None,
) -> None:
    """Upsert the watermark row for (source, key).

    Callers must invoke this inside their own transaction — the
    watermark write and the actual data upsert must land atomically
    so a crash mid-ingest doesn't leave the watermark ahead of the
    data (next run would skip work that wasn't finished).
    """
    conn.execute(
        """
        INSERT INTO external_data_watermarks (
            source, key, watermark, watermark_at, response_hash, fetched_at
        )
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (source, key) DO UPDATE SET
            watermark      = EXCLUDED.watermark,
            watermark_at   = EXCLUDED.watermark_at,
            response_hash  = EXCLUDED.response_hash,
            fetched_at     = EXCLUDED.fetched_at
        """,
        (source, key, watermark, watermark_at, response_hash),
    )


def list_keys(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    source: str,
) -> list[str]:
    """Return all stored keys for ``source``. Used by adapters to
    enumerate "entities we have a prior watermark for" — typically
    CIKs that have been seen at least once — vs entities still
    needing initial backfill."""
    rows = conn.execute(
        "SELECT key FROM external_data_watermarks WHERE source = %s ORDER BY key",
        (source,),
    ).fetchall()
    return [r[0] for r in rows]
