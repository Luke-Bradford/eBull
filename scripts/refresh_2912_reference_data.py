"""Refresh or verify the immutable #2912 reference-data snapshots."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from typing import Any

import httpx
import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.services.reference_data import (
    AQR_DATASET_KEYS,
    FRED_DATASET_KEYS,
    FRENCH_DATASET_KEYS,
    refresh_reference_group,
)

_GROUPS = {
    "french": FRENCH_DATASET_KEYS,
    "aqr": AQR_DATASET_KEYS,
    "fred": FRED_DATASET_KEYS,
    "all": FRENCH_DATASET_KEYS + AQR_DATASET_KEYS + FRED_DATASET_KEYS,
}


def _verify(conn: psycopg.Connection[Any], dataset_keys: Sequence[str]) -> int:
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT DISTINCT ON (dataset_key)
                   source, dataset_key, parser_version, parse_status,
                   response_sha256, row_count, missing_count,
                   first_observation, last_observation
            FROM reference_data_snapshots
            WHERE dataset_key = ANY(%s) AND parse_status = 'accepted'
            ORDER BY dataset_key, fetched_at DESC, snapshot_id DESC
            """,
            (list(dataset_keys),),
        )
        rows = cursor.fetchall()
    by_key = {str(row["dataset_key"]): row for row in rows}
    for key in dataset_keys:
        row = by_key.get(key)
        if row is None:
            print(f"{key}: MISSING accepted snapshot")
            continue
        print(
            f"{key}: {row['parse_status']} parser={row['parser_version']} "
            f"sha256={row['response_sha256']} rows={row['row_count']} "
            f"missing={row['missing_count']} range={row['first_observation']}..{row['last_observation']}"
        )
    return 0 if len(by_key) == len(dataset_keys) else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=tuple(_GROUPS), default="all")
    parser.add_argument("--verify", action="store_true", help="read-only snapshot census; make no HTTP calls")
    args = parser.parse_args()
    keys = _GROUPS[args.source]
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        if args.verify:
            return _verify(conn, keys)
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            reports = refresh_reference_group(conn, client=client, dataset_keys=keys)
    for report in reports:
        print(
            f"{report.dataset_key}: {report.status} snapshot={report.snapshot_id} "
            f"sha256={report.response_sha256} rows={report.row_count} "
            f"missing={report.missing_count} range={report.first_observation}..{report.last_observation}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
