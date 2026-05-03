"""Backfill ``instrument_cik_history`` + ``instrument_symbol_history``
with one ``imported`` row per instrument.

Run via: ``uv run python scripts/backfill_instrument_history.py``

Idempotent — safe to re-run after Batch 7 lands the real
symbol-change ingester. The migrations 102/103 add the empty tables;
this script is the deploy-time companion that seeds the current
chain so the ownership-rollup service has a stable
``historical_ciks_for(instrument_id)`` answer for every instrument.

Pairs with migration 102 + 103 (Batch 1 of #788).
"""

from __future__ import annotations

import psycopg

from app.config import settings
from app.services.instrument_history import backfill_current_history


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        with conn.transaction():
            cik_inserted, sym_inserted = backfill_current_history(conn)
        print(
            f"instrument_cik_history: inserted {cik_inserted} row(s) "
            f"(no-op when already present).\n"
            f"instrument_symbol_history: inserted {sym_inserted} row(s) "
            f"(no-op when already present)."
        )


if __name__ == "__main__":
    main()
