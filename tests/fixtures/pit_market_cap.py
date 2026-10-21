"""Seed one #3841 point-in-time step so ``instrument_market_cap_pit`` (and so ``instrument_valuation``) has a row.

The step covers ``[valid_from, valid_to)``, defaulting to the session alone, and was built having seen bars through
``bars_through`` (default ``session``) with no revision rows: the view publishes ``close × shares × split_factor``
at the session unless ``refusal`` is given.

The step belongs to build ``build_id``, dated at the first session seeded under it. #3923 (sql/513) refuses a session
a calendar month or more before its build's date, so a case far from the shared build's date takes its own build.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import psycopg

BUILD_ID = 3850


def seed_pit_step(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    *,
    session: date,
    close: float,
    shares: float | None,
    split_factor: float | None = 1.0,
    refusal: str | None = None,
    valid_from: date | None = None,
    valid_to: date | None = None,
    bars_through: date | None = None,
    build_id: int = BUILD_ID,
) -> None:
    conn.execute(
        "INSERT INTO market_cap_pit_builds (build_id, started_at, finished_at, status, build_date, rule_version, "
        "submissions_sha256, summary) VALUES (%s, now(), now(), 'complete', %s, 'r', 's', '{}') "
        "ON CONFLICT (build_id) DO NOTHING",
        (build_id, session),
    )
    conn.execute(
        "INSERT INTO price_daily (instrument_id, price_date, close, volume) VALUES (%s, %s, %s, 1)",
        (instrument_id, session, close),
    )
    conn.execute(
        "INSERT INTO instrument_share_count_pit (instrument_id, valid_from, valid_to, shares_concept, accession, "
        "context_date, accepted_at, shares_reported, split_factor, refusal, evidence, revision_count, "
        "backdated_insert_count, bars_through, build_id) "
        "VALUES (%s, %s, %s, 'cover', 'A-3850', %s, now(), %s, %s, %s, '{}', 0, 0, %s, %s)",
        (
            instrument_id,
            valid_from or session,
            valid_to or session + timedelta(days=1),
            session - timedelta(days=60),
            shares,
            None if refusal else split_factor,
            refusal,
            bars_through or session,
            build_id,
        ),
    )
