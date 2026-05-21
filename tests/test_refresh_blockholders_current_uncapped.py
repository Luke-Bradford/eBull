"""PR11 #1233 §6.3 — ``refresh_blockholders_current`` uncapped contract.

The parent spec exempts the refresh-current step from the 3y retention
cap **by design**: it is a pure deterministic materialisation of
``ownership_blockholders_current`` from whatever rows already exist in
``ownership_blockholders_observations``. The cap fires upstream at the
ingest / sync / rewash / parser chokepoints (gates A-G in PR11). By the
time refresh runs, the observations layer is already retention-filtered.
Refresh must NOT impose its own filter on top — that would cause
pre-cap rows still present from a manual seed / replay / fixture to
silently disappear from the materialised current snapshot.

This test pins both halves of the contract:

  1. Pre-cap observation rows are NOT deleted by the refresh
     (refresh touches ``_current`` only, never ``_observations``).
  2. The pre-cap row IS reflected in ``ownership_blockholders_current``
     after the refresh (no implicit retention filter on the SELECT
     side of the DELETE-then-INSERT pattern in
     ``ownership_observations.refresh_blockholders_current``).

Codex 1b HIGH lesson: the schema columns for
``ownership_blockholders_observations`` come from
``sql/115_ownership_blockholders_observations.sql`` directly — read the
SQL before writing the INSERT. The seed below mirrors the
``record_blockholder_observation`` helper signature (``app/services/
ownership_observations.py:483``) and the cross-CHECK
``obs_submission_type_status_consistent`` (``sql/115:42-47``):
SCHEDULE 13D ↔ active.

Codex 1b MEDIUM lesson: ``instruments.company_name`` is ``NOT NULL``
(``sql/001_init.sql:3``) — the seed must populate it.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

import psycopg
import psycopg.rows
import pytest

from app.services.blockholders import blockholders_retention_cutoff
from app.services.ownership_observations import (
    record_blockholder_observation,
    refresh_blockholders_current,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Seed helpers (mirror canonical idioms from
# ``tests/test_ownership_observations_sync_blockholders_cap.py``).
# ---------------------------------------------------------------------------


def _seed_instrument(conn: psycopg.Connection[Any], *, iid: int, symbol: str) -> None:
    """Seed an ``instruments`` row. ``company_name`` is NOT NULL per
    ``sql/001_init.sql:3``."""
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable, country)
        VALUES (%s, %s, %s, '4', 'USD', TRUE, 'US')
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


# ---------------------------------------------------------------------------
# Refresh contract — uncapped (parent spec §6.3)
# ---------------------------------------------------------------------------


class TestRefreshBlockholdersCurrentUncapped:
    """Refresh helper must NOT impose a retention filter on top of the
    observations layer. Pre-cap rows that are already in
    ``ownership_blockholders_observations`` (e.g. surviving from a
    legacy backfill or a fixture seed) MUST flow through into
    ``ownership_blockholders_current`` and MUST survive in
    ``ownership_blockholders_observations`` itself."""

    def test_pre_cap_observation_flows_into_current_and_survives(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811 — pytest fixture shadow
    ) -> None:
        conn = ebull_test_conn
        instrument_id = 841_911_103
        _seed_instrument(conn, iid=instrument_id, symbol="REFR")

        cutoff = blockholders_retention_cutoff()
        # Strictly before the cutoff midnight → strictly outside the
        # 3y retention window. ±60d is well clear of the date-boundary
        # comparison in ``blockholders_within_retention``.
        cutoff_midnight = datetime.combine(cutoff, datetime.min.time(), tzinfo=UTC)
        pre_cap_dt = cutoff_midnight - timedelta(days=60)
        pre_accn = "0000123450-22-000099"
        primary_filer_cik = "0000123450"

        # Seed one pre-cap observation row directly. SCHEDULE 13D +
        # status='active' satisfies the cross-CHECK
        # ``obs_submission_type_status_consistent`` in sql/115.
        record_blockholder_observation(
            conn,
            instrument_id=instrument_id,
            reporter_cik=primary_filer_cik,
            reporter_name="Test Primary Filer",
            ownership_nature="beneficial",
            submission_type="SCHEDULE 13D",
            status_flag="active",
            source="13d",
            source_document_id=pre_accn,
            source_accession=pre_accn,
            source_field="aggregate_amount_owned",
            source_url=None,
            filed_at=pre_cap_dt,
            period_start=None,
            period_end=pre_cap_dt.date(),
            ingest_run_id=uuid4(),
            aggregate_amount_owned=Decimal("1000000"),
            percent_of_class=Decimal("5.5"),
        )
        conn.commit()

        # Sanity: the pre-cap row is in the observations table BEFORE
        # the refresh.
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT source_accession, filed_at
                FROM ownership_blockholders_observations
                WHERE instrument_id = %s
                """,
                (instrument_id,),
            )
            obs_before = cur.fetchall()
        assert len(obs_before) == 1, f"seed not present: {obs_before}"
        assert obs_before[0]["source_accession"] == pre_accn

        # ACT — run the refresh.
        rows_in_current = refresh_blockholders_current(conn, instrument_id=instrument_id)
        conn.commit()

        # ASSERT 1 — refresh did NOT delete the pre-cap observation.
        # Parent spec §6.3: refresh touches ``_current`` only, never
        # ``_observations``. A defensive retention DELETE on the
        # observations table would be a regression.
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT source_accession, filed_at
                FROM ownership_blockholders_observations
                WHERE instrument_id = %s
                """,
                (instrument_id,),
            )
            obs_after = cur.fetchall()
        assert len(obs_after) == 1, (
            f"refresh deleted the pre-cap observation row (parent spec §6.3 forbids this): {obs_after}"
        )
        assert obs_after[0]["source_accession"] == pre_accn

        # ASSERT 2 — the pre-cap row materialised into
        # ``_current`` (refresh-current is exempt from the cap by
        # design — parent spec §6.3). A retention filter on the
        # refresh SELECT would silently drop this row.
        assert rows_in_current == 1, (
            "refresh_blockholders_current() reported 0 rows "
            "for an instrument with a pre-cap observation — "
            "implies refresh is silently filtering on retention "
            "(parent spec §6.3 forbids this)"
        )
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT source_accession, reporter_cik, ownership_nature,
                       submission_type, status_flag, source
                FROM ownership_blockholders_current
                WHERE instrument_id = %s
                """,
                (instrument_id,),
            )
            current_rows = cur.fetchall()
        assert len(current_rows) == 1, f"refresh did not materialise the pre-cap row into _current: {current_rows}"
        row = current_rows[0]
        assert row["source_accession"] == pre_accn
        assert row["reporter_cik"] == primary_filer_cik
        assert row["ownership_nature"] == "beneficial"
        assert row["submission_type"] == "SCHEDULE 13D"
        assert row["status_flag"] == "active"
        assert row["source"] == "13d"
