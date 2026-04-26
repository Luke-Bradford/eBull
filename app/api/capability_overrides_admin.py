"""Admin endpoint for capability-override drift visibility (#531).

Operators can edit ``exchanges.capabilities`` JSONB to adjust which
providers a given venue uses. Without a drift view, those edits
silently accumulate and a future seed-default change can land on
top of operator overrides without anyone noticing.

This endpoint diffs every exchange row's current ``capabilities``
against the migration-071 seed default for its asset class. Rows
that match the seed are excluded; rows that diverge surface with
both states for operator review.

Read-only; the operator edits via direct SQL or a future write
endpoint (out of scope for #531). Auth: operator session or
service token.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.services.capabilities import V1_CAPABILITIES

router = APIRouter(
    prefix="/admin/capability-overrides",
    tags=["admin", "capabilities"],
    dependencies=[Depends(require_session_or_service_token)],
)


# Seed defaults from sql/071 (us_equity) + sql/072 (drop fmp).
# Hard-coded here rather than re-read from the migration file so
# the diff endpoint stays self-contained. If the seed changes in
# a future migration, update this map AND ship a new migration —
# the test in tests/test_migration_071_exchanges_capabilities.py
# pins the expected post-migration shape so drift is caught at
# CI time.
_SEED_BY_ASSET_CLASS: dict[str, dict[str, list[str]]] = {
    "us_equity": {
        "filings": ["sec_edgar"],
        "fundamentals": ["sec_xbrl"],
        "dividends": ["sec_dividend_summary"],
        "insider": ["sec_form4"],
        "analyst": [],
        "ratings": [],
        "esg": [],
        "ownership": ["sec_13f", "sec_13d_13g"],
        "corporate_events": ["sec_8k_events"],
        "business_summary": ["sec_10k_item1"],
        "officers": [],
    },
}
# All non-us_equity asset classes get the empty-but-correctly-
# shaped default per migration 071.
_EMPTY_SEED: dict[str, list[str]] = {cap: [] for cap in V1_CAPABILITIES}


def _seed_for(asset_class: str) -> dict[str, list[str]]:
    return _SEED_BY_ASSET_CLASS.get(asset_class, _EMPTY_SEED)


class CapabilityCellDiff(BaseModel):
    """One per-capability diff for an exchange row."""

    capability: str
    seed_providers: list[str]
    current_providers: list[str]


class ExchangeOverrideRow(BaseModel):
    """One exchange whose capabilities diverge from seed."""

    exchange_id: str
    exchange_name: str | None
    asset_class: str | None
    diffs: list[CapabilityCellDiff]


class OverridesListResponse(BaseModel):
    checked_at: datetime
    total_overrides: int
    rows: list[ExchangeOverrideRow]


@router.get("", response_model=OverridesListResponse)
def list_overrides(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> OverridesListResponse:
    """List exchange rows whose ``capabilities`` differ from the
    seed default for their asset_class.

    Empty list = every exchange is at its seed default. Each
    row enumerates only the capabilities that diverge — clean
    capabilities are dropped so the dashboard surfaces only the
    drift signal.
    """
    rows: list[ExchangeOverrideRow] = []

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT exchange_id, name, asset_class, capabilities
              FROM exchanges
             ORDER BY exchange_id
            """
        )
        for r in cur.fetchall():
            asset_class_raw = r["asset_class"]
            asset_class = str(asset_class_raw) if asset_class_raw is not None else None
            seed = _seed_for(asset_class) if asset_class is not None else _EMPTY_SEED
            current_raw: Any = r["capabilities"]
            current: dict[str, list[str]] = {}
            if isinstance(current_raw, dict):
                for cap_name, cap_value in current_raw.items():
                    if isinstance(cap_value, list):
                        current[str(cap_name)] = [str(v) for v in cap_value if isinstance(v, str)]

            diffs: list[CapabilityCellDiff] = []
            for cap in V1_CAPABILITIES:
                seed_list = list(seed.get(cap, []))
                current_list = list(current.get(cap, []))
                # Drift compares the *set* of providers, not the
                # ordering. Provider order is meaningful at runtime
                # (resolver renders panels in declared order), but
                # reordering without changing the set isn't useful
                # drift signal — an operator who reordered
                # intentionally doesn't want to see it as drift, and
                # JSONB array round-trips can in principle reorder.
                # Codex review on #531.
                if sorted(seed_list) != sorted(current_list):
                    diffs.append(
                        CapabilityCellDiff(
                            capability=cap,
                            seed_providers=seed_list,
                            current_providers=current_list,
                        )
                    )

            if not diffs:
                continue

            rows.append(
                ExchangeOverrideRow(
                    exchange_id=str(r["exchange_id"]),
                    exchange_name=str(r["name"]) if r["name"] is not None else None,
                    asset_class=asset_class,
                    diffs=diffs,
                )
            )

    return OverridesListResponse(
        checked_at=datetime.now().astimezone(),
        total_overrides=len(rows),
        rows=rows,
    )
