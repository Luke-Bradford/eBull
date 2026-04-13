"""
Runtime config service.

Source of truth for the trading-mode flags consumed by the execution guard
and order client:

  - enable_auto_trading
  - enable_live_trading

These were previously sourced from environment-backed Settings.  They are now
stored in a DB-backed singleton (`runtime_config`) so an operator can toggle
them at runtime via the /config API without redeploying — and so the kill
switch and trading flags share a single audit trail.

Singleton invariant: a single row with id = TRUE.  The row is seeded by
migration 015.  A missing row at runtime is treated as configuration
corruption and surfaced as `RuntimeConfigCorrupt` — every safety-critical
caller (execution guard, order client, /config API) must fail closed on it.

Audit invariant: every mutation writes one `runtime_config_audit` row per
changed field, in the same transaction as the UPDATE.  Kill-switch toggles
also write into this table (`field='kill_switch'`) so a single timeline
covers all config-style changes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

import psycopg
import psycopg.rows

logger = logging.getLogger(__name__)

AuditField = Literal["enable_auto_trading", "enable_live_trading", "kill_switch", "display_currency"]


class RuntimeConfigCorrupt(RuntimeError):
    """Raised when the runtime_config singleton row is missing.

    Callers on safety-critical paths must catch this and fail closed —
    never default to "auto/live trading enabled".
    """


class RuntimeConfigNoOp(ValueError):
    """Raised when a PATCH would not change any flag value.

    Rejected so updated_at/updated_by/reason on the singleton can never drift
    from the audit table — every recorded provenance update has a matching
    audit row.
    """


@dataclass(frozen=True)
class RuntimeConfig:
    enable_auto_trading: bool
    enable_live_trading: bool
    display_currency: str
    updated_at: datetime
    updated_by: str
    reason: str


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


def get_runtime_config(conn: psycopg.Connection[Any]) -> RuntimeConfig:
    """Load the singleton runtime_config row.

    Raises RuntimeConfigCorrupt if the row is missing — every caller on a
    safety-critical path must treat this as fail-closed.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT enable_auto_trading,
                   enable_live_trading,
                   display_currency,
                   updated_at,
                   updated_by,
                   reason
            FROM runtime_config
            WHERE id = TRUE
            """
        )
        row = cur.fetchone()

    if row is None:
        raise RuntimeConfigCorrupt("runtime_config singleton row missing — configuration corrupt")

    return RuntimeConfig(
        enable_auto_trading=bool(row["enable_auto_trading"]),
        enable_live_trading=bool(row["enable_live_trading"]),
        display_currency=str(row["display_currency"]),
        updated_at=row["updated_at"],
        updated_by=str(row["updated_by"]),
        reason=str(row["reason"]),
    )


def update_runtime_config(
    conn: psycopg.Connection[Any],
    *,
    updated_by: str,
    reason: str,
    enable_auto_trading: bool | None = None,
    enable_live_trading: bool | None = None,
    display_currency: str | None = None,
    now: datetime | None = None,
) -> RuntimeConfig:
    """Atomically update the runtime_config singleton.

    Only fields passed as non-None are changed (partial update semantics).
    Writes one audit row per changed field, in the same transaction as the
    UPDATE.  The pre-update row is read inside the transaction with
    SELECT ... FOR UPDATE so the audit `old_value` cannot race a concurrent
    writer (review-prevention-log: "Audit reads outside the write
    transaction", "Read-then-write cap enforcement outside transaction").

    Raises RuntimeConfigCorrupt if the singleton row is missing — never
    auto-recreates, since silently restoring a corrupted config would mask
    the problem.

    Raises ValueError if all field arguments are None (caller passed an
    empty patch).
    """
    if enable_auto_trading is None and enable_live_trading is None and display_currency is None:
        raise ValueError("update_runtime_config: at least one field must be provided")

    now = now or _utcnow()

    with conn.transaction():
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT enable_auto_trading, enable_live_trading, display_currency
                FROM runtime_config
                WHERE id = TRUE
                FOR UPDATE
                """
            )
            current = cur.fetchone()
            if current is None:
                raise RuntimeConfigCorrupt(
                    "runtime_config singleton row missing — cannot update; configuration corrupt"
                )

        new_auto = enable_auto_trading if enable_auto_trading is not None else bool(current["enable_auto_trading"])
        new_live = enable_live_trading if enable_live_trading is not None else bool(current["enable_live_trading"])
        new_currency = display_currency if display_currency is not None else str(current["display_currency"])

        # No-op patch detection: if every provided field already matches the
        # current row, refuse the patch.  Otherwise the UPDATE would silently
        # rewrite updated_at/updated_by/reason on the singleton with no audit
        # row to record the attribution change, leaving config provenance
        # diverged from the audit table.
        auto_changed = enable_auto_trading is not None and bool(current["enable_auto_trading"]) != new_auto
        live_changed = enable_live_trading is not None and bool(current["enable_live_trading"]) != new_live
        currency_changed = display_currency is not None and str(current["display_currency"]) != new_currency
        if not auto_changed and not live_changed and not currency_changed:
            raise RuntimeConfigNoOp("patch would not change any field value")

        # RETURNING updated_at so the caller carries the DB-committed value
        # rather than the application-side `now`, eliminating any clock-skew
        # gap (and surviving any future updated_at trigger).
        with conn.cursor(row_factory=psycopg.rows.dict_row) as upd_cur:
            upd_cur.execute(
                """
                UPDATE runtime_config
                SET enable_auto_trading = %(auto)s,
                    enable_live_trading = %(live)s,
                    display_currency    = %(currency)s,
                    updated_at          = %(at)s,
                    updated_by          = %(by)s,
                    reason              = %(reason)s
                WHERE id = TRUE
                RETURNING updated_at
                """,
                {
                    "auto": new_auto,
                    "live": new_live,
                    "currency": new_currency,
                    "at": now,
                    "by": updated_by,
                    "reason": reason,
                },
            )
            updated_row = upd_cur.fetchone()
        # review-prevention-log: "Single-row UPDATE silent no-op on missing row".
        if updated_row is None:
            raise RuntimeConfigCorrupt("runtime_config UPDATE affected 0 rows — singleton vanished")
        committed_updated_at = updated_row["updated_at"]

        # One audit row per *changed* field.  Unchanged fields produce no row
        # so the audit table is queryable as "history of changes" not "history
        # of patches".
        if auto_changed:
            _insert_audit_row(
                conn,
                changed_at=now,
                changed_by=updated_by,
                reason=reason,
                field="enable_auto_trading",
                old_value=str(bool(current["enable_auto_trading"])).lower(),
                new_value=str(new_auto).lower(),
            )
        if live_changed:
            _insert_audit_row(
                conn,
                changed_at=now,
                changed_by=updated_by,
                reason=reason,
                field="enable_live_trading",
                old_value=str(bool(current["enable_live_trading"])).lower(),
                new_value=str(new_live).lower(),
            )
        if currency_changed:
            _insert_audit_row(
                conn,
                changed_at=now,
                changed_by=updated_by,
                reason=reason,
                field="display_currency",
                old_value=str(current["display_currency"]),
                new_value=new_currency,
            )

    logger.info(
        "runtime_config updated by=%s reason=%s auto=%s live=%s currency=%s",
        updated_by,
        reason,
        new_auto,
        new_live,
        new_currency,
    )

    return RuntimeConfig(
        enable_auto_trading=new_auto,
        enable_live_trading=new_live,
        display_currency=new_currency,
        updated_at=committed_updated_at,
        updated_by=updated_by,
        reason=reason,
    )


def _insert_audit_row(
    conn: psycopg.Connection[Any],
    *,
    changed_at: datetime,
    changed_by: str,
    reason: str,
    field: AuditField,
    old_value: str | None,
    new_value: str,
) -> None:
    """Insert one runtime_config_audit row.

    Must be called inside an open transaction by the caller.
    """
    conn.execute(
        """
        INSERT INTO runtime_config_audit
            (changed_at, changed_by, reason, field, old_value, new_value)
        VALUES
            (%(at)s, %(by)s, %(reason)s, %(field)s, %(old)s, %(new)s)
        """,
        {
            "at": changed_at,
            "by": changed_by,
            "reason": reason,
            "field": field,
            "old": old_value,
            "new": new_value,
        },
    )


def write_kill_switch_audit(
    conn: psycopg.Connection[Any],
    *,
    changed_by: str,
    reason: str,
    old_active: bool | None,
    new_active: bool,
    now: datetime | None = None,
) -> None:
    """Write a kill-switch audit row into runtime_config_audit.

    Called by ops_monitor.activate_kill_switch / deactivate_kill_switch as
    part of the same transaction as the kill_switch UPDATE so the audit
    cannot drift from the underlying state.

    `old_active` may be None on the first activation if the prior state is
    unknown — the column is nullable for this case.
    """
    _insert_audit_row(
        conn,
        changed_at=now or _utcnow(),
        changed_by=changed_by,
        reason=reason,
        field="kill_switch",
        old_value=None if old_active is None else str(old_active).lower(),
        new_value=str(new_active).lower(),
    )
