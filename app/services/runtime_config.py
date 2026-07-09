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
from typing import Any, Literal, cast

import psycopg
import psycopg.rows

logger = logging.getLogger(__name__)

AuditField = Literal[
    "enable_auto_trading",
    "enable_live_trading",
    "kill_switch",
    "display_currency",
    "llm_provider",
    "llm_base_url",
    "llm_model",
]

# Validated currency codes for display_currency. Must match the frontend
# SUPPORTED_CURRENCIES list in DisplayCurrencySection.tsx.
SUPPORTED_CURRENCIES: frozenset[str] = frozenset({"GBP", "USD", "EUR"})

# Valid llm_provider values (#1919). Must match the table CHECK in
# sql/218_llm_provider_config.sql and the frontend LlmProviderSection.tsx.
# Keys are env-only (Settings.anthropic_api_key / Settings.llm_api_key) —
# NEVER stored here: runtime_config_audit records old/new values in
# plaintext.
VALID_LLM_PROVIDERS: frozenset[str] = frozenset({"openai_compatible", "anthropic"})

# Local-first defaults (operator mandate 2026-07-09, spec §2). Single
# source for the migration seed, the boot-recovery guard, and tests.
DEFAULT_LLM_PROVIDER = "openai_compatible"
DEFAULT_LLM_BASE_URL = "http://localhost:11434/v1"
DEFAULT_LLM_MODEL = "qwen3:14b"

# Boot-recovery audit attribution. Operators investigating the audit log can
# search for this exact reason to find re-seed events caused by a vanished
# singleton row (see ensure_runtime_config_singleton).
BOOT_RECOVERY_CHANGED_BY = "boot_recovery"
BOOT_RECOVERY_REASON = "singleton vanished — re-seeded by boot guard"


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
    llm_provider: str
    llm_base_url: str
    llm_model: str
    updated_at: datetime
    updated_by: str
    reason: str


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


def ensure_runtime_config_singleton(conn: psycopg.Connection[Any]) -> None:
    """Re-seed the runtime_config singleton row if it vanished.

    Migration sql/015_runtime_config.sql seeds the row via
    ``INSERT ... ON CONFLICT DO NOTHING`` — a one-time write. If the row
    is later lost (manual ``DELETE``, snapshot restore from pre-seed era,
    future bootstrap reset script), every endpoint that reads
    ``runtime_config`` fail-closes with ``RuntimeConfigCorrupt`` → 503.
    This boot-time guard inspects the singleton and re-seeds with safe
    defaults on absence, writing one ``runtime_config_audit`` row per
    re-seeded field so the module-level audit invariant ("every mutation
    writes one audit row per changed field, in the same transaction as
    the UPDATE") still holds for boot recovery.

    Posture: fail-closed defaults match the migration seed
    (``enable_auto_trading=FALSE``, ``enable_live_trading=FALSE``,
    ``display_currency='GBP'``). A WARNING is logged so the operator
    notices the recovery.

    Idempotent: no-op when exactly one row with ``id=TRUE`` exists.
    Fail-loud when a non-canonical row exists (``id != TRUE``; possible
    only under constraint corruption).

    Connection contract: caller MUST supply a conn in autocommit mode
    (e.g. ``psycopg.connect(url, autocommit=True)``). The helper opens
    its own real new transaction via ``conn.transaction()`` to keep the
    seed INSERT + the three audit INSERTs atomic. Because the caller's
    conn has no outer tx open, ``conn.transaction()`` is a real BEGIN
    (not a SAVEPOINT under an outer tx), so the
    service-no-commit/SAVEPOINT-vs-COMMIT invariant is preserved.

    Race: if another process re-seeds between this helper's SELECT and
    INSERT, the ``ON CONFLICT DO NOTHING`` + ``RETURNING id`` pair
    suppresses our insert AND skips the audit rows — no phantom audit
    rows recorded for a recovery we didn't perform.
    """
    # Enforce the autocommit contract (Codex 2 MEDIUM): if a future
    # caller passes a normal (non-autocommit) connection, psycopg's
    # implicit ``BEGIN`` on the first execute will turn the helper's
    # ``conn.transaction()`` into a SAVEPOINT under that outer tx,
    # silently defeating the atomic-seed-plus-audit guarantee. Fail
    # loud at the boundary instead.
    if not conn.autocommit:
        raise RuntimeError(
            "ensure_runtime_config_singleton requires an autocommit "
            "connection — pass psycopg.connect(url, autocommit=True). "
            "The helper opens its own real BEGIN via conn.transaction(); "
            "a non-autocommit caller would degrade that into a SAVEPOINT."
        )

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM runtime_config")
        rows = cur.fetchall()

    if len(rows) == 1 and rows[0][0] is True:
        return

    if len(rows) > 1 or (rows and rows[0][0] is not True):
        # CHECK (id = TRUE) PRIMARY KEY should forbid this. Fail-loud
        # rather than mask constraint corruption.
        raise RuntimeError(f"runtime_config singleton constraint violated — rows={rows!r}")

    logger.warning(
        "runtime_config singleton vanished — re-seeding with safe defaults "
        "(enable_auto_trading=FALSE, enable_live_trading=FALSE, "
        "display_currency='GBP', llm_provider/base_url/model local-first). "
        "See docs/review-prevention-log.md "
        "section 'Singleton-row migrations need a boot-time presence guard'."
    )
    now = _utcnow()
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO runtime_config
                    (id, enable_auto_trading, enable_live_trading,
                     updated_at, updated_by, reason, display_currency,
                     llm_provider, llm_base_url, llm_model)
                VALUES
                    (TRUE, FALSE, FALSE,
                     %(at)s, %(by)s, %(reason)s, 'GBP',
                     %(llm_provider)s, %(llm_base_url)s, %(llm_model)s)
                ON CONFLICT (id) DO NOTHING
                RETURNING id
                """,
                {
                    "at": now,
                    "by": BOOT_RECOVERY_CHANGED_BY,
                    "reason": BOOT_RECOVERY_REASON,
                    "llm_provider": DEFAULT_LLM_PROVIDER,
                    "llm_base_url": DEFAULT_LLM_BASE_URL,
                    "llm_model": DEFAULT_LLM_MODEL,
                },
            )
            inserted = cur.fetchone()

        if inserted is None:
            # Race: another process re-seeded between our SELECT and
            # INSERT. Their seed row stands; don't write phantom audit
            # rows for a recovery we didn't actually perform.
            return

        for field_name, new_value in (
            ("enable_auto_trading", "false"),
            ("enable_live_trading", "false"),
            ("display_currency", "GBP"),
            ("llm_provider", DEFAULT_LLM_PROVIDER),
            ("llm_base_url", DEFAULT_LLM_BASE_URL),
            ("llm_model", DEFAULT_LLM_MODEL),
        ):
            insert_runtime_config_audit_row(
                conn,
                changed_at=now,
                changed_by=BOOT_RECOVERY_CHANGED_BY,
                reason=BOOT_RECOVERY_REASON,
                field=cast(AuditField, field_name),
                old_value=None,
                new_value=new_value,
            )


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
                   llm_provider,
                   llm_base_url,
                   llm_model,
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
        llm_provider=str(row["llm_provider"]),
        llm_base_url=str(row["llm_base_url"]),
        llm_model=str(row["llm_model"]),
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
    llm_provider: str | None = None,
    llm_base_url: str | None = None,
    llm_model: str | None = None,
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
    provided = (enable_auto_trading, enable_live_trading, display_currency, llm_provider, llm_base_url, llm_model)
    if all(v is None for v in provided):
        raise ValueError("update_runtime_config: at least one field must be provided")

    if display_currency is not None and display_currency not in SUPPORTED_CURRENCIES:
        raise ValueError(f"display_currency must be one of {sorted(SUPPORTED_CURRENCIES)}, got {display_currency!r}")

    if llm_provider is not None and llm_provider not in VALID_LLM_PROVIDERS:
        raise ValueError(f"llm_provider must be one of {sorted(VALID_LLM_PROVIDERS)}, got {llm_provider!r}")

    if llm_base_url is not None and not llm_base_url.startswith(("http://", "https://")):
        raise ValueError(f"llm_base_url must start with http:// or https://, got {llm_base_url!r}")

    if llm_model is not None and not llm_model.strip():
        raise ValueError("llm_model must be a non-empty string")

    now = now or _utcnow()

    with conn.transaction():
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT enable_auto_trading, enable_live_trading, display_currency,
                       llm_provider, llm_base_url, llm_model
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
        new_llm_provider = llm_provider if llm_provider is not None else str(current["llm_provider"])
        new_llm_base_url = llm_base_url if llm_base_url is not None else str(current["llm_base_url"])
        new_llm_model = llm_model if llm_model is not None else str(current["llm_model"])

        # No-op patch detection: if every provided field already matches the
        # current row, refuse the patch.  Otherwise the UPDATE would silently
        # rewrite updated_at/updated_by/reason on the singleton with no audit
        # row to record the attribution change, leaving config provenance
        # diverged from the audit table.
        auto_changed = enable_auto_trading is not None and bool(current["enable_auto_trading"]) != new_auto
        live_changed = enable_live_trading is not None and bool(current["enable_live_trading"]) != new_live
        currency_changed = display_currency is not None and str(current["display_currency"]) != new_currency
        llm_provider_changed = llm_provider is not None and str(current["llm_provider"]) != new_llm_provider
        llm_base_url_changed = llm_base_url is not None and str(current["llm_base_url"]) != new_llm_base_url
        llm_model_changed = llm_model is not None and str(current["llm_model"]) != new_llm_model
        any_changed = (
            auto_changed
            or live_changed
            or currency_changed
            or llm_provider_changed
            or llm_base_url_changed
            or llm_model_changed
        )
        if not any_changed:
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
                    llm_provider        = %(llm_provider)s,
                    llm_base_url        = %(llm_base_url)s,
                    llm_model           = %(llm_model)s,
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
                    "llm_provider": new_llm_provider,
                    "llm_base_url": new_llm_base_url,
                    "llm_model": new_llm_model,
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
            insert_runtime_config_audit_row(
                conn,
                changed_at=now,
                changed_by=updated_by,
                reason=reason,
                field="enable_auto_trading",
                old_value=str(bool(current["enable_auto_trading"])).lower(),
                new_value=str(new_auto).lower(),
            )
        if live_changed:
            insert_runtime_config_audit_row(
                conn,
                changed_at=now,
                changed_by=updated_by,
                reason=reason,
                field="enable_live_trading",
                old_value=str(bool(current["enable_live_trading"])).lower(),
                new_value=str(new_live).lower(),
            )
        if currency_changed:
            insert_runtime_config_audit_row(
                conn,
                changed_at=now,
                changed_by=updated_by,
                reason=reason,
                field="display_currency",
                old_value=str(current["display_currency"]),
                new_value=new_currency,
            )
        if llm_provider_changed:
            insert_runtime_config_audit_row(
                conn,
                changed_at=now,
                changed_by=updated_by,
                reason=reason,
                field="llm_provider",
                old_value=str(current["llm_provider"]),
                new_value=new_llm_provider,
            )
        if llm_base_url_changed:
            insert_runtime_config_audit_row(
                conn,
                changed_at=now,
                changed_by=updated_by,
                reason=reason,
                field="llm_base_url",
                old_value=str(current["llm_base_url"]),
                new_value=new_llm_base_url,
            )
        if llm_model_changed:
            insert_runtime_config_audit_row(
                conn,
                changed_at=now,
                changed_by=updated_by,
                reason=reason,
                field="llm_model",
                old_value=str(current["llm_model"]),
                new_value=new_llm_model,
            )

    logger.info(
        "runtime_config updated by=%s reason=%s auto=%s live=%s currency=%s llm=%s/%s@%s",
        updated_by,
        reason,
        new_auto,
        new_live,
        new_currency,
        new_llm_provider,
        new_llm_model,
        new_llm_base_url,
    )

    return RuntimeConfig(
        enable_auto_trading=new_auto,
        enable_live_trading=new_live,
        display_currency=new_currency,
        llm_provider=new_llm_provider,
        llm_base_url=new_llm_base_url,
        llm_model=new_llm_model,
        updated_at=committed_updated_at,
        updated_by=updated_by,
        reason=reason,
    )


def insert_runtime_config_audit_row(
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

    Public helper — `field='kill_switch'` audit rows are written from
    `app.services.ops_monitor.ensure_kill_switch_singleton` (#1232 bot
    review iter 1 WARNING — cross-module callers should depend on the
    public API, not a private underscore-prefixed symbol).

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
    insert_runtime_config_audit_row(
        conn,
        changed_at=now or _utcnow(),
        changed_by=changed_by,
        reason=reason,
        field="kill_switch",
        old_value=None if old_active is None else str(old_active).lower(),
        new_value=str(new_active).lower(),
    )
