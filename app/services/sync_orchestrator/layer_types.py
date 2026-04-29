"""Typed vocabulary for the freshness state machine (spec sub-project A).

Consumed by the registry (chunk 2), adapters (chunk 3), state
computation (chunk 4), and the v2 API (chunk 5). Must not import from
any orchestrator runtime module — this sits at the bottom of the
orchestrator import graph.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    # psycopg is only used in ContentPredicate's type annotation. Guarding
    # under TYPE_CHECKING keeps this bottom-of-graph module importable in
    # contexts that need only the enums + dataclasses.
    import psycopg


class LayerState(StrEnum):
    HEALTHY = "healthy"
    RUNNING = "running"
    RETRYING = "retrying"
    DEGRADED = "degraded"
    ACTION_NEEDED = "action_needed"
    SECRET_MISSING = "secret_missing"
    CASCADE_WAITING = "cascade_waiting"
    DISABLED = "disabled"


class FailureCategory(StrEnum):
    AUTH_EXPIRED = "auth_expired"
    RATE_LIMITED = "rate_limited"
    SOURCE_DOWN = "source_down"
    SCHEMA_DRIFT = "schema_drift"
    DB_CONSTRAINT = "db_constraint"
    DATA_GAP = "data_gap"
    UPSTREAM_WAITING = "upstream_waiting"
    # #643 — broker-encryption key not loaded into the in-process
    # cache when an adapter tried to encrypt/decrypt. Distinct from
    # AUTH_EXPIRED (which means the credential decrypted but the
    # provider rejected it). Triggers the operator-actionable
    # "restore EBULL_SECRETS_KEY or run /recover" banner instead of
    # the opaque "Unclassified error" the path used to hit.
    MASTER_KEY_MISSING = "master_key_missing"
    INTERNAL_ERROR = "internal_error"


@dataclass(frozen=True)
class Remedy:
    message: str
    operator_fix: str | None
    self_heal: bool


REMEDIES: dict[FailureCategory, Remedy] = {
    FailureCategory.AUTH_EXPIRED: Remedy(
        message="Credential rejected by provider",
        operator_fix="Update the API key in Settings → Providers",
        self_heal=False,
    ),
    FailureCategory.RATE_LIMITED: Remedy(
        message="Rate limit hit — retrying with backoff",
        operator_fix=None,
        self_heal=True,
    ),
    FailureCategory.SOURCE_DOWN: Remedy(
        message="Data source unreachable — retrying with backoff",
        operator_fix=None,
        self_heal=True,
    ),
    FailureCategory.SCHEMA_DRIFT: Remedy(
        message="Provider payload shape changed — needs code update",
        operator_fix="File a bug; the adapter needs a parser update",
        self_heal=False,
    ),
    FailureCategory.DB_CONSTRAINT: Remedy(
        message="Database constraint violated — likely data-model bug",
        operator_fix="Open orchestrator details and inspect the offending row",
        self_heal=False,
    ),
    FailureCategory.DATA_GAP: Remedy(
        message="Source returned no data — will retry next cycle",
        operator_fix=None,
        self_heal=True,
    ),
    FailureCategory.UPSTREAM_WAITING: Remedy(
        message="Waiting on upstream layer",
        operator_fix=None,
        self_heal=True,
    ),
    FailureCategory.MASTER_KEY_MISSING: Remedy(
        message="Broker-encryption key not loaded — credentials cannot decrypt",
        operator_fix=(
            "Restart the backend so master_key.bootstrap() can load the persisted "
            "root secret, or open Settings → /recover if the secret is missing"
        ),
        # No self-heal: a backoff retry won't help — the key has to
        # come back via either the persisted root secret or the
        # operator-driven recovery flow.
        self_heal=False,
    ),
    FailureCategory.INTERNAL_ERROR: Remedy(
        message="Unclassified error — retrying with backoff",
        operator_fix=None,
        self_heal=True,
    ),
}


@dataclass(frozen=True)
class Cadence:
    """Layer refresh cadence.

    Two mutually-exclusive modes:

    * ``interval`` — fixed-width timedelta (the original shape).
      Suitable for hourly, daily, weekly cadences that don't drift
      against the calendar.

    * ``calendar_months`` — calendar-anchored monthly cadence (#335).
      A layer is considered current as long as a counting refresh
      landed within the most-recent ``calendar_months`` calendar
      months in UTC. Anchored to day 1 of the month so a monthly
      cadence does not drift across calendar boundaries
      (Feb-vs-Mar arithmetic with ``timedelta(days=31)`` always
      undershoots short months and overshoots long ones).

    Exactly one of the two MUST be set; the validator enforces it
    so a future caller can't accidentally pass both and pick up an
    ambiguous behavior.
    """

    interval: timedelta | None = None
    calendar_months: int | None = None

    def __post_init__(self) -> None:
        if (self.interval is None) == (self.calendar_months is None):
            raise ValueError("Cadence requires exactly one of interval or calendar_months")
        if self.interval is not None and self.interval <= timedelta(0):
            raise ValueError("interval must be positive")
        if self.calendar_months is not None and self.calendar_months <= 0:
            raise ValueError("calendar_months must be positive")

    @property
    def effective_interval(self) -> timedelta:
        """Best-effort timedelta for display + display-only callers.

        For ``interval`` mode this is the literal interval. For
        ``calendar_months`` mode this is the longest-month upper
        bound (31 days per month) — a coarse approximation only
        suitable for human-readable labels. The orchestrator's
        state-machine age check uses
        :meth:`cadence_seconds_for_state_machine` instead, which is
        calendar-aware and does NOT collapse a 30-day February
        boundary into the 31-day approximation.
        """
        if self.interval is not None:
            return self.interval
        assert self.calendar_months is not None
        return timedelta(days=31 * self.calendar_months)

    def window_start(self, now: datetime) -> datetime:
        """Earliest ``started_at`` that still counts a layer as fresh.

        For ``interval`` mode this is ``now - interval``; the rolling
        window slides with ``now``. For ``calendar_months`` mode this
        is the first instant of the month ``calendar_months - 1``
        months before the month containing ``now``, in UTC. A monthly
        cadence (calendar_months=1) returns the first instant of the
        current calendar month UTC.

        The DB-facing state builder uses this to compute the
        cadence-aware "age boundary" so the orchestrator's
        ``DEGRADED`` rule fires on the day-1 calendar tick instead
        of after a 31-day rolling window.
        """
        if now.tzinfo is None:
            raise ValueError("Cadence.window_start requires a timezone-aware datetime")
        now_utc = now.astimezone(UTC)
        if self.interval is not None:
            return now_utc - self.interval
        assert self.calendar_months is not None
        # Walk back ``calendar_months - 1`` months from the current
        # month start. Handles year wrap (Jan with calendar_months=2
        # → previous Dec) without depending on dateutil.
        month_index = now_utc.year * 12 + (now_utc.month - 1) - (self.calendar_months - 1)
        anchor_year, anchor_month_zero_indexed = divmod(month_index, 12)
        return datetime(anchor_year, anchor_month_zero_indexed + 1, 1, tzinfo=UTC)

    def cadence_seconds_for_state_machine(self, now: datetime, grace_multiplier: float) -> float:
        """Effective cadence-window length in seconds for the
        ``compute_layer_state`` rule 9 (``age_seconds > cadence_seconds * grace_multiplier``).

        For ``interval`` mode this is just ``interval.total_seconds()`` —
        identical to the pre-#335 behavior. For ``calendar_months``
        mode this returns the seconds between ``now`` and
        :meth:`window_start`, divided by ``grace_multiplier`` so the
        existing rule 9 multiplication recovers the calendar
        boundary. The net result: monthly_reports flips to
        ``DEGRADED`` exactly at the day-1 UTC boundary, no grace
        applied (calendar boundaries are sharp by design).
        """
        if grace_multiplier <= 0:
            raise ValueError(f"grace_multiplier must be positive (got {grace_multiplier})")
        if self.interval is not None:
            return self.interval.total_seconds()
        # Calendar mode: pick a cadence_seconds such that
        # cadence_seconds * grace_multiplier == boundary_age.
        # ``max(..., 1.0)`` floors the value to one second so callers
        # that log or divide by ``cadence_seconds`` never see a zero
        # at the exact instant of the calendar tick (where
        # ``now == window_start`` and ``boundary_age`` is otherwise
        # 0). Rule 9's behavior is unchanged: any prior run still
        # has ``age_seconds > 0`` and gets DEGRADED.
        boundary_age = (now.astimezone(UTC) - self.window_start(now)).total_seconds()
        return max(boundary_age, 1.0) / grace_multiplier

    def grace_window(self, grace_multiplier: float) -> timedelta:
        if grace_multiplier <= 0:
            raise ValueError(f"grace_multiplier must be positive (got {grace_multiplier})")
        return self.effective_interval * grace_multiplier


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff_seconds: tuple[int, ...] = (60, 600, 3600)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if len(self.backoff_seconds) != self.max_attempts:
            raise ValueError(
                "backoff_seconds must have exactly max_attempts entries "
                f"(got {len(self.backoff_seconds)} for max_attempts={self.max_attempts})"
            )
        if any(b <= 0 for b in self.backoff_seconds):
            raise ValueError(f"backoff_seconds entries must all be positive (got {self.backoff_seconds})")


DEFAULT_RETRY_POLICY = RetryPolicy()


@dataclass(frozen=True)
class SecretRef:
    env_var: str
    display_name: str


class ContentPredicate(Protocol):
    """Structural signature for a per-layer content check.

    Returns (ok, detail). `ok=True` means the layer's data is
    considered content-current — for example, every Tier 1 ticker has a
    candle for today. `detail` is an operator-visible sentence surfaced
    when the predicate fails. Pure SELECT; must not write.
    """

    def __call__(self, conn: psycopg.Connection[Any]) -> tuple[bool, str]: ...


class LayerRefreshFailed(Exception):
    """Adapter-level failure carrying a categorisation.

    Adapters raise this so the executor can persist the category
    alongside the error message. Use this rather than `RuntimeError`
    when failing from inside a refresh adapter so downstream logging
    and the Admin UI can surface the taxonomy.
    """

    def __init__(self, category: FailureCategory, detail: str) -> None:
        super().__init__(f"{category.value}: {detail}")
        self.category = category
        self.detail = detail


def cadence_display_string(cadence: Cadence) -> str:
    """Short human label used by dashboards where a one-liner is enough."""
    if cadence.calendar_months is not None:
        m = cadence.calendar_months
        return "monthly" if m == 1 else f"{m}mo"
    assert cadence.interval is not None
    total = int(cadence.interval.total_seconds())
    if total % 86400 == 0:
        d = total // 86400
        # d == 1 → "daily" (spoken word); d > 1 → "Nd" (compact label).
        return f"{d}d" if d > 1 else "daily"
    if total % 3600 == 0:
        return f"{total // 3600}h"
    if total % 60 == 0:
        return f"{total // 60}m"
    return f"{total}s"
