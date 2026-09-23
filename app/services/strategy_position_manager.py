"""Exact-position, demo-only strategy risk manager (#2452).

The manager accepts a strategy trade and broker position id together. It never
looks up a position by instrument, so a manual position in the same instrument
is observational risk only and cannot be mutated. Only material PATCH/close
intents are persisted; unchanged bars and polling heartbeats write no rows.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, Decimal
from typing import Any, Literal, cast
from uuid import UUID, uuid4

import psycopg
import psycopg.rows
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

from app.providers.broker import (
    BrokerInstrumentEligibility,
    BrokerPosition,
    BrokerPositionMutationError,
    BrokerPositionMutationUncertain,
    BrokerProvider,
)
from app.services.broker_closed_release import (
    RELEASE_REASON,
    evaluate_whole_close,
    load_whole_close_evidence,
)
from app.services.core_exit_levels import (
    CORE_EXIT_MAX_QUOTE_AGE_SECONDS,
    core_exit_level_satisfied,
    core_exit_levels,
)
from app.services.strategy_control_plane import (
    PAPER_ALLOCATOR_ADVISORY_LOCK,
    StrategyControlError,
    StrategyOwnershipError,
    link_strategy_order,
)
from app.services.strategy_core_arc_sql import core_arm_authorised, core_arm_joins
from app.services.strategy_position_repair_streak import record_repair_visit

logger = logging.getLogger(__name__)

_RATE_QUANTUM = Decimal("0.000001")
_ADVISORY_HASH_SEED = 0


class StrategyPositionManagerError(StrategyControlError):
    """An exact-position mutation cannot safely proceed."""


@dataclass(frozen=True)
class RatchetBar:
    """Completed causal inputs for the registered hybrid ratchet formula."""

    completed_at: datetime
    level_known_at: datetime
    close: Decimal
    highest_close_since_entry: Decimal
    atr: Decimal
    broken_resistance: Decimal


@dataclass(frozen=True)
class PositionManagerResult:
    strategy_trade_id: int
    broker_position_id: int
    state: Literal["no_change", "submitted", "pending", "applied", "rejected", "reconcile_required"]
    reason_code: str
    position_operation_id: int | None = None


@dataclass(frozen=True)
class _OwnedPosition:
    ownership_id: int
    strategy_trade_id: int
    broker_position_id: int
    instrument_id: int
    # ⚠ The four fields below became nullable in #2603 step 2, when
    # ``strategy_trades`` gained the core/cash arm (sql/349).  A core position is
    # authorised by a mandate rebalance intent, so it has no deployment, no
    # entry preflight and no execution policy to read them from.
    #
    # ``is_core`` is the discriminator and is derived from the AUTHORISATION
    # column, never from these being NULL.  Null-by-absence would conflate "this
    # is a core holding" with "this deployment configured no such policy", and a
    # later default would then silently start applying strategy behaviour to a
    # mandate holding.
    is_core: bool
    deployment_id: int | None
    entry_stop: Decimal | None
    entry_take_profit: Decimal | None
    max_position_age_seconds: int | None
    max_quote_age_seconds: int | None
    ratchet_variant_id: int | None
    break_atr_multiple: Decimal | None
    chandelier_atr_multiple: Decimal | None
    structure_atr_multiple: Decimal | None
    quote_bid: Decimal | None
    quoted_at: datetime | None


@contextmanager
def _position_lock(conn: psycopg.Connection[Any], broker_position_id: int) -> Iterator[None]:
    # PostgreSQL's two-key advisory lock accepts signed 32-bit integers only,
    # while broker position ids are valid BIGINTs. Hash the namespaced identity
    # in PostgreSQL so every valid position id maps to one signed 64-bit key.
    lock_identity = f"strategy-position:{broker_position_id}"
    conn.execute(
        "SELECT pg_advisory_lock(hashtextextended(%s, %s))",
        (lock_identity, _ADVISORY_HASH_SEED),
    )
    conn.commit()
    try:
        yield
    finally:
        if conn.info.transaction_status != TransactionStatus.IDLE:
            conn.rollback()
        unlocked = conn.execute(
            "SELECT pg_advisory_unlock(hashtextextended(%s, %s))",
            (lock_identity, _ADVISORY_HASH_SEED),
        ).fetchone()
        conn.commit()
        if unlocked is None or unlocked[0] is not True:
            raise StrategyPositionManagerError("strategy position lock ownership was lost")


@contextmanager
def _paper_allocator_lock(conn: psycopg.Connection[Any]) -> Iterator[None]:
    """Serialize exact-position exits with every shared-pot commitment."""
    conn.execute("SELECT pg_advisory_lock(%s, %s)", PAPER_ALLOCATOR_ADVISORY_LOCK)
    conn.commit()
    try:
        yield
    finally:
        if conn.info.transaction_status != TransactionStatus.IDLE:
            conn.rollback()
        unlocked = conn.execute("SELECT pg_advisory_unlock(%s, %s)", PAPER_ALLOCATOR_ADVISORY_LOCK).fetchone()
        conn.commit()
        if unlocked is None or unlocked[0] is not True:
            raise StrategyPositionManagerError("paper allocator lock ownership was lost")


def register_ratchet_variant(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    promotion_id: int,
    rule_version: str,
    break_atr_multiple: Decimal,
    chandelier_atr_multiple: Decimal,
    structure_atr_multiple: Decimal,
    registered_by: str,
    reason: str,
) -> int:
    """Register immutable formula constants against a promoted backtest arm."""
    row = conn.execute(
        """
        SELECT p.strategy_id, p.strategy_version, p.to_stage,
               count(pr.result_id) AS result_count,
               count(pr.result_id) FILTER (
                   WHERE r.position_rule_set_version = %s
                     AND r.namespace = 'hold_out'
                     AND r.window_start >= DATE '2022-01-01'
               ) AS matching_recent_holdout_count
        FROM strategy_promotions p
        LEFT JOIN strategy_promotion_results pr ON pr.promotion_id=p.promotion_id
        LEFT JOIN strategy_results_store r ON r.result_id=pr.result_id
        WHERE p.promotion_id=%s
        GROUP BY p.promotion_id
        """,
        (rule_version, promotion_id),
    ).fetchone()
    if row is None or row[0] != strategy_id or row[1] != strategy_version:
        raise StrategyPositionManagerError("ratchet registration must match its promoted strategy variant")
    if (
        row[2] not in ("historical_validated", "forward_observation", "paper_enabled", "live_enabled")
        or int(row[3]) < 1
        or int(row[4]) < 1
    ):
        raise StrategyPositionManagerError("ratchet requires a promoted 2022+ hold-out backtest arm")
    inserted = conn.execute(
        """
        INSERT INTO strategy_ratchet_variants (
            strategy_id, strategy_version, promotion_id, rule_version,
            break_atr_multiple, chandelier_atr_multiple, structure_atr_multiple,
            registered_by, reason
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING ratchet_variant_id
        """,
        (
            strategy_id,
            strategy_version,
            promotion_id,
            rule_version,
            break_atr_multiple,
            chandelier_atr_multiple,
            structure_atr_multiple,
            registered_by,
            reason,
        ),
    ).fetchone()
    assert inserted is not None
    return int(inserted[0])


def configure_position_manager(
    conn: psycopg.Connection[Any],
    *,
    deployment_id: int,
    max_position_age_seconds: int | None,
    ratchet_variant_id: int | None,
    updated_by: str,
    reason: str,
) -> int:
    """Replace current paper-manager policy and append one bounded audit event."""
    if max_position_age_seconds is not None and max_position_age_seconds <= 0:
        raise ValueError("max_position_age_seconds must be positive")
    deployment = conn.execute(
        "SELECT strategy_id, strategy_version, mode FROM strategy_deployments WHERE deployment_id=%s FOR UPDATE",
        (deployment_id,),
    ).fetchone()
    if deployment is None or deployment[2] != "paper":
        raise StrategyPositionManagerError("the MVP position manager requires a paper deployment")
    if ratchet_variant_id is not None:
        variant = conn.execute(
            "SELECT strategy_id, strategy_version FROM strategy_ratchet_variants WHERE ratchet_variant_id=%s",
            (ratchet_variant_id,),
        ).fetchone()
        if variant is None or tuple(variant) != tuple(deployment[:2]):
            raise StrategyPositionManagerError("ratchet variant must match the deployment strategy identity")
    prior = conn.execute(
        "SELECT revision FROM strategy_position_manager_policies WHERE deployment_id=%s",
        (deployment_id,),
    ).fetchone()
    revision = int(prior[0]) + 1 if prior else 1
    conn.execute(
        """
        INSERT INTO strategy_position_manager_policies (
            deployment_id, revision, max_position_age_seconds,
            ratchet_variant_id, updated_by, reason
        ) VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (deployment_id) DO UPDATE SET
            revision=EXCLUDED.revision,
            max_position_age_seconds=EXCLUDED.max_position_age_seconds,
            ratchet_variant_id=EXCLUDED.ratchet_variant_id,
            updated_by=EXCLUDED.updated_by,
            reason=EXCLUDED.reason,
            updated_at=now()
        """,
        (deployment_id, revision, max_position_age_seconds, ratchet_variant_id, updated_by, reason),
    )
    conn.execute(
        """
        INSERT INTO strategy_position_manager_policy_events (
            deployment_id, revision, max_position_age_seconds,
            ratchet_variant_id, changed_by, reason
        ) VALUES (%s,%s,%s,%s,%s,%s)
        """,
        (deployment_id, revision, max_position_age_seconds, ratchet_variant_id, updated_by, reason),
    )
    return revision


def calculate_ratchet_stop(
    *,
    current_stop: Decimal,
    bar: RatchetBar,
    break_atr_multiple: Decimal,
    chandelier_atr_multiple: Decimal,
    structure_atr_multiple: Decimal,
) -> Decimal | None:
    """Return the registered causal candidate, or ``None`` when it did not fire."""
    values = (
        current_stop,
        bar.close,
        bar.highest_close_since_entry,
        bar.atr,
        bar.broken_resistance,
        break_atr_multiple,
        chandelier_atr_multiple,
        structure_atr_multiple,
    )
    if any(not value.is_finite() or value <= 0 for value in values):
        raise ValueError("ratchet prices and constants must be finite and positive")
    if bar.completed_at.tzinfo is None or bar.level_known_at.tzinfo is None:
        raise ValueError("ratchet timestamps must be timezone aware")
    if bar.level_known_at > bar.completed_at:
        raise ValueError("the resistance level was not causal at bar completion")
    if bar.highest_close_since_entry < bar.close:
        raise ValueError("highest close since entry cannot be below the completed close")
    if bar.close < bar.broken_resistance + break_atr_multiple * bar.atr:
        return None
    candidate = min(
        bar.highest_close_since_entry - chandelier_atr_multiple * bar.atr,
        bar.broken_resistance - structure_atr_multiple * bar.atr,
    ).quantize(_RATE_QUANTUM, rounding=ROUND_DOWN)
    return candidate if candidate > current_stop else None


# The manager's loader.  Signal-arm behaviour is unchanged: the former INNER
# chain (funding -> deployment mode='paper' -> preflight verdict='allocated' ->
# execution policy) is reproduced exactly by the LEFT chain plus the four
# witnesses below.  Converting an INNER JOIN that also FILTERS into a LEFT JOIN
# moves that filter into the WHERE clause or deletes it; there is no third
# outcome, so each is restated rather than assumed.
_LOAD_OWNED_SQL = f"""
            SELECT own.ownership_id, own.strategy_trade_id, own.broker_position_id,
                   t.instrument_id, d.deployment_id,
                   t.core_rebalance_intent_id,
                   pre.stop_loss_rate AS entry_stop,
                   pre.take_profit_rate AS entry_take_profit,
                   manager.max_position_age_seconds,
                   execution.max_quote_age_seconds,
                   manager.ratchet_variant_id,
                   variant.break_atr_multiple,
                   variant.chandelier_atr_multiple,
                   variant.structure_atr_multiple,
                   q.bid AS quote_bid, q.quoted_at
            FROM strategy_position_ownership own
            JOIN strategy_trades t ON t.strategy_trade_id=own.strategy_trade_id
            LEFT JOIN strategy_funding_decisions funding
              ON funding.funding_decision_id=t.funding_decision_id
            LEFT JOIN strategy_deployments d
              ON d.deployment_id=funding.deployment_id AND d.mode='paper'
            LEFT JOIN strategy_entry_preflights pre
              ON pre.signal_id=funding.signal_id AND pre.verdict='allocated'
            LEFT JOIN strategy_execution_policies execution
              ON execution.deployment_id=d.deployment_id
{core_arm_joins("t")}
            LEFT JOIN strategy_position_manager_policies manager ON manager.deployment_id=d.deployment_id
            LEFT JOIN strategy_ratchet_variants variant
              ON variant.ratchet_variant_id=manager.ratchet_variant_id
            LEFT JOIN quotes q ON q.instrument_id=t.instrument_id
            WHERE own.strategy_trade_id=%s AND own.broker_position_id=%s AND own.status='active'
              AND t.status IN ('open','closing','reconcile_required')
              AND (
                (
                  -- ⚠ EVERY LINK NEEDS ITS OWN WITNESS.  `pre` joins on
                  -- funding.signal_id and is independent of `d`, so witnessing
                  -- `pre` alone would let a LIVE deployment load (d NULL, pre
                  -- resolved).  `execution` likewise carries the quote-age
                  -- policy that the casts below assume; without its own witness
                  -- a signal trade could load with max_quote_age_seconds NULL
                  -- and fail at the quote check instead of never loading.
                  t.funding_decision_id IS NOT NULL
                  AND d.deployment_id IS NOT NULL          -- carries mode='paper'
                  AND execution.deployment_id IS NOT NULL  -- carries the quote-age policy
                  AND pre.signal_id IS NOT NULL            -- carries verdict='allocated'
                )
                OR (
                  {core_arm_authorised("t")}
                  -- ⚠ The arc alone does not bind the trade's instrument to the
                  -- intent's.  Without this, a malformed core trade would
                  -- authorise the manager to close a DIFFERENT instrument's
                  -- position.  The two live in different tables, so this is a
                  -- load-time predicate rather than a CHECK -- and here is where
                  -- the manager is about to act on it.
                  AND core_intent.core_instrument_id=t.instrument_id
                )
              )
"""


def _load_owned(conn: psycopg.Connection[Any], *, strategy_trade_id: int, broker_position_id: int) -> _OwnedPosition:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            _LOAD_OWNED_SQL,
            (strategy_trade_id, broker_position_id),
        )
        row = cur.fetchone()
    if row is None:
        raise StrategyOwnershipError("exact active strategy position ownership is required before broker I/O")
    is_core = row["core_rebalance_intent_id"] is not None
    return _OwnedPosition(
        ownership_id=int(row["ownership_id"]),
        strategy_trade_id=int(row["strategy_trade_id"]),
        broker_position_id=int(row["broker_position_id"]),
        instrument_id=int(row["instrument_id"]),
        is_core=is_core,
        deployment_id=int(row["deployment_id"]) if row["deployment_id"] is not None else None,
        entry_stop=Decimal(str(row["entry_stop"])) if row["entry_stop"] is not None else None,
        entry_take_profit=(Decimal(str(row["entry_take_profit"])) if row["entry_take_profit"] is not None else None),
        max_position_age_seconds=(
            int(row["max_position_age_seconds"]) if row["max_position_age_seconds"] is not None else None
        ),
        max_quote_age_seconds=(int(row["max_quote_age_seconds"]) if row["max_quote_age_seconds"] is not None else None),
        ratchet_variant_id=(int(row["ratchet_variant_id"]) if row["ratchet_variant_id"] is not None else None),
        break_atr_multiple=(Decimal(str(row["break_atr_multiple"])) if row["break_atr_multiple"] is not None else None),
        chandelier_atr_multiple=(
            Decimal(str(row["chandelier_atr_multiple"])) if row["chandelier_atr_multiple"] is not None else None
        ),
        structure_atr_multiple=(
            Decimal(str(row["structure_atr_multiple"])) if row["structure_atr_multiple"] is not None else None
        ),
        quote_bid=Decimal(str(row["quote_bid"])) if row["quote_bid"] is not None else None,
        quoted_at=cast(datetime | None, row["quoted_at"]),
    )


def _exact_broker_position(broker: BrokerProvider, owned: _OwnedPosition) -> BrokerPosition | None:
    portfolio = broker.get_portfolio()
    matches = [position for position in portfolio.positions if position.position_id == owned.broker_position_id]
    if len(matches) > 1:
        raise StrategyPositionManagerError("broker returned duplicate exact position ids")
    if matches and matches[0].instrument_id != owned.instrument_id:
        raise StrategyPositionManagerError("owned broker position changed instrument identity")
    return matches[0] if matches else None


def _eligibility_for_owned(broker: BrokerProvider, owned: _OwnedPosition) -> BrokerInstrumentEligibility:
    response = broker.check_instrument_eligibility([owned.instrument_id])
    matches = [row for row in response.eligibilities if row.instrument_id == owned.instrument_id]
    if response.currency.upper() != "USD" or len(matches) != 1:
        raise StrategyPositionManagerError("current broker eligibility is unresolved")
    return matches[0]


def _terminal(
    conn: psycopg.Connection[Any],
    *,
    operation_id: int,
    status: Literal["applied", "rejected", "reconcile_required"],
    error_code: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE strategy_position_operations
        SET status=%s, last_error_code=%s, resolved_at=now(), updated_at=now()
        WHERE position_operation_id=%s AND status IN ('intent_persisted','submitting','submitted')
        """,
        (status, error_code, operation_id),
    )


def _finish_close(conn: psycopg.Connection[Any], *, owned: _OwnedPosition, operation_id: int, reason: str) -> None:
    _terminal(conn, operation_id=operation_id, status="applied")
    conn.execute(
        """
        UPDATE strategy_position_ownership
        SET status='released', released_at=now(), release_reason=%s
        WHERE ownership_id=%s AND status='active'
        """,
        (reason, owned.ownership_id),
    )
    remaining = conn.execute(
        "SELECT count(*) FROM strategy_position_ownership WHERE strategy_trade_id=%s AND status='active'",
        (owned.strategy_trade_id,),
    ).fetchone()
    assert remaining is not None
    conn.execute(
        "UPDATE strategy_trades SET status=%s, updated_at=now() WHERE strategy_trade_id=%s",
        ("closed" if int(remaining[0]) == 0 else "open", owned.strategy_trade_id),
    )


def _release_whole_broker_close(conn: psycopg.Connection[Any], *, owned: _OwnedPosition, observed_at: datetime) -> bool:
    """Release an absent position's ownership iff the broker recorded ONE whole close (#2965).

    ⚠ Unlike ``_finish_close`` this writes ``released_at`` as the broker's close time, not
    ``now()`` -- NAV is marked by that date -- and it never writes the trade ``open``: a
    sibling ownership's own visit decides that.  Any refusal leaves today's behaviour
    (``reconcile_required``) for a human; the reason is logged with the witness ids.
    """
    evidence = load_whole_close_evidence(
        conn,
        ownership_id=owned.ownership_id,
        strategy_trade_id=owned.strategy_trade_id,
        broker_position_id=owned.broker_position_id,
        instrument_id=owned.instrument_id,
    )
    conn.commit()
    verdict = evaluate_whole_close(evidence, observed_at=observed_at)
    if not verdict.release or verdict.released_at is None:
        logger.info(
            "strategy position %d (trade %d) absent; whole-close release refused: %s",
            owned.broker_position_id,
            owned.strategy_trade_id,
            verdict.reason_code,
        )
        return False
    with conn.transaction():
        released = conn.execute(
            """
            UPDATE strategy_position_ownership
            SET status='released', released_at=%s, release_reason=%s
            WHERE ownership_id=%s AND status='active'
            RETURNING ownership_id
            """,
            (verdict.released_at, RELEASE_REASON, owned.ownership_id),
        ).fetchone()
        if released is None:
            return False
        conn.execute(
            "SELECT 1 FROM strategy_trades WHERE strategy_trade_id=%s FOR UPDATE",
            (owned.strategy_trade_id,),
        )
        remaining = conn.execute(
            "SELECT count(*) FROM strategy_position_ownership WHERE strategy_trade_id=%s AND status='active'",
            (owned.strategy_trade_id,),
        ).fetchone()
        assert remaining is not None
        if int(remaining[0]) == 0:
            conn.execute(
                "UPDATE strategy_trades SET status='closed', updated_at=now() WHERE strategy_trade_id=%s",
                (owned.strategy_trade_id,),
            )
    logger.info(
        "strategy position %d (trade %d) released: broker closed it whole at %s",
        owned.broker_position_id,
        owned.strategy_trade_id,
        verdict.released_at.isoformat(),
    )
    return True


def _persist_operation_response(
    conn: psycopg.Connection[Any], *, operation_id: int, raw_payload: dict[str, Any]
) -> None:
    """Persist the untouched broker object before its adapter normalises it."""
    with conn.transaction():
        conn.execute(
            "UPDATE strategy_position_operations SET broker_response_json=%s, updated_at=now() "
            "WHERE position_operation_id=%s",
            (Jsonb(raw_payload), operation_id),
        )


def _persist_order_response(conn: psycopg.Connection[Any], *, order_id: int, raw_payload: dict[str, Any]) -> None:
    """Persist one close-submission response on its already-durable order."""
    with conn.transaction():
        conn.execute(
            "UPDATE orders SET raw_payload_json=%s WHERE order_id=%s",
            (Jsonb(raw_payload), order_id),
        )


def _edit_landed(
    *,
    owned: _OwnedPosition,
    position: BrokerPosition | None,
    desired_stop: Any,
    desired_take: Any,
) -> bool:
    """Did the broker's exact position reach the rates this operation intended?

    ⚠⚠ **The comparison must use the SAME satisfaction rule the arm evaluates with**,
    and on the core arm that rule carries a one-cent tolerance
    (``core_exit_level_satisfied``).  Comparing with exact equality here while deciding
    with a tolerance there is not a stricter check, it is an INCONSISTENT one, and
    Codex checkpoint 2 traced both of its outcomes: an ``intent_persisted`` edit whose
    rates landed one cent off would be recorded ``reconcile_required`` although it
    applied, and a ``submitted`` one would stay ``broker_edit_pending`` forever --
    which, because ``_resume_operation`` runs BEFORE close handling, would also make
    the position **unclosable**.  A tolerance that only one side of the system honours
    is worse than no tolerance at all.

    The signal arm keeps exact equality: its rates come from a stored preflight and it
    has no tolerance to be inconsistent with.
    """
    if position is None:
        return False
    stop = Decimal(str(desired_stop))
    if owned.is_core:
        if not core_exit_level_satisfied(observed=position.stop_loss_rate, desired=stop):
            return False
        if desired_take is None:
            return True
        return core_exit_level_satisfied(observed=position.take_profit_rate, desired=Decimal(str(desired_take)))
    if position.stop_loss_rate != stop:
        return False
    return desired_take is None or position.take_profit_rate == Decimal(str(desired_take))


def _resume_operation(
    conn: psycopg.Connection[Any], *, broker: BrokerProvider, owned: _OwnedPosition, observed_at: datetime
) -> PositionManagerResult | None:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT * FROM strategy_position_operations
            WHERE ownership_id=%s AND status IN ('intent_persisted','submitting','submitted')
            ORDER BY position_operation_id DESC LIMIT 1
            """,
            (owned.ownership_id,),
        )
        operation = cur.fetchone()
    conn.commit()
    if operation is None:
        return None
    operation_id = int(operation["position_operation_id"])
    # ⚠⚠ A core edit operation USED TO BE REFUSED HERE, and #3284 is exactly why
    # that refusal had to go rather than be kept as a safety net.  Its stated
    # premise was "no such operation can be created today (nothing writes an edit
    # on the core arm)", which `manage_owned_position` now falsifies: the core arm
    # writes `fixed_exit_repair`.  Left standing, the refusal would have caught
    # every core repair that crashed mid-flight and terminalised it as `rejected`
    # -- including one the broker had already APPLIED, which is the one outcome
    # that must never be recorded as rejected.
    #
    # Nothing replaces it.  The generic resume below compares the broker's exact
    # position against the persisted intent, and that comparison is arm-agnostic:
    # it asks what the broker did, not which arm asked.
    position = _exact_broker_position(broker, owned)
    if operation["status"] == "intent_persisted" and operation["operation_type"] == "close":
        # `mark_close_submitting` commits BEFORE the broker verb is entered, so a
        # close still sitting at `intent_persisted` never reached it (#2979).  That
        # is a fact about our own write ordering, not an inference about the broker,
        # which is what makes it safe to terminalise cleanly: the request is
        # abandoned, the POSITION is untouched, and the trade goes back to `open`
        # rather than carrying a reconciliation the operator would have to clear.
        #
        # ⚠ The position is NOT re-closed here.  Abandoning the request leaves the
        # scheduled cycle free to request a fresh close on its own terms; inventing
        # one inside a recovery path would make a crash trigger a broker mutation.
        with conn.transaction():
            conn.execute(
                "UPDATE orders SET status='rejected' WHERE order_id=%s",
                (operation["order_id"],),
            )
            _terminal(
                conn,
                operation_id=operation_id,
                status="rejected",
                error_code="close_never_submitted",
            )
            conn.execute(
                "UPDATE strategy_trades SET status='open', updated_at=now() WHERE strategy_trade_id=%s",
                (owned.strategy_trade_id,),
            )
        return PositionManagerResult(
            owned.strategy_trade_id,
            owned.broker_position_id,
            "rejected",
            "close_never_submitted",
            operation_id,
        )
    if operation["status"] in ("intent_persisted", "submitting"):
        # An EDIT has no marker, so it arrives here at `intent_persisted` and is
        # resolved by comparing the broker's exact position to the intent, as before.
        #
        # A CLOSE arrives here only as `submitting`: the verb WAS entered, and there
        # is no close lookup by request UUID, so what the broker did stays unknown.
        # That ambiguity is #2979's remaining half and is unresolved by design here
        # — it is not guessed at.
        landed = operation["operation_type"] != "close" and _edit_landed(
            owned=owned,
            position=position,
            desired_stop=operation["desired_stop_rate"],
            desired_take=operation["desired_take_profit_rate"],
        )
        with conn.transaction():
            if landed:
                _terminal(conn, operation_id=operation_id, status="applied")
            else:
                _terminal(
                    conn,
                    operation_id=operation_id,
                    status="reconcile_required",
                    error_code="crash_before_submission_identity",
                )
                conn.execute(
                    "UPDATE strategy_trades SET status='reconcile_required', updated_at=now() "
                    "WHERE strategy_trade_id=%s",
                    (owned.strategy_trade_id,),
                )
        # ⚠ Bound ONCE and shared with the recorder below, not repeated at each call site:
        # the streak's `last_refusal_reason` and the returned `reason_code` must be the
        # same string, and two copies of a literal are two things to rename.
        resumed_state: Literal["applied", "reconcile_required"] = "applied" if landed else "reconcile_required"
        resumed_reason = "broker_state_matches_intent" if landed else "crash_before_submission_identity"
        # #3284 item 4a — recorded HERE rather than at the caller, because this function
        # holds the exact position it already fetched. See `_record_resumed_repair_visit`.
        _record_resumed_repair_visit(
            conn,
            owned=owned,
            operation_type=str(operation["operation_type"]),
            position=position,
            state=resumed_state,
            reason_code=resumed_reason,
            observed_at=observed_at,
        )
        return PositionManagerResult(
            owned.strategy_trade_id,
            owned.broker_position_id,
            resumed_state,
            resumed_reason,
            operation_id,
        )
    if operation["operation_type"] == "close":
        try:
            detail = broker.get_close_order(
                order_id=str(operation["broker_order_ref"]),
                persist_response=lambda raw: _persist_operation_response(
                    conn, operation_id=operation_id, raw_payload=raw
                ),
            )
        except BrokerPositionMutationError:
            return PositionManagerResult(
                owned.strategy_trade_id, owned.broker_position_id, "pending", "close_lookup_unavailable", operation_id
            )
        if detail.status == "pending":
            return PositionManagerResult(
                owned.strategy_trade_id, owned.broker_position_id, "pending", "broker_close_pending", operation_id
            )
        exact_reference = detail.reference_id is None or detail.reference_id == operation["request_id"]
        # #3007 half 2 (Codex checkpoint 2, P2): a close-order response naming a
        # DIFFERENT instrument is the broker answering about something else, and
        # this branch releases ownership.
        #
        # ⚠ An ABSENT ``instrument_id`` is tolerated here and REFUSED in
        # ``order_client._poll_one_pending_order``, deliberately. The asymmetry is
        # the witness each path already holds: this one matches on the exact
        # ``broker_position_id`` it owns, which names one instrument by
        # construction, so the instrument is a corroborating check rather than
        # the identity. The poller has no position to match on — its only
        # identity is the order id — so for it a missing instrument is the whole
        # of the check and has to fail closed. The field has been observed on
        # this route once, through our own parser, which is why neither path
        # makes absence an error.
        exact_instrument = detail.instrument_id is None or detail.instrument_id == owned.instrument_id
        exact = (
            detail.status == "filled"
            and detail.position_ids == (owned.broker_position_id,)
            and exact_reference
            and exact_instrument
        )
        with conn.transaction():
            order_status = "filled" if exact else "rejected"
            conn.execute("UPDATE orders SET status=%s WHERE order_id=%s", (order_status, operation["order_id"]))
            if exact:
                _finish_close(
                    conn,
                    owned=owned,
                    operation_id=operation_id,
                    reason=str(operation["trigger_code"]),
                )
            else:
                _terminal(
                    conn,
                    operation_id=operation_id,
                    status="reconcile_required",
                    error_code="close_order_did_not_affect_exact_position",
                )
                conn.execute(
                    "UPDATE strategy_trades SET status='reconcile_required', updated_at=now() "
                    "WHERE strategy_trade_id=%s",
                    (owned.strategy_trade_id,),
                )
        return PositionManagerResult(
            owned.strategy_trade_id,
            owned.broker_position_id,
            "applied" if exact else "reconcile_required",
            "exact_position_closed" if exact else "close_order_did_not_affect_exact_position",
            operation_id,
        )
    landed = _edit_landed(
        owned=owned,
        position=position,
        desired_stop=operation["desired_stop_rate"],
        desired_take=operation["desired_take_profit_rate"],
    )
    if not landed:
        # #3284 item 4a — THE case the counter exists for. This branch never terminalises
        # the operation, so a submitted edit whose levels never arrive returns `pending`
        # here forever while `idx_strategy_position_one_unresolved_operation` blocks any
        # fresh repair. Unrecorded, that is a permanently naked position reading zero.
        pending_reason = "broker_edit_pending"
        _record_resumed_repair_visit(
            conn,
            owned=owned,
            operation_type=str(operation["operation_type"]),
            position=position,
            state="pending",
            reason_code=pending_reason,
            observed_at=observed_at,
        )
        return PositionManagerResult(
            owned.strategy_trade_id, owned.broker_position_id, "pending", pending_reason, operation_id
        )
    with conn.transaction():
        _terminal(conn, operation_id=operation_id, status="applied")
    applied_reason = "broker_edit_applied"
    _record_resumed_repair_visit(
        conn,
        owned=owned,
        operation_type=str(operation["operation_type"]),
        position=position,
        state="applied",
        reason_code=applied_reason,
        observed_at=observed_at,
    )
    return PositionManagerResult(
        owned.strategy_trade_id, owned.broker_position_id, "applied", applied_reason, operation_id
    )


def _persist_edit_intent(
    conn: psycopg.Connection[Any],
    *,
    owned: _OwnedPosition,
    operation_type: Literal["fixed_exit_repair", "stop_ratchet"],
    trigger_code: Literal["entry_exit_gap", "causal_resistance_break"],
    prior_stop: Decimal | None,
    desired_stop: Decimal,
    desired_take: Decimal | None,
    bar: RatchetBar | None,
) -> tuple[int, UUID]:
    request_id = uuid4()
    row = conn.execute(
        """
        INSERT INTO strategy_position_operations (
            ownership_id, operation_type, trigger_code, request_id, status,
            prior_stop_rate, desired_stop_rate, desired_take_profit_rate,
            completed_bar_at, level_known_at, close_rate,
            highest_close_since_entry, atr_rate, resistance_rate,
            ratchet_variant_id
        ) VALUES (%s,%s,%s,%s,'intent_persisted',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING position_operation_id
        """,
        (
            owned.ownership_id,
            operation_type,
            trigger_code,
            request_id,
            prior_stop,
            desired_stop,
            desired_take,
            bar.completed_at if bar else None,
            bar.level_known_at if bar else None,
            bar.close if bar else None,
            bar.highest_close_since_entry if bar else None,
            bar.atr if bar else None,
            bar.broken_resistance if bar else None,
            owned.ratchet_variant_id if bar else None,
        ),
    ).fetchone()
    assert row is not None
    return int(row[0]), request_id


def _prior_same_edit(
    conn: psycopg.Connection[Any],
    *,
    owned: _OwnedPosition,
    operation_type: Literal["fixed_exit_repair", "stop_ratchet"],
    desired_stop: Decimal,
    desired_take: Decimal | None,
    bar: RatchetBar | None,
) -> PositionManagerResult | None:
    # ⚠⚠ ON THE CORE ARM AN `applied` PRIOR OPERATION MUST NOT BLOCK, and that
    # exclusion is what makes #3284 item 3 true rather than aspirational.  The desired
    # rates are a pure function of the position's entry, so they are STABLE across
    # cycles -- which means a repair that applied last week matches today's intent
    # exactly.  Without this clause, the very scenario the ticket names as acceptance
    # ("clearing the stop manually at the broker is detected on the next check and
    # repaired") would find that applied row and return `rejected` /
    # `prior_material_operation`, and the position would stay NAKED indefinitely while
    # reporting a terminal state.  Codex checkpoint 2 found it; an add at an unchanged
    # entry price is a second route to the same wedge.
    #
    # A `rejected` or `reconcile_required` prior still blocks, on both arms: the broker
    # refused that exact edit, and re-entering the same verb every five minutes is
    # hammering, not repair.
    #
    # ⚠ The SIGNAL arm is deliberately left as it was.  It has the same latent wedge --
    # `max(current_stop, entry_stop)` also reproduces a stable pair -- but changing it
    # alters alpha-arm behaviour that nothing in this ticket exercises.  Noted on the
    # PR rather than fixed in passing.
    #
    # ⚠ Expressed as a PARAMETER, not an interpolated fragment: psycopg types
    # ``execute`` to ``LiteralString``, so an f-string here fails pyright -- and the
    # parameter form is the one that cannot become an injection site later.
    row = conn.execute(
        """
        SELECT position_operation_id, status,
               COALESCE(last_error_code, 'prior_material_operation')
        FROM strategy_position_operations
        WHERE ownership_id=%s AND operation_type=%s
          AND desired_stop_rate=%s
          AND desired_take_profit_rate IS NOT DISTINCT FROM %s
          AND completed_bar_at IS NOT DISTINCT FROM %s
          AND (status <> 'applied' OR NOT %s)
        ORDER BY position_operation_id DESC LIMIT 1
        """,
        (
            owned.ownership_id,
            operation_type,
            desired_stop,
            desired_take,
            bar.completed_at if bar else None,
            owned.is_core,
        ),
    ).fetchone()
    if row is None:
        return None
    state: Literal["rejected", "reconcile_required"] = (
        "reconcile_required" if row[1] == "reconcile_required" else "rejected"
    )
    return PositionManagerResult(
        owned.strategy_trade_id,
        owned.broker_position_id,
        state,
        str(row[2]),
        int(row[0]),
    )


def _submit_edit(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    owned: _OwnedPosition,
    operation_id: int,
    request_id: UUID,
    desired_stop: Decimal,
    desired_take: Decimal | None,
) -> PositionManagerResult:
    try:
        submission = broker.edit_demo_strategy_position(
            position_id=owned.broker_position_id,
            stop_loss_rate=desired_stop,
            take_profit_rate=desired_take,
            request_id=request_id,
            persist_response=lambda raw: _persist_operation_response(conn, operation_id=operation_id, raw_payload=raw),
        )
    except BrokerPositionMutationError as exc:
        uncertain = isinstance(exc, BrokerPositionMutationUncertain)
        with conn.transaction():
            _terminal(
                conn,
                operation_id=operation_id,
                status="reconcile_required" if uncertain else "rejected",
                error_code="broker_edit_uncertain" if uncertain else "broker_edit_rejected",
            )
            if uncertain:
                conn.execute(
                    "UPDATE strategy_trades SET status='reconcile_required', updated_at=now() "
                    "WHERE strategy_trade_id=%s",
                    (owned.strategy_trade_id,),
                )
        return PositionManagerResult(
            owned.strategy_trade_id,
            owned.broker_position_id,
            "reconcile_required" if uncertain else "rejected",
            "broker_edit_uncertain" if uncertain else "broker_edit_rejected",
            operation_id,
        )
    with conn.transaction():
        conn.execute(
            """
            UPDATE strategy_position_operations
            SET status='submitted', broker_operation_id=%s, submitted_at=now(), updated_at=now()
            WHERE position_operation_id=%s AND status='intent_persisted'
            """,
            (submission.operation_id, operation_id),
        )
    # The 202 response is acceptance only. A future invocation re-syncs the
    # exact position before changing this operation to applied.
    return PositionManagerResult(
        owned.strategy_trade_id, owned.broker_position_id, "submitted", "broker_edit_accepted", operation_id
    )


def mark_close_submitting(conn: psycopg.Connection[Any], *, operation_id: int) -> None:
    """Commit "the broker verb is about to be entered" BEFORE entering it.

    This is the whole of #2979's separation, and it is durable-before-the-call by
    construction: the UPDATE commits, and only then does ``_submit_close`` touch the
    broker.  So a crash leaving ``intent_persisted`` PROVES the call was never
    entered, while ``submitting`` proves only that it was -- never what the broker
    then did.

    ⚠ Module-level and public rather than inlined, because the #2949 harness has no
    other way to place a fault between the two commits.  A fault it can only arm by
    name is the difference between testing the ordering and asserting it.
    """
    with conn.transaction():
        conn.execute(
            "UPDATE strategy_position_operations SET status='submitting', updated_at=now() "
            "WHERE position_operation_id=%s AND status='intent_persisted'",
            (operation_id,),
        )


def _submit_close(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    owned: _OwnedPosition,
    trigger_code: Literal["timeout", "strategy_exit", "emergency_risk", "operator_close"],
) -> PositionManagerResult:
    request_id = uuid4()
    with conn.transaction():
        order = conn.execute(
            """
            INSERT INTO orders (
                instrument_id, action, order_type, status, raw_payload_json,
                execution_origin, strategy_request_id
            ) VALUES (%s,'EXIT','MARKET','submitted',NULL,'strategy',%s)
            RETURNING order_id
            """,
            (owned.instrument_id, request_id),
        ).fetchone()
        assert order is not None
        order_id = int(order[0])
        link_strategy_order(conn, strategy_trade_id=owned.strategy_trade_id, order_id=order_id, purpose="exit")
        operation = conn.execute(
            """
            INSERT INTO strategy_position_operations (
                ownership_id, order_id, operation_type, trigger_code, request_id, status
            ) VALUES (%s,%s,'close',%s,%s,'intent_persisted')
            RETURNING position_operation_id
            """,
            (owned.ownership_id, order_id, trigger_code, request_id),
        ).fetchone()
        assert operation is not None
        operation_id = int(operation[0])
        conn.execute(
            "UPDATE strategy_trades SET status='closing', updated_at=now() WHERE strategy_trade_id=%s",
            (owned.strategy_trade_id,),
        )
    # ⚠ Its own committed transaction, deliberately not folded into the intent
    # above: folding them would make the marker commit WITH the intent, so every
    # close would read as "may have reached the broker" and the separation would
    # be vacuous.  The whole value is that these two commits are distinct points.
    mark_close_submitting(conn, operation_id=operation_id)
    try:
        submission = broker.close_demo_strategy_position(
            position_id=owned.broker_position_id,
            instrument_id=owned.instrument_id,
            request_id=request_id,
            persist_response=lambda raw: _persist_order_response(conn, order_id=order_id, raw_payload=raw),
        )
    except BrokerPositionMutationError as exc:
        uncertain = isinstance(exc, BrokerPositionMutationUncertain)
        with conn.transaction():
            conn.execute(
                "UPDATE orders SET status=%s WHERE order_id=%s",
                ("submitted" if uncertain else "rejected", order_id),
            )
            _terminal(
                conn,
                operation_id=operation_id,
                status="reconcile_required" if uncertain else "rejected",
                error_code="broker_close_uncertain" if uncertain else "broker_close_rejected",
            )
            conn.execute(
                "UPDATE strategy_trades SET status=%s, updated_at=now() WHERE strategy_trade_id=%s",
                ("reconcile_required" if uncertain else "open", owned.strategy_trade_id),
            )
        return PositionManagerResult(
            owned.strategy_trade_id,
            owned.broker_position_id,
            "reconcile_required" if uncertain else "rejected",
            "broker_close_uncertain" if uncertain else "broker_close_rejected",
            operation_id,
        )
    with conn.transaction():
        conn.execute(
            "UPDATE orders SET broker_order_ref=%s WHERE order_id=%s",
            (submission.broker_order_ref, order_id),
        )
        conn.execute(
            """
            UPDATE strategy_position_operations
            SET status='submitted', broker_order_ref=%s, submitted_at=now(), updated_at=now()
            WHERE position_operation_id=%s
            """,
            (int(submission.broker_order_ref), operation_id),
        )
    return PositionManagerResult(
        owned.strategy_trade_id, owned.broker_position_id, "submitted", "broker_close_accepted", operation_id
    )


@dataclass(frozen=True)
class _ExitIntent:
    """The desired exit pair for one owned position, and whether the broker is short of it.

    One function so there is ONE gap policy. It is needed in two places — the repair arm
    and #3284 item 4a's resume-path recorder — and Codex checkpoint 2 caught the cost of
    letting the second place infer it: the signal arm's ``_edit_landed`` is exact equality,
    so a position whose stop was manually TIGHTENED above the requested one resumes
    ``pending`` forever while this logic rightly calls it protected.
    """

    current_stop: Decimal | None
    desired_stop: Decimal
    desired_take: Decimal | None
    max_quote_age_seconds: int
    stop_gap: bool
    take_gap: bool

    @property
    def has_gap(self) -> bool:
        return self.stop_gap or self.take_gap


def _exit_intent(*, owned: _OwnedPosition, position: BrokerPosition) -> _ExitIntent:
    """Derive the desired stop/target pair and the gap, per arm. Moved verbatim (#3284)."""
    if owned.is_core:
        # The anchor is the position's OWN entry, read back from the broker, and
        # that choice is what makes #3284 item 2 ("re-apply after every change")
        # fall out with no extra machinery: eToro re-weights ``open_price`` when
        # units are added, so the next cycle derives the new levels unprompted.
        levels = core_exit_levels(position.open_price)
        current_stop = position.stop_loss_rate
        desired_stop = levels.stop_loss_rate
        desired_take: Decimal | None = levels.take_profit_rate
        # ⚠ NO `max(current_stop, ...)` CLAMP ON THIS ARM, deliberately.  The
        # signal arm's clamp is a ratchet: a stop there only ever tightens.  A
        # mandate stop is a pure function of the current weighted entry, and
        # #3284 item 2 requires it to be re-applied to that value -- so an ADD at
        # a lower price must be allowed to move the stop DOWN.  Clamping would
        # silently keep a stop computed from a previous, higher entry.
        return _ExitIntent(
            current_stop=current_stop,
            desired_stop=desired_stop,
            desired_take=desired_take,
            max_quote_age_seconds=CORE_EXIT_MAX_QUOTE_AGE_SECONDS,
            stop_gap=position.is_no_stop_loss
            or not core_exit_level_satisfied(observed=current_stop, desired=desired_stop),
            take_gap=position.is_no_take_profit
            or not core_exit_level_satisfied(observed=position.take_profit_rate, desired=desired_take),
        )
    # Past this point the signal arm is guaranteed by _LOAD_OWNED_SQL's
    # witnesses: a loaded non-core position has a preflight and an execution
    # policy, so these are non-null.
    #
    # ⚠ `raise`, NOT `assert`. `python -O` strips asserts, and this guard is
    # what stands between a mis-witnessed load predicate and a `NoneType`
    # comparison inside the stop/take-profit arithmetic below. Stripped, the
    # failure mode is not "no check" but "a confusing TypeError three lines
    # later, in the code that decides where a stop goes".
    if owned.entry_stop is None or owned.entry_take_profit is None or owned.max_quote_age_seconds is None:
        raise StrategyPositionManagerError(
            "a non-core owned position must carry its entry preflight and execution policy"
        )
    current_stop = position.stop_loss_rate
    return _ExitIntent(
        current_stop=current_stop,
        desired_stop=max(current_stop, owned.entry_stop) if current_stop is not None else owned.entry_stop,
        desired_take=owned.entry_take_profit,
        max_quote_age_seconds=owned.max_quote_age_seconds,
        stop_gap=position.is_no_stop_loss or current_stop is None or current_stop < owned.entry_stop,
        take_gap=position.is_no_take_profit or position.take_profit_rate != owned.entry_take_profit,
    )


def _record_resumed_repair_visit(
    conn: psycopg.Connection[Any],
    *,
    owned: _OwnedPosition,
    operation_type: str,
    position: BrokerPosition | None,
    state: str,
    reason_code: str,
    observed_at: datetime,
) -> None:
    """Count a resumed FIXED-EXIT REPAIR against the ownership's refusal streak.

    ⚠⚠ THIS SITE IS NOT OPTIONAL, and its absence was the defect Codex checkpoint 2 found
    in the first draft of #3284 item 4a.  ``_resume_operation`` runs at the TOP of
    ``manage_owned_position`` and returns before the fixed-exit arm, so without this the
    one case that matters most is unrecordable: a ``submitted`` edit whose levels never
    reach the broker stays ``submitted`` forever (the resume path returns
    ``broker_edit_pending`` and never terminalises it), ``idx_strategy_position_one_unresolved_operation``
    blocks any fresh repair, and the position is naked indefinitely with a ZERO streak.

    ⚠ Filtered on ``operation_type='fixed_exit_repair'``, and the filter is load-bearing:
    ``_resume_operation`` also returns ``pending`` for a close lookup
    (``close_lookup_unavailable`` / ``broker_close_pending``) and ``applied`` for a
    completed close, neither of which is evidence about a stop.  Counting a pending CLOSE
    as a stop refusal would alert that the safety net is broken on a position being
    deliberately closed.  A ``stop_ratchet`` is excluded for the same reason: a ratchet
    that has not moved leaves the position protected at its existing stop.

    ⚠⚠ A ``pending`` RESUME IS NOT BY ITSELF A REFUSAL, and Codex checkpoint 2 caught the
    version that assumed it was.  ``_edit_landed`` is EXACT equality on the signal arm, so a
    position whose stop was manually TIGHTENED above the requested one (submitted 95, broker
    shows 100) resumes ``pending`` on every visit forever — while ``_exit_intent`` correctly
    calls it PROTECTED, because a tighter stop is not a gap.  Counting that would accrue
    refusals against a position carrying a perfectly good stop.  So ``pending`` is resolved
    against the SAME gap policy the repair arm uses: a real gap is the refusal, no gap is
    protection.

    ⚠ The unresolved operation row in that scenario is a REAL defect — it stays ``submitted``
    and ``idx_strategy_position_one_unresolved_operation`` then blocks later repairs — but it
    is a DIFFERENT one, and "the stop cannot be set" is the wrong thing to say about a
    position whose stop is set.  Noted on the PR rather than conflated with this counter.

    ⚠ ``position`` is PASSED IN, never re-fetched.  ``_resume_operation`` has already
    fetched the exact position to reach its verdict, and Codex checkpoint 2 flagged the
    version that called the broker a second time: a transient failure on that redundant
    request raises AFTER a valid result was obtained, and `run_strategy_paper_cycle`'s
    loop has no per-position guard, so it would abort every later position in the cycle.
    A refusal counter that can stop the repair of other positions is worse than no counter.
    """
    if operation_type != "fixed_exit_repair":
        return
    if state == "pending":
        if position is None:
            # Nothing to judge the gap against. The resume path owns that case; recording a
            # refusal here would be an inference, not an observation.
            return
        if not _exit_intent(owned=owned, position=position).has_gap:
            state, reason_code = "no_change", "position_protected_operation_unresolved"
    record_repair_visit(
        conn,
        ownership_id=owned.ownership_id,
        state=state,
        reason_code=reason_code,
        observed_at=observed_at,
    )


def _repair_fixed_exit(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    owned: _OwnedPosition,
    observed_at: datetime,
    current_stop: Decimal | None,
    desired_stop: Decimal,
    desired_take: Decimal | None,
    max_quote_age_seconds: int,
) -> PositionManagerResult:
    """Close one observed gap between a position's exit levels and the desired pair.

    Extracted from ``manage_owned_position`` unchanged (#3284 item 4a) so that every
    outcome -- including every refusal added later -- passes through a single
    ``record_repair_visit`` call at the caller.  The gap detection stays with the caller
    because it is the part that differs between the core and signal arms.
    """
    eligibility = _eligibility_for_owned(broker, owned)
    arms = [
        arm
        for arm in eligibility.leverage_configs
        if arm.settlement_type.lower() == "real" and arm.direction.upper() == "LONG"
    ]
    if len(arms) != 1 or arms[0].allow_edit_stop_loss is not True or arms[0].allow_edit_take_profit is not True:
        return PositionManagerResult(
            owned.strategy_trade_id, owned.broker_position_id, "rejected", "broker_fixed_exit_edit_not_allowed"
        )
    # ⚠ ``max_quote_age_seconds`` is the arm-selected bound, NOT
    # ``owned.max_quote_age_seconds`` -- that column is NULL on the core arm,
    # which has no execution policy to carry it.  Reading the column here
    # would raise `TypeError` inside `timedelta` on the first core repair.
    if (
        owned.quote_bid is None
        or owned.quoted_at is None
        or owned.quoted_at < observed_at - timedelta(seconds=max_quote_age_seconds)
        or owned.quoted_at > observed_at + timedelta(seconds=5)
        or desired_stop >= owned.quote_bid
    ):
        return PositionManagerResult(
            owned.strategy_trade_id, owned.broker_position_id, "rejected", "fixed_exit_quote_unsafe"
        )
    prior = _prior_same_edit(
        conn,
        owned=owned,
        operation_type="fixed_exit_repair",
        desired_stop=desired_stop,
        desired_take=desired_take,
        bar=None,
    )
    conn.commit()
    if prior is not None:
        return prior
    with conn.transaction():
        operation_id, request_id = _persist_edit_intent(
            conn,
            owned=owned,
            operation_type="fixed_exit_repair",
            trigger_code="entry_exit_gap",
            prior_stop=current_stop,
            desired_stop=desired_stop,
            desired_take=desired_take,
            bar=None,
        )
    return _submit_edit(
        conn,
        broker=broker,
        owned=owned,
        operation_id=operation_id,
        request_id=request_id,
        desired_stop=desired_stop,
        desired_take=desired_take,
    )


def manage_owned_position(
    conn: psycopg.Connection[Any],
    *,
    broker: BrokerProvider,
    strategy_trade_id: int,
    broker_position_id: int,
    ratchet_bar: RatchetBar | None = None,
    close_reason: Literal["strategy_exit", "emergency_risk", "operator_close"] | None = None,
    now: datetime | None = None,
) -> PositionManagerResult:
    """Verify or de-risk one exact owned position.

    Kill switches intentionally do not block this path: they block new risk,
    while fixed-stop repair, ratcheting, timeout and explicit closes reduce
    already-owned risk. Live credentials are refused by the provider adapter.
    """
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise StrategyPositionManagerError("position management requires an idle connection")
    observed_at = (now or datetime.now(UTC)).astimezone(UTC)
    with _paper_allocator_lock(conn), _position_lock(conn, broker_position_id):
        owned = _load_owned(conn, strategy_trade_id=strategy_trade_id, broker_position_id=broker_position_id)
        conn.commit()
        # ⚠ #3284 item 4a records from INSIDE `_resume_operation`, not from here: that is
        # the function which already holds the fetched position, and re-fetching it to
        # classify the visit adds a broker call that can raise AFTER a valid result and
        # abort every later position in the cycle.
        resumed = _resume_operation(conn, broker=broker, owned=owned, observed_at=observed_at)
        if resumed is not None:
            return resumed
        position = _exact_broker_position(broker, owned)
        if position is None:
            if _release_whole_broker_close(conn, owned=owned, observed_at=observed_at):
                return PositionManagerResult(strategy_trade_id, broker_position_id, "applied", RELEASE_REASON)
            with conn.transaction():
                conn.execute(
                    "UPDATE strategy_trades SET status='reconcile_required', updated_at=now() "
                    "WHERE strategy_trade_id=%s",
                    (strategy_trade_id,),
                )
            return PositionManagerResult(
                strategy_trade_id, broker_position_id, "reconcile_required", "owned_position_missing"
            )

        # ⚠ Age-out is exempted by an EXPLICIT `is_core` test, never by relying on
        # ``max_position_age_seconds`` being NULL for a core position.  Null-by-
        # absence conflates "core is exempt" with "this deployment configured no
        # age policy", so the day a default is introduced, mandate holdings would
        # silently start being closed for being old.  A core holding has no
        # horizon by construction -- that is what makes it core.
        age_seconds = None if owned.is_core else owned.max_position_age_seconds
        timed_out = (
            age_seconds is not None
            and position.open_date_time is not None
            and position.open_date_time <= observed_at - timedelta(seconds=age_seconds)
        )
        if age_seconds is not None and position.open_date_time is None:
            with conn.transaction():
                conn.execute(
                    "UPDATE strategy_trades SET status='reconcile_required', updated_at=now() "
                    "WHERE strategy_trade_id=%s",
                    (strategy_trade_id,),
                )
            return PositionManagerResult(
                strategy_trade_id,
                broker_position_id,
                "reconcile_required",
                "position_open_time_missing",
            )
        if close_reason is not None or timed_out:
            eligibility = _eligibility_for_owned(broker, owned)
            if not eligibility.allow_close_position:
                return PositionManagerResult(
                    strategy_trade_id, broker_position_id, "rejected", "broker_close_not_allowed"
                )
            return _submit_close(
                conn,
                broker=broker,
                owned=owned,
                trigger_code=close_reason or "timeout",
            )

        # ⚠⚠ THE CORE ARM USED TO RETURN "exempt" HERE.  Reversed by operator
        # decision, 2026-09-21 (#3284): *"would expect a safety net of sl and tp in
        # place at all times for this to safe guard spikes"*.  Every engine-held
        # position now carries a broker-side stop and target.
        #
        # The old exemption's reasoning is NOT refuted and is worth keeping in view,
        # because it is the cost being paid: a stop on a benchmark holding sells the
        # benchmark into a drawdown, and "return to core/cash" is what the viability
        # plan falls back TO, so this gives the fallback a fallback.  The operator
        # weighed that against spike risk and chose the stop.  `core_exit_levels`
        # carries the accepted cost in full; read it before touching either constant.
        #
        # ⚠ What stays exempt: AGE-OUT (guarded by its own `is_core` test above) and
        # RATCHETING.  A core position reaches the ratchet block below with
        # ``ratchet_variant_id`` NULL and returns `position_protected` there, so the
        # ratchet is unreachable on this arm by data rather than by a second branch.
        # That matters -- ratcheting a mandate holding's stop upward on strength is
        # precisely the market-timing behaviour the price-only steer cut.
        # ⚠ Derived by `_exit_intent`, which the resume-path recorder ALSO calls.  #3284
        # item 4a needs the gap answer in two places, and Codex checkpoint 2 caught what
        # happens when only one of them has it: a signal-arm position whose stop was
        # manually TIGHTENED above the requested one resumes `pending` forever (the signal
        # arm's `_edit_landed` is exact equality) while this logic considers it protected.
        # Two gap tests that agree today is the shape that drifts; one function is not.
        intent = _exit_intent(owned=owned, position=position)
        current_stop = intent.current_stop
        max_quote_age_seconds = intent.max_quote_age_seconds
        # ⚠⚠ THE TWO `record_repair_visit` CALLS ARE THE WHOLE OF #3284 ITEM 4a, and the
        # arm above is extracted into `_repair_fixed_exit` PRECISELY so there is exactly
        # ONE of them on the repair side.  Before this, both refusals returned straight
        # out of this function without writing anything and the caller
        # (`run_strategy_paper_cycle`) discarded the result -- so a position refusing
        # repair on every visit was indistinguishable from one that needed none.  Adding
        # a third refusal inside the extracted arm is now counted automatically; four
        # separate call sites would not have that property.
        if intent.has_gap:
            result = _repair_fixed_exit(
                conn,
                broker=broker,
                owned=owned,
                observed_at=observed_at,
                current_stop=intent.current_stop,
                desired_stop=intent.desired_stop,
                desired_take=intent.desired_take,
                max_quote_age_seconds=intent.max_quote_age_seconds,
            )
            record_repair_visit(
                conn,
                ownership_id=owned.ownership_id,
                state=result.state,
                reason_code=result.reason_code,
                observed_at=observed_at,
            )
            return result

        # The position is observed carrying both levels: the episode, if there was one,
        # is over.  Recorded rather than skipped, because a streak that is never reset is
        # a stale alarm waiting for the next unrelated gap.
        record_repair_visit(
            conn,
            ownership_id=owned.ownership_id,
            state="no_change",
            reason_code="position_protected",
            observed_at=observed_at,
        )

        if ratchet_bar is None or owned.ratchet_variant_id is None:
            return PositionManagerResult(strategy_trade_id, broker_position_id, "no_change", "position_protected")
        if ratchet_bar.completed_at > observed_at:
            raise ValueError("ratchet bar must be completed before evaluation")
        assert current_stop is not None
        assert owned.break_atr_multiple is not None
        assert owned.chandelier_atr_multiple is not None
        assert owned.structure_atr_multiple is not None
        candidate = calculate_ratchet_stop(
            current_stop=current_stop,
            bar=ratchet_bar,
            break_atr_multiple=owned.break_atr_multiple,
            chandelier_atr_multiple=owned.chandelier_atr_multiple,
            structure_atr_multiple=owned.structure_atr_multiple,
        )
        if candidate is None:
            return PositionManagerResult(strategy_trade_id, broker_position_id, "no_change", "ratchet_not_fired")
        if (
            owned.quote_bid is None
            or owned.quoted_at is None
            or owned.quoted_at < observed_at - timedelta(seconds=max_quote_age_seconds)
            or owned.quoted_at > observed_at + timedelta(seconds=5)
            or candidate >= owned.quote_bid
        ):
            return PositionManagerResult(strategy_trade_id, broker_position_id, "rejected", "ratchet_quote_unsafe")
        eligibility = _eligibility_for_owned(broker, owned)
        arms = [
            arm
            for arm in eligibility.leverage_configs
            if arm.settlement_type.lower() == "real" and arm.direction.upper() == "LONG"
        ]
        if len(arms) != 1 or arms[0].allow_edit_stop_loss is not True:
            return PositionManagerResult(
                strategy_trade_id,
                broker_position_id,
                "rejected",
                "broker_ratchet_not_allowed",
            )
        prior = _prior_same_edit(
            conn,
            owned=owned,
            operation_type="stop_ratchet",
            desired_stop=candidate,
            desired_take=intent.desired_take,
            bar=ratchet_bar,
        )
        conn.commit()
        if prior is not None:
            return prior
        with conn.transaction():
            operation_id, request_id = _persist_edit_intent(
                conn,
                owned=owned,
                operation_type="stop_ratchet",
                trigger_code="causal_resistance_break",
                prior_stop=current_stop,
                desired_stop=candidate,
                desired_take=intent.desired_take,
                bar=ratchet_bar,
            )
        return _submit_edit(
            conn,
            broker=broker,
            owned=owned,
            operation_id=operation_id,
            request_id=request_id,
            desired_stop=candidate,
            desired_take=intent.desired_take,
        )


__all__ = [
    "PositionManagerResult",
    "RatchetBar",
    "StrategyPositionManagerError",
    "calculate_ratchet_stop",
    "configure_position_manager",
    "manage_owned_position",
    "register_ratchet_variant",
]
