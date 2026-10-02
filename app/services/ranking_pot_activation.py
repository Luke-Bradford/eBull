"""Ranking-pot-v1's activation and resumption — the only writer of ``→ executing`` (#2842 slice 5c-ii-b).

Spec ``docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md`` §7.3, "The activation script and precheck". A
supervisor step, run by ``scripts/ranking_pot_activate.py`` from the main checkout; it refuses, never reserves.

* **Activation** (``shadow_only``, no activation row): the ticket minimum over the frozen S₀, the read-only capital
  preview (``_capacities`` at the policy's stop cap), then the pot's paper deployment, execution policy and manager
  policy, the ``ranking_pot_activations`` row and the ``executing`` event, in ONE transaction.
* **Resumption** (a halt, the activation row present): the configuration intact and the §7.4 loss check, then the
  ``executing`` event. No capital and no configuration is written.

Lock order (one READ COMMITTED transaction inside the session-level allocator lock): allocator → declaration row
``FOR NO KEY UPDATE`` → ``ai_trial_declarations`` SHARE + rows ``FOR SHARE`` → the strategy lock. Every lock the state
trigger takes is held before the event insert. Policy-hashed (``ranking_pot_policy``).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, time, timedelta
from decimal import ROUND_DOWN, Decimal
from typing import Any, Final, Literal

import psycopg
import psycopg.rows
from psycopg import errors
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

from app.providers.broker import BrokerAccountRiskSnapshot
from app.services import ranking_pot as pot
from app.services import ranking_pot_exec as exec_book
from app.services import ranking_pot_loss as loss
from app.services.ai_trial_start_gate import PreviewShared
from app.services.ranking_pot_intent import _rehashes
from app.services.ranking_pot_policy import RANKING_POT_POLICY_HASH, STRATEGY_VERSION
from app.services.strategy_capital_sandbox import SANDBOX_EXCEEDED
from app.services.strategy_control_plane import (
    DEPLOYMENT_CURRENCY,
    EFFECTIVE_MAX_CONCURRENT_SQL,
    configure_deployment,
    configure_execution_policy,
    lock_strategy_control,
)
from app.services.strategy_core_mandate import load_core_mandate
from app.services.strategy_engine_capital import (
    EngineCapitalObservationError,
    load_engine_capital_authority,
    resolve_engine_capital_usage,
)
from app.services.strategy_paper_executor import (
    _CENT,
    _MANDATE_OBSERVATION_SQL,
    _NY,
    _PENDING_RISK_SQL,
    _age_ok,
    _allocator_lock,
    _capacities,
)
from app.services.strategy_position_manager import configure_position_manager

Conn = psycopg.Connection[Any]

#: §7.3 / ``sql/450``'s CHECK: a multiple of $500, capped at $12,000 (the 30% active-risk term on the $40,000 pool).
POT_CAPITAL_QUANTUM: Final = Decimal("500")
POT_CAPITAL_MAX: Final = Decimal("12000")
CAPITAL_UNAVAILABLE: Final = "pot_capital_unavailable:"
MINIMUM_UNOBSERVED: Final = "pot_minimum_unobserved"

#: §7.3 "Execution policy": v1's go-live policy (#3471 step 2, deployment 8, 2026-09-30) for every field the pot
#: path reads — measured on the account, quote feed, halt feed and cost endpoint the pot shares (derivations in that
#: row's ``reason``) — and neutral values for the fields it does not read (scan age, expectancy, take-profit).
#: ``fixed_ticket_amount`` and ``max_ticket_amount`` depend on ``POT_CAPITAL`` (``execution_policy``).
POT_EXECUTION_POLICY: Final[Mapping[str, Any]] = {
    "ticket_sizing_mode": "fixed",
    "ticket_fraction": None,
    # `ai_trial_decision.STOP_PCT_MAX`, written as a literal so this hashed file carries it (Codex ckpt-2: that
    # module is not hashed); `test_ranking_pot_activation` pins the two equal.
    "stop_loss_pct": Decimal("25"),
    "take_profit_pct": Decimal("100"),
    "max_quote_age_seconds": 5400,
    "max_scan_age_seconds": 86400,
    "max_halt_feed_age_seconds": 600,
    "max_cost_age_seconds": 900,
    "max_reconciliation_age_seconds": 3600,
    "max_instrument_exposure_pct": Decimal("10"),
    "max_portfolio_exposure_pct": Decimal("100"),
    "max_drawdown_pct": Decimal("25"),
    "min_net_expectancy_pct": Decimal("0"),
    "cost_stress_multiplier": Decimal("1"),
}


def slot_ticket(pot_capital: Decimal, n: int) -> Decimal:
    """``POT_CAPITAL / N`` rounded down to the cent: the opening slot's wealth."""
    return (pot_capital / n).quantize(_CENT, rounding=ROUND_DOWN)


def execution_policy(pot_capital: Decimal, n: int) -> dict[str, Any]:
    """The pot's complete execution policy. ``max_ticket_amount = POT_CAPITAL`` is non-binding: the slot ledger is
    the ticket's authority and the deployment cap bounds the total. The fixed ticket is unread (the requested ticket
    is the slot's wealth) and is recorded as the opening slot."""
    return {
        **POT_EXECUTION_POLICY,
        "fixed_ticket_amount": slot_ticket(pot_capital, n),
        "max_ticket_amount": pot_capital,
    }


# ---------------------------------------------------------------------------
# Pure rules
# ---------------------------------------------------------------------------
def capital_entry_reason(entered: Decimal | None) -> str | None:
    """The entered ``POT_CAPITAL`` is a positive multiple of $500, at most $12,000."""
    if entered is None:
        return "pot_capital_not_entered"
    if not entered.is_finite() or entered <= 0 or entered > POT_CAPITAL_MAX or entered % POT_CAPITAL_QUANTUM != 0:
        return "pot_capital_invalid"
    return None


#: One eligibility row: (answer, allow_open_position, min_position_exposure).
EligibilityRow = tuple[str, bool | None, Decimal | None]


def max_open_minimum(s0_ids: Sequence[int], rows: Mapping[int, EligibilityRow]) -> Decimal | str:
    """The largest broker open minimum over the enterable S₀ names, or ``pot_minimum_unobserved`` (fail closed).

    A name that cannot be entered at all (``not_found``, or ``allow_open_position`` false) is skipped; a missing row,
    or an enterable one with a NULL, non-finite or negative minimum, refuses, and so does an empty enterable set.
    """
    worst: Decimal | None = None
    for instrument_id in s0_ids:
        row = rows.get(instrument_id)
        if row is None:
            return MINIMUM_UNOBSERVED
        answer, allow_open, minimum = row
        if answer == "not_found" or allow_open is False:
            continue
        if minimum is None or not minimum.is_finite() or minimum < 0:
            return MINIMUM_UNOBSERVED
        worst = minimum if worst is None else max(worst, minimum)
    return MINIMUM_UNOBSERVED if worst is None else worst


@dataclass(frozen=True)
class CapitalPreview:
    binding_term: str
    binding_value: Decimal
    preview: Decimal
    terms: dict[str, Decimal]


def preview_capital(
    shared: PreviewShared,
    *,
    n: int,
    stop_loss_pct: Decimal,
    max_instrument_exposure_pct: Decimal,
    max_portfolio_exposure_pct: Decimal,
    max_drawdown_pct: Decimal,
) -> CapitalPreview | str:
    """§7.3: the largest ``POT_CAPITAL`` the capacity arithmetic admits for N new non-core positions.

    Refusal order follows ``_risk_and_amount``; the stop is the policy's cap (adverse for loss-at-stop), instrument
    exposure is zero (favourable) and the deployment term is excluded (activation sets it to ``POT_CAPITAL``).
    """

    def refuse(code: str) -> str:
        return CAPITAL_UNAVAILABLE + code

    if not shared.within_bound:
        return refuse(SANDBOX_EXCEEDED)
    if shared.open_lifecycles + n > shared.mandate_max_concurrent_positions:
        return refuse("portfolio_concurrency_limit")
    if shared.daily_realised_pnl <= -(shared.pool_base * shared.mandate_max_daily_loss_pct / Decimal("100")):
        return refuse("portfolio_daily_loss_limit")
    if shared.drawdown_pct >= max_drawdown_pct:
        return refuse("account_drawdown_limit")
    if shared.drawdown_pct >= shared.mandate_max_drawdown_pct:
        return refuse("portfolio_drawdown_limit")
    capacities = _capacities(
        pool_base=shared.pool_base,
        committed=shared.committed,
        active_committed=shared.active_committed,
        deployment_base=Decimal("0"),
        deployment_reserved=Decimal("0"),
        equity=shared.equity,
        total_invested=shared.total_invested,
        available_cash=shared.available_cash,
        pending_total=shared.pending_total,
        pending_instrument=Decimal("0"),
        current_instrument=Decimal("0"),
        max_portfolio_exposure_pct=max_portfolio_exposure_pct,
        max_instrument_exposure_pct=max_instrument_exposure_pct,
        mandate_cash_reserve_pct=shared.mandate_cash_reserve_pct,
        mandate_active_risk_budget_pct=shared.mandate_active_risk_budget_pct,
        mandate_max_loss_per_position_pct=shared.mandate_max_loss_per_position_pct,
        stop_loss_pct=stop_loss_pct,
    )
    if isinstance(capacities, str):
        return refuse(capacities)
    terms = {
        "pool_remaining": capacities.pool_remaining,
        "available_cash": capacities.cash,
        "portfolio_exposure": capacities.portfolio,
        "active_risk": capacities.active_risk,
        "cash_reserve": capacities.cash_reserve,
        "instrument_exposure": n * capacities.instrument,
        "loss_at_stop": n * capacities.loss_at_stop,
        "pot_capital_cap": POT_CAPITAL_MAX,
    }
    terms = {name: value.quantize(_CENT, rounding=ROUND_DOWN) for name, value in terms.items()}
    # `min` keeps the first of equal values, so ties are named in the order above.
    binding_term = min(terms, key=lambda name: terms[name])
    binding_value = terms[binding_term]
    preview = (binding_value // POT_CAPITAL_QUANTUM) * POT_CAPITAL_QUANTUM
    if preview <= 0:
        return refuse(binding_term)
    return CapitalPreview(binding_term=binding_term, binding_value=binding_value, preview=preview, terms=terms)


def capital_reason(entered: Decimal, preview: CapitalPreview) -> str | None:
    """Above the binding term → unavailable on that term; any other value but the preview → not the preview."""
    if entered > preview.binding_value:
        return CAPITAL_UNAVAILABLE + preview.binding_term
    if entered != preview.preview:
        return "pot_capital_not_preview"
    return None


def configuration_drift(
    *,
    pot_capital: Decimal,
    n: int,
    deployment: Mapping[str, Any] | None,
    policy: Mapping[str, Any] | None,
    manager: Mapping[str, Any] | None,
) -> bool:
    """Resumption: the configuration activation wrote, unchanged (``pot_configuration_drift`` when not)."""
    if deployment is None or policy is None or manager is None:
        return True
    if not (
        deployment["mode"] == "paper"
        and deployment["currency"] == DEPLOYMENT_CURRENCY
        and bool(deployment["enabled"])
        and Decimal(str(deployment["capital_limit"])) == pot_capital
    ):
        return True
    for name, expected in execution_policy(pot_capital, n).items():
        actual = policy.get(name)
        if expected is None or actual is None:
            if expected is not actual:
                return True
        elif isinstance(expected, Decimal):
            if Decimal(str(actual)) != expected:
                return True
        elif actual != expected:
            return True
    return manager["max_position_age_seconds"] is not None or manager["ratchet_variant_id"] is not None


# ---------------------------------------------------------------------------
# The transaction
# ---------------------------------------------------------------------------
Mode = Literal["activation", "resumption", "already_active", "none"]


@dataclass(frozen=True)
class ActivationReport:
    applied: bool
    mode: Mode
    declaration_id: int | None
    refusals: tuple[str, ...]
    state: str | None = None
    pot_capital: Decimal | None = None
    preview: CapitalPreview | None = None
    minimum: Decimal | None = None
    detail: dict[str, Any] = field(default_factory=dict)


class _Rollback(Exception):
    pass


_DECLARATION_SQL: Final = """
    SELECT d.declaration_id, d.doc, d.doc_sha256
      FROM ranking_pot_declarations d
     WHERE d.strategy_id = %s
       AND coalesce((SELECT e.to_state FROM ranking_pot_state_events e
                      WHERE e.declaration_id = d.declaration_id ORDER BY e.event_id DESC LIMIT 1),
                    '<none>') <> 'completed'
"""

_POOL_SQL: Final = f"""
    SELECT enabled, capital_limit, risk_profile, max_portfolio_drawdown_pct, max_loss_per_position_pct,
           max_daily_loss_pct, active_risk_budget_pct, cash_reserve_pct, {EFFECTIVE_MAX_CONCURRENT_SQL}
    FROM strategy_paper_pool_events
    ORDER BY strategy_paper_pool_event_id DESC
    LIMIT 1
"""


def _read_shared(conn: Conn, risk: BrokerAccountRiskSnapshot, now: datetime) -> PreviewShared | str:
    """``ai_trial_start_gate._preview``'s shared facts, read-only (never ``_observe_local_mandate_risk``)."""
    pool = conn.execute(_POOL_SQL).fetchone()
    if (
        pool is None
        or not bool(pool[0])
        or pool[1] is None
        or pool[2] is None
        or pool[2] == "unconfigured"
        or any(value is None for value in pool[3:])
    ):
        return CAPITAL_UNAVAILABLE + "paper_pool_unavailable"
    try:
        authority = load_engine_capital_authority(conn)
        if authority is None or not authority.enabled:
            return CAPITAL_UNAVAILABLE + SANDBOX_EXCEEDED
        core_mandate = load_core_mandate(conn)
        usage = resolve_engine_capital_usage(
            authority, risk, core_instrument_id=None if core_mandate is None else core_mandate.core_instrument_id
        )
    except EngineCapitalObservationError as exc:
        return CAPITAL_UNAVAILABLE + exc.reason_code
    pending = conn.execute(_PENDING_RISK_SQL, (None,)).fetchone()
    day_start = datetime.combine(now.astimezone(_NY).date(), time.min, tzinfo=_NY)
    mandate_row = conn.execute(
        _MANDATE_OBSERVATION_SQL, (day_start.astimezone(UTC), (day_start + timedelta(days=1)).astimezone(UTC))
    ).fetchone()
    high_water_row = conn.execute(
        "SELECT equity_high_water FROM strategy_paper_account_risk_state WHERE id = true"
    ).fetchone()
    assert pending is not None and mandate_row is not None  # aggregate SELECTs always return a row
    high_water = max(Decimal(str(high_water_row[0])) if high_water_row else risk.equity, risk.equity)
    drawdown = (high_water - risk.equity) / high_water * Decimal("100") if high_water > 0 else Decimal("100")
    return PreviewShared(
        within_bound=usage.headroom.within_bound,
        pool_base=usage.headroom.bound,
        committed=usage.committed,
        active_committed=authority.alpha_committed,
        equity=risk.equity,
        total_invested=risk.total_invested,
        available_cash=risk.available_cash,
        pending_total=Decimal(str(pending[0])),
        drawdown_pct=drawdown,
        open_lifecycles=int(mandate_row[0]),
        daily_realised_pnl=Decimal(str(mandate_row[1])),
        mandate_max_drawdown_pct=Decimal(str(pool[3])),
        mandate_max_loss_per_position_pct=Decimal(str(pool[4])),
        mandate_max_daily_loss_pct=Decimal(str(pool[5])),
        mandate_active_risk_budget_pct=Decimal(str(pool[6])),
        mandate_cash_reserve_pct=Decimal(str(pool[7])),
        mandate_max_concurrent_positions=int(pool[8]),
    )


def _read_minimum(conn: Conn, s0_ids: Sequence[int]) -> tuple[Decimal | str, dict[str, Any]]:
    """The latest ``complete`` perishables snapshot's eligibility rows for S₀, reduced by ``max_open_minimum``."""
    snapshot = conn.execute(
        "SELECT snapshot_id, finished_at FROM etoro_perishable_snapshots WHERE status = 'complete' "
        "ORDER BY snapshot_id DESC LIMIT 1"
    ).fetchone()
    if snapshot is None:
        return MINIMUM_UNOBSERVED, {"eligibility_snapshot_id": None}
    rows = conn.execute(
        "SELECT instrument_id, answer, allow_open_position, min_position_exposure "
        "FROM etoro_eligibility_observations WHERE snapshot_id = %s AND instrument_id = ANY(%s)",
        (snapshot[0], list(s0_ids)),
    ).fetchall()
    observed = {
        int(r[0]): (str(r[1]), None if r[2] is None else bool(r[2]), None if r[3] is None else Decimal(str(r[3])))
        for r in rows
    }
    detail = {"eligibility_snapshot_id": int(snapshot[0]), "eligibility_finished_at": snapshot[1].isoformat()}
    return max_open_minimum(s0_ids, observed), detail


def _read_configuration(
    conn: Conn,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        deployment = cur.execute(
            "SELECT deployment_id, mode, currency, enabled, capital_limit FROM strategy_deployments "
            "WHERE strategy_id = %s AND strategy_version = %s AND mode = 'paper' FOR SHARE",
            (pot.STRATEGY_ID, STRATEGY_VERSION),
        ).fetchone()
        if deployment is None:
            return None, None, None
        policy = cur.execute(
            "SELECT * FROM strategy_execution_policies WHERE deployment_id = %s FOR SHARE",
            (deployment["deployment_id"],),
        ).fetchone()
        manager = cur.execute(
            "SELECT max_position_age_seconds, ratchet_variant_id FROM strategy_position_manager_policies "
            "WHERE deployment_id = %s FOR SHARE",
            (deployment["deployment_id"],),
        ).fetchone()
    return deployment, policy, manager


def _insert_event(conn: Conn, declaration_id: int, from_state: str, reason: str) -> str | None:
    """The ``executing`` event in a savepoint; the trigger's refusal comes back as a code, not an exception."""
    try:
        with conn.transaction():
            conn.execute(
                "INSERT INTO ranking_pot_state_events (declaration_id, from_state, to_state, reason, actor) "
                "VALUES (%s, %s, 'executing', %s, 'supervisor')",
                (declaration_id, from_state, reason),
            )
    except errors.RaiseException as exc:
        message = (exc.diag.message_primary or str(exc)).splitlines()[0]
        return f"pot_transition_refused:{message}"
    return None


def activate_pot(
    conn: Conn,
    *,
    fetch_risk: Callable[[], BrokerAccountRiskSnapshot],
    pot_capital: Decimal | None,
    declared_by: str,
    apply: bool,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> ActivationReport:
    """Activate or resume the one non-terminal ``ranking-pot-v1`` declaration (dry run unless ``apply``)."""
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise ValueError("activation requires an idle connection")
    if apply and not declared_by.strip():
        raise ValueError("--apply requires --declared-by")
    outcome: list[ActivationReport] = []
    with _allocator_lock(conn):
        # After the allocator lock (no engine entry moves capacity from here to the commit) and before the row locks
        # (no HTTP inside the transaction).
        risk_error: str | None = None
        risk: BrokerAccountRiskSnapshot | None = None
        try:
            risk = fetch_risk()
        except Exception as exc:  # the broker's failure is a refusal here, as `account_risk_unavailable` on entry
            risk_error = f"account_risk_unavailable:{type(exc).__name__}"
        try:
            with conn.transaction():
                outcome.append(_activate_locked(conn, risk, risk_error, pot_capital, declared_by, apply, now()))
                if not outcome[-1].applied:
                    raise _Rollback
        except _Rollback:
            pass
    return outcome[-1]


def _activate_locked(
    conn: Conn,
    risk: BrokerAccountRiskSnapshot | None,
    risk_error: str | None,
    pot_capital: Decimal | None,
    declared_by: str,
    apply: bool,
    now: datetime,
) -> ActivationReport:
    rows = conn.execute(_DECLARATION_SQL, (pot.STRATEGY_ID,)).fetchall()
    if not rows:
        return ActivationReport(False, "none", None, ("pot_not_declared",))
    declaration_id, doc, doc_sha256 = int(rows[0][0]), rows[0][1], rows[0][2]
    conn.execute(
        "SELECT 1 FROM ranking_pot_declarations WHERE declaration_id = %s FOR NO KEY UPDATE", (declaration_id,)
    )
    conn.execute("LOCK TABLE ai_trial_declarations IN SHARE MODE")
    conn.execute("SELECT 1 FROM ai_trial_declarations FOR SHARE")
    lock_strategy_control(conn, pot.STRATEGY_ID, STRATEGY_VERSION)

    state_row = conn.execute(
        "SELECT to_state FROM ranking_pot_state_events WHERE declaration_id = %s ORDER BY event_id DESC LIMIT 1",
        (declaration_id,),
    ).fetchone()
    state = None if state_row is None else str(state_row[0])
    activation_row = conn.execute(
        "SELECT pot_capital FROM ranking_pot_activations WHERE declaration_id = %s", (declaration_id,)
    ).fetchone()
    stored = None if activation_row is None else Decimal(str(activation_row[0]))

    def report(mode: Mode, refusals: list[str], **kw: Any) -> ActivationReport:
        return ActivationReport(
            applied=False, mode=mode, declaration_id=declaration_id, refusals=tuple(refusals), state=state, **kw
        )

    refusals: list[str] = []
    if not _rehashes(doc, doc_sha256):
        refusals.append("pot_not_intact")
    if not isinstance(doc, dict) or doc.get("policy_hash") != RANKING_POT_POLICY_HASH:
        refusals.append("pot_policy_drift")
    if state == "executing" and stored is not None:
        return report("already_active", [], pot_capital=stored)
    if state in ("winding_down", "completed", None) or state == "executing":
        return report("none", [*refusals, f"pot_not_activatable:{state}"])
    if (state == "shadow_only") != (stored is None):
        return report("none", [*refusals, "pot_activation_inconsistent"])
    n = int(doc["terms"]["n"]) if isinstance(doc, dict) else pot.N
    v1 = conn.execute(exec_book.V1_ACTIVE_SQL).fetchone()
    if v1 is None or bool(v1[0]):
        refusals.append("v1_active")
    if risk is None:
        refusals.append(risk_error or "account_risk_unavailable")
    elif not _age_ok(risk.observed_at, now=now, max_seconds=int(POT_EXECUTION_POLICY["max_quote_age_seconds"])):
        refusals.append("account_risk_stale")
        risk = None

    if state == "shadow_only":
        return _activation(conn, report, refusals, declaration_id, doc, n, risk, pot_capital, declared_by, apply, now)
    assert stored is not None
    return _resumption(conn, report, refusals, declaration_id, state, n, risk, stored, pot_capital, declared_by, apply)


def _activation(
    conn: Conn,
    report: Callable[..., ActivationReport],
    refusals: list[str],
    declaration_id: int,
    doc: dict[str, Any],
    n: int,
    risk: BrokerAccountRiskSnapshot | None,
    pot_capital: Decimal | None,
    declared_by: str,
    apply: bool,
    now: datetime,
) -> ActivationReport:
    entry_reason = capital_entry_reason(pot_capital)
    if entry_reason is not None:
        refusals.append(entry_reason)
    exists = conn.execute(
        "SELECT 1 FROM strategy_deployments WHERE strategy_id = %s AND strategy_version = %s AND mode = 'paper'",
        (pot.STRATEGY_ID, STRATEGY_VERSION),
    ).fetchone()
    if exists is not None:
        refusals.append("pot_deployment_exists")
    s0_ids = [int(i) for i in doc["s0"]["instrument_ids"]]
    minimum, detail = _read_minimum(conn, s0_ids)
    if isinstance(minimum, str):
        refusals.append(minimum)
    elif entry_reason is None and pot_capital is not None and slot_ticket(pot_capital, n) < minimum:
        refusals.append("pot_ticket_below_minimum")
    preview: CapitalPreview | None = None
    if risk is not None:
        detail["account_observed_at"] = risk.observed_at.isoformat()
        shared = _read_shared(conn, risk, now)
        policy = POT_EXECUTION_POLICY
        previewed = (
            shared
            if isinstance(shared, str)
            else preview_capital(
                shared,
                n=n,
                stop_loss_pct=policy["stop_loss_pct"],
                max_instrument_exposure_pct=policy["max_instrument_exposure_pct"],
                max_portfolio_exposure_pct=policy["max_portfolio_exposure_pct"],
                max_drawdown_pct=policy["max_drawdown_pct"],
            )
        )
        if isinstance(previewed, str):
            refusals.append(previewed)
        else:
            preview = previewed
            if entry_reason is None and pot_capital is not None:
                reason = capital_reason(pot_capital, preview)
                if reason is not None:
                    refusals.append(reason)
    minimum_value = None if isinstance(minimum, str) else minimum
    if refusals or pot_capital is None or preview is None:
        return report(
            "activation", refusals, pot_capital=pot_capital, preview=preview, minimum=minimum_value, detail=detail
        )

    deployment = configure_deployment(
        conn,
        strategy_id=pot.STRATEGY_ID,
        strategy_version=STRATEGY_VERSION,
        mode="paper",
        capital_limit=pot_capital,
        enabled=True,
        changed_by=declared_by or "dry-run",
        reason=f"#2842 §7.3 activation: capital_limit = POT_CAPITAL {pot_capital}",
    )
    configure_execution_policy(
        conn,
        deployment_id=deployment.deployment_id,
        **execution_policy(pot_capital, n),
        changed_by=declared_by or "dry-run",
        reason="#2842 §7.3 activation: POT_EXECUTION_POLICY (v1's go-live policy for every field the pot reads)",
    )
    configure_position_manager(
        conn,
        deployment_id=deployment.deployment_id,
        max_position_age_seconds=None,
        ratchet_variant_id=None,
        updated_by=declared_by or "dry-run",
        reason="#2842 §7.4: no age exit",
    )
    detail.update(
        {
            "preview": str(preview.preview),
            "binding_term": preview.binding_term,
            "binding_value": str(preview.binding_value),
            "terms": {name: str(value) for name, value in preview.terms.items()},
            "max_open_minimum": str(minimum_value),
            "n": n,
            "declared_by": declared_by,
        }
    )
    conn.execute(
        "INSERT INTO ranking_pot_activations (declaration_id, pot_capital, detail) VALUES (%s, %s, %s)",
        (declaration_id, pot_capital, Jsonb(detail)),
    )
    refused = _insert_event(
        conn,
        declaration_id,
        "shadow_only",
        f"§7.3 activation: POT_CAPITAL {pot_capital} (preview {preview.preview}, binding {preview.binding_term}); "
        f"declared by {declared_by or 'dry-run'}",
    )
    refusals = [refused] if refused else []
    return ActivationReport(
        applied=apply and not refusals,
        mode="activation",
        declaration_id=declaration_id,
        refusals=tuple(refusals),
        state="shadow_only",
        pot_capital=pot_capital,
        preview=preview,
        minimum=minimum_value,
        detail=detail,
    )


def _resumption(
    conn: Conn,
    report: Callable[..., ActivationReport],
    refusals: list[str],
    declaration_id: int,
    state: str,
    n: int,
    risk: BrokerAccountRiskSnapshot | None,
    stored: Decimal,
    pot_capital: Decimal | None,
    declared_by: str,
    apply: bool,
) -> ActivationReport:
    if pot_capital is not None and pot_capital != stored:
        refusals.append("pot_capital_fixed")
    deployment, policy, manager = _read_configuration(conn)
    if configuration_drift(pot_capital=stored, n=n, deployment=deployment, policy=policy, manager=manager):
        refusals.append("pot_configuration_drift")
    detail: dict[str, Any] = {}
    if risk is not None:
        checked = loss.check_loss(conn, declaration_id, pot_capital=stored, risk=risk)
        if checked is None:
            refusals.append(loss.LOSS_UNAVAILABLE)
        else:
            detail["net_pnl"] = str(checked.net)
            if checked.breached:
                refusals.append(loss.LOSS_LIMIT)
    if refusals:
        return report("resumption", refusals, pot_capital=stored, detail=detail)
    refused = _insert_event(
        conn, declaration_id, state, f"§7.3 resumption from {state}; declared by {declared_by or 'dry-run'}"
    )
    refusals = [refused] if refused else []
    return ActivationReport(
        applied=apply and not refusals,
        mode="resumption",
        declaration_id=declaration_id,
        refusals=tuple(refusals),
        state=state,
        pot_capital=stored,
        detail=detail,
    )


__all__ = [
    "CAPITAL_UNAVAILABLE",
    "MINIMUM_UNOBSERVED",
    "POT_CAPITAL_MAX",
    "POT_CAPITAL_QUANTUM",
    "POT_EXECUTION_POLICY",
    "ActivationReport",
    "CapitalPreview",
    "activate_pot",
    "capital_entry_reason",
    "capital_reason",
    "configuration_drift",
    "execution_policy",
    "max_open_minimum",
    "preview_capital",
    "slot_ticket",
]
