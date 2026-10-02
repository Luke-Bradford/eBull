"""The trade ticket every engine entry order carries (#3542, gap register P3).

Spec: ``docs/proposals/execution/2026-10-02-3542-entry-trade-ticket.md``.

Rule id, evidence id, rationale class, exit rule (or ``not_applicable: <reason>``) and expected cost,
written once, in the path's authority transaction, from the values that path itself persists -- never
re-derived. Exits and protective orders never get one: an exit is never blocked.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal

import psycopg

RationaleClass = Literal["signal", "rebalance", "experiment"]
EvidenceKind = Literal["strategy_promotion", "ai_trial_declaration", "ranking_pot_declaration", "core_mandate_event"]


class EntryTicketError(RuntimeError):
    """A ticket cannot be stated truthfully; the entry must not commit."""


@dataclass(frozen=True)
class EntryTicket:
    order_id: int
    strategy_trade_id: int
    rationale_class: RationaleClass
    rule_id: str
    evidence_kind: EvidenceKind
    evidence_id: int
    why_now: str
    exit_rule: str
    expected_cost_usd: Decimal
    cost_basis: str


def format_rate(rate: Decimal) -> str:
    """A rate independent of its scale: ``Decimal("94")`` and ``Decimal("94.000000")`` read alike."""
    return format(rate.normalize(), "f")


def protective_exit_rule(stop_loss_rate: Decimal | None, take_profit_rate: Decimal | None, *, then: str) -> str:
    """The exit as the path sends it: its protective levels, then the path's own exit rule."""
    stop = "none" if stop_loss_rate is None else format_rate(stop_loss_rate)
    take = "none" if take_profit_rate is None else format_rate(take_profit_rate)
    return f"stop loss {stop} / take profit {take}; {then}"


def position_manager_exit(conn: psycopg.Connection[Any], deployment_id: int) -> str:
    """The deployment's position-manager exit rule in force now, with its revision."""
    row = conn.execute(
        "SELECT revision, max_position_age_seconds, ratchet_variant_id "
        "FROM strategy_position_manager_policies WHERE deployment_id = %s",
        (deployment_id,),
    ).fetchone()
    if row is None:
        return "no position-manager policy: held until the protective levels trigger"
    age = "no age exit" if row[1] is None else f"age exit after {row[1]}s"
    ratchet = "no stop ratchet" if row[2] is None else f"stop ratchet variant {row[2]}"
    return f"position-manager policy revision {row[0]}: {age}, {ratchet}"


def write_entry_ticket(conn: psycopg.Connection[Any], ticket: EntryTicket) -> None:
    """Insert ``ticket`` in the caller's transaction. A second ticket for one order is an error."""
    if not ticket.expected_cost_usd.is_finite() or ticket.expected_cost_usd < 0:
        raise EntryTicketError(f"order {ticket.order_id}: expected cost {ticket.expected_cost_usd} is not stated")
    conn.execute(
        """
        INSERT INTO strategy_entry_tickets (
            order_id, strategy_trade_id, rationale_class, rule_id, evidence_kind, evidence_id,
            why_now, exit_rule, expected_cost_usd, cost_basis
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            ticket.order_id,
            ticket.strategy_trade_id,
            ticket.rationale_class,
            ticket.rule_id,
            ticket.evidence_kind,
            ticket.evidence_id,
            ticket.why_now,
            ticket.exit_rule,
            ticket.expected_cost_usd,
            ticket.cost_basis,
        ),
    )


def authorising_promotion_id(conn: psycopg.Connection[Any], strategy_id: str, strategy_version: str) -> int:
    """The promotion ``current_stage`` reads -- the one ``decide_funding`` just admitted under its lock."""
    row = conn.execute(
        """
        SELECT promotion_id, to_stage FROM strategy_promotions
        WHERE strategy_id = %s AND strategy_version = %s
        ORDER BY promotion_id DESC LIMIT 1
        """,
        (strategy_id, strategy_version),
    ).fetchone()
    if row is None or row[1] not in ("paper_enabled", "live_enabled"):
        raise EntryTicketError(f"{strategy_id}@{strategy_version} has no funding promotion to cite")
    return int(row[0])


__all__ = [
    "EntryTicket",
    "EntryTicketError",
    "EvidenceKind",
    "RationaleClass",
    "authorising_promotion_id",
    "format_rate",
    "position_manager_exit",
    "protective_exit_rule",
    "write_entry_ticket",
]
