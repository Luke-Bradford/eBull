"""Activate the momentum book (#3822) in the demo account.

Writes, in one transaction:

* the ``momentum-30`` paper deployment (``--capital``, default $10,500 = 30 names x $350);
* its execution policy: a fixed $capital/30 ticket, broker-side stop-loss 25% and take-profit 100% below/above the
  entry ask (wide levels, so the monthly rule and not the stop decides most exits; operator rule: every non-core
  position carries both), the shared quote/halt/cost/reconciliation age gates;
* its position-manager policy: no age exit, no ratchet (exits are the monthly formation's);
* the shared pool's concurrency override raised to ``--max-concurrent`` (default 40): the 6 core-sleeve lifecycles
  count against the cap, so 30 left no room for a 30-name book. Every other pool term is carried forward unchanged.

Demo only. Dry run by default; ``--apply`` writes.

    PYTHONPATH=. uv run python scripts/activate_3822_momentum_book.py            # dry run
    PYTHONPATH=. uv run python scripts/activate_3822_momentum_book.py --apply
"""

from __future__ import annotations

import argparse
from decimal import ROUND_DOWN, Decimal

import psycopg

from app.config import settings
from app.services import momentum_book as mb
from app.services.strategy_control_plane import (
    configure_deployment,
    configure_execution_policy,
    configure_paper_pool,
)
from app.services.strategy_position_manager import configure_position_manager

REASON = "#3822: momentum book demo activation (operator direction 2026-10-15, #2437)"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--capital", type=Decimal, default=Decimal("10500"))
    parser.add_argument("--max-concurrent", type=int, default=40)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if settings.etoro_env != "demo":
        raise SystemExit("the momentum book is demo-only")
    ticket = (args.capital / mb.BOOK_SIZE).quantize(Decimal("0.01"), ROUND_DOWN)

    with psycopg.connect(settings.database_url) as conn:
        pool = conn.execute(
            """
            SELECT enabled, capital_limit, capital_mode, risk_profile, approval_mode, max_concurrent_positions_override
            FROM strategy_paper_pool_events ORDER BY strategy_paper_pool_event_id DESC LIMIT 1
            """
        ).fetchone()
        if pool is None:
            raise SystemExit("no shared paper pool is configured")
        print(f"pool now: {pool}")
        print(f"deployment: capital {args.capital}, ticket {ticket}; pool override -> {args.max_concurrent}")
        if not args.apply:
            print("dry run: nothing written (pass --apply)")
            return
        with conn.transaction():
            configure_paper_pool(
                conn,
                enabled=bool(pool[0]),
                capital_limit=Decimal(str(pool[1])),
                capital_mode=pool[2],
                risk_profile=pool[3],
                approval_mode=pool[4],
                max_concurrent_positions_override=args.max_concurrent,
                changed_by="claude-supervisor",
                reason=REASON,
            )
            deployment = configure_deployment(
                conn,
                strategy_id=mb.STRATEGY_ID,
                strategy_version=mb.STRATEGY_VERSION,
                mode="paper",
                capital_limit=args.capital,
                enabled=True,
                changed_by="claude-supervisor",
                reason=REASON,
            )
            configure_execution_policy(
                conn,
                deployment_id=deployment.deployment_id,
                ticket_sizing_mode="fixed",
                ticket_fraction=None,
                fixed_ticket_amount=ticket,
                max_ticket_amount=ticket,
                stop_loss_pct=Decimal("25"),
                take_profit_pct=Decimal("100"),
                max_quote_age_seconds=900,
                max_scan_age_seconds=86400,
                max_halt_feed_age_seconds=600,
                max_cost_age_seconds=900,
                max_reconciliation_age_seconds=3600,
                max_instrument_exposure_pct=Decimal("10"),
                max_portfolio_exposure_pct=Decimal("100"),
                max_drawdown_pct=Decimal("25"),
                min_net_expectancy_pct=Decimal("0"),
                cost_stress_multiplier=Decimal("1"),
                changed_by="claude-supervisor",
                reason=REASON,
            )
            configure_position_manager(
                conn,
                deployment_id=deployment.deployment_id,
                max_position_age_seconds=None,
                ratchet_variant_id=None,
                updated_by="claude-supervisor",
                reason="#3822: exits are the monthly formation's; no age exit",
            )
        print(f"activated: deployment {deployment.deployment_id}")


if __name__ == "__main__":
    main()
