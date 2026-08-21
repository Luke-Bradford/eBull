"""#2807 — a strategy's owned P&L pools across the versions it holds positions under.

Pure-logic tests over `pool_owned_pnl_by_strategy`. The defect is latent on dev
(0 `strategy_deployments` rows, 0 allocated `strategy_funding_decisions`), so a
full-population A/B of the endpoint is a 0 → 0 no-op and seeded inputs are the
only evidence available. See the module docstring on
`pool_owned_pnl_by_strategy` for the combination rule these pin.
"""

from __future__ import annotations

from decimal import Decimal

from app.services.strategy_monitoring import (
    StrategyPnl,
    pool_owned_pnl_by_strategy,
    realised_pnl_for_keys,
)


def _pnl(
    *,
    realised: Decimal | None = Decimal("0"),
    unrealised: Decimal | None = Decimal("0"),
    invested: Decimal | None = Decimal("0"),
    fees: Decimal | None = Decimal("0"),
    trades: int = 0,
    owned: int = 0,
    active: int = 0,
    closes: int = 0,
    reasons: tuple[str, ...] = (),
    reconciled: Decimal = Decimal("0"),
) -> StrategyPnl:
    total = realised + unrealised if realised is not None and unrealised is not None else None
    return StrategyPnl(
        strategy_trade_count=trades,
        owned_position_count=owned,
        active_position_count=active,
        close_event_count=closes,
        invested_capital=invested,
        realised_pnl=realised,
        unrealised_pnl=unrealised,
        total_pnl=total,
        observed_fees=fees,
        complete=not reasons,
        incomplete_reasons=reasons,
        reconciled_realised_pnl=reconciled,
    )


def test_no_owned_rows_pools_to_no_key_at_all() -> None:
    """An absent key is what the card reads as never-owned; do not mint a zero row."""
    assert pool_owned_pnl_by_strategy({}) == {}


def test_a_single_version_pools_to_itself_unchanged() -> None:
    """The common case must not drift: one version in, byte-identical value out."""
    only = _pnl(realised=Decimal("12.50"), unrealised=Decimal("-3"), invested=Decimal("100"), trades=2, owned=2)
    assert pool_owned_pnl_by_strategy({("s5-support-bounce", "v1"): only}) == {"s5-support-bounce": only}


def test_cash_and_counts_add_across_two_versions() -> None:
    """The deployment's dollars and the current scan's are one pot (#2807)."""
    pooled = pool_owned_pnl_by_strategy(
        {
            ("s5-support-bounce", "deployed-v1"): _pnl(
                realised=Decimal("40"),
                unrealised=Decimal("5"),
                invested=Decimal("300"),
                fees=Decimal("1.25"),
                trades=3,
                owned=3,
                active=1,
                closes=2,
                reconciled=Decimal("40"),
            ),
            ("s5-support-bounce", "current-v2"): _pnl(
                realised=Decimal("-10"),
                unrealised=Decimal("2.50"),
                invested=Decimal("100"),
                fees=Decimal("0.75"),
                trades=1,
                owned=1,
                active=1,
                closes=1,
                reconciled=Decimal("-10"),
            ),
        }
    )["s5-support-bounce"]

    realised, unrealised = pooled.realised_pnl, pooled.unrealised_pnl
    assert realised is not None and unrealised is not None
    assert realised == Decimal("30")
    assert unrealised == Decimal("7.50")
    assert pooled.total_pnl == Decimal("37.50")
    assert pooled.total_pnl == realised + unrealised
    assert pooled.invested_capital == Decimal("400")
    assert pooled.observed_fees == Decimal("2.00")
    assert pooled.strategy_trade_count == 4
    assert pooled.owned_position_count == 4
    assert pooled.active_position_count == 2
    assert pooled.close_event_count == 3
    assert pooled.reconciled_realised_pnl == Decimal("30")
    assert pooled.complete is True
    assert pooled.incomplete_reasons == ()


def test_one_unknown_version_makes_only_that_figure_unknown() -> None:
    """`None` is unreconcilable, never zero — it poisons its own pooled sum and no other."""
    pooled = pool_owned_pnl_by_strategy(
        {
            ("s6-resistance-breakout", "deployed-v1"): _pnl(
                realised=None,
                invested=Decimal("300"),
                reasons=("realised_pnl_missing_from_history",),
            ),
            ("s6-resistance-breakout", "current-v2"): _pnl(realised=Decimal("25"), invested=Decimal("100")),
        }
    )["s6-resistance-breakout"]

    assert pooled.realised_pnl is None
    assert pooled.total_pnl is None
    # The unrelated figures stay known: both parts reconciled their capital.
    assert pooled.invested_capital == Decimal("400")
    assert pooled.unrealised_pnl == Decimal("0")


def test_completeness_fails_closed_and_reasons_union() -> None:
    """One hole in one version is a hole in the pooled card, with every reason named."""
    pooled = pool_owned_pnl_by_strategy(
        {
            ("s7-trend-pullback", "deployed-v1"): _pnl(
                unrealised=None,
                reasons=("active_position_mark_unavailable", "trade_not_reconciled_to_position"),
            ),
            ("s7-trend-pullback", "current-v2"): _pnl(
                unrealised=None,
                reasons=("active_position_mark_unavailable",),
            ),
        }
    )["s7-trend-pullback"]

    assert pooled.complete is False
    assert pooled.incomplete_reasons == (
        "active_position_mark_unavailable",
        "trade_not_reconciled_to_position",
    )


def test_versions_of_different_strategies_never_pool_together() -> None:
    pooled = pool_owned_pnl_by_strategy(
        {
            ("s5-support-bounce", "v1"): _pnl(realised=Decimal("10"), reconciled=Decimal("10")),
            ("s6-resistance-breakout", "v1"): _pnl(realised=Decimal("99"), reconciled=Decimal("99")),
        }
    )
    assert pooled["s5-support-bounce"].realised_pnl == Decimal("10")
    assert pooled["s6-resistance-breakout"].realised_pnl == Decimal("99")


def test_card_matches_the_capital_base_after_a_rotation() -> None:
    """The ticket's acceptance: one dict, two consumers, one figure.

    Before the fix the card read a single key — the CURRENT scan version — and so
    reported `StrategyPnl()` for a deployment left behind at a prior version,
    while `realised_pnl_for_keys` still counted that same deployment in the
    capital base.
    """
    deployment_key = ("s5-support-bounce", "deployed-v1")
    current_key = ("s5-support-bounce", "current-v2")
    pnl_by_strategy = {
        deployment_key: _pnl(realised=Decimal("40"), trades=1, owned=1, closes=1, reconciled=Decimal("40")),
        current_key: _pnl(realised=Decimal("0"), reconciled=Decimal("0")),
    }

    capital_base = realised_pnl_for_keys(pnl_by_strategy, [deployment_key])
    assert capital_base is not None

    card = pool_owned_pnl_by_strategy(pnl_by_strategy)["s5-support-bounce"]
    assert card.reconciled_realised_pnl == sum(capital_base.values())
    assert card.realised_pnl == Decimal("40")
    # And the pre-fix read is what it replaced: the current key alone is blind.
    assert pnl_by_strategy[current_key].realised_pnl == Decimal("0")
