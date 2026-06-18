"""Position-vs-portfolio risk — marginal risk contribution (#1636).

The single-instrument risk layer (#591) measures a name in isolation (β vs SPY).
This answers the PM's sizing question instead: *how much does a candidate move my
existing book's risk?* — covariance vs the current holdings.

ON-READ, NOT persisted: the book is dynamic (weights change with every fill), so
a versioned evidence row would be stale immediately. Reuses the risk_metrics
primitives (no new estimator math).

**Convention (read the caveat):** the portfolio return series is **today's
market-value weights applied to past returns** — a *current-exposure covariance
estimate*, NOT the book's realized historical return. MCR's marginal
interpretation assumes a small add funded pro-rata from the book.

Source: Markowitz portfolio risk; standard MCR/component decomposition
(`∂σ_p/∂w_i = (Σw)_i/σ_p`). Spec:
docs/specs/metrics/2026-06-18-position-vs-portfolio-risk.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Literal

import psycopg

from app.db.snapshot import snapshot_read
from app.services.portfolio import _load_positions
from app.services.risk_metrics import (
    MIN_RETURNS_VOL_BETA,
    ReturnPoint,
    annualized_vol,
    load_close_series,
    ols_beta,
    simple_returns,
)

_ZERO = Decimal("0")
_ONE = Decimal("1")

PortfolioRiskStatus = Literal[
    "ok",
    "empty_book",
    "book_history_unavailable",
    "insufficient_history",
    "single_holding_is_candidate",
]


@dataclass(frozen=True)
class PortfolioRelativeRisk:
    """Candidate-vs-current-book risk. Every scalar is nullable + status-flagged;
    a degraded book yields a flagging status, never a fabricated zero."""

    symbol: str
    as_of_date: date | None
    status: PortfolioRiskStatus
    holdings_count: int
    already_held: bool
    current_weight: Decimal | None
    portfolio_beta: Decimal | None
    correlation: Decimal | None
    candidate_vol: Decimal | None
    portfolio_vol: Decimal | None
    marginal_risk_contribution: Decimal | None
    n_obs: int


# ---------------------------------------------------------------------------
# Pure math (no DB — table-tested against synthetic series)
# ---------------------------------------------------------------------------


def build_portfolio_returns(
    holdings: list[tuple[Decimal, dict[date, Decimal]]],
) -> list[ReturnPoint]:
    """Current-weight portfolio return series over the date window where ALL
    holdings have a return (the common intersection — so the weighted sum always
    spans the same asset set). ``holdings`` = ``(market_value, {date: return})``;
    weights are normalised by total market value. Empty when there are no
    holdings, total weight is 0, or the histories don't overlap.
    """
    if not holdings:
        return []
    total_w = sum((w for w, _ in holdings), _ZERO)
    if total_w <= _ZERO:
        return []
    date_sets = [set(m) for _, m in holdings]
    common = sorted(set.intersection(*date_sets)) if date_sets else []
    out: list[ReturnPoint] = []
    for d in common:
        r = sum((w / total_w * m[d] for w, m in holdings), _ZERO)
        out.append((d, r))
    return out


@dataclass(frozen=True)
class _RelMetrics:
    beta: Decimal | None
    correlation: Decimal | None
    candidate_vol: Decimal | None
    portfolio_vol: Decimal | None
    n_obs: int
    last_date: date | None


def relative_risk_metrics(
    candidate_returns: list[ReturnPoint],
    portfolio_returns: list[ReturnPoint],
) -> _RelMetrics:
    """β / correlation / vols over the SHARED candidate∩portfolio date window.

    σ_p, σ_c and β all use the same intersection so ``MCR = β·σ_p`` is
    self-consistent (Codex ckpt-1). correlation = ``sign(β)·√r²`` — ``None`` (never
    0) when β / r² is null.
    """
    c_map = dict(candidate_returns)
    p_map = dict(portfolio_returns)
    shared = sorted(set(c_map) & set(p_map))
    n = len(shared)
    last = shared[-1] if shared else None
    if n < 2:
        return _RelMetrics(None, None, None, None, n, last)
    aligned_c = [(d, c_map[d]) for d in shared]
    aligned_p = [(d, p_map[d]) for d in shared]
    fit = ols_beta(aligned_c, aligned_p)
    corr: Decimal | None = None
    if fit.beta is not None and fit.r2 is not None:
        corr = (_ONE if fit.beta >= _ZERO else -_ONE) * fit.r2.sqrt()
    cvol = annualized_vol([c_map[d] for d in shared])
    pvol = annualized_vol([p_map[d] for d in shared])
    return _RelMetrics(fit.beta, corr, cvol, pvol, n, last)


def marginal_risk_contribution(beta: Decimal | None, portfolio_vol: Decimal | None) -> Decimal | None:
    """MCR = β·σ_p (annualized). None when either input is null — never 0."""
    if beta is None or portfolio_vol is None:
        return None
    return beta * portfolio_vol


# ---------------------------------------------------------------------------
# DB orchestration
# ---------------------------------------------------------------------------


def compute_portfolio_relative_risk(
    conn: psycopg.Connection[Any],
    candidate_instrument_id: int,
    symbol: str,
    end_date: date,
) -> PortfolioRelativeRisk:
    """Compute the candidate's risk relative to the current book. Always returns
    a payload — the status field carries the degraded cases (empty book, no book
    history, single-holding-is-candidate, thin overlap)."""

    def _degraded(
        status: PortfolioRiskStatus,
        holdings_count: int,
        held: bool,
        weight: Decimal | None = None,
    ) -> PortfolioRelativeRisk:
        return PortfolioRelativeRisk(
            symbol=symbol,
            as_of_date=None,
            status=status,
            holdings_count=holdings_count,
            already_held=held,
            current_weight=weight,
            portfolio_beta=None,
            correlation=None,
            candidate_vol=None,
            portfolio_vol=None,
            marginal_risk_contribution=None,
            n_obs=0,
        )

    with snapshot_read(conn):
        positions = _load_positions(conn)
        holdings_count = len(positions)
        if holdings_count == 0:
            return _degraded("empty_book", 0, held=False)

        total_mv = sum((Decimal(str(p.market_value)) for p in positions.values()), _ZERO)
        held = candidate_instrument_id in positions
        current_weight: Decimal | None = None
        if held and total_mv > _ZERO:
            current_weight = Decimal(str(positions[candidate_instrument_id].market_value)) / total_mv

        # A book that is ONLY the candidate yields a trivial β=1 — flag it.
        if holdings_count == 1 and held:
            return _degraded("single_holding_is_candidate", 1, held=True, weight=current_weight)

        # Include EVERY holding, even one with no usable returns: an empty return
        # map forces an empty common-date intersection in build_portfolio_returns
        # → book_history_unavailable, rather than silently computing risk on a
        # renormalized subset of the book (Codex ckpt-2 P1).
        holdings: list[tuple[Decimal, dict[date, Decimal]]] = [
            (
                Decimal(str(pos.market_value)),
                dict(simple_returns(load_close_series(conn, iid, end_date))),
            )
            for iid, pos in positions.items()
        ]

        portfolio_returns = build_portfolio_returns(holdings)
        if not portfolio_returns:
            return PortfolioRelativeRisk(
                symbol=symbol,
                as_of_date=None,
                status="book_history_unavailable",
                holdings_count=holdings_count,
                already_held=held,
                current_weight=current_weight,
                portfolio_beta=None,
                correlation=None,
                candidate_vol=None,
                portfolio_vol=None,
                marginal_risk_contribution=None,
                n_obs=0,
            )

        candidate_returns = simple_returns(load_close_series(conn, candidate_instrument_id, end_date))

    m = relative_risk_metrics(candidate_returns, portfolio_returns)
    status: PortfolioRiskStatus = "ok" if m.n_obs >= MIN_RETURNS_VOL_BETA else "insufficient_history"
    return PortfolioRelativeRisk(
        symbol=symbol,
        as_of_date=m.last_date,
        status=status,
        holdings_count=holdings_count,
        already_held=held,
        current_weight=current_weight,
        portfolio_beta=m.beta,
        correlation=m.correlation,
        candidate_vol=m.candidate_vol,
        portfolio_vol=m.portfolio_vol,
        marginal_risk_contribution=marginal_risk_contribution(m.beta, m.portfolio_vol),
        n_obs=m.n_obs,
    )
