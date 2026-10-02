"""Daily risk snapshot of the engine book vs its mandate (#3543, gap register P4).

Spec: ``docs/proposals/risk/2026-10-02-3543-engine-book-risk-snapshot.md``.

Measurement only: nothing reads a snapshot to refuse, size or rebalance, and nothing here pages
the operator (#2843 alerts are refusal surfaces; a measurement refuses nothing).

The book is the exact-owned engine population (``engine_pot_risk.engine_book_rows``) as held at
``measured_at``, marked at the latest completed NYSE session's close, over the effective pot
capital (``strategy_capital_sandbox.sandbox_bound``, #2844). Volatility and beta are of the
CURRENT weights over trailing history -- a constant-weight hypothetical, not the book's own
realised path. The stress figures are a linear single-factor approximation, not a historical
book loss. Every window, decay and scenario is frozen in ``ENGINE_BOOK_RISK_POLICY``.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Final, Literal

import psycopg

from app.db.snapshot import snapshot_read
from app.services.engine_pot_risk import book_membership, engine_book_rows
from app.services.market_calendar import latest_completed_us_session, us_market_status
from app.services.risk_metrics import (
    MIN_RETURNS_VOL_BETA,
    SPY_SYMBOL,
    TRADING_DAYS,
    ReturnPoint,
    _resolve_benchmark_instrument_ids,
    annualized_vol,
    ols_beta,
)
from app.services.strategy_capital_sandbox import sandbox_bound
from app.services.strategy_control_plane import EFFECTIVE_MAX_CONCURRENT_SQL
from app.services.strategy_engine_capital import load_engine_capital_authority

ENGINE_BOOK_RISK_POLICY: Final = "engine-book-risk-v1"
WINDOW_RETURNS: Final = TRADING_DAYS
MIN_OBS: Final = MIN_RETURNS_VOL_BETA
EWMA_LAMBDA: Final = Decimal("0.94")
# SPY close, peak -> trough (spec "Source rule"; reproduce from research series 7694).
STRESS_2020: Final = Decimal("-0.34104747")  # 2020-02-19 -> 2020-03-23
STRESS_2022: Final = Decimal("-0.25360574")  # 2022-01-03 -> 2022-10-12
# Calendar-day bound on the WINDOW_RETURNS + 1 session window (asserted per run).
LOOKBACK_DAYS: Final = 400

_ZERO = Decimal("0")
_ONE = Decimal("1")
_HUNDRED = Decimal("100")
_SQRT_YEAR = Decimal(TRADING_DAYS).sqrt()
_Q8 = Decimal("1e-8")

HistoryStatus = Literal["ok", "insufficient_history", "degenerate", "empty_book"]
RefusalCode = Literal["no_pool", "pot_exhausted", "benchmark_not_ready", "book_shape_unsupported"]


class EngineBookRiskRefusal(RuntimeError):
    def __init__(self, reason_code: RefusalCode, message: str) -> None:
        super().__init__(f"{reason_code}: {message}")
        self.reason_code = reason_code


@dataclass(frozen=True)
class HeldPosition:
    trade_id: int
    position_id: int
    instrument_id: int
    units: Decimal
    initial_units: Decimal | None
    initial_amount_usd: Decimal


@dataclass(frozen=True)
class MandateLimits:
    configured: bool
    target_volatility_pct: Decimal | None
    max_portfolio_drawdown_pct: Decimal | None
    max_concurrent_positions: int | None


@dataclass(frozen=True)
class BookInputs:
    session: date
    measured_at: datetime
    pool_event_id: int
    capital: Decimal
    # NYSE sessions up to and including ``session``, ascending, WINDOW_RETURNS + 1 long. Built from
    # the exchange calendar, never from bars, so a missing close cannot merge two sessions.
    calendar: tuple[date, ...]
    positions: tuple[HeldPosition, ...]
    open_trade_count: int
    mandate: MandateLimits
    # instrument -> {date: close}; SPY likewise. A close may be invalid (None, non-finite, <= 0).
    closes: Mapping[int, Mapping[date, Decimal | None]]
    spy_closes: Mapping[date, Decimal | None]


@dataclass(frozen=True)
class Snapshot:
    session_date: date
    measured_at: datetime
    pool_event_id: int
    capital_usd: Decimal
    gross_usd: Decimal
    position_count: int
    instrument_count: int
    open_trade_count: int
    cost_marked_count: int
    stale_count: int
    largest_share_pct: Decimal | None
    top5_share_pct: Decimal | None
    hhi: Decimal | None
    hist_vol_pct: Decimal | None
    ewma_vol_pct: Decimal | None
    beta: Decimal | None
    vol_n_obs: int
    beta_n_obs: int
    sample_first: date | None
    sample_last: date | None
    history_status: HistoryStatus
    beta_defaulted_count: int
    beta_defaulted_weight_pct: Decimal
    stress_2020_pct: Decimal
    stress_2022_pct: Decimal
    checks: dict[str, dict[str, Any]]
    positions: list[dict[str, Any]] = field(default_factory=list)
    policy_version: str = ENGINE_BOOK_RISK_POLICY


def _valid(close: Decimal | None) -> bool:
    return close is not None and close.is_finite() and close > 0


def session_returns(closes: Mapping[date, Decimal | None], calendar: Sequence[date]) -> list[ReturnPoint]:
    """Returns between CONSECUTIVE calendar sessions only: a missing or invalid close drops both
    returns it touches, so no return ever spans a gap."""
    out: list[ReturnPoint] = []
    for prev, cur in zip(calendar, calendar[1:], strict=False):
        a, b = closes.get(prev), closes.get(cur)
        if a is not None and b is not None and _valid(a) and _valid(b):
            out.append((cur, b / a - _ONE))
    return out


def ewma_vol(returns: Sequence[Decimal]) -> Decimal | None:
    """RiskMetrics zero-mean EWMA, seeded with the first squared return, annualised by sqrt(252)."""
    if not returns:
        return None
    variance = returns[0] * returns[0]
    for r in returns[1:]:
        variance = EWMA_LAMBDA * variance + (_ONE - EWMA_LAMBDA) * r * r
    return variance.sqrt() * _SQRT_YEAR


def _check(value: Decimal | int | None, limit: Decimal | int | None, flagged: bool | None) -> dict[str, Any]:
    if limit is None:
        status = "no_limit"
    elif value is None:
        status = "unknown"
    else:
        status = "evaluated"
    return {
        "status": status,
        "value": None if value is None else str(_q(value) if isinstance(value, Decimal) else value),
        "limit": None if limit is None else str(limit),
        "flagged": bool(flagged) if status == "evaluated" else False,
    }


def _q(value: Decimal) -> Decimal:
    return value.quantize(_Q8)


def compute_snapshot(inputs: BookInputs) -> Snapshot:
    """The snapshot of ``inputs``. Pure: no I/O."""
    session = inputs.session
    calendar = list(inputs.calendar)

    # Marks: last valid close on or before the session; cost basis (remaining) when none.
    rows: list[dict[str, Any]] = []
    mv_by_instrument: dict[int, Decimal] = {}
    cost_marked = stale = 0
    for p in inputs.positions:
        bars = inputs.closes.get(p.instrument_id, {})
        mark_date = max((d for d, c in bars.items() if d <= session and _valid(c)), default=None)
        if mark_date is None:
            mark: Decimal | None = None
            if p.initial_units is not None and p.initial_units > 0:
                mv = p.initial_amount_usd * p.units / p.initial_units
            else:
                mv = p.initial_amount_usd
            cost_marked += 1
            stale += 1
        else:
            mark = bars[mark_date]
            assert mark is not None
            mv = p.units * mark
            stale += mark_date < session
        mv_by_instrument[p.instrument_id] = mv_by_instrument.get(p.instrument_id, _ZERO) + mv
        rows.append(
            {
                "trade_id": p.trade_id,
                "position_id": p.position_id,
                "instrument_id": p.instrument_id,
                "units": str(p.units),
                "mark": None if mark is None else str(mark),
                "mark_date": "cost" if mark_date is None else mark_date.isoformat(),
                "market_value_usd": str(_q(mv)),
                "weight_of_capital_pct": str(_q(mv / inputs.capital * _HUNDRED)),
            }
        )

    gross = sum(mv_by_instrument.values(), _ZERO)
    weights = {i: mv / inputs.capital for i, mv in mv_by_instrument.items()}
    spy_returns = session_returns(inputs.spy_closes, calendar)

    # Per-instrument beta (stress); beta = 1 when undefined, counted with its exposure.
    betas: dict[int, Decimal] = {}
    beta_obs: dict[int, int] = {}
    inst_returns: dict[int, dict[date, Decimal]] = {}
    for i in mv_by_instrument:
        series = session_returns(inputs.closes.get(i, {}), calendar)
        inst_returns[i] = dict(series)
        result = ols_beta(series, spy_returns)
        beta_obs[i] = result.n_obs
        if result.beta is not None and result.n_obs >= MIN_OBS:
            betas[i] = result.beta
    defaulted = [i for i in mv_by_instrument if i not in betas]
    for row in rows:
        i = row["instrument_id"]
        row["beta"] = str(_q(betas[i])) if i in betas else "defaulted"
        row["beta_n_obs"] = beta_obs[i]

    def stress(shock: Decimal) -> Decimal:
        return sum((w * max(-_ONE, betas.get(i, _ONE) * shock) for i, w in weights.items()), _ZERO) * _HUNDRED

    hist_vol = ewma = beta = None
    vol_n = beta_n = 0
    first = last = None
    status: HistoryStatus
    if not mv_by_instrument:
        status = "empty_book"
        hist_vol = ewma = beta = _ZERO
    else:
        dates = [d for d, _ in spy_returns if all(d in inst_returns[i] for i in weights)]
        book = [(d, sum((w * inst_returns[i][d] for i, w in weights.items()), _ZERO)) for d in dates]
        vol_n = len(book)
        if book:
            first, last = book[0][0], book[-1][0]
        fit = ols_beta(book, spy_returns)
        beta_n = fit.n_obs
        if vol_n < MIN_OBS:
            status = "insufficient_history"
        else:
            values = [r for _, r in book]
            hist_vol = annualized_vol(values)
            ewma = ewma_vol(values)
            beta = fit.beta
            status = "ok" if beta is not None else "degenerate"

    shares = sorted((mv / gross * _HUNDRED for mv in mv_by_instrument.values()), reverse=True) if gross > 0 else []
    stress_2020 = stress(STRESS_2020)
    stress_2022 = stress(STRESS_2022)
    ewma_pct = None if ewma is None else ewma * _HUNDRED
    m = inputs.mandate
    limit_vol = m.target_volatility_pct if m.configured else None
    limit_dd = m.max_portfolio_drawdown_pct if m.configured else None
    limit_n = m.max_concurrent_positions if m.configured else None
    checks = {
        "forecast_vol_vs_target": _check(
            ewma_pct, limit_vol, ewma_pct is not None and limit_vol is not None and ewma_pct > limit_vol
        ),
        "stress_2020_vs_max_drawdown": _check(stress_2020, limit_dd, limit_dd is not None and stress_2020 < -limit_dd),
        "stress_2022_vs_max_drawdown": _check(stress_2022, limit_dd, limit_dd is not None and stress_2022 < -limit_dd),
        "positions_vs_max_concurrent": _check(
            inputs.open_trade_count, limit_n, limit_n is not None and inputs.open_trade_count > limit_n
        ),
        # Always evaluated: staleness does not depend on the mandate.
        "stale_marks": {"status": "evaluated", "value": str(stale), "limit": "0", "flagged": stale > 0},
    }

    return Snapshot(
        session_date=session,
        measured_at=inputs.measured_at,
        pool_event_id=inputs.pool_event_id,
        capital_usd=inputs.capital,
        gross_usd=_q(gross),
        position_count=len(inputs.positions),
        instrument_count=len(mv_by_instrument),
        open_trade_count=inputs.open_trade_count,
        cost_marked_count=cost_marked,
        stale_count=stale,
        largest_share_pct=_q(shares[0]) if shares else None,
        top5_share_pct=_q(sum(shares[:5], _ZERO)) if shares else None,
        hhi=_q(sum((s * s for s in shares), _ZERO)) if shares else None,
        hist_vol_pct=None if hist_vol is None else _q(hist_vol * _HUNDRED),
        ewma_vol_pct=None if ewma_pct is None else _q(ewma_pct),
        beta=None if beta is None else _q(beta),
        vol_n_obs=vol_n,
        beta_n_obs=beta_n,
        sample_first=first,
        sample_last=last,
        history_status=status,
        beta_defaulted_count=len(defaulted),
        beta_defaulted_weight_pct=_q(sum((weights[i] for i in defaulted), _ZERO) * _HUNDRED),
        stress_2020_pct=_q(stress_2020),
        stress_2022_pct=_q(stress_2022),
        checks=checks,
        positions=rows,
    )


def nyse_sessions(last: date, count: int) -> tuple[date, ...]:
    """The ``count`` NYSE sessions ending at ``last``, ascending."""
    out: list[date] = []
    day = last
    while len(out) < count:
        if us_market_status(day) != "closed":
            out.append(day)
        day -= timedelta(days=1)
    return tuple(reversed(out))


def load_inputs(conn: psycopg.Connection[Any], measured_at: datetime) -> BookInputs:
    """Read everything one snapshot needs in one REPEATABLE READ snapshot. Raises on a refusal."""
    session = latest_completed_us_session(measured_at)
    calendar = nyse_sessions(session, WINDOW_RETURNS + 1)
    assert (session - calendar[0]).days <= LOOKBACK_DAYS
    with snapshot_read(conn):
        authority = load_engine_capital_authority(conn)
        if authority is None:
            raise EngineBookRiskRefusal("no_pool", "no paper pool has been assigned")
        capital = sandbox_bound(
            capital_limit=authority.capital_limit,
            capital_mode=authority.capital_mode,
            realised_delta=authority.realised_delta,
        )
        if capital <= 0:
            raise EngineBookRiskRefusal("pot_exhausted", f"effective pot capital is {capital}")
        mandate_row = conn.execute(
            f"""
            SELECT risk_profile <> 'unconfigured', target_volatility_pct, max_portfolio_drawdown_pct,
                   {EFFECTIVE_MAX_CONCURRENT_SQL}
            FROM strategy_paper_pool_events WHERE strategy_paper_pool_event_id = %s
            """,
            (authority.pool_event_id,),
        ).fetchone()
        assert mandate_row is not None
        mandate = MandateLimits(
            configured=bool(mandate_row[0]),
            target_volatility_pct=mandate_row[1],
            max_portfolio_drawdown_pct=mandate_row[2],
            max_concurrent_positions=mandate_row[3],
        )

        rows = engine_book_rows(conn, authority.epoch_started_at)
        held, _released = book_membership(rows, measured_at)
        open_trades = len({row[0] for row in rows if row[1] not in ("closed", "failed")})
        trade_of = {int(row[3]): int(row[0]) for row in rows if row[3] is not None}

        positions: list[HeldPosition] = []
        if held:
            shape = conn.execute(
                """
                SELECT bp.position_id, bp.instrument_id, bp.units, bp.initial_units, bp.initial_amount_in_dollars,
                       bp.is_buy, bp.leverage, i.currency
                FROM broker_positions bp JOIN instruments i ON i.instrument_id = bp.instrument_id
                WHERE bp.position_id = ANY(%s::bigint[])
                """,
                (list(held),),
            ).fetchall()
            by_id = {int(r[0]): r for r in shape}
            for position_id, instrument_id in sorted(held.items()):
                r = by_id.get(position_id)
                if r is None:
                    raise EngineBookRiskRefusal(
                        "book_shape_unsupported", f"held position {position_id} has no broker row"
                    )
                if int(r[1]) != instrument_id or not r[5] or r[6] != 1 or r[7] != "USD":
                    raise EngineBookRiskRefusal(
                        "book_shape_unsupported",
                        f"position {position_id}: instrument {r[1]}, is_buy {r[5]}, leverage {r[6]}, currency {r[7]}",
                    )
                positions.append(
                    HeldPosition(
                        trade_id=trade_of[position_id],
                        position_id=position_id,
                        instrument_id=instrument_id,
                        units=Decimal(str(r[2])),
                        initial_units=None if r[3] is None else Decimal(str(r[3])),
                        initial_amount_usd=Decimal(str(r[4])),
                    )
                )

        spy_id = _resolve_benchmark_instrument_ids(conn).get(SPY_SYMBOL)
        if spy_id is None:
            raise EngineBookRiskRefusal("benchmark_not_ready", "SPY does not resolve to an instrument")
        wanted = sorted({p.instrument_id for p in positions} | {spy_id})
        closes: dict[int, dict[date, Decimal | None]] = {i: {} for i in wanted}
        for instrument_id, price_date, close in conn.execute(
            """
            SELECT instrument_id, price_date, close FROM price_daily
            WHERE instrument_id = ANY(%s::bigint[]) AND price_date BETWEEN %s AND %s
            """,
            (wanted, calendar[0], session),
        ):
            closes[int(instrument_id)][price_date] = None if close is None else Decimal(str(close))
        # The mark is the last valid close however old: a position stale past the return window
        # is still marked, never silently valued at cost.
        for instrument_id, price_date, close in conn.execute(
            """
            SELECT DISTINCT ON (instrument_id) instrument_id, price_date, close FROM price_daily
            WHERE instrument_id = ANY(%s::bigint[]) AND price_date <= %s AND close > 0 AND close <> 'NaN'
            ORDER BY instrument_id, price_date DESC
            """,
            (wanted, session),
        ):
            closes[int(instrument_id)][price_date] = Decimal(str(close))

    spy_closes = dict(closes[spy_id])
    if spy_id not in {p.instrument_id for p in positions}:
        del closes[spy_id]
    if not _valid(spy_closes.get(session)):
        raise EngineBookRiskRefusal("benchmark_not_ready", f"SPY has no valid close for session {session}")
    return BookInputs(
        session=session,
        measured_at=measured_at,
        pool_event_id=authority.pool_event_id,
        capital=capital,
        calendar=calendar,
        positions=tuple(positions),
        open_trade_count=open_trades,
        mandate=mandate,
        closes=closes,
        spy_closes=spy_closes,
    )


def write_snapshot(conn: psycopg.Connection[Any], snap: Snapshot) -> bool:
    """Insert ``snap``; ``False`` when its (session, policy) row already exists."""
    inserted = conn.execute(
        """
        INSERT INTO engine_book_risk_snapshots (
            session_date, policy_version, measured_at, pool_event_id, capital_usd, gross_usd,
            position_count, instrument_count, open_trade_count, cost_marked_count, stale_count,
            largest_share_pct, top5_share_pct, hhi, hist_vol_pct, ewma_vol_pct, beta,
            vol_n_obs, beta_n_obs, sample_first, sample_last, history_status,
            beta_defaulted_count, beta_defaulted_weight_pct, stress_2020_pct, stress_2022_pct,
            checks, positions
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s::jsonb, %s::jsonb)
        ON CONFLICT (session_date, policy_version) DO NOTHING
        RETURNING 1
        """,
        (
            snap.session_date,
            snap.policy_version,
            snap.measured_at,
            snap.pool_event_id,
            snap.capital_usd,
            snap.gross_usd,
            snap.position_count,
            snap.instrument_count,
            snap.open_trade_count,
            snap.cost_marked_count,
            snap.stale_count,
            snap.largest_share_pct,
            snap.top5_share_pct,
            snap.hhi,
            snap.hist_vol_pct,
            snap.ewma_vol_pct,
            snap.beta,
            snap.vol_n_obs,
            snap.beta_n_obs,
            snap.sample_first,
            snap.sample_last,
            snap.history_status,
            snap.beta_defaulted_count,
            snap.beta_defaulted_weight_pct,
            snap.stress_2020_pct,
            snap.stress_2022_pct,
            json.dumps(snap.checks),
            json.dumps(snap.positions),
        ),
    ).fetchone()
    return inserted is not None


def run_engine_book_risk_snapshot(conn: psycopg.Connection[Any], measured_at: datetime) -> Snapshot | None:
    """Measure and persist one snapshot. ``None`` when the session's row already exists."""
    snap = compute_snapshot(load_inputs(conn, measured_at))
    with conn.transaction():
        written = write_snapshot(conn, snap)
    return snap if written else None
