"""Ranking-pot-v1 readout: what §9.4 reports and never decides on (#2842 slice 6c-i).

Spec ``docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md`` §9.4, "The readout" paragraph. ``readout`` values
any stepped session E ≥ T₀ (a look's endpoint, or the latest stepped session as an interim view) from the
append-only rows a look reads — the step rows T₀ … E through the look's own accumulator and checks, and the decided
snapshots — plus SPY's masked bars for the regime label. Read-only: it writes nothing and gates nothing.

Fixed here by construction (each closes a spec item; the PR lists them):

- **Lifecycles.** Condition 4's set and values (``ranking_pot_look.LookFacts``): r = terminal / invested − 1. Skew is
  the Fisher–Pearson moment coefficient; the profit factor is the per-trade net-return definition of
  ``strategy_cohort_report`` (re-implemented in Decimal: that module is outside the hash).
- **Turnover.** One-sided buy turnover per applied rebalance: ``bought`` at the target session / the book's NAV at the
  step before it (Novy-Marx & Velikov's value-traded measure, buy side). Controls are the median over K of each
  control's own value.
- **Intervals.** Every book's interval ratio is p[end] / p[start] on the look's path (1.0, each ``nav``, E's
  ``nav_charged``); SPY's is on the look's ``spy_path``. The labels' products multiply to the whole-window ratios.
- **Regime (#2901's rule).** Sign of SPY's stored close at the last NYSE session ≤ D over the one ≤ D − 365 days, D =
  the snapshot's ``last_session``; ``unavailable`` when either close is unusable.
- **SPY total return: not reported** (no ex-dated distribution source; the spec lists what was checked).
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Any, Final, Literal

import psycopg

from app.services import ranking_pot_look as look
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_sim as sim
from app.services.ai_trial_pack import canonical_sha256
from app.services.market_calendar import us_market_status
from app.services.price_masked_bars import load_masked_bars
from app.services.ranking_pot_policy import RANKING_POT_POLICY_HASH

Conn = psycopg.Connection[Any]

#: §9.4: the shadow's missing-bar + ineligibility exits above this share of its lifecycles are flagged (strict).
CORPORATE_ACTION_FLAG: Final = Fraction(1, 20)
#: §9.4 regime look-back (#2901 spec): the last NYSE session on or before D − 365 days.
REGIME_LOOKBACK: Final = timedelta(days=365)
#: Best-tail share of lifecycles (⌈count × this⌉, at least one).
BEST_TAIL: Final = Fraction(1, 100)
SPY_TOTAL_RETURN_REASON: Final = "no_ex_dated_distribution_source"

Regime = Literal["up", "down", "flat", "unavailable"]
_STEP_CONTROL_FIELDS: Final = (
    "records",
    "sum_return",
    "sum_return_charged",
    "nav",
    "nav_charged",
    "held",
    "refusals",
    "missing_exits",
    "ineligible_exits",
    "rescales",
    "bought",
    "entered",
    "decision",
)


# ---------------------------------------------------------------------------
# Pure: statistics
# ---------------------------------------------------------------------------
def _s(x: Decimal | Fraction | None) -> str | None:
    return None if x is None else str(x)


def median(values: Sequence[Decimal]) -> Decimal | None:
    """The middle value; the mean of the two middle values for an even count; ``None`` for none."""
    if not values:
        return None
    v = sorted(values)
    mid = len(v) // 2
    with localcontext(sim.CTX):
        return v[mid] if len(v) % 2 else (v[mid - 1] + v[mid]) / 2


def mean(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    with localcontext(sim.CTX):
        return sum(values, Decimal(0)) / len(values)


def skew(values: Sequence[Decimal]) -> Decimal | None:
    """Fisher–Pearson g₁ = m₃ / m₂^{3/2}, population central moments; ``None`` below 3 values or with m₂ = 0."""
    if len(values) < 3:
        return None
    with localcontext(sim.CTX):
        mu = sum(values, Decimal(0)) / len(values)
        m2 = sum(((x - mu) ** 2 for x in values), Decimal(0)) / len(values)
        m3 = sum(((x - mu) ** 3 for x in values), Decimal(0)) / len(values)
        return None if m2 == 0 else m3 / (m2 * m2.sqrt())


def profit_factor(values: Sequence[Decimal]) -> dict[str, Any]:
    """Σ positive ÷ |Σ negative| over per-trade net returns; ``None`` with no loser. Counts always."""
    with localcontext(sim.CTX):
        gains = sum((x for x in values if x > 0), Decimal(0))
        losses = sum((x for x in values if x < 0), Decimal(0))
        losers = sum(1 for x in values if x < 0)
        return {
            "value": _s(gains / -losses) if losers else None,
            "wins": sum(1 for x in values if x > 0),
            "losses": losers,
            "flat": sum(1 for x in values if x == 0),
        }


def returns_summary(values: Sequence[Decimal]) -> dict[str, Any]:
    return {
        "count": len(values),
        "mean": _s(mean(values)),
        "median": _s(median(values)),
        "profit_factor": profit_factor(values),
    }


@dataclass(frozen=True)
class Lifecycle:
    lifecycle: int
    instrument_id: int
    entry_session: date
    invested: Decimal
    terminal: Decimal

    @property
    def r(self) -> Decimal:
        with localcontext(sim.CTX):
            return self.terminal / self.invested - 1

    @property
    def pnl(self) -> Decimal:
        with localcontext(sim.CTX):
            return self.terminal - self.invested


def lifecycle_distribution(lifecycles: Sequence[Lifecycle]) -> dict[str, Any]:
    """§9.4 lifecycle return distribution: count, mean, median, skew, best 1% and profit factor."""
    ordered = sorted(lifecycles, key=lambda lc: lc.lifecycle)
    rs = [lc.r for lc in ordered]
    out = returns_summary(rs) | {"skew": _s(skew(rs))}
    if not ordered:
        return out | {"best_1pct": None}
    top_n = max(1, math.ceil(len(ordered) * BEST_TAIL))
    # Largest r first; ties by ascending lifecycle number (``sorted`` is stable over the lifecycle order).
    ranked = sorted(ordered, key=lambda lc: lc.r, reverse=True)
    top, rest = ranked[:top_n], ranked[top_n:]
    with localcontext(sim.CTX):
        total_pnl = sum((lc.pnl for lc in ordered), Decimal(0))
        top_pnl = sum((lc.pnl for lc in top), Decimal(0))
        share = top_pnl / total_pnl if total_pnl > 0 else None
    return out | {
        "best_1pct": {
            "selected": top_n,
            "mean": _s(mean([lc.r for lc in top])),
            "rest_mean": _s(mean([lc.r for lc in rest])),
            "pnl": _s(top_pnl),
            "total_pnl": _s(total_pnl),
            "pnl_share": _s(share),
        }
    }


# ---------------------------------------------------------------------------
# Pure: the SPY regime (#2901 spec rule)
# ---------------------------------------------------------------------------
def session_on_or_before(d: date) -> date:
    while us_market_status(d) == "closed":
        d -= timedelta(days=1)
    return d


def _usable(close: Decimal | None) -> bool:
    return close is not None and close.is_finite() and close > 0


@dataclass(frozen=True)
class RegimeLabel:
    label: Regime
    session: date
    back_session: date
    close: Decimal | None
    back_close: Decimal | None

    def doc(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "session": self.session.isoformat(),
            "close": _s(self.close),
            "back_session": self.back_session.isoformat(),
            "back_close": _s(self.back_close),
        }


def regime(spy_closes: Mapping[date, Decimal | None], d: date) -> RegimeLabel:
    """Sign of SPY's close at the last session ≤ D over its close at the last session ≤ D − 365 days."""
    now, back = session_on_or_before(d), session_on_or_before(d - REGIME_LOOKBACK)
    c, b = spy_closes.get(now), spy_closes.get(back)
    label: Regime
    if not (_usable(c) and _usable(b)):
        label = "unavailable"
    else:
        assert c is not None and b is not None
        label = "up" if c > b else "down" if c < b else "flat"
    return RegimeLabel(label, now, back, c, b)


# ---------------------------------------------------------------------------
# Pure: streaming the step rows
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class RebalanceInfo:
    """One applied decided snapshot, as the readout needs it."""

    attempt_id: int
    target_session: date
    last_session: date
    as_of: datetime
    r_size: int
    #: name → (thesis_id, created_at, model, prompt_version) for every S₀ name whose score consumed a thesis.
    theses: Mapping[int, rb.ThesisUsed]
    regime: RegimeLabel


@dataclass(frozen=True)
class ReadoutRow:
    session: date
    forced: bool
    applied_attempt_id: int | None
    wind_down: bool
    shadow: Mapping[str, Any]
    controls: Mapping[str, Any]
    spy: sim.Bar | None

    def look_row(self) -> look.StepRow:
        return look.StepRow(self.session, self.forced, self.shadow, self.controls, self.spy)


def _d(values: Sequence[Any]) -> list[Decimal]:
    return [Decimal(v) for v in values]


@dataclass
class _Interval:
    attempt_id: int
    #: Index into the look's paths of the value before the target session (path[0] = 1.0 before T₀).
    start: int
    end: int = -1
    shadow_held: int = 0
    control_held: list[int] = field(default_factory=list)
    sessions: int = 0


@dataclass
class ReadoutFacts:
    """Everything the readout reports, accumulated from the step rows T₀ … E in order. Controls are reduced as they
    stream (K running values, never K × sessions)."""

    t0: date
    endpoint: date
    n: int
    k: int
    rebalances: Mapping[int, RebalanceInfo]
    facts: look.LookFacts = field(init=False)
    entry_session: dict[int, date] = field(default_factory=dict)
    exit_reasons: Counter[str] = field(default_factory=Counter)
    shadow_rescales: int = 0
    wind_down_session: date | None = None
    per_rebalance: list[dict[str, Any]] = field(default_factory=list)
    intervals: list[_Interval] = field(default_factory=list)
    shadow_turnover: list[Decimal] = field(default_factory=list)
    shadow_refusals: int = 0
    # Per control, in book order.
    prev_nav: list[Decimal] = field(default_factory=list)
    start_nav: list[Decimal] = field(default_factory=list)
    held_total: list[int] = field(default_factory=list)
    turnover_sum: list[Decimal] = field(default_factory=list)
    refusals: list[int] = field(default_factory=list)
    missing_exits: list[int] = field(default_factory=list)
    ineligible_exits: list[int] = field(default_factory=list)
    rescales: list[int] = field(default_factory=list)
    missing_donors: list[int] = field(default_factory=list)
    r_total: int = 0
    #: label → per-control product of interval ratios.
    control_products: dict[str, list[Decimal]] = field(default_factory=dict)
    control_interval_occupancy: list[Decimal | None] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.facts = look.LookFacts(t0=self.t0, endpoint=self.endpoint, k=self.k)
        one = Decimal(1)
        self.prev_nav, self.start_nav = [one] * self.k, [one] * self.k
        for name in ("held_total", "refusals", "missing_exits", "ineligible_exits", "rescales", "missing_donors"):
            setattr(self, name, [0] * self.k)
        self.turnover_sum = [Decimal(0)] * self.k

    # -- the stream ---------------------------------------------------------
    def add(self, row: ReadoutRow) -> None:
        with localcontext(sim.CTX):
            self._add(row)

    def _add(self, row: ReadoutRow) -> None:
        index = self.facts.sessions  # this row's path index is index + 1
        self.facts.add(row.look_row())
        at_end = row.session == self.endpoint
        sh, co = row.shadow, row.controls
        for width_field in _STEP_CONTROL_FIELDS[:-1]:
            if len(co[width_field]) != self.k:
                raise ValueError(f"{row.session}: control column {width_field} is not {self.k} wide")
        if row.wind_down:
            self.wind_down_session = row.session
        if row.applied_attempt_id is not None:
            self._open(row, index)
        elif not self.intervals:
            raise ValueError(f"{row.session}: a step row before the first applied rebalance")
        for r in sh["records"]:
            self.entry_session.setdefault(int(r[1]), row.session)
        for c in sh["closed"]:
            self.exit_reasons[str(c[9])] += 1
        self.shadow_rescales += len(sh["rescales"])
        current = self.intervals[-1]
        current.end = index + 1
        current.sessions += 1
        current.shadow_held += int(sh["held"])
        navs = _d(co["nav_charged" if at_end else "nav"])
        with localcontext(sim.CTX):
            for i in range(self.k):
                held = int(co["held"][i])
                current.control_held[i] += held
                self.held_total[i] += held
                self.refusals[i] += int(co["refusals"][i])
                self.missing_exits[i] += int(co["missing_exits"][i])
                self.ineligible_exits[i] += int(co["ineligible_exits"][i])
                self.rescales[i] += int(co["rescales"][i])
                self.prev_nav[i] = navs[i]
        self.shadow_refusals += len(sh["refusals"])
        if row.applied_attempt_id is not None:
            self._rebalance_row(row)

    def _open(self, row: ReadoutRow, index: int) -> None:
        """An applied rebalance: close the previous interval for every control, open the next."""
        info = self.rebalances.get(int(row.applied_attempt_id or 0))
        if info is None or info.target_session != row.session:
            raise ValueError(f"{row.session}: applied attempt {row.applied_attempt_id} is not a decided snapshot here")
        if self.intervals:
            self._close_controls()
        self.start_nav = list(self.prev_nav)
        self.intervals.append(_Interval(info.attempt_id, start=index, control_held=[0] * self.k))

    def _close_controls(self) -> None:
        last = self.intervals[-1]
        label = self.rebalances[last.attempt_id].regime.label
        products = self.control_products.setdefault(label, [Decimal(1)] * self.k)
        with localcontext(sim.CTX):
            for i in range(self.k):
                if not (self.start_nav[i] > 0 and self.prev_nav[i] > 0):
                    raise ValueError("a non-positive control NAV")
                products[i] *= self.prev_nav[i] / self.start_nav[i]
            self.control_interval_occupancy.append(
                median([Decimal(h) / (last.sessions * self.n) for h in last.control_held])
            )

    def _rebalance_row(self, row: ReadoutRow) -> None:
        """The target session's own step row: turnover, decision-time quantities, refusals, missing donors."""
        info = self.rebalances[int(row.applied_attempt_id or 0)]
        sh, co = row.shadow, row.controls
        dec, cdec = sh.get("decision"), co.get("decision")
        if dec is None or cdec is None or any(len(cdec[f]) != self.k for f in cdec):
            raise ValueError(f"{row.session}: an applied rebalance without its decision rows")
        n = Decimal(self.n)
        start_index = self.intervals[-1].start
        with localcontext(sim.CTX):
            shadow_before = self.facts.shadow_path[start_index]
            shadow_turnover = Decimal(sh["bought"]) / shadow_before
            self.shadow_turnover.append(shadow_turnover)
            turnovers = []
            for i, bought in enumerate(_d(co["bought"])):
                t = bought / self.start_nav[i]
                turnovers.append(t)
                self.turnover_sum[i] += t
                self.missing_donors[i] += int(cdec["missing_donors"][i])
            self.r_total += info.r_size
            donor_shares = [Decimal(int(m)) / info.r_size for m in cdec["missing_donors"]] if info.r_size else []
            self.per_rebalance.append(
                {
                    "attempt_id": info.attempt_id,
                    "target_session": info.target_session.isoformat(),
                    "regime": info.regime.doc(),
                    "shadow": {
                        "turnover": _s(shadow_turnover),
                        "entered_per_n": _s(Decimal(int(sh["entered"])) / n),
                        "decision_exits_per_n": _s(Decimal(len(dec["exits"])) / n),
                        "decision_occupied_per_n": _s(Decimal(int(dec["occupied"])) / n),
                        "decision_slots_unfilled": int(dec["slots_unfilled"]),
                        "entry_refusals": len(sh["refusals"]),
                    },
                    "controls_median": {
                        "turnover": _s(median(turnovers)),
                        "entered_per_n": _s(median([Decimal(int(x)) / n for x in co["entered"]])),
                        "decision_exits_per_n": _s(median([Decimal(int(x)) / n for x in cdec["exits"]])),
                        "decision_occupied_per_n": _s(median([Decimal(int(x)) / n for x in cdec["occupied"]])),
                        "decision_slots_unfilled": _s(median([Decimal(int(x)) for x in cdec["unfilled"]])),
                        "entry_refusals": _s(median([Decimal(int(x)) for x in co["refusals"]])),
                    },
                    "missing_donor_share": {
                        "r_size": info.r_size,
                        "median": _s(median(donor_shares)),
                        "max": _s(max(donor_shares) if donor_shares else None),
                    },
                }
            )

    # -- the end ------------------------------------------------------------
    def finish(self, *, h0: Decimal, h_end: Decimal) -> dict[str, Any]:
        """Every ratio below runs under ``sim.CTX`` (34 digits), never the process-wide context."""
        with localcontext(sim.CTX):
            return self._finish(h0=h0, h_end=h_end)

    def _finish(self, *, h0: Decimal, h_end: Decimal) -> dict[str, Any]:
        f = self.facts
        if f.last_session != self.endpoint:
            raise ValueError(f"the rows end at {f.last_session}, not at the endpoint {self.endpoint}")
        self._close_controls()
        lifecycles = self._lifecycles()
        spy = look.spy_path(f.spy_closes, h0=h0, h_end=h_end)
        return {
            "t0": self.t0.isoformat(),
            "endpoint": self.endpoint.isoformat(),
            "sessions": f.sessions,
            "forced_sessions": f.forced_sessions,
            "spy_invalid_sessions": f.spy_invalid_sessions,
            "wind_down_session": None if self.wind_down_session is None else self.wind_down_session.isoformat(),
            "lifecycles": lifecycle_distribution(lifecycles),
            "turnover_occupancy": self._turnover_occupancy(),
            "regime_cohorts": self._cohorts(lifecycles, spy),
            "exits": self._exits(len(lifecycles)),
            "missing_donors": self._missing_donors(),
            "thesis_provenance": self._theses(lifecycles),
            "spy_total_return": None,
            "spy_total_return_reason": SPY_TOTAL_RETURN_REASON,
        }

    def _lifecycles(self) -> list[Lifecycle]:
        f = self.facts
        if set(f.invested) != set(f.terminal):
            raise ValueError("every lifecycle needs both an entry record and a terminal value")
        targets = {info.target_session for info in self.rebalances.values()}
        out = []
        for lc in sorted(f.invested):
            iid, invested = f.invested[lc]
            entry, terminal = self.entry_session[lc], f.terminal[lc]
            if entry not in targets:
                raise ValueError(f"lifecycle {lc} entered at {entry}, which is no applied rebalance's target")
            if not (invested > 0 and terminal > 0 and invested.is_finite() and terminal.is_finite()):
                raise ValueError(f"lifecycle {lc}: a non-positive value ({invested}, {terminal})")
            out.append(Lifecycle(lc, iid, entry, invested, terminal))
        return out

    def _info_at(self, session: date) -> RebalanceInfo:
        return next(i for i in self.rebalances.values() if i.target_session == session)

    def _turnover_occupancy(self) -> dict[str, Any]:
        n = self.n
        intervals = [
            {
                "attempt_id": iv.attempt_id,
                "sessions": iv.sessions,
                "shadow_occupancy": _s(Decimal(iv.shadow_held) / (iv.sessions * n)),
                "controls_median_occupancy": _s(occ),
            }
            for iv, occ in zip(self.intervals, self.control_interval_occupancy, strict=True)
        ]
        sessions = self.facts.sessions
        rebalances = len(self.per_rebalance)
        with localcontext(sim.CTX):
            return {
                "per_rebalance": self.per_rebalance,
                "intervals": intervals,
                "window": {
                    "shadow_turnover_mean": _s(mean(self.shadow_turnover)),
                    "controls_median_turnover_mean": _s(
                        median([t / rebalances for t in self.turnover_sum]) if rebalances else None
                    ),
                    "shadow_occupancy": _s(Decimal(self.facts.held_total) / (sessions * n)),
                    "controls_median_occupancy": _s(median([Decimal(h) / (sessions * n) for h in self.held_total])),
                    "shadow_entry_refusals": self.shadow_refusals,
                    "controls_median_entry_refusals": _s(median([Decimal(r) for r in self.refusals])),
                },
            }

    def _cohorts(self, lifecycles: Sequence[Lifecycle], spy: Sequence[Decimal] | None) -> dict[str, Any]:
        shadow_path = self.facts.shadow_path
        out: dict[str, Any] = {}
        labels = sorted({info.regime.label for info in self.rebalances.values()})
        with localcontext(sim.CTX):
            for label in labels:
                ivs = [iv for iv in self.intervals if self.rebalances[iv.attempt_id].regime.label == label]
                shadow = spy_product = None
                if ivs:
                    shadow, spy_product = Decimal(1), (Decimal(1) if spy is not None else None)
                    for iv in ivs:
                        shadow *= shadow_path[iv.end] / shadow_path[iv.start]
                        if spy is not None and spy_product is not None:
                            spy_product *= spy[iv.end] / spy[iv.start]
                controls = self.control_products.get(label)
                control_median = median(controls) if controls and ivs else None
                entered = [lc.r for lc in lifecycles if self._info_at(lc.entry_session).regime.label == label]
                out[label] = {
                    "lifecycles": returns_summary(entered),
                    "intervals": len(ivs),
                    "shadow_return": _s(None if shadow is None else shadow - 1),
                    "spy_return": _s(None if spy_product is None else spy_product - 1),
                    "controls_median_return": _s(None if control_median is None else control_median - 1),
                }
        return out

    def _exits(self, lifecycles: int) -> dict[str, Any]:
        reasons = self.exit_reasons
        ineligible = {k: v for k, v in sorted(reasons.items()) if k.startswith("ineligible:")}
        flagged = reasons["missing_bars"] + sum(ineligible.values())

        def dist(values: Sequence[int]) -> dict[str, Any]:
            return {"median": _s(median([Decimal(v) for v in values])), "max": max(values) if values else None}

        return {
            "shadow": {
                "missing_bars": reasons["missing_bars"],
                "ineligible": ineligible,
                "rescales": self.shadow_rescales,
                "by_reason": dict(sorted(reasons.items())),
            },
            "controls": {
                "missing_bars": dist(self.missing_exits),
                "ineligible": dist(self.ineligible_exits),
                "rescales": dist(self.rescales),
            },
            "flag": None if lifecycles == 0 else Fraction(flagged, lifecycles) > CORPORATE_ACTION_FLAG,
        }

    def _missing_donors(self) -> dict[str, Any]:
        if not self.r_total:
            return {"median": None, "max": None}
        shares = [Decimal(m) / self.r_total for m in self.missing_donors]
        return {"median": _s(median(shares)), "max": _s(max(shares))}

    def _theses(self, lifecycles: Sequence[Lifecycle]) -> dict[str, Any]:
        rows, ages = [], []
        by: Counter[tuple[str, str]] = Counter()
        none = 0
        for lc in lifecycles:
            info = self._info_at(lc.entry_session)
            t = info.theses.get(lc.instrument_id)
            if t is None:
                none += 1
                rows.append([lc.lifecycle, lc.instrument_id, None, None, None, None])
                continue
            if t.created_at.tzinfo is None or info.as_of.tzinfo is None or t.created_at > info.as_of:
                raise ValueError(f"lifecycle {lc.lifecycle}: thesis {t.thesis_id} has no valid age at {info.as_of}")
            with localcontext(sim.CTX):
                age = Decimal((info.as_of - t.created_at) / timedelta(microseconds=1)) / Decimal(86_400_000_000)
            ages.append(age)
            by[(t.model or "", t.prompt_version or "")] += 1
            rows.append([lc.lifecycle, lc.instrument_id, t.thesis_id, _s(age), t.model, t.prompt_version])
        return {
            "lifecycles": rows,
            "no_thesis": none,
            "counts": [
                {"model": model or None, "prompt_version": version or None, "count": count}
                for (model, version), count in sorted(by.items())
            ],
            "age_days_median": _s(median(ages)),
            "age_days_max": _s(max(ages) if ages else None),
        }


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------
def _spy_closes(conn: Conn) -> dict[date, Decimal | None]:
    series = load_masked_bars(conn, rb.SPY_INSTRUMENT_ID).series
    return {
        d: r["close"] if isinstance(r["close"], Decimal) else None
        for d, r in zip(series.dates, series.rows, strict=True)
    }


def _rebalances(conn: Conn, declaration_id: int, end: date) -> dict[int, RebalanceInfo]:
    spy = _spy_closes(conn)
    rows = conn.execute(
        "SELECT attempt_id, snapshot, snapshot_sha256 FROM ranking_pot_rebalance_attempts "
        "WHERE declaration_id = %s AND outcome = 'decided' AND target_session <= %s ORDER BY target_session",
        (declaration_id, end),
    ).fetchall()
    out = {}
    for attempt_id, doc, sha in rows:
        if canonical_sha256(doc) != sha:
            raise rb.SnapshotIntegrityError(f"attempt {attempt_id}: the stored snapshot does not hash to its sha256")
        inputs = rb.decode_snapshot(doc)
        universes = rb.universes_of(inputs)
        if isinstance(universes, str):
            raise rb.SnapshotIntegrityError(f"attempt {attempt_id}: a decided snapshot re-derives {universes}")
        out[int(attempt_id)] = RebalanceInfo(
            attempt_id=int(attempt_id),
            target_session=inputs.target_session,
            last_session=inputs.last_session,
            as_of=inputs.as_of,
            r_size=len(universes.r_ids),
            theses=inputs.theses,
            regime=regime(spy, inputs.last_session),
        )
    return out


def _rows(conn: Conn, declaration_id: int, *, t0: date, end: date) -> Iterator[ReadoutRow]:
    """The step rows T₀ … E through a server-side cursor: K-wide control columns never sit in memory together."""
    fields = ", ".join(f"'{f}', controls -> '{f}'" for f in _STEP_CONTROL_FIELDS)
    with conn.cursor(name="ranking_pot_readout_rows") as cur:
        cur.itersize = 8
        cur.execute(
            "SELECT session, forced, applied_attempt_id, wind_down_event_id IS NOT NULL, shadow, "
            f"inputs -> 'spy', jsonb_build_object({fields}) "  # noqa: S608 — field names are module literals
            "FROM ranking_pot_steps WHERE declaration_id = %s AND session BETWEEN %s AND %s ORDER BY session",
            (declaration_id, t0, end),
        )
        for session, forced, applied, wind, shadow, spy, controls in cur:
            bar = None if spy is None else sim.Bar(*(Decimal(v) for v in spy))
            yield ReadoutRow(session, forced, applied, wind, shadow, controls, bar)


def readout(conn: Conn, decl: rb.PotDeclaration, endpoint: date) -> dict[str, Any]:
    """The §9.4 readout at a stepped session E ≥ T₀. Runs its own REPEATABLE READ READ ONLY transaction."""
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        raise RuntimeError("readout opens its own transaction")
    with conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        t0 = look.first_target_session(conn, decl.declaration_id)
        if t0 is None or endpoint < t0:
            raise ValueError(f"declaration {decl.declaration_id}: no stepped window ends at {endpoint}")
        terms = look.terms_of(decl)
        acc = ReadoutFacts(
            t0=t0,
            endpoint=endpoint,
            n=terms.n,
            k=terms.k,
            rebalances=_rebalances(conn, decl.declaration_id, endpoint),
        )
        for row in _rows(conn, decl.declaration_id, t0=t0, end=endpoint):
            acc.add(row)
        spreads = look._spy_half_spreads(conn, decl.declaration_id, endpoint)
        if not spreads or spreads[0][0] != t0:
            raise rb.SnapshotIntegrityError("the first decided snapshot does not target T0")
        out = acc.finish(h0=spreads[0][1], h_end=spreads[-1][1])
    return out | {
        "declaration_id": decl.declaration_id,
        "policy_hash": RANKING_POT_POLICY_HASH,
        "declared_policy_hash": decl.doc.get("policy_hash"),
        "policy_drift": not rb._policy_ok(decl),
    }
