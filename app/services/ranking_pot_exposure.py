"""§9.4 exposures and beta of the pot's books (#2842 slice 6c-ii-c-1, by construction; r3-144). Pure; policy-hashed.

Descriptive only: nothing here enters a decision, a look or a gate.

- **Characteristics** (one table per applied rebalance, from its decided snapshot; D = the snapshot's
  ``last_session``): sector (the SIC → sector-SPDR crosswalk, bucket ``none`` when unmapped), ln of the overlaid cap
  (Barra USE4's LNCAP size descriptor), ATR% (the F_t rule's ATR14 over the close on D) and beta (Sharpe's
  single-index market model: the OLS slope of the name's daily simple returns on SPY's, over paired sessions in the
  365 days to D, at least ``BETA_MIN_PAIRS`` pairs).
- **Sums** (one per book per stepped session): every position held after the step weighs its ``value``. The stored
  quantities are additive (capital, per-sector capital, and per characteristic the covered capital and Σ value·x),
  never a ratio, so each control's window value is formed first and the median over K second (the two do not
  commute).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal, localcontext
from typing import Any, Final, Literal

from app.services import ranking_pot as pot
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_sim as sim
from app.services.market_calendar import us_market_status
from app.services.sector_classification import resolve_sector_spdr

BETA_MIN_PAIRS: Final = 200
BETA_WINDOW: Final = timedelta(days=365)
NO_SECTOR: Final = "none"
#: The weighted characteristics, in storage order.
FIELDS: Final = ("ln_cap", "beta", "atr_pct", "div_yield")

BetaReason = Literal["too_few_pairs", "zero_market_variance"]


@dataclass(frozen=True)
class Characteristic:
    sector: str
    ln_cap: Decimal | None
    beta: Decimal | None
    #: Why ``beta`` is ``None``; ``None`` when it is defined.
    beta_reason: BetaReason | None
    atr_pct: Decimal | None
    #: Slice 6c-ii-c-2a: trailing dividend yield (XBRL TTM DPS / close), ``None`` when the name is not covered.
    div_yield: Decimal | None = None

    def value(self, name: str) -> Decimal | None:
        return {"ln_cap": self.ln_cap, "beta": self.beta, "atr_pct": self.atr_pct, "div_yield": self.div_yield}[name]


def _usable(close: Any) -> bool:
    return isinstance(close, Decimal) and close.is_finite() and close > 0


def usable_closes(dates: Sequence[date], rows: Sequence[Mapping[str, Any]]) -> dict[date, Decimal]:
    """Each session's close when finite and > 0 (close-only, as the regime label reads SPY). Dates must be strictly
    ascending: a duplicate would make the pairing depend on the order of the rows."""
    if any(b <= a for a, b in zip(dates, dates[1:], strict=False)):
        raise rb.SnapshotIntegrityError("a snapshot bar series is not strictly ascending")
    return {d: r["close"] for d, r in zip(dates, rows, strict=True) if _usable(r["close"])}


def previous_session(d: date) -> date:
    d -= timedelta(days=1)
    while us_market_status(d) == "closed":
        d -= timedelta(days=1)
    return d


def beta(
    name: Mapping[date, Decimal], spy: Mapping[date, Decimal], d: date
) -> tuple[Decimal | None, BetaReason | None]:
    """r3-144: a session s with d − 365 days < s ≤ d pairs when s and the NYSE session before it both have a usable
    close in both series; y = the name's simple return over that step, x = SPY's. Population moments, summed in
    session order under ``sim.CTX``."""
    xs: list[Decimal] = []
    ys: list[Decimal] = []
    with localcontext(sim.CTX):
        for s in sorted(name):
            if not d - BETA_WINDOW < s <= d or us_market_status(s) == "closed":
                continue
            p = previous_session(s)
            if s in spy and p in spy and p in name:
                xs.append(spy[s] / spy[p] - 1)
                ys.append(name[s] / name[p] - 1)
        if len(xs) < BETA_MIN_PAIRS:
            return None, "too_few_pairs"
        if len(set(xs)) == 1:
            return None, "zero_market_variance"
        n = len(xs)
        mx = sum(xs, Decimal(0)) / n
        my = sum(ys, Decimal(0)) / n
        sxy = sum(((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True)), Decimal(0))
        sxx = sum(((x - mx) ** 2 for x in xs), Decimal(0))
        if sxx == 0:  # distinct 34-digit returns cannot square-sum to 0; kept so a division can never trap
            return None, "zero_market_variance"
        return sxy / sxx, None


def dividend_yield(f: pot.NameFacts, close: Decimal | None) -> Decimal | None:
    """Trailing XBRL DPS per tradable unit / the close on D; ``None`` = not covered. A DPS of 0 is 0 whatever the ADS
    ratio. A positive DPS is taken per tradable unit from the valuation view's own ratio-corrected yield ×
    its price / 100 (= DPS × the ADS ratio; NULL for a ratio-unknown ADR, ``sql/241``), never the raw per-ordinary
    DPS against a per-ADS close (Codex ckpt-2)."""
    dps = f.dps_ttm
    if dps is None or not dps.is_finite() or dps < 0 or not _usable(close):
        return None
    assert close is not None
    if dps == 0:
        return Decimal(0)
    pct, price = f.valuation_yield_pct, f.valuation_price
    if not (_usable(pct) and _usable(price)):
        return None
    assert pct is not None and price is not None
    with localcontext(sim.CTX):
        return pct * price / 100 / close


def characteristics(inputs: rb.SnapshotInputs, ids: Iterable[int]) -> dict[int, Characteristic]:
    """The table for ``ids`` (each an S₀ name of the snapshot) at D = ``inputs.last_session``."""
    d = inputs.last_session
    facts = {f.instrument_id: f for f in inputs.facts}
    spy = usable_closes([b[0] for b in inputs.spy.bars], [b[1] for b in inputs.spy.bars])
    out: dict[int, Characteristic] = {}
    with localcontext(sim.CTX):
        for iid in sorted(set(ids)):
            f = facts.get(iid)
            if f is None:
                raise rb.SnapshotIntegrityError(f"{iid}: an exposure name outside the snapshot's S₀")
            sector = resolve_sector_spdr(f.sic)
            cap = f.market_cap_usd
            closes = usable_closes(f.bar_dates, f.bar_rows)
            b, reason = beta(closes, spy, d)
            atr = pot.atr14(f, last_session=d)
            close = closes.get(d)
            out[iid] = Characteristic(
                sector=NO_SECTOR if sector is None else sector.spdr_symbol,
                ln_cap=cap.ln() if cap is not None and cap.is_finite() and cap > 0 else None,
                beta=b,
                beta_reason=reason,
                atr_pct=None if atr is None or close is None else Decimal(atr.numerator) / atr.denominator / close,
                div_yield=dividend_yield(f, close),
            )
    return out


def _s(x: Decimal | None) -> str | None:
    return None if x is None else str(x)


def _d(x: str | None) -> Decimal | None:
    return None if x is None else Decimal(x)


def encode_table(table: Mapping[int, Characteristic]) -> list[list[Any]]:
    return [
        [iid, c.sector, _s(c.ln_cap), _s(c.beta), c.beta_reason, _s(c.atr_pct), _s(c.div_yield)]
        for iid, c in sorted(table.items())
    ]


def decode_table(doc: Sequence[Sequence[Any]]) -> dict[int, Characteristic]:
    out: dict[int, Characteristic] = {}
    for iid, sector, ln_cap, b, reason, atr, dy in doc:
        c = Characteristic(str(sector), _d(ln_cap), _d(b), reason, _d(atr), _d(dy))
        if any((x := c.value(name)) is not None and not x.is_finite() for name in FIELDS):
            raise rb.SnapshotIntegrityError(f"{iid}: a stored characteristic is not finite")
        out[int(iid)] = c
    return out


@dataclass
class Sums:
    """Additive exposure sums: one book over one session, or accumulated over a window."""

    capital: Decimal = Decimal(0)
    sector: dict[str, Decimal] = field(default_factory=dict)
    covered: dict[str, Decimal] = field(default_factory=lambda: dict.fromkeys(FIELDS, Decimal(0)))
    weighted: dict[str, Decimal] = field(default_factory=lambda: dict.fromkeys(FIELDS, Decimal(0)))

    def add_weight(self, v: Decimal, c: Characteristic) -> None:
        self.capital += v
        self.sector[c.sector] = self.sector.get(c.sector, Decimal(0)) + v
        for name in FIELDS:
            x = c.value(name)
            if x is not None:
                self.covered[name] += v
                self.weighted[name] += v * x

    def add(self, other: Sums) -> None:
        self.capital += other.capital
        for k, v in other.sector.items():
            self.sector[k] = self.sector.get(k, Decimal(0)) + v
        for name in FIELDS:
            self.covered[name] += other.covered[name]
            self.weighted[name] += other.weighted[name]

    def doc(self) -> dict[str, Any]:
        sector = {k: str(v) for k, v in sorted(self.sector.items())}
        out: dict[str, Any] = {"capital": str(self.capital), "sector": sector}
        for name in FIELDS:
            out[name] = [str(self.covered[name]), str(self.weighted[name])]
        return out

    @classmethod
    def of_doc(cls, doc: Mapping[str, Any]) -> Sums:
        return cls(
            capital=Decimal(doc["capital"]),
            sector={str(k): Decimal(v) for k, v in doc["sector"].items()},
            covered={name: Decimal(doc[name][0]) for name in FIELDS},
            weighted={name: Decimal(doc[name][1]) for name in FIELDS},
        )

    def window(self) -> dict[str, Any]:
        """The window values: sector shares of capital, and per characteristic Σ value·x / Σ covered value with its
        coverage. ``null`` over a zero denominator."""
        with localcontext(sim.CTX):
            out: dict[str, Any] = {
                "sector": {k: _s(v / self.capital) for k, v in sorted(self.sector.items())} if self.capital else {}
            }
            for name in FIELDS:
                cov = self.covered[name]
                out[name] = {
                    "value": _s(self.weighted[name] / cov) if cov else None,
                    "coverage": _s(cov / self.capital) if self.capital else None,
                }
            ln = out["ln_cap"]["value"]
            out["ln_cap"]["exp"] = None if ln is None else str(Decimal(ln).exp())
            return out


def book_sums(state: sim.BookState, table: Mapping[int, Characteristic]) -> Sums:
    """The positions held after a step, each weighing its ``value``, in the book's position order."""
    sums = Sums()
    with localcontext(sim.CTX):
        for p in state.positions:
            c = table.get(p.instrument_id)
            if c is None:
                raise rb.SnapshotIntegrityError(f"{p.instrument_id}: a held name is missing from the table")
            if not (p.value.is_finite() and p.value >= 0):
                raise rb.SnapshotIntegrityError(f"{p.instrument_id}: a held value {p.value} is not finite and ≥ 0")
            sums.add_weight(p.value, c)
    return sums


def universe_sums(r_ids: Iterable[int], table: Mapping[int, Characteristic], sessions: int) -> Sums:
    """R_t at equal weight, 1 / |R_t| per member per session, over ``sessions`` sessions."""
    ids = sorted(set(r_ids))
    sums = Sums()
    if not ids:
        return sums
    with localcontext(sim.CTX):
        w = Decimal(sessions) / len(ids)
        for iid in ids:
            c = table.get(iid)
            if c is None:
                raise rb.SnapshotIntegrityError(f"{iid}: an R_t name missing from the characteristics table")
            sums.add_weight(w, c)
    return sums
