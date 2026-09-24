"""Frozen measurement mechanics for preregistered R6 arm #2908, plus #3362's termination policies.

#3362 (spec ``docs/proposals/ta/2026-09-24-3362-termination-wiring.md``) replaces #2908's
``best``/``worst`` literal with :class:`TerminationPolicy`. The two ``legacy_*`` policies reproduce
#2908 exactly; the three programme policies split a missing bar into terminated / gap / alive at
capture on the STORED series bounds and price terminations with ``series_termination``'s classes.

⚠ ``series_termination``'s docstring still says it "SHIPS UNWIRED". That is stale (#2721 step 3
wired it into ``backtest_run``) but deliberately NOT edited: its ``_code_hash()`` covers comments,
so a prose fix would move ``TERMINATION_RULE_VERSION`` and every strategy identity carrying it. The
same holds for ``universe_selection.py`` (``UNIVERSE_SELECTION_RULE_VERSION``), which is why the
evidence reader below lives here rather than beside ``load_universe_selection``.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import statistics
import zipfile
from collections import Counter, defaultdict
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Final, Literal

import psycopg

from app.services.r6_dilution_exclusion import NsiInput, assign_nsi_portfolios, exclusions
from app.services.r6_pit_bundle import R6PitBundle
from app.services.series_termination import (
    TERMINATION_RULE_VERSION,
    TerminationClass,
    TerminationEvidence,
    classify_termination,
    terminal_value_fraction,
)
from app.services.universe_selection import (
    ALIVE_CUT_DAYS,
    INTRADER_CAPTURE_DATE,
    SURVIVORSHIP_FREE_VENDOR,
    vendor_symbol_has_bankruptcy_suffix,
)

HALF_SPREAD: Final = 0.00725
WINDOW_END: Final = date(2024, 9, 27)
REFERENCE_MEMBER: Final = "inv_monthly_2025/portf_nsi_monthly_2025.csv"
TerminationCase = Literal["best", "worst"]
RealisationStatus = Literal["terminated", "gap", "alive_at_capture", "unsplit"]

#: The linkage provenance of the stored Form 25 association (#3361 cross-check (b)): the dated CIK
#: link, built without Form 25, against the Form 25 ``issuer_cik`` over 1,041 series — 879 agree,
#: 3 disagree, 159 not comparable. A source cross-check on a subset; it certifies nothing about the
#: rest, and the verdict policy reads no class anyway.
LINKAGE_PROVENANCE: Final[Mapping[str, str]] = {
    "artefact": "research/security_linkage_3361/crosscheck-2026-09-24-f6ae1edd.json",
    "artefact_sha256": "63bb68ee21e535663f24e7f9903113bd438d89c9456073ae106d3d004c45ae40",
    "bundle_manifest_sha256": "32a281d8e3188aed9a8b985b06a838f2a1a116a2d9b676918c5cf84cc80d4eb6",
}


@dataclass(frozen=True)
class SeriesEvidence:
    """One series' stored bounds and termination evidence (``research_price_series``)."""

    series_id: int
    first_bar: date
    last_bar: date
    evidence: TerminationEvidence

    @property
    def termination_class(self) -> TerminationClass:
        return classify_termination(self.evidence)


_EVIDENCE_SQL: Final = """
    SELECT series_id, vendor_symbol, first_bar, last_bar, delisting_source, delisting_provision
    FROM research_price_series
    WHERE vendor = %(vendor)s
      AND bar_count IS NOT NULL
    ORDER BY series_id
"""


def load_series_evidence(conn: psycopg.Connection[Any], *, symbols: Collection[str]) -> dict[str, SeriesEvidence]:
    """Stored evidence for every requested Intrader symbol; a missing or duplicated symbol raises.

    The evidence is built exactly as ``universe_selection.load_universe_selection`` builds it
    (``linked = delisting_source == 'sec_form25'``, the stored provision, the vendor-symbol Q
    suffix) — the full-population parity check in ``scripts/measure_3362_termination_census.py``
    is the guard against the two spellings drifting.
    """
    row = conn.execute(
        "SELECT max(last_bar) FROM research_price_series WHERE vendor = %(vendor)s",
        {"vendor": SURVIVORSHIP_FREE_VENDOR},
    ).fetchone()
    if (row[0] if row else None) != INTRADER_CAPTURE_DATE:
        raise RuntimeError(f"Intrader capture moved: max(last_bar) is {row[0] if row else None}")
    wanted = {symbol.upper() for symbol in symbols}
    found: dict[str, SeriesEvidence] = {}
    for series_id, vendor_symbol, first_bar, last_bar, source, provision in conn.execute(
        _EVIDENCE_SQL, {"vendor": SURVIVORSHIP_FREE_VENDOR}
    ):
        symbol = str(vendor_symbol).strip().upper()
        if symbol not in wanted:
            continue
        if symbol in found:
            raise RuntimeError(f"two Intrader series carry symbol {symbol}")
        found[symbol] = SeriesEvidence(
            series_id=int(series_id),
            first_bar=first_bar,
            last_bar=last_bar,
            evidence=TerminationEvidence(
                linked=(source == "sec_form25"),
                provision=provision,
                q_suffix=vendor_symbol_has_bankruptcy_suffix(str(vendor_symbol)),
            ),
        )
    missing = sorted(wanted - set(found))
    if missing:
        raise RuntimeError(f"{len(missing)} priced symbols have no Intrader series row: {missing[:10]}")
    return found


@dataclass(frozen=True)
class TerminationPolicy:
    """What a held symbol with no bar on a valuation session realises, as a fraction of its last close.

    ``terminal_fractions is None`` is a LEGACY policy: no status split, every missing bar realises
    ``gap_fraction`` (#2908's ``best``/``worst`` literal, reproduced exactly). Otherwise the policy
    is evidence-bound and must price every ``TerminationClass`` exactly once.
    """

    label: str
    gap_fraction: float
    terminal_fractions: tuple[tuple[TerminationClass, float], ...] | None

    def __post_init__(self) -> None:
        fractions = [self.gap_fraction] + [f for _, f in self.terminal_fractions or ()]
        if not all(math.isfinite(f) and 0.0 <= f <= 1.0 for f in fractions):
            raise ValueError(f"policy {self.label!r} has a fraction outside [0, 1]")
        if self.terminal_fractions is not None:
            classes = [c for c, _ in self.terminal_fractions]
            if classes != sorted(TerminationClass):
                raise ValueError(f"policy {self.label!r} must price every TerminationClass once, in order")

    @property
    def needs_evidence(self) -> bool:
        return self.terminal_fractions is not None

    def terminal_fraction(self, termination_class: TerminationClass) -> float:
        if self.terminal_fractions is None:
            raise ValueError(f"legacy policy {self.label!r} has no class split")
        return dict(self.terminal_fractions)[termination_class]

    def identity(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "gap_fraction": self.gap_fraction,
            "status_split": self.needs_evidence,
            "terminal_fractions": None
            if self.terminal_fractions is None
            else {str(c): f for c, f in self.terminal_fractions},
        }


def _by_class(fraction: Any) -> tuple[tuple[TerminationClass, float], ...]:
    return tuple((c, float(fraction(c))) for c in sorted(TerminationClass))


LEGACY_BEST: Final = TerminationPolicy("legacy_best", 1.0, None)
LEGACY_WORST: Final = TerminationPolicy("legacy_worst", 0.0, None)
#: GOVERNING: every termination and every gap realises nothing (#2908's worst case), except that a
#: symbol alive at capture is valued at its last close at the window end.
ZERO_RECOVERY: Final = TerminationPolicy("zero_recovery", 0.0, _by_class(lambda _c: 0.0))
CLASSIFIED_WORST: Final = TerminationPolicy(
    "classified_worst", 0.0, _by_class(lambda c: terminal_value_fraction(c, "worst_case"))
)
CLASSIFIED_BEST: Final = TerminationPolicy(
    "classified_best", 1.0, _by_class(lambda c: terminal_value_fraction(c, "best_case"))
)
PROGRAMME_POLICIES: Final = (ZERO_RECOVERY, CLASSIFIED_WORST, CLASSIFIED_BEST)
_LEGACY_CASES: Final[Mapping[TerminationCase, TerminationPolicy]] = {"best": LEGACY_BEST, "worst": LEGACY_WORST}


@dataclass(frozen=True)
class Realisation:
    """One held position valued off a missing bar. Values are position values, before cost."""

    session: date
    last_bar: date
    symbol: str
    status: RealisationStatus
    termination_class: TerminationClass | None
    shares: float
    last_close_value: float
    realised_value: float


@dataclass(frozen=True)
class PriceBar:
    day: date
    adjusted_open: float
    adjusted_close: float


@dataclass(frozen=True)
class PriceSeries:
    symbol: str
    bars: tuple[PriceBar, ...]
    invalid_rows: int

    @property
    def by_date(self) -> dict[date, PriceBar]:
        return {bar.day: bar for bar in self.bars}


@dataclass(frozen=True)
class RebalanceEvent:
    day: date
    pre_cost_wealth: float
    traded_notional: float
    spread_cost: float
    target_count: int
    censored_holdings: int


@dataclass(frozen=True)
class PortfolioResult:
    total_return: float
    events: tuple[RebalanceEvent, ...]
    realisations: tuple[Realisation, ...] = ()

    @property
    def traded_notional_over_initial_capital(self) -> float:
        return sum(event.traded_notional for event in self.events)

    @property
    def spread_cost_over_initial_capital(self) -> float:
        return sum(event.spread_cost for event in self.events)


@dataclass(frozen=True)
class FactorValidation:
    months: int
    window_start: str
    window_end: str
    correlation: float
    alpha: float
    beta: float
    lag_correlation: float
    lead_correlation: float
    passed: bool


def read_price_series(path: Path) -> PriceSeries:
    bars: list[PriceBar] = []
    invalid = 0
    with path.open(encoding="utf-8", errors="strict", newline="") as handle:
        for row in csv.reader(handle):
            if len(row) != 9:
                invalid += 1
                continue
            try:
                day = date.fromisoformat(row[0])
                raw_open = float(row[1])
                raw_close = float(row[4])
                adjusted_close = float(row[8])
            except ValueError:
                invalid += 1
                continue
            if not all(math.isfinite(value) and value > 0 for value in (raw_open, raw_close, adjusted_close)):
                invalid += 1
                continue
            bars.append(
                PriceBar(
                    day=day,
                    adjusted_open=raw_open * adjusted_close / raw_close,
                    adjusted_close=adjusted_close,
                )
            )
    days = [bar.day for bar in bars]
    if days != sorted(set(days)):
        raise RuntimeError(f"valid price bars are duplicate or unordered: {path}")
    if not bars:
        raise RuntimeError(f"price series has no valid bars: {path}")
    return PriceSeries(symbol=path.stem.upper(), bars=tuple(bars), invalid_rows=invalid)


def load_required_prices(bundle: R6PitBundle, price_dir: Path) -> dict[str, PriceSeries]:
    symbols = sorted({row.symbol for row in bundle.records})
    return {symbol: read_price_series(price_dir / f"{symbol}.csv") for symbol in symbols}


def signal_sets(bundle: R6PitBundle, prices: dict[str, PriceSeries]) -> dict[datetime, dict[str, frozenset[str]]]:
    result: dict[datetime, dict[str, frozenset[str]]] = {}
    for formation in sorted({row.formation_close for row in bundle.records}):
        inputs = tuple(
            NsiInput(
                symbol=row.symbol,
                exchange=row.exchange,
                current_shares=row.current_shares,
                prior_shares=row.prior_shares,
                red_flag_scores=row.red_flag_scores,
                red_flag_history_complete=row.red_flag_history_complete,
            )
            for row in bundle.records_at(formation)
        )
        portfolios = assign_nsi_portfolios(inputs, nyse_exchange_names=frozenset({"NYSE"}))
        excluded = exclusions(inputs, portfolios)
        executable = frozenset(
            row.symbol
            for row in inputs
            if any(
                bar.day > formation.date() and (bar.day - formation.date()).days <= 7 for bar in prices[row.symbol].bars
            )
        )
        result[formation] = {
            "full": executable,
            **{name: executable - symbols for name, symbols in excluded.items()},
            "dilution_excluded": excluded["dilution"] & executable,
            "red_flag_excluded": excluded["red_flag"] & executable,
            "union_excluded": excluded["union"] & executable,
        }
    return result


def _execution_day(formation: datetime, members: frozenset[str], prices: dict[str, PriceSeries]) -> date:
    observed: set[date] = set()
    for symbol in members:
        later = [bar.day for bar in prices[symbol].bars if bar.day > formation.date()]
        if not later or (later[0] - formation.date()).days > 7:
            raise RuntimeError(f"target {symbol} has no admissible post-formation fill")
        observed.add(later[0])
    if len(observed) != 1:
        raise RuntimeError(f"formation {formation.isoformat()} has asynchronous entry sessions: {sorted(observed)}")
    return next(iter(observed))


def _exact_price(series: PriceSeries, day: date, field: Literal["open", "close"]) -> float | None:
    exact = next((bar for bar in series.bars if bar.day == day), None)
    if exact is None:
        return None
    return exact.adjusted_open if field == "open" else exact.adjusted_close


def _holding_value(
    symbol: str,
    series: PriceSeries,
    shares: float,
    day: date,
    *,
    field: Literal["open", "close"],
    policy: TerminationPolicy,
    evidence: Mapping[str, SeriesEvidence] | None,
    window_end: date,
) -> tuple[float, Realisation | None]:
    """A holding's value on ``day``. A bar on ``day`` is always an ordinary price; only a MISSING bar
    consults the policy, and it prices off the last valid close before ``day``."""
    price = _exact_price(series, day, field)
    if price is not None:
        return shares * price, None
    prior = [bar for bar in series.bars if bar.day < day]
    if not prior:
        raise RuntimeError(f"{series.symbol} has no price at or before required session {day}")
    last = prior[-1]
    termination_class: TerminationClass | None = None
    status: RealisationStatus
    if policy.terminal_fractions is None:
        # #2908's literal: a halted holding is no more executable than a terminated one on the
        # rebalance session, so both take the same bound.
        status, fraction = "unsplit", policy.gap_fraction
    else:
        if evidence is None:
            raise RuntimeError(f"policy {policy.label!r} needs series evidence")
        stored = evidence[symbol]
        alive_at_capture = stored.last_bar > INTRADER_CAPTURE_DATE - timedelta(days=ALIVE_CUT_DAYS)
        if alive_at_capture and day == window_end:
            # Alive at capture (``universe_selection``'s rule): missing the final bar is capture
            # timing, not a termination.
            status, fraction = "alive_at_capture", 1.0
        elif alive_at_capture or stored.last_bar > day:
            # An alive series never terminates; any earlier missing session is a gap.
            status, fraction = "gap", policy.gap_fraction
        else:
            termination_class = stored.termination_class
            status, fraction = "terminated", policy.terminal_fraction(termination_class)
    last_close_value = shares * last.adjusted_close
    realised = last_close_value * fraction
    if shares <= 0:
        return realised, None
    return realised, Realisation(
        session=day,
        last_bar=last.day,
        symbol=symbol,
        status=status,
        termination_class=termination_class,
        shares=shares,
        last_close_value=last_close_value,
        realised_value=realised,
    )


def _target_value(
    pre_cost_wealth: float,
    current: dict[str, float],
    target: frozenset[str],
    half_spread: float,
) -> float:
    if not target:
        raise RuntimeError("portfolio target is empty")

    def residual(value: float) -> float:
        traded = sum(abs(value - current.get(symbol, 0.0)) for symbol in target)
        traded += sum(amount for symbol, amount in current.items() if symbol not in target)
        return len(target) * value + half_spread * traded - pre_cost_wealth

    lower = 0.0
    upper = pre_cost_wealth / len(target)
    for _ in range(100):
        middle = (lower + upper) / 2
        if residual(middle) > 0:
            upper = middle
        else:
            lower = middle
    return (lower + upper) / 2


Schedule = tuple[tuple[datetime, frozenset[str]], ...]


def simulate_portfolio(
    *,
    schedule: Schedule,
    prices: dict[str, PriceSeries],
    policy: TerminationPolicy,
    half_spread: float,
    window_end: date = WINDOW_END,
    evidence: Mapping[str, SeriesEvidence] | None = None,
) -> PortfolioResult:
    if policy.needs_evidence and evidence is None:
        raise RuntimeError(f"policy {policy.label!r} needs series evidence")
    holdings: dict[str, float] = {}
    cash = 1.0
    events: list[RebalanceEvent] = []
    realisations: list[Realisation] = []

    def value_holdings(day: date, field: Literal["open", "close"]) -> dict[str, float]:
        values: dict[str, float] = {}
        for symbol, shares in holdings.items():
            value, realisation = _holding_value(
                symbol,
                prices[symbol],
                shares,
                day,
                field=field,
                policy=policy,
                evidence=evidence,
                window_end=window_end,
            )
            values[symbol] = value
            if realisation is not None:
                realisations.append(realisation)
        return values

    for formation, target in schedule:
        day = _execution_day(formation, target, prices)
        if day > window_end:
            raise RuntimeError(
                f"formation {formation.isoformat()} executes on {day}, after the window end {window_end}"
            )
        current = value_holdings(day, "open")
        censored = sum(day not in prices[symbol].by_date for symbol in holdings)
        pre_cost = cash + sum(current.values())
        target_value = _target_value(pre_cost, current, target, half_spread)
        traded = sum(abs(target_value - current.get(symbol, 0.0)) for symbol in target)
        traded += sum(amount for symbol, amount in current.items() if symbol not in target)
        cost = half_spread * traded
        if not math.isclose(len(target) * target_value + cost, pre_cost, rel_tol=1e-10, abs_tol=1e-12):
            raise RuntimeError("rebalance cash conservation failed")
        target_prices: dict[str, float] = {}
        for symbol in target:
            price = _exact_price(prices[symbol], day, "open")
            if price is None:
                raise RuntimeError(f"target {symbol} has no bar on its execution session {day}")
            target_prices[symbol] = price
        holdings = {symbol: target_value / target_prices[symbol] for symbol in target}
        cash = 0.0
        events.append(RebalanceEvent(day, pre_cost, traded, cost, len(target), censored))

    final_mid = sum(value_holdings(window_end, "close").values())
    final_traded = final_mid
    final_cost = half_spread * final_traded
    final_censored = sum(window_end not in prices[symbol].by_date for symbol in holdings)
    events.append(RebalanceEvent(window_end, final_mid, final_traded, final_cost, 0, final_censored))
    return PortfolioResult(
        total_return=final_mid - final_cost - 1.0,
        events=tuple(events),
        realisations=tuple(realisations),
    )


def simulate_legacy_case(
    *,
    schedule: Schedule,
    prices: dict[str, PriceSeries],
    case: TerminationCase,
    half_spread: float,
    window_end: date = WINDOW_END,
) -> PortfolioResult:
    """#2908's frozen ``best``/``worst`` call, unchanged in output."""
    return simulate_portfolio(
        schedule=schedule, prices=prices, policy=_LEGACY_CASES[case], half_spread=half_spread, window_end=window_end
    )


def simulate_under_policies(
    *,
    schedules: Mapping[str, Schedule],
    prices: dict[str, PriceSeries],
    policies: tuple[TerminationPolicy, ...],
    half_spread: float,
    window_end: date = WINDOW_END,
    evidence: Mapping[str, SeriesEvidence] | None = None,
) -> dict[str, dict[str, PortfolioResult]]:
    """Every named schedule (arm AND control) under each policy — the one entry point a declaration
    harness uses, so the termination rule is applied identically to both by construction."""
    labels = [policy.label for policy in policies]
    if not policies or len(set(labels)) != len(labels):
        raise ValueError(f"policy labels must be non-empty and unique: {labels}")
    for policy in policies:
        if policy.label == ZERO_RECOVERY.label and policy != ZERO_RECOVERY:
            raise ValueError("a policy labelled zero_recovery must be ZERO_RECOVERY itself")
    if any(policy.needs_evidence for policy in policies):
        if ZERO_RECOVERY not in policies:
            raise ValueError("programme policies require the governing ZERO_RECOVERY policy")
        if evidence is None:
            raise ValueError("programme policies require series evidence")
        for symbol, series in prices.items():
            stored = evidence.get(symbol)
            if stored is None:
                raise ValueError(f"no series evidence for priced symbol {symbol}")
            if (series.bars[0].day, series.bars[-1].day) != (stored.first_bar, stored.last_bar):
                raise ValueError(
                    f"{symbol}: loaded bars {series.bars[0].day}..{series.bars[-1].day} do not match the stored "
                    f"series {stored.first_bar}..{stored.last_bar} — a clipped load or a mirror/DB drift"
                )
    for name, schedule in schedules.items():
        formations = [formation for formation, _ in schedule]
        if not formations or formations != sorted(set(formations)):
            raise ValueError(f"schedule {name!r} must be non-empty, sorted and unique by formation")
        if formations[-1].date() >= window_end:
            raise ValueError(f"schedule {name!r} forms on or after the window end {window_end}")
    return {
        policy.label: {
            name: simulate_portfolio(
                schedule=schedule,
                prices=prices,
                policy=policy,
                half_spread=half_spread,
                window_end=window_end,
                evidence=evidence,
            )
            for name, schedule in schedules.items()
        }
        for policy in policies
    }


def binding_policy(edges: Mapping[str, float]) -> tuple[str, float]:
    """The binding arm-minus-control figure: the MINIMUM across the programme policies.

    A lower recovery can hurt the control more than the arm, so the governing policy is not
    automatically the arm's worst case; the classified policies can turn a pass into a fail, never
    rescue one. ``zero_recovery`` must be present.
    """
    if ZERO_RECOVERY.label not in edges:
        raise ValueError("the governing zero_recovery edge is missing")
    label = min(edges, key=lambda key: (edges[key], key))
    return label, edges[label]


def evidence_sha256(evidence: Mapping[str, SeriesEvidence]) -> str:
    rows = [
        {
            "symbol": symbol,
            "series_id": item.series_id,
            "first_bar": item.first_bar.isoformat(),
            "last_bar": item.last_bar.isoformat(),
            "linked": item.evidence.linked,
            "provision": item.evidence.provision,
            "q_suffix": item.evidence.q_suffix,
            "class": str(item.termination_class),
        }
        for symbol, item in sorted(evidence.items())
    ]
    return hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def termination_identity(
    policies: tuple[TerminationPolicy, ...], evidence: Mapping[str, SeriesEvidence] | None
) -> dict[str, Any]:
    """What a result must record so its termination treatment is reproducible."""
    return {
        "termination_rule_version": TERMINATION_RULE_VERSION,
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "policies": [policy.identity() for policy in policies],
        "verdict": "minimum arm-minus-control edge across the policies; zero_recovery governs",
        "evidence_source": "research_price_series (delisting_source, delisting_provision, vendor_symbol)",
        "evidence_sha256": None if evidence is None else evidence_sha256(evidence),
        "intrader_capture_date": INTRADER_CAPTURE_DATE.isoformat(),
        "alive_cut_days": ALIVE_CUT_DAYS,
        "linkage_provenance": dict(LINKAGE_PROVENANCE),
    }


def universe_census(
    prices: Mapping[str, PriceSeries], evidence: Mapping[str, SeriesEvidence], window_end: date = WINDOW_END
) -> dict[str, Any]:
    """Priced symbols split into live / alive at capture / terminated (by class); reconciles to the total."""
    alive_floor = INTRADER_CAPTURE_DATE - timedelta(days=ALIVE_CUT_DAYS)
    live = alive = 0
    terminated: Counter[str] = Counter()
    for symbol in prices:
        stored = evidence[symbol]
        if stored.last_bar >= window_end:
            live += 1
        elif stored.last_bar > alive_floor:
            alive += 1
        else:
            terminated[str(stored.termination_class)] += 1
    if live + alive + sum(terminated.values()) != len(prices):
        raise RuntimeError("universe census does not reconcile to the priced symbols")
    return {
        "priced": len(prices),
        "live": live,
        "alive_at_capture": alive,
        "terminated_by_class": dict(sorted(terminated.items())),
    }


def realisation_census(result: PortfolioResult) -> dict[str, dict[str, float]]:
    """Count and summed position values per ``status:class``."""
    out: dict[str, dict[str, float]] = {}
    for item in result.realisations:
        key = f"{item.status}:{item.termination_class or '-'}"
        cell = out.setdefault(key, {"count": 0, "last_close_value": 0.0, "realised_value": 0.0})
        cell["count"] += 1
        cell["last_close_value"] += item.last_close_value
        cell["realised_value"] += item.realised_value
    return dict(sorted(out.items()))


def month_pairs(start: tuple[int, int], end: tuple[int, int]) -> tuple[tuple[int, int], ...]:
    result: list[tuple[int, int]] = []
    year, month = start
    while (year, month) <= end:
        result.append((year, month))
        month += 1
        if month == 13:
            year += 1
            month = 1
    return tuple(result)


def _last_bar_on_or_before(series: PriceSeries, cutoff: date) -> PriceBar | None:
    eligible = [bar for bar in series.bars if bar.day <= cutoff]
    return eligible[-1] if eligible else None


def _monthly_return(series: PriceSeries, start_session: date, end_session: date) -> float | None:
    start = _last_bar_on_or_before(series, start_session)
    end = _last_bar_on_or_before(series, end_session)
    if start is None or end is None or end.day <= start.day:
        return None
    if series.bars[-1].day < end_session:
        return -1.0
    return end.adjusted_close / start.adjusted_close - 1.0


def constructed_nsi_factor(
    bundle: R6PitBundle,
    prices: dict[str, PriceSeries],
    *,
    calendar_symbol: str = "A",
) -> dict[tuple[int, int], float]:
    calendar = prices[calendar_symbol]
    formations = sorted({row.formation_close for row in bundle.records})
    assignments: dict[datetime, tuple[frozenset[str], frozenset[str]]] = {}
    for formation in formations:
        rows = tuple(
            NsiInput(
                row.symbol,
                row.exchange,
                row.current_shares,
                row.prior_shares,
                row.red_flag_scores,
                row.red_flag_history_complete,
            )
            for row in bundle.records_at(formation)
        )
        ranked = assign_nsi_portfolios(rows, nyse_exchange_names=frozenset({"NYSE"}))
        assignments[formation] = (
            frozenset(symbol for symbol, rank in ranked.items() if rank == 1),
            frozenset(symbol for symbol, rank in ranked.items() if rank == 10),
        )

    sessions_by_month: dict[tuple[int, int], list[date]] = defaultdict(list)
    for bar in calendar.bars:
        if date(2022, 6, 1) <= bar.day <= WINDOW_END:
            sessions_by_month[(bar.day.year, bar.day.month)].append(bar.day)
    output: dict[tuple[int, int], float] = {}
    for year, month in month_pairs((2022, 7), (2024, 9)):
        formation = max(value for value in formations if value.date() < date(year, month, 28))
        low, high = assignments[formation]
        prior_month = (year - 1, 12) if month == 1 else (year, month - 1)
        start_session = sessions_by_month[prior_month][-1]
        end_session = sessions_by_month[(year, month)][-1]
        low_returns = [
            value
            for symbol in low
            if (value := _monthly_return(prices[symbol], start_session, end_session)) is not None
        ]
        high_returns = [
            value
            for symbol in high
            if (value := _monthly_return(prices[symbol], start_session, end_session)) is not None
        ]
        if not low_returns or not high_returns:
            raise RuntimeError(f"empty Nsi factor leg for {year:04d}-{month:02d}")
        output[(year, month)] = statistics.fmean(high_returns) - statistics.fmean(low_returns)
    return output


def read_global_q_nsi(path: Path) -> dict[tuple[int, int], float]:
    legs: dict[tuple[int, int], dict[int, float]] = defaultdict(dict)
    with zipfile.ZipFile(path) as archive, archive.open(REFERENCE_MEMBER) as raw:
        import io

        reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig"))
        if reader.fieldnames != ["year", "month", "rank_NSI", "nstocks", "ret_vw"]:
            raise RuntimeError(f"unexpected global-q Nsi header: {reader.fieldnames}")
        for row in reader:
            key = (int(row["year"]), int(row["month"]))
            rank = int(row["rank_NSI"])
            value = float(row["ret_vw"]) / 100.0
            if rank in legs[key]:
                raise RuntimeError(f"duplicate global-q Nsi rank {rank} at {key}")
            legs[key][rank] = value
    return {key: values[10] - values[1] for key, values in legs.items() if 1 in values and 10 in values}


def _pearson(left: list[float], right: list[float]) -> float:
    left_mean = statistics.fmean(left)
    right_mean = statistics.fmean(right)
    numerator = sum((x - left_mean) * (y - right_mean) for x, y in zip(left, right, strict=True))
    denominator = math.sqrt(sum((x - left_mean) ** 2 for x in left) * sum((y - right_mean) ** 2 for y in right))
    if denominator == 0:
        raise RuntimeError("factor correlation has zero variance")
    return numerator / denominator


def validate_factor(ours: dict[tuple[int, int], float], reference: dict[tuple[int, int], float]) -> FactorValidation:
    keys = sorted(set(ours) & set(reference))
    if len(keys) < 24:
        raise RuntimeError(f"factor validation needs at least 24 overlapping months, got {len(keys)}")
    left = [ours[key] for key in keys]
    right = [reference[key] for key in keys]
    correlation = _pearson(left, right)
    right_mean = statistics.fmean(right)
    left_mean = statistics.fmean(left)
    variance = sum((value - right_mean) ** 2 for value in right)
    beta = sum((x - right_mean) * (y - left_mean) for x, y in zip(right, left, strict=True)) / variance
    alpha = left_mean - beta * right_mean
    lag = _pearson(left[1:], right[:-1])
    lead = _pearson(left[:-1], right[1:])
    passed = correlation >= 0.20 and beta > 0 and abs(correlation) >= max(abs(lag), abs(lead))
    return FactorValidation(
        months=len(keys),
        window_start=f"{keys[0][0]:04d}-{keys[0][1]:02d}",
        window_end=f"{keys[-1][0]:04d}-{keys[-1][1]:02d}",
        correlation=correlation,
        alpha=alpha,
        beta=beta,
        lag_correlation=lag,
        lead_correlation=lead,
        passed=passed,
    )


def haircut_net_return(
    *,
    strategy_gross: float,
    strategy_net: float,
    buy_hold_gross: float,
    haircut: float,
) -> float:
    edge = strategy_gross - buy_hold_gross
    adjusted_edge = edge if edge <= 0 else edge * (1 - haircut)
    full_strategy_cost_drag = strategy_gross - strategy_net
    return buy_hold_gross + adjusted_edge - full_strategy_cost_drag


__all__ = [
    "CLASSIFIED_BEST",
    "CLASSIFIED_WORST",
    "HALF_SPREAD",
    "LEGACY_BEST",
    "LEGACY_WORST",
    "LINKAGE_PROVENANCE",
    "PROGRAMME_POLICIES",
    "WINDOW_END",
    "ZERO_RECOVERY",
    "FactorValidation",
    "PortfolioResult",
    "Realisation",
    "SeriesEvidence",
    "TerminationPolicy",
    "binding_policy",
    "constructed_nsi_factor",
    "evidence_sha256",
    "haircut_net_return",
    "load_required_prices",
    "load_series_evidence",
    "read_global_q_nsi",
    "read_price_series",
    "realisation_census",
    "signal_sets",
    "simulate_legacy_case",
    "simulate_portfolio",
    "simulate_under_policies",
    "termination_identity",
    "universe_census",
    "validate_factor",
]
