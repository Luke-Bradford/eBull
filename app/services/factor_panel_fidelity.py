"""#3609 step 1 slice 4: the construction-fidelity comparison, its registration gate and its ledger.

Spec: ``docs/research/2026-10-04-3609-step1-factor-panel.md`` §"Fidelity", §"Registration, ledger and what step 2
inherits" and §"Slices" item 4 (the slice 4 plan).

Pure apart from the ledger's file appends. Our factor series lives only in memory: nothing here returns, stores or
puts into an error message a monthly factor value, its mean, a cumulative return or the offset's magnitude. The
offset bar's only output is its boolean (§"Fidelity", "Only the boolean is printed or stored").
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import statistics
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

from app.services.factor_panel import ACCOUNTING_CHARACTERISTICS, Group, SortInput, lag_eligible, tercile_groups
from app.services.trial_register import DeclaredTrial, TrialExactness, TrialRegister

ARMS: Final = ("best_case", "worst_case")
ACCOUNTING: Final = ACCOUNTING_CHARACTERISTICS
PRICE_CHARACTERISTICS: Final = ("ret_12_1", "rvol_21d")
CHARACTERISTICS: Final = (*ACCOUNTING, *PRICE_CHARACTERISTICS)
#: §"Fidelity": fewer than 5 names in either leg makes the month missing.
MIN_LEG: Final = 5
#: §"Fidelity": each mask needs at least 60 pairs.
MIN_PAIRS: Final = 60
#: The pass bars, fixed by construction before any run; they never change.
CORRELATION_BAR: Final = {**dict.fromkeys(PRICE_CHARACTERISTICS, 0.90), **dict.fromkeys(ACCOUNTING, 0.80)}
BETA_LOW: Final = 0.7
BETA_HIGH: Final = 1.3
OFFSET_BAR: Final = 0.03
MAX_UNDERSIZED: Final = 2
#: §"Fidelity": a characteristic is rejected after 3 versions without a PASS.
MAX_VERSIONS: Final = 3
#: 8 characteristics x 2 arms (§"Registration").
FIDELITY_SEARCHES: Final = len(CHARACTERISTICS) * len(ARMS)
HOLDING_STATUSES: Final = ("observed", "terminal", "coverage_exit")


class FidelityError(ValueError):
    """A refusal with a stable code; the message names what refused and never a return value."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


class Bar(StrEnum):
    CORRELATION = "correlation"
    LEAD_LAG = "lead_lag"
    BETA = "beta"
    OFFSET = "offset"
    UNDERSIZED = "undersized"


class Verdict(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    INSUFFICIENT = "INSUFFICIENT"
    REJECTED = "REJECTED"


# --------------------------------------------------------------------------- months


def month_key(day: date) -> str:
    return f"{day.year:04d}-{day.month:02d}"


def shift_month(key: str, delta: int) -> str:
    year, month = int(key[:4]), int(key[5:7])
    index = year * 12 + (month - 1) + delta
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def month_last_day(key: str) -> date:
    following = shift_month(key, 1)
    return date.fromordinal(date(int(following[:4]), int(following[5:7]), 1).toordinal() - 1)


# --------------------------------------------------------------------------- one formation


@dataclass(frozen=True)
class Holding:
    name_key: int
    value: float
    me: float
    status: str
    by_arm: Mapping[str, float]


@dataclass(frozen=True)
class FactorMonth:
    low_n: int
    high_n: int
    #: Per arm, sign x (high - low); ``None`` when a leg is undersized.
    returns: Mapping[str, float] | None
    #: Per leg ("low", "high"), each holding status's share of the leg's normalised weight; ``None`` when undersized.
    status_shares: Mapping[str, Mapping[str, float]] | None


def factor_month(holdings: Sequence[Holding], micro_cutoff_usd: float, cap_usd: float, sign: int) -> FactorMonth:
    """JKP terciles on non-micro names, capped value weights (min(ME, NYSE p80)), factor = sign x (high - low)."""
    if sign not in (1, -1):
        raise FidelityError("sign", "a Table 9 sign is 1 or -1")
    for cutoff in (micro_cutoff_usd, cap_usd):
        if not math.isfinite(cutoff) or cutoff <= 0:
            raise FidelityError("cutoff", "an NYSE cutoff is not finite and positive")
    for h in holdings:
        if not math.isfinite(h.me) or h.me <= 0:
            raise FidelityError("me", f"name {h.name_key}: ME is not finite and positive")
        if h.status not in HOLDING_STATUSES:
            raise FidelityError("holding_status", f"name {h.name_key}: unknown holding status")
        finite = all(isinstance(r, float | int) and math.isfinite(r) for r in h.by_arm.values())
        if set(h.by_arm) != set(ARMS) or not finite:
            raise FidelityError("arm_return", f"name {h.name_key}: an arm return is missing or not finite")
    groups = tercile_groups([SortInput(h.name_key, h.value, h.me) for h in holdings], micro_cutoff_usd).groups
    legs = {
        "low": [h for h in holdings if groups[h.name_key] is Group.LOW],
        "high": [h for h in holdings if groups[h.name_key] is Group.HIGH],
    }
    low_n, high_n = len(legs["low"]), len(legs["high"])
    if low_n < MIN_LEG or high_n < MIN_LEG:
        return FactorMonth(low_n, high_n, None, None)

    def leg_return(leg: Sequence[Holding], arm: str) -> float:
        weights = [min(h.me, cap_usd) for h in leg]
        return sum(w * h.by_arm[arm] for w, h in zip(weights, leg, strict=True)) / sum(weights)

    def shares(leg: Sequence[Holding]) -> dict[str, float]:
        total = sum(min(h.me, cap_usd) for h in leg)
        return {s: sum(min(h.me, cap_usd) for h in leg if h.status == s) / total for s in HOLDING_STATUSES}

    returns = {arm: sign * (leg_return(legs["high"], arm) - leg_return(legs["low"], arm)) for arm in ARMS}
    return FactorMonth(low_n, high_n, returns, {leg: shares(names) for leg, names in legs.items()})


# --------------------------------------------------------------------------- comparison


@dataclass(frozen=True)
class ArmResult:
    verdict: Verdict
    failed_bars: tuple[Bar, ...]
    insufficient: tuple[str, ...]
    pairs: Mapping[str, int]
    correlation: Mapping[str, float | None]
    beta: float | None
    te_ratio: float | None
    #: The offset bar's boolean; ``None`` when the comparison is insufficient. Its magnitude is never kept.
    offset_ok: bool | None
    undersized: int

    def as_json(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict.value,
            "failed_bars": [b.value for b in self.failed_bars],
            "insufficient": list(self.insufficient),
            "pairs": dict(self.pairs),
            "correlation": dict(self.correlation),
            "beta": self.beta,
            "te_ratio": self.te_ratio,
            "offset_ok": self.offset_ok,
            "undersized": self.undersized,
        }


def _pearson(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    if statistics.pvariance(xs) == 0 or statistics.pvariance(ys) == 0:
        return None
    return statistics.correlation(xs, ys)


def compare_arm(
    ours: Mapping[str, float],
    published: Mapping[str, float],
    grid: Sequence[str],
    undersized: int,
    correlation_bar: float,
) -> ArmResult:
    """§"Fidelity" comparison contract for one arm. ``ours`` and ``published`` are keyed by calendar month.

    Contemporaneous: m in the grid with both present. Lag: (ours[m], published[m-1]) with m-1 in the grid. Lead:
    (ours[m], published[m+1]) with m+1 in the grid, which on the stage-A grid is m <= 2021-04.
    """
    on_grid = set(grid)
    masks = {
        "contemporaneous": [(ours[m], published[m]) for m in grid if m in ours and m in published],
        "lag": [
            (ours[m], published[p])
            for m in grid
            if (p := shift_month(m, -1)) in on_grid and m in ours and p in published
        ],
        "lead": [
            (ours[m], published[n])
            for m in grid
            if (n := shift_month(m, 1)) in on_grid and m in ours and n in published
        ],
    }
    pairs = {name: len(values) for name, values in masks.items()}
    correlation: dict[str, float | None] = {}
    insufficient: list[str] = []
    for name, values in masks.items():
        if len(values) < MIN_PAIRS:
            insufficient.append(f"{name}_pairs")
            correlation[name] = None
            continue
        got = _pearson([o for o, _ in values], [p for _, p in values])
        if got is None:
            insufficient.append(f"{name}_zero_variance")
        correlation[name] = got
    if insufficient:
        reasons = tuple(insufficient)
        return ArmResult(Verdict.INSUFFICIENT, (), reasons, pairs, correlation, None, None, None, undersized)
    o = [x for x, _ in masks["contemporaneous"]]
    p = [y for _, y in masks["contemporaneous"]]
    beta = statistics.covariance(o, p) / statistics.variance(p)
    diff = [x - y for x, y in zip(o, p, strict=True)]
    te_ratio = statistics.stdev(diff) / statistics.stdev(p)
    offset_ok = abs(12 * statistics.fmean(diff)) <= OFFSET_BAR
    contemporaneous, lag, lead = (correlation[k] for k in ("contemporaneous", "lag", "lead"))
    assert contemporaneous is not None and lag is not None and lead is not None
    failed: list[Bar] = []
    if contemporaneous < correlation_bar:
        failed.append(Bar.CORRELATION)
    if abs(contemporaneous) < abs(lag) or abs(contemporaneous) < abs(lead):
        failed.append(Bar.LEAD_LAG)
    if not BETA_LOW <= beta <= BETA_HIGH:
        failed.append(Bar.BETA)
    if not offset_ok:
        failed.append(Bar.OFFSET)
    if undersized > MAX_UNDERSIZED:
        failed.append(Bar.UNDERSIZED)
    verdict = Verdict.FAIL if failed else Verdict.PASS
    return ArmResult(verdict, tuple(failed), (), pairs, correlation, beta, te_ratio, offset_ok, undersized)


def characteristic_verdict(arms: Mapping[str, ArmResult]) -> Verdict:
    """PASS only if both arms pass every bar; INSUFFICIENT if either arm is; otherwise FAIL."""
    verdicts = {arms[arm].verdict for arm in ARMS}
    if Verdict.INSUFFICIENT in verdicts:
        return Verdict.INSUFFICIENT
    return Verdict.PASS if verdicts == {Verdict.PASS} else Verdict.FAIL


@dataclass
class SeriesAccumulator:
    """One characteristic's monthly results over the grid, kept in memory only."""

    grid: Sequence[str]
    ours: dict[str, dict[str, float]] = field(default_factory=lambda: {arm: {} for arm in ARMS})
    undersized: int = 0
    leg_counts: dict[str, list[int]] = field(default_factory=lambda: {"low": [], "high": []})
    status_shares: dict[str, dict[str, list[float]]] = field(
        default_factory=lambda: {leg: {s: [] for s in HOLDING_STATUSES} for leg in ("low", "high")}
    )

    def add(self, month: str, got: FactorMonth) -> None:
        if month not in self.grid or month in self._seen:
            raise FidelityError("grid", f"holding month {month} is off the grid or repeated")
        self._seen.add(month)
        self.leg_counts["low"].append(got.low_n)
        self.leg_counts["high"].append(got.high_n)
        if got.returns is None:
            self.undersized += 1
            return
        for arm in ARMS:
            self.ours[arm][month] = got.returns[arm]
        assert got.status_shares is not None
        for leg, shares in got.status_shares.items():
            for status, share in shares.items():
                self.status_shares[leg][status].append(share)

    def __post_init__(self) -> None:
        self._seen: set[str] = set()

    def result(self, published: Mapping[str, float], name: str) -> dict[str, Any]:
        # A month with no names at all never reaches ``add``; it is undersized too (no leg has 5 names).
        undersized = self.undersized + (len(self.grid) - len(self._seen))
        arms = {
            arm: compare_arm(self.ours[arm], published, self.grid, undersized, CORRELATION_BAR[name]) for arm in ARMS
        }
        on_grid = [m for m in self.grid if m in published]
        return {
            "verdict": characteristic_verdict(arms).value,
            "arms": {arm: result.as_json() for arm, result in arms.items()},
            "months_used": arms[ARMS[0]].pairs["contemporaneous"],
            "missing_ours": [m for m in self.grid if m not in self.ours[ARMS[0]]],
            "missing_published": [m for m in self.grid if m not in published],
            "published_months": len(on_grid),
            "undersized": undersized,
            "leg_counts": {
                leg: {"min": min(counts) if counts else 0, "median": statistics.median(counts) if counts else 0}
                for leg, counts in self.leg_counts.items()
            },
            # Holding status does not depend on the arm, so one figure serves both.
            "holding_status_weight_both_arms": {
                leg: {s: statistics.fmean(v) if v else None for s, v in by_status.items()}
                for leg, by_status in self.status_shares.items()
            },
        }


# --------------------------------------------------------------------------- registration


def versions_digest(versions: Mapping[str, str]) -> str:
    """sha256 of the canonical JSON of the per-characteristic construction versions."""
    return hashlib.sha256(json.dumps(dict(versions), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def evidence_sha256(trial: DeclaredTrial) -> str:
    return hashlib.sha256(trial.evidence.encode()).hexdigest()


def fidelity_evidence(spec_sha256: str, digest: str) -> str:
    """The register entry's evidence text: the two frozen inputs, each with a fixed label."""
    return (
        'docs/research/2026-10-04-3609-step1-factor-panel.md §"Registration" and §"Slices" item 4; '
        f"spec_sha256={spec_sha256}; construction_versions_sha256={digest}; "
        "ledger var/research/3609_step1/ledger.jsonl, committed to docs/research/3609-ledger.jsonl"
    )


def register_gate(
    register: TrialRegister,
    spec_sha256: str,
    digest: str,
    history: Sequence[Mapping[str, Any]],
) -> DeclaredTrial:
    """The register entry this run is counted under; refuses unless exactly one matches and the past is intact.

    ``history`` is every earlier ``evaluation_began`` ledger row (committed and local). A run entry's evidence can
    never change, each entry names exactly one versions digest, and exactly one entry may name the current one, so
    a digest already run under one trial cannot pass under another, and a later version needs a new row.
    """
    by_id = {trial.trial_id: trial for trial in register.trials}
    for row in history:
        past = by_id.get(row["trial_id"])
        if past is None or evidence_sha256(past) != row["trial_evidence_sha256"]:
            raise FidelityError("register_history", f"trial {row['trial_id']} was run and its entry moved or vanished")
    wanted = f"spec_sha256={spec_sha256}; construction_versions_sha256={digest};"
    matches = [trial for trial in register.trials if wanted in trial.evidence]
    if len(matches) != 1:
        raise FidelityError("register_entry", f"{len(matches)} register entries name this spec and versions digest")
    (trial,) = matches
    if (
        trial.declared_for is not None
        or trial.searches != FIDELITY_SEARCHES
        or trial.exactness is not TrialExactness.EXACT
        or not trial.trial_id.startswith("3609-step1-fidelity-v")
    ):
        raise FidelityError("register_entry", f"{trial.trial_id} is not a non-claiming exact 16-search fidelity entry")
    if trial.evidence.count("construction_versions_sha256=") != 1:
        raise FidelityError("register_entry", f"{trial.trial_id} must name exactly one versions digest")
    return trial


def version_history(history: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    """Per characteristic: the distinct versions with a completed verdict, and whether it is rejected.

    ``history`` is every earlier ``completed`` ledger row. A version is a versions digest; INSUFFICIENT counts.
    """
    seen: dict[str, dict[str, str]] = {name: {} for name in CHARACTERISTICS}
    for row in history:
        for name, verdict in row["verdicts"].items():
            if verdict != Verdict.REJECTED:
                if verdict == Verdict.PASS or row["versions_digest"] not in seen[name]:
                    seen[name][row["versions_digest"]] = verdict
    out: dict[str, dict[str, Any]] = {}
    for name, versions in seen.items():
        passed = Verdict.PASS in versions.values()
        out[name] = {"versions": len(versions), "rejected": not passed and len(versions) >= MAX_VERSIONS}
    return out


# --------------------------------------------------------------------------- ledger


def append_ledger(path: Path, row: Mapping[str, Any]) -> None:
    """Append one JSON row under an exclusive lock, then fsync: the row is durable before this returns."""
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(dict(row), sort_keys=True, separators=(",", ":")) + "\n"
    with path.open("a", encoding="utf-8") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def read_ledger(*paths: Path) -> list[dict[str, Any]]:
    """Every row of each ledger that exists, de-duplicated (a committed copy repeats the local rows)."""
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for path in paths:
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip() and line not in seen:
                seen.add(line)
                rows.append(json.loads(line))
    return rows


# --------------------------------------------------------------------------- alignment diagnostics


def delayed_name_months(periods: Mapping[int, Sequence[tuple[date, date | None]]]) -> int:
    """Name-months delayed by acceptance, inferred from later use (a lower bound).

    ``periods`` maps a series to its (M, period end used at M or None) rows. A name-month counts when a later row
    of the same series uses a period end E' newer than the one used at M, with E' + 4 calendar months <= M: that
    period was lag-eligible at M but not yet usable. Names that leave the panel are not seen.
    """
    count = 0
    for rows in periods.values():
        ordered = sorted(rows)
        for i, (formation, used) in enumerate(ordered):
            for _, later in ordered[i + 1 :]:
                if later is not None and (used is None or later > used) and lag_eligible(later, formation):
                    count += 1
                    break
    return count


def strip_q_run(symbol: str) -> str:
    return symbol.rstrip("Q")


@dataclass(frozen=True)
class Form25Record:
    issuer_cik: str
    symbol: str
    filed_date: date
    event_date: date


def form25_table(
    records: Sequence[Form25Record], last_bars: Mapping[str, Sequence[date]], window_days: int = 30
) -> list[dict[str, Any]]:
    """Premise 1's table: per filing year, records, those with a series, and those ending within the window.

    A record matches a series whose ``vendor_symbol`` equals its symbol or the symbol with its trailing run of
    ``Q`` removed; its distance is the smallest |last_bar - event date| over the matched series.
    """
    by_year: dict[int, list[int]] = {}
    for r in records:
        keys = {r.symbol, strip_q_run(r.symbol)}
        candidates = [bar for key in keys for bar in last_bars.get(key, ())]
        row = by_year.setdefault(r.filed_date.year, [0, 0, 0])
        row[0] += 1
        if candidates:
            row[1] += 1
            if min(abs((bar - r.event_date).days) for bar in candidates) <= window_days:
                row[2] += 1
    return [
        {"year": year, "records": n, "with_series": matched, "within_window": near}
        for year, (n, matched, near) in sorted(by_year.items())
    ]


__all__ = [
    "ARMS",
    "CHARACTERISTICS",
    "PRICE_CHARACTERISTICS",
    "FIDELITY_SEARCHES",
    "ArmResult",
    "Bar",
    "FactorMonth",
    "FidelityError",
    "Form25Record",
    "Holding",
    "SeriesAccumulator",
    "Verdict",
    "append_ledger",
    "characteristic_verdict",
    "compare_arm",
    "delayed_name_months",
    "evidence_sha256",
    "factor_month",
    "fidelity_evidence",
    "form25_table",
    "month_last_day",
    "month_key",
    "read_ledger",
    "register_gate",
    "shift_month",
    "versions_digest",
    "version_history",
]
