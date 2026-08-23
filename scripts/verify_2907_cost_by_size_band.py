"""Measure the frozen #2907 spread diagnostic by company-size band.

Run only from a clean implementation commit. One invocation reads one database
snapshot and emits both canonical artefacts from the same typed evidence object.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, Literal

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.services.cost_model import (
    BANDS,
    CARRY_CLOSURE,
    COST_MODEL_ID,
    FX_CLOSURE,
    STRUCTURAL_ZERO_LANE,
    cost_band_for,
)
from app.services.strategies.validated_universe import (
    VALIDATED_UNIVERSE_RULE_VERSION,
    load_validated_universe,
)
from scripts._dev_guard import assert_dev_environment

DECLARATION_PATH: Final = Path("docs/proposals/ta/2026-08-23-r6-cost-by-size-band-declaration.md")
DECLARATION_SHA256: Final = "1dfd13d6835dc3370dd4cabe9828ff8bfaa8a00de2cb68a7972f7cb00e44d559"
DECLARATION_COMMIT: Final = "538d5b3db7fe6088110c91fd96b6e9a0b62ef460"
SCHEMA_VERSION: Final = "r6-2907-cost-by-size-band-v1"
EXPECTED_COST_MODEL_ID: Final = "static-p75-insession-v3+split-adjusted-max+carry-fx-structural-zero-long-x1-real-usd"
EXPECTED_UNIVERSE_VERSION: Final = "validated-universe-us-stocks-v1"
VENDOR_RETURN: Final = Decimal("0.203")
HAIRCUTS: Final[Mapping[str, Decimal]] = {
    "15pct": Decimal("0.15"),
    "58pct": Decimal("0.58"),
}
ROUND_TRIPS_PER_YEAR: Final = 3

CapBand = Literal["micro", "small", "mid", "large", "unknown_market_cap"]
KnownCapBand = Literal["micro", "small", "mid", "large"]
PriceStatus = Literal["priced", "unpriced"]
CostVerdict = Literal["COST-KILLED", "CONTINGENT", "COST-SURVIVES-ROBUST"]

KNOWN_CAP_BANDS: Final[tuple[KnownCapBand, ...]] = ("micro", "small", "mid", "large")
ALL_CAP_BANDS: Final[tuple[CapBand, ...]] = (*KNOWN_CAP_BANDS, "unknown_market_cap")
PRICE_STATUSES: Final[tuple[PriceStatus, ...]] = ("priced", "unpriced")
_SEVERITY: Final[Mapping[CostVerdict, int]] = {
    "COST-SURVIVES-ROBUST": 0,
    "CONTINGENT": 1,
    "COST-KILLED": 2,
}

_CENSUS_SQL: Final = """
    SELECT universe.instrument_id,
           valuation.market_cap_live,
           quote.last,
           quote.quoted_at
    FROM unnest(%(instrument_ids)s::bigint[]) AS universe(instrument_id)
    LEFT JOIN instrument_valuation valuation
      ON valuation.instrument_id = universe.instrument_id
    LEFT JOIN quotes quote
      ON quote.instrument_id = universe.instrument_id
    ORDER BY universe.instrument_id
"""


@dataclass(frozen=True)
class PopulationRow:
    instrument_id: int
    market_cap_live: Decimal | None
    last: Decimal | None
    quoted_at: datetime | None


@dataclass(frozen=True)
class MetricSummary:
    p50: Decimal | None
    p75: Decimal | None
    p95: Decimal | None
    maximum: Decimal | None


@dataclass(frozen=True)
class BandEvidence:
    total: int
    priced: int
    unpriced: int
    cost_band_counts: Mapping[str, int]
    spread_pct: MetricSummary
    one_round_trip_loss_pct: MetricSummary
    loss_gbp_per_1000: MetricSummary
    loss_gbp_per_10000: MetricSummary
    three_round_trip_loss_pct: MetricSummary


@dataclass(frozen=True)
class HaircutEvidence:
    haircut_pct: Decimal
    return_after_haircut_pct: Decimal
    p75_net_return_pct: Decimal | None
    p95_net_return_pct: Decimal | None


@dataclass(frozen=True)
class ComparisonEvidence:
    micro_minus_large_p75_three_round_trip_loss_pp: Decimal | None
    micro_div_large_p75_three_round_trip_loss: Decimal | None


@dataclass(frozen=True)
class QuoteEvidence:
    earliest_quoted_at: datetime | None
    latest_quoted_at: datetime | None
    oldest_age_seconds: Decimal | None
    newest_age_seconds: Decimal | None


@dataclass(frozen=True)
class Evidence:
    schema_version: str
    measured_at: datetime
    execution_commit: str
    declaration_commit: str
    declaration_sha256: str
    query_sha256: str
    source_sha256: Mapping[str, str]
    universe_version: str
    cost_model_id: str
    cost_lane: Mapping[str, object]
    carry_closure: str
    fx_closure: str
    universe_size: int
    distinct_ids: int
    census_cells: Mapping[str, int]
    market_cap_unavailable_reasons: Mapping[str, int]
    price_unavailable_reasons: Mapping[str, int]
    market_cap_coverage_pct: Decimal
    nominal_price_coverage_pct: Decimal
    quote_evidence: QuoteEvidence
    bands: Mapping[str, BandEvidence]
    comparison: ComparisonEvidence
    haircuts: Mapping[str, HaircutEvidence]
    p75_classification: CostVerdict | None
    p95_classification: CostVerdict | None
    verdict: str


def _git(*args: str) -> str:
    return subprocess.run(("git", *args), check=True, text=True, capture_output=True).stdout.strip()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def classify_market_cap(value: Decimal | None) -> CapBand:
    if value is None or not value.is_finite() or value <= 0:
        return "unknown_market_cap"
    if value < Decimal("300000000"):
        return "micro"
    if value < Decimal("2000000000"):
        return "small"
    if value < Decimal("10000000000"):
        return "mid"
    return "large"


def classify_price(value: Decimal | None) -> PriceStatus:
    if value is None or not value.is_finite() or value <= 0:
        return "unpriced"
    return "priced"


def _unavailable_reason(value: Decimal | None) -> str | None:
    if value is None:
        return "null"
    if not value.is_finite():
        return "non_finite"
    if value == 0:
        return "zero"
    if value < 0:
        return "negative"
    return None


def nearest_rank(values: Sequence[Decimal], percentile: Decimal) -> Decimal:
    if not values:
        raise ValueError("nearest-rank percentile requires at least one value")
    if percentile <= 0 or percentile > 1:
        raise ValueError(f"percentile must be in (0, 1], got {percentile}")
    ordered = sorted(values)
    index = math.ceil(len(ordered) * percentile) - 1
    return ordered[index]


def summarise(values: Sequence[Decimal]) -> MetricSummary:
    if not values:
        return MetricSummary(None, None, None, None)
    return MetricSummary(
        p50=nearest_rank(values, Decimal("0.50")),
        p75=nearest_rank(values, Decimal("0.75")),
        p95=nearest_rank(values, Decimal("0.95")),
        maximum=max(values),
    )


def spread_losses(spread_pct: Decimal) -> tuple[Decimal, Decimal]:
    half_spread = spread_pct / Decimal(200)
    retention = (Decimal(1) - half_spread) / (Decimal(1) + half_spread)
    one_loss = Decimal(1) - retention
    compounded_loss = Decimal(1) - retention**ROUND_TRIPS_PER_YEAR
    return one_loss, compounded_loss


def classify_cost(loss_fraction: Decimal) -> CostVerdict:
    returns = {name: VENDOR_RETURN * (Decimal(1) - haircut) for name, haircut in HAIRCUTS.items()}
    net_15 = (Decimal(1) + returns["15pct"]) * (Decimal(1) - loss_fraction) - Decimal(1)
    net_58 = (Decimal(1) + returns["58pct"]) * (Decimal(1) - loss_fraction) - Decimal(1)
    if net_15 <= 0:
        return "COST-KILLED"
    if net_58 <= 0:
        return "CONTINGENT"
    return "COST-SURVIVES-ROBUST"


def derive_verdict(
    p75_loss_pct: Decimal | None,
    p95_loss_pct: Decimal | None,
) -> tuple[CostVerdict | None, CostVerdict | None, str]:
    if p75_loss_pct is None or p95_loss_pct is None:
        return None, None, "DATA-FAIL"
    p75 = classify_cost(p75_loss_pct / Decimal(100))
    p95 = classify_cost(p95_loss_pct / Decimal(100))
    suffix = " — TAIL-WARNING" if _SEVERITY[p95] > _SEVERITY[p75] else ""
    return p75, p95, f"{p75}{suffix}"


def _band_evidence(rows: Sequence[PopulationRow], band: KnownCapBand) -> BandEvidence:
    members = [row for row in rows if classify_market_cap(row.market_cap_live) == band]
    priced = [row for row in members if classify_price(row.last) == "priced"]
    spreads: list[Decimal] = []
    one_losses: list[Decimal] = []
    three_losses: list[Decimal] = []
    cost_band_counts: Counter[str] = Counter({cost_band.label: 0 for cost_band in BANDS})
    for row in priced:
        if row.last is None:
            raise RuntimeError("priced row has no price")
        cost_band = cost_band_for(row.last, price_basis="as_traded")
        cost_band_counts[cost_band.label] += 1
        spreads.append(cost_band.p75_spread_pct)
        one_loss, three_loss = spread_losses(cost_band.p75_spread_pct)
        one_losses.append(one_loss * Decimal(100))
        three_losses.append(three_loss * Decimal(100))
    return BandEvidence(
        total=len(members),
        priced=len(priced),
        unpriced=len(members) - len(priced),
        cost_band_counts=dict(sorted(cost_band_counts.items())),
        spread_pct=summarise(spreads),
        one_round_trip_loss_pct=summarise(one_losses),
        loss_gbp_per_1000=summarise([loss * Decimal(10) for loss in one_losses]),
        loss_gbp_per_10000=summarise([loss * Decimal(100) for loss in one_losses]),
        three_round_trip_loss_pct=summarise(three_losses),
    )


def _load_rows(conn: psycopg.Connection[Any], instrument_ids: Sequence[int]) -> tuple[PopulationRow, ...]:
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(_CENSUS_SQL, {"instrument_ids": list(instrument_ids)})
        raw_rows = cursor.fetchall()
    rows = tuple(
        PopulationRow(
            instrument_id=int(row["instrument_id"]),
            market_cap_live=row["market_cap_live"],
            last=row["last"],
            quoted_at=row["quoted_at"],
        )
        for row in raw_rows
    )
    if len(rows) != len(instrument_ids) or len({row.instrument_id for row in rows}) != len(instrument_ids):
        raise RuntimeError(
            "census query did not conserve the validated universe: "
            f"universe={len(instrument_ids)}, rows={len(rows)}, distinct={len({row.instrument_id for row in rows})}"
        )
    return rows


def _validate_frozen_inputs() -> None:
    if COST_MODEL_ID != EXPECTED_COST_MODEL_ID:
        raise RuntimeError(f"cost model moved: expected {EXPECTED_COST_MODEL_ID}, got {COST_MODEL_ID}")
    if VALIDATED_UNIVERSE_RULE_VERSION != EXPECTED_UNIVERSE_VERSION:
        raise RuntimeError(
            f"validated universe moved: expected {EXPECTED_UNIVERSE_VERSION}, got {VALIDATED_UNIVERSE_RULE_VERSION}"
        )
    expected_lane = {
        "direction": "long",
        "leverage": 1,
        "settlement": "real",
        "order_currency": "USD",
        "account_currency": "USD",
        "instrument_denomination": "USD",
    }
    if asdict(STRUCTURAL_ZERO_LANE) != expected_lane:
        raise RuntimeError(f"cost lane moved: expected {expected_lane}, got {asdict(STRUCTURAL_ZERO_LANE)}")
    if CARRY_CLOSURE != "structural_zero" or FX_CLOSURE != "structural_zero":
        raise RuntimeError(f"cost closures moved: carry={CARRY_CLOSURE}, fx={FX_CLOSURE}")


def _coverage(numerator: int, denominator: int) -> Decimal:
    return Decimal(numerator) / Decimal(denominator) * Decimal(100)


def _reason_counts(values: Sequence[Decimal | None]) -> Counter[str]:
    reasons: Counter[str] = Counter()
    for value in values:
        reason = _unavailable_reason(value)
        if reason is not None:
            reasons[reason] += 1
    return reasons


def build_evidence(
    rows: Sequence[PopulationRow],
    *,
    measured_at: datetime,
    execution_commit: str,
    source_sha256: Mapping[str, str],
) -> Evidence:
    if not rows:
        raise RuntimeError("validated universe is empty")
    distinct_ids = len({row.instrument_id for row in rows})
    if distinct_ids != len(rows):
        raise RuntimeError(f"population contains duplicate IDs: rows={len(rows)}, distinct={distinct_ids}")

    cells = Counter(f"{classify_market_cap(row.market_cap_live)}|{classify_price(row.last)}" for row in rows)
    complete_cells = {
        f"{cap_band}|{price_status}": cells[f"{cap_band}|{price_status}"]
        for cap_band in ALL_CAP_BANDS
        for price_status in PRICE_STATUSES
    }
    if sum(complete_cells.values()) != len(rows):
        raise RuntimeError("Cartesian census does not conserve the population")

    cap_reasons = _reason_counts([row.market_cap_live for row in rows])
    price_reasons = _reason_counts([row.last for row in rows])
    cap_known = sum(1 for row in rows if classify_market_cap(row.market_cap_live) != "unknown_market_cap")
    price_known = sum(1 for row in rows if classify_price(row.last) == "priced")
    bands = {band: _band_evidence(rows, band) for band in KNOWN_CAP_BANDS}

    micro_p75 = bands["micro"].three_round_trip_loss_pct.p75
    micro_p95 = bands["micro"].three_round_trip_loss_pct.p95
    large_p75 = bands["large"].three_round_trip_loss_pct.p75
    comparison = ComparisonEvidence(
        micro_minus_large_p75_three_round_trip_loss_pp=(
            micro_p75 - large_p75 if micro_p75 is not None and large_p75 is not None else None
        ),
        micro_div_large_p75_three_round_trip_loss=(
            micro_p75 / large_p75 if micro_p75 is not None and large_p75 is not None and large_p75 != 0 else None
        ),
    )
    p75_classification, p95_classification, verdict = derive_verdict(micro_p75, micro_p95)

    priced_quote_times = sorted(
        row.quoted_at.astimezone(UTC)
        for row in rows
        if classify_price(row.last) == "priced" and row.quoted_at is not None
    )
    measured_utc = measured_at.astimezone(UTC)
    quote_evidence = QuoteEvidence(
        earliest_quoted_at=priced_quote_times[0] if priced_quote_times else None,
        latest_quoted_at=priced_quote_times[-1] if priced_quote_times else None,
        oldest_age_seconds=(
            Decimal(str((measured_utc - priced_quote_times[0]).total_seconds())) if priced_quote_times else None
        ),
        newest_age_seconds=(
            Decimal(str((measured_utc - priced_quote_times[-1]).total_seconds())) if priced_quote_times else None
        ),
    )
    haircuts: dict[str, HaircutEvidence] = {}
    for name, haircut in HAIRCUTS.items():
        return_after_haircut = VENDOR_RETURN * (Decimal(1) - haircut)
        haircuts[name] = HaircutEvidence(
            haircut_pct=haircut * Decimal(100),
            return_after_haircut_pct=return_after_haircut * Decimal(100),
            p75_net_return_pct=(
                ((Decimal(1) + return_after_haircut) * (Decimal(1) - micro_p75 / Decimal(100)) - Decimal(1))
                * Decimal(100)
                if micro_p75 is not None
                else None
            ),
            p95_net_return_pct=(
                ((Decimal(1) + return_after_haircut) * (Decimal(1) - micro_p95 / Decimal(100)) - Decimal(1))
                * Decimal(100)
                if micro_p95 is not None
                else None
            ),
        )

    return Evidence(
        schema_version=SCHEMA_VERSION,
        measured_at=measured_at,
        execution_commit=execution_commit,
        declaration_commit=DECLARATION_COMMIT,
        declaration_sha256=DECLARATION_SHA256,
        query_sha256=hashlib.sha256(_CENSUS_SQL.encode()).hexdigest(),
        source_sha256=source_sha256,
        universe_version=VALIDATED_UNIVERSE_RULE_VERSION,
        cost_model_id=COST_MODEL_ID,
        cost_lane=asdict(STRUCTURAL_ZERO_LANE),
        carry_closure=CARRY_CLOSURE,
        fx_closure=FX_CLOSURE,
        universe_size=len(rows),
        distinct_ids=distinct_ids,
        census_cells=complete_cells,
        market_cap_unavailable_reasons=dict(sorted(cap_reasons.items())),
        price_unavailable_reasons=dict(sorted(price_reasons.items())),
        market_cap_coverage_pct=_coverage(cap_known, len(rows)),
        nominal_price_coverage_pct=_coverage(price_known, len(rows)),
        quote_evidence=quote_evidence,
        bands=bands,
        comparison=comparison,
        haircuts=haircuts,
        p75_classification=p75_classification,
        p95_classification=p95_classification,
        verdict=verdict,
    )


def collect_evidence() -> Evidence:
    assert_dev_environment()
    if _git("status", "--porcelain"):
        raise RuntimeError("verifier requires a clean worktree")
    execution_commit = _git("rev-parse", "HEAD")
    if _git("merge-base", "--is-ancestor", DECLARATION_COMMIT, execution_commit):
        raise RuntimeError("declaration commit is not an ancestor of the execution commit")
    measured_declaration = _sha256(DECLARATION_PATH)
    if measured_declaration != DECLARATION_SHA256:
        raise RuntimeError(f"declaration hash moved: expected {DECLARATION_SHA256}, measured {measured_declaration}")
    _validate_frozen_inputs()
    source_paths = (
        Path("app/services/cost_model.py"),
        Path("app/services/strategies/validated_universe.py"),
        Path("scripts/verify_2907_cost_by_size_band.py"),
    )
    source_sha256 = {str(path): _sha256(path) for path in source_paths}

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        with conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute("SELECT transaction_timestamp() AS measured_at")
            measured_row = cursor.fetchone()
        if measured_row is None or measured_row["measured_at"] is None:
            raise RuntimeError("measurement timestamp query returned no value")
        measured_at = measured_row["measured_at"]
        instrument_ids = load_validated_universe(conn)
        if not instrument_ids:
            raise RuntimeError("validated universe is empty")
        rows = _load_rows(conn, instrument_ids)
        conn.rollback()
    return build_evidence(
        rows,
        measured_at=measured_at,
        execution_commit=execution_commit,
        source_sha256=source_sha256,
    )


def _canonical(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    raise TypeError(f"cannot serialise {type(value).__name__}")


def render_json(evidence: Evidence) -> str:
    return json.dumps(asdict(evidence), sort_keys=True, separators=(",", ":"), default=_canonical) + "\n"


def _metric(value: Decimal | None) -> str:
    return "—" if value is None else f"{value:.6f}"


def render_markdown(evidence: Evidence) -> str:
    lines = [
        "# R6 cost by size band result (#2907)",
        "",
        f"Verdict: **{evidence.verdict}**",
        "",
        f"Declaration SHA-256: `{evidence.declaration_sha256}` at commit `{evidence.declaration_commit}`.",
        f"Execution commit: `{evidence.execution_commit}`. "
        f"Measured: `{evidence.measured_at.astimezone(UTC).isoformat()}`.",
        f"Universe: `{evidence.universe_version}`; cost model: `{evidence.cost_model_id}`.",
        "This is a live-snapshot long/x1/real/USD spread diagnostic, not a backtest or return claim.",
        "",
        "## Population and coverage",
        "",
        f"- Full population: {evidence.universe_size}; distinct IDs: {evidence.distinct_ids}.",
        f"- Market-cap coverage: {_metric(evidence.market_cap_coverage_pct)}%.",
        f"- Latest-stored nominal-price coverage: {_metric(evidence.nominal_price_coverage_pct)}%.",
        f"- Unavailable cap reasons: `{dict(evidence.market_cap_unavailable_reasons)}`.",
        f"- Unavailable price reasons: `{dict(evidence.price_unavailable_reasons)}`.",
        f"- Cartesian cells: `{dict(evidence.census_cells)}`.",
        f"- Quote range: `{evidence.quote_evidence.earliest_quoted_at}` to "
        f"`{evidence.quote_evidence.latest_quoted_at}`; "
        f"oldest/newest age seconds: `{evidence.quote_evidence.oldest_age_seconds}` / "
        f"`{evidence.quote_evidence.newest_age_seconds}`.",
        "",
        "## Size-band distribution",
        "",
        "| Size | N total | N priced | Statistic | Spread % | 1 RT loss % | £/£1k | £/£10k | 3 RT loss % |",
        "|---|---:|---:|---|---:|---:|---:|---:|---:|",
    ]
    for band in KNOWN_CAP_BANDS:
        result = evidence.bands[band]
        for statistic in ("p50", "p75", "p95", "maximum"):
            lines.append(
                f"| {band} | {result.total} | {result.priced} | {statistic} | "
                f"{_metric(getattr(result.spread_pct, statistic))} | "
                f"{_metric(getattr(result.one_round_trip_loss_pct, statistic))} | "
                f"{_metric(getattr(result.loss_gbp_per_1000, statistic))} | "
                f"{_metric(getattr(result.loss_gbp_per_10000, statistic))} | "
                f"{_metric(getattr(result.three_round_trip_loss_pct, statistic))} |"
            )
        lines.append(f"| {band} cost bands |  |  | counts | `{dict(result.cost_band_counts)}` |  |  |  |  |")
    lines.extend(
        [
            "",
            "## Haircut test",
            "",
            "| Haircut | Return ceiling | Micro p75 net | Micro p95 net |",
            "|---:|---:|---:|---:|",
        ]
    )
    for result in evidence.haircuts.values():
        lines.append(
            f"| {_metric(result.haircut_pct)}% | {_metric(result.return_after_haircut_pct)}% | "
            f"{_metric(result.p75_net_return_pct)}% | {_metric(result.p95_net_return_pct)}% |"
        )
    lines.extend(
        [
            "",
            f"- p75 classification: `{evidence.p75_classification}`; p95: `{evidence.p95_classification}`.",
            "- Micro-minus-large p75 three-round-trip loss: "
            f"`{evidence.comparison.micro_minus_large_p75_three_round_trip_loss_pp}` percentage points; ratio "
            f"`{evidence.comparison.micro_div_large_p75_three_round_trip_loss}`.",
            "",
            "## Consequence",
            "",
            "A surviving verdict means only that the frozen spread table does not itself falsify the avenue under the "
            "vendor total-return ceiling. It establishes no absolute return and no edge versus buy-and-hold. #2900 "
            "still blocks every Tier 2 arm; any future size hypothesis belongs inside #2901 rather than a standalone "
            "microcap sleeve.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json-output", type=Path, required=True)
    parser.add_argument("--markdown-output", type=Path, required=True)
    args = parser.parse_args()
    if args.json_output.resolve() == args.markdown_output.resolve():
        raise ValueError("JSON and Markdown outputs must be different paths")
    evidence = collect_evidence()
    args.json_output.write_text(render_json(evidence))
    args.markdown_output.write_text(render_markdown(evidence))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
