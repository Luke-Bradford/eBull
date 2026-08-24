"""Open #2833's prospective core-sleeve cost verdict without tuning.

The declaration starts after the already-seen 2026-08-24 observations.  Before
five complete common UTC dates exist this script reports readiness only: it does
not compute or reveal a candidate spread statistic.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, Literal

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from scripts._dev_guard import assert_dev_environment

DECLARATION_PATH: Final = Path("docs/proposals/ta/2026-08-24-core-selection-declaration.json")
# Filled from the exact declaration bytes before the prospective boundary.
DECLARATION_SHA256: Final = "e935af30754a72b685097b650acb28a21017e1ba321e9a55fa903d771ea20649"

Verdict = Literal["PASS", "FAIL"]


@dataclass(frozen=True)
class Observation:
    instrument_id: int
    symbol: str
    sample_bucket: datetime
    status: str
    spread_bps: Decimal | None
    conversion_rate: Decimal | None


@dataclass(frozen=True)
class Eligibility:
    instrument_id: int
    observed_at: datetime | None
    verdict: str | None
    settlement_type: str | None
    direction: str | None
    leverage_values: tuple[int, ...] | None
    allow_open_position: bool | None
    response_digest: str | None


@dataclass(frozen=True)
class CandidateVerdict:
    instrument_id: int
    symbol: str
    row_count: int
    median_spread_bps: Decimal
    p75_spread_bps: Decimal
    verdict: Verdict
    refusals: tuple[str, ...]
    eligibility_observed_at: datetime | None
    eligibility_response_digest: str | None


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_declaration(path: Path = DECLARATION_PATH) -> Mapping[str, Any]:
    actual = _sha256(path)
    if actual != DECLARATION_SHA256:
        raise RuntimeError(f"declaration digest mismatch: expected {DECLARATION_SHA256}, got {actual}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("declaration must be a JSON object")
    return payload


def percentile_cont(values: Sequence[Decimal], percentile: Decimal) -> Decimal:
    """Continuous interpolation matching PostgreSQL ``percentile_cont``."""
    if not values:
        raise ValueError("percentile_cont requires at least one value")
    if percentile < 0 or percentile > 1:
        raise ValueError("percentile must be in [0, 1]")
    ordered = sorted(values)
    rank = Decimal(len(ordered) - 1) * percentile
    lower = int(rank)
    upper = lower if rank == lower else lower + 1
    fraction = rank - Decimal(lower)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _common_dates(observations: Sequence[Observation], candidate_ids: Sequence[int]) -> tuple[date, ...]:
    dates_by_id = {
        instrument_id: {
            row.sample_bucket.astimezone(UTC).date()
            for row in observations
            if row.instrument_id == instrument_id and row.status == "observed"
        }
        for instrument_id in candidate_ids
    }
    return tuple(sorted(set.intersection(*(dates_by_id[instrument_id] for instrument_id in candidate_ids))))


def _population_for(
    observations: Sequence[Observation],
    *,
    instrument_id: int,
    selected_dates: Sequence[date],
) -> tuple[list[Observation], list[str]]:
    population: list[Observation] = []
    refusals: list[str] = []
    for selected_date in selected_dates:
        day_rows = sorted(
            (
                row
                for row in observations
                if row.instrument_id == instrument_id and row.sample_bucket.astimezone(UTC).date() == selected_date
            ),
            key=lambda row: row.sample_bucket,
        )
        observed = [row for row in day_rows if row.status == "observed"]
        if not observed:
            refusals.append("incomplete_population")
            continue
        first = observed[0].sample_bucket
        last = observed[-1].sample_bucket
        interval = [row for row in day_rows if first <= row.sample_bucket <= last]
        expected_buckets = int((last - first).total_seconds() // 3600) + 1
        if len(interval) != expected_buckets or any(row.status != "observed" for row in interval):
            refusals.append("incomplete_population")
        population.extend(row for row in interval if row.status == "observed")
    return population, refusals


def evaluate(
    observations: Sequence[Observation],
    eligibilities: Mapping[int, Eligibility],
    declaration: Mapping[str, Any],
    *,
    now: datetime,
) -> Mapping[str, Any]:
    candidate_ids = tuple(int(value) for value in declaration["candidate_ids"])
    not_before = datetime.fromisoformat(str(declaration["evidence_not_before"]).replace("Z", "+00:00"))
    eligible_rows = [row for row in observations if row.sample_bucket >= not_before]
    common_dates = _common_dates(eligible_rows, candidate_ids)
    required_dates = int(declaration["required_common_utc_dates"])
    if len(common_dates) < required_dates:
        return {
            "schema_version": declaration["schema_version"],
            "outcome": "evidence_collecting",
            "common_dates_observed": len(common_dates),
            "required_common_dates": required_dates,
            "declaration_sha256": DECLARATION_SHA256,
        }

    selected_dates = common_dates[:required_dates]
    opens_at = datetime.combine(selected_dates[-1] + timedelta(days=1), time.min, tzinfo=UTC)
    if now.astimezone(UTC) < opens_at:
        return {
            "schema_version": declaration["schema_version"],
            "outcome": "evidence_collecting",
            "common_dates_observed": len(common_dates),
            "required_common_dates": required_dates,
            "verdict_opens_at": opens_at,
            "declaration_sha256": DECLARATION_SHA256,
        }

    pass_bar = Decimal(str(declaration["pass_bar_bps"]))
    candidates: list[CandidateVerdict] = []
    for instrument_id in candidate_ids:
        population, refusals = _population_for(
            eligible_rows,
            instrument_id=instrument_id,
            selected_dates=selected_dates,
        )
        spreads = [row.spread_bps for row in population if row.spread_bps is not None]
        if len(spreads) != len(population) or not spreads:
            refusals.append("spread_unmeasured")
        if any(row.conversion_rate != Decimal(1) for row in population):
            refusals.append("fx_unmodelled")
        eligibility = eligibilities.get(instrument_id)
        if eligibility is None or (
            eligibility.verdict != "underlying"
            or eligibility.settlement_type != "real"
            or eligibility.direction != "long"
            or eligibility.leverage_values != (1,)
            or eligibility.allow_open_position is not True
        ):
            refusals.append("not_proved_real_long_x1")
        median = percentile_cont(spreads, Decimal("0.50")) if spreads else Decimal("NaN")
        p75 = percentile_cont(spreads, Decimal("0.75")) if spreads else Decimal("NaN")
        if p75.is_finite() and p75 > pass_bar:
            refusals.append("cost_above_60_bps")
        candidates.append(
            CandidateVerdict(
                instrument_id=instrument_id,
                symbol=next(row.symbol for row in eligible_rows if row.instrument_id == instrument_id),
                row_count=len(population),
                median_spread_bps=median,
                p75_spread_bps=p75,
                verdict="FAIL" if refusals else "PASS",
                refusals=tuple(sorted(set(refusals))),
                eligibility_observed_at=None if eligibility is None else eligibility.observed_at,
                eligibility_response_digest=None if eligibility is None else eligibility.response_digest,
            )
        )
    passing = [candidate for candidate in candidates if candidate.verdict == "PASS"]
    selected = min(passing, key=lambda candidate: (candidate.p75_spread_bps, candidate.instrument_id), default=None)
    return {
        "schema_version": declaration["schema_version"],
        "outcome": "pass" if selected is not None else "cash",
        "selected_instrument_id": None if selected is None else selected.instrument_id,
        "selected_symbol": None if selected is None else selected.symbol,
        "window_dates": [value.isoformat() for value in selected_dates],
        "declaration_sha256": DECLARATION_SHA256,
        "candidates": [asdict(candidate) for candidate in candidates],
    }


_OBSERVATIONS_SQL: Final = """
SELECT o.instrument_id, i.symbol, o.sample_bucket,
       o.observation_status AS status, o.spread_bps, o.conversion_rate
FROM strategy_core_quote_observations o
JOIN instruments i USING (instrument_id)
WHERE o.instrument_id = ANY(%(candidate_ids)s)
  AND o.sample_bucket >= %(not_before)s
ORDER BY o.instrument_id, o.sample_bucket
"""

_ELIGIBILITY_SQL: Final = """
SELECT DISTINCT ON (instrument_id)
       instrument_id, observed_at, verdict, settlement_type, direction,
       leverage_values, allow_open_position, response_digest
FROM strategy_core_eligibility_proofs
WHERE instrument_id = ANY(%(candidate_ids)s)
ORDER BY instrument_id, observed_at DESC, core_eligibility_proof_id DESC
"""


def _git(*args: str) -> str:
    return subprocess.run(("git", *args), check=True, text=True, capture_output=True).stdout.strip()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="write the completed canonical JSON result")
    args = parser.parse_args(argv)
    declaration = load_declaration()
    declaration_commit = _git("log", "-1", "--format=%H", "--", str(DECLARATION_PATH))
    execution_commit = _git("rev-parse", "HEAD")
    subprocess.run(("git", "merge-base", "--is-ancestor", declaration_commit, execution_commit), check=True)
    not_before = datetime.fromisoformat(str(declaration["evidence_not_before"]).replace("Z", "+00:00"))
    candidate_ids = [int(value) for value in declaration["candidate_ids"]]
    assert_dev_environment()
    with psycopg.connect(settings.database_url) as conn, conn.cursor(row_factory=dict_row) as cursor:
        observation_rows = cursor.execute(
            _OBSERVATIONS_SQL,
            {"candidate_ids": candidate_ids, "not_before": not_before},
        ).fetchall()
        eligibility_rows = cursor.execute(_ELIGIBILITY_SQL, {"candidate_ids": candidate_ids}).fetchall()
        now_row = cursor.execute("SELECT now() AS now").fetchone()
        if now_row is None:
            raise RuntimeError("database clock query returned no row")
        now = now_row["now"]
    observations = [Observation(**row) for row in observation_rows]
    eligibilities = {
        int(row["instrument_id"]): Eligibility(
            instrument_id=int(row["instrument_id"]),
            observed_at=row["observed_at"],
            verdict=row["verdict"],
            settlement_type=row["settlement_type"],
            direction=row["direction"],
            leverage_values=None if row["leverage_values"] is None else tuple(row["leverage_values"]),
            allow_open_position=row["allow_open_position"],
            response_digest=row["response_digest"],
        )
        for row in eligibility_rows
    }
    result = dict(evaluate(observations, eligibilities, declaration, now=now))
    result.update(
        declaration_commit=declaration_commit,
        execution_commit=execution_commit,
        measured_at=now,
    )
    encoded = json.dumps(result, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False) + "\n"
    if args.output is not None:
        if result["outcome"] == "evidence_collecting":
            raise RuntimeError("refusing to write a result before the declared population is complete")
        args.output.write_text(encoded, encoding="utf-8")
    sys.stdout.write(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "DECLARATION_PATH",
    "DECLARATION_SHA256",
    "CandidateVerdict",
    "Eligibility",
    "Observation",
    "evaluate",
    "load_declaration",
    "main",
    "percentile_cont",
]
