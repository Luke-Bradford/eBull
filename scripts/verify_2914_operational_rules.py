"""Verify the frozen #2914 zero-turnover operational-rule contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Final

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.services.r6_operational_rules import (
    FACTOR_VALUATION_RULE_VERSION,
    REFERENCE_RETURN_UNITS,
    TURN_OF_MONTH_OFFSETS,
    TURN_OF_MONTH_RULE_VERSION,
    FactorValuationRecord,
)
from scripts._dev_guard import assert_dev_environment

DECLARATION_PATH: Final = Path("docs/proposals/ta/2026-08-23-r6-operational-rules-declaration.md")
DECLARATION_SHA256: Final = "1f81ceb1675e6636d52ec0de6d685643a810d56688b9d9c8b4c4786987338c50"
DECLARATION_COMMIT: Final = "5fbde41f924c29daafd56b56b2678e9a0d557bfb"
SCHEMA_VERSION: Final = "r6-2914-operational-rules-result-v1"

# Frozen empty by construction: #2912 ingests return and macro-context series,
# not factor valuation spreads. A future genuine spread dataset needs a new
# declaration/version rather than admission by a suggestive series name.
VALUATION_SPREAD_SERIES_ALLOWLIST: Final[frozenset[tuple[str, str, str]]] = frozenset()

_CENSUS_SQL: Final = """
    SELECT snapshot.snapshot_id,
           snapshot.source,
           snapshot.dataset_key,
           snapshot.response_sha256,
           snapshot.parser_version,
           snapshot.row_count AS declared_row_count,
           observation.series_key,
           observation.unit,
           count(observation.observation_date) AS observation_count,
           min(observation.observation_date) AS first_observation,
           max(observation.observation_date) AS last_observation
    FROM reference_data_snapshots snapshot
    LEFT JOIN reference_data_observations observation
      ON observation.snapshot_id = snapshot.snapshot_id
    WHERE snapshot.parse_status = 'accepted'
    GROUP BY snapshot.snapshot_id,
             snapshot.source,
             snapshot.dataset_key,
             snapshot.response_sha256,
             snapshot.parser_version,
             snapshot.row_count,
             observation.series_key,
             observation.unit
    ORDER BY snapshot.snapshot_id, observation.series_key
"""


@dataclass(frozen=True)
class SeriesCensus:
    snapshot_id: int
    source: str
    dataset_key: str
    response_sha256: str
    parser_version: str
    series_key: str
    unit: str
    observation_count: int
    first_observation: date
    last_observation: date


@dataclass(frozen=True)
class Evidence:
    schema_version: str
    measured_at: datetime
    execution_commit: str
    declaration_commit: str
    declaration_sha256: str
    query_sha256: str
    source_sha256: dict[str, str]
    turn_of_month_rule_version: str
    turn_of_month_offsets: tuple[int, ...]
    factor_valuation_rule_version: str
    accepted_snapshot_count: int
    snapshot_series_count: int
    observation_count: int
    observation_unit_counts: dict[str, int]
    eligible_valuation_spread_series: int
    factor_valuation_record: dict[str, object]
    series: tuple[SeriesCensus, ...]
    haircuts: dict[str, str]
    return_vs_buy_and_hold: str
    verdict: str


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git(*args: str) -> str:
    return subprocess.run(("git", *args), check=True, text=True, capture_output=True).stdout.strip()


def _assert_declaration_ancestor(execution_commit: str) -> None:
    result = subprocess.run(
        ("git", "merge-base", "--is-ancestor", DECLARATION_COMMIT, execution_commit),
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode == 0:
        return
    detail = result.stderr.strip() or "declaration commit is not an ancestor"
    raise RuntimeError(f"declaration ancestry check failed with exit {result.returncode}: {detail}")


def _build_evidence(rows: list[dict[str, Any]], *, measured_at: datetime, execution_commit: str) -> Evidence:
    if not rows:
        raise RuntimeError("accepted #2912 reference snapshot census is empty")

    declared_by_snapshot: dict[int, int] = {}
    observed_by_snapshot: defaultdict[int, int] = defaultdict(int)
    series: list[SeriesCensus] = []
    unit_counts: Counter[str] = Counter()
    eligible = 0
    for row in rows:
        snapshot_id = int(row["snapshot_id"])
        declared = int(row["declared_row_count"])
        previous = declared_by_snapshot.setdefault(snapshot_id, declared)
        if previous != declared:
            raise RuntimeError(f"snapshot {snapshot_id} has inconsistent declared row counts")
        if row["series_key"] is None or row["unit"] is None:
            raise RuntimeError(f"accepted snapshot {snapshot_id} has no typed observations")
        item = SeriesCensus(
            snapshot_id=snapshot_id,
            source=str(row["source"]),
            dataset_key=str(row["dataset_key"]),
            response_sha256=str(row["response_sha256"]),
            parser_version=str(row["parser_version"]),
            series_key=str(row["series_key"]),
            unit=str(row["unit"]),
            observation_count=int(row["observation_count"]),
            first_observation=row["first_observation"],
            last_observation=row["last_observation"],
        )
        if item.unit not in REFERENCE_RETURN_UNITS:
            raise RuntimeError(f"snapshot {snapshot_id}/{item.series_key} has an unknown unit {item.unit!r}")
        observed_by_snapshot[snapshot_id] += item.observation_count
        unit_counts[item.unit] += item.observation_count
        if (item.source, item.dataset_key, item.series_key) in VALUATION_SPREAD_SERIES_ALLOWLIST:
            eligible += 1
        series.append(item)

    for snapshot_id, declared in declared_by_snapshot.items():
        observed = observed_by_snapshot[snapshot_id]
        if observed != declared:
            raise RuntimeError(
                f"accepted snapshot {snapshot_id} row conservation failed: declared {declared}, observed {observed}"
            )
    if eligible:
        raise RuntimeError("frozen #2914 declaration admits no valuation-spread reference series")

    unavailable = FactorValuationRecord(
        factor_id="all-r6-arms",
        status="unavailable",
        reason="#2912 accepted corpus contains factor returns and macro context, not valuation-spread levels",
    )
    source_paths = (
        Path("app/services/r6_operational_rules.py"),
        Path("scripts/verify_2914_operational_rules.py"),
    )
    return Evidence(
        schema_version=SCHEMA_VERSION,
        measured_at=measured_at,
        execution_commit=execution_commit,
        declaration_commit=DECLARATION_COMMIT,
        declaration_sha256=DECLARATION_SHA256,
        query_sha256=hashlib.sha256(_CENSUS_SQL.encode()).hexdigest(),
        source_sha256={str(path): _sha256(path) for path in source_paths},
        turn_of_month_rule_version=TURN_OF_MONTH_RULE_VERSION,
        turn_of_month_offsets=TURN_OF_MONTH_OFFSETS,
        factor_valuation_rule_version=FACTOR_VALUATION_RULE_VERSION,
        accepted_snapshot_count=len(declared_by_snapshot),
        snapshot_series_count=len(series),
        observation_count=sum(observed_by_snapshot.values()),
        observation_unit_counts=dict(sorted(unit_counts.items())),
        eligible_valuation_spread_series=eligible,
        factor_valuation_record=asdict(unavailable),
        series=tuple(series),
        haircuts={
            "15pct": "N/A — operational rule, no return outcome",
            "58pct": "N/A — operational rule, no return outcome",
        },
        return_vs_buy_and_hold="N/A — no strategy arm or return window",
        verdict="PASS — OPERATIONAL RULES INSTALLED; FACTOR VALUATION UNAVAILABLE",
    )


def collect_evidence() -> Evidence:
    assert_dev_environment()
    if _git("status", "--porcelain"):
        raise RuntimeError("verifier requires a clean worktree")
    execution_commit = _git("rev-parse", "HEAD")
    _assert_declaration_ancestor(execution_commit)
    measured_declaration = _sha256(DECLARATION_PATH)
    if measured_declaration != DECLARATION_SHA256:
        raise RuntimeError(f"declaration hash moved: expected {DECLARATION_SHA256}, measured {measured_declaration}")

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        with conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute("SELECT transaction_timestamp() AS measured_at")
            measured_row = cursor.fetchone()
            cursor.execute(_CENSUS_SQL)
            rows = list(cursor.fetchall())
        conn.rollback()
    if measured_row is None or measured_row["measured_at"] is None:
        raise RuntimeError("measurement timestamp query returned no value")
    return _build_evidence(rows, measured_at=measured_row["measured_at"], execution_commit=execution_commit)


def _json_default(value: object) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"cannot encode {type(value).__name__}")


def render_markdown(evidence: Evidence) -> str:
    lines = [
        "# R6 #2914 operational-rules result",
        "",
        f"Verdict: **{evidence.verdict}**",
        "",
        f"Measured at `{evidence.measured_at.isoformat()}` from execution commit `{evidence.execution_commit}`.",
        f"Declaration SHA-256: `{evidence.declaration_sha256}` at `{evidence.declaration_commit}`.",
        "",
        "## Rules",
        "",
        f"- Turn of month: `{evidence.turn_of_month_rule_version}`, offsets `{list(evidence.turn_of_month_offsets)}`.",
        f"- Factor valuation: `{evidence.factor_valuation_rule_version}`; status `unavailable`.",
        "- The preference creates no order, holding, amount, turnover or execution authority.",
        "- Recent factor returns are explicitly ineligible as a valuation-spread proxy.",
        "",
        "## Full accepted #2912 corpus",
        "",
        f"- Accepted snapshots: {evidence.accepted_snapshot_count}",
        f"- Snapshot-series cells: {evidence.snapshot_series_count}",
        f"- Typed observations: {evidence.observation_count}",
        f"- Observation units: `{evidence.observation_unit_counts}`",
        f"- Genuine valuation-spread series: {evidence.eligible_valuation_spread_series}",
        "",
        "## Return boundary",
        "",
        f"- 15% haircut: {evidence.haircuts['15pct']}",
        f"- 58% haircut: {evidence.haircuts['58pct']}",
        f"- Buy-and-hold: {evidence.return_vs_buy_and_hold}",
        "",
        "| source | dataset | parser | series | unit | observations | window |",
        "| --- | --- | --- | --- | --- | ---: | --- |",
    ]
    for item in evidence.series:
        lines.append(
            f"| {item.source} | {item.dataset_key} | {item.parser_version} | {item.series_key} | {item.unit} | "
            f"{item.observation_count} | {item.first_observation.isoformat()}..{item.last_observation.isoformat()} |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    args = parser.parse_args()
    evidence = collect_evidence()
    payload = json.dumps(asdict(evidence), default=_json_default, indent=2, sort_keys=True) + "\n"
    markdown = render_markdown(evidence)
    if args.json_output:
        args.json_output.write_text(payload)
    else:
        print(payload, end="")
    if args.markdown_output:
        args.markdown_output.write_text(markdown)


if __name__ == "__main__":
    main()
