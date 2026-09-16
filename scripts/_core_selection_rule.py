"""The shared core/tilt selection rule behind #2833's and #2834's sealed verdicts.

⚠⚠ EXTRACTED, NOT REWRITTEN (#2834 ARM A, 2026-09-16). Every rule in here was
implemented by ``scripts/verify_2833_core_selection.py`` first and is moved
verbatim. #2833's verdict opens 2026-09-18, so the extraction had to be
provably non-behaviour-changing for ``pass_bar_bps = "60"``,
``binding_percentile = "0.75"`` and ``descriptive_percentile = "0.50"`` -- see
``tests/test_2833_core_selection_verdict.py`` (untouched by the extraction, so
its continuing to pass is part of the evidence) and
``tests/test_2834_arm_a_selection_verdict.py::test_both_declarations_keep_their_own_identity``.

Why a shared module rather than a second copy: ``docs/review-prevention-log.md``
-- *a hand-copied predicate has no compiler*. ARM A needs the same percentile,
population, missingness, FX and eligibility rules against a different candidate
set and a different bar, which is a declaration difference, not a code one.

Three things used to be baked in that the declaration already described:

* the declaration path and sha256 -- now ARGUMENTS, because they are identity
  rather than rule, and nothing here carries one as a default;
* the refusal label ``cost_above_60_bps``, which NAMED 60 while READING
  ``pass_bar_bps`` from the declaration -- two sources for one number;
* the percentiles, passed as ``Decimal("0.75")`` / ``Decimal("0.50")`` literals
  while ``binding_percentile`` / ``descriptive_percentile`` sat in the
  declaration describing exactly those values.

⚠ Fields the code cannot read are PINNED (``_PINNED_RULE_TEXT``) rather than
ignored. Without that, a declaration could state a population or FX rule this
module does not perform and load cleanly -- which would make the declaration
decorative on exactly the axes it exists to fix.
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
from app.services.operators import sole_operator_id
from scripts._dev_guard import assert_dev_environment

Verdict = Literal["PASS", "FAIL"]

#: #2833 froze its declaration before ``verdict_mode`` existed and its digest
#: forbids adding the key.  So the default is keyed on IDENTITY, never on
#: absence: any other schema must say which mode it wants, or the run raises.
#: A blanket "missing means select_one" would silently reinterpret a future
#: declaration (Codex ckpt-1 finding 4).
LEGACY_SELECT_ONE_SCHEMA: Final = "core-selection-2833-v1"

#: Rule text the code IMPLEMENTS but cannot READ.  A declaration that differs on
#: any of these is refused at load time.  ``binding_percentile`` and
#: ``descriptive_percentile`` are deliberately absent: they are read and
#: honoured, so a declaration is free to state different ones.
_PINNED_RULE_TEXT: Final[Mapping[str, str]] = {
    "all_in_cost_rule": (
        "p75_full_round_trip_spread_bps; documented entry-sizing conversion markup is 0 bps; "
        "carry is structural zero only for a real long at x1"
    ),
    "completion_rule": "evaluate only at or after 00:00Z following the fifth common UTC observation date",
    "eligibility_account_rule": (
        "sole operator, provider etoro, environment demo, and the current non-revoked "
        "api_key and user_key credential ids"
    ),
    "fx_rule": (
        "every included observed row must have conversion_rate exactly 1; otherwise the candidate fails fx_unmodelled"
    ),
    "missingness_rule": (
        "within each candidate/date interval from its first through last observed hourly bucket, "
        "every hourly bucket must exist and be observed; otherwise the candidate fails incomplete_population"
    ),
    "percentile_method": "continuous linear interpolation at (n - 1) * p, matching PostgreSQL percentile_cont",
    "population_rule": (
        "all immutable hourly rows in each candidate/date interval on the first five common UTC dates "
        "at or after evidence_not_before"
    ),
    "spread_metric": (
        "strategy_core_quote_observations.spread_bps, the full bid-ask spread divided by midpoint in basis points"
    ),
}

#: ``selection_rule`` / ``no_pass_action`` legitimately differ per mode, so they
#: are pinned against the MODE instead of globally.  Both strings live here so
#: there is one source, not one per declaration.
_MODE_RULES: Final[Mapping[str, Mapping[str, str]]] = {
    "select_one": {
        "selection_rule": (
            "among passing candidates choose the lowest p75 full round-trip spread bps; "
            "break an exact tie by ascending instrument_id"
        ),
        "no_pass_action": "cash",
    },
    "per_candidate": {
        "selection_rule": (
            "each candidate passes or fails on its own refusals; no winner is chosen because a tilt "
            "sleeve holds every factor it can hold"
        ),
        "no_pass_action": "tilt sleeve not executable at the declared bar; ARM A closes",
    },
}


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
    proof_id: int
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
    eligibility_proof_id: int | None
    eligibility_response_digest: str | None


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_declaration(path: Path, expected_sha256: str) -> Mapping[str, Any]:
    """Load *path*, refusing any byte drift and any rule this module cannot perform."""
    actual = sha256_of(path)
    if actual != expected_sha256:
        raise RuntimeError(f"declaration digest mismatch: expected {expected_sha256}, got {actual}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("declaration must be a JSON object")
    mode = verdict_mode_of(payload)
    for field, expected in _PINNED_RULE_TEXT.items():
        if str(payload.get(field, "")) != expected:
            raise RuntimeError(f"declaration field {field!r} states a rule this verifier does not implement")
    for field, expected in _MODE_RULES[mode].items():
        if str(payload.get(field, "")) != expected:
            raise RuntimeError(f"declaration field {field!r} does not match verdict_mode {mode!r}")
    if not [int(value) for value in payload.get("candidate_ids", [])]:
        raise RuntimeError("declaration has no candidate_ids; an empty set cannot produce a verdict")
    if int(payload["required_common_utc_dates"]) < 1:
        raise RuntimeError("required_common_utc_dates must be at least 1")
    return payload


def verdict_mode_of(declaration: Mapping[str, Any]) -> str:
    """Resolve the declaration's verdict mode, defaulting only by schema identity."""
    declared = declaration.get("verdict_mode")
    if declared is None:
        if str(declaration.get("schema_version")) == LEGACY_SELECT_ONE_SCHEMA:
            return "select_one"
        raise RuntimeError("declaration must state verdict_mode; only core-selection-2833-v1 predates the field")
    mode = str(declared)
    if mode not in _MODE_RULES:
        raise RuntimeError(f"unrecognised verdict_mode {mode!r}")
    return mode


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
    declaration_sha256: str,
) -> Mapping[str, Any]:
    """Apply the declared rule. ``declaration_sha256`` is bound on every return path."""
    mode = verdict_mode_of(declaration)
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
            "declaration_sha256": declaration_sha256,
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
            "declaration_sha256": declaration_sha256,
        }

    pass_bar = Decimal(str(declaration["pass_bar_bps"]))
    binding = Decimal(str(declaration["binding_percentile"]))
    descriptive = Decimal(str(declaration["descriptive_percentile"]))
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
            or eligibility.leverage_values is None
            or 1 not in eligibility.leverage_values
            or eligibility.allow_open_position is not True
        ):
            refusals.append("not_proved_real_long_x1")
        median = percentile_cont(spreads, descriptive) if spreads else Decimal("NaN")
        p75 = percentile_cont(spreads, binding) if spreads else Decimal("NaN")
        if p75.is_finite() and p75 > pass_bar:
            # ⚠ The label is DERIVED from the declared bar rather than written
            # beside it. `"60"` renders `cost_above_60_bps` byte-identically to
            # the literal it replaced; ARM A's `"50"` renders
            # `cost_above_50_bps`. It is declaration-faithful, so `"60.0"` would
            # render `cost_above_60.0_bps` -- not normalised, because
            # normalising re-introduces a second source for the number.
            refusals.append(f"cost_above_{declaration['pass_bar_bps']}_bps")
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
                eligibility_proof_id=None if eligibility is None else eligibility.proof_id,
                eligibility_response_digest=None if eligibility is None else eligibility.response_digest,
            )
        )
    passing = [candidate for candidate in candidates if candidate.verdict == "PASS"]
    common: dict[str, Any] = {
        "schema_version": declaration["schema_version"],
        "window_dates": [value.isoformat() for value in selected_dates],
        "declaration_sha256": declaration_sha256,
        "candidates": [asdict(candidate) for candidate in candidates],
    }
    if mode == "per_candidate":
        # ⚠ `verdict_mode` is emitted ONLY here. Adding a key to the
        # `select_one` payload would change #2833's sealed artefact two days
        # before its verdict opens, for no gain -- its declaration's sha is
        # already in the payload and determines the mode.
        return {
            "outcome": "pass" if len(passing) == len(candidates) else ("partial" if passing else "fail"),
            "verdict_mode": mode,
            "passing_instrument_ids": sorted(candidate.instrument_id for candidate in passing),
            **common,
        }
    selected = min(passing, key=lambda candidate: (candidate.p75_spread_bps, candidate.instrument_id), default=None)
    return {
        "outcome": "pass" if selected is not None else "cash",
        "selected_instrument_id": None if selected is None else selected.instrument_id,
        "selected_symbol": None if selected is None else selected.symbol,
        **common,
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
       core_eligibility_proof_id AS proof_id,
       instrument_id, observed_at, verdict, settlement_type, direction,
       leverage_values, allow_open_position, response_digest
FROM strategy_core_eligibility_proofs
WHERE instrument_id = ANY(%(candidate_ids)s)
  AND operator_id = %(operator_id)s
  AND provider = %(provider)s
  AND environment = %(environment)s
  AND api_key_credential_id = (
      SELECT id FROM broker_credentials
      WHERE operator_id = %(operator_id)s AND provider = %(provider)s
        AND environment = %(environment)s AND label = 'api_key' AND revoked_at IS NULL
  )
  AND user_key_credential_id = (
      SELECT id FROM broker_credentials
      WHERE operator_id = %(operator_id)s AND provider = %(provider)s
        AND environment = %(environment)s AND label = 'user_key' AND revoked_at IS NULL
  )
ORDER BY instrument_id, observed_at DESC, core_eligibility_proof_id DESC
"""


def _git(*args: str) -> str:
    return subprocess.run(("git", *args), check=True, text=True, capture_output=True).stdout.strip()


def assert_verifier_sources_clean(paths: Sequence[Path]) -> None:
    """Refuse provenance that labels uncommitted verifier bytes as ``HEAD``.

    ⚠ *paths* must include this module. With the rule extracted, an uncommitted
    edit here would otherwise produce a verdict stamped with a clean ``HEAD``
    (Codex ckpt-1 finding 2).
    """
    result = subprocess.run(
        ("git", "diff", "--quiet", "HEAD", "--", *(str(path) for path in paths)),
        check=False,
    )
    if result.returncode == 0:
        return
    if result.returncode == 1:
        raise RuntimeError("declaration or verifier sources differ from HEAD; commit before opening evidence")
    raise RuntimeError(f"git diff failed with exit {result.returncode}")


def run_verifier(
    *,
    # ``str | None`` because every caller passes its module ``__doc__``.
    description: str | None,
    declaration_path: Path,
    declaration_sha256: str,
    verifier_path: Path,
    source_paths: Sequence[Path],
    argv: Sequence[str] | None = None,
) -> int:
    """Run one caller's verdict.

    ``verifier_path`` is named separately from ``source_paths`` on purpose:
    ``verifier_sha256`` in the emitted result must stay the CALLER's digest, and
    deriving it from a list position would make that a property of argument
    order.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--output", type=Path, help="write the completed canonical JSON result")
    args = parser.parse_args(argv)
    assert_verifier_sources_clean(source_paths)
    declaration = load_declaration(declaration_path, declaration_sha256)
    declaration_commit = _git("log", "-1", "--format=%H", "--", str(declaration_path))
    execution_commit = _git("rev-parse", "HEAD")
    subprocess.run(("git", "merge-base", "--is-ancestor", declaration_commit, execution_commit), check=True)
    not_before = datetime.fromisoformat(str(declaration["evidence_not_before"]).replace("Z", "+00:00"))
    candidate_ids = [int(value) for value in declaration["candidate_ids"]]
    provider = str(declaration["eligibility_provider"])
    environment = str(declaration["eligibility_environment"])
    assert_dev_environment()
    with psycopg.connect(settings.database_url) as conn, conn.cursor(row_factory=dict_row) as cursor:
        operator_id = sole_operator_id(conn)
        observation_rows = cursor.execute(
            _OBSERVATIONS_SQL,
            {"candidate_ids": candidate_ids, "not_before": not_before},
        ).fetchall()
        eligibility_rows = cursor.execute(
            _ELIGIBILITY_SQL,
            {
                "candidate_ids": candidate_ids,
                "operator_id": operator_id,
                "provider": provider,
                "environment": environment,
            },
        ).fetchall()
        now_row = cursor.execute("SELECT now() AS now").fetchone()
        if now_row is None:
            raise RuntimeError("database clock query returned no row")
        now = now_row["now"]
    observations = [Observation(**row) for row in observation_rows]
    eligibilities = {
        int(row["instrument_id"]): Eligibility(
            proof_id=int(row["proof_id"]),
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
    result = dict(evaluate(observations, eligibilities, declaration, now=now, declaration_sha256=declaration_sha256))
    result.update(
        declaration_commit=declaration_commit,
        execution_commit=execution_commit,
        measured_at=now,
        eligibility_provider=provider,
        eligibility_environment=environment,
        query_sha256=hashlib.sha256((_OBSERVATIONS_SQL + "\0" + _ELIGIBILITY_SQL).encode()).hexdigest(),
        verifier_sha256=sha256_of(verifier_path),
    )
    encoded = json.dumps(result, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False) + "\n"
    if args.output is not None:
        if result["outcome"] == "evidence_collecting":
            raise RuntimeError("refusing to write a result before the declared population is complete")
        args.output.write_text(encoded, encoding="utf-8")
    sys.stdout.write(encoded)
    return 0
