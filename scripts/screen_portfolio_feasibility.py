"""Emit the #2947 dry-run feasibility artifact for a candidate portfolio.

Advisory. Reads the DB, calls the pure screen, writes JSON. Writes nothing to the
database, touches no broker, and authorises no trade.

⚠ This script does NOT enforce anything.  #2947's acceptance asks for *"a
reviewable dry-run artifact"*, and that is what this is.  An optional standalone
script establishes no ORDERING relative to an acceptance run, so the ticket's
*"before an outcome run"* gap stays open until either a checked workflow step or a
change to the ``evaluate_*`` scripts closes it.

Example -- #2833's sleeve.  It selects ONE instrument, so screen it single-leg:

    PYTHONPATH=. uv run python -m scripts.screen_portfolio_feasibility \\
        --leg 3417:1 --assigned-capital 1000 --capital-currency USD \\
        --usd-per-capital-unit 1 --cash-reserve-fraction 0.02 \\
        --out var/feasibility/2833-spy-rth.json

⚠ Screening the three candidates together at 1/3 each is STRICTER, not weaker --
each leg gets a third of the capital against the same floor -- and it answers "can
all three be funded at once", which is not a question the sleeve asks.

Exit codes: 0 feasible · 1 refused · 2 indeterminate · 3 configuration error.

Spec: ``docs/proposals/execution/2026-09-13-feasibility-screen-caller.md``
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import sys
import tempfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg

from app.config import settings
from app.services.portfolio_feasibility import (
    FeasibilityLeg,
    PortfolioFeasibilityError,
    screen_portfolio_feasibility,
)
from app.services.portfolio_feasibility_loader import (
    FeasibilityLoaderError,
    load_leg_eligibility,
    resolve_account_scope,
)

ARTIFACT_SCHEMA_VERSION = "feasibility-artifact-v1"

EXIT_FEASIBLE = 0
EXIT_REFUSED = 1
EXIT_INDETERMINATE = 2
EXIT_CONFIG = 3

_EXIT_BY_VERDICT = {
    "feasible": EXIT_FEASIBLE,
    "refused": EXIT_REFUSED,
    "indeterminate": EXIT_INDETERMINATE,
}


class ConfigError(RuntimeError):
    """A malformed invocation. Never rendered as a feasibility verdict."""


def _decimal(raw: str, *, field: str) -> Decimal:
    """Parse straight to ``Decimal`` from the literal string.

    ⚠ No float hop -- ``Decimal(float(s))`` silently renormalises, and a weight is
    a declaration.  ⚠ ``NaN``/``Infinity`` are rejected HERE rather than passed on:
    ``Decimal`` accepts both, and Postgres NUMERIC NaN does not compare like IEEE
    (``'NaN' >= 0`` is TRUE), so admitting one anywhere in this path is a known
    trap rather than a hypothetical.
    """
    try:
        value = Decimal(raw)
    except (InvalidOperation, ValueError) as exc:
        raise ConfigError(f"{field}: {raw!r} is not a decimal") from exc
    if not value.is_finite():
        raise ConfigError(f"{field}: {raw!r} is not finite")
    return value


def _parse_legs(args: argparse.Namespace) -> list[tuple[int, Decimal]]:
    """``--leg ID:WEIGHT`` (repeatable) or ``--equal-weight ID,ID,…``, never both."""
    if bool(args.leg) == bool(args.equal_weight):
        raise ConfigError("give exactly one of --leg (repeatable) or --equal-weight")

    if args.equal_weight:
        try:
            ids = [int(part) for part in args.equal_weight.split(",") if part.strip()]
        except ValueError as exc:
            raise ConfigError(f"--equal-weight: {args.equal_weight!r} is not a comma-separated id list") from exc
        if not ids:
            raise ConfigError("--equal-weight: no instrument ids given")
        _reject_duplicates(ids)
        # Distribute exactly: the last leg absorbs the remainder so the weights sum
        # to 1 exactly rather than to 1 +/- representation error.  The screen has a
        # tolerance, but relying on it would put an arithmetic artefact into a
        # DECLARATION.
        share = (Decimal(1) / Decimal(len(ids))).quantize(Decimal("1e-18"))
        weights = [share] * (len(ids) - 1)
        weights.append(Decimal(1) - sum(weights, Decimal(0)))
        return list(zip(ids, weights, strict=True))

    legs: list[tuple[int, Decimal]] = []
    for spec in args.leg:
        instrument_id, _, weight = spec.partition(":")
        if not weight:
            raise ConfigError(f"--leg {spec!r}: expected ID:WEIGHT")
        try:
            parsed_id = int(instrument_id)
        except ValueError as exc:
            raise ConfigError(f"--leg {spec!r}: {instrument_id!r} is not an instrument id") from exc
        legs.append((parsed_id, _decimal(weight, field=f"--leg {spec}")))
    _reject_duplicates([i for i, _ in legs])
    return legs


def _reject_duplicates(ids: list[int]) -> None:
    seen = {i for i in ids if ids.count(i) > 1}
    if seen:
        raise ConfigError(f"duplicate instrument ids: {sorted(seen)}")


def _symbols(conn: psycopg.Connection[Any], instrument_ids: list[int]) -> dict[int, str]:
    """Symbols for every requested id, or a configuration error.

    ⚠ NEVER shrink the candidate.  An id absent from ``instruments`` is an explicit
    error, not a dropped leg -- the screen's contract is that it returns a verdict
    per leg of the caller's OWN list and never a subset, and a join that silently
    dropped one would defeat that from underneath.
    """
    rows = conn.execute(
        "SELECT instrument_id, symbol FROM instruments WHERE instrument_id = ANY(%(ids)s::bigint[])",
        {"ids": instrument_ids},
    ).fetchall()
    found = {int(instrument_id): str(symbol) for instrument_id, symbol in rows}
    missing = [i for i in instrument_ids if i not in found]
    if missing:
        raise ConfigError(f"unknown instrument ids: {missing}")
    return found


def _jsonable(value: Any) -> Any:
    """Declared encoding: Decimal -> string, datetime -> RFC-3339 UTC,
    timedelta -> integer seconds, UUID -> string.

    ⚠ Decimal must NOT become a float: the artifact exists to be re-checked, and a
    float round-trip would quietly move the numbers it is checked against.
    """
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {k: _jsonable(v) for k, v in dataclasses.asdict(value).items()}
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, timedelta):
        return int(value.total_seconds())
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(v) for v in value]
    return value


def _write_atomic(path: Path, payload: dict[str, Any]) -> None:
    """Temp file then ``os.replace``, so a reader never sees a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False, encoding="utf-8", suffix=".tmp")
    try:
        with handle as out:
            json.dump(payload, out, indent=2, sort_keys=True, allow_nan=False)
            out.write("\n")
        os.replace(handle.name, path)
    except BaseException:
        Path(handle.name).unlink(missing_ok=True)
        raise


def _discard_stale_artifact(path: Path | None) -> None:
    """Remove any PREVIOUS artifact at ``path`` when this run produced none.

    ⚠ Atomic replacement alone does not cover this, and the docstring here used to
    claim it did.  A failed run writes nothing, so yesterday's artifact survives at
    the same path with a fresh-looking name and nothing to mark it superseded --
    silent stale evidence, which is the failure mode this whole ticket exists to
    avoid on the account side.  "No artifact" is the honest state.

    ⚠ ``path`` is ``None`` when argument parsing itself failed: no output location
    was successfully declared, so nothing was promised and nothing is removed.
    """
    if path is None:
        return
    try:
        if path.exists():
            path.unlink()
            print(f"removed stale artifact {path} (this run produced no verdict)", file=sys.stderr)
    except OSError as exc:  # pragma: no cover -- reported, never masked
        print(f"WARNING: could not remove stale artifact {path}: {exc}", file=sys.stderr)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--leg", action="append", default=[], metavar="ID:WEIGHT")
    parser.add_argument("--equal-weight", metavar="ID,ID,...")
    parser.add_argument("--assigned-capital", required=True)
    parser.add_argument("--capital-currency", required=True)
    parser.add_argument("--usd-per-capital-unit", default=None)
    parser.add_argument("--cash-reserve-fraction", required=True)
    parser.add_argument("--provider", default="etoro")
    parser.add_argument("--environment", default="demo", choices=["demo", "real"])
    parser.add_argument("--out", required=True, type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    """⚠⚠ The exit codes are a VERDICT vocabulary, so no non-verdict failure may
    borrow one.

    Two leaks had to be closed explicitly, and both are silent in the direction
    that matters -- they report a confident answer about the portfolio when the
    question was never answered:

    * ``argparse`` exits **2** on a bad flag, which is this script's
      ``indeterminate``. A typo'd option would read as a feasibility finding.
    * an uncaught exception exits **1**, which is ``refused``. A database outage
      would read as "this portfolio is infeasible".
    """
    out_path: Path | None = None
    try:
        try:
            args = _build_parser().parse_args(argv)
        except SystemExit as exc:
            # `--help` / `--version` exit 0 and must keep doing so; every other
            # argparse exit is a malformed invocation, not a verdict.
            if exc.code in (0, None):
                raise
            return EXIT_CONFIG
        out_path = args.out
        legs_spec = _parse_legs(args)
        assigned_capital = _decimal(args.assigned_capital, field="--assigned-capital")
        cash_reserve_fraction = _decimal(args.cash_reserve_fraction, field="--cash-reserve-fraction")
        usd_per_capital_unit = (
            None
            if args.usd_per_capital_unit is None
            else _decimal(args.usd_per_capital_unit, field="--usd-per-capital-unit")
        )

        with psycopg.connect(settings.database_url) as conn:
            scope = resolve_account_scope(conn, provider=args.provider, environment=args.environment)
            symbols = _symbols(conn, [i for i, _ in legs_spec])
            loaded = load_leg_eligibility(conn, instrument_ids=[i for i, _ in legs_spec], scope=scope)

        legs = tuple(FeasibilityLeg(instrument_id=i, symbol=symbols[i], weight=w) for i, w in legs_spec)
        report = screen_portfolio_feasibility(
            assigned_capital=assigned_capital,
            capital_currency=args.capital_currency,
            usd_per_capital_unit=usd_per_capital_unit,
            cash_reserve_fraction=cash_reserve_fraction,
            scope=scope,
            legs=legs,
            eligibility=loaded.by_instrument,
            now=datetime.now(UTC),
        )
    except (ConfigError, FeasibilityLoaderError, PortfolioFeasibilityError) as exc:
        # ⚠ No artifact is written.  "The question could not be asked" is not a
        # feasibility verdict and must never be rendered as one.
        print(f"configuration error: {exc}", file=sys.stderr)
        _discard_stale_artifact(out_path)
        return EXIT_CONFIG
    except psycopg.Error as exc:
        # A database failure says nothing about the portfolio.  Left uncaught it
        # would exit 1 -- `refused` -- which is a confident wrong answer.
        print(f"database error: {exc}", file=sys.stderr)
        _discard_stale_artifact(out_path)
        return EXIT_CONFIG

    payload = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "report": _jsonable(report),
        # The screen's inputs, so the artifact can be re-checked without the DB.
        # `proof_id` above all: it is the only field naming WHICH row was selected,
        # and therefore the only way to tell a correct selection from a
        # plausible-looking wrong one.
        "evidence": {
            "requested": _jsonable([{"instrument_id": i, "symbol": symbols[i], "weight": w} for i, w in legs_spec]),
            "assigned_capital": str(assigned_capital),
            "capital_currency": args.capital_currency,
            "proofs": _jsonable(loaded.evidence),
            # Present only where an instrument had NO in-scope proof: rows exist
            # under OTHER credentials. "Investigate a credential swap", which is a
            # different action from "run a census".
            "out_of_scope_proofs": _jsonable(loaded.out_of_scope),
        },
    }
    _write_atomic(args.out, payload)

    print(f"verdict={report.verdict} portfolio_code={report.portfolio_code} artifact={args.out}")
    for leg in report.legs:
        print(f"  {leg.leg.symbol} ({leg.leg.instrument_id}): {leg.disposition} {leg.code}")
    if loaded.out_of_scope:
        print(f"  out-of-scope proofs exist for: {sorted(loaded.out_of_scope)}")
    return _EXIT_BY_VERDICT[report.verdict]


if __name__ == "__main__":
    raise SystemExit(main())
