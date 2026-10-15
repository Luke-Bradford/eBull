"""#3616 slice 3d-3c-3 fact-finding: non-reporting fund disposals under TCGA 1992 s106A, on the dev DB.

Read-only. In one REPEATABLE READ snapshot it reads the effective generation as matching reads it
(``tax_matching._read_inputs``), computes today's matching in memory (``compute_matching``), and takes every security
whose character is ``exchange_traded_fund``. For each it reads the newest fund determination run and applies
3d-3c-2 Design 3 (``reporting_status``) at every disposal date and at the run's cutoff date, then identifies the
security's admitted lots as relevant securities (spec Design 2) under both readings of s106A(5)'s period:

- ``date``: acquisitions on the UK dates d+1 .. d+30 (the policy reading, spec Source rules);
- ``time``: acquisitions after the disposal's execution instant, up to the end of UK date d+30.

Each disposal is identified in turn (s106A(4)), first with acquisitions in its period, earliest first (s106A(5)),
then with acquisitions made before it, latest first (s106A(6)). Each lot is its own acquisition or disposal: s105
does not apply to relevant securities (s104(3), s108(1)(c)). Partial withdrawals use ``tax_matching.apportion``;
every other sum is exact. It prints the basic gain (reg 39(1)) and its character (spec Design 3) per disposal, the
remaining acquisitions, and where the two readings differ. Exit status is non-zero on a broken invariant.

The printout carries dates, symbols, amounts and lot keys (the demo account's order and position ids), never an
account identifier.

    PYTHONPATH=. uv run python -m scripts.measure_3616_slice3d3c3_fund_disposals
"""

from __future__ import annotations

import hashlib
import json
import sys
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, Inexact, Rounded, localcontext
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.services.tax_fund_determination import (
    Decision,
    RunState,
    offshore_fund,
    reporting_status,
)
from app.services.tax_matching import (
    _read_inputs,
    apportion,
    compute_matching,
    placements,
    window_elapses_at,
)

ZERO = Decimal(0)


@dataclass
class Lot:
    lot_key: str
    direction: str
    executed_at: datetime
    uk_date: date
    tax_year: str
    units: Decimal
    amount: Decimal | None
    reasons: frozenset[str]
    left: Decimal = ZERO
    cost_left: Decimal | None = None


@dataclass
class Ident:
    rule: str
    acquisition: Lot
    units: Decimal
    cost: Decimal | None


@dataclass
class Disposal:
    lot: Lot
    idents: list[Ident] = field(default_factory=list)


class Refused(Exception):
    """A Design 2 refusal: the security is withheld."""


def identify(lots: list[Lot], reading: str) -> tuple[list[Disposal], list[Lot]]:
    """Spec Design 2 over one security's lots: the disposals in identification order and the remaining acquisitions.
    Raises ``Refused`` for a Design 2 refusal and ``AssertionError`` for a broken invariant."""
    instants = [x.executed_at for x in lots]
    if len(set(instants)) != len(instants):
        raise Refused("time_order_unestablished: two lots share an execution instant")
    acqs = sorted((x for x in lots if x.direction == "acquisition"), key=lambda x: (x.executed_at, x.lot_key))
    if any(a.uk_date < PRE_2009 for a in acqs):
        raise Refused("pre_2009_rights: an acquisition before 2009-12-01")
    for a in acqs:
        a.left, a.cost_left = a.units, a.amount
    disps = sorted((x for x in lots if x.direction == "disposal"), key=lambda x: (x.uk_date, x.executed_at, x.lot_key))
    out: list[Disposal] = []
    for d in disps:
        disp = Disposal(d)
        need = d.units
        end = d.uk_date + timedelta(days=30)
        if reading == "date":
            period = [a for a in acqs if d.uk_date < a.uk_date <= end]
        else:
            period = [a for a in acqs if a.executed_at > d.executed_at and a.uk_date <= end]
        earlier = [a for a in reversed(acqs) if a.executed_at < d.executed_at]
        for rule, pool in (("thirty_day", period), ("lifo", earlier)):
            for a in pool:
                if need == ZERO:
                    break
                if a.left == ZERO:
                    continue
                take = min(need, a.left)
                if a.cost_left is None:
                    cost = None
                elif take == a.left:
                    cost = a.cost_left
                else:
                    cost = apportion(take, a.cost_left, a.left)
                disp.idents.append(Ident(rule, a, take, cost))
                a.left -= take
                a.cost_left = None if cost is None or a.cost_left is None else a.cost_left - cost
                need -= take
        if need != ZERO:
            raise Refused(f"identification_incomplete: {d.lot_key} leaves {need} units")
        out.append(disp)
    # Design 2's invariants.
    for a in acqs:
        taken = [i for x in out for i in x.idents if i.acquisition is a]
        assert sum((i.units for i in taken), ZERO) + a.left == a.units, f"units not conserved: {a.lot_key}"
        if a.amount is not None:
            costs = [i.cost for i in taken]
            assert all(c is not None for c in costs) and a.cost_left is not None
            assert sum((c for c in costs if c is not None), ZERO) + a.cost_left == a.amount, f"cost: {a.lot_key}"
    for x in out:
        assert sum((i.units for i in x.idents), ZERO) == x.lot.units, f"disposal units: {x.lot.lot_key}"
        for i in x.idents:
            if i.rule == "thirty_day" and reading == "date":
                assert x.lot.uk_date < i.acquisition.uk_date <= x.lot.uk_date + timedelta(days=30)
            if i.rule == "lifo":
                assert i.acquisition.executed_at < x.lot.executed_at
    return out, [a for a in acqs if a.left > ZERO]


def character(computed_gain: Decimal, transparent: str) -> tuple[str, Decimal, Decimal | None, Decimal | None]:
    """Spec Design 3: ``(gain_character, basic_gain, offshore_income_gain, chargeable_gain)``."""
    if computed_gain < ZERO:
        return "capital_loss", ZERO, ZERO, computed_gain
    if computed_gain == ZERO:
        return "nil", ZERO, ZERO, ZERO
    if transparent == "opaque":
        return "offshore_income_gain", computed_gain, computed_gain, ZERO
    return "oig_or_capital_unresolved", computed_gain, None, None


def _determination_inputs(conn: psycopg.Connection[Any], security: int) -> tuple[Any, Any]:
    run = conn.execute(
        "SELECT run_id, status, cutoff FROM tax_fund_determination_runs ORDER BY run_id DESC LIMIT 1"
    ).fetchone()
    if run is None:
        return None, None
    obs = conn.execute(
        """
        SELECT o.outcome, o.refusal, o.determination_id, d.body, d.file_sha256, f.search_outcome
        FROM tax_fund_determination_observations o
        LEFT JOIN tax_fund_determinations d ON d.determination_id = o.determination_id
        LEFT JOIN tax_fund_observations f ON f.observation_id = o.fund_observation_id
        WHERE o.run_id = %s AND o.tax_security_id = %s
        """,
        [run[0], security],
    ).fetchone()
    return run, obs


def statuses(run: Any, obs: Any, lots: list[Lot], as_of: datetime) -> tuple[list[tuple[str, Any]], str | None]:
    """Spec Design 1: ``reporting_status`` at each unique lot date, then at the holding date when the newest run
    succeeded. Returns the results and the first failing status (None when every one is ``non_reporting_assumed``)."""
    state = None if run is None else RunState(run[1], run[2])
    decision = None if obs is None else Decision(obs[0], obs[1], obs[2])
    det = None if obs is None else obs[3]
    search = None if obs is None else obs[5]
    dates: list[tuple[str, date]] = [(d.isoformat(), d) for d in sorted({x.uk_date for x in lots})]
    first = reporting_status(state, decision, det, search, date.min, as_of)
    if first.status in ("no_run", "run_failed", "not_observed") or run[2] is None:
        return [("any", first)], first.status
    dates.append(("holding", run[2].astimezone(UTC).date()))
    results = [(label, reporting_status(state, decision, det, search, d, as_of)) for label, d in dates]
    failing = next((r.status for _, r in results if r.status != "non_reporting_assumed"), None)
    return results, failing


def reasons(disposals: list[Disposal], remaining: list[Lot], fetch_started: datetime | None) -> dict[str, list[str]]:
    """Spec Design 4's matrix: reasons per disposal, identification, balance and the holding."""
    base = {HISTORY, STATUS}
    out: dict[str, list[str]] = {}
    carried: set[str] = set()
    for x in disposals:
        own: set[str] = set()
        if fetch_started is None or fetch_started < window_elapses_at(x.lot.uk_date):
            own.add(WINDOW)
        if any(a.uk_date == x.lot.uk_date and a.executed_at > x.lot.executed_at for a in ALL_ACQS):
            own.add(F2)
        carried |= own
        for n, i in enumerate(x.idents):
            out[f"ident {x.lot.lot_key} #{n}"] = sorted(base | i.acquisition.reasons | carried)
        idents = set().union(*(i.acquisition.reasons for i in x.idents)) if x.idents else set()
        out[f"disposal {x.lot.lot_key}"] = sorted(base | x.lot.reasons | idents | carried)
    for a in remaining:
        out[f"balance {a.lot_key}"] = sorted(base | a.reasons | carried)
    out["holding"] = sorted(base | set().union(*(a.reasons for a in remaining)) | carried) if remaining else []
    return out


ALL_ACQS: list[Lot] = []
HISTORY, STATUS, WINDOW, F2 = (
    "history_coverage_unestablished",
    "non_reporting_status_assumed",
    "window_open",
    "thirty_day_period_assumed",
)
PRE_2009 = date(2009, 12, 1)
ETF_TYPE_ID = 6


def _fund(out: Any, inputs: Any, result: Any, s: Any, lots: list[Lot], conn: Any) -> int:
    """Print one fund security; returns 1 when a fund with lots and no other cause is not computed."""
    sym = inputs.instruments[s.tax_security_id].symbol
    print(f"\n[fund {s.tax_security_id} {sym}] matching today: {s.status} {s.character.character}", file=out)
    other = sorted(
        d.reason
        for d in result.dependencies
        if d.tax_security_id == s.tax_security_id and d.reason != "character_unestablished"
    )
    print(f"  other withholding causes: {other}; withheld_all {result.withheld_all}; lots {len(lots)}", file=out)
    for lot in sorted(lots, key=lambda x: (x.executed_at, x.lot_key)):
        print(
            f"    {lot.direction:11} {lot.executed_at.isoformat()} {lot.uk_date} {lot.units} £{lot.amount}"
            f" {lot.lot_key} {sorted(lot.reasons)}",
            file=out,
        )
    run, obs = _determination_inputs(conn, s.tax_security_id)
    print(f"  determination run {run and run[:3]}; observation {obs and obs[:3]}", file=out)
    det = None if obs is None else obs[3]
    transparent = None
    if det is not None:
        transparent = next(f["answer"] for f in det["findings"] if f["question"] == "transparent_fund")
        print(f"  file sha256 {obs[4]}; offshore_fund {offshore_fund(det)}", file=out)
        print(f"  transparent_fund {transparent}; search {obs[5]}", file=out)
    results, failing = statuses(run, obs, lots, inputs.as_of)
    for label, r in results:
        print(f"  status at {label}: {r.status} {r.reason or ''} rests on {list(r.assumptions)}", file=out)
    if other or result.withheld_all:
        print("  withheld by other causes", file=out)
        return 0
    if failing is not None:
        print(f"  WITHHELD: fund_status {failing}", file=out)
        return 1 if lots else 0
    print("  character non_reporting_fund", file=out)
    ALL_ACQS[:] = [x for x in lots if x.direction == "acquisition"]
    computed: dict[str, list[tuple[str, str, Decimal, Decimal | None]]] = {}
    for reading in ("date", "time"):
        fresh = [
            Lot(x.lot_key, x.direction, x.executed_at, x.uk_date, x.tax_year, x.units, x.amount, x.reasons)
            for x in lots
        ]
        with localcontext() as ctx:
            ctx.prec = 60
            ctx.traps[Inexact] = True
            ctx.traps[Rounded] = True
            try:
                disposals, remaining = identify(fresh, reading)
            except Refused as e:
                print(f"  reading {reading}: WITHHELD {e}", file=out)
                return 1
            print(f"  reading {reading}{' (policy)' if reading == 'date' else ''}:", file=out)
            computed[reading] = [
                (x.lot.lot_key, i.acquisition.lot_key, i.units, i.cost) for x in disposals for i in x.idents
            ]
            for x in disposals:
                ids = "; ".join(f"{i.rule} {i.acquisition.lot_key} {i.units} £{i.cost}" for i in x.idents)
                costs = [i.cost for i in x.idents]
                if x.lot.amount is None or any(c is None for c in costs):
                    print(f"    disposal {x.lot.lot_key}: amount_missing [{ids}]", file=out)
                    continue
                cost = sum((c for c in costs if c is not None), ZERO)
                gain = x.lot.amount - cost
                ch, basic, oig, chargeable = character(gain, transparent or "unresolved")
                print(
                    f"    disposal {x.lot.uk_date} {x.lot.tax_year} {x.lot.units} proceeds £{x.lot.amount} cost £{cost}"
                    f" computed gain £{gain} basic gain £{basic} {ch} OIG {oig} chargeable {chargeable} [{ids}]",
                    file=out,
                )
            units = sum((a.left for a in remaining), ZERO)
            cl = [a.cost_left for a in remaining]
            total = "undefined" if any(c is None for c in cl) else f"£{sum((c for c in cl if c is not None), ZERO)}"
            print(f"    holding: {len(remaining)} acquisitions, {units} units, cost {total}", file=out)
            for a in remaining:
                print(f"      {a.uk_date} {a.left} of {a.units} £{a.cost_left} {a.lot_key}", file=out)
            if reading == "date":
                for k, v in reasons(disposals, remaining, inputs.history_fetch_started_at).items():
                    print(f"    reasons {k}: {v}", file=out)
    print(f"  readings agree: {computed['date'] == computed['time']}", file=out)
    return 0


def main() -> None:
    out = sys.stdout
    script = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    print(f"run {datetime.now(UTC).isoformat()}; script sha256 {script}", file=out)
    print(
        f"python {sys.version.split()[0]}; module {__spec__.name if __spec__ else __file__};"
        f" argv {json.dumps(sys.argv[1:])}",
        file=out,
    )
    failures = 0
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with conn.transaction():
            inputs = _read_inputs(conn)
            if inputs is None:
                print("no effective generation", file=out)
                sys.exit(1)
            result = compute_matching(inputs)
            print(
                f"evidence run {inputs.evidence_run_id} ({inputs.evidence_policy_id});"
                f" as_of {inputs.as_of.isoformat()}; history fetch started {inputs.history_fetch_started_at}",
                file=out,
            )
            security_of = {p.instrument_id: p.tax_security_id for p in placements(inputs)}
            rows = conn.execute(
                """SELECT lot_key, direction, executed_at, uk_date, tax_year, units, amount_gbp, instrument_id
                   FROM tax_evidence_lots WHERE run_id = %s""",
                [inputs.evidence_run_id],
            ).fetchall()
            print("\n[securities]", file=out)
            for s in result.securities:
                deps = [d.reason for d in result.dependencies if d.tax_security_id == s.tax_security_id]
                print(
                    f"  {s.tax_security_id} {inputs.instruments[s.tax_security_id].symbol}: {s.status},"
                    f" character {s.character.character}/{s.character.detail}, dependencies {sorted(deps)}",
                    file=out,
                )
            # Fund securities by instrument type, independent of the character today's code assigns.
            for s in result.securities:
                if inputs.characters[s.tax_security_id].types != frozenset({ETF_TYPE_ID}):
                    continue
                lots = [
                    Lot(r[0], r[1], r[2], r[3], r[4], r[5], r[6], frozenset(result.lot_classes[r[0]].reasons))
                    for r in rows
                    if security_of.get(r[7]) == s.tax_security_id
                ]
                try:
                    failures += _fund(out, inputs, result, s, lots, conn)
                except AssertionError as e:
                    print(f"  INVARIANT BROKEN: {e}", file=out)
                    failures += 1
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
