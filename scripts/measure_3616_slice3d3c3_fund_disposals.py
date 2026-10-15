"""#3616 slice 3d-3c-3 fact-finding: non-reporting fund disposals under TCGA 1992 s106A, on the dev DB.

Read-only. In one REPEATABLE READ snapshot it reads the effective generation as matching reads it
(``tax_matching._read_inputs``) and computes matching in memory with whatever ``compute_matching`` is deployed. It
takes every security whose members are all instrument type 6, independent of the character it was given, with its
lots by the same member mapping matching uses. An earlier character-table line (such as ``member_conflict``) stops
fund evaluation. Another upstream cause (a dependency the fund rules do not produce, or ``withheld_all``) lets the
character be evaluated but prevents identification. Otherwise the Script recomputes the fund rules itself:

1. spec Design 1: ``reporting_status`` at every distinct lot date and at the holding date (date-independent failures
   first, with no date); a failing status withholds;
2. spec Design 2 under both readings of s106A(5)'s period, with its refusals and invariants (including column
   capacities):
   - ``date``: acquisitions on the UK dates d+1 .. d+30 (the policy, F2);
   - ``time``: acquisitions after the disposal's instant, up to the end of UK date d+30;
3. spec Design 3 (computed and basic gain, character, splits) and Design 4 (reasons) for the policy reading.

For the policy reading it prints one ``RECORD`` line per expected stored row (character, disposal, identification,
balance, holding) for field-by-field acceptance, and whether the readings agree. It exits 1 on a broken invariant and
2 when a security named by ``--expect-computed`` is not computed; refusals and failing statuses are outcomes, listed
under ``[outcomes]``.

The printout carries dates, symbols, amounts and lot keys (the demo account's order and position ids), never an
account identifier.

    PYTHONPATH=. uv run python -m scripts.measure_3616_slice3d3c3_fund_disposals --expect-computed 3000
"""

from __future__ import annotations

import argparse
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
    GBP_PRECISION,
    GBP_SCALE,
    UNITS_PRECISION,
    UNITS_SCALE,
    _read_inputs,
    apportion,
    compute_matching,
    fits,
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
    subject: str = ""


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
    closes = [x.subject for x in lots if x.direction == "disposal"]
    if len(set(closes)) != len(closes):
        raise Refused("close_fragments_unestablished: two disposal lots of one order")
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


def statuses(run: Any, obs: Any, lots: list[Lot], as_of: datetime) -> tuple[list[tuple[str, Any]], Any]:
    """Spec Design 1: ``reporting_status`` at each unique lot date, then at the holding date when the newest run
    succeeded. Returns the results and the first failing ``(status, reason, date)``, None when every one is
    ``non_reporting_assumed``; the date is kept only for ``outside_interval``, the one date-dependent refusal."""
    state = None if run is None else RunState(run[1], run[2])
    decision = None if obs is None else Decision(obs[0], obs[1], obs[2])
    det = None if obs is None else obs[3]
    search = None if obs is None else obs[5]
    first = reporting_status(state, decision, det, search, date.min, as_of)
    if first.status in ("no_run", "run_failed", "not_observed"):
        return [("any", first)], (first.status, None, None)
    dates: list[tuple[str, date]] = [(d.isoformat(), d) for d in sorted({x.uk_date for x in lots})]
    dates.append(("holding", run[2].astimezone(UTC).date()))
    results = [(label, d, reporting_status(state, decision, det, search, d, as_of)) for label, d in dates]
    for _, d, r in results:
        if r.status != "non_reporting_assumed":
            code = r.reason if r.reason in ("list_match", "search_unavailable") else None
            return [(label, r) for label, _, r in results], (
                r.status,
                code,
                d if r.status == "outside_interval" else None,
            )
    return [(label, r) for label, _, r in results], None


def reasons(
    disposals: list[Disposal], remaining: list[Lot], acquisitions: list[Lot], fetch_started: datetime | None
) -> dict[str, list[str]]:
    """Spec Design 4's matrix: reasons per disposal, identification (from 1), balance and the holding."""
    base = {HISTORY, STATUS}
    out: dict[str, list[str]] = {}
    carried: set[str] = set()
    for x in disposals:
        if fetch_started is None or fetch_started < window_elapses_at(x.lot.uk_date):
            carried.add(WINDOW)
        if any(a.uk_date == x.lot.uk_date and a.executed_at > x.lot.executed_at for a in acquisitions):
            carried.add(F2)
        for n, i in enumerate(x.idents, start=1):
            out[f"identification {x.lot.lot_key} seq {n}"] = sorted(base | i.acquisition.reasons | carried)
        idents: set[str] = set().union(*(i.acquisition.reasons for i in x.idents))
        out[f"disposal {x.lot.lot_key}"] = sorted(base | x.lot.reasons | idents | carried)
    for a in remaining:
        out[f"balance {a.lot_key}"] = sorted(base | a.reasons | carried)
    out["holding"] = sorted(base | set().union(*(a.reasons for a in remaining)) | carried)
    return out


HISTORY, STATUS, WINDOW, F2 = (
    "history_coverage_unestablished",
    "non_reporting_status_assumed",
    "window_open",
    "thirty_day_period_assumed",
)
PRE_2009 = date(2009, 12, 1)
#: Spec Design 1 (F7): determinations whose fund-level exceptions (regs 28, 31A-31C) were reviewed by this spec.
EXCEPTIONS_REVIEWED = frozenset({"fund_determination_v1:0000884394:1"})
ETF_TYPE_ID = 6
#: Dependencies the fund rules themselves produce (the Script recomputes these; any other cause is upstream).
FUND_RULE_REASONS = frozenset(
    {
        "character_unestablished",
        "time_order_unestablished",
        "close_fragments_unestablished",
        "pre_2009_rights",
        "identification_incomplete",
    }
)


def _record(out: Any, kind: str, **fields: Any) -> None:
    """One canonical expected row, for field-by-field acceptance."""
    print(f"    RECORD {kind} {json.dumps(fields, default=str, sort_keys=True)}", file=out)


def _character(
    out: Any,
    character: str,
    detail: str | None,
    *,
    status: tuple[str, str | None, date | None] | None = None,
    determination: str | None = None,
    transparent: tuple[str | None, str | None] = (None, None),
    assumptions: list[str] | None = None,
) -> None:
    """The complete expected character row (spec Design 5's truth table), every fund column present."""
    _record(
        out,
        "character",
        character=character,
        detail=detail,
        fund_status=None if status is None else status[0],
        fund_status_reason=None if status is None else status[1],
        fund_status_date=None if status is None else status[2],
        fund_determination_id=determination,
        transparent_fund=transparent[0],
        transparent_basis=transparent[1],
        fund_assumptions=assumptions,
    )


def _dependency(
    out: Any,
    reason: str,
    *,
    subject: str | None = None,
    lot_key: str | None = None,
    instrument_id: int | None = None,
    detail: str | None = None,
) -> None:
    """One expected dependency row by its semantic key (``seq`` and the run id are not compared)."""
    _record(
        out, "dependency", reason=reason, subject=subject, lot_key=lot_key, instrument_id=instrument_id, detail=detail
    )


def _withheld(out: Any) -> None:
    """The expected security row of a withheld security: no basis, every pool column NULL."""
    _record(
        out,
        "security",
        status="withheld",
        holding_basis=None,
        pool_units=None,
        pool_cost_gbp=None,
        pool_missing_lot_keys=None,
        pool_reasons=None,
        window_open_since=None,
    )


def _check_capacity(value: Decimal | None, precision: int, scale: int, what: str) -> None:
    assert value is None or fits(value, precision, scale), f"{what} {value} does not fit NUMERIC({precision},{scale})"


def _fund(out: Any, inputs: Any, result: Any, s: Any, lots: list[Lot], conn: Any) -> str:
    """Print one fund security; returns its outcome: ``computed``, ``upstream``, ``status`` or ``refused``."""
    sym = inputs.instruments[s.tax_security_id].symbol
    print(f"\n[fund {s.tax_security_id} {sym}] matching today: {s.status} {s.character.character}", file=out)
    upstream = sorted(
        d.reason
        for d in result.dependencies
        if d.tax_security_id == s.tax_security_id and d.reason not in FUND_RULE_REASONS
    )
    earlier = s.character.character == "unestablished" and s.character.detail != "exchange_traded_fund"
    print(
        f"  upstream causes: {upstream}; earlier character line: {s.character.detail if earlier else None};"
        f" withheld_all {result.withheld_all}; lots {len(lots)}",
        file=out,
    )
    for lot in sorted(lots, key=lambda x: (x.executed_at, x.lot_key)):
        print(
            f"    {lot.direction:11} {lot.executed_at.isoformat()} {lot.uk_date} {lot.units} £{lot.amount}"
            f" {lot.lot_key} {sorted(lot.reasons)}",
            file=out,
        )
    for d in sorted(
        (
            d
            for d in result.dependencies
            if d.tax_security_id == s.tax_security_id and d.reason not in FUND_RULE_REASONS
        ),
        key=lambda d: (d.reason, d.subject or "", d.lot_key or "", d.instrument_id or 0, d.detail or ""),
    ):
        _dependency(out, d.reason, subject=d.subject, lot_key=d.lot_key, instrument_id=d.instrument_id, detail=d.detail)
    if earlier:
        _character(out, s.character.character, s.character.detail)
        _withheld(out)
        _dependency(out, "character_unestablished", detail=s.character.detail)
        print("  an earlier character line decides: not evaluated as a fund", file=out)
        return "upstream"
    run, obs = _determination_inputs(conn, s.tax_security_id)
    print(f"  determination run {run and run[:3]}; observation {obs and obs[:3]}", file=out)
    det = None if obs is None else obs[3]
    transparent = basis = None
    if det is not None:
        finding = next(f for f in det["findings"] if f["question"] == "transparent_fund")
        transparent, basis = finding["answer"], finding["basis"]
        print(f"  file sha256 {obs[4]}; offshore_fund {offshore_fund(det)}; search {obs[5]}", file=out)
    results, failing = statuses(run, obs, lots, inputs.as_of)
    for label, r in results:
        print(f"  status at {label}: {r.status} {r.reason or ''} rests on {list(r.assumptions)}", file=out)
    if failing is None and obs[2] not in EXCEPTIONS_REVIEWED:
        failing = ("exceptions_unreviewed", None, None)
    if failing is not None:
        _character(out, "unestablished", "exchange_traded_fund", status=failing)
        _withheld(out)
        _dependency(out, "character_unestablished", detail="exchange_traded_fund")
        return "status"
    assumptions = sorted({a for _, r in results for a in r.assumptions})
    # The run id is provenance, not compared: an equivalent later determination run reuses the matching run.
    _character(
        out, "non_reporting_fund", None, determination=obs[2], transparent=(transparent, basis), assumptions=assumptions
    )
    print(f"  (provenance: determination run {run[0]}; stored run must hold a determined observation of it)", file=out)
    if upstream or result.withheld_all:
        _withheld(out)
        print("  withheld upstream: character evaluated, not identified", file=out)
        return "upstream"
    acquisitions = [x for x in lots if x.direction == "acquisition"]
    computed: dict[str, list[tuple[str, str, Decimal, Decimal | None]]] = {}
    for reading in ("date", "time"):
        fresh = [
            Lot(
                x.lot_key,
                x.direction,
                x.executed_at,
                x.uk_date,
                x.tax_year,
                x.units,
                x.amount,
                x.reasons,
                subject=x.subject,
            )
            for x in lots
        ]
        with localcontext() as ctx:
            ctx.prec = 60
            ctx.traps[Inexact] = True
            ctx.traps[Rounded] = True
            try:
                disposals, remaining = identify(fresh, reading)
            except Refused as e:
                print(f"  reading {reading}: REFUSED {e}", file=out)
                if reading == "date":
                    _withheld(out)
                    reason, _, detail = str(e).partition(": ")
                    _dependency(out, reason, detail=detail)
                    return "refused"
                continue
            policy = reading == "date"
            print(f"  reading {reading}{' (policy)' if policy else ''}:", file=out)
            computed[reading] = [
                (x.lot.lot_key, i.acquisition.lot_key, i.units, i.cost) for x in disposals for i in x.idents
            ]
            why = reasons(disposals, remaining, acquisitions, inputs.history_fetch_started_at) if policy else {}
            for seq, x in enumerate(disposals, start=1):
                costs = [i.cost for i in x.idents]
                cost = None if any(c is None for c in costs) else sum((c for c in costs if c is not None), ZERO)
                missing = sorted(
                    ({x.lot.lot_key} if x.lot.amount is None else set())
                    | {i.acquisition.lot_key for i in x.idents if i.cost is None}
                )
                if x.lot.amount is None or cost is None:
                    status, gain, ch, basic, oig, chargeable = "amount_missing", None, None, None, None, None
                else:
                    status, gain = "computed", x.lot.amount - cost
                    ch, basic, oig, chargeable = character(gain, transparent or "unresolved")
                for value, what in ((x.lot.units, "units"),):
                    _check_capacity(value, UNITS_PRECISION, UNITS_SCALE, what)
                for value in (x.lot.amount, cost, gain, basic, oig, chargeable):
                    _check_capacity(value, GBP_PRECISION, GBP_SCALE, "disposal sterling")
                print(
                    f"    disposal {x.lot.uk_date} {x.lot.tax_year} {x.lot.units} proceeds £{x.lot.amount} cost £{cost}"
                    f" computed gain £{gain} basic gain £{basic} {ch} OIG {oig} chargeable {chargeable}",
                    file=out,
                )
                if policy:
                    _record(
                        out,
                        "disposal",
                        lot_key=x.lot.lot_key,
                        seq=seq,
                        executed_at=x.lot.executed_at,
                        uk_date=x.lot.uk_date,
                        tax_year=x.lot.tax_year,
                        units=x.lot.units,
                        proceeds_gbp=x.lot.amount,
                        cost_gbp=cost,
                        computed_gain_gbp=gain,
                        basic_gain_gbp=basic,
                        offshore_income_gain_gbp=oig,
                        chargeable_gain_gbp=chargeable,
                        status=status,
                        gain_character=ch,
                        missing_lot_keys=missing,
                        reasons=why[f"disposal {x.lot.lot_key}"],
                    )
                for n, i in enumerate(x.idents, start=1):
                    _check_capacity(i.units, UNITS_PRECISION, UNITS_SCALE, "identification units")
                    _check_capacity(i.cost, GBP_PRECISION, GBP_SCALE, "identification cost")
                    if reading == "time" and i.rule == "thirty_day":
                        assert i.acquisition.executed_at > x.lot.executed_at
                        assert i.acquisition.uk_date <= x.lot.uk_date + timedelta(days=30)
                    if policy:
                        _record(
                            out,
                            "identification",
                            disposal_lot_key=x.lot.lot_key,
                            seq=n,
                            rule=i.rule,
                            acquisition_lot_key=i.acquisition.lot_key,
                            acquisition_executed_at=i.acquisition.executed_at,
                            acquisition_uk_date=i.acquisition.uk_date,
                            units=i.units,
                            cost_gbp=i.cost,
                            missing_lot_keys=[] if i.cost is not None else [i.acquisition.lot_key],
                            reasons=why[f"identification {x.lot.lot_key} seq {n}"],
                        )
            units = sum((a.left for a in remaining), ZERO)
            cl = [a.cost_left for a in remaining]
            total = None if any(c is None for c in cl) else sum((c for c in cl if c is not None), ZERO)
            _check_capacity(units, UNITS_PRECISION, UNITS_SCALE, "holding units")
            _check_capacity(total, GBP_PRECISION, GBP_SCALE, "holding cost")
            print(f"    holding: {len(remaining)} acquisitions, {units} units, cost {total}", file=out)
            for a in remaining:
                _check_capacity(a.left, UNITS_PRECISION, UNITS_SCALE, "balance units")
                _check_capacity(a.cost_left, GBP_PRECISION, GBP_SCALE, "balance cost")
                print(f"      {a.uk_date} {a.left} of {a.units} £{a.cost_left} {a.lot_key}", file=out)
                if policy:
                    _record(
                        out,
                        "balance",
                        lot_key=a.lot_key,
                        units_left=a.left,
                        cost_left_gbp=a.cost_left,
                        missing_lot_keys=[] if a.cost_left is not None else [a.lot_key],
                        reasons=why[f"balance {a.lot_key}"],
                    )
            if policy:
                open_since = min(
                    (
                        x.lot.uk_date
                        for x in disposals
                        if inputs.history_fetch_started_at is None
                        or inputs.history_fetch_started_at < window_elapses_at(x.lot.uk_date)
                    ),
                    default=None,
                )
                _record(out, "security", status="computed", holding_basis="relevant_securities")
                _record(
                    out,
                    "holding",
                    holding_basis="relevant_securities",
                    pool_units=units,
                    pool_cost_gbp=total,
                    pool_missing_lot_keys=sorted(a.lot_key for a in remaining if a.cost_left is None),
                    pool_reasons=why["holding"],
                    window_open_since=open_since,
                )
    print(f"  readings agree: {computed.get('date') == computed.get('time')}", file=out)
    return "computed"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--expect-computed",
        type=int,
        nargs="*",
        default=[],
        help="tax security ids that must come out computed (acceptance fixtures); exit 2 otherwise",
    )
    args = parser.parse_args()
    out = sys.stdout
    script = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    print(f"run {datetime.now(UTC).isoformat()}; script sha256 {script}", file=out)
    print(
        f"python {sys.version.split()[0]}; module {__spec__.name if __spec__ else __file__};"
        f" argv {json.dumps(sys.argv[1:])}",
        file=out,
    )
    broken = False
    outcomes: dict[int, str] = {}
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
            rows = conn.execute(
                """SELECT lot_key, direction, executed_at, uk_date, tax_year, units, amount_gbp, instrument_id, subject
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
            print(f"  withheld_all {result.withheld_all}; run-level dependencies:", file=out)
            for d in sorted(
                (d for d in result.dependencies if d.tax_security_id is None),
                key=lambda d: (d.reason, d.subject or "", d.lot_key or "", d.instrument_id or 0, d.detail or ""),
            ):
                _dependency(
                    out, d.reason, subject=d.subject, lot_key=d.lot_key, instrument_id=d.instrument_id, detail=d.detail
                )
            # Fund securities by instrument type, independent of the character today's code assigns; lots by the
            # same member mapping matching uses (an unestablished identity's member is its own security).
            for s in result.securities:
                if inputs.characters[s.tax_security_id].types != frozenset({ETF_TYPE_ID}):
                    continue
                members = set(s.instrument_ids)
                lots = [
                    Lot(
                        r[0],
                        r[1],
                        r[2],
                        r[3],
                        r[4],
                        r[5],
                        r[6],
                        frozenset(result.lot_classes[r[0]].reasons),
                        subject=r[8],
                    )
                    for r in rows
                    if r[7] in members
                ]
                try:
                    outcomes[s.tax_security_id] = _fund(out, inputs, result, s, lots, conn)
                except AssertionError as e:
                    print(f"  INVARIANT BROKEN: {e}", file=out)
                    broken = True
    print(f"\n[outcomes] {json.dumps(outcomes, sort_keys=True)}", file=out)
    if broken:
        sys.exit(1)
    missing = [i for i in args.expect_computed if outcomes.get(i) != "computed"]
    if missing:
        print(f"expected computed but not: {missing}", file=out)
        sys.exit(2)


if __name__ == "__main__":
    main()
