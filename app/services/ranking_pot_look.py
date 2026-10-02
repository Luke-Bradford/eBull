"""Ranking-pot-v1 looks: the §9.3 verdict at the 12- and 24-month endpoints (#2842 slice 6b).

Spec ``docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md`` §9.3, "The looks" paragraph. The step job
(``ranking_pot_step``) calls ``compute_due_looks`` after every committed step and ``reconcile_state`` at the start of
every fire; the rebalance job calls ``look_pending`` before scoring.

A look reads only append-only rows — the step rows through its endpoint, the decided snapshots and the state
events — never a book checkpoint, so it is reproducible from what was stored when the endpoint was stepped.

Writes: one ``ranking_pot_looks`` ``result`` row per (declaration, look) (``sql/448``), and the engine's
``winding_down`` state event a harm look or the last look requires, in a separate transaction.

Fixed here by construction (each closes a spec item; the PR lists them):

- **Endpoints.** T₀ = the first decided snapshot's target session; A_m = T₀ with its year advanced by m/12; E_m = the
  first NYSE session ≥ A_m.
- **T.** Per book: Σ over sessions before E of the stored session sum, then E's liquidation-charged sum, added in
  session order under ``ranking_pot_sim.CTX``, over Σ record counts through E. The shadow's session sums are
  recomputed from its stored records with ``ranking_pot_sim.session_sum``, the function that produced the controls'.
- **Paths, drawdown (r3-27..29).** Both paths start at 1.0 and end at their charged endpoint value; SPY's entry
  carries the first snapshot's half-spread, its exit the latest decided snapshot's at or before E (as of, r3-44).
- **Execution (condition 5).** The executed ledger is slice 5's: ``executed_lifecycles`` reads 0 until then, which
  can only withhold a pass, never grant one.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Any, Final, Literal
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.services import ranking_pot as pot
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_sim as sim
from app.services.market_calendar import us_market_status
from app.services.ranking_pot_policy import LOOK_MONTHS, RANKING_POT_POLICY_HASH

logger = logging.getLogger(__name__)

Conn = psycopg.Connection[Any]

#: §9.3 minimum evidence: lifecycles ≥ ``MIN_LIFECYCLES_PER_SLOT`` × N, mean occupancy ≥ ``MIN_OCCUPANCY``.
MIN_LIFECYCLES_PER_SLOT: Final = 2
MIN_OCCUPANCY: Final = Fraction(1, 2)
#: §9.3 condition 3: shadow drawdown ≤ ``DRAWDOWN_MULTIPLE`` × max(SPY drawdown, ``DRAWDOWN_FLOOR``).
DRAWDOWN_MULTIPLE: Final = Fraction(5, 4)
DRAWDOWN_FLOOR: Final = Fraction(1, 20)
#: §9.3 condition 5: "≥ 6 months" in ``executing``. The time is a sum of intervals, not a calendar span, so it is
#: fixed as half the mean Gregorian year: 365.2425 / 2 days = 182 days 53,676 s.
EXECUTION_MIN: Final = timedelta(days=182, seconds=53_676)
_NEW_YORK: Final = ZoneInfo("America/New_York")

Verdict = Literal["pass", "shadow_pass_execution_unproven", "not_passed", "unevaluable"]


# ---------------------------------------------------------------------------
# Pure: endpoints
# ---------------------------------------------------------------------------
def anniversary(t0: date, months: int) -> date:
    """A_m: ``t0`` with its year advanced by ``months / 12``; a date that does not exist (29 February) is 1 March."""
    if months <= 0 or months % 12:
        raise ValueError("looks are whole years")
    year = t0.year + months // 12
    try:
        return t0.replace(year=year)
    except ValueError:
        return date(year, 3, 1)


def endpoint(t0: date, months: int) -> date:
    """E_m: the first NYSE session on or after A_m."""
    d = anniversary(t0, months)
    while us_market_status(d) == "closed":
        d += timedelta(days=1)
    return d


# ---------------------------------------------------------------------------
# Pure: streaming the step rows into one look's facts
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class StepRow:
    session: date
    forced: bool
    shadow: Mapping[str, Any]
    #: The controls' columns this look reads: ``records`` and ``sum_return`` (or ``sum_return_charged`` at E).
    controls: Mapping[str, Any]
    spy: sim.Bar | None


def _records(rows: Sequence[Sequence[Any]], session: date) -> list[sim.PositionSession]:
    return [sim.PositionSession(int(r[0]), int(r[1]), session, Decimal(r[2]), Decimal(r[3])) for r in rows]


@dataclass
class LookFacts:
    """Everything §9.3 decides on, accumulated from the step rows T₀ … E in order."""

    t0: date
    endpoint: date
    k: int
    sessions: int = 0
    last_session: date | None = None
    endpoint_forced: bool = False
    forced_sessions: int = 0
    spy_invalid_sessions: int = 0
    shadow_sum: Decimal = Decimal(0)
    shadow_count: int = 0
    control_sum: list[Decimal] = field(default_factory=list)
    control_count: list[int] = field(default_factory=list)
    #: 1.0, then each session's NAV, the endpoint's charged.
    shadow_path: list[Decimal] = field(default_factory=lambda: [Decimal(1)])
    held_total: int = 0
    #: lifecycle → (instrument_id, invested): the start value of its entry-session record.
    invested: dict[int, tuple[int, Decimal]] = field(default_factory=dict)
    #: lifecycle → terminal value: proceeds when closed at a session ≤ E, else E's charged end value.
    terminal: dict[int, Decimal] = field(default_factory=dict)
    #: SPY's stored close per session, ``None`` where its bar is invalid.
    spy_closes: list[Decimal | None] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.control_sum = [Decimal(0)] * self.k
        self.control_count = [0] * self.k

    def add(self, row: StepRow) -> None:
        expected = self.t0 if self.last_session is None else sim.next_session(self.last_session)
        if row.session != expected:
            raise ValueError(f"step row {row.session} where {expected} was due: the rows are not contiguous from T0")
        if row.session > self.endpoint:
            raise ValueError("a step row after the endpoint")
        at_end = row.session == self.endpoint
        sh = row.shadow
        records = _records(sh["records"], row.session)
        valued = _records(sh["records_charged"], row.session) if at_end else records
        sums = row.controls["sum_return_charged" if at_end else "sum_return"]
        counts = row.controls["records"]
        if len(sums) != self.k or len(counts) != self.k:
            raise ValueError(f"{row.session}: control columns are not {self.k} wide")
        with localcontext(sim.CTX):
            self.shadow_sum += sim.session_sum(valued)
            self.shadow_count += len(records)
            for i in range(self.k):
                self.control_sum[i] += Decimal(sums[i])
                self.control_count[i] += int(counts[i])
        self.shadow_path.append(Decimal(sh["nav_charged" if at_end else "nav"]))
        self.held_total += int(sh["held"])
        for r in records:
            self.invested.setdefault(r.lifecycle, (r.instrument_id, r.start_value))
        for c in sh["closed"]:
            lifecycle, invested = int(c[1]), Decimal(c[7])
            if self.invested.get(lifecycle) != (int(c[0]), invested):
                raise ValueError(f"lifecycle {lifecycle}: closed invested {invested} is not its entry record's")
            self.terminal[lifecycle] = Decimal(c[8])
        if at_end:
            for r in valued:
                self.terminal.setdefault(r.lifecycle, r.end_value)
            self.endpoint_forced = row.forced
        spy = row.spy if row.spy is not None and sim.valid_bar(row.spy) else None
        spy_valid = spy is not None
        self.spy_closes.append(spy.close if spy is not None else None)
        self.forced_sessions += row.forced
        self.spy_invalid_sessions += not spy_valid
        self.last_session = row.session
        self.sessions += 1


def _t(total: Decimal, count: int) -> Decimal | None:
    if count == 0:
        return None
    with localcontext(sim.CTX):
        return total / count


def max_drawdown(path: Sequence[Decimal]) -> Decimal:
    """max over t of 1 − p_t / max_{u ≤ t} p_u."""
    worst, peak = Decimal(0), path[0]
    with localcontext(sim.CTX):
        for p in path:
            peak = max(peak, p)
            worst = max(worst, 1 - p / peak)
    return worst


def spy_path(closes: Sequence[Decimal | None], *, h0: Decimal, h_end: Decimal) -> list[Decimal] | None:
    """SPY's path (§9.3, "Paths"): ``None`` when T₀'s bar is invalid. An invalid later bar marks at the last close."""
    if not closes or closes[0] is None:
        return None
    with localcontext(sim.CTX):
        basis = closes[0] * (1 + h0)
        path, last = [Decimal(1)], closes[0]
        for i, c in enumerate(closes):
            last = c if c is not None else last
            path.append(last * (1 - h_end) / basis if i == len(closes) - 1 else last / basis)
    return path


def breadth_median(invested: Mapping[int, tuple[int, Decimal]], terminal: Mapping[int, Decimal]) -> Decimal | None:
    """Condition 4: the median over names of each name's summed lifecycle ln(terminal / invested)."""
    if set(invested) != set(terminal):
        raise ValueError("every lifecycle needs both an entry record and a terminal value")
    per_name: dict[int, Decimal] = {}
    with localcontext(sim.CTX):
        for lifecycle in sorted(invested):
            iid, cost = invested[lifecycle]
            end = terminal[lifecycle]
            if not (cost > 0 and end > 0):
                raise ValueError(f"lifecycle {lifecycle}: a non-positive value ({cost}, {end})")
            per_name[iid] = per_name.get(iid, Decimal(0)) + (end / cost).ln()
        values = sorted(per_name.values())
        if not values:
            return None
        mid = len(values) // 2
        return values[mid] if len(values) % 2 else (values[mid - 1] + values[mid]) / 2


# ---------------------------------------------------------------------------
# Pure: the verdict
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Execution:
    executing: timedelta
    lifecycles: int


@dataclass(frozen=True)
class Terms:
    n: int
    #: K: every control column must be exactly this wide.
    k: int
    per_look_alpha: Fraction
    harm_alpha: Fraction


@dataclass(frozen=True)
class LookResult:
    verdict: Verdict
    harm: bool
    reasons: tuple[str, ...]
    detail: dict[str, Any]


def _f(x: Decimal | Fraction | None) -> str | None:
    return None if x is None else str(x)


def evaluate(facts: LookFacts, terms: Terms, *, h0: Decimal, h_end: Decimal, execution: Execution) -> LookResult:
    """§9.3: unevaluable reasons, conditions 1–5, verdict and harm."""
    t_obs = _t(facts.shadow_sum, facts.shadow_count)
    t_controls = [_t(s, c) for s, c in zip(facts.control_sum, facts.control_count, strict=True)]
    p = sim.p_values(t_obs, t_controls)
    lifecycles = len(facts.invested)
    occupancy = Fraction(facts.held_total, facts.sessions * terms.n) if facts.sessions else Fraction(0)
    spy = spy_path(facts.spy_closes, h0=h0, h_end=h_end)

    reasons: list[str] = []
    if facts.endpoint_forced:
        reasons.append("endpoint_forced")
    if p == "unevaluable":
        reasons.append("t_obs_undefined")
    if lifecycles < MIN_LIFECYCLES_PER_SLOT * terms.n:
        reasons.append("lifecycles_below_minimum")
    if occupancy < MIN_OCCUPANCY:
        reasons.append("occupancy_below_minimum")
    if spy is None:
        reasons.append("spy_unavailable")

    shadow_end = facts.shadow_path[-1]
    shadow_dd = max_drawdown(facts.shadow_path)
    spy_end = None if spy is None else spy[-1]
    spy_dd = None if spy is None else max_drawdown(spy)
    median = breadth_median(facts.invested, facts.terminal)
    # A condition whose operands do not exist is ``None`` (not evaluable), never ``False``.
    conditions: dict[str, bool | None] = {
        "1_p_up": None if p == "unevaluable" else p.p_up <= terms.per_look_alpha,
        "2_beats_spy": None if spy_end is None else shadow_end >= spy_end,
        "3_drawdown": None
        if spy_dd is None
        else Fraction(shadow_dd) <= DRAWDOWN_MULTIPLE * max(Fraction(spy_dd), DRAWDOWN_FLOOR),
        "4_breadth": None if median is None else median > 0,
        "5_execution": execution.executing >= EXECUTION_MIN
        and execution.lifecycles >= MIN_LIFECYCLES_PER_SLOT * terms.n,
    }
    harm = p != "unevaluable" and p.p_down <= terms.harm_alpha
    verdict: Verdict
    if reasons:
        verdict = "unevaluable"
    elif all(conditions[c] is True for c in ("1_p_up", "2_beats_spy", "3_drawdown", "4_breadth")):
        verdict = "pass" if conditions["5_execution"] else "shadow_pass_execution_unproven"
    else:
        verdict = "not_passed"
    finite = [t for t in t_controls if t is not None]
    detail = {
        "t0": facts.t0.isoformat(),
        "endpoint": facts.endpoint.isoformat(),
        "t_obs": _f(t_obs),
        "p_up": None if p == "unevaluable" else _f(p.p_up),
        "p_down": None if p == "unevaluable" else _f(p.p_down),
        "controls": len(t_controls),
        "controls_without_t": len(t_controls) - len(finite),
        "sessions": facts.sessions,
        "forced_sessions": facts.forced_sessions,
        "spy_invalid_sessions": facts.spy_invalid_sessions,
        "lifecycles": lifecycles,
        "names": len({iid for iid, _ in facts.invested.values()}),
        "occupancy": _f(occupancy),
        "shadow_return": _f(shadow_end - 1),
        "spy_return": None if spy_end is None else _f(spy_end - 1),
        "shadow_drawdown": _f(shadow_dd),
        "spy_drawdown": _f(spy_dd),
        "breadth_median": _f(median),
        "h0": _f(h0),
        "h_end": _f(h_end),
        "executing_days": execution.executing / timedelta(days=1),
        "executed_lifecycles": execution.lifecycles,
        "per_look_alpha": _f(terms.per_look_alpha),
        "harm_alpha": _f(terms.harm_alpha),
        "conditions": conditions,
    }
    return LookResult(verdict, harm, tuple(reasons), detail)


def executing_time(events: Iterable[tuple[datetime, str]], start: datetime, end: datetime) -> timedelta:
    """Total time in ``executing`` within [start, end] from the ordered (at, to_state) events."""
    total = timedelta(0)
    since: datetime | None = None
    for at, to_state in events:
        if since is not None and to_state != "executing":
            total += max(timedelta(0), min(at, end) - max(since, start))
            since = None
        elif since is None and to_state == "executing":
            since = at
    if since is not None:
        total += max(timedelta(0), end - max(since, start))
    return total


def window_bounds(t0: date, end: date) -> tuple[datetime, datetime]:
    """The New York calendar days T₀ … E: 00:00 on T₀ to 00:00 on the day after E."""
    return (
        datetime.combine(t0, time(0), _NEW_YORK).astimezone(UTC),
        datetime.combine(end + timedelta(days=1), time(0), _NEW_YORK).astimezone(UTC),
    )


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------
def _dec(x: Fraction) -> Decimal:
    with localcontext(sim.CTX):
        return Decimal(x.numerator) / Decimal(x.denominator)


def first_target_session(conn: Conn, declaration_id: int) -> date | None:
    """T₀: the first decided snapshot's target session (the step job's ``_first_decided`` order)."""
    row = conn.execute(
        "SELECT target_session FROM ranking_pot_rebalance_attempts "
        "WHERE declaration_id = %s AND outcome = 'decided' ORDER BY attempt_id LIMIT 1",
        (declaration_id,),
    ).fetchone()
    return None if row is None else row[0]


def _spy_half_spreads(conn: Conn, declaration_id: int, through: date) -> list[tuple[date, Decimal]]:
    """Each decided snapshot's SPY half-spread with a target session ≤ ``through``, ascending."""
    rows = conn.execute(
        "SELECT target_session, snapshot -> 'spy' ->> 'bid', snapshot -> 'spy' ->> 'ask' "
        "FROM ranking_pot_rebalance_attempts WHERE declaration_id = %s AND outcome = 'decided' "
        "AND target_session <= %s ORDER BY target_session",
        (declaration_id, through),
    ).fetchall()
    out = []
    for target, bid, ask in rows:
        h = pot.half_spread(None if bid is None else Decimal(bid), None if ask is None else Decimal(ask))
        if h is None:
            raise rb.SnapshotIntegrityError(f"the decided snapshot for {target} has no valid SPY quote")
        out.append((target, _dec(h)))
    return out


def _step_rows(conn: Conn, declaration_id: int, *, t0: date, end: date, at_end: bool) -> Iterable[StepRow]:
    col = "sum_return_charged" if at_end else "sum_return"
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT session, forced, shadow, inputs -> 'spy' AS spy, controls -> 'records' AS records, "
            f"controls -> '{col}' AS sums "  # noqa: S608 — `col` is one of two literals above
            "FROM ranking_pot_steps WHERE declaration_id = %s AND session >= %s AND session "
            + ("= %s" if at_end else "< %s")
            + " ORDER BY session",
            (declaration_id, t0, end),
        )
        for r in cur:
            spy = None if r["spy"] is None else sim.Bar(*(Decimal(v) for v in r["spy"]))
            yield StepRow(r["session"], r["forced"], r["shadow"], {"records": r["records"], col: r["sums"]}, spy)


def executed_lifecycles(conn: Conn, declaration_id: int, *, start: datetime, end: datetime) -> int:
    """Condition 5's executed-book lifecycles. The executed ledger is slice 5's; until it lands this reads 0, which
    can only withhold a pass (spec §9.3, "The looks"). Slice 5 replaces this reader."""
    del conn, declaration_id, start, end
    return 0


def _execution(conn: Conn, declaration_id: int, *, t0: date, end: date) -> Execution:
    start, stop = window_bounds(t0, end)
    events = conn.execute(
        "SELECT at, to_state FROM ranking_pot_state_events WHERE declaration_id = %s AND at <= %s ORDER BY event_id",
        (declaration_id, stop),
    ).fetchall()
    return Execution(
        executing=executing_time([(r[0], r[1]) for r in events], start, stop),
        lifecycles=executed_lifecycles(conn, declaration_id, start=start, end=stop),
    )


def terms_of(decl: rb.PotDeclaration) -> Terms:
    t = decl.doc["terms"]
    return Terms(
        n=int(t["n"]),
        k=int(t["k_controls"]),
        per_look_alpha=Fraction(t["per_look_alpha"]),
        harm_alpha=Fraction(t["harm_alpha"]),
    )


def compute_look(conn: Conn, decl: rb.PotDeclaration, *, months: int, t0: date) -> LookResult:
    """One look from the stored rows (pure apart from the reads). The caller has checked E_m was stepped."""
    end = endpoint(t0, months)
    terms = terms_of(decl)
    facts = LookFacts(t0=t0, endpoint=end, k=terms.k)
    for row in _step_rows(conn, decl.declaration_id, t0=t0, end=end, at_end=False):
        facts.add(row)
    last = list(_step_rows(conn, decl.declaration_id, t0=t0, end=end, at_end=True))
    if len(last) != 1:
        raise rb.SnapshotIntegrityError(f"declaration {decl.declaration_id}: endpoint {end} is not stepped")
    facts.add(last[0])
    if facts.sessions != sessions_between(t0, end):
        raise rb.SnapshotIntegrityError(f"declaration {decl.declaration_id}: step rows {t0}..{end} are not contiguous")
    spreads = _spy_half_spreads(conn, decl.declaration_id, end)
    if not spreads or spreads[0][0] != t0:
        raise rb.SnapshotIntegrityError("the first decided snapshot does not target T0")
    result = evaluate(
        facts,
        terms,
        h0=spreads[0][1],
        h_end=spreads[-1][1],
        execution=_execution(conn, decl.declaration_id, t0=t0, end=end),
    )
    result.detail.update(look_months=months, anniversary=anniversary(t0, months).isoformat())
    return result


def sessions_between(t0: date, end: date) -> int:
    """NYSE sessions in [t0, end]."""
    count, d = 1, t0
    while d < end:
        d = sim.next_session(d)
        count += 1
    if d != end:
        raise ValueError(f"{end} is not a NYSE session on or after {t0}")
    return count


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class StoredLook:
    months: int
    endpoint: date
    verdict: str
    harm: bool


def _stored_looks(conn: Conn, declaration_id: int) -> dict[int, StoredLook]:
    rows = conn.execute(
        "SELECT look_months, endpoint_session, verdict, harm FROM ranking_pot_looks "
        "WHERE declaration_id = %s AND kind = 'result'",
        (declaration_id,),
    ).fetchall()
    return {int(r[0]): StoredLook(int(r[0]), r[1], str(r[2]), bool(r[3])) for r in rows}


def _lock(conn: Conn, declaration_id: int) -> None:
    """The step's lock order: ``ranking_pot_state_events`` SHARE before the first query (REPEATABLE READ), then the
    declaration row, which serialises a look against a step."""
    rb.begin_rebalance(conn)
    conn.execute(
        "SELECT 1 FROM ranking_pot_declarations WHERE declaration_id = %s FOR NO KEY UPDATE", (declaration_id,)
    )


def compute_due_looks(conn: Conn, decl: rb.PotDeclaration, *, as_of: datetime) -> list[str]:
    """Every look whose endpoint has been stepped and which has no ``result`` row, each in its own transaction.
    ``conn`` must be autocommit. Returns one note per look written, or a closing note when
    policy drift stops the pass (nothing is computed under drifted code)."""
    notes: list[str] = []
    for months in LOOK_MONTHS:
        with conn.transaction():
            _lock(conn, decl.declaration_id)
            t0 = first_target_session(conn, decl.declaration_id)
            if t0 is None or months in _stored_looks(conn, decl.declaration_id):
                continue
            end = endpoint(t0, months)
            row = conn.execute(
                "SELECT max(session), min(session) FILTER (WHERE wind_down_event_id IS NOT NULL) "
                "FROM ranking_pot_steps WHERE declaration_id = %s",
                (decl.declaration_id,),
            ).fetchone()
            last, wound = (None, None) if row is None else (row[0], row[1])
            # A wind-down applied before E liquidated the books inside the window: no look (spec §9.3).
            if last is None or last < end or (wound is not None and wound < end):
                continue
            if not rb._policy_ok(decl):
                # r3-92: the verdict runs only under the declared code. No raise here: the step that follows records
                # its own ``policy_drift`` refusal and fails the run, which a raise before it would pre-empt.
                notes.append(f"look {months}m not computed: policy drift")
                return notes
            result = compute_look(conn, decl, months=months, t0=t0)
            conn.execute(
                "INSERT INTO ranking_pot_looks (declaration_id, look_months, endpoint_session, kind, verdict, harm, "
                "reasons, detail, policy_hash, computed_at) VALUES (%s, %s, %s, 'result', %s, %s, %s, %s, %s, %s)",
                (
                    decl.declaration_id,
                    months,
                    end,
                    result.verdict,
                    result.harm,
                    list(result.reasons),
                    Jsonb(result.detail),
                    RANKING_POT_POLICY_HASH,
                    as_of,
                ),
            )
            notes.append(f"look {months}m at {end}: {result.verdict}{', harm' if result.harm else ''}")
    return notes


def wind_down_due(looks: Mapping[int, StoredLook]) -> Literal["harm", "completed_window"] | None:
    """The engine wind-down the stored looks require: ``harm`` from any harm look, else ``completed_window`` once the
    last look is stored."""
    if any(look.harm for look in looks.values()):
        return "harm"
    if LOOK_MONTHS[-1] in looks:
        return "completed_window"
    return None


def reconcile_state(conn: Conn, declaration_id: int) -> str | None:
    """Write the engine ``winding_down`` event the stored looks require, unless the trial already winds down or is
    complete. Idempotent; its own READ COMMITTED transaction (the state writer's lock order, ``sql/445``)."""
    with conn.transaction():
        looks = _stored_looks(conn, declaration_id)
        reason = wind_down_due(looks)
        if reason is None:
            return None
        state = _state(conn, declaration_id)
        if state in (None, "winding_down", "completed"):
            return None
        why = ", ".join(f"look {m}m {look.verdict}{' harm' if look.harm else ''}" for m, look in sorted(looks.items()))
        conn.execute(
            "INSERT INTO ranking_pot_state_events "
            "(declaration_id, from_state, to_state, wind_down_reason, reason, actor) "
            "VALUES (%s, %s, 'winding_down', %s, %s, 'engine')",
            (declaration_id, state, reason, f"§9.3: {why}"),
        )
        return f"winding_down ({reason})"


def _state(conn: Conn, declaration_id: int) -> str | None:
    row = conn.execute(
        "SELECT to_state FROM ranking_pot_state_events WHERE declaration_id = %s ORDER BY event_id DESC LIMIT 1",
        (declaration_id,),
    ).fetchone()
    return None if row is None else str(row[0])


def look_pending(conn: Conn, declaration_id: int, target: date) -> dict[str, Any] | None:
    """§9.3 ``look_pending``: the earliest look whose endpoint is before ``target`` with no ``result`` row, or a
    stored look whose wind-down event is not yet written. Both only ever clear."""
    t0 = first_target_session(conn, declaration_id)
    if t0 is None:
        return None
    stored = _stored_looks(conn, declaration_id)
    for months in LOOK_MONTHS:
        end = endpoint(t0, months)
        if end < target and months not in stored:
            return {"look_months": months, "endpoint": end.isoformat()}
    reason = wind_down_due(stored)
    if reason is not None and _state(conn, declaration_id) not in ("winding_down", "completed"):
        return {"wind_down_unwritten": reason}
    return None


__all__ = [
    "DRAWDOWN_FLOOR",
    "DRAWDOWN_MULTIPLE",
    "EXECUTION_MIN",
    "MIN_LIFECYCLES_PER_SLOT",
    "MIN_OCCUPANCY",
    "Execution",
    "LookFacts",
    "LookResult",
    "StepRow",
    "StoredLook",
    "Terms",
    "anniversary",
    "breadth_median",
    "compute_due_looks",
    "compute_look",
    "endpoint",
    "evaluate",
    "executed_lifecycles",
    "executing_time",
    "first_target_session",
    "look_pending",
    "max_drawdown",
    "reconcile_state",
    "sessions_between",
    "spy_path",
    "terms_of",
    "wind_down_due",
    "window_bounds",
]
