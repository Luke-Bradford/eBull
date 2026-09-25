"""#3381 slice 3: the daily perishables recorder — rates, eligibility and what-if open costs.

eToro serves none of these as history, so the recording clock is the dataset. Spec (the contract every rule
here implements): ``docs/proposals/etl/2026-09-25-3381-perishables-recorder.md``; schema
``sql/426_etoro_perishables.sql``.

One run = one ``etoro_perishable_snapshots`` row, failed runs included. Every HTTP request is stored with its
body as served; the observation rows are thin projections of those bodies. Everything a run collected is
written in ONE transaction at the end, so a failed run still keeps its evidence.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Final, Protocol

import httpx
import psycopg

from app.providers.broker import BrokerWhatIfOrder, PreflightTransaction, SettlementType
from app.providers.implementations.etoro_broker import what_if_request_body
from app.providers.implementations.etoro_perishables import (
    ELIGIBILITY_BATCH_SIZE,
    RATES_BATCH_SIZE,
    RawResponse,
)
from app.services.sync_orchestrator.layer_types import FailureCategory, LayerRefreshFailed

logger = logging.getLogger(__name__)

RECORDER_VERSION: Final = "1"
WHATIF_PANEL_RULE: Final = "cohort-long50-short50-sticky-v1"
WHATIF_PANEL_TOP_N_PER_SIDE: Final = 50
MAX_WHATIF_PANEL: Final = 250
PANEL_SOURCE_MAX_AGE: Final = timedelta(hours=48)
WHATIF_TICKET_USD: Final = Decimal("1000")
MAX_UNIVERSE: Final = 30_000
MAX_CONSECUTIVE_ERRORS: Final = 10
#: Total order for the long x1 settlement (spec §"Local projections"). A settlement outside it is malformed.
SETTLEMENT_ORDER: Final[tuple[SettlementType, ...]] = ("real", "cfd", "realFutures", "marginTrade")
#: Credential-wide refusals: any one stops the whole run.
_SYSTEMIC_STATUSES: Final = frozenset({401, 403})

STATUS_COMPLETE: Final = "complete"
STATUS_PARTIAL: Final = "partial"
STATUS_FAILED: Final = "failed"

PHASE_ELIGIBILITY: Final = "eligibility"
PHASE_WHATIF: Final = "whatif"
PHASE_RATES: Final = "rates"

ARM_AVAILABLE: Final = "available"
ARM_POTENTIAL: Final = "potential"
ARM_ABSENT: Final = "absent"


class PerishablesSource(Protocol):
    def get_rates(self, instrument_ids: list[int]) -> RawResponse: ...

    def post_eligibility(self, instrument_ids: list[int]) -> RawResponse: ...

    def post_what_if(self, order: BrokerWhatIfOrder) -> RawResponse: ...


class PerishableSnapshotPartial(LayerRefreshFailed):
    """The snapshot committed as ``partial``: some requests errored. Raised so the run is not a success.

    Carries the category of its errored requests (``partial_category``) so ``classify_exception`` reports what
    actually went wrong instead of flattening it to ``INTERNAL_ERROR``.
    """


class PerishableSnapshotRefused(LayerRefreshFailed):
    """A systemic condition stopped the run (spec §Failure semantics), with the triggering request's category."""


class EnvelopeViolation(ValueError):
    """A 2xx body that breaks the response contract: the request is ``error``, its raw kept."""


# ---------------------------------------------------------------------------
# Pure parsing (spec §"Local projections" and §"Envelope rules")
# ---------------------------------------------------------------------------


def number(value: object) -> Decimal | None:
    """A JSON number or numeric string as a finite Decimal; anything else (bool included) → None."""
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        return None
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        return None
    return result if result.is_finite() else None


def timestamp(value: object) -> datetime | None:
    """A timezone-aware ISO-8601 string as a datetime; anything else → None."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _boolean(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _settlement(value: object) -> SettlementType | None:
    for settlement in SETTLEMENT_ORDER:
        if settlement == value:
            return settlement
    return None


def _instrument_id(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


@dataclass(frozen=True)
class ArmCapacity:
    long_x1: str | None
    short_x1: str | None
    long_x1_settlement: SettlementType | None
    max_short_leverage: int | None


def arm_capacity(configs: object) -> ArmCapacity:
    """Project ``leverageConfigs`` onto the x1 arms. Any malformed config → the affected status is None."""
    if not isinstance(configs, list):
        return ArmCapacity(None, None, None, None)
    long_states: list[tuple[SettlementType, bool]] = []  # (settlement, is_potential) of long x1 configs
    short_states: list[bool] = []  # is_potential of cfd short x1 configs
    short_leverages: list[int] = []
    long_malformed = short_malformed = max_short_malformed = False
    for config in configs:
        fields = config if isinstance(config, dict) else {}
        direction = fields.get("direction") if fields.get("direction") in ("long", "short") else None
        settlement = _settlement(fields.get("settlementType"))
        raw_leverages = fields.get("leverageValues")
        leverages = (
            raw_leverages
            if isinstance(raw_leverages, list)
            and all(isinstance(v, int) and not isinstance(v, bool) for v in raw_leverages)
            else None
        )
        potential = _boolean(fields.get("isPotential"))
        if direction is None or settlement is None or leverages is None or potential is None:
            # Unreadable: it makes unknown only the projections it could belong to (spec #28 — a config
            # of that direction with 1 in its leverage values; short reads cfd only).
            x1_possible = leverages is None or 1 in leverages
            cfd_possible = settlement is None or settlement == "cfd"
            if direction != "short" and x1_possible:
                long_malformed = True
            if direction != "long" and cfd_possible:
                short_malformed = short_malformed or x1_possible
                max_short_malformed = max_short_malformed or potential is not True
            continue
        if direction == "long":
            if 1 in leverages:
                long_states.append((settlement, potential))
        elif settlement == "cfd":
            if 1 in leverages:
                short_states.append(potential)
            if not potential:
                short_leverages.extend(leverages)

    def status(states: Sequence[bool], malformed: bool) -> str | None:
        if malformed:
            return None
        if any(not potential for potential in states):
            return ARM_AVAILABLE
        return ARM_POTENTIAL if states else ARM_ABSENT

    long_x1 = status([potential for _, potential in long_states], long_malformed)
    chosen: SettlementType | None = None
    if long_x1 == ARM_AVAILABLE:
        available = {s for s, potential in long_states if not potential}
        chosen = next(s for s in SETTLEMENT_ORDER if s in available)
    short_x1 = status(short_states, short_malformed)
    max_short = None if max_short_malformed or not short_leverages else max(short_leverages)
    return ArmCapacity(long_x1, short_x1, chosen, max_short)


@dataclass(frozen=True)
class EligibilityRow:
    instrument_id: int
    answer: str  # found | not_found
    allow_open_position: bool | None = None
    allow_close_position: bool | None = None
    min_position_exposure: Decimal | None = None
    max_units_per_order: Decimal | None = None
    capacity: ArmCapacity = ArmCapacity(None, None, None, None)


def parse_eligibility(requested: Sequence[int], body: object) -> list[EligibilityRow]:
    """Found + not-found rows for one eligibility response; an omitted id gets no row."""
    if not isinstance(body, dict):
        raise EnvelopeViolation("eligibility body is not an object")
    found, not_found = body.get("eligibilities"), body.get("notFoundInstrumentIds")
    if not isinstance(found, list) or not isinstance(not_found, list):
        raise EnvelopeViolation("eligibilities / notFoundInstrumentIds are not arrays")
    wanted = set(requested)
    seen: set[int] = set()
    rows: list[EligibilityRow] = []

    def claim(value: object) -> int:
        instrument_id = _instrument_id(value)
        if instrument_id is None or instrument_id not in wanted or instrument_id in seen:
            raise EnvelopeViolation(f"eligibility answered an unrequested, duplicate or invalid id {value!r}")
        seen.add(instrument_id)
        return instrument_id

    for item in found:
        if not isinstance(item, dict):
            raise EnvelopeViolation("an eligibilities entry is not an object")
        rows.append(
            EligibilityRow(
                instrument_id=claim(item.get("instrumentId")),
                answer="found",
                allow_open_position=_boolean(item.get("allowOpenPosition")),
                allow_close_position=_boolean(item.get("allowClosePosition")),
                min_position_exposure=number(item.get("minPositionExposure")),
                max_units_per_order=number(item.get("maxUnitsPerOrder")),
                capacity=arm_capacity(item.get("leverageConfigs")),
            )
        )
    rows.extend(EligibilityRow(instrument_id=claim(value), answer="not_found") for value in not_found)
    return rows


@dataclass(frozen=True)
class RateRow:
    instrument_id: int
    quote_at: datetime | None
    bid: Decimal | None
    ask: Decimal | None
    last_execution: Decimal | None
    conversion_rate_bid: Decimal | None
    conversion_rate_ask: Decimal | None


def parse_rates(requested: Sequence[int], body: object) -> list[RateRow]:
    """One row per identified rates entry. An entry with no usable ``instrumentID`` stays in the raw only."""
    if not isinstance(body, dict) or not isinstance(body.get("rates"), list):
        raise EnvelopeViolation("rates body is not an object carrying a `rates` array")
    wanted = set(requested)
    rows: list[RateRow] = []
    seen: set[int] = set()
    for item in body["rates"]:
        instrument_id = _instrument_id(item.get("instrumentID")) if isinstance(item, dict) else None
        if instrument_id is None:
            continue
        if instrument_id not in wanted or instrument_id in seen:
            raise EnvelopeViolation(f"rates answered an unrequested or duplicate id {instrument_id}")
        seen.add(instrument_id)
        assert isinstance(item, dict)
        rows.append(
            RateRow(
                instrument_id=instrument_id,
                quote_at=timestamp(item.get("date")),
                bid=number(item.get("bid")),
                ask=number(item.get("ask")),
                last_execution=number(item.get("lastExecution")),
                conversion_rate_bid=number(item.get("conversionRateBid")),
                conversion_rate_ask=number(item.get("conversionRateAsk")),
            )
        )
    return rows


def parse_what_if(requested: int, body: object) -> datetime | None:
    """Check one what-if body's identity and shape; return its ``lastUpdated`` (None if unparseable)."""
    if not isinstance(body, dict) or not isinstance(body.get("costs"), list):
        raise EnvelopeViolation("what-if body is not an object carrying a `costs` array")
    if _instrument_id(body.get("instrumentId")) != requested:
        raise EnvelopeViolation(f"what-if answered instrument {body.get('instrumentId')!r}, not {requested}")
    return timestamp(body.get("lastUpdated"))


# ---------------------------------------------------------------------------
# Arm planning (spec §"Arms")
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ArmDecision:
    instrument_id: int
    arm: str  # long | short
    outcome: str | None  # None = planned (becomes ok / error / unattempted)
    #: ``seq`` of the eligibility request whose answer decided the arm (resolved to its request_id at write);
    #: None when there was no answer.
    eligibility_seq: int | None
    order: BrokerWhatIfOrder | None = None


def decide_arms(instrument_id: int, answer: tuple[EligibilityRow, int] | None) -> tuple[ArmDecision, ArmDecision]:
    """Long then short for one panel instrument, from THIS run's eligibility answer (or its absence)."""
    if answer is None:
        return (
            ArmDecision(instrument_id, "long", "undecided", None),
            ArmDecision(instrument_id, "short", "undecided", None),
        )
    row, seq = answer
    if row.answer == "not_found" or row.allow_open_position is False:
        return (
            ArmDecision(instrument_id, "long", "not_eligible", seq),
            ArmDecision(instrument_id, "short", "not_eligible", seq),
        )

    def one(
        arm: str, status: str | None, transaction: PreflightTransaction, settlement: SettlementType | None
    ) -> ArmDecision:
        if row.allow_open_position is None or status is None:
            return ArmDecision(instrument_id, arm, "undecided", seq)
        if status != ARM_AVAILABLE or settlement is None:
            return ArmDecision(instrument_id, arm, "not_offered", seq)
        order = BrokerWhatIfOrder(
            instrument_id=instrument_id,
            transaction=transaction,
            settlement_type=settlement,
            amount=WHATIF_TICKET_USD,
            leverage=1,
        )
        return ArmDecision(instrument_id, arm, None, seq, order)

    capacity = row.capacity
    return (
        one("long", capacity.long_x1, "buy", capacity.long_x1_settlement),
        one("short", capacity.short_x1, "sellShort", "cfd"),
    )


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------


@dataclass
class Request:
    phase: str
    seq: int
    instrument_ids: tuple[int, ...]
    request_body: object
    observed_at: datetime
    outcome: str  # ok | error
    http_status: int | None
    raw: object
    request_id: int | None = None  # assigned at write


def _decode_text(response: httpx.Response) -> object:
    try:
        return response.json()
    except ValueError:
        return response.text


def execute(
    phase: str,
    seq: int,
    instrument_ids: Sequence[int],
    request_body: object,
    call: Callable[[], RawResponse],
    parse: Callable[[object], object],
    clock: Callable[[], datetime],
) -> tuple[Request, object | None]:
    """Make one request and classify it. Returns the request row and the parsed value (None on error)."""
    ids = tuple(instrument_ids)
    try:
        response = call()
    except httpx.HTTPStatusError as exc:  # retries exhausted on 429/5xx
        return Request(
            phase, seq, ids, request_body, clock(), "error", exc.response.status_code, _decode_text(exc.response)
        ), None
    except httpx.RequestError as exc:
        return Request(phase, seq, ids, request_body, clock(), "error", None, f"{type(exc).__name__}: {exc}"), None
    request = Request(phase, seq, ids, request_body, response.observed_at, "error", response.status, response.body)
    if not 200 <= response.status <= 299:
        return request, None
    try:
        parsed = parse(response.body)
    except EnvelopeViolation as exc:
        logger.warning("perishables %s request %d: %s", phase, seq, exc)
        return request, None
    request.outcome = "ok"
    return request, parsed


def failure_category(request: Request) -> FailureCategory:
    """What an errored request means for the operator (``classify_exception``'s status mapping)."""
    status = request.http_status
    if status is None or 500 <= status <= 599:
        return FailureCategory.SOURCE_DOWN
    if status in _SYSTEMIC_STATUSES:
        return FailureCategory.AUTH_EXPIRED
    if status == 429:
        return FailureCategory.RATE_LIMITED
    if 200 <= status <= 299:
        return FailureCategory.SCHEMA_DRIFT  # a 2xx that broke the envelope contract
    return FailureCategory.INTERNAL_ERROR


#: Most operator-actionable first: a failure over several errored requests reports the first category present.
_CATEGORY_PRECEDENCE: Final = (
    FailureCategory.AUTH_EXPIRED,
    FailureCategory.SCHEMA_DRIFT,
    FailureCategory.INTERNAL_ERROR,
    FailureCategory.RATE_LIMITED,
    FailureCategory.SOURCE_DOWN,
)


def partial_category(errored: Sequence[Request]) -> FailureCategory:
    categories = {failure_category(r) for r in errored if r.outcome == "error"}
    return next((c for c in _CATEGORY_PRECEDENCE if c in categories), FailureCategory.INTERNAL_ERROR)


@dataclass
class _Phase:
    expected: int
    requests: list[Request] = field(default_factory=list)
    consecutive_errors: int = 0

    @property
    def ok(self) -> int:
        return sum(r.outcome == "ok" for r in self.requests)

    @property
    def errored(self) -> int:
        return sum(r.outcome == "error" for r in self.requests)

    def add(self, request: Request) -> None:
        """Record one request and apply the per-request systemic rules."""
        self.requests.append(request)
        if request.http_status in _SYSTEMIC_STATUSES:
            raise PerishableSnapshotRefused(
                FailureCategory.AUTH_EXPIRED, f"{request.phase} request {request.seq} answered {request.http_status}"
            )
        self.consecutive_errors = self.consecutive_errors + 1 if request.outcome == "error" else 0
        if self.consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            raise PerishableSnapshotRefused(
                failure_category(request), f"{self.consecutive_errors} consecutive {request.phase} requests errored"
            )

    def finish(self, phase: str) -> None:
        if self.requests and self.ok == 0:
            raise PerishableSnapshotRefused(
                partial_category(self.requests), f"no {phase} request succeeded out of {len(self.requests)}"
            )


# ---------------------------------------------------------------------------
# Universe and panel
# ---------------------------------------------------------------------------

_UNIVERSE_SQL: Final = """
SELECT instrument_id FROM etoro_perishable_universe
UNION
SELECT instrument_id FROM instruments WHERE is_tradable
ORDER BY 1
"""

_PANEL_LEDGER_SQL: Final = "SELECT instrument_id FROM etoro_whatif_panel ORDER BY first_snapshot_id, instrument_id"

_PANEL_SOURCE_SQL: Final = """
SELECT snapshot_id FROM etoro_investor_snapshots
WHERE status IN ('complete', 'partial') AND finished_at >= %(cutoff)s
ORDER BY finished_at DESC, snapshot_id DESC
LIMIT 1
"""

# Distinct cohort holders per (instrument, side) among tradable instruments, top N per side.
_PANEL_CANDIDATES_SQL: Final = """
WITH holders AS (
    SELECT p.instrument_id, p.is_buy, count(DISTINCT p.cid)::int AS holders
    FROM etoro_investor_positions p
    JOIN instruments i ON i.instrument_id = p.instrument_id AND i.is_tradable
    WHERE p.snapshot_id = %(source)s
    GROUP BY p.instrument_id, p.is_buy
), ranked AS (
    SELECT instrument_id, is_buy, holders,
           row_number() OVER (PARTITION BY is_buy ORDER BY holders DESC, instrument_id) AS side_rank
    FROM holders
)
SELECT instrument_id,
       max(holders) FILTER (WHERE is_buy) AS long_holders,
       max(holders) FILTER (WHERE NOT is_buy) AS short_holders,
       min(side_rank) AS best_rank
FROM ranked
WHERE side_rank <= %(top_n)s
GROUP BY instrument_id
ORDER BY best_rank, instrument_id
"""


@dataclass(frozen=True)
class PanelAddition:
    instrument_id: int
    long_holders: int | None
    short_holders: int | None


@dataclass
class _Plan:
    universe: tuple[int, ...] = ()
    panel_source_id: int | None = None
    panel: tuple[int, ...] = ()
    additions: tuple[PanelAddition, ...] = ()
    refused: int = 0


def _read_plan(conn: psycopg.Connection[Any], started_at: datetime) -> _Plan:
    with conn.transaction():
        universe = tuple(int(r[0]) for r in conn.execute(_UNIVERSE_SQL).fetchall())
        ledger = [int(r[0]) for r in conn.execute(_PANEL_LEDGER_SQL).fetchall()]
        source = conn.execute(_PANEL_SOURCE_SQL, {"cutoff": started_at - PANEL_SOURCE_MAX_AGE}).fetchone()
        candidates = (
            conn.execute(_PANEL_CANDIDATES_SQL, {"source": source[0], "top_n": WHATIF_PANEL_TOP_N_PER_SIDE}).fetchall()
            if source
            else []
        )
    in_ledger = set(ledger)
    new = [PanelAddition(int(r[0]), r[1], r[2]) for r in candidates if int(r[0]) not in in_ledger]
    room = max(MAX_WHATIF_PANEL - len(ledger), 0)
    additions = tuple(new[:room])
    return _Plan(
        universe=universe,
        panel_source_id=int(source[0]) if source else None,
        panel=(*ledger, *(a.instrument_id for a in additions)),
        additions=additions,
        refused=len(new) - len(additions),
    )


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------


@dataclass
class _Run:
    """Everything collected so far — written whole whatever the outcome."""

    plan: _Plan | None = None
    eligibility: _Phase | None = None
    whatif: _Phase | None = None
    rates: _Phase | None = None
    eligibility_rows: list[tuple[EligibilityRow, Request]] = field(default_factory=list)
    rate_rows: list[tuple[RateRow, Request]] = field(default_factory=list)
    arms: list[ArmDecision] = field(default_factory=list)
    # Planned arm -> its request and lastUpdated, once attempted.
    arm_results: dict[tuple[int, str], tuple[Request, datetime | None]] = field(default_factory=dict)


@dataclass(frozen=True)
class PerishablesSnapshotResult:
    snapshot_id: int
    status: str
    universe_size: int
    eligibility_rows: int
    rate_rows: int
    whatif_ok: int

    @property
    def row_count(self) -> int:
        return self.eligibility_rows + self.rate_rows + self.whatif_ok


def _batches(ids: Sequence[int], size: int) -> list[list[int]]:
    return [list(ids[i : i + size]) for i in range(0, len(ids), size)]


def _collect(
    conn: psycopg.Connection[Any],
    source: PerishablesSource,
    run: _Run,
    started_at: datetime,
    clock: Callable[[], datetime],
) -> None:
    plan = run.plan = _read_plan(conn, started_at)
    if not plan.universe:
        raise PerishableSnapshotRefused(FailureCategory.DATA_GAP, "the universe is empty")
    if len(plan.universe) > MAX_UNIVERSE:
        raise PerishableSnapshotRefused(
            FailureCategory.INTERNAL_ERROR, f"universe {len(plan.universe)} exceeds MAX_UNIVERSE {MAX_UNIVERSE}"
        )

    # Eligibility.
    batches = _batches(plan.universe, ELIGIBILITY_BATCH_SIZE)
    phase = run.eligibility = _Phase(len(batches))
    answers: dict[int, tuple[EligibilityRow, int]] = {}
    for seq, ids in enumerate(batches):
        request, parsed = execute(
            PHASE_ELIGIBILITY,
            seq,
            ids,
            {"instrumentIds": ids, "currency": "USD"},
            lambda ids=ids: source.post_eligibility(ids),
            lambda body, ids=ids: parse_eligibility(ids, body),
            clock,
        )
        if isinstance(parsed, list):
            run.eligibility_rows.extend((row, request) for row in parsed)
            # request_id is assigned at write, so an answer carries its request's seq until then.
            answers.update({row.instrument_id: (row, seq) for row in parsed})
        phase.add(request)
    phase.finish(PHASE_ELIGIBILITY)

    # What-if: decide every panel arm from this run's eligibility, then request the planned ones.
    for instrument_id in plan.panel:
        run.arms.extend(decide_arms(instrument_id, answers.get(instrument_id)))
    planned = [arm for arm in run.arms if arm.outcome is None]
    phase = run.whatif = _Phase(len(planned))
    for seq, arm in enumerate(planned):
        assert arm.order is not None
        order = arm.order
        request, parsed = execute(
            PHASE_WHATIF,
            seq,
            [arm.instrument_id],
            what_if_request_body(order),
            lambda order=order: source.post_what_if(order),
            lambda body, iid=arm.instrument_id: parse_what_if(iid, body),
            clock,
        )
        run.arm_results[(arm.instrument_id, arm.arm)] = (request, parsed if isinstance(parsed, datetime) else None)
        phase.add(request)
    phase.finish(PHASE_WHATIF)

    # Rates.
    batches = _batches(plan.universe, RATES_BATCH_SIZE)
    phase = run.rates = _Phase(len(batches))
    for seq, ids in enumerate(batches):
        request, parsed = execute(
            PHASE_RATES,
            seq,
            ids,
            {"instrumentIds": ids},
            lambda ids=ids: source.get_rates(ids),
            lambda body, ids=ids: parse_rates(ids, body),
            clock,
        )
        if isinstance(parsed, list):
            run.rate_rows.extend((row, request) for row in parsed)
        phase.add(request)
    phase.finish(PHASE_RATES)


def record_perishables_snapshot(
    conn: psycopg.Connection[Any],
    source: PerishablesSource,
    *,
    request_params: Mapping[str, object],
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> PerishablesSnapshotResult:
    """Collect and append one perishables snapshot (spec §Failure semantics).

    Any abort commits a ``failed`` header with everything collected so far, then re-raises the ORIGINAL
    exception so ``classify_exception`` sees its type. A ``partial`` snapshot commits, then raises
    ``PerishableSnapshotPartial`` outside the failure path, so no second header is written.
    """
    started_at = clock()
    params = {
        **request_params,
        "whatif_panel_rule": WHATIF_PANEL_RULE,
        "whatif_ticket_usd": str(WHATIF_TICKET_USD),
        "max_whatif_panel": MAX_WHATIF_PANEL,
        "panel_source_max_age_h": PANEL_SOURCE_MAX_AGE.total_seconds() / 3600,
        "max_universe": MAX_UNIVERSE,
        "max_consecutive_errors": MAX_CONSECUTIVE_ERRORS,
    }
    run = _Run()
    try:
        _collect(conn, source, run, started_at, clock)
        errored = sum(p.errored for p in (run.eligibility, run.whatif, run.rates) if p is not None)
        status = STATUS_PARTIAL if errored else STATUS_COMPLETE
        snapshot_id = _write(conn, run, started_at, clock(), status, params, None)
    except Exception as exc:
        _record_failure(conn, run, started_at, clock(), params, exc)
        raise
    result = PerishablesSnapshotResult(
        snapshot_id=snapshot_id,
        status=status,
        universe_size=len(run.plan.universe) if run.plan else 0,
        eligibility_rows=len(run.eligibility_rows),
        rate_rows=len(run.rate_rows),
        whatif_ok=run.whatif.ok if run.whatif else 0,
    )
    if status == STATUS_PARTIAL:
        errored_requests = [
            r
            for p in (run.eligibility, run.whatif, run.rates)
            if p is not None
            for r in p.requests
            if r.outcome == "error"
        ]
        raise PerishableSnapshotPartial(
            partial_category(errored_requests),
            f"perishables snapshot {snapshot_id} committed partial: {errored} requests errored",
        )
    return result


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

_INSERT_SNAPSHOT_SQL: Final = """
INSERT INTO etoro_perishable_snapshots (
    started_at, finished_at, status, recorder_version, request_params, universe_size, whatif_panel_source_id,
    whatif_panel_added, whatif_panel_refused, eligibility_expected, eligibility_ok, eligibility_errored,
    whatif_expected, whatif_ok, whatif_errored, rates_expected, rates_ok, rates_errored, error
) VALUES (
    %(started_at)s, %(finished_at)s, %(status)s, %(recorder_version)s, %(request_params)s::jsonb, %(universe_size)s,
    %(panel_source_id)s, %(panel_added)s, %(panel_refused)s, %(eligibility_expected)s, %(eligibility_ok)s,
    %(eligibility_errored)s, %(whatif_expected)s, %(whatif_ok)s, %(whatif_errored)s, %(rates_expected)s,
    %(rates_ok)s, %(rates_errored)s, %(error)s
)
RETURNING snapshot_id
"""

_INSERT_UNIVERSE_SQL: Final = """
INSERT INTO etoro_perishable_universe (instrument_id, first_snapshot_id)
SELECT unnest(%(ids)s::bigint[]), %(snapshot_id)s
ON CONFLICT (instrument_id) DO NOTHING
"""

_INSERT_PANEL_SQL: Final = """
INSERT INTO etoro_whatif_panel (instrument_id, first_snapshot_id, source_investor_snapshot_id, long_holders,
                                short_holders)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (instrument_id) DO NOTHING
"""

_REQUEST_IDS_SQL: Final = "SELECT nextval('etoro_perishable_requests_request_id_seq') FROM generate_series(1, %s)"

_COPY_REQUESTS_SQL: Final = (
    "COPY etoro_perishable_requests (request_id, snapshot_id, phase, seq, instrument_ids, request_body, observed_at, "
    "outcome, http_status, raw) FROM STDIN"
)
_COPY_ELIGIBILITY_SQL: Final = (
    "COPY etoro_eligibility_observations (snapshot_id, instrument_id, request_id, observed_at, answer, "
    "allow_open_position, allow_close_position, min_position_exposure, max_units_per_order, long_x1, short_x1, "
    "long_x1_settlement, max_short_leverage) FROM STDIN"
)
_COPY_RATES_SQL: Final = (
    "COPY etoro_rate_observations (snapshot_id, instrument_id, request_id, observed_at, quote_at, bid, ask, "
    "last_execution, conversion_rate_bid, conversion_rate_ask) FROM STDIN"
)
_COPY_WHATIF_SQL: Final = (
    "COPY etoro_whatif_observations (snapshot_id, instrument_id, arm, outcome, eligibility_request_id, request_id, "
    "transaction, settlement_type, amount_usd, leverage, last_updated) FROM STDIN"
)


def _json(value: object) -> str:
    # allow_nan=False: a non-JSON value refuses the write instead of storing an unparseable raw.
    return json.dumps(value, allow_nan=False, sort_keys=True, default=str)


def _counters(phase: _Phase | None) -> tuple[int | None, int | None, int | None]:
    return (None, None, None) if phase is None else (phase.expected, phase.ok, phase.errored)


def _write(
    conn: psycopg.Connection[Any],
    run: _Run,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    params: Mapping[str, object],
    error: str | None,
) -> int:
    plan = run.plan
    phases = [p for p in (run.eligibility, run.whatif, run.rates) if p is not None]
    requests = [r for p in phases for r in p.requests]
    elig, whatif, rates = _counters(run.eligibility), _counters(run.whatif), _counters(run.rates)
    with conn.transaction():
        row = conn.execute(
            _INSERT_SNAPSHOT_SQL,
            {
                "started_at": started_at,
                "finished_at": finished_at,
                "status": status,
                "recorder_version": RECORDER_VERSION,
                "request_params": _json(params),
                "universe_size": len(plan.universe) if plan else None,
                "panel_source_id": plan.panel_source_id if plan else None,
                "panel_added": len(plan.additions) if plan else None,
                "panel_refused": plan.refused if plan else None,
                "eligibility_expected": elig[0],
                "eligibility_ok": elig[1],
                "eligibility_errored": elig[2],
                "whatif_expected": whatif[0],
                "whatif_ok": whatif[1],
                "whatif_errored": whatif[2],
                "rates_expected": rates[0],
                "rates_ok": rates[1],
                "rates_errored": rates[2],
                "error": error,
            },
        ).fetchone()
        assert row is not None
        snapshot_id = int(row[0])
        if plan is not None:
            conn.execute(_INSERT_UNIVERSE_SQL, {"ids": list(plan.universe), "snapshot_id": snapshot_id})
            if plan.additions:
                assert plan.panel_source_id is not None
                with conn.cursor() as cur:
                    cur.executemany(
                        _INSERT_PANEL_SQL,
                        [
                            (a.instrument_id, snapshot_id, plan.panel_source_id, a.long_holders, a.short_holders)
                            for a in plan.additions
                        ],
                    )
        if requests:
            ids = [int(r[0]) for r in conn.execute(_REQUEST_IDS_SQL, (len(requests),)).fetchall()]
            for request, request_id in zip(requests, ids, strict=True):
                request.request_id = request_id
        # Eligibility answers were keyed by seq during collection; resolve them to request ids now.
        elig_ids = {r.seq: r.request_id for r in run.eligibility.requests} if run.eligibility else {}
        with conn.cursor() as cur:
            if requests:
                with cur.copy(_COPY_REQUESTS_SQL) as copy:
                    for r in requests:
                        copy.write_row(
                            (
                                r.request_id,
                                snapshot_id,
                                r.phase,
                                r.seq,
                                list(r.instrument_ids),
                                _json(r.request_body),
                                r.observed_at,
                                r.outcome,
                                r.http_status,
                                _json(r.raw),
                            )
                        )
            if run.eligibility_rows:
                with cur.copy(_COPY_ELIGIBILITY_SQL) as copy:
                    for e, r in run.eligibility_rows:
                        c = e.capacity
                        copy.write_row(
                            (
                                snapshot_id,
                                e.instrument_id,
                                r.request_id,
                                r.observed_at,
                                e.answer,
                                e.allow_open_position,
                                e.allow_close_position,
                                e.min_position_exposure,
                                e.max_units_per_order,
                                c.long_x1,
                                c.short_x1,
                                c.long_x1_settlement,
                                c.max_short_leverage,
                            )
                        )
            if run.rate_rows:
                with cur.copy(_COPY_RATES_SQL) as copy:
                    for q, r in run.rate_rows:
                        copy.write_row(
                            (
                                snapshot_id,
                                q.instrument_id,
                                r.request_id,
                                r.observed_at,
                                q.quote_at,
                                q.bid,
                                q.ask,
                                q.last_execution,
                                q.conversion_rate_bid,
                                q.conversion_rate_ask,
                            )
                        )
            if run.arms:
                with cur.copy(_COPY_WHATIF_SQL) as copy:
                    for arm in run.arms:
                        copy.write_row(_whatif_row(snapshot_id, arm, run, elig_ids))
    return snapshot_id


def _whatif_row(
    snapshot_id: int, arm: ArmDecision, run: _Run, elig_ids: Mapping[int, int | None]
) -> tuple[object, ...]:
    eligibility_request_id = elig_ids[arm.eligibility_seq] if arm.eligibility_seq is not None else None
    if arm.order is None:
        return (snapshot_id, arm.instrument_id, arm.arm, arm.outcome, eligibility_request_id, *(None,) * 6)
    result = run.arm_results.get((arm.instrument_id, arm.arm))
    request, last_updated = result if result is not None else (None, None)
    outcome = "unattempted" if request is None else request.outcome
    return (
        snapshot_id,
        arm.instrument_id,
        arm.arm,
        outcome,
        eligibility_request_id,
        request.request_id if request is not None else None,
        arm.order.transaction,
        arm.order.settlement_type,
        arm.order.amount,
        arm.order.leverage,
        last_updated if outcome == "ok" else None,
    )


def _record_failure(
    conn: psycopg.Connection[Any],
    run: _Run,
    started_at: datetime,
    finished_at: datetime,
    params: Mapping[str, object],
    exc: BaseException,
) -> None:
    """Commit a ``failed`` header with everything collected; failing that, a bare one. Best-effort only —
    never allowed to replace the error the caller re-raises."""
    error = f"{type(exc).__name__}: {exc}"
    for attempt in (run, _Run()):
        try:
            snapshot_id = _write(conn, attempt, started_at, finished_at, STATUS_FAILED, params, error)
        except Exception:
            logger.exception("perishables snapshot failed (%s) and its failed row could not be written", error)
            continue
        logger.warning("perishables snapshot %d failed: %s", snapshot_id, error)
        return


__all__ = [
    "MAX_CONSECUTIVE_ERRORS",
    "MAX_UNIVERSE",
    "MAX_WHATIF_PANEL",
    "RECORDER_VERSION",
    "WHATIF_PANEL_RULE",
    "ArmCapacity",
    "EnvelopeViolation",
    "PerishableSnapshotPartial",
    "PerishableSnapshotRefused",
    "PerishablesSnapshotResult",
    "arm_capacity",
    "decide_arms",
    "number",
    "parse_eligibility",
    "parse_rates",
    "parse_what_if",
    "record_perishables_snapshot",
    "timestamp",
]
