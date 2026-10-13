"""#3616 slice 3b — tax FX parsers, planner, resolver and HTTP adapter (pure).

Every rate here is synthetic: the repository is public and the Bank's exchange-rate data is excluded from the Open
Government Licence. Bodies are built in the Bank's format with invented values; the calendar is the committed GOV.UK
file (OGL v3.0).
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, localcontext
from pathlib import Path

import httpx
import pytest

from app.providers.implementations.boe import BoeParseError, parse_xudluss_csv, xudluss_url
from app.providers.implementations.govuk_calendar import CalendarParseError, parse_bank_holidays
from app.services import tax_fx
from app.services.tax_fx import (
    CALENDAR_SEED_SHA256,
    FX_FLOOR,
    TAX_FX_POLICY_ID,
    Confirmer,
    Fetch,
    Plan,
    TaxFxInvariantError,
    TaxFxRate,
    TaxFxState,
    answer_is_stale,
    beyond_move_bound,
    feed_stale_origin,
    fetch_capped,
    is_working_day,
    plan_run,
    resolve_tax_fx,
)

SEED_BYTES = Path("docs/research/3616-slice3b-inputs/bank-holidays_2026-10-13.json").read_bytes()
SEED = parse_bank_holidays(SEED_BYTES)
MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
RUN_DATE = date(2026, 10, 13)  # Tuesday
LAST = date(2026, 10, 12)  # Monday
BASE = Decimal("1.25")  # synthetic
FETCHED_AT = datetime(2026, 10, 13, 10, 43, tzinfo=UTC)


def working_days(last: date, start: date = FX_FLOOR) -> list[date]:
    days, day = [], start
    while day <= last:
        if is_working_day(day, SEED):
            days.append(day)
        day += timedelta(days=1)
    return days


def series(last: date = LAST, *, scale: Decimal = Decimal(1)) -> dict[date, Decimal]:
    """A constant synthetic series over the seed calendar's working days, so consecutive moves are zero."""
    return {day: BASE * scale for day in working_days(last)}


def csv(observations: dict[date, Decimal]) -> bytes:
    rows = [f"{d.day:02d} {MONTHS[d.month - 1]} {d.year},{v}" for d, v in sorted(observations.items())]
    return ("\r\n".join(["DATE,XUDLUSS", *rows]) + "\r\n").encode("ascii")


def boe(
    body: bytes | dict[date, Decimal], *, status: int = 200, length: str | None = "auto", encoding: str | None = None
) -> Fetch:
    raw = body if isinstance(body, bytes) else csv(body)
    return Fetch(
        tax_fx.SOURCE_BOE,
        "https://boe.test/x",
        FETCHED_AT,
        "received",
        http_status=status,
        content_length_header=str(len(raw)) if length == "auto" else length,
        content_encoding_header=encoding,
        body=raw,
    )


def calendar_body(
    *, add: tuple[str, ...] = (), remove: tuple[str, ...] = (), drop_years: tuple[int, ...] = ()
) -> bytes:
    data = json.loads(SEED_BYTES)
    division = data["england-and-wales"]
    events = [e for e in division["events"] if e["date"] not in remove and int(e["date"][:4]) not in drop_years]
    events += [{"title": "Synthetic", "date": d, "notes": "", "bunting": False} for d in add]
    division["events"] = events
    return json.dumps(data).encode()


def calendar(body: bytes = SEED_BYTES) -> Fetch:
    return Fetch(
        tax_fx.SOURCE_CALENDAR,
        "https://gov.test/bh.json",
        FETCHED_AT,
        "received",
        http_status=200,
        content_length_header=str(len(body)),
        body=body,
    )


NO_CALENDAR = Fetch(tax_fx.SOURCE_CALENDAR, "https://gov.test/bh.json", FETCHED_AT, "transport_error", "down")


def confirmer(
    observations: dict[date, Decimal] | None, *, eligible: bool = True, policy: str = TAX_FX_POLICY_ID
) -> Confirmer | None:
    return None if observations is None else Confirmer(1, eligible, policy, observations)


def plan(
    body: Fetch | dict[date, Decimal],
    *,
    run_date: date = RUN_DATE,
    stored: dict[date, Decimal] | None = None,
    stored_calendar: dict[int, frozenset[date]] | None = None,
    cal: Fetch | None = None,
    previous: dict[date, Decimal] | None = None,
    previous_eligible: bool = True,
) -> Plan:
    return plan_run(
        run_date=run_date,
        stored_observations=stored or {},
        stored_calendar=SEED if stored_calendar is None else stored_calendar,
        boe=body if isinstance(body, Fetch) else boe(body),
        calendar_fetch=calendar() if cal is None else cal,
        confirmer=confirmer(previous, eligible=previous_eligible),
    )


def test_committed_seed_hash_is_the_constant() -> None:
    assert hashlib.sha256(SEED_BYTES).hexdigest() == CALENDAR_SEED_SHA256


# --- parsers ---------------------------------------------------------------------------------------------------


def test_boe_parser_accepts_valid_and_header_only() -> None:
    parsed = parse_xudluss_csv(csv(series(date(2019, 1, 10))), FX_FLOOR, RUN_DATE)
    assert [o.observation_date for o in parsed] == working_days(date(2019, 1, 10))
    assert parse_xudluss_csv(b"DATE,XUDLUSS\r\n", FX_FLOOR, RUN_DATE) == []


@pytest.mark.parametrize(
    "body",
    [
        b"<html><body>Moved</body></html>",  # the synthetic 302 page
        b"DATE,XUDLUSS\n05 Oct 2026,1.25\n",  # LF only
        b"DATE,XUDLUSX\r\n05 Oct 2026,1.25\r\n",  # wrong header
        b"DATE,XUDLUSS\r\n5 OCT 2026, +1_000e-3 \r\n",
        b"DATE,XUDLUSS\r\n05 Oct 2026, 1.25\r\n",  # whitespace
        b"DATE,XUDLUSS\r\n05 Oct 2026,+1.25\r\n",  # sign
        b"DATE,XUDLUSS\r\n05 Oct 2026,1e0\r\n",  # exponent
        b"DATE,XUDLUSS\r\n05 Oct 2026,1_25\r\n",  # underscore
        b"DATE,XUDLUSS\r\n05 OCT 2026,1.25\r\n",  # month case
        b"DATE,XUDLUSS\r\n30 Feb 2026,1.25\r\n",
        b"DATE,XUDLUSS\r\n05 Oct 2026,0\r\n",
        b"DATE,XUDLUSS\r\n05 Oct 2026,1000000\r\n",
        b"DATE,XUDLUSS\r\n05 Oct 2026,1.2345678\r\n",
        b"DATE,XUDLUSS\r\n05 Oct 2026,1.25\r\n05 Oct 2026,1.25\r\n",  # repeated
        b"DATE,XUDLUSS\r\n06 Oct 2026,1.25\r\n05 Oct 2026,1.25\r\n",  # decreasing
        b"DATE,XUDLUSS\r\n14 Oct 2026,1.25\r\n",  # after the interval
        b"DATE,XUDLUSS\r\n31 Dec 2018,1.25\r\n",  # before the interval
        "DATE,XUDLUSS\r\n05 Oct 2026,1.25 \r\n".encode(),  # non-ASCII
    ],
)
def test_boe_parser_refuses(body: bytes) -> None:
    with pytest.raises(BoeParseError):
        parse_xudluss_csv(body, FX_FLOOR, RUN_DATE)


def test_calendar_parser() -> None:
    assert min(SEED) == 2019 and max(SEED) == 2028
    with pytest.raises(CalendarParseError):
        parse_bank_holidays(json.dumps({"scotland": {"division": "scotland", "events": []}}).encode())
    with pytest.raises(CalendarParseError, match="duplicate"):
        parse_bank_holidays(calendar_body(add=("2026-12-25",)))
    with pytest.raises(CalendarParseError, match="year 2026 holds 7"):
        parse_bank_holidays(calendar_body(remove=("2026-08-31",)))


def test_request_url_uses_english_months_and_no_provisional_data() -> None:
    assert xudluss_url(FX_FLOOR, RUN_DATE).endswith(
        "&Datefrom=01/Jan/2019&Dateto=13/Oct/2026&SeriesCodes=XUDLUSS&CSVF=TN&UsingCodes=Y&VPD=N&VFD=N"
    )


# --- planner, empty store --------------------------------------------------------------------------------------


def test_bootstrap_takes_two_agreeing_runs() -> None:
    body = series()
    first = plan(body)
    assert (first.state, first.eligible, first.inserts, first.accepted_through) == ("applied", True, [], None)
    assert first.awaiting_confirmation == len(body)
    second = plan(body, previous=body)
    assert second.inserts == sorted(body.items()) and second.accepted_through == LAST


def test_disagreement_on_a_friday_stores_through_thursday() -> None:
    friday = date(2026, 10, 9)
    a = series(friday)
    b = {**a, friday: Decimal("1.2501")}
    stored = plan(b, run_date=LAST, previous=a)
    assert stored.accepted_through == date(2026, 10, 8) and stored.awaiting_confirmation == 1
    state = _state(dict(stored.inserts))
    assert resolve_tax_fx(state, date(2026, 10, 10)).status == "pending"
    c = {**b, LAST: BASE}
    nxt = plan(c, stored=dict(stored.inserts), previous=b)
    assert nxt.inserts == [(friday, Decimal("1.2501"))] and nxt.accepted_through == friday


def test_a_scaled_body_then_the_true_body_confirms_nothing() -> None:
    result = plan(series(), previous=series(scale=Decimal(10)))
    assert result.state == "applied" and result.inserts == []


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (b"DATE,XUDLUSS\r\n", "boe_empty"),
        ({d: BASE for d in working_days(LAST, date(2026, 1, 2))}, "coverage_mismatch"),  # suffix only
        ({d: v for d, v in series().items() if d != date(2024, 6, 5)}, "coverage_mismatch"),  # interior gap
        ({**series(), date(2026, 10, 10): BASE}, "coverage_mismatch"),  # a Saturday
        (series(date(2026, 10, 6)), "bootstrap_stale"),  # 5 working days before RUN_DATE
        ({**series(), date(2025, 3, 3): BASE * Decimal("1.2")}, "implausible_move"),
    ],
)
def test_empty_store_refusals(body: bytes | dict[date, Decimal], reason: str) -> None:
    result = plan(boe(body))
    assert result.state == "refused" and result.reason is not None and result.reason.startswith(reason)
    assert not result.eligible


def test_three_working_days_stale_is_still_fresh() -> None:
    assert plan(series(date(2026, 10, 8))).state == "applied"


# --- planner, stored state -------------------------------------------------------------------------------------


def test_a_missing_stored_date_refuses() -> None:
    assert (plan(series(date(2026, 10, 9)), stored=series()).reason or "").startswith("regression")


def test_a_revision_is_refused_once_then_applied() -> None:
    stored = series()
    revised = {**stored, date(2026, 10, 1): Decimal("1.2600")}
    first = plan(revised, stored=stored, previous=stored)
    assert (first.state, first.eligible) == ("refused", True)
    assert first.reason == "revision_unconfirmed: 2026-10-01"
    second = plan(revised, stored=stored, previous=revised)
    assert second.state == "applied"
    assert second.revisions == [(date(2026, 10, 1), BASE, Decimal("1.2600"))]


def test_an_ineligible_confirmer_confirms_nothing() -> None:
    stored = series()
    revised = {**stored, date(2026, 10, 1): Decimal("1.2600")}
    result = plan(revised, stored=stored, previous=revised, previous_eligible=False)
    assert (result.reason or "").startswith("revision_unconfirmed")


def test_revision_count_and_size_gates() -> None:
    stored = series()
    days = working_days(LAST, date(2026, 9, 1))
    five = {**stored, **{d: Decimal("1.2600") for d in days[:5]}}
    assert plan(five, stored=stored, previous=five).state == "applied"
    six = {**stored, **{d: Decimal("1.2600") for d in days[:6]}}
    assert (plan(six, stored=stored, previous=six).reason or "").startswith("too_many_revisions: 6")
    # A self-consistent rescale isolates the revision gate from the consecutive-move gate.
    for sign in (1, -1):
        with localcontext() as ctx:
            ctx.prec = 30
            factor = Decimal(math.exp(sign * 0.1501))
        scaled = {d: (v * factor).quantize(Decimal("1E-6")) for d, v in stored.items()}
        assert (plan(scaled, stored=stored, previous=scaled).reason or "").startswith("implausible_revision")
    assert not beyond_move_bound(Decimal("0.15"))
    assert beyond_move_bound(Decimal("0.1500001"))


def test_a_new_tail_date_waits_one_run() -> None:
    friday = date(2026, 10, 9)
    stored = series(friday)
    first = plan(series(), stored=stored, previous=stored)
    assert (first.inserts, first.awaiting_confirmation, first.accepted_through) == ([], 1, friday)
    second = plan(series(), stored=stored, previous=series())
    assert second.inserts == [(LAST, BASE)] and second.accepted_through == LAST


# --- calendar ----------------------------------------------------------------------------------------------------


def test_dropping_a_year_keeps_it() -> None:
    result = plan(series(), cal=calendar(calendar_body(drop_years=(2019,))))
    assert result.state == "applied" and result.calendar_changes == [] and 2019 in result.calendar


def test_removing_the_2022_special_holidays_refuses() -> None:
    result = plan(series(), cal=calendar(calendar_body(remove=("2022-06-03", "2022-09-19"))))
    assert result.reason == "calendar_history_changed: 2022-06-03, 2022-09-19 on or before 2026-10-13"


def test_a_holiday_added_on_a_past_working_day_refuses() -> None:
    gap = date(2026, 10, 7)
    body = {d: v for d, v in series().items() if d != gap}
    cal = calendar(calendar_body(add=("2026-10-07",)))
    assert (plan(body, cal=cal).reason or "").startswith("calendar_history_changed: 2026-10-07")
    # The same pairing between the stored L and the run date.
    stored = series(date(2026, 10, 5))
    assert (plan(body, stored=stored, previous=body, cal=cal).reason or "").startswith(
        "calendar_history_changed: 2026-10-07"
    )


def test_a_holiday_after_the_run_date_applies_and_is_recorded() -> None:
    result = plan(series(), cal=calendar(calendar_body(add=("2026-10-14",))))
    assert result.state == "applied"
    [(year, old, new)] = result.calendar_changes
    assert year == 2026 and old == SEED[2026] and new == SEED[2026] | {date(2026, 10, 14)}


def test_next_year_absent_from_both_calendars_refuses() -> None:
    stored_calendar = {y: d for y, d in SEED.items() if y != 2027}
    result = plan(series(), stored_calendar=stored_calendar, cal=NO_CALENDAR)
    assert result.calendar_used == "stored" and result.reason == "calendar_discontinuous: no calendar for [2027]"


# --- framing -----------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("fetch", "reason"),
    [
        (boe(series(), status=206), "boe_http_status"),
        (boe(series(), status=302), "boe_http_status"),
        (boe(series(), status=500), "boe_http_status"),
        (boe(series(), length="99999999"), "boe_length_mismatch"),
        (boe(series(), encoding="gzip"), "boe_content_encoding"),
    ],
)
def test_framing_refusals(fetch: Fetch, reason: str) -> None:
    assert (plan(fetch).reason or "").startswith(reason)


def test_oversize_refuses_and_a_missing_body_is_not_planned() -> None:
    over = Fetch(tax_fx.SOURCE_BOE, "u", FETCHED_AT, "oversize", "big", 200, None, None, b"x")
    assert (plan(over).reason or "").startswith("boe_oversize")
    with pytest.raises(ValueError, match="skipped"):
        plan(Fetch(tax_fx.SOURCE_BOE, "u", FETCHED_AT, "incomplete", "cut", 200, None, None, b""))


# --- the stale alert -------------------------------------------------------------------------------------------


def test_feed_stale_origin() -> None:
    # A first run with nothing confirmed raises no alert.
    assert feed_stale_origin(None, RUN_DATE, RUN_DATE, SEED) is None
    # Persistent disagreement on the first dates, measured from the first eligible run.
    assert feed_stale_origin(None, date(2026, 10, 6), date(2026, 10, 12), SEED) is None
    assert feed_stale_origin(None, date(2026, 10, 6), RUN_DATE, SEED) == date(2026, 10, 6)
    # A populated store whose L stands still (a stale body, or fresh bodies whose new dates keep changing).
    assert feed_stale_origin(date(2026, 10, 6), None, RUN_DATE, SEED) == date(2026, 10, 6)
    assert feed_stale_origin(date(2026, 10, 7), None, RUN_DATE, SEED) is None


# --- resolver ----------------------------------------------------------------------------------------------------


def _state(
    observations: dict[date, Decimal],
    *,
    health: str | None = "applied",
    policy: str | None = TAX_FX_POLICY_ID,
    enabled: bool = True,
) -> TaxFxState:
    return TaxFxState(enabled, health, policy, 7, observations, SEED, tuple(sorted(observations)))


def test_resolver_order() -> None:
    observations = {d: BASE + Decimal(i % 9) / 1000 for i, d in enumerate(working_days(LAST))}
    state = _state(observations)

    def check(day: date, status: str, observed: date | None) -> None:
        answer = resolve_tax_fx(state, day)
        assert (answer.status, answer.observation_date) == (status, observed)
        assert answer.usd_per_gbp == (None if observed is None else observations[observed])
        assert answer.run_id == 7 and answer.policy_id == TAX_FX_POLICY_ID

    check(LAST, "published", LAST)
    check(date(2026, 10, 10), "carried_forward", date(2026, 10, 9))
    check(date(2026, 4, 3), "carried_forward", date(2026, 4, 2))  # Good Friday
    check(date(2026, 4, 6), "carried_forward", date(2026, 4, 2))  # Easter Monday
    check(date(2025, 12, 25), "carried_forward", date(2025, 12, 24))
    check(date(2019, 1, 1), "out_of_range", None)
    check(date(2018, 12, 31), "out_of_range", None)
    check(RUN_DATE, "pending", None)
    check(date(2026, 10, 17), "pending", None)
    gap = {d: v for d, v in observations.items() if d != date(2026, 10, 7)}
    with pytest.raises(TaxFxInvariantError, match="2026-10-07"):
        resolve_tax_fx(_state(gap), date(2026, 10, 7))


@pytest.mark.parametrize(
    "state",
    [
        _state(series(), health="in_progress"),
        _state(series(), health="abandoned"),
        _state(series(), health="refused"),
        _state({}),
        _state(series(), policy="boe_xudluss_ew_calendar_v0"),
        _state(series(), enabled=False),
        _state(series(), health=None),
    ],
)
def test_every_date_is_unavailable_when_unhealthy(state: TaxFxState) -> None:
    for day in (LAST, date(2026, 10, 10), date(2018, 12, 31), RUN_DATE):
        answer = resolve_tax_fx(state, day)
        assert answer.status == "unavailable" and answer.usd_per_gbp is None and answer.gbp_per_usd is None


@pytest.mark.parametrize("value", ["0.000001", "999999.999999", "1.234567", "3"])
def test_gbp_per_usd_rounds_half_even_to_ten_places(value: str) -> None:
    answer = resolve_tax_fx(_state({**series(), LAST: Decimal(value)}), LAST)
    with localcontext() as ctx:
        ctx.prec = 60
        exact = Decimal(1) / Decimal(value)
    assert answer.gbp_per_usd is not None and answer.gbp_per_usd == exact.quantize(Decimal("1E-10"))
    assert abs(answer.gbp_per_usd - exact) <= Decimal("5E-11")


def test_staleness_of_an_earlier_answer() -> None:
    base = series()
    state = _state(base)
    earlier = resolve_tax_fx(state, date(2026, 10, 1))
    assert not answer_is_stale(earlier, _state(dict(base)))
    assert answer_is_stale(earlier, _state({**base, date(2026, 10, 1): Decimal("1.2600")}))
    saturday = date(2026, 10, 10)
    carried = resolve_tax_fx(state, saturday)
    assert carried.status == "carried_forward"
    assert answer_is_stale(carried, _state({**base, saturday: Decimal("1.2600")}))
    old_policy = TaxFxRate(**{**earlier.__dict__, "policy_id": "boe_xudluss_ew_calendar_v0"})
    assert answer_is_stale(old_policy, state)


# --- HTTP adapter ------------------------------------------------------------------------------------------------


class _Chunks(httpx.SyncByteStream):
    """A lazily streamed body. ``httpx.Response(content=...)`` is read eagerly, which ``iter_raw`` then refuses."""

    def __init__(self, body: bytes) -> None:
        self._body = body

    def __iter__(self) -> Iterator[bytes]:
        for start in range(0, len(self._body), 4096):
            yield self._body[start : start + 4096]


def _response(status: int, body: bytes, headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.Response(status, headers={"content-length": str(len(body)), **(headers or {})}, stream=_Chunks(body))


class _Breaking(httpx.SyncByteStream):
    def __iter__(self) -> Iterator[bytes]:
        yield b"DATE,"
        raise httpx.ReadError("connection reset")


def _client(handler: httpx.MockTransport) -> httpx.Client:
    return httpx.Client(transport=handler)


def _fetch(client: httpx.Client, *, max_bytes: int = 1024, slow: bool = False) -> Fetch:
    clock = iter(range(0, 1000, 61)) if slow else None
    return fetch_capped(
        client,
        source=tax_fx.SOURCE_BOE,
        url="https://boe.test/x",
        max_bytes=max_bytes,
        now=lambda: FETCHED_AT,
        monotonic=(lambda: float(next(clock))) if clock else (lambda: 0.0),
    )


def test_fetch_sends_the_identifying_agent_and_keeps_raw_bytes() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return _response(200, b"DATE,XUDLUSS\r\n")

    fetch = _fetch(_client(httpx.MockTransport(handler)))
    assert (fetch.outcome, fetch.http_status, fetch.body, fetch.content_length_header) == (
        "received",
        200,
        b"DATE,XUDLUSS\r\n",
        "14",
    )
    assert seen[0].headers["user-agent"] == "eBull/1.0 (+https://github.com/Luke-Bradford/eBull)"
    assert seen[0].headers["accept-encoding"] == "identity"


def test_fetch_does_not_follow_a_redirect() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "elsewhere.test":
            return _response(200, b"DATE,XUDLUSS\r\n")
        return _response(302, b"<html/>", {"location": "https://elsewhere.test/"})

    fetch = _fetch(_client(httpx.MockTransport(handler)))
    assert (fetch.outcome, fetch.http_status, fetch.body) == ("received", 302, b"<html/>")


def test_fetch_failures() -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route", request=request)

    failed = _fetch(_client(httpx.MockTransport(refuse)))
    assert (failed.outcome, failed.http_status, failed.body) == ("transport_error", None, None)

    partial = _fetch(_client(httpx.MockTransport(lambda r: httpx.Response(200, stream=_Breaking()))))
    assert (partial.outcome, partial.http_status, partial.body) == ("incomplete", 200, b"DATE,")

    big = _fetch(_client(httpx.MockTransport(lambda r: _response(200, b"x" * 2000))), max_bytes=1000)
    assert big.outcome == "oversize" and big.body == b"x" * 2000

    slow = _fetch(_client(httpx.MockTransport(lambda r: _response(200, b"DATE,XUDLUSS\r\n"))), slow=True)
    assert slow.outcome == "incomplete" and "deadline" in (slow.error or "")


def test_a_slow_header_phase_stops_before_the_first_read() -> None:
    # Headers took 51 s: one more 10 s read could pass 60 s, so no body read is attempted.
    clock = iter([0.0, 51.0])
    fetch = fetch_capped(
        _client(httpx.MockTransport(lambda r: _response(200, b"DATE,XUDLUSS\r\n"))),
        source=tax_fx.SOURCE_BOE,
        url="https://boe.test/x",
        max_bytes=1024,
        now=lambda: FETCHED_AT,
        monotonic=lambda: next(clock),
    )
    assert (fetch.outcome, fetch.body) == ("incomplete", b"")


def test_oversize_keeps_only_the_first_64_kib() -> None:
    body = b"y" * (tax_fx.OVERSIZE_KEEP_BYTES * 2)
    fetch = _fetch(_client(httpx.MockTransport(lambda r: _response(200, body))), max_bytes=100_000)
    assert fetch.outcome == "oversize" and fetch.body == body[: tax_fx.OVERSIZE_KEEP_BYTES]
