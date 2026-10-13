"""#3616 slice 3b — the tax FX run against real tables, with a synthetic HTTP transport.

All rates are synthetic (the repository is public; the Bank's rates are excluded from the Open Government Licence).
"""

from __future__ import annotations

import threading
import time
from datetime import UTC, date, datetime
from decimal import Decimal

import httpx
import psycopg
import pytest

from app.services.tax_fx import (
    TAX_FX_LOCK_KEY,
    TaxFxRefreshError,
    load_tax_fx_state,
    replay_from_evidence,
    resolve_tax_fx,
    run_tax_fx_refresh,
    tax_fx_state_is_current,
)
from app.services.tax_ledger import _load_fx_rate
from tests.fixtures.ebull_test_db import test_database_url
from tests.test_tax_fx import LAST, SEED, SEED_BYTES, _response, calendar_body, csv, series

NOW = datetime(2026, 10, 13, 10, 43, tzinfo=UTC)  # run_date 2026-10-13
REVISED_DAY = date(2026, 10, 1)


class _Server:
    """Serves the Bank by host and GOV.UK otherwise; an exception set on either is raised for that request."""

    def __init__(self) -> None:
        self.boe: bytes = csv(series())
        self.calendar: bytes = SEED_BYTES
        self.boe_error: Exception | None = None
        self.calendar_error: Exception | None = None

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == "www.bankofengland.co.uk":
            if self.boe_error is not None:
                raise self.boe_error
            return _response(200, self.boe)
        assert request.url.host == "www.gov.uk"
        if self.calendar_error is not None:
            raise self.calendar_error
        return _response(200, self.calendar)


def _connect(*, autocommit: bool = True) -> psycopg.Connection[tuple]:
    return psycopg.connect(test_database_url(), autocommit=autocommit)


def _run(server: _Server):  # noqa: ANN202 - RunReport
    with _connect() as conn, httpx.Client(transport=httpx.MockTransport(server.handler)) as client:
        return run_tax_fx_refresh(conn, client, now=lambda: NOW)


def _state():  # noqa: ANN202 - TaxFxState
    with _connect() as conn:
        return load_tax_fx_state(conn)


def _rows(sql: str) -> list[tuple]:
    with _connect() as conn:
        return conn.execute(sql).fetchall()  # type: ignore[arg-type]


def _is_current(state) -> bool:  # noqa: ANN001
    with _connect() as conn, conn.transaction():
        return tax_fx_state_is_current(conn, state)


def test_runs_apply_refuse_skip_crash_and_replay(
    ebull_test_conn: psycopg.Connection[tuple], seeded_instrument_id: int
) -> None:
    conn = ebull_test_conn
    # Non-interference: the existing tax FX table, the tax lots and the ECB table are untouched by every run.
    conn.execute(
        "INSERT INTO fx_rates (rate_date, from_currency, to_currency, rate) VALUES ('2026-10-12','USD','GBP',0.75)"
    )
    conn.execute(
        "INSERT INTO fx_rates_daily (rate_date, base_currency, quote_currency, rate) "
        "VALUES ('2026-10-12','USD','GBP',0.74)"
    )
    conn.execute(
        "INSERT INTO tax_lots (instrument_id, event_time, event_type, quantity, cost_or_proceeds, tax_year) "
        "VALUES (%s, '2026-10-12T15:00Z', 'acquisition', 1, 100, '2026/27')",
        (seeded_instrument_id,),
    )
    conn.commit()
    untouched = (
        "SELECT (SELECT array_agg(f::text) FROM fx_rates f), (SELECT array_agg(t::text) FROM tax_lots t), "
        "(SELECT array_agg(d::text) FROM fx_rates_daily d)"
    )
    before = _rows(untouched)

    server = _Server()
    first = _run(server)
    assert (first.state, first.inserted, first.accepted_through) == ("applied", 0, None)
    assert not _state().healthy  # bootstrap takes two runs

    second = _run(server)
    assert second.inserted == len(series()) and second.accepted_through == LAST
    healthy = _state()
    assert healthy.healthy and healthy.latest_applied_run_id == second.run_id
    assert resolve_tax_fx(healthy, LAST).status == "published"
    assert _is_current(healthy)

    # A revision is refused once: run and fetch rows stay, no derived row changes, the source is unavailable.
    server.boe = csv({**series(), REVISED_DAY: Decimal("1.2600")})
    with pytest.raises(TaxFxRefreshError, match="refused: revision_unconfirmed: 2026-10-01"):
        _run(server)
    assert _rows("SELECT state, eligible FROM tax_fx_runs ORDER BY run_id DESC LIMIT 1") == [("refused", True)]
    assert _rows("SELECT count(*) FROM tax_fx_fetches WHERE run_id = (SELECT max(run_id) FROM tax_fx_runs)") == [(2,)]
    assert _rows(f"SELECT usd_per_gbp FROM tax_fx_observations WHERE observation_date = '{REVISED_DAY}'") == [
        (Decimal("1.250000"),)
    ]
    assert resolve_tax_fx(_state(), LAST).status == "unavailable"
    assert not _is_current(healthy)

    revised = _run(server)
    assert revised.revised == 1
    assert _rows("SELECT observation_date, old_usd_per_gbp, new_usd_per_gbp FROM tax_fx_revisions") == [
        (REVISED_DAY, Decimal("1.250000"), Decimal("1.260000"))
    ]

    # A holiday declared for the day after the run date is accepted and recorded.
    server.calendar = calendar_body(add=("2026-10-14",))
    declared = _run(server)
    assert declared.calendar_years_changed == 1
    assert _rows("SELECT source FROM tax_fx_calendar_years WHERE year = 2026") == [(f"run:{declared.run_id}",)]
    assert _rows("SELECT year, old_dates IS NULL FROM tax_fx_calendar_revisions") == [(2026, False)]

    # A skipped run (no BoE response) leaves the source healthy.
    server.boe_error = httpx.ConnectError("down")
    with pytest.raises(TaxFxRefreshError, match="skipped: boe_transport_error"):
        _run(server)
    assert _state().healthy

    # A crash after the BoE fetch commits leaves the run in_progress and the source unavailable until a run applies.
    server.boe_error = None
    server.calendar_error = RuntimeError("simulated crash")
    with pytest.raises(RuntimeError, match="simulated crash"):
        _run(server)
    assert _rows("SELECT state FROM tax_fx_runs ORDER BY run_id DESC LIMIT 1") == [("in_progress",)]
    assert not _state().healthy
    server.calendar_error = None
    recovered = _run(server)
    assert _state().healthy
    assert _rows(f"SELECT state FROM tax_fx_runs WHERE run_id = {recovered.run_id - 1}") == [("abandoned",)]

    # Replay from the seed and the stored evidence rebuilds both derived tables.
    with _connect() as replay_conn:
        observations, calendar = replay_from_evidence(replay_conn, SEED)
    live_observations = dict(_rows("SELECT observation_date, usd_per_gbp FROM tax_fx_observations ORDER BY 1"))
    live_calendar = {y: frozenset(d) for y, d in _rows("SELECT year, holiday_dates FROM tax_fx_calendar_years")}
    assert observations == live_observations and calendar == live_calendar

    assert _rows(untouched) == before
    assert _load_fx_rate(conn, date(2026, 10, 12), "USD") == Decimal("0.75")

    conn.rollback()
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        conn.execute("UPDATE tax_fx_fetches SET error = 'x'")
    conn.rollback()


def _in_thread(server: _Server) -> tuple[threading.Thread, list[object]]:
    outcome: list[object] = []

    def target() -> None:
        try:
            outcome.append(_run(server))
        except Exception as exc:  # noqa: BLE001 - reported to the test
            outcome.append(exc)

    thread = threading.Thread(target=target)
    thread.start()
    return thread, outcome


def _lock_is_free() -> bool:
    with _connect() as conn:
        taken = conn.execute("SELECT pg_try_advisory_lock(%s)", (TAX_FX_LOCK_KEY,)).fetchone()
        if taken and taken[0]:
            conn.execute("SELECT pg_advisory_unlock(%s)", (TAX_FX_LOCK_KEY,))
            return True
        return False


def test_runs_serialise_and_wait_for_publishers(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    server = _Server()
    with _connect() as holder:
        holder.execute("SELECT pg_advisory_lock(%s)", (TAX_FX_LOCK_KEY,))
        thread, outcome = _in_thread(server)
        time.sleep(0.5)
        assert thread.is_alive() and outcome == []
        holder.execute("SELECT pg_advisory_unlock(%s)", (TAX_FX_LOCK_KEY,))
        thread.join(30)
    assert getattr(outcome[0], "state", None) == "applied"

    state = _state()
    with _connect() as publisher, publisher.transaction():
        tax_fx_state_is_current(publisher, state)  # takes the shared lock until this transaction commits
        thread, outcome = _in_thread(server)
        time.sleep(0.5)
        assert thread.is_alive() and outcome == []
    thread.join(30)
    assert getattr(outcome[0], "inserted", None) == len(series())

    # After a refusal, a skip and an unexpected exception, the lock is free.
    server.boe = b"DATE,XUDLUSS\r\n"
    with pytest.raises(TaxFxRefreshError, match="refused: boe_empty"):
        _run(server)
    assert _lock_is_free()
    server.boe_error = httpx.ReadTimeout("slow")
    with pytest.raises(TaxFxRefreshError, match="skipped"):
        _run(server)
    assert _lock_is_free()
    server.boe_error = RuntimeError("unexpected")
    with pytest.raises(RuntimeError, match="unexpected"):
        _run(server)
    assert _lock_is_free()


def test_seed_control_and_disable(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    seeded = {y: frozenset(d) for y, d in _rows("SELECT year, holiday_dates FROM tax_fx_calendar_years")}
    assert seeded == SEED
    assert {s for (s,) in _rows("SELECT DISTINCT source FROM tax_fx_calendar_years")} == {
        "seed:538b3482c28b85ecd2db606a0d5ae6ad17248900b6498700ce0a48d26a3ecde6"
    }
    assert _rows("SELECT enabled FROM tax_fx_source_control") == [(True,)]

    with _connect(autocommit=False) as open_conn:
        open_conn.execute("SELECT 1")
        with pytest.raises(RuntimeError, match="idle connection"):
            load_tax_fx_state(open_conn)

    server = _Server()
    _run(server)
    _run(server)
    state = _state()
    assert state.healthy

    with _connect() as admin, admin.transaction():
        admin.execute("SELECT pg_advisory_xact_lock(%s)", (TAX_FX_LOCK_KEY,))
        admin.execute("UPDATE tax_fx_source_control SET enabled = FALSE, reason = 'test', changed_at = now()")
    assert resolve_tax_fx(_state(), LAST).status == "unavailable"
    assert not _is_current(state)
    with pytest.raises(TaxFxRefreshError, match="skipped: source_disabled"):
        _run(server)
