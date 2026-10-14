"""The slice 3c run against the test database (#3616; spec Design 3 and 6, sql/486 and sql/488).

Auto-marked ``db`` (pulls ``ebull_test_conn``). The run commits each lookup on its own autocommit connection, as
the job does; the fixture connection seeds the ledger and wipes between tests. No test reaches the broker, and every
rate is synthetic (the 3b test series).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.providers.broker import RawOrderLookup
from app.services import tax_broker_evidence
from app.services.tax_broker_derivation import TAX_EVIDENCE_POLICY_ID
from app.services.tax_broker_evidence import TAX_EVIDENCE_LOCK_KEY, run_tax_broker_evidence
from tests.fixtures.ebull_test_db import test_database_url
from tests.test_tax_broker_derivation import sync_payload
from tests.test_tax_fx_db import _run as run_tax_fx
from tests.test_tax_fx_db import _Server as FxServer

ACCOUNT = 900  # the derivation tests' synthetic account
OPENED = datetime(2026, 10, 1, 13, 30, tzinfo=UTC)  # a working day inside the synthetic 3b series


class _FakeBroker:
    def __init__(self, responses: dict[int, RawOrderLookup]) -> None:
        self.responses = responses
        self.asked: list[int] = []

    def lookup_order_raw(self, order_id: int) -> RawOrderLookup:
        self.asked.append(order_id)
        return self.responses.get(order_id, RawOrderLookup(404, '{"errorMessage": "not found"}', None))


def _connect() -> psycopg.Connection[tuple]:
    return psycopg.connect(test_database_url(), autocommit=True)


def _rows(sql: str) -> list[tuple]:
    with _connect() as conn:
        return conn.execute(sql).fetchall()  # type: ignore[arg-type]


def _run(broker: _FakeBroker) -> tax_broker_evidence.EvidenceRunReport:
    with _connect() as conn:
        return run_tax_broker_evidence(conn, broker, "demo")


def _seed(conn: psycopg.Connection[Any], *, scope: str | None = "demo") -> None:
    """The capture seed: order 7 named by the column on a history close, 8 only by a sync payload's orderID."""
    if scope is not None:
        conn.execute(
            "INSERT INTO trade_ledger_scope (singleton, environment, account_id, established_at) "
            "VALUES (TRUE, %s, 4242, now())",
            (scope,),
        )
    for position_id, kind, source, order_col, payload in (
        (501, "open", "etoro_history", 7, {"orderId": 7}),
        (501, "close", "etoro_history", 7, {"orderId": 7}),
        (502, "open", "etoro_sync", None, {"orderID": 8, "CID": 4242}),
    ):
        conn.execute(
            """
            INSERT INTO trade_events (position_id, etoro_instrument_id, event_kind, side, units, executed_at,
                                      order_id, source, raw_payload, environment)
            VALUES (%s, 4077, %s, 'buy', 1, now() - interval '1 day', %s, %s, %s, 'demo')
            """,
            (position_id, kind, order_col, source, Jsonb(payload)),
        )
    conn.commit()


def _seed_held_order(conn: psycopg.Connection[Any], instrument_id: int, position_id: int, order_id: int) -> None:
    """One admitted shape: a sync-observed, still-held order with no lookup (``sync_only``)."""
    payload = {
        **sync_payload(position_id, orderID=order_id, openDateTime="2026-10-01T13:30:00Z"),
        "units": 1,
        "amount": 100,
    }
    payload = {k: (float(v) if not isinstance(v, (bool, int, str)) else v) for k, v in payload.items()}
    conn.execute(
        """
        INSERT INTO trade_events (position_id, etoro_instrument_id, instrument_id, event_kind, side, units, price,
                                  executed_at, source, raw_payload, environment)
        VALUES (%s, 1001, %s, 'open', 'buy', 1, 100, %s, 'etoro_sync', %s, 'demo')
        """,
        (position_id, instrument_id, OPENED, Jsonb(payload)),
    )
    conn.execute(
        """
        INSERT INTO broker_positions (position_id, instrument_id, is_buy, units, amount, initial_amount_in_dollars,
                                      open_rate, open_conversion_rate, open_date_time, raw_payload)
        VALUES (%s, %s, TRUE, 1, 100, 100, 100, 1, %s, %s)
        """,
        (position_id, instrument_id, OPENED, Jsonb(payload)),
    )


def _fetch_row(
    conn: psycopg.Connection[Any],
    *,
    outcome: str = "ok",
    environment: str = "demo",
    account: int | None = ACCOUNT,
    real_positions: int = 1,
) -> None:
    conn.execute(
        """
        INSERT INTO trade_history_fetches (environment, account_id, portfolio_fetched_at, portfolio_real_positions,
                                           outcome, row_count)
        VALUES (%s, %s, now(), %s, %s, %s)
        """,
        (environment, account, real_positions, outcome, 0 if outcome == "ok" else None),
    )


@pytest.fixture
def published_ledger(ebull_test_conn: psycopg.Connection[Any], seeded_instrument_id: int) -> int:
    """A healthy 3b state, the scope, one held sync-only order and an ``ok`` refresh."""
    server = FxServer()
    run_tax_fx(server)
    run_tax_fx(server)  # bootstrap takes two runs
    conn = ebull_test_conn
    conn.execute(
        "INSERT INTO trade_ledger_scope (singleton, environment, account_id, established_at) "
        "VALUES (TRUE, 'demo', %s, now())",
        (ACCOUNT,),
    )
    _seed_held_order(conn, seeded_instrument_id, 21, 1000)
    _fetch_row(conn)
    conn.commit()
    return seeded_instrument_id


# --- capture (Design 3) inside the run --------------------------------------------------------------------------


def test_capture_stores_each_attempt_as_received(ebull_test_conn: psycopg.Connection[Any]) -> None:
    _seed(ebull_test_conn)
    body = '{"orderId": 7, "accountId": 4242, "totalCosts": 0.10}'
    broker = _FakeBroker({7: RawOrderLookup(200, body, None)})
    report = _run(broker)
    # No portfolio refresh was ever recorded: the run refuses after capturing.
    assert (report.state, report.reason) == ("refused", "portfolio_unobserved")
    assert report.capture is not None and report.capture.outcomes == {"found": 1, "not_found": 1}
    assert sorted(broker.asked) == [7, 8]
    assert _rows(
        "SELECT order_id, outcome, http_status, body_text, account_id, body_sha256 IS NOT NULL "
        "FROM tax_order_lookups ORDER BY order_id"
    ) == [(7, "found", 200, body, 4242, True), (8, "not_found", 404, '{"errorMessage": "not found"}', None, True)]
    # The decimal text is kept as received: 0.10, not 0.1.
    assert "0.10" in _rows("SELECT body_text FROM tax_order_lookups WHERE order_id = 7")[0][0]
    assert _rows("SELECT state, reason, lookups_attempted, lookups_deferred FROM tax_evidence_runs") == [
        ("refused", "portfolio_unobserved", 2, 0)
    ]
    # A second run asks nothing: no new events, and nothing is due.
    assert _run(broker).capture.attempted == 0  # type: ignore[union-attr]
    with _connect() as conn, pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        conn.execute("UPDATE tax_order_lookups SET error_detail = 'x'")


def test_rate_limited_stops_the_capture(ebull_test_conn: psycopg.Connection[Any]) -> None:
    _seed(ebull_test_conn)
    broker = _FakeBroker({7: RawOrderLookup(429, "", None), 8: RawOrderLookup(429, "", None)})
    capture = _run(broker).capture
    assert capture is not None
    assert (capture.attempted, capture.stopped_by, capture.deferred) == (1, "rate_limited", 1)
    assert len(broker.asked) == 1
    assert _rows("SELECT lookups_stopped_by FROM tax_evidence_runs") == [("rate_limited",)]


@pytest.mark.parametrize(("scope", "reason"), [(None, "scope_unestablished"), ("real", "scope_mismatch")])
def test_no_lookup_before_the_scope_check_passes(
    ebull_test_conn: psycopg.Connection[Any], scope: str | None, reason: str
) -> None:
    _seed(ebull_test_conn, scope=scope)
    broker = _FakeBroker({})
    report = _run(broker)
    assert (report.state, report.reason, broker.asked) == ("refused", reason, [])
    assert _rows("SELECT count(*) FROM tax_order_lookups") == [(0,)]
    assert _rows("SELECT state, reason FROM tax_evidence_runs") == [("refused", reason)]


def test_the_lock_skips_a_second_run_and_a_stale_run_is_abandoned(ebull_test_conn: psycopg.Connection[Any]) -> None:
    _seed(ebull_test_conn)
    broker = _FakeBroker({})
    with _connect() as holder:
        holder.execute("SELECT pg_advisory_lock(%s)", [TAX_EVIDENCE_LOCK_KEY])
        assert _run(broker).state == "skipped"
        holder.execute("SELECT pg_advisory_unlock(%s)", [TAX_EVIDENCE_LOCK_KEY])
    assert broker.asked == [] and _rows("SELECT count(*) FROM tax_evidence_runs") == [(0,)]
    # A run whose process died left its row in progress; the next run marks it abandoned.
    ebull_test_conn.execute(
        "INSERT INTO tax_evidence_runs (environment, started_at, state, evidence_policy_id) "
        "VALUES ('demo', now(), 'in_progress', 'broker_evidence_v1')"
    )
    ebull_test_conn.commit()
    with _connect() as conn:
        run_tax_broker_evidence(conn, broker, "demo")
        row = conn.execute("SELECT pg_try_advisory_lock(%s)", [TAX_EVIDENCE_LOCK_KEY]).fetchone()
        assert row is not None and row[0] is True  # released after the run
    assert _rows("SELECT state FROM tax_evidence_runs ORDER BY run_id") == [("abandoned",), ("refused",)]


def test_a_nul_body_is_kept_as_its_hash(ebull_test_conn: psycopg.Connection[Any]) -> None:
    _seed(ebull_test_conn)
    _run(_FakeBroker({7: RawOrderLookup(200, '{"orderId": 7}\x00', None)}))
    [(outcome, body, sha, detail)] = _rows(
        "SELECT outcome, body_text, body_sha256, error_detail FROM tax_order_lookups WHERE order_id = 7"
    )
    assert (outcome, body, sha) == ("malformed", None, None)
    assert detail.startswith("body not stored: contains NUL (sha256 ")


class _BrokenBroker(_FakeBroker):
    def lookup_order_raw(self, order_id: int) -> RawOrderLookup:
        self.asked.append(order_id)
        raise KeyError("defect")


def test_an_unexpected_exception_is_recorded_then_raised(ebull_test_conn: psycopg.Connection[Any]) -> None:
    _seed(ebull_test_conn)
    with _connect() as conn:
        with pytest.raises(KeyError):
            run_tax_broker_evidence(conn, _BrokenBroker({}), "demo")
        # The lock is free and the connection runs another query.
        row = conn.execute("SELECT pg_try_advisory_lock(%s)", [TAX_EVIDENCE_LOCK_KEY]).fetchone()
        assert row is not None and row[0] is True
        conn.execute("SELECT pg_advisory_unlock(%s)", [TAX_EVIDENCE_LOCK_KEY])
    assert _rows("SELECT outcome, http_status, error_detail FROM tax_order_lookups") == [
        ("transport_error", None, "KeyError: 'defect'")
    ]
    assert _rows("SELECT state, reason FROM tax_evidence_runs") == [("abandoned", "KeyError: 'defect'")]


# --- publication (Design 6) -------------------------------------------------------------------------------------


def test_a_run_publishes_one_generation_and_a_later_one_leaves_it_unchanged(
    ebull_test_conn: psycopg.Connection[Any], published_ledger: int
) -> None:
    first = _run(_FakeBroker({}))
    assert (first.state, first.counts) == (
        "published",
        {"admitted": 1, "excluded_non_share_product": 0, "unresolved": 0, "acquisitions": 1, "disposals": 0},
    )
    lot_sql = (
        "SELECT run_id, lot_key, units, usd_amount, fx_status, fx_rate_date::text, amount_gbp IS NOT NULL, "
        "execution_evidence, fingerprint FROM tax_evidence_lots ORDER BY run_id, lot_key"
    )
    first_lots = _rows(lot_sql)
    assert [row[1:8] for row in first_lots] == [("acq:demo:1000", 1, 100, "published", "2026-10-01", True, "sync_only")]
    assert _rows("SELECT subject, outcome, reasons, held_positions FROM tax_evidence_subjects") == [
        ("order:1000", "admitted", [], {"21": {"units": "1", "amount": "100", "cid": ACCOUNT}})
    ]
    # Slice 3d-3b: the v3 derivation always writes A8's predicate, empty when no totalFees is non-zero.
    assert _rows("SELECT total_fees_nonzero_position_ids FROM tax_evidence_subjects") == [([],)]
    assert _rows(
        "SELECT state, account_id, trade_events_max_event_id IS NOT NULL, "
        "history_fetch_latest_id = history_fetch_ok_id, fx_run_id IS NOT NULL, evidence_policy_id "
        "FROM tax_evidence_runs"
    ) == [("published", ACCOUNT, True, True, True, TAX_EVIDENCE_POLICY_ID)]

    _seed_held_order(ebull_test_conn, published_ledger, 22, 1001)
    ebull_test_conn.commit()
    second = _run(_FakeBroker({}))
    assert second.state == "published" and second.counts["admitted"] == 2
    lots = _rows(lot_sql)
    assert [row for row in lots if row[0] == first.run_id] == first_lots
    # The unchanged lot carries the same fingerprint in both generations.
    assert {row[8] for row in lots if row[1] == "acq:demo:1000"} == {first_lots[0][8]}
    with _connect() as conn, pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        conn.execute("DELETE FROM tax_evidence_lots")
    with _connect() as conn, pytest.raises(psycopg.errors.RaiseException, match="cannot change"):
        conn.execute("UPDATE tax_evidence_runs SET reason = 'x'")


def test_fx_state_changing_before_publication_refuses(
    ebull_test_conn: psycopg.Connection[Any], published_ledger: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    del ebull_test_conn, published_ledger
    monkeypatch.setattr(tax_broker_evidence, "tax_fx_state_is_current", lambda conn, state: False)
    report = _run(_FakeBroker({}))
    assert (report.state, report.reason) == ("refused", "fx_state_changed")
    assert _rows("SELECT state, reason FROM tax_evidence_runs") == [("refused", "fx_state_changed")]
    assert _rows("SELECT (SELECT count(*) FROM tax_evidence_subjects), (SELECT count(*) FROM tax_evidence_lots)") == [
        (0, 0)
    ]


@pytest.mark.parametrize(
    ("fetch", "expected"),
    [
        ({"outcome": "scope_refused", "account": None}, "portfolio_out_of_scope"),
        ({"environment": "real"}, "portfolio_out_of_scope"),
        ({"account": ACCOUNT + 1}, "portfolio_out_of_scope"),
        ({"outcome": "scope_unverified", "account": None, "real_positions": 0}, None),
    ],
)
def test_the_latest_refresh_must_be_the_scopes(
    ebull_test_conn: psycopg.Connection[Any], published_ledger: int, fetch: dict[str, Any], expected: str | None
) -> None:
    del published_ledger
    _fetch_row(ebull_test_conn, **fetch)
    ebull_test_conn.commit()
    report = _run(_FakeBroker({}))
    assert (report.state, report.reason) == (("refused", expected) if expected else ("published", None))


def test_a_held_position_of_another_account_refuses(
    ebull_test_conn: psycopg.Connection[Any], published_ledger: int
) -> None:
    del published_ledger
    ebull_test_conn.execute(
        "UPDATE broker_positions SET raw_payload = jsonb_set(raw_payload, '{CID}', to_jsonb(%s::bigint))",
        (ACCOUNT + 1,),
    )
    ebull_test_conn.commit()
    assert _run(_FakeBroker({})).reason == "positions_out_of_scope"
