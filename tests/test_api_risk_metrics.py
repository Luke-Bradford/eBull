"""DB-tier tests for GET /instruments/{symbol}/risk-metrics (#591 PR-B, Task B6).

Seeds synthetic price_daily for a subject instrument + a fake SPY, runs
``compute_and_store_risk_metrics`` to persist the two-layer tables, then drives
the endpoint via a TestClient whose ``get_conn`` yields the test connection.

Asserts the honest-status contract: populated windows + non-empty series for a
long clean series; FLAGGED (not zeroed) statuses for thin history; null beta +
``benchmark_missing`` when SPY is absent; 404 for an unknown symbol.

Auto-marked ``db`` (pulls ``ebull_test_conn`` + TestClient).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, timedelta

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app
from app.services.risk_metrics import (
    RISK_METRICS_VERSION,
    compute_and_store_risk_metrics,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

# Fixed synthetic ids well outside any real-data range.
_SPY_ID = 991001
_SUBJECT_ID = 991002
_THIN_ID = 991003
_NOBENCH_ID = 991004

# Anchor the series ~5 days in the past so trailing windows resolve and
# CURRENT_DATE is always after the last bar.
_END = date.today() - timedelta(days=5)


@pytest.fixture
def client(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> Iterator[TestClient]:
    """TestClient with get_conn overridden to yield the test connection."""

    def _dep() -> Iterator[psycopg.Connection[tuple]]:
        yield ebull_test_conn

    app.dependency_overrides[get_conn] = _dep
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_conn, None)


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, currency, country, is_tradable)
        VALUES (%s, %s, %s, 'USD', 'US', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} co"),
    )


def _seed_series(
    conn: psycopg.Connection[tuple],
    iid: int,
    closes: list[float | None],
    end: date = _END,
) -> None:
    """Insert ``closes`` ending at ``end`` (one calendar day apart, ASC)."""
    n = len(closes)
    for i, close in enumerate(closes):
        d = end - timedelta(days=(n - 1 - i))
        conn.execute(
            """
            INSERT INTO price_daily (instrument_id, price_date, close)
            VALUES (%s, %s, %s)
            ON CONFLICT (instrument_id, price_date) DO UPDATE SET close = EXCLUDED.close
            """,
            (iid, d, close),
        )


def _clean_series(n: int, start: float = 100.0, step: float = 0.5) -> list[float | None]:
    """A monotone-up clean series of ``n`` closes (no NULLs, all > 0)."""
    return [start + step * i for i in range(n)]


def _window(body: dict[str, object], key: str) -> dict[str, object]:
    for w in body["windows"]:  # type: ignore[union-attr]
        if w["window_key"] == key:  # type: ignore[index]
            return w  # type: ignore[return-value]
    raise AssertionError(f"window {key!r} not present in {[w['window_key'] for w in body['windows']]}")  # type: ignore[index,union-attr]


def test_data_symbol_returns_populated_windows_and_series(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Long clean series w/ SPY → 200, windows populated, statuses ok, series non-empty."""
    conn = ebull_test_conn
    _seed_instrument(conn, _SPY_ID, "SPY")
    _seed_instrument(conn, _SUBJECT_ID, "FAKE")
    _seed_series(conn, _SPY_ID, _clean_series(400, start=400.0, step=0.3))
    _seed_series(conn, _SUBJECT_ID, _clean_series(400, start=100.0, step=0.5))
    conn.commit()
    compute_and_store_risk_metrics(conn)
    conn.commit()

    resp = client.get("/instruments/FAKE/risk-metrics")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["symbol"] == "FAKE"
    assert body["metric_version"] == RISK_METRICS_VERSION
    assert body["benchmark_symbol"] == "SPY"
    assert body["as_of_date"] == _END.isoformat()
    assert {w["window_key"] for w in body["windows"]} == {"1y", "3y", "full"}
    # Windows are ordered shortest → full.
    assert [w["window_key"] for w in body["windows"]] == ["1y", "3y", "full"]

    full = _window(body, "full")
    assert full["cagr"] is not None
    assert full["vol_annualized"] is not None
    assert full["max_drawdown"] is not None
    # 400-row clean series clears every "ok" threshold.
    assert full["cagr_status"] == "ok"
    assert full["vol_status"] == "ok"
    assert full["drawdown_status"] == "ok"
    assert full["distribution_status"] == "ok"
    assert full["beta_status"] == "ok"
    # No metric coerced to zero — they are real fractions.

    series = body["series"]
    assert series is not None
    assert len(series["drawdown_curve"]) > 0
    assert len(series["rolling_vol"]) > 0
    assert len(series["return_histogram"]) > 0
    assert len(series["beta_scatter"]) > 0
    assert series["beta"] is not None
    # Beta scatter pairs are aligned (spy, inst).
    pt = series["beta_scatter"][0]
    assert "spy_return" in pt and "inst_return" in pt


def test_insufficient_history_flags_status_not_zero(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Thin (10-close) history → 200, windows present, FLAGGED statuses, not zeros."""
    conn = ebull_test_conn
    _seed_instrument(conn, _SPY_ID, "SPY")
    _seed_instrument(conn, _THIN_ID, "THIN")
    _seed_series(conn, _SPY_ID, _clean_series(400, start=400.0, step=0.3))
    _seed_series(conn, _THIN_ID, _clean_series(10, start=50.0, step=0.2))
    conn.commit()
    compute_and_store_risk_metrics(conn)
    conn.commit()

    resp = client.get("/instruments/THIN/risk-metrics")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert {w["window_key"] for w in body["windows"]} == {"1y", "3y", "full"}

    full = _window(body, "full")
    # 9 returns < thresholds → flagged, NOT 'ok', NOT fabricated zeros.
    assert full["vol_status"] == "insufficient_history"
    assert full["cagr_status"] == "partial_window"
    assert full["distribution_status"] == "partial_window"
    assert full["beta_status"] == "benchmark_insufficient_history"
    # Monotone-up → zero drawdown → calmar mathematically null (not zero).
    assert full["calmar"] is None


def test_missing_benchmark_beta_null_status_flagged(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """No SPY rows at all → beta null + beta_status flagged benchmark_missing, not 0."""
    conn = ebull_test_conn
    # Deliberately do NOT seed SPY.
    _seed_instrument(conn, _NOBENCH_ID, "NOBENCH")
    _seed_series(conn, _NOBENCH_ID, _clean_series(400, start=100.0, step=0.5))
    conn.commit()
    compute_and_store_risk_metrics(conn)
    conn.commit()

    resp = client.get("/instruments/NOBENCH/risk-metrics")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # No benchmark resolved.
    assert body["benchmark_symbol"] is None
    full = _window(body, "full")
    assert full["beta"] is None
    assert full["beta_status"] == "benchmark_missing"
    assert full["excess_cagr_vs_spy"] is None
    assert full["excess_cagr_status"] == "benchmark_missing"
    # Series still computes the instrument-only curves; beta fit is null.
    series = body["series"]
    assert series is not None
    assert len(series["drawdown_curve"]) > 0
    assert series["beta"] is None
    assert series["beta_scatter"] == []


def test_unknown_symbol_returns_404(client: TestClient) -> None:
    resp = client.get("/instruments/NOTREAL/risk-metrics")
    assert resp.status_code == 404


def test_never_computed_instrument_returns_empty_payload(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Instrument exists but has no persisted risk rows → 200, empty windows, null as_of."""
    conn = ebull_test_conn
    _seed_instrument(conn, 991009, "EMPTY")
    conn.commit()

    resp = client.get("/instruments/EMPTY/risk-metrics")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["symbol"] == "EMPTY"
    assert body["as_of_date"] is None
    assert body["windows"] == []
    assert body["series"] is None
    assert body["metric_version"] == RISK_METRICS_VERSION
