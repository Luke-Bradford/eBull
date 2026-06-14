"""DB-tier tests for the risk-metrics persist layer (#591 PR-B, Task B3).

Exercises ``compute_and_store_risk_metrics`` against a real Postgres:
content-dedup append into the append-only observations log, deterministic
rebuild of the _current write-through, and the correction-advances-current
audit path (the rev2 BLOCKER scenario). Auto-marked ``db`` (pulls
``ebull_test_conn``).

Synthetic instruments only — a fake subject + a fake SPY (resolved by symbol
the same way the candle-refresh scope resolves benchmarks).
"""

from __future__ import annotations

from datetime import date, timedelta

import psycopg
import psycopg.rows

from app.services.risk_metrics import (
    RISK_METRICS_VERSION,
    WINDOW_KEYS,
    compute_and_store_risk_metrics,
)

# Fixed synthetic ids well outside any real-data range.
_SPY_ID = 990001
_SUBJECT_ID = 990002
_THIN_ID = 990003
_ONE_CLOSE_ID = 990004

# Anchor the series in the recent past so trailing windows resolve but the
# data is deterministic. End ~5 days ago so CURRENT_DATE is always after it.
_END = date.today() - timedelta(days=5)


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


def _count_obs(conn: psycopg.Connection[tuple], iid: int, window: str = "1y") -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) FROM instrument_risk_metrics_observations
        WHERE instrument_id = %s AND metric_version = %s AND window_key = %s
        """,
        (iid, RISK_METRICS_VERSION, window),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _current_row(conn: psycopg.Connection[tuple], iid: int, window: str = "1y") -> dict[str, object] | None:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT * FROM instrument_risk_metrics_current
            WHERE instrument_id = %s AND metric_version = %s AND window_key = %s
            """,
            (iid, RISK_METRICS_VERSION, window),
        )
        return cur.fetchone()


def test_long_clean_series_persists_ok_status(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """(a) A long clean series → observation + current rows with non-null
    scalars and *_status='ok'."""
    conn = ebull_test_conn
    _seed_instrument(conn, _SPY_ID, "SPY")
    _seed_instrument(conn, _SUBJECT_ID, "FAKE")
    # 400 closes → > MIN_RETURNS_ANNUALIZED (252) and > MIN_OBS_MOMENTS (250).
    _seed_series(conn, _SPY_ID, _clean_series(400, start=400.0, step=0.3))
    _seed_series(conn, _SUBJECT_ID, _clean_series(400, start=100.0, step=0.5))
    conn.commit()

    written = compute_and_store_risk_metrics(conn)
    assert written > 0

    # One observation row per window for the subject.
    for window in WINDOW_KEYS:
        assert _count_obs(conn, _SUBJECT_ID, window) == 1

    cur = _current_row(conn, _SUBJECT_ID, "full")
    assert cur is not None
    assert cur["cagr"] is not None
    assert cur["vol_annualized"] is not None
    assert cur["max_drawdown"] is not None
    # 400-row clean series clears every "ok" threshold.
    assert cur["cagr_status"] == "ok"
    assert cur["vol_status"] == "ok"
    assert cur["drawdown_status"] == "ok"
    assert cur["distribution_status"] == "ok"
    assert cur["beta_status"] == "ok"
    # Beta is computed against SPY → benchmark_instrument_id carried.
    assert cur["benchmark_instrument_id"] == _SPY_ID
    assert cur["as_of_date"] == _END


def test_rerun_identical_data_no_new_observation(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """(b) Re-run with identical data → NO new observation row (content-dedup);
    current unchanged."""
    conn = ebull_test_conn
    _seed_instrument(conn, _SPY_ID, "SPY")
    _seed_instrument(conn, _SUBJECT_ID, "FAKE")
    _seed_series(conn, _SPY_ID, _clean_series(400, start=400.0, step=0.3))
    _seed_series(conn, _SUBJECT_ID, _clean_series(400, start=100.0, step=0.5))
    conn.commit()

    compute_and_store_risk_metrics(conn)
    before = _count_obs(conn, _SUBJECT_ID, "1y")
    cur_before = _current_row(conn, _SUBJECT_ID, "1y")
    assert cur_before is not None
    refreshed_before = cur_before["refreshed_at"]

    # Identical re-run.
    compute_and_store_risk_metrics(conn)
    after = _count_obs(conn, _SUBJECT_ID, "1y")
    assert after == before  # no new observation

    cur_after = _current_row(conn, _SUBJECT_ID, "1y")
    assert cur_after is not None
    # No-op rebuild must not churn refreshed_at (prevention-log MERGE-bloat).
    assert cur_after["refreshed_at"] == refreshed_before


def test_correction_appends_new_observation_and_advances_current(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """(c) Correct a HISTORICAL close (latest date unchanged) + re-run →
    a NEW observation row for the same as_of_date AND current advances to it.

    This is the rev2 BLOCKER scenario: a vendor correction to a past bar that
    does not move the latest close date still produces different metrics; the
    append-only log must record it and _current must advance.
    """
    conn = ebull_test_conn
    _seed_instrument(conn, _SPY_ID, "SPY")
    _seed_instrument(conn, _SUBJECT_ID, "FAKE")
    _seed_series(conn, _SPY_ID, _clean_series(400, start=400.0, step=0.3))
    subject = _clean_series(400, start=100.0, step=0.5)
    _seed_series(conn, _SUBJECT_ID, subject)
    conn.commit()

    compute_and_store_risk_metrics(conn)
    obs_before = _count_obs(conn, _SUBJECT_ID, "full")
    cur_before = _current_row(conn, _SUBJECT_ID, "full")
    assert cur_before is not None
    vol_before = cur_before["vol_annualized"]
    computed_at_before = cur_before["computed_at"]
    as_of_before = cur_before["as_of_date"]

    # Correct a HISTORICAL bar (index 100) — a big spike injects volatility but
    # leaves the LATEST close (index 399) and therefore as_of_date unchanged.
    corrected = list(subject)
    corrected[100] = 100.0 + 0.5 * 100 + 75.0  # large upward correction
    _seed_series(conn, _SUBJECT_ID, corrected)
    conn.commit()

    compute_and_store_risk_metrics(conn)
    obs_after = _count_obs(conn, _SUBJECT_ID, "full")
    # A NEW observation row appended for the SAME as_of_date.
    assert obs_after == obs_before + 1

    cur_after = _current_row(conn, _SUBJECT_ID, "full")
    assert cur_after is not None
    # as_of_date unchanged (correction did not move the latest close).
    assert cur_after["as_of_date"] == as_of_before
    # current advanced to the corrected computation: new computed_at, new vol.
    assert cur_after["computed_at"] != computed_at_before
    assert cur_after["vol_annualized"] != vol_before


def test_thin_history_persists_flagged_not_absent(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """(d) Thin-history instrument (10 closes) → row PERSISTED with FLAGGED
    statuses, not absent.

    Contract note: the compute layer computes a scalar whenever it is
    mathematically possible (cagr/vol from >= 2 valid closes) and flags
    *reliability* via the per-metric status — it does NOT null the scalar just
    because the window is short. The endpoint therefore gets a flagged-not-
    absent row, which is the required behaviour. We assert the row exists and
    the short-history statuses are flagged (not 'ok'); a genuinely-null scalar
    only appears when a metric is mathematically uncomputable (e.g. calmar with
    a zero drawdown on a monotone-up series).
    """
    conn = ebull_test_conn
    _seed_instrument(conn, _SPY_ID, "SPY")
    _seed_instrument(conn, _THIN_ID, "THIN")
    _seed_series(conn, _SPY_ID, _clean_series(400, start=400.0, step=0.3))
    _seed_series(conn, _THIN_ID, _clean_series(10, start=50.0, step=0.2))
    conn.commit()

    compute_and_store_risk_metrics(conn)

    # Row EXISTS (flagged-not-absent) for every window.
    for window in WINDOW_KEYS:
        assert _count_obs(conn, _THIN_ID, window) == 1

    cur = _current_row(conn, _THIN_ID, "full")
    assert cur is not None
    # 9 returns < MIN_RETURNS_ANNUALIZED (252) → cagr flagged partial_window.
    assert cur["cagr_status"] == "partial_window"
    # 9 returns < MIN_RETURNS_VOL_BETA (60) → vol flagged insufficient_history.
    assert cur["vol_status"] == "insufficient_history"
    # Aligned overlap with SPY too short → beta flagged.
    assert cur["beta_status"] == "benchmark_insufficient_history"
    # 9 returns < MIN_OBS_MOMENTS (250) → distribution flagged partial_window.
    assert cur["distribution_status"] == "partial_window"
    # Monotone-up series → zero drawdown → calmar mathematically null (epsilon
    # guard), flagged via calmar_status. This is the genuinely-null metric.
    assert cur["calmar"] is None
    assert cur["calmar_status"] == "partial_window"


def test_trailing_columns_non_null_for_long_series(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """(f) BUG B: trailing_* and excess_trailing_* persist non-null for a long
    series with SPY (the columns previously always wrote NULL)."""
    conn = ebull_test_conn
    _seed_instrument(conn, _SPY_ID, "SPY")
    _seed_instrument(conn, _SUBJECT_ID, "FAKE")
    _seed_series(conn, _SPY_ID, _clean_series(400, start=400.0, step=0.3))
    _seed_series(conn, _SUBJECT_ID, _clean_series(400, start=100.0, step=0.5))
    conn.commit()

    compute_and_store_risk_metrics(conn)

    cur = _current_row(conn, _SUBJECT_ID, "full")
    assert cur is not None
    for col in ("trailing_1m", "trailing_3m", "trailing_6m", "trailing_1y"):
        assert cur[col] is not None, f"{col} should be populated"
    for col in (
        "excess_trailing_1m",
        "excess_trailing_3m",
        "excess_trailing_6m",
        "excess_trailing_1y",
    ):
        assert cur[col] is not None, f"{col} should be populated (SPY present)"

    # Trailing scalars are window-INDEPENDENT → identical across window rows.
    one_y = _current_row(conn, _SUBJECT_ID, "1y")
    assert one_y is not None
    assert one_y["trailing_1m"] == cur["trailing_1m"]
    assert one_y["trailing_1y"] == cur["trailing_1y"]


def test_window_rows_differ_in_db(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """(g) BUG A: the 1y and full rows in instrument_risk_metrics_current have
    DIFFERENT vol/cagr (they were identical before the window-slice fix).

    The series has a quiet first ~2 years and a volatile last year so the 1y
    slice is materially different from the full series.
    """
    import random

    conn = ebull_test_conn
    _seed_instrument(conn, _SPY_ID, "SPY")
    _seed_instrument(conn, _SUBJECT_ID, "FAKE")
    _seed_series(conn, _SPY_ID, _clean_series(1100, start=400.0, step=0.1))

    rng = random.Random(99)
    price = 100.0
    subject: list[float | None] = []
    for i in range(1100):
        if i < 735:
            price *= 1.0 + rng.uniform(-0.001, 0.0015)  # quiet
        else:
            price *= 1.0 + rng.uniform(-0.05, 0.06)  # volatile last ~1y
        subject.append(price)
    _seed_series(conn, _SUBJECT_ID, subject)
    conn.commit()

    compute_and_store_risk_metrics(conn)

    one_y = _current_row(conn, _SUBJECT_ID, "1y")
    full = _current_row(conn, _SUBJECT_ID, "full")
    assert one_y is not None and full is not None
    assert one_y["vol_annualized"] is not None and full["vol_annualized"] is not None
    assert one_y["vol_annualized"] != full["vol_annualized"]
    assert one_y["cagr"] is not None and full["cagr"] is not None
    assert one_y["cagr"] != full["cagr"]
    # n_returns / window_days evidence reflects the slice too.
    assert int(one_y["n_returns"]) < int(full["n_returns"])
    assert one_y["window_days"] is not None and full["window_days"] is not None
    assert int(one_y["window_days"]) < int(full["window_days"])


def test_fewer_than_two_valid_closes_no_row(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """(e) Instrument with < 2 valid closes → no row at all."""
    conn = ebull_test_conn
    _seed_instrument(conn, _SPY_ID, "SPY")
    _seed_instrument(conn, _ONE_CLOSE_ID, "ONE")
    _seed_series(conn, _SPY_ID, _clean_series(400, start=400.0, step=0.3))
    # A single valid close (1 < 2) → out of scope.
    _seed_series(conn, _ONE_CLOSE_ID, [123.45])
    conn.commit()

    compute_and_store_risk_metrics(conn)

    assert _count_obs(conn, _ONE_CLOSE_ID, "1y") == 0
    assert _current_row(conn, _ONE_CLOSE_ID, "1y") is None
