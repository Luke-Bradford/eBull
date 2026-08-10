"""#2448 bounded strategy observation storage contracts."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import pytest

from app.services.signal_ledger import LedgerRow
from app.services.strategy_observation_storage import (
    INTRADAY_TIERS,
    IntradayBar,
    drop_expired_partitions,
    ensure_intraday_partition,
    ensure_signal_partition,
    store_intraday_bars,
    store_strategy_observations,
)
from scripts.verify_2437_observation_storage import _verify_signal_parity_and_dependencies


@pytest.fixture
def observation_instruments(ebull_test_conn: psycopg.Connection[tuple]) -> tuple[int, int]:
    with ebull_test_conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)
            VALUES (%s, %s, %s, TRUE)
            """,
            [(998801, "OBS1", "Observation One"), (998802, "OBS2", "Observation Two")],
        )
    return 998801, 998802


def _ledger_row(
    *,
    instrument_id: int,
    verdict: str,
    signal_bar_date: date = date(2026, 8, 7),
) -> LedgerRow:
    fired = verdict == "fired"
    not_evaluable = verdict == "not_evaluable"
    return LedgerRow(
        strategy_id="s-storage-test",
        strategy_version="strategy-registry-v1+storage",
        instrument_id=instrument_id,
        signal_bar_date=signal_bar_date,
        signal_kind="entry",
        verdict=verdict,  # type: ignore[arg-type]
        universe="survivor_only",
        input_rule_set_versions={"indicator_series": "indicator-v1"},
        not_evaluable_reason="no_fill_bar" if not_evaluable else None,
        fill_bar_date=date(2026, 8, 8) if fired else None,
        fill_price=Decimal("101.25") if fired else None,
    )


def test_schema_separates_durable_counts_retained_detail_and_intraday_tiers(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    kinds = dict(
        ebull_test_conn.execute(
            """
            SELECT relname, relkind
            FROM pg_class
            WHERE relname IN (
                'strategy_signal_daily_counts',
                'strategy_signal_observations',
                'strategy_intraday_bars',
                'strategy_intraday_bars_30m',
                'strategy_intraday_bars_5m',
                'strategy_intraday_bars_1m'
            )
            """
        ).fetchall()
    )
    assert kinds == {
        "strategy_signal_daily_counts": "r",
        "strategy_signal_observations": "p",
        "strategy_intraday_bars": "p",
        "strategy_intraday_bars_30m": "p",
        "strategy_intraday_bars_5m": "p",
        "strategy_intraday_bars_1m": "p",
    }


def test_signal_batch_keeps_only_fired_detail_durable_and_census_matches(
    ebull_test_conn: psycopg.Connection[tuple], observation_instruments: tuple[int, int]
) -> None:
    first, second = observation_instruments
    rows = [
        _ledger_row(instrument_id=first, verdict="fired"),
        _ledger_row(instrument_id=second, verdict="not_fired"),
        _ledger_row(instrument_id=first, verdict="not_evaluable", signal_bar_date=date(2026, 8, 6)),
    ]

    report = store_strategy_observations(ebull_test_conn, rows)

    assert report.logical_rows == 3
    assert report.fired_rows == 1
    assert report.retained_observation_rows == 2
    assert report.aggregate_rows == 3
    assert report.input_payload_bytes > 0
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_signals WHERE strategy_id = 's-storage-test'"
    ).fetchone() == (1,)
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_signal_observations WHERE strategy_id = 's-storage-test'"
    ).fetchone() == (2,)
    assert ebull_test_conn.execute(
        "SELECT sum(row_count) FROM strategy_signal_daily_counts WHERE strategy_id = 's-storage-test'"
    ).fetchone() == (Decimal("3"),)


def test_signal_split_preserves_one_terminal_verdict_per_logical_key(
    ebull_test_conn: psycopg.Connection[tuple], observation_instruments: tuple[int, int]
) -> None:
    first, _ = observation_instruments
    routine = _ledger_row(instrument_id=first, verdict="not_fired")
    fired = _ledger_row(instrument_id=first, verdict="fired")

    with pytest.raises(ValueError, match="duplicate logical signal"):
        store_strategy_observations(ebull_test_conn, [routine, fired])
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_signal_daily_counts WHERE strategy_id = 's-storage-test'"
    ).fetchone() == (0,)

    store_strategy_observations(ebull_test_conn, [routine])
    with pytest.raises(ValueError, match="conflicts with a verdict in the other storage tier"):
        store_strategy_observations(ebull_test_conn, [fired])
    assert ebull_test_conn.execute(
        "SELECT sum(row_count) FROM strategy_signal_daily_counts WHERE strategy_id = 's-storage-test'"
    ).fetchone() == (Decimal("1"),)


def test_intraday_writer_enforces_caps_alignment_and_completed_bars(
    ebull_test_conn: psycopg.Connection[tuple], observation_instruments: tuple[int, int]
) -> None:
    first, second = observation_instruments
    rows = [
        IntradayBar(
            timeframe="5m",
            bar_time=datetime(2026, 8, 7, 10, 30, tzinfo=UTC),
            instrument_id=instrument_id,
            open=Decimal("100"),
            high=Decimal("102"),
            low=Decimal("99"),
            close=Decimal("101"),
            volume=Decimal("1000"),
            source="fixture",
        )
        for instrument_id in (first, second)
    ]

    report = store_intraday_bars(
        ebull_test_conn,
        rows,
        observed_at=datetime(2026, 8, 7, 10, 31, tzinfo=UTC),
    )
    assert report.rows_written == 2
    assert report.instruments == 2
    assert report.partitions_touched == 1
    assert report.input_payload_bytes > 0
    with pytest.raises(ValueError, match="at or behind stored watermark"):
        store_intraday_bars(
            ebull_test_conn,
            [rows[0]],
            observed_at=datetime(2026, 8, 7, 10, 32, tzinfo=UTC),
        )

    misaligned = replace(rows[0], bar_time=datetime(2026, 8, 7, 10, 32, tzinfo=UTC))
    with pytest.raises(ValueError, match="not aligned"):
        store_intraday_bars(
            ebull_test_conn,
            [misaligned],
            observed_at=datetime(2026, 8, 7, 10, 33, tzinfo=UTC),
        )

    expired = replace(rows[0], bar_time=datetime(2024, 1, 1, 10, 30, tzinfo=UTC))
    with pytest.raises(ValueError, match="before retained horizon"):
        store_intraday_bars(
            ebull_test_conn,
            [expired],
            observed_at=datetime(2026, 8, 7, 10, 33, tzinfo=UTC),
        )

    cap = INTRADAY_TIERS["1m"].max_instruments
    too_wide = [
        IntradayBar(
            timeframe="1m",
            bar_time=datetime(2026, 8, 7, 10, 30, tzinfo=UTC),
            instrument_id=10_000 + index,
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100"),
            volume=None,
            source="fixture",
        )
        for index in range(cap + 1)
    ]
    with pytest.raises(ValueError, match="cap is 50"):
        store_intraday_bars(
            ebull_test_conn,
            too_wide,
            observed_at=datetime(2026, 8, 7, 10, 31, tzinfo=UTC),
        )


def test_intraday_caps_apply_across_repeated_batches(
    ebull_test_conn: psycopg.Connection[tuple], observation_instruments: tuple[int, int]
) -> None:
    first, _ = observation_instruments
    base = IntradayBar(
        timeframe="30m",
        bar_time=datetime(2026, 8, 7, 9, 0, tzinfo=UTC),
        instrument_id=first,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=None,
        source="fixture",
    )
    first_batch = [
        replace(base, bar_time=base.bar_time.replace(hour=9 + offset // 2, minute=30 * (offset % 2)))
        for offset in range(7)
    ]
    second_batch = [
        replace(base, bar_time=base.bar_time.replace(hour=12 + (offset + 1) // 2, minute=30 * ((offset + 1) % 2)))
        for offset in range(7)
    ]
    store_intraday_bars(
        ebull_test_conn,
        first_batch,
        observed_at=datetime(2026, 8, 7, 12, 1, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="would contain 14 bars"):
        store_intraday_bars(
            ebull_test_conn,
            second_batch,
            observed_at=datetime(2026, 8, 7, 16, 1, tzinfo=UTC),
        )
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_intraday_bars WHERE timeframe = '30m' AND instrument_id = %s",
        (first,),
    ).fetchone() == (7,)

    instrument_ids = list(range(999_000, 999_051))
    with ebull_test_conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)
            VALUES (%s, %s, %s, TRUE)
            """,
            [
                (instrument_id, f"WIDTH{offset}", f"Width {offset}")
                for offset, instrument_id in enumerate(instrument_ids)
            ],
        )
    minute_base = replace(base, timeframe="1m", bar_time=datetime(2026, 8, 7, 10, 0, tzinfo=UTC))
    store_intraday_bars(
        ebull_test_conn,
        [replace(minute_base, instrument_id=instrument_id) for instrument_id in instrument_ids[:25]],
        observed_at=datetime(2026, 8, 7, 10, 1, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="retained universe would contain 51 instruments"):
        store_intraday_bars(
            ebull_test_conn,
            [
                replace(
                    minute_base,
                    instrument_id=instrument_id,
                    bar_time=datetime(2026, 8, 7, 10, 1, tzinfo=UTC),
                )
                for instrument_id in instrument_ids[25:]
            ],
            observed_at=datetime(2026, 8, 7, 10, 2, tzinfo=UTC),
        )
    assert ebull_test_conn.execute("SELECT count(*) FROM strategy_intraday_bars WHERE timeframe = '1m'").fetchone() == (
        25,
    )


def test_retention_drops_whole_expired_partitions_and_keeps_current(
    ebull_test_conn: psycopg.Connection[tuple],
    observation_instruments: tuple[int, int],
) -> None:
    first, _ = observation_instruments
    expired_signal = ensure_signal_partition(ebull_test_conn, date(2020, 1, 15))
    current_signal = ensure_signal_partition(ebull_test_conn, date(2026, 8, 1))
    expired_intraday = ensure_intraday_partition(
        ebull_test_conn,
        "1m",
        datetime(2020, 1, 1, tzinfo=UTC),
    )
    current_intraday = ensure_intraday_partition(
        ebull_test_conn,
        "30m",
        datetime(2026, 8, 1, tzinfo=UTC),
    )
    as_of = datetime(2026, 8, 9, tzinfo=UTC)
    store_strategy_observations(
        ebull_test_conn,
        [_ledger_row(instrument_id=first, verdict="not_fired", signal_bar_date=date(2020, 1, 1))],
    )
    # Deliberate raw fixture: production writes reject already-expired bars, but
    # retention still needs to prove it can clean legacy data and its watermark.
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_intraday_bars (
            timeframe, bar_time, instrument_id, open, high, low, close, source
        ) VALUES ('1m', TIMESTAMPTZ '2020-01-01 10:00:00+00', %s, 100, 101, 99, 100, 'expired-fixture')
        """,
        (first,),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_intraday_watermarks (timeframe, instrument_id, last_bar_time)
        VALUES ('1m', %s, TIMESTAMPTZ '2020-01-01 10:00:00+00')
        """,
        (first,),
    )
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_intraday_gaps (
            universe_version, timeframe, instrument_id, gap_start, gap_end
        ) VALUES (
            'ETORO-RTH-V2', '1m', %s,
            TIMESTAMPTZ '2020-01-01 10:01:00+00',
            TIMESTAMPTZ '2020-01-01 10:02:00+00'
        )
        """,
        (first,),
    )

    plan = drop_expired_partitions(ebull_test_conn, as_of=as_of)
    assert expired_signal in plan.signal_partitions
    assert expired_intraday in plan.intraday_partitions
    assert current_signal not in plan.partitions
    assert current_intraday not in plan.partitions
    assert plan.intraday_gap_rows == 1

    dropped = drop_expired_partitions(ebull_test_conn, as_of=as_of, dry_run=False)
    assert dropped == plan
    assert ebull_test_conn.execute("SELECT to_regclass(%s)", (expired_signal,)).fetchone() == (None,)
    assert ebull_test_conn.execute("SELECT to_regclass(%s)", (expired_intraday,)).fetchone() == (None,)
    assert ebull_test_conn.execute("SELECT to_regclass(%s)", (current_signal,)).fetchone() == (current_signal,)
    assert ebull_test_conn.execute("SELECT to_regclass(%s)", (current_intraday,)).fetchone() == (current_intraday,)
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_signal_daily_counts WHERE strategy_id = 's-storage-test'"
    ).fetchone() == (1,)
    _verify_signal_parity_and_dependencies(ebull_test_conn)
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_scan_watermark (
            strategy_id, strategy_version, frontier_date
        ) VALUES ('s-storage-test', 'strategy-registry-v1+storage', DATE '2020-01-02')
        """
    )
    with pytest.raises(ValueError, match="precedes terminal watermark"):
        store_strategy_observations(
            ebull_test_conn,
            [_ledger_row(instrument_id=first, verdict="fired", signal_bar_date=date(2020, 1, 1))],
        )
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_intraday_watermarks WHERE timeframe = '1m' AND instrument_id = %s",
        (first,),
    ).fetchone() == (0,)
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_intraday_gaps WHERE instrument_id = %s",
        (first,),
    ).fetchone() == (0,)
