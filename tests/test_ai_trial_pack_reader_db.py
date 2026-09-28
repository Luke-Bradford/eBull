"""#3471 slice 1b-iii — the AI-trial pack reader against real SQL (``app/services/ai_trial_pack_reader``).

One seeded world, two mechanisms: the §3 step-1 gates at several ``as_of`` values, and the
end-to-end pack with its knowledge-time bounds (O2) and per-name incompleteness (O1).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.providers.market_data import IntradayBar
from app.services import ai_trial_pack_reader as r
from app.services.ai_trial_pack import canonical_sha256
from app.services.scoring import _DEFAULT_MODEL_VERSION

Conn = psycopg.Connection[Any]

AS_OF = datetime(2026, 10, 2, 23, 30, tzinfo=UTC)  # Friday; the session closed 20:00Z
PINNED_RUN = datetime(2026, 10, 1, 22, 0, tzinfo=UTC)
LATER_RUN = datetime(2026, 10, 4, 12, 0, tzinfo=UTC)  # after AS_OF: must not be read
AAA, BBB, CCC, DDD, EEE = 1, 2, 3, 4, 5


def _sessions(last: date, n: int) -> list[date]:
    out: list[date] = []
    day = last
    while len(out) < n:
        if day.weekday() < 5:
            out.append(day)
        day -= timedelta(days=1)
    return out[::-1]


def _seed(conn: Conn) -> None:
    conn.execute("INSERT INTO exchanges (exchange_id, asset_class) VALUES ('ait3471', 'us_equity')")
    for iid, symbol, tradable in (
        (AAA, "AAA", True),  # complete
        (BBB, "BBB", True),  # last bar is a session behind → stale_last_bar
        (CCC, "CCC", True),  # intraday fetch raises → intraday_fetch_failed
        (DDD, "DDD", True),  # spread 2% → not eligible, not shortlisted
        (EEE, "EEE", False),  # not tradable
    ):
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable, exchange) "
            "VALUES (%s, %s, %s, %s, 'ait3471')",
            (iid, symbol, f"{symbol} Inc", tradable),
        )
        conn.execute(
            "INSERT INTO quotes (instrument_id, quoted_at, bid, ask) VALUES (%s, %s, %s, %s)",
            (iid, AS_OF - timedelta(hours=1), Decimal("20"), Decimal("20.40") if iid == DDD else Decimal("20.02")),
        )
        for scored_at, score in ((PINNED_RUN, 0.9 - iid / 100), (LATER_RUN, 0.1 + iid / 100)):
            conn.execute(
                "INSERT INTO scores (instrument_id, model_version, scored_at, rank, total_score, quality_score, "
                "value_score, turnaround_score, momentum_score, sentiment_score, confidence_score) "
                "VALUES (%s, %s, %s, %s, %s, 0.5, 0.4, 0.3, 0.2, 0.1, 0.5)",
                (iid, _DEFAULT_MODEL_VERSION, scored_at, iid, score),
            )
        last = date(2026, 10, 1) if iid == BBB else date(2026, 10, 2)
        for k, day in enumerate(_sessions(last, 80)):
            conn.execute(
                "INSERT INTO price_daily (instrument_id, price_date, open, high, low, close, volume) "
                "VALUES (%s, %s, %s, %s, %s, %s, 1000)",
                (iid, day, 20 + k / 10, 21 + k / 10, 19 + k / 10, 20.5 + k / 10),
            )
    for started, status in (
        (datetime(2026, 10, 2, 21, 52, tzinfo=UTC), "complete"),
        (datetime(2026, 10, 3, 21, 52, tzinfo=UTC), "complete"),
    ):
        row = conn.execute(
            "INSERT INTO etoro_crowd_snapshots (started_at, finished_at, status, request_params, pages, "
            "reported_total_items, discarded_items, recorded_items) "
            "VALUES (%s, %s, %s, '{}', 1, 1, 0, 1) RETURNING snapshot_id",
            (started, started + timedelta(seconds=7), status),
        ).fetchone()
        assert row is not None
        conn.execute(
            "INSERT INTO etoro_crowd_observations (snapshot_id, instrument_id, observed_at, buy_holding_pct, "
            "sell_holding_pct, traders_change_7d, raw) VALUES (%s, %s, %s, 91, 9, 2.5, '{}')",
            (row[0], AAA, started),
        )
    for filed, created, items, accession in (
        (date(2026, 10, 1), AS_OF - timedelta(hours=20), ["2.02", "9.01"], "0000000001-26-000001"),
        (date(2026, 10, 2), AS_OF + timedelta(hours=1), ["8.01"], "0000000001-26-000002"),  # ingested later
        (date(2026, 8, 1), AS_OF - timedelta(days=60), ["7.01"], "0000000001-26-000003"),  # outside 30d
    ):
        conn.execute(
            "INSERT INTO filing_events (instrument_id, filing_date, filing_type, provider, provider_filing_id, "
            "items, created_at) VALUES (%s, %s, '8-K', 'sec', %s, %s, %s)",
            (AAA, filed, accession, items, created),
        )
    for headline, event, created in (
        ("AAA beats\x07 estimates", AS_OF - timedelta(days=1), AS_OF - timedelta(hours=12)),
        ("AAA late story", AS_OF - timedelta(hours=2), AS_OF + timedelta(minutes=5)),  # ingested later
    ):
        conn.execute(
            "INSERT INTO news_events (instrument_id, event_time, headline, url_hash, created_at) "
            "VALUES (%s, %s, %s, %s, %s)",
            (AAA, event, headline, headline, created),
        )
    conn.commit()


def _fetch(instrument_id: int) -> Sequence[IntradayBar]:
    if instrument_id == CCC:
        raise RuntimeError("provider down")
    start = AS_OF - timedelta(days=40)
    return [
        IntradayBar(start + timedelta(hours=4 * k), Decimal("20"), Decimal("21"), Decimal("19"), Decimal("20.5"), 50)
        for k in range(240)
    ]


@pytest.mark.parametrize(
    ("as_of", "refusal"),
    [
        (datetime(2026, 9, 19, 12, 0, tzinfo=UTC), "scores_run_missing"),
        (datetime(2026, 10, 8, 23, 30, tzinfo=UTC), "scores_run_stale"),  # LATER_RUN is 4.5 days old
        (datetime(2026, 10, 5, 21, 0, tzinfo=UTC), "price_daily_stale"),  # 10-05 closed, bars end 10-02
        (datetime(2026, 10, 2, 21, 0, tzinfo=UTC), "crowd_snapshot_missing"),  # snapshot starts 21:52
    ],
)
def test_step1_gates_refuse(ebull_test_conn: Conn, as_of: datetime, refusal: str) -> None:
    _seed(ebull_test_conn)
    assert r.read_step1(ebull_test_conn, as_of=as_of).refusal == refusal


def test_pack_is_point_in_time_and_drops_incomplete_names(ebull_test_conn: Conn) -> None:
    _seed(ebull_test_conn)
    step1 = r.read_step1(ebull_test_conn, as_of=AS_OF)
    assert step1.refusal is None
    assert step1.scores_run == r.ScoresRun(_DEFAULT_MODEL_VERSION, PINNED_RUN)  # not LATER_RUN
    assert (step1.last_session, step1.session_date) == (date(2026, 10, 2), date(2026, 10, 5))

    account = r.AccountContext(open_positions=(), free_slots=4, max_new_entries=2)
    fetched = datetime(2026, 10, 2, 23, 31, tzinfo=UTC)
    got = r.assemble_pack(ebull_test_conn, step1=step1, account=account, fetch_intraday=_fetch, clock=lambda: fetched)

    assert got.complete == {"AAA": AAA}
    assert got.incomplete == {"BBB": "stale_last_bar", "CCC": "intraday_fetch_failed"}
    assert got.sha256 == canonical_sha256(got.pack)
    pack = got.pack
    assert pack["eligible_count"] == 3  # DDD's spread and EEE's tradability exclude them
    assert pack["crowd_snapshot_id"] is not None and pack["scores_run"]["scored_at"] == PINNED_RUN
    (name,) = pack["names"]
    assert len(name["bars"]) == 60 and name["bars"][-1]["d"] == date(2026, 10, 2)
    assert name["indicators"]["sma200"] is None  # 80 bars
    assert name["crowd"] == {
        "buy_holding_pct": Decimal("91"),
        "sell_holding_pct": Decimal("9"),
        "traders_change_7d": Decimal("2.5"),
    }
    # Knowledge time: the filing and the headline ingested after as_of are absent.
    assert [f["title"] for f in name["filings"]] == ["8-K filed 2026-10-01 (items 2.02, 9.01)"]
    assert [n["title"] for n in name["news"]] == ["AAA beats estimates"]
    assert name["ranking"]["families"]["quality"] == Decimal("0.5")
    intraday = name["intraday"]
    assert intraday["fetched_at"] == fetched and intraday["returned_count"] == 240
    assert all(b["t"] + r.INTRADAY_BAR_LENGTH <= AS_OF for b in intraday["bars"])
    assert min(b["t"] for b in intraday["bars"]) >= AS_OF - r.INTRADAY_WINDOW
