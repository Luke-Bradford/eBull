"""#3947 A/B snapshot against real SQL (spec Amendment 1, "Acceptance and rollback, as amended").

The sorted normaliser locks come before the snapshot's first write; a refill inside the snapshot feeds the gated
derivation; a group outcome that is not A3's branch fails the run.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.providers.fundamentals import XbrlFact
from app.services import dps_inline_scale_scan as scan
from app.services import fundamentals
from app.services.fundamentals import DpsGroupOutcome
from scripts import ab_3947_dps_inline_scale as ab
from tests.test_3947_dps_inline_scale_correction_db import Q1_FACT, _seed, _seed_swept
from tests.test_3947_dps_inline_scale_scan_db import ACC, _doc, _Provider, scan_run

Conn = psycopg.Connection[Any]

# The Q3 fact `_seed` writes into raw, as companyfacts serves it.
Q3_FACT = XbrlFact(
    concept="CommonStockDividendsPerShareDeclared",
    taxonomy="us-gaap",
    unit="USD/shares",
    period_start=date(2025, 7, 1),
    period_end=date(2025, 9, 30),
    val=Decimal("390"),
    frame=None,
    accession_number=ACC,
    form_type="10-Q",
    filed_date=date(2025, 11, 1),
    fiscal_year=2025,
    fiscal_period="Q3",
    decimals=None,
)


def _snapshot(
    conn: Conn, payloads: dict[int, list[XbrlFact]], mode: ab.Mode = "stage1", unrefilled: dict[int, str] | None = None
) -> tuple[dict[str, Any], list[str]]:
    conn.commit()
    conn.execute("SELECT 1")  # the script's snapshot: every conn.transaction() inside is a savepoint
    try:
        return ab.run_snapshot(conn, mode=mode, payloads=payloads, unrefilled=unrefilled or {})
    finally:
        conn.rollback()


def test_the_sorted_locks_come_before_the_first_write(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    _seed(conn, 2)
    _seed(conn, 1)
    scan_run(conn, _Provider(_doc(3)))  # both become candidates
    events: list[tuple[str, Any]] = []

    def record(name: str, real: Any, arg: int | None = None) -> Any:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            events.append((name, args[arg] if arg is not None else kwargs.get("instrument_id")))
            return real(*args, **kwargs)

        return wrapper

    monkeypatch.setattr(ab, "lock_instrument_for_normalize", record("lock", ab.lock_instrument_for_normalize, 1))
    monkeypatch.setattr(scan, "seed_population", record("write", scan.seed_population))
    monkeypatch.setattr(ab, "start_ingestion_run", record("write", ab.start_ingestion_run))
    monkeypatch.setattr(ab, "upsert_facts_for_instrument", record("write", ab.upsert_facts_for_instrument))
    _snapshot(conn, {2: [], 1: []})
    first_write = next(i for i, (name, _) in enumerate(events) if name == "write")
    assert events[:first_write] == [("lock", 1), ("lock", 2)]
    assert all(name == "write" for name, _ in events[first_write:])


def test_a_verified_restore_takes_the_on_branch_and_explains_its_cell(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed(conn)
    scan_run(conn, _Provider(_doc(3)))  # a scale-3 generation for the Q3 fact: restored to 0.39
    report, failures = _snapshot(conn, {1: [Q3_FACT]})
    assert failures == []
    found = report["instruments"]["1"]
    assert [(g["branch"], g["expected"]) for g in found["groups"]] == [("on", "on")]
    assert [(c["off"], c["on"], c["branch"], c["explained"]) for c in found["dps_cells_changed"]] == [
        ("390.0000", "0.3900", "on", True)
    ]
    assert report["summary"]["restored"] == 1


def test_a_refill_completes_a_swept_group_which_falls_back_on_its_untested_leaf(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed_swept(conn)  # stored Q1 and Q3; Q1's fact swept from raw
    scan_run(conn, _Provider(_doc(3)))
    report, failures = _snapshot(conn, {1: [Q1_FACT, Q3_FACT]})
    assert failures == []
    found = report["instruments"]["1"]
    # Without the refill this group is incomplete (kept_stored); refilled, its Q1 leaf is untested (A3: off).
    assert [(g["branch"], g["reason"]) for g in found["groups"]] == [("off", "unverified_leaf")]
    assert found["dps_cells_changed"] == [] and found["unreachable"] == []


def test_an_outcome_that_is_not_a3s_branch_fails(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    _seed(conn)
    scan_run(conn, _Provider(_doc(3)))
    real = ab.derive_gated_dps_periods

    def wrong(*args: Any, **kwargs: Any) -> Any:
        rows, outcomes = real(*args, **kwargs)
        return rows, {fy: DpsGroupOutcome(fy, "off", "unverified_leaf") for fy in outcomes}

    monkeypatch.setattr(ab, "derive_gated_dps_periods", wrong)
    _, failures = _snapshot(conn, {1: [Q3_FACT]})
    assert any("took off, A3 gives on" in f for f in failures)


def test_the_on_arm_reads_the_stored_rows_not_the_off_arms_output(ebull_test_conn: Conn) -> None:
    # Unrefilled, the swept group is incomplete, so its Q3 keeps the stored value. The stored 391 differs from what
    # the off arm writes (390); the on arm must still read 391, so the off arm's normalisation is undone first.
    conn = ebull_test_conn
    _seed_swept(conn)
    conn.execute("UPDATE financial_periods SET dps_declared = 391 WHERE instrument_id = 1 AND period_type = 'Q3'")
    scan_run(conn, _Provider(_doc(3)))
    report, failures = _snapshot(conn, {}, unrefilled={1: "fetch failed"})
    assert failures == []
    found = report["instruments"]["1"]
    assert [(g["branch"], g["reason"]) for g in found["groups"]] == [("kept_stored", "incomplete_group")]
    assert [(c["off"], c["on"], c["branch"], c["explained"]) for c in found["dps_cells_changed"]] == [
        ("390.0000", "391.0000", "kept_stored", True)
    ]
    assert report["unrefilled"] == {"1": "fetch failed"}


def test_a_serialization_failure_reaches_the_snapshot_retry(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Swallowed as `instruments_failed`, it would read as an acceptance failure rather than an operational retry.
    conn = ebull_test_conn
    _seed(conn)

    def conflict(*_: Any, **__: Any) -> Any:
        raise psycopg.errors.SerializationFailure("could not serialize access due to concurrent update")

    monkeypatch.setattr(fundamentals, "_derive_periods_from_facts", conflict)
    conn.execute("SELECT 1")
    with pytest.raises(psycopg.errors.SerializationFailure):
        fundamentals.normalize_financial_periods(conn, [1], dps_inline_scale=False)
    conn.rollback()


def test_a_relabelled_surviving_row_is_still_unreachable() -> None:
    end = date(2025, 3, 31)
    row = {"dps_declared": Decimal("0"), "period_type": "Q4", "period_end_date": end, "source_ref": "a"}
    stored = {(1, end, "Q4", 2025, 4): row}
    normalised = {(1, end, "Q4", 2024, 4): {**row, "fiscal_year": 2024}}
    assert [u["key"] for u in ab._unreachable(stored, normalised, [])] == [["1", "2025-03-31", "Q4", "2025", "4"]]
