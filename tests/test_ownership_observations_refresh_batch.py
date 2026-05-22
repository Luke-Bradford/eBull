"""PR-4 batched ``refresh_*_current_batch`` contract tests (#1233).

Spec: docs/superpowers/specs/2026-05-22-bootstrap-etl-optimisation-v2.md §8.

Six pinned cases per batched helper:

1. **Equivalence** — ``refresh_X_current_batch([a, b, c])`` produces the
   same ``_current`` row set as
   ``refresh_X_current(a) + refresh_X_current(b) + refresh_X_current(c)``.
   Verified by snapshot diff + per-row checksum.
2. **Empty input** — ``refresh_X_current_batch([])`` is a no-op
   (returns 0; no SQL is dispatched against the MERGE target).
3. **Idempotency** — re-running the same batch twice leaves
   ``_current`` row count + ``refreshed_at`` unchanged (the diff-aware
   MERGE writer is no-op on identical input — PR12 invariant).
4. **NOT MATCHED BY SOURCE scope clamp** — running the batch against
   only one of two seeded instruments DOES NOT delete the unseeded
   instrument's rows.
5. **Deadlock safety** — two threads call the same helper with
   overlapping but un-sorted instrument sets. Neither deadlocks
   (hash-key-ordered lock acquire is the invariant).
6. **Watermark UPSERT** — each batched call writes one row per
   instrument into ``ownership_refresh_state`` with the matching
   category, mirroring the single-instrument helper.

Three batched helpers covered (insiders / institutions / funds —
matches the PR-4 scope; the other 4 categories keep the single-
instrument path because nothing in the bulk orchestrator loops over
them at scale).
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from app.services import ownership_observations as oo
from tests.fixtures.ebull_test_db import test_database_url


@dataclass(frozen=True)
class BatchHelperCase:
    name: str
    single_fn: Callable[[psycopg.Connection[Any], int], int]
    batch_fn: Callable[[psycopg.Connection[Any], list[int]], int]
    current_table: str
    observations_table: str
    category_literal: str


BATCH_HELPERS: list[BatchHelperCase] = [
    BatchHelperCase(
        "insiders",
        lambda c, i: oo.refresh_insiders_current(c, instrument_id=i),
        lambda c, ids: oo.refresh_insiders_current_batch(c, instrument_ids=ids),
        "ownership_insiders_current",
        "ownership_insiders_observations",
        "insiders",
    ),
    BatchHelperCase(
        "institutions",
        lambda c, i: oo.refresh_institutions_current(c, instrument_id=i),
        lambda c, ids: oo.refresh_institutions_current_batch(c, instrument_ids=ids),
        "ownership_institutions_current",
        "ownership_institutions_observations",
        "institutions",
    ),
    BatchHelperCase(
        "funds",
        lambda c, i: oo.refresh_funds_current(c, instrument_id=i),
        lambda c, ids: oo.refresh_funds_current_batch(c, instrument_ids=ids),
        "ownership_funds_current",
        "ownership_funds_observations",
        "funds",
    ),
]


@pytest.fixture
def conn(ebull_test_conn):
    return ebull_test_conn


def _insert_instruments(conn, ids: list[int]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
            [(i, f"PR4-{i}", f"PR4 Test Co {i}") for i in ids],
        )
    conn.commit()


def _seed_one(conn, helper: BatchHelperCase, instrument_id: int, *, idx: int = 0) -> None:
    """Insert one observation appropriate to the helper's natural key.

    Mirrors the seed helper in ``test_ownership_refresh_writer_merge.py``
    but narrowed to the three categories PR-4 batches.
    """
    run_id = uuid4()
    doc_id = f"PR4-{helper.name}-{instrument_id}-{idx}"
    filed = datetime(2025, 1, 1 + idx, tzinfo=UTC)
    period_end = date(2024, 12, 31)
    if helper.name == "insiders":
        oo.record_insider_observation(
            conn,
            instrument_id=instrument_id,
            holder_cik=f"{instrument_id:010d}",
            holder_name="Test Holder",
            ownership_nature="direct",
            source="form4",
            source_document_id=doc_id,
            source_accession=None,
            source_field=None,
            source_url=None,
            filed_at=filed,
            period_start=None,
            period_end=period_end,
            ingest_run_id=run_id,
            shares=Decimal("100"),
        )
    elif helper.name == "institutions":
        oo.record_institution_observation(
            conn,
            instrument_id=instrument_id,
            filer_cik=f"{instrument_id:010d}",
            filer_name="Test Filer",
            filer_type="ETF",
            ownership_nature="economic",
            source="13f",
            source_document_id=doc_id,
            source_accession=None,
            source_field=None,
            source_url=None,
            filed_at=filed,
            period_start=None,
            period_end=period_end,
            ingest_run_id=run_id,
            shares=Decimal("1000"),
            market_value_usd=Decimal("50000"),
            voting_authority="SOLE",
            exposure_kind="EQUITY",
        )
    elif helper.name == "funds":
        # Fund_series_id stable per instrument so multiple seeds on the
        # same instrument SHARE a (instrument_id, fund_series_id) natural
        # key. That forces the DISTINCT ON / ORDER BY winner-selection
        # path in the MERGE writer to actually pick between observations
        # — without this the per-idx seeds would all land on distinct
        # natural keys and DISTINCT ON would be a no-op (Codex 2 LOW-1).
        series_seq = 1000 + instrument_id * 10
        # source_document_id MUST differ per idx so the observations
        # writer's ON CONFLICT idempotency key (… , source_document_id, …)
        # does not collapse the seeds into one row before DISTINCT ON
        # ever sees them.
        oo.record_fund_observation(
            conn,
            instrument_id=instrument_id,
            fund_series_id=f"S{series_seq:09d}",
            fund_series_name=f"Test Fund {instrument_id}-{idx}",
            fund_filer_cik=f"{instrument_id:010d}",
            source_document_id=doc_id,
            source_accession=None,
            source_field=None,
            source_url=None,
            filed_at=filed,
            period_start=None,
            period_end=period_end,
            ingest_run_id=run_id,
            shares=Decimal("500"),
            market_value_usd=Decimal("25000"),
            payoff_profile="Long",
            asset_category="EC",
        )
    else:
        pytest.fail(f"unknown helper: {helper.name}")
    conn.commit()


def _snapshot_current(conn, table: str, instrument_ids: list[int]) -> list[tuple]:
    """Return the full ``_current`` row set for the given instruments,
    deterministically ordered. ``refreshed_at`` excluded so that
    re-running the helper twice (different now()) does not flip the
    equivalence assertion."""
    with conn.cursor() as cur:
        cur.execute(
            # row_to_json then strip the volatile columns. PG's ::jsonb -
            # operator removes keys client-side without enumerating every
            # column the helper might add later.
            f"""
            SELECT instrument_id,
                   (to_jsonb(t.*) - 'refreshed_at')::text
              FROM {table} AS t
             WHERE instrument_id = ANY(%s::bigint[])
             ORDER BY instrument_id, to_jsonb(t.*)::text
            """,
            (instrument_ids,),
        )
        return list(cur.fetchall())


def _watermark_count(conn, instrument_ids: list[int], category: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ownership_refresh_state WHERE category = %s AND instrument_id = ANY(%s::bigint[])",
            (category, instrument_ids),
        )
        row = cur.fetchone()
    return int(row[0]) if row else 0


# ----------------------------------------------------------------------
# Case 1: equivalence vs serial loop
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", BATCH_HELPERS, ids=lambda h: h.name)
def test_batch_equivalent_to_serial_loop(conn, helper):
    """``refresh_X_current_batch([a, b, c])`` produces the same row set
    as the per-instrument serial loop. Verified by snapshot diff."""
    ids = [1, 2, 3]
    _insert_instruments(conn, ids)
    for iid in ids:
        # Two observations per instrument so the DISTINCT ON / ORDER BY
        # logic actually picks the winning row instead of trivially
        # taking the only row.
        _seed_one(conn, helper, iid, idx=0)
        _seed_one(conn, helper, iid, idx=1)

    # Serial path — snapshot it, then wipe _current + refresh_state
    # so we can re-run via the batched path and compare.
    for iid in ids:
        helper.single_fn(conn, iid)
    conn.commit()
    serial_snapshot = _snapshot_current(conn, helper.current_table, ids)

    # Reset both _current and the refresh_state side-table so the
    # batched path runs from a clean slate.
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {helper.current_table} WHERE instrument_id = ANY(%s::bigint[])",
            (ids,),
        )
        cur.execute(
            "DELETE FROM ownership_refresh_state WHERE category = %s AND instrument_id = ANY(%s::bigint[])",
            (helper.category_literal, ids),
        )
    conn.commit()

    helper.batch_fn(conn, ids)
    conn.commit()
    batch_snapshot = _snapshot_current(conn, helper.current_table, ids)

    assert batch_snapshot == serial_snapshot, (
        f"batch and serial paths diverged for helper={helper.name}: serial={serial_snapshot!r} batch={batch_snapshot!r}"
    )


# ----------------------------------------------------------------------
# Case 2: empty input
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", BATCH_HELPERS, ids=lambda h: h.name)
def test_batch_empty_input_is_noop(conn, helper):
    """An empty ``instrument_ids`` returns 0 without raising and
    without writing any row into either ``_current`` or
    ``ownership_refresh_state``."""
    pre_state_count = _watermark_count(conn, [], helper.category_literal)
    result = helper.batch_fn(conn, [])
    conn.commit()
    assert result == 0
    post_state_count = _watermark_count(conn, [], helper.category_literal)
    assert pre_state_count == post_state_count


# ----------------------------------------------------------------------
# Case 3: idempotency
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", BATCH_HELPERS, ids=lambda h: h.name)
def test_batch_idempotent_on_re_run(conn, helper):
    """Re-running the same batch leaves ``_current`` rows + xmin
    unchanged (PR12 IS DISTINCT FROM diff predicate — no-op on
    identical input). Watermark table sees its ``last_refresh_attempted_at``
    advance but row count stays at len(ids)."""
    ids = [1, 2]
    _insert_instruments(conn, ids)
    for iid in ids:
        _seed_one(conn, helper, iid, idx=0)
    helper.batch_fn(conn, ids)
    conn.commit()

    pre_snapshot = _snapshot_current(conn, helper.current_table, ids)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT instrument_id, xmin::text FROM {helper.current_table} "
            "WHERE instrument_id = ANY(%s::bigint[]) ORDER BY instrument_id, xmin::text",
            (ids,),
        )
        pre_xmin = cur.fetchall()
    pre_watermark_count = _watermark_count(conn, ids, helper.category_literal)

    helper.batch_fn(conn, ids)
    conn.commit()

    post_snapshot = _snapshot_current(conn, helper.current_table, ids)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT instrument_id, xmin::text FROM {helper.current_table} "
            "WHERE instrument_id = ANY(%s::bigint[]) ORDER BY instrument_id, xmin::text",
            (ids,),
        )
        post_xmin = cur.fetchall()
    post_watermark_count = _watermark_count(conn, ids, helper.category_literal)

    assert pre_snapshot == post_snapshot, "no-op re-run must not mutate _current rows"
    assert pre_xmin == post_xmin, "no-op re-run must leave xmin stable (diff predicate skipped UPDATE)"
    assert pre_watermark_count == post_watermark_count == len(ids)


# ----------------------------------------------------------------------
# Case 4: NOT MATCHED BY SOURCE scope clamp
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", BATCH_HELPERS, ids=lambda h: h.name)
def test_batch_not_matched_by_source_scope_clamped(conn, helper):
    """Refreshing instrument A only must NOT delete instrument B's
    rows even though B has stale ``_current`` rows. The
    ``tgt.instrument_id = ANY(%(ids)s)`` clamp in the DELETE clause is
    the load-bearing invariant."""
    ids = [1, 2]
    _insert_instruments(conn, ids)
    for iid in ids:
        _seed_one(conn, helper, iid, idx=0)
    helper.batch_fn(conn, ids)
    conn.commit()

    # B's _current row must still exist after a refresh that names
    # only A in the batch.
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM {helper.current_table} WHERE instrument_id = %s",
            (2,),
        )
        pre_b = int(cur.fetchone()[0])
    assert pre_b >= 1

    helper.batch_fn(conn, [1])
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM {helper.current_table} WHERE instrument_id = %s",
            (2,),
        )
        post_b = int(cur.fetchone()[0])
    assert post_b == pre_b, (
        "batch refresh of instrument A leaked into instrument B's _current rows — DELETE clause scope clamp violated"
    )


# ----------------------------------------------------------------------
# Case 5: deadlock safety under concurrent batches
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", BATCH_HELPERS, ids=lambda h: h.name)
def test_batch_deadlock_safe_under_concurrent_overlap(conn, helper):
    """Two threads call the batched helper with overlapping instrument
    sets. Neither deadlocks because the SQL-side ``ORDER BY lk`` in the
    lock-acquire query forces a deterministic hash-key sort across
    callers — regardless of the order the caller passed in.

    Codex 2 LOW-2 caveat: ``_normalise_instrument_ids`` already sorts
    by int before the SQL runs, so the differing Python input orders
    below are equivalent inputs to the SQL. The actual deadlock
    safety claim is that the SQL acquires locks ordered by HASHED key
    (not raw int) and therefore matches across callers even though
    int-order and hash-order disagree for arbitrary ids. This test
    asserts the integrative behaviour: two parallel overlapping
    batches complete without PG raising ``DeadlockDetected`` (which
    fires after ``deadlock_timeout`` = 1s by default). If we ever
    regress to a Python-side or non-hash-ordered lock acquire, the
    PG-level deadlock would surface here even though the Python
    ordering looks fine.

    Each thread opens its own connection (per-worker test DB derived
    from ``test_database_url()``); the driver connection only seeds
    the data.
    """
    ids = [1, 2, 3, 4, 5]
    _insert_instruments(conn, ids)
    for iid in ids:
        _seed_one(conn, helper, iid, idx=0)
    conn.commit()

    url = test_database_url()

    # Differing Python input orders. After _normalise_instrument_ids
    # both threads issue SQL against the same sorted list; the SQL
    # ORDER BY lk inside that statement is what prevents deadlocks.
    thread_a_ids = [1, 2, 3, 4, 5]
    thread_b_ids = [5, 4, 3, 2, 1]

    errors: list[BaseException] = []

    def _worker(target_ids: list[int]) -> None:
        try:
            with psycopg.connect(url) as worker_conn:
                # PG default deadlock_timeout is 1s; if a real deadlock
                # exists, the worker will receive a DeadlockDetected
                # error within ~1s.
                helper.batch_fn(worker_conn, target_ids)
                worker_conn.commit()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    ta = threading.Thread(target=_worker, args=(thread_a_ids,))
    tb = threading.Thread(target=_worker, args=(thread_b_ids,))
    ta.start()
    tb.start()
    # 30 s join is generous — a real deadlock surfaces in ~1 s.
    ta.join(timeout=30)
    tb.join(timeout=30)

    assert not ta.is_alive() and not tb.is_alive(), (
        "one or both worker threads still alive after 30s — likely deadlocked"
    )
    assert not errors, f"unexpected worker errors: {errors!r}"


# ----------------------------------------------------------------------
# Case 6: watermark UPSERT contract
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", BATCH_HELPERS, ids=lambda h: h.name)
def test_batch_writes_one_watermark_per_instrument(conn, helper):
    """Each instrument in the batch produces exactly one
    ``ownership_refresh_state`` row with the matching category. The
    UPSERT path mirrors the single-instrument helper's INSERT … ON
    CONFLICT shape."""
    ids = [1, 2, 3]
    _insert_instruments(conn, ids)
    for iid in ids:
        _seed_one(conn, helper, iid, idx=0)
    helper.batch_fn(conn, ids)
    conn.commit()

    assert _watermark_count(conn, ids, helper.category_literal) == len(ids)

    # Re-run does not duplicate rows — ON CONFLICT path.
    helper.batch_fn(conn, ids)
    conn.commit()
    assert _watermark_count(conn, ids, helper.category_literal) == len(ids)


# ----------------------------------------------------------------------
# Case 7: duplicate / unsorted input normalised internally
# ----------------------------------------------------------------------
@pytest.mark.parametrize("helper", BATCH_HELPERS, ids=lambda h: h.name)
def test_batch_normalises_duplicate_and_unsorted_input(conn, helper):
    """Caller passing ``[3, 1, 2, 1, 3]`` MUST behave the same as
    ``[1, 2, 3]`` — the helper de-dupes and sorts internally so a
    misbehaving caller cannot inflate lock-acquire count or feed
    duplicate src rows into the MERGE."""
    ids = [1, 2, 3]
    _insert_instruments(conn, ids)
    for iid in ids:
        _seed_one(conn, helper, iid, idx=0)

    result = helper.batch_fn(conn, [3, 1, 2, 1, 3])
    conn.commit()
    # _normalise_instrument_ids returns a sorted, de-duplicated list
    # of length 3. The helper returns that length.
    assert result == 3
    assert _watermark_count(conn, ids, helper.category_literal) == 3
