"""Benchmark three shapes of `financial_facts_raw` upsert.

Issue: #414 investigation phase A.

Goal: quantify the DB-write cost of each candidate upsert shape
against the current row-by-row shape at
`app/services/fundamentals.py:300-374`. Three shapes are benchmarked
against a representative single-10-K payload (~10_000 synthetic facts)
under three scenarios:

    seed             — empty index, every fact is a new INSERT.
    re-upsert no-op  — same payload re-run; the WHERE IS DISTINCT FROM
                       filter short-circuits, no row is rewritten.
    restatement      — same identity tuple, mutated ``val`` field; the
                       DO UPDATE path actually rewrites rows.

Shapes under test:

    A. row-by-row (current production shape).
    B. executemany(page_size=1000) — same SQL, batched.
    C. COPY STDIN into a TEMP staging table, then one
       INSERT … SELECT … ON CONFLICT against `financial_facts_raw`.

Runs against the isolated `ebull_test` Postgres, NEVER the dev DB.
Reuses the same guard as `tests/fixtures/ebull_test_db.py`. Safe to
re-run — each invocation TRUNCATEs the planner tables it writes to.

Usage:
    uv run python scripts/bench_fundamentals_upsert.py [--facts N]

The script is measurement-only — it does not change any production
code path. The numbers it produces feed the ADR at
`docs/adr/0004-fundamentals-ingest-shape.md`.

Scope limits (do NOT extrapolate beyond these):
- Single-CIK, single-threaded, isolated DB.
- No concurrent HTTP load, no parser CPU, no OS-level lock-wait
  measurement.  The bench quantifies the DB write path only — it
  cannot by itself decide whether residual site-freeze comes from the
  Python XBRL parser (GIL) or transaction-lock contention.  Those
  need a separate in-process probe after the shape change ships.
- The starting table is ~0 rows; production carries millions.  B-tree
  insert cost is O(log N), so absolute seed durations on a large
  existing index will be higher than the numbers printed here.  The
  *ratio* between shapes is still useful; the absolute wall-clock is
  not a production prediction.
"""

from __future__ import annotations

import argparse
import io
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import psycopg

from app.providers.fundamentals import XbrlFact
from app.services.fundamentals import upsert_facts_for_instrument
from tests.fixtures.ebull_test_db import (
    apply_migrations_to_test_db,
    ensure_test_db_exists,
    test_database_url,
)

# ---------------------------------------------------------------------------
# Guard — refuse to run against anything except ebull_test
# ---------------------------------------------------------------------------


def _assert_test_db(conn: psycopg.Connection[tuple]) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        row = cur.fetchone()
    if row is None or row[0] != "ebull_test":
        raise RuntimeError(f"refusing to run bench against database {row!r}; expected 'ebull_test'")


# ---------------------------------------------------------------------------
# Synthetic payload
# ---------------------------------------------------------------------------


_UNITS = ("USD", "USD/shares", "shares", "pure")


def _generate_facts(count: int) -> list[XbrlFact]:
    """Produce ``count`` unique XBRL facts that hit every branch of the
    production identity tuple.

    Each fact is unique under the `financial_facts_raw` unique index
    ``(instrument_id, concept, unit, COALESCE(period_start), period_end,
    accession_number)``. The generator deliberately varies:

    - ``unit`` across four real-world XBRL units (USD, USD/shares,
      shares, pure), so the bench exercises the ``unit`` column of the
      index rather than measuring a single-unit hot path.
    - ``period_start`` — roughly 1 in 5 facts use ``None`` to model
      **instant** XBRL facts (balance-sheet items), which go through
      the ``COALESCE(period_start, '0001-01-01'::date)`` branch of the
      unique index. The other 4/5 are duration facts with real
      period_start / period_end ranges.
    - ``period_end`` across multiple fiscal period ends so index
      locality spans pages, not a single hot page.
    - ``accession_number`` across multiple filings, since the real
      index is per-filing.

    Re-upsert-no-op scenario: running the same list twice exercises
    the ``ON CONFLICT … DO UPDATE … WHERE IS DISTINCT FROM`` short-
    circuit (zero field changes), which is the idempotent no-op path
    the scheduler hits every time a CIK's watermark is unchanged.

    Restatement scenario (covered elsewhere in this module): the
    harness builds a second list with the same identity tuple and a
    mutated ``val`` to drive the DO UPDATE write path.
    """
    facts: list[XbrlFact] = []
    # ~200 concepts × multiple periods × multiple units gives a
    # realistic mixture for a 10-K-sized 10k-fact payload.
    concepts = 200
    for i in range(count):
        concept_id = i % concepts
        period_id = i // concepts
        # Instant facts (balance-sheet) get NULL period_start roughly
        # 1-in-5 — matches the production mix.
        is_instant = (i % 5) == 0
        period_end = date(2023, 3 * (period_id % 4 + 1) % 12 or 12, 28)
        period_start = None if is_instant else date(2023, 1, 1)
        facts.append(
            XbrlFact(
                concept=f"us-gaap:Concept{concept_id:04d}",
                taxonomy="us-gaap",
                unit=_UNITS[i % len(_UNITS)],
                period_start=period_start,
                period_end=period_end,
                val=Decimal(f"{100.0 + i}"),
                frame=f"CY2023Q{(period_id % 4) + 1}",
                accession_number=f"0000000000-24-{period_id:06d}",
                form_type="10-K",
                filed_date=date(2024, 3, 15),
                fiscal_year=2023,
                fiscal_period="FY",
                decimals="-3",
            )
        )
    return facts


# ---------------------------------------------------------------------------
# Shape B — executemany(page_size=1000)
# ---------------------------------------------------------------------------


def upsert_facts_executemany(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    facts: Sequence[XbrlFact],
    ingestion_run_id: int,
    page_size: int = 1000,
) -> None:
    if not facts:
        return
    rows = [
        {
            "instrument_id": instrument_id,
            "taxonomy": f.taxonomy,
            "concept": f.concept,
            "unit": f.unit,
            "period_start": f.period_start,
            "period_end": f.period_end,
            "val": f.val,
            "frame": f.frame,
            "accession_number": f.accession_number,
            "form_type": f.form_type,
            "filed_date": f.filed_date,
            "fiscal_year": f.fiscal_year,
            "fiscal_period": f.fiscal_period,
            "decimals": f.decimals,
            "ingestion_run_id": ingestion_run_id,
        }
        for f in facts
    ]
    sql = """
        INSERT INTO financial_facts_raw (
            instrument_id, taxonomy, concept, unit,
            period_start, period_end, val, frame,
            accession_number, form_type, filed_date,
            fiscal_year, fiscal_period, decimals,
            ingestion_run_id
        ) VALUES (
            %(instrument_id)s, %(taxonomy)s, %(concept)s, %(unit)s,
            %(period_start)s, %(period_end)s, %(val)s, %(frame)s,
            %(accession_number)s, %(form_type)s, %(filed_date)s,
            %(fiscal_year)s, %(fiscal_period)s, %(decimals)s,
            %(ingestion_run_id)s
        )
        ON CONFLICT (
            instrument_id, concept, unit,
            COALESCE(period_start, '0001-01-01'::date),
            period_end, accession_number
        )
        DO UPDATE SET
            val = EXCLUDED.val,
            frame = EXCLUDED.frame,
            form_type = EXCLUDED.form_type,
            filed_date = EXCLUDED.filed_date,
            fiscal_year = EXCLUDED.fiscal_year,
            fiscal_period = EXCLUDED.fiscal_period,
            decimals = EXCLUDED.decimals,
            ingestion_run_id = EXCLUDED.ingestion_run_id,
            fetched_at = NOW()
        WHERE financial_facts_raw.val IS DISTINCT FROM EXCLUDED.val
           OR financial_facts_raw.frame IS DISTINCT FROM EXCLUDED.frame
    """
    with conn.cursor() as cur:
        for start in range(0, len(rows), page_size):
            cur.executemany(sql, rows[start : start + page_size])


# ---------------------------------------------------------------------------
# Shape C — COPY STDIN → temp staging → INSERT SELECT ON CONFLICT
# ---------------------------------------------------------------------------


def upsert_facts_copy(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    facts: Sequence[XbrlFact],
    ingestion_run_id: int,
) -> None:
    if not facts:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS _stg_facts (
                instrument_id BIGINT, taxonomy TEXT, concept TEXT, unit TEXT,
                period_start DATE, period_end DATE, val NUMERIC(30,6), frame TEXT,
                accession_number TEXT, form_type TEXT, filed_date DATE,
                fiscal_year INT, fiscal_period TEXT, decimals TEXT,
                ingestion_run_id BIGINT
            ) ON COMMIT DROP
            """
        )
        with cur.copy(
            """
            COPY _stg_facts (
                instrument_id, taxonomy, concept, unit,
                period_start, period_end, val, frame,
                accession_number, form_type, filed_date,
                fiscal_year, fiscal_period, decimals, ingestion_run_id
            ) FROM STDIN
            """
        ) as copy:
            for f in facts:
                copy.write_row(
                    (
                        instrument_id,
                        f.taxonomy,
                        f.concept,
                        f.unit,
                        f.period_start,
                        f.period_end,
                        f.val,
                        f.frame,
                        f.accession_number,
                        f.form_type,
                        f.filed_date,
                        f.fiscal_year,
                        f.fiscal_period,
                        f.decimals,
                        ingestion_run_id,
                    )
                )
        cur.execute(
            """
            INSERT INTO financial_facts_raw (
                instrument_id, taxonomy, concept, unit,
                period_start, period_end, val, frame,
                accession_number, form_type, filed_date,
                fiscal_year, fiscal_period, decimals, ingestion_run_id
            )
            SELECT instrument_id, taxonomy, concept, unit,
                   period_start, period_end, val, frame,
                   accession_number, form_type, filed_date,
                   fiscal_year, fiscal_period, decimals, ingestion_run_id
            FROM _stg_facts
            ON CONFLICT (
                instrument_id, concept, unit,
                COALESCE(period_start, '0001-01-01'::date),
                period_end, accession_number
            )
            DO UPDATE SET
                val = EXCLUDED.val,
                frame = EXCLUDED.frame,
                form_type = EXCLUDED.form_type,
                filed_date = EXCLUDED.filed_date,
                fiscal_year = EXCLUDED.fiscal_year,
                fiscal_period = EXCLUDED.fiscal_period,
                decimals = EXCLUDED.decimals,
                ingestion_run_id = EXCLUDED.ingestion_run_id,
                fetched_at = NOW()
            WHERE financial_facts_raw.val IS DISTINCT FROM EXCLUDED.val
               OR financial_facts_raw.frame IS DISTINCT FROM EXCLUDED.frame
            """
        )


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


@dataclass
class BenchResult:
    shape: str
    scenario: str  # "seed" | "re-upsert no-op" | "restatement"
    facts: int
    seconds: float
    facts_per_sec: float


_BENCH_INSTRUMENT_ID = 999_999_999


def _reset_and_seed(conn: psycopg.Connection[tuple]) -> tuple[int, int]:
    _assert_test_db(conn)
    with conn.cursor() as cur:
        # TRUNCATE facts + runs; keep instruments row stable across
        # shape iterations so FKs remain valid. CASCADE ensures the
        # dependent financial_facts_raw rows are removed each run.
        cur.execute("TRUNCATE financial_facts_raw, data_ingestion_runs RESTART IDENTITY CASCADE")
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, exchange, is_tradable)
            VALUES (%s, 'BENCH', 'Bench Inc.', 'TEST', true)
            ON CONFLICT (instrument_id) DO NOTHING
            """,
            (_BENCH_INSTRUMENT_ID,),
        )
        cur.execute(
            """
            INSERT INTO data_ingestion_runs (source, endpoint, instrument_count, status)
            VALUES ('bench', 'bench', 1, 'running')
            RETURNING ingestion_run_id
            """
        )
        row = cur.fetchone()
        assert row is not None
        ingestion_run_id = row[0]
    conn.commit()
    return _BENCH_INSTRUMENT_ID, ingestion_run_id


def _time(fn: Callable[[], None]) -> float:
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


def bench(facts_count: int) -> list[BenchResult]:
    ensure_test_db_exists()
    apply_migrations_to_test_db()
    results: list[BenchResult] = []
    facts = _generate_facts(facts_count)

    shapes: list[tuple[str, Callable[..., object]]] = [
        ("A: row-loop (current prod)", upsert_facts_for_instrument),
        ("B: executemany(1000)", upsert_facts_executemany),
        ("C: COPY -> INSERT SELECT", upsert_facts_copy),
    ]

    # A second fact list with mutated ``val`` fields — same identity
    # tuple, different value — models a filing restatement, which
    # exercises the DO UPDATE write path (the WHERE IS DISTINCT FROM
    # filter passes and Postgres actually rewrites the row).
    facts_restated = [
        XbrlFact(
            concept=f.concept,
            taxonomy=f.taxonomy,
            unit=f.unit,
            period_start=f.period_start,
            period_end=f.period_end,
            val=f.val + Decimal("1"),
            frame=f.frame,
            accession_number=f.accession_number,
            form_type=f.form_type,
            filed_date=f.filed_date,
            fiscal_year=f.fiscal_year,
            fiscal_period=f.fiscal_period,
            decimals=f.decimals,
        )
        for f in facts
    ]

    for shape, fn in shapes:
        with psycopg.connect(test_database_url()) as conn:
            instrument_id, ingestion_run_id = _reset_and_seed(conn)

            def _invoke(
                payload: Sequence[XbrlFact],
                f: Callable[..., object] = fn,
            ) -> None:
                # Default-arg capture pins the current loop iteration's
                # ``fn`` into the closure so the transaction block below
                # cannot silently call a later iteration's function.
                with conn.transaction():
                    f(
                        conn,
                        instrument_id=instrument_id,
                        facts=payload,
                        ingestion_run_id=ingestion_run_id,
                    )

            scenarios: list[tuple[str, Callable[[], None]]] = [
                ("seed", lambda: _invoke(facts)),
                ("re-upsert no-op", lambda: _invoke(facts)),
                ("restatement", lambda: _invoke(facts_restated)),
            ]
            for scenario, action in scenarios:
                seconds = _time(action)
                results.append(
                    BenchResult(
                        shape=shape,
                        scenario=scenario,
                        facts=facts_count,
                        seconds=seconds,
                        facts_per_sec=facts_count / seconds if seconds > 0 else float("inf"),
                    )
                )

    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--facts", type=int, default=10_000)
    args = parser.parse_args()

    results = bench(args.facts)

    # Render a text table — easy to paste into the ADR.
    width_shape = max(len(r.shape) for r in results) + 2
    buf = io.StringIO()
    buf.write(f"Benchmark: {args.facts:,} synthetic XBRL facts, one CIK\n")
    buf.write("=" * 80 + "\n")
    buf.write(f"{'shape'.ljust(width_shape)} {'scenario':<14} {'seconds':>10} {'facts/sec':>12}\n")
    buf.write("-" * 80 + "\n")
    for r in results:
        buf.write(f"{r.shape.ljust(width_shape)} {r.scenario:<14} {r.seconds:>10.3f} {r.facts_per_sec:>12.1f}\n")
    buf.write("=" * 80 + "\n")
    print(buf.getvalue())


if __name__ == "__main__":
    main()
