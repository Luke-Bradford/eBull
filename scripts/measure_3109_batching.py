"""Reproduce the #3109 batching measurements — read-only, full population.

Every figure quoted in
``docs/proposals/etl/2026-09-16-3109-per-cik-multi-source-batching.md`` §2 is
computed here rather than written down, per the engineering standard "never
hardcode a derived statistic into prose — compute it, or omit it".

⚠ Each query runs in its own READ COMMITTED statement against a live DB, so the
output is a SNAPSHOT, not an invariant. Two queries can see different
populations.

Read-only: the connection is opened ``read_only`` so a mistake fails loudly
rather than writing to the dev DB.

Usage::

    PYTHONPATH=. uv run python scripts/measure_3109_batching.py
"""

from __future__ import annotations

from typing import Any, LiteralString

import psycopg

from app.config import settings

# The states each lane treats as candidates (``app/services/data_freshness.py``),
# inlined as literals because this script must describe what the selector does
# today, not track a refactor of it.
_POLL_STATES = "('unknown','current','expected_filing_overdue')"
_RECHECK_STATES = "('never_filed','error')"

# Budget as the scheduler runs it today: 100 → poll 66 / recheck 34
# (``run_per_cik_poll``; ``scheduler.py`` passes no override).
_POLL_BUDGET = 66
_RECHECK_BUDGET = 34

# The CIK shape the SEC submissions URL requires (``_zero_pad_cik``) and the
# manifest writer enforces (``_MANIFEST_CIK_RE``). The new selector filters on
# this; the old ``cik IS NULL`` guard did not.
_CIK_SHAPE = r"^[0-9]{1,10}$"


def _one(cur: psycopg.Cursor[Any], sql: LiteralString, params: list[Any] | None = None) -> tuple[Any, ...]:
    cur.execute(sql, params or [])
    row = cur.fetchone()
    assert row is not None, f"query returned no row: {sql}"
    return row


def _m5_state_census(cur: psycopg.Cursor[Any]) -> None:
    cur.execute("SELECT state, count(*) FROM data_freshness_index GROUP BY state ORDER BY 2 DESC")
    print("M5 state census of data_freshness_index")
    for state, n in cur.fetchall():
        print(f"    {state:26s} {n:>8,}")


def _m0_cik_shape(cur: psycopg.Cursor[Any]) -> None:
    total, nulls, bad = _one(
        cur,
        f"""
        SELECT count(*), count(*) FILTER (WHERE cik IS NULL),
               count(*) FILTER (WHERE cik IS NOT NULL AND cik !~ '{_CIK_SHAPE}')
        FROM data_freshness_index
        """,
    )
    print(f"\nM0 cik shape (full population): rows={total:,} null={nulls} non_numeric={bad}")
    cur.execute(
        f"""
        SELECT cik, subject_type, subject_id, source, state FROM data_freshness_index
        WHERE cik IS NOT NULL AND cik !~ '{_CIK_SHAPE}' ORDER BY cik
        """
    )
    # A narrowing gate is stated by what it REJECTS, enumerated, not described.
    for rejected in cur.fetchall():
        print(f"    rejected by the new selector: {rejected}")
    (variants,) = _one(
        cur,
        f"""
        SELECT count(*) FROM (
          SELECT lpad(cik, 10, '0') FROM data_freshness_index
          WHERE cik ~ '{_CIK_SHAPE}' GROUP BY 1 HAVING count(DISTINCT cik) > 1
        ) t
        """,
    )
    print(f"    padding variants that would split one entity into two batches: {variants}")


def _m1_prefixes(cur: psycopg.Cursor[Any]) -> None:
    rows, ciks = _one(
        cur,
        f"""
        WITH p AS (
          SELECT cik FROM data_freshness_index
          WHERE state IN {_POLL_STATES}
            AND (expected_next_at IS NULL OR expected_next_at <= now())
          ORDER BY expected_next_at ASC NULLS FIRST LIMIT %s
        )
        SELECT count(*), count(DISTINCT cik) FROM p
        """,
        [_POLL_BUDGET],
    )
    fanout = f"{rows / ciks:.3f}x" if ciks else "n/a"
    print(f"\nM1 poll prefix({_POLL_BUDGET}): rows={rows} distinct_ciks={ciks} in-prefix fan-out={fanout}")

    # The prefix has no tie-break, so its CIK count is not reproducible when
    # deadlines tie at the boundary. Measure the tie density rather than assert
    # the count is stable.
    seen, distinct = _one(
        cur,
        f"""
        WITH p AS (
          SELECT expected_next_at FROM data_freshness_index
          WHERE state IN {_POLL_STATES}
            AND (expected_next_at IS NULL OR expected_next_at <= now())
          ORDER BY expected_next_at ASC NULLS FIRST LIMIT 200
        )
        SELECT count(*), count(DISTINCT expected_next_at) FROM p
        """,
    )
    print(f"    tie density over the first {seen} due rows: {distinct} distinct deadlines")

    r_rows, r_ciks = _one(
        cur,
        f"""
        WITH p AS (
          SELECT cik FROM data_freshness_index
          WHERE state IN {_RECHECK_STATES}
            AND (next_recheck_at IS NULL OR next_recheck_at <= now())
          ORDER BY next_recheck_at ASC NULLS FIRST LIMIT %s
        )
        SELECT count(*), count(DISTINCT cik) FROM p
        """,
        [_RECHECK_BUDGET],
    )
    print(f"M1b recheck prefix({_RECHECK_BUDGET}): rows={r_rows} distinct_ciks={r_ciks}")


def _m2_cik_budget(cur: psycopg.Cursor[Any]) -> None:
    # Ranked by the SAME expression the proposed selector uses — COALESCE inside
    # the MIN (``MIN`` ignores NULL, which would invert NULL-is-most-urgent) and
    # the padded CIK as the deterministic tie-break.
    ciks, triples, worst, fanout = _one(
        cur,
        f"""
        WITH due AS (
          SELECT lpad(cik, 10, '0') AS cik_padded, expected_next_at
          FROM data_freshness_index
          WHERE state IN {_POLL_STATES}
            AND (expected_next_at IS NULL OR expected_next_at <= now())
            AND cik ~ '{_CIK_SHAPE}'
        ), ranked AS (
          SELECT cik_padded, count(*) AS t FROM due GROUP BY cik_padded
          ORDER BY min(COALESCE(expected_next_at, '-infinity'::timestamptz)), cik_padded
          LIMIT %s
        )
        SELECT count(*), sum(t), max(t), round(sum(t)::numeric / count(*), 3) FROM ranked
        """,
        [_POLL_BUDGET],
    )
    print(
        f"M2 CIK-denominated budget({_POLL_BUDGET}): ciks={ciks} triples={triples} "
        f"max_per_cik={worst} fan-out={fanout}x"
    )

    # How much of that selection replays a whole ``recent`` array because it has
    # no accession watermark — the work bound Codex 20 asked for.
    selected, unwatermarked = _one(
        cur,
        f"""
        WITH due AS (
          SELECT lpad(cik, 10, '0') AS cik_padded, expected_next_at, last_known_filing_id
          FROM data_freshness_index
          WHERE state IN {_POLL_STATES}
            AND (expected_next_at IS NULL OR expected_next_at <= now())
            AND cik ~ '{_CIK_SHAPE}'
        ), ranked AS (
          SELECT cik_padded FROM due GROUP BY cik_padded
          ORDER BY min(COALESCE(expected_next_at, '-infinity'::timestamptz)), cik_padded
          LIMIT %s
        )
        SELECT count(*), count(*) FILTER (WHERE d.last_known_filing_id IS NULL)
        FROM due d JOIN ranked r USING (cik_padded)
        """,
        [_POLL_BUDGET],
    )
    print(f"    of {selected} selected triples, {unwatermarked} carry no accession watermark (full replay)")


def _m6_population(cur: psycopg.Cursor[Any]) -> None:
    lanes: tuple[tuple[str, LiteralString], ...] = (
        ("candidate", ""),
        ("due", " AND (expected_next_at IS NULL OR expected_next_at <= now())"),
    )
    for label, predicate in lanes:
        triples, ciks = _one(
            cur,
            f"SELECT count(*), count(DISTINCT cik) FROM data_freshness_index WHERE state IN {_POLL_STATES}{predicate}",
        )
        fanout = f"{triples / ciks:.2f}x" if ciks else "n/a"
        print(f"M6 {label}: triples={triples:,} ciks={ciks:,} population fan-out={fanout}")

    (unwatermarked,) = _one(
        cur,
        f"""
        SELECT count(*) FROM data_freshness_index
        WHERE state IN {_POLL_STATES}
          AND (expected_next_at IS NULL OR expected_next_at <= now())
          AND last_known_filing_id IS NULL
        """,
    )
    print(f"    due rows with no accession watermark: {unwatermarked:,}")

    cur.execute(
        f"""
        SELECT source, count(*) FROM data_freshness_index
        WHERE state IN {_POLL_STATES}
          AND (expected_next_at IS NULL OR expected_next_at <= now())
        GROUP BY source ORDER BY 2 DESC
        """
    )
    print("    due-source census: " + " · ".join(f"{s} {n:,}" for s, n in cur.fetchall()))


def _m3_collisions(cur: psycopg.Cursor[Any]) -> None:
    pairs, involved, worst = _one(
        cur,
        """
        SELECT count(*), coalesce(sum(n), 0), coalesce(max(n), 0) FROM (
          SELECT cik, source, count(*) n FROM data_freshness_index
          WHERE cik IS NOT NULL GROUP BY cik, source HAVING count(*) > 1
        ) t
        """,
    )
    # Counts collisions; does NOT classify them. No identity join is run here, so
    # "share-class siblings" would be a hypothesis about these rows, not a fact.
    print(f"M3 duplicate (cik,source): pairs={pairs} rows_involved={involved} worst={worst}")


def _m4_runtime(cur: psycopg.Cursor[Any]) -> None:
    runs, avg_s, max_s, ok, bad = _one(
        cur,
        """
        SELECT count(*),
               round(avg(extract(epoch FROM (finished_at - started_at)))::numeric, 2),
               round(max(extract(epoch FROM (finished_at - started_at)))::numeric, 2),
               count(*) FILTER (WHERE status = 'success'),
               count(*) FILTER (WHERE status NOT IN ('success', 'skipped'))
        FROM job_runs
        WHERE job_name = 'sec_per_cik_poll' AND started_at > now() - interval '48 hours'
        """,
    )
    # ⚠ Bounds the OLD path only, and status='success' permits nonzero poll_errors.
    print(f"M4 sec_per_cik_poll 48h (old path): runs={runs} avg={avg_s}s max={max_s}s success={ok} failed={bad}")


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        with conn.cursor() as cur:
            _m5_state_census(cur)
            _m0_cik_shape(cur)
            _m1_prefixes(cur)
            _m2_cik_budget(cur)
            _m6_population(cur)
            _m3_collisions(cur)
            _m4_runtime(cur)


if __name__ == "__main__":
    main()
