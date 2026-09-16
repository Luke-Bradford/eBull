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

import sys
from typing import Any, LiteralString

import psycopg

from app.config import settings
from app.services.data_freshness import POLL_REPOLL_INTERVAL

# The states each lane treats as candidates (``app/services/data_freshness.py``),
# inlined as literals because this script must describe what the selector does
# today, not track a refactor of it.
_POLL_STATES = "('unknown','current','expected_filing_overdue')"
_RECHECK_STATES = "('never_filed','error')"

# Budget as the scheduler runs it today. ``scheduler.sec_per_cik_poll`` passes
# no override and ``_adapt_zero_arg`` discards the params dict, so max_ciks=100
# is a code constant and NOT operator-tunable.
#
# ⚠ Since #3109 the split is asymmetric: ``_RECHECK_BUDGET`` is a CAP selected
# FIRST, and the poll lane gets ``_TOTAL_BUDGET - len(recheck_due)``. So 66 is
# the poll lane's WORST case (recheck full), not its value.
_TOTAL_BUDGET = 100
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
            AND next_poll_at <= now()
          ORDER BY next_poll_at ASC, cik ASC LIMIT %s
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
          SELECT next_poll_at FROM data_freshness_index
          WHERE state IN {_POLL_STATES}
            AND next_poll_at <= now()
          ORDER BY next_poll_at ASC, cik ASC LIMIT 200
        )
        SELECT count(*), count(DISTINCT next_poll_at) FROM p
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
          SELECT lpad(cik, 10, '0') AS cik_padded, next_poll_at
          FROM data_freshness_index
          WHERE state IN {_POLL_STATES}
            AND next_poll_at <= now()
            AND cik ~ '{_CIK_SHAPE}'
        ), ranked AS (
          SELECT cik_padded, count(*) AS t FROM due GROUP BY cik_padded
          ORDER BY min(next_poll_at), cik_padded
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
          SELECT lpad(cik, 10, '0') AS cik_padded, next_poll_at, last_known_filing_id
          FROM data_freshness_index
          WHERE state IN {_POLL_STATES}
            AND next_poll_at <= now()
            AND cik ~ '{_CIK_SHAPE}'
        ), ranked AS (
          SELECT cik_padded FROM due GROUP BY cik_padded
          ORDER BY min(next_poll_at), cik_padded
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
        ("due", " AND next_poll_at <= now()"),
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
          AND next_poll_at <= now()
          AND last_known_filing_id IS NULL
        """,
    )
    print(f"    due rows with no accession watermark: {unwatermarked:,}")

    cur.execute(
        f"""
        SELECT source, count(*) FROM data_freshness_index
        WHERE state IN {_POLL_STATES}
          AND next_poll_at <= now()
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


def _r1_rotation_discriminator(conn: psycopg.Connection[Any], cur: psycopg.Cursor[Any]) -> None:
    """Does a completed poll actually REMOVE a CIK from the queue head? (#3109)

    ⚠ This is the discriminator, and it exists because the obvious measurement
    cannot answer the question. ``last_polled_at`` is overwritten in place, so
    its distribution across hours cannot distinguish "the same rows every hour"
    from "different rows that happen to share a timestamp". Codex checkpoint 1
    was right to reject that inference.

    So ask the queue directly instead: run the REAL selector now, and diff its
    CIK set against the CIKs the most recent run actually polled.

      polled_only == 0  →  polling removed nothing. The head is pinned.
      polled_only  > 0  →  polling rotates the queue.

    Measured 2026-09-16T23:30Z, before the fix: selector 66, polled 48,
    intersection 48, **polled_only 0**.

    The selector is imported rather than re-expressed here on purpose: the
    thing under test is the selector, and the comparand (``last_polled_at``)
    comes from the corpus, so this is not a verifier pinned to its own code.
    ⚠ All four counts are printed, not just the verdict — an EMPTY selector
    would otherwise produce ``polled_only = len(polled)`` and read as a pass.
    """
    from app.services.data_freshness import ciks_due_for_poll

    selected = {b[0].cik for b in ciks_due_for_poll(conn, limit=_POLL_BUDGET) if b}
    cur.execute(
        """
        SELECT DISTINCT lpad(cik, 10, '0') FROM data_freshness_index
        WHERE last_polled_at = (SELECT max(last_polled_at) FROM data_freshness_index)
        """
    )
    polled = {r[0] for r in cur.fetchall()}
    print(
        f"R1 rotation discriminator: selector_now={len(selected)} last_run_polled={len(polled)} "
        f"intersection={len(selected & polled)} polled_only={len(polled - selected)}"
    )
    if not selected:
        print("    ⚠ selector returned NOTHING — polled_only is meaningless here, not a pass")
    elif polled and not (polled - selected):
        print("    ⚠ polled_only=0 — a completed poll removed no CIK from the queue. HEAD IS PINNED.")
    else:
        print("    ✅ polling removes CIKs from the queue head")


def _r2_reach(cur: psycopg.Cursor[Any]) -> None:
    """How much of the index has EVER been polled, and how fast can it rotate?"""
    rows, ciks, polled_rows, polled_ciks, oldest = _one(
        cur,
        f"""
        SELECT count(*) FILTER (WHERE state IN {_POLL_STATES}),
               count(DISTINCT lpad(cik, 10, '0'))
                   FILTER (WHERE state IN {_POLL_STATES} AND cik ~ %s),
               count(*) FILTER (WHERE last_polled_at IS NOT NULL),
               count(DISTINCT lpad(cik, 10, '0')) FILTER (WHERE last_polled_at IS NOT NULL),
               min(last_polled_at)
        FROM data_freshness_index
        """,
        [_CIK_SHAPE],
    )
    share = f"{polled_rows / rows:.2%}" if rows else "n/a"
    print(f"R2 reach: poll-lane rows={rows:,} ciks={ciks:,}")
    print(f"    ever polled: rows={polled_rows:,} ({share}) ciks={polled_ciks:,} since={oldest}")

    # ⚠ Read this BEFORE trusting R1. Immediately after the sql/389 backfill
    # every row shares one ``next_poll_at``, so the queue is ordered purely by
    # the ``cik`` tie-break — and R1's ``polled_only`` flips to non-zero for
    # that reason alone, not because exclusion is working. Only once the
    # clocks have SPREAD (distinct > 1, max in the future) is R1 reporting on
    # rotation rather than on the tie-break.
    distinct, lo, hi, future = _one(
        cur,
        f"""
        SELECT count(DISTINCT next_poll_at), min(next_poll_at), max(next_poll_at),
               count(*) FILTER (WHERE next_poll_at > now())
        FROM data_freshness_index WHERE state IN {_POLL_STATES}
        """,
    )
    print(f"    next_poll_at spread: distinct={distinct:,} min={lo} max={hi} excluded_now={future:,}")
    if distinct <= 1:
        print("    ⚠ all clocks identical (fresh backfill) — R1 is measuring the cik tie-break, not exclusion")

    # ⚠ Computed, never written down — the interval and the budget both live in
    # code, and a hand-copied cycle time goes stale the moment either moves.
    # ``_RECHECK_BUDGET`` is the CAP; the poll lane gets the residual, so the
    # drain rate depends on how full the recheck lane actually is right now.
    (recheck_rows,) = _one(cur, f"SELECT count(*) FROM data_freshness_index WHERE state IN {_RECHECK_STATES}")
    recheck_ciks_now = min(recheck_rows, _RECHECK_BUDGET)
    per_tick = _TOTAL_BUDGET - recheck_ciks_now
    per_day = per_tick * 24
    if ciks and per_day:
        cycle_d = ciks / per_day
        arrivals = ciks / (POLL_REPOLL_INTERVAL.total_seconds() / 86400)
        print(
            f"    budget: total={_TOTAL_BUDGET} recheck_claim={recheck_ciks_now} "
            f"→ poll={per_tick}/tick = {per_day:,}/day"
        )
        print(
            f"    full sweep={cycle_d:.2f}d vs re-poll interval="
            f"{POLL_REPOLL_INTERVAL.days}d · steady arrivals={arrivals:,.0f}/day "
            f"vs drain={per_day:,}/day → {'DRAINS' if per_day > arrivals else 'SATURATED'}"
        )


def main() -> None:
    rotation_only = "--rotation" in sys.argv
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        with conn.cursor() as cur:
            if not rotation_only:
                _m5_state_census(cur)
                _m0_cik_shape(cur)
                _m1_prefixes(cur)
                _m2_cik_budget(cur)
                _m6_population(cur)
                _m3_collisions(cur)
                _m4_runtime(cur)
            _r1_rotation_discriminator(conn, cur)
            _r2_reach(cur)


if __name__ == "__main__":
    main()
