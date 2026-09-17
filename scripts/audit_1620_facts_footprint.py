"""#1620 instrument — what ``financial_facts_raw``'s footprint actually is.

#3117 ranks this relation priority 4 and asks each child to separate four cost
sources a single size figure conflates: **unnecessary content**, **redundant
representation**, **indexes**, and **reclaimable free space**. #1620 itself names
two hypotheses — concept-interning and a droppable surrogate PK — and asks for
them to be *rechecked, not blindly reversed*.

This script produces that separation, and answers both hypotheses with measured
figures. Every number is computed at run time; none is written into prose here
or on the ticket without the query that produced it (project rule: "never
hardcode a derived statistic").

Read the sections in this order:

* ``--census`` (default) — read-only. Sizes, per-band fill, index scan counts.
* ``--rebuild-probe`` — a NON-destructive ``TEMP`` reconstruction that measures
  the minimum size of the same rows, plus the interning arm. Writable session.
* ``--plan`` — dry-run reclaim scope. A proposal, not an authorisation.
* ``--apply`` — executes the plan, one partition at a time.

Measuring live bytes — the trap this script fell into first
-----------------------------------------------------------
The obvious estimator is ``sum(pg_column_size(row)) + tuple_header``. **That
double-counts.** ``pg_column_size`` of a whole-row composite ALREADY includes the
23-byte tuple header: on a table of one ``bigint NOT NULL`` column it returns 32,
not 8. The first draft of this instrument added a further 27 bytes per row and
overstated live bytes by 101.1 MiB, which is why several partitions reported a
fill ratio above 100%.

The estimator here is therefore ``pg_column_size(row) + 4`` — the composite plus
its 4-byte line pointer — and it is **cross-validated against an independent
measurement** rather than trusted: ``--rebuild-probe`` builds the same rows into
a fresh relation and compares. At the time of writing those two agree to 1.4%
(755.9 MiB estimated vs 766.5 MiB rebuilt), the residual being per-page headers
and alignment, which the estimator does not model.

⚠ **The error direction matters and is the opposite of what it looks like.**
Under-stating live bytes lowers the computed fill ratio, which makes a partition
look emptier and therefore MORE eligible — i.e. it OVERSTATES reclaimable space.
Over-stating live bytes is the conservative direction for this predicate. The
``+4`` estimator errs low by roughly 2.3 bytes/row (page headers), so
:func:`_selected_partitions` inflates its projection by
:data:`_COMPACT_OVERHEAD_ALLOWANCE` before treating any difference as
reclaimable.

Selection predicate
-------------------
Selection is an **absolute reclaimable-byte floor**, not a fill ratio. A ratio
has two defects this relation actually exhibits: a partition holding a handful
of rows still occupies at least one 8 KiB page, so it can sit permanently below
any ratio threshold and be re-selected on every run forever; and a ratio
threshold has an arbitrary boundary where a 19.9% partition qualifies outright
on evidence gathered from a 1.1-6.6% population.

An absolute floor converges, but ONLY together with the overhead allowance
above: the estimator's unmodelled page overhead scales with partition size, so
without the allowance a compacted 2 GiB partition would report ~28.7 MiB of
phantom reclaim, clear a 16 MiB floor on its own, and be rewritten forever.
With both, a compacted partition projects to its current size at any size and
drops out. Verified empirically — a re-run immediately after the first apply
selected 0 partitions.

What ``--apply`` does and does not guarantee
--------------------------------------------
``VACUUM FULL`` is row-preserving by PostgreSQL's own definition; it rewrites the
relation, it does not filter it. So a before/after ``count(*)`` is NOT the
safety property it appears to be:

* it races with legitimate concurrent writes, so a **correct** rewrite can fail
  the assertion and a broken one can pass it under balanced insert/delete;
* it cannot distinguish a rewrite from a no-op.

This script therefore captures ``n_tup_ins``/``n_tup_del`` around each partition
and reports what it OBSERVED rather than asserting quiescence. ⚠ Those counters
are flushed asynchronously by each backend, so "no writes observed" is
corroboration and never proof — a write committing during a sub-second rewrite
can still read as zero. A count inequality is escalated to an invariant failure
whatever the counters say, because a rewrite is row-preserving by definition and
an inequality always wants a human.

``lock_timeout`` is set before each statement. Read its guarantee narrowly: it
bounds how long the ``ACCESS EXCLUSIVE`` request WAITS, not how long it is held,
and while that request waits it queues other conflicting requests behind it.
It is a blast-radius limiter, not an "only runs when idle" guarantee.

⚠ ``financial_facts_retention_sweep`` runs daily at 02:45 UTC and its DELETE is
not partition-key-restricted, so it can touch every leaf. ``--apply`` refuses to
start inside that window.

⚠ ``VACUUM FULL`` does not update planner statistics, so each partition is
``ANALYZE``d immediately afterwards. Skipping that trades disk for plans.

Run::

    PYTHONPATH=. uv run python scripts/audit_1620_facts_footprint.py
    PYTHONPATH=. uv run python scripts/audit_1620_facts_footprint.py --rebuild-probe
    PYTHONPATH=. uv run python scripts/audit_1620_facts_footprint.py --plan
    PYTHONPATH=. uv run python scripts/audit_1620_facts_footprint.py --apply

Exit status is 1 when any invariant fails, so a silent pass cannot be mistaken
for a clean one.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import UTC, datetime
from typing import Any, LiteralString, cast

import psycopg
from psycopg import sql

from app.config import settings
from scripts._dev_guard import assert_dev_environment

PARENT = "financial_facts_raw"

# Absolute floor, in bytes, of reclaimable space that makes a partition worth an
# ACCESS EXCLUSIVE rewrite. Chosen by construction, not from a published rule —
# there is no PostgreSQL formulation for "worth compacting". 16 MiB is two
# orders of magnitude above the 8 KiB page granularity that makes a ratio
# predicate oscillate, and below the smallest partition this relation actually
# wants compacted. Frozen here so the choice is one line to audit, not implicit.
RECLAIM_FLOOR_BYTES = 16 * 1024 * 1024

# Headroom added to the projected post-rewrite size, absorbing the per-page
# headers and alignment the live-bytes estimator does not model. Measured drift
# against a real rebuild is 1.4%; 5% is ~3.5x that, so a compacted partition
# projects to its current size and drops out of selection at ANY size. Without
# this the phantom gap scales with the partition and the predicate never
# converges (Codex checkpoint 2).
_COMPACT_OVERHEAD_ALLOWANCE = 0.05

# Bounds the ACCESS EXCLUSIVE *acquisition* wait. See the module docstring for
# what this does and does not promise.
LOCK_TIMEOUT = "5s"

# financial_facts_retention_sweep, daily 02:45 UTC (app/workers/scheduler.py).
# Its DELETE is not partition-key-restricted, so it can touch every leaf.
_SWEEP_WINDOW_UTC = (2, 30, 3, 15)

# The five indexes migration 156 builds on the parent. Re-created verbatim by
# --rebuild-probe so the reconstruction measures the same objects.
REBUILD_INDEXES: list[tuple[str, str]] = [
    (
        "uq_facts_raw_identity",
        "CREATE UNIQUE INDEX {name} ON {tbl} (instrument_id, concept, unit, "
        "COALESCE(period_start, '0001-01-01'::date), period_end, accession_number)",
    ),
    (
        "idx_facts_raw_instrument_concept",
        "CREATE INDEX {name} ON {tbl} (instrument_id, concept, period_end DESC)",
    ),
    (
        "financial_facts_raw_pkey",
        "CREATE UNIQUE INDEX {name} ON {tbl} (fact_id, period_end)",
    ),
    (
        "idx_facts_raw_retention_ranking",
        "CREATE INDEX {name} ON {tbl} (instrument_id, form_type, accession_number, filed_date)",
    ),
    (
        "idx_facts_raw_retention_evict",
        "CREATE INDEX {name} ON {tbl} (instrument_id, accession_number)",
    ),
]

# ``pg_column_size(row)`` already carries the 23-byte tuple header (see the
# module docstring). The only per-row cost it omits that this estimator models
# is the 4-byte line pointer; per-page headers are left unmodelled and show up
# as the ~1.4% residual against the rebuild.
_LIVE_BYTES = "sum(pg_column_size(f.*) + 4)"

_PARTITION_FOOTPRINT = f"""
WITH payload AS (
    SELECT f.tableoid AS oid, count(*) AS rows, {_LIVE_BYTES} AS live
    FROM {PARENT} f GROUP BY 1
)
SELECT c.oid,
       c.relname,
       coalesce(p.rows, 0) AS rows,
       coalesce(p.live, 0) AS live,
       pg_relation_size(c.oid) AS heap,
       pg_total_relation_size(c.oid) - pg_relation_size(c.oid) AS aux,
       coalesce(s.n_tup_ins, 0) AS ins,
       coalesce(s.n_tup_del, 0) AS del,
       coalesce(s.n_dead_tup, 0) AS dead,
       coalesce(s.autovacuum_count, 0) AS autovac
FROM pg_class c
JOIN pg_inherits i ON i.inhrelid = c.oid
JOIN pg_class par ON par.oid = i.inhparent
LEFT JOIN payload p ON p.oid = c.oid
LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
WHERE par.relname = %(parent)s AND c.relkind = 'r'
ORDER BY pg_total_relation_size(c.oid) DESC
"""


def _mib(n: float) -> str:
    return f"{n / 1048576:.1f}"


def _sql(text: str) -> LiteralString:
    """Narrow a build-time-constant SQL string for the type checker.

    Every caller passes a module-level literal or a literal built from one; no
    caller interpolates a runtime value. Identifiers that DO vary at runtime
    (partition names) go through :func:`sql.Identifier` instead.
    """
    return cast(LiteralString, text)


def _fetch(cur: psycopg.Cursor[Any], query: str, params: Any = None) -> list[tuple[Any, ...]]:
    cur.execute(_sql(query), params)
    return cur.fetchall()


# ─────────────────────────── census (read-only) ────────────────────────────


def _census(cur: psycopg.Cursor[Any]) -> list[str]:
    """Sections 1-4. Returns a list of invariant failures (empty == clean)."""
    failures: list[str] = []

    parts = _fetch(cur, _PARTITION_FOOTPRINT, {"parent": PARENT})
    total_rows = _fetch(cur, f"SELECT count(*) FROM {PARENT}")[0][0]

    print("\n── 1. Relation totals ──────────────────────────────────────────")
    heap = sum(p[4] for p in parts)
    aux = sum(p[5] for p in parts)
    live = sum(p[3] for p in parts)
    print(f"leaf partitions           {len(parts)}")
    print(f"rows (exact count(*))     {total_rows:,}")
    print(f"heap (main fork)          {_mib(heap)} MiB")
    print(f"live payload (estimated)  {_mib(live)} MiB   [pg_column_size(row) + 4]")
    print(f"indexes + aux forks       {_mib(aux)} MiB")
    print(f"TOTAL                     {_mib(heap + aux)} MiB")

    # n_live_tup is an estimator, not a count. Printed next to the exact count
    # precisely so the gap is visible rather than inherited by the next reader.
    est = _fetch(
        cur,
        """SELECT coalesce(sum(s.n_live_tup), 0) FROM pg_stat_user_tables s
           JOIN pg_class c ON c.oid = s.relid
           JOIN pg_inherits i ON i.inhrelid = c.oid
           JOIN pg_class p ON p.oid = i.inhparent WHERE p.relname = %(parent)s""",
        {"parent": PARENT},
    )[0][0]
    print(f"\n⚠ pg_stat n_live_tup      {est:,}  — an ESTIMATE; exact count is {total_rows:,}")

    print("\n── 2. Index footprint and lifetime use ─────────────────────────")
    idx = _fetch(
        cur,
        """
        SELECT parent.relname,
               sum(pg_relation_size(child.oid)) AS bytes,
               sum(coalesce(s.idx_scan, 0)) AS scans,
               count(*) AS n_parts
        FROM pg_class parent
        JOIN pg_inherits i ON i.inhparent = parent.oid
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_index x ON x.indexrelid = child.oid
        JOIN pg_class t ON t.oid = x.indrelid
        JOIN pg_inherits ti ON ti.inhrelid = t.oid
        JOIN pg_class tp ON tp.oid = ti.inhparent
        LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = child.oid
        WHERE tp.relname = %(parent)s
        GROUP BY 1 ORDER BY 2 DESC
        """,
        {"parent": PARENT},
    )
    print(f"{'index':<36} {'MiB':>9} {'lifetime scans':>16} {'leaves':>7}")
    for name, b, scans, n in idx:
        print(f"{name:<36} {_mib(b):>9} {scans:>16,} {n:>7}")
    idx_total = sum(r[1] for r in idx)
    print(f"{'indexes proper':<36} {_mib(idx_total):>9}")
    print(
        f"{'auxiliary forks (fsm/vm/toast)':<36} {_mib(aux - idx_total):>9}   — why 'indexes' and 'total - heap' differ"
    )
    if len(idx) != len(REBUILD_INDEXES):
        failures.append(f"expected {len(REBUILD_INDEXES)} parent indexes, found {len(idx)}")

    print("\n── 3. Where the bytes are ──────────────────────────────────────")
    bands: dict[str, list[tuple[Any, ...]]] = {"EMPTY (0-byte heap)": [], "COLD": [], "HOT": []}
    for p in parts:
        if p[4] == 0:
            bands["EMPTY (0-byte heap)"].append(p)
        elif p[3] / p[4] < 0.20:
            bands["COLD"].append(p)
        else:
            bands["HOT"].append(p)
    print(f"{'band':<20} {'parts':>6} {'rows':>10} {'live':>9} {'heap':>9} {'idx+aux':>9} {'total':>9}")
    for label, members in bands.items():
        if not members:
            continue
        r = sum(m[2] for m in members)
        lv = sum(m[3] for m in members)
        hp = sum(m[4] for m in members)
        ax = sum(m[5] for m in members)
        print(f"{label:<20} {len(members):>6} {r:>10,} {_mib(lv):>9} {_mib(hp):>9} {_mib(ax):>9} {_mib(hp + ax):>9}")
    # The bands must reconstruct the whole, or one of them is being double-counted.
    if sum(len(m) for m in bands.values()) != len(parts):
        failures.append("band partition does not cover every leaf")
    if sum(sum(m[2] for m in v) for v in bands.values()) != total_rows:
        failures.append("band row counts do not sum to the exact relation count")

    print("\n── 4. Churn: which band is actually written ────────────────────")
    print(f"{'band':<20} {'n_tup_ins':>12} {'n_tup_del':>12} {'dead':>10} {'autovacuums':>12}")
    for label, members in bands.items():
        if not members:
            continue
        print(
            f"{label:<20} {sum(m[6] for m in members):>12,} {sum(m[7] for m in members):>12,} "
            f"{sum(m[8] for m in members):>10,} {sum(m[9] for m in members):>12,}"
        )
    print(
        "\n⚠ These counters are cumulative with no recorded reset, so they are a"
        "\n  RATIO between bands, never a rate. pg_stat_database.stats_reset is"
        f"\n  {_fetch(cur, 'SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()')[0][0]}."
    )
    return failures


# ───────────────────── rebuild probe (writable, TEMP) ──────────────────────


def _rebuild(cur: psycopg.Cursor[Any], label: str, tbl: str, source: str) -> tuple[int, int]:
    """Build ``source`` rows into a fresh TEMP relation + the five indexes."""
    cur.execute(_sql(f"CREATE TEMP TABLE {tbl} AS {source}"))
    heap = _fetch(cur, "SELECT pg_relation_size(%s)", (tbl,))[0][0]
    idx_total = 0
    for i, (name, ddl) in enumerate(REBUILD_INDEXES):
        iname = f"{tbl}_i{i}"
        cur.execute(_sql(ddl.format(name=iname, tbl=tbl)))
        size = _fetch(cur, "SELECT pg_relation_size(%s)", (iname,))[0][0]
        idx_total += size
        print(f"    {name:<36} {_mib(size):>9} MiB")
    print(f"  {label}: heap {_mib(heap)} MiB + indexes {_mib(idx_total)} MiB = {_mib(heap + idx_total)} MiB")
    return heap, idx_total


def _rebuild_probe(cur: psycopg.Cursor[Any]) -> list[str]:
    """Measure the minimum size of the same rows, and the interning arm.

    Non-destructive: everything lands in TEMP relations dropped at disconnect.
    This cannot run inside the census's read-only transaction, which is why it
    is a separate session and a separate flag.
    """
    failures: list[str] = []
    cur.execute("SET temp_buffers = '256MB'")

    print("\n── 5. Minimum size, measured by reconstruction ─────────────────")
    print("  whole relation, as-is:")
    heap, idx = _rebuild(cur, "whole relation", "fp_all", f"SELECT * FROM {PARENT}")

    # Cross-validate the live-bytes estimator against this independent build.
    live = _fetch(cur, f"SELECT {_LIVE_BYTES} FROM {PARENT} f")[0][0]
    drift = abs(heap - live) / heap
    print(f"\n  estimator cross-check: estimate {_mib(live)} MiB vs rebuilt heap {_mib(heap)} MiB")
    print(f"  drift {drift * 100:.1f}% (page headers + alignment, unmodelled by the estimator)")
    if drift > 0.05:
        failures.append(
            f"live-bytes estimator drifts {drift * 100:.1f}% from the rebuilt heap "
            "(>5%); the estimator no longer describes this relation"
        )

    print("\n── 6. Lever 1 — concept + taxonomy interning ───────────────────")
    card = _fetch(
        cur,
        f"""SELECT count(DISTINCT concept), avg(length(concept))::numeric(6,2),
                   count(DISTINCT taxonomy), avg(length(taxonomy))::numeric(6,2)
            FROM {PARENT}""",
    )[0]
    print(f"  concept: {card[0]} distinct, mean length {card[1]}")
    print(f"  taxonomy: {card[2]} distinct, mean length {card[3]}")
    cur.execute(
        _sql(
            f"""CREATE TEMP TABLE fp_intern AS SELECT f.fact_id, f.instrument_id,
                (dense_rank() OVER (ORDER BY f.taxonomy))::smallint AS taxonomy_id,
                (dense_rank() OVER (ORDER BY f.concept))::smallint AS concept_id,
                f.unit, f.period_start, f.period_end, f.val, f.frame, f.accession_number,
                f.form_type, f.filed_date, f.fiscal_year, f.fiscal_period, f.decimals,
                f.ingestion_run_id, f.fetched_at
            FROM {PARENT} f"""
        )
    )
    i_heap = _fetch(cur, "SELECT pg_relation_size('fp_intern')")[0][0]
    cur.execute(
        _sql(
            "CREATE UNIQUE INDEX fp_intern_id ON fp_intern (instrument_id, concept_id, unit, "
            "COALESCE(period_start, '0001-01-01'::date), period_end, accession_number)"
        )
    )
    cur.execute(_sql("CREATE INDEX fp_intern_ic ON fp_intern (instrument_id, concept_id, period_end DESC)"))
    i_id, i_ic = _fetch(cur, "SELECT pg_relation_size('fp_intern_id'), pg_relation_size('fp_intern_ic')")[0]
    asis_id = _fetch(cur, "SELECT pg_relation_size('fp_all_i0'), pg_relation_size('fp_all_i1')")[0]
    saving = (heap - i_heap) + (asis_id[0] - i_id) + (asis_id[1] - i_ic)
    print(f"  heap               {_mib(heap):>9} -> {_mib(i_heap):>9} MiB")
    print(f"  identity index     {_mib(asis_id[0]):>9} -> {_mib(i_id):>9} MiB")
    print(f"  instrument_concept {_mib(asis_id[1]):>9} -> {_mib(i_ic):>9} MiB")
    print(f"  interning saves {_mib(saving)} MiB against a {_mib(heap + idx)} MiB rebuilt relation")
    print("\n  ⚠ A prototype figure, not a net migration saving: it excludes the")
    print("    dictionary table, the FK, and the partitioned layout's own overhead.")
    return failures


# ──────────────────────────── plan and apply ───────────────────────────────


def _selected_partitions(cur: psycopg.Cursor[Any]) -> list[dict[str, Any]]:
    """Partitions whose reclaimable bytes clear the absolute floor.

    ``projected`` scales the partition's CURRENT total by the ratio the whole
    relation achieves on reconstruction. Using a relation-wide ratio rather than
    a per-partition rebuild keeps this cheap enough to recompute at apply time;
    it is a projection and is labelled as one everywhere it is printed.

    ⚠ The live-bytes estimator does not model per-page headers or alignment, so
    even a freshly-rewritten partition reports a small phantom gap (measured at
    1.4% on this relation). Left unallowed-for, that gap SCALES WITH SIZE and
    eventually exceeds the absolute floor on its own: a compacted 2 GiB partition
    would show ~28.7 MiB of nonexistent reclaim and be re-selected on every run
    forever, which is precisely the non-convergence the floor exists to avoid.
    :data:`_COMPACT_OVERHEAD_ALLOWANCE` absorbs it.
    """
    parts = _fetch(cur, _PARTITION_FOOTPRINT, {"parent": PARENT})
    out: list[dict[str, Any]] = []
    for oid, name, rows, live, heap, aux, *_ in parts:
        if heap == 0:
            continue
        # Index bytes scale with entry count, heap with payload; both collapse to
        # roughly the live fraction. Projection, not measurement.
        frac = min(1.0, (live / heap) * (1.0 + _COMPACT_OVERHEAD_ALLOWANCE)) if heap else 1.0
        projected = int((heap + aux) * frac)
        reclaimable = (heap + aux) - projected
        if reclaimable >= RECLAIM_FLOOR_BYTES:
            out.append(
                {
                    "oid": oid,
                    "name": name,
                    "rows": rows,
                    "current": heap + aux,
                    "projected": projected,
                    "reclaimable": reclaimable,
                }
            )
    out.sort(key=lambda r: -r["reclaimable"])
    return out


def _print_plan(sel: list[dict[str, Any]]) -> None:
    print("\n── 7. Reclaim plan (dry run) ───────────────────────────────────")
    print(f"floor = {_mib(RECLAIM_FLOOR_BYTES)} MiB reclaimable per partition\n")
    print(f"{'partition':<36} {'rows':>9} {'current':>9} {'projected':>10} {'reclaim':>9}")
    for r in sel:
        print(
            f"{r['name']:<36} {r['rows']:>9,} {_mib(r['current']):>9} "
            f"{_mib(r['projected']):>10} {_mib(r['reclaimable']):>9}"
        )
    print(
        f"\n{len(sel)} partitions; {_mib(sum(r['current'] for r in sel))} MiB current, "
        f"{_mib(sum(r['reclaimable'] for r in sel))} MiB projected reclaim."
    )
    print("\nStatements (one per partition, each its own autocommit statement):")
    for r in sel[:3]:
        print(f"  VACUUM (FULL, ANALYZE) {r['name']};")
    if len(sel) > 3:
        print(f"  … and {len(sel) - 3} more")
    print("\n⚠ A proposal, not an authorisation. #3117 authorises no sweep.")


def _refuse_in_sweep_window() -> None:
    now = datetime.now(UTC)
    h0, m0, h1, m1 = _SWEEP_WINDOW_UTC
    if (h0, m0) <= (now.hour, now.minute) <= (h1, m1):
        raise SystemExit(
            f"refusing to run: {now:%H:%M} UTC is inside the "
            f"financial_facts_retention_sweep window ({h0:02d}:{m0:02d}-{h1:02d}:{m1:02d} UTC). "
            "Its DELETE is not partition-key-restricted and can touch every leaf."
        )


def _apply(conn: psycopg.Connection[Any]) -> list[str]:
    """Compact each selected partition. Row-preserving; deletes nothing."""
    _refuse_in_sweep_window()
    failures: list[str] = []
    cur = conn.cursor()
    sel = _selected_partitions(cur)
    if not sel:
        print("nothing selected — every partition is below the reclaim floor.")
        return failures

    before_total = _fetch(cur, f"SELECT count(*) FROM {PARENT}")[0][0]
    print("\n── 8. Apply ────────────────────────────────────────────────────")
    print(f"relation count(*) before: {before_total:,}\n")

    for r in sel:
        name = sql.Identifier(r["name"])
        stats = "SELECT coalesce(n_tup_ins,0), coalesce(n_tup_del,0) FROM pg_stat_user_tables WHERE relid = %s"
        rows_q = sql.SQL("SELECT count(*) FROM {}").format(name)

        cur.execute(_sql(stats), (r["oid"],))
        ins0, del0 = cur.fetchone() or (0, 0)
        cur.execute(rows_q)
        n0 = (cur.fetchone() or (0,))[0]

        cur.execute(_sql(f"SET lock_timeout = '{LOCK_TIMEOUT}'"))
        t0 = time.monotonic()
        try:
            cur.execute(sql.SQL("VACUUM (FULL, ANALYZE) {}").format(name))
        except psycopg.errors.LockNotAvailable:
            print(f"{r['name']:<36} SKIPPED — lock not acquired within {LOCK_TIMEOUT}")
            continue
        except psycopg.Error as exc:
            # Never truncate: the discriminating detail is usually at the end.
            print(f"{r['name']:<36} FAILED — {exc}")
            failures.append(f"{r['name']}: {exc}")
            continue
        held = time.monotonic() - t0

        cur.execute(rows_q)
        n1 = (cur.fetchone() or (0,))[0]
        cur.execute(_sql(stats), (r["oid"],))
        ins1, del1 = cur.fetchone() or (0, 0)
        after = _fetch(cur, "SELECT pg_total_relation_size(%s)", (r["oid"],))[0][0]

        # ⚠ Unchanged counters are CORROBORATION, never proof of quiescence:
        # backends flush the cumulative statistics asynchronously, so a write
        # that committed during a sub-second rewrite can still read as zero.
        # The wording below says "observed" for that reason, and a count
        # inequality is escalated whatever the counters say — a rewrite is
        # row-preserving by definition, so an inequality always wants a human.
        no_writes_seen = (ins1 - ins0) == 0 and (del1 - del0) == 0
        if n0 != n1:
            seen = (
                "no writes observed (counters flush asynchronously, so this is corroboration, not proof)"
                if no_writes_seen
                else f"{ins1 - ins0} ins / {del1 - del0} del observed concurrently"
            )
            failures.append(f"{r['name']}: rows {n0:,} -> {n1:,}; {seen}")
            verdict = f"⛔ ROWS CHANGED {n0:,} -> {n1:,}"
        elif no_writes_seen:
            verdict = f"rows {n1:,} preserved (no writes observed)"
        else:
            verdict = f"rows {n1:,} preserved, {ins1 - ins0} ins / {del1 - del0} del concurrent"
        print(f"{r['name']:<36} {_mib(r['current']):>8} -> {_mib(after):>8} MiB  lock {held:5.2f}s  {verdict}")

    print(f"\nrelation count(*) after:  {_fetch(cur, f'SELECT count(*) FROM {PARENT}')[0][0]:,}")
    return failures


# ────────────────────────────────── main ───────────────────────────────────


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--census", action="store_true", help="read-only audit (the default; explicit alias)")
    ap.add_argument("--rebuild-probe", action="store_true", help="TEMP reconstruction (writable session)")
    ap.add_argument("--plan", action="store_true", help="dry-run reclaim scope")
    ap.add_argument("--apply", action="store_true", help="execute the reclaim plan")
    args = ap.parse_args()
    assert_dev_environment()

    failures: list[str] = []
    if args.apply:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            failures += _apply(conn)
    elif args.rebuild_probe:
        # Writable (TEMP DDL) but REPEATABLE READ, so the as-is rebuild, the
        # estimator cross-check and the interned rebuild all describe ONE row
        # population. Under autocommit each would take its own snapshot and any
        # concurrent ingest would be reported as an interning saving.
        with psycopg.connect(settings.database_url) as conn:
            conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            failures += _rebuild_probe(conn.cursor())
    else:
        with psycopg.connect(settings.database_url) as conn:
            conn.read_only = True
            conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            cur = conn.cursor()
            db, xact = _fetch(cur, "SELECT current_database(), now()")[0]
            print(f"database={db}  snapshot=REPEATABLE READ  read_only=True  at={xact}")
            failures += _census(cur)
            if args.plan:
                _print_plan(_selected_partitions(cur))

    if failures:
        print("\n⛔ INVARIANT FAILURES:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\n✅ no invariant failures")
    return 0


if __name__ == "__main__":
    sys.exit(main())
