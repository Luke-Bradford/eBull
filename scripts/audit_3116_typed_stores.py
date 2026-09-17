"""#3116 spike instrument — measure the large typed ownership + price stores.

#3117 ranks this priority 5 (the last child) and asks each store to end in
keep / compact / retire with evidence, separating four cost sources that a
single size figure conflates: **unnecessary content**, **redundant
representation**, **indexes**, and **reclaimable free space**.

Five stores are in scope, named by #3116's own measured starting point::

    ownership_institutions_observations   (partitioned)
    ownership_funds_observations          (partitioned)
    institutional_holdings                (legacy per-accession 13F rows)
    ownership_institutions_current        (mutable dedup snapshot)
    research_price_daily                  (research backtest corpus)

``financial_facts_raw`` is deliberately absent — #1620 owns it and discharged
it on 2026-09-17 (``ad0e75af``).

Every figure is computed at run time. None is written into prose here or on the
ticket without the query that produced it (project rule: "never hardcode a
derived statistic"). Read-only: the connection is opened ``read_only`` at
REPEATABLE READ, so all sections describe ONE database snapshot — that is not
decoration, since sections 1 and 4 are multi-minute full-table scans and under
READ COMMITTED the widths, the fill ratios and the no-op rate would each
describe a different state while reading as one report. The claim is *verified*
against ``pg_stat_activity.xact_start`` at the end rather than asserted.

**Exit status is 1 when any invariant fails.** Each invariant below names the
input that would fail it, because a check that cannot fail is not a check:

* ``source_document_id = source_accession`` on every institutions row — fails
  the moment any writer stores a per-row document id there (the insiders and
  funds writers already do), which would kill the drop-column lever.
* ``known_to IS NULL`` on every institutions observation — fails as soon as
  anything is soft-deleted, which would make free space a retention question
  rather than an upsert-churn question.
* no foreign key references any of the five — fails if a later migration adds
  one, which would change every retire/compact answer.

⚠ What this instrument does NOT measure, stated so its silence is not read as a
negative result: it reads no query plans, so a zero ``idx_scan`` here is a
lifetime counter and **not** evidence an index is dispensable (#3116 says so
explicitly); and it cannot see a reader outside the tree (a psql runbook, a
saved BI query).

⚠⚠ **Section 2's scan counts include this script's own scans.** Section 5 joins
``institutional_holdings`` to the observations table, and that join increments
``idx_scan`` on the very indexes section 2 reports — measured on 2026-09-17,
``idx_holdings_instrument_period`` read 0 before a run of this audit and 3
after. So a small non-zero count here may be nothing but the auditor walking
past. Treat anything under ~10 as indistinguishable from zero, and never quote
a scan count from a run of this tool as evidence that an application reader
exists.

⚠ ``pg_stat_*`` write counters on this cluster accumulate from an unknown epoch
(``pg_stat_database.stats_reset`` is NULL, same as #1620 found). Section 3
therefore reports them rate-free, as totals against the live row count, and
never as "per day".

Run::

    PYTHONPATH=. uv run python scripts/audit_3116_typed_stores.py
    PYTHONPATH=. uv run python scripts/audit_3116_typed_stores.py --rebuild-probe
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any, LiteralString, cast

import psycopg

from app.config import settings
from scripts._dev_guard import assert_dev_environment

PLAIN = ["research_price_daily", "institutional_holdings", "ownership_institutions_current"]
PARTITIONED = ["ownership_institutions_observations", "ownership_funds_observations"]
ALL_STORES = PLAIN + PARTITIONED

# ``pg_column_size(row)`` ALREADY carries the 23-byte tuple header — #1620's
# correction, extracted to docs/review-prevention-log.md — and, being a
# composite, it also carries inter-column alignment padding. The only per-row
# cost this estimator adds is the 4-byte line pointer. What stays unmodelled is
# the per-PAGE header and any partly-filled last page, which surface as a ~1-2%
# residual against a real rebuild (measured at 1.8% by --rebuild-probe against
# the 2025q4 leaf — which is why that probe exists rather than this estimate
# standing alone).
LIVE_BYTES = "sum(pg_column_size(t.*) + 4)"

# A leaf smaller than this is noise in the per-partition table; it is a display
# threshold only and no decision is taken from it.
LEAF_DISPLAY_FLOOR = 8 * 1024 * 1024

FAILURES: list[str] = []
WARNINGS: list[str] = []


def _fail(msg: str) -> None:
    FAILURES.append(msg)
    print(f"  ⚠ INVARIANT FAILED: {msg}")


def _sql(text: str) -> LiteralString:
    """Narrow a build-time-constant SQL string for the type checker.

    Every caller passes a module-level literal or a literal built from one.
    Table names come from the module-level lists above, never from input.
    """
    return cast(LiteralString, text)


def _q(cur: psycopg.Cursor[Any], query: str, params: Any = None) -> list[tuple[Any, ...]]:
    cur.execute(_sql(query), params)
    return cur.fetchall()


def _mib(n: float) -> str:
    return f"{n / 1048576:.1f}"


def _cols(cur: psycopg.Cursor[Any], table: str) -> list[str]:
    return [
        r[0]
        for r in _q(
            cur,
            """SELECT a.attname FROM pg_attribute a
               JOIN pg_class c ON c.oid = a.attrelid
               JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = %s
                 AND a.attnum > 0 AND NOT a.attisdropped
               ORDER BY a.attnum""",
            (table,),
        )
    ]


# ───────────────────────────── 1. footprint ──────────────────────────────


def _footprint(cur: psycopg.Cursor[Any]) -> None:
    """Heap / TOAST / index / live bytes per store, and fill ratio.

    Fill ratio is the whole point of the section: it separates "large because
    it holds data" from "large because it holds a high-water mark". #3116
    clause 3 requires heap, TOAST, indexes and reusable free space reported
    separately, so they are four columns and never one total.
    """
    print("\n═══ 1. FOOTPRINT (heap / toast / index / live bytes, separately) ═══")
    print(
        f"{'store':38s} {'rows':>12s} {'live MiB':>10s} {'heap MiB':>10s} "
        f"{'fill%':>7s} {'free MiB':>9s} {'toast':>7s} {'idx MiB':>9s}"
    )
    for t in ALL_STORES:
        t0 = time.time()
        rows, live = _q(cur, f"SELECT count(*), {LIVE_BYTES} FROM {t} t")[0]
        live = float(live or 0)
        heap, idx, toast = _q(
            cur,
            """SELECT sum(pg_relation_size(c.oid)),
                      sum(pg_indexes_size(c.oid)),
                      sum(coalesce(pg_total_relation_size(c.reltoastrelid), 0))
               FROM pg_class c
               WHERE c.oid = %s::regclass
                  OR c.oid IN (SELECT inhrelid FROM pg_inherits WHERE inhparent = %s::regclass)""",
            (t, t),
        )[0]
        heap, idx, toast = float(heap or 0), float(idx or 0), float(toast or 0)
        fill = 100 * live / heap if heap else 0.0
        print(
            f"{t:38s} {rows:12,d} {_mib(live):>10s} {_mib(heap):>10s} {fill:6.1f}% "
            f"{_mib(heap - live):>9s} {_mib(toast):>7s} {_mib(idx):>9s}  ({time.time() - t0:.0f}s)"
        )

    for p in PARTITIONED:
        print(f"\n  -- {p}: leaves above {_mib(LEAF_DISPLAY_FLOOR)} MiB heap --")
        for name, n, live, heap, idx in _q(
            cur,
            f"""WITH payload AS (
                    SELECT t.tableoid AS oid, count(*) AS n, {LIVE_BYTES} AS live
                    FROM {p} t GROUP BY 1)
                SELECT c.relname, coalesce(p.n, 0), coalesce(p.live, 0),
                       pg_relation_size(c.oid), pg_indexes_size(c.oid)
                FROM pg_class c
                JOIN pg_inherits i ON i.inhrelid = c.oid
                LEFT JOIN payload p ON p.oid = c.oid
                WHERE i.inhparent = %s::regclass AND c.relkind = 'r'
                  AND pg_relation_size(c.oid) >= %s
                ORDER BY c.relname DESC""",
            (p, LEAF_DISPLAY_FLOOR),
        ):
            print(
                f"    {name:52s} {n:10,d} live={_mib(live):>8s} heap={_mib(heap):>8s} "
                f"fill={100 * live / heap if heap else 0:5.1f}% idx={_mib(idx):>8s}"
            )


# ────────────────────────────── 2. indexes ───────────────────────────────


def _indexes(cur: psycopg.Cursor[Any]) -> None:
    """Index inventory with LIFETIME scan counts.

    ⚠ #3116 clause 3: "low index scan count alone cannot establish
    dispensability", and clause 3 also counts constraint/uniqueness indexes and
    rare recovery/backfill paths as consumers. Nothing here proposes dropping
    an index; the column exists so the proposal can name which claims rest on
    a counter and which on a reader.
    """
    print("\n═══ 2. INDEXES (size + lifetime scans; scans are NOT a dispensability test) ═══")
    for name, idxname, sz, scans, uniq, isprim in _q(
        cur,
        """SELECT c.relname, i.relname, pg_relation_size(i.oid),
                  coalesce(s.idx_scan, 0), x.indisunique, x.indisprimary
           FROM pg_index x
           JOIN pg_class i ON i.oid = x.indexrelid
           JOIN pg_class c ON c.oid = x.indrelid
           LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.oid
           WHERE c.relname = ANY(%s)
           ORDER BY c.relname, pg_relation_size(i.oid) DESC""",
        (PLAIN,),
    ):
        flag = "PK" if isprim else ("UQ" if uniq else "  ")
        print(f"  {flag} {name:32s} {idxname:48s} {_mib(sz):>9s} MiB  scans={scans:,}")

    for parent, pidx, sz, scans, uniq, isprim in _q(
        cur,
        """SELECT par.relname, pi.relname, sum(pg_relation_size(i.oid)),
                  sum(coalesce(s.idx_scan, 0)), x.indisunique, x.indisprimary
           FROM pg_class par
           JOIN pg_inherits ti ON ti.inhparent = par.oid
           JOIN pg_class leaf ON leaf.oid = ti.inhrelid
           JOIN pg_index x ON x.indrelid = leaf.oid
           JOIN pg_class i ON i.oid = x.indexrelid
           JOIN pg_inherits ii ON ii.inhrelid = i.oid
           JOIN pg_class pi ON pi.oid = ii.inhparent
           LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.oid
           WHERE par.relname = ANY(%s)
           GROUP BY 1, 2, 5, 6
           ORDER BY 1, 3 DESC""",
        (PARTITIONED,),
    ):
        flag = "PK" if isprim else ("UQ" if uniq else "  ")
        print(f"  {flag} {parent:32s} {pidx:48s} {_mib(sz):>9s} MiB  scans={scans:,}  (summed leaves)")


# ─────────────────────────── 3. churn / HOT health ────────────────────────


def _churn(cur: psycopg.Cursor[Any]) -> None:
    """Update churn and HOT eligibility — the mechanism behind heap fill.

    A HOT (heap-only tuple) update rewrites the row WITHOUT touching any index.
    Postgres takes that path only when **both** conditions hold: no indexed
    column changes, AND the new tuple fits on the original page. So
    ``n_tup_hot_upd = 0`` against a large ``n_tup_upd`` establishes that HOT
    never applied — it does NOT by itself say which of the two conditions
    failed. Pair it with the schema: if an indexed column is written
    unconditionally by the writer, the first condition is provably violated on
    every update and the counter is explained; otherwise page pressure is the
    live hypothesis and this counter cannot separate them.

    ⚠ ``n_tup_newpage_upd`` counts updates whose new tuple landed on a
    DIFFERENT page, which is not the same as extending the relation — the
    destination can be an existing page with reusable space. Do not read it as
    a growth figure.

    Reported rate-free: ``pg_stat_database.stats_reset`` is NULL on this
    cluster, so the epoch these totals accumulate from is unknown. They are
    compared against the live row count, never against elapsed time.
    """
    print("\n═══ 3. CHURN + HOT ELIGIBILITY (rate-free; stats epoch is unknown) ═══")
    print(f"{'store':38s} {'n_tup_upd':>12s} {'hot_upd':>10s} {'newpage':>10s} {'ins':>10s} {'dead':>8s}")
    for t in ALL_STORES:
        row = _q(
            cur,
            """SELECT coalesce(sum(s.n_tup_upd), 0), coalesce(sum(s.n_tup_hot_upd), 0),
                      coalesce(sum(s.n_tup_newpage_upd), 0), coalesce(sum(s.n_tup_ins), 0),
                      coalesce(sum(s.n_dead_tup), 0), coalesce(sum(s.autovacuum_count), 0)
               FROM pg_stat_user_tables s
               WHERE s.relid = %s::regclass
                  OR s.relid IN (SELECT inhrelid FROM pg_inherits WHERE inhparent = %s::regclass)""",
            (t, t),
        )[0]
        upd, hot, newpage, ins, dead, vac = (int(x) for x in row)
        hot_pct = f"{100 * hot / upd:.0f}%" if upd else "n/a"
        print(f"{t:38s} {upd:12,d} {hot:10,d} {newpage:10,d} {ins:10,d} {dead:8,d}   HOT={hot_pct} autovac={vac}")

    print("\n  -- ingest recency per leaf (is the free space being re-used, or idle?) --")
    for p in PARTITIONED:
        for name, n, lo, hi, days, last7 in _q(
            cur,
            f"""SELECT c.relname, count(*), min(t.ingested_at)::date, max(t.ingested_at)::date,
                       count(DISTINCT t.ingested_at::date),
                       count(*) FILTER (WHERE t.ingested_at > now() - interval '7 days')
                FROM {p} t JOIN pg_class c ON c.oid = t.tableoid
                GROUP BY 1 HAVING count(*) > 100000 ORDER BY 1 DESC""",
        ):
            print(f"    {name:52s} {n:10,d}  {lo} → {hi}  days={days:<3d} last7d={last7:,}")


# ──────────────────────── 4. redundancy census ───────────────────────────


def _redundancy(cur: psycopg.Cursor[Any]) -> None:
    """Per-column bytes and cardinality — the redundant-representation axis.

    #3116 clause 2 asks whether "repeated accession/identity strings can be
    compacted without expensive joins or a large new abstraction". Answering
    that needs both halves per column: how many bytes it costs, and how many
    distinct values those bytes encode. A column with one distinct value over
    millions of rows is storing a constant; a column whose distinct count
    equals the row count is genuinely per-row and cannot be interned.
    """
    print("\n═══ 4. REDUNDANCY CENSUS (bytes vs distinct values, per column) ═══")
    for t in ALL_STORES:
        cols = _cols(cur, t)
        sel = ", ".join(f'sum(pg_column_size("{c}"))' for c in cols)
        dis = ", ".join(f'count(DISTINCT "{c}")' for c in cols)
        t0 = time.time()
        row = _q(cur, f"SELECT count(*), {sel}, {dis} FROM {t}")[0]
        n = row[0]
        sizes = row[1 : 1 + len(cols)]
        dists = row[1 + len(cols) :]
        print(f"\n  ── {t}   rows={n:,}   ({time.time() - t0:.0f}s)")
        print(f"     {'column':26s} {'MiB':>9s} {'B/row':>7s} {'distinct':>13s}  note")
        total = 0
        for c, s, d in sorted(zip(cols, sizes, dists, strict=True), key=lambda x: -(x[1] or 0)):
            s = int(s or 0)
            total += s
            if d == 0:
                note = "ALL NULL (costs only a null-bitmap bit)"
            elif d == 1:
                note = "CONSTANT — one distinct value"
            elif n and d == n:
                note = "per-row — cannot be interned"
            elif n and d < n / 100:
                note = f"internable — {n / d:,.0f} rows per value"
            else:
                note = ""
            print(f"     {c:26s} {_mib(s):>9s} {s / n if n else 0:7.1f} {d:13,d}  {note}")
        print(f"     {'── column payload':26s} {_mib(total):>9s} {total / n if n else 0:7.1f}")


# ───────────────── 5. no-op upsert rate (full population) ─────────────────


def _noop_rate(cur: psycopg.Cursor[Any]) -> None:
    """How much of the institutions update volume changes nothing?

    ``ownership_observations_sync.sync_institutions`` (:383) re-reads every
    ``institutional_holdings`` row inside the 8-quarter 13F retention cap and
    calls ``record_institution_observation`` on each. That writer is
    ``ON CONFLICT … DO UPDATE`` with no ``IS DISTINCT FROM`` guard
    (``app/services/ownership_observations.py:506``), so a re-run rewrites every
    matched row whether or not any value moved.

    Measured on the FULL population, not a sample: join each stored observation
    back to the source row it is mirrored from, and compare **every column the
    DO UPDATE sets**, one counter per column so a mismatch can be attributed
    rather than guessed at.

    Two of the DO UPDATE columns are excluded from the no-op test and named
    here so their absence is not silent: ``ingested_at`` is
    ``clock_timestamp()`` and ``ingest_run_id`` is a fresh UUID per run, so
    both differ by construction on every single candidate row. That is exactly
    why a whole-row ``IS DISTINCT FROM`` cannot be used as a guard.

    The comparison reproduces the writer's own normalisation rather than
    comparing raw columns: ``filed_at`` falls back to midnight UTC on
    ``period_of_report`` when the source is NULL, blank ``voting_authority``
    becomes NULL, and ``source_field`` / ``source_url`` / ``period_start`` are
    compared against the literal ``None`` the sync passes.

    ⚠ There is deliberately NO "do the two writers disagree?" check here. An
    earlier draft counted source groups spanning several INFOTABLE rows and
    read 0 as evidence the per-row and SUM models coincide. That check is
    **structurally incapable of returning anything else**:
    ``uq_holdings_accession_instrument_putcall`` (sql/090:89) already enforces
    uniqueness on precisely that grouping key, so the count is 0 against any
    corpus. The real answer is a source rule, not a query —
    ``.claude/skills/data-engineer/SKILL.md`` ("Multi-row source positions: SUM
    at every locus") requires every writer to sum components, and the
    per-filing path does so in ``normalise_13f_holdings`` BEFORE it writes
    ``institutional_holdings`` (``app/services/institutional_holdings.py:1665``).

    ⚠ Coverage is reported as anti-joins in both directions. The matched set is
    an intersection, and a no-op *rate* over an intersection says nothing about
    rows that fall outside it.
    """
    print("\n═══ 5. NO-OP UPSERT RATE — institutions observations vs their source ═══")
    from app.services.institutional_holdings import thirteen_f_retention_cutoff

    cutoff = thirteen_f_retention_cutoff()
    print(f"  13F retention cutoff in force: {cutoff}")
    t0 = time.time()
    row = _q(
        cur,
        """
        WITH src AS (
          SELECT ih.instrument_id, f.cik AS filer_cik, ih.period_of_report AS period_end,
                 ih.accession_number AS doc_id,
                 CASE WHEN ih.is_put_call IN ('PUT','CALL') THEN ih.is_put_call ELSE 'EQUITY' END
                     AS exposure_kind,
                 ih.shares, ih.market_value_usd, ih.accession_number,
                 f.name AS filer_name, f.filer_type,
                 COALESCE(ih.filed_at, (ih.period_of_report::timestamp AT TIME ZONE 'UTC'))
                     AS filed_at,
                 nullif(ih.voting_authority, '') AS voting_authority
          FROM institutional_holdings ih
          JOIN institutional_filers f ON f.filer_id = ih.filer_id
          WHERE ih.period_of_report >= %(cutoff)s
            AND f.cik IS NOT NULL AND btrim(f.cik) <> ''
        )
        SELECT count(*),
          count(*) FILTER (WHERE o.filer_name       IS DISTINCT FROM s.filer_name),
          count(*) FILTER (WHERE o.filer_type       IS DISTINCT FROM s.filer_type),
          count(*) FILTER (WHERE o.source_accession IS DISTINCT FROM s.accession_number),
          count(*) FILTER (WHERE o.source_field     IS NOT NULL),
          count(*) FILTER (WHERE o.source_url       IS NOT NULL),
          count(*) FILTER (WHERE o.filed_at         IS DISTINCT FROM s.filed_at),
          count(*) FILTER (WHERE o.period_start     IS NOT NULL),
          count(*) FILTER (WHERE o.shares           IS DISTINCT FROM s.shares),
          count(*) FILTER (WHERE o.market_value_usd IS DISTINCT FROM s.market_value_usd),
          count(*) FILTER (WHERE o.voting_authority IS DISTINCT FROM s.voting_authority),
          count(*) FILTER (WHERE o.filer_name IS NOT DISTINCT FROM s.filer_name
                             AND o.filer_type IS NOT DISTINCT FROM s.filer_type
                             AND o.source_accession IS NOT DISTINCT FROM s.accession_number
                             AND o.source_field IS NULL AND o.source_url IS NULL
                             AND o.filed_at IS NOT DISTINCT FROM s.filed_at
                             AND o.period_start IS NULL
                             AND o.shares IS NOT DISTINCT FROM s.shares
                             AND o.market_value_usd IS NOT DISTINCT FROM s.market_value_usd
                             AND o.voting_authority IS NOT DISTINCT FROM s.voting_authority)
        FROM src s
        JOIN ownership_institutions_observations o
          ON o.instrument_id = s.instrument_id AND o.filer_cik = s.filer_cik
         AND o.ownership_nature = 'economic' AND o.period_end = s.period_end
         AND o.source_document_id = s.doc_id AND o.exposure_kind = s.exposure_kind
        """,
        {"cutoff": cutoff},
    )[0]
    labels = [
        "filer_name",
        "filer_type",
        "source_accession",
        "source_field",
        "source_url",
        "filed_at",
        "period_start",
        "shares",
        "market_value_usd",
        "voting_authority",
    ]
    matched = int(row[0])
    noop = int(row[-1])
    print(f"  matched source↔observation rows : {matched:,}   ({time.time() - t0:.0f}s)")
    if matched == 0:
        _fail(
            "the source↔observation join matched ZERO rows — the no-op rate below is "
            "vacuous, not a clean result. A retention-cutoff change or a renamed column "
            "produces exactly this."
        )
        return
    print("  per-column mismatches (attribution, not a guess):")
    for label, v in zip(labels, (int(x) for x in row[1:-1]), strict=True):
        v_int = int(v)
        print(f"    {label:20s} {v_int:>12,d}  {100 * v_int / matched:7.3f}%")
    print(f"  COMPLETE NO-OPS (every compared DO UPDATE column identical): {noop:,} = {100 * noop / matched:.3f}%")
    print(f"  rows a re-run would genuinely change                       : {matched - noop:,}")

    src_only, obs_only = _q(
        cur,
        """
        WITH src AS (
          SELECT ih.instrument_id, f.cik AS filer_cik, ih.period_of_report AS period_end,
                 ih.accession_number AS doc_id,
                 CASE WHEN ih.is_put_call IN ('PUT','CALL') THEN ih.is_put_call ELSE 'EQUITY' END
                     AS exposure_kind
          FROM institutional_holdings ih
          JOIN institutional_filers f ON f.filer_id = ih.filer_id
          WHERE ih.period_of_report >= %(cutoff)s
            AND f.cik IS NOT NULL AND btrim(f.cik) <> ''
        )
        SELECT
          (SELECT count(*) FROM src s WHERE NOT EXISTS (
              SELECT 1 FROM ownership_institutions_observations o
              WHERE o.instrument_id = s.instrument_id AND o.filer_cik = s.filer_cik
                AND o.ownership_nature = 'economic' AND o.period_end = s.period_end
                AND o.source_document_id = s.doc_id AND o.exposure_kind = s.exposure_kind)),
          (SELECT count(*) FROM ownership_institutions_observations o
           WHERE o.period_end >= %(cutoff)s AND NOT EXISTS (
              SELECT 1 FROM src s
              WHERE o.instrument_id = s.instrument_id AND o.filer_cik = s.filer_cik
                AND o.period_end = s.period_end AND o.source_document_id = s.doc_id
                AND o.exposure_kind = s.exposure_kind))
        """,
        {"cutoff": cutoff},
    )[0]
    print(f"  coverage — source rows with no observation : {int(src_only):,}")
    print(f"  coverage — in-window observations with no source row : {int(obs_only):,}")

    # Which writer last touched a row is visible without a history table:
    # sync_institutions passes source_url=None while the bulk drain supplies an
    # SEC URL, so the NULL-URL set identifies the rows the sync has mirrored.
    url_null, obs_total, ih_rows = _q(
        cur,
        """SELECT (SELECT count(*) FROM ownership_institutions_observations
                   WHERE source_url IS NULL),
                  (SELECT count(*) FROM ownership_institutions_observations),
                  (SELECT count(*) FROM institutional_holdings)""",
    )[0]
    print(
        f"  source_url IS NULL on {int(url_null):,} of {int(obs_total):,} observations "
        f"({100 * url_null / obs_total:.1f}%); institutional_holdings holds {int(ih_rows):,} rows"
    )
    # ⚠ Equal COUNTS do not prove equal SETS. Test the set relation directly:
    # how many NULL-source_url observations have no matching institutional_holdings
    # row at all? If that is 0 AND the counts match, the sets coincide.
    unmatched_null_url = _q(
        cur,
        """SELECT count(*)
           FROM ownership_institutions_observations o
           WHERE o.source_url IS NULL
             AND NOT EXISTS (
                 SELECT 1
                 FROM institutional_holdings ih
                 JOIN institutional_filers f ON f.filer_id = ih.filer_id
                 WHERE ih.instrument_id = o.instrument_id
                   AND f.cik = o.filer_cik
                   AND ih.period_of_report = o.period_end
                   AND ih.accession_number = o.source_document_id
                   AND CASE WHEN ih.is_put_call IN ('PUT','CALL') THEN ih.is_put_call
                            ELSE 'EQUITY' END = o.exposure_kind)""",
    )[0][0]
    print(f"    NULL-source_url observations with no institutional_holdings row: {int(unmatched_null_url):,}")
    if int(url_null) == int(ih_rows) and int(unmatched_null_url) == 0:
        print(
            "    ⚠ counts are equal AND the anti-join is empty, so the NULL-source_url set IS "
            "the set sync_institutions mirrors — that writer passes source_url=None. Whether "
            "the bulk drain had populated any of them FIRST is still NOT established here; "
            "this shows which rows the sync owns, not what they held before."
        )
    else:
        print(
            "    (counts and/or membership differ — the NULL-source_url set is NOT simply the "
            "sync-mirrored set; do not attribute it to one writer)"
        )


# ───────────────────────── 6. consumers / invariants ──────────────────────


def _consumers(cur: psycopg.Cursor[Any]) -> None:
    """Foreign keys and the invariants the proposal's levers rest on."""
    print("\n═══ 6. CONSUMERS + INVARIANTS ═══")
    fks = _q(
        cur,
        """SELECT conrelid::regclass::text, confrelid::regclass::text, conname
           FROM pg_constraint
           WHERE contype = 'f' AND confrelid::regclass::text = ANY(%s)""",
        (ALL_STORES,),
    )
    print(f"  foreign keys referencing any store: {len(fks)}")
    for child, parent, name in fks:
        print(f"    {child} → {parent} ({name})")
        _fail(f"a foreign key now references {parent}; the retire/compact analysis assumed none")

    dup, total = _q(
        cur,
        """SELECT count(*) FILTER (WHERE source_document_id IS DISTINCT FROM source_accession),
                  count(*) FROM ownership_institutions_observations""",
    )[0]
    print(f"  institutions: source_document_id <> source_accession on {dup:,} of {total:,} rows")
    if dup:
        _fail(
            f"{dup:,} institutions rows now carry a source_document_id that differs from "
            "source_accession — the duplicate-column lever in the proposal no longer applies"
        )

    tomb, total2 = _q(
        cur,
        """SELECT count(*) FILTER (WHERE known_to IS NOT NULL), count(*)
           FROM ownership_institutions_observations""",
    )[0]
    print(f"  institutions: known_to IS NOT NULL (soft-deleted) on {tomb:,} of {total2:,} rows")
    if tomb:
        _fail(
            f"{tomb:,} institutions rows are now soft-deleted — heap free space is partly a "
            "retention question and section 3's upsert-churn explanation is incomplete"
        )

    fdup, ftotal = _q(
        cur,
        """SELECT count(*) FILTER (WHERE source_document_id IS DISTINCT FROM source_accession),
                  count(*) FROM ownership_funds_observations""",
    )[0]
    print(
        f"  funds: source_document_id <> source_accession on {fdup:,} of {ftotal:,} rows "
        "(expected NON-zero — funds store accession:holding_id there)"
    )
    # ⚠ An earlier draft only warned when the difference vanished ENTIRELY, so a
    # partial collapse — the realistic failure — passed silently. Gate on the
    # rate instead. 0.90 is a by-construction floor, not a published one: today's
    # value is >0.999, so anything below 0.90 is a structural change in the
    # writer rather than drift at the margin.
    if ftotal:
        rate = fdup / ftotal
        print(f"  funds: per-row document ids are {100 * rate:.3f}% of rows")
        if rate < 0.90:
            _fail(
                f"funds source_document_id differs from source_accession on only "
                f"{100 * rate:.1f}% of rows (was >99.9%); the institutions-vs-funds "
                "separation this proposal rests on has changed and both levers need re-deriving"
            )
    # ⚠ This measures INEQUALITY with source_accession, which is not the same as
    # per-row UNIQUENESS, and a 0.90 floor still lets a partial collapse between
    # 90% and 99.98% pass. It is a tripwire for a writer change, not a proof of
    # the funds identity model.


# ─────────────── 7. claims the proposal makes about live state ────────────


def _live_state(cur: psycopg.Cursor[Any]) -> None:
    """Measure the things the proposal asserts about the CURRENT system.

    Each of these was an ad-hoc query in the session that wrote the proposal.
    An ad-hoc query is not evidence a later reader can re-run, and a proposal
    that cites a number its own instrument cannot produce is asking to be
    believed rather than checked.
    """
    print("\n═══ 7. LIVE-STATE CLAIMS (each one the proposal depends on) ═══")

    drifted, total = _q(
        cur,
        """WITH obs_max AS (
               SELECT instrument_id, MAX(ingested_at) AS m
               FROM ownership_institutions_observations GROUP BY 1)
           SELECT count(*) FILTER (
                      WHERE s.last_drained_observations_max_ingested_at
                            IS DISTINCT FROM o.m),
                  count(*)
           FROM ownership_refresh_state s
           LEFT JOIN obs_max o USING (instrument_id)
           WHERE s.category = 'institutions'""",
    )[0]
    print(f"  repair-sweep drift right now (institutions): {int(drifted):,} of {int(total):,}")
    print(
        "    ⚠ this is a point-in-time reading between sweeps, NOT a claim that drift "
        "never fires. The proposal must not read 0 as 'the watermark never moves'."
    )

    for p_name in PARTITIONED:
        lo, hi, n = _q(
            cur,
            f"SELECT min(ingested_at)::date, max(ingested_at)::date, count(*) FROM {p_name}",
        )[0]
        recent = _q(
            cur,
            f"SELECT count(*) FROM {p_name} WHERE ingested_at > now() - interval '30 days'",
        )[0][0]
        print(f"  {p_name}: {n:,} rows, ingested {lo} → {hi}, last 30d = {int(recent):,}")
        if recent == 0:
            print(
                "    ⚠ zero rows ingested in 30 days. That is consistent with a quiesced "
                "store AND with a stalled ingest; this query cannot tell them apart, and "
                "it cannot see rows written then deleted."
            )

    avail = _q(
        cur,
        "SELECT count(*) FROM pg_available_extensions WHERE name = 'pgstattuple'",
    )[0][0]
    inst = _q(cur, "SELECT count(*) FROM pg_extension WHERE extname = 'pgstattuple'")[0][0]
    print(f"  pgstattuple: available={int(avail)} installed={int(inst)}")


# ──────────────────────────── rebuild probe ──────────────────────────────

_REBUILD_INDEXES = {
    "ownership_institutions_observations_2025q4": [
        (
            "rb_pk",
            "CREATE UNIQUE INDEX {n} ON {t} (instrument_id, filer_cik, ownership_nature, "
            "period_end, source_document_id, exposure_kind)",
        ),
        ("rb_instr_ingested", "CREATE INDEX {n} ON {t} (instrument_id, ingested_at DESC)"),
        ("rb_filer_period", "CREATE INDEX {n} ON {t} (filer_cik, period_end DESC)"),
        ("rb_instr_period", "CREATE INDEX {n} ON {t} (instrument_id, period_end DESC)"),
    ],
    "institutional_holdings": [
        (
            "rb_uq",
            "CREATE UNIQUE INDEX {n} ON {t} (accession_number, instrument_id, COALESCE(is_put_call, 'EQUITY'))",
        ),
        ("rb_pkey", "CREATE UNIQUE INDEX {n} ON {t} (holding_id)"),
        ("rb_filer_period", "CREATE INDEX {n} ON {t} (filer_id, period_of_report DESC)"),
        ("rb_instr_period", "CREATE INDEX {n} ON {t} (instrument_id, period_of_report DESC)"),
    ],
    "ownership_funds_observations_2025q4": [
        (
            "rb_pk",
            "CREATE UNIQUE INDEX {n} ON {t} (instrument_id, fund_series_id, period_end, source_document_id)",
        ),
        ("rb_instr_ingested", "CREATE INDEX {n} ON {t} (instrument_id, ingested_at DESC)"),
        ("rb_series_period", "CREATE INDEX {n} ON {t} (fund_series_id, period_end DESC)"),
        ("rb_filer_period", "CREATE INDEX {n} ON {t} (fund_filer_cik, period_end DESC)"),
        ("rb_instr_period", "CREATE INDEX {n} ON {t} (instrument_id, period_end DESC)"),
    ],
    "ownership_institutions_current": [
        (
            "rb_pkey",
            "CREATE UNIQUE INDEX {n} ON {t} (instrument_id, filer_cik, ownership_nature, exposure_kind)",
        ),
        ("rb_filer", "CREATE INDEX {n} ON {t} (filer_cik)"),
        ("rb_filer_period", "CREATE INDEX {n} ON {t} (filer_cik, period_end DESC)"),
    ],
}


def _rebuild_probe() -> int:
    """Reconstruct relations into session-local TEMP tables to get their MINIMUM.

    This measures bloat directly instead of estimating it. ``pgstattuple`` is
    available on this cluster but NOT installed, and installing it is a schema
    change that #3116 clause 6 forbids during the spike — a reconstruction is
    also the better instrument, since it reports the size a rewrite would
    actually produce rather than an estimate of the size it would save.

    TEMP objects only: nothing in a permanent schema is created, altered or
    dropped, and everything disappears when the session ends.

    ⚠ The row count is asserted against the source, not printed from a
    ``count(*)`` with no FROM clause — that mistake returns 1 for any table and
    reads exactly like a real answer.
    """
    assert_dev_environment()
    print("═══ REBUILD PROBE — TEMP reconstruction (writable session, TEMP objects only) ═══")
    with psycopg.connect(settings.database_url, autocommit=True) as conn, conn.cursor() as cur:
        for src, indexes in _REBUILD_INDEXES.items():
            heap0, idx0, want = _q(
                cur,
                """SELECT pg_relation_size(c.oid), pg_indexes_size(c.oid), c.reltuples::bigint
                   FROM pg_class c WHERE c.oid = %s::regclass""",
                (src,),
            )[0]
            src_rows = _q(cur, f"SELECT count(*) FROM {src}")[0][0]
            t0 = time.time()
            cur.execute(_sql(f"CREATE TEMP TABLE rb AS SELECT * FROM {src}"))
            got = _q(cur, "SELECT count(*) FROM rb")[0][0]
            if got != src_rows:
                _fail(f"{src}: reconstruction holds {got:,} rows, source has {src_rows:,}")
            heap1 = _q(cur, "SELECT pg_relation_size('rb')")[0][0]
            print(f"\n  {src}  (reltuples estimate {want:,}, actual {src_rows:,})")
            print(
                f"    heap      live={_mib(heap0):>9s}  minimum={_mib(heap1):>9s} MiB  "
                f"({100 * heap1 / heap0:.1f}% of live)   [{time.time() - t0:.0f}s]"
            )
            total = 0
            for name, ddl in indexes:
                cur.execute(_sql(ddl.format(n=name, t="rb")))
                sz = _q(cur, "SELECT pg_relation_size(%s)", (name,))[0][0]
                total += sz
                print(f"    idx {name:20s} minimum={_mib(sz):>9s} MiB")
            print(
                f"    idx TOTAL live={_mib(idx0):>9s}  minimum={_mib(total):>9s} MiB  "
                f"({100 * total / idx0:.1f}% of live)"
            )
            print(
                f"    → reclaimable by rewrite: heap {_mib(heap0 - heap1)} MiB "
                f"+ index {_mib(idx0 - total)} MiB = {_mib((heap0 - heap1) + (idx0 - total))} MiB"
            )

            # Project a measured LEAF onto its parent, in the script rather than
            # in prose, so the projection is reproducible and cannot go stale.
            # Projection is by BYTES PER ROW, never by the leaf's fill ratio:
            # leaf fill ranges 41.4%-96.3% across this parent, so a ratio
            # extrapolation would inherit whichever leaf happened to be picked.
            # Bytes-per-row is still an assumption — it presumes the sampled
            # leaf's row widths and key cardinality are typical — and the
            # printed line says so.
            parent = _q(
                cur,
                """SELECT par.relname FROM pg_class par
                   JOIN pg_inherits i ON i.inhparent = par.oid
                   WHERE i.inhrelid = %s::regclass""",
                (src,),
            )
            if parent and src_rows:
                pname = parent[0][0]
                p_rows, p_heap, p_idx = _q(
                    cur,
                    f"""SELECT (SELECT count(*) FROM {pname}),
                               sum(pg_relation_size(c.oid)), sum(pg_indexes_size(c.oid))
                        FROM pg_class c
                        WHERE c.oid IN (SELECT inhrelid FROM pg_inherits
                                        WHERE inhparent = %s::regclass)""",
                    (pname,),
                )[0]
                proj_heap = heap1 / src_rows * p_rows
                proj_idx = total / src_rows * p_rows
                print(
                    f"    PROJECTED to {pname} ({int(p_rows):,} rows, from this leaf's "
                    f"{heap1 / src_rows:.1f} heap + {total / src_rows:.1f} index B/row):"
                )
                print(
                    f"      minimum {_mib(proj_heap + proj_idx)} MiB vs live "
                    f"{_mib(float(p_heap) + float(p_idx))} MiB → reclaimable "
                    f"{_mib(float(p_heap) + float(p_idx) - proj_heap - proj_idx)} MiB"
                    "   [PROJECTION, one leaf, not a measurement of the parent]"
                )
            cur.execute("DROP TABLE rb")

        # research_price_daily is probed on a SAMPLE, not in full: a complete
        # reconstruction is a ~7 GiB TEMP heap plus a 2.3 GiB index build
        # against the cluster the dev stack is live on. ``series_id % 10``
        # keeps WHOLE series together, so the bytes-per-row it measures is not
        # distorted by splitting a series across the sample boundary. The
        # figure below is therefore a projection and is labelled as one.
        print("\n  -- research_price_daily (SAMPLED probe, projected to full table) --")
        r_heap0, r_idx0 = _q(
            cur,
            "SELECT pg_relation_size(c.oid), pg_indexes_size(c.oid) FROM pg_class c "
            "WHERE c.oid = 'research_price_daily'::regclass",
        )[0]
        r_total = _q(cur, "SELECT count(*) FROM research_price_daily")[0][0]
        t0 = time.time()
        cur.execute(_sql("CREATE TEMP TABLE rpd AS SELECT * FROM research_price_daily WHERE series_id % 10 = 0"))
        r_n = _q(cur, "SELECT count(*) FROM rpd")[0][0]
        r_heap = _q(cur, "SELECT pg_relation_size('rpd')")[0][0]
        cur.execute(_sql("CREATE UNIQUE INDEX rpd_pk ON rpd (series_id, bar_date)"))
        r_idx = _q(cur, "SELECT pg_relation_size('rpd_pk')")[0][0]
        if not r_n:
            _fail("research_price_daily sample is empty — the projection below is meaningless")
        else:
            print(f"    sample rows={r_n:,} of {r_total:,} ({100 * r_n / r_total:.1f}%)   [{time.time() - t0:.0f}s]")
            print(
                f"    heap {r_heap / r_n:.1f} B/row → projected minimum {_mib(r_heap / r_n * r_total)} "
                f"MiB vs live {_mib(r_heap0)} MiB ({100 * (r_heap / r_n * r_total) / r_heap0:.1f}%)"
            )
            print(
                f"    pkey {r_idx / r_n:.1f} B/row → projected minimum {_mib(r_idx / r_n * r_total)} "
                f"MiB vs live {_mib(r_idx0)} MiB ({100 * (r_idx / r_n * r_total) / r_idx0:.1f}%)"
            )
        cur.execute("DROP TABLE rpd")

        # Interning variant, measured on the same rows rather than estimated.
        print("\n  -- interned-representation variant (same rows, narrowed identity) --")
        cur.execute(
            _sql(
                """CREATE TEMP TABLE rbc AS
                   SELECT instrument_id,
                          dense_rank() OVER (ORDER BY filer_cik)::int            AS cik_id,
                          dense_rank() OVER (ORDER BY source_document_id)::bigint AS accession_id,
                          dense_rank() OVER (ORDER BY exposure_kind)::smallint    AS exposure_id,
                          dense_rank() OVER (ORDER BY ingest_run_id)::int         AS run_id,
                          dense_rank() OVER (ORDER BY voting_authority)::smallint AS voting_id,
                          filed_at, period_end, known_from, ingested_at, shares, market_value_usd
                   FROM ownership_institutions_observations_2025q4"""
            )
        )
        heap2 = _q(cur, "SELECT pg_relation_size('rbc')")[0][0]
        cur.execute(
            _sql("CREATE UNIQUE INDEX rbc_pk ON rbc (instrument_id, cik_id, period_end, accession_id, exposure_id)")
        )
        cur.execute(_sql("CREATE INDEX rbc_ii ON rbc (instrument_id, ingested_at DESC)"))
        cur.execute(_sql("CREATE INDEX rbc_fp ON rbc (cik_id, period_end DESC)"))
        cur.execute(_sql("CREATE INDEX rbc_ip ON rbc (instrument_id, period_end DESC)"))
        idx2 = _q(
            cur,
            "SELECT sum(pg_relation_size(c.oid)) FROM pg_class c "
            "WHERE c.relname IN ('rbc_pk','rbc_ii','rbc_fp','rbc_ip')",
        )[0][0]
        print(
            f"    ownership_institutions_observations_2025q4 interned: "
            f"heap={_mib(heap2)} MiB  idx={_mib(idx2)} MiB  total={_mib(heap2 + idx2)} MiB"
        )
        cur.execute(
            _sql(
                "CREATE TEMP TABLE ihc AS SELECT "
                "dense_rank() OVER (ORDER BY accession_number)::bigint AS acc_id, "
                "instrument_id, COALESCE(is_put_call, 'EQUITY') AS pc FROM institutional_holdings"
            )
        )
        cur.execute(_sql("CREATE UNIQUE INDEX ihc_uq ON ihc (acc_id, instrument_id, pc)"))
        sz = _q(cur, "SELECT pg_relation_size('ihc_uq')")[0][0]
        print(f"    institutional_holdings unique index with interned accession: {_mib(sz)} MiB")
    return 1 if FAILURES else 0


# ─────────────────────────────── driver ──────────────────────────────────


def _census() -> int:
    assert_dev_environment()
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with conn.cursor() as cur:
            start_xact = _q(cur, "SELECT xact_start FROM pg_stat_activity WHERE pid = pg_backend_pid()")[0][0]
            # xact_start alone does NOT prove REPEATABLE READ — a READ COMMITTED
            # transaction passes the same equality check at the end. Assert the
            # isolation level explicitly so the snapshot claim rests on the
            # setting and not on a proxy for it.
            iso = _q(cur, "SELECT current_setting('transaction_isolation')")[0][0]
            if iso != "repeatable read":
                _fail(f"transaction_isolation is {iso!r}, not 'repeatable read' — sections do not share a snapshot")
            ver, reset = _q(
                cur,
                "SELECT current_setting('server_version'), "
                "(SELECT stats_reset FROM pg_stat_database WHERE datname = current_database())",
            )[0]
            print(f"PostgreSQL {ver}   pg_stat_database.stats_reset = {reset}")
            if reset is None:
                WARNINGS.append(
                    "stats_reset is NULL — the epoch pg_stat_* counters accumulate from is "
                    "unknown, so section 2 scan counts and section 3 churn totals are "
                    "lower bounds with no rate attached."
                )
            installed = [r[0] for r in _q(cur, "SELECT extname FROM pg_extension")]
            if "pgstattuple" not in installed:
                WARNINGS.append(
                    "pgstattuple is NOT installed (it IS available on this cluster), so "
                    "reclaimable-vs-OS-returned space is not decomposed from the catalog "
                    "alone; --rebuild-probe measures the minimum directly instead."
                )

            _footprint(cur)
            _indexes(cur)
            _churn(cur)
            _redundancy(cur)
            _noop_rate(cur)
            _consumers(cur)
            _live_state(cur)

            end_xact = _q(cur, "SELECT xact_start FROM pg_stat_activity WHERE pid = pg_backend_pid()")[0][0]
            if end_xact != start_xact:
                _fail(
                    f"snapshot broke mid-run: transaction started {start_xact}, now {end_xact}. "
                    "Sections do not describe one database state."
                )
            else:
                print(f"\nSnapshot held: one transaction from {start_xact} to close.")

    if WARNINGS:
        print(f"\n⚠ {len(WARNINGS)} measurement(s) this run could NOT make:")
        for w in WARNINGS:
            print(f"  - {w}")
    if FAILURES:
        print(f"\n⚠ {len(FAILURES)} INVARIANT(S) FAILED — the proposal's levers are NOT safe as written:")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("\nAll invariants held.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    # Mutually exclusive: --census was previously parsed and never read, so
    # `--census --rebuild-probe` silently ran the probe and reported it as if
    # the caller had asked for the census. A mode flag that can be passed and
    # ignored is worse than no flag.
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--census", action="store_true", help="read-only audit (the default)")
    mode.add_argument(
        "--rebuild-probe",
        action="store_true",
        help="TEMP reconstruction to measure minimum heap + index size (writable session)",
    )
    args = ap.parse_args()
    if args.rebuild_probe:
        return _rebuild_probe()
    return _census()


if __name__ == "__main__":
    sys.exit(main())
