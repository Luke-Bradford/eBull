"""Read-only evidence behind #3232 — which Table I line becomes the CHART's point.

#3232: ``ownership_history._insiders_history`` ended its ``DISTINCT ON`` ``ORDER BY`` with
``source_document_id ASC``. When one filing reports several Table I lines for the same
``(period_end, ownership_nature)``, every prior key ties, so ascending document id decided
the point — and ascending picks the filing's **FIRST** line. Form 4 General Instruction
4(a)(i) wants the **LAST**: *"Report total beneficial ownership following the reported
transaction(s)"*.

⚠ The string-vs-numeric hazard (``{accn}:NDT:1000`` sorting before ``{accn}:NDT:999``) is
real but its live population is **zero** — ``--gain`` reports differing-width SKs within one
filing. Lead with first-vs-last, which is 100% of the movement; the ``::numeric`` cast is a
guard, not the defect.

#3146 removed exactly that rule from ``refresh_insiders_current`` and missed this reader, so
the projection and the chart could name different lines of the same filing. The fix shares
``_INSIDER_WINNER_ORDER_TAIL`` between them; this script is its evidence.

**Every figure is computed at run time** — nothing is hand-written into prose, so a
re-harvest cannot leave a number lying (prevention-log: "never hardcode a derived
statistic").

⚠ On the control arm. ``full-population-ab.md`` says never SIMULATE the control. There is no
stored control to read here — unlike ``ownership_insiders_current``, a history point is
computed per request and never persisted, so the production artefact IS the SQL. The control
below is therefore the literal pre-#3232 ``ORDER BY`` suffix, frozen as ``_OLD_TAIL``, run
against the same snapshot as the new one. The treatment arm imports the shared constant from
``app.services.ownership_observations`` rather than restating it, so this script cannot drift
from what ships.

``--ab``
    Full-population A/B of the chart's point set, old rule vs new, under BOTH partitionings
    the reader actually serves: unfiltered (``category=insiders``, all holders) and per-holder
    (the drill-through, where the caller pins ``holder_cik``). Reports rows moved, VALUE
    moved, distinct instruments, and direction.

``--gain``
    Gain-side inspection: the provenance of every moved pick (``:NDT:`` / ``:NDH:`` / XML,
    before and after). A line-ORDER fix must move picks WITHIN one source; a pick that
    changes provenance would mean the tail is doing something else.

Usage (read-only; one REPEATABLE READ snapshot per run):

    PYTHONPATH=. uv run python -m scripts.audit_3232_insider_history_line_order --ab
    PYTHONPATH=. uv run python -m scripts.audit_3232_insider_history_line_order --gain
"""

from __future__ import annotations

import argparse
from typing import Any, LiteralString, cast

import psycopg

from app.config import settings
from app.services.ownership_observations import _INSIDER_WINNER_ORDER_TAIL

# The pre-#3232 tie-break, frozen. This is the CONTROL and must never be re-pointed at the
# shared constant — the whole question is what the old rule did.
_OLD_TAIL: LiteralString = "source_document_id ASC"

# The reader's two call shapes. Unfiltered is `category=insiders`; per-holder is the
# drill-through, which pins `holder_identity_key` via the `holder_cik` parameter. The
# DISTINCT ON itself does not carry the holder, so both have to be measured.
_PARTITIONS: dict[str, LiteralString] = {
    "unfiltered": "instrument_id, period_end, ownership_nature",
    "per_holder": "instrument_id, holder_identity_key, period_end, ownership_nature",
}

_BASE: LiteralString = """
    SELECT instrument_id, holder_identity_key, period_end, ownership_nature,
           source_document_id, shares, filed_at,
           CASE source WHEN 'form4' THEN 1 WHEN 'form3' THEN 2 WHEN 'def14a' THEN 4 ELSE 10 END AS srank
      FROM ownership_insiders_observations
     WHERE known_to IS NULL AND shares IS NOT NULL
"""

_AB_SQL: LiteralString = """
WITH base AS ({base}),
old_pick AS (
  SELECT DISTINCT ON ({part}) {part}, shares AS shares_old, source_document_id AS doc_old
    FROM base ORDER BY {part}, srank ASC, filed_at DESC, {old_tail}
),
new_pick AS (
  SELECT DISTINCT ON ({part}) {part}, shares AS shares_new, source_document_id AS doc_new
    FROM base ORDER BY {part}, srank ASC, filed_at DESC, {new_tail}
)
SELECT count(*)                                                        AS buckets,
       count(*) FILTER (WHERE doc_old    IS DISTINCT FROM doc_new)     AS pick_moved,
       count(*) FILTER (WHERE shares_old IS DISTINCT FROM shares_new)  AS value_moved,
       count(DISTINCT instrument_id)
         FILTER (WHERE shares_old IS DISTINCT FROM shares_new)         AS instruments,
       count(*) FILTER (WHERE shares_new > shares_old)                 AS value_up,
       count(*) FILTER (WHERE shares_new < shares_old)                 AS value_down
  FROM old_pick JOIN new_pick USING ({part})
"""

_GAIN_SQL: LiteralString = """
WITH base AS ({base}),
old_pick AS (
  SELECT DISTINCT ON ({part}) {part}, source_document_id AS doc_old
    FROM base ORDER BY {part}, srank ASC, filed_at DESC, {old_tail}
),
new_pick AS (
  SELECT DISTINCT ON ({part}) {part}, source_document_id AS doc_new
    FROM base ORDER BY {part}, srank ASC, filed_at DESC, {new_tail}
)
SELECT CASE WHEN doc_old ~ ':NDT:' THEN 'NDT' WHEN doc_old ~ ':NDH:' THEN 'NDH' ELSE 'xml' END AS was,
       CASE WHEN doc_new ~ ':NDT:' THEN 'NDT' WHEN doc_new ~ ':NDH:' THEN 'NDH' ELSE 'xml' END AS now,
       count(*) AS n,
       count(*) FILTER (WHERE split_part(doc_old, ':', 1) <> split_part(doc_new, ':', 1)) AS crossed_accession,
       count(*) FILTER (WHERE length(split_part(doc_old, ':NDT:', 2))
                           <> length(split_part(doc_new, ':NDT:', 2)))                    AS differing_sk_width,
       count(*) FILTER (WHERE doc_old ~ ':NDT:[0-9]+$' AND doc_new ~ ':NDT:[0-9]+$'
                          AND split_part(doc_new, ':NDT:', 2)::numeric
                            > split_part(doc_old, ':NDT:', 2)::numeric)                   AS moved_to_later_line
  FROM old_pick JOIN new_pick USING ({part})
 WHERE doc_old IS DISTINCT FROM doc_new
 GROUP BY 1, 2 ORDER BY n DESC
"""


def _fmt(sql: LiteralString, part: LiteralString) -> LiteralString:
    """Compose the arm. Every input is a module constant — no caller value reaches this.

    ``str.format`` widens ``LiteralString`` to ``str``, so the cast restores psycopg3's
    injection guard rather than waiving it (same shape as
    ``scripts/audit_3146_insider_line_order``).
    """
    return cast(
        LiteralString,
        sql.format(base=_BASE, part=part, old_tail=_OLD_TAIL, new_tail=_INSIDER_WINNER_ORDER_TAIL),
    )


def _connect() -> psycopg.Connection[Any]:
    conn = psycopg.connect(settings.database_url)
    with conn.cursor() as cur:
        cur.execute("SET default_transaction_isolation = 'repeatable read'")
        cur.execute("SET statement_timeout = '900s'")
    return conn


def run_ab(conn: psycopg.Connection[Any]) -> None:
    print(f"control tail : {_OLD_TAIL}")
    print(f"treatment    : {' '.join(_INSIDER_WINNER_ORDER_TAIL.split())}\n")
    with conn.cursor() as cur:
        for label, part in _PARTITIONS.items():
            cur.execute(_fmt(_AB_SQL, part))
            cols = [d.name for d in cur.description or []]
            row = cur.fetchone() or ()
            print(f"=== {label} — DISTINCT ON ({part}) ===")
            for name, value in zip(cols, row, strict=False):
                print(f"  {name:12s} {value:>12,}")
            print()


def run_gain(conn: psycopg.Connection[Any]) -> None:
    with conn.cursor() as cur:
        for label, part in _PARTITIONS.items():
            cur.execute(_fmt(_GAIN_SQL, part))
            print(f"=== {label}: provenance of every moved pick ===")
            rows = cur.fetchall()
            if not rows:
                print("  (no pick moves)")
            for was, now, n, crossed, width, later in rows:
                flag = "  ⚠ CROSSED ACCESSION" if crossed else ""
                print(
                    f"  {was:3s} -> {now:3s} {n:>12,}   crossed_accession={crossed:,}"
                    f"   differing_sk_width={width:,}   moved_to_later_line={later:,}{flag}"
                )
            print()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ab", action="store_true", help="full-population A/B of the chart's point set")
    parser.add_argument("--gain", action="store_true", help="provenance of every moved pick")
    args = parser.parse_args()
    if not (args.ab or args.gain):
        parser.error("pick a mode: --ab or --gain")
    with _connect() as conn:
        if args.ab:
            run_ab(conn)
        if args.gain:
            run_gain(conn)


if __name__ == "__main__":
    main()
