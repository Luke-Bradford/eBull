"""#3115 spike instrument — measure the ``filing_documents`` catalogue.

``filing_documents`` (sql/062) is the per-document directory listing parsed
from each SEC filing's ``/index.json``. #3117 ranks it priority 3 and asks each
child to end in keep / compact / retire / retain-until-resolved *with
evidence*, separating four cost sources that a single size figure conflates:
**unnecessary content**, **redundant representation**, **indexes**, and
**reclaimable free space**.

This script produces that separation. Every figure is computed at run time —
none is written into prose here or on the ticket without the query that
produced it (project rule: "never hardcode a derived statistic").

Read-only: the connection is opened ``read_only`` at REPEATABLE READ, so every
section describes ONE database snapshot. That is not decoration — the sections
are multi-minute full-table scans, and under READ COMMITTED the counts, the
widths, the fan-out and the exclusion union would each describe a different
state while reading as one coherent report (the #2414 two-snapshot lesson).

``--inventory`` prints a *dry-run* eligible-row count per class. It is a
proposal, not an authorised deletion scope (#3117: "no destructive sweep is
authorised by this planning ticket"). **Exit status is 1 when any invariant
fails** — the section-5 safety checks, and the single-snapshot claim above,
which is verified against ``pg_stat_activity.xact_start`` at the end rather than
merely asserted in this docstring. An inventory whose safety checks only *print*
their verdict is an inventory that gets read as clean.

Two measured facts shape how the output should be read, both re-checkable with
``--consumers``:

* **``document_type`` and ``description`` are unreachable by construction.**
  #723 established that SEC's ``index.json`` ``type`` field is a content-type
  icon name (``text.gif``), not the document-type label (``EX-99.1``), so
  :func:`app.services.filing_documents.parse_filing_index` writes ``None`` to
  both. ``idx_filing_documents_type`` is therefore a partial index no current
  writer can satisfy. (Not "can never be": nothing in the schema or in
  ``upsert_filing_documents`` forbids another writer populating it.)
* **Child-row existence is the completion signal.**
  :func:`app.services.filing_documents.ingest_filing_documents` selects
  candidates with ``LEFT JOIN filing_documents … WHERE fd.id IS NULL``, so a
  policy that empties a ``filing_event`` hands it back to the fetch queue.

⚠ What this instrument does NOT measure, stated so its silence is not read as
a negative result: it fetches no document bodies, so no claim here about two
files being byte-identical is supported; and it cannot see a reader that lives
outside the tree (a psql runbook, a BI tool, a colleague's saved query).

⚠⚠ "No consumer" is the wrong summary and section 7 is scoped to refute it.
There is no *application* reader — but there is a demonstrated *diagnostic*
one: ``docs/proposals/etl/2026-09-16-3109-per-cik-poll-eligibility.md`` used
catalogue membership to show 3,415 of 3,420 manifest-absent accessions are in
fact held, which voided that investigation's entire first-draft data-loss
narrative. The catalogue's realised value to date is as a **discovery-
completeness witness**, not as the type-scoped query surface sql/062 describes.

Run::

    PYTHONPATH=. uv run python scripts/audit_3115_document_catalogue.py
    PYTHONPATH=. uv run python scripts/audit_3115_document_catalogue.py --inventory
    PYTHONPATH=. uv run python scripts/audit_3115_document_catalogue.py --consumers

Full-table aggregates over ~11.7M rows take several minutes.
"""

from __future__ import annotations

import argparse
import io
import subprocess
import sys
from typing import Any, LiteralString, cast

import psycopg

from app.config import settings
from scripts._dev_guard import assert_dev_environment

TABLE = "filing_documents"

# Applied to EVERY exclusion class, never to one of them. A primary document is
# the filing's identity and is not eligible however its filename reads — the
# first run of this script found 3 filings whose primary document IS the
# complete-submission ``{accession}.txt``.
_ELIGIBILITY_GUARD = "NOT is_primary"

# Row classes proposed for exclusion, most-defensible first: (label, predicate,
# rationale). Predicates are DISJOINT — section 5 asserts that, because an
# overlapping class set silently double-counts the projected saving.
#
# ⚠ Each predicate is expressed against the row's OWN ``accession_number``
# rather than as a name-shape regex. A regex like ``'-index\.html?$'`` also
# matches a filer-supplied ``exhibit-index.html``, which would put real content
# in the eligible set; ``document_name = accession_number || '-index.html'``
# cannot. The equality is also the rationale in executable form: what makes
# these rows excludable is precisely that the name is a function of the
# accession, so the locator survives the row.
#
# ⚠ ``R\d+\.htm``, the image classes and the XBRL linkbases are deliberately
# ABSENT. They are larger, but their content value is not settled by this
# spike's evidence, and the operator's refinement is explicit that "scanned
# documents / meaningful charts, financial XML/schema relationships and cited
# exhibits are protected by purpose, not filename extension". ``--inventory``
# reports them separately as candidates.
EXCLUSION_CANDIDATES: list[tuple[str, LiteralString, str]] = [
    (
        "self-reference: {accession}-index.html",
        "document_name IN (accession_number || '-index.html', accession_number || '-index.htm')",
        "Address is a function of the accession, so dropping the ROW does not lose the locator. "
        "⚠ NOT information-equivalent to this table: filing_documents.py records that the rich SEC "
        "type labels (EX-99.1, GRAPHIC) live ONLY in this rendering — it is the source any #723 "
        "follow-up would parse to populate document_type. Excludable as a row, not as a capability.",
    ),
    (
        "self-reference: {accession}-index-headers.html",
        "document_name = accession_number || '-index-headers.html'",
        "The SGML submission header rendered as HTML; address derivable from the accession. "
        "⚠ Its CONTENT carries submission metadata this table never held, so this is a locator "
        "the ingester can rebuild, not a body we already store.",
    ),
    (
        "self-reference: complete submission {accession}.txt",
        "document_name = accession_number || '.txt'",
        "SEC documents this as 'the raw text version of the complete disseminated filing content'. "
        "Address derivable from the accession. ⚠ Its content is not a concatenation of this table's "
        "rows (which hold no bodies) — the claim here is about the locator only.",
    ),
    (
        "viewer asset: Show.js / report.css",
        "document_name IN ('Show.js', 'report.css')",
        "XBRL-viewer chrome. Fixed filenames, no primary, no disclosure text. ⚠ This instrument "
        "fetches no bodies, so 'byte-identical across filings' is NOT established here — the case "
        "rests on the filename being a constant of the viewer package, not on content hashing.",
    ),
]

# Measured and reported but NOT proposed — content value unsettled by this spike.
UNDECIDED_CLASSES: list[tuple[str, LiteralString]] = [
    ("R<n>.htm (FilingSummary report set)", r"document_name ~ '^R\d+\.htm$'"),
    ("MetaLinks.json / FilingSummary.xml", "document_name IN ('MetaLinks.json', 'FilingSummary.xml')"),
    ("Financial_Report.xlsx", "document_name = 'Financial_Report.xlsx'"),
    ("images (.jpg/.jpeg/.gif/.png)", r"document_name ~* '\.(jpg|jpeg|gif|png)$'"),
    ("XBRL linkbases (_cal/_def/_lab/_pre.xml, .xsd)", r"document_name ~* '(_(cal|def|lab|pre)\.xml|\.xsd)$'"),
]

# Invariant failures collected across sections; a non-empty list exits 1.
FAILURES: list[str] = []

# Measurements this run could NOT make. Surfaced in the summary rather than
# printed once mid-report, so a coverage hole is not mistaken for a null result.
WARNINGS: list[str] = []


def _fail(msg: str) -> None:
    FAILURES.append(msg)
    print(f"    ⚠ INVARIANT FAILED: {msg}")


def _warn(msg: str) -> None:
    WARNINGS.append(msg)
    print(f"  ⚠ NOT MEASURED: {msg}")


def _q(cur: psycopg.Cursor[Any], sql: LiteralString) -> list[tuple[Any, ...]]:
    cur.execute(sql)
    return cur.fetchall()


def _pct(part: float, whole: float) -> str:
    return f"{100.0 * part / whole:6.2f}%" if whole else "   n/a"


def _mib(raw_bytes: float) -> str:
    """Bytes as MiB, labelled MiB.

    #3117 quotes this relation in GiB and the local cache in decimal GB, and
    warns against mixing the two. ``pg_size_pretty`` prints binary units with
    decimal labels ("MB" for 2^20), so this formatter states the unit it means.
    """
    return f"{raw_bytes / 1024 / 1024:9,.0f} MiB"


# ---------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------


def physical(cur: psycopg.Cursor[Any]) -> dict[str, int]:
    """Heap / TOAST / per-index bytes, with each index's lifetime scan count.

    ``idx_scan`` is the cheapest consumer evidence available: a large index with
    a zero scan count is paid for and never asked a question. It is a lifetime
    counter since the last ``pg_stat_reset`` — so a zero is evidence of no use,
    not proof. Cross-check against the tree with ``--consumers`` before acting.
    """
    print("\n=== 1. PHYSICAL FOOTPRINT ===")
    # ⚠ idx_scan below is a lifetime counter. It only means "never used" if the
    # stats have not been reset since the index was built, so print the reset
    # point rather than leaving the reader to assume it. pg_stat_user_tables
    # carries no stats_reset column — it lives on pg_stat_database.
    reset_at = _q(cur, "SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()")[0][0]
    print(f"  database stats_reset: {reset_at}  (idx_scan counts accumulate from here)")
    (total, heap, idx, toast) = _q(
        cur,
        f"""
        SELECT pg_total_relation_size('{TABLE}'), pg_relation_size('{TABLE}'),
               pg_indexes_size('{TABLE}'),
               COALESCE(pg_total_relation_size(reltoastrelid), 0)
        FROM pg_class WHERE relname = '{TABLE}'
        """,  # noqa: S608 - TABLE is a module constant, not caller input
    )[0]
    print(f"  total {_mib(total)}   heap {_mib(heap)} ({_pct(heap, total)})")
    print(f"  indexes {_mib(idx)} ({_pct(idx, total)})   toast {int(toast):,} bytes")

    print("\n  per index (size, lifetime idx_scan):")
    index_bytes: dict[str, int] = {}
    for name, size, scans in _q(
        cur,
        f"""
        SELECT indexrelname, pg_relation_size(indexrelid), idx_scan
        FROM pg_stat_user_indexes WHERE relname = '{TABLE}'
        ORDER BY pg_relation_size(indexrelid) DESC
        """,  # noqa: S608
    ):
        index_bytes[str(name)] = int(size)
        flag = "  <- never scanned" if int(scans) == 0 else ""
        print(f"    {str(name):<58} {_mib(size)}  scans={int(scans):>9,}{flag}")
    return {"total": int(total), "heap": int(heap), "indexes": int(idx), **index_bytes}


def free_space(cur: psycopg.Cursor[Any], phys: dict[str, int]) -> None:
    """#3117's fourth bucket — reclaimable free space, kept separate from live bytes.

    "Report ... reclaimable versus OS-returned space. Separate logical
    compaction from physical reclaim." Dead tuples are space a plain VACUUM can
    already reuse WITHOUT a rewrite, so they are not a saving any policy here
    can claim credit for — and conversely they inflate the relation size that
    every percentage in this report is measured against.
    """
    print("\n=== 1b. RECLAIMABLE FREE SPACE (already reusable; NOT a policy saving) ===")
    live, dead, last_vac, last_autovac = _q(
        cur,
        f"""
        SELECT n_live_tup, n_dead_tup, last_vacuum, last_autovacuum
        FROM pg_stat_user_tables WHERE relname = '{TABLE}'
        """,  # noqa: S608
    )[0]
    print(f"  n_live_tup (estimate) {int(live):>12,}   n_dead_tup {int(dead):>12,}")
    print(f"  last_vacuum {last_vac}   last_autovacuum {last_autovac}")

    # ⚠⚠ ASK whether the extension exists; do NOT call it and catch the error.
    # A failed statement aborts the transaction, and the only way to continue
    # from there is a rollback — which ENDS the REPEATABLE READ transaction this
    # whole script's single-snapshot guarantee depends on. Every later section
    # would then run in a new, later snapshot while the report still reads as
    # one coherent measurement. That is not hypothetical: pgstattuple is not
    # installed on this dev cluster, so the first version took that path on
    # every run. A probe that degrades gracefully must not degrade the caller.
    installed = bool(_q(cur, "SELECT count(*) FROM pg_extension WHERE extname = 'pgstattuple'")[0][0])
    if installed:
        row = _q(
            cur,
            f"SELECT table_len, tuple_len, dead_tuple_len, free_space FROM pgstattuple('{TABLE}')",  # noqa: S608
        )[0]
        table_len, tuple_len, dead_len, free = (int(v) for v in row)
        print(f"  pgstattuple: table {_mib(table_len)}  live tuples {_mib(tuple_len)} ({_pct(tuple_len, table_len)})")
        print(f"               dead {_mib(dead_len)}  free space {_mib(free)} ({_pct(free, table_len)})")
        print("    free space + dead bytes are reclaimable by VACUUM without a rewrite.")
    else:
        _warn(
            "pgstattuple is not installed: heap bloat is NOT decomposed, so no figure in "
            "this report separates live bytes from reusable free space. Install it before "
            "sizing any rewrite."
        )
    print(f"  (heap as reported in section 1: {_mib(phys['heap'])})")


def counts(cur: psycopg.Cursor[Any]) -> dict[str, int]:
    """Row totals plus per-column population — the dead-column evidence."""
    print("\n=== 2. ROW + COLUMN POPULATION ===")
    row = _q(
        cur,
        f"""
        SELECT count(*), count(DISTINCT filing_event_id), count(DISTINCT accession_number),
               count(*) FILTER (WHERE is_primary),
               count(document_type), count(description), count(size_bytes)
        FROM {TABLE}
        """,  # noqa: S608
    )[0]
    rows, events, accessions, primaries, typed, described, sized = (int(v) for v in row)
    print(f"  rows                     {rows:>12,}")
    print(f"  distinct filing_event_id {events:>12,}")
    print(f"  distinct accession       {accessions:>12,}")
    print(f"  is_primary = TRUE        {primaries:>12,}  ({_pct(primaries, rows)})")
    print(f"  document_type NOT NULL   {typed:>12,}  ({_pct(typed, rows)})  <- #723: NULL by construction")
    print(f"  description  NOT NULL    {described:>12,}  ({_pct(described, rows)})  <- ditto")
    print(f"  size_bytes   NOT NULL    {sized:>12,}  ({_pct(sized, rows)})  (describes the REMOTE file)")
    return {"rows": rows, "filing_events": events, "accessions": accessions}


def duplication(cur: psycopg.Cursor[Any], cnt: dict[str, int]) -> None:
    """#3115 step 2 — the same directory stored once per filing event.

    One accession can anchor several ``filing_events`` (one per instrument it
    touches) and the catalogue keys on ``filing_event_id``, so the directory is
    re-stored per event.

    ⚠ The redundancy is MEASURED as ``count(*) - count(DISTINCT (accession,
    name))``, not estimated as ``rows × (fan-1)/fan``. The estimate assumes
    every event under an accession captured an identical document set, which is
    exactly the kind of assumption a snapshot taken at different times breaks.
    """
    print("\n=== 3. ACCESSION FAN-OUT (same directory stored per filing event) ===")
    rows = cnt["rows"]
    distinct_pairs = int(
        _q(cur, f"SELECT count(*) FROM (SELECT DISTINCT accession_number, document_name FROM {TABLE}) t")[0][0]  # noqa: S608
    )
    repeated = rows - distinct_pairs
    print(f"  rows {rows:,}   distinct (accession, document_name) {distinct_pairs:,}")
    print(f"  rows repeating an (accession, name) pair across events: {repeated:,} ({_pct(repeated, rows)})")

    print("\n  fan-out distribution (accessions by number of filing_events):")
    for fan, accs, class_rows in _q(
        cur,
        f"""
        SELECT fan, count(*), sum(docs) FROM (
          SELECT accession_number, count(DISTINCT filing_event_id) AS fan, count(*) AS docs
          FROM {TABLE} GROUP BY accession_number
        ) t GROUP BY fan ORDER BY fan
        """,  # noqa: S608
    ):
        print(f"    fan={int(fan)}  accessions {int(accs):>8,}  rows {int(class_rows):>10,}")
    print("  ⚠ De-duplicating across events must preserve the per-event association:")
    print("    filing_documents cascades from filing_events, so one parent's delete")
    print("    must not remove a directory another live event still points at.")


def redundant_representation(cur: psycopg.Cursor[Any], phys: dict[str, int], cnt: dict[str, int]) -> None:
    """#3117's 'redundant representation' bucket — priced as an UPPER BOUND.

    ⚠⚠ ``avg(pg_column_size(col)) × rows`` is the **stored value bytes** of a
    column. It is NOT the heap that dropping the column returns, for three
    reasons worth stating every time this number is quoted:

    1. ``ALTER TABLE … DROP COLUMN`` is catalogue-only. It reclaims nothing
       until the table is rewritten, and #3117 rejects "blanket VACUUM
       FULL/rewrite as the default fix" — so the rewrite is itself a decision
       with peak-disk, WAL and lock cost, not a free consequence.
    2. Tuple headers, line pointers, page headers, alignment padding and
       fillfactor mean heap does not shrink in proportion to value bytes.
    3. The buckets OVERLAP. A row removed by a section-5 exclusion takes its
       ``document_url`` bytes with it, so adding section 4 and section 5
       savings double-counts. They are alternatives to price, not a sum.

    The figure is still the right one for ranking candidates by size — which is
    all this section claims.
    """
    print("\n=== 4. REDUNDANT REPRESENTATION (stored value bytes — an UPPER BOUND on reclaim) ===")
    url_w, acc_w, name_w, row_w = _q(
        cur,
        f"""
        SELECT avg(pg_column_size(document_url)), avg(pg_column_size(accession_number)),
               avg(pg_column_size(document_name)), avg(pg_column_size(t.*))
        FROM {TABLE} t
        """,  # noqa: S608
    )[0]
    rows = cnt["rows"]
    print(
        f"  measured avg widths: url {float(url_w):.1f} B   accession {float(acc_w):.1f} B"
        f"   name {float(name_w):.1f} B   whole row {float(row_w):.1f} B"
    )

    items: list[tuple[str, float, str]] = [
        (
            "document_url column",
            float(url_w) * rows,
            "rebuilt at ingest from (cik, accession, name); see the CIK caveat below",
        ),
        (
            "accession_number column",
            float(acc_w) * rows,
            "sql/062: 'Denormalised from the parent row for readable queries'",
        ),
    ]
    for label, key in (
        ("surrogate PK index", f"{TABLE}_pkey"),
        ("accession index", f"idx_{TABLE}_accession"),
        ("document_type partial index", f"idx_{TABLE}_type"),
    ):
        if key in phys:
            items.append((f"{label} ({key})", float(phys[key]), "index bytes — reclaimed on DROP, no rewrite"))

    subtotal = 0.0
    for label, raw_bytes, why in items:
        subtotal += raw_bytes
        print(f"  {label:<48} {_mib(raw_bytes)}   {why}")
    print(f"  {'SUBTOTAL (upper bound)':<48} {_mib(subtotal)}   ({_pct(subtotal, phys['total'])} of relation)")
    print("\n  ⚠ document_url is NOT unconditionally reconstructible. ingest builds it from the")
    print("    ISSUER cik resolved via external_identifiers (provider='sec', is_primary=TRUE).")
    print("    That mapping can move — and the sec-edgar skill records that the archive")
    print("    directory lives under the issuer/filer CIK and 404s under a filing-agent CIK.")
    print("    Dropping the column therefore requires pinning the ingest-time CIK per")
    print("    filing_event, not re-deriving it from today's primary identifier.")


def content_classes(cur: psycopg.Cursor[Any], cnt: dict[str, int], *, inventory: bool) -> None:
    """The #3117 'unnecessary content' bucket — proposed vs undecided."""
    rows = cnt["rows"]
    events = cnt["filing_events"]
    print("\n=== 5. CONTENT CLASSES ===")

    print("\n  (a) PROPOSED for exclusion — locator derivable, or viewer chrome:")
    predicates = [p for _, p, _ in EXCLUSION_CANDIDATES]
    eligible = 0
    carved_out = 0
    for label, pred, why in EXCLUSION_CANDIDATES:
        n, prim, evts = _q(
            cur,
            cast(
                LiteralString,
                f"""
                SELECT count(*), count(*) FILTER (WHERE is_primary),
                       count(DISTINCT filing_event_id)
                FROM {TABLE} WHERE {pred}
                """,  # noqa: S608
            ),
        )[0]
        eligible += int(n) - int(prim)
        carved_out += int(prim)
        note = f"  (minus {int(prim)} primary → retained)" if int(prim) else ""
        print(f"    {label:<52} {int(n):>10,} ({_pct(int(n), rows)}){note}")
        # ⚠ "917,999 matches and 917,999 filing_events exist" does NOT establish
        # one per event: two matches in one event balance zero in another. The
        # row count must be compared against the DISTINCT event count of the
        # SAME predicate, which is what this does.
        shape = "exactly 1 per matched event" if int(n) == int(evts) else "NOT 1 per event"
        print(f"        spans {int(evts):,} of {events:,} filing_events  ({shape})")
        print(f"        {why}")

    class_sql = " OR ".join(f"({p})" for p in predicates)
    eligible_sql = f"({class_sql}) AND {_ELIGIBILITY_GUARD}"

    # Disjointness: an overlapping class set double-counts the saving.
    class_union = int(_q(cur, cast(LiteralString, f"SELECT count(*) FROM {TABLE} WHERE {class_sql}"))[0][0])  # noqa: S608
    eligible_union = int(_q(cur, cast(LiteralString, f"SELECT count(*) FROM {TABLE} WHERE {eligible_sql}"))[0][0])  # noqa: S608
    print(f"\n    eligible summed per class {eligible:,}   eligible union {eligible_union:,}")
    if eligible_union != eligible:
        _fail(f"exclusion classes OVERLAP ({eligible:,} summed vs {eligible_union:,} union) — saving double-counted")
    else:
        print("      → DISJOINT")

    # ⚠ The guard-wiring check must interrogate the UNGUARDED class set. Asking
    # "is_primary AND NOT is_primary" of the guarded set is a contradiction that
    # returns 0 against any implementation — it proves the SQL is consistent,
    # not that the guard protects anything (Codex ckpt-1 caught exactly that
    # shape here; it is the same defect as a verifier pinned to its own hash).
    print(f"    primary documents inside the proposed CLASSES:  {carved_out}")
    print(f"    class rows {class_union:,} − eligible rows {eligible_union:,} = {class_union - eligible_union}")
    if class_union - eligible_union != carved_out:
        _fail(f"guard arithmetic inconsistent: {class_union - eligible_union} carved vs {carved_out} counted")
    elif carved_out == 0:
        print("      → no primaries in these classes; the guard is inert but correct")
    else:
        print(f"      → the guard is WIRED: it removes exactly the {carved_out} primary row(s)")

    print("\n  (b) UNDECIDED — measured, deliberately NOT proposed (content value unsettled):")
    for label, pred in UNDECIDED_CLASSES:
        n, p = _q(
            cur,
            cast(LiteralString, f"SELECT count(*), count(*) FILTER (WHERE is_primary) FROM {TABLE} WHERE {pred}"),  # noqa: S608
        )[0]
        print(f"    {label:<52} {int(n):>10,} ({_pct(int(n), rows)})  primaries={int(p)}")

    if not inventory:
        return

    print("\n=== 6. DRY-RUN INVENTORY (a PROPOSAL — #3117 authorises no sweep) ===")
    orphaned = int(
        _q(
            cur,
            cast(
                LiteralString,
                f"""
                SELECT count(*) FROM (
                  SELECT filing_event_id FROM {TABLE}
                  GROUP BY filing_event_id
                  HAVING count(*) FILTER (WHERE NOT ({eligible_sql})) = 0
                ) t
                """,  # noqa: S608
            ),
        )[0][0]
    )
    print(f"  eligible rows                                  {eligible_union:>12,} ({_pct(eligible_union, rows)})")
    print(f"  filing_events left with ZERO children          {orphaned:>12,}")
    if orphaned:
        _fail(f"{orphaned:,} filing_events would be emptied and re-queued for an SEC fetch")
    else:
        print("    → every affected filing_event keeps at least one child row, so the")
        print("      existing completion signal still reads 'fetched'.")
    print("\n  ⚠ Three limits on that reassurance, none of them removable by re-running:")
    print("    1. This query starts FROM filing_documents, so it cannot see filing_events")
    print("       that already have no children — those are re-queued today, by design.")
    print("    2. It is a property of THIS scope, not of the design. Adding R-files or")
    print("       images to the class list can empty events, and nothing would catch it")
    print("       except re-running this section.")
    print("    3. One surviving child proves the event is not re-queued; it does NOT")
    print("       prove the retained manifest is complete or useful. A durable")
    print("       'directory processed under policy vN' receipt is the invariant that")
    print("       would, and it is the thing #3115 step 3 actually asks for.")


def consumers() -> None:
    """Re-derive the consumer classification from the tree, not from memory.

    #3115 step 1 asks which of {document browser, exhibit discovery, thesis
    context, financial parsers, replay} actually read this catalogue. Greps
    rather than asserts, so the answer re-checks itself as the tree moves.

    ⚠ A grep over the tree cannot see a reader that is not IN the tree — a psql
    runbook, a saved BI query, dynamic SQL assembled at runtime. It also cannot
    see a database-side reader, which is why ``--inventory`` reports view and
    FK dependants separately.

    Uses ``git grep`` rather than ``rg``: the first run of this script died with
    ``FileNotFoundError: 'rg'`` because ripgrep is on the interactive shell's
    PATH but not the subprocess environment's. ``git`` is a hard repo
    dependency, and restricting to tracked files is wanted anyway — an
    untracked scratch file is not a consumer.
    """
    print("\n=== 7. CONSUMER CLASSIFICATION (grepped, not remembered) ===")
    probes: list[tuple[str, list[str]]] = [
        (
            "SQL reads of the table outside its own service",
            ["git", "grep", "-nI", TABLE, "--", "app/", f":(exclude)app/services/{TABLE}.py"],
        ),
        (
            # ⚠ tests/ MUST be in scope. An earlier version searched only
            # app/, frontend/src and scripts/, then concluded "the only caller
            # is a test" — a claim its own probe could not have produced.
            "callers of the reader list_filing_documents (incl. tests)",
            ["git", "grep", "-nI", f"list_{TABLE}", "--", "app/", "frontend/src", "scripts/", "tests/"],
        ),
        ("frontend references", ["git", "grep", "-nIiE", "filing.?document", "--", "frontend/src"]),
        (
            # Raw SQL and runbooks are consumers too, and none of them import
            # anything. #3109 used catalogue membership as a discovery witness.
            "raw SQL / runbooks / specs referencing the table",
            ["git", "grep", "-lI", TABLE, "--", "sql/", "docs/"],
        ),
        (
            "parsers that re-fetch index.json over HTTP instead",
            ["git", "grep", "-nI", "fetch_filing_index", "--", "app/services/manifest_parsers/"],
        ),
    ]
    for label, cmd in probes:
        out = subprocess.run(cmd, capture_output=True, text=True, check=False).stdout.strip()
        hits = [ln for ln in out.splitlines() if ln]
        print(f"\n  {label}: {len(hits)} hit(s)")
        for ln in hits[:12]:
            print(f"    {ln}")


def db_dependants(cur: psycopg.Cursor[Any]) -> None:
    """Readers the tree-grep structurally cannot see: views and foreign keys.

    A grep answers "does any Python read this table". It does not answer "does
    any DATABASE object depend on it" — a view, a materialised view, or an FK
    from another table onto ``filing_documents.id``. The second question also
    decides whether the surrogate PK is droppable.
    """
    print("\n=== 8. DATABASE-SIDE DEPENDANTS (what a code grep cannot see) ===")
    views = _q(
        cur,
        f"""
        SELECT DISTINCT dependent.relname, dependent.relkind
        FROM pg_depend d
        JOIN pg_rewrite r ON r.oid = d.objid
        JOIN pg_class dependent ON dependent.oid = r.ev_class
        JOIN pg_class source ON source.oid = d.refobjid
        WHERE source.relname = '{TABLE}' AND dependent.relname <> '{TABLE}'
        """,  # noqa: S608
    )
    print(f"  views / matviews selecting from {TABLE}: {len(views)}")
    for name, kind in views:
        print(f"    {name} (relkind={kind})")

    fks = _q(
        cur,
        f"""
        SELECT conrelid::regclass::text, conname
        FROM pg_constraint
        WHERE contype = 'f' AND confrelid = '{TABLE}'::regclass
        """,  # noqa: S608
    )
    print(f"  foreign keys REFERENCING {TABLE}: {len(fks)}")
    for rel, con in fks:
        print(f"    {rel}.{con}")
    if not fks:
        print("    → nothing references this table, so the surrogate PK carries no")
        print("      referential load and can simply be DROPPED. ⚠ It cannot be")
        print("      'replaced' via ADD PRIMARY KEY USING INDEX on the natural key:")
        print("      PostgreSQL rejects that with 'index ... is already associated")
        print("      with a constraint' (verified on a TEMP table, not assumed).")
        print("      Dropping the PK constraint alone leaves the UNIQUE constraint")
        print("      enforcing (filing_event_id, document_name) — also verified.")


def main() -> int:
    # Line-buffer stdout. Every section is a multi-minute full-table aggregate,
    # and Python block-buffers a redirected stream — so a healthy run sent to a
    # file sits at 0 bytes and reads as a stall (prevention log: "a pipe also
    # BUFFERS, which blinds a long-running job").
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(line_buffering=True)

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--inventory", action="store_true", help="add the dry-run eligible-row inventory")
    ap.add_argument("--consumers", action="store_true", help="only re-derive the consumer classification")
    args = ap.parse_args()

    if args.consumers:
        consumers()
        return 0

    assert_dev_environment()
    with psycopg.connect(settings.database_url) as conn:
        # One snapshot for the whole report, and no write path at all.
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '1800s'")
            db, start_xact = _q(
                cur,
                "SELECT current_database(), (SELECT xact_start FROM pg_stat_activity WHERE pid = pg_backend_pid())",
            )[0]
            print(f"database={db}  snapshot=REPEATABLE READ  read_only=True  xact_start={start_xact}")
            phys = physical(cur)
            free_space(cur, phys)
            cnt = counts(cur)
            duplication(cur, cnt)
            redundant_representation(cur, phys, cnt)
            content_classes(cur, cnt, inventory=args.inventory)
            db_dependants(cur)
            # The single-snapshot claim is checked, not asserted. A mid-run
            # rollback (the defect this replaced) starts a new transaction, so
            # the backend's transaction start time would move away from the one
            # section 1 ran under. Same snapshot → same xact_start.
            end_xact = _q(cur, "SELECT xact_start FROM pg_stat_activity WHERE pid = pg_backend_pid()")[0][0]
            if end_xact != start_xact:
                _fail(
                    f"snapshot broke mid-run: transaction started {start_xact}, now {end_xact}. "
                    "Sections do not describe one database state."
                )
            else:
                print(f"\nSnapshot held: one transaction from {start_xact} to close.")
    consumers()

    if WARNINGS:
        print(f"\n⚠ {len(WARNINGS)} measurement(s) this run could NOT make:")
        for w in WARNINGS:
            print(f"  - {w}")

    if FAILURES:
        print(f"\n⚠ {len(FAILURES)} INVARIANT(S) FAILED — this inventory is NOT a safe scope:")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("\nAll section-5 invariants held.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
