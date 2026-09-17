"""#2790 — correct stored insider observations whose date postdates their filing.

PR #3145 shipped the ingest gate and left the stored rows, blocked on one
question: does #1687's ``transaction_timeliness == 'E'`` exemption stand? It
does, and it keys on the wrong field — see
``app.services.insider_transactions.is_early_form5_line`` and
``docs/specs/ownership/2026-09-17-2790-form5-exemption-and-correction.md``.

The exemption is (submission type × line form type), and **neither field is
stored** in this repo (``transactionFormType`` has no column on either writer).
The SEC's own quarterly Form 345 datasets carry both — ``SUBMISSION.DOCUMENT_TYPE``
and ``NONDERIV_TRANS.TRANS_FORM_TYPE`` — so this script adjudicates every stored
candidate against the cached archives rather than against a stored proxy or a
list baked into the file.

Classification, per row, into three DISJOINT sets:

* **correctable** — the resolved line is governed by Rule 16a-3(g) (a form-type-4
  line on any submission) or by 16a-3(f) as its own filing (a form-type-5 line on
  a Form 5 submission). A reported date after the filing date is impossible.
* **exempt** — a form-type-5 line on a Form 4 submission: volunteered early
  against 16a-3(f)'s later deadline, so a future date is legitimate.
* **unresolved** — the archives do not adjudicate the row. **Kept, always.**

⚠⚠ ``--apply`` REFUSES to run while anything is unresolved. "Keep what you could
not classify" plus "0 breaches remain" is a pair that passes vacuously on an
empty archive cache — the run would report success having deleted nothing and
proved nothing. Completeness is asserted, not hoped for.

Read-only by default. ``--apply`` performs ONE transaction: soft-delete
(``known_to``, per I6 — never a hard delete) + ``refresh_insiders_current_batch``
over the affected instruments + a post-refresh assertion, all or nothing. The
repair sweep cannot rescue a half-done correction: its drift predicate compares
``MAX(ingested_at)``, which setting ``known_to`` does not move.

Run::

    PYTHONPATH=. uv run python scripts/correct_2790_insider_future_period.py
    PYTHONPATH=. uv run python scripts/correct_2790_insider_future_period.py --apply --ledger /tmp/2790.jsonl
"""

from __future__ import annotations

import argparse
import json
import zipfile
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
import psycopg.rows

from app.config import settings
from app.security.master_key import resolve_data_dir
from app.services.insider_transactions import is_early_form5_line
from app.services.ownership_observations import refresh_insiders_current_batch
from app.services.sec_insider_dataset_ingest import _iter_tsv, _parse_iso_date

# ⚠ IMPORTED, not restated. The scope predicate is PR #3145's and the two must not
# drift: a correction whose scope is a COPY of the audit's scope measures a proxy
# for the population it deletes from (the #2788 census lesson, prevention log).
from scripts.audit_2790_insider_future_period import _SCOPE


def _default_archive_dir() -> Path:
    """The bulk cache the ingest orchestrator itself uses (``_bulk_dir``)."""
    return resolve_data_dir() / "sec" / "bulk"


@dataclass(frozen=True)
class _Line:
    """One resolved ``NONDERIV_TRANS`` line, with the archive it came from."""

    trans_form_type: str
    timeliness: str
    trans_date: date | None
    archive: str


def _archive_paths(explicit: Path | None) -> list[Path]:
    base = explicit or _default_archive_dir()
    paths = sorted(base.glob("insider_*.zip"))
    if not paths:
        raise SystemExit(f"no insider_*.zip archives under {base}")
    return paths


def _load_archives(paths: list[Path], accessions: set[str]) -> tuple[dict[str, str], dict[tuple[str, str], _Line]]:
    """Resolve submission types and transaction lines for ``accessions``.

    Aborts on a cross-archive disagreement rather than letting iteration order
    decide which quarter wins: an accession republished with a different form
    type is a real event, and silently picking one would make a destructive
    classification depend on a filesystem sort.
    """
    submissions: dict[str, str] = {}
    lines: dict[tuple[str, str], _Line] = {}
    for path in paths:
        with zipfile.ZipFile(path) as zf:
            for row in _iter_tsv(zf, "SUBMISSION.tsv"):
                accession = (row.get("ACCESSION_NUMBER") or "").strip()
                if accession not in accessions:
                    continue
                doc_type = (row.get("DOCUMENT_TYPE") or row.get("FORM_TYPE") or row.get("FORM") or "").strip()
                prior = submissions.get(accession)
                if prior is not None and prior != doc_type:
                    raise SystemExit(
                        f"conflicting DOCUMENT_TYPE for {accession}: {prior!r} vs {doc_type!r} ({path.name})"
                    )
                submissions[accession] = doc_type
            for row in _iter_tsv(zf, "NONDERIV_TRANS.tsv", "NON_DERIV_TRANS.tsv"):
                accession = (row.get("ACCESSION_NUMBER") or "").strip()
                if accession not in accessions:
                    continue
                sk = (row.get("NONDERIV_TRANS_SK") or row.get("NON_DERIV_TRANS_SK") or "").strip()
                line = _Line(
                    trans_form_type=(row.get("TRANS_FORM_TYPE") or "").strip(),
                    timeliness=(row.get("TRANS_TIMELINESS") or "").strip(),
                    trans_date=_parse_iso_date(row.get("TRANS_DATE")),
                    archive=path.name,
                )
                prior_line = lines.get((accession, sk))
                if prior_line is not None and prior_line.trans_form_type != line.trans_form_type:
                    raise SystemExit(
                        f"conflicting TRANS_FORM_TYPE for {accession}:{sk}: "
                        f"{prior_line.trans_form_type!r} ({prior_line.archive}) "
                        f"vs {line.trans_form_type!r} ({path.name})"
                    )
                lines[(accession, sk)] = line
    return submissions, lines


_BREACHING_CURRENT_SQL = """
    SELECT c.instrument_id, c.holder_identity_key, c.ownership_nature, c.source,
           c.source_document_id, c.period_end, c.source_accession
      FROM ownership_insiders_current c
     WHERE c.period_end > (c.filed_at AT TIME ZONE 'UTC')::date
       AND c.source_document_id NOT LIKE '%:NDH:%'
"""


def _key(row: dict[str, Any]) -> tuple[Any, ...]:
    """The six-column identity — this table has no surrogate id."""
    return (
        row["instrument_id"],
        row["holder_identity_key"],
        row["ownership_nature"],
        row["source"],
        row["source_document_id"],
        row["period_end"],
    )


def _resolve(
    row: dict[str, Any],
    submissions: dict[str, str],
    lines: dict[tuple[str, str], _Line],
) -> tuple[str, _Line | None, str]:
    """Return ``(verdict, line, why)`` for one stored observation row.

    Two resolvers, in order:

    1. the row's own ``:NDT:`` surrogate key — exact, one stored row to one
       archive line;
    2. for a row with no ``:NDT:`` key (the XML write path stores the bare
       accession), every archive line on that accession whose ``TRANS_DATE``
       equals the stored ``period_end``, requiring UNANIMITY on the form type.
       Accession-level agreement is not assumed: a mixed filing carries both
       form types, which is exactly the RRC shape, so a non-unanimous date match
       stays unresolved.
    """
    accession = row["source_accession"]
    submission = submissions.get(accession)
    if submission is None:
        return ("unresolved", None, "no SUBMISSION row in any archive")

    document_id = row["source_document_id"] or ""
    line: _Line | None = None
    if ":NDT:" in document_id:
        sk = document_id.split(":NDT:", 1)[1]
        line = lines.get((accession, sk))
        if line is None:
            return ("unresolved", None, f"no NONDERIV_TRANS line for SK {sk}")
        # #18 — a key that resolves is not the same as a line that DESCRIBES this
        # row. Confirm the archive line carries the date we are about to delete
        # for, so a re-keyed or revised extract cannot adjudicate a different one.
        if line.trans_date is not None and line.trans_date != row["period_end"]:
            mismatch = f"archive TRANS_DATE {line.trans_date} != stored period_end {row['period_end']}"
            return ("unresolved", None, mismatch)
    else:
        matches = [
            candidate
            for (candidate_accession, _sk), candidate in lines.items()
            if candidate_accession == accession and candidate.trans_date == row["period_end"]
        ]
        if not matches:
            return ("unresolved", None, "no archive line at the stored period_end")
        form_types = {candidate.trans_form_type for candidate in matches}
        if len(form_types) != 1:
            return ("unresolved", None, f"date match is not unanimous on form type: {sorted(form_types)}")
        line = matches[0]

    why = f"submission={submission!r} line={line.trans_form_type!r} timeliness={line.timeliness or '(blank)'!r}"
    # ⚠ "Not exempt" is not the same as "adjudicated". A blank or unrecognised
    # TRANS_FORM_TYPE with no usable ``E`` fallback establishes NOTHING about the
    # line — the archive simply did not say — and falling through to "correctable"
    # would soft-delete it under a rule that never fired. The failure is silent in
    # the destructive direction, so it is checked before the verdict, not after.
    if line.trans_form_type not in {"4", "5"} and not is_early_form5_line(submission, None, line.timeliness):
        return ("unresolved", line, f"archive does not establish the line form type ({why})")

    verdict = "exempt" if is_early_form5_line(submission, line.trans_form_type, line.timeliness) else "correctable"
    return (verdict, line, why)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="soft-delete the correctable rows (default: census only)")
    parser.add_argument("--archives", type=Path, default=None, help="directory of insider_*.zip archives")
    parser.add_argument("--ledger", type=Path, default=None, help="JSONL ledger path (required with --apply)")
    args = parser.parse_args()

    if args.apply and args.ledger is None:
        raise SystemExit("--apply requires --ledger: an unlogged destructive run cannot be reversed row by row")

    run_id = str(uuid4())
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SET statement_timeout='900s'")
            cur.execute(
                f"""
                SELECT o.instrument_id, o.holder_identity_key, o.ownership_nature, o.source,
                       o.source_document_id, o.period_end, o.source_accession, o.shares,
                       (o.filed_at AT TIME ZONE 'UTC')::date AS filed_date
                  FROM ownership_insiders_observations o
                 WHERE {_SCOPE}
                 ORDER BY o.instrument_id, o.holder_identity_key, o.source_document_id, o.period_end
                """  # noqa: S608 — _SCOPE is a module constant, no interpolation of input
            )
            rows = cur.fetchall()

        print(f"scope (PR #3145 predicate): {len(rows):,} live breaching rows")
        submissions, lines = _load_archives(_archive_paths(args.archives), {r["source_accession"] for r in rows})
        print(f"archives resolved: {len(submissions):,} submissions, {len(lines):,} transaction lines")

        buckets: dict[str, list[dict[str, Any]]] = {"correctable": [], "exempt": [], "unresolved": []}
        shapes: Counter[str] = Counter()
        for row in rows:
            verdict, line, why = _resolve(row, submissions, lines)
            buckets[verdict].append({**row, "_why": why, "_archive": line.archive if line else None})
            shapes[f"{verdict}: {why}" if verdict == "unresolved" else f"{verdict}: {why}"] += 1

        for verdict in ("correctable", "exempt", "unresolved"):
            print(f"  {verdict:12s} {len(buckets[verdict]):>6,}")
        assert sum(len(v) for v in buckets.values()) == len(rows), "buckets are not a partition of the scope"
        print("\nshapes:")
        for shape, count in shapes.most_common(12):
            print(f"  {count:>6,}  {shape}")

        if not args.apply:
            print("\ncensus only — pass --apply --ledger PATH to correct")
            return 0

        if buckets["unresolved"]:
            print(f"\nREFUSED: {len(buckets['unresolved']):,} rows unresolved.")
            print("A run that keeps what it cannot classify and then reports '0 breaches remain' passes")
            print("vacuously on a partial archive cache. Resolve or exclude them explicitly first.")
            return 2

        targets = buckets["correctable"]
        instrument_ids = sorted({int(r["instrument_id"]) for r in targets})
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(_BREACHING_CURRENT_SQL)
            breaching_before = {_key(row) for row in cur.fetchall()}
        print(f"breaching _current rows before: {len(breaching_before):,}")
        stamped = datetime.now(UTC)
        # ⚠ The table has NO surrogate id — its PK is the six-column identity
        # below, and one source row fans out across share-class siblings (#1117),
        # so instrument_id is part of what a reversal has to name.
        # ``x`` — exclusive create. A rerun against the same path would otherwise
        # truncate the only row-by-row reversal record of the first run, and the
        # second run has nothing left to write, so the ledger would end up empty
        # for a correction that did happen.
        with args.ledger.open("x", encoding="utf-8") as handle:
            for row in targets:
                handle.write(
                    json.dumps(
                        {
                            "run_id": run_id,
                            "ticket": "2790",
                            "key": {
                                "instrument_id": row["instrument_id"],
                                "holder_identity_key": row["holder_identity_key"],
                                "ownership_nature": row["ownership_nature"],
                                "source": row["source"],
                                "source_document_id": row["source_document_id"],
                                "period_end": row["period_end"].isoformat(),
                            },
                            "source_accession": row["source_accession"],
                            "filed_date": row["filed_date"].isoformat() if row["filed_date"] else None,
                            "shares": str(row["shares"]),
                            "prior_known_to": None,  # the scope selects known_to IS NULL only
                            "known_to_set_to": stamped.isoformat(),
                            "evidence": row["_why"],
                            "archive": row["_archive"],
                        },
                        sort_keys=True,
                    )
                    + "\n"
                )
        print(f"\nledger written: {args.ledger} ({len(targets):,} rows, run {run_id})")

        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE ownership_insiders_observations o
                       SET known_to = %(t)s
                      FROM unnest(
                             %(instrument_ids)s::bigint[], %(holders)s::text[], %(natures)s::text[],
                             %(sources)s::text[], %(documents)s::text[], %(periods)s::date[]
                           ) AS k(instrument_id, holder_identity_key, ownership_nature, source,
                                  source_document_id, period_end)
                     WHERE o.instrument_id       = k.instrument_id
                       AND o.holder_identity_key = k.holder_identity_key
                       AND o.ownership_nature    = k.ownership_nature
                       AND o.source              = k.source
                       AND o.source_document_id  = k.source_document_id
                       AND o.period_end          = k.period_end
                       AND o.known_to IS NULL
                    """,
                    {
                        "t": stamped,
                        "instrument_ids": [int(r["instrument_id"]) for r in targets],
                        "holders": [r["holder_identity_key"] for r in targets],
                        "natures": [r["ownership_nature"] for r in targets],
                        "sources": [r["source"] for r in targets],
                        "documents": [r["source_document_id"] for r in targets],
                        "periods": [r["period_end"] for r in targets],
                    },
                )
                updated = cur.rowcount
            if updated != len(targets):
                raise SystemExit(f"expected {len(targets)} soft-deletes, applied {updated} — rolled back")
            refreshed = refresh_insiders_current_batch(conn, instrument_ids=instrument_ids)

            # POSTCONDITION, asserted inside the transaction — soft-deleting a
            # winner PROMOTES an older observation, which can itself breach, so
            # the projection is re-examined rather than inferred from the delete
            # count. It is checked against ``filed_at``, the invariant actually
            # being corrected: ``period_end > current_date`` would go quiet on a
            # historical wrong-year row the moment that date passes.
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(_BREACHING_CURRENT_SQL)
                remaining = cur.fetchall()
            # ⚠ ``submissions``/``lines`` were loaded for the SCOPE's accessions,
            # so a promoted row on any other accession resolves as "unresolved"
            # here — which must not read as "fine". Hence the second clause: an
            # unadjudicated breach is tolerated only if it was ALREADY breaching
            # before this run. The correction may not introduce one.
            unexpected = []
            for row in remaining:
                verdict = _resolve(row, submissions, lines)[0]
                if verdict == "correctable" or (verdict == "unresolved" and _key(row) not in breaching_before):
                    unexpected.append((verdict, row))
            if unexpected:
                for verdict, row in unexpected[:10]:
                    print(
                        f"  UNEXPECTED [{verdict}] {row['source_accession']} "
                        f"{row['source_document_id']} {row['period_end']}"
                    )
                raise SystemExit(
                    f"{len(unexpected)} rows still breach after the refresh and are not exempt — rolled back"
                )
            print(
                f"soft-deleted {updated:,} rows; refreshed {refreshed:,} instruments; "
                f"{len(remaining)} breaching _current rows remain, none correctable"
            )
        print(f"COMMITTED run {run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
