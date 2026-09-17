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
                    raise SystemExit(f"conflicting DOCUMENT_TYPE for {accession}: {prior!r} vs {doc_type!r} ({path.name})")
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
                        f"{prior_line.trans_form_type!r} ({prior_line.archive}) vs {line.trans_form_type!r} ({path.name})"
                    )
                lines[(accession, sk)] = line
    return submissions, lines


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
            return ("unresolved", None, f"archive TRANS_DATE {line.trans_date} != stored period_end {row['period_end']}")
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

    exempt = is_early_form5_line(submission, line.trans_form_type, line.timeliness)
    verdict = "exempt" if exempt else "correctable"
    return (verdict, line, f"submission={submission!r} line={line.trans_form_type!r} timeliness={line.timeliness or '(blank)'!r}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="soft-delete the correctable rows (default: census only)")
    parser.add_argument("--archives", type=Path, default=None, help="directory of insider_*.zip archives")
    parser.add_argument("--ledger", type=Path, default=None, help="JSONL ledger path (required with --apply)")
    args = parser.parse_args()

    if args.apply and args.ledger is None:
        raise SystemExit("--apply requires --ledger: an unlogged destructive run cannot be reversed row by row")

    run_id = str(uuid4())
    with psycopg.connect(settings.database_url, row_factory=psycopg.rows.dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout='900s'")
            cur.execute(
                f"""
                SELECT o.id, o.instrument_id, o.holder_identity_key, o.source, o.source_accession,
                       o.source_document_id, o.period_end, o.shares,
                       (o.filed_at AT TIME ZONE 'UTC')::date AS filed_date
                  FROM ownership_insiders_observations o
                 WHERE {_SCOPE}
                 ORDER BY o.id
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

        target_ids = [r["id"] for r in buckets["correctable"]]
        instrument_ids = sorted({int(r["instrument_id"]) for r in buckets["correctable"]})
        stamped = datetime.now(UTC)
        with args.ledger.open("w", encoding="utf-8") as handle:
            for row in buckets["correctable"]:
                handle.write(
                    json.dumps(
                        {
                            "run_id": run_id,
                            "ticket": "2790",
                            "observation_id": row["id"],
                            "instrument_id": row["instrument_id"],
                            "holder_identity_key": row["holder_identity_key"],
                            "source": row["source"],
                            "source_accession": row["source_accession"],
                            "source_document_id": row["source_document_id"],
                            "period_end": row["period_end"].isoformat(),
                            "filed_date": row["filed_date"].isoformat() if row["filed_date"] else None,
                            "shares": str(row["shares"]),
                            "prior_known_to": None,  # scope selects known_to IS NULL only
                            "evidence": row["_why"],
                            "archive": row["_archive"],
                        },
                        sort_keys=True,
                    )
                    + "\n"
                )
        print(f"\nledger written: {args.ledger} ({len(target_ids):,} rows, run {run_id})")

        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE ownership_insiders_observations SET known_to = %(t)s"
                    " WHERE id = ANY(%(ids)s::bigint[]) AND known_to IS NULL",
                    {"t": stamped, "ids": target_ids},
                )
                updated = cur.rowcount
            if updated != len(target_ids):
                raise SystemExit(f"expected {len(target_ids)} soft-deletes, applied {updated} — rolled back")
            refreshed = refresh_insiders_current_batch(conn, instrument_ids=instrument_ids)
            with conn.cursor() as cur:
                # #26 — removing a winner can PROMOTE another future-dated
                # observation, so assert the projection after the refresh rather
                # than inferring it from the delete count.
                cur.execute("SELECT count(*) AS n FROM ownership_insiders_current WHERE period_end > current_date")
                residual = int(cur.fetchone()["n"])
            print(f"soft-deleted {updated:,} rows; refreshed {refreshed:,} instruments; future-dated _current now {residual}")
        print(f"COMMITTED run {run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
