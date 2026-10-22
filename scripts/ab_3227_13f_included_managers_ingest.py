"""Ingest A/B for the #3227 tier (IM) capture backfill.

Spec: docs/specs/ownership/2026-10-22-3227-13f-included-managers.md § Change 3 ("Ingest A/B").

``run ARCHIVE ...`` holds one session open across the backfill:

1. Before-image: a TEMP table of every ``ownership_institutions_observations`` row's key hash and the hash of its every
   pre-existing column (``ingest_run_id`` and ``ingested_at`` included; ``thirteen_f_segments`` excluded), at T0.
2. The backfill (``scripts.backfill_13f_included_managers``) on its own connections.
3. Gates, non-zero exit on any failure:
   - every row present before and after, and not rewritten by a live writer after T0 (``ingested_at`` ≤ T0), has
     every pre-existing column identical. Keys deleted, inserted or rewritten after T0 are live ingest (the backfill
     only UPDATEs ``thirteen_f_segments``) and are reported;
   - every accession in the archives has a capture record whose status, counts, value totals and list entries (as
     filed) equal those this script's own reader computes from SUBMISSION, SUMMARYPAGE, OTHERMANAGER2 and INFOTABLE;
   - every ``source = '13f'`` observation of those accessions, of every exposure, has non-NULL segments carrying its
     accession's ``capture_id``, equal to the reader's lines for that key (``cusips`` included) and self-consistent
     (the lines' shares sum to the observation's shares). A mismatch on an observation rewritten after T0 is
     reported as live ingest, and a NULL on a zero-share observation (written before the #1433 shares > 0 guard;
     no writer line describes it) as residue, neither gated. Segments equal to the reader on an observation whose
     shares were summed under an older CUSIP map are reported as stale against the map: the read's
     self-consistency check refuses them;
   - no observation outside those accessions, other than one rewritten after T0, has segments.
   Status counts are reported.

``check ARCHIVE ...`` runs the capture gates alone (after a deploy, for instance). With no T0, live captures outside
the archives are expected, so their count is reported rather than gated.

The reader is written separately from ``app.services.thirteen_f_capture`` and the bulk SQL: its own grammar, its own
status rule and a Python aggregation, compared against what the SQL stored. Only the CUSIP map
(``load_bulk_cusip_map``) is shared, because it decides the observation key, not the capture.

Usage: ``PYTHONPATH=. uv run python -m scripts.ab_3227_13f_included_managers_ingest run|check A.zip B.zip ...``
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import zipfile
from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.services.cusip_resolver import load_bulk_cusip_map
from scripts.backfill_13f_included_managers import main as backfill_main

_PRE_EXISTING = (
    "instrument_id, filer_cik, filer_name, filer_type, ownership_nature, source, source_document_id, source_accession, "
    "source_field, source_url, filed_at, period_start, period_end, known_from, known_to, ingest_run_id, shares, "
    "market_value_usd, voting_authority, exposure_kind, ingested_at"
)
_KEY = "instrument_id, filer_cik, ownership_nature, period_end, source_document_id, exposure_kind"

_HASHED = f"hashtextextended(({_KEY})::text, 0) AS k, hashtextextended(({_PRE_EXISTING})::text, 0) AS h"

_BEFORE_SQL = f"CREATE TEMP TABLE ab_before AS SELECT {_HASHED} FROM ownership_institutions_observations"

_DIFF_SQL = f"""
SELECT count(*) FILTER (WHERE c.k IS NULL),
       count(*) FILTER (WHERE b.k IS NULL),
       count(*) FILTER (WHERE b.k IS NOT NULL AND c.k IS NOT NULL AND c.ingested_at > %(t0)s),
       count(*) FILTER (WHERE b.k IS NOT NULL AND c.k IS NOT NULL AND c.ingested_at <= %(t0)s AND c.h <> b.h)
FROM ab_before b
FULL JOIN (SELECT {_HASHED}, ingested_at FROM ownership_institutions_observations) c ON c.k = b.k
"""

_DIGITS = re.compile(r"^[0-9]{1,9}$")


def _seq(raw: str | None) -> int | None:
    if raw is None or not _DIGITS.match(raw):
        return None
    return int(raw) or None


def _column7(raw: str) -> list[int] | None:
    text = raw.strip()
    if text == "":
        return []
    out: set[int] = set()
    for token in re.split(r"[\s,]+", text):
        number = _seq(token)
        if number is None:
            return None
        out.add(number)
    return sorted(out)


def _rows(zf: zipfile.ZipFile, name: str) -> Any:
    member = next(n for n in zf.namelist() if n == name or n.endswith("/" + name))
    with zf.open(member) as fh:
        yield from csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8", newline=""), delimiter="\t")


def _strip(value: str | None) -> str | None:
    text = (value or "").strip()
    return text or None


def _number(value: str | None) -> Decimal | None:
    text = (value or "").strip()
    try:
        return Decimal(text) if text else None
    except ArithmeticError:
        return None


def _count(value: str | None) -> int | None:
    text = (value or "").strip()
    return int(text) if text.isascii() and text.isdigit() else None


def _read_archive(path: Path, cusip_map: dict[str, int]) -> tuple[dict[str, dict[str, Any]], dict[tuple, dict]]:
    """Expected capture per accession, and expected lines per observation key."""
    with zipfile.ZipFile(path) as zf:
        submissions = {r["ACCESSION_NUMBER"]: r for r in _rows(zf, "SUBMISSION.tsv")}
        covers = {r["ACCESSION_NUMBER"]: r for r in _rows(zf, "COVERPAGE.tsv")}
        summaries = {r["ACCESSION_NUMBER"]: r for r in _rows(zf, "SUMMARYPAGE.tsv")}
        entries: dict[str, list[tuple]] = {}
        for r in _rows(zf, "OTHERMANAGER2.tsv"):
            acc = (r.get("ACCESSION_NUMBER") or "").strip()
            if acc:
                raw = _strip(r.get("SEQUENCENUMBER"))
                entries.setdefault(acc, []).append(
                    (raw, _seq(raw), _strip(r.get("CIK")), _strip(r.get("NAME")), _strip(r.get("FORM13FFILENUMBER")))
                )
        lines_by_acc: Counter[str] = Counter()
        value_by_acc: dict[str, Decimal] = {}
        keyed: dict[tuple, dict] = {}
        for r in _rows(zf, "INFOTABLE.tsv"):
            acc = (r.get("ACCESSION_NUMBER") or "").strip()
            if not acc:
                continue
            lines_by_acc[acc] += 1
            value_by_acc[acc] = value_by_acc.get(acc, Decimal(0)) + (_number(r.get("VALUE")) or Decimal(0))
            sub, cover = submissions.get(acc), covers.get(acc)
            cusip = (r.get("CUSIP") or "").strip().upper()
            kind = (r.get("SSHPRNAMTTYPE") or "").strip().upper()
            shares = _number(r.get("SSHPRNAMT"))
            if sub is None or cover is None or kind not in ("", "SH") or shares is None or shares <= 0:
                continue
            iid = cusip_map.get(cusip)
            if iid is None:
                continue
            putcall = (r.get("PUTCALL") or "").strip().upper()
            exposure = putcall if putcall in ("PUT", "CALL") else "EQUITY"
            key = (iid, str(sub["CIK"]).strip().zfill(10), acc, exposure)
            slot = keyed.setdefault(key, {"cusips": set(), "lines": {}})
            slot["cusips"].add(cusip)
            om = _column7(r.get("OTHERMANAGER") or "")
            seg = (_strip(r.get("INVESTMENTDISCRETION")), None if om is None else tuple(om))
            slot["lines"][seg] = slot["lines"].get(seg, Decimal(0)) + shares

    captures: dict[str, dict[str, Any]] = {}
    for acc in submissions:
        summary = summaries.get(acc) or {}
        listed = entries.get(acc, [])
        numbers = [e[1] for e in listed]
        expected_list = _count(summary.get("OTHERINCLUDEDMANAGERSCOUNT"))
        expected_lines = _count(summary.get("TABLEENTRYTOTAL"))
        expected_value = _number(summary.get("TABLEVALUETOTAL"))
        line_count, value = lines_by_acc.get(acc, 0), value_by_acc.get(acc, Decimal(0))
        if None in numbers or len(set(numbers)) != len(numbers):
            status = "invalid"
        elif (
            expected_list == len(listed)
            and expected_lines == line_count
            and expected_value is not None
            and abs(value - expected_value) <= Decimal("0.5") * line_count
        ):
            status = "complete"
        else:
            status = "incomplete"
        captures[acc] = {
            "status": status,
            "counts": (len(listed), expected_list, line_count, expected_lines, value, expected_value),
            "entries": [(i, *e) for i, e in enumerate(listed)],
        }
    return captures, keyed


def _expected_lines(slot: dict) -> list[dict[str, Any]]:
    def order(item: tuple) -> tuple[str, str]:
        (discretion, om), _ = item
        return (discretion or "", "~" if om is None else ",".join(map(str, om)))

    out = []
    for (discretion, om), shares in sorted(slot["lines"].items(), key=order):
        text = format(shares.normalize(), "f")
        out.append({"discretion": discretion, "other_managers": None if om is None else list(om), "shares": text})
    return out


def _capture_gates(
    conn: psycopg.Connection[Any], archives: list[Path], t0: Any, failures: list[str], notes: list[str]
) -> None:
    cusip_map = load_bulk_cusip_map(conn)
    statuses: Counter[str] = Counter()
    observations_checked = 0
    with_segments = 0
    for archive in archives:
        expected, keyed = _read_archive(archive, cusip_map)
        stored = {
            r[0]: r[1:]
            for r in conn.execute(
                "SELECT accession_number, capture_id::text, status, list_count, expected_list_count, line_count, "
                "expected_line_count, value_total, expected_value_total FROM institutional_13f_captures "
                "WHERE accession_number = ANY(%s)",
                (list(expected),),
            ).fetchall()
        }
        stored_entries: dict[str, list[tuple]] = {}
        for r in conn.execute(
            "SELECT accession_number, entry_index, sequence_number_raw, sequence_number, cik, name, "
            "form13f_file_number FROM institutional_13f_other_managers WHERE accession_number = ANY(%s) "
            "ORDER BY accession_number, entry_index",
            (list(expected),),
        ).fetchall():
            stored_entries.setdefault(r[0], []).append(tuple(r[1:]))
        bad = 0
        for acc, exp in expected.items():
            statuses[exp["status"]] += 1
            rec = stored.get(acc)
            if (
                rec is None
                or rec[1] != exp["status"]
                or tuple(rec[2:]) != exp["counts"]
                or stored_entries.get(acc, []) != exp["entries"]
            ):
                bad += 1
                if bad <= 5:
                    failures.append(f"{archive.name} capture {acc}: stored {rec} vs expected {exp}")
        if bad:
            failures.append(f"{archive.name}: {bad} capture records differ")

        seen_keys: set[tuple] = set()
        obs_bad = 0
        live_written = 0
        zero_share = 0
        stale_map = 0
        with conn.cursor(name=f"obs_{archive.stem.replace('-', '_')}") as cur:
            cur.itersize = 50_000
            cur.execute(
                "SELECT instrument_id, filer_cik, source_document_id, exposure_kind, shares, thirteen_f_segments, "
                "ingested_at "
                "FROM ownership_institutions_observations WHERE source = '13f' "
                "AND source_document_id = ANY(%s)",
                (list(expected),),
            )
            for iid, cik, acc, exposure, shares, segments, ingested_at in cur:
                observations_checked += 1
                with_segments += segments is not None and (t0 is None or ingested_at <= t0)
                key = (iid, cik, acc, exposure)
                seen_keys.add(key)
                slot = keyed.get(key)
                rec = stored.get(acc)
                matches = (
                    segments is not None
                    and slot is not None
                    and rec is not None
                    and segments.get("capture_id") == rec[0]
                    and segments.get("cusips") == sorted(slot["cusips"])
                    and segments.get("lines") == _expected_lines(slot)
                )
                consistent = matches and sum(Decimal(line["shares"]) for line in segments["lines"]) == shares
                ok = matches and consistent
                if matches and not consistent:
                    # The capture equals the archive under today's CUSIP map, but the observation's shares were
                    # summed under an older one (a CUSIP mapped to the instrument since). The read's
                    # self-consistency check (spec § Rule 3) refuses it; a re-ingest of the accession repairs it.
                    stale_map += 1
                    if stale_map <= 5:
                        notes.append(
                            f"stale vs CUSIP map: {archive.name} {key} shares={shares} cusips={segments['cusips']}"
                        )
                elif not ok and segments is None and shares is not None and shares <= 0:
                    # No writer line describes a non-positive position (the #1433 guard, and the normaliser's): a
                    # zero-share observation written before that guard has nothing to capture.
                    zero_share += 1
                elif not ok and t0 is not None and ingested_at > t0:
                    live_written += 1
                    if live_written <= 5:
                        notes.append(f"live ingest after T0: {archive.name} observation {key} shares={shares}")
                elif not ok:
                    obs_bad += 1
                    if obs_bad <= 5:
                        failures.append(f"{archive.name} observation {key} shares={shares}: {segments}")
        if obs_bad:
            failures.append(f"{archive.name}: {obs_bad} observations' segments differ")
        print(
            f"{archive.name}: accessions={len(expected)} reader_keys={len(keyed)} "
            f"observations={len(seen_keys)} reader_keys_without_observation={len(set(keyed) - seen_keys)} "
            f"capture_mismatches={bad} segment_mismatches={obs_bad} live_written_after_t0={live_written} "
            f"zero_share_residue={zero_share} stale_vs_cusip_map={stale_map}",
            flush=True,
        )

    # Every observation with segments and not rewritten after T0 was seen above, so none outside the archives'
    # accessions has any.
    row = conn.execute(
        "SELECT count(*) FROM ownership_institutions_observations "
        "WHERE thirteen_f_segments IS NOT NULL AND (%(t0)s::timestamptz IS NULL OR ingested_at <= %(t0)s)",
        {"t0": t0},
    ).fetchone()
    total = row[0] if row is not None else None
    if t0 is None:
        notes.append(f"{total} observations carry segments, {with_segments} of them in the archives")
    elif total != with_segments:
        failures.append(f"{total} observations carry segments, {with_segments} of them in the archives")
    print(f"status counts: {dict(statuses)}; observations checked: {observations_checked}", flush=True)


def _report(failures: list[str], notes: list[str]) -> int:
    for note in notes:
        print(f"NOTE {note}", flush=True)
    for failure in failures:
        print(f"FAIL {failure}", flush=True)
    print("PASS" if not failures else f"FAILED ({len(failures)})", flush=True)
    return 0 if not failures else 1


def run(archives: list[Path], *, backfill: bool) -> int:
    failures: list[str] = []
    notes: list[str] = []
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        conn.execute("SET statement_timeout = 0")
        t0 = None
        if backfill:
            row = conn.execute("SELECT clock_timestamp()").fetchone()
            assert row is not None
            t0 = row[0]
            conn.execute(_BEFORE_SQL)
            conn.execute("CREATE INDEX ON ab_before (k)")
            print(f"before-image taken at T0={t0.isoformat()}", flush=True)
            if backfill_main([str(a) for a in archives]) != 0:
                failures.append("backfill exited non-zero")
                return _report(failures, notes)
            row = conn.execute(_DIFF_SQL, {"t0": t0}).fetchone()
            assert row is not None
            deleted, inserted, live_updated, changed = row
            print(
                f"rows: deleted_after_t0={deleted} inserted_after_t0={inserted} rewritten_after_t0={live_updated} "
                f"changed_without_a_live_write={changed}",
                flush=True,
            )
            if changed:
                failures.append(f"{changed} rows changed a pre-existing column without a live write")
            if deleted or inserted or live_updated:
                notes.append(f"live ingest after T0: {deleted} deleted, {inserted} inserted, {live_updated} rewritten")
        conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        _capture_gates(conn, archives, t0, failures, notes)
        conn.execute("ROLLBACK")
    return _report(failures, notes)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("mode", choices=("run", "check"))
    parser.add_argument("archives", nargs="+", type=Path)
    args = parser.parse_args(argv)
    return run(args.archives, backfill=args.mode == "run")


if __name__ == "__main__":
    sys.exit(main())
