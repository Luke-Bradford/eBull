"""Build the #3361 security-linkage bundle from Form 3/4/5 data sets + #3360 submissions.

Spec: ``docs/proposals/ta/2026-09-24-3361-security-linkage.md`` ("Artefact"). Admission,
grammar and reading live in ``app/services/security_linkage.py``; this script snapshots
the inputs, walks the archives and publishes.

Publish protocol (as ``scripts/build_3360_pit_fundamentals.py``): ``mkdir <bundle>``
exclusively; every input copied or dumped into ``<bundle>/inputs/`` and hashed from the
copy, which is the only thing read; each series document written with fsync and the
no-replace link, then re-read through the loader's checks; the ledger, then the manifest,
written LAST and the directory fsynced. No manifest = not a bundle. A crashed build is
deleted, never resumed.

Usage::

    PYTHONPATH=. uv run python scripts/build_3361_security_linkage.py \\
        --pit-bundle ~/Library/Application\\ Support/eBull/research/pit_fundamentals_3360/<build> \\
        --pit-manifest-sha256 <#3360 manifest digest> \\
        --out <new bundle directory> [--bulk-dir DIR] [--without-form25]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import zipfile
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path, PurePosixPath
from typing import Any

from app.services.pit_fundamentals import SubmissionsIndex, parse_submissions
from app.services.r6_pit_bundle import read_verified_document
from app.services.security_linkage import (
    COVERAGE_START,
    FIRST_QUARTER,
    IN_SCOPE_VENDOR,
    LEDGER_FILENAME,
    MANIFEST_FILENAME,
    MANIFEST_SCHEMA,
    REQUIRED_COLUMNS,
    SERIES_DIRNAME,
    SERIES_SCHEMA,
    Admitted,
    RowOutcome,
    admit_observation,
    admit_row,
    canonical_cik,
    collisions,
    form25_order,
    load_series,
    match_set,
    observation_order,
    parse_vendor_symbol,
    policy_sha256,
)
from scripts.build_2900_pit_bundle import _write_exclusive

LEDGER_SCHEMA = "security-linkage-ledger-v1"
_QUARTER = re.compile(r"insider_(\d{4})q([1-4])\.zip")
_SUBMISSION_MEMBER = "SUBMISSION.tsv"

#: The DB-side inputs, each snapshotted as a sorted JSON row dump (spec rule 1 + "Artefact").
#: ``crosscheck_series`` and ``instrument_cik_history`` feed acceptance item 3 only.
DB_INPUTS = ("series_inventory", "form25", "crosscheck_series", "instrument_cik_history")


# --------------------------------------------------------------------------- DB snapshot


def snapshot_db(conn: Any) -> dict[str, Any]:
    """Read the DB inputs from ONE snapshot. Values are JSON-ready; sorting happens in :func:`build`.

    ⚠ ``conn`` must not be in autocommit and this must be its first statement: under READ
    COMMITTED an ingest committing between the SELECTs would tear the hash-bound inputs (a
    Form 25 row filed after the ``span`` read would sit outside its own observed span).
    """
    conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")

    def rows(sql: str) -> list[list[Any]]:
        return [[v.isoformat() if isinstance(v, date) else v for v in row] for row in conn.execute(sql).fetchall()]

    span = conn.execute("SELECT min(filed_date), max(filed_date) FROM sec_form25_register").fetchone()
    return {
        "series_inventory": rows(
            "SELECT series_id, vendor, vendor_symbol, first_bar, last_bar FROM research_price_series"
        ),
        "form25": {
            "rows": rows(
                "SELECT accession_number, filed_date, issuer_cik, resolved_symbol FROM sec_form25_register"
                " WHERE provision_class = 'equity_delisting'"
            ),
            "span": None if span[0] is None else [span[0].isoformat(), span[1].isoformat()],
        },
        "crosscheck_series": rows(
            "SELECT series_id, instrument_id, delisting_provision, delisting_filed_date FROM research_price_series"
        ),
        "instrument_cik_history": rows(
            "SELECT instrument_id, cik, effective_from, effective_to, source_event FROM instrument_cik_history"
        ),
    }


def _nulls_first(row: Sequence[Any]) -> tuple[Any, ...]:
    return tuple((value is not None, value) for value in row)


# --------------------------------------------------------------------------- build


def _snapshot(source: Path, destination: Path) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    with destination.open("rb") as handle:
        os.fsync(handle.fileno())
        handle.seek(0)
        return hashlib.file_digest(handle, "sha256").hexdigest()


def quarter_end(year: int, quarter: int) -> date:
    first_of_next = date(year + quarter // 4, quarter % 4 * 3 + 1, 1)
    return first_of_next - timedelta(days=1)


def _quarters(paths: Sequence[Path]) -> list[tuple[str, int, int, Path]]:
    """Rule 1: quarter files named once each, contiguous from 2006q1."""
    parsed = []
    for path in paths:
        match = _QUARTER.fullmatch(path.name)
        if match is None:
            raise RuntimeError(f"not a Form 3/4/5 quarter file: {path.name}")
        parsed.append((path.name, int(match.group(1)), int(match.group(2)), path))
    names = [p[0] for p in parsed]
    if len(set(names)) != len(names):
        raise RuntimeError("a quarter file appears twice")
    parsed.sort(key=lambda p: (p[1], p[2]))
    expected = FIRST_QUARTER
    for _, year, quarter, _ in parsed:
        if (year, quarter) != expected:
            raise RuntimeError(
                f"quarter files are not contiguous from 2006q1: expected {expected}, got {(year, quarter)}"
            )
        expected = (year + 1, 1) if quarter == 4 else (year, quarter + 1)
    if not parsed:
        raise RuntimeError("no Form 3/4/5 quarter files")
    return parsed


def _submission_text(archive: zipfile.ZipFile, label: str) -> tuple[list[str], dict[str, int]]:
    """Rule 1 + precision rule [6]: one member, strict UTF-8, no BOM, unique header."""
    names = archive.namelist()
    if len(names) != len(set(names)):
        raise RuntimeError(f"{label}: duplicate zip members")
    members = [n for n in names if PurePosixPath(n).name == _SUBMISSION_MEMBER]
    if members != [_SUBMISSION_MEMBER]:
        raise RuntimeError(f"{label}: expected exactly one top-level {_SUBMISSION_MEMBER}, found {members}")
    data = archive.read(_SUBMISSION_MEMBER)
    if data.startswith(b"\xef\xbb\xbf"):
        raise RuntimeError(f"{label}: byte-order mark")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RuntimeError(f"{label}: not strict UTF-8") from exc
    lines = text.split("\n")
    if text.endswith("\n"):
        lines.pop()
    header = lines[0].split("\t") if lines else []
    if len(set(header)) != len(header) or not set(REQUIRED_COLUMNS) <= set(header):
        raise RuntimeError(f"{label}: header repeats a name or lacks {REQUIRED_COLUMNS}")
    return lines, {name: i for i, name in enumerate(header)}


def _dump(inputs: Path, name: str, value: Any) -> tuple[str, Any]:
    """Write a DB dump, then return (sha, value) from the COPY -- the only thing read."""
    path = inputs / f"{name}.json"
    _write_exclusive(path, value)
    digest, data = read_verified_document(path)
    return digest, json.loads(data)


def build(
    *,
    quarters: Sequence[Path],
    pit_bundle: Path,
    pit_manifest_sha256: str,
    db: Mapping[str, Any],
    out: Path,
    form25: bool = True,
) -> dict[str, Any]:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.mkdir()  # exclusive: an existing bundle directory is refused, never resumed
    try:
        return _build(quarters, pit_bundle, pit_manifest_sha256, db, out, form25)
    except BaseException:
        # ``out`` was created by the exclusive mkdir above, so it holds only this build's output.
        shutil.rmtree(out, ignore_errors=True)
        raise


def _build(
    quarter_paths: Sequence[Path],
    pit_bundle: Path,
    pit_manifest_sha256: str,
    db: Mapping[str, Any],
    out: Path,
    form25_mode: bool,
) -> dict[str, Any]:
    inputs = out / "inputs"
    quarters = _quarters(quarter_paths)
    input_sha: dict[str, Any] = {"quarters": {}}
    for name, _, _, path in quarters:
        input_sha["quarters"][name] = _snapshot(path, inputs / "form345" / name)

    # #3360 pin: its manifest digest, and its submissions.zip byte-identical to the one it hashed.
    pit_digest, pit_data = read_verified_document(pit_bundle / "manifest.json")
    if pit_digest != pit_manifest_sha256:
        raise RuntimeError(f"#3360 manifest digest {pit_digest} != pinned {pit_manifest_sha256}")
    pit_manifest = json.loads(pit_data)
    input_sha["pit_manifest"] = pit_digest
    input_sha["submissions"] = _snapshot(pit_bundle / "inputs" / "submissions.zip", inputs / "submissions.zip")
    if input_sha["submissions"] != pit_manifest["input_sha256"]["submissions"]:
        raise RuntimeError("#3360 submissions.zip does not match its manifest")

    dumps: dict[str, Any] = {}
    for name in DB_INPUTS:
        if name == "form25" and not form25_mode:
            continue  # cross-check (b) mode: the Form 25 input is absent, not empty
        value = db[name]
        if name == "form25":
            value = {"rows": sorted(value["rows"], key=_nulls_first), "span": value["span"]}
        else:
            value = sorted(value, key=_nulls_first)
        input_sha[name], dumps[name] = _dump(inputs, name, value)

    series = _validate_inventory(dumps["series_inventory"])
    register = _validate_form25(dumps["form25"]) if form25_mode else {"rows": [], "span": None}

    capture_end: dict[str, str] = {}
    for row in series:
        capture_end[row["vendor"]] = max(capture_end.get(row["vendor"], ""), row["last_bar"])
    last_name, last_year, last_quarter, _ = quarters[-1]
    supported_through = min(
        quarter_end(last_year, last_quarter),
        date.fromisoformat(pit_manifest["supported_through"]),
        date.fromisoformat(capture_end[IN_SCOPE_VENDOR]),
    )

    grammars = {row["series_id"]: parse_vendor_symbol(row["vendor_symbol"]) for row in series if row["in_scope"]}
    match_sets = {sid: ms for sid, g in grammars.items() if (ms := match_set(g))}
    collided = collisions(match_sets)
    wanted = frozenset().union(*match_sets.values())

    with zipfile.ZipFile(inputs / "submissions.zip") as subs_zip:
        ledger, stored = _admit_all([(name, inputs / "form345" / name) for name, _, _, _ in quarters], subs_zip, wanted)

    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for obs in stored:
        by_symbol[obs["symbol"]].append(obs)
    form25_by_cik: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in register["rows"]:
        form25_by_cik[row["issuer_cik"]].append(row)

    entries: list[dict[str, Any]] = []
    for row in series:
        entry: dict[str, Any] = {"series_id": row["series_id"], "vendor": row["vendor"], "path": None, "sha256": None}
        if row["in_scope"]:
            grammar = grammars[row["series_id"]]
            symbols = match_sets.get(row["series_id"], frozenset())
            observations = sorted((o for s in symbols for o in by_symbol.get(s, [])), key=observation_order)
            ciks = {o["cik"] for o in observations}
            document = {
                "schema": SERIES_SCHEMA,
                "series_id": row["series_id"],
                "vendor_symbol": row["vendor_symbol"],
                "first_bar": row["first_bar"],
                "last_bar": row["last_bar"],
                "grammar": grammar.to_json(),
                "collision": row["series_id"] in collided,
                "observations": observations,
                "form25": sorted((r for c in sorted(ciks) for r in form25_by_cik.get(c, [])), key=form25_order),
            }
            relative = f"{SERIES_DIRNAME}/{row['series_id']}.json"
            _write_exclusive(out / relative, document)
            digest, _ = read_verified_document(out / relative)
            entry.update(path=relative, sha256=digest)
            load_series(out / relative, entry)  # re-read through the loader's own checks
        entries.append(entry)

    ledger_document = {"schema": LEDGER_SCHEMA, **ledger}
    _write_exclusive(out / LEDGER_FILENAME, ledger_document)
    ledger_digest, _ = read_verified_document(out / LEDGER_FILENAME)
    manifest = {
        "schema": MANIFEST_SCHEMA,
        "policy": policy_sha256(form25=form25_mode),
        "form25_mode": form25_mode,
        "input_sha256": input_sha,
        "supported_through": supported_through.isoformat(),
        "coverage_start": COVERAGE_START.isoformat(),
        "capture_end": dict(sorted(capture_end.items())),
        "last_quarter": last_name,
        "form25_span": register["span"],
        "collisions": sorted(collided),
        "ledger": {"path": LEDGER_FILENAME, "sha256": ledger_digest},
        "series": entries,
    }
    _write_exclusive(out / MANIFEST_FILENAME, manifest)
    descriptor = os.open(out, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return manifest


def _validate_inventory(rows: Sequence[Sequence[Any]]) -> list[dict[str, Any]]:
    """Rule 1 + precision rules [10, 11]."""
    series: list[dict[str, Any]] = []
    for series_id, vendor, vendor_symbol, first_bar, last_bar in rows:
        if type(series_id) is not int or series_id <= 0:
            raise RuntimeError(f"series_id must be a positive integer: {series_id!r}")
        if not isinstance(vendor_symbol, str) or not vendor_symbol.strip():
            raise RuntimeError(f"series {series_id}: empty vendor symbol")
        if first_bar is None or last_bar is None or date.fromisoformat(first_bar) > date.fromisoformat(last_bar):
            raise RuntimeError(f"series {series_id}: bounds missing or reversed")
        series.append(
            {
                "series_id": series_id,
                "vendor": vendor,
                "vendor_symbol": vendor_symbol,
                "first_bar": first_bar,
                "last_bar": last_bar,
                "in_scope": vendor == IN_SCOPE_VENDOR,
            }
        )
    ids = [s["series_id"] for s in series]
    if len(set(ids)) != len(ids):
        raise RuntimeError("series_id repeats")
    if not any(s["in_scope"] for s in series):
        raise RuntimeError(f"no {IN_SCOPE_VENDOR} series in the inventory")
    series.sort(key=lambda s: s["series_id"])
    return series


def _validate_form25(dump: Mapping[str, Any]) -> dict[str, Any]:
    """Precision rules [7, 8, 9]: canonical issuer CIK, a filed date, span from the register."""
    rows = []
    for accession, filed_date, issuer_cik, resolved_symbol in dump["rows"]:
        cik = canonical_cik(issuer_cik) if isinstance(issuer_cik, str) else None
        if cik is None or filed_date is None:
            raise RuntimeError(f"Form 25 {accession}: malformed issuer_cik or NULL filed_date")
        date.fromisoformat(filed_date)
        rows.append(
            {"accession": accession, "filed_date": filed_date, "issuer_cik": cik, "resolved_symbol": resolved_symbol}
        )
    return {"rows": rows, "span": dump["span"] if rows else None}


def _admit_all(
    quarters: Sequence[tuple[str, Path]], subs_zip: zipfile.ZipFile, wanted: frozenset[str]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Rule 2 over every row. Returns the ledger and the stored observations whose symbol
    some in-scope series can match (every other stored observation is counted only)."""
    rows_in: dict[str, int] = {}
    outcomes: dict[str, Counter[str]] = {}
    locators: dict[str, list[list[Any]]] = defaultdict(list)
    # accession -> [Admitted.key, (quarter, index), ...]; a second distinct key moves it to ``conflicted``.
    groups: dict[str, list[Any]] = {}
    conflicted: dict[str, list[tuple[str, int]]] = {}

    for name, path in quarters:
        outcomes[name] = Counter()
        with zipfile.ZipFile(path) as archive:
            lines, columns = _submission_text(archive, name)
        rows_in[name] = len(lines) - 1
        for index, line in enumerate(lines[1:]):
            fields = line.split("\t")
            admitted = admit_row(fields, columns)
            if not isinstance(admitted, Admitted):
                outcomes[name][admitted.value] += 1
                raw = [fields[columns["ISSUERCIK"]]] if admitted is RowOutcome.MALFORMED_CIK else []
                locators[admitted.value].append([name, index, *raw])
                continue
            locator = (sys.intern(name), index)
            if admitted.accession in conflicted:
                conflicted[admitted.accession].append(locator)
                continue
            group = groups.get(admitted.accession)
            key = tuple(sys.intern(v) for v in admitted.key)
            if group is None:
                groups[admitted.accession] = [key, locator]
            elif group[0] == key:
                group.append(locator)
            else:
                conflicted[admitted.accession] = [*group[1:], locator]
                del groups[admitted.accession]

    for accession in sorted(conflicted):
        for quarter, index in conflicted[accession]:
            outcomes[quarter][RowOutcome.ACCESSION_CONFLICT.value] += 1
            locators[RowOutcome.ACCESSION_CONFLICT.value].append([quarter, index])

    by_cik: dict[str, list[str]] = defaultdict(list)
    for accession, group in groups.items():
        by_cik[group[0][0]].append(accession)
    sub_names = set(subs_zip.namelist())

    def read_page(page: str) -> object | None:
        return json.loads(subs_zip.read(page)) if page in sub_names else None

    integrity: dict[str, str] = {}
    observation_outcomes: Counter[str] = Counter()
    stored: list[dict[str, Any]] = []
    for cik in sorted(by_cik):
        member = f"CIK{cik}.json"
        issuer_index: SubmissionsIndex | str = (
            parse_submissions(cik, json.loads(subs_zip.read(member)), read_page)
            if member in sub_names
            else "no_submissions_entry"
        )
        if isinstance(issuer_index, str):
            integrity[cik] = issuer_index
        for accession in by_cik[cik]:
            group = groups[accession]
            (_, symbol, document_type), located = group[0], group[1:]
            outcome, acceptance = admit_observation(accession, issuer_index)
            observation_outcomes[outcome.value] += 1
            for quarter, row_index in located:
                outcomes[quarter][outcome.value] += 1
                if outcome is not RowOutcome.STORED:
                    locators[outcome.value].append([quarter, row_index])
            if outcome is RowOutcome.STORED and symbol in wanted:
                stored.append(
                    {
                        "acceptance": acceptance,
                        "accession": accession,
                        "cik": cik,
                        "symbol": symbol,
                        "document_type": document_type,
                        "multiplicity": len(located),
                    }
                )

    for name, total in rows_in.items():
        if sum(outcomes[name].values()) != total:
            raise RuntimeError(f"ledger does not reconcile for {name}: {total} rows in, {dict(outcomes[name])}")
    ledger = {
        "rows_in": rows_in,
        "row_outcomes": {name: dict(sorted(counts.items())) for name, counts in outcomes.items()},
        "observation_outcomes": dict(sorted(observation_outcomes.items())),
        "issuer_integrity_failures": dict(sorted(integrity.items())),
        "locators": {outcome: sorted(rows) for outcome, rows in sorted(locators.items())},
    }
    return ledger, stored


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bulk-dir", type=Path, help="default: the bulk downloader's <data dir>/sec/bulk")
    parser.add_argument("--pit-bundle", type=Path, required=True)
    parser.add_argument("--pit-manifest-sha256", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--without-form25", action="store_true", help="cross-check (b) mode")
    args = parser.parse_args()

    import psycopg

    from app.config import settings
    from app.security.master_key import resolve_data_dir

    bulk = args.bulk_dir or resolve_data_dir() / "sec" / "bulk"
    with psycopg.connect(settings.database_url) as conn:
        db = snapshot_db(conn)
    manifest = build(
        quarters=sorted(bulk.glob("insider_*.zip")),
        pit_bundle=args.pit_bundle,
        pit_manifest_sha256=args.pit_manifest_sha256,
        db=db,
        out=args.out,
        form25=not args.without_form25,
    )
    with (args.out / MANIFEST_FILENAME).open("rb") as handle:
        manifest_sha = hashlib.file_digest(handle, "sha256").hexdigest()
    ledger = json.loads((args.out / LEDGER_FILENAME).read_bytes())
    totals: Counter[str] = Counter()
    for counts in ledger["row_outcomes"].values():
        totals.update(counts)
    json.dump(
        {
            "manifest_sha256": manifest_sha,
            "supported_through": manifest["supported_through"],
            "form25_mode": manifest["form25_mode"],
            "series": len(manifest["series"]),
            "in_scope_series": sum(1 for e in manifest["series"] if e["path"]),
            "collisions": len(manifest["collisions"]),
            "rows_in": sum(ledger["rows_in"].values()),
            "row_outcomes": dict(sorted(totals.items())),
            "observation_outcomes": ledger["observation_outcomes"],
            "issuer_integrity_failures": Counter(ledger["issuer_integrity_failures"].values()),
        },
        sys.stdout,
        indent=1,
        sort_keys=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
