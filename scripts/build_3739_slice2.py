"""#3739 slice 2a: K and G calibration on pre-stage data, and U pass 1. §3.2's candidate screens are slice 2b.

Spec: ``docs/research/2026-10-09-3739-stage-c-panel.md`` §2 (U, K), §9 (G). Nothing here decodes a stage-C price:
the calibrations read the published stage-A and stage-B panel artefacts (formations to 2024-07-31), and U reads SEC
structured data only.

``calibrate`` writes K's pair table and the G table from the two artefacts, each verified against its manifest
digest before a row is read.

``extract`` reads EDGAR's ``submissions.zip`` (every filer's filing index, main file and overflow pages) and writes the
filings §2 and §3.2 read: the entrant forms and every 8-K and 8-K/A, accepted on or after ``EXTRACT_FROM``.

``register`` writes the Form 25 register events filed from 2024-08-01 through the harvest date.

``universe`` writes U pass 1 (§2 items 1-3) from the stage-B artefact's base formation, the calibrated K, the
extract and the register extract.

    PYTHONPATH=. uv run python scripts/build_3739_slice2.py calibrate --out <calibration.json>
    PYTHONPATH=. uv run python scripts/build_3739_slice2.py extract --zip <submissions.zip> --out <extract.jsonl.gz>
    PYTHONPATH=. uv run python scripts/build_3739_slice2.py register --through <harvest date> --out <register.csv> \\
        --view-sha256 <d>
    PYTHONPATH=. uv run python scripts/build_3739_slice2.py universe --calibration <calibration.json> \\
        --calibration-sha256 <d> --extract <extract.jsonl.gz> --extract-sha256 <d> \\
        --register <register.csv> --register-sha256 <d> --out <u-pass1.csv>
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import gzip
import hashlib
import io
import json
import math
import re
import zipfile
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Final

import psycopg

from app.config import settings
from app.services.factor_book import UNIVERSE_SIZE
from app.services.factor_panel_artefact import read_gz_lines, write_gz_lines
from app.services.pit_fundamentals import acceptance_ny_date
from app.services.sec_form25_register import normalise_cik
from scripts.build_3609_factor_panel import (
    PUBLISH_ROOT,
    STAGE_A_PINS,
    read_verified_artefact,
    stage_b_pins,
)

#: The published panels the calibrations read (stage A republished under Amendment 3 by #3730; stage B as #3621 v2
#: read it). ME does not depend on the clip, so either stage-B publication gives the same final ME.
STAGE_A: Final = (
    PUBLISH_ROOT / "2026-10-09-af7889cc-stageA",
    "e0924564ef1639f9473586523679894eb5d1741a75a3a3b8ddce5858917cae6e",
    STAGE_A_PINS,
)
STAGE_B: Final = (
    PUBLISH_ROOT / "2026-10-09-25577cff-stageB-1c5de22cccda4937b55eb7e16e505b7f",
    "83422f50c09d2c9c2b9613d12804a1f73187824cd5200e9ae2c75a439c288592",
    stage_b_pins("d0f3f9a90f4c0fb49a51266ad55f9f7b3a8d7857338cb2661ad7e0545452f8dd"),
)
#: §2: the pass-1 base formation, the last pre-stage formation.
BASE_FORMATION: Final = date(2024, 7, 31)
#: §2: K is the smallest of these with the outside-K share at or below the limit at every pair; else the largest.
K_CANDIDATES: Final = (1500, 2000, 2500, 3000)
K_SHARE_LIMIT: Final = 0.0025
MAX_HORIZON: Final = 24
#: §9: G(g) is this quantile of max(r, 1/r); g runs over the gaps a stage-C size can need (formations
#: 2024-07-31 .. 2026-07-31 are 25, so at most 24 apart).
G_QUANTILE: Final = 0.999
G_GAPS: Final = tuple(range(1, 25))


@dataclass(frozen=True)
class Security:
    """An admitted security at one formation: the panel's name key, its issuer and its final ME."""

    name_key: int
    cik: str
    me: float


Formation = dict[int, Security]


def admitted(rows: Iterable[Mapping[str, Any]]) -> dict[date, Formation]:
    """Admitted rows (no exclusion) by formation, keyed by name key. An admitted row without a finite positive ME
    or a CIK refuses: step 1 admits neither."""
    out: dict[date, Formation] = defaultdict(dict)
    for row in rows:
        if row["exclusion"] is not None:
            continue
        me = float(row["me"]["value"])
        if not (math.isfinite(me) and me > 0) or not row.get("cik"):
            raise ValueError(f"admitted row {row['M']} {row['name_key']} has no usable ME or CIK")
        month = date.fromisoformat(row["M"])
        if row["name_key"] in out[month]:
            raise ValueError(f"name key {row['name_key']} repeats at {month}")
        out[month][row["name_key"]] = Security(row["name_key"], row["cik"], me)
    return dict(out)


def ranked(formation: Formation) -> list[Security]:
    """Admitted securities by final ME, descending, ties by name key (``factor_book.universe``'s order)."""
    return sorted(formation.values(), key=lambda s: (-s.me, s.name_key))


def issuer_ranks(formation: Formation) -> dict[str, int]:
    """§2 item 1: an issuer's rank is its largest admitted security's 1-based rank among all admitted securities."""
    ranks: dict[str, int] = {}
    for position, security in enumerate(ranked(formation), start=1):
        ranks.setdefault(security.cik, position)
    return ranks


def outside_k_shares(start: Formation, end: Formation, ks: Sequence[int]) -> tuple[float, dict[int, float]]:
    """§2's K measure for one pair: of the top 1,000's final ME at the later formation, held by issuers present at
    the earlier one, the share held by issuers ranked > K there. Returns the denominator and the share per K."""
    ranks = issuer_ranks(start)
    top = [s for s in ranked(end)[:UNIVERSE_SIZE] if s.cik in ranks]
    total = sum(s.me for s in top)
    return total, {k: sum(s.me for s in top if ranks[s.cik] > k) / total for k in ks}


def k_pairs(panel: Mapping[date, Formation], first: date, last: date) -> Iterator[dict[str, Any]]:
    """Every pair (M0, M0 + h) with M0 in [first, last], 1 <= h <= 24 and M0 + h <= last, in formation order."""
    months = sorted(m for m in panel if first <= m <= last)
    for i, start in enumerate(months):
        for h in range(1, MAX_HORIZON + 1):
            if i + h >= len(months):
                break
            total, shares = outside_k_shares(panel[start], panel[months[i + h]], K_CANDIDATES)
            yield {
                "M0": start.isoformat(),
                "M1": months[i + h].isoformat(),
                "h": h,
                "denominator": total,
                "shares": {str(k): v for k, v in shares.items()},
            }


def choose_k(pairs: Sequence[Mapping[str, Any]]) -> tuple[int, dict[int, float]]:
    """The smallest K whose maximum share over all pairs is at or below the limit, else the largest candidate."""
    worst = {k: max(p["shares"][str(k)] for p in pairs) for k in K_CANDIDATES}
    return next((k for k in K_CANDIDATES if worst[k] <= K_SHARE_LIMIT), K_CANDIDATES[-1]), worst


def quantile(values: Sequence[float], q: float) -> float:
    """Linear interpolation between closest ranks (Postgres ``percentile_cont``)."""
    ordered = sorted(values)
    position = q * (len(ordered) - 1)
    low = math.floor(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def g_table(panel: Mapping[date, Formation], gaps: Sequence[int]) -> dict[int, dict[str, float]]:
    """§9's G: for each gap g, the quantile of max(r, 1/r) over every security admitted at both M and M + g, r the
    ratio of its final ME. A terminated security contributes the pairs ending at its last admitted formation."""
    months = sorted(panel)
    out: dict[int, dict[str, float]] = {}
    for g in gaps:
        values = [
            max(panel[later][key].me / security.me, security.me / panel[later][key].me)
            for i, later in enumerate(months[g:], start=g)
            for key, security in panel[months[i - g]].items()
            if key in panel[later]
        ]
        out[g] = {"pairs": len(values), "G": quantile(values, G_QUANTILE)}
    return out


# --------------------------------------------------------------------------- U pass 1

#: §2 item 2: registration of a class on an exchange (8-A12B, 10-12B), a successor's registration (8-K12B), or an
#: 8-K reporting a change in shell company status.
ENTRANT_FORMS: Final = frozenset({"8-A12B", "10-12B", "8-K12B"})
SHELL_STATUS_ITEM: Final = "5.06"
ENTRANT_LOOKBACK: Final = timedelta(days=92)
#: F + 24 months.
ENTRANT_LAST: Final = date(2026, 7, 31)
#: The extract keeps these forms; 8-K and 8-K/A also feed §3.2's Item 5.03 screen.
EXTRACT_FORMS: Final = ENTRANT_FORMS | {"8-K", "8-K/A"}
EXTRACT_FROM: Final = date(2024, 4, 1)
_CIK: Final = re.compile(r"\d{10}")
_SUBMISSION_NAME: Final = re.compile(r"CIK(\d{10})(?:-submissions-\d+)?\.json")


@dataclass(frozen=True)
class Filing:
    cik: str
    accession: str
    form: str
    #: ``acceptanceDateTime`` as EDGAR states it (UTC).
    accepted: str
    items: tuple[str, ...]

    @property
    def accepted_date(self) -> date:
        """The New York date of acceptance (step 1's ``acceptance_ny_date``)."""
        return acceptance_ny_date(self.accepted)


def submission_filings(cik: str, payload: Mapping[str, Any]) -> Iterator[Filing]:
    """The filings of one ``submissions.zip`` member: a main file holds them under ``filings.recent``, an overflow
    page at the top level. Columns are index-aligned; ``items`` is a comma-separated string."""
    columns = payload["filings"]["recent"] if "filings" in payload else payload
    for i, form in enumerate(columns.get("form", [])):
        raw_items = columns["items"][i] if "items" in columns else ""
        yield Filing(
            cik=cik,
            accession=columns["accessionNumber"][i],
            form=form,
            accepted=columns["acceptanceDateTime"][i],
            items=tuple(item.strip() for item in raw_items.split(",") if item.strip()),
        )


def extract_filings(archive: zipfile.ZipFile) -> Iterator[Filing]:
    """Every filing of ``EXTRACT_FORMS`` accepted on or after ``EXTRACT_FROM``, over all members. A member whose name
    is not a CIK file refuses, so a format change cannot silently drop filers; EDGAR's 16-byte
    ``placeholder.txt`` is the one known non-CIK member."""
    for name in sorted(archive.namelist()):
        if name == "placeholder.txt":
            continue
        match = _SUBMISSION_NAME.fullmatch(name)
        if match is None:
            raise ValueError(f"unexpected submissions.zip member {name!r}")
        for filing in submission_filings(match.group(1), json.loads(archive.read(name))):
            if filing.form in EXTRACT_FORMS and filing.accepted_date >= EXTRACT_FROM:
                yield filing


def is_entrant_filing(filing: Filing, base: date) -> bool:
    """§2 item 2's filing test, accepted in (F - 92 days, F + 24 months]."""
    if not base - ENTRANT_LOOKBACK < filing.accepted_date <= ENTRANT_LAST:
        return False
    return filing.form in ENTRANT_FORMS or (filing.form == "8-K" and SHELL_STATUS_ITEM in filing.items)


def pass1(
    base: Formation, base_date: date, k: int, filings: Iterable[Filing], register: Iterable[tuple[str, str]]
) -> dict[str, dict[str, list[str]]]:
    """U pass 1: CIK -> {basis: evidence}. ``incumbent`` evidence is the issuer's rank at F; ``entrant`` the
    qualifying accessions; ``terminated`` the register events' accessions. ``register`` is (issuer CIK, accession)
    for every register event in §9's population. Every CIK must be EDGAR's 10-digit form, as the panel, the
    extract and the register store it; another spelling would split one issuer into two entries, so it refuses."""
    u: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    ranks = issuer_ranks(base)
    filings = list(filings)
    register = list(register)
    spellings = set(ranks) | {f.cik for f in filings} | {cik for cik, _ in register}
    bad = sorted(cik for cik in spellings if not _CIK.fullmatch(cik))
    if bad:
        raise ValueError(f"{len(bad)} CIKs are not 10-digit: {bad[:5]}")
    for cik, rank in ranks.items():
        if rank <= k:
            u[cik]["incumbent"].append(str(rank))
    for filing in filings:
        if filing.cik not in ranks and is_entrant_filing(filing, base_date):
            u[filing.cik]["entrant"].append(filing.accession)
    for cik, accession in register:
        u[cik]["terminated"].append(accession)
    return {cik: {basis: sorted(evidence) for basis, evidence in bases.items()} for cik, bases in sorted(u.items())}


#: §9: register events are the common-equity delistings filed from the stage start (the view applies the register's
#: cohort rules, including the issuer-filed paragraph (c) exclusion).
REGISTER_SQL: Final = """
SELECT issuer_cik, accession_number, form, filed_date, rule_provision
  FROM sec_form25_common_equity_delistings
 WHERE filed_date BETWEEN %(start)s AND %(through)s
 ORDER BY issuer_cik, accession_number
"""
REGISTER_START: Final = date(2024, 8, 1)
VIEW_DEFINITION_SQL: Final = "SELECT pg_get_viewdef('sec_form25_common_equity_delistings'::regclass, true)"
REGISTER_COLUMNS: Final = ("issuer_cik", "accession_number", "form", "filed_date", "rule_provision")


# --------------------------------------------------------------------------- io


def read_panel(artefact: tuple[Path, str, Mapping[str, str]]) -> tuple[dict[date, Formation], dict[str, Any]]:
    path, digest, pins = artefact
    verified = read_verified_artefact(path, digest, pins)
    rows = verified.files[verified.manifest["rows"]["path"]]
    with gzip.GzipFile(fileobj=io.BytesIO(rows)) as handle:
        panel = admitted(json.loads(line) for line in handle)
    return panel, verified.manifest


def calibrate(out: Path) -> None:
    stage_a, manifest_a = read_panel(STAGE_A)
    stage_b, manifest_b = read_panel(STAGE_B)
    if set(stage_a) & set(stage_b):
        raise ValueError("the stage-A and stage-B artefacts share a formation")
    first_b = min(stage_b)
    if max(stage_b) != BASE_FORMATION:
        raise ValueError(f"stage B ends at {max(stage_b)}, not the base formation {BASE_FORMATION}")
    pairs = list(k_pairs(stage_b, first_b, BASE_FORMATION))
    k, worst = choose_k(pairs)
    panel = {**stage_a, **stage_b}
    result = {
        "spec": "docs/research/2026-10-09-3739-stage-c-panel.md §2 (K), §9 (G)",
        "artefacts": {
            "stage_a": {"path": STAGE_A[0].name, "manifest_sha256": STAGE_A[1], "run_id": manifest_a.get("run_id")},
            "stage_b": {"path": STAGE_B[0].name, "manifest_sha256": STAGE_B[1], "run_id": manifest_b.get("run_id")},
        },
        "K": {
            "chosen": k,
            "limit": K_SHARE_LIMIT,
            "max_share_by_k": {str(c): v for c, v in worst.items()},
            "pair_count": len(pairs),
            "pairs": pairs,
        },
        "G": {
            "quantile": G_QUANTILE,
            "formations": [min(panel).isoformat(), max(panel).isoformat()],
            "by_gap": {str(g): v for g, v in g_table(panel, G_GAPS).items()},
        },
    }
    out.write_text(json.dumps(result, indent=1, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "K": k,
                "max_share_by_k": worst,
                "pairs": len(pairs),
                "G": {g: round(v["G"], 4) for g, v in result["G"]["by_gap"].items()},
            },
            indent=1,
        )
    )


def sha256_of(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def checked(path: Path, digest: str) -> Path:
    if sha256_of(path) != digest:
        raise ValueError(f"{path} does not match its pinned sha256")
    return path


def extract(archive: Path, out: Path) -> None:
    with zipfile.ZipFile(archive) as zf:
        count = write_gz_lines(out, (dataclasses.asdict(filing) for filing in extract_filings(zf)))
    print(json.dumps({"zip_sha256": sha256_of(archive), "rows": count, "extract_sha256": sha256_of(out)}))


def read_extract(path: Path) -> Iterator[Filing]:
    for row in read_gz_lines(path):
        yield Filing(**{**row, "items": tuple(row["items"])})


def register_extract(through: date, out: Path, expected_view_sha256: str) -> None:
    """The register events filed from the stage start through ``through`` (the harvest date), as a CSV that U
    reads instead of the live view, so a later harvest cannot move a frozen U. It refuses unless the view's
    definition, as the database holds it, has the expected sha256 (``VIEW_DEFINITION_SQL``; sql/252). Every check
    runs before the file is opened, and both reads share one read-only transaction."""
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        definition = conn.execute(VIEW_DEFINITION_SQL).fetchone()
        if definition is None or not definition[0]:
            raise ValueError("the register view has no definition")
        view_sha256 = hashlib.sha256(definition[0].encode()).hexdigest()
        if view_sha256 != expected_view_sha256:
            raise ValueError(f"the register view's definition is {view_sha256}, not the pinned {expected_view_sha256}")
        rows = conn.execute(REGISTER_SQL, {"start": REGISTER_START, "through": through}).fetchall()
    lines: list[list[str]] = []
    for cik, accession, form, filed, provision in rows:
        normalised = normalise_cik(cik)
        if normalised is None:
            raise ValueError(f"register event {accession} has an unusable issuer CIK {cik!r}")
        lines.append([normalised, accession, form, filed.isoformat(), provision])
    with out.open("x", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(REGISTER_COLUMNS)
        writer.writerows(lines)
    print(
        json.dumps(
            {
                "events": len(rows),
                "through": through.isoformat(),
                "register_sha256": sha256_of(out),
                "view_definition_sha256": view_sha256,
            }
        )
    )


def read_register(path: Path) -> list[tuple[str, str]]:
    with path.open(newline="") as handle:
        return [(row["issuer_cik"], row["accession_number"]) for row in csv.DictReader(handle)]


def calibrated_k(calibration: Mapping[str, Any]) -> int:
    """K from a calibration file, after checking that the artefact digests it records are this script's pins and
    that its recorded choice is what ``choose_k`` gives on its own pairs. This is a consistency check, not proof of
    derivation: the file's content is guarded by its pinned sha256 (``checked``), and ``calibrate`` rebuilds it."""
    pinned = {"stage_a": STAGE_A[1], "stage_b": STAGE_B[1]}
    recorded = {stage: calibration["artefacts"][stage]["manifest_sha256"] for stage in pinned}
    if recorded != pinned:
        raise ValueError(f"the calibration was computed from other artefacts: {recorded}")
    k = calibration["K"]["chosen"]
    if k != choose_k(calibration["K"]["pairs"])[0]:
        raise ValueError(f"the calibration's K {k} is not the one its pairs give")
    return k


def universe(calibration: Path, extract_path: Path, register_path: Path, out: Path) -> None:
    k = calibrated_k(json.loads(calibration.read_text()))
    stage_b, _ = read_panel(STAGE_B)
    register = read_register(register_path)
    u = pass1(stage_b[BASE_FORMATION], BASE_FORMATION, k, read_extract(extract_path), register)
    with out.open("x", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["cik", "incumbent_rank", "entrant_accessions", "register_accessions"])
        for cik, bases in u.items():
            writer.writerow(
                [
                    cik,
                    ";".join(bases.get("incumbent", [])),
                    ";".join(bases.get("entrant", [])),
                    ";".join(bases.get("terminated", [])),
                ]
            )
    counts = {basis: sum(1 for b in u.values() if basis in b) for basis in ("incumbent", "entrant", "terminated")}
    print(
        json.dumps(
            {
                "K": k,
                "issuers": len(u),
                "by_basis": counts,
                "register_events": len(register),
                "u_sha256": sha256_of(out),
            }
        )
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="#3739 slice 2: calibration, U pass 1, screens")
    sub = parser.add_subparsers(dest="command", required=True)
    cal = sub.add_parser("calibrate")
    cal.add_argument("--out", type=Path, required=True)
    ext = sub.add_parser("extract")
    ext.add_argument("--zip", type=Path, required=True)
    ext.add_argument("--out", type=Path, required=True)
    reg = sub.add_parser("register")
    reg.add_argument("--through", type=date.fromisoformat, required=True)
    reg.add_argument("--out", type=Path, required=True)
    reg.add_argument("--view-sha256", required=True, help="the digest the live view's definition must have")
    uni = sub.add_parser("universe")
    uni.add_argument("--calibration", type=Path, required=True)
    uni.add_argument("--calibration-sha256", required=True)
    uni.add_argument("--extract", type=Path, required=True)
    uni.add_argument("--extract-sha256", required=True)
    uni.add_argument("--register", type=Path, required=True)
    uni.add_argument("--register-sha256", required=True)
    uni.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "calibrate":
        calibrate(args.out)
    elif args.command == "extract":
        extract(args.zip, args.out)
    elif args.command == "register":
        register_extract(args.through, args.out, args.view_sha256)
    else:
        universe(
            checked(args.calibration, args.calibration_sha256),
            checked(args.extract, args.extract_sha256),
            checked(args.register, args.register_sha256),
            args.out,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
