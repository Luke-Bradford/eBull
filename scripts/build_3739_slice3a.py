"""#3739 slice 3a: the §3.4 replica's U pass 1 and split candidate lists, pinned before any replica adjudication.

Spec: ``docs/research/2026-10-09-3739-stage-c-panel.md`` §3.4. The screens and protocol run on a replica: §2's pass 1
items 1-2 with base formation F = 2022-07-31, events effective 2022-08-01 .. 2024-07-31 (on a first fail, once more
with F = 2020-07-31, events 2020-08-01 .. 2022-07-31). The replica measures split screens only, so it pins no symbol
or termination list and reads no insider data. The screens are slice 2b's code with the replica's window; slice 2a's
calibrated K is reused, because K is fixed by construction once (§2), not per base formation.

``extract`` writes the filing extract (slice 2a's) from the replica's ``extract_from``.

``notes-reduce`` keeps one Financial Statement and Notes archive's §3.2 facts, so archives can be downloaded,
reduced and deleted one at a time; ``notes-extract`` joins the reduced files, refuses overlapping archives, and adds
each filing's EDGAR acceptance, as slice 2b's ``notes-extract`` does from the archives themselves.

``universe`` writes the replica's U pass 1 (§2 items 1-2) from formation F of the stage-B artefact (stage A's for
the 2020 fallback).

``screens`` writes the replica's split candidate list.

    PYTHONPATH=. uv run python scripts/build_3739_slice3a.py extract --replica 2022 --zip <submissions.zip> \\
        --out <extract.jsonl.gz>
    PYTHONPATH=. uv run python scripts/build_3739_slice3a.py notes-reduce --out <reduced.jsonl.gz> <fsnds_*_notes.zip>
    PYTHONPATH=. uv run python scripts/build_3739_slice3a.py notes-extract --submissions <submissions.zip> \\
        --out <notes.jsonl.gz> <reduced.jsonl.gz ...>
    PYTHONPATH=. uv run python scripts/build_3739_slice3a.py universe --replica 2022 --calibration <f> \\
        --calibration-sha256 <d> --extract <f> --extract-sha256 <d> --out <u-pass1.csv>
    PYTHONPATH=. uv run python scripts/build_3739_slice3a.py screens --replica 2022 --notes <f> --notes-sha256 <d> \\
        --extract <f> --extract-sha256 <d> --out-dir <dir>
"""

from __future__ import annotations

import argparse
import gzip
import json
import zipfile
from collections import Counter
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Final

from app.services.factor_panel_artefact import read_gz_lines
from scripts.build_3739_slice2 import (
    ENTRANT_LOOKBACK,
    STAGE_A,
    STAGE_B,
    calibrated_k,
    checked,
    extract,
    pass1,
    read_extract,
    read_panel,
    sha256_of,
)
from scripts.build_3739_slice2b import (
    ITEM_503_SPAN,
    SPLIT_COLUMNS,
    NoteFact,
    archive_label,
    cover_candidates,
    covers,
    csv_bytes,
    item_503_candidates,
    publish,
    publish_all,
    read_notes,
    read_notes_archive,
    split_rows,
    with_acceptance,
    xbrl_candidates,
)


@dataclass(frozen=True)
class Replica:
    """§3.4's replica window. ``through`` is both the last event date and the evidence cutoff, as slice 2b's
    ``--through`` is for stage C; ``entrant_last`` is F + 24 months (§2 item 2)."""

    base: date
    event_start: date
    through: date
    entrant_last: date
    #: The filing extract's first acceptance date: the first day of the month before F - 92 days, as slice 2a's
    #: 2024-04-01 is for F = 2024-07-31.
    extract_from: date

    def __post_init__(self) -> None:
        # The extract must reach the entrant look-back and the first Item 5.03 acceptance whose interval meets the
        # window; item_503_candidates also refuses on the second, by business day.
        if self.extract_from > self.base - ENTRANT_LOOKBACK or self.extract_from > self.event_start - ITEM_503_SPAN:
            raise ValueError(f"extract_from {self.extract_from} does not cover the entrant or Item 5.03 look-back")


REPLICAS: Final = {
    "2022": Replica(date(2022, 7, 31), date(2022, 8, 1), date(2024, 7, 31), date(2024, 7, 31), date(2022, 4, 1)),
    "2020": Replica(date(2020, 7, 31), date(2020, 8, 1), date(2022, 7, 31), date(2022, 7, 31), date(2020, 4, 1)),
}


# --------------------------------------------------------------------------- notes, one archive at a time


def notes_reduce(archive: Path, out: Path) -> None:
    """One archive's §3.2 facts (acceptance not yet attached), headed by a line naming the archive, its sha256 and
    its ledger, so the joined extract can pin the archive after the archive is deleted."""
    ledger: Counter[str] = Counter()
    facts = read_notes_archive(archive, ledger)
    head = {"archive": archive.name, "archive_sha256": sha256_of(archive), "ledger": dict(sorted(ledger.items()))}
    lines = [json.dumps(head, sort_keys=True)] + [json.dumps(asdict(f), sort_keys=True) for f in facts]
    publish(out, gzip.compress(("\n".join(lines) + "\n").encode(), mtime=0))
    print(json.dumps({**head, "facts": len(facts), "reduced_sha256": sha256_of(out)}, indent=1))


def read_reduced(path: Path) -> tuple[dict[str, Any], list[NoteFact]]:
    rows = iter(read_gz_lines(path))
    head = next(rows, None)
    if not isinstance(head, dict) or set(head) != {"archive", "archive_sha256", "ledger"}:
        raise ValueError(f"{path} does not start with a reduced-archive head line")
    label = archive_label(Path(head["archive"]))
    facts = [NoteFact(**row) for row in rows]
    if any(f.archive != label for f in facts):
        raise ValueError(f"{path} holds facts of another archive than {head['archive']}")
    return head, facts


def notes_extract(reduced: Sequence[Path], submissions: Path, out: Path) -> None:
    ledger: Counter[str] = Counter()
    archives: dict[str, str] = {}
    seen: dict[str, str] = {}
    facts: list[NoteFact] = []
    for path in reduced:
        head, archive_facts = read_reduced(path)
        if head["archive"] in archives:
            raise ValueError(f"{head['archive']} is reduced twice")
        archives[head["archive"]] = head["archive_sha256"]
        ledger.update(head["ledger"])
        for adsh in {f.adsh for f in archive_facts}:
            if adsh in seen:
                raise ValueError(f"{adsh} is in both {seen[adsh]} and {head['archive']}: the archives overlap")
            seen[adsh] = head["archive"]
        facts.extend(archive_facts)
    with zipfile.ZipFile(submissions) as zf:
        facts = with_acceptance(facts, zf, ledger)
    lines = "".join(json.dumps(asdict(f), sort_keys=True) + "\n" for f in facts)
    publish(out, gzip.compress(lines.encode(), mtime=0))
    print(
        json.dumps(
            {
                "archives": dict(sorted(archives.items())),
                "submissions_sha256": sha256_of(submissions),
                "facts": len(facts),
                "filings": len(seen),
                "ledger": dict(sorted(ledger.items())),
                "notes_sha256": sha256_of(out),
            },
            indent=1,
        )
    )


# --------------------------------------------------------------------------- U and screens


def universe(replica: Replica, calibration: Path, extract_path: Path, out: Path) -> None:
    k = calibrated_k(json.loads(calibration.read_text()))
    # 2022-07-31 is a stage-B formation, the fallback's 2020-07-31 a stage-A one.
    formation = next(
        (p[replica.base] for p in (read_panel(s)[0] for s in (STAGE_B, STAGE_A)) if replica.base in p), None
    )
    if formation is None:
        raise ValueError(f"neither published panel has the formation {replica.base}")
    u = pass1(formation, replica.base, k, read_extract(extract_path), [], replica.entrant_last)
    rows = ([cik, ";".join(b.get("incumbent", [])), ";".join(b.get("entrant", []))] for cik, b in u.items())
    publish(out, csv_bytes(("cik", "incumbent_rank", "entrant_accessions"), rows))
    counts = {basis: sum(1 for b in u.values() if basis in b) for basis in ("incumbent", "entrant")}
    print(
        json.dumps(
            {
                "base": replica.base.isoformat(),
                "K": k,
                "issuers": len(u),
                "by_basis": counts,
                "u_sha256": sha256_of(out),
            }
        )
    )


def screens(replica: Replica, notes: Path, extract_path: Path, out_dir: Path) -> None:
    ledger: Counter[str] = Counter()
    facts = read_notes(notes)
    start, through = replica.event_start, replica.through
    split = [
        *xbrl_candidates(facts, through, start),
        *cover_candidates(covers(facts, through, ledger, start), through, start),
        *item_503_candidates(read_extract(extract_path), through, start, replica.extract_from),
    ]
    target = out_dir / "candidates-split.csv.gz"
    if target.exists():
        raise FileExistsError(f"refusing to replace a pinned candidate list: {target}")
    publish_all({target: gzip.compress(csv_bytes(SPLIT_COLUMNS, split_rows(split)), mtime=0)})
    print(
        json.dumps(
            {
                "window": [start.isoformat(), through.isoformat()],
                "split_by_screen": dict(Counter(c.screen for c in split)),
                "split_issuers": len({c.cik for c in split}),
                "ledger": dict(sorted(ledger.items())),
                "sha256": sha256_of(target),
            },
            indent=1,
        )
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="#3739 slice 3a: the §3.4 replica's U and split candidates")
    sub = parser.add_subparsers(dest="command", required=True)
    ext = sub.add_parser("extract")
    ext.add_argument("--replica", choices=sorted(REPLICAS), required=True)
    ext.add_argument("--zip", type=Path, required=True)
    ext.add_argument("--out", type=Path, required=True)
    red = sub.add_parser("notes-reduce")
    red.add_argument("--out", type=Path, required=True)
    red.add_argument("archive", type=Path)
    notes = sub.add_parser("notes-extract")
    notes.add_argument("--submissions", type=Path, required=True)
    notes.add_argument("--out", type=Path, required=True)
    notes.add_argument("reduced", type=Path, nargs="+")
    uni = sub.add_parser("universe")
    uni.add_argument("--replica", choices=sorted(REPLICAS), required=True)
    scr = sub.add_parser("screens")
    scr.add_argument("--replica", choices=sorted(REPLICAS), required=True)
    for command, names in ((uni, ("calibration", "extract")), (scr, ("notes", "extract"))):
        for name in names:
            command.add_argument(f"--{name}", type=Path, required=True)
            command.add_argument(f"--{name}-sha256", required=True)
    uni.add_argument("--out", type=Path, required=True)
    scr.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "extract":
        extract(args.zip, args.out, REPLICAS[args.replica].extract_from)
    elif args.command == "notes-reduce":
        notes_reduce(args.archive, args.out)
    elif args.command == "notes-extract":
        notes_extract(args.reduced, args.submissions, args.out)
    elif args.command == "universe":
        universe(
            REPLICAS[args.replica],
            checked(args.calibration, args.calibration_sha256),
            checked(args.extract, args.extract_sha256),
            args.out,
        )
    else:
        screens(
            REPLICAS[args.replica],
            checked(args.notes, args.notes_sha256),
            checked(args.extract, args.extract_sha256),
            args.out_dir,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
