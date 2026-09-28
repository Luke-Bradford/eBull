"""Hunt 3's one validation look (#3454).

Spec ``docs/proposals/ta/2026-09-28-3454-hunt-3-spec.md``: "Hunt 3's identity" fixes
:func:`build_spec` (hunt 2's stored discovery spec, relabelled; never re-typed), "Flag →
validation declaration" fixes the ``hunt_gate`` block the door builds, and "At validation,
one look" fixes the promotion rule (``hunt_gate.promotion``), which reads the bar and the t
threshold from the FROZEN block. The readout writes an immutable record; ``HUNT_CLOSED`` and
``CLOSED_LINEAGES`` land only after it exists.

    PYTHONPATH=. uv run python -m scripts.run_hunt_3_validation                        # dry run: codes + gate
    PYTHONPATH=. uv run python -m scripts.run_hunt_3_validation --write                # write the document
    PYTHONPATH=. uv run python -m scripts.run_hunt_3_validation --freeze --by "<session>"
    PYTHONPATH=. uv run python -m scripts.run_hunt_3_validation --look --by "<session>"   # THE one look
    PYTHONPATH=. uv run python -m scripts.run_hunt_3_validation --readout --record     # the immutable record

⚠ ``--look`` is the lineage's last historical look (spec "Lineage closure"): its registration
commits before a price is read, and from then on ``evaluate`` refuses ``lineage_closed`` for
every other spec of the family or signal hash.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from collections.abc import Mapping
from typing import Any, Final

import psycopg

from app.config import settings
from app.services import hunt_door, hunt_gate
from app.services import hunt_harness as hh

HUNT_ID: Final = "hunt-3"
#: The declaration document the declaration PR commits (with its ``hunt-3-validation`` register entry).
DOC_PATH: Final = "docs/hunts/hunt-3-validation.json"
#: The immutable readout record. Written once; the closure PR commits it.
RECORD_PATH: Final = "docs/hunts/hunt-3-validation-readout.json"
RECORD_KIND: Final = "hunt-3-validation-readout-v1"

_SOURCE_SPEC: Final = "SELECT hunt_id, split, purpose, spec FROM hunt_trials WHERE hunt_trial_id = %(trial)s"


def build_spec(conn: psycopg.Connection[Any]) -> hh.TrialSpec:
    """Hunt 2's registered discovery spec with only ``hunt_id`` and ``split`` changed (spec "The trial")."""
    source = hh.HUNT_INHERITED_DISCOVERY[HUNT_ID]
    row = conn.execute(_SOURCE_SPEC, {"trial": source.hunt_trial_id}).fetchone()
    conn.commit()
    if row is None or tuple(row[:3]) != (source.source_hunt_id, "discovery", "evaluate"):
        raise SystemExit(f"row {source.hunt_trial_id} is not {source.source_hunt_id}'s discovery evaluate row")
    return dataclasses.replace(hh.TrialSpec.from_form(row[3]), hunt_id=HUNT_ID, split="validation")


def record(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    """The readout and its promotion, from stored outcomes and the frozen block only."""
    readout = hunt_door.validation_readout(conn, HUNT_ID)
    declaration = hh.load_hunt_declaration(conn, readout.declaration_id)
    conn.commit()
    if declaration is None:
        raise SystemExit(f"declaration {readout.declaration_id} has no document")
    (candidate,) = readout.candidates
    statistics: Mapping[str, Any] = {}
    outcome_sha256 = None
    if candidate.hunt_trial_id is not None and candidate.verdict is not None:
        outcome = hh.read_outcome(conn, candidate.hunt_trial_id)
        conn.commit()
        statistics, outcome_sha256 = outcome.statistics, outcome.outcome_sha256
    block = hh.decode_form(declaration.doc["hunt_gate"])
    promotion = hunt_gate.promotion(
        block,
        verdict=None if candidate.verdict is None else str(candidate.verdict),
        reasons=candidate.reasons,
        statistics=statistics,
    )
    return {
        "kind": RECORD_KIND,
        "hunt_id": HUNT_ID,
        "complete": readout.complete,
        "declaration_id": readout.declaration_id,
        "declaration_sha256": declaration.doc_sha256,
        "hunt_trial_id": candidate.hunt_trial_id,
        "outcome_sha256": outcome_sha256,
        "m": readout.m,
        "m_is_floor": readout.m_is_floor,
        "labels": list(readout.labels),
        "dsr": dict(candidate.dsr),
        "survivor_subspans": {k: dict(v) for k, v in candidate.survivor_subspans.items()},
        "regimes": statistics.get("regimes"),
        "utilisation": hunt_gate.tracker_utilisation(statistics),
        **promotion,
    }


def _write_once(path: str, canonical_doc: Mapping[str, Any]) -> str:
    """Write an ALREADY canonical document once; an existing file is never overwritten."""
    target = hunt_door.REPO_ROOT / path
    if target.exists():
        raise SystemExit(f"{path} exists: it is written once")
    target.parent.mkdir(parents=True, exist_ok=True)
    return hunt_door.write_declaration(target, canonical_doc)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Hunt 3's one validation look (#3454).")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true", help=f"write the declaration document to {DOC_PATH}")
    mode.add_argument("--freeze", action="store_true", help="freeze hunt-3-validation from the committed document")
    mode.add_argument("--look", action="store_true", help="THE one validation look")
    mode.add_argument("--readout", action="store_true", help="the readout and its promotion")
    parser.add_argument("--record", action="store_true", help=f"with --readout: write {RECORD_PATH} (once)")
    parser.add_argument("--by", help="who freezes or looks (required with --freeze / --look)")
    args = parser.parse_args(argv)
    if (args.freeze or args.look) and not args.by:
        parser.error("--freeze and --look need --by")
    with psycopg.connect(settings.database_url) as conn:
        if args.readout:
            result = record(conn)
            print(json.dumps(result, indent=2, default=str))
            if args.record:
                if not result["complete"]:
                    print("refused: the look has no outcome yet", file=sys.stderr)
                    return 1
                print(f"record sha256 {_write_once(RECORD_PATH, hh.canonical_form(result))}")
            return 0
        if args.freeze:
            declaration_id = hunt_door.freeze_validation_declaration(conn, doc_path=DOC_PATH, declared_by=args.by)
            print(f"frozen hunt-3-validation as declaration {declaration_id}")
            return 0
        spec = build_spec(conn)
        print(f"spec_sha256 {spec.spec_sha256}\ncandidate_sha256 {spec.candidate_sha256}")
        if args.look:
            result = hh.evaluate(conn, spec, registered_by=args.by)
            if isinstance(result, hh.HuntRefused):
                print(f"refused: {result.reason}: {result.detail}", file=sys.stderr)
                return 1
            print(json.dumps(record(conn), indent=2, default=str))
            return 0
        doc, codes = hunt_door.build_validation_declaration(conn, hunt_id=HUNT_ID, pins=[spec])
        print(json.dumps({"codes": codes, "hunt_gate": hh.decode_form(doc.get("hunt_gate"))}, indent=2, default=str))
        if args.write:
            if codes:
                print("refused: the document would not freeze", file=sys.stderr)
                return 1
            print(f"document sha256 {_write_once(DOC_PATH, doc)}")
        return 0 if not codes else 1


if __name__ == "__main__":
    raise SystemExit(main())
