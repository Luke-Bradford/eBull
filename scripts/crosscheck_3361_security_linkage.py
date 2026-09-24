"""#3361 acceptance item 3: link cross-checks against two non-ground-truth comparators.

Spec: ``docs/proposals/ta/2026-09-24-3361-security-linkage.md`` (acceptance item 3 and the
"Cross-checks" precision rules [37-41]). Each check is on a SUBSET, so it establishes nothing
about the rest of the population. Every outcome is agree / disagree / not_comparable, with
denominators, not_comparable broken down by reason, and every disagreement listed.

* **(a) eToro.** Intrader series with an ``instrument_id``, whose ``instrument_cik_history``
  holds exactly one row containing ``last_bar`` (``effective_from <= last_bar`` and
  ``effective_to`` NULL or ``last_bar < effective_to``). The query runs at ``last_bar``.
* **(b) Form 25.** Intrader series with a ``delisting_provision``. The link comes from a
  bundle built ``--without-form25`` and is compared with the ``issuer_cik`` of register rows
  filed ON ``delisting_filed_date`` whose ``resolved_symbol`` matches the series under
  rule 4. The query runs at ``min(last_bar, delisting_filed_date)``. The series↔Form 25
  association is itself symbol-based, so this is an independent SOURCE, not an independent
  association.

Both bundles must snapshot the same inputs, apart from the Form 25 input the second one lacks.

Usage::

    PYTHONPATH=. uv run python scripts/crosscheck_3361_security_linkage.py \\
        --bundle <with-form25 dir> --manifest-sha256 <sha> \\
        --bundle-without-form25 <dir> --manifest-sha256-without-form25 <sha> > /tmp/crosscheck3361.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from app.services import security_linkage as sl

#: Inputs the two bundles must share byte-for-byte.
SHARED_INPUTS = (
    "quarters",
    "submissions",
    "pit_manifest",
    "series_inventory",
    "crosscheck_series",
    "instrument_cik_history",
)


@dataclass
class Check:
    population: int = 0
    agree: int = 0
    disagree: list[dict[str, Any]] = field(default_factory=list)
    not_comparable: Counter[str] = field(default_factory=Counter)

    def record(self, series_id: int, comparator: str | None, why: str | None, result: sl.LinkResult, at: date) -> None:
        self.population += 1
        if comparator is None:
            self.not_comparable[f"comparator:{why}"] += 1
        elif result.reason is not sl.Reason.LINKED:
            self.not_comparable[f"reader:{result.label}"] += 1
        elif result.cik == comparator:
            self.agree += 1
        else:
            self.disagree.append(
                {
                    "series_id": series_id,
                    "at": at.isoformat(),
                    "link": result.cik,
                    "basis": result.basis,
                    "comparator": comparator,
                }
            )

    def to_json(self) -> dict[str, Any]:
        return {
            "population": self.population,
            "agree": self.agree,
            "disagree": len(self.disagree),
            "not_comparable": sum(self.not_comparable.values()),
            "not_comparable_by_reason": dict(sorted(self.not_comparable.items())),
            "disagreements": self.disagree,
        }


def _intrader(inventory: Sequence[Sequence[Any]]) -> dict[int, tuple[str, date]]:
    return {row[0]: (row[2], date.fromisoformat(row[4])) for row in inventory if row[1] == sl.IN_SCOPE_VENDOR}


def _valid_history(row: Sequence[Any]) -> tuple[str, date, date | None] | None:
    """Precision rule [39]: a malformed history row is not comparable."""
    _, cik, start, end, _ = row
    try:
        start_d = date.fromisoformat(start)
        end_d = None if end is None else date.fromisoformat(end)
    except TypeError, ValueError:
        return None
    canonical = sl.canonical_cik(cik) if isinstance(cik, str) else None
    if canonical is None or canonical != cik or (end_d is not None and end_d <= start_d):
        return None
    return cik, start_d, end_d


def check_etoro(
    bundle: sl.SecurityLinkageBundle,
    inventory: Sequence[Sequence[Any]],
    crosscheck: Sequence[Sequence[Any]],
    history: Sequence[Sequence[Any]],
) -> Check:
    series = _intrader(inventory)
    by_instrument: dict[int, list[Sequence[Any]]] = {}
    for row in history:
        by_instrument.setdefault(row[0], []).append(row)
    check = Check()
    for series_id, instrument_id, _, _ in crosscheck:
        if series_id not in series or instrument_id is None:
            continue
        last_bar = series[series_id][1]
        rows = [_valid_history(r) for r in by_instrument.get(instrument_id, [])]
        comparator = why = None
        if any(r is None for r in rows):
            why = "malformed_history_row"
        else:
            containing = [r for r in rows if r and r[1] <= last_bar and (r[2] is None or last_bar < r[2])]
            if len(containing) == 1:
                comparator = containing[0][0]
            else:
                why = "no_history_row_contains_last_bar" if not containing else "several_history_rows_contain_last_bar"
        check.record(series_id, comparator, why, bundle.link_as_of(series_id, last_bar), last_bar)
    return check


def check_form25(
    bundle_without: sl.SecurityLinkageBundle,
    inventory: Sequence[Sequence[Any]],
    crosscheck: Sequence[Sequence[Any]],
    register: Sequence[Sequence[Any]],
) -> Check:
    series = _intrader(inventory)
    by_filed: dict[str, list[Sequence[Any]]] = {}
    for row in register:
        by_filed.setdefault(row[1], []).append(row)
    check = Check()
    for series_id, _, provision, filed in crosscheck:
        if series_id not in series or provision is None:
            continue
        symbol, last_bar = series[series_id]
        at = last_bar if filed is None else min(last_bar, date.fromisoformat(filed))
        symbols = sl.match_set(sl.parse_vendor_symbol(symbol))
        comparator = why = None
        if filed is None:
            why = "no_delisting_filed_date"
        elif not symbols:
            why = "no_rule4_match_set"
        else:
            ciks = {sl.canonical_cik(r[2]) for r in by_filed.get(filed, []) if sl.evidence_symbol(r[3]) in symbols}
            if len(ciks) == 1:
                comparator = next(iter(ciks))
            else:
                why = "no_register_row" if not ciks else "several_register_ciks"
        check.record(series_id, comparator, why, bundle_without.link_as_of(series_id, at), at)
    return check


def _load(root: Path, sha: str, *, form25: bool) -> tuple[sl.SecurityLinkageBundle, dict[str, Any]]:
    bundle = sl.load_security_linkage(root, expected_manifest_sha256=sha)
    if bundle.form25_mode is not form25:
        raise SystemExit(f"{root}: form25_mode is {bundle.form25_mode}, expected {form25}")
    return bundle, json.loads((root / sl.MANIFEST_FILENAME).read_bytes())


def _dump(root: Path, name: str, expected: str) -> Any:
    data = (root / "inputs" / f"{name}.json").read_bytes()
    if hashlib.sha256(data).hexdigest() != expected:
        raise SystemExit(f"{root}: input {name} digest differs from its manifest")
    return json.loads(data)


def run(with_root: Path, with_sha: str, without_root: Path, without_sha: str) -> dict[str, Any]:
    bundle, manifest = _load(with_root, with_sha, form25=True)
    bundle_without, manifest_without = _load(without_root, without_sha, form25=False)
    for name in SHARED_INPUTS:
        if manifest["input_sha256"][name] != manifest_without["input_sha256"][name]:
            raise SystemExit(f"the two bundles snapshot different {name}")
    sha: Mapping[str, Any] = manifest["input_sha256"]
    inventory = _dump(with_root, "series_inventory", sha["series_inventory"])
    crosscheck = _dump(with_root, "crosscheck_series", sha["crosscheck_series"])
    history = _dump(with_root, "instrument_cik_history", sha["instrument_cik_history"])
    register = _dump(with_root, "form25", sha["form25"])["rows"]
    return {
        "manifest_sha256": with_sha,
        "manifest_sha256_without_form25": without_sha,
        "etoro": check_etoro(bundle, inventory, crosscheck, history).to_json(),
        "form25": check_form25(bundle_without, inventory, crosscheck, register).to_json(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--bundle-without-form25", type=Path, required=True)
    parser.add_argument("--manifest-sha256-without-form25", required=True)
    args = parser.parse_args()
    report = run(args.bundle, args.manifest_sha256, args.bundle_without_form25, args.manifest_sha256_without_form25)
    with Path(__file__).open("rb") as handle:
        report["script_sha256"] = hashlib.file_digest(handle, "sha256").hexdigest()
    json.dump(report, sys.stdout, indent=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
