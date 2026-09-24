"""#3361 acceptance item 2: full-population causal reference for the security-linkage bundle.

Spec: ``docs/proposals/ta/2026-09-24-3361-security-linkage.md`` (acceptance item 2 and the
"Causal test dates" precision rules). For every series in the bundle and every date in the
spec's decision-date set, ``link_as_of(S, D)`` must equal the answer rebuilt here from the
snapshotted RAW inputs, with only the observations accepted before ``D`` in play.

Independent path:

* **Integrity first, on the full snapshot.** The reference re-reads the retained quarter
  archives and ``submissions.zip`` and re-derives, with its own grouping, the three
  snapshot-integrity masks: accession conflicts, issuer integrity and vendor-symbol
  collisions. Those are the declared exceptions to causality (spec "Declared residuals").
  Row admission (``admit_row`` / ``admit_observation``) and the vendor grammar are shared.
  They are pure functions of one row, or of one accession's own submissions record, or of
  the vendor symbol, so no date enters them.
* **Then the prefix.** Rules 5 and 6 are recomputed here, and the reader is never called
  on the reference side: the window, the strict-succession test, the q-alias flag, the
  Form 25 flags and the whole reason precedence.

The script exits non-zero on any mismatch.

Usage::

    PYTHONPATH=. uv run python scripts/causal_3361_security_linkage.py \\
        --bundle <bundle dir> --manifest-sha256 <sha> > /tmp/causal3361.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import zipfile
from bisect import bisect_left
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from app.services import security_linkage as sl
from app.services.pit_fundamentals import acceptance_ny_date, parse_submissions
from scripts.build_3361_security_linkage import _submission_text

_MISMATCH_EXAMPLES = 20
_ONE = timedelta(days=1)


@dataclass
class Tally:
    counts: Counter[str] = field(default_factory=Counter)
    mismatches: list[str] = field(default_factory=list)

    def check(self, expected: object, actual: object, where: str) -> None:
        self.counts["compared"] += 1
        if expected != actual:
            self.counts["mismatch"] += 1
            if len(self.mismatches) < _MISMATCH_EXAMPLES:
                self.mismatches.append(f"{where}: expected {expected!r} actual {actual!r}")


@dataclass(frozen=True)
class Obs:
    acceptance: str
    accession: str
    cik: str
    symbol: str
    multiplicity: int
    ny: date


@dataclass
class Reference:
    """Everything the reference needs, re-derived from the bundle's retained raw inputs."""

    supported_through: date
    span: tuple[date, date] | None
    inventory: dict[int, tuple[str, str, date, date]]  # id -> (vendor, symbol, first, last)
    collided: frozenset[int]
    by_symbol: dict[str, list[Obs]]
    register_by_cik: dict[str, list[tuple[str, str, str | None]]]  # cik -> (filed, accession, symbol)
    counts: Counter[str] = field(default_factory=Counter)


def load_reference(root: Path, manifest: Mapping[str, Any]) -> Reference:
    inputs = root / "inputs"
    for relative, expected in _input_files(manifest):
        with (inputs / relative).open("rb") as handle:
            if hashlib.file_digest(handle, "sha256").hexdigest() != expected:
                raise SystemExit(f"input {relative} digest differs from the manifest")
    counts: Counter[str] = Counter()

    inventory: dict[int, tuple[str, str, date, date]] = {}
    for series_id, vendor, symbol, first, last in json.loads((inputs / "series_inventory.json").read_bytes()):
        inventory[series_id] = (vendor, symbol, date.fromisoformat(first), date.fromisoformat(last))
    grammars = {sid: sl.parse_vendor_symbol(row[1]) for sid, row in inventory.items() if row[0] == sl.IN_SCOPE_VENDOR}
    symbol_owners: dict[str, set[int]] = defaultdict(set)
    for sid, grammar in grammars.items():
        for symbol in sl.match_set(grammar):
            symbol_owners[symbol].add(sid)
    collided = frozenset(sid for owners in symbol_owners.values() if len(owners) > 1 for sid in owners)

    # Integrity mask 1: accession conflicts, from the full snapshot with our own grouping.
    keys: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    rows: Counter[str] = Counter()
    for name in manifest["input_sha256"]["quarters"]:
        with zipfile.ZipFile(inputs / "form345" / name) as archive:
            lines, columns = _submission_text(archive, name)
        for line in lines[1:]:
            admitted = sl.admit_row(line.split("\t"), columns)
            if isinstance(admitted, sl.Admitted):
                keys[admitted.accession].add(admitted.key)
                rows[admitted.accession] += 1
    conflicted = {accn for accn, found in keys.items() if len(found) > 1}
    counts["accessions_conflicted"] = len(conflicted)

    # Integrity mask 2: issuer integrity, and each accession's acceptance.
    by_cik: dict[str, list[str]] = defaultdict(list)
    for accn, found in keys.items():
        if accn not in conflicted:
            by_cik[next(iter(found))[0]].append(accn)
    by_symbol: dict[str, list[Obs]] = defaultdict(list)
    wanted = set(symbol_owners)
    with zipfile.ZipFile(inputs / "submissions.zip") as subs:
        names = set(subs.namelist())

        def read_page(page: str) -> object | None:
            return json.loads(subs.read(page)) if page in names else None

        for cik, accessions in by_cik.items():
            member = f"CIK{cik}.json"
            index = parse_submissions(cik, json.loads(subs.read(member)), read_page) if member in names else "absent"
            if isinstance(index, str):
                counts["issuers_integrity_excluded"] += 1
            for accn in accessions:
                (_, symbol, _) = next(iter(keys[accn]))
                outcome, acceptance = sl.admit_observation(accn, index)
                counts[f"observations_{outcome.value}"] += 1
                if acceptance is not None and symbol in wanted:
                    obs = Obs(acceptance, accn, cik, symbol, rows[accn], acceptance_ny_date(acceptance))
                    by_symbol[symbol].append(obs)

    register_by_cik: dict[str, list[tuple[str, str, str | None]]] = defaultdict(list)
    span = None
    if manifest["form25_mode"]:
        dump = json.loads((inputs / "form25.json").read_bytes())
        for accession, filed, issuer_cik, symbol in dump["rows"]:
            register_by_cik[f"{int(issuer_cik):010d}"].append((filed, accession, symbol))
        if dump["rows"]:
            span = (date.fromisoformat(dump["span"][0]), date.fromisoformat(dump["span"][1]))
    return Reference(
        supported_through=date.fromisoformat(manifest["supported_through"]),
        span=span,
        inventory=inventory,
        collided=collided,
        by_symbol=by_symbol,
        register_by_cik=register_by_cik,
        counts=counts,
    )


def _input_files(manifest: Mapping[str, Any]) -> list[tuple[str, str]]:
    sha = manifest["input_sha256"]
    files = [(f"form345/{name}", digest) for name, digest in sha["quarters"].items()]
    files += [("submissions.zip", sha["submissions"]), ("series_inventory.json", sha["series_inventory"])]
    if manifest["form25_mode"]:
        files.append(("form25.json", sha["form25"]))
    return files


# --------------------------------------------------------------------------- rules 5-6, again


def _match(symbol: str | None, symbols: frozenset[str]) -> str:
    text = (symbol or "").strip().upper()
    if text in sl.PLACEHOLDERS:
        return "symbol_null"
    unified = "".join("." if ch in "./_-" else ch for ch in text)
    return "symbol_match" if unified in symbols else "symbol_other"


def expected_result(ref: Reference, series_id: int, decision: date, observations: Sequence[Obs]) -> sl.LinkResult:
    """Rule 6 precedence and rule 5 on the prefix, without the reader."""
    row = ref.inventory.get(series_id)
    if row is None:
        return sl.LinkResult(sl.Reason.SERIES_NOT_IN_BUNDLE)
    vendor, vendor_symbol, first, last = row
    if vendor != sl.IN_SCOPE_VENDOR:
        return sl.LinkResult(sl.Reason.VENDOR_OUT_OF_SCOPE)
    if decision > ref.supported_through:
        return sl.LinkResult(sl.Reason.AFTER_CAPTURE)
    if decision < first or decision > last:
        return sl.LinkResult(sl.Reason.OUTSIDE_SERIES)
    grammar = sl.parse_vendor_symbol(vendor_symbol)
    kind_reason = {
        "test": sl.Reason.VENDOR_TEST_SYMBOL,
        "unparsed": sl.Reason.UNPARSED_SYMBOL_FORM,
        "non_common": sl.Reason.NON_COMMON_SYMBOL_FORM,
    }
    if grammar.kind in kind_reason:
        return sl.LinkResult(kind_reason[grammar.kind], grammar.label, detail=grammar.token)
    if series_id in ref.collided:
        return sl.LinkResult(sl.Reason.VENDOR_SYMBOL_COLLISION, grammar.label)
    if decision - timedelta(days=sl.WINDOW_DAYS) < sl.COVERAGE_START:
        return sl.LinkResult(sl.Reason.BEFORE_COVERAGE, grammar.label)

    ny = [o.ny for o in observations]
    public_end = bisect_left(ny, decision)
    window = observations[bisect_left(ny, decision - timedelta(days=sl.WINDOW_DAYS)) : public_end]
    alias = {o.accession: grammar.kind == "plain" and o.symbol != grammar.root for o in window}
    shown = tuple(sl.Observation(o.accession, o.cik, o.acceptance, o.multiplicity, alias[o.accession]) for o in window)
    ciks = {o.cik for o in window}
    symbols = sl.match_set(grammar)
    unobserved = ref.span is None or decision < ref.span[0] or decision > ref.span[1]
    flags: tuple[sl.Form25Flag, ...] = ()
    if not unobserved:
        candidates = sorted(
            (filed, accession, cik, symbol)
            for cik in ciks
            for filed, accession, symbol in ref.register_by_cik.get(cik, ())
            if date.fromisoformat(filed) < decision
        )
        flags = tuple(sl.Form25Flag(a, f, c, _match(s, symbols)) for f, a, c, s in candidates)
    evidence: dict[str, Any] = {"observations": shown, "form25_unobserved": unobserved, "form25": flags}
    if not window:
        detail = None if public_end else "never_seen"
        return sl.LinkResult(sl.Reason.NO_RECENT_EVIDENCE, grammar.label, detail=detail, **evidence)
    firsts: dict[str, str] = {}
    lasts: dict[str, str] = {}
    for o in window:  # in acceptance order: the first sighting is the first, the latest the last
        firsts.setdefault(o.cik, o.acceptance)
        lasts[o.cik] = o.acceptance
    order = sorted(firsts, key=lambda c: (firsts[c], c))
    if len(order) == 1:
        cik, basis = order[0], "single_cik"
    elif all(lasts[a] < firsts[b] for a, b in zip(order, order[1:], strict=False)):
        cik, basis = order[-1], "succession"
    else:
        return sl.LinkResult(sl.Reason.CONFLICTING_EVIDENCE, grammar.label, **evidence)
    q_alias = any(alias[o.accession] for o in window if o.cik == cik)
    return sl.LinkResult(sl.Reason.LINKED, grammar.label, cik=cik, basis=basis, q_alias=q_alias, **evidence)


def decision_dates(ref: Reference, series_id: int, observations: Sequence[Obs]) -> list[date]:
    """Spec item 2 + precision rules [42-44]."""
    _, _, first, last = ref.inventory[series_id]
    dates = {first - _ONE, first, last, last + _ONE, ref.supported_through, ref.supported_through + _ONE}
    coverage = sl.COVERAGE_START + timedelta(days=sl.WINDOW_DAYS)
    dates |= {coverage - _ONE, coverage}
    if ref.span is not None:
        dates |= {ref.span[0], ref.span[1] + _ONE}
    window = timedelta(days=sl.WINDOW_DAYS)
    for o in observations:
        dates |= {o.ny, o.ny + _ONE, o.ny + window, o.ny + window + _ONE}
    if observations:
        dates.add(observations[0].ny - _ONE)
    for cik in {o.cik for o in observations}:
        for filed, _, _ in ref.register_by_cik.get(cik, ()):
            day = date.fromisoformat(filed)
            dates |= {day, day + _ONE}
    return sorted(dates)


def series_observations(ref: Reference, series_id: int) -> list[Obs]:
    vendor, symbol, _, _ = ref.inventory[series_id]
    if vendor != sl.IN_SCOPE_VENDOR:
        return []
    found = [o for s in sl.match_set(sl.parse_vendor_symbol(symbol)) for o in ref.by_symbol.get(s, ())]
    return sorted(found, key=lambda o: (o.acceptance, o.accession, o.cik))


def verify_series(bundle: sl.SecurityLinkageBundle, ref: Reference, series_id: int) -> Tally:
    tally = Tally()
    observations = series_observations(ref, series_id)
    for decision in decision_dates(ref, series_id, observations):
        tally.counts["decisions"] += 1
        expected = expected_result(ref, series_id, decision, observations)
        tally.counts[f"reason_{expected.reason.value}"] += 1
        tally.check(expected, bundle.link_as_of(series_id, decision), f"series {series_id} D={decision}")
    return tally


def verify(bundle: sl.SecurityLinkageBundle, ref: Reference) -> Tally:
    total = Tally()
    missing = max(ref.inventory, default=0) + 1
    total.check(
        expected_result(ref, missing, ref.supported_through, []),
        bundle.link_as_of(missing, ref.supported_through),
        "absent id",
    )
    for series_id in sorted(ref.inventory):
        tally = verify_series(bundle, ref, series_id)
        total.counts.update(tally.counts)
        total.mismatches.extend(tally.mismatches[: _MISMATCH_EXAMPLES - len(total.mismatches)])
    total.counts["series"] = len(ref.inventory)
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    args = parser.parse_args()
    bundle = sl.load_security_linkage(args.bundle, expected_manifest_sha256=args.manifest_sha256)
    manifest = json.loads((args.bundle / sl.MANIFEST_FILENAME).read_bytes())
    ref = load_reference(args.bundle, manifest)
    total = verify(bundle, ref)
    with Path(__file__).open("rb") as handle:
        script_sha = hashlib.file_digest(handle, "sha256").hexdigest()
    json.dump(
        {
            "manifest_sha256": args.manifest_sha256,
            "policy": manifest["policy"],
            "script_sha256": script_sha,
            "supported_through": manifest["supported_through"],
            "reference": dict(sorted(ref.counts.items())),
            "counts": dict(sorted(total.counts.items())),
            "mismatch_examples": total.mismatches,
        },
        sys.stdout,
        indent=1,
    )
    return 1 if total.counts["mismatch"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
