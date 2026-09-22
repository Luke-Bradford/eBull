"""#2840 — full-population A/B for the ``BarSeries`` row/cache freeze.

Read-only. Writes nothing to the database, takes no lock, and runs inside one
``REPEATABLE READ, READ ONLY`` transaction.

WHAT IT PROVES, AND HOW THE ARMS ARE HELD STILL
-----------------------------------------------
The freeze is meant to change NOTHING a strategy computes. So the arm is the
COMMIT, not a flag: run this file from a checkout at the baseline, run it again
from the branch, and ``--compare`` the two JSON outputs. Nothing here simulates
the control — the baseline arm has no ``MappingProxyType`` to disable.

⚠⚠ TWO RUNS IN TWO WORKTREES DO NOT SHARE A DATABASE SNAPSHOT, and an earlier
A/B on this ticket was refused at checkpoint 1 for exactly that. The answer here
is not to claim a shared snapshot but to MEASURE the shared input: every
instrument's bars are digested, and ``--compare`` refuses outright if the
``corpus`` digests differ. A concurrent write to the corpus then surfaces as a
refusal to compare rather than as a finding about this diff.

⚠ The REGIME is a fixed ``unconstrained_regime``, identical in both arms. That
is a harness input, NOT a market claim: ``market_regime.unconstrained_regime``
warns that using it on the regime-gated strategies would "assert a market
condition nobody measured", and it is right — so **no verdict counted here is
evidence about any strategy**. It is chosen because it maximises the number of
bars that reach arithmetic, which is the only property an A/B wants from it.

⚠ THE PERMITTED-CHANGE SET HAS THREE MEMBERS, and the first draft named one.
``strategy_version`` (all 12, both universes), ``INPUT_RULE_SETS
["indicator_series"]`` — and ``PRICE_BASIS_RULE_VERSION``, because the diff
widens ``bind_bar``'s annotation and ``strategy_price_basis.py``'s bytes are
inside it, which is inside S-12's params. Each is asserted to have ACTUALLY
rotated: a rotation that did not happen means the edit never reached the
identity, which is its own defect (prevention-log #3017).

COVERAGE, ASSERTED RATHER THAN ASSUMED
--------------------------------------
S-12's live path takes an UNDECLARED carrier and short-circuits before
arithmetic, so a sweep that only exercises it would prove nothing. Each arm
therefore records a per-cell ``evaluated`` count and ``--compare`` fails if any
declared cell is zero. Cells: {10 per-series strategies + 2 cross-sectional
members} x {both universes} x {undeclared, certified} x {flat, segmented}.

Refs #2840, #2437.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import logging
import sys
from collections import Counter
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import psycopg

from app.config import settings
from app.services.cost_model import COST_MODEL_ID
from app.services.indicator_series import BarSeries
from app.services.market_regime import unconstrained_regime
from app.services.price_masked_bars import load_bar_spans, load_masked_bars
from app.services.price_segments import load_unresolved_breaks
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_price_basis import from_archive_basis, from_undeclared_source
from app.services.strategy_registry import INPUT_RULE_SETS
from app.services.strategy_segmented_evaluation import segmented_member, segmented_signals
from app.services.strategy_signal_scan import choose_frontier

logger = logging.getLogger("ab_2840_barseries")

_UNIVERSES = ("survivor_only", "survivorship_free")
_MASKED_REASON = "quarantined_bar"

#: ⚠ Feeds EVERY cell digest once per instrument, before that instrument's
#: parts and even when it produced none (Codex checkpoint 2, P2). Without it the
#: digest is one undelimited stream, so "instrument A loses result M and
#: instrument B gains the same M" hashes IDENTICALLY in both arms — and the
#: `evaluated` / `cell_parts` counts stay equal too, so nothing else catches it.
#: A per-item boundary is what makes a streamed digest a comparison of the
#: sequence rather than of the concatenation.
_MARK = "@{}"

#: ⚠⚠ EXCLUDED FROM THE OUTPUT ENCODING BECAUSE THEY ARE THE PERMITTED CHANGE.
#: ``CrossSectionalMember`` carries ``inputs``/``score``, which are
#: ``IndicatorSeries``, which carry ``rule_set_version`` — and this diff rotates
#: exactly that. The first run reported "VERDICTS MOVED in 8 cells", every one a
#: `flat` cross-sectional cell, and the movement was the identity rotation
#: appearing inside the payload rather than any change in behaviour. Per-series
#: signals were unaffected because `_signal_parts` never touches provenance, and
#: `StagedMember` has no `inputs` — which is why exactly the 8 cells that CAN
#: carry it were the 8 that moved.
#:
#: ⚠ Excluding them is safe only because they are asserted SEPARATELY, in
#: `_compare`, as having actually rotated. Dropping a field from a comparison
#: without checking it elsewhere would be hiding it.
_PROVENANCE_FIELDS = frozenset({"rule_set_version"})

#: ⚠ Counts BarSeries construction and bars copied, so the cost claim is an
#: end-to-end measurement rather than a per-bar microbenchmark extrapolated
#: (ckpt-1 finding 28). Every sub-series build re-copies, and those are what a
#: microbenchmark on a single construction cannot see.
_CONSTRUCTIONS = Counter[str]()
_original_init = BarSeries.__init__


def _counting_init(self: BarSeries, *args: Any, **kwargs: Any) -> None:
    _original_init(self, *args, **kwargs)
    _CONSTRUCTIONS["series"] += 1
    _CONSTRUCTIONS["bars"] += len(self.rows)


class _RunningDigest:
    """sha256 fed incrementally, because accumulating the parts does not fit.

    ⚠ The first version built a `list[str]` per cell and joined at the end. On
    the real population that reached **2.5 GB RSS within 2.5 minutes** and
    extrapolated to ~48 GB — 5,797 instruments x 96 cells x one string per
    emitted signal, plus one per corpus bar. Killed and rewritten rather than
    left to swap; an A/B that OOMs halfway produces no evidence at all.

    Order and multiplicity are still exact: parts are fed in emission order
    with a separator, which is what a set-based comparison would have lost.
    """

    __slots__ = ("_hash", "count")

    def __init__(self) -> None:
        self._hash = hashlib.sha256()
        self.count = 0

    def update(self, parts: list[str]) -> None:
        for part in parts:
            self._hash.update(part.encode())
            self._hash.update(b"\x1e")
            self.count += 1

    def hexdigest(self) -> str:
        return self._hash.hexdigest()[:16]


def _corpus_parts(instrument_id: int, series: BarSeries, breaks: tuple[date, ...]) -> list[str]:
    """The INPUT, digested per instrument. This is what makes the two arms
    comparable: if a concurrent write moved an input, these differ and the
    comparison refuses rather than attributing the difference to the diff.

    ⚠ THE UNRESOLVED BREAKS ARE PART OF THE INPUT, and the first version left
    them out (Codex checkpoint 2, P2). They are not price data, so it is easy to
    think of the bars as "the corpus" — but they are passed to
    ``segmented_signals`` and ``segmented_member``, where they cut the series and
    reset strategy state. A break created or resolved between the two snapshots
    while the OHLCV rows stood still would have left the corpus digests MATCHING
    and the outputs differing, which is precisely the drift this digest exists to
    refuse.

    ⚠ The instrument id leads, so the digest cannot absorb a shift across an
    instrument boundary.
    """
    return [
        f"@{instrument_id}",
        "breaks:" + ",".join(day.isoformat() for day in breaks),
        *(
            f"{day.isoformat()}|{row.get('open')!r}|{row.get('high')!r}|{row.get('low')!r}"
            f"|{row.get('close')!r}|{row.get('volume')!r}"
            for day, row in zip(series.dates, series.rows, strict=True)
        ),
    ]


def _signal_parts(signals: list[Any]) -> list[str]:
    """Ordered, with multiplicity — a SET would hide a duplicate-output
    regression, which is the failure an A/B on a scan is most likely to see."""
    return [f"{s.kind}|{s.verdict}|{s.reason}|{s.signal_index}" for s in signals]


def _encode(value: Any) -> str:
    """Order-stable structural encoding of any strategy output.

    ⚠ Structural rather than a hand-written field list, because the two
    cross-sectional entry points return DIFFERENT shapes — ``entry.member``
    gives a ``CrossSectionalMember`` (``score``, ``*_indices``) and
    ``segmented_member`` gives a ``StagedMember`` (``verdicts``, ``scores``,
    ``*_dates``). A hand-written list would have to know both, and would go
    silently incomplete the next time either gains a field — which on an A/B
    means a changed output that digests identically.

    ⚠ ``repr`` per scalar, never a rounded format: a float difference in the
    last bit is exactly the kind of regression this is looking for. ⚠ ``None``
    is preserved rather than coerced — an absent ``mandatory_dates`` and an
    empty one are different states, and collapsing them is the vacuous-truth
    class the codebase already refuses elsewhere.
    """
    if value is None or isinstance(value, bool | int | float | str | date):
        return repr(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        fields = ",".join(
            f"{f.name}={_encode(getattr(value, f.name))}"
            for f in dataclasses.fields(value)
            if f.name not in _PROVENANCE_FIELDS
        )
        return f"{type(value).__name__}({fields})"
    if isinstance(value, frozenset | set):
        # Sorted by encoding, not by value — the members may not be orderable.
        return "{" + ",".join(sorted(_encode(item) for item in value)) + "}"
    if isinstance(value, Mapping):
        return "{" + ",".join(f"{_encode(k)}:{_encode(v)}" for k, v in sorted(value.items())) + "}"
    if isinstance(value, tuple | list):
        return "[" + ",".join(_encode(item) for item in value) + "]"
    if isinstance(value, np.ndarray):
        return f"ndarray({value.tolist()!r})"
    return repr(value)


def _member_parts(member: Any) -> list[str]:
    return [_encode(member)]


def _measure(limit: int) -> dict[str, Any]:
    cells: dict[str, _RunningDigest] = {}
    evaluated = Counter[str]()
    corpus = _RunningDigest()

    BarSeries.__init__ = _counting_init  # type: ignore[method-assign]
    try:
        with psycopg.connect(settings.database_url) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            universe = load_validated_universe(conn)
            spans = load_bar_spans(conn, universe)
            frontier = choose_frontier({iid: span.last_bar for iid, span in spans.items()})
            if frontier is None:
                raise SystemExit("no loadable instruments")
            eligible = sorted(
                iid for iid, span in spans.items() if span.last_bar == frontier.bar_date and span.bars >= 2
            )
            if limit:
                eligible = eligible[:limit]
            breaks = load_unresolved_breaks(conn, eligible)

            for seen, instrument_id in enumerate(eligible, start=1):
                series = load_masked_bars(conn, instrument_id).series
                if len(series) < 2:
                    continue
                regime = unconstrained_regime(len(series))
                instrument_breaks = tuple(breaks.get(instrument_id, ()))
                corpus.update(_corpus_parts(instrument_id, series, instrument_breaks))

                for uni in _UNIVERSES:
                    for route in ("undeclared", "certified"):
                        basis = (
                            from_undeclared_source(series=series)
                            if route == "undeclared"
                            else from_archive_basis("unadjusted", series=series)
                        )
                        for sid, entry in sorted(STRATEGY_MANIFEST.items()):
                            common = {
                                "universe": uni,
                                "masked_reason": _MASKED_REASON,
                                "regime": regime,
                                "price_basis": basis,
                            }
                            if entry.signals is not None:
                                flat = entry.signals(series, **common)  # type: ignore[arg-type]
                                seg = segmented_signals(entry, series, unresolved_breaks=instrument_breaks, **common)  # type: ignore[arg-type]
                                for shape, produced in (("flat", flat), ("segmented", seg)):
                                    key = f"{sid}|{uni}|{route}|{shape}"
                                    cells.setdefault(key, _RunningDigest()).update(
                                        [_MARK.format(instrument_id), *_signal_parts(produced)]
                                    )
                                    evaluated[key] += 1
                            elif entry.member is not None:
                                panel = frozenset(series.dates)
                                # ⚠ Member-only: the per-series interfaces take no ratio basis.
                                flat_m = entry.member(series, panel_decision_dates=panel, ratio_basis=series, **common)  # type: ignore[arg-type]
                                seg_m = segmented_member(
                                    entry,
                                    series,
                                    panel_decision_dates=panel,
                                    unresolved_breaks=instrument_breaks,
                                    ratio_basis=series,
                                    **common,  # type: ignore[arg-type]
                                )
                                for shape, member in (("flat", flat_m), ("segmented", seg_m)):
                                    key = f"{sid}|{uni}|{route}|{shape}"
                                    cells.setdefault(key, _RunningDigest()).update(
                                        [
                                            _MARK.format(instrument_id),
                                            *([] if member is None else _member_parts(member)),
                                        ]
                                    )
                                    evaluated[key] += 1
                if seen % 250 == 0:
                    logger.info("… %d/%d instruments", seen, len(eligible))
    finally:
        BarSeries.__init__ = _original_init  # type: ignore[method-assign]

    versions = {
        sid: {u: entry.identity(universe=u, cost_model_id=COST_MODEL_ID).version for u in _UNIVERSES}
        for sid, entry in sorted(STRATEGY_MANIFEST.items())
    }
    return {
        "eligible": len(eligible),
        "corpus": corpus.hexdigest(),
        "corpus_bars": corpus.count,
        "indicator_series_rule_set": INPUT_RULE_SETS["indicator_series"],
        "price_basis_rule": _price_basis_rule(),
        "strategy_versions": versions,
        "attachment": _attachment_census(versions),
        "cells": {key: running.hexdigest() for key, running in sorted(cells.items())},
        "cell_parts": {key: running.count for key, running in sorted(cells.items())},
        "evaluated": dict(sorted(evaluated.items())),
        "constructions": dict(_CONSTRUCTIONS),
    }


def _price_basis_rule() -> str:
    """⚠ A SECOND permitted identity change, and it was missed at first.

    This diff widens ``bind_bar``'s annotation, which changes
    ``strategy_price_basis.py``'s bytes, which is inside
    ``PRICE_BASIS_RULE_VERSION``, which is inside S-12's params. Naming only
    ``indicator_series`` in the permitted set would have made the comparison
    fail for a change that is expected.
    """
    from app.services.strategy_price_basis import PRICE_BASIS_RULE_VERSION

    return PRICE_BASIS_RULE_VERSION


def _attachment_census(versions: dict[str, dict[str, str]]) -> dict[str, Any]:
    """How much stored evidence hangs off THIS arm's identities.

    ⚠⚠ RUN PER ARM, AND THAT IS THE WHOLE POINT. ``indicator_series`` hashes
    its own source, so a census taken from the CANDIDATE checkout reports zero
    attachment by construction — the edit already detached everything, and the
    query cannot tell that apart from "there was never anything attached". Only
    the BASELINE arm's number answers the question the disposition needs, which
    is why this travels with the measurement and its commit rather than being
    quoted loose.
    """
    wanted = {
        (strategy_id, version) for strategy_id, per_universe in versions.items() for version in per_universe.values()
    }
    census: dict[str, Any] = {}
    with psycopg.connect(settings.database_url) as conn, conn.cursor() as cur:
        cur.execute("SET TRANSACTION READ ONLY")
        for table in ("strategy_signals", "strategy_signal_observations", "strategy_scan_watermark"):
            cur.execute(f"select strategy_id, strategy_version, count(*) from {table} group by 1, 2")  # noqa: S608
            rows = cur.fetchall()
            census[table] = {
                "rows": sum(int(n) for _, _, n in rows),
                "groups": len(rows),
                "attached_rows": sum(int(n) for sid, ver, n in rows if (sid, ver) in wanted),
            }
    return census


def _compare(baseline: dict[str, Any], candidate: dict[str, Any]) -> int:
    problems: list[str] = []

    if baseline["corpus"] != candidate["corpus"]:
        # ⚠ REFUSE, do not report a finding. The arms read different data, so
        # nothing downstream of here is attributable to the diff.
        logger.error(
            "REFUSING TO COMPARE — corpus digests differ (%s vs %s, %d vs %d bars). "
            "The two arms did not read the same input; re-run them closer together.",
            baseline["corpus"],
            candidate["corpus"],
            baseline["corpus_bars"],
            candidate["corpus_bars"],
        )
        return 2

    if baseline["eligible"] != candidate["eligible"]:
        problems.append(f"eligible population moved: {baseline['eligible']} -> {candidate['eligible']}")

    # The two PERMITTED changes, asserted to have actually happened. A rotation
    # that did NOT occur would mean the edit never reached the identity, which is
    # its own defect (prevention-log #3017).
    for name in ("indicator_series_rule_set", "price_basis_rule"):
        if baseline[name] == candidate[name]:
            problems.append(f"{name} did NOT rotate — the edit never reached the identity")

    # ⚠ The disposition that matters comes from the BASELINE arm, not this one.
    # A candidate-side census reports zero by construction.
    attached = {
        table: figures["attached_rows"] for table, figures in baseline["attachment"].items() if figures["attached_rows"]
    }
    if attached:
        # NOT a failure — a rotation that detaches live evidence is a real
        # decision, not an error. It must be stated and disposed of on the PR
        # rather than discovered after merge.
        logger.warning(
            "⚠ the rotation DETACHES stored evidence at the baseline: %s. "
            "Record the disposition on the PR before merging.",
            attached,
        )
    else:
        logger.info("baseline attachment: 0 rows on any current identity — the rotation detaches nothing")
    rotated = sum(
        1
        for sid, versions in candidate["strategy_versions"].items()
        for uni, version in versions.items()
        if baseline["strategy_versions"][sid][uni] != version
    )
    expected = len(candidate["strategy_versions"]) * len(_UNIVERSES)
    if rotated != expected:
        problems.append(f"expected all {expected} strategy identities to rotate, {rotated} did")

    missing = sorted(set(baseline["cells"]) ^ set(candidate["cells"]))
    if missing:
        problems.append(f"cell sets differ: {missing[:5]}")

    moved = sorted(k for k in baseline["cells"] if candidate["cells"].get(k) != baseline["cells"][k])
    if moved:
        problems.append(f"VERDICTS MOVED in {len(moved)} cells: {moved[:10]}")

    # Coverage. A cell that evaluated nothing proves nothing, and S-12's live
    # route short-circuits before arithmetic — the exact case that makes a green
    # sweep meaningless if it goes unchecked.
    dead = sorted(k for k, n in candidate["evaluated"].items() if n == 0)
    if dead:
        problems.append(f"{len(dead)} cells evaluated ZERO instruments: {dead[:10]}")
    if not candidate["evaluated"]:
        problems.append("no cells were evaluated at all")

    for problem in problems:
        logger.error("FAIL %s", problem)

    logger.info(
        "cells=%d evaluated_min=%d corpus=%s (%d bars) constructions=%s -> %s",
        len(candidate["cells"]),
        min(candidate["evaluated"].values()) if candidate["evaluated"] else 0,
        candidate["corpus"],
        candidate["corpus_bars"],
        baseline["constructions"],
        candidate["constructions"],
    )
    if not problems:
        logger.info("PASS — every cell byte-identical across the two commits")
    return 1 if problems else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, help="write this arm's measurement to a JSON file")
    parser.add_argument("--limit", type=int, default=0, help="stop after N instruments (0 = whole population)")
    parser.add_argument("--compare", nargs=2, type=Path, metavar=("BASELINE", "CANDIDATE"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.compare:
        baseline, candidate = (json.loads(path.read_text()) for path in args.compare)
        return _compare(baseline, candidate)

    if not args.out:
        parser.error("--out is required unless --compare is given")
    measurement = _measure(args.limit)
    args.out.write_text(json.dumps(measurement, indent=2, sort_keys=True))
    logger.info(
        "wrote %s — eligible=%d corpus=%s (%d bars) cells=%d constructions=%s",
        args.out,
        measurement["eligible"],
        measurement["corpus"],
        measurement["corpus_bars"],
        len(measurement["cells"]),
        measurement["constructions"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
