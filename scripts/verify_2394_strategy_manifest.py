"""Full-population verification of the strategy manifest (#2394 §2).

    PYTHONPATH=. uv run python scripts/verify_2394_strategy_manifest.py --all

⚠ NOTHING IS WRITTEN. Every arm reads and prints. Gate on the EXIT CODE — 0
means every arm passed, 1 means at least one did not.

⚠ NEVER PIPE THIS INTO ``head``/``tail``. A pipe returns the pipe's status, so a
failure reads as success, and it buffers the progress lines a long run is judged
by (`.claude/CLAUDE.md`).

THREE ARMS, MEASURING DIFFERENT THINGS
--------------------------------------
``--census`` — the defect, counted on this tree rather than quoted from the
issue. How many call sites name a strategy module directly, per module, and how
many strategies the manifest covers. This is the asymmetry that made every
phase-5 run report carry S-1 and S-3 figures and not S-2 and S-4.

``--identity`` — ⚠⚠ THE CLAIM MOST WORTH FALSIFYING. ``StrategyIdentity.version``
hashes the bytes of ``strategy_registry.py`` and of the defining strategy
module, so a manifest placed in either would move every stored strategy version
the moment a strategy is added. This arm diffs all five identity-bearing files
against ``origin/main`` and requires them BYTE-IDENTICAL, then prints the four
versions this tree produces. It is a proof about the change, not about the data.

``--equivalence`` — the full research corpus, every series, through the fail-
closed masked loader. For each per-series strategy the manifest's uniform call
is compared verdict-for-verdict against the module's own function, and for S-2
the staged member is compared field-for-field. The adapters absorb a keyword
difference (``close_reason`` vs ``masked_reason``); a wrapper that forwards the
wrong keyword or the wrong module's function type-checks and fails only when
called. It also reports the per-strategy verdict distribution, which is what a
manifest-driven runner would cover — the same population for all four, where an
import list covered two.

⚠ S-2's PANEL VERDICTS ARE NOT RESOLVED HERE. Ranking needs the panel's union
calendar and a cross-section per date, which ``verify_2240_s2_cross_sectional.py``
already streams. The claim made here is about the manifest's INVOCATION
contract, not about S-2's verdicts, and the two must not be pooled.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

import psycopg

from app.config import settings
from app.services.cost_model import COST_MODEL_ID
from app.services.indicator_series import BarSeries
from app.services.research_price_structure_store import load_masked_series
from app.services.strategies.s1_time_series_momentum import s1_signals
from app.services.strategies.s2_cross_sectional_momentum import s2_member
from app.services.strategies.s3_mean_reversion_in_trend import s3_signals
from app.services.strategies.s4_volatility_compression_breakout import s4_signals
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.technical_analysis import OHLCVRow

REPO = Path(__file__).resolve().parent.parent

#: The research corpus is survivor-only (#2284) and every figure below inherits
#: that label (#2288).
UNIVERSE = "survivor_only"

#: What ``load_masked_series`` means by an absent field.
MASKED_REASON = "quarantined_bar"

#: ⚠ Every file whose BYTES are inside a ``StrategyIdentity.version``: the
#: registry (via ``_module_hash``) and each strategy module (via its own
#: ``_source_hash``). Editing any of them moves stored versions.
IDENTITY_BEARING = (
    "app/services/strategy_registry.py",
    "app/services/strategies/s1_time_series_momentum.py",
    "app/services/strategies/s2_cross_sectional_momentum.py",
    "app/services/strategies/s3_mean_reversion_in_trend.py",
    "app/services/strategies/s4_volatility_compression_breakout.py",
)

#: The direct function each manifest adapter is required to agree with. ⚠ Named
#: here rather than reached through the manifest, which would compare it to
#: itself.
DIRECT_PER_SERIES = {
    "s1-time-series-momentum": s1_signals,
    "s3-mean-reversion-in-trend": s3_signals,
}


def _series_bars(bars: object) -> BarSeries:
    rows: list[OHLCVRow] = [
        {"open": b.open, "high": b.high, "low": b.low, "close": b.close, "volume": b.volume}  # type: ignore[typeddict-item, union-attr]
        for b in bars  # type: ignore[attr-defined]
    ]
    return BarSeries(dates=tuple(b.bar_date for b in bars), rows=tuple(rows))  # type: ignore[attr-defined]


def census() -> bool:
    """The import asymmetry, counted on this tree."""
    print("CENSUS — how the strategy set was decided before the manifest")
    roots = [REPO / "app", REPO / "scripts", REPO / "tests"]
    sites: Counter[str] = Counter()
    files: Counter[str] = Counter()
    pattern = re.compile(r"strategies\.(s\d+)_")
    for root in roots:
        for path in sorted(root.rglob("*.py")):
            if path.name == "strategy_manifest.py":
                continue
            text = path.read_text()
            seen: set[str] = set()
            for match in pattern.finditer(text):
                sites[match.group(1)] += 1
                seen.add(match.group(1))
            for module in seen:
                files[module] += 1

    total_sites = sum(sites.values())
    print(f"  call sites naming a strategy module directly: {total_sites}")
    for module in sorted(set(sites) | set(files)):
        print(f"    {module}: {sites[module]} sites across {files[module]} files")
    print(f"  manifest entries: {len(STRATEGY_MANIFEST)} — {', '.join(sorted(STRATEGY_MANIFEST))}")

    classes = Counter(entry.strategy_class for entry in STRATEGY_MANIFEST.values())
    print(f"  by class: {dict(sorted(classes.items()))}")
    legs = {key: sorted(entry.signal_kinds) for key, entry in sorted(STRATEGY_MANIFEST.items())}
    for key, kinds in legs.items():
        print(f"    {key}: emits {kinds}")

    spread = max(sites.values()) - min(sites.values()) if sites else 0
    print(f"  ⚠ the spread between the most- and least-imported module is {spread} call sites")
    return True


def identity() -> bool:
    """⚠⚠ No identity-bearing file may have moved. See the module docstring."""
    print("IDENTITY — does this change move any stored strategy version?")
    ok = True
    for relative in IDENTITY_BEARING:
        head = subprocess.run(
            ["git", "show", f"origin/main:{relative}"],
            cwd=REPO,
            capture_output=True,
        )
        if head.returncode != 0:
            print(f"  ✗ {relative}: not readable at origin/main")
            ok = False
            continue
        current = (REPO / relative).read_bytes()
        same = current == head.stdout
        print(f"  {'✓' if same else '✗'} {relative}: {'byte-identical to origin/main' if same else 'CHANGED'}")
        ok = ok and same

    print("  versions this tree produces:")
    for key, entry in sorted(STRATEGY_MANIFEST.items()):
        version = entry.identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID).version
        print(f"    {key}: {version}")
    if not ok:
        print("  ⚠⚠ an identity-bearing file moved — every stored signal for that strategy is now a different version")
    return ok


def equivalence() -> bool:
    """Full corpus: the manifest's call must equal the module's own."""
    print("EQUIVALENCE — the manifest's uniform call vs the module's own function")
    started = time.monotonic()
    with psycopg.connect(settings.database_url) as conn:
        series_ids = [
            int(row[0])
            for row in conn.execute("SELECT series_id FROM research_price_series ORDER BY series_id").fetchall()
        ]
        print(f"  research series {len(series_ids)}", flush=True)

        empty = 0
        bars = 0
        mismatches: list[str] = []
        verdicts: Counter[tuple[str, str, str]] = Counter()
        s2_participating = 0
        for n, series_id in enumerate(series_ids, start=1):
            masked = load_masked_series(conn, series_id)
            if not masked.bars:
                empty += 1
                continue
            series = _series_bars(masked.bars)
            bars += len(series)

            for key, direct in DIRECT_PER_SERIES.items():
                entry = STRATEGY_MANIFEST[key]
                assert entry.signals is not None
                got = entry.signals(series, universe=UNIVERSE, masked_reason=MASKED_REASON)
                want = direct(series, universe=UNIVERSE, close_reason=MASKED_REASON)
                if got != want:
                    mismatches.append(f"{key} series {series_id}")
                for signal in got:
                    verdicts[(key, signal.kind, signal.verdict)] += 1

            s4 = STRATEGY_MANIFEST["s4-volatility-compression-breakout"]
            assert s4.signals is not None
            got_s4 = s4.signals(series, universe=UNIVERSE, masked_reason=MASKED_REASON)
            if got_s4 != s4_signals(series, universe=UNIVERSE, masked_reason=MASKED_REASON):
                mismatches.append(f"s4-volatility-compression-breakout series {series_id}")
            for signal in got_s4:
                verdicts[("s4-volatility-compression-breakout", signal.kind, signal.verdict)] += 1

            s2 = STRATEGY_MANIFEST["s2-cross-sectional-momentum"]
            assert s2.member is not None
            dates = s2.decision_calendar(series.dates)
            assert dates is not None
            got_member = s2.member(series, panel_decision_dates=dates, universe=UNIVERSE, masked_reason=MASKED_REASON)
            want_member = s2_member(series, panel_rebalance_dates=dates, universe=UNIVERSE, close_reason=MASKED_REASON)
            if (
                got_member.dates != want_member.dates
                or got_member.decision_indices != want_member.decision_indices
                or got_member.score.values != want_member.score.values
                or got_member.score.not_evaluable_indices != want_member.score.not_evaluable_indices
            ):
                mismatches.append(f"s2-cross-sectional-momentum series {series_id}")
            s2_participating += len(got_member.decision_indices)

            if n % 250 == 0:
                print(f"  {n}/{len(series_ids)} series, {bars} bars ({time.monotonic() - started:.0f}s)", flush=True)

    print(f"  series with bars {len(series_ids) - empty}   (fail-closed empties: {empty})")
    print(f"  bars {bars}")
    print(f"  s2 member decision bars {s2_participating}")
    print("  verdict distribution, per strategy — the population a manifest-driven runner covers:")
    for (key, kind, verdict), count in sorted(verdicts.items()):
        print(f"    {key} {kind} {verdict}: {count}")
    print(f"  mismatches {len(mismatches)}")
    for line in mismatches[:20]:
        print(f"    {line}")
    print(f"  elapsed {time.monotonic() - started:.1f}s")
    return not mismatches


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--census", action="store_true")
    parser.add_argument("--identity", action="store_true")
    parser.add_argument("--equivalence", action="store_true")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()
    if not (args.census or args.identity or args.equivalence or args.all):
        parser.error("pick at least one arm: --census / --identity / --equivalence / --all")

    results: list[tuple[str, bool]] = []
    if args.census or args.all:
        results.append(("census", census()))
        print()
    if args.identity or args.all:
        results.append(("identity", identity()))
        print()
    if args.equivalence or args.all:
        results.append(("equivalence", equivalence()))
        print()

    for name, passed in results:
        print(f"{name}: {'PASS' if passed else 'FAIL'}")
    return 0 if all(passed for _, passed in results) else 1


if __name__ == "__main__":
    sys.exit(main())
