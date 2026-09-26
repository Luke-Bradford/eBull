"""#2903 power-first statement for the net-share-issuance leg, from PRE-WINDOW published data only.

Queue rule 1 (#2437, 2026-09-26), same method and window as ``scripts.measure_2902_power`` (whose readers
this reuses, so every month on or after 2013-07 is discarded before any value is used). The programme's
headline signal for #2903 is net share issuance (Pontiff-Woodgate); its long-only arm holds the net
repurchasers. Proxies, each against the firm-count-weighted average of the equal-weight portfolios that
partition its universe (the monthly-rebalanced equal-weight universe):

- ``all-cap repurchasers``: ``< 0`` against ``< 0``, ``ZERO`` and the five positive-NI quintiles of
  Kenneth French's ``Portfolios_Formed_on_NI`` (NI = change in log split-adjusted shares, fiscal t-2 to t-1).
- ``small-cap repurchasers``: ``SMALL NegNI`` against the seven ``ME1`` portfolios of the 5x5 size x NI sort.

Usage::

    uv run python -m scripts.measure_2903_power --ni <Portfolios_Formed_on_NI_CSV.zip> --ni-sha256 <sha> \\
        --size-ni <25_Portfolios_ME_NI_5x5_CSV.zip> --size-ni-sha256 <sha>
"""

from __future__ import annotations

import argparse
import math
import statistics
from pathlib import Path
from typing import Final

from scripts.measure_2902_power import EDGES, MISSING, T_BARS, WINDOW_YEARS, expected_months, power, read_section

ALL_CAP: Final = ("< 0", "ZERO", "Lo 20", "Qnt 2", "Qnt 3", "Qnt 4", "Hi 20")
SMALL_CAP: Final = ("SMALL NegNI", "SMALL ZeroNI", "SMALL LoNI", "ME1 NI2", "ME1 NI3", "ME1 NI4", "SMALL HiNI")


def active_returns(
    returns: dict[tuple[int, int], dict[str, float]],
    counts: dict[tuple[int, int], dict[str, float]],
    universe: tuple[str, ...],
) -> list[float]:
    """The universe's FIRST member (the repurchasers) minus the count-weighted universe. The universe must be
    the leading run of both sections' headers, so a wrong-but-existing column refuses."""
    for section in (returns, counts):
        if tuple(list(next(iter(section.values())))[: len(universe)]) != universe:
            raise SystemExit(f"universe {universe} is not the leading header run")
    if all(v == int(v) for row in returns.values() for v in row.values()):
        raise SystemExit("every return is an integer: a count section was read as returns")
    out: list[float] = []
    for month in expected_months():
        r, n = returns[month], counts[month]
        if any(r[c] in MISSING or n[c] <= 0 or n[c] != int(n[c]) for c in universe):
            raise SystemExit(f"missing return or invalid count at {month}")
        control = sum(r[c] * n[c] for c in universe) / sum(n[c] for c in universe)
        out.append((r[universe[0]] - control) / 100.0)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--ni", type=Path, required=True)
    parser.add_argument("--ni-sha256", required=True)
    parser.add_argument("--size-ni", type=Path, required=True)
    parser.add_argument("--size-ni-sha256", required=True)
    args = parser.parse_args()

    proxies = {
        "all-cap repurchasers": active_returns(
            read_section(args.ni, args.ni_sha256, "Equal Weighted Returns -- Monthly"),
            read_section(args.ni, args.ni_sha256, "Number of Firms in Portfolios"),
            ALL_CAP,
        ),
        "small-cap repurchasers": active_returns(
            read_section(args.size_ni, args.size_ni_sha256, "Average Equal Weighted Returns -- Monthly"),
            read_section(args.size_ni, args.size_ni_sha256, "Number of Firms in Portfolios"),
            SMALL_CAP,
        ),
    }
    months = expected_months()
    print(f"pre-window months {months[0]}..{months[-1]} n={len(months)}")
    for label, active in proxies.items():
        te = statistics.stdev(active) * math.sqrt(12)
        mean = statistics.fmean(active) * 12
        print(f"{label}: TE={te:.4f} gross_mean={mean:.4f} gross_IR={mean / te:.4f}")
        for edge in (*EDGES, mean):
            for years in WINDOW_YEARS:
                cells = ", ".join(f"t>{bar} {power(edge, te, years, bar):.4f}" for bar in T_BARS)
                print(f"  edge {edge:.4f} (IR {edge / te:.4f}) over {years:.3f}y: power {cells}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
