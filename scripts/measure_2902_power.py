"""#2902 power-first statement, from PRE-WINDOW published data only.

Queue rule 1 (#2437, 2026-09-26): before any #2902 arm is declared, compute power at the smallest
economically worthwhile edge; below 0.5, the arm is not run as specified. This reads Kenneth French's
Data Library (CRSP, monthly, equal-weight) and discards every month on or after the first holding month
of the #2901 harness window before any value is used, so the window's value premium never enters.

Proxies (long-only arm minus its own 1/N control, as the programme judges every arm). The control is the
firm-count-weighted average of the equal-weight portfolios that partition the universe, which is the
monthly-rebalanced equal-weight universe of those firms:

- ``all-cap value quintile``: ``Hi 20`` against the five B/M quintiles (firms with BE > 0).
- ``all-cap value tertile``: ``Hi 30`` against the three B/M tertiles. A broader tilt, reported to show
  whether breadth moves the information ratio.
- ``small-cap value``: ``SMALL HiBM`` against the five ``ME1`` portfolios of the 5x5 size x B/M sort
  (bottom NYSE size quintile; includes micro-caps eToro may not list).

Tracking error is the sample standard deviation of monthly active returns x sqrt(12) (IID, stated).
Power of the one-sided test at bar t is Phi(edge x sqrt(years) / TE - t), so it depends on the edge only
through the information ratio edge / TE; ``ir_for_half_power`` = t / sqrt(years) is the ratio at which
the expected t equals the bar. The pre-window GROSS mean active return is printed as an optimistic
reference (no costs, pre-publication years included), never as the declared edge.

Usage::

    uv run python -m scripts.measure_2902_power --be-me <Portfolios_Formed_on_BE-ME_CSV.zip> \\
        --be-me-sha256 <sha> --size-bm <25_Portfolios_5x5_CSV.zip> --size-bm-sha256 <sha>
"""

from __future__ import annotations

import argparse
import hashlib
import io
import math
import statistics
import zipfile
from pathlib import Path
from typing import Final

FIRST: Final = (1963, 7)  # Compustat-era start used by Fama-French (1993)
FIRST_WINDOW_MONTH: Final = (2013, 7)  # #2901 harness: first holding month of the 2013-06 formation
WINDOW_YEARS: Final = (134 / 12, 13.0)  # the #2901 harness window (2013-07 .. 2024-08); the programme's 13
T_BARS: Final = (3.0, 1.96)
EDGES: Final = (0.015, 0.03)  # 1.5%/yr = declared minimum worthwhile net edge; 3% sensitivity
MISSING: Final = (-99.99, -999.0)
# Universes, in header order; the arm is the last (highest B/M) member of each.
QUINTILES: Final = ("Lo 20", "Qnt 2", "Qnt 3", "Qnt 4", "Hi 20")
TERTILES: Final = ("Lo 30", "Med 40", "Hi 30")
SMALL_CAP_ROW: Final = ("SMALL LoBM", "ME1 BM2", "ME1 BM3", "ME1 BM4", "SMALL HiBM")


def power(edge: float, tracking_error: float, years: float, t_bar: float) -> float:
    """One-sided normal-approximation power of ``mean / se > t_bar`` for an annual edge."""
    return statistics.NormalDist().cdf(edge * math.sqrt(years) / tracking_error - t_bar)


def expected_months() -> list[tuple[int, int]]:
    out, (year, month) = [], FIRST
    while (year, month) < FIRST_WINDOW_MONTH:
        out.append((year, month))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return out


def read_section(path: Path, digest: str, title: str) -> dict[tuple[int, int], dict[str, float]]:
    """Rows of one titled monthly section, keyed (year, month), restricted to FIRST <= month < window."""
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != digest:
        raise SystemExit(f"{path.name} digest moved")
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        (member,) = archive.namelist()
        lines = archive.read(member).decode("latin-1").splitlines()
    starts = [i for i, line in enumerate(lines) if line.strip() == title]
    if len(starts) != 1:
        raise SystemExit(f"{path.name}: {len(starts)} sections titled {title!r}")
    header = [h.strip() for h in lines[starts[0] + 1].split(",")]
    if header[0] != "" or len(set(header[1:])) != len(header) - 1:
        raise SystemExit(f"{path.name}: malformed header under {title!r}")
    rows: dict[tuple[int, int], dict[str, float]] = {}
    for line in lines[starts[0] + 2 :]:
        cells = [c.strip() for c in line.split(",")]
        if not (cells[0].isdigit() and len(cells[0]) == 6):
            break
        key = (int(cells[0][:4]), int(cells[0][4:]))
        if not 1 <= key[1] <= 12 or key in rows:
            raise SystemExit(f"{path.name}: bad or duplicate month {cells[0]} under {title!r}")
        if key < FIRST or key >= FIRST_WINDOW_MONTH:
            continue  # never use the test window
        values = [float(c) for c in cells[1:]]
        if len(values) != len(header) - 1 or not all(math.isfinite(v) for v in values):
            raise SystemExit(f"{path.name}: ragged or non-finite row {cells[0]} under {title!r}")
        rows[key] = dict(zip(header[1:], values, strict=True))
    if sorted(rows) != expected_months():
        raise SystemExit(f"{path.name}: months under {title!r} are not the contiguous pre-window sequence")
    return rows


def active_returns(
    returns: dict[tuple[int, int], dict[str, float]],
    counts: dict[tuple[int, int], dict[str, float]],
    arm: str,
    universe: tuple[str, ...],
) -> list[float]:
    """Arm minus the count-weighted universe. The universe must be a unique, contiguous run of the section's
    own header, the arm its last member, and both sections must share the header, so a wrong-but-existing
    column refuses instead of silently re-weighting the control."""
    for section in (returns, counts):
        header = list(next(iter(section.values())))
        start = header.index(universe[0]) if universe[0] in header else -1
        if start < 0 or tuple(header[start : start + len(universe)]) != universe or arm != universe[-1]:
            raise SystemExit(f"universe {universe} is not a contiguous header run ending at {arm!r}")
    if all(v == int(v) for row in returns.values() for v in row.values()):
        raise SystemExit("every return is an integer: a count section was read as returns")
    out: list[float] = []
    for month in expected_months():
        r, n = returns[month], counts[month]
        if any(r[c] in MISSING or n[c] <= 0 or n[c] != int(n[c]) for c in universe):
            raise SystemExit(f"missing return or invalid count at {month}")
        control = sum(r[c] * n[c] for c in universe) / sum(n[c] for c in universe)
        out.append((r[arm] - control) / 100.0)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--be-me", type=Path, required=True)
    parser.add_argument("--be-me-sha256", required=True)
    parser.add_argument("--size-bm", type=Path, required=True)
    parser.add_argument("--size-bm-sha256", required=True)
    args = parser.parse_args()

    be_me_ret = read_section(args.be_me, args.be_me_sha256, "Equal Weight Returns -- Monthly")
    be_me_n = read_section(args.be_me, args.be_me_sha256, "Number of Firms in Portfolios")
    size_ret = read_section(args.size_bm, args.size_bm_sha256, "Average Equal Weighted Returns -- Monthly")
    size_n = read_section(args.size_bm, args.size_bm_sha256, "Number of Firms in Portfolios")
    proxies = {
        "all-cap value quintile": active_returns(be_me_ret, be_me_n, QUINTILES[-1], QUINTILES),
        "all-cap value tertile": active_returns(be_me_ret, be_me_n, TERTILES[-1], TERTILES),
        "small-cap value": active_returns(size_ret, size_n, SMALL_CAP_ROW[-1], SMALL_CAP_ROW),
    }
    months = expected_months()
    print(f"pre-window months {months[0]}..{months[-1]} n={len(months)}")
    for years in WINDOW_YEARS:
        for bar in T_BARS:
            print(f"window {years:.3f} years t>{bar}: information ratio for 50% power {bar / math.sqrt(years):.4f}")
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
