"""#3384 slice 1 — hunt admissibility census over the HELD price/dividend corpus (read-only).

Build step 2 of ``docs/proposals/ta/2026-09-25-pattern-hunt-programme.md``. No outcome return is
computed anywhere in this script: it counts bars, series, dividends and flat/zero-volume prints, and
it enumerates which calendar years every prior study already evaluated. The hunt's
discovery/validation/holdout windows are declared against these counts, not against assumptions.

Four parts:

1. **Population** — the survivorship-free universe exactly as the harness admits it
   (``universe_selection.load_universe_selection`` over ``load_validated_universe``), plus the
   harvested series it does NOT admit, so the exclusion is a counted stratum, not a silent one.
2. **Coverage by year x size x survival stratum** — per (series, calendar year): bars, dividend
   prints, zero/null-volume bars, flat bars (open = high = low = close, or open NULL) and the
   median daily dollar volume. Size = tercile of that median among series trading that year (see
   "Size stratum" below). Survival stratum = alive at capture, or the series' ``TerminationClass``.
3. **Identity route** — how each admitted series links to an issuer (eToro instrument, CIK, Form 25).
4. **Contamination inventory** — every stored result window (``strategy_results_store``), every
   holdout access (``strategy_holdout_accesses``), every frozen declaration, plus the file-recorded
   studies in ``FILE_STUDIES`` (each row cites its source document). Rolled up per calendar year and
   per hunt window.

Size stratum (by construction, no published formulation applies): the programme needs a size axis
before shares outstanding exist point-in-time (#3360 starts 2011), so size is proxied by median
daily DOLLAR volume (unadjusted close x unadjusted volume — the Intrader archive is unadjusted, so
the product is dollars actually traded), cut into terciles among the series with at least
``MIN_VOLUME_BARS`` positive-volume bars that calendar year. Terciles, not NYSE breakpoints: exchange
membership is not stored for the archive.

    PYTHONPATH=. uv run python -m scripts.census_3384_hunt_admissibility --out <path.json>

Prints the tables; writes the full census JSON to ``--out``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Final

import psycopg

from app.config import settings
from app.services.research_corpus_ingest import vendor_symbol_has_bankruptcy_suffix
from app.services.security_linkage import COVERAGE_START as LINKAGE_COVERAGE_START
from app.services.security_linkage import WINDOW_DAYS as LINKAGE_WINDOW_DAYS
from app.services.series_termination import TerminationEvidence, classify_termination
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_result import HOLDOUT_BOUNDARY
from app.services.universe_selection import (
    ALIVE_CUT_DAYS,
    INTRADER_CAPTURE_DATE,
    SURVIVORSHIP_FREE_VENDOR,
    load_universe_selection,
)

CENSUS_VERSION: Final = "hunt-admissibility-census-v1"

#: The programme's windows (pattern-hunt-programme.md, "The rules", rule 1). Validation ends the
#: day before the repo's HOLDOUT_BOUNDARY; the holdout runs to the archive capture.
HUNT_WINDOWS: Final[tuple[tuple[str, date, date], ...]] = (
    ("pre_discovery", date(1962, 1, 1), date(1989, 12, 31)),
    ("discovery", date(1990, 1, 1), date(2008, 12, 31)),
    ("validation", date(2009, 1, 1), HOLDOUT_BOUNDARY - timedelta(days=1)),
    ("holdout", HOLDOUT_BOUNDARY, INTRADER_CAPTURE_DATE),
)

#: A series-year needs this many positive-volume bars to receive a size tercile; fewer is
#: ``unsized`` (a median over a handful of prints is not a size).
MIN_VOLUME_BARS: Final = 20

ALIVE: Final = "alive_at_capture"
ALIVE_UNADMITTED: Final = "alive_unadmitted"
EXCLUDED_TERMINATED: Final = "excluded_terminated_test_issue"


@dataclass(frozen=True)
class FileStudy:
    """A prior study whose evaluated window is recorded in a document, not a result table."""

    study: str
    source: str
    construction: str
    start: date
    end: date
    universe: str
    note: str


#: Studies that evaluated outcomes on this corpus (or its survivor sibling) and are NOT rows of
#: ``strategy_results_store``. Each window is transcribed from the cited document; the document is
#: the authority and this table is its index.
FILE_STUDIES: Final[tuple[FileStudy, ...]] = (
    FileStudy(
        "strategy-evidence §2.8 autocorrelation term structure",
        ".claude/skills/quant/strategy-evidence.md §2.8; scripts/verify_2437_autocorrelation_term_structure.py",
        "non-overlapping 1d..3y return autocorrelation by price band, pooled then year-clustered",
        date(1962, 1, 2),
        date(2024, 9, 27),
        "survivor_only (paperswithbacktest, 7,709 series)",
        "every year of the span; short-horizon reversal (1d/5d/1mo) was the surviving finding",
    ),
    FileStudy(
        "strategy-evidence §2.8b momentum panic state",
        ".claude/skills/quant/strategy-evidence.md §2.8b",
        "SPY 2-year-negative / 1-month-positive state count (no strategy return)",
        date(1995, 1, 1),
        date(2024, 9, 30),
        "SPY only (series 7694)",
        "state frequency only; market-level, not cross-sectional",
    ),
    FileStudy(
        "#2908 R6 dilution-exclusion arm",
        "docs/proposals/ta/2026-08-24-r6-exclusion-result.md",
        "annual exclusion screen vs buy-and-hold vs identical annual 1/N",
        date(2022, 7, 1),
        date(2024, 9, 27),
        "survivorship_free",
        "holdout access recorded (r6-dilution-exclusion)",
    ),
    FileStudy(
        "#2901 quality arm (GP/A) identity gate",
        "docs/proposals/ta/2026-09-25-2901-quality-result.md",
        "monthly GP/A decile spread correlated with global-q; arm never computed past the gate",
        date(2013, 7, 1),
        date(2024, 8, 31),
        "survivorship_free",
        "GATE_FAIL; only the gate correlation was published",
    ),
    FileStudy(
        "#2827 gross-vs-net re-measurement of s1..s10",
        "var/measurements/2827_gross_vs_net_2026-08-22.txt (operator checkout); #2827",
        "the ten daily-bar TA strategies at zero cost",
        date(2022, 1, 3),
        date(2024, 9, 27),
        "survivorship_free (17,290 series)",
        "primary-2022-plus window; same axis as the stored hold_out rows",
    ),
)


def _git_head() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    except OSError, subprocess.CalledProcessError:
        return "unknown"


_HARVESTED_SQL: Final = """
    SELECT series_id, vendor_symbol, first_bar, last_bar, instrument_id, cik, delisting_source,
           delisting_provision
    FROM research_price_series
    WHERE vendor = %(vendor)s AND bar_count IS NOT NULL
    ORDER BY series_id
"""

#: One row per (series, calendar year). ``flat`` counts bars with no usable open-to-close range —
#: the overnight family's admissibility hinges on real opens.
_SERIES_YEAR_SQL: Final = """
    SELECT d.series_id,
           extract(year FROM d.bar_date)::int AS yr,
           count(*) AS bars,
           count(*) FILTER (WHERE d.dividend > 0) AS dividend_bars,
           count(*) FILTER (WHERE d.volume IS NULL OR d.volume = 0) AS no_volume_bars,
           count(*) FILTER (
               WHERE d.open IS NULL OR (d.open = d.high AND d.high = d.low AND d.low = d.close)
           ) AS flat_bars,
           count(*) FILTER (WHERE d.volume > 0) AS volume_bars,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY d.close * d.volume)
               FILTER (WHERE d.volume > 0) AS median_dollar_volume
    FROM research_price_daily d
    JOIN research_price_series s ON s.series_id = d.series_id
    WHERE s.vendor = %(vendor)s AND s.bar_count IS NOT NULL
    GROUP BY 1, 2
"""


def _stratum(row: tuple[Any, ...], admitted: set[int], alive_floor: date) -> str:
    series_id, symbol, _first, last_bar, _iid, _cik, source, provision = row
    alive = last_bar > alive_floor
    if int(series_id) not in admitted:
        return ALIVE_UNADMITTED if alive else EXCLUDED_TERMINATED
    if alive:
        return ALIVE
    return str(
        classify_termination(
            TerminationEvidence(
                linked=(source == "sec_form25"),
                provision=provision,
                q_suffix=vendor_symbol_has_bankruptcy_suffix(str(symbol)),
            )
        )
    )


def _window_of(year: int) -> list[str]:
    """Hunt windows overlapping calendar ``year`` (validation/holdout share 2021)."""
    first, last = date(year, 1, 1), date(year, 12, 31)
    return [name for name, start, end in HUNT_WINDOWS if start <= last and end >= first]


def _overlaps(a0: date, a1: date, b0: date, b1: date) -> bool:
    return a0 <= b1 and b0 <= a1


def _terciles(values: list[float]) -> tuple[float, float]:
    ordered = sorted(values)
    n = len(ordered)
    return ordered[n // 3], ordered[(2 * n) // 3]


def _coverage(conn: psycopg.Connection[Any], strata: dict[int, str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    by_year: dict[int, list[tuple[int, int, int, int, int, int, float | None]]] = defaultdict(list)
    for series_id, yr, bars, div, novol, flat, volbars, mdv in conn.execute(
        _SERIES_YEAR_SQL, {"vendor": SURVIVORSHIP_FREE_VENDOR}
    ):
        by_year[int(yr)].append(
            (
                int(series_id),
                int(bars),
                int(div),
                int(novol),
                int(flat),
                int(volbars),
                None if mdv is None else float(mdv),
            )
        )

    cells: list[dict[str, Any]] = []
    breakpoints: dict[str, Any] = {}
    for yr in sorted(by_year):
        rows = by_year[yr]
        sized = [mdv for _, _, _, _, _, vb, mdv in rows if mdv is not None and vb >= MIN_VOLUME_BARS]
        cuts = _terciles(sized) if len(sized) >= 3 else None
        breakpoints[str(yr)] = None if cuts is None else {"t1_upper": cuts[0], "t2_upper": cuts[1], "n": len(sized)}
        agg: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
        for series_id, bars, div, novol, flat, vb, mdv in rows:
            if cuts is None or mdv is None or vb < MIN_VOLUME_BARS:
                size = "unsized"
            else:
                size = "small" if mdv < cuts[0] else "mid" if mdv < cuts[1] else "large"
            c = agg[(strata[series_id], size)]
            c["series"] += 1
            c["bars"] += bars
            c["dividend_bars"] += div
            c["dividend_series"] += div > 0
            c["no_volume_bars"] += novol
            c["flat_bars"] += flat
        for (stratum, size), c in sorted(agg.items()):
            cells.append({"year": yr, "windows": _window_of(yr), "stratum": stratum, "size": size, **c})
    return cells, breakpoints


def _terminations_by_year(harvested: list[tuple[Any, ...]], strata: dict[int, str]) -> dict[str, Counter[str]]:
    """Admitted terminating series by the calendar year of their last bar — the survivorship profile."""
    out: dict[str, Counter[str]] = defaultdict(Counter)
    for row in harvested:
        stratum = strata[int(row[0])]
        if stratum not in (ALIVE, ALIVE_UNADMITTED, EXCLUDED_TERMINATED):
            out[str(row[3].year)][stratum] += 1
    return dict(sorted(out.items()))


def _identity(
    conn: psycopg.Connection[Any], harvested: list[tuple[Any, ...]], strata: dict[int, str]
) -> dict[str, Any]:
    """Stored issuer links per stratum, plus the dated routes' own bounds.

    ``research_price_series.cik`` is not the CIK route for this vendor — #3361's dated linkage bundle
    is (``security_linkage.link_as_of``), and it answers ``before_coverage`` for any D with
    D - WINDOW_DAYS < COVERAGE_START. The stored column is still counted so an empty one is visible.
    """
    per_stratum: dict[str, Counter[str]] = defaultdict(Counter)
    for series_id, _sym, _first, _last, iid, cik, source, _prov in harvested:
        c = per_stratum[strata[int(series_id)]]
        c["series"] += 1
        c["instrument_linked"] += iid is not None
        c["stored_cik"] += cik is not None
        c["form25_linked"] += source == "sec_form25"
        c["no_stored_issuer_link"] += iid is None and cik is None and source != "sec_form25"
    span = conn.execute("SELECT min(filed_date), max(filed_date), count(*) FROM sec_form25_register").fetchone()
    return {
        "per_stratum": dict(sorted(per_stratum.items())),
        "form25_register": {
            "first_filed": str(span[0]) if span else None,
            "last_filed": str(span[1]) if span else None,
            "rows": int(span[2]) if span else 0,
        },
        "cik_linkage_3361": {
            "coverage_start": str(LINKAGE_COVERAGE_START),
            "window_days": LINKAGE_WINDOW_DAYS,
            "first_linkable_date": str(LINKAGE_COVERAGE_START + timedelta(days=LINKAGE_WINDOW_DAYS)),
        },
    }


def _contamination(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    stored = [
        {
            "study": f"{sid}@{ver}",
            "source": "strategy_results_store",
            "namespace": ns,
            "universe": basis,
            "start": str(start),
            "end": str(end),
            "rows": int(n),
        }
        for sid, ver, ns, basis, start, end, n in conn.execute(
            """
            SELECT strategy_id, strategy_version, namespace, universe_basis,
                   coalesce(metric_axis_start, window_start), coalesce(metric_axis_end, window_end), count(*)
            FROM strategy_results_store
            GROUP BY 1, 2, 3, 4, 5, 6
            ORDER BY 1, 2, 3, 5
            """
        )
    ]
    accesses = [
        {"strategy": sid, "version": ver, "kind": kind, "count": int(n), "first": str(first), "last": str(last)}
        for sid, ver, kind, n, first, last in conn.execute(
            """
            SELECT strategy_id, strategy_version, access_kind, count(*), min(accessed_at)::date, max(accessed_at)::date
            FROM strategy_holdout_accesses GROUP BY 1, 2, 3 ORDER BY 5, 1
            """
        )
    ]
    declarations = [
        {"declaration_id": int(i), "strategy": sid, "version": ver, "purpose": purpose, "frozen": str(frozen)}
        for i, sid, ver, purpose, frozen in conn.execute(
            """
            SELECT declaration_id, strategy_id, strategy_version, prereg_purpose, frozen_at::date
            FROM strategy_preregistration_declarations ORDER BY declaration_id
            """
        )
    ]
    mt1_row = conn.execute("SELECT count(*) FROM strategy_mt1_trial_results").fetchone()
    files = [
        {
            "study": f.study,
            "source": f.source,
            "construction": f.construction,
            "universe": f.universe,
            "start": str(f.start),
            "end": str(f.end),
            "note": f.note,
        }
        for f in FILE_STUDIES
    ]

    # Per hunt window, by DATE overlap (a calendar year would put a 2021-09-28 holdout row into
    # validation). A strategy family is the id prefix before its first '-'.
    spans = [
        (
            f"{s['study'].split('-', 1)[0]}:{s['namespace']}:{s['universe']}",
            date.fromisoformat(s["start"]),
            date.fromisoformat(s["end"]),
        )
        for s in stored
    ] + [(f.study, f.start, f.end) for f in FILE_STUDIES]
    per_window = {
        name: sorted({label for label, s0, s1 in spans if _overlaps(s0, s1, start, end)})
        for name, start, end in HUNT_WINDOWS
    }
    return {
        "stored_results": stored,
        "holdout_accesses": accesses,
        "declarations": declarations,
        "mt1_trial_results": int(mt1_row[0]) if mt1_row else 0,
        "file_studies": files,
        "per_window": per_window,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, required=True, help="census JSON path")
    args = parser.parse_args()

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        conn.execute("SET statement_timeout = '3600s'")
        validated = load_validated_universe(conn)
        selection = load_universe_selection(conn, universe="survivorship_free", validated_ids=frozenset(validated))
        admitted = {s.series_id for s in selection.admitted}
        harvested = conn.execute(_HARVESTED_SQL, {"vendor": SURVIVORSHIP_FREE_VENDOR}).fetchall()
        alive_floor = INTRADER_CAPTURE_DATE - timedelta(days=ALIVE_CUT_DAYS)
        strata = {int(row[0]): _stratum(row, admitted, alive_floor) for row in harvested}
        if {sid for sid, st in strata.items() if st not in (ALIVE_UNADMITTED, EXCLUDED_TERMINATED)} != admitted:
            raise RuntimeError("stratum assignment disagrees with the admitted set")
        cells, breakpoints = _coverage(conn, strata)
        contamination = _contamination(conn)
        identity = _identity(conn, harvested, strata)

    population = {
        "vendor": SURVIVORSHIP_FREE_VENDOR,
        "capture_date": str(INTRADER_CAPTURE_DATE),
        "alive_floor": str(alive_floor),
        "harvested": len(harvested),
        "admitted": len(admitted),
        "validated_instruments": len(validated),
        "unlinked_alive_excluded": selection.unlinked_alive_excluded,
        "exchange_test_issues_excluded": selection.exchange_test_issues_excluded,
        "linked_early_reuse_suspect": selection.linked_early_reuse_suspect,
        # Before this date the archive holds no series that ends: every name trading earlier is
        # conditioned on surviving to at least here.
        "earliest_last_bar": str(min(row[3] for row in harvested)),
        "strata": dict(Counter(strata.values()).most_common()),
    }
    census = {
        "census_version": CENSUS_VERSION,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "git_head": _git_head(),
        "hunt_windows": [{"name": n, "start": str(s), "end": str(e)} for n, s, e in HUNT_WINDOWS],
        "min_volume_bars": MIN_VOLUME_BARS,
        "population": population,
        "coverage": cells,
        "size_breakpoints": breakpoints,
        "terminations_by_last_bar_year": _terminations_by_year(harvested, strata),
        "identity": identity,
        "contamination": contamination,
    }
    body = json.dumps(census, indent=1, sort_keys=True, default=str)
    args.out.write_text(body + "\n")
    print(f"wrote {args.out} sha256 {hashlib.sha256((body + chr(10)).encode()).hexdigest()}")
    print(json.dumps(population, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
