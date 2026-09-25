"""#3384 slice 1 — hunt admissibility census over the HELD price/dividend corpus (read-only).

Build step 2 of ``docs/proposals/ta/2026-09-25-pattern-hunt-programme.md``. No outcome return is
computed anywhere in this script: it counts bars, series, dividends and flat/zero-volume prints, and
it enumerates which calendar years every prior study already evaluated. The hunt's
discovery/validation/holdout windows are declared against these counts, not against assumptions.

Four parts:

1. **Population** — the survivorship-free universe exactly as the harness admits it
   (``universe_selection.load_universe_selection`` over ``load_validated_universe``), plus the
   harvested series it does NOT admit, so the exclusion is a counted stratum, not a silent one.
2. **Coverage by calendar year x liquidity tercile x survival stratum**, aggregated from per
   (series, calendar year) counts: bars, dividend-positive bars, NULL/zero-volume bars, NULL-open
   bars, zero-range bars (open = high = low = close), invalid bars (a non-positive price or a close/
   open outside [low, high]) and the median positive-volume daily dollar volume. Survival stratum =
   last bar within ``ALIVE_CUT_DAYS`` of capture (admitted), or the series' ``TerminationClass``.
   Calendar-year cells are DESCRIPTIVE: a full-year median uses bars after any date inside the year.
3. **Identity route** — stored links per stratum, the Form 25 register span, and #3361's linkage
   coverage floor (the constants; ``link_as_of`` is not exercised here).
4. **Contamination inventory** — stored result windows (``strategy_results_store``, grouped), the
   holdout access log (grouped), the frozen declarations, and the file-recorded studies in
   ``FILE_STUDIES`` (transcribed from the cited documents, not re-measured), rolled up per hunt
   window by date overlap. A strategy with a holdout access but no stored holdout window is listed
   as ``window_unrecorded``, never dropped.

Liquidity stratum (by construction, no published formulation applies): point-in-time shares
outstanding do not exist before #3360's 2011 start, so no size axis is available across the span.
The stratum is liquidity: the median daily close x volume over positive-volume bars (an
approximation that values the day's volume at the close; the vendor rows are ``unadjusted``), cut
into terciles among ADMITTED series with at least ``MIN_VOLUME_BARS`` positive-volume bars that year.
Terciles and the 20-bar floor are construction choices, frozen in ``CENSUS_VERSION``.

    PYTHONPATH=. uv run python -m scripts.census_3384_hunt_admissibility --out <path.json>

Writes the census JSON to ``--out`` and prints the population block.
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
from app.services.strategy_result import EVALUATION_WINDOW_START, HOLDOUT_BOUNDARY
from app.services.universe_selection import (
    ALIVE_CUT_DAYS,
    EXCHANGE_TEST_ISSUE_SYMBOLS,
    INTRADER_CAPTURE_DATE,
    SURVIVORSHIP_FREE_VENDOR,
    load_universe_selection,
)

CENSUS_VERSION: Final = "hunt-admissibility-census-v2"

#: The programme's windows (pattern-hunt-programme.md, "The rules", rule 1). Validation ends the
#: day before the repo's HOLDOUT_BOUNDARY; the holdout runs to the archive capture.
HUNT_WINDOWS: Final[tuple[tuple[str, date, date], ...]] = (
    ("pre_discovery", EVALUATION_WINDOW_START, date(1989, 12, 31)),
    ("discovery", date(1990, 1, 1), date(2008, 12, 31)),
    ("validation", date(2009, 1, 1), HOLDOUT_BOUNDARY - timedelta(days=1)),
    ("holdout", HOLDOUT_BOUNDARY, INTRADER_CAPTURE_DATE),
)

#: A series-year needs this many positive-volume bars to receive a size tercile; fewer is
#: ``unsized`` (a median over a handful of prints is not a size).
MIN_VOLUME_BARS: Final = 20

ALIVE: Final = "alive_at_capture"
#: Last bar within ALIVE_CUT_DAYS of capture but not admitted: no link to a validated eToro
#: instrument. Absence of a link is not proof the name is absent from eToro.
ALIVE_UNADMITTED: Final = "alive_unadmitted"
EXCLUDED_TEST_ISSUE: Final = "excluded_test_issue"
NOT_ADMITTED: Final = frozenset({ALIVE_UNADMITTED, EXCLUDED_TEST_ISSUE})


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


def _git_provenance() -> dict[str, Any]:
    """HEAD and whether the tree was dirty. Raises rather than recording an unknown provenance."""

    def git(*argv: str) -> str:
        return subprocess.run(["git", *argv], capture_output=True, text=True, check=True).stdout.strip()

    status = git("status", "--porcelain")
    return {"git_head": git("rev-parse", "HEAD"), "git_dirty": bool(status), "git_status": status.splitlines()}


_HARVESTED_SQL: Final = """
    SELECT series_id, vendor_symbol, first_bar, last_bar, instrument_id, cik, delisting_source,
           delisting_provision
    FROM research_price_series
    WHERE vendor = %(vendor)s AND bar_count IS NOT NULL
    ORDER BY series_id
"""

#: One row per (series, calendar year). NULL-open and zero-range bars are separate: a NULL open makes
#: the intraday return unknown, a zero-range bar makes it zero (real, or a stale print — provenance is
#: not measurable here).
_SERIES_YEAR_SQL: Final = """
    SELECT d.series_id,
           extract(year FROM d.bar_date)::int AS yr,
           count(*) AS bars,
           count(*) FILTER (WHERE d.dividend > 0) AS dividend_bars,
           count(*) FILTER (WHERE d.volume IS NULL OR d.volume <= 0) AS no_volume_bars,
           count(*) FILTER (WHERE d.open IS NULL) AS open_null_bars,
           count(*) FILTER (
               WHERE d.open = d.high AND d.high = d.low AND d.low = d.close
           ) AS zero_range_bars,
           count(*) FILTER (
               WHERE d.close <= 0 OR d.open <= 0 OR d.low <= 0 OR d.high < d.low
                  OR d.close > d.high OR d.close < d.low OR d.open > d.high OR d.open < d.low
           ) AS invalid_bars,
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
        if str(symbol).strip().upper() in EXCHANGE_TEST_ISSUE_SYMBOLS:
            return EXCLUDED_TEST_ISSUE
        if alive:
            return ALIVE_UNADMITTED
        raise RuntimeError(f"terminating series {series_id} is neither admitted nor a test issue")
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
    fields = ("bars", "dividend_bars", "no_volume_bars", "open_null_bars", "zero_range_bars", "invalid_bars")
    by_year: dict[int, list[tuple[int, dict[str, int], int, float | None]]] = defaultdict(list)
    for series_id, yr, *counts, volbars, mdv in conn.execute(_SERIES_YEAR_SQL, {"vendor": SURVIVORSHIP_FREE_VENDOR}):
        by_year[int(yr)].append(
            (
                int(series_id),
                dict(zip(fields, (int(c) for c in counts), strict=True)),
                int(volbars),
                None if mdv is None else float(mdv),
            )
        )

    cells: list[dict[str, Any]] = []
    breakpoints: dict[str, Any] = {}
    for yr in sorted(by_year):
        rows = by_year[yr]
        # Cuts over ADMITTED series only: excluded names must not move the admitted strata.
        sized = [
            mdv
            for sid, _, vb, mdv in rows
            if mdv is not None and vb >= MIN_VOLUME_BARS and strata[sid] not in NOT_ADMITTED
        ]
        cuts = _terciles(sized) if len(sized) >= 3 else None
        breakpoints[str(yr)] = None if cuts is None else {"t1_upper": cuts[0], "t2_upper": cuts[1], "n": len(sized)}
        agg: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
        for sid, counts, vb, mdv in rows:
            if cuts is None or mdv is None or vb < MIN_VOLUME_BARS:
                tier = "unsized"
            else:
                tier = "low" if mdv < cuts[0] else "mid" if mdv < cuts[1] else "high"
            c = agg[(strata[sid], tier)]
            c["series"] += 1
            c["dividend_series"] += counts["dividend_bars"] > 0
            c.update(counts)
        for (stratum, tier), c in sorted(agg.items()):
            cells.append({"year": yr, "windows": _window_of(yr), "stratum": stratum, "liquidity": tier, **c})
    return cells, breakpoints


def _terminations_by_year(harvested: list[tuple[Any, ...]], strata: dict[int, str]) -> dict[str, Counter[str]]:
    """Admitted terminating series by the calendar year of their last bar — the survivorship profile."""
    out: dict[str, Counter[str]] = defaultdict(Counter)
    for row in harvested:
        stratum = strata[int(row[0])]
        if stratum != ALIVE and stratum not in NOT_ADMITTED:
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
        c["no_stored_link"] += iid is None and cik is None and source != "sec_form25"
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
            "window_start": str(w0),
            "window_end": str(w1),
            "metric_axis_start": None if a0 is None else str(a0),
            "metric_axis_end": None if a1 is None else str(a1),
            # The span counted as seen: the metric axis where recorded, else the stored window (an
            # upper bound on what was evaluated, not a measurement of it).
            "start": str(a0 if a0 is not None else w0),
            "end": str(a1 if a1 is not None else w1),
            "rows": int(n),
        }
        for sid, ver, ns, basis, w0, w1, a0, a1, n in conn.execute(
            """
            SELECT strategy_id, strategy_version, namespace, universe_basis,
                   window_start, window_end, metric_axis_start, metric_axis_end, count(*)
            FROM strategy_results_store
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
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
        {
            "declaration_id": int(i),
            "strategy": sid,
            "version": ver,
            "purpose": purpose,
            "frozen": str(frozen),
            "has_stored_result": bool(has_result),
            "has_holdout_access": bool(has_access),
        }
        for i, sid, ver, purpose, frozen, has_result, has_access in conn.execute(
            """
            SELECT d.declaration_id, d.strategy_id, d.strategy_version, d.prereg_purpose, d.frozen_at::date,
                   EXISTS (SELECT 1 FROM strategy_results_store r
                           WHERE r.strategy_id = d.strategy_id AND r.strategy_version = d.strategy_version),
                   EXISTS (SELECT 1 FROM strategy_holdout_accesses a
                           WHERE a.strategy_id = d.strategy_id AND a.strategy_version = d.strategy_version)
            FROM strategy_preregistration_declarations d ORDER BY d.declaration_id
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
    # A holdout access with no stored holdout window: the holdout was opened, the dates it read are
    # not recorded. Listed, never dropped.
    stored_holdout = {s["study"] for s in stored if s["namespace"] == "hold_out"}
    per_window["holdout"] = sorted(
        set(per_window["holdout"])
        | {
            f"{a['strategy']}@{a['version']}:holdout_access:window_unrecorded"
            for a in accesses
            if f"{a['strategy']}@{a['version']}" not in stored_holdout
        }
    )
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
        unharvested_row = conn.execute(
            "SELECT count(*) FROM research_price_series WHERE vendor = %(v)s AND bar_count IS NULL",
            {"v": SURVIVORSHIP_FREE_VENDOR},
        ).fetchone()
        unharvested = unharvested_row[0] if unharvested_row else 0
        alive_floor = INTRADER_CAPTURE_DATE - timedelta(days=ALIVE_CUT_DAYS)
        strata = {int(row[0]): _stratum(row, admitted, alive_floor) for row in harvested}
        if {sid for sid, st in strata.items() if st not in NOT_ADMITTED} != admitted:
            raise RuntimeError("stratum assignment disagrees with the admitted set")
        cells, breakpoints = _coverage(conn, strata)
        contamination = _contamination(conn)
        identity = _identity(conn, harvested, strata)

    population = {
        "vendor": SURVIVORSHIP_FREE_VENDOR,
        "capture_date": str(INTRADER_CAPTURE_DATE),
        "alive_floor": str(alive_floor),
        "harvested": len(harvested),
        "unharvested": int(unharvested),
        "admitted": len(admitted),
        "validated_instruments": len(validated),
        "validated_ids_sha256": hashlib.sha256(",".join(map(str, sorted(validated))).encode()).hexdigest(),
        "unlinked_alive_excluded": selection.unlinked_alive_excluded,
        "exchange_test_issues_excluded": selection.exchange_test_issues_excluded,
        "linked_early_reuse_suspect": selection.linked_early_reuse_suspect,
        # No harvested series ends before this date: every name trading earlier is conditioned on
        # its series continuing to at least here.
        "earliest_last_bar": str(min(row[3] for row in harvested)),
        "strata": dict(Counter(strata.values()).most_common()),
    }
    census = {
        "census_version": CENSUS_VERSION,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        **_git_provenance(),
        "hunt_windows": [{"name": n, "start": str(s), "end": str(e)} for n, s, e in HUNT_WINDOWS],
        "min_volume_bars": MIN_VOLUME_BARS,
        "population": population,
        "coverage": cells,
        "liquidity_breakpoints": breakpoints,
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
