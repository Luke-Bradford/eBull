"""#3609 step 1 slices 3b-3c: the stage-A panel's universe, market equity, characteristics and holding returns.

Spec: ``docs/research/2026-10-04-3609-step1-factor-panel.md`` (§"Universe at M", §"Market equity",
§"Accounting", §"Price characteristics and daily data", §"Returns and holdings", §"Census"). The accounting and
ME rules live in ``app/services/factor_panel.py`` and the price rules in ``app/services/factor_panel_prices.py``;
this script supplies the DB reads and the pinned #3360 / #3361 / slice-1 artefacts, walks the 80 stage-A
formations and writes one row per (M, series) examined, admitted or excluded, plus the census.

Hold-out: every stage-A price read is bounded at ``PRICE_BOUND`` (2021-05-31); bundle and SUB reads are bounded
by s(M) <= 2021-04-30.

Stage B (#3609 step 2 slice 2, ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"Slices" item 2) runs the
same walk over formations 2021-05 .. 2024-07 with prices bounded at 2024-08-31, and adds the extended SUB artefact
to the SIC reads. It is a stage-B read, so ``publish_stage_b`` refuses unless the step 2 declaration matches this
checkout and the run's hold-out access is in its ledger and committed in the access log; it runs only through
``--publish-stage-b``, never as a scratch run. Each stage's artefact verifies under its own pin map.

Run: ``PYTHONPATH=. uv run python -m scripts.build_3609_factor_panel [--symbols AAPL,MSFT] [--formations
2019-06-30]``.
"""

from __future__ import annotations

import argparse
import dataclasses
import gzip
import hashlib
import heapq
import io
import itertools
import json
import shutil
import sys
from bisect import bisect_right
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

import psycopg

from app.config import settings
from app.services.factor_book_declaration import CodeHashes, check_declaration
from app.services.factor_book_ledger import (
    COMMITTED_LEDGER_PATH,
    LEDGER_PATH,
    end_run_failed,
    recorded_access_id,
    require_committed_access,
    run_event,
)
from app.services.factor_book_reference import STAGE_B_FIRST_FORMATION, STAGE_B_LAST_FORMATION, STAGE_B_PRICE_BOUND
from app.services.factor_panel import (
    ACCOUNTING_CHARACTERISTICS,
    DO_BRANCHES,
    FALLBACK_LABELS,
    REIT_SIC,
    STAGE_A_LAST_FORMATION,
    VETO,
    Check,
    CikView,
    MarketEquity,
    PanelError,
    PrefixCache,
    ShareReference,
    SicStatus,
    SplitStamp,
    add_months,
    characteristic,
    decision_session,
    formation_months,
    is_filer,
    market_equity,
    month_end,
    sic_as_of,
    usable_reference,
)
from app.services.factor_panel_artefact import (
    GzLines,
    construction_versions,
    fsync_dir,
    gz_content_sha256,
    import_closure,
    read_gz_lines,
    sha256_file,
    write_gz_lines,
    write_json_once,
)
from app.services.factor_panel_fidelity import PRICE_CHARACTERISTICS, append_ledger, read_ledger
from app.services.factor_panel_prices import (
    DailyBar,
    DailyMonthly,
    FormationPrices,
    PriceCharacteristic,
    ReturnBounds,
    SessionGrid,
    liquidity_terciles,
    me_discontinuity,
    series_prices,
)
from app.services.factor_panel_reference import parse_fsds_sub
from app.services.pit_fundamentals import PitFundamentalsBundle, load_pit_fundamentals
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.security_linkage import Reason, load_security_linkage
from app.services.series_termination import TERMINATION_RULE_VERSION, TerminationEvidence, classify_termination
from app.services.strategies.validated_universe import load_validated_universe
from app.services.total_return_reader import TOTAL_RETURN_SPLICE_VERSION, Month, month_of
from app.services.trial_register import TRIAL_REGISTER
from app.services.universe_selection import (
    SURVIVORSHIP_FREE_VENDOR,
    UNIVERSE_SELECTION_RULE_VERSION,
    AdmittedSeries,
    load_universe_selection,
)
from app.system.git_identity import head_commit, is_dirty

RESEARCH_ROOT: Final = Path.home() / "Library/Application Support/eBull/research"
#: Pins from the slice 1 close-out on #3609 (2026-10-04) and slice 3d-v part 1 (2026-10-05), which added
#: Amendment 2c's 23 concepts and rebuilt the linkage by replay (3d-iv part 1 added Amendment 2b's ten).
BUNDLE: Final = (
    RESEARCH_ROOT / "pit_fundamentals_3360/2026-10-05-d063343f",
    "1175f5f979ce40c3887471c00d90cbc2315dbdc05d85ef798fcd943af267ff94",
)
LINKAGE: Final = (
    RESEARCH_ROOT / "security_linkage_3361/2026-10-05-d063343f",
    "1738bc39c29b9dd5da5cc2d64cce2e28df8bc2eb8ad67b03a7fa76ecc0f7f470",
)
REFERENCE: Final = (
    RESEARCH_ROOT / "factor_panel_3609_reference/2026-10-04-f59b9578",
    "0789833c2421c71b3952ab4a7d0ae7e74771ac3eea9f1a7b3ccd3d3a35d5dc60",
)
#: Stage A: the last holding month is 2021-05; nothing after it is read. Decision sessions stay <= 2021-04-30
#: whatever this bound admits: s(M) is the last SPY session on or before M, and ``main`` refuses M > 2021-04-30.
PRICE_BOUND: Final = date(2021, 5, 31)
#: Per formation, the census lists this many largest-ME admitted rows: a threshold-free flag for scale errors
#: in filed share counts (EEFT's cover count is ~10^9 too large), which dominate any ME-weighted share.
LARGEST_ME_LISTED: Final = 5
#: Months before the first formation the daily read starts: ``ret_12_1`` at t needs month t-12's month-end.
#: The 126-session liquidity window reaches about six months back, so this covers every lookback.
DAILY_LOOKBACK_MONTHS: Final = 12
RF_DATASET: Final = "french_three_factor_daily"
RF_UNIT: Final = "decimal_return"
#: Amendment 3 (#3730): JKP ``return_cutoffs.csv``, pinned here rather than in the slice 1 reference artefact so
#: that artefact's manifest, and so ``STAGE_A_PINS``, still verify every published artefact. Loaded 2026-10-09 by
#: ``scripts.refresh_2912_reference_data --source factor_library --dataset jkp_return_cutoffs``.
RETURN_CUTOFFS: Final = {
    "dataset_key": "jkp_return_cutoffs",
    "snapshot_id": 91,
    "response_sha256": "074e09d6ea2b2a74181888d09f395775aa60c4f4bc9992d290ad9598ed01566e",
    "row_count": 15600,
}
RETURN_CUTOFF_LOW: Final = "ret_0_1"
RETURN_CUTOFF_HIGH: Final = "ret_99_9"
SPY_SYMBOL: Final = "SPY"
OUT_DIR: Final = Path("var/research/3609_step1")
REPO_ROOT: Final = Path(__file__).resolve().parents[1]
SPEC_PATH: Final = REPO_ROOT / "docs/research/2026-10-04-3609-step1-factor-panel.md"
REPORT_PATH: Final = REPO_ROOT / "scripts/report_3609_fidelity.py"
TRIAL_REGISTER_PATH: Final = "app/services/trial_register.py"
UNHASHED_SOURCES: Final = frozenset({TRIAL_REGISTER_PATH})
PUBLISH_ROOT: Final = RESEARCH_ROOT / "factor_panel_3609"
MANIFEST_SCHEMA: Final = "factor-panel-3609-v1"
MANIFEST_FILE: Final = "manifest.json"
ROWS_FILE: Final = "rows.jsonl.gz"
CENSUS_FILE: Final = "census.json"
#: Stage A's pin map, the three manifests ``verify_artefact`` has always checked; stage B adds the extended SUB's.
STAGE_A_PINS: Final = {
    "pit_fundamentals_3360": BUNDLE[1],
    "security_linkage_3361": LINKAGE[1],
    "reference_3609": REFERENCE[1],
}
STEP2_SUB_PIN: Final = "reference_3609_step2_sub"
STAGE_B_EVENT: Final = "stage_b_published"


class Step:
    """§"Universe at M" exclusion reasons, in funnel order. Linkage and ME reasons are stored as their own."""

    NOT_PRICED = "not_priced"
    # linked: every non-LINKED ``Reason`` value
    # bundle gate: ``Exclusion`` values
    NOT_FILER = "not_filer"
    REIT = "reit"
    MULTIPLE_SECURITIES = "multiple_securities"
    # ME: ``MeMissing`` values


# --------------------------------------------------------------------------- pure


def price_bound(formations: Sequence[date]) -> date:
    """The stage's price bound: 2021-05-31 for stage-A formations, 2024-08-31 for stage-B ones; a mix refuses."""
    if max(formations) <= STAGE_A_LAST_FORMATION:
        return PRICE_BOUND
    if STAGE_B_FIRST_FORMATION <= min(formations) and max(formations) <= STAGE_B_LAST_FORMATION:
        return STAGE_B_PRICE_BOUND
    raise PanelError(f"formations {min(formations)} .. {max(formations)} are not within one stage")


def chain_complete(formations: Sequence[date]) -> bool:
    """Check 4's chain runs over the run's formations only: a full stage's grid is complete, a subset is not."""
    return tuple(formations) in (formation_months(), formation_months(STAGE_B_FIRST_FORMATION, STAGE_B_LAST_FORMATION))


def stage_b_pins(step2_sub_sha256: str) -> dict[str, str]:
    return {**STAGE_A_PINS, STEP2_SUB_PIN: step2_sub_sha256}


def multiple_security_ciks(links: Mapping[int, str]) -> frozenset[str]:
    """CIKs that more than one linked, priced series maps to at one M (step 5)."""
    counts = Counter(links.values())
    return frozenset(cik for cik, n in counts.items() if n > 1)


def holding_month(formation: date) -> str:
    following = add_months(date(formation.year, formation.month, 1), 1)
    return f"{following.year:04d}-{following.month:02d}"


# --------------------------------------------------------------------------- loads


#: An admitted bar: the ``total_return_reader`` month-end semantics (coverage row, quarantine verdict
#: ``return_usable``, finite positive close and adj_close), up to the stage's price bound.
_ADMITTED_BARS = """
FROM research_price_daily d
JOIN research_price_quarantine_coverage cov
  ON cov.series_id = d.series_id
 AND cov.rule_set_version = %(quarantine_version)s
 AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
LEFT JOIN research_bar_quarantine q
  ON q.series_id = d.series_id
 AND q.bar_date = d.bar_date
 AND q.rule_set_version = %(quarantine_version)s
WHERE d.series_id = ANY(%(series_ids)s::bigint[])
  AND d.bar_date <= %(bound)s
  AND COALESCE(q.return_usable, TRUE)
  AND d.adj_close > 0 AND d.adj_close < 'Infinity'::numeric
  AND d.close > 0 AND d.close < 'Infinity'::numeric
"""
#: Admitted bars on the decision sessions.
_DECISION_BARS_SQL = (
    "SELECT d.series_id, d.bar_date, d.close" + _ADMITTED_BARS + "  AND d.bar_date = ANY(%(sessions)s::date[])\n"
)
#: Each series' first admitted bar, with no lower bound (step 2 spec §"Source rules", archive seasoning): the
#: 12-month daily window cannot supply it. A date only; a series with no admitted bar has no row.
_FIRST_BARS_SQL = (
    "SELECT d.series_id, min(d.bar_date)" + _ADMITTED_BARS + "GROUP BY d.series_id\nORDER BY d.series_id\n"
)

#: Every split stamp, quarantined bar or not (a split moves the raw close regardless), as
#: ``total_return_reader.load_split_dates`` reads them, plus the factor.
_SPLITS_SQL = """
SELECT series_id, bar_date, split_factor
FROM research_price_daily
WHERE series_id = ANY(%(series_ids)s::bigint[])
  AND split_factor IS NOT NULL
  AND split_factor <> 1
  AND bar_date <= %(bound)s
ORDER BY series_id, bar_date
"""

_SPY_SESSIONS_SQL = """
SELECT d.bar_date
FROM research_price_daily d
JOIN research_price_series s USING (series_id)
WHERE s.vendor = %(vendor)s AND s.vendor_symbol = %(symbol)s AND d.bar_date <= %(bound)s
ORDER BY d.bar_date
"""


def spy_sessions(conn: psycopg.Connection[Any], bound: date) -> list[date]:
    rows = conn.execute(
        _SPY_SESSIONS_SQL, {"vendor": SURVIVORSHIP_FREE_VENDOR, "symbol": SPY_SYMBOL, "bound": bound}
    ).fetchall()
    return [row[0] for row in rows]


def decision_bars(
    conn: psycopg.Connection[Any], series_ids: Sequence[int], sessions: Sequence[date], bound: date
) -> dict[tuple[int, date], Decimal]:
    params = {
        "series_ids": list(series_ids),
        "sessions": list(sessions),
        "bound": bound,
        "quarantine_version": QUARANTINE_RULE_SET_VERSION,
    }
    return {(int(sid), day): close for sid, day, close in conn.execute(_DECISION_BARS_SQL, params).fetchall()}


def first_bars(conn: psycopg.Connection[Any], series_ids: Sequence[int], bound: date) -> list[tuple[int, date]]:
    params = {"series_ids": list(series_ids), "bound": bound, "quarantine_version": QUARANTINE_RULE_SET_VERSION}
    return [(int(sid), day) for sid, day in conn.execute(_FIRST_BARS_SQL, params).fetchall()]


def split_stamps(conn: psycopg.Connection[Any], series_ids: Sequence[int], bound: date) -> dict[int, list[SplitStamp]]:
    out: dict[int, list[SplitStamp]] = defaultdict(list)
    for sid, day, factor in conn.execute(_SPLITS_SQL, {"series_ids": list(series_ids), "bound": bound}):
        out[int(sid)].append(SplitStamp(day, Decimal(factor)))
    return out


def series_symbols(conn: psycopg.Connection[Any], series_ids: Sequence[int]) -> dict[int, str]:
    rows = conn.execute(
        "SELECT series_id, vendor_symbol FROM research_price_series WHERE series_id = ANY(%(ids)s::bigint[])",
        {"ids": list(series_ids)},
    ).fetchall()
    return {int(sid): symbol for sid, symbol in rows}


#: Every bar in the daily window with its admission verdict, under ``_DECISION_BARS_SQL``'s predicates; stamps
#: are read on every bar, usable or not.
_DAILY_SQL = """
SELECT d.series_id, d.bar_date, d.close::float8, d.adj_close::float8, d.volume,
       (d.split_factor IS NOT NULL AND d.split_factor <> 1) OR COALESCE(d.dividend, 0) > 0 AS stamped,
       cov.series_id IS NOT NULL
         AND COALESCE(q.return_usable, TRUE)
         AND d.adj_close > 0 AND d.adj_close < 'Infinity'::numeric
         AND d.close > 0 AND d.close < 'Infinity'::numeric AS usable
FROM research_price_daily d
LEFT JOIN research_price_quarantine_coverage cov
  ON cov.series_id = d.series_id
 AND cov.rule_set_version = %(quarantine_version)s
 AND d.bar_date BETWEEN cov.first_bar AND cov.last_bar
LEFT JOIN research_bar_quarantine q
  ON q.series_id = d.series_id
 AND q.bar_date = d.bar_date
 AND q.rule_set_version = %(quarantine_version)s
WHERE d.series_id = ANY(%(series_ids)s::bigint[])
  AND d.bar_date BETWEEN %(first)s AND %(bound)s
ORDER BY d.series_id, d.bar_date
"""

_SNAPSHOT_FROM = """
FROM reference_data_observations o
JOIN reference_data_snapshots s USING (snapshot_id)
WHERE o.snapshot_id = %(snapshot_id)s
  AND s.dataset_key = %(dataset_key)s
  AND s.response_sha256 = %(response_sha256)s
"""
#: The whole snapshot is counted against the slice 1 manifest (integrity, no values read); only observations up to
#: the stage's price bound are read and frozen, so a stage-A build reads no hold-out value (§"Dates and stages").
_SNAPSHOT_COUNT_SQL = "SELECT count(*)" + _SNAPSHOT_FROM
_SNAPSHOT_SQL = (
    "SELECT o.series_key, o.observation_date, o.value::text, o.unit"
    + _SNAPSHOT_FROM
    + "  AND o.observation_date <= %(bound)s\nORDER BY o.series_key, o.observation_date\n"
)


def holding_last_sessions(formations: Iterable[date], sessions: Sequence[date]) -> dict[date, date]:
    """Each formation's holding month (t+1) mapped to its last SPY session."""
    out: dict[date, date] = {}
    for formation in formations:
        following = add_months(date(formation.year, formation.month, 1), 1)
        last_day = add_months(following, 1) - timedelta(days=1)
        i = bisect_right(sessions, last_day) - 1
        if i < 0 or sessions[i] < following:
            raise PanelError(f"no SPY session in the holding month after {formation}")
        out[formation] = sessions[i]
    return out


def reference_manifest(root: Path, expected_manifest_sha256: str) -> dict[str, Any]:
    manifest_path = root / "manifest.json"
    if sha256_file(manifest_path) != expected_manifest_sha256:
        raise PanelError(f"reference manifest digest moved: {manifest_path}")
    return json.loads(manifest_path.read_bytes())


def reference_files(manifest: Mapping[str, Any]) -> list[tuple[str, str]]:
    """(relative path, sha256) of every file the slice 1 manifest lists."""
    files = [(entry["path"], entry["sha256"]) for entry in manifest["fsds_sub"]]
    files += [(manifest[key]["path"], manifest[key]["sha256"]) for key in ("jkp_documentation", "table9_signs")]
    return files


def load_sub_sic(
    root: Path, expected_manifest_sha256: str, into: dict[str, int | None] | None = None
) -> dict[str, int | None]:
    """accession -> SUB ``sic`` over every pinned quarter, each file checked against its artefact's manifest.

    ``into`` merges a second artefact's quarters (stage B's extended SUB) into the first's, under the same refusal
    of an accession carrying two SIC codes."""
    manifest = reference_manifest(root, expected_manifest_sha256)
    out: dict[str, int | None] = {} if into is None else into
    for entry in manifest["fsds_sub"]:
        path = root / entry["path"]
        payload = path.read_bytes()
        if hashlib.sha256(payload).hexdigest() != entry["sha256"]:
            raise PanelError(f"SUB file digest moved: {path}")
        for record in parse_fsds_sub(payload, quarter=entry["quarter"]).records:
            if record.adsh in out and out[record.adsh] != record.sic:
                raise PanelError(f"SUB accession {record.adsh} carries two SIC codes")
            out[record.adsh] = record.sic
    return out


# --------------------------------------------------------------------------- frozen inputs


class Frozen:
    """File names under ``inputs/``. Every DB read lands in one of these; the build reads nothing else."""

    SESSIONS = "spy_sessions.jsonl.gz"
    SELECTION = "universe_selection.json"
    ADMITTED = "admitted.jsonl.gz"
    DECISION_BARS = "decision_bars.jsonl.gz"
    FIRST_BARS = "first_bars.jsonl.gz"
    SPLITS = "split_stamps.jsonl.gz"
    DAILY = "daily.jsonl.gz"
    REFERENCE = "reference"
    STEP2_SUB = "reference_step2_sub"

    @staticmethod
    def snapshot(dataset: str) -> str:
        return f"reference_snapshot_{dataset}.jsonl.gz"


def daily_start(formations: Sequence[date]) -> date:
    first = min(formations)
    return add_months(date(first.year, first.month, 1), -DAILY_LOOKBACK_MONTHS)


def _admitted_json(series: AdmittedSeries, symbol: str | None) -> dict[str, Any]:
    evidence = series.termination
    return {
        "series_id": series.series_id,
        "name_key": series.name_key,
        "instrument_id": series.instrument_id,
        "termination": None if evidence is None else dataclasses.asdict(evidence),
        "last_bar": None if series.last_bar is None else series.last_bar.isoformat(),
        "symbol": symbol,
    }


def _admitted_from(line: Mapping[str, Any]) -> AdmittedSeries:
    evidence = line["termination"]
    return AdmittedSeries(
        series_id=line["series_id"],
        name_key=line["name_key"],
        instrument_id=line["instrument_id"],
        termination=None if evidence is None else TerminationEvidence(**evidence),
        last_bar=None if line["last_bar"] is None else date.fromisoformat(line["last_bar"]),
    )


def _copy_pinned(source: Path, target: Path, files: Iterable[tuple[str, str]]) -> None:
    for relative, digest in files:
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / relative, destination)
        if sha256_file(destination) != digest:
            raise PanelError(f"reference file digest moved: {relative}")


def dump_inputs(
    conn: psycopg.Connection[Any],
    inputs: Path,
    *,
    symbols: frozenset[str] | None,
    formations: Sequence[date],
    step2_sub: tuple[Path, str] | None = None,
) -> None:
    """Every DB read and reference file the build uses, written into ``inputs`` (which must not exist).

    ``step2_sub`` is stage B's extended SUB artefact (root, manifest sha256), frozen beside step 1's reference."""
    inputs.mkdir(parents=True)
    bound = price_bound(formations)
    sessions = spy_sessions(conn, bound)
    # Every session is frozen; the window and the decisions use ``read_inputs``'s filtered list, so both phases
    # derive s(M) from the same sessions.
    windowed = [s for s in sessions if s >= daily_start(formations)]
    first_session = windowed[0]
    decision_days = sorted({decision_session(m, windowed) for m in formations})
    selection = load_universe_selection(
        conn, universe="survivorship_free", validated_ids=frozenset(load_validated_universe(conn))
    )
    admitted = list(selection.admitted)
    symbol_of = series_symbols(conn, [a.series_id for a in admitted])
    if symbols is not None:
        admitted = [a for a in admitted if symbol_of.get(a.series_id) in symbols]
    ids = [a.series_id for a in admitted]

    write_gz_lines(inputs / Frozen.SESSIONS, (s.isoformat() for s in sessions))
    write_json_once(
        inputs / Frozen.SELECTION,
        {
            "universe": selection.universe,
            "vendor": selection.vendor,
            "capture_date": None if selection.capture_date is None else selection.capture_date.isoformat(),
            "admitted_total": len(selection.admitted),
            "unlinked_alive_excluded": selection.unlinked_alive_excluded,
            "linked_early_reuse_suspect": selection.linked_early_reuse_suspect,
            "exchange_test_issues_excluded": selection.exchange_test_issues_excluded,
            "unharvested_excluded": selection.unharvested_excluded,
            "vendor_series_total": selection.vendor_series_total,
            "symbols_scope": None if symbols is None else sorted(symbols),
        },
    )
    write_gz_lines(inputs / Frozen.ADMITTED, (_admitted_json(a, symbol_of.get(a.series_id)) for a in admitted))
    bars = decision_bars(conn, ids, decision_days, bound)
    write_gz_lines(
        inputs / Frozen.DECISION_BARS, ([sid, day.isoformat(), str(bars[sid, day])] for sid, day in sorted(bars))
    )
    write_gz_lines(inputs / Frozen.FIRST_BARS, ([sid, day.isoformat()] for sid, day in first_bars(conn, ids, bound)))
    write_gz_lines(
        inputs / Frozen.SPLITS,
        (
            [sid, s.day.isoformat(), str(s.factor)]
            for sid, stamps in sorted(split_stamps(conn, ids, bound).items())
            for s in stamps
        ),
    )

    manifest = reference_manifest(*REFERENCE)
    pins = {**manifest["reference_snapshots"], RETURN_CUTOFFS["dataset_key"]: RETURN_CUTOFFS}
    for dataset, pinned in sorted(pins.items()):
        params = {
            "snapshot_id": pinned["snapshot_id"],
            "dataset_key": dataset,
            "response_sha256": pinned["response_sha256"],
        }
        row = conn.execute(_SNAPSHOT_COUNT_SQL, params).fetchone()
        total = 0 if row is None else row[0]
        if total != pinned["row_count"]:
            raise PanelError(f"pinned snapshot {dataset} holds {total} rows, manifest says {pinned['row_count']}")
        rows = conn.execute(_SNAPSHOT_SQL, {**params, "bound": bound}).fetchall()
        write_gz_lines(inputs / Frozen.snapshot(dataset), ([k, d.isoformat(), v, u] for k, d, v, u in rows))
    _copy_pinned(REFERENCE[0], inputs / Frozen.REFERENCE, [("manifest.json", REFERENCE[1]), *reference_files(manifest)])
    if step2_sub is not None:
        sub_manifest = reference_manifest(*step2_sub)
        _copy_pinned(
            step2_sub[0],
            inputs / Frozen.STEP2_SUB,
            [("manifest.json", step2_sub[1]), *((e["path"], e["sha256"]) for e in sub_manifest["fsds_sub"])],
        )

    params = {
        "series_ids": ids,
        "first": first_session,
        "bound": bound,
        "quarantine_version": QUARANTINE_RULE_SET_VERSION,
    }
    # A named cursor needs a transaction: on a scratch run's autocommit connection this opens one; inside a
    # publish's repeatable-read snapshot it is a savepoint, so the daily bars read the same snapshot.
    with GzLines(inputs / Frozen.DAILY) as out, conn.transaction(), conn.cursor(name="factor_panel_daily") as cur:
        cur.itersize = 100_000
        cur.execute(_DAILY_SQL, params)
        for sid, rows in itertools.groupby(cur, key=lambda row: row[0]):
            out.write([int(sid), [[day.isoformat(), *rest] for _, day, *rest in rows]])


def read_rf(inputs: Path, first: date, bound: date) -> dict[date, float]:
    """French daily RF from the frozen pinned snapshot, ``first`` .. ``bound``."""
    out: dict[date, float] = {}
    for key, day, value, unit in read_gz_lines(inputs / Frozen.snapshot(RF_DATASET)):
        observed = date.fromisoformat(day)
        if key != "RF" or not first <= observed <= bound:
            continue
        if unit != RF_UNIT:
            raise PanelError(f"RF unit {unit!r} on {day}, expected {RF_UNIT!r}")
        out[observed] = float(value)
    if not out:
        raise PanelError("frozen RF snapshot has no observation in the window")
    return out


def read_return_bounds(inputs: Path) -> dict[Month, ReturnBounds]:
    """Holding month -> JKP ``ret_0_1`` / ``ret_99_9`` from the frozen snapshot (Amendment 3).

    A month end that is not one, a unit other than a decimal return, a duplicate, or a month with only one of the
    two bounds refuses; ``ReturnBounds`` refuses a non-finite bound and ``low > high``."""
    found: dict[Month, dict[str, float]] = defaultdict(dict)
    for key, day, value, unit in read_gz_lines(inputs / Frozen.snapshot(RETURN_CUTOFFS["dataset_key"])):
        if key not in (RETURN_CUTOFF_LOW, RETURN_CUTOFF_HIGH):
            continue
        eom = date.fromisoformat(day)
        if (eom + timedelta(days=1)).day != 1:
            raise PanelError(f"return cutoff {key} dated {day}, not a month end")
        if unit != RF_UNIT:
            raise PanelError(f"return cutoff {key} unit {unit!r} on {day}, expected {RF_UNIT!r}")
        month = month_of(eom)
        if key in found[month]:
            raise PanelError(f"return cutoff {key} twice for {day}")
        try:
            found[month][key] = float(value)
        except (TypeError, ValueError) as exc:
            raise PanelError(f"return cutoff {key} on {day}: {value!r} is not a number") from exc
    out: dict[Month, ReturnBounds] = {}
    for month, pair in found.items():
        if set(pair) != {RETURN_CUTOFF_LOW, RETURN_CUTOFF_HIGH}:
            raise PanelError(f"return cutoffs for {month} hold only {sorted(pair)}")
        out[month] = ReturnBounds(month, pair[RETURN_CUTOFF_LOW], pair[RETURN_CUTOFF_HIGH])
    return out


def price_series(
    inputs: Path,
    admitted: Sequence[AdmittedSeries],
    grid: SessionGrid,
    holding_last: Mapping[date, date],
    splits: Mapping[int, Sequence[SplitStamp]],
    return_bounds: Mapping[Month, ReturnBounds],
) -> tuple[dict[int, Mapping[date, FormationPrices]], Counter[int], Counter[int]]:
    """Stream the frozen daily window series by series into each series' formation prices."""
    by_id = {a.series_id: a for a in admitted}
    prices: dict[int, Mapping[date, FormationPrices]] = {}
    flags: Counter[int] = Counter()
    excused: Counter[int] = Counter()
    for sid, rows in read_gz_lines(inputs / Frozen.DAILY):
        series = by_id[sid]
        termination = None
        if series.termination is not None:
            if series.last_bar is None:
                raise PanelError(f"terminating series {sid} has no stored last_bar")
            termination = (classify_termination(series.termination), series.last_bar)
        bars = [
            DailyBar(date.fromisoformat(day), close, adj, volume, stamped, usable)
            for day, close, adj, volume, stamped, usable in rows
        ]
        got = series_prices(
            bars,
            grid,
            holding_last_session=holding_last,
            termination=termination,
            return_bounds=return_bounds,
            split_stamps=splits.get(sid, ()),
        )
        prices[sid] = got.by_formation
        flags.update(got.flags_by_year)
        excused.update(got.excused_unexplained_by_year)
    return prices, flags, excused


# --------------------------------------------------------------------------- the walk


@dataclass(frozen=True)
class Candidate:
    """One (M, admitted series) examined at s(M)."""

    formation: date
    session: date
    series_id: int
    name_key: int
    terminating: bool
    close: Decimal | None  # None: no admitted bar on s(M)


def _base_row(c: Candidate, symbol: str | None) -> dict[str, Any]:
    return {
        "M": c.formation.isoformat(),
        "s_M": c.session.isoformat(),
        "holding_month": holding_month(c.formation),
        "series_id": c.series_id,
        "name_key": c.name_key,
        "symbol": symbol,
        # Retrospective diagnostic only (§"Census"): never a filter.
        "terminating": c.terminating,
    }


def _me_json(me: MarketEquity, close: Decimal | None) -> dict[str, Any]:
    return {
        "value": None if me.value is None else str(me.value),
        "missing": None if me.missing is None else me.missing.value,
        "close": None if close is None else str(close),
        "shares": None if me.shares is None else str(me.shares),
        "shares_scope": me.shares_scope,
        "basis": None if me.basis is None else me.basis.isoformat(),
        "split_product": None if me.split_product is None else str(me.split_product),
        "facts": [f.to_json() for f in me.facts],
        # Amendment 2: the unchecked ME of a row a check removed (contaminated), and each check's own outcome.
        "raw": None if me.raw_value is None or me.raw_value == me.value else str(me.raw_value),
        "checks": {name: outcome.value for name, outcome in me.checks.items()},
        "verified": me.verified,
    }


def _prices_json(got: FormationPrices, tercile: int | None) -> dict[str, Any]:
    def char(c: PriceCharacteristic) -> dict[str, Any]:
        missing = None if c.missing is None else c.missing.value
        return {"value": c.value, "observations": c.observations, "missing": missing}

    holding = got.holding
    return {
        "adj_close": got.adj_close,
        "ret_12_1": char(got.ret_12_1),
        "rvol_21d": char(got.rvol_21d),
        "dollar_volume": got.dollar_volume,
        "dollar_volume_bars": got.dollar_volume_bars,
        "liquidity_screened": got.liquidity_screened,
        "liquidity_tercile": tercile,
        "holding": {
            "status": holding.status.value,
            "period_return": holding.period_return,
            "end_bar": None if holding.end_bar is None else holding.end_bar.isoformat(),
            "by_arm": dict(holding.by_arm),
            "raw_by_arm": dict(holding.raw_by_arm),
            "bounds": {
                "month": f"{holding.bounds.month[0]:04d}-{holding.bounds.month[1]:02d}",
                "low": holding.bounds.low,
                "high": holding.bounds.high,
            },
        },
        "month_end_after_decision": got.month_end_after_decision,
        "daily_monthly": got.daily_monthly.value,
    }


def build_cik_rows(
    bundle: PitFundamentalsBundle,
    cik10: str,
    candidates: Sequence[tuple[Candidate, dict[str, Any]]],
    multi: Mapping[date, frozenset[str]],
    sub_sic: Mapping[str, int | None],
    splits: Mapping[int, Sequence[SplitStamp]],
    link_runs: Mapping[tuple[int, date], date] | None = None,
) -> Iterable[dict[str, Any]]:
    """Steps 3-6 and the characteristics for every linked candidate of one CIK.

    ``link_runs`` maps (series, formation) to the first formation of the series' current unbroken link to this CIK:
    check 4's reference must come from the same run, so a series that left the CIK and came back starts afresh.

    ME is computed for every name past the bundle gate, before the filer / REIT / one-security steps, so the
    census can weight those exclusions too; the funnel order is unchanged.
    """
    cache = PrefixCache(bundle, cik10, max(c.session for c, _ in candidates))
    #: Amendment 2 check 4: series -> its latest verified count; Amendment 2.1: series -> its latest admitted count
    #: that passed check 3 and was not recovered (the consensus partner). ``candidates`` are in formation order.
    references: dict[int, ShareReference] = {}
    partners: dict[int, ShareReference] = {}
    for candidate, row in candidates:
        view = CikView(bundle, cik10, candidate.session, prefixes=cache)
        if view.exclusion is not None:
            yield {**row, "exclusion": view.exclusion.value}
            continue
        sid = candidate.series_id

        def usable(held: Mapping[int, ShareReference]) -> ShareReference | None:
            return usable_reference(
                _same_run(held.get(sid), link_runs, sid, candidate.formation), candidate.formation, cik10
            )

        me = market_equity(
            view,
            candidate.close,
            splits.get(sid, ()),
            dollar_volume=row["prices"]["dollar_volume"],
            reference=usable(references),
            partner=usable(partners),
        )
        if me.value is not None and me.checks["turnover"] is Check.PASS and me.checks["scale"] is not Check.RECOVERED:
            assert me.shares is not None
            fact = me.facts[0]
            count = ShareReference(candidate.formation, candidate.session, me.shares, cik10, fact.accns, fact.key.end)
            partners[sid] = count
            if me.verified:
                references[sid] = count
        sic = sic_as_of(view.filings, sub_sic, candidate.session)
        row = {
            **row,
            "me": _me_json(me, candidate.close),
            "sic": sic.sic,
            "sic_status": sic.status.value,
            "sic_accn": sic.accn,
            "unanchored_accessions": len(view.unanchored),
        }
        if not is_filer(view.filings, candidate.session):
            yield {**row, "exclusion": Step.NOT_FILER}
        elif sic.status is SicStatus.SIC and sic.sic == REIT_SIC:
            yield {**row, "exclusion": Step.REIT}
        elif cik10 in multi[candidate.formation]:
            yield {**row, "exclusion": Step.MULTIPLE_SECURITIES}
        elif me.missing is not None:
            yield {**row, "exclusion": me.missing.value}
        else:
            chars: dict[str, Any] = {}
            for name in ACCOUNTING_CHARACTERISTICS:
                got = characteristic(name, view, candidate.formation, me.value)
                chars[name] = {
                    "value": got.value,
                    "missing": None if got.missing is None else got.missing.value,
                    "period_end": None if got.period_end is None else got.period_end.isoformat(),
                    "kind": None if got.kind is None else got.kind.value,
                    "branches": list(got.branches),
                    "periods_tested": got.candidates_tested,
                    "vetoes": list(got.vetoes),
                    "facts": [f.to_json() for f in got.facts],
                }
                # Only Amendment 2c's bound records guard reads; the key is absent when there are none, so no other
                # characteristic's row changes shape.
                if got.guards:
                    chars[name]["guards"] = [f.to_json() for f in got.guards]
            yield {**row, "exclusion": None, "characteristics": chars}


def _same_run(
    reference: ShareReference | None, link_runs: Mapping[tuple[int, date], date] | None, sid: int, formation: date
) -> ShareReference | None:
    if reference is None or link_runs is None:
        return reference
    return reference if reference.formation >= link_runs.get((sid, formation), formation) else None


def _known_me(row: Mapping[str, Any]) -> float | None:
    me = row.get("me")
    return None if me is None or me["value"] is None else float(me["value"])


def _add_vetoes(tally: Counter[str], vetoes: Iterable[str]) -> None:
    for companion, n in Counter(v.split(":", 1)[0].removeprefix(VETO) for v in vetoes).items():
        tally[f"veto evaluations: {companion}"] += n
        tally[f"name-months with a veto: {companion}"] += 1


class Census:
    """Built row by row. Every ME share is within one formation (a cross-section), never pooled across years."""

    def __init__(self) -> None:
        self.funnel: dict[str, Counter[str]] = defaultdict(Counter)
        self.funnel_me: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self.chars: dict[str, dict[str, Counter[str]]] = defaultdict(lambda: defaultdict(Counter))
        self.chars_me: dict[str, dict[str, dict[str, float]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(float))
        )
        #: Per characteristic, name-months per branch label; a label's ``:``-suffix (an interval) is dropped. Only the
        #: ``zero_*`` and ``veto_*`` labels carry one, so only their keys differ from slice 3d-iv part 2's census,
        #: which keyed each interval apart; ``branch_use`` is read by no code (``git grep branch_use``).
        self.branches: dict[str, Counter[str]] = defaultdict(Counter)
        #: Amendment 2b's ``ni_me`` / ``ocf_me`` counts (spec §"Slices" 3d-iv), in the measurement's terms.
        self.fallback: dict[str, Counter[str]] = defaultdict(Counter)
        #: Every other characteristic's refused zeros (Amendment 2c's ``ope_be`` / ``gp_at``), in the same terms.
        self.vetoes: dict[str, Counter[str]] = defaultdict(Counter)
        self.kinds: dict[str, Counter[str]] = defaultdict(Counter)
        self.sic_status: Counter[str] = Counter()
        self.shares_scope: Counter[str] = Counter()
        self.largest_me: dict[str, list[tuple[float, str]]] = defaultdict(list)
        #: Admitted-row price diagnostics per formation: holding status, liquidity tercile, daily-monthly check.
        self.diagnostics: dict[str, dict[str, Counter[str]]] = defaultdict(lambda: defaultdict(Counter))
        self.diagnostics_me: dict[str, dict[str, dict[str, float]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(float))
        )
        self.unexplained_daily_monthly: list[dict[str, Any]] = []
        #: series -> M -> (ME, adj_close at s(M), admitted, label), for every row with a known ME.
        self.me_points: dict[int, dict[str, tuple[float, float, bool, str]]] = defaultdict(dict)
        #: The same on unchecked ME (Amendment 2: ``me.raw`` where a check removed the value), and which check did.
        self.me_points_raw: dict[int, dict[str, tuple[float, float, bool, str]]] = defaultdict(dict)
        self.me_removed_by: dict[int, dict[str, str]] = defaultdict(dict)
        self.check_outcomes: dict[str, Counter[str]] = defaultdict(Counter)
        self.removed_rows: Counter[str] = Counter()
        self.removed_names: dict[str, set[int]] = defaultdict(set)
        self.removed_raw_me: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self.raw_me_total: dict[str, float] = defaultdict(float)
        self.turnover_failures_screened = 0

    def add(self, row: Mapping[str, Any]) -> None:
        m, reason = row["M"], row["exclusion"] or "admitted"
        self.funnel[m][reason] += 1
        me = _known_me(row)
        if me is not None:
            self.funnel_me[m][reason] += me
        if "sic_status" in row:
            self.sic_status[row["sic_status"]] += 1
        label = row.get("symbol") or f"series:{row['series_id']}"
        admitted = row["exclusion"] is None
        if me is not None:
            self.me_points[row["series_id"]][m] = (me, row["prices"]["adj_close"], admitted, label)
        self._add_checks(row, m, me, admitted, label)
        if not admitted:
            return
        if me is None:
            raise PanelError(f"admitted row without ME: {row['M']} series {row.get('series_id')}")
        self.shares_scope[row["me"]["shares_scope"]] += 1
        heapq.heappush(self.largest_me[m], (me, label))
        if len(self.largest_me[m]) > LARGEST_ME_LISTED:
            heapq.heappop(self.largest_me[m])
        for name, got in row["characteristics"].items():
            outcome = got["missing"] or "value"
            self.chars[m][name][outcome] += 1
            self.chars_me[m][name][outcome] += me
            labels = {b.split(":", 1)[0] for b in got["branches"]}
            self.branches[name].update(labels)
            if got["missing"] is None:
                self.kinds[name][got["kind"]] += 1
            if name in FALLBACK_LABELS:
                self._add_fallback(name, got, labels)
            else:
                _add_vetoes(self.vetoes[name], got["vetoes"])
        prices = row["prices"]
        for name in PRICE_CHARACTERISTICS:
            outcome = prices[name]["missing"] or "value"
            self.chars[m][name][outcome] += 1
            self.chars_me[m][name][outcome] += me
        tercile = prices["liquidity_tercile"]
        for kind, outcome in (
            ("holding_status", prices["holding"]["status"]),
            ("liquidity_tercile", "unclassified" if tercile is None else str(tercile)),
            ("daily_monthly", prices["daily_monthly"]),
            ("month_end_after_decision", str(prices["month_end_after_decision"]).lower()),
        ):
            self.diagnostics[m][kind][outcome] += 1
            self.diagnostics_me[m][kind][outcome] += me
        if prices["daily_monthly"] == DailyMonthly.UNEXPLAINED:
            self.unexplained_daily_monthly.append({"M": m, "series_id": row["series_id"], "symbol": label})

    def _add_fallback(self, name: str, got: Mapping[str, Any], labels: set[str]) -> None:
        """Vetoes on every admitted name-month; branch and imputed-zero counts on values that use the fallback."""
        primary, fallback, companions = FALLBACK_LABELS[name]
        tally = self.fallback[name]
        _add_vetoes(tally, got["vetoes"])
        if got["missing"] is not None or fallback not in labels:
            return
        tally["values using the fallback"] += 1
        if primary in labels:
            tally["TTM mixing primary and fallback quarters"] += 1
        for companion in companions:
            if f"zero_{companion}" in labels:
                tally[f"imputed zero: {companion}"] += 1
        for label in labels & set(DO_BRANCHES):
            tally[f"DO branch: {label}"] += 1

    def _add_checks(self, row: Mapping[str, Any], m: str, me: float | None, admitted: bool, label: str) -> None:
        """Amendment 2 census: per-check outcomes, and per removal reason its rows, names and raw-ME share."""
        got = row.get("me")
        if got is None:
            return
        for name, outcome in got.get("checks", {}).items():
            self.check_outcomes[name][outcome] += 1
        raw = me if got.get("raw") is None else float(got["raw"])
        if raw is None:
            return
        sid = row["series_id"]
        self.raw_me_total[m] += raw
        unchecked_admitted = admitted or (me is None and row["exclusion"] == got["missing"])
        self.me_points_raw[sid][m] = (raw, row["prices"]["adj_close"], unchecked_admitted, label)
        if me is None:
            reason = got["missing"]
            self.me_removed_by[sid][m] = reason
            self.removed_rows[reason] += 1
            self.removed_names[reason].add(sid)
            self.removed_raw_me[reason][m] += raw
            if reason == "shares_turnover_implausible" and row["prices"].get("liquidity_screened"):
                self.turnover_failures_screened += 1

    def checks_json(self) -> dict[str, Any]:
        return {
            "outcomes": {name: dict(sorted(c.items())) for name, c in sorted(self.check_outcomes.items())},
            "removed": {
                reason: {
                    "rows": n,
                    "names": len(self.removed_names[reason]),
                    "raw_me_share_contaminated_by_formation": {
                        m: v / self.raw_me_total[m] for m, v in sorted(self.removed_raw_me[reason].items())
                    },
                }
                for reason, n in sorted(self.removed_rows.items())
            },
            "turnover_failures_with_a_screened_liquidity_bar": self.turnover_failures_screened,
        }

    def to_json(self) -> dict[str, Any]:
        def shares(counts: Counter[str], weights: Mapping[str, float]) -> dict[str, Any]:
            total = sum(weights.values())
            return {
                key: {"count": n, "me_share": (weights[key] / total) if key in weights and total else None}
                for key, n in sorted(counts.items())
            }

        pooled: dict[str, Counter[str]] = defaultdict(Counter)
        for per_name in self.chars.values():
            for name, counts in per_name.items():
                pooled[name].update(counts)
        return {
            "funnel_by_formation": {m: shares(c, self.funnel_me[m]) for m, c in sorted(self.funnel.items())},
            "funnel_total_counts": dict(sorted(sum(self.funnel.values(), Counter()).items())),
            "characteristics_by_formation": {
                m: {name: shares(c, self.chars_me[m][name]) for name, c in sorted(per.items())}
                for m, per in sorted(self.chars.items())
            },
            "characteristics_total_counts": {name: dict(sorted(c.items())) for name, c in sorted(pooled.items())},
            "branch_use": {name: dict(sorted(c.items())) for name, c in sorted(self.branches.items())},
            "fallback_use": {name: dict(sorted(c.items())) for name, c in sorted(self.fallback.items())},
            "veto_use": {name: dict(sorted(c.items())) for name, c in sorted(self.vetoes.items()) if c},
            "period_kind": {name: dict(sorted(c.items())) for name, c in sorted(self.kinds.items())},
            "sic_status": dict(sorted(self.sic_status.items())),
            "shares_scope": dict(sorted(self.shares_scope.items())),
            "largest_me_by_formation": {
                m: [{"symbol": symbol, "me": me} for me, symbol in sorted(rows, reverse=True)]
                for m, rows in sorted(self.largest_me.items())
            },
            "diagnostics_by_formation": {
                m: {kind: shares(c, self.diagnostics_me[m][kind]) for kind, c in sorted(per.items())}
                for m, per in sorted(self.diagnostics.items())
            },
            "diagnostics_total_counts": {
                kind: dict(sorted(sum((per[kind] for per in self.diagnostics.values()), Counter()).items()))
                for kind in sorted({k for per in self.diagnostics.values() for k in per})
            },
            "daily_monthly_unexplained": self.unexplained_daily_monthly,
        }


def me_reconciliation(
    points: Mapping[int, Mapping[str, tuple[float, float, bool, str]]],
    decisions: Mapping[date, date],
    splits: Mapping[int, Sequence[SplitStamp]],
) -> dict[str, Any]:
    """§"Market equity": the discontinuity census and the split reconciliation, over consecutive formations.

    A pair is two consecutive formations at which the series has a known ME. Its relative move is the ME ratio
    over the ``adj_close`` ratio between the two decision sessions. Every pair whose later row is admitted is a
    panel name-month for the discontinuity census; a pair with a split stamp in ``(s(prev), s(cur)]`` and either
    side admitted is in the split reconciliation.
    """
    formations = sorted(decisions)
    pairs = flagged_pairs = split_pairs = 0
    flagged: list[dict[str, Any]] = []
    split_failures: list[dict[str, Any]] = []
    flagged_by_year: Counter[str] = Counter()
    for sid, per in points.items():
        stamp_dates = [s.day for s in splits.get(sid, ())]
        for prev, cur in itertools.pairwise(formations):
            before, after = per.get(prev.isoformat()), per.get(cur.isoformat())
            if before is None or after is None or month_end(add_months(prev, 1)) != cur:
                continue  # a --formations subset can skip months; only calendar-consecutive pairs are monthly
            relative = me_discontinuity(after[0] / before[0], after[1] / before[1])
            split = bisect_right(stamp_dates, decisions[cur]) > bisect_right(stamp_dates, decisions[prev])
            entry = {"M": cur.isoformat(), "series_id": sid, "symbol": after[3], "relative": relative}
            if after[2]:
                pairs += 1
                if relative is not None:
                    flagged_pairs += 1
                    flagged_by_year[cur.isoformat()[:4]] += 1
                    flagged.append(entry)
            if split and (before[2] or after[2]):
                split_pairs += 1
                if relative is not None:
                    split_failures.append(entry)
    return {
        "discontinuity": {
            "admitted_pairs": pairs,
            "flagged": flagged_pairs,
            "flagged_by_year": dict(sorted(flagged_by_year.items())),
            "flagged_name_months": sorted(flagged, key=lambda e: (e["M"], e["series_id"])),
        },
        "split_reconciliation": {
            "pairs_with_a_stamp": split_pairs,
            "failures": sorted(split_failures, key=lambda e: (e["M"], e["series_id"])),
        },
    }


def split_failure_dispositions(
    raw_failures: Sequence[Mapping[str, Any]],
    final: Mapping[str, Any],
    removed_by: Mapping[int, Mapping[str, str]],
    decisions: Mapping[date, date],
) -> list[dict[str, Any]]:
    """Amendment 2: every split-reconciliation failure on unchecked ME, with its status on final ME."""
    still = {(e["series_id"], e["M"]) for e in final["split_reconciliation"]["failures"]}
    previous = {cur.isoformat(): prev.isoformat() for prev, cur in itertools.pairwise(sorted(decisions))}
    out: list[dict[str, Any]] = []
    for entry in raw_failures:
        sid, m = entry["series_id"], entry["M"]
        sides = [k for k in (previous.get(m), m) if k is not None]
        reasons = sorted({r for k in sides if (r := removed_by.get(sid, {}).get(k)) is not None})
        status = "still_failing" if (sid, m) in still else "unavailable" if reasons else "reconciled"
        out.append({**entry, "status": status, "removed_by": reasons})
    return out


@dataclass(frozen=True)
class Inputs:
    """Everything read from the DB, so the connection closes before the CPU-bound walk."""

    decisions: Mapping[date, date]
    admitted: Sequence[AdmittedSeries]
    symbols: Mapping[int, str]
    bars: Mapping[tuple[int, date], Decimal]
    splits: Mapping[int, Sequence[SplitStamp]]
    #: series -> formation -> its price quantities, for every formation the series is priced at.
    prices: Mapping[int, Mapping[date, FormationPrices]]
    #: formation -> series -> liquidity tercile, among the loaded names classified at that formation.
    terciles: Mapping[date, Mapping[int, int]]
    flags_by_year: Counter[int]
    excused_unexplained_by_year: Counter[int]


def read_inputs(inputs: Path, formations: Sequence[date]) -> Inputs:
    """``Inputs`` from a frozen ``inputs/`` directory only."""
    first_day = daily_start(formations)
    sessions = [d for d in map(date.fromisoformat, read_gz_lines(inputs / Frozen.SESSIONS)) if d >= first_day]
    decisions = {m: decision_session(m, sessions) for m in formations}
    lines = list(read_gz_lines(inputs / Frozen.ADMITTED))
    admitted = [_admitted_from(line) for line in lines]
    symbol_of = {line["series_id"]: line["symbol"] for line in lines if line["symbol"] is not None}
    bars = {
        (sid, date.fromisoformat(day)): Decimal(close)
        for sid, day, close in read_gz_lines(inputs / Frozen.DECISION_BARS)
    }
    splits: dict[int, list[SplitStamp]] = defaultdict(list)
    for sid, day, factor in read_gz_lines(inputs / Frozen.SPLITS):
        splits[sid].append(SplitStamp(date.fromisoformat(day), Decimal(factor)))
    grid = SessionGrid.build(sessions, read_rf(inputs, first_day, price_bound(formations)), decisions)
    prices, flags, excused = price_series(
        inputs, admitted, grid, holding_last_sessions(formations, sessions), splits, read_return_bounds(inputs)
    )
    # The two reads share their admission predicates: a decision bar the stream does not price is a drift.
    priced = {(sid, decisions[m]) for sid, per in prices.items() for m in per}
    if priced != set(bars):
        raise PanelError(f"daily stream and decision bars disagree on {len(priced ^ set(bars))} (series, s(M)) pairs")
    name_key = {a.series_id: a.name_key for a in admitted}
    terciles = {
        m: liquidity_terciles({sid: per[m].dollar_volume for sid, per in prices.items() if m in per}, name_key)
        for m in formations
    }
    return Inputs(decisions, admitted, symbol_of, bars, splits, prices, terciles, flags, excused)


def walk(inputs: Inputs, reference: Path, step2_sub: tuple[Path, str] | None = None) -> Iterator[dict[str, Any]]:
    """Every (M, admitted series) row, in formation order for steps 1-2 and then CIK by CIK.

    ``reference`` is the frozen copy of the slice 1 artefact, and ``step2_sub`` the frozen copy of stage B's
    extended SUB with its manifest sha256; the #3360 bundle and #3361 linkage are published artefacts read in
    place, pinned by manifest digest.
    """
    bundle = load_pit_fundamentals(BUNDLE[0], expected_manifest_sha256=BUNDLE[1])
    # The shard cache is private to ``pit_fundamentals``, a hashed policy file (#3360 and #3361 manifests), so it
    # gains no public eviction method here. Resolve it once and refuse if it moved, so memory bounding cannot stop
    # silently.
    shard_cache = getattr(bundle, "_cache", None)
    if not isinstance(shard_cache, dict):
        raise PanelError("PitFundamentalsBundle._cache moved: per-CIK shard eviction would silently stop")
    linkage = load_security_linkage(LINKAGE[0], expected_manifest_sha256=LINKAGE[1])
    sub_sic = load_sub_sic(reference, REFERENCE[1])
    if step2_sub is not None:
        load_sub_sic(*step2_sub, into=sub_sic)
    print(
        f"admitted series {len(inputs.admitted)}; decision bars {len(inputs.bars)}; SUB accessions {len(sub_sic)}",
        flush=True,
    )
    by_cik: dict[str, list[tuple[Candidate, dict[str, Any]]]] = defaultdict(list)
    multi: dict[date, frozenset[str]] = {}
    # Amendment 2 check 4: per series, the CIK of its current priced-link run and the run's first formation.
    run: dict[int, tuple[str | None, date]] = {}
    link_runs: dict[tuple[int, date], date] = {}
    for formation, session in sorted(inputs.decisions.items()):
        links: dict[int, str] = {}
        for series in inputs.admitted:
            close = inputs.bars.get((series.series_id, session))
            candidate = Candidate(
                formation, session, series.series_id, series.name_key, series.termination is not None, close
            )
            row = _base_row(candidate, inputs.symbols.get(series.series_id))
            if close is None:
                yield {**row, "exclusion": Step.NOT_PRICED}
                continue
            tercile = inputs.terciles[formation].get(series.series_id)
            row["prices"] = _prices_json(inputs.prices[series.series_id][formation], tercile)
            link = linkage.link_as_of(series.series_id, session)
            row = {**row, "link_reason": link.reason.value, "link_basis": link.basis, "cik": link.cik}
            linked = link.cik if link.reason is Reason.LINKED else None
            current = run.get(series.series_id)
            if current is None or current[0] != linked:
                run[series.series_id] = current = (linked, formation)
            link_runs[(series.series_id, formation)] = current[1]
            if link.reason is not Reason.LINKED:
                yield {**row, "exclusion": link.label}
                continue
            assert link.cik is not None
            links[series.series_id] = link.cik
            by_cik[link.cik].append((candidate, row))
        multi[formation] = multiple_security_ciks(links)
    print(f"linked candidates {sum(len(v) for v in by_cik.values())} over {len(by_cik)} CIKs", flush=True)
    for done, (cik10, candidates) in enumerate(sorted(by_cik.items()), start=1):
        candidates.sort(key=lambda item: item[0].formation)
        yield from build_cik_rows(bundle, cik10, candidates, multi, sub_sic, inputs.splits, link_runs)
        shard_cache.pop(cik10, None)  # bound memory: each CIK's shard is read once, by this loop only
        if done % 500 == 0:
            print(f"  {done}/{len(by_cik)} CIKs", flush=True)


def build(
    inputs: Path,
    formations: Sequence[date],
    rows_path: Path,
    census_path: Path,
    step2_sub_sha256: str | None = None,
) -> dict[str, Any]:
    """Rows and census from a frozen ``inputs/``; returns the census summary plus the rows count.

    ``step2_sub_sha256`` is stage B's: the extended SUB is read from ``inputs/`` under that manifest digest."""
    loaded = read_inputs(inputs, formations)
    step2_sub = None if step2_sub_sha256 is None else (inputs / Frozen.STEP2_SUB, step2_sub_sha256)
    tally = Census()
    with GzLines(rows_path) as handle:
        for row in walk(loaded, inputs / Frozen.REFERENCE, step2_sub):
            tally.add(row)
            handle.write(row)
        count = handle.count
    summary = tally.to_json()
    summary["me_reconciliation"] = me_reconciliation(tally.me_points, loaded.decisions, loaded.splits)
    raw = me_reconciliation(tally.me_points_raw, loaded.decisions, loaded.splits)
    summary["me_reconciliation_raw_contaminated"] = raw
    summary["split_failure_dispositions"] = split_failure_dispositions(
        raw["split_reconciliation"]["failures"], summary["me_reconciliation"], tally.me_removed_by, loaded.decisions
    )
    summary["me_checks"] = tally.checks_json()
    # Check 4's chain runs over the run's formations only: a subset run's rows are diagnostics (Amendment 2).
    summary["chain_complete"] = chain_complete(formations)
    summary["daily_screen_flags_by_year"] = dict(sorted(loaded.flags_by_year.items()))
    summary["daily_screen_excused_unexplained_by_year"] = dict(sorted(loaded.excused_unexplained_by_year.items()))
    write_json_once(census_path, summary)
    return {"summary": summary, "rows": count}


def construction_sources() -> dict[str, str]:
    """The builder, the fidelity report and every ``app``/``scripts`` module they import, transitively, except the
    trial register: it records these versions, so hashing it into them would be a fixed point (slice 4 plan).
    Its imports are still followed, and the report's ledger records its sha256 on every run."""
    return import_closure([Path(__file__), REPORT_PATH], REPO_ROOT, unhashed=UNHASHED_SOURCES)


def _clean_head() -> str:
    if is_dirty() is not False:
        raise PanelError("refusing to publish from a dirty (or unreadable) checkout")
    head = head_commit()
    if head is None:
        raise PanelError("refusing to publish without a readable HEAD")
    return head


def _publish_artefact(
    out: Path,
    formations: Sequence[date],
    *,
    head: str,
    stage: str,
    pins: Mapping[str, str],
    versions: Callable[[], dict[str, Any]],
    step2_sub: tuple[Path, str] | None = None,
    provenance: Mapping[str, Any] | None = None,
) -> None:
    """Inputs frozen first under one repeatable-read snapshot, then the build, the manifest last.

    ``versions`` returns the spec and construction hashes the manifest records; it runs before and after the build
    and a change refuses. ``out`` must already exist and be empty; the caller removes it on failure.

    Stage A's earlier publish used autocommit reads. That this snapshot reproduces it is checked by publishing to a
    scratch root (``--publish --publish-root /tmp/x``) and comparing its manifest with the pinned stage-A artefact:
    ``inputs``, ``rows.content_sha256`` and ``census.sha256`` must be equal; only ``construction_versions`` move
    (#3685).
    """
    before = versions()  # before the run: what is hashed is what ran
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.read_only = True
        with conn.transaction():
            dump_inputs(conn, out / "inputs", symbols=None, formations=formations, step2_sub=step2_sub)
    result = build(
        out / "inputs", formations, out / ROWS_FILE, out / CENSUS_FILE, None if step2_sub is None else step2_sub[1]
    )
    if versions() != before:
        raise PanelError("construction sources or the spec changed during the run")
    manifest = {
        "schema": MANIFEST_SCHEMA,
        "stage": stage,
        "git_sha": head,
        "published_at": datetime.now(UTC).isoformat(),
        **dict(provenance or {}),
        "pinned_manifests": dict(pins),
        "formations": [m.isoformat() for m in formations],
        "inputs": {
            p.relative_to(out).as_posix(): sha256_file(p) for p in sorted((out / "inputs").rglob("*")) if p.is_file()
        },
        "rule_versions": {
            "TOTAL_RETURN_SPLICE_VERSION": TOTAL_RETURN_SPLICE_VERSION,
            "UNIVERSE_SELECTION_RULE_VERSION": UNIVERSE_SELECTION_RULE_VERSION,
            "TERMINATION_RULE_VERSION": TERMINATION_RULE_VERSION,
            "QUARANTINE_RULE_SET_VERSION": QUARANTINE_RULE_SET_VERSION,
        },
        **before,
        "rows": {
            "path": ROWS_FILE,
            "count": result["rows"],
            "sha256": sha256_file(out / ROWS_FILE),
            "content_sha256": gz_content_sha256(out / ROWS_FILE),
        },
        "census": {"path": CENSUS_FILE, "sha256": sha256_file(out / CENSUS_FILE)},
    }
    write_json_once(out / MANIFEST_FILE, manifest)
    fsync_dir(out)


def _stage_a_versions() -> dict[str, Any]:
    sources = construction_sources()
    spec_sha256 = sha256_file(SPEC_PATH)
    return {
        "spec_sha256": spec_sha256,
        "construction_sources": sources,
        "construction_versions": construction_versions(
            [*ACCOUNTING_CHARACTERISTICS, *PRICE_CHARACTERISTICS], spec_sha256, sources
        ),
    }


def publish(root: Path, formations: Sequence[date]) -> Path:
    """§"Panel artefact": exclusive directory, inputs frozen first, manifest last; a dirty checkout is refused."""
    head = _clean_head()
    out = root / f"{datetime.now(UTC).date().isoformat()}-{head[:8]}-stageA"
    root.mkdir(parents=True, exist_ok=True)
    out.mkdir()  # exclusive: an existing artefact directory is refused, never resumed
    try:
        _publish_artefact(out, formations, head=head, stage="A", pins=STAGE_A_PINS, versions=_stage_a_versions)
    except BaseException:
        # ``out`` was created by the exclusive mkdir above, so it holds only this build's output.
        shutil.rmtree(out, ignore_errors=True)
        raise
    return out


def publish_stage_b(
    root: Path,
    run_id: str,
    *,
    confirm_access: Callable[[str, int], None],
    ledger: Path = LEDGER_PATH,
    committed_ledger: Path = COMMITTED_LEDGER_PATH,
) -> tuple[Path, str]:
    """Step 2 slice 2: the stage-B artefact, built only inside the declared run; returns it and its manifest sha256.

    Refuses before any stage-B read unless the step 2 declaration matches this checkout, the run's ledger holds
    ``started``, ``access_recorded`` and ``sub_published`` with no ``stage_b_published`` or terminal row, and the
    access row is committed. The extended SUB is the run's own ``sub_published`` artefact. Any failure after the
    gate ends the run with a ``failed`` row.
    """
    head = _clean_head()
    rows = read_ledger(committed_ledger, ledger)
    check_declaration(TRIAL_REGISTER, rows, CodeHashes.current())
    access_id = recorded_access_id(rows, run_id, before=STAGE_B_EVENT)
    published_sub = run_event(rows, run_id, "sub_published")
    step2_sub = (Path(published_sub["artefact"]), str(published_sub["manifest_sha256"]))
    confirm_access(run_id, access_id)
    out = root / f"{datetime.now(UTC).date().isoformat()}-{head[:8]}-stageB-{run_id}"
    root.mkdir(parents=True, exist_ok=True)
    out.mkdir()  # exclusive: an existing artefact directory is refused, never resumed
    try:
        # The first stage-B file read, after every gate above: a SUB artefact of another run ends this run.
        if reference_manifest(*step2_sub).get("run_id") != run_id:
            raise PanelError(f"the extended SUB artefact {step2_sub[0]} was not published by run {run_id}")
        _publish_artefact(
            out,
            formation_months(STAGE_B_FIRST_FORMATION, STAGE_B_LAST_FORMATION),
            head=head,
            stage="B",
            pins=stage_b_pins(step2_sub[1]),
            versions=lambda: dataclasses.asdict(CodeHashes.current()),
            step2_sub=step2_sub,
            provenance={"run_id": run_id, "access_id": access_id},
        )
        manifest_sha256 = sha256_file(out / MANIFEST_FILE)
        # If this row cannot be written the build is unbound: the except below deletes it and ends the run, by
        # design. An artefact the ledger does not name is never used; a retry is a new run with its own access.
        append_ledger(
            ledger,
            {
                "run_id": run_id,
                "event": STAGE_B_EVENT,
                "at": datetime.now(UTC).isoformat(),
                "artefact": str(out),
                "manifest_sha256": manifest_sha256,
            },
        )
        return out, manifest_sha256
    except BaseException as exc:
        try:
            shutil.rmtree(out, ignore_errors=True)
        finally:
            end_run_failed(ledger, run_id, STAGE_B_EVENT, exc)
        raise


@dataclass(frozen=True)
class VerifiedArtefact:
    """A verified artefact's manifest and the bytes of every file the caller kept, as hashed.

    Step 2 spec §"Registration" (finding 149): each consumed file is read once, hashed, and parsed from those same
    bytes, so a file replaced after its check is never read. ``files`` is keyed by the path relative to the
    artefact; ``rows`` and ``census`` are always kept."""

    manifest: dict[str, Any]
    files: Mapping[str, bytes]

    @property
    def rows(self) -> bytes:
        return self.files[self.manifest["rows"]["path"]]

    @property
    def census(self) -> bytes:
        return self.files[self.manifest["census"]["path"]]


def _read_checked(path: Path, digest: str, what: str) -> bytes:
    payload = path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != digest:
        raise PanelError(f"{what} digest moved: {path.name}")
    return payload


def read_verified_artefact(
    artefact: Path,
    manifest_sha256: str,
    pins: Mapping[str, str] = STAGE_A_PINS,
    keep: Iterable[str] = (),
) -> VerifiedArtefact:
    """Verify the artefact's manifest digest, schema, pins, inputs and published outputs, reading each file once.

    ``pins`` is the stage's expected pin map: ``STAGE_A_PINS``, or ``stage_b_pins`` of the run's extended SUB
    manifest digest. An artefact of the other stage, or with another SUB, refuses. ``keep`` names the inputs (paths
    relative to the artefact, as the manifest lists them) whose verified bytes are returned; the rest are hashed and
    dropped."""
    kept = set(keep)
    manifest_bytes = _read_checked(artefact / MANIFEST_FILE, manifest_sha256, "artefact manifest")
    manifest = json.loads(manifest_bytes)
    if manifest.get("schema") != MANIFEST_SCHEMA:
        raise PanelError(f"artefact schema {manifest.get('schema')!r}, expected {MANIFEST_SCHEMA!r}")
    if manifest["pinned_manifests"] != dict(pins):
        raise PanelError("the artefact's pinned bundle, linkage or reference differs from this stage's pins")
    listed = {p.relative_to(artefact).as_posix() for p in (artefact / "inputs").rglob("*") if p.is_file()}
    if listed != set(manifest["inputs"]):
        raise PanelError("the artefact's inputs/ does not match its manifest's file list")
    if not kept <= listed:
        raise PanelError(f"kept inputs not in the manifest: {sorted(kept - listed)}")
    files: dict[str, bytes] = {}
    for relative, digest in manifest["inputs"].items():
        payload = _read_checked(artefact / relative, digest, "frozen input")
        if relative in kept:
            files[relative] = payload
    # The published outputs too: a match against the manifest proves nothing if the files beside it were replaced.
    for key in ("rows", "census"):
        entry = manifest[key]
        files[entry["path"]] = _read_checked(artefact / entry["path"], entry["sha256"], f"published {key} file")
    with gzip.GzipFile(fileobj=io.BytesIO(files[manifest["rows"]["path"]])) as content:
        if hashlib.file_digest(content, "sha256").hexdigest() != manifest["rows"]["content_sha256"]:
            raise PanelError("published rows content digest moved")
    return VerifiedArtefact(manifest, files)


def verify_artefact(artefact: Path, manifest_sha256: str, pins: Mapping[str, str] = STAGE_A_PINS) -> dict[str, Any]:
    """The artefact's manifest, after :func:`read_verified_artefact`'s checks."""
    return read_verified_artefact(artefact, manifest_sha256, pins).manifest


#: Step 2 spec §"Registration": the manifest fields a republished stage A may change; every other field is equal.
REPUBLISH_MAY_DIFFER: Final = frozenset(
    {"git_sha", "published_at", "spec_sha256", "construction_sources", "construction_versions"}
)


def check_republished(original: Mapping[str, Any], republished: Mapping[str, Any]) -> None:
    """Step 2 spec §"Registration", replay identity: ``inputs`` gains exactly ``inputs/first_bars.jsonl.gz`` with
    every other input unchanged; ``rows`` may differ only in ``sha256`` (the compressed bytes); every field outside
    :data:`REPUBLISH_MAY_DIFFER` is equal. Both manifests must already have passed :func:`read_verified_artefact`."""
    added = f"inputs/{Frozen.FIRST_BARS}"
    for manifest in (original, republished):
        if not isinstance(manifest.get("inputs"), dict) or not isinstance(manifest.get("rows"), dict):
            raise PanelError("a manifest without an inputs or rows map cannot be compared")
    if (
        added in original["inputs"]
        or {k: v for k, v in republished["inputs"].items() if k != added} != original["inputs"]
    ):
        raise PanelError(f"the republished inputs are not the original's plus {added}")
    if added not in republished["inputs"]:
        raise PanelError(f"the republished artefact has no {added}")
    if {k: v for k, v in republished["rows"].items() if k != "sha256"} != {
        k: v for k, v in original["rows"].items() if k != "sha256"
    }:
        raise PanelError("the republished rows differ in path, count or content")
    fixed = (set(original) | set(republished)) - REPUBLISH_MAY_DIFFER - {"inputs", "rows"}
    moved = sorted(k for k in fixed if original.get(k) != republished.get(k))
    if moved:
        raise PanelError(f"the republished manifest moved fields that must be equal: {moved}")


def replay(artefact: Path, manifest_sha256: str, stem: Path) -> bool:
    """Rebuild a published stage-A artefact from its own ``inputs/`` under the current code; True when rows and
    census match. Stage A only: a stage-B artefact fails ``verify_artefact`` under stage A's pins, so no stage-B
    data is read outside the declared run."""
    manifest = verify_artefact(artefact, manifest_sha256)
    rows_path, census_path = stem.with_suffix(".jsonl.gz"), stem.with_suffix(".census.json")
    rows_path.unlink(missing_ok=True)
    census_path.unlink(missing_ok=True)
    result = build(artefact / "inputs", [date.fromisoformat(m) for m in manifest["formations"]], rows_path, census_path)
    rows_match = gz_content_sha256(rows_path) == manifest["rows"]["content_sha256"]
    census_match = sha256_file(census_path) == manifest["census"]["sha256"]
    print(json.dumps({"rows": result["rows"], "rows_match": rows_match, "census_match": census_match}, indent=1))
    return rows_match and census_match


def _print_summary(summary: Mapping[str, Any]) -> None:
    reconciliation = summary["me_reconciliation"]
    printed = {
        **{k: summary[k] for k in ("funnel_total_counts", "characteristics_total_counts", "diagnostics_total_counts")},
        "daily_screen_flags_by_year": summary["daily_screen_flags_by_year"],
        "daily_monthly_unexplained": len(summary["daily_monthly_unexplained"]),
        "discontinuity": {k: v for k, v in reconciliation["discontinuity"].items() if k != "flagged_name_months"},
        "split_reconciliation": {
            "pairs_with_a_stamp": reconciliation["split_reconciliation"]["pairs_with_a_stamp"],
            "failures": len(reconciliation["split_reconciliation"]["failures"]),
        },
    }
    print(json.dumps(printed, indent=1))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--symbols", help="comma-separated vendor symbols (hand checks); default all")
    parser.add_argument("--formations", help="comma-separated month-ends; default the 80 stage-A formations")
    parser.add_argument(
        "--out", type=Path, help="scratch output stem (default var/research/3609_step1/panel-stageA-<scope>)"
    )
    parser.add_argument("--publish", action="store_true", help="publish the full stage-A artefact (clean checkout)")
    parser.add_argument("--publish-root", type=Path, default=PUBLISH_ROOT)
    parser.add_argument(
        "--publish-stage-b", metavar="RUN_ID", help="step 2's declared run: publish the stage-B artefact (gated)"
    )
    parser.add_argument("--replay", type=Path, help="a published stage-A artefact to rebuild from its own inputs/")
    parser.add_argument("--replay-manifest-sha256", help="the replayed artefact's pinned manifest digest")
    parser.add_argument(
        "--check-republish",
        nargs=4,
        metavar=("ORIGINAL", "ORIGINAL_SHA256", "REPUBLISHED", "REPUBLISHED_SHA256"),
        help="verify both stage-A artefacts and the step 2 replay identity between them",
    )
    args = parser.parse_args(argv)
    if args.check_republish is not None:
        original, original_sha, republished, republished_sha = args.check_republish
        check_republished(
            verify_artefact(Path(original), original_sha), verify_artefact(Path(republished), republished_sha)
        )
        print(json.dumps({"republished": republished, "replay_identity": "pass"}, indent=1))
        return 0
    if (args.replay is None) != (args.replay_manifest_sha256 is None):
        parser.error("--replay and --replay-manifest-sha256 go together")
    modes = [args.publish, args.replay is not None, args.publish_stage_b is not None]
    if any(modes) and (args.symbols or args.formations):
        parser.error(
            "--publish, --publish-stage-b and --replay cover a full stage; --symbols/--formations are scratch-only"
        )
    if sum(modes) > 1:
        parser.error("--publish, --publish-stage-b and --replay are separate runs")
    if args.publish_stage_b is not None:

        def confirm(run_id: str, access_id: int) -> None:
            # A fresh connection: it sees the access row only if the run committed it.
            with psycopg.connect(settings.database_url) as conn:
                require_committed_access(conn, run_id, access_id)

        out, digest = publish_stage_b(args.publish_root, args.publish_stage_b, confirm_access=confirm)
        print(json.dumps({"published": str(out), "manifest_sha256": digest}, indent=1))
        return 0
    if args.replay:
        stem = args.out or OUT_DIR / f"replay-{args.replay.name}"
        stem.parent.mkdir(parents=True, exist_ok=True)
        return 0 if replay(args.replay, args.replay_manifest_sha256, stem) else 1
    symbols = frozenset(args.symbols.split(",")) if args.symbols else None
    formations = (
        tuple(date.fromisoformat(m) for m in args.formations.split(",")) if args.formations else formation_months()
    )
    if max(formations) > STAGE_A_LAST_FORMATION:
        raise PanelError("stage A ends at 2021-04-30; later formations are step 2's, only via --publish-stage-b")
    if args.publish:
        out = publish(args.publish_root, formations)
        print(json.dumps({"published": str(out), "manifest_sha256": sha256_file(out / MANIFEST_FILE)}, indent=1))
        return 0
    scope = "all" if symbols is None else "-".join(sorted(symbols))
    stem = args.out or OUT_DIR / f"panel-stageA-{scope}"
    stem.parent.mkdir(parents=True, exist_ok=True)
    inputs = stem.with_name(f"{stem.name}.inputs")
    shutil.rmtree(inputs, ignore_errors=True)  # scratch only: a published artefact is never rewritten
    rows_path, census_path = stem.with_suffix(".jsonl.gz"), stem.with_suffix(".census.json")
    rows_path.unlink(missing_ok=True)
    census_path.unlink(missing_ok=True)
    # Read phase, then close: no transaction stays open through the CPU-bound walk.
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        dump_inputs(conn, inputs, symbols=symbols, formations=formations)
    _print_summary(build(inputs, formations, rows_path, census_path)["summary"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
