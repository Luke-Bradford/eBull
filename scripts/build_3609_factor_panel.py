"""#3609 step 1 slices 3b-3c: the stage-A panel's universe, market equity, characteristics and holding returns.

Spec: ``docs/research/2026-10-04-3609-step1-factor-panel.md`` (§"Universe at M", §"Market equity",
§"Accounting", §"Price characteristics and daily data", §"Returns and holdings", §"Census"). The accounting and
ME rules live in ``app/services/factor_panel.py`` and the price rules in ``app/services/factor_panel_prices.py``;
this script supplies the DB reads and the pinned #3360 / #3361 / slice-1 artefacts, walks the 80 stage-A
formations and writes one row per (M, series) examined, admitted or excluded, plus the census.

Not here yet (slice 3c-ii): the published artefact with frozen inputs. Until then the output is a scratch file
under ``var/research/3609_step1/``, not a step-2 input.

Hold-out: every price read is bounded at ``PRICE_BOUND`` (2021-05-31); bundle and SUB reads are bounded by
s(M) <= 2021-04-30.

Run: ``PYTHONPATH=. uv run python -m scripts.build_3609_factor_panel [--symbols AAPL,MSFT] [--formations
2019-06-30]``.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import heapq
import itertools
import json
import sys
from bisect import bisect_right
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

import psycopg

from app.config import settings
from app.services.factor_panel import (
    ACCOUNTING_CHARACTERISTICS,
    REIT_SIC,
    STAGE_A_LAST_FORMATION,
    CikView,
    MarketEquity,
    PanelError,
    PrefixCache,
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
)
from app.services.factor_panel_prices import (
    DailyBar,
    DailyMonthly,
    FormationPrices,
    PriceCharacteristic,
    SessionGrid,
    liquidity_terciles,
    me_discontinuity,
    series_prices,
)
from app.services.factor_panel_reference import parse_fsds_sub
from app.services.pit_fundamentals import PitFundamentalsBundle, load_pit_fundamentals
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.security_linkage import Reason, load_security_linkage
from app.services.series_termination import classify_termination
from app.services.strategies.validated_universe import load_validated_universe
from app.services.universe_selection import SURVIVORSHIP_FREE_VENDOR, AdmittedSeries, load_universe_selection

RESEARCH_ROOT: Final = Path.home() / "Library/Application Support/eBull/research"
#: Pins from the slice 1 and slice 2 close-outs on #3609 (2026-10-04, 2026-10-05).
BUNDLE: Final = (
    RESEARCH_ROOT / "pit_fundamentals_3360/2026-10-04-805bc9e6",
    "29e6249e504826f69c243087c03e7ba713b26e26ec4afa7c90222e7a11032545",
)
LINKAGE: Final = (
    RESEARCH_ROOT / "security_linkage_3361/2026-10-05-805bc9e6",
    "5bf20cfa5fed538e197c4ec0db8be0cdab8eca8f5e4b87b98865c73a7437058e",
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
SPY_SYMBOL: Final = "SPY"
PRICE_CHARACTERISTICS: Final = ("ret_12_1", "rvol_21d")
OUT_DIR: Final = Path("var/research/3609_step1")


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


def multiple_security_ciks(links: Mapping[int, str]) -> frozenset[str]:
    """CIKs that more than one linked, priced series maps to at one M (step 5)."""
    counts = Counter(links.values())
    return frozenset(cik for cik, n in counts.items() if n > 1)


def holding_month(formation: date) -> str:
    following = add_months(date(formation.year, formation.month, 1), 1)
    return f"{following.year:04d}-{following.month:02d}"


def sha256_file(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


# --------------------------------------------------------------------------- loads


#: An admitted bar on a session: the ``total_return_reader`` month-end semantics (coverage row, quarantine
#: verdict ``return_usable``, finite positive close and adj_close), restricted to the decision sessions.
_DECISION_BARS_SQL = """
SELECT d.series_id, d.bar_date, d.close
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
  AND d.bar_date = ANY(%(sessions)s::date[])
  AND d.bar_date <= %(bound)s
  AND COALESCE(q.return_usable, TRUE)
  AND d.adj_close > 0 AND d.adj_close < 'Infinity'::numeric
  AND d.close > 0 AND d.close < 'Infinity'::numeric
"""

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


def spy_sessions(conn: psycopg.Connection[Any]) -> list[date]:
    rows = conn.execute(
        _SPY_SESSIONS_SQL, {"vendor": SURVIVORSHIP_FREE_VENDOR, "symbol": SPY_SYMBOL, "bound": PRICE_BOUND}
    ).fetchall()
    return [row[0] for row in rows]


def decision_bars(
    conn: psycopg.Connection[Any], series_ids: Sequence[int], sessions: Sequence[date]
) -> dict[tuple[int, date], Decimal]:
    params = {
        "series_ids": list(series_ids),
        "sessions": list(sessions),
        "bound": PRICE_BOUND,
        "quarantine_version": QUARANTINE_RULE_SET_VERSION,
    }
    return {(int(sid), day): close for sid, day, close in conn.execute(_DECISION_BARS_SQL, params).fetchall()}


def split_stamps(conn: psycopg.Connection[Any], series_ids: Sequence[int]) -> dict[int, list[SplitStamp]]:
    out: dict[int, list[SplitStamp]] = defaultdict(list)
    for sid, day, factor in conn.execute(_SPLITS_SQL, {"series_ids": list(series_ids), "bound": PRICE_BOUND}):
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

_RF_SQL = """
SELECT o.observation_date, o.value::float8, o.unit
FROM reference_data_observations o
JOIN reference_data_snapshots s USING (snapshot_id)
WHERE o.snapshot_id = %(snapshot_id)s
  AND s.dataset_key = %(dataset_key)s
  AND s.response_sha256 = %(response_sha256)s
  AND o.series_key = 'RF'
  AND o.observation_date BETWEEN %(first)s AND %(bound)s
"""


def load_rf(conn: psycopg.Connection[Any], manifest: Mapping[str, Any], first: date) -> dict[date, float]:
    """French daily RF from the snapshot the slice 1 manifest pins, checked by its response digest."""
    pinned = manifest["reference_snapshots"][RF_DATASET]
    params = {
        "snapshot_id": pinned["snapshot_id"],
        "dataset_key": RF_DATASET,
        "response_sha256": pinned["response_sha256"],
        "first": first,
        "bound": PRICE_BOUND,
    }
    out: dict[date, float] = {}
    for day, value, unit in conn.execute(_RF_SQL, params).fetchall():
        if unit != RF_UNIT:
            raise PanelError(f"RF unit {unit!r} on {day}, expected {RF_UNIT!r}")
        out[day] = value
    if not out:
        raise PanelError(f"pinned RF snapshot {pinned['snapshot_id']} returned nothing (digest or dataset moved?)")
    return out


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


def load_prices(
    conn: psycopg.Connection[Any],
    admitted: Sequence[AdmittedSeries],
    grid: SessionGrid,
    holding_last: Mapping[date, date],
) -> tuple[dict[int, Mapping[date, FormationPrices]], Counter[int]]:
    """Stream the daily window series by series (server-side cursor) into each series' formation prices."""
    by_id = {a.series_id: a for a in admitted}
    params = {
        "series_ids": list(by_id),
        "first": grid.sessions[0],
        "bound": PRICE_BOUND,
        "quarantine_version": QUARANTINE_RULE_SET_VERSION,
    }
    prices: dict[int, Mapping[date, FormationPrices]] = {}
    flags: Counter[int] = Counter()
    # A named cursor needs a transaction; the connection is autocommit.
    with conn.transaction(), conn.cursor(name="factor_panel_daily") as cur:
        cur.itersize = 100_000
        cur.execute(_DAILY_SQL, params)
        for sid, rows in itertools.groupby(cur, key=lambda row: row[0]):
            series = by_id[int(sid)]
            termination = None
            if series.termination is not None:
                if series.last_bar is None:
                    raise PanelError(f"terminating series {sid} has no stored last_bar")
                termination = (classify_termination(series.termination), series.last_bar)
            bars = [
                DailyBar(day, close, adj, volume, stamped, usable)
                for _, day, close, adj, volume, stamped, usable in rows
            ]
            got = series_prices(bars, grid, holding_last_session=holding_last, termination=termination)
            prices[int(sid)] = got.by_formation
            flags.update(got.flags_by_year)
    return prices, flags


def reference_manifest(root: Path, expected_manifest_sha256: str) -> dict[str, Any]:
    manifest_path = root / "manifest.json"
    if sha256_file(manifest_path) != expected_manifest_sha256:
        raise PanelError(f"reference manifest digest moved: {manifest_path}")
    return json.loads(manifest_path.read_bytes())


def load_sub_sic(root: Path, expected_manifest_sha256: str) -> dict[str, int | None]:
    """accession -> SUB ``sic`` over every pinned quarter, each file checked against the slice 1 manifest."""
    manifest = reference_manifest(root, expected_manifest_sha256)
    out: dict[str, int | None] = {}
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
        "liquidity_tercile": tercile,
        "holding": {
            "status": holding.status.value,
            "period_return": holding.period_return,
            "end_bar": None if holding.end_bar is None else holding.end_bar.isoformat(),
            "by_arm": dict(holding.by_arm),
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
) -> Iterable[dict[str, Any]]:
    """Steps 3-6 and the characteristics for every linked candidate of one CIK.

    ME is computed for every name past the bundle gate, before the filer / REIT / one-security steps, so the
    census can weight those exclusions too; the funnel order is unchanged.
    """
    cache = PrefixCache(bundle, cik10, max(c.session for c, _ in candidates))
    for candidate, row in candidates:
        view = CikView(bundle, cik10, candidate.session, prefixes=cache)
        if view.exclusion is not None:
            yield {**row, "exclusion": view.exclusion.value}
            continue
        me = market_equity(view, candidate.close, splits.get(candidate.series_id, ()))
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
                    "facts": [f.to_json() for f in got.facts],
                }
            yield {**row, "exclusion": None, "characteristics": chars}


def _known_me(row: Mapping[str, Any]) -> float | None:
    me = row.get("me")
    return None if me is None or me["value"] is None else float(me["value"])


class Census:
    """Built row by row. Every ME share is within one formation (a cross-section), never pooled across years."""

    def __init__(self) -> None:
        self.funnel: dict[str, Counter[str]] = defaultdict(Counter)
        self.funnel_me: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self.chars: dict[str, dict[str, Counter[str]]] = defaultdict(lambda: defaultdict(Counter))
        self.chars_me: dict[str, dict[str, dict[str, float]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(float))
        )
        self.branches: dict[str, Counter[str]] = defaultdict(Counter)
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
            self.branches[name].update(set(got["branches"]))
            if got["missing"] is None:
                self.kinds[name][got["kind"]] += 1
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


def load_inputs(conn: psycopg.Connection[Any], *, symbols: frozenset[str] | None, formations: Sequence[date]) -> Inputs:
    first_formation = min(formations)
    first_day = add_months(date(first_formation.year, first_formation.month, 1), -DAILY_LOOKBACK_MONTHS)
    sessions = [s for s in spy_sessions(conn) if s >= first_day]
    decisions = {m: decision_session(m, sessions) for m in formations}
    selection = load_universe_selection(
        conn, universe="survivorship_free", validated_ids=frozenset(load_validated_universe(conn))
    )
    admitted = list(selection.admitted)
    symbol_of = series_symbols(conn, [a.series_id for a in admitted])
    if symbols is not None:
        admitted = [a for a in admitted if symbol_of.get(a.series_id) in symbols]
    ids = [a.series_id for a in admitted]
    bars = decision_bars(conn, ids, sorted(set(decisions.values())))
    rf = load_rf(conn, reference_manifest(*REFERENCE), first_day)
    grid = SessionGrid.build(sessions, rf, decisions)
    prices, flags = load_prices(conn, admitted, grid, holding_last_sessions(formations, sessions))
    # The two reads share their admission predicates: a decision bar the stream does not price is a drift.
    priced = {(sid, decisions[m]) for sid, per in prices.items() for m in per}
    if priced != set(bars):
        raise PanelError(f"daily stream and decision bars disagree on {len(priced ^ set(bars))} (series, s(M)) pairs")
    name_key = {a.series_id: a.name_key for a in admitted}
    terciles = {
        m: liquidity_terciles({sid: per[m].dollar_volume for sid, per in prices.items() if m in per}, name_key)
        for m in formations
    }
    return Inputs(decisions, admitted, symbol_of, bars, split_stamps(conn, ids), prices, terciles, flags)


def walk(inputs: Inputs) -> Iterator[dict[str, Any]]:
    """Every (M, admitted series) row, in formation order for steps 1-2 and then CIK by CIK."""
    bundle = load_pit_fundamentals(BUNDLE[0], expected_manifest_sha256=BUNDLE[1])
    # The shard cache is private to ``pit_fundamentals``, a hashed policy file (#3360 and #3361 manifests), so it
    # gains no public eviction method here. Resolve it once and refuse if it moved, so memory bounding cannot stop
    # silently.
    shard_cache = getattr(bundle, "_cache", None)
    if not isinstance(shard_cache, dict):
        raise PanelError("PitFundamentalsBundle._cache moved: per-CIK shard eviction would silently stop")
    linkage = load_security_linkage(LINKAGE[0], expected_manifest_sha256=LINKAGE[1])
    sub_sic = load_sub_sic(*REFERENCE)
    print(
        f"admitted series {len(inputs.admitted)}; decision bars {len(inputs.bars)}; SUB accessions {len(sub_sic)}",
        flush=True,
    )
    by_cik: dict[str, list[tuple[Candidate, dict[str, Any]]]] = defaultdict(list)
    multi: dict[date, frozenset[str]] = {}
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
        yield from build_cik_rows(bundle, cik10, candidates, multi, sub_sic, inputs.splits)
        shard_cache.pop(cik10, None)  # bound memory: each CIK's shard is read once, by this loop only
        if done % 500 == 0:
            print(f"  {done}/{len(by_cik)} CIKs", flush=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--symbols", help="comma-separated vendor symbols (hand checks); default all")
    parser.add_argument("--formations", help="comma-separated month-ends; default the 80 stage-A formations")
    parser.add_argument("--out", type=Path, help="output stem (default var/research/3609_step1/panel-stageA-<scope>)")
    args = parser.parse_args(argv)
    symbols = frozenset(args.symbols.split(",")) if args.symbols else None
    formations = (
        tuple(date.fromisoformat(m) for m in args.formations.split(",")) if args.formations else formation_months()
    )
    if max(formations) > STAGE_A_LAST_FORMATION:
        raise PanelError("stage A ends at 2021-04-30; later formations are step 2's, under its declaration")
    # Read phase, then close: no transaction stays open through the CPU-bound walk.
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        inputs = load_inputs(conn, symbols=symbols, formations=formations)
    scope = "all" if symbols is None else "-".join(sorted(symbols))
    stem = args.out or OUT_DIR / f"panel-stageA-{scope}"
    stem.parent.mkdir(parents=True, exist_ok=True)
    tally = Census()
    with gzip.open(stem.with_suffix(".jsonl.gz"), "wt") as handle:
        for row in walk(inputs):
            tally.add(row)
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    summary = tally.to_json()
    summary["me_reconciliation"] = me_reconciliation(tally.me_points, inputs.decisions, inputs.splits)
    summary["daily_screen_flags_by_year"] = dict(sorted(inputs.flags_by_year.items()))
    stem.with_suffix(".census.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
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
    return 0


if __name__ == "__main__":
    sys.exit(main())
