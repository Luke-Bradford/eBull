"""#3609 step 1 slice 3b: the stage-A panel's universe, market equity and accounting characteristics.

Spec: ``docs/research/2026-10-04-3609-step1-factor-panel.md`` (§"Universe at M", §"Market equity",
§"Accounting", §"Census"). The accounting and ME rules live in ``app/services/factor_panel.py``; this script
supplies the DB reads and the pinned #3360 / #3361 / slice-1 artefacts, walks the 80 stage-A formations and
writes one row per (M, series) examined, admitted or excluded, plus the census.

Not here yet (slice 3c): ``ret_12_1`` / ``rvol_21d`` and the daily screen, holdings and returns per arm, the
liquidity tercile, the split and discontinuity reconciliations, and the published artefact with frozen inputs.
Until then the output is a scratch file under ``var/research/3609_step1/``, not a step-2 input.

Hold-out: every price read is bounded at ``PRICE_BOUND`` (2021-05-31); bundle and SUB reads are bounded by
s(M) <= 2021-04-30.

Run: ``PYTHONPATH=. uv run python -m scripts.build_3609_factor_panel [--symbols AAPL,MSFT] [--formations
2019-06-30]``.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
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
    sic_as_of,
)
from app.services.factor_panel_reference import parse_fsds_sub
from app.services.pit_fundamentals import PitFundamentalsBundle, load_pit_fundamentals
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.security_linkage import Reason, load_security_linkage
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
#: Stage A: the last holding month is 2021-05; nothing after it is read.
PRICE_BOUND: Final = date(2021, 5, 31)
SPY_SYMBOL: Final = "SPY"
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


def load_sub_sic(root: Path, expected_manifest_sha256: str) -> dict[str, int | None]:
    """accession -> SUB ``sic`` over every pinned quarter, each file checked against the slice 1 manifest."""
    manifest_path = root / "manifest.json"
    if sha256_file(manifest_path) != expected_manifest_sha256:
        raise PanelError(f"reference manifest digest moved: {manifest_path}")
    manifest = json.loads(manifest_path.read_bytes())
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

    def add(self, row: Mapping[str, Any]) -> None:
        m, reason = row["M"], row["exclusion"] or "admitted"
        self.funnel[m][reason] += 1
        me = _known_me(row)
        if me is not None:
            self.funnel_me[m][reason] += me
        if "sic_status" in row:
            self.sic_status[row["sic_status"]] += 1
        if row["exclusion"] is not None:
            return
        assert me is not None
        self.shares_scope[row["me"]["shares_scope"]] += 1
        for name, got in row["characteristics"].items():
            outcome = got["missing"] or "value"
            self.chars[m][name][outcome] += 1
            self.chars_me[m][name][outcome] += me
            self.branches[name].update(set(got["branches"]))
            if got["missing"] is None:
                self.kinds[name][got["kind"]] += 1

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
        }


@dataclass(frozen=True)
class Inputs:
    """Everything read from the DB, so the connection closes before the CPU-bound walk."""

    decisions: Mapping[date, date]
    admitted: Sequence[AdmittedSeries]
    symbols: Mapping[int, str]
    bars: Mapping[tuple[int, date], Decimal]
    splits: Mapping[int, Sequence[SplitStamp]]


def load_inputs(conn: psycopg.Connection[Any], *, symbols: frozenset[str] | None, formations: Sequence[date]) -> Inputs:
    sessions = spy_sessions(conn)
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
    return Inputs(decisions, admitted, symbol_of, bars, split_stamps(conn, ids))


def walk(inputs: Inputs) -> Iterator[dict[str, Any]]:
    """Every (M, admitted series) row, in formation order for steps 1-2 and then CIK by CIK."""
    bundle = load_pit_fundamentals(BUNDLE[0], expected_manifest_sha256=BUNDLE[1])
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
        bundle._cache.pop(cik10, None)  # bound memory: each CIK's shard is read once, by this loop only
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
    stem.with_suffix(".census.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps({k: summary[k] for k in ("funnel_total_counts", "characteristics_total_counts")}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
