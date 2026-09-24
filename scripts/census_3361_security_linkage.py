"""#3361 acceptance item 4: descriptive census of the security-linkage bundle.

Spec: ``docs/proposals/ta/2026-09-24-3361-security-linkage.md`` ("Census", and the
"Liquidity" / "Form 25 outcome channels" / "Identity descriptives" / "Join to #3360"
precision rules). Descriptive only: it measures abstention and link structure, never link
CORRECTNESS, and it makes no strategy look.

* **Formations:** the #3360 grid (``census_3360_pit_fundamentals.formation_dates``). Every
  count is one observation per (series, formation), so nothing is date-weighted.
* **Population at D** (in-scope Intrader series): ``bar_on_d`` (the primary group);
  ``no_bar_on_d`` (``first_bar <= D <= last_bar`` with no bar ON D); ``ended_in_window``
  (``last_bar`` in ``[D - 730, D)``). The last group is tabulated with its
  ``link_as_of(S, last_bar)`` diagnostic, because at D itself it is ``outside_series``.
* **Bars** are read from ``research_price_daily`` into ``bars.tsv`` in the evidence
  directory, which is the snapshot the census reads: every in-scope series' bars in
  ``[D - 42, D]`` for each formation, plus both endpoint bars.
* **Liquidity decile** (``bar_on_d`` only): the median of ``close x volume`` as stored,
  over the latest 21 valid bars strictly before D, whose oldest must be ``>= D - 42``. The
  rank runs over ``(value, series_id)``, and decile = ``floor(10 * rank / n)``.
* **Form 25 outcome** in ``[D, D + 730]``, over two channels that are never merged (by the
  linked CIK; by rule-4 symbol match). The unit is series with at least one row.
  ``unobserved_horizon`` applies when the window leaves the register span.

Usage::

    PYTHONPATH=. uv run python scripts/census_3361_security_linkage.py \\
        --bundle <bundle dir> --manifest-sha256 <sha> --out-dir <new directory>

The #3360 membership join reads the bundle's own ``inputs/submissions.zip``, which is
byte-identical to the #3360 bundle's copy (the builder checks it against that manifest).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing
import sys
import zipfile
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final

from app.services import security_linkage as sl
from app.services.pit_fundamentals import acceptance_ny_date, canonical_json
from scripts.census_3360_pit_fundamentals import (
    _MAIN_MEMBER,  # pyright: ignore[reportPrivateUsage]
    _init_population,  # pyright: ignore[reportPrivateUsage]
    _population,  # pyright: ignore[reportPrivateUsage]
    formation_dates,
)

EVIDENCE_SCHEMA: Final = "security-linkage-census-v1"
WINDOW_DAYS: Final = sl.WINDOW_DAYS
LIQUIDITY_BARS: Final = 21
LIQUIDITY_SPAN_DAYS: Final = 2 * LIQUIDITY_BARS  # by construction: twice the count
CAPTURE_SLACK_DAYS: Final = 7
OUTCOME_HORIZON_DAYS: Final = 730
AGE_BUCKETS: Final = (30, 90, 180, 365, 730)

BAR_ON_D: Final = "bar_on_d"
NO_BAR_ON_D: Final = "no_bar_on_d"
ENDED_IN_WINDOW: Final = "ended_in_window"
LIQUIDITY_UNAVAILABLE: Final = "liquidity_unavailable"
NOT_APPLICABLE: Final = "not_applicable"


class CensusError(RuntimeError):
    """The bars snapshot or an input violates a census precondition."""


Bar = tuple[date, Decimal | None, int | None]


# --------------------------------------------------------------------------- pure pieces


def population_group(first_bar: date, last_bar: date, bar_dates: frozenset[date], decision: date) -> str | None:
    if decision in bar_dates:
        return BAR_ON_D
    if first_bar <= decision <= last_bar:
        return NO_BAR_ON_D
    if decision - timedelta(days=WINDOW_DAYS) <= last_bar < decision:
        return ENDED_IN_WINDOW
    return None


def liquidity(bars: Sequence[Bar], decision: date) -> Decimal | None:
    """Median close x volume over the latest 21 valid bars before D; ``bars`` in date order."""
    valid = [
        (day, close * volume)
        for day, close, volume in bars
        if day < decision and close is not None and volume is not None and close > 0 and volume > 0
    ][-LIQUIDITY_BARS:]
    if len(valid) < LIQUIDITY_BARS or valid[0][0] < decision - timedelta(days=LIQUIDITY_SPAN_DAYS):
        return None
    return sorted(value for _, value in valid)[LIQUIDITY_BARS // 2]


def deciles(values: Mapping[int, Decimal]) -> dict[int, int]:
    ranked = sorted(values, key=lambda sid: (values[sid], sid))
    n = len(ranked)
    return {sid: 10 * rank // n for rank, sid in enumerate(ranked)}


def capture_status(last_bar: date, capture_end: date) -> str:
    return "runs_to_capture" if last_bar >= capture_end - timedelta(days=CAPTURE_SLACK_DAYS) else "ends_before_capture"


def form25_outcome(filed: Sequence[date], decision: date, span: tuple[date, date] | None) -> tuple[str, int]:
    """(category, rows) for Form 25 rows filed in ``[D, D + 730]``."""
    horizon = decision + timedelta(days=OUTCOME_HORIZON_DAYS)
    rows = sum(1 for day in filed if decision <= day <= horizon)
    if rows:
        return "form25_observed", rows
    observed = span is not None and span[0] <= decision and horizon <= span[1]
    return ("no_form25_observed" if observed else "unobserved_horizon"), 0


def age_bucket(days: int) -> str:
    for bound in AGE_BUCKETS:
        if days <= bound:
            return f"<={bound}d"
    return f">{AGE_BUCKETS[-1]}d"


def reversion_status(
    switch: date, predecessor: str, later: Sequence[tuple[date, str | None]], covered_through: date
) -> str:
    """Precision rules [32-35]: ``reverted`` > ``unobservable`` > ``not_reverted``.

    ``later``: (formation, linked cik or None) for this series' later grid formations.
    """
    horizon = switch + timedelta(days=WINDOW_DAYS)
    if any(day <= horizon and cik == predecessor for day, cik in later):
        return "reverted"
    if horizon > covered_through:
        return "unobservable"
    return "not_reverted"


def immediate_predecessor(result: sl.LinkResult) -> str | None:
    """For a ``succession`` link: the CIK ordered just before the linked one."""
    firsts: dict[str, str] = {}
    for o in result.observations:
        firsts.setdefault(o.cik, o.acceptance)
    order = sorted(firsts, key=lambda c: (firsts[c], c))
    return order[-2] if len(order) > 1 else None


def collision_groups(symbols_by_series: Mapping[int, frozenset[str]]) -> dict[int, int]:
    """Series -> smallest series_id of its connected collision component (union-find)."""
    parent = {sid: sid for sid in symbols_by_series}

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    owner: dict[str, int] = {}
    for sid in sorted(symbols_by_series):
        for symbol in symbols_by_series[sid]:
            if symbol in owner:
                a, b = find(owner[symbol]), find(sid)
                parent[max(a, b)] = min(a, b)
            else:
                owner[symbol] = sid
    return {sid: find(sid) for sid in parent}


# --------------------------------------------------------------------------- bars snapshot


def snapshot_bars(conn: Any, series: Mapping[int, tuple[date, date]], formations: Sequence[date], path: Path) -> None:
    """Write every in-scope series' bars in ``[D - 42, D]`` per formation + both endpoints."""
    conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
    ids = sorted(series)
    rows: set[tuple[int, date, Any, Any]] = set()
    for decision in formations:
        rows.update(
            conn.execute(
                "SELECT series_id, bar_date, close, volume FROM research_price_daily"
                " WHERE series_id = ANY(%(ids)s) AND bar_date BETWEEN %(lo)s AND %(hi)s",
                {"ids": ids, "lo": decision - timedelta(days=LIQUIDITY_SPAN_DAYS), "hi": decision},
            ).fetchall()
        )
    rows.update(
        conn.execute(
            "SELECT d.series_id, d.bar_date, d.close, d.volume FROM research_price_daily d"
            " JOIN unnest(%(ids)s::bigint[], %(firsts)s::date[], %(lasts)s::date[]) AS e(series_id, f, l)"
            " ON d.series_id = e.series_id AND d.bar_date IN (e.f, e.l)",
            {"ids": ids, "firsts": [series[i][0] for i in ids], "lasts": [series[i][1] for i in ids]},
        ).fetchall()
    )
    with path.open("x") as handle:
        handle.write("series_id\tbar_date\tclose\tvolume\n")
        for sid, day, close, volume in sorted(rows):
            handle.write(
                f"{sid}\t{day.isoformat()}\t{'' if close is None else close}\t{'' if volume is None else volume}\n"
            )


def read_bars(path: Path, series: Mapping[int, tuple[date, date]]) -> dict[int, list[Bar]]:
    """Precision rules [23-25]: duplicates, non-finite values, out-of-bounds bars and missing
    endpoint bars all fail the census."""
    bars: dict[int, list[Bar]] = defaultdict(list)
    lines = path.read_text().split("\n")
    if lines[0] != "series_id\tbar_date\tclose\tvolume" or lines[-1] != "":
        raise CensusError("bars snapshot header or terminator malformed")
    seen: set[tuple[int, date]] = set()
    for line in lines[1:-1]:
        sid_raw, day_raw, close_raw, volume_raw = line.split("\t")
        sid, day = int(sid_raw), date.fromisoformat(day_raw)
        try:
            close = None if close_raw == "" else Decimal(close_raw)
            volume = None if volume_raw == "" else int(volume_raw)
        except (InvalidOperation, ValueError) as exc:
            raise CensusError(f"series {sid} {day}: unparseable bar") from exc
        if close is not None and not close.is_finite():
            raise CensusError(f"series {sid} {day}: non-finite close")
        if (sid, day) in seen:
            raise CensusError(f"series {sid} {day}: duplicate bar")
        seen.add((sid, day))
        first, last = series[sid]
        if not first <= day <= last:
            raise CensusError(f"series {sid} {day}: bar outside [{first}, {last}]")
        bars[sid].append((day, close, volume))
    for sid, (first, last) in series.items():
        if (sid, first) not in seen or (sid, last) not in seen:
            raise CensusError(f"series {sid}: endpoint bar missing from the snapshot")
    for rows in bars.values():
        rows.sort(key=lambda bar: bar[0])
    return bars


# --------------------------------------------------------------------------- tabulation


@dataclass
class Formation:
    decision: date
    cross_tab: Counter[str]
    counts: Counter[str]
    outcomes: Counter[str]
    ages: Counter[str]
    linked: dict[int, str]  # series -> linked cik (groups 1-2)


def _result_label(result: sl.LinkResult) -> str:
    return f"linked:{result.basis}" if result.reason is sl.Reason.LINKED else result.label


def tabulate(
    bundle: sl.SecurityLinkageBundle,
    decision: date,
    inventory: Mapping[int, tuple[str, date, date]],
    bars: Mapping[int, Sequence[Bar]],
    capture_end: date,
    register_by_cik: Mapping[str, Sequence[date]],
    register_by_symbol: Mapping[str, Sequence[date]],
    span: tuple[date, date] | None,
    groups: Mapping[int, int],
) -> Formation:
    population: dict[int, str] = {}
    for sid, (_, first, last) in inventory.items():
        group = population_group(first, last, frozenset(day for day, _, _ in bars.get(sid, ())), decision)
        if group is not None:
            population[sid] = group
    values = {
        sid: value
        for sid, group in population.items()
        if group == BAR_ON_D and (value := liquidity(bars.get(sid, ()), decision)) is not None
    }
    ranks = deciles(values)
    formation = Formation(decision, Counter(), Counter(), Counter(), Counter(), {})
    linked_series: dict[str, set[int]] = defaultdict(set)
    collided: dict[str, set[int]] = defaultdict(set)
    for sid, group in sorted(population.items()):
        symbol, _, last = inventory[sid]
        at = last if group == ENDED_IN_WINDOW else decision
        result = bundle.link_as_of(sid, at)
        label = _result_label(result)
        if group == BAR_ON_D:
            liquidity_label = f"decile_{ranks[sid]}" if sid in ranks else LIQUIDITY_UNAVAILABLE
        else:
            liquidity_label = NOT_APPLICABLE
        capture = capture_status(last, capture_end)
        formation.cross_tab[f"{group}|{label}|{liquidity_label}|{capture}"] += 1
        formation.counts[f"series:{group}"] += 1
        if result.reason is sl.Reason.VENDOR_SYMBOL_COLLISION:
            collided[group].add(sid)
        if result.reason in (sl.Reason.NO_RECENT_EVIDENCE, sl.Reason.CONFLICTING_EVIDENCE, sl.Reason.LINKED):
            formation.counts[f"flag_denominator:{group}"] += 1
            if result.form25_unobserved:
                formation.counts[f"flag:{group}:form25_unobserved"] += 1
            for kind in sorted({flag.match for flag in result.form25}):
                formation.counts[f"flag:{group}:{kind}"] += 1
        if result.reason is sl.Reason.LINKED and result.cik is not None:
            linked_series[f"{group}:{result.cik}"].add(sid)
            if group != ENDED_IN_WINDOW:  # at-D links only: the #3360 join and reversion read these
                formation.linked[sid] = result.cik
            if result.q_alias:
                formation.counts[f"q_alias:{group}"] += 1
            newest = max(o.acceptance for o in result.observations if o.cik == result.cik)
            age = (at - acceptance_ny_date(newest)).days
            formation.ages[f"{group}:{age_bucket(age)}"] += 1
            channel = form25_outcome(register_by_cik.get(result.cik, ()), decision, span)
            formation.outcomes[f"{group}|by_linked_cik|{channel[0]}"] += 1
            formation.outcomes[f"{group}|by_linked_cik|rows"] += channel[1]
        symbols = sl.match_set(sl.parse_vendor_symbol(symbol))
        if symbols:
            filed = [day for s in sorted(symbols) for day in register_by_symbol.get(s, ())]
            channel = form25_outcome(filed, decision, span)
            formation.outcomes[f"{group}|by_symbol|{label}|{channel[0]}"] += 1
            formation.outcomes[f"{group}|by_symbol|{label}|rows"] += channel[1]
    for key, sids in linked_series.items():
        group = key.split(":", 1)[0]
        formation.counts[f"distinct_linked_ciks:{group}"] += 1
        if len(sids) > 1:
            formation.counts[f"ciks_with_several_linked_series:{group}"] += 1
    for group, sids in collided.items():
        formation.counts[f"collision_series:{group}"] = len(sids)
        formation.counts[f"collision_groups:{group}"] = len({groups[s] for s in sids})
    return formation


def _sha256_file(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--out-dir", type=Path, required=True, help="evidence directory; must not exist")
    parser.add_argument("--processes", type=int, default=max(1, (multiprocessing.cpu_count() or 2) - 2))
    args = parser.parse_args()
    args.out_dir.mkdir(parents=False)  # exclusive

    bundle = sl.load_security_linkage(args.bundle, expected_manifest_sha256=args.manifest_sha256)
    manifest = json.loads((args.bundle / sl.MANIFEST_FILENAME).read_bytes())
    inputs = args.bundle / "inputs"
    for name in ("series_inventory", "form25"):
        if _sha256_file(inputs / f"{name}.json") != manifest["input_sha256"][name]:
            raise SystemExit(f"input {name} digest differs from the manifest")
    if _sha256_file(inputs / "submissions.zip") != manifest["input_sha256"]["submissions"]:
        raise SystemExit("submissions.zip digest differs from the manifest")
    formations = formation_dates()
    if max(formations) > bundle.supported_through:
        raise SystemExit("a formation is after the bundle's supported_through")
    inventory = {
        row[0]: (row[2], date.fromisoformat(row[3]), date.fromisoformat(row[4]))
        for row in json.loads((inputs / "series_inventory.json").read_bytes())
        if row[1] == sl.IN_SCOPE_VENDOR
    }
    capture_end = date.fromisoformat(manifest["capture_end"][sl.IN_SCOPE_VENDOR])
    register = json.loads((inputs / "form25.json").read_bytes())
    raw_span = manifest["form25_span"]
    span = None if raw_span is None else (date.fromisoformat(raw_span[0]), date.fromisoformat(raw_span[1]))
    register_by_cik: dict[str, list[date]] = defaultdict(list)
    register_by_symbol: dict[str, list[date]] = defaultdict(list)
    for _, filed, cik, symbol in register["rows"]:
        register_by_cik[cik].append(date.fromisoformat(filed))
        if (unified := sl.evidence_symbol(symbol)) is not None:
            register_by_symbol[unified].append(date.fromisoformat(filed))

    import psycopg

    from app.config import settings

    bars_path = args.out_dir / "bars.tsv"
    bounds = {sid: (first, last) for sid, (_, first, last) in inventory.items()}
    with psycopg.connect(settings.database_url) as conn:
        snapshot_bars(conn, bounds, formations, bars_path)
    bars = read_bars(bars_path, bounds)
    print(f"bars: {sum(len(v) for v in bars.values())} rows", file=sys.stderr)

    symbols = {
        sid: ms for sid, (symbol, _, _) in inventory.items() if (ms := sl.match_set(sl.parse_vendor_symbol(symbol)))
    }
    groups = collision_groups(symbols)
    table = [
        tabulate(bundle, d, inventory, bars, capture_end, register_by_cik, register_by_symbol, span, groups)
        for d in formations
    ]

    # Identity descriptive: successions and whether the immediate predecessor comes back.
    covered = max(d for d in formations if d <= bundle.supported_through)
    reversion: Counter[str] = Counter()
    for i, decision in enumerate(formations):
        for sid in sorted(table[i].linked):
            result = bundle.link_as_of(sid, decision)
            if result.basis != "succession" or (predecessor := immediate_predecessor(result)) is None:
                continue
            later = [(formations[j], table[j].linked.get(sid)) for j in range(i + 1, len(formations))]
            reversion[reversion_status(decision, predecessor, later, covered)] += 1

    # Join to #3360: membership from the pinned submissions.zip with the #3360 census code.
    with zipfile.ZipFile(inputs / "submissions.zip") as archive:
        members = sorted(n for n in archive.namelist() if _MAIN_MEMBER.fullmatch(n))
    masks: dict[str, int] = {}
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(
        args.processes, initializer=_init_population, initargs=(str(inputs / "submissions.zip"), formations)
    ) as pool:
        for cik10, failure, mask in pool.imap_unordered(_population, members, chunksize=512):
            if failure is None and mask:
                masks[cik10] = mask
    join = []
    for i, formation in enumerate(table):
        member_ciks = {cik for cik, mask in masks.items() if mask >> i & 1}
        linked_ciks = set(formation.linked.values())
        join.append(
            {
                "decision": formation.decision.isoformat(),
                "linked_ciks": len(linked_ciks),
                "linked_ciks_that_are_3360_members": len(linked_ciks & member_ciks),
                "linked_ciks_not_3360_members": len(linked_ciks - member_ciks),
                "3360_members": len(member_ciks),
                "3360_members_without_linked_series": len(member_ciks - linked_ciks),
            }
        )

    evidence = {
        "schema": EVIDENCE_SCHEMA,
        "bundle_manifest_sha256": args.manifest_sha256,
        "policy": manifest["policy"],
        "pit_manifest_sha256": manifest["input_sha256"]["pit_manifest"],
        "bars_sha256": _sha256_file(bars_path),
        "script_sha256": _sha256_file(Path(__file__)),
        "parameters": {
            "formations": [d.isoformat() for d in formations],
            "window_days": WINDOW_DAYS,
            "liquidity_bars": LIQUIDITY_BARS,
            "liquidity_span_days": LIQUIDITY_SPAN_DAYS,
            "capture_slack_days": CAPTURE_SLACK_DAYS,
            "outcome_horizon_days": OUTCOME_HORIZON_DAYS,
            "age_buckets": list(AGE_BUCKETS),
            "capture_end": capture_end.isoformat(),
            "form25_span": manifest["form25_span"],
        },
        "formations": [
            {
                "decision": f.decision.isoformat(),
                "counts": dict(sorted(f.counts.items())),
                "cross_tab": dict(sorted(f.cross_tab.items())),
                "form25_outcomes": dict(sorted(f.outcomes.items())),
                "newest_observation_age": dict(sorted(f.ages.items())),
            }
            for f in table
        ],
        "succession_reversion": dict(sorted(reversion.items())),
        "join_3360": join,
    }
    body = json.dumps(evidence, sort_keys=True, indent=1).encode()
    with (args.out_dir / "census.json").open("xb") as handle:
        handle.write(body)
    receipt = {
        "census_sha256": hashlib.sha256(body).hexdigest(),
        "bars_sha256": evidence["bars_sha256"],
        "bundle_manifest_sha256": args.manifest_sha256,
        "script_sha256": evidence["script_sha256"],
        "parameters_sha256": hashlib.sha256(canonical_json(evidence["parameters"])).hexdigest(),
    }
    with (args.out_dir / "manifest.json").open("x") as handle:
        handle.write(json.dumps(receipt, sort_keys=True, indent=1) + "\n")
    json.dump(receipt, sys.stdout, indent=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
