# ruff: noqa: E501
"""Post-2024-09 research price series with delistings stated (#3843).

The survivorship-free Intrader archive ends 2024-09-27, so every out-of-sample test after it has run on
survivor-only data. This assembles the dead side for 2024-10 onwards and reports how much of it we can price.

Sources, in precedence order, all read-only:
1. PWB `Stocks-Daily-Price@2026-09-09` (`total_return_reader.PWB_VENDOR`; carries `adj_close`, so total return);
2. PWB `Stocks-Daily-Price` 2026-07-08 load (`universe_selection.SURVIVOR_ONLY_VENDOR`; also `adj_close`);
3. eToro `price_daily` (split-adjusted closes, no dividends, so price return only).

The register is `sec_form25_common_equity_delistings`, one EVENT per (issuer CIK, resolved symbol) with its earliest and
latest filing; two filings for one event are trap 1 of sec-edgar.md §2.6, not two delistings. A PWB series is found with
`research_corpus_ingest.resolve_archive_symbol` (exact, then the Chapter 11 `Q` strip and separator variants). An eToro
instrument is found by the same symbol candidates against `instruments.symbol`, with the issuer CIK used only to choose
between two instruments sharing a symbol (`resolve_etoro_instrument`). An event with no resolved symbol is unpriceable.

Source rules for the verdicts (`classify_source`):
- `conflict` / `identity_unverified`: `research_corpus_ingest.classify_form25_match`, unchanged, so this script and the
  corpus linkage refuse the same cases. A series that starts after every filing is not the removed security's history.
- `terminates`: the last bar is on or before the latest filing + 10 days. 17 CFR 240.12d2-2(d)(1): "An application on
  Form 25 to strike a class of securities from listing ... will be effective 10 days after Form 25 is filed with the
  Commission." A series that ends by then ended with the listing. No lower bound on the gap is applied: a `(b)` filing
  trails the exchange suspension by months (sec-edgar.md §2.6 trap 5), and `series_termination` keys on the last bar,
  never on a Form 25 date.
- `continues`: the series runs past the effective date. Correct for an `(a)(3)` holding-company reorganisation; for
  `(b)` it is an OTC continuation or a later occupant of the ticker. Data is present, the delisting is not stated.
- `dead_before_window`: the last bar precedes 2024-10-01. The name was not investable inside the window, so it adds no
  survivorship bias to a post-2024 test whatever its filing date.
- `after_capture`: the effective date falls after the source's last bar date, so the source cannot show termination.

An event is COVERED when any source says `terminates` (the first in precedence order is the one recorded), and NOT
APPLICABLE when its best verdict is `dead_before_window`. Everything else is UNCOVERED and is labelled survivor-only:
a test on this series is survivorship-free only for the covered events.

Writes nothing to the database. Writing the linkage onto `research_price_series` was rejected: `hunt_harness`'s archive
identity hashes `delisting_source` and `updated_at`, so it would move the identity of every pinned PWB series.

Run: PYTHONPATH=. uv run python scripts/research_3843/post_2024_series.py [--json PATH]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any, Final

import psycopg

sys.path.insert(0, ".")
from app.config import settings  # noqa: E402
from app.services.research_corpus_ingest import (  # noqa: E402
    Form25Match,
    archive_symbol_candidates,
    classify_form25_match,
    resolve_archive_symbol,
)
from app.services.strategies.validated_universe import US_EQUITY_ASSET_CLASS, resolve_stocks_type_id  # noqa: E402
from app.services.total_return_reader import PWB_VENDOR  # noqa: E402
from app.services.universe_selection import INTRADER_CAPTURE_DATE, SURVIVOR_ONLY_VENDOR  # noqa: E402

#: First day after the survivorship-free archive's capture, as a month boundary.
WINDOW_START: Final = (INTRADER_CAPTURE_DATE.replace(day=1) + timedelta(days=32)).replace(day=1)
#: 17 CFR 240.12d2-2(d)(1): a Form 25 is effective 10 days after filing.
EFFECTIVE_DAYS: Final = 10
ETORO: Final = "etoro/price_daily"
SOURCES: Final = (PWB_VENDOR, SURVIVOR_ONLY_VENDOR, ETORO)

EVENTS_SQL = """
SELECT issuer_cik, resolved_symbol, min(filed_date), max(filed_date),
       count(DISTINCT rule_provision), min(rule_provision),
       count(DISTINCT suspension_date), min(suspension_date), min(issuer_name)
FROM sec_form25_common_equity_delistings
WHERE filed_date >= %(start)s
GROUP BY issuer_cik, resolved_symbol
ORDER BY 3, 2
"""

LISTED_SQL = """
SELECT count(*) AS listed,
       count(*) FILTER (WHERE EXISTS (SELECT 1 FROM research_price_series s
                                      WHERE s.instrument_id = i.instrument_id AND s.vendor = %(pwb)s)) AS with_pwb,
       count(*) FILTER (WHERE EXISTS (SELECT 1 FROM price_daily p
                                      WHERE p.instrument_id = i.instrument_id AND p.price_date >= %(start)s)) AS with_etoro
FROM instruments i JOIN exchanges e ON e.exchange_id = i.exchange
WHERE i.is_tradable AND e.asset_class = %(asset_class)s AND i.instrument_type_id = %(type_id)s
  AND i.canonical_instrument_id IS NULL
"""


@dataclass(frozen=True)
class Event:
    issuer_cik: str | None
    symbol: str | None
    issuer_name: str | None
    earliest_filed: date
    latest_filed: date
    provision_variants: int
    provision: str | None
    suspension_variants: int
    suspension_date: date | None


@dataclass(frozen=True)
class Found:
    """One source's series for an event: a PWB series id or an eToro instrument id, with its bar range."""

    ref: int
    first_bar: date
    last_bar: date
    last_close: float


def classify_source(event: Event, found: Found | None, capture_end: date, *, shared: bool = False) -> str:
    """One source's verdict on one event. Pure; the module docstring states the source rule for each branch.

    ``shared`` is True when a second event resolved to the same series in this source: two delisting events
    against one price history, which ``classify_form25_match`` calls a conflict and refuses to attribute.
    """
    if found is None:
        return "absent"
    if shared:
        return "conflict"
    verdict = classify_form25_match(
        Form25Match(
            symbol=event.symbol or "",
            first_bar=found.first_bar,
            last_bar=found.last_bar,
            earliest_filed=event.earliest_filed,
            latest_filed=event.latest_filed,
            provision_variants=event.provision_variants,
            provision=event.provision,
            suspension_variants=event.suspension_variants,
            suspension_date=event.suspension_date,
        )
    )
    if verdict in ("conflict", "identity_unverified"):
        return verdict
    if found.last_bar < WINDOW_START:
        return "dead_before_window"
    effective = event.latest_filed + timedelta(days=EFFECTIVE_DAYS)
    if effective > capture_end:
        return "after_capture"
    if found.last_bar <= effective:
        return "terminates"
    return "continues"


def event_status(verdicts: dict[str, str]) -> tuple[str, str | None]:
    """(status, covering source). ``covered`` by the first source in ``SOURCES`` order that says ``terminates``."""
    for source in SOURCES:
        if verdicts.get(source) == "terminates":
            return "covered", source
    if "dead_before_window" in verdicts.values() and not any(
        v in ("continues", "after_capture") for v in verdicts.values()
    ):
        return "not_applicable", None
    return "uncovered", None


def load_events(conn: psycopg.Connection[Any]) -> list[Event]:
    return [
        Event(
            issuer_cik=cik,
            symbol=sym,
            issuer_name=name,
            earliest_filed=first,
            latest_filed=last,
            provision_variants=int(pv),
            provision=prov,
            suspension_variants=int(sv),
            suspension_date=susp,
        )
        for cik, sym, first, last, pv, prov, sv, susp, name in conn.execute(EVENTS_SQL, {"start": WINDOW_START})
    ]


def load_pwb(conn: psycopg.Connection[Any], vendor: str) -> tuple[dict[str, Found], date]:
    """Vendor symbol → series bar range, and the capture's last bar date. Closes are read later, for matches only."""
    found = {
        str(sym): Found(int(sid), first, last, 0.0)
        for sym, sid, first, last in conn.execute(
            "SELECT vendor_symbol, series_id, first_bar, last_bar FROM research_price_series WHERE vendor = %s",
            (vendor,),
        )
    }
    assert found, f"no series for {vendor}"
    return found, max(f.last_bar for f in found.values())


def pwb_last_closes(conn: psycopg.Connection[Any], ids: list[int]) -> dict[int, float]:
    rows = conn.execute(
        """
        SELECT DISTINCT ON (series_id) series_id, close::float FROM research_price_daily
        WHERE series_id = ANY(%s) ORDER BY series_id, bar_date DESC
        """,
        (ids,),
    ).fetchall()
    return {int(sid): float(close) for sid, close in rows}


def resolve_etoro_instrument(symbol: str | None, issuer_ids: set[int], by_symbol: dict[str, list[int]]) -> int | None:
    """The eToro instrument for a Form 25 event, or ``None``. Pure.

    Keyed on the SECURITY's symbol, never on the issuer CIK alone: a Form 25 removes one class, and a CIK names the
    issuer, so a CIK-only match can hand the removed class a surviving sibling's bars. Candidates follow
    ``archive_symbol_candidates`` (exact before the Chapter 11 ``Q`` strip). The first candidate eToro carries
    decides; when two instruments share it, the issuer's CIK may narrow it to one, otherwise nothing resolves.
    """
    if symbol is None:
        return None
    for candidate in archive_symbol_candidates(symbol):
        ids = by_symbol.get(candidate)
        if not ids:
            continue
        narrowed = ids if len(ids) == 1 else [i for i in ids if i in issuer_ids]
        return narrowed[0] if len(narrowed) == 1 else None
    return None


def load_etoro(conn: psycopg.Connection[Any], events: list[Event]) -> tuple[dict[int, int], date]:
    """Event index → instrument id via ``resolve_etoro_instrument``, and the last ``price_daily`` date."""
    by_cik: dict[str, set[int]] = defaultdict(set)
    for cik, iid in conn.execute(
        "SELECT identifier_value, instrument_id FROM external_identifiers WHERE provider = 'sec' AND identifier_type = 'cik'"
    ):
        by_cik[str(cik)].add(int(iid))
    by_symbol: dict[str, list[int]] = defaultdict(list)
    for sym, iid in conn.execute(
        "SELECT upper(symbol), instrument_id FROM instruments WHERE canonical_instrument_id IS NULL"
    ):
        by_symbol[str(sym)].append(int(iid))
    out: dict[int, int] = {}
    for k, ev in enumerate(events):
        iid = resolve_etoro_instrument(ev.symbol, by_cik.get(ev.issuer_cik or "", set()), by_symbol)
        if iid is not None:
            out[k] = iid
    capture = conn.execute("SELECT max(price_date) FROM price_daily").fetchone()
    assert capture is not None and capture[0] is not None
    return out, capture[0]


def etoro_ranges(conn: psycopg.Connection[Any], ids: list[int]) -> dict[int, Found]:
    rows = conn.execute(
        """
        SELECT p.instrument_id, min(p.price_date), max(p.price_date),
               (array_agg(p.close::float ORDER BY p.price_date DESC))[1]
        FROM price_daily p WHERE p.instrument_id = ANY(%s) GROUP BY 1
        """,
        (ids,),
    ).fetchall()
    return {int(i): Found(int(i), first, last, float(close)) for i, first, last, close in rows}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Post-2024-09 research price series with delistings stated (#3843).")
    parser.add_argument("--json", help="write the per-event manifest here")
    args = parser.parse_args(argv)

    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        events = load_events(conn)
        found: dict[str, dict[int, Found]] = {}
        captures: dict[str, date] = {}
        for vendor in (PWB_VENDOR, SURVIVOR_ONLY_VENDOR):
            series, captures[vendor] = load_pwb(conn, vendor)
            found[vendor] = {}
            available = set(series)
            for k, ev in enumerate(events):
                sym = resolve_archive_symbol(ev.symbol, available) if ev.symbol else None
                if sym is not None:
                    found[vendor][k] = series[sym]
            closes = pwb_last_closes(conn, sorted({f.ref for f in found[vendor].values()}))
            found[vendor] = {k: Found(f.ref, f.first_bar, f.last_bar, closes[f.ref]) for k, f in found[vendor].items()}
        etoro_map, captures[ETORO] = load_etoro(conn, events)
        ranges = etoro_ranges(conn, sorted(set(etoro_map.values())))
        found[ETORO] = {k: ranges[iid] for k, iid in etoro_map.items() if iid in ranges}
        listed = conn.execute(
            LISTED_SQL,
            {
                "pwb": PWB_VENDOR,
                "start": WINDOW_START,
                "asset_class": US_EQUITY_ASSET_CLASS,
                "type_id": resolve_stocks_type_id(conn),
            },
        ).fetchone()
        assert listed is not None

    refs = {source: Counter(f.ref for f in found[source].values()) for source in SOURCES}
    manifest = []
    for k, ev in enumerate(events):
        verdicts = {}
        for source in SOURCES:
            f = found[source].get(k)
            verdicts[source] = classify_source(
                ev, f, captures[source], shared=f is not None and refs[source][f.ref] > 1
            )
        status, covering = event_status(verdicts)
        f = found[covering].get(k) if covering else None
        manifest.append(
            {
                **{key: str(v) if isinstance(v, date) else v for key, v in asdict(ev).items()},
                "month": ev.earliest_filed.strftime("%Y-%m"),
                "verdicts": verdicts,
                "status": status,
                "source": covering,
                "ref": f.ref if f else None,
                "last_bar": str(f.last_bar) if f else None,
                "last_close": f.last_close if f else None,
            }
        )

    print(f"window from {WINDOW_START}; captures: " + ", ".join(f"{s} {d}" for s, d in captures.items()))
    print(
        f"listed US stocks today: {listed[0]}; with a PWB@2026-09-09 series {listed[1]}; with eToro bars in window {listed[2]}"
    )
    print(f"Form 25 common-equity events filed from {WINDOW_START}: {len(manifest)}")
    print("\nverdict counts per source:")
    for source in SOURCES:
        print(f"  {source}: {dict(sorted(Counter(m['verdicts'][source] for m in manifest).items()))}")
    print("\nmonth    events covered pwb0909 pwb0708 etoro  n/a uncovered  covered/applicable")
    by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for m in manifest:
        by_month[m["month"]].append(m)
    tot = Counter[str]()
    for month in sorted(by_month):
        c = Counter(m["status"] for m in by_month[month])
        s = Counter(m["source"] for m in by_month[month] if m["status"] == "covered")
        tot.update(c)
        applicable = c["covered"] + c["uncovered"]
        share = f"{c['covered'] / applicable:.0%}" if applicable else "-"
        print(
            f"{month}  {len(by_month[month]):6d} {c['covered']:7d} {s[PWB_VENDOR]:7d} {s[SURVIVOR_ONLY_VENDOR]:7d} {s[ETORO]:5d}"
            f" {c['not_applicable']:4d} {c['uncovered']:9d}  {share:>8}"
        )
    applicable = tot["covered"] + tot["uncovered"]
    share = f"{tot['covered'] / applicable:.1%}" if applicable else "-"
    print(
        f"total    {len(manifest):6d} {tot['covered']:7d}{'':25} {tot['not_applicable']:4d} {tot['uncovered']:9d}"
        f"  {share:>8}"
    )
    print("\nuncovered, by why (absent from every source / otherwise the best verdict any source gave):")
    rank = ("continues", "after_capture", "conflict", "identity_unverified")
    why = Counter[tuple[str, str]]()
    for m in manifest:
        if m["status"] == "uncovered":
            best = next((r for r in rank if r in m["verdicts"].values()), "absent")
            why[(best, "no symbol" if m["symbol"] is None else m["provision"] or "(unparsed)")] += 1
    for (best, detail), n in sorted(why.items()):
        print(f"  {best:<20} {detail:<10} {n:4d}")
    print("\nuncovered events are SURVIVOR-ONLY: a test over this window is survivorship-free only for covered events.")
    if args.json:
        with open(args.json, "w") as fh:
            json.dump(manifest, fh, indent=1, sort_keys=True)
            fh.write("\n")
        print(f"manifest: {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
