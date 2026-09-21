"""#2834 ARM B, verdict §7 item 4 — can SEC XBRL adjudicate Intrader's split stamps?

The basis verdict (`docs/proposals/ta/2026-09-21-armb-split-only-basis-verdict.md`)
settled on candidate 1 — derive a split-only series from the per-bar split ratio the
Intrader archive ships — and recorded that EVERY check available to it is circular:

  * the internal check (``adj_close`` honours the stamp) is derived FROM the stamp,
    and 364 of 383 events a second processing rejects still pass it;
  * the cross-vendor check is a Yahoo redistribution, and `research_corpus_ingest.py`
    says so in terms: *"the two are ONE observation and agreement between them is
    circular, never corroborating"*.

§7 item 4 names the one untried direction: **a source outside the Yahoo lineage.**
SEC filings are the obvious candidate and nothing in this repo had looked.

This script measures whether that candidate can do the job. It proposes nothing, and
makes no persistent write.

Source rule
-----------
The governing artefact is the US GAAP taxonomy element definition, as recorded by our
own ingest in ``sec_facts_concept_catalog`` (label + description come from SEC):

  ``StockholdersEquityNoteStockSplitConversionRatio1``
    "Ratio applied to the conversion of stock split, for example but not limited to,
    one share converted to two or two shares converted to one."   units: Rate, pure

⚠ The definition names both orientations under one element. That does NOT by itself
prove a given value of ``2`` is used both ways in practice — it establishes only that
the element does not forbid it. The "orientation census" block in ``main()`` measures
what filers actually do; the two are reported separately and never merged into one
claim. ⚠ That census reads split DIRECTION off the stamp — the artefact under test — so
it is an association and is labelled as one in its own output.

The deprecated twin ``StockholdersEquityNoteStockSplitConversionRatio`` (deprecated
2013-01-31) carries eleven unit spellings in our catalog. It is probed as a fallback
and **matched separately** — a headline that pooled the two elements would be reporting
a different quantity than its own source-rule section describes.

Bounds, and one that is NOT structural
---------------------------------------
``research_price_series.cik`` exists (sql/250, built for #2282 2c) and is NULL on every
row — nothing has ever written it — so the CIK is re-derived here through the two
evidenced paths sql/250's vocabulary names. ``cik_null_census()`` measures that claim
rather than asserting it.

⚠ An earlier draft excluded pre-2009 events as "outside XBRL coverage by construction".
**That is false and this script refutes it**: a filing made today can disclose a split
from decades ago, and the corpus contains facts at instants as early as 1987 filed in
2013. The XBRL mandate bounds when a FILING exists, not which EVENT DATES it can
describe. No era filter is applied; the era breakdown of what actually matched is
measured and reported instead.

⚠ A third bound no census here can remove: sec-edgar.md §7.17 — companyfacts /
companyconcept return ONLY the non-dimensional default member, so a ratio tagged on
``us-gaap:StatementClassOfStockAxis`` is dropped ENTIRELY. Multi-class issuers are
exactly the population most likely to tag that way. **A 404 is not evidence that the
filer did not disclose the split**, and this measurement cannot distinguish the two.

Run
---
    PYTHONPATH=. uv run python -m scripts.measure_2834_armb_sec_adjudicator
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider
from scripts.measure_2834_armb_split_only_basis import (
    _EVENT_TOLERANCE,
    _admitted,
    check_events,
    read_split_events,
)

FACT_CACHE = Path("/tmp/2834_sec_split_facts.json")

CURRENT_CONCEPT = "StockholdersEquityNoteStockSplitConversionRatio1"
DEPRECATED_CONCEPT = "StockholdersEquityNoteStockSplitConversionRatio"

#: Date windows reported side by side. There is no published rule fixing how far a
#: disclosed context instant may sit from an ex-date, so no single window is
#: defensible on its own — every headline is quoted with its window attached.
WINDOWS = (3, 10, 45)

#: Headline magnitude tolerance. Pinned to the PREDECESSOR's ``_EVENT_TOLERANCE`` so
#: that "SEC agrees" and "the reference disagrees" are decided at the same threshold.
#: A looser cut is reported as sensitivity, never as the headline.
TOLERANCES = (_EVENT_TOLERANCE, Decimal("0.02"))


@dataclass(frozen=True)
class SecFact:
    """One companyconcept fact.

    ⚠ ``instant`` is populated ONLY for an instant context (SEC sends ``end`` with no
    ``start``). A duration fact fixes the ratio to a reporting PERIOD and recovers no
    event date, so it is retained for the census and excluded from matching — an
    earlier draft matched on ``end`` regardless and silently admitted every one of
    them. ⚠ The count is deliberately not written here: ``main()`` prints it as
    "DURATION context", and a hand-copied figure goes stale the moment the probe set
    changes (it did — an earlier cache over fewer CIKs gave a different number).
    """

    cik: str
    concept: str
    unit: str
    instant: date | None
    period_start: date | None
    period_end: date | None
    val: Decimal
    form: str | None
    filed: date | None

    @property
    def is_instant(self) -> bool:
        return self.instant is not None and self.period_start is None


def _parse_date(raw: str | None) -> date | None:
    return date.fromisoformat(raw) if raw else None


def cik_null_census(conn: psycopg.Connection[tuple[Any, ...]]) -> tuple[int, int]:
    """``(rows, rows_with_cik)`` on ``research_price_series`` — measured, not asserted."""
    row = conn.execute("SELECT count(*), count(cik) FROM research_price_series").fetchone()
    assert row is not None
    return int(row[0]), int(row[1])


def cik_map(
    conn: psycopg.Connection[tuple[Any, ...]], series_ids: list[int]
) -> tuple[dict[int, tuple[str, str]], int, int]:
    """``series_id -> (cik, cik_source)`` via sql/250's own evidenced vocabulary.

    Returns the map, the number of series both paths resolve, and the number where
    they DISAGREE.

    ⚠ ``instrument_sec_profile`` wins where both exist — it is an identity carried
    from a resolved instrument rather than one recovered from a delisting filing.
    The disagreement count is returned rather than swallowed, because a silent
    ``setdefault`` over conflicting rows is how an identity bug hides.

    ⚠ The Form 25 path joins on ``resolved_symbol`` equality. sql/353 records that
    this equality no longer reproduces the Q-suffix resolution, and a CIK identifies
    an ISSUER, not the share class a series tracks. Both are unguarded here and bound
    the precision of every figure derived from this path.
    """
    profile: dict[int, str] = {}
    rows = conn.execute(
        """SELECT r.series_id, p.cik
             FROM research_price_series r
             JOIN instrument_sec_profile p ON p.instrument_id = r.instrument_id
            WHERE r.series_id = ANY(%s) AND p.cik IS NOT NULL""",
        (series_ids,),
    ).fetchall()
    for series_id, cik in rows:
        profile[int(series_id)] = str(cik)

    form25: dict[int, str] = {}
    rows = conn.execute(
        """SELECT r.series_id, d.issuer_cik
             FROM research_price_series r
             JOIN sec_form25_common_equity_delistings d
               ON d.resolved_symbol = r.vendor_symbol
            WHERE r.series_id = ANY(%s)
              AND r.delisting_source = 'sec_form25'
              AND d.issuer_cik IS NOT NULL""",
        (series_ids,),
    ).fetchall()
    for series_id, cik in rows:
        form25[int(series_id)] = str(cik)

    both = set(profile) & set(form25)
    conflicts = sum(1 for s in both if profile[s] != form25[s])

    out: dict[int, tuple[str, str]] = {s: (c, "instrument_sec_profile") for s, c in profile.items()}
    for series_id, cik in form25.items():
        out.setdefault(series_id, (cik, "sec_form25"))
    return out, len(both), conflicts


def fetch_facts(provider: SecFundamentalsProvider, cik: str) -> list[SecFact]:
    """Both concepts for one CIK.

    The deprecated twin is tried whenever the current element yields NO usable fact —
    that is a 404 *or* a 200 whose ``units`` are empty. Both are "the current element
    did not answer", and treating only the 404 as such would undercount the fallback.
    """
    facts: list[SecFact] = []
    for concept in (CURRENT_CONCEPT, DEPRECATED_CONCEPT):
        payload = provider.fetch_concept(cik, "us-gaap", concept)
        if payload is None:
            continue
        for unit, entries in (payload.get("units") or {}).items():
            for entry in entries:
                raw_val = entry.get("val")
                if raw_val is None:
                    continue
                start = _parse_date(entry.get("start"))
                end = _parse_date(entry.get("end"))
                facts.append(
                    SecFact(
                        cik=cik,
                        concept=concept,
                        unit=str(unit),
                        instant=end if start is None else None,
                        period_start=start,
                        period_end=end,
                        val=Decimal(str(raw_val)),
                        form=entry.get("form"),
                        filed=_parse_date(entry.get("filed")),
                    )
                )
        if facts:
            break
    return facts


def load_cached_facts(ciks: list[str]) -> dict[str, list[SecFact]] | None:
    """Re-read a previous probe, but ONLY if it covers the same CIK set.

    ⚠ A cache keyed on nothing would silently answer a DIFFERENT question than the one
    asked — a narrower CIK set would read as "SEC has no fact for these".
    """
    if not FACT_CACHE.exists():
        return None
    blob = json.loads(FACT_CACHE.read_text())
    if set(blob.get("ciks") or []) != set(ciks) or blob.get("schema") != 2:
        return None
    out: dict[str, list[SecFact]] = {}
    for cik, rows in blob["facts"].items():
        out[cik] = [
            SecFact(
                cik=cik,
                concept=r["concept"],
                unit=r["unit"],
                instant=_parse_date(r["instant"]),
                period_start=_parse_date(r["period_start"]),
                period_end=_parse_date(r["period_end"]),
                val=Decimal(r["val"]),
                form=r["form"],
                filed=_parse_date(r["filed"]),
            )
            for r in rows
        ]
    return out


def save_cached_facts(facts_by_cik: dict[str, list[SecFact]]) -> None:
    FACT_CACHE.write_text(
        json.dumps(
            {
                "schema": 2,
                "ciks": sorted(facts_by_cik),
                "facts": {
                    cik: [
                        {
                            "concept": f.concept,
                            "unit": f.unit,
                            "instant": f.instant.isoformat() if f.instant else None,
                            "period_start": f.period_start.isoformat() if f.period_start else None,
                            "period_end": f.period_end.isoformat() if f.period_end else None,
                            "val": str(f.val),
                            "form": f.form,
                            "filed": f.filed.isoformat() if f.filed else None,
                        }
                        for f in rows
                    ]
                    for cik, rows in facts_by_cik.items()
                },
            }
        )
    )


def agrees(stamp: Decimal, val: Decimal, tol: Decimal) -> str:
    """Classify a (stamp, SEC value) pair as direct / reciprocal / disagree.

    ⚠ ``reciprocal`` is NOT a near-miss to fold into agreement. It is the orientation
    ambiguity the element definition admits, and counting it as agreement would assert
    a direction the source does not carry.
    """
    if val <= 0 or stamp <= 0:
        return "unusable"
    if abs(val - stamp) / stamp <= tol:
        return "direct"
    if abs(Decimal(1) / val - stamp) / stamp <= tol:
        return "reciprocal"
    return "disagree"


def pick_fact(candidates: list[SecFact], target: date, window: int) -> tuple[SecFact | None, bool]:
    """Nearest INSTANT fact within ``window`` days, plus whether the tie was material.

    ⚠ ``min`` alone picks whichever equally-near fact happens to sort first, and the
    cache contains events whose tied facts carry DIFFERENT values. So the tie-break is
    explicit — prefer the current element, then the latest filing — and a tie whose
    candidates disagree on value is flagged so the count can be reported rather than
    silently resolved by array order.
    """
    usable = [f for f in candidates if f.is_instant and abs((f.instant - target).days) <= window]  # type: ignore[operator,arg-type]
    if not usable:
        return None, False
    best_delta = min(abs((f.instant - target).days) for f in usable)  # type: ignore[operator,arg-type]
    tied = [f for f in usable if abs((f.instant - target).days) == best_delta]  # type: ignore[operator,arg-type]
    material_tie = len({f.val for f in tied}) > 1
    tied.sort(key=lambda f: (f.concept != CURRENT_CONCEPT, -(f.filed.toordinal() if f.filed else 0), f.val))
    return tied[0], material_tie


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        admitted = _admitted(conn)
        events, _missing, _divbars = read_split_events(admitted)
        series_ids = sorted({e.series_id for e in events})
        mapping, dual_path, conflicts = cik_map(conn, series_ids)
        rps_rows, rps_with_cik = cik_null_census(conn)
        checks, skipped = check_events(conn, events)

    total = len(events)
    reachable = [e for e in events if e.series_id in mapping]

    print("=" * 74)
    print("#2834 ARM B §7.4 — SEC XBRL as a non-Yahoo adjudicator for split stamps")
    print("=" * 74)
    print(f"research_price_series rows / with a CIK   {rps_rows} / {rps_with_cik}")
    print(f"stamped events                           {total}")
    print(f"  on a CIK-reachable series              {len(reachable)} ({100 * len(reachable) / total:.1f}%)")
    print(f"  series resolved by BOTH cik paths      {dual_path} (disagreeing: {conflicts})")
    by_source = Counter(mapping[e.series_id][1] for e in reachable)
    print(f"  reachable events by cik_source         {dict(by_source)}")
    print("  ⚠ NO era filter — a filing made today can disclose a decades-old split.")

    ciks = sorted({mapping[e.series_id][0] for e in reachable})
    facts_by_cik = load_cached_facts(ciks)
    if facts_by_cik is None:
        print(f"\nprobing companyconcept for {len(ciks)} CIKs (<= {2 * len(ciks)} requests, shared SEC budget)\n")
        facts_by_cik = {}
        with SecFundamentalsProvider(user_agent=settings.sec_user_agent) as provider:
            for idx, cik in enumerate(ciks, start=1):
                facts_by_cik[cik] = fetch_facts(provider, cik)
                if idx % 200 == 0:
                    print(f"  {idx}/{len(ciks)} CIKs probed", flush=True)
        save_cached_facts(facts_by_cik)
    else:
        print(f"\n(reusing {FACT_CACHE} for {len(ciks)} CIKs — delete it to re-probe SEC)")

    all_facts = [f for fl in facts_by_cik.values() for f in fl]
    tagged = {c for c, fl in facts_by_cik.items() if fl}
    print(
        f"\nCIKs returning >=1 split-ratio fact      {len(tagged)}/{len(ciks)} ({100 * len(tagged) / len(ciks):.1f}%)"
    )
    print("  ⚠ a 404 is NOT 'the filer did not disclose' — sec-edgar.md §7.17 drops")
    print("    every dimensional (StatementClassOfStockAxis) fact from this API.")
    print(f"facts retrieved                          {len(all_facts)}")
    print(f"  by concept                             {dict(Counter(f.concept for f in all_facts))}")
    print(f"  by unit                                {dict(Counter(f.unit for f in all_facts))}")
    instants = sum(1 for f in all_facts if f.is_instant)
    print(f"  INSTANT context (usable for dating)    {instants}")
    print(f"  DURATION context (no event date)       {len(all_facts) - instants}  [excluded from matching]")
    pre2009 = [f for f in all_facts if f.is_instant and f.instant and f.instant.year < 2009]
    print(
        f"  instant facts dated before 2009        {len(pre2009)}"
        f"  (all filed later: {sum(1 for f in pre2009 if f.filed and f.filed.year >= 2009)})"
    )
    print("    ^ this is why no era filter is applied.")

    # --- window + tolerance sensitivity ----------------------------------------
    print(f"\nmatched events by window x tolerance (denominator = all {total} stamps):")
    print(f"  {'window':>7} {'matched':>8} {'pct':>7}  {'tol':>5} {'direct':>7} {'recip':>6} {'disagree':>9}")
    matches: dict[int, dict[int, SecFact]] = {}
    material_ties: dict[int, int] = {}
    for window in WINDOWS:
        picked: dict[int, SecFact] = {}
        ties = 0
        for idx, event in enumerate(events):
            if event.series_id not in mapping:
                continue
            fact, tie = pick_fact(facts_by_cik.get(mapping[event.series_id][0], []), event.bar_date, window)
            if fact is not None:
                picked[idx] = fact
                ties += int(tie)
        matches[window] = picked
        material_ties[window] = ties
        for tol in TOLERANCES:
            verdict = Counter(agrees(events[i].factor, f.val, tol) for i, f in picked.items())
            print(
                f"  {'+/-' + str(window) + 'd':>7} {len(picked):>8} {100 * len(picked) / total:>6.2f}% "
                f"{str(tol):>5} {verdict['direct']:>7} {verdict['reciprocal']:>6} {verdict['disagree']:>9}"
            )
    print(f"  material ties (tied facts disagreeing on value): {material_ties}")

    headline_window, headline_tol = 10, _EVENT_TOLERANCE
    picked = matches[headline_window]
    print(
        f"\nHEADLINE = +/-{headline_window}d, tolerance {headline_tol} (pinned to the predecessor's dispute threshold)"
    )
    deprecated_used = sum(1 for f in picked.values() if f.concept == DEPRECATED_CONCEPT)
    print(f"  matches drawn from the DEPRECATED element: {deprecated_used} (reported, not pooled)")
    reuse = Counter((f.cik, f.instant, f.val) for f in picked.values())
    print(f"  fact records serving >1 stamped event:     {sum(1 for v in reuse.values() if v > 1)}")

    # --- orientation census ------------------------------------------------------
    print("\norientation census — is the reciprocal class systematic?")
    print("  ⚠ 'forward'/'reverse' is read off the STAMP, which is the artefact under")
    print("    test. This is an association, not an established filing convention.")
    orient: Counter[tuple[str, str]] = Counter()
    for idx, fact in picked.items():
        kind = "reverse(stamp<1)" if events[idx].factor < 1 else "forward(stamp>1)"
        orient[(kind, agrees(events[idx].factor, fact.val, headline_tol))] += 1
    for key in sorted(orient):
        print(f"  {key[0]:<18} {key[1]:<11} {orient[key]}")

    # --- dispute classes ---------------------------------------------------------
    print("\n" + "-" * 74)
    print("cross-tab vs the predecessor's §3 dispute classes")
    print("⚠ those class names are 1%-ratio PREDICATES, not established causes — the")
    print("  predecessor allows date offsets, reference errors and symbol collisions.")
    print("-" * 74)
    by_event = {(events[i].series_id, events[i].bar_date): f for i, f in picked.items()}
    classes: dict[str, list[Any]] = {"no_step": [], "magnitude": [], "ref_unadjusted": [], "agreeing": []}
    unreferenced = []
    for check in checks:
        if check.ref_ratio is None:
            unreferenced.append(check)
        elif check.agrees:
            classes["agreeing"].append(check)
        elif check.reference_is_unadjusted:
            classes["ref_unadjusted"].append(check)
        elif check.implied_factor is not None and abs(check.implied_factor - 1) <= _EVENT_TOLERANCE:
            classes["no_step"].append(check)
        else:
            classes["magnitude"].append(check)

    covered_totals: dict[str, int] = {}
    for name, members in list(classes.items()) + [("no reference at all", unreferenced)]:
        covered = [c for c in members if (c.event.series_id, c.event.bar_date) in by_event]
        verdict = Counter(
            agrees(c.event.factor, by_event[(c.event.series_id, c.event.bar_date)].val, headline_tol) for c in covered
        )
        covered_totals[name] = len(covered)
        pct = 0.0 if not members else 100.0 * len(covered) / len(members)
        print(f"  {name:<22} {len(members):>5} events, SEC covers {len(covered):>4} ({pct:5.1f}%)  {dict(verdict)}")
    print(f"  {'(events skipped by check_events)':<22} {skipped:>5}")

    disputed = len(classes["no_step"]) + len(classes["magnitude"])
    disputed_all = disputed + len(classes["ref_unadjusted"])
    cov = covered_totals["no_step"] + covered_totals["magnitude"]
    print(f"\n  SEC covers {cov} of the {disputed} UNCORROBORATED events ({100 * cov / disputed:.1f}%)")
    print(
        f"  SEC covers {cov + covered_totals['ref_unadjusted']} of all {disputed_all} DISAGREEING events "
        f"({100 * (cov + covered_totals['ref_unadjusted']) / disputed_all:.1f}%)"
    )
    print("\n⚠ 'covers' means a dated fact within the window whose magnitude was compared.")
    print("  It does NOT mean the per-bar treatment was established. A 'disagree' backs")
    print("  neither the stamp nor the reference — it is not a vote for the reference.")

    json.dump(
        {
            "events_total": total,
            "events_cik_reachable": len(reachable),
            "ciks_probed": len(ciks),
            "ciks_tagged": len(tagged),
            "headline_window": headline_window,
            "headline_tolerance": str(headline_tol),
            "matched": {str(w): len(m) for w, m in matches.items()},
            "material_ties": material_ties,
            "dispute_coverage": covered_totals,
        },
        open("/tmp/2834_sec_adjudicator.json", "w"),
        indent=2,
    )


if __name__ == "__main__":
    main()
