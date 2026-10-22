"""Full-population A/B for the #3947 DPS inline-scale correction (writes nothing: one rolled-back transaction).

Spec: docs/specs/fundamentals/2026-10-21-3947-dps-inline-scale.md, Acceptance items 1, 2 and 4, and Rollback step 4.

Everything runs in one REPEATABLE READ transaction, opened with ``SELECT 1`` first (``conn.transaction()`` on an idle
psycopg connection commits), and is rolled back at the end. The normaliser runs with decisions off and on through
its ``dps_inline_scale`` argument, not the flag, and every figure is read from that one snapshot.

1. **Coverage.** Population accessions by active generation (``inline``, ``not_inline``, none) and by work state;
   exhausted accessions listed by ``last_error`` (unknown coverage, kept as stored); every stored fact of the concept
   with its decision, counted by reason for all facts and for derivation-eligible ones (``dropped_before_decision``
   when a structural filter drops the raw value); every ``unresolved``, ``entity_mismatch`` and ``no_match``
   accession. Gate: 0 pending and 0 retrying (``--allow-incomplete`` reports anyway, for a dry run).
2. **Fact-level A/B**, for every instrument with a restore or a refusal: ``financial_periods`` with decisions off
   against on. Gates: the same rows, every column but ``dps_declared`` equal, and every changed ``dps_declared`` cell
   explained by its lineage (a decided fact ending at that period, or a derived Q4 whose FY or Q1–Q3 cell changed).
   Every restored fact is listed with its display text, scale, restored value and the other accessions' ``unscaled``
   facts for the same period, classified ``agrees`` / ``disagrees`` / ``none`` (reported, not gated).
4. **Downstream**, off against on: ``instrument_dividend_summary.ttm_dps`` / ``latest_dps`` and
   ``financial_periods_ttm.dps_declared_ttm``.

Also reported, not gated: ``unreachable``, the affected instruments' stored DPS cells that no normalisation rewrites.
The canonical merge keeps a period row that the current raw facts no longer derive (durable history; its source
filing's facts were retention-swept), so neither arm touches it (WHR Q2 2023 holds 1,750,000 for a displayed 1.75).

``--verify-stored off|on`` is Rollback step 4 (or a post-enablement check): every instrument holding the concept is
normalised in that mode and compared with its stored rows and downstream fields; any difference fails. It is only
meaningful after step 3's re-normalisation: stored rows drift from a fresh normalisation as ingestion and code move
(run it before any rebuild to see that drift), while two normalisations in one snapshot agree exactly.

Usage: ``PYTHONPATH=. uv run python -m scripts.ab_3947_dps_inline_scale [--out PATH] [--allow-incomplete]
[--verify-stored off|on]``
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from collections.abc import Sequence
from typing import Any

import psycopg

from app.config import settings
from app.services import dps_inline_scale_scan as scan
from app.services.fundamentals import (
    DpsScaleDecision,
    FactRow,
    _dps_fact_inline_scale_decisions,
    _fact_period_ended_by_filing,
    _per_share_value_is_representable,
    normalize_financial_periods,
)
from app.services.inline_xbrl_dps import CONCEPT, Occurrence

_KEY = ("period_end_date", "period_type", "fiscal_year", "fiscal_quarter")


def _facts(conn: psycopg.Connection[Any]) -> list[tuple[int, bool, FactRow]]:
    """Every stored fact of the concept: ``(instrument_id, derivation_eligible, fact)``."""
    rows = conn.execute(
        """
        SELECT instrument_id, concept, unit, period_start, period_end, val, frame, form_type, fiscal_year,
               fiscal_period, accession_number, filed_date
        FROM financial_facts_raw WHERE concept = %s
        """,
        (CONCEPT,),
    ).fetchall()
    return [
        (
            r[0],
            r[8] is not None and r[9] is not None,
            FactRow(r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8] or 0, r[9] or "", r[10], r[11]),
        )
        for r in rows
    ]


def _inputs(
    conn: psycopg.Connection[Any],
) -> tuple[dict[str, str], dict[int, frozenset[str]], dict[str, list[Occurrence]]]:
    scans = dict(
        conn.execute("SELECT accession_number, outcome FROM sec_inline_scale_scan WHERE outcome IS NOT NULL").fetchall()
    )
    ciks: dict[int, set[str]] = defaultdict(set)
    for iid, cik in conn.execute(
        "SELECT instrument_id, identifier_value FROM external_identifiers WHERE provider = 'sec'"
        " AND identifier_type = 'cik'"
    ).fetchall():
        ciks[iid].add(str(cik).zfill(10))
    occurrences: dict[str, list[Occurrence]] = defaultdict(list)
    for r in conn.execute(
        """
        SELECT accession_number, document, element_ordinal, fact_id, context_ref_raw, unit_ref_raw, scale_raw,
               sign_raw, format_raw, decimals_raw, display_text, entity_cik, period_start, period_end, dimensional,
               unit, scale, instance_value, unresolved
        FROM sec_inline_dps_occurrence ORDER BY accession_number, document, element_ordinal
        """
    ).fetchall():
        occurrences[r[0]].append(Occurrence(*r[1:]))
    return scans, {k: frozenset(v) for k, v in ciks.items()}, dict(occurrences)


def _coverage(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    generation = conn.execute(
        scan._POPULATION_SQL + "SELECT COALESCE(s.outcome, 'none'), count(*) FROM pop p"
        " LEFT JOIN sec_inline_scale_scan s USING (accession_number) GROUP BY 1 ORDER BY 1",
        {"concept": CONCEPT, "scope_start": scan.SCOPE_START},
    ).fetchall()
    exhausted = conn.execute(
        scan._POPULATION_SQL
        + "SELECT s.last_error, count(*) FROM pop p JOIN sec_inline_scale_scan s USING (accession_number)"
        " WHERE s.parser_version IS DISTINCT FROM %(version)s AND s.attempt_version = %(version)s"
        " AND s.attempts >= %(max_attempts)s GROUP BY 1 ORDER BY 2 DESC",
        {
            "concept": CONCEPT,
            "scope_start": scan.SCOPE_START,
            "version": scan.PARSER_VERSION,
            "max_attempts": scan.MAX_ATTEMPTS,
        },
    ).fetchall()
    state, queue = scan.work_state(conn)
    return {
        "active_generation": dict(generation),
        "work_state": state,
        "queue_length": queue,
        "exhausted_by_last_error": [[e, n] for e, n in exhausted],
    }


def _periods(conn: psycopg.Connection[Any], ids: Sequence[int]) -> dict[tuple[Any, ...], dict[str, Any]]:
    cur = conn.execute("SELECT * FROM financial_periods WHERE instrument_id = ANY(%s)", (list(ids),))
    names = [d.name for d in cur.description or ()]
    out: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in cur.fetchall():
        record = dict(zip(names, row, strict=True))
        out[(record["instrument_id"], *(record[k] for k in _KEY))] = record
    return out


def _downstream(conn: psycopg.Connection[Any], ids: Sequence[int]) -> dict[int, tuple[Any, ...]]:
    rows = conn.execute(
        """
        SELECT i.id, d.ttm_dps, d.latest_dps, t.dps_declared_ttm
        FROM unnest(%s::bigint[]) AS i(id)
        LEFT JOIN instrument_dividend_summary d ON d.instrument_id = i.id
        LEFT JOIN financial_periods_ttm t ON t.instrument_id = i.id
        """,
        (list(ids),),
    ).fetchall()
    return {r[0]: tuple(r[1:]) for r in rows}


def _normalise(conn: psycopg.Connection[Any], ids: Sequence[int], *, on: bool) -> int:
    return normalize_financial_periods(conn, list(ids), dps_inline_scale=on).instruments_failed


def _diff(
    off: dict[tuple[Any, ...], dict[str, Any]],
    on: dict[tuple[Any, ...], dict[str, Any]],
    labels: tuple[str, str] = ("off", "on"),
) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Structural differences (rows or any column but ``dps_declared``), and the changed DPS cells."""
    problems = [f"row only {labels[0]}: {k}" for k in off.keys() - on.keys()] + [
        f"row only {labels[1]}: {k}" for k in on.keys() - off.keys()
    ]
    changed: list[tuple[Any, ...]] = []
    for key in off.keys() & on.keys():
        a, b = off[key], on[key]
        other = [c for c in a if c != "dps_declared" and a[c] != b[c]]
        if other:
            problems.append(f"{key}: columns changed {other}")
        if a["dps_declared"] != b["dps_declared"]:
            changed.append(key)
    return problems, changed


def _explained(
    key: tuple[Any, ...],
    row: dict[str, Any],
    changed: set[tuple[Any, ...]],
    decided_ends: dict[int, set[Any]],
) -> bool:
    iid = key[0]
    if row["is_derived"] and row["period_type"] == "Q4":
        return any(k[0] == iid and k[3] == row["fiscal_year"] and k[2] in ("FY", "Q1", "Q2", "Q3") for k in changed)
    return row["period_end_date"] in decided_ends.get(iid, set())


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--out", help="write the full JSON report here")
    parser.add_argument("--allow-incomplete", action="store_true", help="report even with pending/retrying work")
    parser.add_argument("--verify-stored", choices=("off", "on"), help="Rollback step 4: stored rows vs one mode")
    args = parser.parse_args(argv)

    report: dict[str, Any] = {}
    failures: list[str] = []
    with psycopg.connect(settings.database_url) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.execute("SELECT 1")  # open the snapshot: conn.transaction() on an idle connection would commit
        try:
            facts = _facts(conn)
            scans, ciks, occurrences = _inputs(conn)
            by_instrument: dict[int, list[FactRow]] = defaultdict(list)
            eligible: dict[FactRow, bool] = {}
            for iid, ok, fact in facts:
                by_instrument[iid].append(fact)
                eligible[fact] = eligible.get(fact, False) or ok
            decisions: dict[int, dict[FactRow, DpsScaleDecision]] = {
                iid: _dps_fact_inline_scale_decisions(fs, scans, ciks.get(iid, frozenset()), occurrences)
                for iid, fs in by_instrument.items()
            }

            if args.verify_stored:
                ids = sorted(by_instrument)
                stored, stored_down = _periods(conn, ids), _downstream(conn, ids)
                failed = _normalise(conn, ids, on=args.verify_stored == "on")
                after, after_down = _periods(conn, ids), _downstream(conn, ids)
                problems, changed = _diff(stored, after, ("stored", args.verify_stored))
                down = [i for i in ids if stored_down.get(i) != after_down.get(i)]
                report["verify_stored"] = {
                    "mode": args.verify_stored,
                    "instruments": len(ids),
                    "normalise_failures": failed,
                    "structural": problems,
                    "dps_cells_differing": [list(map(str, k)) for k in changed],
                    "downstream_differing": down,
                }
                if failed or problems or changed or down:
                    failures.append("stored rows differ from the requested mode")
            else:
                coverage = _coverage(conn)
                report["coverage"] = coverage
                if coverage["work_state"]["pending"] or coverage["work_state"]["retrying"]:
                    msg = f"scan incomplete: {coverage['work_state']}"
                    if args.allow_incomplete:
                        print(f"WARNING: {msg}", file=sys.stderr)
                    else:
                        failures.append(msg)

                all_reasons: Counter[str] = Counter()
                eligible_reasons: Counter[str] = Counter()
                doubt: dict[str, set[str]] = defaultdict(set)
                for iid, ds in decisions.items():
                    for fact, d in ds.items():
                        all_reasons[d.reason] += 1
                        if eligible[fact]:
                            survives = _per_share_value_is_representable(fact) and _fact_period_ended_by_filing(fact)
                            eligible_reasons["dropped_before_decision" if not survives else d.reason] += 1
                        if d.reason in ("unresolved", "entity_mismatch", "no_match"):
                            doubt[fact.accession_number].add(d.reason)
                report["decisions"] = {
                    "facts": len(facts),
                    "by_reason_all": dict(all_reasons.most_common()),
                    "by_reason_derivation_eligible": dict(eligible_reasons.most_common()),
                    "doubt_accessions": {a: sorted(r) for a, r in sorted(doubt.items())},
                }

                affected = sorted(iid for iid, ds in decisions.items() if any(d.action != "keep" for d in ds.values()))
                restored = []
                for iid in affected:
                    for fact, d in decisions[iid].items():
                        if d.action != "restore":
                            continue
                        others = sorted(
                            {
                                o.val
                                for o, od in decisions[iid].items()
                                if od.reason == "unscaled"
                                and o.unit == fact.unit
                                and (o.period_start, o.period_end) == (fact.period_start, fact.period_end)
                            }
                        )
                        assert d.value is not None
                        verdict = (
                            "none" if not others else "agrees" if all(v == d.value for v in others) else "disagrees"
                        )
                        displays = sorted(
                            {
                                o.display_text
                                for o in occurrences.get(fact.accession_number, ())
                                if (o.document, o.element_ordinal) in d.occurrences
                            }
                        )
                        restored.append(
                            {
                                "instrument_id": iid,
                                "accession": fact.accession_number,
                                "period": [str(fact.period_start), str(fact.period_end)],
                                "display_text": displays,
                                "scale": d.scale,
                                "stored": str(fact.val),
                                "restored": str(d.value),
                                "evidence": [str(v) for v in others],
                                "verdict": verdict,
                            }
                        )
                refused = [
                    {
                        "instrument_id": iid,
                        "accession": f.accession_number,
                        "period": [str(f.period_start), str(f.period_end)],
                    }
                    for iid in affected
                    for f, d in decisions[iid].items()
                    if d.action == "refuse"
                ]

                failed_off = _normalise(conn, affected, on=False)
                off, off_down = _periods(conn, affected), _downstream(conn, affected)
                failed_on = _normalise(conn, affected, on=True)
                on, on_down = _periods(conn, affected), _downstream(conn, affected)
                unreachable = conn.execute(
                    """
                    SELECT p.instrument_id, p.period_end_date, p.period_type, p.dps_declared, p.source_ref
                    FROM financial_periods p
                    WHERE p.instrument_id = ANY(%s) AND p.dps_declared IS NOT NULL AND NOT EXISTS (
                        SELECT 1 FROM financial_periods_raw r
                        WHERE r.instrument_id = p.instrument_id AND r.source = 'sec_edgar'
                          AND r.fiscal_year = p.fiscal_year AND r.period_type = p.period_type
                          AND r.fiscal_quarter IS NOT DISTINCT FROM p.fiscal_quarter)
                    ORDER BY 1, 2
                    """,
                    (affected,),
                ).fetchall()
                problems, changed = _diff(off, on)
                decided_ends: dict[int, set[Any]] = defaultdict(set)
                for iid in affected:
                    for f, d in decisions[iid].items():
                        if d.action != "keep":
                            decided_ends[iid].add(f.period_end)
                changed_set = set(changed)
                unexplained = [k for k in changed if not _explained(k, on[k], changed_set, decided_ends)]
                report["ab"] = {
                    "instruments": len(affected),
                    "normalise_failures": {"off": failed_off, "on": failed_on},
                    "structural": problems,
                    "dps_cells_changed": [
                        {"key": list(map(str, k)), "off": str(off[k]["dps_declared"]), "on": str(on[k]["dps_declared"])}
                        for k in sorted(changed, key=str)
                    ],
                    "unexplained": [list(map(str, k)) for k in unexplained],
                    "unreachable": [list(map(str, r)) for r in unreachable],
                    "restored": restored,
                    "restored_by_verdict": dict(Counter(r["verdict"] for r in restored)),
                    "refused": refused,
                    "downstream": [
                        {
                            "instrument_id": i,
                            "off": list(map(str, off_down.get(i, ()))),
                            "on": list(map(str, on_down.get(i, ()))),
                        }
                        for i in affected
                        if off_down.get(i) != on_down.get(i)
                    ],
                }
                if failed_off or failed_on:
                    failures.append(f"normalise failures off={failed_off} on={failed_on}")
                if problems:
                    failures.append(f"{len(problems)} structural differences")
                if unexplained:
                    failures.append(f"{len(unexplained)} unexplained dps_declared changes")
        finally:
            conn.rollback()

    text = json.dumps(report, indent=2, default=str)
    if args.out:
        with open(args.out, "w") as fh:
            fh.write(text)
    summary = {k: v for k, v in report.items() if k != "ab"}
    if "ab" in report:
        ab = report["ab"]
        summary["ab"] = {
            "instruments": ab["instruments"],
            "dps_cells_changed": len(ab["dps_cells_changed"]),
            "unexplained": ab["unexplained"],
            "structural": ab["structural"][:20],
            "restored": len(ab["restored"]),
            "restored_by_verdict": ab["restored_by_verdict"],
            "refused": len(ab["refused"]),
            "unreachable": len(ab["unreachable"]),
            "downstream_changed": len(ab["downstream"]),
        }
    print(json.dumps(summary, indent=2, default=str))
    for failure in failures:
        print(f"FAIL: {failure}", file=sys.stderr)
    print("PASS" if not failures else "FAIL")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
