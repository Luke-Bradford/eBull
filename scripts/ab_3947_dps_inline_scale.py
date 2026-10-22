"""Full-population A/B for the #3947 DPS inline-scale correction (writes nothing: one rolled-back transaction).

Spec: docs/specs/fundamentals/2026-10-21-3947-dps-inline-scale.md, Acceptance items 1, 2 and 4 and Rollback step 4,
as Amendment 1 ("Acceptance and rollback, as amended") overrides them.

**Quiescence.** Run it with ``sec_dps_inline_scale_scan`` paused and no ``daily_financial_facts`` run in progress.

1. **Scope.** ``--stage 1``: the candidate instruments (A4). ``--stage 2`` and ``--verify-stored``: the ledger's
   instruments (``sec_inline_scale_renormalise``, drained or not).
2. **Prefetch**, before any snapshot: every in-scope instrument's companyfacts payload, through the provider's shared
   SEC limiter, spilled to a temporary directory. No SEC request runs while the script holds a lock or the snapshot.
   A fetch failure lists the instrument as ``unrefilled``.
3. **The snapshot.** One ``REPEATABLE READ`` transaction, opened with ``SELECT 1`` (``conn.transaction()`` on an idle
   psycopg connection commits). Its first statements after that read the scope and take
   ``lock_instrument_for_normalize`` for every in-scope instrument in sorted order, before any write, which matches
   the drain's lock-before-upsert order. Then the coverage seed and gate, then per instrument: the refill
   (``upsert_facts_for_instrument`` in the snapshot's transaction), both derivations and the gated one over the
   refilled raw, and the real normalisations. Everything is rolled back. A serialization failure retries the whole
   snapshot, at most :data:`RETRIES` times; it is reported and is not an acceptance failure.

**Coverage gate** (A5), recomputed inside the snapshot: ``--stage 1`` needs the ``raw`` class, the ``history``
accessions of candidate instruments and the discovery of every candidate instrument at 0 pending and 0 retrying;
``--stage 2`` needs both classes and every population instrument's discovery there. ``--allow-incomplete`` reports
anyway, for a dry run. ``--verify-stored`` has no coverage gate.

**Invariants (fail):**

* a group whose outcome (``DpsGroupOutcome``) differs from the branch this script computes from A3's rule;
* a gated cell that is not its branch's result: decisions-on, decisions-off, or the same row's stored value;
* an ``on`` group with an unverified leaf, or a cell negative where its decisions-off cell is not;
* off against on through the real normaliser: a row or any column but ``dps_declared`` differing, or a changed
  ``dps_declared`` cell that is not its gated value.

**Reports:** coverage, decisions by reason, restored facts with their evidence (agrees / disagrees / none),
``unrefilled`` and ``undiscovered`` (a concept accession the refill reveals that the population lacked), refill drift
(stored rows against the decisions-off normalisation), ``unreachable`` (a stored ``dps_declared`` cell, not deleted by
the normaliser's structural rules, whose (``period_type``, ``period_end_date``) has no value in the decisions-off
derivation over the refilled inputs), groups by branch and reason, and the downstream fields off against on.

``--verify-stored off|on``: Rollback step 4 (``off``) or Stage 2 stored convergence (``on``). Each ledger instrument
is refilled and normalised in that mode inside the snapshot and compared with its stored rows and downstream fields;
any difference fails. ``unreachable`` cells are listed as rollback exceptions.

Usage: ``PYTHONPATH=. uv run python -m scripts.ab_3947_dps_inline_scale (--stage 1|2 | --verify-stored off|on)
[--out PATH] [--allow-incomplete]``
"""

from __future__ import annotations

import argparse
import json
import pickle
import sys
import tempfile
from collections import Counter, defaultdict
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Literal

import psycopg

from app.config import settings
from app.providers.fundamentals import XbrlFact
from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider
from app.services import dps_inline_scale_scan as scan
from app.services.fundamentals import (
    DpsGroupOutcome,
    DpsScaleDecision,
    FactRow,
    PeriodRow,
    _derive_periods_from_facts,
    _dps_fact_inline_scale_decisions,
    _fact_period_ended_by_filing,
    _load_dps_inline_scale_inputs,
    _per_share_value_is_representable,
    derive_gated_dps_periods,
    lock_instrument_for_normalize,
    normalize_financial_periods,
    start_ingestion_run,
    upsert_facts_for_instrument,
)
from app.services.inline_xbrl_dps import CONCEPT, Occurrence
from app.workers.scheduler import JOB_DAILY_FINANCIAL_FACTS, JOB_SEC_DPS_INLINE_SCALE_SCAN

RETRIES = 3
_KEY = ("period_end_date", "period_type", "fiscal_year", "fiscal_quarter")
# A3 "Verified decisions", restated from the spec rather than imported, so the expected branch is computed
# independently of the gate it checks.
_VERIFIED = frozenset({"scaled", "unscaled", "not_inline"})

Mode = Literal["stage1", "stage2", "verify_off", "verify_on"]
RowKey = tuple[int, str, Any]  # (fiscal_year, period_type, period_end_date): one derivation row


class _Spill(Mapping[int, Sequence[XbrlFact]]):
    """Prefetched payloads on disk: a full companyfacts payload per instrument does not stay in memory."""

    def __init__(self, directory: Path) -> None:
        self._dir = directory
        self._ids: dict[int, None] = {}  # insertion-ordered set

    def put(self, instrument_id: int, facts: Sequence[XbrlFact]) -> None:
        with (self._dir / f"{instrument_id}.pickle").open("wb") as fh:
            pickle.dump(list(facts), fh)
        self._ids[instrument_id] = None

    def __getitem__(self, instrument_id: int) -> Sequence[XbrlFact]:
        if instrument_id not in self._ids:
            raise KeyError(instrument_id)
        with (self._dir / f"{instrument_id}.pickle").open("rb") as fh:
            # Trusted input: this process wrote the file, into a TemporaryDirectory only its user can read (0700).
            return pickle.load(fh)  # noqa: S301

    def __iter__(self) -> Iterator[int]:
        return iter(self._ids)

    def __len__(self) -> int:
        return len(self._ids)


_CANDIDATES_SQL = (
    "SELECT DISTINCT p.instrument_id FROM sec_inline_scale_population p WHERE "
    + scan._CANDIDATE_SQL.format(iid="p.instrument_id")
    + " ORDER BY 1"
)
_LEDGER_SQL = "SELECT instrument_id FROM sec_inline_scale_renormalise ORDER BY 1"
# Any `running` row of either writer refuses the run, however old: `daily_financial_facts` runs can last many hours
# (`SELECT max(finished_at - started_at) FROM job_runs WHERE job_name = 'daily_financial_facts'`), so no age window
# is safe. A stale row left by an interrupted worker is listed by run id and fails closed.
_LIVE_WRITERS_SQL = "SELECT run_id, job_name, started_at FROM job_runs WHERE status = 'running' AND job_name = ANY(%s)"


def _scope(conn: psycopg.Connection[Any], mode: Mode) -> list[int]:
    return [r[0] for r in conn.execute(_CANDIDATES_SQL if mode == "stage1" else _LEDGER_SQL).fetchall()]


def prefetch(
    conn: psycopg.Connection[Any], provider: scan.FactsProvider, ids: Sequence[int], spill: _Spill
) -> dict[int, str]:
    """Every instrument's companyfacts, outside any transaction; returns the fetch failures by instrument."""
    failed: dict[int, str] = {}
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        raise RuntimeError("prefetch runs outside any transaction: no SEC request may hold a snapshot or a lock")
    for iid in ids:
        try:
            spill.put(iid, scan.fetch_companyfacts(conn, provider, iid))
        except Exception as exc:  # noqa: BLE001 — a fetch failure is reported: the instrument is listed as unrefilled
            failed[iid] = repr(exc)[:500]
    return failed


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


def _coverage(conn: psycopg.Connection[Any], mode: Mode, scope: Sequence[int]) -> tuple[dict[str, Any], list[str]]:
    """The population, classes and discovery recomputed in the snapshot (A5), and the stage's coverage gaps."""
    # Recompute the population inside the snapshot, so an unseeded table cannot read as full coverage. The seed
    # writes (population, discovery, queue rows), but only inside main()'s one REPEATABLE READ transaction, opened by
    # `SELECT 1` so seed_population's `conn.transaction()` is a savepoint, and undone by the final rollback.
    # tests/test_3947_dps_population_db.py pins that the seed is a savepoint here.
    scan.seed_population(conn)
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
    # Discovery of the scope's instruments: Stage 1 reads it for the candidates.
    scope_discovery = conn.execute(
        """
        SELECT count(*) FILTER (WHERE d.discovered_at IS NOT NULL),
               count(*) FILTER (WHERE d.discovered_at IS NULL AND COALESCE(d.attempts, 0) = 0),
               count(*) FILTER (WHERE d.discovered_at IS NULL AND d.attempts BETWEEN 1 AND %(max)s - 1),
               count(*) FILTER (WHERE d.discovered_at IS NULL AND d.attempts >= %(max)s)
        FROM unnest(%(ids)s::bigint[]) AS s(instrument_id)
        LEFT JOIN sec_inline_scale_discovery d USING (instrument_id)
        """,
        {"ids": list(scope), "max": scan.MAX_ATTEMPTS},
    ).fetchone() or (0, 0, 0, 0)
    state = scan.work_state(conn)
    scope_disc: dict[str, int] = dict(
        zip(("discovered", "pending", "retrying", "exhausted"), scope_discovery, strict=True)
    )
    required: dict[str, dict[str, int]]
    if mode == "stage1":
        required = {name: state.classes[name] for name in ("raw", "history_candidate")}
        required["discovery_of_candidates"] = scope_disc
    elif mode == "stage2":
        required = {name: state.classes[name] for name in ("raw", "history")}
        required["discovery_of_population"] = state.discovery
    else:
        required = {}
    gaps = [f"{name}: {counts}" for name, counts in required.items() if counts["pending"] or counts["retrying"]]
    return {
        "active_generation": dict(generation),
        "work_state": state.classes,
        "undated": state.undated,
        "discovery": state.discovery,
        "discovery_of_scope": scope_disc,
        "queue_length": state.queue,
        "exhausted_by_last_error": [[e, n] for e, n in exhausted],
    }, gaps


def _periods(conn: psycopg.Connection[Any], iid: int) -> dict[tuple[Any, ...], dict[str, Any]]:
    cur = conn.execute("SELECT * FROM financial_periods WHERE instrument_id = %s", (iid,))
    names = [d.name for d in cur.description or ()]
    out: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in cur.fetchall():
        record = dict(zip(names, row, strict=True))
        out[(record["instrument_id"], *(record[k] for k in _KEY))] = record
    return out


def _downstream(conn: psycopg.Connection[Any], iid: int) -> tuple[Any, ...]:
    row = conn.execute(
        """
        SELECT d.ttm_dps, d.latest_dps, t.dps_declared_ttm
        FROM (SELECT %s::bigint AS id) i
        LEFT JOIN instrument_dividend_summary d ON d.instrument_id = i.id
        LEFT JOIN financial_periods_ttm t ON t.instrument_id = i.id
        """,
        (iid,),
    ).fetchone()
    return tuple(row or ())


def _fact_rows(conn: psycopg.Connection[Any], iid: int) -> list[FactRow]:
    """The instrument's facts as ``normalize_financial_periods`` reads them."""
    return [
        FactRow(*r)
        for r in conn.execute(
            """
            SELECT concept, unit, period_start, period_end, val, frame, form_type, fiscal_year, fiscal_period,
                   accession_number, filed_date
            FROM financial_facts_raw
            WHERE instrument_id = %s AND fiscal_year IS NOT NULL AND fiscal_period IS NOT NULL
            ORDER BY period_end, concept
            """,
            (iid,),
        ).fetchall()
    ]


def _diff(
    a: Mapping[tuple[Any, ...], dict[str, Any]],
    b: Mapping[tuple[Any, ...], dict[str, Any]],
    labels: tuple[str, str] = ("off", "on"),
) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Structural differences (rows or any column but ``dps_declared``), and the changed DPS cells."""
    problems = [f"row only {labels[0]}: {k}" for k in a.keys() - b.keys()] + [
        f"row only {labels[1]}: {k}" for k in b.keys() - a.keys()
    ]
    changed: list[tuple[Any, ...]] = []
    for key in a.keys() & b.keys():
        other = [c for c in a[key] if c != "dps_declared" and a[key][c] != b[key][c]]
        if other:
            problems.append(f"{key}: columns changed {other}")
        if a[key]["dps_declared"] != b[key]["dps_declared"]:
            changed.append(key)
    return problems, changed


def _q(value: Decimal | None) -> Decimal | None:
    """A value as ``NUMERIC(12,4)`` stores it."""
    return None if value is None else value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def _row_key(row: PeriodRow) -> RowKey:
    return row.fiscal_year, row.period_type, row.period_end_date


def expected_branches(
    off: Sequence[PeriodRow],
    on: Sequence[PeriodRow],
    decisions: Mapping[FactRow, DpsScaleDecision],
    stored: Mapping[RowKey, Decimal | None],
) -> dict[int, tuple[str, frozenset[str]]]:
    """Each fiscal-year group's branch by A3's rule, and every reason found in it (an ``on`` group has none).

    Written from the spec, not from ``_gate_dps_groups``: incomplete when a stored (label, type) is not produced;
    otherwise ``on`` only when every leaf of every non-NULL cell is a verified decision of the concept and no cell
    is negative on while non-negative off. ``off`` carries the leaves; ``on`` the decisions-on values.
    """
    if [_row_key(r) for r in off] != [_row_key(r) for r in on]:
        return {fy: ("off", frozenset({"structure_mismatch"})) for fy in {r.fiscal_year for r in off}}
    on_by_key = {_row_key(r): r.dps_declared for r in on}
    out: dict[int, tuple[str, frozenset[str]]] = {}
    for fy in sorted({r.fiscal_year for r in off}):
        rows = [r for r in off if r.fiscal_year == fy]
        if {kind for f, kind, _ in stored if f == fy} - {r.period_type for r in rows}:
            out[fy] = ("kept_stored", frozenset({"incomplete_group"}))
            continue
        reasons: set[str] = set()
        for row in rows:
            if row.dps_declared is None:
                continue
            if not row.dps_leaves:
                reasons.add("unverified_leaf")
            for leaf in row.dps_leaves:
                if leaf.concept != CONCEPT:
                    reasons.add("other_concept")
                elif leaf not in decisions or decisions[leaf].reason not in _VERIFIED:
                    reasons.add("unverified_leaf")
            value_on = on_by_key[_row_key(row)]
            if value_on is not None and value_on < 0 and row.dps_declared >= 0:
                reasons.add("negative_derived")
        out[fy] = ("off" if reasons else "on", frozenset(reasons))
    return out


@dataclass
class _Instrument:
    """One instrument's findings; ``failures`` are invariant violations."""

    failures: list[str] = field(default_factory=list)
    groups: list[dict[str, Any]] = field(default_factory=list)
    changed: list[dict[str, Any]] = field(default_factory=list)
    structural: list[str] = field(default_factory=list)
    drift: dict[str, Any] = field(default_factory=dict)
    unreachable: list[dict[str, Any]] = field(default_factory=list)
    undiscovered: list[str] = field(default_factory=list)
    downstream: dict[str, list[str]] | None = None
    normalise_failures: int = 0


def _drift(
    before: Mapping[tuple[Any, ...], dict[str, Any]], after: Mapping[tuple[Any, ...], dict[str, Any]]
) -> dict[str, Any]:
    """Stored rows against a fresh normalisation: rows added and removed, label date changes, cells by column."""
    by_column: Counter[str] = Counter()
    for key in before.keys() & after.keys():
        for column, value in before[key].items():
            if after[key][column] != value:
                by_column[column] += 1

    def label(r: dict[str, Any]) -> tuple[Any, ...]:
        return r["fiscal_year"], r["fiscal_quarter"], r["period_type"], r["source"]

    before_ends = {label(r): r["period_end_date"] for r in before.values() if r["superseded_at"] is None}
    after_ends = {label(r): r["period_end_date"] for r in after.values() if r["superseded_at"] is None}
    return {
        "rows_added": [list(map(str, k)) for k in sorted(after.keys() - before.keys(), key=str)],
        "rows_removed": [list(map(str, k)) for k in sorted(before.keys() - after.keys(), key=str)],
        "date_changes": [
            [list(map(str, k)), str(before_ends[k]), str(after_ends[k])]
            for k in sorted(before_ends.keys() & after_ends.keys(), key=str)
            if before_ends[k] != after_ends[k]
        ],
        "cells_by_column": dict(by_column.most_common()),
    }


def _unreachable(
    stored: Mapping[tuple[Any, ...], dict[str, Any]],
    normalised: Mapping[tuple[Any, ...], dict[str, Any]],
    off: Sequence[PeriodRow],
) -> list[dict[str, Any]]:
    """Stored DPS cells the decisions-off derivation over the refilled inputs does not produce; a row the normaliser
    deleted (B2/B3) is a deletion, reported in drift, not unreachable."""
    produced = {(r.period_type, r.period_end_date) for r in off if r.dps_declared is not None}
    # Survival by the table's row identity (instrument, period end, type): a refill may relabel a surviving row.
    surviving = {(k[1], k[2]) for k in normalised}
    return [
        {"key": list(map(str, k)), "stored": str(row["dps_declared"]), "source_ref": row["source_ref"]}
        for k, row in sorted(stored.items(), key=lambda kv: str(kv[0]))
        if row["dps_declared"] is not None
        and (k[1], k[2]) in surviving
        and (row["period_type"], row["period_end_date"]) not in produced
    ]


def _check_instrument(
    conn: psycopg.Connection[Any],
    iid: int,
    payload: Sequence[XbrlFact] | None,
    run_id: int,
    mode: Mode,
) -> _Instrument:
    """Refill one instrument inside the snapshot, then check it (A/B) or compare it with its stored rows (verify)."""
    found = _Instrument()
    stored = _periods(conn, iid)
    stored_down = _downstream(conn, iid)
    if payload is not None:
        known = {r[0] for r in conn.execute(
            "SELECT accession_number FROM sec_inline_scale_population WHERE instrument_id = %s", (iid,)
        ).fetchall()}  # fmt: skip
        found.undiscovered = sorted({f.accession_number for f in payload if f.concept == CONCEPT} - known)
        upsert_facts_for_instrument(
            conn, instrument_id=iid, facts=payload, ingestion_run_id=run_id, complete_payload=True
        )
    facts = _fact_rows(conn, iid)
    decisions: dict[FactRow, DpsScaleDecision] = {}
    accessions = sorted({f.accession_number for f in facts if f.concept == CONCEPT})
    if accessions:
        decisions = _dps_fact_inline_scale_decisions(facts, *_load_dps_inline_scale_inputs(conn, iid, accessions))
    off = _derive_periods_from_facts(facts)
    on = _derive_periods_from_facts(
        facts, dps_values={f: d.value for f, d in decisions.items() if d.action == "restore"}
    )

    if mode in ("verify_off", "verify_on"):
        failed = normalize_financial_periods(conn, [iid], dps_inline_scale=mode == "verify_on").instruments_failed
        after = _periods(conn, iid)
        found.normalise_failures = failed
        found.unreachable = _unreachable(stored, after, off)
        problems, changed = _diff(stored, after, ("stored", mode))
        found.structural = problems
        found.changed = [{"key": list(map(str, k))} for k in changed]
        after_down = _downstream(conn, iid)
        if stored_down != after_down:
            found.downstream = {"stored": list(map(str, stored_down)), mode: list(map(str, after_down))}
        if failed or problems or changed or found.downstream:
            # Any difference fails, as the spec requires: Rollback step 4 ("an instrument that differs because ingestion
            # moved its inputs is re-normalised and checked again") and Stage 2 convergence ("any difference is listed
            # and fails, except unreachable cells"). Both run after a refill_and_normalise rebuild, so drift is 0 then.
            found.failures.append("stored rows differ from a fresh refill and normalisation")
        return found

    # Pure derivations over the refilled raw, then the gate as the normaliser runs it (it reads the stored rows,
    # which no normalisation has touched yet in this snapshot).
    gated, outcomes = derive_gated_dps_periods(conn, iid, facts)
    stored_dps = {
        (r["fiscal_year"], r["period_type"], r["period_end_date"]): r["dps_declared"]
        for r in stored.values()
        if r["source"] == "sec_edgar" and r["superseded_at"] is None
    }
    expected = expected_branches(off, on, decisions, stored_dps)
    branch_of: dict[int, str] = {}
    for fy in sorted(expected.keys() | outcomes.keys()):
        outcome: DpsGroupOutcome | None = outcomes.get(fy)
        want, reasons = expected.get(fy, ("missing", frozenset()))
        got = outcome.branch if outcome else "missing"
        branch_of[fy] = got
        found.groups.append({"fiscal_year": fy, "branch": got, "reason": outcome.reason if outcome else None,
                             "expected": want, "reasons": sorted(reasons)})  # fmt: skip
        if got != want:
            found.failures.append(f"group {fy}: took {got}, A3 gives {want} {sorted(reasons)}")
        if got == "on" and reasons:
            found.failures.append(f"group {fy}: on with {sorted(reasons)}")
    if [_row_key(r) for r in gated] != [_row_key(r) for r in off]:
        found.failures.append("gated rows differ from the decisions-off rows")
    else:
        for g, o, n in zip(gated, off, on, strict=True):
            branch = branch_of.get(g.fiscal_year)
            result = {
                "on": n.dps_declared,
                "off": o.dps_declared,
                "kept_stored": stored_dps.get(_row_key(g), o.dps_declared),
            }.get(branch or "", None)
            if g.dps_declared != result:
                found.failures.append(f"{_row_key(g)}: {g.dps_declared} is not the {branch} result {result}")

    # The real normaliser, both modes, from the same stored rows: off inside a savepoint that is then undone.
    with conn.transaction() as tx:
        failed_off = normalize_financial_periods(conn, [iid], dps_inline_scale=False).instruments_failed
        off_db, off_down = _periods(conn, iid), _downstream(conn, iid)
        raise psycopg.Rollback(tx)
    failed_on = normalize_financial_periods(conn, [iid], dps_inline_scale=True).instruments_failed
    on_db, on_down = _periods(conn, iid), _downstream(conn, iid)
    found.normalise_failures = failed_off + failed_on
    if found.normalise_failures:
        found.failures.append(f"normalise failures off={failed_off} on={failed_on}")
    found.drift = _drift(stored, off_db)
    found.unreachable = _unreachable(stored, off_db, off)
    found.structural, changed = _diff(off_db, on_db)
    if found.structural:
        found.failures.append(f"{len(found.structural)} structural differences off vs on")
    gated_by_key = {_row_key(r): r.dps_declared for r in gated}
    for key in sorted(changed, key=str):
        row_key = (on_db[key]["fiscal_year"], on_db[key]["period_type"], on_db[key]["period_end_date"])
        want_value = _q(gated_by_key[row_key]) if row_key in gated_by_key else None
        explained = row_key in gated_by_key and on_db[key]["dps_declared"] == want_value
        found.changed.append({
            "key": list(map(str, key)),
            "off": str(off_db[key]["dps_declared"]),
            "on": str(on_db[key]["dps_declared"]),
            "branch": branch_of.get(row_key[0]),
            "explained": explained,
        })  # fmt: skip
        if not explained:
            found.failures.append(f"{key}: dps_declared {on_db[key]['dps_declared']} is not its gated value")
    if off_down != on_down:
        found.downstream = {"off": list(map(str, off_down)), "on": list(map(str, on_down))}
    return found


def _restored(
    ids: Sequence[int],
    decisions: Mapping[int, Mapping[FactRow, DpsScaleDecision]],
    occurrences: Mapping[str, Sequence[Occurrence]],
) -> list[dict[str, Any]]:
    """Every restored fact of the scope with the other accessions' ``unscaled`` facts of the same period."""
    out = []
    for iid in ids:
        for fact, d in decisions.get(iid, {}).items():
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
            verdict = "none" if not others else "agrees" if all(v == d.value for v in others) else "disagrees"
            displays = sorted(
                {
                    o.display_text
                    for o in occurrences.get(fact.accession_number, ())
                    if (o.document, o.element_ordinal) in d.occurrences
                }
            )
            out.append(
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
    return out


def run_snapshot(
    conn: psycopg.Connection[Any],
    *,
    mode: Mode,
    payloads: Mapping[int, Sequence[XbrlFact]],
    unrefilled: Mapping[int, str],
    allow_incomplete: bool = False,
) -> tuple[dict[str, Any], list[str]]:
    """Everything inside the snapshot; the caller opened it (``SELECT 1``) and rolls it back."""
    report: dict[str, Any] = {"mode": mode}
    failures: list[str] = []
    # Quiescence (spec): no live scan or companyfacts refresh. The locks below stop any later normaliser write;
    # one that committed after `SELECT 1` but before its lock is invisible here, and writing over it raises a
    # serialization failure, which main() retries with a fresh snapshot.
    live = conn.execute(_LIVE_WRITERS_SQL, ([JOB_SEC_DPS_INLINE_SCALE_SCAN, JOB_DAILY_FINANCIAL_FACTS],)).fetchall()
    if live:
        failures.append(
            f"not quiescent: running {[list(map(str, r)) for r in live]}; pause the scan, wait for the run to finish,"
            " and rerun. A job_runs row left running by an interrupted worker is normally marked status='failure'"
            " during jobs-process startup (app/jobs/__main__.py calls reap_orphaned_job_runs(reap_all=True))"
        )
    ids = sorted(set(_scope(conn, mode)) | set(payloads) | set(unrefilled))
    # Before any write: every in-scope instrument's normaliser lock, in sorted order (the drain's order).
    for iid in ids:
        lock_instrument_for_normalize(conn, iid)
    coverage, gaps = _coverage(conn, mode, ids)
    report["coverage"] = coverage
    for gap in gaps:
        if allow_incomplete:
            print(f"WARNING: scan coverage incomplete: {gap}", file=sys.stderr)
        else:
            failures.append(f"scan coverage incomplete: {gap}")
    grown = sorted(set(_scope(conn, mode)) - set(ids))
    if grown:
        failures.append(f"the in-snapshot seed added in-scope instruments {grown}: rerun so they are prefetched")
    not_prefetched = {iid: "not in scope at prefetch" for iid in ids if iid not in payloads and iid not in unrefilled}
    report["unrefilled"] = {str(k): v for k, v in sorted({**unrefilled, **not_prefetched}.items())}

    run_id = start_ingestion_run(conn, source="sec_edgar", endpoint="/api/xbrl/companyfacts", instrument_count=len(ids))
    found = {iid: _check_instrument(conn, iid, payloads.get(iid), run_id, mode) for iid in ids}
    for iid, f in found.items():
        failures += [f"instrument {iid}: {msg}" for msg in f.failures]

    # Decisions over every stored fact of the concept, read after the refills.
    facts = _facts(conn)
    scans, ciks, occurrences = _inputs(conn)
    by_instrument: dict[int, list[FactRow]] = defaultdict(list)
    eligible: dict[FactRow, bool] = {}
    for iid, ok, fact in facts:
        by_instrument[iid].append(fact)
        eligible[fact] = eligible.get(fact, False) or ok
    decisions = {
        iid: _dps_fact_inline_scale_decisions(fs, scans, ciks.get(iid, frozenset()), occurrences)
        for iid, fs in by_instrument.items()
    }
    all_reasons: Counter[str] = Counter()
    eligible_reasons: Counter[str] = Counter()
    doubt: dict[str, set[str]] = defaultdict(set)
    for ds in decisions.values():
        for fact, d in ds.items():
            all_reasons[d.reason] += 1
            if eligible[fact]:
                survives = _per_share_value_is_representable(fact) and _fact_period_ended_by_filing(fact)
                eligible_reasons[d.reason if survives else "dropped_before_decision"] += 1
            if d.reason in ("unresolved", "entity_mismatch", "no_match"):
                doubt[fact.accession_number].add(d.reason)
    report["decisions"] = {
        "facts": len(facts),
        "by_reason_all": dict(all_reasons.most_common()),
        "by_reason_derivation_eligible": dict(eligible_reasons.most_common()),
        "doubt_accessions": {a: sorted(r) for a, r in sorted(doubt.items())},
    }
    restored = _restored(ids, decisions, occurrences)
    groups = Counter((g["branch"], g["reason"]) for f in found.values() for g in f.groups)
    report["instruments"] = {
        str(iid): {
            "failures": f.failures,
            "groups": f.groups,
            "dps_cells_changed": f.changed,
            "structural": f.structural,
            "refill_drift": f.drift,
            "unreachable": f.unreachable,
            "undiscovered": f.undiscovered,
            "downstream": f.downstream,
        }
        for iid, f in found.items()
    }
    report["summary"] = {
        "instruments": len(ids),
        "groups_by_branch_reason": {f"{b}/{r}": n for (b, r), n in sorted(groups.items(), key=str)},
        "dps_cells_changed": sum(len(f.changed) for f in found.values()),
        "unreachable": sum(len(f.unreachable) for f in found.values()),
        "undiscovered": sum(len(f.undiscovered) for f in found.values()),
        "downstream_changed": sum(f.downstream is not None for f in found.values()),
        "restored": len(restored),
        "restored_by_verdict": dict(Counter(r["verdict"] for r in restored)),
        "refill_drift_cells_by_column": dict(
            sum((Counter(f.drift.get("cells_by_column", {})) for f in found.values()), Counter()).most_common()
        ),
    }
    report["restored"] = restored
    return report, failures


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    which = parser.add_mutually_exclusive_group(required=True)
    which.add_argument("--stage", choices=("1", "2"), help="enablement stage: its scope and coverage gate (A5)")
    which.add_argument("--verify-stored", choices=("off", "on"), help="Rollback step 4 / Stage 2 convergence")
    parser.add_argument("--out", help="write the full JSON report here")
    parser.add_argument("--allow-incomplete", action="store_true", help="report even with pending/retrying work")
    args = parser.parse_args(argv)
    modes: dict[str, Mode] = {"1": "stage1", "2": "stage2", "off": "verify_off", "on": "verify_on"}
    mode = modes[args.stage or args.verify_stored]

    with psycopg.connect(settings.database_url) as conn, tempfile.TemporaryDirectory() as tmp:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with conn.transaction():
            ids = _scope(conn, mode)
        spill = _Spill(Path(tmp))
        with SecFundamentalsProvider(user_agent=settings.sec_user_agent) as provider:
            unrefilled = prefetch(conn, provider, ids, spill)
        retries = 0
        while True:
            conn.execute("SELECT 1")  # open the snapshot: conn.transaction() on an idle connection would commit
            try:
                report, failures = run_snapshot(
                    conn, mode=mode, payloads=spill, unrefilled=unrefilled, allow_incomplete=args.allow_incomplete
                )
                break
            except psycopg.errors.SerializationFailure:
                retries += 1
                if retries > RETRIES:
                    raise
                print(f"serialization failure: retry {retries} of {RETRIES}", file=sys.stderr)
            finally:
                conn.rollback()
        report["serialization_retries"] = retries

    text = json.dumps(report, indent=2, default=str)
    if args.out:
        Path(args.out).write_text(text)
    print(json.dumps({k: report[k] for k in ("mode", "coverage", "unrefilled", "summary", "serialization_retries")},
                     indent=2, default=str))  # fmt: skip
    for failure in failures:
        print(f"FAIL: {failure}", file=sys.stderr)
    print("PASS" if not failures else "FAIL")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
