"""DERA FSDS bulk loader for dimensional XBRL facts (#1590).

Quick-and-dirty bootstrap tier: stream a quarter's ``num.txt``, route the
``segments`` cells that carry a single business-segment / product-or-service /
geographic dimension (us-gaap revenue / operating income / assets), and write
``instrument_dimensional_facts`` for every share-class sibling of the issuer CIK —
in minutes, vs the ~4 h #554 per-filing drain. The precise #554 per-filing path
CONVERGES on top: it replaces a bulk accession's rows the moment its manifest
worker re-parses that filing (delete-then-insert by accession).

Correctness rules (see docs/specs/etl/2026-06-19-fsds-bulk-dimensional-facts.md):
- **10-K only** — FSDS includes 10-Qs; a later-filed 10-Q annual-duration row could
  outrank the real 10-K in the accession-winner reader. Match #554's 10-K grain.
- **Exact-set classifier** — route ONLY a cell whose axis-token set is exactly one of
  the known routes; reject any cross-dimensional cell (segment×product, …) that would
  double-count. Parse to a LIST so a repeated axis token rejects, never collapses.
- **Convergence guard + advisory lock** — write an accession's rows only when no
  ``instrument_dimensional_facts`` row exists for it yet (FSDS ``US`` ≠ per-filing
  ``country:US`` → they do NOT collide on the unique index, so a naive append would
  double-count). A transaction-scoped advisory lock — the SAME key the per-filing
  ``replace_accession_rows`` takes — makes the check-then-insert race-tight.

This module does NOT touch the per-filing extractor's routing (#554's
``_classify_context`` matches FULL localnames; FSDS strips namespaces + affixes), but
REUSES #554's concept→metric map, per-route metric filter, and value-overage subtotal
marking (``mark_value_overage_subtotals``) so bulk and per-filing agree.
"""

from __future__ import annotations

import calendar
import logging
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import psycopg

from app.services.dimensional_facts import (
    _AXIS_METRICS,
    _CONCEPT_TO_METRIC,
    _INSTANT_METRICS,
    PER_FILING_AXES,
    DimensionalAxis,
    DimensionalFact,
    DimensionalMetric,
    mark_value_overage_subtotals,
)
from app.services.fsds_class_shares import iter_fsds_num, read_fsds_sub
from app.services.sec_identity import siblings_for_issuer_cik

logger = logging.getLogger(__name__)

PARSER_VERSION = "fsds_dimensional_v1"
_TENK_FORMS = frozenset({"10-K", "10-K/A"})

# FSDS axis-token → route. The token is the namespace-stripped localname minus the
# ``Statement`` prefix and ``Axis`` suffix (empirically verified, 2025q1 — see spec
# §"Source rule"). #1623's parse_class_member relies on the same encoding.
_ROUTE_BY_AXES: dict[frozenset[str], DimensionalAxis] = {
    frozenset({"BusinessSegments"}): "business_segment",
    frozenset({"ProductOrService"}): "product_service",
    frozenset({"Geographical"}): "geographic",
}


def _classify_fsds_segments(segments_cell: str) -> tuple[DimensionalAxis, str] | None:
    """Route an FSDS ``segments`` cell to ``(axis, member_localname)`` or ``None``.

    EXACT-SET rule (mirrors #554's ``_classify_context`` semantics on the FSDS token
    form): the cell's axis-token set must be exactly one of the known routes, OR the
    ``{BusinessSegments, ConsolidationItems}`` pair where ``ConsolidationItems`` is the
    ``OperatingSegments`` member (the clean operating-segment value; eliminations /
    corporate reconciling members are NOT a segment). Any extra axis → ``None`` (a
    cross-dimensional cell like ``BusinessSegments=…;ProductOrService=…`` would
    double-count). Parsed to a LIST so a repeated axis token rejects rather than
    silently collapsing to one member.
    """
    cell = segments_cell.strip()
    if not cell:
        return None
    pairs: list[tuple[str, str]] = []
    for part in cell.split(";"):
        part = part.strip()
        if not part:
            continue
        axis, eq, member = part.partition("=")
        if not eq or not axis or not member:
            return None  # malformed / truncated fragment → reject (fail safe)
        pairs.append((axis, member))
    if not pairs:
        return None
    axes = [a for a, _ in pairs]
    axis_set = frozenset(axes)
    if len(axes) != len(axis_set):
        return None  # a repeated axis token — ambiguous, reject
    members = dict(pairs)

    single = _ROUTE_BY_AXES.get(axis_set)
    if single is not None:
        return (single, members[next(iter(axis_set))])
    if axis_set == frozenset({"BusinessSegments", "ConsolidationItems"}):
        if members["ConsolidationItems"] == "OperatingSegments":
            return ("business_segment", members["BusinessSegments"])
        return None  # eliminations / corporate / reconciling — not a clean segment
    return None


def _prettify_member(localname: str) -> str:
    """Quick-tier member label: split camelCase to spaced words (no label linkbase in
    FSDS). ``SpecialtyDiagnostics`` → ``Specialty Diagnostics``; ``US`` → ``US``;
    ``FoodAndBeverage`` → ``Food And Beverage``. The per-filing rewash replaces this
    with the real linkbase label on convergence."""
    out: list[str] = []
    for i, ch in enumerate(localname):
        if i > 0 and ch.isupper() and (localname[i - 1].islower() or localname[i - 1].isdigit()):
            out.append(" ")
        out.append(ch)
    return "".join(out)


def _subtract_months(d: date, months: int) -> date:
    """``d`` minus ``months`` calendar months, clamping the day to the target month's
    length (only used to reconstruct a duration ``period_start`` for the reader's
    annual-window filter; ±1 day vs the true XBRL startDate is moot under
    accession-winner-takes-all)."""
    total = d.year * 12 + (d.month - 1) - months
    year, month0 = divmod(total, 12)
    month = month0 + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _reconstruct_period(ddate: str, qtrs: str, metric: DimensionalMetric) -> tuple[date | None, date] | None:
    """``(period_start, period_end)`` from num.txt ``ddate`` (yyyymmdd period-end) +
    ``qtrs`` (duration in quarters; ``0`` = instant). Enforces #554's instant/duration
    contract: ``assets`` must be an instant; ``revenue``/``operating_income`` must be a
    duration. ``None`` on bad data or a metric/period mismatch."""
    try:
        end = datetime.strptime(ddate, "%Y%m%d").date()
        q = int(qtrs)
    except ValueError, TypeError:
        return None
    if q < 0:
        return None
    is_instant = q == 0
    if metric in _INSTANT_METRICS:
        return (None, end) if is_instant else None
    if is_instant:
        return None  # a duration metric reported as an instant — skip
    return (_subtract_months(end, q * 3), end)


def _parse_value(raw: str) -> Decimal | None:
    try:
        return Decimal(raw)
    except InvalidOperation, ValueError, TypeError:
        return None


# Identity grain a member fact is deduplicated on — exactly the columns the
# uq_dimensional_facts_identity index keys on (minus instrument_id/accession, which
# are constant within one buffer). Two num.txt rows that collapse to this key (e.g.
# one segment+period reported under two revenue aliases) MUST arbitrate, not both
# insert, or the unique index rejects the batch.
_MemberKey = tuple[DimensionalAxis, str, DimensionalMetric, date | None, date]


@dataclass
class _AccBuf:
    """Per-accession buffer accumulated while streaming the unsorted num.txt."""

    # _MemberKey -> (alias_priority, fact). Arbitrated exactly like #554's per-filing
    # ``candidates`` (minus the decimals-precision tie-break FSDS lacks): the lowest
    # alias-priority concept wins; a same-priority value conflict drops the key (added
    # to ``conflicted``). Prevents the unique-index collision when one member+period is
    # tagged under two revenue aliases (Revenues + RevenueFromContractWithCustomer…).
    members: dict[_MemberKey, tuple[int, DimensionalFact]] = field(default_factory=dict)
    conflicted: set[_MemberKey] = field(default_factory=set)
    # (metric, period_start, period_end) -> (alias_priority, value | None). ``None`` =
    # a same-priority value conflict for that period → drop (value-overage skips, never
    # marks a false subtotal). Only revenue totals are buffered (the only ones the
    # value-overage pass consumes).
    rev_totals: dict[tuple[DimensionalMetric, date | None, date], tuple[int, Decimal | None]] = field(
        default_factory=dict
    )


@dataclass
class FsdsDimensionalResult:
    rows_written: int = 0
    accessions_written: int = 0
    accessions_skipped_existing: int = 0  # every sibling already had rows (convergence)
    accessions_no_instrument: int = 0  # CIK not in our universe
    instruments_written: int = 0


def _write_bulk_accession(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession: str,
    form_type: str,
    filed_at: datetime,
    facts: list[DimensionalFact],
) -> int:
    """Insert one accession's facts for one instrument — ONLY if no row exists for
    ``(instrument_id, accession)`` yet (never augment a per-filing accession). Runs in
    the caller's transaction. Takes the SAME advisory lock key as the per-filing
    ``replace_accession_rows`` so the check-then-insert is race-tight against it."""
    # Bigint-safe combined key (instrument_id is BIGINT — do NOT cast to int4; both
    # params ::text so `||` never depends on psycopg3 OID inference). MUST be
    # byte-identical to replace_accession_rows' lock so the two writers serialize —
    # tests/test_fsds_dimensional_facts.py::test_lock_templates_byte_identical pins it.
    conn.execute(
        "SELECT pg_advisory_xact_lock(hashtext(%s::text || ':' || %s::text)::bigint)",
        (instrument_id, accession),
    )
    # Axis-scoped existence check (#844): the FSNDS notes loader writes
    # ``award_type`` rows for the same 10-K accessions — an unscoped check
    # would see them and permanently skip this accession's segment facts.
    existing = conn.execute(
        "SELECT 1 FROM instrument_dimensional_facts"
        " WHERE instrument_id = %s AND source_accession = %s AND axis = ANY(%s) LIMIT 1",
        (instrument_id, accession, list(PER_FILING_AXES)),
    ).fetchone()
    if existing is not None or not facts:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO instrument_dimensional_facts (
                instrument_id, axis, member_qname, member_label, metric,
                unit, is_subtotal, period_start, period_end, val, decimals,
                source_accession, form_type, filed_at, parser_version
            ) VALUES (
                %(instrument_id)s, %(axis)s, %(member_qname)s, %(member_label)s,
                %(metric)s, %(unit)s, %(is_subtotal)s, %(period_start)s,
                %(period_end)s, %(val)s, %(decimals)s, %(source_accession)s,
                %(form_type)s, %(filed_at)s, %(parser_version)s
            )
            """,
            [
                {
                    "instrument_id": instrument_id,
                    "axis": f.axis,
                    "member_qname": f.member_qname,
                    "member_label": f.member_label,
                    "metric": f.metric,
                    "unit": f.unit,
                    "is_subtotal": f.is_subtotal,
                    "period_start": f.period_start,
                    "period_end": f.period_end,
                    "val": f.val,
                    "decimals": f.decimals,
                    "source_accession": accession,
                    "form_type": form_type,
                    "filed_at": filed_at,
                    "parser_version": PARSER_VERSION,
                }
                for f in facts
            ],
        )
        # psycopg3 executemany rowcount is cumulative across the batch.
        return cur.rowcount


def _accumulate(per_adsh: dict[str, _AccBuf], row: dict[str, str]) -> None:
    """Route one num.txt row into the per-accession buffer (members + revenue totals).
    The caller has already gated the row to a 10-K accession."""
    if (row.get("coreg") or "").strip():
        return  # co-registrant facts belong to a different entity, not the filing issuer
    tag = row.get("tag", "")
    concept = _CONCEPT_TO_METRIC.get(tag)
    if concept is None:
        return
    metric, priority = concept
    if not row.get("version", "").startswith("us-gaap"):
        return  # reject filer-extension concepts (matches #554)
    period = _reconstruct_period(row.get("ddate", ""), row.get("qtrs", ""), metric)
    if period is None:
        return
    period_start, period_end = period
    value = _parse_value(row.get("value", ""))
    if value is None:
        return
    adsh = row["adsh"]
    buf = per_adsh.setdefault(adsh, _AccBuf())
    segments = (row.get("segments", "") or "").strip()

    if not segments:
        # Dimensionless consolidated row — only revenue feeds the value-overage pass.
        if metric != "revenue":
            return
        total_key = ("revenue", period_start, period_end)
        existing = buf.rev_totals.get(total_key)
        if existing is None:
            buf.rev_totals[total_key] = (priority, value)
        else:
            ex_priority, ex_value = existing
            if priority < ex_priority:
                buf.rev_totals[total_key] = (priority, value)
            elif priority == ex_priority and ex_value is not None and value != ex_value:
                buf.rev_totals[total_key] = (priority, None)  # conflicting total → drop
        return

    routed = _classify_fsds_segments(segments)
    if routed is None:
        return
    axis, member = routed
    if metric not in _AXIS_METRICS[axis]:
        return
    member_key: _MemberKey = (axis, member, metric, period_start, period_end)
    if member_key in buf.conflicted:
        return
    fact = DimensionalFact(
        axis=axis,
        member_qname=member,
        member_label=_prettify_member(member),
        metric=metric,
        unit=row.get("uom", "") or "",
        is_subtotal=False,
        period_start=period_start,
        period_end=period_end,
        val=value,
        decimals=None,  # num.txt carries no decimals column
    )
    incumbent = buf.members.get(member_key)
    if incumbent is None:
        buf.members[member_key] = (priority, fact)
        return
    inc_priority, inc_fact = incumbent
    if priority < inc_priority:
        buf.members[member_key] = (priority, fact)  # better revenue alias wins
    elif priority == inc_priority and value != inc_fact.val:
        # Same alias, two different values for one identity (e.g. duplicate seg row) and
        # no decimals/precision to break the tie → drop the key (conservative; matches
        # #554's equal-precision conflict path). Equal values collapse (keep incumbent).
        del buf.members[member_key]
        buf.conflicted.add(member_key)


def ingest_fsds_dimensional_archive(
    conn: psycopg.Connection[Any],
    *,
    archive_path: Path,
    fsds_qtr: str,
) -> FsdsDimensionalResult:
    """Stream one ``fsds_{q}.zip`` and bulk-load dimensional facts. Commits per
    accession (so the per-accession advisory locks release promptly); the caller need
    not wrap this in a transaction."""
    result = FsdsDimensionalResult()
    with zipfile.ZipFile(archive_path) as zf:
        subs = read_fsds_sub(zf)
        tenk = {adsh: sub for adsh, sub in subs.items() if sub.form in _TENK_FORMS and sub.filed is not None}
        per_adsh: dict[str, _AccBuf] = {}
        for row in iter_fsds_num(zf):
            if row.get("adsh", "") in tenk:
                _accumulate(per_adsh, row)

    for adsh, buf in per_adsh.items():
        if not buf.members:
            continue
        sub = tenk[adsh]
        siblings = siblings_for_issuer_cik(conn, sub.cik)
        if not siblings:
            result.accessions_no_instrument += 1
            conn.commit()  # close the read txn opened by the lookup above
            continue
        totals = {key: val for key, (_prio, val) in buf.rev_totals.items() if val is not None}
        member_facts = [fact for _prio, fact in buf.members.values()]
        facts, _rej = mark_value_overage_subtotals(member_facts, totals, accession=adsh)
        # sub.filed is a date (10-K acceptance day); the reader orders by filed_at DESC —
        # a midnight-UTC stamp sorts before a same-day per-filing precise time, so a real
        # per-filing row wins a same-date tie (correct: prefer the precise path).
        assert sub.filed is not None  # tenk filter guarantees this
        filed_at = datetime.combine(sub.filed, time.min, tzinfo=UTC)
        wrote_any = False
        with conn.transaction():
            for instrument_id in siblings:
                n = _write_bulk_accession(
                    conn,
                    instrument_id=instrument_id,
                    accession=adsh,
                    form_type=sub.form,
                    filed_at=filed_at,
                    facts=facts,
                )
                if n > 0:
                    result.rows_written += n
                    result.instruments_written += 1
                    wrote_any = True
        # COMMIT the top-level transaction per accession. `with conn.transaction()` only
        # opens a SAVEPOINT here (an implicit txn is already open from the read above), so
        # it does NOT durably commit on its own. The explicit commit both persists the
        # rows and RELEASES the per-accession advisory xact lock (held until top-level
        # commit) so a concurrent per-filing writer is not blocked for the whole archive.
        conn.commit()
        if wrote_any:
            result.accessions_written += 1
        else:
            result.accessions_skipped_existing += 1

    logger.info(
        "sec_fsds_dimensional_ingest: qtr=%s rows=%d accessions_written=%d skipped_existing=%d no_instrument=%d",
        fsds_qtr,
        result.rows_written,
        result.accessions_written,
        result.accessions_skipped_existing,
        result.accessions_no_instrument,
    )
    return result
