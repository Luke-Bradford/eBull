"""#3841 slice 2: the ``market_cap_pit_rebuild`` build -- readers, gates and publication.

Spec: ``docs/specs/fundamentals/2026-10-16-3841-pit-market-cap.md`` ("Build and publication"). The rules are in
``app.services.pit_market_cap`` and are not restated here; this module reads their inputs from one database snapshot
and ``submissions.zip``, runs them per instrument, and replaces ``instrument_share_count_pit``.

Order (spec): take the build lock -> open one REPEATABLE READ transaction -> build and run Gates 1 and 2 on the staged
steps -> replace every step and append the build row, in that transaction. A build that raises rolls back whole and
the previous build stays active. The lock is a session advisory lock taken BEFORE the transaction, because a
REPEATABLE READ snapshot is fixed by the first statement: a lock taken inside it would be acquired on a snapshot that
predates the build it waited for.

Readers, each fixed by its source rule:

* scope -- ``load_validated_universe`` (the US-equity source) with a primary SEC CIK;
* share facts -- ``financial_facts_raw`` cover (``dei:EntityCommonStockSharesOutstanding``) and balance-sheet
  (``us-gaap:CommonStockSharesOutstanding``) counts, unit ``shares``; ``period_end`` is the context. Form and
  acceptance come from the CIK's ``submissions.zip`` index (``pit_fundamentals.parse_submissions``), never
  ``form_type`` or ``filed_date``. A CIK whose index fails integrity, or that holds a share-count fact whose accession
  the index cannot time, is refused whole (``CikRefusal``) rather than published on the counts it can time: a newer
  count it cannot time may already have been public;
* 13F unit values -- per (instrument, quarter, filer): the filer's latest-filed report for the quarter that holds the
  instrument (an amendment replaces it), the median value / shares over rows with finite shares > 0, value > 0 and no
  put/call. The latest report is chosen per instrument, not per filer and quarter, because a 13F-HR/A is either a
  RESTATEMENT (replaces) or NEW HOLDINGS (adds rows and leaves the original standing) and ``institutional_holdings``
  does not store which. Per filer and quarter would drop every original that a NEW HOLDINGS amendment followed;
  per instrument keeps a restated-away holding's original row as that filer's value (an accepted limit: one filer
  among the >= 20 in the median);
* dollar volume -- the mean close x volume over the 21 stored bars ending at the session, only when all 21 carry one;
* multi-class -- every CIK with a row in ``instrument_class_shares_outstanding`` (by instrument or ``source_cik``);
* Form 4 trades (Gate 1) -- non-derivative P or S rows with a price > 0 on a title naming common stock, from Form 4
  and 4/A, ``txn_date_invalid`` excluded, qualified only after an amendment has replaced its original
  (``resolve_amendments``);
* basis stamp -- the instrument's ``price_daily_revision`` and ``price_daily_backdated_insert`` counts and its last
  stored bar, so the view refuses (``basis_stale``) any bar written or revised after the snapshot;
* public float (Gate 2) -- ``dei:EntityPublicFloat`` in USD, the latest-accepted accession per context date.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import zipfile
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

import psycopg

from app.services.factor_panel import MAX_SHARES_AGE_MONTHS, add_months
from app.services.market_calendar import us_market_status
from app.services.pit_fundamentals import SubmissionsIndex, form_family, submissions_reader
from app.services.pit_market_cap import (
    FIRST_QUARTER,
    Anchor,
    Concept,
    FloatFact,
    ShareFact,
    Step,
    Trade,
    apply_gate1,
    apply_gate2,
    build_steps,
    classify_anchor,
    gate1,
    gate2,
    last_session_on_or_before,
    quarter_ends,
    rule_version,
)
from app.services.strategies.validated_universe import load_validated_universe

logger = logging.getLogger(__name__)

#: Spec "Not in scope": sessions before 2024-07.
FIRST_SESSION: Final = date(2024, 7, 1)
#: Spec "Readers": mean dollar volume over the 21 sessions ending at S (#3609 check 3's operand).
DOLLAR_VOLUME_SESSIONS: Final = 21
#: ``pg_advisory_lock`` key serialising builds; any distinct bigint.
BUILD_LOCK_KEY: Final = 3841_0002
#: Instruments read per batch: bounds memory (one batch's bars, 13F rows and trades) inside the one snapshot.
BATCH_SIZE: Final = 400

COVER: Final = ("dei", "EntityCommonStockSharesOutstanding")
BALANCE_SHEET: Final = ("us-gaap", "CommonStockSharesOutstanding")
PUBLIC_FLOAT: Final = ("dei", "EntityPublicFloat")


class CikRefusal(StrEnum):
    """Why a CIK has no steps at all (counted in the build summary)."""

    UNTIMED_ACCESSION = "untimed_accession"  # a share-count fact whose accession the submissions index cannot time


@dataclass(frozen=True)
class FactRow:
    """One ``financial_facts_raw`` row as read."""

    taxonomy: str
    concept: str
    accession: str
    context: date
    value: Decimal


@dataclass(frozen=True)
class InsiderRow:
    """One Form 4 / 4/A transaction row as read, before amendment resolution and qualification."""

    accession: str
    row: int
    document_type: str
    filer_cik: str | None
    period_of_report: date | None
    signature_date: date | None
    day: date | None
    price: float | None
    code: str | None = "P"
    title: str | None = "Common Stock"
    derivative: bool = False
    date_invalid: bool = False

    def qualifies(self, first: date) -> bool:
        """Gate 1's input rule: a non-derivative P or S row on a title naming common stock, a finite price > 0, a
        valid transaction date on or after ``first``."""
        return (
            not self.derivative
            and not self.date_invalid
            and self.code in ("P", "S")
            and self.title is not None
            and "common" in self.title.lower()
            and self.price is not None
            and math.isfinite(self.price)
            and self.price > 0
            and self.day is not None
            and self.day >= first
        )


@dataclass
class BuildReport:
    build_id: int
    instruments: int
    steps: int
    summary: dict[str, Any] = field(default_factory=dict)


# --------------------------------------------------------------------------- pure helpers


def nyse_sessions(first: date, through: date) -> tuple[date, ...]:
    """Every NYSE session in [first, through] (``nyse-market-calendar-v1``; a half day is a session)."""
    out: list[date] = []
    day = first
    while day <= through:
        if us_market_status(day) != "closed":
            out.append(day)
        day += timedelta(days=1)
    return tuple(out)


def timed_share_facts(
    rows: Iterable[FactRow], index: SubmissionsIndex
) -> tuple[tuple[ShareFact, ...], CikRefusal | None]:
    """Share-count facts timed by the CIK's submissions index; refused whole when any accession cannot be timed.

    Form is the index's (authoritative for the accession), so an FPI form drops here by ``form_family`` downstream.
    """
    concepts = {COVER: Concept.COVER, BALANCE_SHEET: Concept.BALANCE_SHEET}
    out: list[ShareFact] = []
    for row in rows:
        concept = concepts.get((row.taxonomy, row.concept))
        if concept is None:
            continue
        filing = index.filings.get(row.accession)
        if filing is None or filing.acceptance is None:
            return (), CikRefusal.UNTIMED_ACCESSION
        out.append(
            ShareFact(
                concept=concept,
                accession=row.accession,
                form=filing.form,
                context=row.context,
                accepted_at=datetime.fromisoformat(filing.acceptance),
                value=row.value,
            )
        )
    return tuple(out), None


def public_floats(rows: Iterable[FactRow], index: SubmissionsIndex) -> tuple[FloatFact, ...]:
    """Per context date, the value from the latest-accepted accession; skipped when that accession holds two values
    for it, or when no accession for it can be timed. In-scope forms only."""
    by_context: dict[date, list[tuple[str, str, Decimal]]] = defaultdict(list)
    for row in rows:
        if (row.taxonomy, row.concept) != PUBLIC_FLOAT:
            continue
        filing = index.filings.get(row.accession)
        if filing is None or filing.acceptance is None or form_family(filing.form) not in ("10-K", "10-Q"):
            continue
        by_context[row.context].append((filing.acceptance, row.accession, row.value))
    out: list[FloatFact] = []
    for context, entries in sorted(by_context.items()):
        latest = max((acceptance, accession) for acceptance, accession, _ in entries)
        values = {value for acceptance, accession, value in entries if (acceptance, accession) == latest}
        if len(values) != 1:
            continue
        value = float(values.pop())
        if math.isfinite(value) and value > 0:
            out.append(FloatFact(context=context, value=value))
    return tuple(out)


def resolve_amendments(rows: Iterable[InsiderRow], first: date) -> tuple[Trade, ...]:
    """Gate 1's trades, an amendment replacing its original, then each surviving row qualified (``qualifies``).

    A 4/A carries no structured link to the accession it amends (``insider_filings`` holds only Box 4's date of the
    original), so the replacement unit is the reporting owner's (filer CIK, period of report) -- Box 3's "date of
    earliest transaction", which an amendment of the same report repeats; a row missing either part is its own unit
    (its accession). Where any 4/A exists for a unit, only the latest 4/A's rows are kept (by signature date, then
    accession); otherwise every original's rows are kept.
    Rows are already unique by (accession, row) (``insider_transactions`` unique index). Replacement runs BEFORE the
    qualification filters: a 4/A that corrects a row away from P/S, its price or its date must still displace the
    original row it corrects.
    """
    groups: dict[tuple[str, ...], list[InsiderRow]] = defaultdict(list)
    for row in rows:
        # A row missing either key part cannot be matched to an original, so it is its own report: pooling the NULLs
        # would let one such 4/A displace every unrelated report sharing the gap.
        if row.filer_cik is None or row.period_of_report is None:
            groups[("accession", row.accession)].append(row)
        else:
            groups[("report", row.filer_cik, row.period_of_report.isoformat())].append(row)
    out: list[Trade] = []
    for members in groups.values():
        amendments = [r for r in members if r.document_type == "4/A"]
        if amendments:
            latest = max((r.signature_date or date.min, r.accession) for r in amendments)
            members = [r for r in amendments if (r.signature_date or date.min, r.accession) == latest]
        out.extend(Trade(day=r.day, price=r.price) for r in members if r.qualifies(first) and r.day and r.price)
    return tuple(sorted(out, key=lambda t: (t.day, t.price)))


def build_instrument(
    *,
    facts: Sequence[ShareFact],
    anchors: Sequence[Anchor],
    sessions: Sequence[date],
    step_sessions: Sequence[date],
    closes: Mapping[date, float],
    dollar_volume: Mapping[date, float],
    multiclass: bool,
    trades: Sequence[Trade],
    floats: Sequence[FloatFact],
) -> tuple[tuple[Step, ...], dict[str, Counter[str]]]:
    """Steps for one instrument with Gates 1 and 2 applied (Gate 2 on Gate-1-applied steps), and the gate tallies."""
    steps = build_steps(
        facts, anchors, step_sessions, closes=closes, dollar_volume=dollar_volume, multiclass=multiclass
    )
    quarters, untested = gate1(trades, closes, anchors)
    steps = apply_gate1(steps, quarters)
    results = gate2(floats, steps, closes, sessions)
    steps = apply_gate2(steps, results)
    tallies = {
        "gate1": Counter(str(q.outcome) for q in quarters.values()) + Counter({"untested_trades": untested}),
        "gate2": Counter(str(r.outcome) for r in results),
    }
    return steps, tallies


# --------------------------------------------------------------------------- readers

_SCOPE_SQL = """
    SELECT x.instrument_id, x.identifier_value
    FROM external_identifiers x
    WHERE x.provider = 'sec' AND x.identifier_type = 'cik' AND x.is_primary
      AND x.instrument_id = ANY(%(ids)s::bigint[])
    ORDER BY x.instrument_id
"""

_MULTICLASS_SQL = """
    SELECT x.identifier_value
    FROM instrument_class_shares_outstanding s
    JOIN external_identifiers x
      ON x.instrument_id = s.instrument_id AND x.provider = 'sec' AND x.identifier_type = 'cik' AND x.is_primary
    UNION
    SELECT source_cik FROM instrument_class_shares_outstanding WHERE source_cik IS NOT NULL
"""

_FACTS_SQL = """
    SELECT f.instrument_id, f.taxonomy, f.concept, f.accession_number, f.period_end, f.val
    FROM financial_facts_raw f
    WHERE f.instrument_id = ANY(%(ids)s::bigint[])
      AND ((f.taxonomy = 'dei' AND f.concept = 'EntityCommonStockSharesOutstanding' AND f.unit = 'shares')
        OR (f.taxonomy = 'us-gaap' AND f.concept = 'CommonStockSharesOutstanding' AND f.unit = 'shares')
        OR (f.taxonomy = 'dei' AND f.concept = 'EntityPublicFloat' AND f.unit = 'USD'))
      AND f.period_end >= %(min_context)s
      AND f.val IS NOT NULL AND f.val <> 'NaN'::numeric
"""

_BARS_SQL = """
    SELECT instrument_id, price_date, close::double precision,
           avg(close * volume) OVER w, count(close * volume) OVER w
    FROM price_daily
    WHERE instrument_id = ANY(%(ids)s::bigint[]) AND price_date >= %(first)s
    WINDOW w AS (PARTITION BY instrument_id ORDER BY price_date ROWS BETWEEN %(lag)s PRECEDING AND CURRENT ROW)
"""

# The premise script's shape (``scripts/measure_3841_premise.py``): GROUP BY ... max(filed_at) picks each filer's
# latest report for the quarter in about a minute where DISTINCT ON took over 20.
_UNIT_VALUES_SQL = """
    WITH latest AS (
        SELECT h.filer_id, h.instrument_id, h.period_of_report, max(h.filed_at) AS filed_at
        FROM institutional_holdings h
        WHERE h.instrument_id = ANY(%(ids)s::bigint[]) AND h.period_of_report = ANY(%(quarters)s::date[])
        GROUP BY 1, 2, 3
    )
    SELECT h.instrument_id, h.period_of_report, h.filer_id,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY (h.market_value_usd / h.shares)::double precision),
           array_agg(DISTINCT h.accession_number), max(h.filed_at)
    FROM institutional_holdings h
    JOIN latest l USING (filer_id, instrument_id, period_of_report, filed_at)
    WHERE h.is_put_call IS NULL AND h.shares > 0 AND h.market_value_usd > 0
      AND h.shares <> 'NaN'::numeric AND h.market_value_usd <> 'NaN'::numeric
    GROUP BY 1, 2, 3
"""

# Every row of every Form 4 / 4/A whose report period could hold a trade on or after ``first``: the qualification
# filters run in ``resolve_amendments``, after an amendment has replaced its original.
_TRADES_SQL = """
    SELECT t.instrument_id, t.accession_number, t.txn_row_num, f.document_type, t.filer_cik, f.period_of_report,
           f.signature_date, t.txn_date, t.price::double precision, t.txn_code, t.security_title, t.is_derivative,
           coalesce(t.txn_date_invalid, false)
    FROM insider_transactions t
    JOIN insider_filings f ON f.accession_number = t.accession_number
    WHERE t.instrument_id = ANY(%(ids)s::bigint[])
      AND f.document_type IN ('4', '4/A') AND NOT f.is_tombstone
      AND (f.period_of_report >= %(report_floor)s OR t.txn_date >= %(first)s)
"""

_REVISIONS_SQL = """
    SELECT instrument_id, count(*) FROM price_daily_revision
    WHERE instrument_id = ANY(%(ids)s::bigint[]) GROUP BY instrument_id
"""

# A bar inserted below the series frontier writes no revision row (sql/388); the view compares this count too.
_BACKDATED_SQL = """
    SELECT instrument_id, count(*) FROM price_daily_backdated_insert
    WHERE instrument_id = ANY(%(ids)s::bigint[]) GROUP BY instrument_id
"""


def _cik10(raw: str) -> str:
    return f"{int(raw):010d}"


def load_scope(conn: psycopg.Connection[Any]) -> list[tuple[int, str]]:
    """(instrument_id, 10-digit CIK) for the validated universe's instruments with a primary SEC CIK."""
    ids = list(load_validated_universe(conn))
    return [(int(iid), _cik10(cik)) for iid, cik in conn.execute(_SCOPE_SQL, {"ids": ids}).fetchall()]


def load_multiclass_ciks(conn: psycopg.Connection[Any]) -> frozenset[str]:
    return frozenset(_cik10(row[0]) for row in conn.execute(_MULTICLASS_SQL).fetchall() if row[0])


# --------------------------------------------------------------------------- the build


def _sha256_file(handle: Any) -> str:
    digest = hashlib.sha256()
    for chunk in iter(lambda: handle.read(1 << 20), b""):
        digest.update(chunk)
    handle.seek(0)
    return digest.hexdigest()


@dataclass(frozen=True)
class BasisStamp:
    """What the build saw of the instrument's price series; the view refuses a bar the stamp does not cover."""

    revision_count: int
    backdated_insert_count: int
    bars_through: date | None


def _step_row(instrument_id: int, step: Step, stamp: BasisStamp, build_id: int) -> tuple[Any, ...]:
    selection = step.selection
    return (
        instrument_id,
        step.valid_from,
        step.valid_to,
        str(selection.concept),
        selection.accession,
        selection.context,
        selection.accepted_at,
        selection.shares,
        step.split_factor,
        None if step.refusal is None else str(step.refusal),
        json.dumps(step.evidence, sort_keys=True),
        stamp.revision_count,
        stamp.backdated_insert_count,
        stamp.bars_through,
        build_id,
    )


def run_market_cap_pit_rebuild(
    conn: psycopg.Connection[Any],
    *,
    submissions_path: Path,
    build_date: date,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> BuildReport:
    """Rebuild every step for ``build_date`` (the last session the steps reach). ``conn`` must be autocommit: the
    build lock is a session lock taken before the snapshot opens."""
    if not conn.autocommit:
        raise ValueError("run_market_cap_pit_rebuild needs an autocommit connection (lock before snapshot)")
    started_at = now()
    with submissions_path.open("rb") as handle:
        submissions_sha256 = _sha256_file(handle)
        with zipfile.ZipFile(handle) as archive:
            index_of = submissions_reader(archive)
            conn.execute("SELECT pg_advisory_lock(%s)", (BUILD_LOCK_KEY,))
            try:
                with conn.transaction():
                    conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    return _build(
                        conn,
                        index_of=index_of,
                        build_date=build_date,
                        started_at=started_at,
                        submissions_sha256=submissions_sha256,
                        now=now,
                    )
            finally:
                conn.execute("SELECT pg_advisory_unlock(%s)", (BUILD_LOCK_KEY,))


def _build(
    conn: psycopg.Connection[Any],
    *,
    index_of: Callable[[str], SubmissionsIndex | str],
    build_date: date,
    started_at: datetime,
    submissions_sha256: str,
    now: Callable[[], datetime],
) -> BuildReport:
    quarters = quarter_ends(FIRST_QUARTER, build_date)
    sessions = nyse_sessions(FIRST_QUARTER - timedelta(days=10), build_date)
    step_sessions = tuple(s for s in sessions if s >= FIRST_SESSION)
    anchor_sessions = {q: last_session_on_or_before(sessions, q) for q in quarters}
    first_bar = sessions[0] - timedelta(days=45)  # room for the 21-bar window behind the first session
    min_context = add_months(FIRST_SESSION, -MAX_SHARES_AGE_MONTHS)
    scope = load_scope(conn)
    multiclass_ciks = load_multiclass_ciks(conn)

    sequence = "SELECT nextval(pg_get_serial_sequence('market_cap_pit_builds', 'build_id'))"
    build_id_row = conn.execute(sequence).fetchone()
    assert build_id_row is not None
    build_id = int(build_id_row[0])
    conn.execute("DELETE FROM instrument_share_count_pit")

    summary: dict[str, Counter[str]] = defaultdict(Counter)
    holdings_max_filed_at: datetime | None = None
    indexes: dict[str, SubmissionsIndex | str] = {}
    step_total = 0
    for offset in range(0, len(scope), BATCH_SIZE):
        batch = scope[offset : offset + BATCH_SIZE]
        ids = [iid for iid, _ in batch]
        params = {"ids": ids}

        facts_by: dict[int, list[FactRow]] = defaultdict(list)
        for iid, taxonomy, concept, accession, context, value in conn.execute(
            _FACTS_SQL, {**params, "min_context": min_context}
        ):
            facts_by[iid].append(FactRow(taxonomy, concept, accession, context, value))

        closes_by: dict[int, dict[date, float]] = defaultdict(dict)
        bars_through: dict[int, date] = {}
        volume_by: dict[int, dict[date, float]] = defaultdict(dict)
        for iid, day, close, mean_dv, n in conn.execute(
            _BARS_SQL, {**params, "first": first_bar, "lag": DOLLAR_VOLUME_SESSIONS - 1}
        ):
            bars_through[iid] = max(day, bars_through.get(iid, day))
            if close is not None:
                closes_by[iid][day] = close
            # A NaN numeric close reaches here as a float NaN; it must not reach jsonb evidence, which refuses it.
            if mean_dv is not None and n == DOLLAR_VOLUME_SESSIONS and math.isfinite(float(mean_dv)):
                volume_by[iid][day] = float(mean_dv)

        units: dict[tuple[int, date], list[tuple[float, list[str]]]] = defaultdict(list)
        for iid, quarter, _filer, unit_value, accessions, filed_at in conn.execute(
            _UNIT_VALUES_SQL, {**params, "quarters": list(quarters)}
        ):
            if unit_value is not None and math.isfinite(unit_value) and unit_value > 0:
                units[(iid, quarter)].append((unit_value, accessions))
            if holdings_max_filed_at is None or filed_at > holdings_max_filed_at:
                holdings_max_filed_at = filed_at

        trades_by: dict[int, list[InsiderRow]] = defaultdict(list)
        trade_params = {**params, "first": sessions[0], "report_floor": sessions[0] - timedelta(days=366)}
        for iid, *fields in conn.execute(_TRADES_SQL, trade_params):
            trades_by[iid].append(InsiderRow(*fields))

        revisions = dict(conn.execute(_REVISIONS_SQL, params).fetchall())
        backdated = dict(conn.execute(_BACKDATED_SQL, params).fetchall())

        rows: list[tuple[Any, ...]] = []
        for iid, cik in batch:
            if cik not in indexes:
                indexes[cik] = index_of(cik)
            index = indexes[cik]
            if isinstance(index, str):
                summary["cik_refused"][index] += 1
                continue
            facts, refused = timed_share_facts(facts_by.get(iid, ()), index)
            if refused is not None:
                summary["cik_refused"][str(refused)] += 1
                continue
            closes = closes_by.get(iid, {})
            anchors = []
            for quarter in quarters:
                session = anchor_sessions[quarter]
                assert session is not None
                filers = units.get((iid, quarter), [])
                anchor = classify_anchor(
                    quarter,
                    session,
                    [value for value, _ in filers],
                    closes.get(session),
                    as_of=build_date,
                    accessions=tuple(sorted({a for _, accessions in filers for a in accessions})),
                )
                summary["anchor_status"][str(anchor.status)] += 1
                anchors.append(anchor)
            steps, tallies = build_instrument(
                facts=facts,
                anchors=anchors,
                sessions=sessions,
                step_sessions=step_sessions,
                closes=closes,
                dollar_volume=volume_by.get(iid, {}),
                multiclass=cik in multiclass_ciks,
                trades=resolve_amendments(trades_by.get(iid, ()), sessions[0]),
                floats=public_floats(facts_by.get(iid, ()), index),
            )
            for name, tally in tallies.items():
                summary[name].update(tally)
            if not steps:
                summary["instruments"]["no_eligible_count"] += 1
                continue
            summary["instruments"]["with_steps"] += 1
            for step in steps:
                summary["step_refusal"][str(step.refusal) if step.refusal else "published"] += 1
                stamp = BasisStamp(int(revisions.get(iid, 0)), int(backdated.get(iid, 0)), bars_through.get(iid))
                rows.append(_step_row(iid, step, stamp, build_id))
        step_total += len(rows)
        _copy_steps(conn, rows)
        logger.info("market_cap_pit_rebuild: %d/%d instruments, %d steps", offset + len(batch), len(scope), step_total)

    summary["instruments"]["scope"] = len(scope)
    finished_at = now()
    code_sha_row = conn.execute("SELECT current_setting('ebull.code_commit', true)").fetchone()
    conn.execute(
        """
        INSERT INTO market_cap_pit_builds (build_id, started_at, finished_at, status, build_date, code_sha,
                                           rule_version, submissions_sha256, holdings_max_filed_at, summary)
        VALUES (%s, %s, %s, 'complete', %s, %s, %s, %s, %s, %s)
        """,
        (
            build_id,
            started_at,
            finished_at,
            build_date,
            code_sha_row[0] if code_sha_row else None,
            rule_version(),
            submissions_sha256,
            holdings_max_filed_at,
            json.dumps({name: dict(tally) for name, tally in sorted(summary.items())}, sort_keys=True),
        ),
    )
    return BuildReport(
        build_id=build_id,
        instruments=len(scope),
        steps=step_total,
        summary={name: dict(tally) for name, tally in summary.items()},
    )


def _copy_steps(conn: psycopg.Connection[Any], rows: Sequence[tuple[Any, ...]]) -> None:
    if not rows:
        return
    with conn.cursor().copy(
        """
        COPY instrument_share_count_pit (instrument_id, valid_from, valid_to, shares_concept, accession, context_date,
                                         accepted_at, shares_reported, split_factor, refusal, evidence,
                                         revision_count, backdated_insert_count, bars_through, build_id) FROM STDIN
        """
    ) as copy:
        for row in rows:
            copy.write_row(row)
