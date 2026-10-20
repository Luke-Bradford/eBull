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
  acceptance come from the instrument's ``submissions.zip`` index (``instrument_submissions_reader``, Amendment 1),
  never ``form_type`` or ``filed_date``. An instrument whose primary CIK's index fails integrity, or that holds a
  share-count fact whose accession the index cannot time, is refused whole (``CikRefusal``) rather than published on
  the counts it can time: a newer count it cannot time may already have been public. A fact whose accession only a
  non-primary CIK lists is a predecessor registrant's own filing and is dropped before selection;
* 13F unit values -- read from the canonical observation layer, ``ownership_institutions_observations`` (spec
  Amendment 2, #3908): active economic 13F rows (``source = '13f'``, ``ownership_nature = 'economic'``, ``known_to IS
  NULL``). Per (instrument, quarter, filer CIK): the filer's latest-filed report for the quarter that holds the
  instrument (an amendment replaces it), the median value / shares over its EQUITY rows with shares > 0 and value > 0.
  The legacy ``institutional_holdings`` table lacks the history the bulk 13F drain restores after a late CUSIP mapping
  (#2328). The latest report is chosen per instrument, not per filer and quarter, because a 13F-HR/A is either a
  RESTATEMENT (replaces) or NEW HOLDINGS (adds rows and leaves the original standing) and neither table stores
  which. Per filer and quarter would drop every original that a NEW HOLDINGS amendment followed;
  per instrument keeps a restated-away holding's original row as that filer's value (an accepted limit: one filer
  among the >= 20 in the median);
* dollar volume -- the mean close x volume over the 21 stored bars ending at the session, only when all 21 carry one;
* multi-class -- every CIK with a row in ``instrument_class_shares_outstanding`` (by instrument or ``source_cik``),
  matched against any of the instrument's SEC CIKs;
* FPI / ADS -- an ``ads_ratio`` row, ``coverage.filings_status = 'fpi'`` or the ADR/ADS name marker refuses the
  instrument (``fpi_adr``); the detection of ``xbrl_derived_stats`` and sql/241's ``fpi_adr`` CTE, kept in sync;
* Form 4 trades (Gate 1) -- non-derivative P or S rows with a price > 0 on a title naming common stock, from Form 4
  and 4/A, ``txn_date_invalid`` excluded, qualified only after an amendment has replaced its original
  (``resolve_amendments``). The filing's issuer (Box 2, ``insider_filings.issuer_cik``) must be one of the
  instrument's SEC CIKs, primary or not: ``insider_transactions`` also attributes to an instrument the Form 4s its
  issuer FILED as a reporting owner of another issuer (BAC's rows include PIMCO and Nuveen fund trades), and a
  non-primary CIK keeps an (a)(3) reorganisation's predecessor (XOM, CLBK, XPRO);
* price integrity -- the view refuses an instrument with a ``price_series_break`` whose ``resolved_by`` is NULL
  (sql/246: "NULL = unresolved = the two sides cannot be joined");
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
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, cast

import psycopg

from app.services.factor_panel import MAX_SHARES_AGE_MONTHS, add_months
from app.services.market_calendar import us_market_status
from app.services.pit_fundamentals import (
    REQUIRED_SUBMISSION_COLUMNS,
    Filing,
    SubmissionsIndex,
    acceptance_ny_date,
    form_family,
    normalise_acceptance,
    parse_submissions,
)
from app.services.pit_market_cap import (
    FIRST_QUARTER,
    Anchor,
    CikRefusal,
    Concept,
    FloatFact,
    Gate1Quarter,
    Gate2Result,
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


# --------------------------------------------------------------------------- submissions (spec Amendment 1)


@dataclass(frozen=True)
class InstrumentIndex(SubmissionsIndex):
    """The primary CIK's de-conflicted index, with what Amendment 1 needs beside it.

    ``listed``: every accession the primary CIK's submissions list, before de-confliction. ``conflicted``: accessions
    dropped under rules 1 and 3 (untimed, never resolved). ``predecessor``: accessions a non-primary CIK's index lists
    and the primary never does (rule 2: excluded before selection).
    """

    listed: frozenset[str] = frozenset()
    conflicted: frozenset[str] = frozenset()
    predecessor: frozenset[str] = frozenset()


@dataclass(frozen=True)
class _CikIndex:
    index: SubmissionsIndex | str
    listed: frozenset[str]
    conflicted: frozenset[str]


def _rows(block: object) -> list[list[Any]] | None:
    """The block's required columns when they are lists of equal length; else None (left for the frozen parser)."""
    if not isinstance(block, dict):
        return None
    columns = [cast(dict[str, Any], block).get(name) for name in REQUIRED_SUBMISSION_COLUMNS]
    if not all(isinstance(column, list) for column in columns):
        return None
    lists = cast(list[list[Any]], columns)
    return lists if len({len(column) for column in lists}) == 1 else None


def _clock(acceptance: str | None, form: str) -> tuple[date | None, str]:
    """What the build's clock reads of a record: the New York date of acceptance (source rule "when public") and the
    form. Two records agreeing on it admit the filing on the same session."""
    return (acceptance_ny_date(acceptance) if acceptance else None), form


def deconflict_submissions(
    cik10: str, main: object, read_page: Callable[[str], object | None]
) -> tuple[SubmissionsIndex | str, frozenset[str], frozenset[str]]:
    """Rule 1: drop each accession whose copies disagree on the clock (``_clock``), collapse agreeing copies to one
    carrying the latest acceptance, then run the frozen ``parse_submissions`` on what remains. Returns (index or
    failure, listed, conflicted).

    Only rows of a well-formed block whose accession and form are strings are filtered or compared; anything else
    reaches the frozen parser unchanged, so rule 5's other integrity failures still fire.
    """
    blocks: dict[str | None, object] = {}
    filings = cast(dict[str, Any], main).get("filings") if isinstance(main, dict) else None
    if isinstance(filings, dict):
        blocks[None] = cast(dict[str, Any], filings).get("recent")
        pages = cast(dict[str, Any], filings).get("files")
        for page in cast(list[Any], pages) if isinstance(pages, list) else []:
            name = page.get("name") if isinstance(page, dict) else None
            if isinstance(name, str) and name not in blocks:
                blocks[name] = read_page(name)
    records: dict[str, set[tuple[date | None, str]]] = defaultdict(set)
    latest: dict[str, str | None] = {}
    for block in blocks.values():
        lists = _rows(block)
        for accn, accepted, form, _ in zip(*lists, strict=True) if lists else ():
            if isinstance(accn, str) and isinstance(form, str):
                acceptance = normalise_acceptance(accepted)
                records[accn].add(_clock(acceptance, form))
                if acceptance is not None and (latest.get(accn) or "") < acceptance:
                    latest[accn] = acceptance
                latest.setdefault(accn, None)
    listed = frozenset(records)
    conflicted = frozenset(accn for accn, recs in records.items() if len(recs) > 1)
    seen: set[str] = set()

    def filtered(block: object) -> object:
        lists = _rows(block)
        if lists is None:
            return block
        keep: list[int] = []
        for i, (accn, _, form, _) in enumerate(zip(*lists, strict=True)):
            if isinstance(accn, str) and isinstance(form, str):
                if accn in conflicted or accn in seen:
                    continue
                seen.add(accn)
            keep.append(i)
        rows = dict(zip(REQUIRED_SUBMISSION_COLUMNS, ([column[i] for i in keep] for column in lists), strict=True))
        # An agreeing copy set carries its latest acceptance (normalised strings order as instants).
        rows["acceptanceDateTime"] = [
            latest.get(accn) or accepted if isinstance(accn, str) else accepted
            for accn, accepted in zip(rows["accessionNumber"], rows["acceptanceDateTime"], strict=True)
        ]
        return {**cast(dict[str, Any], block), **rows}

    if not isinstance(filings, dict):
        return parse_submissions(cik10, main, read_page), listed, conflicted
    pages_out = {name: filtered(body) for name, body in blocks.items() if name is not None and body is not None}
    main_out = {
        **cast(dict[str, Any], main),
        "filings": {**cast(dict[str, Any], filings), "recent": filtered(blocks[None])},
    }
    index = parse_submissions(cik10, main_out, lambda name: pages_out[name] if name in pages_out else read_page(name))
    return index, listed, conflicted


def instrument_submissions_reader(archive: zipfile.ZipFile) -> Callable[[str, Sequence[str]], InstrumentIndex | str]:
    """``index_of(primary, others)``: the instrument's index under Amendment 1, or the primary CIK's integrity-failure
    reason (``no_submissions_entry`` when the archive holds no file for it). Each CIK is parsed once per reader."""
    names = set(archive.namelist())
    cache: dict[str, _CikIndex] = {}

    def cik_index(cik: str) -> _CikIndex:
        if cik not in cache:
            member = f"CIK{cik}.json"
            if member not in names:
                cache[cik] = _CikIndex("no_submissions_entry", frozenset(), frozenset())
            else:

                def read_page(page: str) -> object | None:
                    return json.loads(archive.read(page)) if page in names else None

                cache[cik] = _CikIndex(*deconflict_submissions(cik, json.loads(archive.read(member)), read_page))
        return cache[cik]

    def index_of(primary: str, others: Sequence[str]) -> InstrumentIndex | str:
        own = cik_index(primary)
        if isinstance(own.index, str):
            return own.index
        return merge_indexes(own.index, own.listed, own.conflicted, [cik_index(c).index for c in others])

    return index_of


def merge_indexes(
    primary: SubmissionsIndex,
    listed: frozenset[str],
    conflicted: frozenset[str],
    others: Iterable[SubmissionsIndex | str],
) -> InstrumentIndex:
    """Rules 2 and 3: a non-primary index's accession the primary never lists is a predecessor's; one the primary
    times on a different clock (``_clock``) is a conflict, and an agreeing one keeps the later acceptance. A failed
    non-primary index contributes nothing."""
    filings: dict[str, Filing] = dict(primary.filings)
    dropped = set(conflicted)
    predecessor: set[str] = set()
    for other in others:
        if isinstance(other, str):
            continue
        for accn, filing in other.filings.items():
            if accn not in listed:
                predecessor.add(accn)
            elif accn in filings:
                mine = filings[accn]
                if _clock(mine.acceptance, mine.form) != _clock(filing.acceptance, filing.form):
                    del filings[accn]
                    dropped.add(accn)
                elif (filing.acceptance or "") > (mine.acceptance or ""):
                    filings[accn] = filing
    return InstrumentIndex(filings, listed, frozenset(dropped), frozenset(predecessor))


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
    """Share-count facts timed by the instrument's submissions index; refused whole when any accession cannot be timed.

    Form is the index's (authoritative for the accession), so an FPI form drops here by ``form_family`` downstream.
    Amendment 1: a predecessor's accession is skipped (rule 2), and a public-float fact on a dropped conflict refuses
    like a share-count fact (rule 4).
    """
    concepts = {COVER: Concept.COVER, BALANCE_SHEET: Concept.BALANCE_SHEET}
    predecessor = index.predecessor if isinstance(index, InstrumentIndex) else frozenset()
    conflicted = index.conflicted if isinstance(index, InstrumentIndex) else frozenset()
    out: list[ShareFact] = []
    for row in rows:
        if row.accession in predecessor:
            continue
        if (row.taxonomy, row.concept) == PUBLIC_FLOAT and row.accession in conflicted:
            return (), CikRefusal.UNTIMED_ACCESSION
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
) -> tuple[tuple[Step, ...], dict[str, Counter[str]], dict[date, Gate1Quarter], tuple[Gate2Result, ...]]:
    """Steps for one instrument with Gates 1 and 2 applied (Gate 2 on Gate-1-applied steps), the gate tallies, and
    each gate's per-quarter / per-float results."""
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
    return steps, tallies, quarters, results


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

_OTHER_CIKS_SQL = """
    SELECT x.instrument_id, x.identifier_value
    FROM external_identifiers x
    WHERE x.provider = 'sec' AND x.identifier_type = 'cik' AND NOT x.is_primary
      AND x.instrument_id = ANY(%(ids)s::bigint[])
"""

# Amendment 1 rule 6: the detection of ``xbrl_derived_stats`` and sql/241's ``fpi_adr`` CTE plus ``ads_ratio``
# membership. Keep the three in sync.
_FPI_ADR_SQL = """
    SELECT instrument_id FROM ads_ratio WHERE instrument_id = ANY(%(ids)s::bigint[])
    UNION
    SELECT instrument_id FROM coverage WHERE filings_status = 'fpi' AND instrument_id = ANY(%(ids)s::bigint[])
    UNION
    SELECT instrument_id FROM instruments
    WHERE company_name ~* '\\y(ADR|ADS)\\y' AND instrument_id = ANY(%(ids)s::bigint[])
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
    WITH active AS (
        SELECT o.filer_cik, o.instrument_id, o.period_end, o.source_accession, o.filed_at, o.exposure_kind,
               o.shares, o.market_value_usd
        FROM ownership_institutions_observations o
        WHERE o.instrument_id = ANY(%(ids)s::bigint[]) AND o.period_end = ANY(%(quarters)s::date[])
          AND o.source = '13f' AND o.ownership_nature = 'economic' AND o.known_to IS NULL
    ),
    latest AS (
        SELECT filer_cik, instrument_id, period_end, max(filed_at) AS filed_at FROM active GROUP BY 1, 2, 3
    )
    SELECT a.instrument_id, a.period_end, a.filer_cik,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY (a.market_value_usd / a.shares)::double precision),
           array_agg(DISTINCT a.source_accession), max(a.filed_at)
    FROM active a
    JOIN latest l USING (filer_cik, instrument_id, period_end, filed_at)
    WHERE a.exposure_kind = 'EQUITY' AND a.shares > 0 AND a.market_value_usd > 0
      AND a.shares <> 'NaN'::numeric AND a.market_value_usd <> 'NaN'::numeric
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
      AND EXISTS (
          SELECT 1 FROM external_identifiers x
          WHERE x.instrument_id = t.instrument_id AND x.provider = 'sec' AND x.identifier_type = 'cik'
            AND ltrim(x.identifier_value, '0') = ltrim(f.issuer_cik, '0')
      )
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


def step_row(instrument_id: int, step: Step, stamp: BasisStamp, build_id: int) -> tuple[Any, ...]:
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
    # One handle serves the hash and the reads. sec_bulk_refresh replaces the archive by rename
    # (``partial_path.replace(archive_path)``), so an open handle keeps reading the file it hashed.
    with submissions_path.open("rb") as handle:
        submissions_sha256 = _sha256_file(handle)
        with zipfile.ZipFile(handle) as archive:
            index_of = instrument_submissions_reader(archive)
            conn.execute("SELECT pg_advisory_lock(%s)", (BUILD_LOCK_KEY,))
            try:
                with conn.transaction():
                    # The first statement after BEGIN, before any query fixes the snapshot; asserted, not assumed.
                    conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    level = conn.execute("SELECT current_setting('transaction_isolation')").fetchone()
                    if level is None or level[0] != "repeatable read":
                        raise RuntimeError(f"market_cap_pit_rebuild needs REPEATABLE READ, got {level}")
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


@dataclass(frozen=True)
class InstrumentBuild:
    """One instrument's build result: ``refused`` (the CIK-level reason it has no steps) or its gated steps."""

    instrument_id: int
    cik: str
    refused: str | None
    facts: tuple[ShareFact, ...]
    steps: tuple[Step, ...]
    anchor_status: Counter[str]
    tallies: dict[str, Counter[str]]
    stamp: BasisStamp
    gate1: dict[date, Gate1Quarter] = field(default_factory=dict)
    gate2: tuple[Gate2Result, ...] = ()


@dataclass(frozen=True)
class BatchBuild:
    instruments: tuple[InstrumentBuild, ...]
    holdings_max_filed_at: datetime | None


def iter_instrument_builds(
    conn: psycopg.Connection[Any],
    *,
    scope: Sequence[tuple[int, str]],
    index_of: Callable[[str, Sequence[str]], SubmissionsIndex | str],
    build_date: date,
) -> Iterator[BatchBuild]:
    """Read and build every instrument in ``scope`` for ``build_date``, one batch at a time; writes nothing.

    The build's only reader path: ``_build`` publishes what this yields, and the acceptance script
    (``scripts/measure_3841_acceptance.py``) reports on it, so the two cannot drift. Run it inside the caller's
    REPEATABLE READ transaction so every batch reads one snapshot.
    """
    quarters = quarter_ends(FIRST_QUARTER, build_date)
    sessions = nyse_sessions(FIRST_QUARTER - timedelta(days=10), build_date)
    step_sessions = tuple(s for s in sessions if s >= FIRST_SESSION)
    anchor_sessions = {q: last_session_on_or_before(sessions, q) for q in quarters}
    first_bar = sessions[0] - timedelta(days=45)  # room for the 21-bar window behind the first session
    min_context = add_months(FIRST_SESSION, -MAX_SHARES_AGE_MONTHS)
    multiclass_ciks = load_multiclass_ciks(conn)

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

        # This batch's max only: the query below reads this batch's ids. ``_build`` takes the max across batches.
        holdings_max_filed_at: datetime | None = None
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

        others: dict[int, list[str]] = defaultdict(list)
        for iid, other in conn.execute(_OTHER_CIKS_SQL, params):
            others[iid].append(_cik10(other))
        fpi_adr = {int(row[0]) for row in conn.execute(_FPI_ADR_SQL, params)}

        revisions = dict(conn.execute(_REVISIONS_SQL, params).fetchall())
        backdated = dict(conn.execute(_BACKDATED_SQL, params).fetchall())

        results: list[InstrumentBuild] = []
        for iid, cik in batch:
            stamp = BasisStamp(int(revisions.get(iid, 0)), int(backdated.get(iid, 0)), bars_through.get(iid))
            if iid in fpi_adr:
                results.append(InstrumentBuild(iid, cik, str(CikRefusal.FPI_ADR), (), (), Counter(), {}, stamp))
                continue
            ciks = sorted(others.get(iid, ()))
            index = index_of(cik, ciks)
            if isinstance(index, str):
                results.append(InstrumentBuild(iid, cik, index, (), (), Counter(), {}, stamp))
                continue
            submissions: dict[str, Counter[str]] = {}
            if isinstance(index, InstrumentIndex):
                # Counted before any refusal, so the summary also sees the conflicts and exclusions that refuse.
                submissions["submissions"] = Counter(
                    {
                        "conflicts_dropped": len(index.conflicted),
                        "predecessor_facts_excluded": sum(
                            row.accession in index.predecessor for row in facts_by.get(iid, ())
                        ),
                    }
                )
            facts, refused = timed_share_facts(facts_by.get(iid, ()), index)
            if refused is not None:
                results.append(InstrumentBuild(iid, cik, str(refused), (), (), Counter(), submissions, stamp))
                continue
            closes = closes_by.get(iid, {})
            anchors = []
            anchor_status: Counter[str] = Counter()
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
                anchor_status[str(anchor.status)] += 1
                anchors.append(anchor)
            steps, tallies, gate1_quarters, gate2_results = build_instrument(
                facts=facts,
                anchors=anchors,
                sessions=sessions,
                step_sessions=step_sessions,
                closes=closes,
                dollar_volume=volume_by.get(iid, {}),
                multiclass=any(c in multiclass_ciks for c in (cik, *ciks)),
                trades=resolve_amendments(trades_by.get(iid, ()), sessions[0]),
                floats=public_floats(facts_by.get(iid, ()), index),
            )
            tallies.update(submissions)
            results.append(
                InstrumentBuild(
                    iid, cik, None, facts, steps, anchor_status, tallies, stamp, gate1_quarters, gate2_results
                )
            )
        yield BatchBuild(tuple(results), holdings_max_filed_at)


def _build(
    conn: psycopg.Connection[Any],
    *,
    index_of: Callable[[str, Sequence[str]], SubmissionsIndex | str],
    build_date: date,
    started_at: datetime,
    submissions_sha256: str,
    now: Callable[[], datetime],
) -> BuildReport:
    scope = load_scope(conn)

    sequence = "SELECT nextval(pg_get_serial_sequence('market_cap_pit_builds', 'build_id'))"
    build_id_row = conn.execute(sequence).fetchone()
    assert build_id_row is not None
    build_id = int(build_id_row[0])
    conn.execute("DELETE FROM instrument_share_count_pit")

    summary: dict[str, Counter[str]] = defaultdict(Counter)
    holdings_max_filed_at: datetime | None = None
    step_total = 0
    done = 0
    for batch in iter_instrument_builds(conn, scope=scope, index_of=index_of, build_date=build_date):
        if batch.holdings_max_filed_at is not None and (
            holdings_max_filed_at is None or batch.holdings_max_filed_at > holdings_max_filed_at
        ):
            holdings_max_filed_at = batch.holdings_max_filed_at
        rows: list[tuple[Any, ...]] = []
        for result in batch.instruments:
            if result.refused is not None:
                summary["cik_refused"][result.refused] += 1
                for name, tally in result.tallies.items():
                    summary[name].update(tally)
                continue
            summary["anchor_status"].update(result.anchor_status)
            for name, tally in result.tallies.items():
                summary[name].update(tally)
            if not result.steps:
                summary["instruments"]["no_eligible_count"] += 1
                continue
            summary["instruments"]["with_steps"] += 1
            for step in result.steps:
                summary["step_refusal"][str(step.refusal) if step.refusal else "published"] += 1
                rows.append(step_row(result.instrument_id, step, result.stamp, build_id))
        step_total += len(rows)
        _copy_steps(conn, rows)
        done += len(batch.instruments)
        logger.info("market_cap_pit_rebuild: %d/%d instruments, %d steps", done, len(scope), step_total)

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
