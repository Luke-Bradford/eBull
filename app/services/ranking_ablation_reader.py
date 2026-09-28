"""#1822 route F slice 1b: the database half of the ablation readout.

Spec ``docs/proposals/ta/2026-09-28-1822-ranking-ablation.md`` (v6). :mod:`ranking_ablation`
is the pure construction; this module loads what it consumes and nothing else. Every read
is SELECT-only, and the caller runs them all in one REPEATABLE READ transaction (spec
"Reader": prices and metadata come from one snapshot).

What is read, each rule the spec's:

- **Population** (Route F "Population", "Lane eligibility"). Every ``scores`` row of
  :data:`ranking_ablation.MODEL_VERSION`. A row leaves both keys and the control when its
  instrument is not a ``Stocks`` type or not USD, or when :func:`ranking_ablation.parse_score_row`
  refuses it; each reason is counted.
- **Witness** (Route F "Timeline"). A run is known at the ``finished_at`` of the one
  successful ``morning_candidate_review`` whose [``started_at``, ``finished_at``] covers its
  ``scored_at``. None covering → ``no_witness``; more than one → ``ambiguous_witness``.
- **Prices** ("Reader"). ``price_daily`` from the last bar before the first formation
  through c_k, with the bar and transition verdicts COMPUTED here by
  :func:`price_quarantine.evaluate_series` under the asset class the quarantine store uses.
  ``price_bar_quarantine`` is never read. Masking follows
  ``research_price_structure_store.load_masked_series``: close on ``return_usable``, high
  and low on ``range_usable``, the open on its value.
- **Cutoff** ("Grid"). c_k is calendar-only: the latest session no bar of which
  :func:`price_quarantine.evaluate_bars` can mark provisional at ``as_of`` = the readout date.
  ⚠ The spec says "at least ``PROVISIONAL_WINDOW_DAYS`` calendar days before"; the rule
  marks ``price_date >= as_of − 5 days`` provisional, so a session exactly 5 days back IS
  provisional. The spec's own test ("no bar ≤ c_k provisional") decides it: strictly more.
- **Termination** (Route F "Termination source"). Form 25 link through
  ``instrument_cik_history``, common-equity delistings only, filed in [first formation, c_k]; the
  Q suffix from the current symbol. A series stopping before c_k terminates only when
  :func:`series_termination.classify_termination` is not ``UNKNOWN``.

Every query goes through :func:`execute`, which runs only a name in :data:`ROUTE_F_SQL`
(spec v8 "Sealed-outcome boundary").
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Final, Literal

import psycopg

from app.services.price_quarantine import PROVISIONAL_WINDOW_DAYS, Bar, SeriesVerdicts, evaluate_series
from app.services.price_quarantine_store import _SCOPE_SQL as QUARANTINE_SCOPE_SQL
from app.services.ranking_ablation import FAMILY_ORDER, MODEL_VERSION, Run, ScoreRow, parse_score_row
from app.services.research_corpus_ingest import vendor_symbol_has_bankruptcy_suffix
from app.services.series_termination import TerminationClass, TerminationEvidence, classify_termination
from app.services.strategies.validated_universe import STOCKS_TYPE_DESCRIPTION

LANE_CURRENCY: Final = "USD"
#: ``scheduler.JOB_MORNING_CANDIDATE_REVIEW``, the one job ``compute_rankings`` runs inside.
#: Not imported: a service must not import the scheduler. A test pins the two equal.
WITNESS_JOB: Final = "morning_candidate_review"
WITNESS_STATUS: Final = "success"
#: The branch markers ``scoring.compute_score`` writes into ``explanation`` (``_value_score``'s
#: fallback note, and the confidence default). Both predate v1.5; a test pins them to the writer.
VALUE_FALLBACK_MARKER: Final = "fundamentals fallback"
CONFIDENCE_NO_THESIS_MARKER: Final = "confidence: no thesis"

WitnessRefusal = Literal["no_witness", "ambiguous_witness"]


# ---------------------------------------------------------------------------
# Population
# ---------------------------------------------------------------------------

_POPULATION_SQL: Final = """
SELECT s.scored_at, s.instrument_id, s.rank,
       s.quality_score, s.value_score, s.turnaround_score,
       s.momentum_score, s.sentiment_score, s.confidence_score,
       s.raw_total, s.total_score, s.penalties_json,
       t.description, i.currency, i.symbol, s.explanation
FROM scores s
JOIN instruments i ON i.instrument_id = s.instrument_id
LEFT JOIN etoro_instrument_types t ON t.instrument_type_id = i.instrument_type_id
WHERE s.model_version = %(model_version)s
ORDER BY s.scored_at, s.instrument_id
"""


@dataclass(frozen=True)
class StoredRow:
    """One population row that entered both keys, with the stored totals kept for the census."""

    row: ScoreRow
    raw_total: float
    total_score: float | None
    #: The branch the writer recorded: value on its thesis path, confidence from a thesis.
    value_from_thesis: bool
    confidence_from_thesis: bool


@dataclass(frozen=True)
class Population:
    #: scored_at → instrument → row, lane-eligible and valid only. Every run read is a key,
    #: including one whose rows were all excluded.
    runs: Mapping[datetime, Mapping[int, StoredRow]]
    #: reason → rows excluded for it (lane reasons first; a lane-excluded row is not parsed).
    excluded_rows: Mapping[str, int]
    #: reason → distinct instruments excluded for it at least once.
    excluded_names: Mapping[str, int]
    #: The symbol current at the read, for every population instrument.
    symbols: Mapping[int, str]


def lane_exclusion(type_description: str | None, currency: str | None) -> str | None:
    """``non_stock`` / ``non_usd``, or None when the row is priced as a real-stock long."""
    if type_description != STOCKS_TYPE_DESCRIPTION:
        return "non_stock"
    if currency != LANE_CURRENCY:
        return "non_usd"
    return None


def _as_float(value: Any) -> float | None:
    return None if value is None else float(value)


def build_population(rows: Iterable[Sequence[Any]]) -> Population:
    """Pure half of :func:`load_population`, over ``_POPULATION_SQL``'s column order."""
    runs: dict[datetime, dict[int, StoredRow]] = {}
    excluded_rows: Counter[str] = Counter()
    excluded_names: dict[str, set[int]] = {}
    symbols: dict[int, str] = {}
    for (
        scored_at,
        instrument_id,
        rank,
        quality,
        value,
        turnaround,
        momentum,
        sentiment,
        confidence,
        raw_total,
        total_score,
        penalties_json,
        type_description,
        currency,
        symbol,
        explanation,
    ) in rows:
        iid = int(instrument_id)
        symbols[iid] = symbol
        # A run whose every row is excluded still exists: it can supersede an earlier run
        # at the same entry session, and its formation is then idle (spec "Timeline").
        run = runs.setdefault(scored_at, {})
        parsed: ScoreRow | str = lane_exclusion(type_description, currency) or parse_score_row(
            iid,
            rank=rank,
            family_scores=dict(
                zip(FAMILY_ORDER, (quality, value, turnaround, momentum, sentiment, confidence), strict=True)
            ),
            raw_total=raw_total,
            penalties_json=penalties_json,
        )
        if isinstance(parsed, str):
            excluded_rows[parsed] += 1
            excluded_names.setdefault(parsed, set()).add(iid)
            continue
        text = explanation or ""
        run[iid] = StoredRow(
            row=parsed,
            raw_total=float(raw_total),
            total_score=_as_float(total_score),
            value_from_thesis=VALUE_FALLBACK_MARKER not in text,
            confidence_from_thesis=CONFIDENCE_NO_THESIS_MARKER not in text,
        )
    return Population(
        runs=runs,
        excluded_rows=dict(excluded_rows),
        excluded_names={reason: len(names) for reason, names in excluded_names.items()},
        symbols=symbols,
    )


def load_population(conn: psycopg.Connection[Any]) -> Population:
    return build_population(execute(conn, "population", {"model_version": MODEL_VERSION}))


# ---------------------------------------------------------------------------
# Witness
# ---------------------------------------------------------------------------

_WITNESS_SQL: Final = """
SELECT started_at, finished_at
FROM job_runs
WHERE job_name = %(job_name)s AND status = %(status)s AND finished_at IS NOT NULL
ORDER BY started_at
"""


@dataclass(frozen=True)
class JobWindow:
    started_at: datetime
    finished_at: datetime


def witness(scored_at: datetime, windows: Iterable[JobWindow]) -> datetime | WitnessRefusal:
    """``finished_at`` of the one successful run covering ``scored_at`` (bounds inclusive)."""
    covering = [window for window in windows if window.started_at <= scored_at <= window.finished_at]
    if not covering:
        return "no_witness"
    if len(covering) > 1:
        return "ambiguous_witness"
    return covering[0].finished_at


@dataclass(frozen=True)
class WitnessedRuns:
    runs: tuple[Run, ...]
    #: scored_at → why the run has no known time.
    refused: Mapping[datetime, WitnessRefusal]


def witness_runs(scored_ats: Iterable[datetime], windows: Sequence[JobWindow]) -> WitnessedRuns:
    runs: list[Run] = []
    refused: dict[datetime, WitnessRefusal] = {}
    for scored_at in sorted(scored_ats):
        known = witness(scored_at, windows)
        if isinstance(known, str):
            refused[scored_at] = known
        else:
            runs.append(Run(scored_at=scored_at, known_at=known))
    return WitnessedRuns(runs=tuple(runs), refused=refused)


def load_job_windows(conn: psycopg.Connection[Any]) -> tuple[JobWindow, ...]:
    rows = execute(conn, "witness", {"job_name": WITNESS_JOB, "status": WITNESS_STATUS})
    return tuple(JobWindow(started_at=row[0], finished_at=row[1]) for row in rows)


# ---------------------------------------------------------------------------
# Cutoff
# ---------------------------------------------------------------------------


def cutoff_session(readout_date: date, sessions: Sequence[date]) -> date | None:
    """c_k: the latest session strictly before ``readout_date − PROVISIONAL_WINDOW_DAYS``."""
    provisional_from = readout_date - timedelta(days=PROVISIONAL_WINDOW_DAYS)
    eligible = [day for day in sessions if day < provisional_from]
    return max(eligible) if eligible else None


# ---------------------------------------------------------------------------
# Prices
# ---------------------------------------------------------------------------

_SERIES_SQL: Final = """
WITH starts AS (
    SELECT ids.instrument_id,
           (SELECT max(q.price_date) FROM price_daily q
            WHERE q.instrument_id = ids.instrument_id AND q.price_date < %(first_formation)s) AS start_date
    FROM unnest(%(instrument_ids)s::bigint[]) AS ids(instrument_id)
)
SELECT p.instrument_id, p.price_date, p.open, p.high, p.low, p.close, p.volume
FROM starts s
JOIN price_daily p ON p.instrument_id = s.instrument_id
WHERE p.price_date >= COALESCE(s.start_date, %(first_formation)s)
  AND p.price_date <= %(cutoff)s
ORDER BY p.instrument_id, p.price_date
"""


@dataclass(frozen=True)
class ReadSeries:
    """One instrument's bars as stored, its computed verdicts, and the masked bars."""

    asset_class: str | None
    bars: tuple[Bar, ...]
    verdicts: SeriesVerdicts
    masked: tuple[Bar, ...]


def mask_bars(bars: Sequence[Bar], verdicts: SeriesVerdicts) -> tuple[Bar, ...]:
    """``load_masked_series``' rule on computed verdicts: per field, the open on its value."""
    if [bar.price_date for bar in bars] != [verdict.price_date for verdict in verdicts.bars]:
        raise ValueError("verdicts must be the series' own, one per bar in order")
    return tuple(
        Bar(
            price_date=bar.price_date,
            open=bar.open if bar.open is not None and bar.open > 0 else None,
            high=bar.high if verdict.range_usable else None,
            low=bar.low if verdict.range_usable else None,
            close=bar.close if verdict.return_usable else None,
            volume=bar.volume,
        )
        for bar, verdict in zip(bars, verdicts.bars, strict=True)
    )


def read_series(bars: Sequence[Bar], asset_class: str | None, *, as_of: date) -> ReadSeries:
    """Verdicts computed in-process over the bars as read, then masked."""
    verdicts = evaluate_series(bars, asset_class, as_of=as_of)
    return ReadSeries(asset_class=asset_class, bars=tuple(bars), verdicts=verdicts, masked=mask_bars(bars, verdicts))


def load_series(
    conn: psycopg.Connection[Any],
    instrument_ids: Sequence[int],
    *,
    first_formation: date,
    cutoff: date,
    as_of: date,
) -> dict[int, ReadSeries]:
    """Every requested instrument with at least one bar in the read range; the rest are absent."""
    ids = sorted(set(instrument_ids))
    # ``price_quarantine_store.asset_classes``' query, run through the registry.
    classes = {int(iid): asset_class for iid, asset_class in execute(conn, "asset_class", {"instrument_ids": ids})}
    grouped: dict[int, list[Bar]] = {}
    for iid, price_date, open_, high, low, close, volume in execute(
        conn, "series", {"instrument_ids": ids, "first_formation": first_formation, "cutoff": cutoff}
    ):
        grouped.setdefault(int(iid), []).append(
            Bar(price_date=price_date, open=open_, high=high, low=low, close=close, volume=volume)
        )
    return {iid: read_series(bars, classes.get(iid), as_of=as_of) for iid, bars in grouped.items()}


def input_sha256(series: Mapping[int, ReadSeries]) -> str:
    """sha256 over the ordered rows read and each series' asset class (the vintage identity's price half)."""
    digest = hashlib.sha256()
    for iid in sorted(series):
        read = series[iid]
        digest.update(json.dumps([iid, read.asset_class]).encode())
        for bar in read.bars:
            fields = (bar.price_date, bar.open, bar.high, bar.low, bar.close, bar.volume)
            digest.update(json.dumps([None if f is None else str(f) for f in fields]).encode())
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# Termination
# ---------------------------------------------------------------------------

#: ``sec_form25_common_equity_delistings`` (sql/252): an equity delisting of COMMON equity. A
#: warrant, unit, preferred or note delisting by the same issuer does not end the stock.
_FORM25_SQL: Final = """
SELECT h.instrument_id, r.rule_provision
FROM instrument_cik_history h
JOIN sec_form25_common_equity_delistings r ON r.issuer_cik = h.cik
WHERE h.instrument_id = ANY(%(instrument_ids)s::bigint[])
  AND r.filed_date BETWEEN %(first_formation)s AND %(cutoff)s
ORDER BY h.instrument_id, r.filed_date, r.accession_number
"""


def load_form25_links(
    conn: psycopg.Connection[Any], instrument_ids: Sequence[int], *, first_formation: date, cutoff: date
) -> dict[int, str | None]:
    """instrument → the provision of its latest linked common-equity Form 25 in the window."""
    links: dict[int, str | None] = {}
    for iid, provision in execute(
        conn,
        "form25",
        {
            "instrument_ids": sorted(set(instrument_ids)),
            "first_formation": first_formation,
            "cutoff": cutoff,
        },
    ):
        links[int(iid)] = provision  # ordered by filed_date: the latest wins
    return links


def stopped_series(series: Mapping[int, ReadSeries], cutoff: date) -> dict[int, date]:
    """instrument → its last stored bar, for every series whose last bar is before c_k."""
    return {iid: read.bars[-1].price_date for iid, read in series.items() if read.bars[-1].price_date < cutoff}


def termination_classes(
    stopped: Iterable[int], *, links: Mapping[int, str | None], symbols: Mapping[int, str]
) -> dict[int, TerminationClass]:
    """The class of every stopped series; only a non-``UNKNOWN`` class terminates it."""
    return {
        iid: classify_termination(
            TerminationEvidence(
                linked=iid in links,
                provision=links.get(iid),
                q_suffix=vendor_symbol_has_bankruptcy_suffix(symbols.get(iid, "")),
            )
        )
        for iid in stopped
    }


# ---------------------------------------------------------------------------
# Census, dividends and the declaration's freeze time
# ---------------------------------------------------------------------------

#: The spec's "Premise check" figures, reprinted by ``--census``.
_PREMISE_SQL: Final = {
    "premise.financial_facts_min_filed_date": "SELECT min(filed_date) FROM financial_facts_raw",
    "premise.financial_facts_instruments_filed_before_2009": (
        "SELECT count(DISTINCT instrument_id) FROM financial_facts_raw WHERE filed_date < DATE '2009-01-01'"
    ),
    "premise.theses_min_created_at": "SELECT min(created_at) FROM theses",
    "premise.theses_instruments": "SELECT count(DISTINCT instrument_id) FROM theses",
    "premise.news_events_min_event_time": "SELECT min(event_time) FROM news_events",
}
_DIVIDEND_SQL: Final = """
SELECT count(*), count(DISTINCT instrument_id) FROM dividend_events
WHERE ex_date BETWEEN %(start)s AND %(end)s AND instrument_id = ANY(%(ids)s::bigint[])
"""
#: ``load_preregistration`` does not return ``frozen_at``; the prospective boundary needs it.
_FROZEN_AT_SQL: Final = """
SELECT frozen_at FROM strategy_preregistration_declarations WHERE declaration_id = %(declaration_id)s
"""

# ---------------------------------------------------------------------------
# The registry (spec "Sealed-outcome boundary")
# ---------------------------------------------------------------------------

#: Every query route F issues, by name. :func:`execute` runs nothing else, so the runtime path
#: cannot reach a relation the boundary test has not audited. The ledger's freeze/load SQL is
#: the one exception: it runs inside ``result_ledger``. The declaration's semantic terms carry
#: this mapping, so changing a query is a new declaration.
ROUTE_F_SQL: Final[Mapping[str, str]] = {
    "population": _POPULATION_SQL,
    "witness": _WITNESS_SQL,
    "asset_class": QUARANTINE_SCOPE_SQL,
    "series": _SERIES_SQL,
    "form25": _FORM25_SQL,
    **_PREMISE_SQL,
    "dividend_events": _DIVIDEND_SQL,
    "frozen_at": _FROZEN_AT_SQL,
}


class UnregisteredQuery(KeyError):
    """A route F query name outside :data:`ROUTE_F_SQL`."""


def execute(conn: psycopg.Connection[Any], name: str, params: Mapping[str, Any] | None = None) -> list[tuple[Any, ...]]:
    """Run the registered query ``name`` and return every row; refuse any other."""
    sql = ROUTE_F_SQL.get(name)
    if sql is None:
        raise UnregisteredQuery(name)
    return conn.execute(sql, params).fetchall()  # type: ignore[arg-type]  # registered constant, not LiteralString-typed


__all__ = [
    "CONFIDENCE_NO_THESIS_MARKER",
    "ROUTE_F_SQL",
    "UnregisteredQuery",
    "execute",
    "LANE_CURRENCY",
    "VALUE_FALLBACK_MARKER",
    "WITNESS_JOB",
    "WITNESS_STATUS",
    "JobWindow",
    "Population",
    "ReadSeries",
    "StoredRow",
    "WitnessedRuns",
    "build_population",
    "cutoff_session",
    "input_sha256",
    "lane_exclusion",
    "load_form25_links",
    "load_job_windows",
    "load_population",
    "load_series",
    "mask_bars",
    "read_series",
    "stopped_series",
    "termination_classes",
    "witness",
    "witness_runs",
]
