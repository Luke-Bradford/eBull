"""Fail-closed evaluator primitives for the frozen #2582 Schedule 13D trial.

The real outcome command is intentionally unusable until the candidate has an
entry in ``app.services.trial_register``.  That entry is only added after this
code has been reviewed.  Keeping source selection, session selection and return
math here lets us test the causal machinery without querying a single price
bar.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, Literal, LiteralString, cast

import psycopg

from app.services.market_calendar import us_market_status
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.result_ledger import (
    HoldoutAccess,
    PreregDeclarationRefused,
    load_preregistration,
    require_outcome_access,
    verify_outcome_access_provenance,
)
from app.services.trial_register import TRIAL_REGISTER
from scripts.schedule13d_challengers import RULE_13G, MatchFeatures, select_random_time_sessions
from scripts.schedule13d_statistics import EventOutcome
from scripts.verify_2582_schedule13d_preregistration import EXPECTED_SHA256, load_and_verify

TRIAL_ID: Final = "c4-schedule13d-public-catalyst-v1"
ACKNOWLEDGEMENT: Final = "OPEN-2582-SEALED-OUTCOMES"

#: #2614 — C-4's identity in ``strategy_preregistration_declarations``. Both are
#: the frozen contract's own fields (``candidate_id`` / ``contract_version``),
#: not a naming invented here; ``test_evaluate_2582_schedule13d_outcomes``
#: asserts they still match the contract bytes the digest protects.
STRATEGY_ID: Final = "c4-schedule13d-public-catalyst"
STRATEGY_VERSION: Final = "schedule13d-public-catalyst-v1"
#: #2601 — §9's random-entry synthetic control, DECLARED rather than absent.
#:
#: ⚠⚠ IT IS NOT MERELY UNRUN HERE, IT DOES NOT APPLY. ``random_entry_cohort``
#: permutes *"the entries this strategy actually made"* inside each series'
#: eligible fill bars, holding the universe, the date axis and the exit-side
#: accounting fixed. C-4 has no such placement space: its entry is a FILING
#: EVENT, not a bar this evaluator chose among others, and its own contract
#: already carries three matched-control arms of the right shape for that design
#: (``paired_..._vs_random``, ``vs_matched_13g_1b``, ``vs_matched_13g_1c``).
#: Redrawing an entry bar would test a null nothing about C-4 is exposed to.
#:
#: ⚠ THE DECLARATION LIVES HERE AND NOT IN THE CONTRACT JSON, because that file
#: is digest-protected (``EXPECTED_SHA256``) and a preregistration whose bytes
#: move is not a preregistration. ``tests/test_synthetic_control_run.py`` walks
#: every ``scripts/evaluate_*.py`` and requires this pair, so the rule is a test
#: rather than a convention — #2614's own lesson, one ticket on.
SYNTHETIC_CONTROL: Final = "not_applicable"
SYNTHETIC_CONTROL_REASON: Final = (
    "the entry is a filing event, not a bar selected from an eligible placement space, so a permuted-entry null "
    "has nothing to permute; the contract's three matched-control arms are this design's equivalent"
)

RESEARCH_VENDOR: Final = "paperswithbacktest/Stocks-Daily-Price"
FIRST_SOURCE_DATE: Final = date(2024, 12, 18)
LAST_COMPLETE_FILING_DATE: Final = date(2026, 6, 18)


@dataclass(frozen=True)
class SourceEvent:
    accession_number: str
    issuer_cik: str
    instrument_id: int | None
    public_filing_date: date
    maximum_percent_of_class: Decimal | None
    prior_active: bool
    prior_passive: bool
    same_public_date_peer: bool
    reporter_identity_complete: bool
    current_security_eligible: bool
    series_ids: tuple[int, ...]
    series_adjustment_bases: tuple[str, ...]

    @property
    def unfiltered_source_refusal(self) -> str | None:
        if not self.reporter_identity_complete:
            return "reporter_identity_missing"
        if self.instrument_id is None:
            return "instrument_mapping_missing"
        if not self.current_security_eligible:
            return "current_security_scope_ineligible"
        if len(self.series_ids) != 1:
            return "research_series_missing_or_ambiguous"
        if self.series_adjustment_bases != ("split_adjusted",):
            return "research_series_adjustment_basis_unexpected"
        return None

    @property
    def primary_source_refusal(self) -> str | None:
        refusal = self.unfiltered_source_refusal
        if refusal is not None:
            return refusal
        if self.prior_active:
            return "prior_active_chain"
        if self.prior_passive:
            return "prior_passive_chain"
        if self.same_public_date_peer:
            return "same_public_date_chain_ambiguous"
        return None


@dataclass(frozen=True)
class Initial13GSourceEvent:
    accession_number: str
    issuer_cik: str
    instrument_id: int | None
    public_filing_date: date
    rule: RULE_13G
    raw_document_count: int
    current_security_eligible: bool
    series_ids: tuple[int, ...]
    series_adjustment_bases: tuple[str, ...]

    @property
    def source_refusal(self) -> str | None:
        if self.raw_document_count != 1:
            return "canonical_raw_document_missing_or_ambiguous"
        if self.instrument_id is None:
            return "instrument_mapping_missing"
        if not self.current_security_eligible:
            return "current_security_scope_ineligible"
        if len(self.series_ids) != 1:
            return "research_series_missing_or_ambiguous"
        if self.series_adjustment_bases != ("split_adjusted",):
            return "research_series_adjustment_basis_unexpected"
        return None


_SOURCE_EVENTS_SQL: LiteralString = """
WITH reporter_events AS (
    SELECT DISTINCT b.accession_number,
           b.issuer_cik,
           coalesce(
               b.reporter_cik,
               nullif(lower(regexp_replace(trim(b.reporter_name), '\\s+', ' ', 'g')), '')
           ) AS reporter_identity,
           m.filed_at::date AS public_filing_date,
           b.submission_type,
           b.status
    FROM blockholder_filings b
    JOIN sec_filing_manifest m USING (accession_number)
    WHERE b.submission_type IN (
        'SCHEDULE 13D', 'SCHEDULE 13D/A',
        'SCHEDULE 13G', 'SCHEDULE 13G/A'
    )
), reporter_history AS (
    SELECT r.*,
           coalesce(bool_or(status = 'active') OVER (
               PARTITION BY issuer_cik, reporter_identity
               ORDER BY public_filing_date
               RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP
           ), false) AS prior_active,
           coalesce(bool_or(status = 'passive') OVER (
               PARTITION BY issuer_cik, reporter_identity
               ORDER BY public_filing_date
               RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP
           ), false) AS prior_passive,
           count(*) OVER (
               PARTITION BY issuer_cik, reporter_identity, public_filing_date
           ) > 1 AS same_public_date_peer
    FROM reporter_events r
), initial_accessions AS (
    SELECT b.accession_number,
           min(b.issuer_cik) AS issuer_cik,
           max(b.instrument_id) FILTER (WHERE b.instrument_id IS NOT NULL) AS instrument_id,
           m.filed_at::date AS public_filing_date,
           max(b.percent_of_class) AS maximum_percent_of_class
    FROM blockholder_filings b
    JOIN sec_filing_manifest m USING (accession_number)
    WHERE b.submission_type = 'SCHEDULE 13D'
      AND m.filed_at::date BETWEEN %(first_source_date)s AND %(last_complete_filing_date)s
    GROUP BY b.accession_number, m.filed_at::date
), classified AS (
    SELECT a.*,
           bool_or(h.prior_active) AS prior_active,
           bool_or(h.prior_passive) AS prior_passive,
           bool_or(h.same_public_date_peer) AS same_public_date_peer,
           bool_and(h.reporter_identity IS NOT NULL) AS reporter_identity_complete
    FROM initial_accessions a
    JOIN reporter_history h USING (accession_number)
    WHERE h.submission_type = 'SCHEDULE 13D'
    GROUP BY a.accession_number, a.issuer_cik, a.instrument_id,
             a.public_filing_date, a.maximum_percent_of_class
)
SELECT c.accession_number, c.issuer_cik, c.instrument_id,
       c.public_filing_date, c.maximum_percent_of_class,
       c.prior_active, c.prior_passive, c.same_public_date_peer,
       c.reporter_identity_complete,
       coalesce(i.is_tradable, false)
           AND i.instrument_type_id = 5
           AND i.exchange IN ('4', '5') AS current_security_eligible,
       coalesce(array_agg(s.series_id ORDER BY s.series_id)
           FILTER (WHERE s.series_id IS NOT NULL), '{}') AS series_ids,
       coalesce(array_agg(s.adjustment_basis ORDER BY s.series_id)
           FILTER (WHERE s.series_id IS NOT NULL), '{}') AS series_adjustment_bases
FROM classified c
LEFT JOIN instruments i ON i.instrument_id = c.instrument_id
LEFT JOIN research_price_series s
  ON s.instrument_id = c.instrument_id AND s.vendor = %(research_vendor)s
GROUP BY c.accession_number, c.issuer_cik, c.instrument_id,
         c.public_filing_date, c.maximum_percent_of_class,
         c.prior_active, c.prior_passive, c.same_public_date_peer,
         c.reporter_identity_complete, i.is_tradable,
         i.instrument_type_id, i.exchange
ORDER BY c.public_filing_date, c.accession_number
"""


def load_source_events(conn: psycopg.Connection[Any]) -> tuple[SourceEvent, ...]:
    """Build the public-information population without loading a price bar."""

    rows = conn.execute(
        _SOURCE_EVENTS_SQL,
        {
            "research_vendor": RESEARCH_VENDOR,
            "first_source_date": FIRST_SOURCE_DATE,
            "last_complete_filing_date": LAST_COMPLETE_FILING_DATE,
        },
    ).fetchall()
    return tuple(
        SourceEvent(
            accession_number=str(row[0]),
            issuer_cik=str(row[1]),
            instrument_id=None if row[2] is None else int(row[2]),
            public_filing_date=row[3],
            maximum_percent_of_class=None if row[4] is None else Decimal(row[4]),
            prior_active=bool(row[5]),
            prior_passive=bool(row[6]),
            same_public_date_peer=bool(row[7]),
            reporter_identity_complete=bool(row[8]),
            current_security_eligible=bool(row[9]),
            series_ids=tuple(int(value) for value in row[10]),
            series_adjustment_bases=tuple(str(value) for value in row[11]),
        )
        for row in rows
    )


_INITIAL_13G_SOURCE_SQL: LiteralString = """
WITH initial_accessions AS (
    SELECT b.accession_number,
           min(b.issuer_cik) AS issuer_cik,
           max(b.instrument_id) FILTER (WHERE b.instrument_id IS NOT NULL) AS instrument_id,
           m.filed_at::date AS public_filing_date
    FROM blockholder_filings b
    JOIN sec_filing_manifest m USING (accession_number)
    WHERE b.submission_type = 'SCHEDULE 13G'
      AND m.filed_at::date BETWEEN %(first_source_date)s AND %(last_complete_filing_date)s
    GROUP BY b.accession_number, m.filed_at::date
), raw_flags AS (
    SELECT r.accession_number,
           count(*) AS raw_document_count,
           bool_or(r.payload ~* '<([[:alnum:]_]+:)?designateRulePursuantThisScheduleFiled[^>]*>'
               '[[:space:]]*Rule[[:space:]]+13d-1\\(b\\)[[:space:]]*'
               '</([[:alnum:]_]+:)?designateRulePursuantThisScheduleFiled>') AS rule_1b,
           bool_or(r.payload ~* '<([[:alnum:]_]+:)?designateRulePursuantThisScheduleFiled[^>]*>'
               '[[:space:]]*Rule[[:space:]]+13d-1\\(c\\)[[:space:]]*'
               '</([[:alnum:]_]+:)?designateRulePursuantThisScheduleFiled>') AS rule_1c
    FROM filing_raw_documents r
    WHERE r.document_kind = 'primary_doc_13dg'
    GROUP BY r.accession_number
)
SELECT a.accession_number, a.issuer_cik, a.instrument_id,
       a.public_filing_date,
       CASE
         WHEN coalesce(raw.rule_1b, false) AND coalesce(raw.rule_1c, false) THEN 'both'
         WHEN coalesce(raw.rule_1b, false) THEN '1b'
         WHEN coalesce(raw.rule_1c, false) THEN '1c'
         ELSE 'unknown'
       END AS rule,
       coalesce(raw.raw_document_count, 0) AS raw_document_count,
       coalesce(i.is_tradable, false)
           AND i.instrument_type_id = 5
           AND i.exchange IN ('4', '5') AS current_security_eligible,
       coalesce(array_agg(s.series_id ORDER BY s.series_id)
           FILTER (WHERE s.series_id IS NOT NULL), '{}') AS series_ids,
       coalesce(array_agg(s.adjustment_basis ORDER BY s.series_id)
           FILTER (WHERE s.series_id IS NOT NULL), '{}') AS series_adjustment_bases
FROM initial_accessions a
LEFT JOIN raw_flags raw USING (accession_number)
LEFT JOIN instruments i ON i.instrument_id = a.instrument_id
LEFT JOIN research_price_series s
  ON s.instrument_id = a.instrument_id AND s.vendor = %(research_vendor)s
GROUP BY a.accession_number, a.issuer_cik, a.instrument_id,
         a.public_filing_date, raw.rule_1b, raw.rule_1c,
         raw.raw_document_count, i.is_tradable, i.instrument_type_id, i.exchange
ORDER BY a.public_filing_date, a.accession_number
"""


def load_initial_13g_source_events(conn: psycopg.Connection[Any]) -> tuple[Initial13GSourceEvent, ...]:
    """Build the passive-filing challenger population without loading prices."""

    rows = conn.execute(
        _INITIAL_13G_SOURCE_SQL,
        {
            "research_vendor": RESEARCH_VENDOR,
            "first_source_date": FIRST_SOURCE_DATE,
            "last_complete_filing_date": LAST_COMPLETE_FILING_DATE,
        },
    ).fetchall()
    return tuple(
        Initial13GSourceEvent(
            accession_number=str(row[0]),
            issuer_cik=str(row[1]),
            instrument_id=None if row[2] is None else int(row[2]),
            public_filing_date=row[3],
            rule=row[4],
            raw_document_count=int(row[5]),
            current_security_eligible=bool(row[6]),
            series_ids=tuple(int(value) for value in row[7]),
            series_adjustment_bases=tuple(str(value) for value in row[8]),
        )
        for row in rows
    )


_CREATE_TEMP_SESSIONS: LiteralString = """
CREATE TEMP TABLE schedule13d_trial_sessions (
    event_index SMALLINT NOT NULL,
    session_ordinal SMALLINT NOT NULL CHECK (session_ordinal BETWEEN 1 AND 70),
    series_id BIGINT NOT NULL,
    bar_date DATE NOT NULL,
    PRIMARY KEY (event_index, session_ordinal)
) ON COMMIT PRESERVE ROWS
"""


def prepare_price_window_workspace(conn: psycopg.Connection[Any]) -> None:
    """Create the connection-local workspace before the read-only outcome transaction.

    PostgreSQL permits DML on an existing temporary table in a read-only
    transaction but forbids CREATE/DROP/TRUNCATE.  Requiring an idle fresh
    connection keeps the commit boundary explicit and lets all durable-table
    outcome reads run under one read-only snapshot.
    """

    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        raise OutcomeGateRefusal("price workspace preparation requires an idle connection")
    existing = conn.execute("SELECT to_regclass('pg_temp.schedule13d_trial_sessions')").fetchone()
    if existing is None or existing[0] is None:
        conn.execute(_CREATE_TEMP_SESSIONS)
    conn.commit()


_PRICE_WINDOWS_SQL: LiteralString = """
WITH joined AS (
    SELECT e.event_index, e.session_ordinal, e.bar_date,
           d.open, d.high, d.low, d.close, d.volume, d.adj_close,
           cov.series_id IS NOT NULL AS quarantine_covered,
           coalesce(q.return_usable, true) AS return_usable,
           spy.close AS market_close
    FROM schedule13d_trial_sessions e
    LEFT JOIN research_price_daily d
      ON d.series_id = e.series_id AND d.bar_date = e.bar_date
    LEFT JOIN research_price_quarantine_coverage cov
      ON cov.series_id = e.series_id
     AND cov.rule_set_version = %(quarantine_version)s
     AND e.bar_date BETWEEN cov.first_bar AND cov.last_bar
    LEFT JOIN research_bar_quarantine q
      ON q.series_id = e.series_id
     AND q.bar_date = e.bar_date
     AND q.rule_set_version = %(quarantine_version)s
    LEFT JOIN research_price_daily spy
      ON spy.series_id = 7713 AND spy.bar_date = e.bar_date
), aggregate AS (
    SELECT event_index,
           count(*) FILTER (WHERE close IS NOT NULL) AS stock_bars_present,
           count(*) FILTER (
               WHERE market_close > 0 AND market_close <> 'NaN'::numeric
           ) AS market_bars_present,
           count(*) FILTER (
               WHERE open > 0 AND open <> 'NaN'::numeric
                 AND high > 0 AND high <> 'NaN'::numeric
                 AND low > 0 AND low <> 'NaN'::numeric
                 AND close > 0 AND close <> 'NaN'::numeric
                 AND volume > 0
           ) AS positive_ohlcv_bars,
           count(*) FILTER (
               WHERE adj_close > 0 AND adj_close <> 'NaN'::numeric
                 AND close > 0 AND close <> 'NaN'::numeric
           ) AS positive_adjustment_bars,
           count(*) FILTER (WHERE quarantine_covered) AS quarantine_covered_bars,
           coalesce(bool_and(return_usable) FILTER (WHERE close IS NOT NULL), false) AS return_usable,
           max(open) FILTER (WHERE session_ordinal = 61) AS entry_open,
           max(close) FILTER (WHERE session_ordinal = 61) AS entry_close,
           max(adj_close) FILTER (WHERE session_ordinal = 61) AS entry_adj_close,
           max(close) FILTER (WHERE session_ordinal = 70) AS exit_close,
           max(adj_close) FILTER (WHERE session_ordinal = 70) AS exit_adj_close,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY close * volume)
               FILTER (WHERE session_ordinal BETWEEN 41 AND 60) AS trailing_median_dollar_volume,
           (max(close) FILTER (WHERE session_ordinal = 60)
               / nullif(max(close) FILTER (WHERE session_ordinal = 40), 0) - 1) * 100
               AS prior_20_stock_return_pct,
           (max(market_close) FILTER (WHERE session_ordinal = 60)
               / nullif(max(market_close) FILTER (WHERE session_ordinal = 40), 0) - 1) * 100
               AS prior_20_market_return_pct,
           (max(market_close) FILTER (WHERE session_ordinal = 70)
               / nullif(max(market_close) FILTER (WHERE session_ordinal = 61), 0) - 1) * 100
               AS holding_market_return_pct
    FROM joined
    GROUP BY event_index
)
SELECT event_index, stock_bars_present, market_bars_present,
       positive_ohlcv_bars, positive_adjustment_bars,
       quarantine_covered_bars, return_usable,
       entry_open, entry_close, entry_adj_close, exit_close, exit_adj_close,
       trailing_median_dollar_volume, prior_20_stock_return_pct,
       prior_20_market_return_pct, holding_market_return_pct
FROM aggregate
ORDER BY event_index
"""


def _validate_gate(gate: OutcomeGate) -> None:
    if (
        gate.contract_sha256 != EXPECTED_SHA256
        or gate.trial_register_version != TRIAL_REGISTER.version
        or gate.trial_id != TRIAL_ID
        or TRIAL_ID not in TRIAL_REGISTER.trial_ids
    ):
        raise OutcomeGateRefusal("invalid or stale outcome gate")


def _validate_gate_provenance(conn: psycopg.Connection[Any], gate: OutcomeGate) -> None:
    """#2614 — re-check #2599's declaration and the access row, from the tables.

    ⚠ THE POINT IS THAT IT DOES NOT TRUST THE GATE. ``OutcomeGate`` is a plain
    frozen dataclass, so ``_validate_gate`` above proves only that a caller can
    copy three constants out of this module. Loading the declaration and the
    access row BY ID is what a caller cannot fake, because it cannot write a row
    that predates its own look.

    ⚠ Read-only, and it must stay that way: every caller is inside
    ``evaluate_historical_falsification``'s ``REPEATABLE READ READ ONLY``
    transaction, where an INSERT fails. The matching write happened in
    ``require_outcome_gate`` and was committed by the runner.
    """
    verify_outcome_access_provenance(
        cast(psycopg.Connection[tuple], conn),
        strategy_id=STRATEGY_ID,
        strategy_version=STRATEGY_VERSION,
        declaration_id=gate.declaration_id,
        access_id=gate.access_id,
    )


def _verify_market_series(conn: psycopg.Connection[Any]) -> None:
    row = conn.execute(
        """
        SELECT vendor, vendor_symbol, comparator_snapshot_id
        FROM research_price_series WHERE series_id = 7713
        """
    ).fetchone()
    expected = ("etoro/etoro-comparators-2026-07-08-v1", "SPY", "etoro-comparators-2026-07-08-v1")
    if row is None or tuple(row) != expected:
        raise OutcomeGateRefusal("frozen SPY market-series identity moved")


@dataclass(frozen=True)
class WindowRequest:
    event: SourceEvent | Initial13GSourceEvent
    sessions: tuple[date, ...]
    population: Literal["primary", "unfiltered", "13g", "random"]


def _load_requested_price_windows(
    conn: psycopg.Connection[Any], gate: OutcomeGate, requests: Sequence[WindowRequest]
) -> tuple[PriceWindow, ...]:
    _validate_gate(gate)
    # ⚠ #2614 — BEFORE the market-series check and before any bar is read. This
    # is the chokepoint all four populations (primary, unfiltered, 13g, random)
    # funnel through, which is the same placement argument #2599 makes for
    # `record_holdout_access`: one check covers every door, and no future loader
    # has to remember a convention.
    _validate_gate_provenance(conn, gate)
    _verify_market_series(conn)
    if len(requests) > 32_767:
        raise OutcomeGateRefusal("price-window request exceeds frozen SMALLINT event bound")
    prepared = conn.execute("SELECT to_regclass('pg_temp.schedule13d_trial_sessions')").fetchone()
    if prepared is None or prepared[0] is None:
        raise OutcomeGateRefusal("connection-local price workspace was not prepared")
    conn.execute("DELETE FROM schedule13d_trial_sessions")
    with conn.cursor() as cursor:
        with cursor.copy(
            "COPY schedule13d_trial_sessions (event_index, session_ordinal, series_id, bar_date) FROM STDIN"
        ) as copy:
            for event_index, request in enumerate(requests):
                if len(request.sessions) != 70:
                    raise OutcomeGateRefusal("every price-window request must contain exactly 70 sessions")
                for ordinal, session_date in enumerate(request.sessions, start=1):
                    copy.write_row((event_index, ordinal, request.event.series_ids[0], session_date))
    rows = conn.execute(
        _PRICE_WINDOWS_SQL,
        {"quarantine_version": QUARANTINE_RULE_SET_VERSION},
    ).fetchall()
    by_index = {int(row[0]): row for row in rows}
    windows: list[PriceWindow] = []
    for event_index, request in enumerate(requests):
        row = by_index[event_index]
        sessions = request.sessions
        decimal_values = [None if value is None else Decimal(value) for value in row[7:16]]
        windows.append(
            PriceWindow(
                event=request.event,
                entry_date=sessions[60],
                exit_date=sessions[69],
                stock_bars_present=int(row[1]),
                market_bars_present=int(row[2]),
                positive_ohlcv_bars=int(row[3]),
                positive_adjustment_bars=int(row[4]),
                quarantine_covered_bars=int(row[5]),
                return_usable=bool(row[6]),
                entry_open=decimal_values[0],
                entry_close=decimal_values[1],
                entry_adj_close=decimal_values[2],
                exit_close=decimal_values[3],
                exit_adj_close=decimal_values[4],
                trailing_median_dollar_volume=decimal_values[5],
                prior_20_stock_return_pct=decimal_values[6],
                prior_20_market_return_pct=decimal_values[7],
                holding_market_return_pct=decimal_values[8],
                population=request.population,
            )
        )
    return tuple(windows)


def load_price_windows(
    conn: psycopg.Connection[Any],
    gate: OutcomeGate,
    events: Sequence[SourceEvent],
    *,
    population: Literal["primary", "unfiltered"] = "primary",
) -> tuple[PriceWindow, ...]:
    """Read treatment or unfiltered-13D windows through identical math."""

    if population == "primary":
        selected = [event for event in events if event.primary_source_refusal is None]
    elif population == "unfiltered":
        selected = [event for event in events if event.unfiltered_source_refusal is None]
    else:
        raise ValueError("unsupported Schedule 13D population")
    requests = tuple(WindowRequest(event, required_event_sessions(event), population) for event in selected)
    return _load_requested_price_windows(conn, gate, requests)


def load_initial_13g_price_windows(
    conn: psycopg.Connection[Any], gate: OutcomeGate, events: Sequence[Initial13GSourceEvent]
) -> tuple[PriceWindow, ...]:
    """Read eligible 13G challenger windows through the shared evaluator."""

    selected = [event for event in events if event.source_refusal is None]
    requests = tuple(WindowRequest(event, required_event_sessions(event), "13g") for event in selected)
    return _load_requested_price_windows(conn, gate, requests)


class OutcomeGateRefusal(RuntimeError):
    """The sealed outcome boundary was not explicitly and correctly opened."""


@dataclass(frozen=True)
class OutcomeGate:
    contract_sha256: str
    trial_register_version: str
    trial_id: str
    #: #2614 — the frozen #2599 declaration this look is authorised by, and the
    #: committed ``strategy_holdout_accesses`` row recording it. ⚠ NEITHER IS
    #: TRUSTED AS CARRIED. This dataclass is constructible by anyone; both ids
    #: are re-loaded from their tables on every price-window read (see
    #: ``_validate_gate_provenance``). They are here as a lookup key, not as
    #: evidence.
    declaration_id: int
    access_id: int


@dataclass(frozen=True)
class PriceWindow:
    event: SourceEvent | Initial13GSourceEvent
    entry_date: date
    exit_date: date
    stock_bars_present: int
    market_bars_present: int
    positive_ohlcv_bars: int
    positive_adjustment_bars: int
    quarantine_covered_bars: int
    return_usable: bool
    entry_open: Decimal | None
    entry_close: Decimal | None
    entry_adj_close: Decimal | None
    exit_close: Decimal | None
    exit_adj_close: Decimal | None
    trailing_median_dollar_volume: Decimal | None
    prior_20_stock_return_pct: Decimal | None
    prior_20_market_return_pct: Decimal | None
    holding_market_return_pct: Decimal | None
    population: Literal["primary", "unfiltered", "13g", "random"] = "primary"

    @property
    def outcome_refusal(self) -> str | None:
        if isinstance(self.event, Initial13GSourceEvent):
            source_refusal = self.event.source_refusal
        elif self.population == "unfiltered":
            source_refusal = self.event.unfiltered_source_refusal
        else:
            source_refusal = self.event.primary_source_refusal
        if source_refusal is not None:
            return source_refusal
        if self.stock_bars_present != 70:
            return "exact_stock_session_missing"
        if self.market_bars_present != 70:
            return "exact_spy_session_missing"
        if self.quarantine_covered_bars != 70:
            return "quarantine_coverage_incomplete"
        if not self.return_usable:
            return "quarantined_return_window"
        if self.positive_ohlcv_bars != 70:
            return "missing_or_nonpositive_ohlcv"
        if self.positive_adjustment_bars != 70:
            return "corporate_action_adjustment_missing_or_nonpositive"
        if self.entry_open is None or self.entry_open < Decimal("5"):
            return "entry_price_below_five_dollars"
        if self.trailing_median_dollar_volume is None or self.trailing_median_dollar_volume < Decimal("10000000"):
            return "trailing_median_dollar_volume_below_floor"
        return None


def accepted_window_return_pct(window: PriceWindow) -> Decimal:
    """Return the frozen net result, refusing every ineligible window."""

    refusal = window.outcome_refusal
    if refusal is not None:
        raise ValueError(f"price window refused: {refusal}")
    required = (
        window.entry_open,
        window.entry_close,
        window.entry_adj_close,
        window.exit_close,
        window.exit_adj_close,
    )
    if any(value is None for value in required):
        raise ValueError("accepted price window is missing a return input")
    return total_return_pct(
        entry_open=cast(Decimal, window.entry_open),
        entry_close=cast(Decimal, window.entry_close),
        entry_adj_close=cast(Decimal, window.entry_adj_close),
        exit_close=cast(Decimal, window.exit_close),
        exit_adj_close=cast(Decimal, window.exit_adj_close),
    )


def window_match_features(window: PriceWindow) -> MatchFeatures:
    """Build the exact pre-outcome matching cell for an accepted window."""

    refusal = window.outcome_refusal
    if refusal is not None:
        raise ValueError(f"price window refused: {refusal}")
    if window.entry_open is None or window.trailing_median_dollar_volume is None:
        raise ValueError("accepted price window is missing matching features")
    if window.prior_20_market_return_pct is None:
        raise ValueError("accepted price window is missing prior market context")
    return MatchFeatures(
        accession_number=window.event.accession_number,
        issuer_cik=window.event.issuer_cik,
        filing_date=window.event.public_filing_date,
        entry_date=window.entry_date,
        entry_price=window.entry_open,
        trailing_median_dollar_volume=window.trailing_median_dollar_volume,
        prior_20_market_return_pct=window.prior_20_market_return_pct,
        rule=window.event.rule if isinstance(window.event, Initial13GSourceEvent) else None,
    )


def treatment_event_outcome(window: PriceWindow, *, sector: str | None = None) -> EventOutcome:
    """Convert only an accepted 13D treatment window to frozen statistics."""

    if not isinstance(window.event, SourceEvent) or window.population != "primary":
        raise ValueError("event outcomes are defined only for the clean primary population")
    return EventOutcome(
        accession_number=window.event.accession_number,
        issuer_cik=window.event.issuer_cik,
        entry_date=window.entry_date,
        exit_date=window.exit_date,
        net_return_pct=float(accepted_window_return_pct(window)),
        maximum_percent_of_class=(
            None if window.event.maximum_percent_of_class is None else float(window.event.maximum_percent_of_class)
        ),
        sector=sector,
    )


def require_outcome_gate_preconditions(*, acknowledgement: str | None, contract_path: Path) -> str:
    """The three checks that need no database. Returns the verified digest.

    ⚠ SPLIT OUT BY #2614 SO THE CHEAP REFUSALS STAY CHEAP. Adding the declaration
    check gave ``require_outcome_gate`` a connection, and folding everything into
    it would mean a wrong acknowledgement opens a database connection before
    being told no. A refusal that costs a connection is a refusal somebody
    eventually routes around.
    """

    if acknowledgement != ACKNOWLEDGEMENT:
        raise OutcomeGateRefusal(
            "sealed outcomes remain closed; pass the exact acknowledgement only after evaluator review"
        )
    _contract, digest = load_and_verify(contract_path)
    if digest != EXPECTED_SHA256:
        raise OutcomeGateRefusal("contract digest does not match the reviewed preregistration")
    if TRIAL_ID not in TRIAL_REGISTER.trial_ids:
        raise OutcomeGateRefusal(
            f"{TRIAL_ID} is absent from {TRIAL_REGISTER.version}; declare the price-data search before reading outcomes"
        )
    return digest


def require_outcome_gate(
    conn: psycopg.Connection[Any], *, acknowledgement: str | None, contract_path: Path
) -> OutcomeGate:
    """Refuse unless the contract, the trial and #2599's declaration are all exact.

    ⚠⚠ #2614 — THIS FUNCTION TAKES A CONNECTION BECAUSE THE GATE IT DESCRIBED
    DID NOT BIND. #2599 put the declaration check at the ledger chokepoint, on
    the correct observation that all three hold-out doors funnel through it. The
    unchecked premise was that opening an outcome always goes through the ledger.
    C-4 computes its statistics from raw price windows and emits a signed
    artifact — it stores no result row, so there was no ledger call to intercept,
    and the register entry that makes this trial runnable would otherwise have
    unlocked an entirely ungated path.

    ⚠ IT DOES NOT FREEZE THE DECLARATION, AND MUST NOT. Freezing one here would
    make it a description of a run already under way rather than a prediction of
    it, which is the exact defect Codex killed in #2599's first draft (*"a caller
    can construct a favourable declaration after seeing/reading outcomes"*), just
    with the constructor moved. Freezing is
    ``scripts/freeze_2582_schedule13d_declaration.py``, run separately and
    earlier.

    ⚠ THE CALLER OWNS THE COMMIT. ``require_outcome_access`` writes in this
    transaction and does not commit it. The caller must ``conn.commit()`` before
    evaluating — not only so the look stays logged if the evaluation dies, but
    because ``evaluate_historical_falsification`` opens with
    ``SET TRANSACTION ISOLATION LEVEL ... READ ONLY``, which is only valid as the
    first statement of a transaction and fails outright while this INSERT is
    still open.
    """

    digest = require_outcome_gate_preconditions(acknowledgement=acknowledgement, contract_path=contract_path)
    frozen = load_preregistration(conn, STRATEGY_ID, STRATEGY_VERSION)
    if frozen is None:
        raise PreregDeclarationRefused(STRATEGY_ID, STRATEGY_VERSION, ("preregistration_not_frozen",))
    # ⚠ `read`, with a NULL result_version, and not `evaluate`. sql/264's
    # `strategy_holdout_accesses_evaluate_names_a_result` requires an `evaluate`
    # to name the result row it authorises, and C-4 never writes one — so
    # `evaluate` here would either be refused or would stand for a row that
    # never arrives. A `read` is what this is: the withheld side being LOOKED AT.
    access_id = require_outcome_access(
        conn,
        HoldoutAccess(
            strategy_id=STRATEGY_ID,
            strategy_version=STRATEGY_VERSION,
            result_version=None,
            access_kind="read",
            accessed_by="scripts/run_2582_schedule13d_outcomes.py",
            purpose=f"open the sealed #2582 Schedule 13D historical falsification under {TRIAL_ID}",
        ),
    )
    return OutcomeGate(digest, TRIAL_REGISTER.version, TRIAL_ID, frozen.declaration_id, access_id)


def next_regular_session_strictly_after(filing_date: date) -> date:
    """The first NYSE session after the filing civil date; never same-day."""

    candidate = filing_date + timedelta(days=1)
    while us_market_status(candidate) == "closed":
        candidate += timedelta(days=1)
    return candidate


def nth_regular_session(first_session: date, n: int) -> date:
    """Return session ``n`` with ``first_session`` counted as session one."""

    if n < 1:
        raise ValueError("n must be positive")
    if us_market_status(first_session) == "closed":
        raise ValueError("first_session is not a regular trading session")
    candidate = first_session
    found = 1
    while found < n:
        candidate += timedelta(days=1)
        if us_market_status(candidate) != "closed":
            found += 1
    return candidate


def regular_sessions_ending_before(anchor: date, count: int) -> tuple[date, ...]:
    """Return exactly ``count`` NYSE sessions before ``anchor``, oldest first."""

    if count < 1:
        raise ValueError("count must be positive")
    found: list[date] = []
    candidate = anchor - timedelta(days=1)
    while len(found) < count:
        if us_market_status(candidate) != "closed":
            found.append(candidate)
        candidate -= timedelta(days=1)
    return tuple(reversed(found))


def required_event_sessions(event: SourceEvent | Initial13GSourceEvent) -> tuple[date, ...]:
    """Sixty formation sessions followed by the exact ten-session position."""

    entry = next_regular_session_strictly_after(event.public_filing_date)
    return required_sessions_for_entry(entry)


def required_sessions_for_entry(entry: date) -> tuple[date, ...]:
    """Sixty prior sessions and ten holding sessions for an exact entry."""

    if us_market_status(entry) == "closed":
        raise ValueError("entry must be a regular trading session")
    prior = regular_sessions_ending_before(entry, 60)
    holding: list[date] = []
    candidate = entry
    while len(holding) < 10:
        if us_market_status(candidate) != "closed":
            holding.append(candidate)
        candidate += timedelta(days=1)
    return prior + tuple(holding)


_ALL_13D_PUBLIC_DATES_SQL: LiteralString = """
SELECT max(b.instrument_id) FILTER (WHERE b.instrument_id IS NOT NULL) AS instrument_id,
       m.filed_at::date AS public_filing_date
FROM blockholder_filings b
JOIN sec_filing_manifest m USING (accession_number)
WHERE b.submission_type IN ('SCHEDULE 13D', 'SCHEDULE 13D/A')
GROUP BY b.accession_number, m.filed_at::date
HAVING max(b.instrument_id) FILTER (WHERE b.instrument_id IS NOT NULL) IS NOT NULL
ORDER BY instrument_id, public_filing_date
"""


def load_all_13d_public_dates(conn: psycopg.Connection[Any]) -> dict[int, tuple[date, ...]]:
    """Load outcome-free event dates used only to construct exclusion halos."""

    grouped: defaultdict[int, list[date]] = defaultdict(list)
    for instrument_id, filing_date in conn.execute(_ALL_13D_PUBLIC_DATES_SQL).fetchall():
        grouped[int(instrument_id)].append(filing_date)
    return {instrument_id: tuple(values) for instrument_id, values in grouped.items()}


def _regular_sessions_in_month(year: int, month: int) -> tuple[date, ...]:
    sessions: list[date] = []
    candidate = date(year, month, 1)
    while candidate.month == month:
        if us_market_status(candidate) != "closed":
            sessions.append(candidate)
        candidate += timedelta(days=1)
    return tuple(sessions)


def _event_entry_halo(public_filing_date: date) -> set[date]:
    entry = next_regular_session_strictly_after(public_filing_date)
    before = regular_sessions_ending_before(entry, 10)
    after = required_sessions_for_entry(entry)[60:]
    final = nth_regular_session(entry, 11)
    return set(before) | set(after) | {final}


def build_random_time_requests(
    treatments: Sequence[SourceEvent],
    all_13d_dates: Mapping[int, Sequence[date]],
) -> tuple[WindowRequest, ...]:
    """Build same-month candidates outside every same-instrument 13D halo."""

    requests: list[WindowRequest] = []
    for treatment in treatments:
        if treatment.primary_source_refusal is not None or treatment.instrument_id is None:
            continue
        treatment_entry = next_regular_session_strictly_after(treatment.public_filing_date)
        prohibited: set[date] = set()
        for filing_date in all_13d_dates.get(treatment.instrument_id, ()):
            prohibited.update(_event_entry_halo(filing_date))
        for candidate in _regular_sessions_in_month(treatment_entry.year, treatment_entry.month):
            if candidate not in prohibited:
                requests.append(WindowRequest(treatment, required_sessions_for_entry(candidate), "random"))
    return tuple(requests)


def load_random_time_price_windows(
    conn: psycopg.Connection[Any], gate: OutcomeGate, treatments: Sequence[SourceEvent]
) -> tuple[PriceWindow, ...]:
    """Return one seeded eligible same-month non-event window per treatment."""

    _validate_gate(gate)
    requests = build_random_time_requests(treatments, load_all_13d_public_dates(conn))
    evaluated = _load_requested_price_windows(conn, gate, requests)
    accepted_by_treatment: defaultdict[str, list[date]] = defaultdict(list)
    by_identity: dict[tuple[str, date], PriceWindow] = {}
    for window in evaluated:
        if window.outcome_refusal is None:
            accession = window.event.accession_number
            accepted_by_treatment[accession].append(window.entry_date)
            by_identity[(accession, window.entry_date)] = window
    selected = select_random_time_sessions(accepted_by_treatment, {})
    return tuple(by_identity[(accession, selected[accession])] for accession in sorted(selected))


def total_return_pct(
    *,
    entry_open: Decimal,
    entry_close: Decimal,
    entry_adj_close: Decimal,
    exit_close: Decimal,
    exit_adj_close: Decimal,
    adverse_cost_bps: int = 50,
) -> Decimal:
    """Causal open-to-close total return using the vendor adjustment factors.

    ``adj_close / close`` is observed at entry and exit.  A split or dividend
    between them changes the factor ratio.  Missing/non-positive inputs refuse;
    silently falling back to raw close would corrupt corporate-action cases.
    """

    values = (entry_open, entry_close, entry_adj_close, exit_close, exit_adj_close)
    if any(not value.is_finite() or value <= 0 for value in values):
        raise ValueError("return inputs must all be finite and positive")
    if adverse_cost_bps < 0:
        raise ValueError("adverse_cost_bps cannot be negative")
    entry_factor = entry_adj_close / entry_close
    exit_factor = exit_adj_close / exit_close
    gross = (exit_close / entry_open) * (exit_factor / entry_factor) - Decimal(1)
    return (gross - Decimal(adverse_cost_bps) / Decimal(10_000)) * Decimal(100)


def bucket(value: Decimal, edges: tuple[Decimal, ...]) -> int:
    """Stable half-open bucket index: values equal to an edge enter its right cell."""

    if not value.is_finite():
        raise ValueError("bucket value must be finite")
    return sum(value >= edge for edge in edges)


def match_tie_break(treatment_accession: str, challenger_accession: str, *, seed: int = 2582) -> str:
    payload = f"{treatment_accession}\x1f{challenger_accession}\x1f{seed}".encode()
    return hashlib.sha256(payload).hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--acknowledgement")
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path("docs/proposals/ta/contracts/schedule13d-public-catalyst-v1.json"),
    )
    args = parser.parse_args(argv)
    # ⚠ #2614 — REFUSES BEFORE THE GATE IS BUILT, NOT AFTER. This entry point is
    # vestigial: it predates `scripts/run_2582_schedule13d_outcomes.py` and its
    # old comment ("does not yet contain the reviewed database evaluator") stopped
    # being true when `schedule13d_orchestrator` landed at `55c75dce`. Building a
    # gate here would now COMMIT AN ACCESS ROW for a look this function never
    # takes, putting a fabricated look in criterion 5's audit log. So the
    # preconditions run — a wrong acknowledgement is still named as such — and
    # then it refuses without touching the ledger.
    require_outcome_gate_preconditions(acknowledgement=args.acknowledgement, contract_path=args.contract)
    raise OutcomeGateRefusal(
        "preconditions satisfied but this is not the evaluator entry point and no access was recorded; "
        "run scripts/run_2582_schedule13d_outcomes.py, which owns the access record and its commit: "
        + json.dumps({"trial_id": TRIAL_ID, "trial_register_version": TRIAL_REGISTER.version}, sort_keys=True)
    )


if __name__ == "__main__":
    raise SystemExit(main())
