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
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, LiteralString

import psycopg

from app.services.market_calendar import us_market_status
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.trial_register import TRIAL_REGISTER
from scripts.verify_2582_schedule13d_preregistration import EXPECTED_SHA256, load_and_verify

TRIAL_ID: Final = "c4-schedule13d-public-catalyst-v1"
ACKNOWLEDGEMENT: Final = "OPEN-2582-SEALED-OUTCOMES"
RESEARCH_VENDOR: Final = "paperswithbacktest/Stocks-Daily-Price"


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
    def primary_source_refusal(self) -> str | None:
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
        if self.prior_active:
            return "prior_active_chain"
        if self.prior_passive:
            return "prior_passive_chain"
        if self.same_public_date_peer:
            return "same_public_date_chain_ambiguous"
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

    rows = conn.execute(_SOURCE_EVENTS_SQL, {"research_vendor": RESEARCH_VENDOR}).fetchall()
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


_CREATE_TEMP_SESSIONS: LiteralString = """
CREATE TEMP TABLE schedule13d_trial_sessions (
    event_index SMALLINT NOT NULL,
    session_ordinal SMALLINT NOT NULL CHECK (session_ordinal BETWEEN 1 AND 70),
    series_id BIGINT NOT NULL,
    bar_date DATE NOT NULL,
    PRIMARY KEY (event_index, session_ordinal)
) ON COMMIT DROP
"""

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


def load_price_windows(
    conn: psycopg.Connection[Any], gate: OutcomeGate, events: Sequence[SourceEvent]
) -> tuple[PriceWindow, ...]:
    """Read exact price windows only after the reviewed sealed gate opens."""

    _validate_gate(gate)
    _verify_market_series(conn)
    selected = [event for event in events if event.primary_source_refusal is None]
    conn.execute("DROP TABLE IF EXISTS schedule13d_trial_sessions")
    conn.execute(_CREATE_TEMP_SESSIONS)
    with conn.cursor() as cursor:
        with cursor.copy(
            "COPY schedule13d_trial_sessions (event_index, session_ordinal, series_id, bar_date) FROM STDIN"
        ) as copy:
            for event_index, event in enumerate(selected):
                for ordinal, session_date in enumerate(required_event_sessions(event), start=1):
                    copy.write_row((event_index, ordinal, event.series_ids[0], session_date))
    rows = conn.execute(
        _PRICE_WINDOWS_SQL,
        {"quarantine_version": QUARANTINE_RULE_SET_VERSION},
    ).fetchall()
    by_index = {int(row[0]): row for row in rows}
    windows: list[PriceWindow] = []
    for event_index, event in enumerate(selected):
        row = by_index[event_index]
        sessions = required_event_sessions(event)
        decimal_values = [None if value is None else Decimal(value) for value in row[7:16]]
        windows.append(
            PriceWindow(
                event=event,
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
            )
        )
    return tuple(windows)


class OutcomeGateRefusal(RuntimeError):
    """The sealed outcome boundary was not explicitly and correctly opened."""


@dataclass(frozen=True)
class OutcomeGate:
    contract_sha256: str
    trial_register_version: str
    trial_id: str


@dataclass(frozen=True)
class PriceWindow:
    event: SourceEvent
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

    @property
    def outcome_refusal(self) -> str | None:
        if self.event.primary_source_refusal is not None:
            return self.event.primary_source_refusal
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


def require_outcome_gate(*, acknowledgement: str | None, contract_path: Path) -> OutcomeGate:
    """Refuse unless the reviewed contract and declared trial are both exact."""

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
    return OutcomeGate(digest, TRIAL_REGISTER.version, TRIAL_ID)


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


def required_event_sessions(event: SourceEvent) -> tuple[date, ...]:
    """Sixty formation sessions followed by the exact ten-session position."""

    entry = next_regular_session_strictly_after(event.public_filing_date)
    prior = regular_sessions_ending_before(entry, 60)
    holding: list[date] = []
    candidate = entry
    while len(holding) < 10:
        if us_market_status(candidate) != "closed":
            holding.append(candidate)
        candidate += timedelta(days=1)
    return prior + tuple(holding)


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
    gate = require_outcome_gate(acknowledgement=args.acknowledgement, contract_path=args.contract)
    # Deliberate second lock.  This PR establishes and tests the outcome
    # boundary; it does not yet contain the reviewed database evaluator.
    raise OutcomeGateRefusal(
        "gate satisfied but database outcome evaluator is not present; no price query was executed: "
        + json.dumps(gate.__dict__, sort_keys=True)
    )


if __name__ == "__main__":
    raise SystemExit(main())
