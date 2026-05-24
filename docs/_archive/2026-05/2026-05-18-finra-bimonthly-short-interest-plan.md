# FINRA bimonthly short interest ingest + schema (#915) — implementation plan

> Spec: ``docs/superpowers/specs/2026-05-18-finra-bimonthly-short-interest.md`` (CLEAN through Codex 1a r4).
> Spike: ``docs/superpowers/spikes/2026-05-18-finra-bimonthly-short-interest-feasibility.md``.
> Parent plan: ``docs/superpowers/plans/2026-05-17-us-etl-completion.md`` §2 Phase 6 PR 11.
> Branch: ``feature/915-finra-bimonthly-short-interest`` (already on).
> Architectural siblings (most recent precedent + cross-references):
> - G12 ``sec_master_idx_quarterly_sweep`` (PR #1196 ``e48eba3``) — ScheduledJob + preloaded resolver + per-fire HTTP fetch + per-target failure isolation + ``_INVOKERS`` registration + ``Lane`` Literal extension.
> - G7 ``sec_xbrl_facts`` (PR #1190 + ``app/services/manifest_parsers/sec_xbrl_facts.py``) — synth no-op manifest parser pattern.
> - #1171 N-PORT raw-payload-before-parse at ``app/services/n_port_ingest.py:786-798`` — store_raw → conn.commit → enter parse-and-upsert txn.

## 1. Task DAG

```
T1  Migrations (sql/151 + sql/152)
T2  Provider — app/providers/implementations/finra_short_interest.py
T3  Service — app/services/finra_short_interest_ingest.py
T4  ScheduledJob body — app/jobs/finra_short_interest_refresh.py
T5  scheduler.py wiring — JOB_FINRA_SHORT_INTEREST_REFRESH constant + ScheduledJob entry + body shim
T6  Lane Literal + MANUAL_TRIGGER_JOB_SOURCES + _INVOKERS in app/jobs/sources.py + app/jobs/runtime.py
T7  Param-metadata entry — DROPPED per Codex 1b r1 HIGH 1 (zero-param manual surface; REPL runbook for extended backfill). Kept in DAG as no-op marker for explicit handling.
T8  Manifest parser — app/services/manifest_parsers/finra_short_interest.py + __init__.py register_all_parsers
T9  DocumentKind Literal widening in app/services/raw_filings.py
T10 Tests (provider + service + scheduledjob + wiring + manifest-parser + universal-gate + fetch-doc-text-callers)
T11 Real-shape fixture seed — tests/fixtures/finra/shrt20260430_sample.csv
T12 Skill doc — .claude/skills/data-sources/finra.md
T13 Matrix updates — .claude/skills/data-engineer/etl-endpoint-coverage.md §2 + §7 G6
T14 Memory note — finra_phase6_bimonthly_short_interest.md + MEMORY.md index entry
T15 Local gates (ruff check + ruff format check + pyright + pytest)
T16 Codex 2 pre-push
T17 PR body + push
T18 Bot review loop → APPROVE + CI green → merge
```

Dependencies (corrected from Codex 1b r1 LOW 1):
- T1 (SQL migrations) is standalone — only needs DB connection. Does NOT depend on T8/T9.
- T9 (DocumentKind Literal widening) blocks T4 + T10 (callers using ``store_raw`` with the new literal).
- T2 blocks T3 + T4 + T10.
- T3 blocks T4 + T10.
- T5 + T6 blocks T4 (job body imports scheduler constants).
- T8 blocks T15 (manifest parser registration in module-import path; tests for parser unit + registry).
- T10 blocks T15.
- T15 blocks T16.
- T16 blocks T17.

Practical ordering: T1 (both migrations, parallel) → T9 (DocumentKind widening) → T2 (provider) → T3 (service) → T6 (Lane + sources.py) → T5 (scheduler constants + body shim) → T4 (job module) → T7 (drop — no params) → T8 (manifest parser) → T11 (fixture seed) → T10 (tests) → T12 → T13 → T14 → T15 (local gates) → T16 (Codex 2) → T17 (push) → T18 (review loop).

## 2. T1 — Migrations

### 2.1 ``sql/151_filing_raw_documents_add_finra_si.sql``

Verbatim per spec §5.1. Test fixture: ``tests/fixtures/ebull_test_db.py`` re-runs migrations as part of per-worker DB template build; the existing ``filing_raw_documents`` constraint refresh is a normal ALTER + DROP + ADD CHECK shape (sql/146 precedent).

### 2.2 ``sql/152_finra_short_interest.sql``

Verbatim per spec §5.2. The ``DO`` block generates 23 partitions covering 2021-Q3 → 2027-Q1.

**Pre-implementation verification step** (caught Codex r3 LOW): run a SQL-script dry-run inside a Postgres docker container to count partitions before locking the migration:

```bash
docker exec ebull-postgres bash -c "psql -U postgres -d ebull -f /tmp/152.sql && \
  psql -U postgres -d ebull -c \"SELECT count(*) FROM pg_inherits WHERE inhparent = 'finra_short_interest_observations'::regclass\""
```

Expected: ``23``. If count diverges from the spec, fix the spec OR fix the DO block bound before committing.

### 2.3 ``_PLANNER_TABLES`` registration

Add ``finra_short_interest_observations`` + ``finra_short_interest_current`` to ``tests/fixtures/ebull_test_db.py::_PLANNER_TABLES`` so cross-test cleanup wipes the dev rows. Mirrors PR #1194 G8 precedent for cross-test cleanup.

## 3. T2 — Provider

### 3.1 File: ``app/providers/implementations/finra_short_interest.py``

```python
"""FINRA Equity Short Interest CDN provider (#915).

Anonymous CDN access at https://cdn.finra.org/equity/otcmarket/biweekly/.
1 req/s polite floor (no published FINRA rate limit; CDN robots.txt is 403).
Disjoint from the SEC rate-limit pool — different host, no shared
per-IP budget. The module-global throttle clock + lock pattern mirrors
``app/providers/implementations/sec_edgar.py:54-80, 237-253``.
"""

from __future__ import annotations
import threading
from datetime import date
from typing import Final

import httpx

from app.providers.resilient_client import ResilientClient

# Module-global throttle state. Shared across multiple
# ``FinraShortInterestProvider`` instances via the ResilientClient
# ``shared_last_request`` + ``shared_throttle_lock`` parameters.
# Preserves the "Multiple ResilientClient instances sharing a rate
# limit must share throttle state" prevention-log rule (#726, lines
# 510-513).
_FINRA_RATE_LIMIT_CLOCK: Final[list[float]] = [0.0]
_FINRA_RATE_LIMIT_LOCK: Final[threading.Lock] = threading.Lock()
_FINRA_MIN_INTERVAL_S: Final[float] = 1.0


class FinraNotFound(Exception):
    """404 from the FINRA CDN — file not yet published or archive purged."""


class FinraShortInterestProvider:
    BASE_URL: Final[str] = "https://cdn.finra.org/equity/otcmarket/biweekly/"

    def __init__(self, http_client: ResilientClient | None = None) -> None:
        if http_client is None:
            inner = httpx.Client(
                timeout=httpx.Timeout(30.0),
                headers={
                    "User-Agent": "eBull/0.1 (luke.bradford@hotmail.co.uk)",
                    "Accept": "text/csv,*/*",
                },
            )
            http_client = ResilientClient(
                inner,
                min_request_interval_s=_FINRA_MIN_INTERVAL_S,
                shared_last_request=_FINRA_RATE_LIMIT_CLOCK,
                shared_throttle_lock=_FINRA_RATE_LIMIT_LOCK,
            )
        self._http = http_client

    def settlement_file_url(self, settlement_date: date) -> str:
        return f"{self.BASE_URL}shrt{settlement_date.strftime('%Y%m%d')}.csv"

    def fetch_settlement_file(self, settlement_date: date) -> bytes:
        url = self.settlement_file_url(settlement_date)
        response = self._http.get(url)
        if response.status_code == 404:
            raise FinraNotFound(f"FINRA settlement file not found: {url}")
        response.raise_for_status()
        return response.content
```

### 3.2 Tests in ``tests/test_finra_short_interest_provider.py``

| Test | Asserts |
|---|---|
| ``test_settlement_file_url_builder_iso_date`` | ``provider.settlement_file_url(date(2026, 4, 30)) == "https://cdn.finra.org/equity/otcmarket/biweekly/shrt20260430.csv"`` |
| ``test_settlement_file_url_builder_leap_year`` | ``date(2024, 2, 29)`` → ``shrt20240229.csv`` |
| ``test_404_raises_finra_not_found`` | ``httpx.MockTransport`` returns 404 → ``FinraNotFound`` raised |
| ``test_5xx_raises_http_status_error`` | ``httpx.MockTransport`` returns 500 with attached ``Request`` (per Codex G10 r1 MED-3 pattern) → ``httpx.HTTPStatusError`` raised after ``max_retries=0`` injected via test wrapper |
| ``test_rate_limit_clock_identity`` | Two ``FinraShortInterestProvider`` instances share ``_FINRA_RATE_LIMIT_CLOCK`` (compare list identities) |
| ``test_back_to_back_throttle_enforces_min_interval`` | ``httpx.MockTransport`` returns 200; two back-to-back fetches with monkeypatched ``time.monotonic`` measure ≥ ``_FINRA_MIN_INTERVAL_S`` gap; teardown resets ``_FINRA_RATE_LIMIT_CLOCK[0] = 0.0`` |

Per the G10 ``test_sec_fundamentals_companyconcept.py`` precedent at ``tests/test_sec_fundamentals_companyconcept.py``:
- 5xx fixture MUST attach ``httpx.Request("GET", url)`` to the ``httpx.Response`` so ``raise_for_status()`` fires cleanly (Codex G10 r1 MED-3).
- Test wrapper injects ``max_retries=0`` to drop test wall-clock from ~7s default backoff to ~0.1s (Codex G10 2 r1 LOW-3).
- ``_FINRA_RATE_LIMIT_CLOCK[0] = 0.0`` reset in teardown (Codex G10 r1 LOW-4).

## 4. T3 — Service

### 4.1 File: ``app/services/finra_short_interest_ingest.py``

```python
"""FINRA bimonthly short interest service (#915).

Parses pipe-delim payloads from the FINRA CDN, resolves symbolCode →
instrument_id via the preloaded resolver, UPSERTs typed observations,
UPSERTs the _current snapshot, and UPSERTs the manifest tracking row.

Transaction contract: ``ingest_settlement_file`` ACCEPTS a caller-
supplied connection and NEVER calls ``conn.commit()`` /
``conn.rollback()`` AND DOES NOT enter its own ``with
conn.transaction():``. The caller MUST wrap the call site in
``with conn.transaction():`` immediately before invoking this
function. The SAVEPOINT-vs-TOPLEVEL ambiguity (Codex 1b r1 HIGH 2) is
avoided by construction: the SERVICE body emits SQL only against the
caller's open transaction. Commit / rollback ownership stays with
the JOB caller — prevention-log "service that accepts an external
connection must not commit" invariant.

Row-shape contract (Codex 1b r2 MED 1): ``csv.DictReader`` sets
missing trailing fields to ``None``, NOT to absent keys. A truncated
row therefore presents as a dict with the expected keys but some
later-position values are ``None`` or ``''``. The per-row defect
counter MUST explicitly count ``len([v for v in row.values() if v is
None])`` and reject the row when ANY required-field value is missing
(``symbolCode``, ``currentShortPositionQuantity``, ``settlementDate``).

Raw-payload-before-parse contract (#1168) is JOB-enforced: the caller
MUST run ``raw_filings.store_raw(...)`` + ``conn.commit()`` BEFORE
calling this function. See spec §4.
"""

from __future__ import annotations
import csv
import io
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

import psycopg

logger = logging.getLogger(__name__)

_NORMALISE_RE = re.compile(r'[^A-Z0-9]+')

# FINRA's pipe-delim header — must match exactly. Header corruption
# (column count mismatch / missing field) is file-level fatal per
# spec §7.4.
_EXPECTED_HEADER: tuple[str, ...] = (
    "accountingYearMonthNumber",
    "symbolCode",
    "issueName",
    "issuerServicesGroupExchangeCode",
    "marketClassCode",
    "currentShortPositionQuantity",
    "previousShortPositionQuantity",
    "stockSplitFlag",
    "averageDailyVolumeQuantity",
    "daysToCoverQuantity",
    "revisionFlag",
    "changePercent",
    "changePreviousNumber",
    "settlementDate",
)


class HeaderCorruptionError(Exception):
    """FINRA file header missing / wrong column count. File-level fatal."""


@dataclass(frozen=True)
class SettlementIngestStats:
    settlement_date: date
    rows_parsed: int = 0
    rows_resolved: int = 0
    rows_upserted: int = 0
    skipped_no_instrument_match: int = 0
    skipped_ambiguous_symbol: int = 0
    skipped_invalid_row: int = 0
    failed: bool = False
    error_detail: str | None = None


def _normalise_symbol(symbol: str) -> str:
    """Strip non-alphanumerics + upper-case.

    'BRK.A' -> 'BRKA'.
    'goog' -> 'GOOG'.
    'ABRPRD' -> 'ABRPRD' (FINRA shape, idempotent).
    """
    return _NORMALISE_RE.sub('', symbol.upper())


def build_preloaded_symbol_resolver(
    conn: psycopg.Connection[Any],
) -> Callable[[str], int | None]:
    """One-shot SELECT all (instrument_id, symbol) FROM instruments.

    Returns a closure mapping normalised symbol → instrument_id.
    On normalised-collision (multiple instruments share the same
    normalised key — e.g. ``BRK.A`` and ``BRKA``), the colliding
    key resolves to ``None``; caller increments
    ``skipped_ambiguous_symbol``.
    """
    multimap: dict[str, set[int]] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT instrument_id, symbol FROM instruments")
        for instrument_id, symbol in cur.fetchall():
            normalised = _normalise_symbol(symbol)
            if not normalised:
                continue
            multimap.setdefault(normalised, set()).add(instrument_id)

    # Resolve to a flat {key -> id_or_None} where None means ambiguous.
    flat: dict[str, int | None] = {
        key: (next(iter(ids)) if len(ids) == 1 else None)
        for key, ids in multimap.items()
    }

    # Track ambiguous keys for counter-incrementing in caller.
    ambiguous_keys: frozenset[str] = frozenset(k for k, v in flat.items() if v is None)

    def resolver(symbol: str) -> int | None:
        key = _normalise_symbol(symbol)
        return flat.get(key)

    resolver.ambiguous_keys = ambiguous_keys  # type: ignore[attr-defined]
    return resolver


def ingest_settlement_file(
    conn: psycopg.Connection[Any],
    settlement_date: date,
    raw_bytes: bytes,
    resolver: Callable[[str], int | None],
    ingest_run_id: UUID,
) -> SettlementIngestStats:
    """Service body — see module docstring + spec §7."""
    # Header validation (file-level).
    text = raw_bytes.decode('utf-8')
    reader = csv.DictReader(io.StringIO(text), delimiter='|')
    if reader.fieldnames is None or tuple(reader.fieldnames) != _EXPECTED_HEADER:
        raise HeaderCorruptionError(
            f"FINRA header mismatch at settlement_date={settlement_date}: "
            f"expected {_EXPECTED_HEADER}, got {reader.fieldnames}"
        )

    source_document_id = settlement_date.strftime('%Y%m%d')
    accession = f"FINRA_SI_{source_document_id}"
    file_url = (
        f"https://cdn.finra.org/equity/otcmarket/biweekly/shrt{source_document_id}.csv"
    )
    filed_at = datetime.combine(settlement_date, datetime.min.time(), tzinfo=UTC)
    ambiguous_keys: frozenset[str] = getattr(resolver, "ambiguous_keys", frozenset())

    rows_parsed = 0
    rows_resolved = 0
    rows_upserted = 0
    skipped_no_instrument_match = 0
    skipped_ambiguous_symbol = 0
    skipped_invalid_row = 0

    # Implementation contract (see spec §7 + this plan §4.1 docstring):
    #
    #  1. Service ENTERS NO transaction context manager — caller wraps
    #     the call site in ``with conn.transaction():`` (Codex 1b
    #     r1 HIGH 2).
    #  2. Row-shape validation BEFORE any UPSERT (Codex 1b r2 MED 1):
    #     reject the row if any required column (symbolCode,
    #     currentShortPositionQuantity, settlementDate) is None / blank
    #     AFTER csv.DictReader has set missing trailing fields to None.
    #  3. UPSERT into finra_short_interest_observations + _current
    #     using the exact SQL in spec §5.2 + §5.4 (PK
    #     (instrument_id, settlement_date, source_document_id);
    #     _current ON CONFLICT predicate uses the date+refreshed_at
    #     compound that handles same-date revisions).
    #  4. UPSERT into sec_filing_manifest using the exact tuple in
    #     spec §7.3, with parser_version = 'finra-si-bimonthly-v1'
    #     (unified, no separate noop-version).
    #
    # The cursor + per-row body is straight-line Python; see
    # tests/test_finra_short_interest_ingest.py for the contract that
    # locks correctness. Plan retains this block as a pointer rather
    # than verbatim code to keep the document compact + survive
    # spec-side schema tweaks without separate edit on this listing.

    cur = conn.cursor()
    for row in reader:
        rows_parsed += 1
        # Row-shape validation: csv.DictReader sets missing trailing
        # fields to None; check explicit required-field presence.
        symbol = (row.get('symbolCode') or '').strip()
        current_short_raw = row.get('currentShortPositionQuantity')
        settlement_raw = row.get('settlementDate')
        if not symbol or current_short_raw in (None, '') or settlement_raw in (None, ''):
            skipped_invalid_row += 1
            continue
        try:
            current_short_int = int(current_short_raw)
        except (ValueError, TypeError):
            skipped_invalid_row += 1
            continue

        # Resolver collision check (resolver returns None for both
        # ambiguous + no-match; disambiguate via ambiguous_keys set).
        key = _normalise_symbol(symbol)
        if key in ambiguous_keys:
            skipped_ambiguous_symbol += 1
            continue
        instrument_id = resolver(symbol)
        if instrument_id is None:
            skipped_no_instrument_match += 1
            continue
        rows_resolved += 1

        # cur.execute(<UPSERT into finra_short_interest_observations>,
        #             <param dict mapping the 14 FINRA columns +
        #             provenance to the table columns>);
        # cur.execute(<UPSERT into finra_short_interest_current>,
        #             <param dict>);
        rows_upserted += 1

    # cur.execute(<UPSERT into sec_filing_manifest>, <synthetic tuple
    # per spec §7.3>);

    return SettlementIngestStats(
        settlement_date=settlement_date,
        rows_parsed=rows_parsed,
        rows_resolved=rows_resolved,
        rows_upserted=rows_upserted,
        skipped_no_instrument_match=skipped_no_instrument_match,
        skipped_ambiguous_symbol=skipped_ambiguous_symbol,
        skipped_invalid_row=skipped_invalid_row,
    )

```

### 4.2 Tests in ``tests/test_finra_short_interest_ingest.py``

Against ``ebull_test_conn`` fixture. Setup helper ``_seed_instruments(conn, panel)`` inserts the smoke panel (AAPL=1001, GME=1002, MSFT=1003, JPM=1004, HD=1005) with deterministic instrument_id values.

| Test | Asserts |
|---|---|
| ``test_normalise_symbol_brk_dot`` | ``_normalise_symbol('BRK.A') == 'BRKA'`` |
| ``test_normalise_symbol_lowercase_uppered`` | ``_normalise_symbol('goog') == 'GOOG'`` |
| ``test_normalise_symbol_idempotent_no_separator`` | ``_normalise_symbol('ABRPRD') == 'ABRPRD'`` |
| ``test_resolver_resolves_panel`` | All 5 panel symbols resolve to the seeded instrument_id |
| ``test_resolver_returns_none_on_no_match`` | ``resolver('NOTREAL') is None`` |
| ``test_resolver_marks_ambiguous_via_attr`` | seed two rows with collide-on-normalise; resolver returns None + ``ambiguous_keys`` contains the key |
| ``test_ingest_happy_path_panel_writes_observations`` | Real-shape fixture (T11) — 5 panel rows land in observations + _current; manifest row UPSERTed parsed with parser_version ``'finra-si-bimonthly-v1'`` |
| ``test_ingest_skip_invalid_row_missing_field`` | Truncated row (missing ``currentShortPositionQuantity``) → ``stats.skipped_invalid_row == 1``, file otherwise succeeds |
| ``test_ingest_skip_invalid_row_malformed_int`` | Non-integer in ``currentShortPositionQuantity`` → ``stats.skipped_invalid_row == 1`` |
| ``test_ingest_skip_no_match`` | Symbol ``NOTREAL12345`` → ``stats.skipped_no_instrument_match == 1`` |
| ``test_ingest_skip_ambiguous`` | Two seeded instruments collide on normalise; FINRA row with that symbol → ``stats.skipped_ambiguous_symbol == 1`` |
| ``test_ingest_header_corruption_raises`` | Fixture with mangled header → ``HeaderCorruptionError`` raised; service did NOT commit (no observations rows) |
| ``test_ingest_no_commit_inside_service`` | Spy on ``conn.commit`` + ``conn.rollback``; call ``ingest_settlement_file`` directly (no caller commit); assert NEITHER spy fired |
| ``test_current_settlement_wins_more_recent`` | Seed observations at 2026-04-15, then ingest 2026-04-30 → _current row reflects 04-30 |
| ``test_current_same_date_revision_wins_later_refresh`` | Seed _current at 2026-04-15 with old ``refreshed_at``; re-ingest same date later → _current ``current_short_interest`` updated |
| ``test_manifest_upsert_keeps_parsed_on_re_ingest`` | Ingest twice; manifest ``ingest_status`` stays ``parsed``, ``last_attempted_at`` advances |

## 5. T4 — ScheduledJob body

### 5.1 File: ``app/jobs/finra_short_interest_refresh.py``

Mirrors G12 ``app/jobs/sec_master_idx_quarterly_sweep.py`` shape:

```python
"""FINRA bimonthly short interest refresh (#915) — Phase 6 PR 11.

Spec: docs/superpowers/specs/2026-05-18-finra-bimonthly-short-interest.md.
Plan: docs/superpowers/plans/2026-05-18-finra-bimonthly-short-interest-plan.md.
"""

from __future__ import annotations
import calendar
import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import psycopg

from app.providers.implementations.finra_short_interest import (
    FinraNotFound,
    FinraShortInterestProvider,
)
from app.services import raw_filings
from app.services.finra_short_interest_ingest import (
    HeaderCorruptionError,
    SettlementIngestStats,
    build_preloaded_symbol_resolver,
    ingest_settlement_file,
)

logger = logging.getLogger(__name__)

JOB_FINRA_SHORT_INTEREST_REFRESH = "finra_short_interest_refresh"


@dataclass(frozen=True)
class FinraRefreshStats:
    settlement_files: list[SettlementIngestStats] = field(default_factory=list)

    @property
    def total_upserted(self) -> int:
        return sum(s.rows_upserted for s in self.settlement_files)

    @property
    def total_parsed(self) -> int:
        return sum(s.rows_parsed for s in self.settlement_files)

    @property
    def total_resolved(self) -> int:
        return sum(s.rows_resolved for s in self.settlement_files)

    @property
    def failed_files(self) -> int:
        return sum(1 for s in self.settlement_files if s.failed)


def _walk_back_to_weekday(d: date) -> date:
    """If ``d`` falls on Saturday/Sunday, walk BACK to the prior Friday.

    FINRA publishes ``shrt{YYYYMMDD}.csv`` keyed by the last *business*
    day of the half-month — not the calendar day. Federal-holiday
    EOMs are sufficiently rare that we tolerate the 404 fall-through.
    """
    while d.weekday() >= 5:  # 5=Saturday, 6=Sunday
        d -= timedelta(days=1)
    return d


def _settlement_dates_to_fetch(
    now: datetime,
    backfill_window_days: int = 400,
) -> list[date]:
    """Enumerate every business-day-adjusted (year, month, 15) +
    (year, month, last_business_day) settlement date in
    ``[now - backfill_window_days, now]``. Sorted ASC.

    Per Codex 1b r1 HIGH 3: calendar-month-end alone is wrong — FINRA
    keys files by the last business day. We adjust each calendar EOM
    via ``_walk_back_to_weekday``; the 15th gets the same treatment
    when it falls on Saturday/Sunday.
    """
    earliest = (now - timedelta(days=backfill_window_days)).date()
    today = now.date()
    out: list[date] = []
    # Walk months from earliest's month through today's month.
    y, m = earliest.year, earliest.month
    while (y, m) <= (today.year, today.month):
        mid = _walk_back_to_weekday(date(y, m, 15))
        last = _walk_back_to_weekday(
            date(y, m, calendar.monthrange(y, m)[1])
        )
        for d in (mid, last):
            if earliest <= d <= today:
                out.append(d)
        m += 1
        if m > 12:
            m = 1
            y += 1
    return sorted(set(out))


def _already_parsed_settlement_dates(conn: psycopg.Connection[Any]) -> set[date]:
    """Read sec_filing_manifest for FINRA short-interest accessions that
    have ingest_status='parsed'; return the parsed settlement_date set.
    """
    out: set[date] = set()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT accession_number
            FROM sec_filing_manifest
            WHERE source = 'finra_short_interest'
              AND ingest_status = 'parsed'
            """
        )
        for (accession,) in cur.fetchall():
            # accession_number shape: 'FINRA_SI_YYYYMMDD'
            if not accession.startswith('FINRA_SI_'):
                continue
            tail = accession[len('FINRA_SI_'):]
            try:
                out.add(datetime.strptime(tail, '%Y%m%d').date())
            except ValueError:
                continue
    return out


def _compute_targets(
    candidate_dates: list[date],
    already_parsed: set[date],
) -> list[date]:
    """Subtract parsed; UNION with revision-window (most-recent two
    candidates) so in-place FINRA revisions don't get masked.

    Returns sorted ASC.
    """
    sorted_candidates = sorted(candidate_dates)
    revision_window = set(sorted_candidates[-2:]) if sorted_candidates else set()
    return sorted((set(candidate_dates) - already_parsed) | revision_window)


def run_finra_short_interest_refresh(
    conn: psycopg.Connection[Any],
    *,
    now: datetime | None = None,
    backfill_window_days: int = 400,
    provider: FinraShortInterestProvider | None = None,
) -> FinraRefreshStats:
    now_ = now or datetime.now(UTC)
    if provider is None:
        provider = FinraShortInterestProvider()

    resolver = build_preloaded_symbol_resolver(conn)
    candidate_dates = _settlement_dates_to_fetch(now_, backfill_window_days)
    already_parsed = _already_parsed_settlement_dates(conn)
    targets = _compute_targets(candidate_dates, already_parsed)

    ingest_run_id = uuid4()
    stats_list: list[SettlementIngestStats] = []

    for settlement_date in targets:
        url = provider.settlement_file_url(settlement_date)
        try:
            raw_bytes = provider.fetch_settlement_file(settlement_date)
        except FinraNotFound:
            logger.info(
                "finra_short_interest_refresh: skip not-yet-published settlement=%s",
                settlement_date.isoformat(),
            )
            continue
        except Exception as exc:  # noqa: BLE001 — captured into stats
            stats_list.append(
                SettlementIngestStats(
                    settlement_date=settlement_date,
                    failed=True,
                    error_detail=f"fetch: {type(exc).__name__}: {exc}",
                )
            )
            continue

        # Empty-file guard (Codex 1b r1 MED 1). raw_filings.store_raw
        # rejects empty payloads at app/services/raw_filings.py:105
        # ('payload is required (empty payload would defeat re-wash)'),
        # so we MUST catch the empty case before attempting store_raw.
        # An empty 200 response is most likely a CDN edge-case;
        # treating it as a per-file failure (with raw NOT stored) is
        # correct — there's no payload to preserve.
        if not raw_bytes:
            stats_list.append(
                SettlementIngestStats(
                    settlement_date=settlement_date,
                    failed=True,
                    error_detail="empty file (0 bytes from FINRA CDN)",
                )
            )
            continue

        # Phase 1: raw payload durable BEFORE parse (#1168).
        raw_filings.store_raw(
            conn,
            accession_number=f"FINRA_SI_{settlement_date.strftime('%Y%m%d')}",
            document_kind='finra_short_interest_csv',
            payload=raw_bytes.decode('utf-8'),
            source_url=url,
        )
        conn.commit()

        # Phase 2: parse + upserts inside JOB-owned transaction
        # (Codex 1b r1 HIGH 2). Service body emits SQL only —
        # commit/rollback is THIS scope's responsibility.
        try:
            with conn.transaction():
                per_file = ingest_settlement_file(
                    conn, settlement_date, raw_bytes, resolver, ingest_run_id,
                )
            stats_list.append(per_file)
        except (HeaderCorruptionError, Exception) as exc:  # noqa: BLE001
            # ``with conn.transaction()`` rolled back automatically on the
            # raised exception; raw payload is durable from the earlier
            # conn.commit() so a future re-ingest can re-attempt parse
            # against the same raw row.
            stats_list.append(
                SettlementIngestStats(
                    settlement_date=settlement_date,
                    failed=True,
                    error_detail=f"parse: {type(exc).__name__}: {exc}",
                )
            )

    stats = FinraRefreshStats(settlement_files=stats_list)

    total_skipped_no_match = sum(s.skipped_no_instrument_match for s in stats_list)
    total_skipped_ambiguous = sum(s.skipped_ambiguous_symbol for s in stats_list)
    total_skipped_invalid = sum(s.skipped_invalid_row for s in stats_list)

    logger.info(
        "finra_short_interest_refresh: files=%d upserted=%d parsed=%d resolved=%d "
        "skipped_no_match=%d skipped_ambiguous=%d skipped_invalid=%d failed=%d",
        len(stats_list),
        stats.total_upserted,
        stats.total_parsed,
        stats.total_resolved,
        total_skipped_no_match,
        total_skipped_ambiguous,
        total_skipped_invalid,
        stats.failed_files,
    )

    if stats.total_parsed > 0:
        match_rate = stats.total_resolved / stats.total_parsed
        if match_rate < 0.50:
            logger.warning(
                "finra_short_interest_refresh: match rate %.2f%% below 50%% threshold "
                "(parsed=%d resolved=%d) — universe drift or FINRA column-shape "
                "regression suspected",
                100 * match_rate,
                stats.total_parsed,
                stats.total_resolved,
            )

    if stats.failed_files > 0:
        raise RuntimeError(
            f"finra_short_interest_refresh: {stats.failed_files} of "
            f"{len(stats_list)} files failed"
        )

    return stats
```

### 5.2 Tests in ``tests/test_finra_short_interest_refresh.py``

Against ``ebull_test_conn``. ``provider=`` parameter is the test-injection hook:

```python
class _FakeProvider:
    def __init__(self, settlements: dict[date, bytes], notfound: set[date] | None = None,
                 errors: dict[date, Exception] | None = None) -> None:
        self._settlements = settlements
        self._notfound = notfound or set()
        self._errors = errors or {}

    def settlement_file_url(self, settlement_date: date) -> str:
        return f"https://cdn.finra.org/equity/otcmarket/biweekly/shrt{settlement_date:%Y%m%d}.csv"

    def fetch_settlement_file(self, settlement_date: date) -> bytes:
        if settlement_date in self._notfound:
            raise FinraNotFound(str(settlement_date))
        if settlement_date in self._errors:
            raise self._errors[settlement_date]
        return self._settlements[settlement_date]
```

| Test | Asserts |
|---|---|
| ``test_settlement_dates_15th_eom_weekday_aware`` | ``_settlement_dates_to_fetch(now=datetime(2026, 5, 18, tzinfo=UTC), backfill_window_days=100)`` returns ``[2026-02-13, 2026-02-27, 2026-03-13, 2026-03-31, 2026-04-15, 2026-04-30, 2026-05-15]`` — Feb 15 (Sun) walks back to Feb 13 (Fri); Feb 28 (Sat) walks back to Feb 27 (Fri); Mar 15 (Sun) walks back to Mar 13 (Fri). Apr 15 + Apr 30 are weekdays so unchanged. May 15 is Friday so unchanged. May 18 is the ``now`` boundary — the trailing EOM 2026-05-29 hasn't happened yet. |
| ``test_walk_back_to_weekday`` | Saturday → prior Friday; Sunday → prior Friday; Monday-Friday → unchanged. |
| ``test_settlement_dates_leap_year`` | February 2024 EOM = 2024-02-29 (Thursday) — no walk-back. |
| ``test_settlement_dates_window_bounds_inclusive`` | settlement on day == earliest is included; settlement on day > today is excluded |
| ``test_compute_targets_subtracts_parsed_excluding_revision_window`` | parsed = {2024-01-15, 2026-04-15}; targets must include 2026-04-15 (within revision window) and exclude 2024-01-15 |
| ``test_compute_targets_empty_candidates_returns_empty`` | ``_compute_targets([], set()) == []`` |
| ``test_run_happy_path_writes_observations_and_manifest`` | Fake provider returns real-shape fixture for 2026-04-30; observations + _current + manifest rows land |
| ``test_run_skips_not_yet_published`` | Fake provider raises ``FinraNotFound`` for one target → log INFO, no stats row, no manifest write for that date |
| ``test_run_fetch_5xx_records_failed_continues`` | Fake provider raises ``httpx.HTTPStatusError`` for one target → ``stats.failed_files == 1``, other targets still process, ``RuntimeError`` raised at end |
| ``test_run_parse_failure_rolls_back_keeps_raw`` | Fake provider returns header-corruption fixture → ``HeaderCorruptionError`` rolled back at parse phase; raw payload row in ``filing_raw_documents`` is durable; ``stats.failed_files == 1`` |
| ``test_run_match_rate_below_threshold_logs_warning`` | Fixture with mostly unresolvable symbols → WARNING logger captured (caplog) |
| ``test_run_revision_window_re_fetches_two_most_recent`` | Seed parsed manifest rows for the two most-recent candidates; the fake provider verifies BOTH are still re-fetched |

## 6. T5 — scheduler wiring

### 6.1 ``app/workers/scheduler.py``

Add near the existing constants block:

```python
JOB_FINRA_SHORT_INTEREST_REFRESH = "finra_short_interest_refresh"
```

Add to ``SCHEDULED_JOBS`` list after the ``JOB_SEC_MASTER_IDX_QUARTERLY_SWEEP`` entry per spec §8.2.

Add the scheduler body:

```python
def finra_short_interest_refresh() -> None:
    """``_INVOKERS['finra_short_interest_refresh']`` — G6/#915.

    Opens its own DB connection (mirror G12 sec_master_idx_quarterly_sweep
    shape). Per-file commit/rollback ownership is inside the job module.
    """
    with _tracked_job(JOB_FINRA_SHORT_INTEREST_REFRESH) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            from app.jobs.finra_short_interest_refresh import (
                run_finra_short_interest_refresh,
            )

            stats = run_finra_short_interest_refresh(conn)
            tracker.row_count = stats.total_upserted
            logger.info(
                "finra_short_interest_refresh: files=%d total_upserted=%d failed=%d",
                len(stats.settlement_files),
                stats.total_upserted,
                stats.failed_files,
            )
            # Match-rate warning + RuntimeError on failed_files live inside
            # run_finra_short_interest_refresh — _tracked_job records
            # 'failure' if the RuntimeError propagates.
```

## 7. T6 — Lane + invoker + manual-trigger

### 7.1 ``app/jobs/sources.py``

Extend ``Lane`` Literal with ``"finra"`` per spec §8.3. Update docstring per spec §8.3.

Add ``MANUAL_TRIGGER_JOB_SOURCES["finra_short_interest_refresh"] = "finra"``.

### 7.2 ``app/jobs/runtime.py``

```python
_INVOKERS[_scheduler.JOB_FINRA_SHORT_INTEREST_REFRESH] = _adapt_zero_arg(
    _scheduler.finra_short_interest_refresh
)
```

## 8. T7 — Param metadata (DROPPED)

Per Codex 1b r1 HIGH 1 + r2 MED 3: v1 manual-trigger surface for ``finra_short_interest_refresh`` ships **zero-param** (current ``_INVOKERS`` adapter at ``app/jobs/runtime.py:_adapt_zero_arg`` discards body params anyway). No ``MANUAL_TRIGGER_JOB_METADATA`` entry is added. Operator extended-window backfill lands via REPL invocation against ``run_finra_short_interest_refresh(conn, backfill_window_days=N)`` per spec §13 acceptance #8 + plan §15.1 PR body ETL DoD #10 row.

T7 is therefore a no-op task; kept in the DAG only to mark it explicitly handled (zero-LoC change to ``app/services/processes/param_metadata.py``).

## 9. T8 — Manifest parser (synth no-op)

### 9.1 File: ``app/services/manifest_parsers/finra_short_interest.py``

Verbatim per spec §9 + module docstring. Mirrors ``sec_xbrl_facts.py`` precedent.

### 9.2 ``app/services/manifest_parsers/__init__.py``

```python
from app.services.manifest_parsers import finra_short_interest as _finra_short_interest
# ...
_finra_short_interest.register()  # synth no-op (G6/#915)
```

### 9.3 Tests in ``tests/test_finra_short_interest_manifest_parser.py``

Mirror ``tests/test_manifest_parser_sec_xbrl_facts.py`` shape:
- ``test_parse_returns_parsed_outcome`` — synth no-op returns ``ParseOutcome(status='parsed', parser_version='finra-si-bimonthly-v1')``
- ``test_parser_form_agnostic`` — accepts any FINRA-shape ``ManifestRow`` (form='SHRT' is the synthetic FINRA form code)
- ``test_registry_wiring_after_clear`` — ``clear_registered_parsers()`` + ``register_all_parsers()`` re-binds the parser
- ``test_durability_invariant_non_caller`` — parser body does NOT call ``conn.execute`` / ``conn.cursor`` / ``conn.transaction`` / ``store_raw`` / ``fetch_document_text``

## 10. T9 — DocumentKind Literal widening

### 10.1 ``app/services/raw_filings.py``

Add ``'finra_short_interest_csv'`` to the ``DocumentKind`` Literal. Pyright will surface every call site that needs updating; only the new job body should appear.

## 11. T11 — Real-shape fixture

### 11.1 Two fixtures: pristine + defect

Per Codex 1b r1 LOW 2 + r2 LOW: do NOT mix verbatim FINRA rows with hand-curated defect rows in one fixture. Split into two files so the "verbatim" claim stays true for the pristine file:

| File | Content | Provenance |
|---|---|---|
| ``tests/fixtures/finra/shrt20260430_sample.csv`` | Pristine subset of live ``shrt20260430.csv`` — header row + 9 grep-filtered real rows for symbols ``AAPL``, ``GME``, ``MSFT``, ``JPM``, ``HD``, ``ABRPRD``, ``ABRPRE``, ``ALLPRB``, ``ANCTF``. **No hand-edits.** | Live fetch + grep. |
| ``tests/fixtures/finra/shrt20260430_defects.csv`` | Synthetic. Header row + (a) one ambiguous-collapse symbol whose normalised key collides with another seeded instrument; (b) one truncated row (missing trailing fields after pipe 5). | Hand-curated for the skip-counter tests. |

Fetch + filter script (one-shot, regenerates only the pristine file):

```bash
curl -s -A "eBull/0.1 (luke.bradford@hotmail.co.uk)" \
  https://cdn.finra.org/equity/otcmarket/biweekly/shrt20260430.csv \
  -o /tmp/shrt20260430_full.csv
head -1 /tmp/shrt20260430_full.csv > tests/fixtures/finra/shrt20260430_sample.csv
grep -E '^[0-9]+\|(AAPL|GME|MSFT|JPM|HD|ABRPRD|ABRPRE|ALLPRB|ANCTF)\|' \
  /tmp/shrt20260430_full.csv \
  >> tests/fixtures/finra/shrt20260430_sample.csv
```

``shrt20260430_defects.csv`` is hand-written + committed once; never regenerated from FINRA.

## 12. T12-T14 — Skill + matrix + memory

### 12.1 ``.claude/skills/data-sources/finra.md``

NEW source-of-truth note. Endpoints, formats, rate-limit posture, symbol-norm discipline. Cross-link with ``.claude/skills/data-engineer/etl-endpoint-coverage.md``.

### 12.2 ``.claude/skills/data-engineer/etl-endpoint-coverage.md``

§2 ``finra_short_interest`` row: ``❌ pending #915 + #916`` → ``✅ WIRED 2026-05-18 (#915 bimonthly; #916 RegSHO daily pending)``.

§7 G6 row: ``OPEN`` → ``✅ CLOSED 2026-05-18 — PR <pr_number> merge <sha> (bimonthly portion; RegSHO daily portion #916 open)``.

### 12.3 Memory

New file: ``/Users/lukebradford/.claude/projects/-Users-lukebradford-Dev-eBull/memory/project_915_finra_bimonthly_short_interest.md`` — Phase 6 PR 11 close-out summary. MEMORY.md index entry.

## 13. T15 — Local gates

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest -x -q \
    tests/test_finra_short_interest_provider.py \
    tests/test_finra_short_interest_ingest.py \
    tests/test_finra_short_interest_refresh.py \
    tests/test_finra_short_interest_scheduler_wiring.py \
    tests/test_finra_short_interest_manifest_parser.py \
    tests/test_layer_123_wiring.py \
    tests/test_universal_gate_carve_out.py \
    tests/test_fetch_document_text_callers.py \
    tests/smoke/test_app_boots.py
```

If targeted tests + smoke pass + `ruff`/`pyright` are green, then the full pytest gate via the pre-push hook is allowed to use ``--no-verify`` only if it hits the documented xdist + Postgres lock OOM environmental flake (per ``feedback_pre_push_xdist_postgres_locks.md``). Codex 2 must agree before the bypass.

## 14. T16 — Codex 2 pre-push

```bash
codex exec "Pre-push review of branch feature/915-finra-bimonthly-short-interest. \
Spec: docs/superpowers/specs/2026-05-18-finra-bimonthly-short-interest.md. \
Plan: docs/superpowers/plans/2026-05-18-finra-bimonthly-short-interest-plan.md. \
Focus: anything a fresh-agent review would flag — invariant gaps, raw-payload-before-parse \
ordering, service-no-commit, partition coverage, revision-window logic, symbol-resolution \
collisions, header-corruption fast-fail. Reply terse." < /dev/null
```

## 15. T17 — Push + PR body

### 15.1 PR body template

```markdown
# feat(#915): FINRA bimonthly short interest ingest + schema

## What

- **Schema**: new `finra_short_interest_observations` (partitioned by settlement_date, 2021-Q3 → 2027-Q1 = 23 quarterly partitions) + `finra_short_interest_current` snapshot.
- **Provider**: `FinraShortInterestProvider` — anonymous CDN access at `https://cdn.finra.org/equity/otcmarket/biweekly/shrt{YYYYMMDD}.csv`, 1 req/s polite floor, NEW `finra` Lane disjoint from `sec_rate`.
- **Service**: `ingest_settlement_file(conn, ...)` parses pipe-delim, resolves symbolCode → instrument_id (strip-non-alnum + upper), UPSERTs observations + _current + manifest. Service NEVER calls `conn.commit()` / `conn.rollback()` — caller (job) owns the commit boundary.
- **ScheduledJob**: `finra_short_interest_refresh` (daily 12:00 UTC, lane=`finra`, prerequisite=_bootstrap_complete). Owns its own connection; per-file store_raw → commit → ingest → commit; revision window re-fetches the two most-recent candidates regardless of manifest status so in-place FINRA revisions (revisionFlag='Y') are caught.
- **Manifest parser**: synth no-op (sec_xbrl_facts shape, parser_version `finra-si-bimonthly-v1`); the ScheduledJob writes manifest rows directly as `parsed`.

## Why

Headline real US-ETL coverage gap per parent plan §2 Phase 6 PR 11. Closes the bimonthly short interest matrix slot at `.claude/skills/data-engineer/etl-endpoint-coverage.md` §2 + §7 G6 (RegSHO daily portion `#916` remains open as the PR 12 follow-up).

## Out of scope

- Memo overlay UI (issue #915 acceptance #2) — plan §1 autonomy contract UI carve-out. OBSERVATIONS PRIMITIVE closure framing.
- RegSHO daily short volume (#916) — separate sequential PR.

## Security model

- Anonymous CDN access only; no authentication credentials handled.
- New `finra` rate-limit pool is module-global, host-disjoint from `sec_rate`; preserves the "multiple ResilientClient instances sharing a rate limit must share throttle state" prevention-log rule (#726).
- Synthetic FINRA manifest row tuple pinned in spec §7.3; instrument_id is NULL per `chk_manifest_issuer_has_instrument`.

## Test plan

- [ ] `tests/test_finra_short_interest_provider.py` — URL builder + 404 + 5xx + rate-limit + throttle smoke.
- [ ] `tests/test_finra_short_interest_ingest.py` — symbol-norm + resolver + happy path + skip cases + header corruption + no-commit invariant + revision UPSERT + _current settlement-date-wins.
- [ ] `tests/test_finra_short_interest_refresh.py` — settlement-date enumeration + manifest filter + revision window + fetch failure isolation + parse rollback keeps raw + match-rate WARNING + RuntimeError on partial failure.
- [ ] `tests/test_finra_short_interest_manifest_parser.py` — synth no-op invariants.
- [ ] `tests/test_finra_short_interest_scheduler_wiring.py` — JOB_* constant + ScheduledJob entry + _INVOKERS identity + source_for() resolves.
- [ ] `tests/test_layer_123_wiring.py` — Layer-4 row added.
- [ ] `tests/test_universal_gate_carve_out.py` — positive: finra_short_interest_refresh NOT in exempt allow-list.
- [ ] `tests/test_fetch_document_text_callers.py` — allow-list extended.
- [ ] `tests/smoke/test_app_boots.py` — passes after migrations.

## ETL DoD #8-#12

| Clause | Evidence |
|---|---|
| #8 Smoke (AAPL/GME/MSFT/JPM/HD against 2026-04-30) | <FILL: PR body records the 5 figures observed> |
| #9 Cross-source (GME 2026-04-30 vs marketbeat.com) | <FILL: source + figure + delta %> |
| #10 Backfill (REPL invocation: `run_finra_short_interest_refresh(conn, backfill_window_days=730)` — v1 manual-trigger surface is zero-param; extended-window backfill uses REPL per Codex 1b r1 HIGH 1) | <FILL: job_runs row + total upserted> |
| #11 Operator-visible figure | `SELECT * FROM finra_short_interest_current WHERE instrument_id IN (1001, 1002)` — output recorded |
| #12 PR records verification + SHA | This table |

Closes #915
```

### 15.2 Push

```bash
git push -u origin feature/915-finra-bimonthly-short-interest
gh pr create --title "feat(#915): FINRA bimonthly short interest ingest + schema" --body "$(cat <<'EOF'
<PR body verbatim from §15.1>
EOF
)"
```

## 16. T18 — Bot review loop

Poll ``gh pr view {n} --comments`` + ``gh pr checks {n}`` after every push. Resolve every BLOCKING / WARNING / NITPICK / PREVENTION comment via FIXED / DEFERRED / REBUTTED contract. Re-run local gates before every follow-up push. Merge only after APPROVE on the most recent commit + CI green.

## 17. Anti-patterns to AVOID

- Service committing the caller's conn (HIGH 1 from spec r1 — must stay fixed).
- Forgetting to widen ``filing_raw_documents.document_kind`` (HIGH 2 from spec r1).
- Backfill > 730 days landing in a missing partition (HIGH 3 from spec r1).
- Skipping the revision window so in-place FINRA corrections never re-ingest (HIGH 4 from spec r2).
- Mismatched manifest tuple shape vs sql/118 CHECK constraints (MED from spec r2).
- Sharing the SEC ``_PROCESS_RATE_LIMIT_CLOCK`` with FINRA (prevention-log #726 violation).
- Marking the test universal-gate as exempt for ``finra_short_interest_refresh`` (it has a real prerequisite).
- Opening follow-up tickets for nits caught during review (plan §1 autonomy contract).

## 18. Risk register

| Risk | Likelihood | Mitigation |
|---|---|---|
| FINRA CDN returns 200 with HTML error page (CDN auth-redirect, etc.) | LOW | Header validation rejects file at parse phase (HeaderCorruptionError raised); ``raise_for_status()`` catches the 4xx/5xx redirects. |
| Universe drift drops match rate below 50% | LOW | WARNING log at the threshold (spec §4 + plan §5.1); operator notices on next fire. |
| Same-day FINRA in-place revision lands while ScheduledJob is mid-write | LOW | Per-file txn rollback; revision window catches on the next fire (next day at 12:00 UTC). |
| Partition past 2027-Q1 needed | LOW | Operator runbook: ALTER + add new partition. Documented in spec §5.5. |
| ``filing_raw_documents`` ``payload`` column type ``TEXT`` chokes on UTF-8 multi-byte | LOW | FINRA file is ASCII-only per empirical inspection; ``raw_bytes.decode('utf-8')`` works. |
