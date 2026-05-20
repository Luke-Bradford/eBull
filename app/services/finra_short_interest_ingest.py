"""FINRA bimonthly short interest service (#915 — Phase 6 PR 11).

Spec: docs/superpowers/specs/2026-05-18-finra-bimonthly-short-interest.md.
Plan: docs/superpowers/plans/2026-05-18-finra-bimonthly-short-interest-plan.md.

Parses pipe-delim payloads from the FINRA CDN, resolves symbolCode →
instrument_id via the preloaded resolver, UPSERTs typed observations
+ ``_current`` snapshot + the synthetic FINRA manifest row.

Transaction contract (Codex 1b r1 HIGH 2): ``ingest_settlement_file``
ACCEPTS a caller-supplied connection. It NEVER calls ``conn.commit()`` /
``conn.rollback()`` AND DOES NOT enter its own ``with
conn.transaction():`` block. The caller MUST wrap the call site in
``with conn.transaction():`` — clean exit of that block commits;
exception triggers automatic rollback. The SAVEPOINT-vs-TOPLEVEL
ambiguity is avoided by construction: the SERVICE emits SQL only into
the caller's open transaction.

Row-shape contract (Codex 1b r2 MED 1): ``csv.DictReader`` sets missing
trailing fields to ``None``, NOT to absent keys. A truncated row therefore
presents as a dict with the expected keys but some later-position values
are ``None`` or ``''``. The per-row defect path explicitly checks
``symbolCode`` + ``currentShortPositionQuantity`` + ``settlementDate``
required-presence and skips rows where any are blank/None.

Raw-payload-before-parse contract (#1168) is JOB-enforced: the caller
MUST run ``raw_filings.store_raw(...)`` + ``conn.commit()`` BEFORE
calling this function.

Manifest atomicity contract (spec §7.3): the manifest UPSERT runs
INSIDE the same caller-owned ``with conn.transaction():`` block, AFTER
the observations + ``_current`` writes. Atomic with-the-data
write — ``manifest.ingest_status='parsed'`` always implies observations
durable.
"""

from __future__ import annotations

import csv
import io
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

import psycopg

logger = logging.getLogger(__name__)

# Symbol normalisation strips non-alphanumerics + upper-cases. FINRA
# uses no-separator symbols (``ABRPRD``, ``ALLPRB``); our
# ``instruments.symbol`` uses dotted form for share-class siblings
# (``BRK.A``, ``BRK.B``) plus no-separator for the rest (``GOOG``,
# ``GOOGL``). One-way collapse to ``[A-Z0-9]*`` lets both forms match.
_NORMALISE_RE = re.compile(r"[^A-Z0-9]+")

# FINRA's pipe-delim header — must match exactly. Header corruption
# (column count mismatch / missing field) is file-level fatal per spec
# §7.4. Verified empirically against shrt20260430.csv in spike §4.1.
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

PARSER_VERSION = "finra-si-bimonthly-v1"


class HeaderCorruptionError(Exception):
    """FINRA file header missing / wrong column count — file-level fatal.

    Caller's ``with conn.transaction():`` rolls back atomically; raw
    payload stays durable from the earlier ``store_raw`` + ``conn.commit()``.
    """


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


def normalise_symbol(symbol: str) -> str:
    """Strip non-alphanumerics + upper-case.

    ``'BRK.A'`` → ``'BRKA'``. ``'goog'`` → ``'GOOG'``. ``'ABRPRD'`` →
    ``'ABRPRD'`` (FINRA shape, idempotent).
    """
    return _NORMALISE_RE.sub("", symbol.upper())


def build_preloaded_symbol_resolver(
    conn: psycopg.Connection[Any],
) -> Callable[[str], int | None]:
    """One-shot SELECT all ``(instrument_id, symbol)`` FROM instruments.

    Returns a closure mapping normalised symbol → ``instrument_id``.
    On normalised-collision (multiple instruments share the same
    normalised key — e.g. ``BRK.A`` and ``BRKA`` both collapse to
    ``BRKA``), the colliding key resolves to ``None``; the closure's
    ``ambiguous_keys`` attribute lets callers increment
    ``skipped_ambiguous_symbol`` instead of
    ``skipped_no_instrument_match``.

    Mirrors ``build_preloaded_subject_resolver`` from G12
    (``app/jobs/sec_master_idx_quarterly_sweep.py:89``) — one-shot
    materialisation + O(1) per-row lookup vs the per-row 3-table
    default-resolver path.
    """
    # Filter ``is_tradable = TRUE`` so delisted instruments don't bloat
    # the resolver's collision space. FINRA bimonthly short-interest
    # reports cover currently-listed securities only; a symbol that no
    # longer trades cannot appear in today's FINRA payload, and keeping
    # its row in the resolver only increases the chance of a normalised-
    # symbol collision that forces the legitimate active row into the
    # ambiguous bucket. #1233 §6.2.
    multimap: dict[str, set[int]] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT instrument_id, symbol FROM instruments WHERE is_tradable = TRUE")
        for instrument_id, symbol in cur.fetchall():
            normalised = normalise_symbol(symbol)
            if not normalised:
                continue
            multimap.setdefault(normalised, set()).add(instrument_id)

    # Resolve to a flat {key -> id_or_None} where None means ambiguous.
    flat: dict[str, int | None] = {key: (next(iter(ids)) if len(ids) == 1 else None) for key, ids in multimap.items()}
    ambiguous_keys: frozenset[str] = frozenset(k for k, v in flat.items() if v is None)

    def resolver(symbol: str) -> int | None:
        key = normalise_symbol(symbol)
        return flat.get(key)

    resolver.ambiguous_keys = ambiguous_keys  # type: ignore[attr-defined]
    return resolver


def _opt_int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except ValueError, TypeError:
        return None


def _opt_decimal(v: Any) -> Decimal | None:
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v))
    except InvalidOperation, ValueError, TypeError:
        return None


def ingest_settlement_file(
    conn: psycopg.Connection[Any],
    settlement_date: date,
    raw_bytes: bytes,
    resolver: Callable[[str], int | None],
    ingest_run_id: UUID,
) -> SettlementIngestStats:
    """Parse + UPSERT + manifest write. SQL-only — caller owns txn.

    See module docstring for the transaction + row-shape contracts.
    """
    text = raw_bytes.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text), delimiter="|")
    if reader.fieldnames is None or tuple(reader.fieldnames) != _EXPECTED_HEADER:
        raise HeaderCorruptionError(
            f"FINRA header mismatch at settlement_date={settlement_date}: "
            f"expected {_EXPECTED_HEADER}, got {reader.fieldnames}"
        )

    source_document_id = settlement_date.strftime("%Y%m%d")
    accession = f"FINRA_SI_{source_document_id}"
    file_url = f"https://cdn.finra.org/equity/otcmarket/biweekly/shrt{source_document_id}.csv"
    filed_at = datetime.combine(settlement_date, datetime.min.time(), tzinfo=UTC)
    ambiguous_keys: frozenset[str] = getattr(resolver, "ambiguous_keys", frozenset())

    rows_parsed = 0
    rows_resolved = 0
    rows_upserted = 0
    skipped_no_instrument_match = 0
    skipped_ambiguous_symbol = 0
    skipped_invalid_row = 0

    with conn.cursor() as cur:
        for row in reader:
            rows_parsed += 1

            # Row-shape validation. ``csv.DictReader`` sets missing trailing
            # fields to ``None``; truncated rows present as a dict with the
            # expected keys but some values None/blank. Explicit required-
            # field check (Codex 1b r2 MED 1).
            symbol = (row.get("symbolCode") or "").strip()
            current_short_raw = row.get("currentShortPositionQuantity")
            settlement_raw = row.get("settlementDate")
            if not symbol or current_short_raw in (None, "") or settlement_raw in (None, ""):
                skipped_invalid_row += 1
                continue
            try:
                current_short_int = int(current_short_raw)  # type: ignore[arg-type]
            except ValueError, TypeError:
                skipped_invalid_row += 1
                continue

            # Ambiguity check BEFORE resolver call (resolver returns None
            # for both ambiguous + no-match; disambiguate for the counter).
            key = normalise_symbol(symbol)
            if key in ambiguous_keys:
                skipped_ambiguous_symbol += 1
                continue
            instrument_id = resolver(symbol)
            if instrument_id is None:
                skipped_no_instrument_match += 1
                continue
            rows_resolved += 1

            cur.execute(
                """
                INSERT INTO finra_short_interest_observations (
                    instrument_id, settlement_date, source_document_id,
                    current_short_interest, previous_short_interest,
                    average_daily_volume, days_to_cover, change_percent,
                    change_previous, accounting_yearmonth,
                    market_class_code, exchange_code, issue_name,
                    stock_split_flag, revision_flag,
                    source, source_url, filed_at, period_end,
                    known_from, ingest_run_id
                ) VALUES (
                    %(instrument_id)s, %(settlement_date)s, %(source_document_id)s,
                    %(current_short_interest)s, %(previous_short_interest)s,
                    %(average_daily_volume)s, %(days_to_cover)s, %(change_percent)s,
                    %(change_previous)s, %(accounting_yearmonth)s,
                    %(market_class_code)s, %(exchange_code)s, %(issue_name)s,
                    %(stock_split_flag)s, %(revision_flag)s,
                    'finra_si', %(source_url)s, %(filed_at)s, %(period_end)s,
                    NOW(), %(ingest_run_id)s
                )
                ON CONFLICT (instrument_id, settlement_date, source_document_id)
                DO UPDATE SET
                    current_short_interest = EXCLUDED.current_short_interest,
                    previous_short_interest = EXCLUDED.previous_short_interest,
                    average_daily_volume = EXCLUDED.average_daily_volume,
                    days_to_cover = EXCLUDED.days_to_cover,
                    change_percent = EXCLUDED.change_percent,
                    change_previous = EXCLUDED.change_previous,
                    accounting_yearmonth = EXCLUDED.accounting_yearmonth,
                    market_class_code = EXCLUDED.market_class_code,
                    exchange_code = EXCLUDED.exchange_code,
                    issue_name = EXCLUDED.issue_name,
                    stock_split_flag = EXCLUDED.stock_split_flag,
                    revision_flag = EXCLUDED.revision_flag,
                    source_url = EXCLUDED.source_url,
                    filed_at = EXCLUDED.filed_at,
                    period_end = EXCLUDED.period_end,
                    known_from = NOW(),
                    ingest_run_id = EXCLUDED.ingest_run_id
                """,
                {
                    "instrument_id": instrument_id,
                    "settlement_date": settlement_date,
                    "source_document_id": source_document_id,
                    "current_short_interest": current_short_int,
                    "previous_short_interest": _opt_int(row.get("previousShortPositionQuantity")),
                    "average_daily_volume": _opt_int(row.get("averageDailyVolumeQuantity")),
                    "days_to_cover": _opt_decimal(row.get("daysToCoverQuantity")),
                    "change_percent": _opt_decimal(row.get("changePercent")),
                    "change_previous": _opt_int(row.get("changePreviousNumber")),
                    "accounting_yearmonth": _opt_int(row.get("accountingYearMonthNumber")),
                    "market_class_code": row.get("marketClassCode"),
                    "exchange_code": row.get("issuerServicesGroupExchangeCode"),
                    "issue_name": row.get("issueName"),
                    "stock_split_flag": row.get("stockSplitFlag") or "",
                    "revision_flag": row.get("revisionFlag") or "",
                    "source_url": file_url,
                    "filed_at": filed_at,
                    "period_end": settlement_date,
                    "ingest_run_id": ingest_run_id,
                },
            )

            cur.execute(
                """
                INSERT INTO finra_short_interest_current (
                    instrument_id, settlement_date, source_document_id,
                    current_short_interest, previous_short_interest,
                    average_daily_volume, days_to_cover, change_percent,
                    change_previous, market_class_code, exchange_code,
                    issue_name, source_url, filed_at, refreshed_at
                ) VALUES (
                    %(instrument_id)s, %(settlement_date)s, %(source_document_id)s,
                    %(current_short_interest)s, %(previous_short_interest)s,
                    %(average_daily_volume)s, %(days_to_cover)s, %(change_percent)s,
                    %(change_previous)s, %(market_class_code)s, %(exchange_code)s,
                    %(issue_name)s, %(source_url)s, %(filed_at)s, NOW()
                )
                ON CONFLICT (instrument_id) DO UPDATE SET
                    settlement_date = EXCLUDED.settlement_date,
                    source_document_id = EXCLUDED.source_document_id,
                    current_short_interest = EXCLUDED.current_short_interest,
                    previous_short_interest = EXCLUDED.previous_short_interest,
                    average_daily_volume = EXCLUDED.average_daily_volume,
                    days_to_cover = EXCLUDED.days_to_cover,
                    change_percent = EXCLUDED.change_percent,
                    change_previous = EXCLUDED.change_previous,
                    market_class_code = EXCLUDED.market_class_code,
                    exchange_code = EXCLUDED.exchange_code,
                    issue_name = EXCLUDED.issue_name,
                    source_url = EXCLUDED.source_url,
                    filed_at = EXCLUDED.filed_at,
                    refreshed_at = NOW()
                WHERE
                    EXCLUDED.settlement_date > finra_short_interest_current.settlement_date
                    OR (
                        EXCLUDED.settlement_date = finra_short_interest_current.settlement_date
                        AND NOW() > finra_short_interest_current.refreshed_at
                    )
                """,
                {
                    "instrument_id": instrument_id,
                    "settlement_date": settlement_date,
                    "source_document_id": source_document_id,
                    "current_short_interest": current_short_int,
                    "previous_short_interest": _opt_int(row.get("previousShortPositionQuantity")),
                    "average_daily_volume": _opt_int(row.get("averageDailyVolumeQuantity")),
                    "days_to_cover": _opt_decimal(row.get("daysToCoverQuantity")),
                    "change_percent": _opt_decimal(row.get("changePercent")),
                    "change_previous": _opt_int(row.get("changePreviousNumber")),
                    "market_class_code": row.get("marketClassCode"),
                    "exchange_code": row.get("issuerServicesGroupExchangeCode"),
                    "issue_name": row.get("issueName"),
                    "source_url": file_url,
                    "filed_at": filed_at,
                },
            )

            rows_upserted += 1

        # Manifest UPSERT — synthetic FINRA tuple per spec §7.3. Inside the
        # caller's open transaction; rolls back atomically with observations
        # if anything raises.
        #
        # NOT using ``record_manifest_entry`` + ``transition_status`` because
        # the transition path (pending → parsed) is asymmetric with the
        # revision-window re-fetch path (already-parsed re-write, which
        # would raise on ``parsed → parsed`` per ``_ALLOWED_TRANSITIONS``).
        # Manual UPSERT keeps the same idempotent semantics; the companion
        # ``seed_freshness_for_manifest_row()`` call below replicates the
        # freshness-index seeding that ``record_manifest_entry`` does
        # internally (Codex 2 r1 HIGH 1).
        cur.execute(
            """
            INSERT INTO sec_filing_manifest (
                accession_number, cik, form, source,
                subject_type, subject_id, instrument_id,
                filed_at, accepted_at, primary_document_url,
                is_amendment, amends_accession,
                ingest_status, parser_version, raw_status,
                last_attempted_at, next_retry_at, error
            ) VALUES (
                %(accession_number)s,
                'FINRA_SI',
                'SHRT',
                'finra_short_interest',
                'finra_universe',
                'FINRA_SI',
                NULL,
                %(filed_at)s,
                NULL,
                %(primary_document_url)s,
                FALSE,
                NULL,
                'parsed',
                %(parser_version)s,
                'stored',
                NOW(),
                NULL,
                NULL
            )
            ON CONFLICT (accession_number) DO UPDATE SET
                filed_at = EXCLUDED.filed_at,
                primary_document_url = EXCLUDED.primary_document_url,
                ingest_status = 'parsed',
                parser_version = EXCLUDED.parser_version,
                raw_status = 'stored',
                last_attempted_at = NOW(),
                next_retry_at = NULL,
                error = NULL
            """,
            {
                "accession_number": accession,
                "filed_at": filed_at,
                "primary_document_url": file_url,
                "parser_version": PARSER_VERSION,
            },
        )

    # Freshness-index seed — replicates the side-effect that
    # ``record_manifest_entry`` performs inline via the same helper
    # (sec_manifest.py:293-304). Without this call, the
    # ``(finra_universe, FINRA_SI, finra_short_interest)`` triple would
    # not be queryable from the freshness panel + scheduler view until
    # a bulk ``seed_scheduler_from_manifest`` ran. Codex 2 r1 HIGH 1.
    from app.services.data_freshness import seed_freshness_for_manifest_row

    seed_freshness_for_manifest_row(
        conn,
        subject_type="finra_universe",
        subject_id="FINRA_SI",
        source="finra_short_interest",
        cik="FINRA_SI",
        instrument_id=None,
        accession_number=accession,
        filed_at=filed_at,
    )

    return SettlementIngestStats(
        settlement_date=settlement_date,
        rows_parsed=rows_parsed,
        rows_resolved=rows_resolved,
        rows_upserted=rows_upserted,
        skipped_no_instrument_match=skipped_no_instrument_match,
        skipped_ambiguous_symbol=skipped_ambiguous_symbol,
        skipped_invalid_row=skipped_invalid_row,
    )
