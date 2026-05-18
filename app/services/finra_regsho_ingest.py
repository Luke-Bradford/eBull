"""FINRA RegSHO daily short volume service (#916 — Phase 6 PR 12).

Spec: docs/superpowers/specs/2026-05-18-finra-regsho-daily.md.
Plan: docs/superpowers/plans/2026-05-18-finra-regsho-daily-plan.md.

Parses pipe-delim daily files from the FINRA CDN, resolves symbol →
instrument_id via the preloaded resolver imported from the bimonthly
sibling, UPSERTs typed observations + writes the synthetic FINRA
manifest row + seeds freshness.

Transaction contract (#915 Codex 1b r1 HIGH 2 lesson):
``ingest_regsho_daily_file`` ACCEPTS a caller-supplied connection. It
NEVER calls ``conn.commit()`` / ``conn.rollback()`` AND DOES NOT enter
its own ``with conn.transaction():`` block. The caller MUST wrap the
call site in ``with conn.transaction():`` — clean exit commits;
exception triggers automatic rollback. The SAVEPOINT-vs-TOPLEVEL
ambiguity is avoided by construction: the SERVICE emits SQL only into
the caller's open transaction.

Raw-payload-before-parse contract (#1168) is JOB-enforced: the caller
MUST run ``raw_filings.store_raw(...)`` + ``conn.commit()`` BEFORE
calling this function.

Header/footer/body-date contract (spec §7.2):
- Header line must match ``_EXPECTED_HEADER`` exactly — else file-level
  ``HeaderCorruptionError``.
- Footer line is a single int (body row count); mismatch is structural
  defect, raises ``HeaderCorruptionError`` AFTER body iteration.
- Body row count of `0` is a SUCCESS path (FNRA legitimate-empty shape).
- Every body row's ``Date`` column MUST match the caller-supplied
  ``trade_date``; mismatch is file-level fatal — raises mid-body.

Manifest atomicity contract: the manifest UPSERT runs INSIDE the same
caller-owned ``with conn.transaction():`` block, AFTER the
observations writes. Atomic-with-the-data —
``manifest.ingest_status='parsed'`` always implies observations
durable.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

import psycopg

# Reused verbatim from the bimonthly sibling — same FINRA symbol
# normalisation convention (strip non-alnum + upper-case) + same
# resolver shape (closure with ``ambiguous_keys`` attribute).
from app.services.finra_short_interest_ingest import (
    HeaderCorruptionError,
    normalise_symbol,
)

logger = logging.getLogger(__name__)

PARSER_VERSION = "finra-regsho-daily-v1"

# Six-column pipe-delim header verified in spike §3.2. Header mismatch
# = file-level fatal (FINRA changed the column shape OR the file was
# truncated in transit).
_EXPECTED_HEADER: tuple[str, ...] = (
    "Date",
    "Symbol",
    "ShortVolume",
    "ShortExemptVolume",
    "TotalVolume",
    "Market",
)


@dataclass(frozen=True)
class RegShoDailyIngestStats:
    trade_date: date
    prefix: str
    rows_parsed: int = 0
    rows_resolved: int = 0
    rows_upserted: int = 0
    skipped_no_instrument_match: int = 0
    skipped_ambiguous_symbol: int = 0
    skipped_invalid_row: int = 0
    failed: bool = False
    error_detail: str | None = None


def _opt_decimal(v: str | None) -> Decimal | None:
    if v is None or v == "":
        return None
    try:
        return Decimal(v)
    except InvalidOperation, ValueError, TypeError:
        return None


def ingest_regsho_daily_file(
    conn: psycopg.Connection[Any],
    trade_date: date,
    prefix: str,
    raw_bytes: bytes,
    resolver: Callable[[str], int | None],
    ingest_run_id: UUID,
) -> RegShoDailyIngestStats:
    """Parse + UPSERT + manifest write. SQL-only — caller owns txn.

    See module docstring for the transaction + row-shape contracts.
    """
    text = raw_bytes.decode("utf-8")

    # Split on \n then strip optional trailing \r — handles both LF and
    # CRLF terminators (FINRA empirically uses CRLF; CDN edge-cases
    # have been observed using LF on some prefixes).
    lines = [ln.rstrip("\r") for ln in text.split("\n")]
    while lines and lines[-1] == "":
        lines.pop()
    if not lines:
        raise HeaderCorruptionError(f"RegSHO daily file empty: trade_date={trade_date} prefix={prefix}")

    # Header.
    header_cols = tuple(lines[0].split("|"))
    if header_cols != _EXPECTED_HEADER:
        raise HeaderCorruptionError(
            f"RegSHO header mismatch at trade_date={trade_date} prefix={prefix}: "
            f"expected {_EXPECTED_HEADER}, got {header_cols}"
        )

    # Footer.
    try:
        footer_int = int(lines[-1].strip())
    except ValueError as exc:
        raise HeaderCorruptionError(
            f"RegSHO footer missing/non-int at trade_date={trade_date} prefix={prefix}: last line={lines[-1]!r}"
        ) from exc

    body = lines[1:-1]

    # Empty body (FNRA shape) is a SUCCESS path — manifest row still
    # written so the audit trail records "fetched + parsed + zero rows".
    expected_date_str = trade_date.strftime("%Y%m%d")
    accession = f"FINRA_REGSHO_{prefix}_{expected_date_str}"
    file_url = f"https://cdn.finra.org/equity/regsho/daily/{prefix}shvol{expected_date_str}.txt"
    filed_at = datetime.combine(trade_date, datetime.min.time(), tzinfo=UTC)

    # Capture ambiguous-key set from the resolver attribute before the
    # loop (mirrors #915 finra_short_interest_ingest.py:194). The
    # closure returns None for both unknown AND ambiguous; this set
    # lets the row loop disambiguate which counter to bump.
    ambiguous_keys: frozenset[str] = getattr(resolver, "ambiguous_keys", frozenset())

    rows_parsed = 0
    rows_resolved = 0
    rows_upserted = 0
    skipped_no_instrument_match = 0
    skipped_ambiguous_symbol = 0
    skipped_invalid_row = 0

    with conn.cursor() as cur:
        for raw_line in body:
            # Bump BEFORE validation — every body line counts toward the
            # parsed/resolved ratio used in the JOB's match-rate
            # WARNING (Codex 1b r2 MED).
            rows_parsed += 1

            parts = raw_line.split("|")  # BARE split — no maxsplit.
            if len(parts) != 6:
                skipped_invalid_row += 1
                continue
            body_date_str, symbol_raw, short_vol_raw, short_exempt_raw, total_vol_raw, market = parts

            # Body-Date validation (spec §7.2 step 6) — file-level fatal.
            # A CDN path mistake or fixture seeded under the wrong date
            # would silently write facts under the caller's trade_date
            # while the body's date column is ignored. Raise so the
            # caller's txn rolls back.
            if body_date_str != expected_date_str:
                raise HeaderCorruptionError(
                    f"RegSHO body-date mismatch at trade_date={trade_date} "
                    f"prefix={prefix}: row date={body_date_str!r} != "
                    f"expected {expected_date_str!r}"
                )

            symbol = symbol_raw.strip()
            if not symbol:
                skipped_invalid_row += 1
                continue
            short_vol = _opt_decimal(short_vol_raw)
            short_exempt = _opt_decimal(short_exempt_raw)
            total_vol = _opt_decimal(total_vol_raw)
            if short_vol is None or short_exempt is None or total_vol is None:
                skipped_invalid_row += 1
                continue
            market_stripped = market.strip()
            if not market_stripped:
                skipped_invalid_row += 1
                continue

            # Ambiguity check BEFORE resolver call (resolver returns
            # None for both unknown + ambiguous; disambiguate for the
            # counter).
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
                INSERT INTO finra_regsho_daily_observations (
                    instrument_id, trade_date, market, source_document_id,
                    short_volume, short_exempt_volume, total_volume,
                    source, source_url, filed_at, period_end,
                    known_from, ingest_run_id
                ) VALUES (
                    %(instrument_id)s, %(trade_date)s, %(market)s, %(source_document_id)s,
                    %(short_volume)s, %(short_exempt_volume)s, %(total_volume)s,
                    'finra_regsho', %(source_url)s, %(filed_at)s, %(period_end)s,
                    NOW(), %(ingest_run_id)s
                )
                ON CONFLICT (instrument_id, trade_date, market, source_document_id)
                DO UPDATE SET
                    short_volume = EXCLUDED.short_volume,
                    short_exempt_volume = EXCLUDED.short_exempt_volume,
                    total_volume = EXCLUDED.total_volume,
                    source_url = EXCLUDED.source_url,
                    filed_at = EXCLUDED.filed_at,
                    period_end = EXCLUDED.period_end,
                    known_from = NOW(),
                    ingest_run_id = EXCLUDED.ingest_run_id
                """,
                {
                    "instrument_id": instrument_id,
                    "trade_date": trade_date,
                    "market": market_stripped,
                    "source_document_id": f"{prefix}_{expected_date_str}",
                    "short_volume": short_vol,
                    "short_exempt_volume": short_exempt,
                    "total_volume": total_vol,
                    "source_url": file_url,
                    "filed_at": filed_at,
                    "period_end": trade_date,
                    "ingest_run_id": ingest_run_id,
                },
            )

            rows_upserted += 1

        # Footer-row-count validation per spec §7.2 step 7. Compares
        # the body line count we iterated to the footer integer.
        # Mismatch = structural defect; raise inside the caller's txn
        # so the whole file rolls back atomically.
        if len(body) != footer_int:
            raise HeaderCorruptionError(
                f"RegSHO footer-count mismatch at trade_date={trade_date} "
                f"prefix={prefix}: parsed {len(body)} body rows, footer "
                f"says {footer_int}"
            )

        # Manifest UPSERT — synthetic FINRA tuple per spec §7.3.
        #
        # NOT using ``record_manifest_entry`` + ``transition_status``
        # because the transition path (pending → parsed) is asymmetric
        # with the revision-window re-fetch path (already-parsed
        # re-write, which would raise on ``parsed → parsed`` per
        # ``_ALLOWED_TRANSITIONS``). Manual UPSERT keeps the same
        # idempotent semantics; the companion
        # ``seed_freshness_for_manifest_row()`` call below replicates
        # the freshness-index seeding that ``record_manifest_entry``
        # does internally (#915 Codex 2 r1 HIGH 1 lesson).
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
                'FINRA_REGSHO',
                'REGSHO',
                'finra_regsho_daily',
                'finra_universe',
                'FINRA_REGSHO',
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
    # ``record_manifest_entry`` performs inline. Without this call, the
    # ``(finra_universe, FINRA_REGSHO, finra_regsho_daily)`` triple
    # would not be queryable from the freshness panel until a bulk
    # ``seed_scheduler_from_manifest`` ran. #915 Codex 2 r1 HIGH 1.
    from app.services.data_freshness import seed_freshness_for_manifest_row

    seed_freshness_for_manifest_row(
        conn,
        subject_type="finra_universe",
        subject_id="FINRA_REGSHO",
        source="finra_regsho_daily",
        cik="FINRA_REGSHO",
        instrument_id=None,
        accession_number=accession,
        filed_at=filed_at,
    )

    return RegShoDailyIngestStats(
        trade_date=trade_date,
        prefix=prefix,
        rows_parsed=rows_parsed,
        rows_resolved=rows_resolved,
        rows_upserted=rows_upserted,
        skipped_no_instrument_match=skipped_no_instrument_match,
        skipped_ambiguous_symbol=skipped_ambiguous_symbol,
        skipped_invalid_row=skipped_invalid_row,
    )
