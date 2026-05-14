"""sec_n_csr real manifest-worker parser (#1171).

REPLACES the #918 / PR #1170 synth no-op with iXBRL extraction of fund-
level + class-level metadata. Spec:
``docs/superpowers/specs/2026-05-14-n-csr-fund-metadata.md``.
Plan:
``docs/superpowers/specs/2026-05-14-n-csr-fund-metadata-plan.md``.

Flow (spec §8):

1. Validate URL.
2. Resolve iXBRL companion URL from the manifest's primary document URL.
3. Fetch iXBRL companion via ``SecFilingsProvider``.
4. Parse via :func:`app.services.n_csr_extractor.extract_fund_metadata_facts`
   — returns one :class:`FundMetadataFacts` per (series_id, class_id).
5. For each class, resolve class_id → instrument_id via
   :func:`_fund_class_resolver.resolve_class_id_to_instrument`. On miss,
   :func:`classify_resolver_miss` discriminates pending_cik_refresh
   (transient) / ext_id_not_yet_written (transient) /
   instrument_not_in_universe (deterministic).
6. Inside one transaction per resolved class: soft-supersede prior rows
   for the same ``(instrument_id, source_accession)`` (``known_to=NOW()``),
   INSERT a fresh observation, and call
   :func:`refresh_fund_metadata_current`.

ParseOutcome aggregation:

- At least one class resolved + written → ``parsed`` (full or partial
  success per spec §7.4 partial-success rule).
- Zero classes resolved + unanimous miss-reason (all
  ``INSTRUMENT_NOT_IN_UNIVERSE``) → ``tombstoned`` with that reason.
- Zero classes resolved + mixed miss-reasons OR any transient miss →
  ``failed`` with 1h backoff (next tick re-classifies).

Raw-payload invariant (#938): registered with
``requires_raw_payload=False`` per operator choice (spec §2). Re-parse
on parser-version bump re-fetches iXBRL from SEC.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from urllib.parse import urlsplit

import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.services.fund_metadata import refresh_fund_metadata_current
from app.services.manifest_parsers._fund_class_resolver import (
    ResolverMissReason,
    classify_resolver_miss,
    resolve_class_id_to_instrument,
)
from app.services.n_csr_extractor import (
    FundMetadataFacts,
    extract_fund_metadata_facts,
)

logger = logging.getLogger(__name__)

_PARSER_VERSION_N_CSR = "n-csr-fund-metadata-v1"

# 1h backoff for transient failures — mirror eight_k.py + sec_10k.py.
_FAILED_RETRY_DELAY = timedelta(hours=1)
# 24h backoff for resolver-pending miss (gives daily cik_refresh time).
_PENDING_CIK_REFRESH_DELAY = timedelta(hours=24)


def _failed_outcome(error: str, *, delay: timedelta = _FAILED_RETRY_DELAY) -> Any:
    """Build a ``failed`` ParseOutcome with retry backoff."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    return ParseOutcome(
        status="failed",
        parser_version=_PARSER_VERSION_N_CSR,
        error=error,
        next_retry_at=datetime.now(tz=UTC) + delay,
    )


def _tombstoned(reason: str) -> Any:
    from app.jobs.sec_manifest_worker import ParseOutcome

    return ParseOutcome(
        status="tombstoned",
        parser_version=_PARSER_VERSION_N_CSR,
        error=reason,
    )


def _ixbrl_companion_url(primary_doc_url: str) -> str:
    """Derive the iXBRL companion URL from the primary document URL.

    Spike §3.3 confirms the convention: ``<basename>_htm.xml`` lives in
    the same accession folder as the primary primary_doc.htm.
    """
    parts = urlsplit(primary_doc_url)
    path = parts.path
    if path.endswith(".htm"):
        base = path[: -len(".htm")]
    elif path.endswith(".html"):
        base = path[: -len(".html")]
    else:
        base = path
    ixbrl_path = f"{base}_htm.xml"
    return f"{parts.scheme}://{parts.netloc}{ixbrl_path}"


def _fetch_ixbrl(url: str) -> bytes | None:
    with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
        # ``fetch_document_text`` returns str; iXBRL is XML — we encode to bytes
        # for lxml. Acceptable: iXBRL is small (<6 MB in spike sample) and the
        # encode roundtrip preserves UTF-8 correctness.
        text = provider.fetch_document_text(url)
        if not text:
            return None
        return text.encode("utf-8")


def _json_serializer(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Type {type(value).__name__} not JSON serialisable")


def _jsonify(payload: Any) -> str | None:
    """Coerce a dict/list/None into a JSONB-compatible JSON string."""
    if payload is None:
        return None
    if isinstance(payload, dict) and not payload:
        return None
    if isinstance(payload, list) and not payload:
        return None
    return json.dumps(payload, default=_json_serializer)


def _write_observation(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    facts: FundMetadataFacts,
    accession: str,
    filed_at: datetime,
) -> None:
    """Inside one savepoint: supersede prior rows + INSERT fresh observation
    + refresh _current."""
    with conn.transaction():
        # Soft-delete prior currently-valid rows for this (instrument, accession).
        conn.execute(
            """
            UPDATE fund_metadata_observations
            SET known_to = NOW()
            WHERE instrument_id = %(instrument_id)s
              AND source_accession = %(accession)s
              AND known_to IS NULL
            """,
            {"instrument_id": instrument_id, "accession": accession},
        )

        # Resolve period_end from extracted facts; fall back to manifest row's
        # filed_at date if absent (very rare; iXBRL should always carry it).
        period_end = facts.period_end or filed_at.date()

        conn.execute(
            """
            INSERT INTO fund_metadata_observations (
                instrument_id, source_accession, filed_at, period_end,
                document_type, amendment_flag, parser_version,
                trust_cik, trust_name, entity_inv_company_type,
                series_id, series_name, class_id, class_name,
                trading_symbol, exchange, inception_date, shareholder_report_type,
                expense_ratio_pct, expenses_paid_amt, net_assets_amt,
                advisory_fees_paid_amt, portfolio_turnover_pct, holdings_count,
                returns_pct, benchmark_returns_pct, sector_allocation,
                region_allocation, credit_quality_allocation, growth_curve,
                material_chng_date, material_chng_notice,
                contact_phone, contact_website, contact_email,
                prospectus_phone, prospectus_website, prospectus_email,
                raw_facts
            ) VALUES (
                %(instrument_id)s, %(accession)s, %(filed_at)s, %(period_end)s,
                %(document_type)s, %(amendment_flag)s, %(parser_version)s,
                %(trust_cik)s, %(trust_name)s, %(entity_inv_company_type)s,
                %(series_id)s, %(series_name)s, %(class_id)s, %(class_name)s,
                %(trading_symbol)s, %(exchange)s, %(inception_date)s, %(shareholder_report_type)s,
                %(expense_ratio_pct)s, %(expenses_paid_amt)s, %(net_assets_amt)s,
                %(advisory_fees_paid_amt)s, %(portfolio_turnover_pct)s, %(holdings_count)s,
                %(returns_pct)s, %(benchmark_returns_pct)s, %(sector_allocation)s,
                %(region_allocation)s, %(credit_quality_allocation)s, %(growth_curve)s,
                %(material_chng_date)s, %(material_chng_notice)s,
                %(contact_phone)s, %(contact_website)s, %(contact_email)s,
                %(prospectus_phone)s, %(prospectus_website)s, %(prospectus_email)s,
                %(raw_facts)s
            )
            """,
            {
                "instrument_id": instrument_id,
                "accession": accession,
                "filed_at": filed_at,
                "period_end": period_end,
                "document_type": facts.document_type or "N-CSR",
                "amendment_flag": facts.amendment_flag,
                "parser_version": _PARSER_VERSION_N_CSR,
                "trust_cik": facts.trust_cik,
                "trust_name": facts.trust_name,
                "entity_inv_company_type": facts.entity_inv_company_type,
                "series_id": facts.series_id,
                "series_name": facts.series_name,
                "class_id": facts.class_id,
                "class_name": facts.class_name,
                "trading_symbol": facts.trading_symbol,
                "exchange": facts.exchange,
                "inception_date": facts.inception_date,
                "shareholder_report_type": facts.shareholder_report_type,
                "expense_ratio_pct": facts.expense_ratio_pct,
                "expenses_paid_amt": facts.expenses_paid_amt,
                "net_assets_amt": facts.net_assets_amt,
                "advisory_fees_paid_amt": facts.advisory_fees_paid_amt,
                "portfolio_turnover_pct": facts.portfolio_turnover_pct,
                "holdings_count": facts.holdings_count,
                "returns_pct": _jsonify(facts.returns_pct),
                "benchmark_returns_pct": _jsonify(facts.benchmark_returns_pct),
                "sector_allocation": _jsonify(facts.sector_allocation),
                "region_allocation": _jsonify(facts.region_allocation),
                "credit_quality_allocation": _jsonify(facts.credit_quality_allocation),
                "growth_curve": _jsonify(facts.growth_curve),
                "material_chng_date": facts.material_chng_date,
                "material_chng_notice": facts.material_chng_notice,
                "contact_phone": facts.contact_phone,
                "contact_website": facts.contact_website,
                "contact_email": facts.contact_email,
                "prospectus_phone": facts.prospectus_phone,
                "prospectus_website": facts.prospectus_website,
                "prospectus_email": facts.prospectus_email,
                "raw_facts": _jsonify(facts.raw_facts),
            },
        )

        refresh_fund_metadata_current(conn, instrument_id=instrument_id)


def _parse_sec_n_csr(
    conn: psycopg.Connection[Any],
    row: Any,  # ManifestRow — forward-ref to avoid circular import
) -> Any:  # ParseOutcome
    """Real parser per spec §8.

    See module docstring for the full flow + outcome rules.
    """
    from app.jobs.sec_manifest_worker import ParseOutcome

    accession = row.accession_number
    url = row.primary_document_url
    filed_at = row.filed_at

    if not url:
        return _tombstoned("missing primary_document_url")

    # 1. Fetch iXBRL companion.
    ixbrl_url = _ixbrl_companion_url(url)
    try:
        ixbrl_bytes = _fetch_ixbrl(ixbrl_url)
    except Exception as exc:  # noqa: BLE001 — transient fetch errors retry
        logger.warning("sec_n_csr fetch raised accession=%s url=%s: %s", accession, ixbrl_url, exc)
        return _failed_outcome(f"fetch error: {exc}")

    if not ixbrl_bytes:
        return _tombstoned("empty or non-200 fetch")

    # 2. Extract.
    try:
        facts_list = extract_fund_metadata_facts(ixbrl_bytes)
    except ValueError as exc:
        # Deterministic parse failure — tombstone after one retry.
        logger.warning("sec_n_csr parse failed accession=%s: %s", accession, exc)
        return _tombstoned(f"parse error: {exc}")
    except Exception as exc:  # noqa: BLE001 — defensive
        logger.exception("sec_n_csr parse raised accession=%s", accession)
        return _failed_outcome(f"parse error: {exc}")

    if not facts_list:
        return _tombstoned("no classes extracted")

    # 3. Per-class fan-out.
    written = 0
    miss_reasons: list[ResolverMissReason] = []
    for facts in facts_list:
        instrument_id = resolve_class_id_to_instrument(conn, facts.class_id)
        if instrument_id is None:
            reason = classify_resolver_miss(conn, facts.class_id)
            miss_reasons.append(reason)
            logger.info(
                "sec_n_csr resolver miss accession=%s class_id=%s reason=%s",
                accession,
                facts.class_id,
                reason.value,
            )
            continue

        try:
            _write_observation(
                conn,
                instrument_id=instrument_id,
                facts=facts,
                accession=accession,
                filed_at=filed_at,
            )
            written += 1
        except Exception as exc:  # noqa: BLE001 — defensive; classify_db_error happens in #1131 helpers
            logger.exception(
                "sec_n_csr DB write failed accession=%s class_id=%s instrument_id=%s",
                accession,
                facts.class_id,
                instrument_id,
            )
            return _failed_outcome(f"DB write error: {exc}")

    # 4. Aggregate outcome.
    if written > 0:
        return ParseOutcome(status="parsed", parser_version=_PARSER_VERSION_N_CSR)

    # Zero resolved — discriminate by miss-reason unanimity.
    if all(r == ResolverMissReason.INSTRUMENT_NOT_IN_UNIVERSE for r in miss_reasons):
        return _tombstoned(ResolverMissReason.INSTRUMENT_NOT_IN_UNIVERSE.value)

    # Any transient miss → failed (24h backoff to give cik_refresh time).
    has_pending = any(
        r in {ResolverMissReason.PENDING_CIK_REFRESH, ResolverMissReason.EXT_ID_NOT_YET_WRITTEN} for r in miss_reasons
    )
    if has_pending:
        return _failed_outcome(
            "resolver pending cik_refresh — no in-universe classes yet",
            delay=_PENDING_CIK_REFRESH_DELAY,
        )

    return _tombstoned("no resolvable classes")


def register() -> None:
    """Register the parser with the manifest worker. Idempotent."""
    from app.jobs.sec_manifest_worker import register_parser

    register_parser("sec_n_csr", _parse_sec_n_csr, requires_raw_payload=False)
