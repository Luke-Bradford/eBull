"""SEC 13F-HR manifest-worker parser adapter (#873).

13F-HR is filed quarterly by every institutional manager with
discretionary AUM > $100M (15 USC 78m(f)). The bulk path
(``app/services/sec_13f_dataset_ingest.py``) ingests the quarterly
ZIP archive for historical drains — that's the workhorse. This thin
adapter handles atom-discovered freshness only: one accession at a
time, fetch + parse both XML attachments, upsert via the existing
``institutional_holdings`` primitives.

ParseOutcome contract:

  * ``status='parsed'`` + ``raw_status='stored'`` — both
    ``primary_doc.xml`` and ``infotable.xml`` persisted in
    ``filing_raw_documents``; ``institutional_filers`` upserted;
    ``institutional_holdings`` rows upserted per resolved CUSIP;
    ``unresolved_13f_cusips`` recorded for un-resolved holdings; one
    ``institutional_holdings_ingest_log`` row with status='success'
    or 'partial' (partial = at least one unresolved CUSIP, gated on
    #740 backfill).
  * ``status='tombstoned'`` — archive index 404 / both attachments
    missing / fetch returned empty body. Mirrors the legacy 'failed'
    accounting in the ingest log so dashboard counts converge.
  * ``status='failed'`` — transient error (fetch raise, store_raw
    error, transient psycopg ``OperationalError`` on upsert). Worker
    schedules a 1h backoff retry per ``_FAILED_RETRY_DELAY``.

Subject identity (sec-edgar §3.6): 13F-HR is filer-scoped — the
manifest row carries ``subject_type='institutional_filer'``,
``subject_id=<filer_cik>``, ``instrument_id=NULL``. Issuer linkage is
per-row by CUSIP inside the infotable, resolved at parse time via
``_resolve_cusip_to_instrument_id``.

Raw-payload invariant (#938): registered with
``requires_raw_payload=True``. Both attachments are stored in
savepoints BEFORE parse so the invariant holds whether parsing
succeeds or raises.

#1131 discrimination: transient psycopg ``OperationalError`` on the
filer/holdings upsert keeps the manifest in ``failed`` with the 1h
backoff; deterministic constraint violations write the audit-log
row with status='failed' and tombstone the manifest. See
``app/services/manifest_parsers/_classify.py``.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET  # noqa: S405 — only ET.ParseError caught; no untrusted parse.
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.sec_13f import (
    parse_infotable,
    parse_primary_doc,
)
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.services.institutional_holdings import (
    _PARSER_VERSION_13F_INFOTABLE,
    _PARSER_VERSION_13F_PRIMARY,
    ThirteenFHolding,
    _archive_file_url,
    _record_13f_observations_for_filing,
    _record_ingest_attempt,
    _record_unresolved_cusip,
    _resolve_cusip_to_instrument_id,
    _upsert_filer,
    _upsert_holding,
    parse_archive_index,
    thirteen_f_within_retention,
)
from app.services.manifest_parsers._classify import (
    format_upsert_error,
    is_transient_upsert_error,
)
from app.services.ownership_observations import refresh_institutions_current
from app.services.raw_filings import store_raw

logger = logging.getLogger(__name__)

_FAILED_RETRY_DELAY = timedelta(hours=1)
# Composite parser version — the manifest stores one ``parser_version``
# per row; the 13F path actually has two (primary_doc + infotable).
# Compose them so a rewash that bumps either triggers a re-parse.
_PARSER_VERSION_13F_HR = f"13f-hr-primary:{_PARSER_VERSION_13F_PRIMARY}+infotable:{_PARSER_VERSION_13F_INFOTABLE}"

# 13F-HR Column 4 (VALUE) unit cutover. SEC EDGAR Release 22.4.1
# switched VALUE from $thousands to whole $dollars effective
# 2023-01-03 (see sec-edgar skill §7.1). The bulk dataset path
# applies this at app/services/sec_13f_dataset_ingest.py:316-322;
# the per-filing parser at app/providers/implementations/sec_13f.py
# does NOT (it preserves the raw value), so the service layer must
# branch on ``filed_at`` — the moment the filer reported — NOT on
# ``period_of_report``. A 2022Q4 restatement filed in March 2023
# was entered by the filer in whole dollars under the new regime
# even though its period_end is pre-cutover; keying on filed_at
# treats it correctly, while keying on period_of_report would
# mis-scale by 1,000.
_VALUE_DOLLARS_CUTOVER = date(2023, 1, 3)


def _failed_outcome(error: str, raw_status: Any = None) -> Any:
    """Build a ``failed`` ParseOutcome with a 1h backoff applied."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    return ParseOutcome(
        status="failed",
        parser_version=_PARSER_VERSION_13F_HR,
        raw_status=raw_status,
        error=error,
        next_retry_at=datetime.now(tz=UTC) + _FAILED_RETRY_DELAY,
    )


def _parse_13f_hr(
    conn: psycopg.Connection[Any],
    row: Any,  # ManifestRow — forward-ref to avoid circular import
) -> Any:  # ParseOutcome — forward-ref
    """Manifest-worker parser for one 13F-HR / 13F-HR/A accession."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    accession = row.accession_number
    filer_cik = (row.cik or "").strip()

    if not filer_cik:
        logger.warning(
            "13F-HR manifest parser: accession=%s has no filer cik; tombstoning",
            accession,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_13F_HR,
            error="missing filer cik",
        )

    # Defense-in-depth (#1249): row.cik MUST NOT be a known filing-agent
    # CIK. Discovery for 13F-HR writes cik=<institutional_adviser_cik>
    # (curated cohort from institutional_filers); filing agents are never
    # in that cohort. A row whose cik resolves to an agent CIK means a
    # future discovery PR has a bug — every _archive_file_url call below
    # would 404 because SEC archives are not mounted under agent CIKs
    # (see sec_edgar.py:83-104 + fetch_filing_index agent-fallback at
    # sec_edgar.py:397-417). Fail loudly here rather than tombstone every
    # accession with a generic "archive 404" that masks the real bug.
    from app.providers.implementations.sec_edgar import (
        KNOWN_FILING_AGENT_CIKS,
        _zero_pad_cik,
    )

    padded_filer_cik = _zero_pad_cik(filer_cik)
    if padded_filer_cik in KNOWN_FILING_AGENT_CIKS:
        logger.warning(
            "13F-HR manifest parser: accession=%s row.cik=%s resolves to "
            "known filing-agent CIK %s — discovery should never enqueue "
            "agent CIKs as filer_cik (institutional_filers cohort excludes "
            "them). Tombstoning to surface the upstream discovery bug.",
            accession,
            filer_cik,
            padded_filer_cik,
        )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_13F_HR,
            error=f"row.cik is a known filing-agent CIK ({padded_filer_cik})",
        )

    # Step 1: archive index walk. Manifest's ``primary_document_url`` is
    # typically the filing-index page (or one of the attachments); we
    # need both ``primary_doc.xml`` and ``infotable.xml`` names which
    # come from ``index.json``. Same pattern as the legacy ingester
    # ``_ingest_single_accession``.
    base_url = _archive_file_url(filer_cik, accession, "")
    index_url = base_url + "index.json"

    try:
        with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
            index_payload = provider.fetch_document_text(index_url)
    except Exception as exc:  # noqa: BLE001 — transient fetch errors retry via backoff
        logger.warning(
            "13F-HR manifest parser: index fetch raised accession=%s url=%s: %s",
            accession,
            index_url,
            exc,
        )
        return _failed_outcome(f"fetch error: {exc}")

    if not index_payload:
        # 404 / non-200 on index.json — accession is unreachable.
        # Mirror legacy ingest-log 'failed' so dashboard counts match.
        try:
            with conn.transaction():
                _record_ingest_attempt(
                    conn,
                    filer_cik=filer_cik,
                    accession_number=accession,
                    period_of_report=None,
                    status="tombstoned",
                    error="archive index.json fetch failed",
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "13F-HR manifest parser: ingest-log INSERT failed accession=%s",
                accession,
            )
            return _failed_outcome(f"log error: {exc}")
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_13F_HR,
            error="archive index.json fetch failed",
        )

    primary_name, infotable_name = parse_archive_index(index_payload)
    if primary_name is None or infotable_name is None:
        # Index found but missing one of the two required attachments.
        # Deterministic — re-fetching the same index yields the same
        # gap (pre-2013 13Fs predate the 2013 infotable-XML mandate).
        # Tombstone with audit-log entry. The ingest-log row is written
        # 'tombstoned' (not 'failed') to MIRROR the ParseOutcome below,
        # so the ingest_sweep_adapter does not red sec_13f_sweep on a
        # non-actionable permanent skip (#1532). This is the dominant
        # log-failure class (~55k pre-2013 accessions).
        log_error = f"archive index missing files (primary={primary_name!r}, infotable={infotable_name!r})"
        try:
            with conn.transaction():
                _record_ingest_attempt(
                    conn,
                    filer_cik=filer_cik,
                    accession_number=accession,
                    period_of_report=None,
                    status="tombstoned",
                    error=log_error,
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "13F-HR manifest parser: ingest-log INSERT failed accession=%s",
                accession,
            )
            return _failed_outcome(f"log error: {exc}")
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_13F_HR,
            error=log_error,
        )

    primary_url = _archive_file_url(filer_cik, accession, primary_name)
    infotable_url = _archive_file_url(filer_cik, accession, infotable_name)

    # Step 2: fetch primary_doc.xml + store_raw inside savepoint.
    try:
        with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
            primary_xml = provider.fetch_document_text(primary_url)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "13F-HR manifest parser: primary_doc fetch raised accession=%s: %s",
            accession,
            exc,
        )
        return _failed_outcome(f"fetch error: {exc}")

    if not primary_xml:
        try:
            with conn.transaction():
                _record_ingest_attempt(
                    conn,
                    filer_cik=filer_cik,
                    accession_number=accession,
                    period_of_report=None,
                    status="tombstoned",
                    error="primary_doc.xml fetch failed",
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "13F-HR manifest parser: ingest-log INSERT failed accession=%s",
                accession,
            )
            return _failed_outcome(f"log error: {exc}")
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_13F_HR,
            error="primary_doc.xml fetch failed",
        )

    try:
        with conn.transaction():
            store_raw(
                conn,
                accession_number=accession,
                document_kind="primary_doc",
                payload=primary_xml,
                parser_version=_PARSER_VERSION_13F_PRIMARY,
                source_url=primary_url,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "13F-HR manifest parser: primary_doc store_raw failed accession=%s",
            accession,
        )
        return _failed_outcome(f"store_raw error: {exc}")

    # Parse primary_doc — single broad-except so unexpected raises
    # still ingest-log + return raw_status='stored' (rule from #1129).
    try:
        info = parse_primary_doc(primary_xml)
    except Exception as exc:  # noqa: BLE001
        is_schema = isinstance(exc, (ValueError, ET.ParseError))
        kind = "primary_doc.xml parse failed" if is_schema else "primary_doc.xml parse failed (unexpected)"
        logger.exception(
            "13F-HR manifest parser: primary_doc parse raised accession=%s (unexpected=%s)",
            accession,
            not is_schema,
        )
        try:
            with conn.transaction():
                _record_ingest_attempt(
                    conn,
                    filer_cik=filer_cik,
                    accession_number=accession,
                    period_of_report=None,
                    status="failed",
                    error=f"{kind}: {exc}",
                )
        except Exception:  # noqa: BLE001
            logger.exception(
                "13F-HR manifest parser: ingest-log INSERT failed after parse error accession=%s",
                accession,
            )
        return _failed_outcome(f"{kind}: {exc}", raw_status="stored")

    # PR6 #1233 §4.5 — post-parse 8-quarter retention gate. We gate on
    # ``info.period_of_report`` (the quarter end intrinsic to the
    # filing) NOT ``row.filed_at`` so 13F-HR/A amendments restating
    # pre-cap quarters are correctly rejected. Pre-fetch placement
    # (before infotable.xml fetch) saves the often-several-MB
    # attachment for pre-cap accessions. Tombstone the manifest so
    # operator's ``sec_rebuild`` is the recovery path if the cap
    # later widens; ingest-log row is written with the parsed period
    # for audit visibility.
    if not thirteen_f_within_retention(info.period_of_report):
        logger.debug(
            "13F-HR manifest parser: accession=%s period=%s pre-8q retention cap; tombstoning",
            accession,
            info.period_of_report,
        )
        try:
            with conn.transaction():
                _record_ingest_attempt(
                    conn,
                    filer_cik=filer_cik,
                    accession_number=accession,
                    period_of_report=info.period_of_report,
                    status="tombstoned",
                    error="retention floor",
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "13F-HR manifest parser: ingest-log INSERT failed accession=%s",
                accession,
            )
            return _failed_outcome(f"log error: {exc}", raw_status="stored")
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_13F_HR,
            raw_status="stored",
            error="retention floor",
        )

    # Step 3: fetch infotable.xml + store_raw inside savepoint.
    try:
        with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
            infotable_xml = provider.fetch_document_text(infotable_url)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "13F-HR manifest parser: infotable fetch raised accession=%s: %s",
            accession,
            exc,
        )
        return _failed_outcome(f"fetch error: {exc}", raw_status="stored")

    if not infotable_xml:
        try:
            with conn.transaction():
                _record_ingest_attempt(
                    conn,
                    filer_cik=filer_cik,
                    accession_number=accession,
                    period_of_report=info.period_of_report,
                    status="tombstoned",
                    error="infotable.xml fetch failed",
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "13F-HR manifest parser: ingest-log INSERT failed accession=%s",
                accession,
            )
            return _failed_outcome(f"log error: {exc}", raw_status="stored")
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_13F_HR,
            raw_status="stored",
            error="infotable.xml fetch failed",
        )

    try:
        with conn.transaction():
            store_raw(
                conn,
                accession_number=accession,
                document_kind="infotable_13f",
                payload=infotable_xml,
                parser_version=_PARSER_VERSION_13F_INFOTABLE,
                source_url=infotable_url,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "13F-HR manifest parser: infotable store_raw failed accession=%s",
            accession,
        )
        return _failed_outcome(f"store_raw error: {exc}", raw_status="stored")

    try:
        holdings: list[ThirteenFHolding] = parse_infotable(infotable_xml)
    except Exception as exc:  # noqa: BLE001
        is_schema = isinstance(exc, (ValueError, ET.ParseError))
        kind = "infotable.xml parse failed" if is_schema else "infotable.xml parse failed (unexpected)"
        logger.exception(
            "13F-HR manifest parser: infotable parse raised accession=%s (unexpected=%s)",
            accession,
            not is_schema,
        )
        try:
            with conn.transaction():
                _record_ingest_attempt(
                    conn,
                    filer_cik=filer_cik,
                    accession_number=accession,
                    period_of_report=info.period_of_report,
                    status="failed",
                    error=f"{kind}: {exc}",
                )
        except Exception:  # noqa: BLE001
            logger.exception(
                "13F-HR manifest parser: ingest-log INSERT failed after parse error accession=%s",
                accession,
            )
        return _failed_outcome(f"{kind}: {exc}", raw_status="stored")

    # Step 4: upsert filer + holdings + observations + ingest-log.
    # Single try block so any DB error during the batch returns
    # ``_failed_outcome(raw_status='stored')`` and stays consistent
    # with the manifest's view of stored raw bytes. #1131 discrimination
    # selects transient (retry) vs deterministic (tombstone + ingest-log).
    period = info.period_of_report
    filed_at = info.filed_at or (
        datetime(period.year, period.month, period.day, tzinfo=UTC) if period is not None else row.filed_at
    )
    inserted = 0
    skipped_no_cusip = 0
    skipped_non_sh = 0

    # VALUE cutover: pre-2023-01-03 filings report Column 4 in
    # thousands. The bulk dataset path applies this; the per-filing
    # parser preserves the raw value, so this adapter applies the
    # transform before passing to ``_upsert_holding``. Skipped if
    # filed_at unavailable (defensive — would only happen with a
    # SEC-supplied filing missing both header and submissions-index
    # timestamps, which the bulk path also fails closed on).
    needs_thousands_scaling = filed_at is not None and filed_at.date() < _VALUE_DOLLARS_CUTOVER

    try:
        with conn.transaction():
            filer_id = _upsert_filer(conn, info)

            # Dedupe by (instrument_id, exposure) to match the DB unique
            # key collapse — same pattern as the legacy ingester
            # (institutional_holdings.py:1302).
            resolved_by_key: dict[tuple[int, str], tuple[int, ThirteenFHolding]] = {}
            for holding in holdings:
                # Codex pre-push (#1133): drop PRN (bond principal)
                # rows so they do not silently land as shares. The
                # bulk dataset path filters at
                # sec_13f_dataset_ingest.py:311; per-filing path
                # historically did NOT, so this is a fix the manifest
                # adapter inherits. SSHPRNAMTTYPE values are 'SH'
                # (shares) or 'PRN' (principal-of-bonds, dollars).
                if holding.shares_or_principal_type != "SH":
                    skipped_non_sh += 1
                    continue

                # VALUE cutover transform (see _VALUE_DOLLARS_CUTOVER).
                if needs_thousands_scaling:
                    holding = replace(holding, value_usd=holding.value_usd * Decimal("1000"))

                instrument_id = _resolve_cusip_to_instrument_id(conn, holding.cusip)
                if instrument_id is None:
                    skipped_no_cusip += 1
                    _record_unresolved_cusip(
                        conn,
                        cusip=holding.cusip,
                        name_of_issuer=holding.name_of_issuer,
                        accession_number=accession,
                    )
                    continue
                if _upsert_holding(
                    conn,
                    filer_id=filer_id,
                    instrument_id=instrument_id,
                    accession_number=accession,
                    period_of_report=period,
                    filed_at=filed_at,
                    holding=holding,
                ):
                    inserted += 1
                exposure_key = holding.put_call if holding.put_call in ("PUT", "CALL") else "EQUITY"
                resolved_by_key.setdefault((instrument_id, exposure_key), (instrument_id, holding))

            resolved_holdings: list[tuple[int, ThirteenFHolding]] = list(resolved_by_key.values())
            if resolved_holdings:
                _record_13f_observations_for_filing(
                    conn,
                    filer_id=filer_id,
                    accession_number=accession,
                    period_of_report=period,
                    filed_at=filed_at,
                    resolved_holdings=resolved_holdings,
                )
                for unique_instrument_id in {iid for iid, _ in resolved_holdings}:
                    refresh_institutions_current(conn, instrument_id=unique_instrument_id)

            total_skipped = skipped_no_cusip + skipped_non_sh
            status = "partial" if total_skipped > 0 else "success"
            error_bits: list[str] = []
            if skipped_no_cusip > 0:
                error_bits.append(f"{skipped_no_cusip} unresolved CUSIPs (gated by #740 backfill)")
            if skipped_non_sh > 0:
                error_bits.append(f"{skipped_non_sh} PRN rows dropped (bond principal, not shares)")
            log_error = "; ".join(error_bits) if error_bits else None
            _record_ingest_attempt(
                conn,
                filer_cik=filer_cik,
                accession_number=accession,
                period_of_report=period,
                status=status,
                holdings_inserted=inserted,
                holdings_skipped=total_skipped,
                error=log_error,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "13F-HR manifest parser: upsert/observation batch failed accession=%s",
            accession,
        )
        if is_transient_upsert_error(exc):
            return _failed_outcome(format_upsert_error(exc), raw_status="stored")
        # Deterministic — tombstone the manifest (re-fetch won't fix a
        # constraint / programming defect) BUT keep the ingest-log row
        # 'failed', NOT 'tombstoned'. Unlike the pre-2013 archive-missing
        # skip (expected, non-actionable), an upsert defect is an
        # actionable code/data bug — the operator must still see it via
        # the sweep's last-run breadcrumb (#1532, Codex ckpt-2 HIGH).
        # This deliberate manifest=tombstoned / log=failed divergence
        # surfaces the defect without re-fetch-storming.
        try:
            with conn.transaction():
                _record_ingest_attempt(
                    conn,
                    filer_cik=filer_cik,
                    accession_number=accession,
                    period_of_report=period,
                    status="failed",
                    error=format_upsert_error(exc),
                )
        except Exception:  # noqa: BLE001
            logger.exception(
                "13F-HR manifest parser: ingest-log INSERT failed after upsert error accession=%s",
                accession,
            )
            return _failed_outcome(
                f"upsert+log error: {type(exc).__name__}: {exc}",
                raw_status="stored",
            )
        return ParseOutcome(
            status="tombstoned",
            parser_version=_PARSER_VERSION_13F_HR,
            raw_status="stored",
            error=format_upsert_error(exc),
        )

    return ParseOutcome(
        status="parsed",
        parser_version=_PARSER_VERSION_13F_HR,
        raw_status="stored",
    )


def register() -> None:
    """Register the 13F-HR parser with the manifest worker.

    Idempotent — ``register_parser`` is last-write-wins. The bulk
    quarterly path (``sec_13f_dataset_ingest``) continues to handle
    historical drains in parallel; the manifest path drives
    atom-discovered freshness one accession at a time.
    """
    from app.jobs.sec_manifest_worker import register_parser

    register_parser("sec_13f_hr", _parse_13f_hr, requires_raw_payload=True)
