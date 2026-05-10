"""C2 — bulk companyfacts.zip ingester (#1022).

Reads the cached ``companyfacts.zip`` archive (downloaded by Phase A3,
#1021) and writes XBRL facts into ``financial_facts_raw`` for every
CIK-mapped instrument in the universe.

Replaces the per-CIK ``/api/xbrl/companyfacts/CIK<10>.json`` HTTP walk
that S16 (`fundamentals_sync`) issues at 7 req/s on a fresh install.
The archive payload shape is identical to the per-CIK API response,
so the existing parser chain is reused unchanged:

  per-CIK JSON
    -> _extract_facts_from_section(gaap_section, taxonomy="us-gaap")
    -> _extract_facts_from_section(dei_section, taxonomy="dei")
    -> upsert_facts_for_instrument(conn, instrument_id, facts, ingestion_run_id)

Spec: docs/superpowers/specs/2026-05-08-bulk-datasets-first-bootstrap.md
"""

from __future__ import annotations

import json
import logging
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg

from app.providers.fundamentals import XbrlFact
from app.providers.implementations.sec_fundamentals import _extract_facts_from_section
from app.services.fundamentals import (
    finish_ingestion_run,
    start_ingestion_run,
    upsert_facts_for_instrument,
)

logger = logging.getLogger(__name__)


_CIK_FILENAME_RE = re.compile(r"^CIK(\d{10})\.json$")


class _SkipEntry(Exception):
    """Sentinel raised inside a per-entry savepoint to roll it back."""


@dataclass
class CompanyFactsIngestResult:
    """Per-archive ingest outcome."""

    archive_entries_seen: int
    instruments_matched: int
    facts_upserted: int
    facts_skipped: int
    archive_entries_skipped_universe_gap: int = 0
    parse_errors: int = 0
    ingestion_run_id: int | None = None


def extract_facts_from_companyfacts_payload(
    payload: dict[str, Any],
) -> list[XbrlFact]:
    """Public wrapper around the existing per-section extractor.

    Routes the ``us-gaap`` + ``dei`` sections through
    ``_extract_facts_from_section`` (the canonical extractor used by
    the per-CIK API path) and returns one flat ``list[XbrlFact]``.

    Wrapping the private helper here means C2 reuses the per-CIK
    parser unchanged — same field semantics, same Decimal handling,
    same NaN/Infinity guards. The wrapper exists because the existing
    public ``extract_facts(symbol, cik)`` method on the provider
    self-fetches over HTTP; for bulk ingest we have the payload
    already.
    """
    raw_facts: dict[str, Any] = payload.get("facts", {})
    gaap_section = raw_facts.get("us-gaap", {})
    dei_section = raw_facts.get("dei", {})
    facts: list[XbrlFact] = []
    if gaap_section:
        facts.extend(_extract_facts_from_section(gaap_section, taxonomy="us-gaap"))
    if dei_section:
        facts.extend(_extract_facts_from_section(dei_section, taxonomy="dei"))
    return facts


def _cik_from_filename(name: str) -> str | None:
    """Parse the 10-digit CIK out of a ``CIK<10>.json`` archive entry name."""
    m = _CIK_FILENAME_RE.match(name)
    return m.group(1) if m else None


def _load_cik_to_instrument(conn: psycopg.Connection[Any]) -> dict[str, list[int]]:
    """Return ``{cik_padded: [instrument_id, ...]}`` for every CIK-mapped instrument.

    Multimap shape — share-class siblings (GOOG/GOOGL, BRK.A/BRK.B) co-bind
    a single SEC CIK per #1102. Collapsing to ``dict[str, int]`` would
    silently drop one sibling on every bulk run, leaving it without
    fundamentals.
    """
    out: dict[str, list[int]] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT instrument_id, identifier_value
            FROM external_identifiers
            WHERE provider = 'sec' AND identifier_type = 'cik'
            """,
        )
        for row in cur.fetchall():
            instrument_id, identifier = row
            cik = str(identifier).zfill(10)
            out.setdefault(cik, []).append(int(instrument_id))
    return out


def ingest_companyfacts_archive(
    *,
    conn: psycopg.Connection[Any],
    archive_path: Path,
    cik_to_instrument: dict[str, list[int]] | None = None,
) -> CompanyFactsIngestResult:
    """Walk every ``CIK<10>.json`` entry in ``archive_path`` and upsert
    XBRL facts into ``financial_facts_raw`` for matching universe instruments.

    Returns a summary suitable for stage telemetry. Per-entry parse
    errors are counted, not raised. The caller (orchestrator) commits
    the surrounding transaction so all facts ingested in this archive
    are atomically visible.
    """
    if cik_to_instrument is None:
        cik_to_instrument = _load_cik_to_instrument(conn)

    # Commit the start row IMMEDIATELY so a later fatal error +
    # rollback does not erase it. Without this, the terminal-status
    # update in the except branch would target a row that no longer
    # exists (Codex pre-push round 1, finding 2). The provenance
    # contract on data_ingestion_runs requires every run to land —
    # success, partial, or failed — so callers can audit retries.
    run_id = start_ingestion_run(
        conn,
        source="sec_companyfacts_bulk",
        endpoint=str(archive_path.name),
        instrument_count=len(cik_to_instrument),
    )
    conn.commit()

    result = CompanyFactsIngestResult(
        archive_entries_seen=0,
        instruments_matched=0,
        facts_upserted=0,
        facts_skipped=0,
        ingestion_run_id=run_id,
    )

    try:
        with zipfile.ZipFile(archive_path) as zf:
            for entry_name in zf.namelist():
                cik = _cik_from_filename(entry_name)
                if cik is None:
                    continue
                result.archive_entries_seen += 1
                matched_instruments = cik_to_instrument.get(cik, [])
                if not matched_instruments:
                    result.archive_entries_skipped_universe_gap += 1
                    continue

                # Per-CIK savepoint: a parse error or DB-write failure
                # for one CIK must not abort the surrounding
                # transaction. Without this, a single corrupted entry
                # poisons the rest of the archive (Codex pre-push
                # round 1, finding 3). Share-class siblings on the
                # same CIK share one savepoint — failure rolls back
                # all sibling writes for that entry together (#1117).
                try:
                    with conn.transaction():
                        try:
                            with zf.open(entry_name) as fh:
                                payload: dict[str, Any] = json.load(fh)
                        except (json.JSONDecodeError, KeyError) as exc:
                            logger.debug(
                                "companyfacts ingest: bad payload for %s: %s",
                                entry_name,
                                exc,
                            )
                            result.parse_errors += 1
                            raise _SkipEntry from exc

                        facts = extract_facts_from_companyfacts_payload(payload)
                        if not facts:
                            raise _SkipEntry  # roll back savepoint cleanly

                        for instrument_id in matched_instruments:
                            result.instruments_matched += 1
                            upserted, skipped = upsert_facts_for_instrument(
                                conn,
                                instrument_id=instrument_id,
                                facts=facts,
                                ingestion_run_id=run_id,
                            )
                            result.facts_upserted += upserted
                            result.facts_skipped += skipped
                except _SkipEntry:
                    # Savepoint already rolled back; advance to next entry.
                    continue
                except Exception as exc:  # noqa: BLE001
                    # Per-CIK DB error or unexpected fault — savepoint
                    # rolled back; record + continue. Do NOT promote to
                    # archive-fatal: one bad CIK must not kill the run.
                    logger.warning(
                        "companyfacts ingest: per-CIK failure for %s: %s",
                        entry_name,
                        exc,
                    )
                    result.parse_errors += 1
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        finish_ingestion_run(
            conn,
            run_id=run_id,
            status="failed",
            rows_upserted=result.facts_upserted,
            rows_skipped=result.facts_skipped,
            error=str(exc),
        )
        conn.commit()
        raise

    terminal_status = "partial" if result.parse_errors else "success"
    finish_ingestion_run(
        conn,
        run_id=run_id,
        status=terminal_status,
        rows_upserted=result.facts_upserted,
        rows_skipped=result.facts_skipped,
    )
    conn.commit()
    return result
