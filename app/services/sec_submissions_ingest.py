"""C1.a — bulk submissions.zip ingester (#1022).

Reads the cached ``submissions.zip`` archive (downloaded by Phase A3,
#1021) and seeds two tables for every CIK-mapped instrument in the
universe:

1. ``filing_events`` — the recent block (`filings.recent`) becomes
   ``provider='sec'`` rows keyed on the SEC accession number.
2. ``instrument_sec_profile`` — the JSON top-level fields (sic,
   ownerOrg, addresses, exchanges, etc) flow through the existing
   ``parse_entity_profile()`` + ``upsert_entity_profile()`` helpers.

This replaces the per-CIK HTTP walk that S5 (``filings_history_seed``;
formerly the bespoke ``bootstrap_filings_history_seed`` wrapper, lifted
in PR1c #1064) issues at 7 req/s on a fresh install. Per-CIK ``filings.files[]``
secondary-page coverage is the responsibility of C1.b, a separate
stage.

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

from app.providers.implementations.sec_edgar import _normalise_submissions_block
from app.services.filings import _upsert_filing
from app.services.sec_entity_profile import parse_entity_profile, upsert_entity_profile

logger = logging.getLogger(__name__)


_CIK_FILENAME_RE = re.compile(r"^CIK(\d{10})\.json$")


class _SkipEntry(Exception):
    """Sentinel raised inside a per-entry savepoint to roll it back."""


@dataclass
class SubmissionsIngestResult:
    """Per-archive ingest outcome."""

    archive_entries_seen: int
    instruments_matched: int
    filings_upserted: int
    profiles_upserted: int
    archive_entries_skipped: int = 0
    parse_errors: int = 0


def _cik_from_filename(name: str) -> str | None:
    """Parse the 10-digit CIK out of a ``CIK<10>.json`` archive entry name."""
    m = _CIK_FILENAME_RE.match(name)
    return m.group(1) if m else None


def _load_cik_to_instrument(
    conn: psycopg.Connection[Any],
) -> dict[str, list[tuple[int, str]]]:
    """Return ``{cik_padded: [(instrument_id, symbol), ...]}`` for every
    CIK-mapped instrument.

    Reads ``external_identifiers`` SEC CIK rows joined to ``instruments``
    so the writer below can pass the canonical ticker symbol — not a
    stringified instrument_id — to ``_normalise_submissions_block`` and
    ``_upsert_filing`` (Codex review BLOCKING for PR #1030).

    Multimap shape — share-class siblings (GOOG/GOOGL, BRK.A/BRK.B)
    co-bind a single SEC CIK per #1102. Collapsing to
    ``dict[str, tuple[int, str]]`` would silently drop one sibling on
    every bulk run, leaving it without filings or entity profile.
    """
    out: dict[str, list[tuple[int, str]]] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ei.instrument_id, ei.identifier_value, i.symbol
            FROM external_identifiers ei
            JOIN instruments i ON i.instrument_id = ei.instrument_id
            WHERE ei.provider = 'sec' AND ei.identifier_type = 'cik'
            """,
        )
        for row in cur.fetchall():
            instrument_id, identifier, symbol = row
            cik = str(identifier).zfill(10)
            out.setdefault(cik, []).append((int(instrument_id), str(symbol or "")))
    return out


def ingest_submissions_archive(
    *,
    conn: psycopg.Connection[Any],
    archive_path: Path,
    cik_to_instrument: dict[str, list[tuple[int, str]]] | None = None,
) -> SubmissionsIngestResult:
    """Walk every ``CIK<10>.json`` entry in ``archive_path`` and upsert
    matching universe instruments into ``filing_events`` + ``instrument_sec_profile``.

    Returns a summary suitable for stage telemetry. Per-entry parse
    errors are counted, not raised — one bad CIK file in a 5-million-row
    archive must not block the rest. The bulk archive is treated as
    a soft source: corrupted entries are logged at DEBUG and counted.
    """
    if cik_to_instrument is None:
        cik_to_instrument = _load_cik_to_instrument(conn)

    result = SubmissionsIngestResult(
        archive_entries_seen=0,
        instruments_matched=0,
        filings_upserted=0,
        profiles_upserted=0,
    )

    with zipfile.ZipFile(archive_path) as zf:
        for entry_name in zf.namelist():
            cik = _cik_from_filename(entry_name)
            if cik is None:
                # Sub-CIK secondary pages are not in this archive; the
                # only valid entries are CIK<10>.json. Anything else is
                # noise we silently skip.
                continue
            result.archive_entries_seen += 1
            matched_instruments = cik_to_instrument.get(cik, [])
            if not matched_instruments:
                # Universe gap — most CIKs in the archive are not in
                # our universe. This is expected, not an error.
                result.archive_entries_skipped += 1
                continue

            # Per-CIK savepoint: a parse error or DB-write failure for
            # one CIK must not abort the surrounding transaction or
            # block the rest of the archive (Codex pre-push round 1,
            # finding 3). Share-class siblings on the same CIK share
            # one savepoint — failure rolls back all sibling writes
            # for that entry together (#1117).
            try:
                with conn.transaction():
                    try:
                        with zf.open(entry_name) as fh:
                            payload: dict[str, Any] = json.load(fh)
                    except (json.JSONDecodeError, KeyError) as exc:
                        logger.debug("submissions ingest: bad payload for %s: %s", entry_name, exc)
                        result.parse_errors += 1
                        raise _SkipEntry from exc

                    for instrument_id, symbol in matched_instruments:
                        result.instruments_matched += 1
                        _ingest_one(
                            conn,
                            instrument_id=instrument_id,
                            cik_padded=cik,
                            symbol=symbol,
                            payload=payload,
                            result=result,
                        )
            except _SkipEntry:
                continue
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "submissions ingest: per-CIK failure for %s: %s",
                    entry_name,
                    exc,
                )
                result.parse_errors += 1
    return result


def _ingest_one(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    cik_padded: str,
    symbol: str,
    payload: dict[str, Any],
    result: SubmissionsIngestResult,
) -> None:
    """Upsert one CIK's submissions payload — filings + profile.

    ``symbol`` is the ticker for ``instrument_id``, threaded through
    so ``_normalise_submissions_block`` and ``_upsert_filing`` write
    the canonical symbol on every ``filing_events`` row instead of a
    stringified instrument id (Codex review BLOCKING for PR #1030).
    """
    profile = parse_entity_profile(payload, instrument_id=instrument_id, cik=cik_padded)
    upsert_entity_profile(conn, profile)
    result.profiles_upserted += 1

    filings_block = payload.get("filings")
    if not isinstance(filings_block, dict):
        return
    recent = filings_block.get("recent")
    if not isinstance(recent, dict):
        return

    # Reuse the existing per-CIK normaliser. It returns a list of
    # ``FilingSearchResult`` ordered oldest-first. The ``symbol``
    # parameter is the ticker (e.g. "AAPL") — passing the
    # stringified instrument_id here would corrupt every
    # ``filing_events`` row. Codex review BLOCKING for PR #1030.
    filings = _normalise_submissions_block(
        recent,
        cik_padded=cik_padded,
        symbol=symbol or cik_padded,
    )
    for filing in filings:
        _upsert_filing(conn, str(instrument_id), "sec", filing)
        result.filings_upserted += 1
