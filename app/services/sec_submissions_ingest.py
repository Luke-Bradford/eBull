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

from app.providers.implementations.sec_edgar import (
    KNOWN_FILING_AGENT_CIKS,
    _normalise_submissions_block,
)
from app.services.filings import _upsert_filing
from app.services.sec_entity_profile import parse_entity_profile, upsert_entity_profile

logger = logging.getLogger(__name__)

__all__ = (
    "SubmissionsIngestResult",
    "ingest_submissions_archive",
    "refresh_cik_sidecar",
    "repair_cik_sidecar_from_archive",
)


# Stream A PR-B T1.3 (#1233): sentinel row inserted into
# ``sec_cik_submissions_files_index`` for CIKs with zero overflow
# pages. Distinguishes "CIK processed; no overflow" from "CIK not
# yet populated". S14 + the Stream-C C7 gate honour this explicitly
# — see sql/172 header + spec §4.
_SIDECAR_SENTINEL_PAGE_NAME: str = "__no_overflow_pages__"


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
    # Stream A PR-B T1.3 (#1233): per-archive sidecar telemetry.
    # ``ciks_sidecared`` counts CIKs for which we wrote ≥ 1 sidecar
    # row (real-page rows OR a sentinel — agent CIKs are excluded
    # by filter and do NOT contribute). ``sidecar_pages_indexed``
    # counts real-page rows only (sentinel rows do not).
    ciks_sidecared: int = 0
    sidecar_pages_indexed: int = 0


def _cik_from_filename(name: str) -> str | None:
    """Parse the 10-digit CIK out of a ``CIK<10>.json`` archive entry name."""
    m = _CIK_FILENAME_RE.match(name)
    return m.group(1) if m else None


def _load_current_bootstrap_run_id(
    conn: psycopg.Connection[Any],
) -> int | None:
    """Return the currently-running ``bootstrap_runs.id`` or ``None``.

    Stream A PR-B T1.3 (#1233) — sidecar populate threads this through
    so each row carries the bootstrap-run lineage when written under
    a tracked bootstrap. A steady-state S8 refresh (no running
    bootstrap) yields ``None``, which the writer stores as the FK
    nullable column and stamps ``populate_origin='steady_state'``.

    Read once per archive ingest; cached for the duration so the
    per-CIK loop does not re-query.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM bootstrap_runs WHERE status = 'running' ORDER BY id DESC LIMIT 1",
        )
        row = cur.fetchone()
    return int(row[0]) if row else None


def refresh_cik_sidecar(
    conn: psycopg.Connection[Any],
    *,
    cik: str,
    payload: dict[str, Any],
    bootstrap_run_id: int | None,
    result: SubmissionsIngestResult,
) -> None:
    """Stream A PR-B T1.3 (#1233): refresh ``sec_cik_submissions_files_index``
    for one CIK from the in-memory submissions payload.

    Public surface (promoted in #1233 PR-D from a leading-underscore
    private) so the sidecar repair runbook
    (``app/runbooks/stream_a_t13_sidecar_repair.py``) and the
    ``repair_cik_sidecar_from_archive`` helper below can re-use the
    SAME writer S8 uses. Behaviour unchanged from the original.

    Called from the OUTER per-CIK transaction in ``ingest_submissions_archive``
    BEFORE the ``for instrument_id, symbol in matched_instruments:`` sibling
    loop — putting it inside ``_ingest_one`` would repeat the DELETE+INSERT
    N times per share-class CIK (one per sibling), per Codex 1 re-pass
    IMPORTANT. Single refresh per (CIK, archive-entry) here.

    Skips agent CIKs (``KNOWN_FILING_AGENT_CIKS`` at
    ``app/providers/implementations/sec_edgar.py:98``) — sidecar stays
    a "real-filer-only" index. Agent CIKs are NOT in the populated
    set; S14 + the Stream-C C7 gate know to expect zero rows for them.

    Per-CIK DELETE + INSERT (not global TRUNCATE). The OUTER per-CIK
    transaction (sec_submissions_ingest.py:148-176) gives atomicity:
    if any sibling-instrument write later raises, the DELETE rolls
    back too — prior committed rows for that CIK SURVIVE.

    On zero overflow pages (e.g. AAPL — ``recent`` fits under 1000-cap),
    writes ONE sentinel row with ``page_name='__no_overflow_pages__'``
    instead of zero rows. Distinguishes "CIK processed; no overflow"
    from "CIK not yet populated" — per sql/172 header + spec §4 / §14.
    """
    if cik in KNOWN_FILING_AGENT_CIKS:
        return

    origin = "bootstrap" if bootstrap_run_id is not None else "steady_state"

    filings_block = payload.get("filings")
    files_entries: list[Any] = []
    if isinstance(filings_block, dict):
        raw_files = filings_block.get("files")
        if isinstance(raw_files, list):
            files_entries = raw_files

    real_pages: list[tuple[str, str, str]] = []
    malformed_count = 0
    for entry in files_entries:
        if not isinstance(entry, dict):
            malformed_count += 1
            continue
        name = entry.get("name")
        filing_from = entry.get("filingFrom")
        filing_to = entry.get("filingTo")
        if not (isinstance(name, str) and isinstance(filing_from, str) and isinstance(filing_to, str)):
            malformed_count += 1
            continue
        real_pages.append((name, filing_from, filing_to))

    # Codex 2 HIGH (pre-push review): if files[] was NON-EMPTY but every
    # entry was malformed, writing the sentinel would falsely tell S14
    # "this CIK has no overflow" when reality is "we could not parse
    # the overflow descriptors". Fail-loud instead: increment parse_errors,
    # skip the sidecar write entirely — S14 will then fail-closed on
    # empty sidecar and the operator will see the cause via
    # ciks_with_empty_sidecar + this parse_errors increment.
    if files_entries and not real_pages:
        logger.warning(
            "submissions ingest: CIK %s files[] had %d entries but ALL malformed; "
            "skipping sidecar write to avoid false-sentinel; S14 will fail-closed",
            cik,
            malformed_count,
        )
        result.parse_errors += 1
        return

    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM sec_cik_submissions_files_index WHERE cik = %s",
            (cik,),
        )
        if real_pages:
            cur.executemany(
                "INSERT INTO sec_cik_submissions_files_index "
                "(cik, page_name, filing_from, filing_to, bootstrap_run_id, populate_origin) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                [
                    (cik, name, filing_from, filing_to, bootstrap_run_id, origin)
                    for name, filing_from, filing_to in real_pages
                ],
            )
            result.sidecar_pages_indexed += len(real_pages)
            if malformed_count:
                # Partial-malformed (some good, some bad) — log + count
                # but DO write the good rows. Operator sees the gap.
                logger.info(
                    "submissions ingest: CIK %s files[] had %d malformed entries "
                    "(skipped); %d well-formed pages indexed",
                    cik,
                    malformed_count,
                    len(real_pages),
                )
        else:
            # Truly empty files[] (or missing filings block) → sentinel.
            # The malformed-only case is handled above.
            cur.execute(
                "INSERT INTO sec_cik_submissions_files_index "
                "(cik, page_name, bootstrap_run_id, populate_origin) "
                "VALUES (%s, %s, %s, %s)",
                (cik, _SIDECAR_SENTINEL_PAGE_NAME, bootstrap_run_id, origin),
            )
    result.ciks_sidecared += 1


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
            WHERE ei.provider = 'sec'
              AND ei.identifier_type = 'cik'
              AND i.is_tradable = TRUE
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

    # Stream A PR-B T1.3 (#1233): captured once per archive ingest so
    # the per-CIK sidecar writer threads run-lineage without re-
    # querying for every entry. ``None`` when running outside a tracked
    # bootstrap (steady-state refresh) — writer stores NULL bootstrap_run_id
    # + ``populate_origin='steady_state'``.
    bootstrap_run_id = _load_current_bootstrap_run_id(conn)

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

                    # Stream A PR-B T1.3 (#1233): refresh sidecar ONCE
                    # PER CIK in the OUTER block (not inside _ingest_one
                    # — that would re-DELETE+INSERT N times for share-
                    # class siblings, per Codex 1 re-pass IMPORTANT).
                    # Promoted to ``refresh_cik_sidecar`` (public) in
                    # PR-D so the repair runbook re-uses the same writer.
                    # Atomicity: the surrounding ``with conn.transaction()``
                    # gives "DELETE rolls back if any sibling write
                    # raises" so the sidecar is always consistent with
                    # the rest of the per-CIK ingest.
                    refresh_cik_sidecar(
                        conn,
                        cik=cik,
                        payload=payload,
                        bootstrap_run_id=bootstrap_run_id,
                        result=result,
                    )

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
        # ``_upsert_filing`` returns False when the 10y retention cap
        # (#1233 §4.2) drops a pre-cutoff filing. Count only accepted
        # rows so ``SubmissionsIngestResult.filings_upserted`` stays
        # accurate during the historical bulk archive walk.
        if _upsert_filing(conn, str(instrument_id), "sec", filing):
            result.filings_upserted += 1


def repair_cik_sidecar_from_archive(
    conn: psycopg.Connection[Any],
    *,
    archive_path: Path,
    cik: str | None = None,
    bootstrap_run_id: int | None = None,
) -> dict[str, int]:
    """Rebuild ``sec_cik_submissions_files_index`` rows from on-disk archive.

    Walks ``submissions.zip`` at ``archive_path`` and calls
    :func:`refresh_cik_sidecar` for each matching CIK entry, in its own
    per-CIK transaction. The writer is the SAME function S8 uses during
    bulk ingest, so semantics match exactly:

      * ``KNOWN_FILING_AGENT_CIKS`` are skipped at the writer layer.
      * Real-page rows have ``page_name`` matching the SEC overflow
        descriptor; zero-overflow CIKs get a single sentinel row.
      * Per-CIK DELETE + INSERT — prior committed rows for a different
        CIK SURVIVE if a later CIK raises.

    IN-UNIVERSE FILTER (per Codex 2 IMPORTANT fold of PR-D pre-push):
    S8's production path resolves a per-CIK ``matched_instruments`` set
    via ``_load_cik_to_instrument`` and skips entries with no match —
    so production NEVER writes sidecar rows for out-of-universe CIKs.
    The repair helper mirrors this: it loads the same in-universe CIK
    set up front and skips any archive entry not in that set. Without
    this filter, repair could inflate the C7 numerator with out-of-
    universe CIKs and false-pass the Stream-C correctness gate.

    Parameters
    ----------
    conn
        Open psycopg connection. Caller owns lifecycle.
    archive_path
        Path to local ``submissions.zip`` (the bulk archive S8 reads
        from). Not refetched — purely on-disk replay.
    cik
        Optional 10-digit padded CIK string. When set, only that entry
        is replayed AND only if it appears in the in-universe set;
        otherwise nothing is touched. When None, every in-universe
        entry in the archive is replayed.
    bootstrap_run_id
        Optional bootstrap-run lineage to stamp on inserted rows.
        ``None`` → ``populate_origin='steady_state'`` + NULL run id.
        Set → ``populate_origin='bootstrap'`` + bound run id. The
        operator runbook surfaces this as ``--bootstrap-run-id``
        (#1233 PR-D F8 fold).

    Returns
    -------
    dict[str, int]
        Telemetry: ``ciks_processed``, ``ciks_sidecared``,
        ``sidecar_pages_indexed``, ``sentinel_rows_written``,
        ``ciks_out_of_universe_skipped``, ``parse_errors``.

    Sole external caller: ``app/runbooks/stream_a_t13_sidecar_repair.py``
    (#1233 PR-D v3 R4 fold).
    """
    in_universe = set(_load_cik_to_instrument(conn).keys())
    result = SubmissionsIngestResult(
        archive_entries_seen=0,
        instruments_matched=0,
        filings_upserted=0,
        profiles_upserted=0,
    )
    ciks_processed = 0
    ciks_out_of_universe_skipped = 0
    sentinel_rows_written = 0

    with zipfile.ZipFile(archive_path) as zf:
        for entry_name in zf.namelist():
            entry_cik = _cik_from_filename(entry_name)
            if entry_cik is None:
                continue
            if cik is not None and entry_cik != cik:
                continue
            if entry_cik not in in_universe:
                # Mirrors S8: out-of-universe CIKs are skipped at the
                # ``_load_cik_to_instrument`` boundary before the writer
                # fires. Without this gate, repair could write sidecar
                # rows for tickers that don't belong to any tradable
                # instrument and inflate the C7 numerator.
                ciks_out_of_universe_skipped += 1
                continue
            result.archive_entries_seen += 1
            ciks_processed += 1
            pages_before = result.sidecar_pages_indexed
            ciks_sidecared_before = result.ciks_sidecared
            try:
                with conn.transaction():
                    try:
                        with zf.open(entry_name) as fh:
                            payload: dict[str, Any] = json.load(fh)
                    except (json.JSONDecodeError, KeyError) as exc:
                        logger.debug(
                            "repair_cik_sidecar_from_archive: bad payload for %s: %s",
                            entry_name,
                            exc,
                        )
                        result.parse_errors += 1
                        raise _SkipEntry from exc
                    refresh_cik_sidecar(
                        conn,
                        cik=entry_cik,
                        payload=payload,
                        bootstrap_run_id=bootstrap_run_id,
                        result=result,
                    )
            except _SkipEntry:
                continue
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "repair_cik_sidecar_from_archive: per-CIK failure for %s: %s",
                    entry_name,
                    exc,
                )
                result.parse_errors += 1
                continue
            # Writer wrote either real-page rows OR exactly one sentinel
            # for THIS CIK iff ciks_sidecared advanced; sentinel iff no
            # real-page delta. Agent CIKs short-circuit at the writer and
            # do not advance either counter — they contribute zero rows.
            if result.ciks_sidecared > ciks_sidecared_before and result.sidecar_pages_indexed == pages_before:
                sentinel_rows_written += 1

    return {
        "ciks_processed": ciks_processed,
        "ciks_sidecared": result.ciks_sidecared,
        "sidecar_pages_indexed": result.sidecar_pages_indexed,
        "sentinel_rows_written": sentinel_rows_written,
        "ciks_out_of_universe_skipped": ciks_out_of_universe_skipped,
        "parse_errors": result.parse_errors,
    }
