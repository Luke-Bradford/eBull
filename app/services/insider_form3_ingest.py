"""Form 3 ingester (#768 PR 2/N).

Walks ``filing_events`` for Form 3 / 3/A accessions without an
``insider_filings`` row, fetches each primary doc XML through the
shared SEC fetcher, parses via
:func:`app.services.insider_transactions.parse_form_3_xml`, and upserts
across the four insider tables:

  * ``insider_filings``           — document_type='3'/'3/A' rows live
                                    alongside Form 4 rows. The table
                                    accepts any ownership form.
  * ``insider_filers``            — reporting owners on the filing.
  * ``insider_transaction_footnotes`` — filing-scoped footnote bodies.
                                    Despite the name, the table is
                                    keyed on accession + footnote_id —
                                    it works for any ownership form.
  * ``insider_initial_holdings``  — Form 3-specific holding rows
                                    (migration 093).

Tombstone path mirrors Form 4: a fetch / parse failure writes a
filing-level tombstone so the ingester never re-fetches a dead URL.

Parser version is shared with Form 4 — bumping the Form 4 parser
intentionally invalidates Form 3 rows too only when ``parser_version``
on the filing is below the current shared value. Form 3 ingestion uses
its own ``_FORM3_PARSER_VERSION`` so a Form 3 parser tweak doesn't
trigger a Form 4 re-ingest cycle.

Cumulative-balance integration into ``get_insider_summary`` ships in
PR 3.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Protocol
from uuid import uuid4

import psycopg

from app.providers.concurrent_fetch import fetch_document_texts
from app.services import raw_filings
from app.services.insider_transactions import (
    ParsedForm3,
    ParsedHolding,
    _canonical_form_4_url,
    parse_form_3_xml,
)
from app.services.ownership_observations import (
    record_insider_observation,
    refresh_insiders_current,
)

logger = logging.getLogger(__name__)


# Bump whenever ``parse_form_3_xml`` shape changes so the ingester can
# re-parse older Form 3 filings under the new parser without a fresh
# SEC fetch. Independent from the Form 4 ``_PARSER_VERSION`` so a
# Form 4 parser tweak doesn't trigger a Form 3 re-ingest cycle.
_FORM3_PARSER_VERSION = 1


# Form 3 backfill floor — unlike Form 4 (5y floor) we keep every
# historical Form 3 we can find. Form 3 is filed once per officer-
# issuer appointment, so volume is bounded (~5-30 lifetime per issuer)
# and the snapshot from 10y ago for a still-serving officer is the
# correct cumulative-balance baseline. No throttling needed.
INSIDER_FORM3_BACKFILL_FLOOR_YEARS: int | None = None


# ---------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------


def upsert_form_3_filing(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    primary_document_url: str,
    parsed: ParsedForm3,
    is_rewash: bool = False,
) -> None:
    """Insert / refresh the Form 3 filing header + filer dim + footnote
    bodies + holding rows for one accession.

    Idempotency: every child table keys on ``(accession, …)`` with
    ON CONFLICT DO UPDATE so re-running on the same accession (e.g.
    after a parser bump) refreshes every field in place. Tombstones
    are flipped back to live via the ``is_tombstone = FALSE`` reset on
    the filings UPDATE branch.

    ``is_rewash``: when True, the conflict branch preserves the
    original ``fetched_at`` (re-parsing a stored body isn't a fresh
    SEC fetch). Same flag pattern as Form 4 (PR #818).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO insider_filings (
                accession_number, instrument_id, document_type,
                period_of_report, date_of_original_submission,
                not_subject_to_section_16,
                form3_holdings_reported, form4_transactions_reported,
                issuer_cik, issuer_name, issuer_trading_symbol,
                remarks, signature_name, signature_date,
                primary_document_url, parser_version, is_tombstone
            ) VALUES (
                %s, %s, %s,
                %s, %s,
                %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, FALSE
            )
            ON CONFLICT (accession_number) DO UPDATE SET
                document_type                = EXCLUDED.document_type,
                period_of_report             = EXCLUDED.period_of_report,
                date_of_original_submission  = EXCLUDED.date_of_original_submission,
                not_subject_to_section_16    = EXCLUDED.not_subject_to_section_16,
                form3_holdings_reported      = EXCLUDED.form3_holdings_reported,
                form4_transactions_reported  = EXCLUDED.form4_transactions_reported,
                issuer_cik                   = EXCLUDED.issuer_cik,
                issuer_name                  = EXCLUDED.issuer_name,
                issuer_trading_symbol        = EXCLUDED.issuer_trading_symbol,
                remarks                      = EXCLUDED.remarks,
                signature_name               = EXCLUDED.signature_name,
                signature_date               = EXCLUDED.signature_date,
                primary_document_url         = EXCLUDED.primary_document_url,
                parser_version               = EXCLUDED.parser_version,
                is_tombstone                 = FALSE,
                fetched_at                   = CASE WHEN %s
                                                    THEN insider_filings.fetched_at
                                                    ELSE NOW() END
            """,
            (
                accession_number,
                instrument_id,
                parsed.document_type,
                parsed.period_of_report,
                parsed.date_of_original_submission,
                # Form 3 doesn't use the section-16 / form3-reported /
                # form4-reported flags; persist as NULL so a future
                # cross-form query can branch on document_type without
                # reading bogus values.
                None,
                None,
                None,
                parsed.issuer_cik,
                parsed.issuer_name,
                parsed.issuer_trading_symbol,
                parsed.remarks,
                parsed.signature_name,
                parsed.signature_date,
                primary_document_url,
                _FORM3_PARSER_VERSION,
                is_rewash,
            ),
        )

        # Filer dim — same shape as Form 4 ingester. Re-running upserts
        # in place because ``insider_filers`` keys on (accession,
        # filer_cik) via the UNIQUE constraint from migration 057.
        #
        # Replace-then-insert (mirrors holdings below): without the
        # DELETE, a parser version that stops emitting a secondary
        # joint-filer would leave the stale row pinned to the
        # accession forever. Codex review of #768 PR2 caught the gap.
        cur.execute(
            "DELETE FROM insider_filers WHERE accession_number = %s",
            (accession_number,),
        )
        for filer in parsed.filers:
            cur.execute(
                """
                INSERT INTO insider_filers (
                    accession_number, filer_cik, filer_name,
                    street1, street2, city, state, zip_code,
                    state_description,
                    is_director, is_officer, officer_title,
                    is_ten_percent_owner, is_other, other_text
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (accession_number, filer_cik) DO UPDATE SET
                    filer_name           = EXCLUDED.filer_name,
                    street1              = EXCLUDED.street1,
                    street2              = EXCLUDED.street2,
                    city                 = EXCLUDED.city,
                    state                = EXCLUDED.state,
                    zip_code             = EXCLUDED.zip_code,
                    state_description    = EXCLUDED.state_description,
                    is_director          = EXCLUDED.is_director,
                    is_officer           = EXCLUDED.is_officer,
                    officer_title        = EXCLUDED.officer_title,
                    is_ten_percent_owner = EXCLUDED.is_ten_percent_owner,
                    is_other             = EXCLUDED.is_other,
                    other_text           = EXCLUDED.other_text
                """,
                (
                    accession_number,
                    filer.filer_cik,
                    filer.filer_name,
                    filer.street1,
                    filer.street2,
                    filer.city,
                    filer.state,
                    filer.zip_code,
                    filer.state_description,
                    filer.is_director,
                    filer.is_officer,
                    filer.officer_title,
                    filer.is_ten_percent_owner,
                    filer.is_other,
                    filer.other_text,
                ),
            )

        # Footnote bodies. Re-uses ``insider_transaction_footnotes`` —
        # the table name is historical (migration 057 named it for
        # Form 4) but its key is (accession, footnote_id), filing-
        # scoped, so it carries Form 3 footnotes equally well. PR3
        # may rename the table; the data shape is correct.
        #
        # Replace-then-insert (matches holdings + filers): a parser
        # version that drops a footnote should not leave the stale
        # body pinned to the accession. Codex review of #768 PR2
        # caught the gap.
        cur.execute(
            "DELETE FROM insider_transaction_footnotes WHERE accession_number = %s",
            (accession_number,),
        )
        for footnote in parsed.footnotes:
            cur.execute(
                """
                INSERT INTO insider_transaction_footnotes (
                    accession_number, footnote_id, footnote_text
                ) VALUES (%s, %s, %s)
                ON CONFLICT (accession_number, footnote_id) DO UPDATE SET
                    footnote_text = EXCLUDED.footnote_text
                """,
                (accession_number, footnote.footnote_id, footnote.footnote_text),
            )

        # Holding rows — replace-then-insert so a re-parse cleanly drops
        # rows that no longer appear in the latest XML (e.g. parser
        # bump that filtered a malformed entry). Mirrors the Form 4
        # transactions pattern but on the new insider_initial_holdings
        # table from migration 093.
        cur.execute(
            "DELETE FROM insider_initial_holdings WHERE accession_number = %s",
            (accession_number,),
        )
        for holding in parsed.holdings:
            cur.execute(
                """
                INSERT INTO insider_initial_holdings (
                    instrument_id, accession_number, row_num,
                    filer_cik, filer_name, filer_role,
                    as_of_date,
                    security_title, shares, value_owned, is_derivative,
                    direct_indirect, nature_of_ownership,
                    conversion_exercise_price,
                    exercise_date, expiration_date,
                    underlying_security_title, underlying_shares,
                    underlying_value
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s,
                    %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s,
                    %s, %s,
                    %s, %s,
                    %s
                )
                """,
                (
                    instrument_id,
                    accession_number,
                    holding.row_num,
                    holding.filer_cik,
                    _filer_name_for(parsed, holding.filer_cik),
                    _filer_role_for(parsed, holding.filer_cik),
                    # Form 3 ``period_of_report`` IS the as_of_date —
                    # the snapshot the filer declares. Required NOT
                    # NULL by the migration; if the parser produced a
                    # NULL period (rare; SEC requires it), fall back
                    # to the signature date so the row still lands.
                    parsed.period_of_report or parsed.signature_date,
                    holding.security_title,
                    holding.shares,
                    holding.value_owned,
                    holding.is_derivative,
                    holding.direct_indirect,
                    holding.nature_of_ownership,
                    holding.conversion_exercise_price,
                    holding.exercise_date,
                    holding.expiration_date,
                    holding.underlying_security_title,
                    holding.underlying_shares,
                    holding.underlying_value,
                ),
            )

    # Write-through observations + refresh _current (#888 / spec §"Eliminate
    # periodic re-scan jobs"). Mirrors the Form 4 path —
    # one observation per (filer_cik, direct_indirect), shares = the
    # holding's reported share count, period_end = period_of_report.
    _record_form3_observations_for_filing(
        conn,
        instrument_id=instrument_id,
        accession_number=accession_number,
        primary_document_url=primary_document_url,
        parsed=parsed,
    )
    refresh_insiders_current(conn, instrument_id=instrument_id)


def _record_form3_observations_for_filing(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    primary_document_url: str,
    parsed: ParsedForm3,
) -> None:
    """Derive one ``ownership_insiders_observations`` row per
    ``(filer_cik, direct_indirect)`` from the parsed Form 3 holdings.

    Mirrors the legacy batch-sync rule in
    ``ownership_observations_sync.sync_insiders``:

      - Filter: ``shares IS NOT NULL`` AND ``is_derivative = FALSE``.
      - Group key: ``(filer_cik, direct_indirect)``. Last row per group
        wins by ``row_num`` ordering — matches the legacy batch sync
        in ownership_observations_sync.sync_insiders, where iterating
        every insider_initial_holdings row sequentially with the same
        observation natural key produced a "last write wins" effect.
        Codex review: summing instead would change observed-shares
        semantics for filings with multi-class common stock listed
        under one filer/nature.
      - ``ownership_nature``: ``'indirect'`` when direct_indirect='I',
        else ``'direct'``.
    """
    as_of: date | None = parsed.period_of_report or parsed.signature_date
    if as_of is None:
        return

    LatestKey = tuple[str | None, str | None]
    latest: dict[LatestKey, ParsedHolding] = {}
    for holding in parsed.holdings:
        if holding.is_derivative:
            continue
        if holding.shares is None:
            continue
        key: LatestKey = (holding.filer_cik, holding.direct_indirect)
        prior = latest.get(key)
        if prior is None or holding.row_num > prior.row_num:
            latest[key] = holding

    if not latest:
        return

    run_id = uuid4()
    for (filer_cik, direct_indirect), holding in latest.items():
        shares = holding.shares
        if shares is None:
            continue
        holder_name = _filer_name_for(parsed, filer_cik)
        if not holder_name and not filer_cik:
            continue
        nature = "indirect" if direct_indirect == "I" else "direct"
        record_insider_observation(
            conn,
            instrument_id=instrument_id,
            holder_cik=filer_cik,
            holder_name=holder_name or (filer_cik or "UNKNOWN"),
            ownership_nature=nature,  # type: ignore[arg-type]
            source="form3",
            source_document_id=accession_number,
            source_accession=accession_number,
            source_field=None,
            source_url=primary_document_url or None,
            filed_at=datetime.combine(as_of, datetime.min.time(), tzinfo=UTC),
            period_start=None,
            period_end=as_of,
            ingest_run_id=run_id,
            shares=shares,
        )


def _filer_name_for(parsed: ParsedForm3, filer_cik: str | None) -> str:
    """Resolve ``filer_cik`` to a display name from the filing's filer
    list. Falls back to the first listed owner when the holding row's
    ``filer_cik`` doesn't match any (joint-filing convention)."""
    if filer_cik is not None:
        for f in parsed.filers:
            if f.filer_cik == filer_cik:
                return f.filer_name
    return parsed.filers[0].filer_name if parsed.filers else "<unknown>"


def _filer_role_for(parsed: ParsedForm3, filer_cik: str | None) -> str | None:
    """Pipe-joined relationship-flag string for the filer matching
    ``filer_cik``. Same encoding as
    :func:`app.services.insider_transactions.filer_role_string`.
    Returns ``None`` when the filer has no relationship data on file
    (rare; SEC requires at least one flag)."""
    target = None
    if filer_cik is not None:
        for f in parsed.filers:
            if f.filer_cik == filer_cik:
                target = f
                break
    if target is None:
        target = parsed.filers[0] if parsed.filers else None
    if target is None:
        return None
    parts: list[str] = []
    if target.is_director:
        parts.append("director")
    if target.is_officer:
        parts.append(f"officer:{target.officer_title}" if target.officer_title else "officer")
    if target.is_ten_percent_owner:
        parts.append("ten_percent_owner")
    if target.is_other:
        parts.append(f"other:{target.other_text}" if target.other_text else "other")
    return "|".join(parts) or None


_TOMBSTONE_DOC_TYPE = "3"


def _write_form_3_tombstone(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    primary_document_url: str,
) -> None:
    """Mark a Form 3 accession as unfetchable / unparseable so the next
    pass skips it. Tombstones carry no children; the cumulative-
    balance reader (PR 3) joins ``insider_filings`` with
    ``is_tombstone = FALSE`` to exclude them. A successful re-parse
    flips the row back to live via :func:`upsert_form_3_filing`."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO insider_filings (
                accession_number, instrument_id, document_type,
                primary_document_url, parser_version, is_tombstone
            ) VALUES (%s, %s, %s, %s, %s, TRUE)
            ON CONFLICT (accession_number) DO NOTHING
            """,
            (
                accession_number,
                instrument_id,
                _TOMBSTONE_DOC_TYPE,
                primary_document_url,
                _FORM3_PARSER_VERSION,
            ),
        )


# ---------------------------------------------------------------------
# Ingester
# ---------------------------------------------------------------------


class _DocFetcher(Protocol):
    def fetch_document_text(self, absolute_url: str) -> str | None: ...


@dataclass(frozen=True)
class IngestForm3Result:
    filings_scanned: int
    filings_parsed: int
    rows_inserted: int
    fetch_errors: int
    parse_misses: int


def ingest_form_3_filings_for_instrument(
    conn: psycopg.Connection[Any],
    fetcher: _DocFetcher,
    *,
    instrument_id: int,
    limit: int = 500,
) -> IngestForm3Result:
    """Targeted backfill for one instrument's Form 3 filings.

    Same candidate-selector contract as
    :func:`ingest_form_3_filings` but scoped to a single instrument.
    """
    conn.commit()
    candidates: list[tuple[int, str, str]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fe.instrument_id,
                   fe.provider_filing_id,
                   fe.primary_document_url
            FROM filing_events fe
            LEFT JOIN insider_filings fil ON fil.accession_number = fe.provider_filing_id
            WHERE fe.provider = 'sec'
              AND fe.filing_type IN ('3', '3/A')
              AND fe.primary_document_url IS NOT NULL
              AND fe.instrument_id = %s
              AND (fil.accession_number IS NULL OR fil.parser_version < %s)
            ORDER BY fe.filing_date DESC, fe.filing_event_id DESC
            LIMIT %s
            """,
            (instrument_id, _FORM3_PARSER_VERSION, limit),
        )
        for row in cur.fetchall():
            candidates.append((int(row[0]), str(row[1]), _canonical_form_4_url(str(row[2]))))
    conn.commit()

    return _process_form_3_candidates(conn, fetcher, candidates)


def ingest_form_3_filings(
    conn: psycopg.Connection[Any],
    fetcher: _DocFetcher,
    *,
    limit: int = 500,
) -> IngestForm3Result:
    """Universe-wide newest-first scan of Form 3 candidates.

    Candidate selector:
      1. ``fe.filing_type IN ('3', '3/A')``.
      2. ``fe.primary_document_url IS NOT NULL``.
      3. No existing ``insider_filings`` row, OR the existing row's
         ``parser_version`` is below the current
         ``_FORM3_PARSER_VERSION`` (re-parse trigger when the parser
         shape changes — Codex round 1 / round 2 review of #768 PR2).
      4. No backfill floor — Form 3 volume per issuer is bounded
         (~5-30 lifetime), and a 10-year-old Form 3 for a still-
         serving officer IS the correct baseline.

    Bounded per run by ``limit``.
    """
    conn.commit()

    candidates: list[tuple[int, str, str]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fe.instrument_id,
                   fe.provider_filing_id,
                   fe.primary_document_url
            FROM filing_events fe
            LEFT JOIN insider_filings fil ON fil.accession_number = fe.provider_filing_id
            WHERE fe.provider = 'sec'
              AND fe.filing_type IN ('3', '3/A')
              AND fe.primary_document_url IS NOT NULL
              AND (fil.accession_number IS NULL OR fil.parser_version < %s)
            ORDER BY fe.filing_date DESC, fe.filing_event_id DESC
            LIMIT %s
            """,
            (_FORM3_PARSER_VERSION, limit),
        )
        for row in cur.fetchall():
            candidates.append((int(row[0]), str(row[1]), _canonical_form_4_url(str(row[2]))))
    conn.commit()

    return _process_form_3_candidates(conn, fetcher, candidates)


def _process_form_3_candidates(
    conn: psycopg.Connection[Any],
    fetcher: _DocFetcher,
    candidates: list[tuple[int, str, str]],
) -> IngestForm3Result:
    """Shared fetch-parse-upsert loop for the Form 3 entry points.

    Mirrors :func:`app.services.insider_transactions._process_candidates`
    but routes through :func:`parse_form_3_xml` and
    :func:`upsert_form_3_filing`. Per-candidate failures (fetch / parse
    / upsert) tombstone the accession and continue to the next so a
    single bad URL doesn't abort the batch.
    """
    filings_parsed = 0
    rows_inserted = 0
    fetch_errors = 0
    parse_misses = 0

    bodies = fetch_document_texts(fetcher, (url for _, _, url in candidates))

    for instrument_id, accession, url in candidates:
        xml = bodies.get(url)
        if xml is None:
            logger.warning(
                "ingest_form_3_filings: fetch failed accession=%s url=%s",
                accession,
                url,
            )
            fetch_errors += 1
            _write_form_3_tombstone(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                primary_document_url=url,
            )
            conn.commit()
            continue

        # Persist raw body BEFORE parsing — re-wash workflows depend
        # on this row even if parsing fails. Operator audit
        # 2026-05-03 + PR #808 contract.
        raw_filings.store_raw(
            conn,
            accession_number=accession,
            document_kind="form3_xml",
            payload=xml,
            parser_version=f"form3-v{_FORM3_PARSER_VERSION}",
            source_url=url,
        )
        conn.commit()

        parsed = parse_form_3_xml(xml)
        if parsed is None:
            parse_misses += 1
            _write_form_3_tombstone(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                primary_document_url=url,
            )
            conn.commit()
            continue

        try:
            upsert_form_3_filing(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                primary_document_url=url,
                parsed=parsed,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            logger.warning(
                "ingest_form_3_filings: upsert failed accession=%s",
                accession,
                exc_info=True,
            )
            # Tombstone the accession so a persistent constraint
            # violation (or any other deterministic upsert failure)
            # doesn't loop the scheduler refetching the same dead
            # XML on every tick. A fix-and-bump of the parser version
            # will re-pick the accession via the parser_version <
            # selector branch above. Codex round 2 review of #768 PR2.
            _write_form_3_tombstone(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                primary_document_url=url,
            )
            conn.commit()
            parse_misses += 1
            continue

        filings_parsed += 1
        rows_inserted += len(parsed.holdings)

    return IngestForm3Result(
        filings_scanned=len(candidates),
        filings_parsed=filings_parsed,
        rows_inserted=rows_inserted,
        fetch_errors=fetch_errors,
        parse_misses=parse_misses,
    )


# ---------------------------------------------------------------------
# Reader — baseline-only insider holdings (#768 PR 3)
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class BaselineInsiderHolding:
    """Form 3 holding for a filer who has no Form 4 activity on file
    for this instrument.

    These are the operationally-meaningful "invisible insiders" — they
    received a grant on appointment, never traded after, and therefore
    never produced a Form 4 row. Without surfacing the baseline
    snapshot, the per-filer ownership ring under-counts current
    insiders.

    The ingester writes one row per holding-line in a Form 3 (non-
    derivative + derivative interleaved). The reader here aggregates
    to one row per (filer_cik, security_title, is_derivative) and
    picks the latest ``as_of_date`` per group, mirroring the Form 4
    reader's ``post_transaction_shares`` latest-observation pattern.
    """

    filer_cik: str
    filer_name: str
    filer_role: str | None
    security_title: str | None
    is_derivative: bool
    direct_indirect: str | None
    shares: Decimal | None
    value_owned: Decimal | None
    as_of_date: date


def list_baseline_only_insider_holdings(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> list[BaselineInsiderHolding]:
    """Return Form 3 baseline holdings for filers who have *no* Form 4
    transaction on file for this instrument.

    Filers with any non-tombstoned Form 4 row in
    ``insider_transactions`` are excluded — their cumulative balance
    is already derivable from
    :func:`app.services.insider_transactions.list_insider_transactions`
    via the latest ``post_transaction_shares`` observation. Returning
    them here would double-count the per-filer ring 3 wedge.

    Tombstoned Form 3 filings (fetch / parse failures) excluded via an
    INNER JOIN to ``insider_filings`` with ``is_tombstone = FALSE``,
    matching the convention used by ``get_insider_summary``.

    Returns rows ordered by ``shares DESC NULLS LAST`` so the consumer
    can render largest-first without a second pass.
    """
    rows: list[BaselineInsiderHolding] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH baseline AS (
                SELECT DISTINCT ON (
                    iih.filer_cik, iih.security_title, iih.is_derivative
                )
                    iih.filer_cik,
                    iih.filer_name,
                    iih.filer_role,
                    iih.security_title,
                    iih.is_derivative,
                    iih.direct_indirect,
                    iih.shares,
                    iih.value_owned,
                    iih.as_of_date
                FROM insider_initial_holdings iih
                INNER JOIN insider_filings f
                    ON f.accession_number = iih.accession_number
                   AND f.is_tombstone = FALSE
                WHERE iih.instrument_id = %s
                ORDER BY
                    iih.filer_cik,
                    iih.security_title,
                    iih.is_derivative,
                    -- Tie-break in priority order:
                    --   1. Latest as_of_date (period_of_report) wins.
                    --   2. Within the same as_of_date (the realistic
                    --      3/A case where the amendment keeps the
                    --      original snapshot date), prefer the larger
                    --      accession_number — within a filer, SEC
                    --      assigns monotonically increasing
                    --      sequence numbers, so the amendment's
                    --      accession sorts after the original. Codex
                    --      review of #768 PR3 caught the original
                    --      ordering's silent loss of amendment
                    --      precedence.
                    --   3. Within the same accession (option exposure
                    --      vs underlying-equity rows on a single
                    --      filing), prefer the lower row_num so the
                    --      first listed holding wins.
                    iih.as_of_date DESC,
                    iih.accession_number DESC,
                    iih.row_num ASC
            )
            SELECT b.*
            FROM baseline b
            WHERE NOT EXISTS (
                SELECT 1
                FROM insider_transactions it
                INNER JOIN insider_filings ft
                    ON ft.accession_number = it.accession_number
                   AND ft.is_tombstone = FALSE
                WHERE it.instrument_id = %s
                  AND it.filer_cik = b.filer_cik
            )
            ORDER BY b.shares DESC NULLS LAST, b.filer_name ASC
            """,
            (instrument_id, instrument_id),
        )
        for row in cur.fetchall():
            rows.append(
                BaselineInsiderHolding(
                    filer_cik=str(row[0]),
                    filer_name=str(row[1]),
                    filer_role=row[2],
                    security_title=row[3],
                    is_derivative=bool(row[4]),
                    direct_indirect=row[5],
                    shares=Decimal(row[6]) if row[6] is not None else None,
                    value_owned=Decimal(row[7]) if row[7] is not None else None,
                    as_of_date=row[8],
                )
            )
    return rows
