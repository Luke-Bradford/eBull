"""#3973 repair: Form 3/4/5 manifest rows lost to a reporting owner's filer subject.

Before #3973 the generic discovery writers stored a Form 3/4/5 under
whichever CIK's index line they read last; a reporting owner that is a
13F or 13D/G filer resolved to a filer subject with ``instrument_id``
NULL, and the insider parsers tombstoned the row ``missing instrument_id``
(``app.services.sec_manifest.ISSUER_SUBJECT_ONLY_SOURCES``).

The issuer comes from the document itself, ``<issuer><issuerCik>``, read
from the stored body or, where the parser would fetch it anyway, fetched
and stored for the worker to reuse; the instrument is that CIK's sibling
set (``siblings_for_issuer_cik``, the resolver the writer routes by). Rows
repaired:

1. filer-subject rows tombstoned ``missing instrument_id``: re-subjected to
   the issuer and reset to ``pending`` for the manifest worker;
2. filer-subject rows already ``parsed`` (re-subjected after their parse):
   re-subjected, so a rebuild reset parses them instead of tombstoning;
3. issuer-subject rows still tombstoned ``missing instrument_id`` (a later
   issuer discovery restored the instrument but not the status, which
   ``transition_status`` owns): reset to ``pending``, and re-subjected when
   their instrument is not the document's issuer.

A filer-subject row is a candidate only when a universe instrument's own
submissions list the accession (``filing_events``, the issuer path's
discovery): a filing no universe instrument lists has no universe issuer,
and fetching it would spend the SEC budget to learn that. That only
selects candidates; the issuer is still read from the document.

Left as they are, by construction: a document whose issuer CIK maps to no
universe instrument (the writer would fail open onto the discovery
instrument, #828, which is another company's); a row with no stored body
that the parser gates before fetching (Form 4/5 retention); and a Form 3
whose every holding owner already has a later insider observation on the
issuer. Form 3 is exempt from the retention bound because it is the only
evidence for a holder who never transacted (#1233 §4.3,
``ownership_rollup._INSIDER_BEYOND_RETENTION_SQL``); once the holder has
later evidence the Form 3 adds nothing current, and its XML row (keyed by
Table I D/I) is not superseded by the holder's later DERA row (keyed by
relationship, #3227), so parsing it would restore a stale initial balance
(A/B 2026-10-23: Apollo's 2017 Form 3 on VST, 45,880,115 shares, after
DERA Form 4s through 2018-12).

Finally the ``data_freshness_index`` rows for (filer subject, Form 3/4/5)
are deleted: ``record_manifest_entry`` refuses every write they could make.

Idempotent and resumable: candidates are read from the manifest alone, and a
repaired row is no longer one, so a re-run (after a crash between the
200-row commits, or for rows skipped on a fetch error) continues where the
last left off. Dry run by default (stored bodies only, rolled back);
``--apply`` fetches and commits every 200 rows.

    PYTHONPATH=. uv run python -m scripts.repair_3973_form345_filer_subjects [--apply]
"""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.services.insider_form3_ingest import _FORM3_PARSER_VERSION
from app.services.insider_transactions import (
    _PARSER_VERSION_FORM4,
    _PARSER_VERSION_FORM5,
    _canonical_form_4_url,
    form4_within_retention,
    form5_within_retention,
    parse_form_3_xml,
    parse_form_4_xml,
    parse_form_5_xml,
)
from app.services.raw_filings import DocumentKind, store_raw, stored_body
from app.services.sec_identity import resolve_insider_writer_routing, siblings_for_issuer_cik
from app.services.sec_manifest import (
    ISSUER_SUBJECT_ONLY_SOURCES,
    record_manifest_entry,
    transition_status,
)

_SOURCES = sorted(ISSUER_SUBJECT_ONLY_SOURCES)
_TOMBSTONE_ERROR = "missing instrument_id"
_KIND: dict[str, DocumentKind] = {"sec_form3": "form3_xml", "sec_form4": "form4_xml", "sec_form5": "form5_xml"}
# The parser_version each manifest parser stamps on the body it stores (insider_345).
_VERSION = {
    "sec_form3": f"form3-v{_FORM3_PARSER_VERSION}",
    "sec_form4": _PARSER_VERSION_FORM4,
    "sec_form5": _PARSER_VERSION_FORM5,
}
_PARSE: dict[str, Callable[[str], Any]] = {
    "sec_form3": parse_form_3_xml,
    "sec_form4": parse_form_4_xml,
    "sec_form5": parse_form_5_xml,
}

Fetch = Callable[[str], str | None]


def _parser_would_fetch(source: str, filed_at: datetime) -> bool:
    """Mirror of the parsers' pre-fetch retention gates (Form 3 has none)."""
    if source == "sec_form4":
        return form4_within_retention(filed_at.date())
    if source == "sec_form5":
        return form5_within_retention(filed_at.astimezone(UTC).date())
    return True


def _owner_has_later_evidence(
    conn: psycopg.Connection[Any], parsed: Any, siblings: list[int], filed_at: datetime
) -> bool:
    """True when every owner the Form 3's holdings are attributed to (the observation
    writer keys each row by the holding's ``filer_cik``) has a later insider observation
    on the issuer. A Form 3 with no attributed holding restores nothing either way."""
    owners = sorted({h.filer_cik.strip().zfill(10) for h in parsed.holdings if (h.filer_cik or "").strip()})
    if not owners:
        return False
    row = conn.execute(
        """
        SELECT count(DISTINCT holder_cik) FROM ownership_insiders_observations
         WHERE instrument_id = ANY(%s) AND holder_cik = ANY(%s) AND filed_at > %s
        """,
        (siblings, owners, filed_at),
    ).fetchone()
    return row is not None and int(row[0]) == len(owners)


def repair(conn: psycopg.Connection[Any], *, fetch: Fetch | None, commit_every: int | None = None) -> Counter[str]:
    """Run the repair on ``conn``; ``fetch`` None reads stored bodies only.

    With ``commit_every`` unset the caller owns the transaction, which must
    already be open (``transition_status`` uses ``conn.transaction()``)."""
    stats: Counter[str] = Counter()
    rows = conn.execute(
        """
        SELECT accession_number, form, source, filed_at, primary_document_url, is_amendment,
               amends_accession, subject_type, instrument_id, ingest_status
          FROM sec_filing_manifest m
         WHERE source = ANY(%(sources)s)
           AND (   (subject_type <> 'issuer'
                    AND (ingest_status = 'parsed' OR (ingest_status = 'tombstoned' AND error = %(err)s))
                    AND EXISTS (SELECT 1 FROM filing_events f
                                 WHERE f.provider = 'sec' AND f.provider_filing_id = m.accession_number))
                OR (subject_type = 'issuer' AND ingest_status = 'tombstoned' AND error = %(err)s))
         ORDER BY accession_number
        """,
        {"sources": _SOURCES, "err": _TOMBSTONE_ERROR},
    ).fetchall()
    stats["candidates"] = len(rows)
    for n, (acc, form, source, filed_at, url, is_amend, amends, subject_type, iid, status) in enumerate(rows, 1):
        if commit_every and n % commit_every == 0:
            conn.commit()
            conn.execute("SELECT 1")
        body = stored_body(conn, accession_number=acc, document_kind=_KIND[source])
        if body is None:
            if fetch is None or not url or not _parser_would_fetch(source, filed_at):
                stats["skipped_no_body"] += 1
                continue
            canonical_url = _canonical_form_4_url(url)
            try:
                body = fetch(canonical_url)
            except Exception:  # noqa: BLE001 — transient; the row stays a candidate for a re-run
                stats["skipped_fetch_error"] += 1
                continue
            if not body:
                stats["skipped_no_body"] += 1
                continue
            with conn.transaction():
                store_raw(
                    conn,
                    accession_number=acc,
                    document_kind=_KIND[source],
                    payload=body,
                    parser_version=_VERSION[source],
                    source_url=canonical_url,
                )
            stats["fetched"] += 1
        try:
            parsed = _PARSE[source](body)
        except Exception:  # noqa: BLE001 — counted below; no issuer can be read from this body
            parsed = None
        issuer_cik = (getattr(parsed, "issuer_cik", None) or "").strip()
        if not issuer_cik.isdigit():
            # No issuer to re-subject to: the row stays tombstoned, as the
            # worker's own parse of the same body would leave it.
            stats["skipped_no_issuer_cik"] += 1
            continue
        siblings = siblings_for_issuer_cik(conn, issuer_cik)
        if not siblings:
            stats["skipped_issuer_outside_universe"] += 1
            continue
        if source == "sec_form3" and _owner_has_later_evidence(conn, parsed, siblings, filed_at):
            stats["skipped_form3_superseded"] += 1
            continue
        if subject_type != "issuer" or iid not in siblings:
            # The entity instrument the #828 writer itself picks for a discovery
            # outside the sibling set (unique history instrument, else the lowest).
            # A filer row has no discovery instrument; 0 is no instrument id, so it
            # takes that same branch (tests/test_3973_form345_filer_subject_repair.py).
            target = resolve_insider_writer_routing(
                conn, discovery_instrument_id=iid if iid is not None else 0, issuer_cik=issuer_cik
            ).entity_instrument_id
            record_manifest_entry(
                conn,
                acc,
                cik=issuer_cik.zfill(10),
                form=form,
                source=source,
                subject_type="issuer",
                subject_id=str(target),
                instrument_id=target,
                filed_at=filed_at,
                primary_document_url=url,
                is_amendment=is_amend,
                amends_accession=amends,
            )
            stats["resubjected"] += 1
        if status == "tombstoned":
            transition_status(conn, acc, ingest_status="pending")
            stats["reset_to_pending"] += 1
    stats["freshness_rows_deleted"] = conn.execute(
        "DELETE FROM data_freshness_index WHERE source = ANY(%s) AND subject_type <> 'issuer'",
        (_SOURCES,),
    ).rowcount
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="#3973 repair: filer-subject Form 3/4/5 manifest rows")
    parser.add_argument("--apply", action="store_true", help="fetch and commit (default: stored bodies, rolled back)")
    args = parser.parse_args()
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("SELECT 1")  # open the transaction: conn.transaction() on an idle connection commits
        if args.apply:
            with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
                stats = repair(conn, fetch=provider.fetch_document_text, commit_every=200)
            conn.commit()
        else:
            stats = repair(conn, fetch=None)
            conn.rollback()
        print(dict(stats), "committed" if args.apply else "dry run: rolled back")


if __name__ == "__main__":
    main()
