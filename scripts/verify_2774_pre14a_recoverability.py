"""#2774 — evidence for the ``pre14a_body`` retention verdict.

Three read-only arms. None of them writes anything, and none of them deletes
a payload — the verdict this backs (born-compaction of NEW rows) leaves every
existing row untouched.

``--census``
    The per-kind figures quoted in ``raw_filings.DocumentKind``'s
    ``pre14a_body`` comment and in the spec: rows, live logical bytes
    (``byte_count``, a generated column over ``octet_length(payload)``),
    mean / median / max, stored datum size, and the parsed-vs-tombstoned split
    reconciled against the typed sink by ANTI-JOIN rather than by manifest
    state alone.

``--urls``
    FULL-POPULATION structural validation of every ``source_url``.
    Born-compaction's whole safety argument is that the bytes are re-obtainable
    from that URL, and ``store_raw`` only checks the string is truthy — so
    whitespace, a wrong host, or a URL pointing at a different accession would
    all pass ingest and fail recovery. Offline: no SEC request, so it can run
    on the full 557 without touching the shared 10 req/s budget.

``--roundtrip N``
    Live byte-identity against EDGAR on a size-stratified sample: re-fetch the
    document and compare its sha256 with the server-side hash of the stored
    payload (``encode(sha256(convert_to(payload,'UTF8')),'hex')`` — the same
    expression ``store_raw`` and the #1014 sweep use, so this tests the exact
    equality born-compaction relies on).

    ⚠ This arm is a SAMPLE and is reported as one. It cannot establish
    recoverability for the whole population, and no check of any size can
    establish FUTURE EDGAR availability — a hash detects that a document
    changed, it cannot restore one that was removed. What it does establish is
    that the stored-hash / re-fetch equality holds on real filings of this kind.

Run::

    PYTHONPATH=. uv run python scripts/verify_2774_pre14a_recoverability.py --census
    PYTHONPATH=. uv run python scripts/verify_2774_pre14a_recoverability.py --urls
    PYTHONPATH=. uv run python scripts/verify_2774_pre14a_recoverability.py --roundtrip 8
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from urllib.parse import urlparse

import psycopg
import psycopg.rows

from app.config import settings
from scripts._dev_guard import assert_dev_environment

_KIND = "pre14a_body"
#: EDGAR serves filing documents from www.sec.gov/Archives/... — a stored URL
#: on any other host is not a recovery location for this kind.
_EXPECTED_HOST = "www.sec.gov"
#: Bound the round-trip transfer. The largest stored body is 313.6 MB; fetching
#: it proves nothing the median does not, and it is 313 MB of shared SEC budget.
_ROUNDTRIP_MAX_BYTES = 20 * 1024 * 1024


def _census(conn: psycopg.Connection[object]) -> None:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT count(*) AS rows,
                   count(*) FILTER (WHERE payload IS NOT NULL) AS live_rows,
                   COALESCE(sum(byte_count), 0) AS logical_bytes,
                   COALESCE(avg(byte_count), 0) AS mean_bytes,
                   COALESCE(percentile_disc(0.5) WITHIN GROUP (ORDER BY byte_count), 0) AS median_bytes,
                   COALESCE(max(byte_count), 0) AS max_bytes,
                   COALESCE(sum(pg_column_size(payload)), 0) AS stored_bytes,
                   count(*) FILTER (WHERE source_url IS NULL) AS no_url
            FROM filing_raw_documents
            WHERE document_kind = %s
            """,
            (_KIND,),
        )
        c = cur.fetchone() or {}
        print(f"=== {_KIND} census ===")
        print(f"  rows                 {int(c.get('rows', 0)):,} ({int(c.get('live_rows', 0)):,} with live payload)")
        print(f"  logical bytes        {int(c.get('logical_bytes', 0)) / 1e9:.3f} GB  (octet_length, sql/107:68)")
        print(f"  stored datum bytes   {int(c.get('stored_bytes', 0)) / 1e9:.3f} GB  (pg_column_size, as compressed)")
        print(f"  mean / median / max  {float(c.get('mean_bytes', 0)) / 1e6:.2f} / ", end="")
        print(f"{int(c.get('median_bytes', 0)) / 1e6:.2f} / {int(c.get('max_bytes', 0)) / 1e6:.1f} MB")
        print(f"  rows without source_url  {int(c.get('no_url', 0))}")

        # Manifest state, and the typed-sink reconciliation. Manifest state
        # alone does NOT establish "produced no typed output" — a re-parse can
        # tombstone an accession whose earlier signal row still exists — so the
        # two anti-joins below are what make that claim a measurement.
        cur.execute(
            """
            SELECT m.ingest_status,
                   count(*) AS rows,
                   COALESCE(sum(r.byte_count), 0) AS bytes,
                   count(*) FILTER (
                       WHERE EXISTS (SELECT 1 FROM pre14a_proposal_signals s
                                     WHERE s.accession_number = m.accession_number)
                   ) AS with_signal_row
            FROM filing_raw_documents r
            JOIN sec_filing_manifest m USING (accession_number)
            WHERE r.document_kind = %s
            GROUP BY 1 ORDER BY 3 DESC
            """,
            (_KIND,),
        )
        print("  by manifest state:")
        for row in cur.fetchall():
            print(
                f"    {str(row['ingest_status']):<12} rows={int(row['rows']):>4} "
                f"bytes={int(row['bytes']) / 1e9:.3f} GB  typed-signal rows={int(row['with_signal_row']):>4}"
            )

        # Exactly what the #1014 sweep would select IF an operator added
        # sec_pre14a to SWEPT_MANIFEST_SOURCES — the real predicate, not a
        # state count. This is the destructive scope to present for approval.
        cur.execute(
            """
            SELECT count(*) AS rows, COALESCE(sum(r.byte_count), 0) AS bytes
            FROM filing_raw_documents r
            JOIN sec_filing_manifest m USING (accession_number)
            WHERE r.document_kind = %s
              AND r.payload IS NOT NULL
              AND m.source = 'sec_pre14a'
              AND m.ingest_status = 'parsed'
              AND m.raw_status IN ('stored', 'compacted')
            """,
            (_KIND,),
        )
        e = cur.fetchone() or {}
        print(
            f"  sweep-eligible IF approved: {int(e.get('rows', 0)):,} rows / "
            f"{int(e.get('bytes', 0)) / 1e9:.3f} GB (full predicate, not a state count)"
        )

        # Does any pre14a_body row hang off a manifest row whose source is NOT
        # sec_pre14a? The sweep joins on accession and filters on SOURCE, so
        # this is what decides whether an unrelated approval could reach these
        # rows. Measured, because "the writer only ever sets sec_pre14a" is an
        # argument about code, and this is the corpus.
        cur.execute(
            """
            SELECT COALESCE(m.source, '(no manifest row)') AS source, count(*) AS rows
            FROM filing_raw_documents r
            LEFT JOIN sec_filing_manifest m USING (accession_number)
            WHERE r.document_kind = %s
            GROUP BY 1 ORDER BY 2 DESC
            """,
            (_KIND,),
        )
        print("  manifest source of these rows:")
        for row in cur.fetchall():
            print(f"    {str(row['source']):<20} {int(row['rows']):>4}")


def _urls(conn: psycopg.Connection[object]) -> int:
    """Full-population structural check of the recovery locator."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT accession_number, source_url FROM filing_raw_documents WHERE document_kind = %s",
            (_KIND,),
        )
        rows = cur.fetchall()

    problems: list[str] = []
    for row in rows:
        accession = str(row["accession_number"])
        raw_url = row["source_url"]
        if raw_url is None or not str(raw_url).strip():
            problems.append(f"{accession}: empty source_url")
            continue
        url = str(raw_url)
        if url != url.strip():
            problems.append(f"{accession}: source_url carries surrounding whitespace")
        parsed = urlparse(url)
        if parsed.scheme != "https":
            problems.append(f"{accession}: scheme={parsed.scheme!r}")
        if parsed.netloc != _EXPECTED_HOST:
            problems.append(f"{accession}: host={parsed.netloc!r}")
        # The accession appears in the archive path undashed; accept the dashed
        # form too rather than assuming one layout.
        undashed = accession.replace("-", "")
        if undashed not in parsed.path and accession not in parsed.path:
            problems.append(f"{accession}: path does not reference the accession ({parsed.path})")

    print(f"=== {_KIND} source_url structural validation (FULL POPULATION) ===")
    print(f"  rows checked   {len(rows):,}")
    print(f"  problems       {len(problems)}")
    for line in problems[:20]:
        print(f"    {line}")
    if len(problems) > 20:
        print(f"    ... {len(problems) - 20} more")
    print("  ⚠ Structural only. A well-formed URL is a precondition for recovery, not a guarantee of it:")
    print("    this arm makes no SEC request and says nothing about future EDGAR availability.")
    return 1 if problems else 0


def _roundtrip(conn: psycopg.Connection[object], sample: int) -> int:
    """Live byte-identity on a size-stratified sample."""
    from app.providers.implementations.sec_edgar import SecFilingsProvider

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # Stratify by size so the sample is not all tiny rows: order by
        # byte_count and take every (n/sample)-th eligible row.
        cur.execute(
            """
            WITH ranked AS (
                SELECT accession_number, source_url, byte_count,
                       encode(sha256(convert_to(payload, 'UTF8')), 'hex') AS stored_sha,
                       row_number() OVER (ORDER BY byte_count) AS rn,
                       count(*) OVER () AS total
                FROM filing_raw_documents
                WHERE document_kind = %s AND payload IS NOT NULL
                  AND source_url IS NOT NULL AND byte_count <= %s
            )
            SELECT accession_number, source_url, byte_count, stored_sha
            FROM ranked
            WHERE rn %% GREATEST(total / %s, 1) = 0
            ORDER BY byte_count
            LIMIT %s
            """,
            (_KIND, _ROUNDTRIP_MAX_BYTES, sample, sample),
        )
        rows = cur.fetchall()

    print(f"=== {_KIND} live byte-identity round-trip (SAMPLE of {len(rows)}) ===")
    mismatches = 0
    with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
        for row in rows:
            accession = str(row["accession_number"])
            url = str(row["source_url"])
            body = provider.fetch_document_text(url)
            if body is None:
                print(f"  UNRECOVERABLE  {accession}  404/410 from {url}")
                mismatches += 1
                continue
            actual = hashlib.sha256(body.encode("utf-8")).hexdigest()
            ok = actual == str(row["stored_sha"])
            mismatches += 0 if ok else 1
            print(f"  {'match      ' if ok else 'MISMATCH   '}  {accession}  {int(row['byte_count']) / 1e6:>7.2f} MB")
    print(f"  mismatches {mismatches} / {len(rows)}")
    print("  ⚠ A SAMPLE. Establishes the stored-hash / re-fetch equality on real filings of this kind;")
    print("    it does not establish population-wide or future recoverability.")
    return 1 if mismatches else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--census", action="store_true", help="per-kind figures + typed-sink reconciliation")
    parser.add_argument("--urls", action="store_true", help="full-population source_url structural validation")
    parser.add_argument("--roundtrip", type=int, default=0, metavar="N", help="live byte-identity on N sampled rows")
    args = parser.parse_args()
    if not (args.census or args.urls or args.roundtrip):
        parser.error("choose at least one of --census / --urls / --roundtrip N")

    assert_dev_environment()
    status = 0
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        if args.census:
            _census(conn)
        if args.urls:
            status |= _urls(conn)
        if args.roundtrip:
            status |= _roundtrip(conn, args.roundtrip)
    return status


if __name__ == "__main__":
    sys.exit(main())
