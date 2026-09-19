"""Full-population census for #3236 — stranded 13D/G accessions.

Read-only. Re-falsifies the inherited 82-row / 19-accession figure from
#3236's issue body (working-order step 3c: an inherited root cause is a
premise, not a finding).

⚠ The resolvable/unresolvable split is decided by CALLING
``blockholders._resolve_issuer_to_instrument_id`` per candidate, never by
restating its two tiers in SQL. PG ``trim()`` is not Python ``.strip()``
and ``lpad(...,10,'0')`` truncates an overlong CIK, so a SQL restatement
is a different predicate wearing the same name (#3236 rejected-design
finding 5). SQL is used only as a *superset* prefilter.

Run:  PYTHONPATH=. uv run python -m scripts.census_3236_stranded_13dg
"""

from __future__ import annotations

import psycopg

from app.config import settings
from app.services.blockholders import _resolve_issuer_to_instrument_id


def main() -> None:
    conn = psycopg.connect(settings.database_url, autocommit=False)
    # ⚠ Set on the CONNECTION, before any statement opens the implicit
    # transaction — then ASSERT it. ``SET default_transaction_isolation``
    # binds transactions started AFTERWARDS, so issuing it under
    # ``autocommit=True`` leaves every statement on its own READ COMMITTED
    # snapshot while the script claims one consistent view. Same defect the
    # #3232 close-out recorded; caught again here at Codex checkpoint 1.
    conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
    with conn:
        level = conn.execute("SHOW transaction_isolation").fetchone()
        assert level is not None and level[0] == "repeatable read", f"isolation is {level}"

        total = conn.execute("SELECT count(*) FROM blockholder_filings").fetchone()
        null_rows, null_accs = conn.execute(
            """
            SELECT count(*), count(DISTINCT accession_number)
            FROM blockholder_filings
            WHERE instrument_id IS NULL
            """
        ).fetchone()  # type: ignore[misc]
        print(f"blockholder_filings total              {total[0]:>9,}")  # type: ignore[index]
        print(f"  instrument_id IS NULL                {null_rows:>9,}  ({null_accs:,} accessions)")

        # Superset prefilter: every NULL-link row, one row per accession
        # carrying the stored issuer keys the resolver would be handed.
        cur = conn.execute(
            """
            SELECT accession_number,
                   min(issuer_cik)   AS issuer_cik,
                   min(issuer_cusip) AS issuer_cusip,
                   count(*)          AS null_rows,
                   min(submission_type) AS submission_type
            FROM blockholder_filings
            WHERE instrument_id IS NULL
            GROUP BY accession_number
            ORDER BY accession_number
            """
        )
        candidates = cur.fetchall()

        resolvable: list[tuple[str, int, int]] = []  # (accession, instrument_id, null_rows)
        for accession, issuer_cik, issuer_cusip, rows, _stype in candidates:
            iid = _resolve_issuer_to_instrument_id(conn, cusip=issuer_cusip, cik=issuer_cik)
            if iid is not None:
                resolvable.append((accession, iid, rows))

        res_rows = sum(r for _, _, r in resolvable)
        print(f"  … resolvable through the resolver    {res_rows:>9,}  ({len(resolvable):,} accessions)")
        print(
            f"  … not resolvable today               {null_rows - res_rows:>9,}  "
            f"({null_accs - len(resolvable):,} accessions)"
        )

        if not resolvable:
            return

        accs = [a for a, _, _ in resolvable]

        # Cause A (fixed by #3235) vs cause B (this ticket): does a live
        # observation exist for the accession at all?
        # ⚠ Both key columns. ``source_accession`` is NULLABLE; the accession
        # travels in ``source_document_id``, which is the natural-key member.
        # Checking only the former understates "has no observation".
        obs = dict(
            conn.execute(
                """
                SELECT coalesce(source_accession, source_document_id), count(*)
                FROM ownership_blockholders_observations
                WHERE source_accession = ANY(%(accs)s)
                   OR source_document_id = ANY(%(accs)s)
                GROUP BY 1
                """,
                {"accs": accs},
            ).fetchall()
        )
        with_obs = [a for a in accs if obs.get(a, 0) > 0]
        without_obs = [a for a in accs if obs.get(a, 0) == 0]
        rows_by_acc = {a: r for a, _, r in resolvable}
        print()
        print(
            f"  cause A (live observation exists)    "
            f"{sum(rows_by_acc[a] for a in with_obs):>9,}  ({len(with_obs):,} accessions)"
        )
        print(
            f"  cause B (NO observation at all)      "
            f"{sum(rows_by_acc[a] for a in without_obs):>9,}  ({len(without_obs):,} accessions)  <- #3236"
        )

        if not without_obs:
            return

        # Raw body availability — the repair reads the stored primary doc.
        raw = dict(
            conn.execute(
                """
                SELECT accession_number, length(payload)
                FROM filing_raw_documents
                WHERE accession_number = ANY(%(accs)s)
                  AND document_kind = 'primary_doc_13dg'
                """,
                {"accs": without_obs},
            ).fetchall()
        )
        print()
        print(f"  raw primary_doc_13dg present         {len(raw):>9,} / {len(without_obs):,}")

        # Which ingest path wrote each accession? The in-house parser sets
        # document_filer_cik == primary_filer_cik == blockholder_filers.cik
        # (sec_13dg.py:610-612); the edgartools manifest adapter can make
        # primary_filer_cik the ISSUER cik (#1638). If filer.cik == issuer_cik
        # the stored filer row cannot stand in for document_filer_cik.
        print()
        print("  per-accession detail")
        print("  accession              rows iid     filer_cik   issuer_cik  filer==issuer  raw_bytes  instruments")
        detail = conn.execute(
            """
            SELECT bf.accession_number,
                   min(f.cik)           AS filer_cik,
                   min(bf.issuer_cik)   AS issuer_cik,
                   count(*)             AS rows
            FROM blockholder_filings bf
            JOIN blockholder_filers f ON f.filer_id = bf.filer_id
            WHERE bf.accession_number = ANY(%(accs)s)
            GROUP BY bf.accession_number
            ORDER BY bf.accession_number
            """,
            {"accs": without_obs},
        ).fetchall()
        iid_by_acc = {a: i for a, i, _ in resolvable}
        collisions = 0
        for accession, filer_cik, issuer_cik, rows in detail:
            iid = iid_by_acc[accession]
            sym = conn.execute("SELECT symbol FROM instruments WHERE instrument_id = %(i)s", {"i": iid}).fetchone()
            same = filer_cik == issuer_cik
            collisions += 1 if same else 0
            print(
                f"  {accession}  {rows:>4} {iid:<7} {filer_cik:<11} {issuer_cik:<11} "
                f"{'YES' if same else 'no':<14} {raw.get(accession, 0):>9}  {sym[0] if sym else '?'}"
            )
        print()
        print(f"  filer_cik == issuer_cik (adapter tell) {collisions} / {len(detail)}")

        # Distinct instruments whose drill-through can move.
        iids = sorted({iid_by_acc[a] for a in without_obs})
        print(f"  distinct instruments affected          {len(iids)}")


if __name__ == "__main__":
    main()
