"""#3236 evidence probe — everything the sweep's design rests on. Read-only.

Answers, on the FULL affected population (not a sample):

  A. Is the snapshot actually repeatable-read?  (asserted, not claimed —
     ``SET default_transaction_isolation`` only binds transactions started
     AFTERWARDS, so under autocommit each statement gets its own snapshot.
     #3232 close-out lesson, repeated here at Codex ckpt-1 finding 36.)
  B. Per-accession header uniformity — is "the stored row" even well defined?
  C. Mixed NULL / non-NULL ``instrument_id`` inside one accession (mislink risk).
  D. Observations keyed on ``source_document_id`` (the natural-key field) as
     well as the nullable ``source_accession``, INCLUDING retired rows
     (``known_to IS NOT NULL``) — a retired row at the key is a silent-failure
     mode because the upsert never clears ``known_to``.
  E. Re-parsed ``filed_at`` vs stored ``filed_at`` — the chokepoint evaluates
     ``filing.filed_at or ref.filed_at``, so a non-NULL re-parse WINS.
  F. Ties on the largest ``aggregate_amount_owned`` (``max()`` takes the first).
  G. Full-column drift, every column the typed table stores.

Run:  PYTHONPATH=. uv run python -m scripts.probe_3236_parser_drift
"""

from __future__ import annotations

from decimal import Decimal

import psycopg

from app.config import settings
from app.providers.implementations.sec_13dg import parse_primary_doc
from app.services.blockholders import (
    _resolve_issuer_to_instrument_id,
    resolve_blockholder_reporter_identity,
)

# Columns compared row-for-row between the stored typed rows and the re-parse.
PERSON_COLS = (
    "reporter_cik",
    "reporter_name",
    "reporter_no_cik",
    "aggregate_amount_owned",
    "percent_of_class",
    "sole_voting_power",
    "shared_voting_power",
    "sole_dispositive_power",
    "shared_dispositive_power",
    "member_of_group",
    "type_of_reporting_person",
    "citizenship",
)
HEADER_COLS = (
    "submission_type",
    "status",
    "issuer_cik",
    "issuer_cusip",
    "securities_class_title",
    "date_of_event",
    "filed_at",
)


def _norm(value: object) -> object:
    if isinstance(value, Decimal):
        return value.normalize()
    return value


def _sortable(row: tuple[object, ...]) -> tuple[tuple[int, str], ...]:
    """Total order over mixed None / str / Decimal tuples — plain ``sorted``
    raises on ``None < str``. Compares repr, which is stable and only used to
    line the two sequences up for a field-by-field diff."""
    return tuple((0, "") if v is None else (1, repr(v)) for v in row)


def main() -> None:
    conn = psycopg.connect(settings.database_url, autocommit=False)
    # Set on the CONNECTION, before any statement opens the implicit
    # transaction — then ASSERT it. Claiming an isolation level is not
    # having one.
    conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
    with conn:
        level = conn.execute("SHOW transaction_isolation").fetchone()
        assert level is not None and level[0] == "repeatable read", f"isolation is {level}"
        print(f"A. snapshot isolation asserted: {level[0]}")
        print()

        targets: list[tuple[str, int]] = []
        rows = conn.execute(
            """
            SELECT accession_number, min(issuer_cik), min(issuer_cusip)
            FROM blockholder_filings
            WHERE instrument_id IS NULL
            GROUP BY accession_number ORDER BY accession_number
            """
        ).fetchall()
        for accession, issuer_cik, issuer_cusip in rows:
            iid = _resolve_issuer_to_instrument_id(conn, cusip=issuer_cusip, cik=issuer_cik)
            if iid is None:
                continue
            seen = conn.execute(
                """
                SELECT 1 FROM ownership_blockholders_observations
                WHERE source_document_id = %(a)s OR source_accession = %(a)s LIMIT 1
                """,
                {"a": accession},
            ).fetchone()
            if seen is None:
                targets.append((accession, iid))
        accs = [a for a, _ in targets]
        print(f"targets (no observation on EITHER key): {len(targets)}")

        # -- B. per-accession header uniformity ------------------------------
        nonuniform = conn.execute(
            f"""
            SELECT accession_number,
                   {", ".join(f"count(DISTINCT {c}) AS d_{c}" for c in HEADER_COLS)},
                   count(DISTINCT filer_id) AS d_filer_id
            FROM blockholder_filings
            WHERE accession_number = ANY(%(accs)s)
            GROUP BY accession_number
            HAVING {" OR ".join(f"count(DISTINCT {c}) > 1" for c in HEADER_COLS)}
               OR count(DISTINCT filer_id) > 1
            """,
            {"accs": accs},
        ).fetchall()
        print(f"B. accessions with non-uniform header/filer across rows: {len(nonuniform)}")
        for r in nonuniform:
            print(f"     {r}")

        # -- C. mixed NULL / non-NULL link inside one accession ---------------
        mixed = conn.execute(
            """
            SELECT accession_number,
                   count(*) FILTER (WHERE instrument_id IS NULL) AS nulls,
                   count(DISTINCT instrument_id)                 AS distinct_links
            FROM blockholder_filings
            GROUP BY accession_number
            HAVING count(*) FILTER (WHERE instrument_id IS NULL) > 0
               AND count(DISTINCT instrument_id) > 0
            """
        ).fetchall()
        print(f"C. accessions mixing NULL and non-NULL links (WHOLE TABLE): {len(mixed)}")
        for r in mixed[:10]:
            print(f"     {r}")
        multi = conn.execute(
            """
            SELECT count(*) FROM (
              SELECT accession_number FROM blockholder_filings
              GROUP BY accession_number HAVING count(DISTINCT instrument_id) > 1
            ) t
            """
        ).fetchone()
        print(f"   accessions pointing at >1 distinct instrument (WHOLE TABLE): {multi[0]}")  # type: ignore[index]

        # -- D. retired / live observations at the key ------------------------
        obs = conn.execute(
            """
            SELECT count(*) FILTER (WHERE known_to IS NULL)     AS live,
                   count(*) FILTER (WHERE known_to IS NOT NULL) AS retired
            FROM ownership_blockholders_observations
            WHERE source_document_id = ANY(%(accs)s) OR source_accession = ANY(%(accs)s)
            """,
            {"accs": accs},
        ).fetchone()
        print(f"D. observations already at these accessions: live={obs[0]} retired={obs[1]}")  # type: ignore[index]
        retired_any = conn.execute(
            "SELECT count(*) FROM ownership_blockholders_observations WHERE known_to IS NOT NULL"
        ).fetchone()
        print(f"   retired blockholder observations anywhere in the table: {retired_any[0]}")  # type: ignore[index]

        # -- E/F/G per accession ---------------------------------------------
        print()
        print("E/F/G. per-accession re-parse comparison")
        drift_by_col: dict[str, int] = {}
        reparse_filed_at_wins = 0
        filed_at_differs = 0
        ties = 0
        samples: dict[str, set[tuple[str, str]]] = {}
        order_drift: list[str] = []
        for accession, _iid in targets:
            body = conn.execute(
                """
                SELECT payload FROM filing_raw_documents
                WHERE accession_number = %(a)s AND document_kind = 'primary_doc_13dg'
                """,
                {"a": accession},
            ).fetchone()
            assert body is not None
            filing = parse_primary_doc(body[0])
            stored = conn.execute(
                f"""
                SELECT {", ".join(PERSON_COLS)}, {", ".join(HEADER_COLS)}
                FROM blockholder_filings WHERE accession_number = %(a)s
                ORDER BY filing_id
                """,
                {"a": accession},
            ).fetchall()

            n = len(PERSON_COLS)
            stored_people = sorted((tuple(_norm(v) for v in r[:n]) for r in stored), key=_sortable)
            parsed_people = sorted(
                [
                    (
                        p.cik,
                        p.name,
                        p.no_cik,
                        _norm(p.aggregate_amount_owned),
                        _norm(p.percent_of_class),
                        _norm(p.sole_voting_power),
                        _norm(p.shared_voting_power),
                        _norm(p.sole_dispositive_power),
                        _norm(p.shared_dispositive_power),
                        p.member_of_group,
                        p.type_of_reporting_person,
                        p.citizenship,
                    )
                    for p in filing.reporting_persons
                ],
                key=_sortable,
            )
            if stored_people != parsed_people:
                drift_by_col["__people__"] = drift_by_col.get("__people__", 0) + 1
                for sr, pr in zip(stored_people, parsed_people):
                    for col, sv, pv in zip(PERSON_COLS, sr, pr):
                        if sv != pv:
                            drift_by_col[col] = drift_by_col.get(col, 0) + 1
                            samples.setdefault(col, set()).add((repr(sv), repr(pv)))

            # ORDER-SENSITIVE comparison: with a tie on the largest aggregate,
            # ``max()`` returns the FIRST element, so the observation identity
            # depends on parser element order. A sorted multiset gate cannot
            # see that. Stored rows are in insert order via filing_id.
            stored_seq = [tuple(_norm(v) for v in r[:n]) for r in stored]
            parsed_seq = [
                (
                    q.cik,
                    q.name,
                    q.no_cik,
                    _norm(q.aggregate_amount_owned),
                    _norm(q.percent_of_class),
                    _norm(q.sole_voting_power),
                    _norm(q.shared_voting_power),
                    _norm(q.sole_dispositive_power),
                    _norm(q.shared_dispositive_power),
                    q.member_of_group,
                    q.type_of_reporting_person,
                    q.citizenship,
                )
                for q in filing.reporting_persons
            ]
            IDCOLS = (0, 1, 3, 4)  # cik, name, aggregate, percent
            if [tuple(r[i] for i in IDCOLS) for r in stored_seq] != [tuple(r[i] for i in IDCOLS) for r in parsed_seq]:
                order_drift.append(accession)

            hdr = dict(zip(HEADER_COLS, stored[0][n:]))
            parsed_hdr = {
                "submission_type": filing.submission_type,
                "status": filing.status,
                "issuer_cik": filing.issuer_cik,
                "issuer_cusip": filing.issuer_cusip,
                "securities_class_title": filing.securities_class_title,
                "date_of_event": filing.date_of_event,
                "filed_at": filing.filed_at,
            }
            for col in HEADER_COLS:
                if col == "filed_at":
                    continue
                if hdr[col] != parsed_hdr[col]:
                    drift_by_col[col] = drift_by_col.get(col, 0) + 1
                    samples.setdefault(col, set()).add((repr(hdr[col]), repr(parsed_hdr[col])))

            if filing.filed_at is not None:
                reparse_filed_at_wins += 1
                if filing.filed_at != hdr["filed_at"]:
                    filed_at_differs += 1
                    print(
                        f"   {accession}  filed_at stored={hdr['filed_at']} "
                        f"reparse={filing.filed_at}  <- REPARSE WOULD WIN"
                    )

            aggs = [p.aggregate_amount_owned for p in filing.reporting_persons if p.aggregate_amount_owned is not None]
            if aggs and aggs.count(max(aggs)) > 1:
                ties += 1
                ident = resolve_blockholder_reporter_identity(
                    filing.reporting_persons, document_filer_cik=filing.document_filer_cik
                )
                print(
                    f"   {accession}  TIE on largest aggregate ({aggs.count(max(aggs))} "
                    f"reporters at {max(aggs)}) -> picked {ident.reporter_cik if ident else None}"
                )

        print()
        print(f"G. drift by column (of {len(targets)} accessions): {drift_by_col or 'NONE'}")
        print(f"E. re-parse supplies a non-NULL filed_at on   {reparse_filed_at_wins} / {len(targets)}")
        print(f"   … and it DIFFERS from the stored value on  {filed_at_differs} / {len(targets)}")
        print(f"F. accessions with a tie on largest aggregate: {ties} / {len(targets)}")
        print()
        print("   drift direction (stored -> reparse), up to 4 distinct per column:")
        for col, vals in sorted(samples.items()):
            for sv, pv in sorted(vals)[:4]:
                print(f"     {col:<24} {sv:<28} -> {pv}")
        print()
        print(f"H. ORDER-SENSITIVE identity-column drift: {len(order_drift)} / {len(targets)}")
        for a in order_drift:
            print(f"     {a}")


if __name__ == "__main__":
    main()
