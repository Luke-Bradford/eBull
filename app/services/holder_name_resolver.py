"""Holder-name → filer CIK resolver shared between DEF 14A drift
detection and the ownership-rollup service.

DEF 14A's beneficial-ownership table stores ``holder_name`` only — no
``filer_cik`` — because SEC's proxy-statement schema names individuals
without requiring an EDGAR identifier. To dedup DEF 14A rows against
Form 4 / Form 3 / 13D/G winners we have to resolve the holder name to
a filer_cik first, then run the canonical CIK-priority dedup.

This module is the single source of truth for that resolution. It
was lifted from ``def14a_drift._normalise_name`` +
``def14a_drift._resolve_holder_match`` so the ownership-rollup
service (#789) and the drift detector (#769) cannot drift apart on
match semantics — Codex spec review caught the duplication risk on
the v1 spec for #789.

Match precedence:

  1. Latest Form 4 ``post_transaction_shares`` for the same instrument
     whose normalised filer_name equals the normalised holder_name.
     Tie-broken by ``txn_date DESC, id DESC``.
  2. Falling back to ``insider_initial_holdings`` (Form 3 baseline)
     when no Form 4 row matches. Same DISTINCT ON cap.
  3. Otherwise ``(False, None, None)`` — a coverage gap from the
     drift detector's perspective; for the ownership-rollup the row
     goes to the ``def14a_unmatched`` slice.

Match accepts ``filer_cik IS NULL`` rows: legacy / backfilled Form 4
rows can carry NULL CIK and a clean reconciliation on such a row is
still a real reconciliation, not a coverage gap.

NULL-CIK identity uses ``LOWER(TRIM(filer_name))`` so two distinct
NULL-CIK officers do not collapse into one bucket. Codex review
caught the prior over-collapse on the v3 spec for #789.
"""

from __future__ import annotations

from decimal import Decimal

import psycopg
import psycopg.rows


def normalise_name(holder_name: str) -> str:
    """Normalise a holder / filer name for exact case-insensitive
    match.

    Strips:
      * Leading / trailing whitespace
      * Trailing role suffixes after the first separator (``", CEO"``
        / ``" - Director"`` / ``" — Director"`` / ``" – Director"``).

    Returns the lowercase residual. Used on both sides of the
    DEF 14A holder ↔ Form 4 filer match — the proxy name is
    normalised once when building the SQL parameter, the Form 4
    filer name is normalised in Python so the strip semantics stay
    in one place.

    Codex pre-push review (on the original DEF 14A drift PR) caught
    the prior ILIKE-substring approach matching false positives
    (``"Ann"`` -> ``"Joanne Smith"``; ``"John Doe"`` -> ``"John Doe
    Jr"``). Exact match after role-suffix strip is conservative —
    name variants fall through to the unmatched bucket, which the
    follow-on curated holder→filer mapping seed table (#790's
    territory) will resolve.
    """
    base = holder_name.strip()
    for sep in (",", " - ", " — ", " – "):
        if sep in base:
            base = base.split(sep, 1)[0].strip()
            break
    return base.lower()


def resolve_holder_to_filer(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    holder_name: str,
) -> tuple[bool, str | None, Decimal | None]:
    """Resolve a DEF 14A holder against Form 4 + Form 3 baseline.

    Returns ``(matched, matched_filer_cik, latest_known_shares)``:

      * ``matched`` is ``True`` when a Form 4 or Form 3 row was found
        whose normalised filer name equals the normalised holder
        name. Distinct from ``matched_filer_cik`` because legacy
        Form 4 rows can have NULL ``filer_cik`` — a zero-drift
        reconciliation on a NULL-CIK row is still a real
        reconciliation, not a coverage gap.
      * ``matched_filer_cik`` is the resolved CIK (when present) or
        ``None`` for legacy NULL-CIK rows.
      * ``latest_known_shares`` is the latest Form 4
        ``post_transaction_shares`` for the matched filer, falling
        back to the Form 3 baseline ``shares`` when no Form 4 row
        exists.

    Implementation note: candidate filers are fetched in bulk per
    instrument and filtered in Python via :func:`normalise_name` so
    the role-suffix strip is the single canonical source. An earlier
    draft did the strip in SQL via ``SPLIT_PART(..., ',', 1)``,
    which only matched the comma case and silently failed for the
    dash variants.
    """
    normalised = normalise_name(holder_name)
    if not normalised:
        return (False, None, None)

    # Form 4 first — the cumulative running total. The DISTINCT ON cap
    # pre-collapses multi-transaction filer histories to one row each
    # (the latest), so the Python filter sees one row per filer
    # regardless of an issuer's transaction volume. COALESCE-keyed
    # filer_cik keeps NULL-CIK rows individually addressable rather
    # than collapsed to a single NULL bucket.
    #
    # CIK-preference re-sort: the DB's natural ASC ordering on
    # ``COALESCE(filer_cik, '')`` puts NULL-CIK rows first, so a
    # filer with both a legacy NULL-CIK row and a newer CIK-backed
    # row would resolve to ``(matched=True, cik=None)`` even though
    # the canonical CIK is on file. Re-sort the fetchall so non-NULL
    # CIK rows match first; legacy rows still match when nothing
    # better exists. Codex pre-push review (Batch 1 of #788) caught
    # this.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (COALESCE(filer_cik, ''), filer_name)
                filer_cik, filer_name, post_transaction_shares
            FROM insider_transactions
            WHERE instrument_id = %(iid)s
              AND post_transaction_shares IS NOT NULL
            ORDER BY COALESCE(filer_cik, ''), filer_name,
                     txn_date DESC NULLS LAST, id DESC
            """,
            {"iid": instrument_id},
        )
        rows = sorted(
            cur.fetchall(),
            key=lambda r: (r["filer_cik"] is None, str(r.get("filer_name") or "")),
        )
        for row in rows:
            if normalise_name(str(row["filer_name"])) == normalised:
                return (
                    True,
                    str(row["filer_cik"]) if row["filer_cik"] is not None else None,
                    row["post_transaction_shares"],
                )

    # Fall back to Form 3 baseline. Same DISTINCT ON cap so the
    # Python filter never sees more than one row per filer.
    # (insider_initial_holdings.filer_cik is NOT NULL by schema, so
    # the CIK-preference re-sort is a no-op here — kept for
    # symmetry / future-proofing.)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (COALESCE(filer_cik, ''), filer_name)
                filer_cik, filer_name, shares
            FROM insider_initial_holdings
            WHERE instrument_id = %(iid)s
              AND shares IS NOT NULL
              AND is_derivative = FALSE
            ORDER BY COALESCE(filer_cik, ''), filer_name,
                     as_of_date DESC NULLS LAST
            """,
            {"iid": instrument_id},
        )
        rows = sorted(
            cur.fetchall(),
            key=lambda r: (r["filer_cik"] is None, str(r.get("filer_name") or "")),
        )
        for row in rows:
            if normalise_name(str(row["filer_name"])) == normalised:
                return (
                    True,
                    str(row["filer_cik"]) if row["filer_cik"] is not None else None,
                    row["shares"],
                )

    return (False, None, None)
