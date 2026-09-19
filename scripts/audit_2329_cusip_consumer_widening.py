"""Read-only census of what the #2329 ``provider='sec'`` CUSIP narrowings cost.

#2329 filed two sites as **latent** — ``n_port_ingest._resolve_cusip_to_instrument_id``
and ``blockholders._resolve_cusip_to_instrument_id`` — on the premise that each has a
provider-wide bulk twin that covers whatever it drops. This script exists because that
premise is a claim, not a measurement, and it is false for the N-PORT site.

The N-PORT arm is the honest one to run because it needs no re-fetch and no sampling:
the manifest parser stores every payload it parses (``filing_raw_documents``,
``document_kind='nport_xml'``), so the FULL population of the live per-filing path can
be re-parsed from disk and each holding re-resolved under both filters. Three things
are reported separately, because averaging them hides the finding:

* **resolution** — distinct CUSIPs resolvable under the narrow filter, the widened one,
  and neither. A CUSIP that resolves to a DIFFERENT instrument under the two filters
  would be a re-attribution and is reported loudly; #2213's full-population A/B over all
  68,694 CUSIPs found 0, and this run asserts the same locally.
* **holdings dropped** — eligible (equity / common / Long / NS / positive-shares /
  9-char-CUSIP) holdings the narrow filter refuses. This is the live arm.
* **twin coverage** — of those dropped ``(accession, instrument)`` pairs, how many
  ``ownership_funds_observations`` already holds via the bulk DERA path. The remainder
  is present-day data loss, and it is the number the "latent" label denies.

⚠ The twin cannot cover a recent filing by construction: DERA publishes N-PORT
quarterly, so an accession the manifest worker parses the week it is filed has no bulk
row for a quarter or more. Expect the uncovered share to be high and to grow with
recency, not to trend to zero.

The blockholders arm is a crosstab rather than a re-parse: that function had no callers
(deleted by #2329, not widened), so there is nothing to replay. What IS worth standing
measurement is the separate defect the ticket flagged — ``blockholder_filings`` rows
left ``instrument_id IS NULL`` despite a mapping existing — split by which provider
holds the mapping, because only the OpenFIGI-only count is attributable to a narrowing.

Usage (read-only; safe against the dev DB at any time):

    PYTHONPATH=. uv run python -m scripts.audit_2329_cusip_consumer_widening
    PYTHONPATH=. uv run python -m scripts.audit_2329_cusip_consumer_widening --json
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

import psycopg

from app.config import settings
from app.services.n_port_ingest import parse_n_port_payload
from app.services.raw_filings import stored_body

# The pre-#2329 lookup, spelled out rather than imported: the whole point is to compare
# it against the shipped one, and importing the shipped function twice would compare it
# with itself. If the shipped query ever diverges in a way this does not model, the
# `wide` arm below catches it — that one IS the imported function.
NARROW_SQL = """
    SELECT instrument_id
    FROM external_identifiers
    WHERE provider = 'sec'
      AND identifier_type = 'cusip'
      AND identifier_value = %(cusip)s
    ORDER BY is_primary DESC, external_identifier_id ASC
    LIMIT 1
"""


def _eligible(holding: Any) -> bool:
    """The write-side filter ladder from ``n_port_ingest._ingest_single_accession``
    and its manifest twin, up to (but not including) CUSIP resolution. A holding that
    fails any of these is dropped for a reason that has nothing to do with #2329."""
    if holding.asset_category != "EC":
        return False
    if holding.payoff_profile != "Long":
        return False
    if holding.units != "NS":
        return False
    if holding.shares is None or holding.shares <= 0:
        return False
    return bool(holding.cusip) and len(holding.cusip) == 9


def _audit_n_port(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    from app.services.n_port_ingest import _resolve_cusip_to_instrument_id

    cur = conn.execute(
        """
        SELECT accession_number
        FROM sec_filing_manifest
        WHERE source = 'sec_n_port' AND ingest_status = 'parsed'
        ORDER BY accession_number
        """
    )
    accessions = [r[0] for r in cur.fetchall()]

    narrow_cache: dict[str, int | None] = {}

    def narrow(cusip: str) -> int | None:
        if cusip not in narrow_cache:
            row = conn.execute(NARROW_SQL, {"cusip": cusip}).fetchone()
            narrow_cache[cusip] = int(row[0]) if row is not None else None
        return narrow_cache[cusip]

    missing_body: list[str] = []
    parse_errors: list[str] = []
    cusips: set[str] = set()
    eligible_holdings = 0
    dropped_holdings = 0
    dropped_pairs: set[tuple[str, int]] = set()
    reattributed: list[tuple[str, int, int]] = []
    lost: list[str] = []

    for accession in accessions:
        body = stored_body(conn, accession_number=accession, document_kind="nport_xml")
        if not body:
            missing_body.append(accession)
            continue
        try:
            parsed = parse_n_port_payload(body)
        except Exception as exc:  # noqa: BLE001 — a parse failure is a finding, not a crash
            parse_errors.append(f"{accession}: {exc}")
            continue
        for holding in parsed.holdings:
            if not _eligible(holding):
                continue
            eligible_holdings += 1
            cusip = holding.cusip.strip().upper()
            cusips.add(cusip)
            narrow_iid = narrow(cusip)
            wide_iid = _resolve_cusip_to_instrument_id(conn, cusip)
            if narrow_iid is None and wide_iid is not None:
                dropped_holdings += 1
                dropped_pairs.add((accession, wide_iid))
            elif narrow_iid is not None and wide_iid is None:
                lost.append(cusip)
            elif narrow_iid is not None and wide_iid is not None and narrow_iid != wide_iid:
                reattributed.append((cusip, narrow_iid, wide_iid))

    covered_by_twin = 0
    uncovered: list[tuple[str, int]] = []
    for accession, instrument_id in sorted(dropped_pairs):
        row = conn.execute(
            """
            SELECT 1 FROM ownership_funds_observations
            WHERE source_accession = %s AND instrument_id = %s
            LIMIT 1
            """,
            (accession, instrument_id),
        ).fetchone()
        if row is not None:
            covered_by_twin += 1
        else:
            uncovered.append((accession, instrument_id))

    narrow_only_cusips = sum(
        1 for c in cusips if narrow(c) is None and _resolve_cusip_to_instrument_id(conn, c) is not None
    )

    return {
        "accessions_parsed": len(accessions),
        "accessions_missing_stored_body": len(missing_body),
        "parse_errors": parse_errors,
        "eligible_holdings": eligible_holdings,
        "distinct_cusips": len(cusips),
        "cusips_resolvable_only_when_widened": narrow_only_cusips,
        "holdings_dropped_by_narrow_filter": dropped_holdings,
        "dropped_accession_instrument_pairs": len(dropped_pairs),
        "pairs_covered_by_bulk_twin": covered_by_twin,
        "pairs_uncovered_real_loss": len(uncovered),
        # Both MUST be empty. A re-attribution means the widening moved a holding to a
        # different issuer, which is a data-integrity failure and not an improvement;
        # a loss means the widened query drops something the narrow one resolved, which
        # is impossible for a superset filter and would indicate an ordering bug.
        "reattributed": reattributed,
        "lost": lost,
    }


def _audit_blockholders(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT
          count(*) AS total,
          count(*) FILTER (WHERE instrument_id IS NULL) AS null_instrument,
          count(*) FILTER (
            WHERE instrument_id IS NULL AND issuer_cusip IS NOT NULL
              AND EXISTS (SELECT 1 FROM external_identifiers e
                          WHERE e.identifier_type = 'cusip' AND e.provider = 'sec'
                            AND e.identifier_value = upper(trim(blockholder_filings.issuer_cusip)))
          ) AS null_with_sec_mapping,
          count(*) FILTER (
            WHERE instrument_id IS NULL AND issuer_cusip IS NOT NULL
              AND EXISTS (SELECT 1 FROM external_identifiers e
                          WHERE e.identifier_type = 'cusip' AND e.provider = 'openfigi'
                            AND e.identifier_value = upper(trim(blockholder_filings.issuer_cusip)))
              AND NOT EXISTS (SELECT 1 FROM external_identifiers e
                              WHERE e.identifier_type = 'cusip' AND e.provider = 'sec'
                                AND e.identifier_value = upper(trim(blockholder_filings.issuer_cusip)))
          ) AS null_with_openfigi_only_mapping
        FROM blockholder_filings
        """
    ).fetchone()
    assert row is not None
    return {
        "blockholder_filings": int(row[0]),
        "instrument_id_null": int(row[1]),
        "null_with_sec_mapping": int(row[2]),
        "null_with_openfigi_only_mapping": int(row[3]),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON only")
    args = parser.parse_args(argv)

    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        result = {
            "n_port_per_filing_path": _audit_n_port(conn),
            "blockholder_null_instrument_crosstab": _audit_blockholders(conn),
        }

    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    n = result["n_port_per_filing_path"]
    b = result["blockholder_null_instrument_crosstab"]
    print("#2329 — N-PORT per-filing path (FULL population of parsed accessions)")
    print(f"  accessions parsed                     {n['accessions_parsed']:>8}")
    print(f"  … missing a stored nport_xml body     {n['accessions_missing_stored_body']:>8}")
    print(f"  eligible holdings                     {n['eligible_holdings']:>8}")
    print(f"  distinct CUSIPs                       {n['distinct_cusips']:>8}")
    print(f"  … resolvable ONLY when widened        {n['cusips_resolvable_only_when_widened']:>8}")
    print(f"  holdings dropped by narrow filter     {n['holdings_dropped_by_narrow_filter']:>8}")
    print(f"  distinct (accession, instrument)      {n['dropped_accession_instrument_pairs']:>8}")
    print(f"  … already covered by the bulk twin    {n['pairs_covered_by_bulk_twin']:>8}")
    print(f"  … NOT covered — real data loss        {n['pairs_uncovered_real_loss']:>8}")
    print(f"  re-attributed (must be 0)             {len(n['reattributed']):>8}")
    print(f"  lost (must be 0)                      {len(n['lost']):>8}")
    print()
    print("#2329 — blockholder_filings NULL-instrument crosstab (the separate defect)")
    print(f"  rows                                  {b['blockholder_filings']:>8}")
    print(f"  instrument_id IS NULL                 {b['instrument_id_null']:>8}")
    print(f"  … with a sec CUSIP mapping            {b['null_with_sec_mapping']:>8}")
    print(f"  … with an OpenFIGI-ONLY mapping       {b['null_with_openfigi_only_mapping']:>8}")

    if n["parse_errors"]:
        print("\nPARSE ERRORS:")
        for err in n["parse_errors"]:
            print(f"  {err}")

    # A re-attribution or a loss is a correctness failure in the widening itself, not a
    # census result — exit non-zero so a scripted run cannot report it as a clean pass.
    return 1 if (n["reattributed"] or n["lost"]) else 0


if __name__ == "__main__":
    sys.exit(main())
