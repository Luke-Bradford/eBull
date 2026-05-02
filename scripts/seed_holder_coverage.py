"""Bootstrap script: seed + ingest holder coverage so the ownership
card renders all 5 categories (#766 + #730 + #740 / #781 + #782).

Without seed data the new holder-coverage tables (institutional /
blockholder filer seeds) stay empty and the ownership card silently
omits Institutions / ETFs / Blockholders, leaving the operator
with only Insiders + Treasury rendered.

This script is idempotent -- running twice doesn't insert duplicate
seed rows or re-fetch already-ingested accessions. Each step:

  1. Adds curated CIK rows to ``institutional_filer_seeds`` and
     ``blockholder_filer_seeds``. Existing CIKs UPSERT in place.
  2. Optionally walks ``etf_filer_cik_seeds`` for the curated
     ETF list when the operator wants to over-tag CIKs from the
     institutional seeds as ETFs.
  3. Runs the 13F-HR batch ingester (``ingest_all_active_filers``
     in :mod:`app.services.institutional_holdings`).
  4. Runs the 13D/G blockholder batch ingester (``ingest_all_active_filers``
     in :mod:`app.services.blockholders`).
  5. Runs the CUSIP resolver (#781) so 13F holdings whose CUSIP
     wasn't in ``external_identifiers`` get fuzzy-matched and
     promoted. Subsequent re-runs of step 3 will then resolve those
     holdings (operator clears the unresolved-cusips log if
     desired; this script does not).
  6. Runs the N-CEN classifier (#782) so filer_type splits ETF /
     INV / INS correctly on the operator UI chip.

Usage:

    uv run python -m scripts.seed_holder_coverage --apply
    uv run python -m scripts.seed_holder_coverage --apply --skip-ingest

``--skip-ingest`` writes the seed rows only -- useful for staging
before the operator wants to trigger the live SEC fetch via the
existing scheduler. Default (no ``--apply``) is dry-run: prints
the seed plan without writing.

Bandwidth: SEC fair-use is 10 req/sec for public traffic. The
provider's internal throttle handles spacing. ~10 institutional
seeds x ~10 quarterly 13F-HR accessions = 100 fetches ~ 10 sec
wall clock. Blockholder seeds vary by activist activity but
typically <5 accessions per filer per year.
"""

from __future__ import annotations

import argparse
import logging
import sys

import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.services.blockholders import (
    ingest_all_active_filers as ingest_all_blockholders,
)
from app.services.blockholders import (
    seed_filer as seed_blockholder_filer,
)
from app.services.cusip_resolver import resolve_unresolved_cusips
from app.services.institutional_holdings import (
    ingest_all_active_filers as ingest_all_institutional,
)
from app.services.institutional_holdings import (
    seed_etf_filer,
)
from app.services.institutional_holdings import (
    seed_filer as seed_institutional_filer,
)
from app.services.ncen_classifier import classify_filers_via_ncen

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Curated seed lists
# ---------------------------------------------------------------------------


# Top 10 institutional managers by AUM. Covers ~80% of US equity
# institutional ownership across the curated cohort. Each entry is
# (CIK, label) -- the CIK is the SEC's filer ID, padded to 10 digits.
_INSTITUTIONAL_SEEDS: list[tuple[str, str]] = [
    ("0000102909", "Vanguard Group, Inc."),
    ("0001364742", "BlackRock Inc."),
    ("0000093751", "State Street Corporation"),
    ("0000315066", "FMR LLC (Fidelity)"),
    ("0001067983", "Berkshire Hathaway Inc."),
    ("0000354204", "T. Rowe Price Associates"),
    ("0000895421", "Capital World Investors"),
    ("0001029160", "Geode Capital Management LLC"),
    ("0000200217", "Northern Trust Corp."),
    ("0000866787", "Wellington Management Group LLP"),
]

# CIKs from above to also tag as ETFs. Vanguard, BlackRock, and
# State Street are the three primary US ETF issuers; Geode Capital
# Management runs the index-fund engine behind Fidelity's index ETFs
# and is treated as ETF-flavoured for the operator-facing chip.
_ETF_OVERRIDES: list[tuple[str, str]] = [
    ("0000102909", "Vanguard ETF franchise"),
    ("0001364742", "iShares (BlackRock) ETF franchise"),
    ("0000093751", "SPDR (State Street) ETF franchise"),
    ("0001029160", "Geode Capital (Fidelity index-fund engine)"),
]

# Activist hedge funds + founder-family holdcos that file 13D/G
# regularly. Operator can extend via the same seed_filer helper
# at runtime.
_BLOCKHOLDER_SEEDS: list[tuple[str, str]] = [
    ("0000921669", "Icahn Capital LP (Carl Icahn)"),
    ("0001336528", "Pershing Square Capital Management LP (Bill Ackman)"),
    ("0001364099", "Elliott Investment Management"),
    ("0001418814", "ValueAct Holdings LP"),
    ("0001603466", "Engaged Capital LLC"),
    ("0001345471", "Trian Fund Management LP"),
    ("0001540531", "Starboard Value LP"),
]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually write seed rows + run ingesters. Default is dry-run.",
    )
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help=(
            "Write seed rows but don't run the SEC EDGAR fetches. "
            "Useful for staging before the operator triggers via "
            "the existing scheduler."
        ),
    )
    parser.add_argument(
        "--skip-resolver",
        action="store_true",
        help="Skip the CUSIP resolver step (#781). Default runs it.",
    )
    parser.add_argument(
        "--skip-ncen",
        action="store_true",
        help="Skip the N-CEN classifier step (#782). Default runs it.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _seed_all(conn: psycopg.Connection[tuple]) -> None:
    """Idempotent seed-row inserts."""
    print("Seeding institutional_filer_seeds...")
    for cik, label in _INSTITUTIONAL_SEEDS:
        seed_institutional_filer(conn, cik=cik, label=label)
    print(f"  {len(_INSTITUTIONAL_SEEDS)} institutional seeds upserted.")

    print("Seeding etf_filer_cik_seeds...")
    for cik, label in _ETF_OVERRIDES:
        seed_etf_filer(conn, cik=cik, label=label)
    print(f"  {len(_ETF_OVERRIDES)} ETF overrides upserted.")

    print("Seeding blockholder_filer_seeds...")
    seen: set[str] = set()
    for cik, label in _BLOCKHOLDER_SEEDS:
        if cik in seen:
            print(f"  skipping duplicate CIK {cik} in seed list")
            continue
        seed_blockholder_filer(conn, cik=cik, label=label)
        seen.add(cik)
    print(f"  {len(seen)} blockholder seeds upserted.")
    conn.commit()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = _parse_args()

    if not args.apply:
        print("DRY RUN -- pass --apply to write rows + run ingesters.\n")
        print(
            f"Would seed {len(_INSTITUTIONAL_SEEDS)} institutional filers, "
            f"{len(_ETF_OVERRIDES)} ETF overrides, "
            f"{len(set(c for c, _ in _BLOCKHOLDER_SEEDS))} blockholder filers."
        )
        return 0

    with psycopg.connect(settings.database_url) as conn:
        _seed_all(conn)

        if args.skip_ingest:
            print("Skipping SEC EDGAR fetches (--skip-ingest).")
            return 0

        with SecFilingsProvider(user_agent=settings.sec_user_agent) as sec:
            print("\nIngesting 13F-HR holdings...")
            inst_summaries = ingest_all_institutional(conn, sec)
            for s in inst_summaries:
                print(
                    f"  cik={s.filer_cik} "
                    f"seen={s.accessions_seen} ingested={s.accessions_ingested} "
                    f"failed={s.accessions_failed} "
                    f"holdings={s.holdings_inserted} "
                    f"skipped_no_cusip={s.holdings_skipped_no_cusip}"
                )

            print("\nIngesting 13D/G blockholders...")
            block_summaries = ingest_all_blockholders(conn, sec)
            for s in block_summaries:
                print(
                    f"  cik={s.filer_cik} "
                    f"seen={s.accessions_seen} ingested={s.accessions_ingested} "
                    f"failed={s.accessions_failed} "
                    f"rows={s.rows_inserted} "
                    f"skipped_no_cusip={s.rows_skipped_no_cusip}"
                )

            if not args.skip_ncen:
                print("\nClassifying filers via N-CEN...")
                ncen_report = classify_filers_via_ncen(conn, sec)
                print(
                    f"  filers_seen={ncen_report.filers_seen} "
                    f"classified={ncen_report.classifications_written} "
                    f"no_ncen={ncen_report.no_ncen_found} "
                    f"fetch_failures={ncen_report.fetch_failures} "
                    f"parse_failures={ncen_report.parse_failures} "
                    f"crash_failures={ncen_report.crash_failures}"
                )

        if not args.skip_resolver:
            print("\nResolving unresolved CUSIPs...")
            with psycopg.connect(settings.database_url) as resolver_conn:
                resolve_report = resolve_unresolved_cusips(resolver_conn, limit=2000)
                resolver_conn.commit()
            print(
                f"  candidates_seen={resolve_report.candidates_seen} "
                f"promotions={resolve_report.promotions} "
                f"already_resolved={resolve_report.already_resolved} "
                f"unresolvable={resolve_report.tombstoned_unresolvable} "
                f"ambiguous={resolve_report.tombstoned_ambiguous} "
                f"conflict={resolve_report.tombstoned_conflict}"
            )

        # Re-run 13F ingest after CUSIP resolver so unresolved
        # holdings get retried. Operator can clear the ingest log
        # to force re-fetch of accessions that were previously
        # tombstoned-as-partial; this script does not.
        print(
            "\nDone. To pick up newly-resolved CUSIPs on existing "
            "accessions, clear matching rows from "
            "institutional_holdings_ingest_log and re-run."
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
