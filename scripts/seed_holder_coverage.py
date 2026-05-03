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
# Top institutional managers by AUM. Each CIK is verified against
# SEC submissions.json — the ``label`` matches the canonical entity
# name. Migration 106 (operator-found 2026-05-03) corrected the
# prior list which had FOUR mis-labelled CIKs:
#
#   * 0000200217 was labelled "Northern Trust Corp." but is
#     actually DODGE & COX
#   * 0000354204 was labelled "T. Rowe Price Associates" but is
#     actually DIMENSIONAL FUND ADVISORS LP
#   * 0000895421 was labelled "Capital World Investors" but is
#     actually MORGAN STANLEY
#   * 0000866787 was labelled "Wellington Management Group LLP"
#     but is actually AUTOZONE INC (not a 13F filer at all —
#     hallucinated row, dropped by migration 106)
#
# Plus the Soros/Geode disambig from migration 104 (Batch 2 of
# #788).
_INSTITUTIONAL_SEEDS: list[tuple[str, str]] = [
    ("0000102909", "Vanguard Group, Inc."),
    ("0001364742", "BlackRock Inc."),
    ("0000093751", "State Street Corporation"),
    ("0000315066", "FMR LLC (Fidelity)"),
    ("0001067983", "Berkshire Hathaway Inc."),
    # CIK-verified relabels (migration 106).
    ("0000200217", "Dodge & Cox"),
    ("0000354204", "Dimensional Fund Advisors LP"),
    ("0000895421", "Morgan Stanley"),
    # Soros / Geode disambig (#790 P2 — migration 104).
    ("0001029160", "Soros Fund Management LLC"),
    ("0001214717", "Geode Capital Management LLC"),
    # Intended top managers added by migration 106 with correct
    # CIKs. The prior list had the LABELS for these but the wrong
    # CIKs.
    ("0000073124", "Northern Trust Corp."),
    ("0000080255", "T. Rowe Price Associates Inc."),
    ("0001422849", "Capital World Investors"),
    ("0000902219", "Wellington Management Group LLP"),
]

# CIKs from above to also tag as ETFs. Two issuers are clearly
# pure-ETF operationally: Vanguard's CIK files most of its ETFs
# under one umbrella, BlackRock's iShares CIK is the dedicated
# ETF-issuer entity, and Geode runs Fidelity's passive-index
# franchise so its 13F holdings track the ETF basket.
#
# State Street (CIK 0000093751) is deliberately NOT tagged ETF
# even though it's the SPDR sponsor — its 13F-HR aggregates the
# whole institutional asset-management business, and tagging it
# ETF would route every State-Street-held position into the ETF
# bucket rather than the Institutions bucket on the ownership
# card. Operators who want the SPDR-only slice can refine via
# fund-level CIKs (each SPDR series has its own CIK) in a
# follow-up curation pass.
_ETF_OVERRIDES: list[tuple[str, str]] = [
    ("0000102909", "Vanguard ETF franchise"),
    ("0001364742", "iShares (BlackRock) ETF franchise"),
    # Soros / Geode disambig (#790 P2 — migration 104). The real
    # Geode Capital Management LLC is CIK 0001214717. Soros (CIK
    # 0001029160) is intentionally NOT in the ETF override list.
    ("0001214717", "Geode Capital (Fidelity index-fund engine)"),
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
    # GameStop / Bed Bath activist — verified via SEC EDGAR
    # full-text search 13D filings on GME / BBBY.
    ("0001822844", "RC Ventures LLC (Ryan Cohen)"),
]


# Curated (instrument_symbol, CUSIP) seed list. Without these in
# ``external_identifiers``, every 13F-HR holding for the named
# tickers stays unresolved and the institutions/ETFs wedge stays
# zero on the ownership card. The CUSIP resolver (#781) is the
# long-tail path; this curation gives the top-25 mega-caps an
# instant working state without waiting for the resolver to
# fuzzy-match through tens of thousands of holdings.
#
# CUSIPs sourced from SEC EDGAR primary_doc filings; revised when
# an issuer renames or undergoes a corporate action. Operator
# can extend at runtime via the ``external_identifiers`` upsert
# helper.
_CURATED_CUSIPS: list[tuple[str, str]] = [
    ("AAPL", "037833100"),
    ("MSFT", "594918104"),
    ("GOOGL", "02079K305"),
    ("GOOG", "02079K107"),
    ("META", "30303M102"),
    ("AMZN", "023135106"),
    ("NVDA", "67066G104"),
    ("TSLA", "88160R101"),
    ("BRK.B", "084670702"),
    ("GME", "36467W109"),
    ("BBY", "086516101"),
    ("PYPL", "70450Y103"),
    ("NFLX", "64110L106"),
    ("AMD", "007903107"),
    ("INTC", "458140100"),
    ("ORCL", "68389X105"),
    ("CRM", "79466L302"),
    ("ADBE", "00724F101"),
    ("WMT", "931142103"),
    ("JPM", "46625H100"),
    ("V", "92826C839"),
    ("MA", "57636Q104"),
    ("DIS", "254687106"),
    ("KO", "191216100"),
    ("PEP", "713448108"),
    ("COST", "22160K105"),
    ("HD", "437076102"),
    ("PG", "742718109"),
    ("BAC", "060505104"),
    ("CVX", "166764100"),
    ("XOM", "30231G102"),
    ("T", "00206R102"),
    ("VZ", "92343V104"),
    ("MRK", "58933Y105"),
    ("PFE", "717081103"),
    ("JNJ", "478160104"),
    ("UNH", "91324P102"),
    ("ABBV", "00287Y109"),
    ("LLY", "532457108"),
    ("AVGO", "11135F101"),
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

    # Stale-row reconciliation for the Soros/Geode disambig (#790 P2,
    # migration 104). The script is upsert-only by design, but a DB
    # that ran the pre-migration version still carries
    # ``cik='0001029160'`` (Soros) tagged as an ETF override —
    # re-running this script after the source change wouldn't
    # converge without an explicit DELETE. Codex pre-push review
    # caught this. Listed inline rather than in a generic "removed
    # CIKs" constant because the migration is the canonical record;
    # this DELETE is a script-level convergence guarantee, not a
    # source-of-truth list.
    _STALE_ETF_CIKS: tuple[str, ...] = ("0001029160",)
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM etf_filer_cik_seeds WHERE cik = ANY(%s)",
            (list(_STALE_ETF_CIKS),),
        )
        if cur.rowcount and cur.rowcount > 0:
            print(f"  removed {cur.rowcount} stale ETF override(s): {list(_STALE_ETF_CIKS)} (Soros mis-seed cleanup)")

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

    print("Seeding curated CUSIPs into external_identifiers...")
    cusip_inserts = 0
    cusip_already_correct = 0
    cusip_missing_instrument = 0
    cusip_corrected = 0
    for symbol, cusip in _CURATED_CUSIPS:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT instrument_id FROM instruments WHERE symbol = %s LIMIT 1",
                (symbol,),
            )
            row = cur.fetchone()
        if row is None:
            cusip_missing_instrument += 1
            continue
        instrument_id = int(row[0])
        # Probe the existing row first so a corrected re-run can
        # overwrite a previously-stored wrong instrument_id with a
        # diagnostic line. Bot review of an earlier draft caught
        # the silent ON-CONFLICT-DO-NOTHING path that would leave
        # a wrong mapping in place across script edits.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT instrument_id FROM external_identifiers
                WHERE provider = 'sec'
                  AND identifier_type = 'cusip'
                  AND identifier_value = %s
                """,
                (cusip,),
            )
            existing = cur.fetchone()
        if existing is not None:
            if int(existing[0]) != instrument_id:
                print(
                    f"  CORRECT {symbol} CUSIP {cusip}: "
                    f"instrument_id {existing[0]} -> {instrument_id} "
                    f"(curated re-run overwriting stale mapping)"
                )
                conn.execute(
                    """
                    UPDATE external_identifiers
                    SET instrument_id = %s, is_primary = TRUE
                    WHERE provider = 'sec'
                      AND identifier_type = 'cusip'
                      AND identifier_value = %s
                    """,
                    (instrument_id, cusip),
                )
                cusip_corrected += 1
            else:
                cusip_already_correct += 1
            continue
        # is_primary=TRUE because curated mappings are operator-
        # verified. Resolver-derived CUSIPs (#781) are
        # is_primary=FALSE so the curated mapping wins on conflict.
        # ON CONFLICT DO NOTHING remains as a race-guard for
        # concurrent re-runs; the probe above is the primary dedup.
        conn.execute(
            """
            INSERT INTO external_identifiers (
                instrument_id, provider, identifier_type, identifier_value, is_primary
            ) VALUES (%s, 'sec', 'cusip', %s, TRUE)
            ON CONFLICT (provider, identifier_type, identifier_value) DO NOTHING
            """,
            (instrument_id, cusip),
        )
        cusip_inserts += 1
    print(
        f"  {cusip_inserts} CUSIPs inserted; "
        f"{cusip_corrected} stale mappings corrected; "
        f"{cusip_already_correct} already correct (no-op); "
        f"{cusip_missing_instrument} skipped (symbol not in instruments)."
    )
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
