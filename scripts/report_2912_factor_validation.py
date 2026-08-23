"""Run and print the frozen #2912 factor-construction validation report.

Reproduce from the repository root:

    PYTHONPATH=. uv run python -m scripts.report_2912_factor_validation

The output is a construction-identity diagnostic, never an arm return.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Final

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.services.factor_validation import (
    FactorComparison,
    compare_factor_series,
    construct_s2_momentum_factor,
    load_reference_series,
)

DECLARATION_PATH: Final = Path("docs/proposals/ta/2026-08-23-r6-factor-validation-declaration.md")
DECLARATION_SHA256: Final = "f63d0cf6084cf7158a37f9fa904fbe892b66d8df7fd94e0cd17e0c69a02ad68b"
CORRECTION_PATH: Final = Path("docs/proposals/ta/2026-08-23-r6-factor-validation-correction-1.md")
CORRECTION_SHA256: Final = "3b111dce37729a63b750c2abb61b8bf53bebb68f0e83e253109148c49e5f0f3c"
COMMAND: Final = "PYTHONPATH=. uv run python -m scripts.report_2912_factor_validation"
FRENCH_MOMENTUM_SHA256: Final = "f405ee2d47a5c75ce05025f789733d0599879361e9836a553504240b89159871"
FRENCH_PARSER_VERSION: Final = "kenneth-french-monthly-csv-v2"
AQR_VME_SHA256: Final = "a2351d0323ab60c715a359c55d70a596560af75da335e7ceaa9326b9737daf49"
AQR_PARSER_VERSION: Final = "aqr-vme-monthly-xlsx-v2"


def _comparison_lines(result: FactorComparison) -> list[str]:
    def metric(value: float | None) -> str:
        return "unavailable" if value is None else f"{value:+.6f}"

    failures = "; ".join(result.failures) if result.failures else "none"
    return [
        f"### {result.label}: {'PASS' if result.passed else 'FAIL'}",
        "",
        f"- Window: {result.overlap_start:%Y-%m} through {result.overlap_end:%Y-%m} ({result.overlap_months} months)",
        f"- Pearson correlation: {metric(result.correlation)}",
        f"- OLS `ebull = alpha + beta × reference`: alpha {result.alpha:+.8f}; beta {result.beta:+.6f}",
        f"- Reference lag one month: {metric(result.reference_lag_one_correlation)}",
        f"- Reference lead one month: {metric(result.reference_lead_one_correlation)}",
        f"- Failures: {failures}",
        "",
    ]


def _source_census(conn: psycopg.Connection[Any]) -> list[str]:
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT snapshot_id, source, dataset_key, parser_version, parse_status,
                   response_sha256, octet_length(payload) AS payload_bytes,
                   row_count, missing_count, first_observation, last_observation,
                   parse_error
            FROM reference_data_snapshots
            ORDER BY snapshot_id
            """
        )
        snapshots = cursor.fetchall()
        cursor.execute(
            """
            SELECT count(*) AS facts,
                   count(DISTINCT instrument_id) AS instruments,
                   min(filed_date) AS first_filed,
                   max(filed_date) AS last_filed,
                   count(DISTINCT ingestion_run_id) FILTER (
                       WHERE ingestion_run_id IS NOT NULL
                   ) AS ingestion_runs
            FROM financial_facts_raw
            """
        )
        sec = cursor.fetchone()
        cursor.execute(
            """
            SELECT source, status, count(*) AS runs,
                   sum(rows_upserted) AS rows_upserted,
                   sum(rows_skipped) AS rows_skipped
            FROM data_ingestion_runs
            WHERE source IN ('sec_companyfacts_bulk', 'sec_edgar')
            GROUP BY source, status
            ORDER BY source, status
            """
        )
        sec_runs = cursor.fetchall()
        cursor.execute(
            """
            SELECT count(*) AS observations,
                   count(DISTINCT instrument_id) AS instruments,
                   min(settlement_date) AS first_date,
                   max(settlement_date) AS last_date
            FROM finra_short_interest_observations
            """
        )
        finra_si = cursor.fetchone()
        cursor.execute(
            """
            SELECT count(*) AS observations,
                   count(DISTINCT instrument_id) AS instruments,
                   min(trade_date) AS first_date,
                   max(trade_date) AS last_date
            FROM finra_regsho_daily_observations
            """
        )
        finra_regsho = cursor.fetchone()
    if sec is None or finra_si is None or finra_regsho is None:
        raise RuntimeError("source census aggregate unexpectedly returned no row")

    lines = ["## Ingested source evidence", "", "### New immutable reference snapshots", ""]
    for row in snapshots:
        coverage = (
            f"{row['first_observation']}..{row['last_observation']}; rows={row['row_count']}; "
            f"missing={row['missing_count']}"
            if row["parse_status"] == "accepted"
            else f"error={row['parse_error']}"
        )
        lines.append(
            f"- snapshot {row['snapshot_id']} `{row['source']}/{row['dataset_key']}` "
            f"`{row['parser_version']}`: **{str(row['parse_status']).upper()}**; "
            f"sha256 `{row['response_sha256']}`; {row['payload_bytes']} raw bytes; {coverage}"
        )
    lines.extend(
        [
            "",
            "The rejected AQR v1 snapshot is retained intentionally: live acceptance exposed "
            "blank-string footer rows, v2 classified those rows as blank, and the same source "
            "bytes were accepted under a new parser identity rather than rewriting history.",
            "",
            "### Existing SEC and FINRA paths (validated, not duplicated)",
            "",
            f"- SEC normalized XBRL: {sec['facts']:,} facts across {sec['instruments']:,} instruments, "
            f"filed {sec['first_filed']}..{sec['last_filed']}, with {sec['ingestion_runs']:,} linked ingest runs.",
        ]
    )
    for row in sec_runs:
        lines.append(
            f"- SEC run ledger `{row['source']}/{row['status']}`: {row['runs']:,} runs, "
            f"rows_upserted={row['rows_upserted'] or 0:,}, rows_skipped={row['rows_skipped'] or 0:,}."
        )
    lines.extend(
        [
            f"- FINRA bimonthly short interest: {finra_si['observations']:,} observations across "
            f"{finra_si['instruments']:,} instruments, {finra_si['first_date']}..{finra_si['last_date']}.",
            f"- FINRA RegSHO daily: {finra_regsho['observations']:,} observations across "
            f"{finra_regsho['instruments']:,} instruments, {finra_regsho['first_date']}..{finra_regsho['last_date']}.",
            "- Provenance/refresh contracts: `docs/etl/sources/sec_xbrl_facts.md`, "
            "`docs/etl/sources/finra_short_interest.md`, and "
            "`docs/etl/sources/finra_regsho_daily.md`.",
            "",
        ]
    )
    return lines


def main() -> int:
    measured_declaration_hash = hashlib.sha256(DECLARATION_PATH.read_bytes()).hexdigest()
    if measured_declaration_hash != DECLARATION_SHA256:
        raise RuntimeError(
            f"declaration hash moved: expected {DECLARATION_SHA256}, measured {measured_declaration_hash}"
        )
    measured_correction_hash = hashlib.sha256(CORRECTION_PATH.read_bytes()).hexdigest()
    if measured_correction_hash != CORRECTION_SHA256:
        raise RuntimeError(f"correction hash moved: expected {CORRECTION_SHA256}, measured {measured_correction_hash}")

    with psycopg.connect(settings.database_url) as conn:
        source_lines = _source_census(conn)
        constructed = construct_s2_momentum_factor(conn)
        french = load_reference_series(
            conn,
            source="kenneth_french",
            dataset_key="french_momentum_monthly",
            series_key="Mom",
            response_sha256=FRENCH_MOMENTUM_SHA256,
            parser_version=FRENCH_PARSER_VERSION,
        )
        aqr = load_reference_series(
            conn,
            source="aqr",
            dataset_key="aqr_vme_monthly",
            series_key="MOMLS_VME_US90",
            response_sha256=AQR_VME_SHA256,
            parser_version=AQR_PARSER_VERSION,
        )
    french_result = compare_factor_series(
        label="eBull S-2 diagnostic vs French MOM",
        dependent=constructed.values,
        reference=french.values,
    )
    aqr_result = compare_factor_series(
        label="eBull S-2 diagnostic vs AQR U.S. MOM",
        dependent=constructed.values,
        reference=aqr.values,
    )
    control_result = compare_factor_series(
        label="French MOM vs AQR U.S. MOM parser/alignment control",
        dependent=french.values,
        reference=aqr.values,
        kind="reference_control",
    )
    passed = french_result.passed and aqr_result.passed and control_result.passed
    census = constructed.census
    selection = census.selection

    lines = [
        "# R6 factor-construction validation result (#2912)",
        "",
        f"Verdict: **{'PASS' if passed else 'FAIL — construction remains unusable'}**",
        "",
        f"Declaration SHA-256: `{DECLARATION_SHA256}` (frozen before results at commit `7f71c9cf`).",
        f"Correction-1 SHA-256: `{CORRECTION_SHA256}` (frozen before the corrected result at commit `3fe48d52`).",
        f"Reproduce: `{COMMAND}`",
        "",
        *source_lines,
        "## Full-population construction census",
        "",
        f"- Validated instruments used by the existing selection rule: {census.validated_instruments:,}",
        f"- Survivorship-free vendor rows: {selection.vendor_series_total:,}; admitted: {len(selection.admitted):,}; "
        f"unlinked alive excluded: {selection.unlinked_alive_excluded:,}; linked early/reuse suspects admitted: "
        f"{selection.linked_early_reuse_suspect:,}; exchange test issues excluded: "
        f"{selection.exchange_test_issues_excluded:,}; unharvested excluded: {selection.unharvested_excluded:,}",
        f"- Fail-closed bars read: {census.bars_read:,}",
        f"- Rebalance dates through 2024-08: {census.rebalance_dates:,}; without an eligible name: "
        f"{census.rebalances_without_eligible_names:,}; thin panels: {census.thin_panels:,}",
        f"- Selected member-legs: {census.selected_member_legs:,}; usable endpoints: "
        f"{census.usable_member_legs:,}; rejected endpoints: {census.rejected_member_endpoints:,}",
        f"- Constructed diagnostic months: {census.factor_months:,}",
        "",
        *(_comparison_lines(french_result)),
        *(_comparison_lines(aqr_result)),
        *(_comparison_lines(control_result)),
        "## Boundary",
        "",
        "This PASS/FAIL validates factor sign, timing and parser alignment only. It is not an arm, "
        "does not establish investability, and creates no haircut, cost-adjusted return or "
        "buy-and-hold verdict. Those remain forbidden until a later arm has its own frozen declaration.",
    ]
    print("\n".join(lines))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
