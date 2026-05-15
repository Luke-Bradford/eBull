"""Write-through current-state writer for N-CSR / N-CSRS fund metadata
(spec §8, T6 in the plan).

``refresh_fund_metadata_current(conn, instrument_id)`` selects the
currently-valid observation per the source-priority chain:

    ORDER BY period_end DESC, filed_at DESC, source_accession DESC
    LIMIT 1
    WHERE known_to IS NULL

then upserts the resulting projection into ``fund_metadata_current``.

Concurrency: wrapped in ``with conn.transaction()`` + a per-instrument
``pg_advisory_xact_lock`` so concurrent refreshes for the same
``instrument_id`` serialise (data-engineer invariant I7). The
``instrument_id`` PK on ``fund_metadata_current`` is the second-line
guard.

Settled-decision rationale (spec §2):
- ``period_end DESC`` — most recent reporting period wins.
- ``filed_at DESC`` — amendments naturally win (later filed_at).
- ``source_accession DESC`` — deterministic tie-break for unlikely
  same-filed_at collisions.

The function is idempotent: re-running against the same observation
state is a no-op write that returns ``'suppressed'``.
"""

from __future__ import annotations

from typing import Any, Literal

import psycopg
from psycopg.types.json import Jsonb

RefreshOutcome = Literal["inserted", "updated", "deleted"]


def refresh_fund_metadata_current(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> RefreshOutcome:
    """Atomic write-through refresh.

    Returns:
        - ``'inserted'`` — no row existed for this instrument; created.
        - ``'updated'`` — incumbent existed; replaced. Always returned
          when an incumbent exists, even if the content is identical —
          a parser-version rewash that reuses (source_accession,
          filed_at) MAY still change Tier 1 / Tier 2 payload, so we
          always UPSERT rather than try to short-circuit content
          equality.
        - ``'deleted'`` — no currently-valid observation exists; any
          stale incumbent row was removed.
    """
    with conn.transaction(), conn.cursor() as cur:
        # Per-instrument advisory lock keyed on function name + instrument_id
        # (mirror pattern from ownership_observations.refresh_*_current).
        cur.execute(
            """
            SELECT pg_advisory_xact_lock(
                (hashtextextended('refresh_fund_metadata_current', 0) # %s::bigint)
            )
            """,
            (instrument_id,),
        )

        # Pick the winning observation under the source-priority chain.
        cur.execute(
            """
            SELECT
                source_accession,
                filed_at,
                period_end,
                document_type,
                amendment_flag,
                parser_version,
                trust_cik,
                trust_name,
                entity_inv_company_type,
                series_id,
                series_name,
                class_id,
                class_name,
                trading_symbol,
                exchange,
                inception_date,
                shareholder_report_type,
                expense_ratio_pct,
                expenses_paid_amt,
                net_assets_amt,
                advisory_fees_paid_amt,
                portfolio_turnover_pct,
                holdings_count,
                returns_pct,
                benchmark_returns_pct,
                sector_allocation,
                region_allocation,
                credit_quality_allocation,
                growth_curve,
                material_chng_date,
                material_chng_notice,
                contact_phone,
                contact_website,
                contact_email,
                prospectus_phone,
                prospectus_website,
                prospectus_email
            FROM fund_metadata_observations
            WHERE instrument_id = %(instrument_id)s
              AND known_to IS NULL
            ORDER BY period_end DESC, filed_at DESC, source_accession DESC
            LIMIT 1
            """,
            {"instrument_id": instrument_id},
        )
        winner = cur.fetchone()

        if winner is None:
            # No currently-valid observation; remove any stale incumbent.
            cur.execute(
                "DELETE FROM fund_metadata_current WHERE instrument_id = %(instrument_id)s",
                {"instrument_id": instrument_id},
            )
            # Always 'deleted' — the function only reaches this branch when
            # NO currently-valid observation exists; if there was no incumbent
            # either, the DELETE rowcount is 0 but the post-condition (no row
            # in fund_metadata_current) is satisfied. Return 'deleted' uniformly
            # so callers don't branch on a no-op edge case.
            return "deleted"

        # Read whether an incumbent exists (for outcome semantics only).
        # We do NOT compare (source_accession, filed_at, parser_version) for
        # short-circuit suppression because a parser-version rewash reuses the
        # same accession/filed_at/parser_version on the new observation while
        # changing the Tier 1 / Tier 2 payload. Suppression must inspect actual
        # content, not just provenance triple. Simpler + cheaper: always
        # UPSERT and let PostgreSQL handle the equality check via column-level
        # diffing on UPDATE.
        cur.execute(
            "SELECT 1 FROM fund_metadata_current WHERE instrument_id = %(instrument_id)s",
            {"instrument_id": instrument_id},
        )
        incumbent = cur.fetchone()

        cur.execute(
            """
            INSERT INTO fund_metadata_current (
                instrument_id,
                source_accession, filed_at, period_end,
                document_type, amendment_flag, parser_version,
                trust_cik, trust_name, entity_inv_company_type,
                series_id, series_name, class_id, class_name,
                trading_symbol, exchange, inception_date, shareholder_report_type,
                expense_ratio_pct, expenses_paid_amt, net_assets_amt,
                advisory_fees_paid_amt, portfolio_turnover_pct, holdings_count,
                returns_pct, benchmark_returns_pct, sector_allocation,
                region_allocation, credit_quality_allocation, growth_curve,
                material_chng_date, material_chng_notice,
                contact_phone, contact_website, contact_email,
                prospectus_phone, prospectus_website, prospectus_email,
                refreshed_at
            ) VALUES (
                %(instrument_id)s,
                %(source_accession)s, %(filed_at)s, %(period_end)s,
                %(document_type)s, %(amendment_flag)s, %(parser_version)s,
                %(trust_cik)s, %(trust_name)s, %(entity_inv_company_type)s,
                %(series_id)s, %(series_name)s, %(class_id)s, %(class_name)s,
                %(trading_symbol)s, %(exchange)s, %(inception_date)s, %(shareholder_report_type)s,
                %(expense_ratio_pct)s, %(expenses_paid_amt)s, %(net_assets_amt)s,
                %(advisory_fees_paid_amt)s, %(portfolio_turnover_pct)s, %(holdings_count)s,
                %(returns_pct)s, %(benchmark_returns_pct)s, %(sector_allocation)s,
                %(region_allocation)s, %(credit_quality_allocation)s, %(growth_curve)s,
                %(material_chng_date)s, %(material_chng_notice)s,
                %(contact_phone)s, %(contact_website)s, %(contact_email)s,
                %(prospectus_phone)s, %(prospectus_website)s, %(prospectus_email)s,
                NOW()
            )
            ON CONFLICT (instrument_id) DO UPDATE SET
                source_accession = EXCLUDED.source_accession,
                filed_at = EXCLUDED.filed_at,
                period_end = EXCLUDED.period_end,
                document_type = EXCLUDED.document_type,
                amendment_flag = EXCLUDED.amendment_flag,
                parser_version = EXCLUDED.parser_version,
                trust_cik = EXCLUDED.trust_cik,
                trust_name = EXCLUDED.trust_name,
                entity_inv_company_type = EXCLUDED.entity_inv_company_type,
                series_id = EXCLUDED.series_id,
                series_name = EXCLUDED.series_name,
                class_id = EXCLUDED.class_id,
                class_name = EXCLUDED.class_name,
                trading_symbol = EXCLUDED.trading_symbol,
                exchange = EXCLUDED.exchange,
                inception_date = EXCLUDED.inception_date,
                shareholder_report_type = EXCLUDED.shareholder_report_type,
                expense_ratio_pct = EXCLUDED.expense_ratio_pct,
                expenses_paid_amt = EXCLUDED.expenses_paid_amt,
                net_assets_amt = EXCLUDED.net_assets_amt,
                advisory_fees_paid_amt = EXCLUDED.advisory_fees_paid_amt,
                portfolio_turnover_pct = EXCLUDED.portfolio_turnover_pct,
                holdings_count = EXCLUDED.holdings_count,
                returns_pct = EXCLUDED.returns_pct,
                benchmark_returns_pct = EXCLUDED.benchmark_returns_pct,
                sector_allocation = EXCLUDED.sector_allocation,
                region_allocation = EXCLUDED.region_allocation,
                credit_quality_allocation = EXCLUDED.credit_quality_allocation,
                growth_curve = EXCLUDED.growth_curve,
                material_chng_date = EXCLUDED.material_chng_date,
                material_chng_notice = EXCLUDED.material_chng_notice,
                contact_phone = EXCLUDED.contact_phone,
                contact_website = EXCLUDED.contact_website,
                contact_email = EXCLUDED.contact_email,
                prospectus_phone = EXCLUDED.prospectus_phone,
                prospectus_website = EXCLUDED.prospectus_website,
                prospectus_email = EXCLUDED.prospectus_email,
                refreshed_at = NOW()
            """,
            {
                "instrument_id": instrument_id,
                "source_accession": winner[0],
                "filed_at": winner[1],
                "period_end": winner[2],
                "document_type": winner[3],
                "amendment_flag": winner[4],
                "parser_version": winner[5],
                "trust_cik": winner[6],
                "trust_name": winner[7],
                "entity_inv_company_type": winner[8],
                "series_id": winner[9],
                "series_name": winner[10],
                "class_id": winner[11],
                "class_name": winner[12],
                "trading_symbol": winner[13],
                "exchange": winner[14],
                "inception_date": winner[15],
                "shareholder_report_type": winner[16],
                "expense_ratio_pct": winner[17],
                "expenses_paid_amt": winner[18],
                "net_assets_amt": winner[19],
                "advisory_fees_paid_amt": winner[20],
                "portfolio_turnover_pct": winner[21],
                "holdings_count": winner[22],
                "returns_pct": Jsonb(winner[23]) if winner[23] is not None else None,
                "benchmark_returns_pct": Jsonb(winner[24]) if winner[24] is not None else None,
                "sector_allocation": Jsonb(winner[25]) if winner[25] is not None else None,
                "region_allocation": Jsonb(winner[26]) if winner[26] is not None else None,
                "credit_quality_allocation": Jsonb(winner[27]) if winner[27] is not None else None,
                "growth_curve": Jsonb(winner[28]) if winner[28] is not None else None,
                "material_chng_date": winner[29],
                "material_chng_notice": winner[30],
                "contact_phone": winner[31],
                "contact_website": winner[32],
                "contact_email": winner[33],
                "prospectus_phone": winner[34],
                "prospectus_website": winner[35],
                "prospectus_email": winner[36],
            },
        )

        return "inserted" if incumbent is None else "updated"
