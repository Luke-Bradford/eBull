"""Integration tests for the ownership rollup service (#789).

Tests run against the real ``ebull_test`` DB so the canonical
union SQL exercises actual joins / DISTINCT ON / NOT EXISTS
semantics. Each scenario seeds the source tables (Form 4, Form 3,
13D/G, 13F, DEF 14A) and asserts dedup priority, residual math,
coverage banner, and snapshot isolation.

Naming convention: instrument_id 789_xxx is reserved for this
suite to avoid collisions with #769 (DEF 14A drift) at 769_xxx.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import psycopg
import psycopg.rows
import pytest

from app.services import ownership_rollup
from app.services.ownership_observations import (
    record_blockholder_observation,
    record_def14a_observation,
    record_insider_observation,
    record_institution_observation,
    record_treasury_observation,
    refresh_blockholders_current,
    refresh_def14a_current,
    refresh_insiders_current,
    refresh_institutions_current,
    refresh_treasury_current,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _seed_outstanding(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    shares: str,
    period_end: date = date(2026, 3, 31),
    treasury: str | None = None,
) -> None:
    """Seed shares_outstanding (and optionally treasury_shares) via
    ``financial_periods`` + ``financial_facts_raw`` so the
    ``instrument_share_count_latest`` view returns the row."""
    conn.execute(
        """
        INSERT INTO financial_periods (
            instrument_id, period_end_date, period_type, fiscal_year,
            fiscal_quarter, source, source_ref, reported_currency,
            is_restated, is_derived, normalization_status,
            treasury_shares, filed_date, superseded_at
        ) VALUES (%s, %s, 'Q4', %s, 4, 'sec_xbrl', %s, 'USD',
                  FALSE, FALSE, 'normalized',
                  %s, %s, NULL)
        ON CONFLICT DO NOTHING
        """,
        (
            instrument_id,
            period_end,
            period_end.year,
            f"OUTSTANDING-{instrument_id}-{period_end}",
            Decimal(treasury) if treasury is not None else None,
            datetime(period_end.year, period_end.month, 1, tzinfo=UTC),
        ),
    )
    # Also seed financial_facts_raw so the share_count_history view
    # returns the row (the view is what feeds
    # instrument_share_count_latest).
    conn.execute(
        """
        INSERT INTO financial_facts_raw (
            instrument_id, taxonomy, concept, unit, period_end, val,
            form_type, filed_date, accession_number,
            fiscal_year, fiscal_period
        ) VALUES (%s, 'dei', 'EntityCommonStockSharesOutstanding',
                  'shares', %s, %s, '10-Q', %s, %s, %s, 'Q4')
        ON CONFLICT DO NOTHING
        """,
        (
            instrument_id,
            period_end,
            Decimal(shares),
            period_end,
            f"OUTSTANDING-{instrument_id}-{period_end}",
            period_end.year,
        ),
    )
    # Mirror treasury_shares to ownership_treasury_current so the
    # post-#905 read path picks it up. The legacy financial_periods
    # write above is kept for any other code path that still reads
    # treasury from there.
    if treasury is not None:
        record_treasury_observation(
            conn,
            instrument_id=instrument_id,
            source="xbrl_dei",
            source_document_id=f"OUTSTANDING-{instrument_id}-{period_end}",
            source_accession=f"OUTSTANDING-{instrument_id}-{period_end}",
            source_field="treasury_shares",
            source_url=None,
            filed_at=datetime(period_end.year, period_end.month, 1, tzinfo=UTC),
            period_start=None,
            period_end=period_end,
            ingest_run_id=uuid4(),
            treasury_shares=Decimal(treasury),
        )
        refresh_treasury_current(conn, instrument_id=instrument_id)


def _seed_form4(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    filer_cik: str | None,
    filer_name: str,
    txn_date: date,
    post_transaction_shares: str,
    is_derivative: bool = False,
    txn_row_num: int = 1,
) -> None:
    conn.execute(
        """
        INSERT INTO insider_filings (
            accession_number, instrument_id, document_type, issuer_cik
        ) VALUES (%s, %s, '4', '0000000789')
        ON CONFLICT (accession_number) DO NOTHING
        """,
        (accession, instrument_id),
    )
    conn.execute(
        """
        INSERT INTO insider_transactions (
            accession_number, txn_row_num, instrument_id, filer_cik, filer_name,
            txn_date, txn_code, shares, post_transaction_shares, is_derivative
        ) VALUES (%s, %s, %s, %s, %s, %s, 'P', 100, %s, %s)
        """,
        (
            accession,
            txn_row_num,
            instrument_id,
            filer_cik,
            filer_name,
            txn_date,
            Decimal(post_transaction_shares),
            is_derivative,
        ),
    )
    # Mirror to ownership_insiders_observations + refresh _current.
    # Matches the production write-through pattern from
    # ``app/services/insider_transactions.py`` (#888); #905 cut the
    # rollup read path over to ``ownership_insiders_current`` so
    # legacy-only seeds would surface as zero rows.
    record_insider_observation(
        conn,
        instrument_id=instrument_id,
        holder_cik=filer_cik,
        holder_name=filer_name,
        ownership_nature="direct",
        source="form4",
        source_document_id=f"{accession}#{txn_row_num}",
        source_accession=accession,
        source_field="post_transaction_shares",
        source_url=None,
        filed_at=datetime.combine(txn_date, datetime.min.time(), tzinfo=UTC),
        period_start=None,
        period_end=txn_date,
        ingest_run_id=uuid4(),
        shares=Decimal(post_transaction_shares),
    )
    refresh_insiders_current(conn, instrument_id=instrument_id)


def _seed_form3(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    filer_cik: str | None,
    filer_name: str,
    shares: str,
    as_of: date,
    row_num: int = 1,
) -> None:
    conn.execute(
        """
        INSERT INTO insider_initial_holdings (
            accession_number, row_num, instrument_id, filer_cik, filer_name,
            security_title, is_derivative, direct_indirect, shares, as_of_date
        ) VALUES (%s, %s, %s, %s, %s, 'Common Stock', FALSE, 'D', %s, %s)
        """,
        (accession, row_num, instrument_id, filer_cik, filer_name, Decimal(shares), as_of),
    )
    record_insider_observation(
        conn,
        instrument_id=instrument_id,
        holder_cik=filer_cik,
        holder_name=filer_name,
        ownership_nature="direct",
        source="form3",
        source_document_id=f"{accession}#{row_num}",
        source_accession=accession,
        source_field="shares",
        source_url=None,
        filed_at=datetime.combine(as_of, datetime.min.time(), tzinfo=UTC),
        period_start=None,
        period_end=as_of,
        ingest_run_id=uuid4(),
        shares=Decimal(shares),
    )
    refresh_insiders_current(conn, instrument_id=instrument_id)


def _seed_block(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    filer_cik: str,
    filer_name: str,
    submission_type: str,
    aggregate_shares: str,
    filed_at: datetime,
    reporter_cik: str | None = None,
    reporter_name: str | None = None,
) -> None:
    """Seed both ``blockholder_filers`` and ``blockholder_filings``."""
    conn.execute(
        """
        INSERT INTO blockholder_filers (cik, name)
        VALUES (%s, %s)
        ON CONFLICT (cik) DO NOTHING
        """,
        (filer_cik, filer_name),
    )
    status = "active" if submission_type.startswith("SCHEDULE 13D") else "passive"
    conn.execute(
        """
        INSERT INTO blockholder_filings (
            filer_id, accession_number, submission_type, status,
            instrument_id, issuer_cik, issuer_cusip,
            reporter_cik, reporter_no_cik, reporter_name,
            aggregate_amount_owned, filed_at
        )
        SELECT filer_id, %s, %s, %s, %s, '0000000789', '999999999',
               %s, %s, %s, %s, %s
        FROM blockholder_filers WHERE cik = %s
        """,
        (
            accession,
            submission_type,
            status,
            instrument_id,
            reporter_cik,
            reporter_cik is None,
            reporter_name or filer_name,
            Decimal(aggregate_shares),
            filed_at,
            filer_cik,
        ),
    )
    source_kind = "13d" if submission_type.startswith("SCHEDULE 13D") else "13g"
    record_blockholder_observation(
        conn,
        instrument_id=instrument_id,
        reporter_cik=filer_cik,
        reporter_name=filer_name,
        ownership_nature="beneficial",
        submission_type=submission_type,
        status_flag=status,
        source=source_kind,
        source_document_id=accession,
        source_accession=accession,
        source_field="aggregate_amount_owned",
        source_url=None,
        filed_at=filed_at,
        period_start=None,
        period_end=filed_at.date(),
        ingest_run_id=uuid4(),
        aggregate_amount_owned=Decimal(aggregate_shares),
        percent_of_class=None,
    )
    refresh_blockholders_current(conn, instrument_id=instrument_id)


def _seed_inst_holding(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    filer_cik: str,
    filer_name: str,
    filer_type: str,
    period_of_report: date,
    shares: str,
    is_put_call: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO institutional_filers (cik, name, filer_type)
        VALUES (%s, %s, %s)
        ON CONFLICT (cik) DO UPDATE SET filer_type = EXCLUDED.filer_type
        """,
        (filer_cik, filer_name, filer_type),
    )
    filed_at = datetime(period_of_report.year, period_of_report.month, 1, tzinfo=UTC)
    conn.execute(
        """
        INSERT INTO institutional_holdings (
            filer_id, instrument_id, accession_number, period_of_report,
            shares, voting_authority, is_put_call, filed_at
        )
        SELECT filer_id, %s, %s, %s, %s, 'SOLE', %s, %s
        FROM institutional_filers WHERE cik = %s
        """,
        (
            instrument_id,
            accession,
            period_of_report,
            Decimal(shares),
            is_put_call,
            filed_at,
            filer_cik,
        ),
    )
    exposure_kind = "EQUITY" if is_put_call is None else ("PUT" if is_put_call == "PUT" else "CALL")
    record_institution_observation(
        conn,
        instrument_id=instrument_id,
        filer_cik=filer_cik,
        filer_name=filer_name,
        filer_type=filer_type,
        ownership_nature="economic",
        source="13f",
        source_document_id=f"{accession}#{filer_cik}#{exposure_kind}",
        source_accession=accession,
        source_field="shares",
        source_url=None,
        filed_at=filed_at,
        period_start=None,
        period_end=period_of_report,
        ingest_run_id=uuid4(),
        shares=Decimal(shares),
        market_value_usd=None,
        voting_authority="SOLE",
        exposure_kind=exposure_kind,
    )
    refresh_institutions_current(conn, instrument_id=instrument_id)


def _seed_def14a(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    holder_name: str,
    shares: str,
    as_of: date = date(2026, 3, 1),
) -> None:
    conn.execute(
        """
        INSERT INTO def14a_beneficial_holdings (
            instrument_id, accession_number, issuer_cik,
            holder_name, holder_role, shares, percent_of_class, as_of_date
        ) VALUES (%s, %s, '0000000789', %s, 'officer', %s, '5.5', %s)
        """,
        (instrument_id, accession, holder_name, Decimal(shares), as_of),
    )
    record_def14a_observation(
        conn,
        instrument_id=instrument_id,
        holder_name=holder_name,
        holder_role="officer",
        ownership_nature="beneficial",
        source="def14a",
        source_document_id=f"{accession}#{holder_name}",
        source_accession=accession,
        source_field="shares",
        source_url=None,
        filed_at=datetime.combine(as_of, datetime.min.time(), tzinfo=UTC),
        period_start=None,
        period_end=as_of,
        ingest_run_id=uuid4(),
        shares=Decimal(shares),
        percent_of_class=Decimal("5.5"),
    )
    refresh_def14a_current(conn, instrument_id=instrument_id)


# ---------------------------------------------------------------------------
# Dedup priority
# ---------------------------------------------------------------------------


class TestDedupPriority:
    """``form4 > form3 > 13d/g > def14a > 13f`` per CIK identity."""

    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=789_001, symbol="GME")
        _seed_outstanding(conn, instrument_id=789_001, shares="448375157")
        conn.commit()
        return conn

    def test_form4_and_13d_both_render_for_same_cik(self, _setup: psycopg.Connection[tuple]) -> None:
        """Cohen-on-GME shape: Form 4 (direct) + 13D/A (beneficial)
        reporting the SAME CIK. Per #837 / #788 P0b, BOTH render —
        Form 4 in insiders (38M direct), 13D/A in blockholders
        (beneficial via family trusts / control entities). Form 4 and
        13D/G describe different facts (Rule 13d-3 beneficial vs
        physical record-name) and must not be deduped against each
        other. The full two-axis ``source × ownership_nature`` model
        lands in Phase 1 (#840); this test pins the immediate fix."""
        conn = _setup
        cik = "0001767470"
        _seed_form4(
            conn,
            accession="F4-RC-2026-001",
            instrument_id=789_001,
            filer_cik=cik,
            filer_name="Cohen Ryan",
            txn_date=date(2026, 1, 21),
            post_transaction_shares="38347842",
        )
        _seed_block(
            conn,
            accession="13D-RC-2025-001",
            instrument_id=789_001,
            filer_cik=cik,
            filer_name="Cohen Ryan",
            submission_type="SCHEDULE 13D/A",
            aggregate_shares="36847842",
            filed_at=datetime(2025, 1, 29, tzinfo=UTC),
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="GME", instrument_id=789_001)

        insider_slices = [s for s in rollup.slices if s.category == "insiders"]
        assert len(insider_slices) == 1
        cohen_insider = insider_slices[0].holders[0]
        assert cohen_insider.filer_cik == cik
        assert cohen_insider.shares == Decimal("38347842")
        assert cohen_insider.winning_source == "form4"
        # 13D no longer drops as a Form 4 ``dropped_source``.
        assert all(d.source != "13d" for d in cohen_insider.dropped_sources)

        # Blockholders slice now carries the 13D/A independently.
        block_slices = [s for s in rollup.slices if s.category == "blockholders"]
        assert len(block_slices) == 1
        cohen_block = block_slices[0].holders[0]
        assert cohen_block.filer_cik == cik
        assert cohen_block.shares == Decimal("36847842")
        assert cohen_block.winning_source == "13d"

    def test_13g_and_13f_both_render_for_same_cik(self, _setup: psycopg.Connection[tuple]) -> None:
        """A large institution that crossed 5% files 13G AND a 13F.
        Per #837, both surface — 13G in blockholders (beneficial
        threshold cross), 13F in institutions/ETFs (quarterly economic
        position). The full two-axis dedup lands in Phase 1; for now,
        13D/G never competes against 13F."""
        conn = _setup
        cik = "0000102909"
        _seed_block(
            conn,
            accession="13G-VG-2025-001",
            instrument_id=789_001,
            filer_cik=cik,
            filer_name="VANGUARD GROUP INC",
            submission_type="SCHEDULE 13G/A",
            aggregate_shares="22000000",
            filed_at=datetime(2025, 12, 31, tzinfo=UTC),
        )
        _seed_inst_holding(
            conn,
            accession="13F-VG-2025-Q4",
            instrument_id=789_001,
            filer_cik=cik,
            filer_name="VANGUARD GROUP INC",
            filer_type="ETF",
            period_of_report=date(2025, 12, 31),
            shares="21800000",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="GME", instrument_id=789_001)

        block_slices = [s for s in rollup.slices if s.category == "blockholders"]
        assert len(block_slices) == 1
        vg_block = block_slices[0].holders[0]
        assert vg_block.filer_cik == cik
        assert vg_block.winning_source == "13g"
        assert vg_block.shares == Decimal("22000000")

        # 13F ETF row now also surfaces in its own slice instead of
        # being dropped under the 13G winner.
        etf_slices = [s for s in rollup.slices if s.category == "etfs"]
        assert len(etf_slices) == 1
        vg_etf = etf_slices[0].holders[0]
        assert vg_etf.filer_cik == cik
        assert vg_etf.winning_source == "13f"
        assert vg_etf.shares == Decimal("21800000")

    def test_form3_baseline_wins_when_no_form4(self, _setup: psycopg.Connection[tuple]) -> None:
        """Officer with Form 3 baseline + no Form 4 — Form 3 supplies
        the holding row."""
        conn = _setup
        _seed_form3(
            conn,
            accession="F3-OF-2024-001",
            instrument_id=789_001,
            filer_cik="0001234001",
            filer_name="Director Alpha",
            shares="50000",
            as_of=date(2024, 1, 15),
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="GME", instrument_id=789_001)

        insiders = [s for s in rollup.slices if s.category == "insiders"][0]
        alpha = insiders.holders[0]
        assert alpha.filer_cik == "0001234001"
        assert alpha.winning_source == "form3"
        assert alpha.shares == Decimal("50000")

    def test_form3_suppressed_when_form4_exists_for_same_cik(self, _setup: psycopg.Connection[tuple]) -> None:
        """Officer with both Form 3 baseline + a Form 4 — Form 4 wins,
        Form 3 row is filtered out at the SQL union (NOT EXISTS)
        rather than landing as a dropped_source."""
        conn = _setup
        cik = "0001234002"
        _seed_form3(
            conn,
            accession="F3-OF-2024-002",
            instrument_id=789_001,
            filer_cik=cik,
            filer_name="Director Beta",
            shares="50000",
            as_of=date(2024, 1, 15),
        )
        _seed_form4(
            conn,
            accession="F4-OF-2026-002",
            instrument_id=789_001,
            filer_cik=cik,
            filer_name="Director Beta",
            txn_date=date(2026, 4, 1),
            post_transaction_shares="125000",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="GME", instrument_id=789_001)

        insiders = [s for s in rollup.slices if s.category == "insiders"][0]
        assert len(insiders.holders) == 1
        beta = insiders.holders[0]
        assert beta.winning_source == "form4"
        assert beta.shares == Decimal("125000")
        assert len(beta.dropped_sources) == 0  # Form 3 filtered pre-dedup

    def test_null_cik_distinct_names_do_not_collapse(self, _setup: psycopg.Connection[tuple]) -> None:
        """Codex v3 review caught the prior bug: two NULL-CIK Form 4
        rows with distinct names (legacy backfill data) must NOT
        collapse into a single bucket. Identity uses the normalised
        name when the CIK is NULL.

        ``insider_initial_holdings.filer_cik`` is NOT NULL by schema
        (Form 3 always has a CIK), so this scenario only applies to
        legacy Form 4 rows where the early ingester left ``filer_cik
        IS NULL``."""
        conn = _setup
        _seed_form4(
            conn,
            accession="F4-NULLCIK-001",
            instrument_id=789_001,
            filer_cik=None,
            filer_name="Smith John",
            txn_date=date(2026, 3, 1),
            post_transaction_shares="100",
        )
        _seed_form4(
            conn,
            accession="F4-NULLCIK-002",
            instrument_id=789_001,
            filer_cik=None,
            filer_name="Jones Jane",
            txn_date=date(2026, 3, 2),
            post_transaction_shares="200",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="GME", instrument_id=789_001)

        insiders = [s for s in rollup.slices if s.category == "insiders"][0]
        names = sorted(h.filer_name for h in insiders.holders)
        assert names == ["Jones Jane", "Smith John"]

    def test_joint_filer_13d_no_fanout_in_canonical_union(self, _setup: psycopg.Connection[tuple]) -> None:
        """Claude PR review round 2 (PR 798) PREVENTION: assert no
        fan-out when a single 13D accession carries multiple
        ``filing_id`` rows (joint reporters). The ``blocks`` CTE in
        ``_CANONICAL_UNION_SQL`` picks one ``filing_id`` per
        accession and the JOIN back to ``blockholder_filings`` is on
        the PK — exactly one survivor per accession enters the
        canonical-holder set, so the per-block aggregate is NOT
        summed across joint reporters."""
        conn = _setup
        # Two reporters under one accession claiming the same
        # aggregate_amount (SEC Rule 13d-1 requires joint reporters
        # to claim the same beneficial ownership).
        for reporter_name, reporter_cik in [
            ("Joint Reporter A", "0009990001"),
            ("Joint Reporter B", "0009990002"),
        ]:
            _seed_block(
                conn,
                accession="13D-JOINT-001",
                instrument_id=789_001,
                filer_cik="0009990000",
                filer_name="Joint Filer Group",
                submission_type="SCHEDULE 13D",
                aggregate_shares="5000000",
                filed_at=datetime(2025, 12, 1, tzinfo=UTC),
                reporter_cik=reporter_cik,
                reporter_name=reporter_name,
            )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="GME", instrument_id=789_001)
        block_slices = [s for s in rollup.slices if s.category == "blockholders"]
        assert len(block_slices) == 1
        # Exactly one block, carrying the per-accession aggregate
        # (5M). Doubled-up to 10M would mean the JOIN re-fanned.
        assert block_slices[0].total_shares == Decimal("5000000")

    def test_837_repro_gme_blockholder_surfaces_without_form4(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Repro of #837: with TWO 13D rows in ``blockholder_filings``
        for an instrument and NO Form 4 same-CIK competitor, the
        rollup must surface a blockholders slice. Prior to the fix
        the slice was always populated for this case (the bug was
        cross-source dedup with same-CIK Form 4); this test pins the
        baseline so any future regression that breaks the
        non-competing case trips."""
        conn = _setup
        # Cohen's 13D + RC Ventures 13D, joint filers on one accession.
        # SEC Rule 13d-1 requires both to claim the same beneficial
        # figure on the cover page — DISTINCT ON in the SQL collapses
        # to one row per accession.
        accession = "0000921895-25-000190"
        _seed_block(
            conn,
            accession=accession,
            instrument_id=789_001,
            filer_cik="0001767470",
            filer_name="Cohen Ryan",
            submission_type="SCHEDULE 13D/A",
            aggregate_shares="36847842",
            filed_at=datetime(2025, 1, 29, tzinfo=UTC),
            reporter_cik="0001767470",
            reporter_name="Cohen Ryan",
        )
        _seed_block(
            conn,
            accession=accession,
            instrument_id=789_001,
            filer_cik="0001767470",  # primary filer same
            filer_name="Cohen Ryan",
            submission_type="SCHEDULE 13D/A",
            aggregate_shares="36847842",
            filed_at=datetime(2025, 1, 29, tzinfo=UTC),
            reporter_cik="0001650235",  # joint reporter — RC Ventures
            reporter_name="RC Ventures LLC",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="GME", instrument_id=789_001)

        block_slices = [s for s in rollup.slices if s.category == "blockholders"]
        assert len(block_slices) == 1
        assert block_slices[0].filer_count == 1  # joint filers collapse
        assert block_slices[0].total_shares == Decimal("36847842")

    def test_837_amendment_chain_with_different_joint_reporters_does_not_double_count(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Codex pre-push review for #837: an amendment chain where
        successive filings pick different joint reporters as the
        ``DISTINCT ON`` representative could double-count if identity
        keyed on ``COALESCE(reporter_cik, primary_cik)``. Pin identity
        to the primary filer (``blockholder_filers.cik``) so amendments
        collapse correctly.

        Setup: two accessions, both joint Cohen + RC Ventures, primary
        filer Cohen on both. Equal aggregate shares. Should yield ONE
        blockholder row (latest amendment), not two."""
        conn = _setup
        primary_cik = "0001767470"
        # Amendment 1 — earlier filing.
        for reporter_cik, reporter_name in [
            ("0001767470", "Cohen Ryan"),
            ("0001650235", "RC Ventures LLC"),
        ]:
            _seed_block(
                conn,
                accession="13D-RC-2024-001",
                instrument_id=789_001,
                filer_cik=primary_cik,
                filer_name="Cohen Ryan",
                submission_type="SCHEDULE 13D/A",
                aggregate_shares="36847842",
                filed_at=datetime(2024, 8, 15, tzinfo=UTC),
                reporter_cik=reporter_cik,
                reporter_name=reporter_name,
            )
        # Amendment 2 — later filing, same primary filer + joint set.
        for reporter_cik, reporter_name in [
            ("0001767470", "Cohen Ryan"),
            ("0001650235", "RC Ventures LLC"),
        ]:
            _seed_block(
                conn,
                accession="13D-RC-2025-001",
                instrument_id=789_001,
                filer_cik=primary_cik,
                filer_name="Cohen Ryan",
                submission_type="SCHEDULE 13D/A",
                aggregate_shares="36847842",
                filed_at=datetime(2025, 1, 29, tzinfo=UTC),
                reporter_cik=reporter_cik,
                reporter_name=reporter_name,
            )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="GME", instrument_id=789_001)
        block_slices = [s for s in rollup.slices if s.category == "blockholders"]
        assert len(block_slices) == 1
        assert block_slices[0].filer_count == 1
        # Both rows share aggregate 36,847,842 — collapsed (latest
        # amendment wins). Doubled to 73,695,684 = double-count bug.
        assert block_slices[0].total_shares == Decimal("36847842")
        # Latest amendment's accession is the survivor. Under #905 the
        # _current path returns the per-(reporter_cik, nature) winning
        # row already, so older amendments do not show up as
        # dropped_sources at the rollup layer — that history is still
        # preserved in ownership_blockholders_observations for
        # drill-through, but the rollup just exposes the latest.
        assert block_slices[0].holders[0].winning_accession == "13D-RC-2025-001"
        # Post-#905 invariant: rollup layer no longer surfaces earlier
        # amendments as dropped_sources because the read path consumes
        # ownership_blockholders_current, which is already per-(reporter_cik,
        # nature) deduped. Earlier amendments still live in
        # ownership_blockholders_observations for drillthrough queries.
        assert block_slices[0].holders[0].dropped_sources == ()

    def test_837_regression_other_instrument_blockholder_surfaces(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Regression coverage for a DIFFERENT instrument with a
        13D/G + same-CIK Form 4. Prevents a future patch from
        accidentally re-introducing the cross-source dedup that
        dropped 13D/G whenever a Form 4 shared the CIK."""
        conn = ebull_test_conn
        iid = 837_900
        cik = "0007770001"
        _seed_instrument(conn, iid=iid, symbol="OTHER")
        _seed_outstanding(conn, instrument_id=iid, shares="100000000")
        _seed_form4(
            conn,
            accession="F4-OTHER-2026-001",
            instrument_id=iid,
            filer_cik=cik,
            filer_name="Other Insider",
            txn_date=date(2026, 2, 14),
            post_transaction_shares="2000000",
        )
        _seed_block(
            conn,
            accession="13G-OTHER-2026-001",
            instrument_id=iid,
            filer_cik=cik,
            filer_name="Other Insider",
            submission_type="SCHEDULE 13G",
            aggregate_shares="6000000",
            filed_at=datetime(2026, 2, 1, tzinfo=UTC),
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="OTHER", instrument_id=iid)

        # Both slices present, both keyed on the same CIK with their
        # respective figures.
        insider_slices = [s for s in rollup.slices if s.category == "insiders"]
        block_slices = [s for s in rollup.slices if s.category == "blockholders"]
        assert len(insider_slices) == 1
        assert len(block_slices) == 1
        assert insider_slices[0].holders[0].shares == Decimal("2000000")
        assert insider_slices[0].holders[0].winning_source == "form4"
        assert block_slices[0].holders[0].shares == Decimal("6000000")
        assert block_slices[0].holders[0].winning_source == "13g"
        assert block_slices[0].filer_count == 1


# ---------------------------------------------------------------------------
# DEF 14A enrichment
# ---------------------------------------------------------------------------


class TestDef14aEnrichment:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=789_010, symbol="DEF14A")
        _seed_outstanding(conn, instrument_id=789_010, shares="100000000")
        conn.commit()
        return conn

    def test_resolver_matches_form4_filer(self, _setup: psycopg.Connection[tuple]) -> None:
        """DEF 14A holder ``"Smith Jane"`` resolves to a Form 4 filer
        with CIK ``0001100100``. Under the two-axis model (#840 P1 +
        #905 read-path cutover), Form 4 (direct) and DEF 14A
        (beneficial) live in separate dedup groups by ownership_nature
        and BOTH surface as holders in the insiders slice. The DEF 14A
        is no longer dropped against Form 4 — they describe different
        natures (record-name vs Rule 13d-3 beneficial) and the
        operator-visible chart shows both."""
        conn = _setup
        cik = "0001100100"
        _seed_form4(
            conn,
            accession="F4-SMITH-001",
            instrument_id=789_010,
            filer_cik=cik,
            filer_name="Smith Jane",
            txn_date=date(2026, 2, 15),
            post_transaction_shares="500000",
        )
        _seed_def14a(
            conn,
            accession="DEF14A-2026-001",
            instrument_id=789_010,
            holder_name="Smith Jane, Director",
            shares="500000",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="DEF14A", instrument_id=789_010)

        insiders = [s for s in rollup.slices if s.category == "insiders"][0]
        sources_present = {h.winning_source for h in insiders.holders}
        assert sources_present == {"form4", "def14a"}, (
            f"insiders should carry both form4 + def14a holders: {sources_present}"
        )
        # def14a was matched against the Form 4 by name resolver, so it
        # must NOT land in the unmatched slice.
        assert not any(s.category == "def14a_unmatched" for s in rollup.slices)

    def test_unmatched_def14a_lands_in_unmatched_slice(self, _setup: psycopg.Connection[tuple]) -> None:
        """Proxy-only holder with no Form 4 / Form 3 — ``def14a_unmatched``
        slice surfaces the row so the operator doesn't lose it."""
        conn = _setup
        _seed_def14a(
            conn,
            accession="DEF14A-2026-002",
            instrument_id=789_010,
            holder_name="Doe Jonathan III",
            shares="123456",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="DEF14A", instrument_id=789_010)

        unmatched = [s for s in rollup.slices if s.category == "def14a_unmatched"]
        assert len(unmatched) == 1
        assert unmatched[0].holders[0].filer_name == "Doe Jonathan III"
        assert unmatched[0].holders[0].shares == Decimal("123456")

    def test_def14a_legacy_null_cik_match_routes_to_insiders(self, _setup: psycopg.Connection[tuple]) -> None:
        """Codex pre-push review (Batch 1 of #788) caught this: a
        DEF 14A holder name that resolves to a legacy NULL-CIK Form 4
        row must route to the insiders slice (not def14a_unmatched).
        The resolver returns ``matched=True, cik=None`` for that case.

        Post-#905 two-axis model: Form 4 (direct) and the resolved
        DEF 14A (beneficial) BOTH render as holders in the insiders
        slice. The matched DEF 14A is no longer ``def14a_unmatched`` —
        which is the regression this test originally caught — but is
        also no longer dropped against the Form 4 row, since they
        describe different ownership natures."""
        conn = _setup
        _seed_form4(
            conn,
            accession="F4-LEGACY-001",
            instrument_id=789_010,
            filer_cik=None,
            filer_name="Legacy Officer",
            txn_date=date(2024, 1, 1),
            post_transaction_shares="42000",
        )
        _seed_def14a(
            conn,
            accession="DEF14A-2026-LEGACY",
            instrument_id=789_010,
            holder_name="Legacy Officer",
            shares="42000",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="DEF14A", instrument_id=789_010)
        insiders = [s for s in rollup.slices if s.category == "insiders"]
        assert len(insiders) == 1
        # Legacy NULL-CIK Form 4 + matched DEF 14A both surface in the
        # insiders slice under the two-axis model.
        sources_present = {h.winning_source for h in insiders[0].holders}
        assert sources_present == {"form4", "def14a"}
        # DEF 14A is matched, not unmatched.
        assert not any(s.category == "def14a_unmatched" for s in rollup.slices)
        # def14a_unmatched slice should NOT contain this holder.
        assert not any(s.category == "def14a_unmatched" for s in rollup.slices)

    def test_resolver_prefers_cik_backed_row_over_legacy_null_cik(self, _setup: psycopg.Connection[tuple]) -> None:
        """Codex pre-push review (Batch 1 of #788) caught this: when
        a filer has BOTH a legacy NULL-CIK Form 4 row and a newer
        CIK-backed Form 4 row, ``resolve_holder_to_filer`` must
        prefer the CIK-backed row so DEF 14A names resolve to the
        canonical CIK identity. The prior version returned the
        NULL-CIK row first because COALESCE(filer_cik,'') sorted
        empty strings ahead of populated CIKs."""
        from app.services.holder_name_resolver import resolve_holder_to_filer

        conn = _setup
        _seed_form4(
            conn,
            accession="F4-LEG-DUP-1",
            instrument_id=789_010,
            filer_cik=None,
            filer_name="Dual Identity",
            txn_date=date(2020, 1, 1),
            post_transaction_shares="100",
        )
        _seed_form4(
            conn,
            accession="F4-LEG-DUP-2",
            instrument_id=789_010,
            filer_cik="0009999009",
            filer_name="Dual Identity",
            txn_date=date(2026, 1, 1),
            post_transaction_shares="500",
        )
        conn.commit()

        matched, cik, shares = resolve_holder_to_filer(conn, instrument_id=789_010, holder_name="Dual Identity")
        assert matched is True
        assert cik == "0009999009"  # CIK-backed row wins
        assert shares == Decimal("500")


# ---------------------------------------------------------------------------
# Residual + concentration + treasury
# ---------------------------------------------------------------------------


class TestResidualAndCoverage:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=789_020, symbol="RESID")
        # 100M outstanding, 10M treasury.
        _seed_outstanding(conn, instrument_id=789_020, shares="100000000", treasury="10000000")
        conn.commit()
        return conn

    def test_residual_label_and_value(self, _setup: psycopg.Connection[tuple]) -> None:
        """30M known + 10M treasury → residual = 60M, label =
        Public / unattributed, oversubscribed=False."""
        conn = _setup
        _seed_form4(
            conn,
            accession="F4-RESID-001",
            instrument_id=789_020,
            filer_cik="0009999001",
            filer_name="Big Holder Inc",
            txn_date=date(2026, 3, 1),
            post_transaction_shares="30000000",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="RESID", instrument_id=789_020)

        assert rollup.residual.label == "Public / unattributed"
        assert rollup.residual.shares == Decimal("60000000")
        assert rollup.residual.oversubscribed is False
        # Concentration: 30M / 100M = 30% (treasury excluded from numerator).
        assert rollup.concentration.pct_outstanding_known == Decimal("0.30")

    def test_oversubscribed_clamps_residual_to_zero(self, _setup: psycopg.Connection[tuple]) -> None:
        """Stale 13F + fresh 13D: holders sum to 110% of outstanding.
        Residual clamps to 0 with ``oversubscribed=True``."""
        conn = _setup
        _seed_block(
            conn,
            accession="13D-OVER-2026-001",
            instrument_id=789_020,
            filer_cik="0008888001",
            filer_name="Stale Block",
            submission_type="SCHEDULE 13D",
            aggregate_shares="110000000",
            filed_at=datetime(2026, 3, 1, tzinfo=UTC),
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="RESID", instrument_id=789_020)

        assert rollup.residual.shares == Decimal(0)
        assert rollup.residual.oversubscribed is True

    def test_treasury_excluded_from_concentration(self, _setup: psycopg.Connection[tuple]) -> None:
        """Concentration numerator = sum(slices). Treasury (10M) is
        NOT added — concentration must stay 30% / 30M, not 40%."""
        conn = _setup
        _seed_form4(
            conn,
            accession="F4-TREAS-001",
            instrument_id=789_020,
            filer_cik="0009998001",
            filer_name="Director X",
            txn_date=date(2026, 1, 1),
            post_transaction_shares="30000000",
        )
        conn.commit()
        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="RESID", instrument_id=789_020)
        assert rollup.concentration.pct_outstanding_known == Decimal("0.30")


# ---------------------------------------------------------------------------
# Coverage banner + states
# ---------------------------------------------------------------------------


class TestCoverageBanner:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=789_030, symbol="BANNER")
        conn.commit()
        return conn

    def test_state_no_data_when_outstanding_missing(self, _setup: psycopg.Connection[tuple]) -> None:
        """No XBRL outstanding row → banner state ``no_data``,
        slices empty, residual=0, 200 OK semantics."""
        conn = _setup
        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="BANNER", instrument_id=789_030)
        assert rollup.banner.state == "no_data"
        assert rollup.banner.variant == "error"
        assert rollup.shares_outstanding is None
        assert rollup.slices == ()
        assert rollup.residual.shares == Decimal(0)

    def test_state_unknown_universe_default(self, _setup: psycopg.Connection[tuple]) -> None:
        """Outstanding present + no per-category estimates seeded
        (Tier 0 default) → banner ``unknown_universe`` regardless
        of the actual filer count."""
        conn = _setup
        _seed_outstanding(conn, instrument_id=789_030, shares="100000000")
        _seed_form4(
            conn,
            accession="F4-BANNER-001",
            instrument_id=789_030,
            filer_cik="0007777001",
            filer_name="Holder One",
            txn_date=date(2026, 1, 1),
            post_transaction_shares="1000000",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="BANNER", instrument_id=789_030)
        assert rollup.banner.state == "unknown_universe"
        assert rollup.banner.variant == "warning"
        # Per-category states should also all be unknown_universe.
        for cov in rollup.coverage.categories.values():
            assert cov.state == "unknown_universe"


# ---------------------------------------------------------------------------
# Snapshot isolation
# ---------------------------------------------------------------------------


class TestSnapshotIsolation:
    """Confirm the FastAPI handler's ``snapshot_read`` wrap holds —
    a write committed mid-rollup must NOT alter the rollup's view."""

    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=789_040, symbol="SNAP")
        _seed_outstanding(conn, instrument_id=789_040, shares="100000000")
        _seed_form4(
            conn,
            accession="F4-SNAP-001",
            instrument_id=789_040,
            filer_cik="0006666001",
            filer_name="Snap Holder",
            txn_date=date(2026, 1, 1),
            post_transaction_shares="1000000",
        )
        conn.commit()
        return conn

    def test_snapshot_holds_under_concurrent_write(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Open a snapshot, run the rollup, write a new Form 4 from a
        SECOND connection mid-flight, then call the rollup AGAIN on
        the still-open snapshot. The second invocation should see
        the same numbers as the first (REPEATABLE READ semantics)."""
        from app.db.snapshot import snapshot_read
        from tests.fixtures.ebull_test_db import test_database_url

        conn = _setup
        with snapshot_read(conn):
            first = ownership_rollup.get_ownership_rollup(conn, symbol="SNAP", instrument_id=789_040)

            # Concurrent write on a separate connection — must point at
            # the same ``ebull_test`` DB the fixture seeded into, NOT
            # the dev DB.
            with psycopg.connect(test_database_url()) as writer:
                writer.execute(
                    """
                    INSERT INTO insider_filings (
                        accession_number, instrument_id, document_type, issuer_cik
                    ) VALUES ('F4-SNAP-002-NEW', 789040, '4', '0000000789')
                    """,
                )
                writer.execute(
                    """
                    INSERT INTO insider_transactions (
                        accession_number, txn_row_num, instrument_id, filer_cik,
                        filer_name, txn_date, txn_code, shares,
                        post_transaction_shares, is_derivative
                    ) VALUES ('F4-SNAP-002-NEW', 1, 789040, '0006666002',
                              'Concurrent Holder', '2026-04-01', 'P', 100,
                              500000, FALSE)
                    """,
                )
                writer.commit()

            second = ownership_rollup.get_ownership_rollup(conn, symbol="SNAP", instrument_id=789_040)

        assert len(first.slices) == len(second.slices)
        first_insiders = [s for s in first.slices if s.category == "insiders"][0]
        second_insiders = [s for s in second.slices if s.category == "insiders"][0]
        assert first_insiders.filer_count == second_insiders.filer_count == 1
        # Cleanup the concurrent write so subsequent tests see a clean state
        # (the write committed on a separate connection so the per-test
        # truncate fixture covers it on next test, but the smoke test
        # within this transaction needed both reads to agree first).


# ---------------------------------------------------------------------------
# Empty / pre-ingest state
# ---------------------------------------------------------------------------


class TestProvenance:
    """Per-holder ``edgar_url`` derivation + shares-outstanding source
    accession threading (#792, Batch 3 of #788)."""

    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=792_001, symbol="PROV")
        _seed_outstanding(conn, instrument_id=792_001, shares="100000000")
        conn.commit()
        return conn

    def test_edgar_archive_url_derivation(self) -> None:
        from app.services.ownership_rollup import edgar_archive_url

        url = edgar_archive_url("0001767470-26-000003")
        assert url == (
            "https://www.sec.gov/Archives/edgar/data/1767470/000176747026000003/0001767470-26-000003-index.htm"
        )
        assert edgar_archive_url(None) is None
        assert edgar_archive_url("") is None
        assert edgar_archive_url("malformed") is None

    def test_holder_carries_winning_edgar_url(self, _setup: psycopg.Connection[tuple]) -> None:
        conn = _setup
        _seed_form4(
            conn,
            accession="0001234567-26-000001",
            instrument_id=792_001,
            filer_cik="0001234567",
            filer_name="Provenance Holder",
            txn_date=date(2026, 4, 1),
            post_transaction_shares="500000",
        )
        conn.commit()
        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="PROV", instrument_id=792_001)
        insiders = [s for s in rollup.slices if s.category == "insiders"][0]
        holder = insiders.holders[0]
        assert holder.winning_edgar_url is not None
        assert "0001234567-26-000001" in holder.winning_edgar_url
        assert holder.winning_edgar_url.startswith("https://www.sec.gov/Archives/edgar/data/")

    def test_shares_outstanding_source_accession_threaded(self, _setup: psycopg.Connection[tuple]) -> None:
        """The shares_outstanding_source payload should carry the
        accession + form_type from financial_facts_raw, not just the
        view's source taxonomy."""
        conn = _setup
        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="PROV", instrument_id=792_001)
        assert rollup.shares_outstanding_source.accession_number is not None
        # Seeded fixture uses 'OUTSTANDING-{iid}-{period_end}' format.
        assert "OUTSTANDING-792001" in rollup.shares_outstanding_source.accession_number
        assert rollup.shares_outstanding_source.form_type == "10-Q"

    def test_shares_outstanding_source_edgar_url_backend_computed(self, _setup: psycopg.Connection[tuple]) -> None:
        """Claude PR 800 review caught the prior frontend ``filenum=``
        URL — ``filenum`` expects a SEC file number (e.g. 001-12345),
        not an accession. The backend now ships the pre-computed
        archive URL so the frontend cannot drift to a wrong endpoint.

        The seeded synthetic accession (``OUTSTANDING-792001-...``)
        does not follow SEC's ``cik-yy-seq`` shape, so URL derivation
        returns None gracefully. The real-format path is exercised by
        :py:meth:`test_edgar_archive_url_derivation`."""
        conn = _setup
        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="PROV", instrument_id=792_001)
        assert rollup.shares_outstanding_source.edgar_url is None


class TestHistoricalSymbols:
    """Symbol-history payload threading (#794 frontend finish, Batch 7
    of #788)."""

    def test_rollup_includes_historical_symbols_from_history_table(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=794_001, symbol="BBBYQ")
        _seed_outstanding(conn, instrument_id=794_001, shares="100000000")
        # Seed a BBBY → BBBYQ chain manually (the real ticker-change
        # ingester is a future epic; here we exercise the read path).
        conn.execute(
            """
            INSERT INTO instrument_symbol_history (
                instrument_id, symbol, effective_from, effective_to,
                source_event
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (794_001, "BBBY", date(2000, 6, 1), date(2023, 4, 1), "delisting"),
        )
        conn.execute(
            """
            INSERT INTO instrument_symbol_history (
                instrument_id, symbol, effective_from, effective_to,
                source_event
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (794_001, "BBBYQ", date(2023, 4, 1), None, "relisting"),
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="BBBYQ", instrument_id=794_001)
        symbols = [h.symbol for h in rollup.historical_symbols]
        assert symbols == ["BBBY", "BBBYQ"]
        # Effective ranges round-trip cleanly.
        bbby_entry = next(h for h in rollup.historical_symbols if h.symbol == "BBBY")
        assert bbby_entry.effective_to == date(2023, 4, 1)
        assert bbby_entry.source_event == "delisting"

    def test_rollup_historical_symbols_empty_when_no_history(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=794_002, symbol="NOHIST")
        _seed_outstanding(conn, instrument_id=794_002, shares="100000000")
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="NOHIST", instrument_id=794_002)
        assert rollup.historical_symbols == ()

    def test_historical_symbols_present_on_no_data_path(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Codex pre-push review (Batch 7 of #788) caught this: an
        instrument missing ``shares_outstanding`` returns
        ``OwnershipRollup.no_data(...)`` — but it must still carry
        ``historical_symbols`` because the BBBY-style ticker-change
        case is exactly when the operator wants the callout."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=794_003, symbol="NODATA")
        # Skip _seed_outstanding intentionally — exercise the no_data
        # path.
        conn.execute(
            """
            INSERT INTO instrument_symbol_history (
                instrument_id, symbol, effective_from, effective_to,
                source_event
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (794_003, "OLDSYM", date(2010, 1, 1), date(2024, 1, 1), "rebrand"),
        )
        conn.execute(
            """
            INSERT INTO instrument_symbol_history (
                instrument_id, symbol, effective_from, effective_to,
                source_event
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (794_003, "NODATA", date(2024, 1, 1), None, "imported"),
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="NODATA", instrument_id=794_003)
        # no_data state — outstanding missing.
        assert rollup.banner.state == "no_data"
        assert rollup.shares_outstanding is None
        # Historical symbols still surface so the callout renders.
        symbols = [h.symbol for h in rollup.historical_symbols]
        assert symbols == ["OLDSYM", "NODATA"]


class TestEmptyStates:
    def test_empty_cohort_residual_equals_outstanding(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Outstanding present, no filings of any kind → residual =
        100% of outstanding, every slice missing."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=789_050, symbol="EMPTY")
        _seed_outstanding(conn, instrument_id=789_050, shares="100000000")
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="EMPTY", instrument_id=789_050)
        assert rollup.slices == ()
        assert rollup.residual.shares == Decimal("100000000")
        assert rollup.banner.state == "unknown_universe"
