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

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import psycopg
import psycopg.rows
import pytest

from app.services import ownership_rollup
from app.services.ownership_observations import (
    record_blockholder_observation,
    record_def14a_observation,
    record_esop_observation,
    record_fund_observation,
    record_insider_observation,
    record_institution_observation,
    record_treasury_observation,
    refresh_blockholders_current,
    refresh_def14a_current,
    refresh_esop_current,
    refresh_funds_current,
    refresh_insiders_current,
    refresh_institutions_current,
    refresh_treasury_current,
    upsert_sec_fund_series,
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
            source_accession="0001234517-25-000017",
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
    #
    # Production ingest filters ``is_derivative = FALSE`` before
    # calling ``record_insider_observation`` (see
    # insider_transactions.py around line 1200) — derivative rows
    # carry option / RSU / etc. exposures that are not part of the
    # equity ownership rollup. Mirror that guard here so seeding a
    # derivative Form 4 in a fixture does not falsely inflate
    # ``ownership_insiders_current`` post-#905. Bot review caught this
    # on PR #911 round 2.
    if not is_derivative:
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

    def test_form4_and_13d_same_cik_counted_once(self, _setup: psycopg.Connection[tuple]) -> None:
        """Cohen-on-GME shape: Form 4 (38.35M) + 13D/A (36.85M) reporting the
        SAME CIK. Per #1640 the holder is counted ONCE — these are the same
        stake through two lenses (live data falsified the #837 38-vs-75M
        premise; 13D ≈ Form 4). Cohen lands in insiders at MAX (38.35M, from
        Form 4, role = insider); his 13D is a ``dropped_source`` for
        provenance; the blockholders slice (he was its only member) does not
        render."""
        conn = _setup
        cik = "0001767470"
        _seed_form4(
            conn,
            accession="0001234500-25-000101",
            instrument_id=789_001,
            filer_cik=cik,
            filer_name="Cohen Ryan",
            txn_date=date(2026, 1, 21),
            post_transaction_shares="38347842",
        )
        _seed_block(
            conn,
            accession="0001234500-25-000102",
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
        assert insider_slices[0].filer_count == 1  # counted ONCE
        cohen = insider_slices[0].holders[0]
        assert cohen.filer_cik == cik
        assert cohen.shares == Decimal("38347842")  # MAX, not 38.35M + 36.85M
        assert cohen.winning_source == "form4"
        # 13D preserved as provenance on the single deduped row.
        assert [d.source for d in cohen.dropped_sources] == ["13d"]
        assert cohen.dropped_sources[0].shares == Decimal("36847842")

        # No standalone blockholders wedge — Cohen was its only member.
        assert not any(s.category == "blockholders" for s in rollup.slices)

    def test_concentration_counts_dual_channel_owner_once(self, _setup: psycopg.Connection[tuple]) -> None:
        """The #1640 GME concentration repro: Cohen insider 38.35M + same-CIK
        13D 36.85M + an institution 140M, outstanding 448.375M. Cohen is
        counted ONCE → known = (38.35M + 140M) / 448.375M, NOT inflated by his
        double-counted 13D. Public residual is the complement."""
        conn = _setup
        cik = "0001767470"
        _seed_form4(
            conn,
            accession="0001234500-25-000201",
            instrument_id=789_001,
            filer_cik=cik,
            filer_name="Cohen Ryan",
            txn_date=date(2026, 1, 21),
            post_transaction_shares="38347842",
        )
        _seed_block(
            conn,
            accession="0001234500-25-000202",
            instrument_id=789_001,
            filer_cik=cik,
            filer_name="Cohen Ryan",
            submission_type="SCHEDULE 13D/A",
            aggregate_shares="36847842",
            filed_at=datetime(2026, 1, 29, tzinfo=UTC),
        )
        _seed_inst_holding(
            conn,
            accession="0001234500-25-000203",
            instrument_id=789_001,
            filer_cik="0000102909",
            filer_name="VANGUARD GROUP INC",
            filer_type="INV",
            period_of_report=date(2025, 12, 31),
            shares="140000000",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="GME", instrument_id=789_001)

        known = Decimal("38347842") + Decimal("140000000")  # Cohen once + institution
        outstanding = Decimal("448375157")
        assert rollup.concentration.pct_outstanding_known == known / outstanding
        # The double-counted figure would have added the 36.85M 13D again.
        inflated = (known + Decimal("36847842")) / outstanding
        assert rollup.concentration.pct_outstanding_known < inflated
        assert rollup.residual.shares == outstanding - known
        assert not any(s.category == "blockholders" for s in rollup.slices)

    def test_13g_and_13f_same_cik_counted_once(self, _setup: psycopg.Connection[tuple]) -> None:
        """A large institution that crossed 5% files 13G (22M beneficial) AND
        a 13F (21.8M, ETF). Per #1640 these report the same book through two
        lenses → counted ONCE. Not an insider; has a 13F → bucketed by the 13F
        filer_type (etfs). Lands at MAX (22M, the 13G); the 13F is a
        ``dropped_source``. No standalone blockholders wedge.

        Uses a neutral filer (not a curated institutional family, #1644) so this
        exercises the generic per-CIK owner-once path, not the family collapse."""
        conn = _setup
        cik = "0000555501"
        _seed_block(
            conn,
            accession="0001234500-25-000103",
            instrument_id=789_001,
            filer_cik=cik,
            filer_name="EXAMPLE INDEX ETF TRUST",
            submission_type="SCHEDULE 13G/A",
            aggregate_shares="22000000",
            filed_at=datetime(2025, 12, 31, tzinfo=UTC),
        )
        _seed_inst_holding(
            conn,
            accession="0001234500-25-000104",
            instrument_id=789_001,
            filer_cik=cik,
            filer_name="EXAMPLE INDEX ETF TRUST",
            filer_type="ETF",
            period_of_report=date(2025, 12, 31),
            shares="21800000",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="GME", instrument_id=789_001)

        # Counted once in the etfs slice (role: has a 13F, filer_type ETF).
        etf_slices = [s for s in rollup.slices if s.category == "etfs"]
        assert len(etf_slices) == 1
        assert etf_slices[0].filer_count == 1
        vg = etf_slices[0].holders[0]
        assert vg.filer_cik == cik
        assert vg.shares == Decimal("22000000")  # MAX(13G 22M, 13F 21.8M)
        assert vg.winning_source == "13g"
        assert [d.source for d in vg.dropped_sources] == ["13f"]

        # No standalone blockholders wedge — the 13G collapsed into the owner.
        assert not any(s.category == "blockholders" for s in rollup.slices)

    def test_form3_baseline_wins_when_no_form4(self, _setup: psycopg.Connection[tuple]) -> None:
        """Officer with Form 3 baseline + no Form 4 — Form 3 supplies
        the holding row."""
        conn = _setup
        _seed_form3(
            conn,
            accession="0001234500-25-000105",
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
            accession="0001234500-25-000106",
            instrument_id=789_001,
            filer_cik=cik,
            filer_name="Director Beta",
            shares="50000",
            as_of=date(2024, 1, 15),
        )
        _seed_form4(
            conn,
            accession="0001234500-25-000107",
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
            accession="0001234500-25-000108",
            instrument_id=789_001,
            filer_cik=None,
            filer_name="Smith John",
            txn_date=date(2026, 3, 1),
            post_transaction_shares="100",
        )
        _seed_form4(
            conn,
            accession="0001234500-25-000109",
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
                accession="0001234500-25-000110",
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
                accession="0001234500-25-000111",
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
                accession="0001234500-25-000112",
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
        assert block_slices[0].holders[0].winning_accession == "0001234500-25-000112"
        # Post-#905 invariant: rollup layer no longer surfaces earlier
        # amendments as dropped_sources because the read path consumes
        # ownership_blockholders_current, which is already per-(reporter_cik,
        # nature) deduped. Earlier amendments still live in
        # ownership_blockholders_observations for drillthrough queries.
        assert block_slices[0].holders[0].dropped_sources == ()

    def test_insider_with_larger_13g_counted_once_at_max(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A Section-16 insider whose 13G beneficial (6M) exceeds their Form 4
        direct (2M), same CIK. Per #1640: counted ONCE, classified by role
        (insider), at their total beneficial ownership (MAX = 6M from the 13G);
        the Form 4 is a ``dropped_source``. No standalone blockholders wedge.
        (Was test_837_regression_*, which pinned the superseded show-both
        posture — see docs/specs/etl/2026-06-15-ownership-owner-once-dedup.md.)"""
        conn = ebull_test_conn
        iid = 837_900
        cik = "0007770001"
        _seed_instrument(conn, iid=iid, symbol="OTHER")
        _seed_outstanding(conn, instrument_id=iid, shares="100000000")
        _seed_form4(
            conn,
            accession="0001234500-25-000113",
            instrument_id=iid,
            filer_cik=cik,
            filer_name="Other Insider",
            txn_date=date(2026, 2, 14),
            post_transaction_shares="2000000",
        )
        _seed_block(
            conn,
            accession="0001234500-25-000114",
            instrument_id=iid,
            filer_cik=cik,
            filer_name="Other Insider",
            submission_type="SCHEDULE 13G",
            aggregate_shares="6000000",
            filed_at=datetime(2026, 2, 1, tzinfo=UTC),
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="OTHER", instrument_id=iid)

        insider_slices = [s for s in rollup.slices if s.category == "insiders"]
        assert len(insider_slices) == 1
        assert insider_slices[0].filer_count == 1
        holder = insider_slices[0].holders[0]
        assert holder.shares == Decimal("6000000")  # MAX(form4 2M, 13G 6M)
        assert holder.winning_source == "13g"
        assert [d.source for d in holder.dropped_sources] == ["form4"]
        assert not any(s.category == "blockholders" for s in rollup.slices)


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
        """DEF 14A holder ``"Smith Jane"`` resolves to a Form 4 filer with CIK
        ``0001100100``. Per #1640 the matched DEF 14A (beneficial) and the
        Form 4 (Section-16) are overlapping restatements of one stake → counted
        ONCE in insiders at MAX (tie at 500K → Form 4 wins on priority). The
        DEF 14A becomes a ``dropped_source``, NOT a second summed row (this is
        the latent insiders double-count Codex surfaced). Still not unmatched."""
        conn = _setup
        cik = "0001100100"
        _seed_form4(
            conn,
            accession="0001234500-25-000115",
            instrument_id=789_010,
            filer_cik=cik,
            filer_name="Smith Jane",
            txn_date=date(2026, 2, 15),
            post_transaction_shares="500000",
        )
        _seed_def14a(
            conn,
            accession="0001234500-25-000116",
            instrument_id=789_010,
            holder_name="Smith Jane, Director",
            shares="500000",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="DEF14A", instrument_id=789_010)

        insiders = [s for s in rollup.slices if s.category == "insiders"][0]
        assert insiders.filer_count == 1  # counted ONCE, not form4 + def14a summed
        assert insiders.total_shares == Decimal("500000")  # MAX, not 1,000,000
        holder = insiders.holders[0]
        assert holder.winning_source == "form4"
        assert [d.source for d in holder.dropped_sources] == ["def14a"]
        # def14a was matched against the Form 4 by name resolver, so it
        # must NOT land in the unmatched slice.
        assert not any(s.category == "def14a_unmatched" for s in rollup.slices)

    def test_unmatched_def14a_lands_in_unmatched_slice(self, _setup: psycopg.Connection[tuple]) -> None:
        """Proxy-only holder with no Form 4 / Form 3 — ``def14a_unmatched``
        slice surfaces the row so the operator doesn't lose it."""
        conn = _setup
        _seed_def14a(
            conn,
            accession="0001234500-25-000117",
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
        # #1659: the unmatched DEF 14A slice is a NON-ADDITIVE memo overlay — it
        # surfaces the holder (inspectable) but contributes nothing to the pie.
        assert unmatched[0].denominator_basis == "proxy_disclosure"
        # With no additive (13F/13D/Form 4) slice, known concentration is 0 and the
        # proxy block is NOT subtracted from residual (it would be if additive).
        assert rollup.concentration.pct_outstanding_known == Decimal(0)
        assert not rollup.residual.oversubscribed

    def test_def14a_legacy_null_cik_match_routes_to_insiders(self, _setup: psycopg.Connection[tuple]) -> None:
        """Codex pre-push review (Batch 1 of #788) caught this: a
        DEF 14A holder name that resolves to a legacy NULL-CIK Form 4
        row must route to the insiders slice (not def14a_unmatched).
        The resolver returns ``matched=True, cik=None`` for that case.

        The matched DEF 14A must still route to insiders (not
        ``def14a_unmatched``) — the regression this test guards. Per #1640 the
        Form 4 and the matched DEF 14A (same NULL-CIK name identity) are now
        counted ONCE at MAX, with the loser as a ``dropped_source`` — not two
        summed rows."""
        conn = _setup
        _seed_form4(
            conn,
            accession="0001234500-25-000118",
            instrument_id=789_010,
            filer_cik=None,
            filer_name="Legacy Officer",
            txn_date=date(2024, 1, 1),
            post_transaction_shares="42000",
        )
        _seed_def14a(
            conn,
            accession="0001234500-25-000119",
            instrument_id=789_010,
            holder_name="Legacy Officer",
            shares="42000",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="DEF14A", instrument_id=789_010)
        insiders = [s for s in rollup.slices if s.category == "insiders"]
        assert len(insiders) == 1
        # Legacy NULL-CIK Form 4 + matched DEF 14A collapse to ONE owner
        # (name-key identity) at MAX, not two summed rows.
        assert insiders[0].filer_count == 1
        assert insiders[0].total_shares == Decimal("42000")  # MAX, not 84000
        holder = insiders[0].holders[0]
        assert holder.winning_source == "form4"
        assert [d.source for d in holder.dropped_sources] == ["def14a"]
        # DEF 14A is matched, not unmatched.
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
            accession="0001234500-25-000120",
            instrument_id=789_010,
            filer_cik=None,
            filer_name="Dual Identity",
            txn_date=date(2020, 1, 1),
            post_transaction_shares="100",
        )
        _seed_form4(
            conn,
            accession="0001234500-25-000121",
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
            accession="0001234500-25-000122",
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
            accession="0001234500-25-000123",
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
            accession="0001234500-25-000124",
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
            accession="0001234500-25-000125",
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

    def test_state_no_data_when_denominator_stale(self, _setup: psycopg.Connection[tuple]) -> None:
        """A shares-outstanding row that EXISTS but is many years stale (the
        #1581 dual-class dimension-only trap — BRK.B's newest un-dimensioned
        count is 2011) suppresses to ``no_data`` with an honest banner rather
        than computing nonsense percentages, EVEN when holder slices are
        present. The stale ``as_of`` is retained as the FE discriminator."""
        conn = _setup
        _seed_outstanding(
            conn,
            instrument_id=789_030,
            shares="941481",
            period_end=date(2011, 4, 29),
        )
        # A holder that WOULD render a slice if the denominator were usable —
        # proves the guard fires before bucketing, not just on empty cohorts.
        _seed_form4(
            conn,
            accession="0001234500-25-000130",
            instrument_id=789_030,
            filer_cik="0007777010",
            filer_name="Stale Holder",
            txn_date=date(2026, 1, 1),
            post_transaction_shares="1000000",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="BANNER", instrument_id=789_030)
        assert rollup.banner.state == "no_data"
        assert rollup.banner.variant == "error"
        assert rollup.shares_outstanding is None
        # as_of retained (absent nulls it) so the FE can tell stale from absent.
        assert rollup.shares_outstanding_as_of == date(2011, 4, 29)
        assert "29 Apr 2011" in rollup.banner.body
        assert "too stale" in rollup.banner.body
        assert rollup.slices == ()


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
            accession="0001234500-25-000126",
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
                    ) VALUES ('0001234500-25-000404', 789040, '4', '0000000789')
                    """,
                )
                writer.execute(
                    """
                    INSERT INTO insider_transactions (
                        accession_number, txn_row_num, instrument_id, filer_cik,
                        filer_name, txn_date, txn_code, shares,
                        post_transaction_shares, is_derivative
                    ) VALUES ('0001234500-25-000404', 1, 789040, '0006666002',
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


def _seed_funds_holding(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    fund_series_id: str,
    fund_series_name: str,
    fund_filer_cik: str,
    accession: str,
    shares: str,
    market_value_usd: str | None = None,
    period_end: date = date(2026, 3, 31),
) -> None:
    """Seed an N-PORT fund holding via the canonical write-through
    helpers — mirrors what ``app.services.n_port_ingest`` does in
    production. Used by #919 funds-slice tests."""
    upsert_sec_fund_series(
        conn,
        fund_series_id=fund_series_id,
        fund_series_name=fund_series_name,
        fund_filer_cik=fund_filer_cik,
        last_seen_period_end=period_end,
    )
    record_fund_observation(
        conn,
        instrument_id=instrument_id,
        fund_series_id=fund_series_id,
        fund_series_name=fund_series_name,
        fund_filer_cik=fund_filer_cik,
        source_document_id=accession,
        source_accession=accession,
        source_field=None,
        source_url=None,
        filed_at=datetime(period_end.year, period_end.month, 1, tzinfo=UTC),
        period_start=None,
        period_end=period_end,
        ingest_run_id=uuid4(),
        shares=Decimal(shares),
        market_value_usd=Decimal(market_value_usd) if market_value_usd is not None else None,
        payoff_profile="Long",
        asset_category="EC",
    )
    refresh_funds_current(conn, instrument_id=instrument_id)


def _seed_esop_holding(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    plan_name: str,
    accession: str,
    shares: str,
    percent_of_class: str | None = None,
    plan_trustee_name: str | None = None,
    period_end: date = date(2026, 3, 31),
) -> None:
    """Seed a DEF-14A ESOP / employee-benefit-plan holding via the
    canonical write-through helpers — mirrors what
    ``app.services.def14a_ingest`` does in production for a
    ``holder_role='esop'`` row. Used by #961 esop-slice tests."""
    record_esop_observation(
        conn,
        instrument_id=instrument_id,
        plan_name=plan_name,
        plan_trustee_name=plan_trustee_name,
        plan_trustee_cik=None,
        source_document_id=accession,
        source_accession=accession,
        source_field=None,
        source_url=None,
        filed_at=datetime(period_end.year, period_end.month, 1, tzinfo=UTC),
        period_start=None,
        period_end=period_end,
        ingest_run_id=uuid4(),
        shares=Decimal(shares),
        percent_of_class=Decimal(percent_of_class) if percent_of_class is not None else None,
    )
    refresh_esop_current(conn, instrument_id=instrument_id)


class TestFundsSlice:
    """Funds slice (#919): N-PORT mutual-fund holdings render as a
    memo-overlay slice with ``denominator_basis='institution_subset'``.
    Memo overlay = renders for visibility but does NOT contribute to
    residual / concentration math, because N-PORT fund holdings are
    fund-level detail INSIDE the 13F-HR institutional aggregate (per
    spec; counting them additively would double-count)."""

    def test_funds_slice_renders_with_memo_overlay_basis(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=789_060, symbol="FUND1")
        _seed_outstanding(conn, instrument_id=789_060, shares="1000000000")
        _seed_funds_holding(
            conn,
            instrument_id=789_060,
            fund_series_id="S000004310",
            fund_series_name="Vanguard 500 Index Fund",
            fund_filer_cik="0000036405",
            accession="0001234500-25-000127",
            shares="50000000",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(
            conn,
            symbol="FUND1",
            instrument_id=789_060,
        )

        funds = [s for s in rollup.slices if s.category == "funds"]
        assert len(funds) == 1, "funds slice must surface when N-PORT data exists"
        funds_slice = funds[0]
        assert funds_slice.denominator_basis == "institution_subset"
        assert funds_slice.label == "Mutual funds (N-PORT)"
        assert funds_slice.total_shares == Decimal("50000000")
        assert funds_slice.filer_count == 1
        # Holder identity carries fund_series_name (operator-visible
        # identity per the #919 acceptance), not the trust/manager CIK.
        holder = funds_slice.holders[0]
        assert holder.filer_name == "Vanguard 500 Index Fund"
        assert holder.filer_cik == "0000036405"
        assert holder.winning_source == "nport"
        assert holder.shares == Decimal("50000000")
        assert holder.filer_type is None

    def test_funds_slice_excluded_from_residual_math(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Critical invariant: funds slice is a memo overlay, NOT
        additive in the pie. Residual must equal outstanding minus
        pie-wedge slices (insiders/blockholders/institutions/etfs/
        def14a_unmatched), with funds total NOT subtracted.

        Set up: 1B outstanding + 100M Form 4 insider + 50M N-PORT funds.
        Expected residual = 1B - 100M = 900M (funds NOT subtracted).
        Naive additive math would give 850M.
        """
        conn = ebull_test_conn
        _seed_instrument(conn, iid=789_061, symbol="FUND2")
        _seed_outstanding(conn, instrument_id=789_061, shares="1000000000")
        _seed_form4(
            conn,
            accession="0001234500-25-000128",
            instrument_id=789_061,
            filer_cik="0001234567",
            filer_name="Founder Holder",
            txn_date=date(2026, 2, 1),
            post_transaction_shares="100000000",
        )
        _seed_funds_holding(
            conn,
            instrument_id=789_061,
            fund_series_id="S000004310",
            fund_series_name="Vanguard 500 Index Fund",
            fund_filer_cik="0000036405",
            accession="0001234500-25-000129",
            shares="50000000",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(
            conn,
            symbol="FUND2",
            instrument_id=789_061,
        )

        # Funds slice present.
        funds = [s for s in rollup.slices if s.category == "funds"]
        assert len(funds) == 1
        assert funds[0].total_shares == Decimal("50000000")

        # Residual = outstanding - insiders only. Funds slice NOT
        # subtracted because it's a memo overlay.
        assert rollup.residual.shares == Decimal("900000000")
        assert not rollup.residual.oversubscribed
        # Concentration also excludes funds — sums pie-wedge slices only.
        # 100M / 1B = 0.10
        assert rollup.concentration.pct_outstanding_known == Decimal("0.1")

    def test_no_funds_slice_when_no_nport_data(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Funds slice is omitted entirely (not zero-row) when no
        N-PORT data exists for the instrument. Bucket router uses
        ``if funds_holders:`` so an empty list = no slice in payload.
        """
        conn = ebull_test_conn
        _seed_instrument(conn, iid=789_062, symbol="FUND3")
        _seed_outstanding(conn, instrument_id=789_062, shares="500000000")
        _seed_form4(
            conn,
            accession="0001234500-25-000130",
            instrument_id=789_062,
            filer_cik="0001234568",
            filer_name="Lone Insider",
            txn_date=date(2026, 2, 1),
            post_transaction_shares="10000000",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(
            conn,
            symbol="FUND3",
            instrument_id=789_062,
        )
        funds = [s for s in rollup.slices if s.category == "funds"]
        assert funds == [], "no funds slice when N-PORT empty"

    def test_funds_slice_does_not_affect_coverage_banner(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Coverage banner state machine iterates ``_CATEGORY_ORDER``
        which deliberately excludes ``'funds'`` (memo overlay → no
        universe estimate, no banner contribution). Adding funds data
        must not change the banner state from what it would be without."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=789_063, symbol="FUND4")
        _seed_outstanding(conn, instrument_id=789_063, shares="500000000")
        # No other filings — banner should be unknown_universe baseline.
        rollup_baseline = ownership_rollup.get_ownership_rollup(
            conn,
            symbol="FUND4",
            instrument_id=789_063,
        )
        baseline_state = rollup_baseline.banner.state

        _seed_funds_holding(
            conn,
            instrument_id=789_063,
            fund_series_id="S000004310",
            fund_series_name="Vanguard 500 Index Fund",
            fund_filer_cik="0000036405",
            accession="0001234500-25-000131",
            shares="20000000",
        )
        conn.commit()

        rollup_with_funds = ownership_rollup.get_ownership_rollup(
            conn,
            symbol="FUND4",
            instrument_id=789_063,
        )
        # Funds slice present
        assert any(s.category == "funds" for s in rollup_with_funds.slices)
        # Banner state unchanged — funds doesn't enter the universe-coverage fold.
        assert rollup_with_funds.banner.state == baseline_state

    def test_funds_slice_multiple_series_ranked_by_shares(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Multiple fund series for one issuer rank by shares descending
        in holders — same as every other slice's ``_build_slice``
        ordering."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=789_064, symbol="FUND5")
        _seed_outstanding(conn, instrument_id=789_064, shares="1000000000")
        _seed_funds_holding(
            conn,
            instrument_id=789_064,
            fund_series_id="S000004310",
            fund_series_name="Vanguard 500 Index Fund",
            fund_filer_cik="0000036405",
            accession="0001234500-25-000132",
            shares="40000000",
        )
        _seed_funds_holding(
            conn,
            instrument_id=789_064,
            fund_series_id="S000004311",
            fund_series_name="Vanguard Total Stock Market",
            fund_filer_cik="0000036405",
            accession="0001234500-25-000133",
            shares="60000000",
        )
        _seed_funds_holding(
            conn,
            instrument_id=789_064,
            fund_series_id="S000005000",
            fund_series_name="iShares Core S&P 500",
            fund_filer_cik="0001100663",
            accession="0001234500-25-000134",
            shares="30000000",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(
            conn,
            symbol="FUND5",
            instrument_id=789_064,
        )
        funds_slice = next(s for s in rollup.slices if s.category == "funds")
        assert funds_slice.filer_count == 3
        assert funds_slice.total_shares == Decimal("130000000")
        # Ranked by shares desc.
        names = [h.filer_name for h in funds_slice.holders]
        assert names == [
            "Vanguard Total Stock Market",
            "Vanguard 500 Index Fund",
            "iShares Core S&P 500",
        ]


class TestEsopSlice:
    """ESOP / employee-benefit-plan slice (#961): DEF 14A-disclosed plan
    holdings render as their own memo-overlay slice with
    ``denominator_basis='proxy_disclosure'`` — same basis as
    ``def14a_unmatched`` (SEC Item 403 beneficial-ownership disclosure),
    NOT ``institution_subset``. #843's original spec called for tagging
    ``ownership_funds_current`` rows via a ``plan_trustee_cik =
    fund_filer_cik`` join, but a full-population check of every
    populated ``ownership_esop_current`` row (2026-07-03) found
    ``plan_trustee_cik`` is NULL on all of them — the DEF 14A table
    gives free-text trustee names, never a resolvable CIK, so that join
    can never match. Rendering ESOP as its own slice (this class)
    surfaces the same data without depending on that dead join."""

    def test_esop_slice_renders_with_memo_overlay_basis(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=789_070, symbol="ESOP1")
        _seed_outstanding(conn, instrument_id=789_070, shares="10000000")
        _seed_esop_holding(
            conn,
            instrument_id=789_070,
            plan_name="ESOP1 Employee Stock Ownership Plan",
            accession="0001234500-26-000200",
            shares="500000",
            percent_of_class="5.0000",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(
            conn,
            symbol="ESOP1",
            instrument_id=789_070,
        )

        esop = [s for s in rollup.slices if s.category == "esop"]
        assert len(esop) == 1, "esop slice must surface when DEF 14A ESOP data exists"
        esop_slice = esop[0]
        assert esop_slice.denominator_basis == "proxy_disclosure"
        assert esop_slice.label == "Employee benefit plans (ESOP)"
        assert esop_slice.total_shares == Decimal("500000")
        assert esop_slice.filer_count == 1
        holder = esop_slice.holders[0]
        assert holder.filer_name == "ESOP1 Employee Stock Ownership Plan"
        assert holder.filer_cik is None
        assert holder.winning_source == "def14a"
        assert holder.shares == Decimal("500000")

    def test_esop_slice_excluded_from_residual_math(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Critical invariant: esop slice is a memo overlay, NOT
        additive in the pie. Residual must equal outstanding minus
        pie-wedge slices only (insiders here), with the esop total NOT
        subtracted."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=789_071, symbol="ESOP2")
        _seed_outstanding(conn, instrument_id=789_071, shares="10000000")
        _seed_form4(
            conn,
            accession="0001234500-26-000201",
            instrument_id=789_071,
            filer_cik="0001234569",
            filer_name="Founder Holder",
            txn_date=date(2026, 2, 1),
            post_transaction_shares="1000000",
        )
        _seed_esop_holding(
            conn,
            instrument_id=789_071,
            plan_name="ESOP2 Employee Stock Ownership Plan",
            accession="0001234500-26-000202",
            shares="500000",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(
            conn,
            symbol="ESOP2",
            instrument_id=789_071,
        )

        esop = [s for s in rollup.slices if s.category == "esop"]
        assert len(esop) == 1
        assert esop[0].total_shares == Decimal("500000")

        # Residual = outstanding - insiders only. Esop slice NOT subtracted.
        assert rollup.residual.shares == Decimal("9000000")
        assert not rollup.residual.oversubscribed
        assert rollup.concentration.pct_outstanding_known == Decimal("0.1")

    def test_no_esop_slice_when_no_def14a_esop_data(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Esop slice is omitted entirely (not zero-row) when no DEF
        14A ESOP data exists — ``if esop_holders:`` in the bucket
        router means an empty list yields no slice in the payload."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=789_072, symbol="ESOP3")
        _seed_outstanding(conn, instrument_id=789_072, shares="5000000")
        _seed_form4(
            conn,
            accession="0001234500-26-000203",
            instrument_id=789_072,
            filer_cik="0001234570",
            filer_name="Lone Insider",
            txn_date=date(2026, 2, 1),
            post_transaction_shares="100000",
        )
        conn.commit()

        rollup = ownership_rollup.get_ownership_rollup(
            conn,
            symbol="ESOP3",
            instrument_id=789_072,
        )
        esop = [s for s in rollup.slices if s.category == "esop"]
        assert esop == [], "no esop slice when DEF 14A ESOP data is empty"


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


def _seed_cik(conn: psycopg.Connection[tuple], *, iid: int, cik: str, is_primary: bool = True) -> None:
    conn.execute(
        """
        INSERT INTO external_identifiers (provider, identifier_type, identifier_value, instrument_id, is_primary)
        VALUES ('sec', 'cik', %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (cik, iid, is_primary),
    )


def _seed_cusip(conn: psycopg.Connection[tuple], *, iid: int, cusip: str) -> None:
    conn.execute(
        """
        INSERT INTO external_identifiers (provider, identifier_type, identifier_value, instrument_id, is_primary)
        VALUES ('sec', 'cusip', %s, %s, FALSE)
        ON CONFLICT DO NOTHING
        """,
        (cusip, iid),
    )


class TestDualClassDenominatorDetector:
    """#1646: the multi-class denominator caveat detector — three gates, all
    required (us-gaap denominator, ≥2 CIK siblings, ≥2 distinct CUSIPs)."""

    def test_fires_for_multiclass_with_usgaap_denominator(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=794_601, symbol="ZGOOG")
        _seed_instrument(conn, iid=794_602, symbol="ZGOOGL")
        _seed_cik(conn, iid=794_601, cik="0009990001")
        _seed_cik(conn, iid=794_602, cik="0009990001")
        _seed_cusip(conn, iid=794_601, cusip="ZZ079K107")
        _seed_cusip(conn, iid=794_602, cusip="ZZ079K305")

        caveat = ownership_rollup._detect_dual_class_denominator(
            conn, 794_601, denominator_concept="CommonStockSharesOutstanding"
        )
        assert caveat is not None
        assert caveat.cik == "0009990001"
        assert caveat.sibling_symbols == ("ZGOOG", "ZGOOGL")
        assert "ZGOOG, ZGOOGL" in caveat.note

    def test_gate1_dei_denominator_suppresses_caveat(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A DEI denominator means a single non-dimensional cover value was
        reported (single-class issuer, ETF trust, or .US dual-listing) — not the
        combined class-blind count, so no caveat even with siblings + CUSIPs."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=794_611, symbol="ZDEI1")
        _seed_instrument(conn, iid=794_612, symbol="ZDEI2")
        _seed_cik(conn, iid=794_611, cik="0009990011")
        _seed_cik(conn, iid=794_612, cik="0009990011")
        _seed_cusip(conn, iid=794_611, cusip="ZZDEI1107")
        _seed_cusip(conn, iid=794_612, cusip="ZZDEI2305")

        caveat = ownership_rollup._detect_dual_class_denominator(
            conn, 794_611, denominator_concept="EntityCommonStockSharesOutstanding"
        )
        assert caveat is None

    def test_gate2_single_class_no_sibling_suppresses_caveat(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=794_621, symbol="ZSOLO")
        _seed_cik(conn, iid=794_621, cik="0009990021")
        _seed_cusip(conn, iid=794_621, cusip="ZZSOLO107")

        caveat = ownership_rollup._detect_dual_class_denominator(
            conn, 794_621, denominator_concept="CommonStockSharesOutstanding"
        )
        assert caveat is None

    def test_gate3_shared_cusip_listing_dup_suppresses_caveat(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Two instruments on one CIK but only one distinct CUSIP between them is a
        same-security .US listing dup, not separate share classes — no caveat."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=794_631, symbol="ZDUP")
        _seed_instrument(conn, iid=794_632, symbol="ZDUP.US")
        _seed_cik(conn, iid=794_631, cik="0009990031")
        _seed_cik(conn, iid=794_632, cik="0009990031")
        # Only one of the two carries a CUSIP → <2 distinct CUSIPs across siblings.
        _seed_cusip(conn, iid=794_632, cusip="ZZDUP1107")

        caveat = ownership_rollup._detect_dual_class_denominator(
            conn, 794_631, denominator_concept="CommonStockSharesOutstanding"
        )
        assert caveat is None

    def test_gate3_two_cusips_on_one_instrument_does_not_pass(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """One instrument carrying two historical CUSIPs + a CUSIP-less sibling must
        NOT pass gate 3 — it is not two separate share-class securities (Codex LOW)."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=794_641, symbol="ZTWO")
        _seed_instrument(conn, iid=794_642, symbol="ZTWO.US")
        _seed_cik(conn, iid=794_641, cik="0009990041")
        _seed_cik(conn, iid=794_642, cik="0009990041")
        # Two distinct CUSIP values, but BOTH on the same instrument; sibling has none.
        _seed_cusip(conn, iid=794_641, cusip="ZZTWO1107")
        _seed_cusip(conn, iid=794_641, cusip="ZZTWO1305")

        caveat = ownership_rollup._detect_dual_class_denominator(
            conn, 794_641, denominator_concept="CommonStockSharesOutstanding"
        )
        assert caveat is None

    def test_gate2_non_tradable_sibling_does_not_count(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A delisted (non-tradable) instrument sharing the CIK does not manufacture
        a spurious sibling — gate 2 counts only live, primary-CIK instruments (Codex MED)."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=794_651, symbol="ZLIVE")
        _seed_instrument(conn, iid=794_652, symbol="ZDEAD")
        conn.execute("UPDATE instruments SET is_tradable = FALSE WHERE instrument_id = 794652")
        _seed_cik(conn, iid=794_651, cik="0009990051")
        _seed_cik(conn, iid=794_652, cik="0009990051")
        _seed_cusip(conn, iid=794_651, cusip="ZZLIVE107")
        _seed_cusip(conn, iid=794_652, cusip="ZZDEAD305")

        caveat = ownership_rollup._detect_dual_class_denominator(
            conn, 794_651, denominator_concept="CommonStockSharesOutstanding"
        )
        assert caveat is None

    def test_gate2_non_primary_cik_sibling_does_not_count(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """An instrument carrying the CIK only as a non-primary (historical) mapping
        is not a current share-class sibling — gate 2 requires is_primary (Codex MED)."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=794_661, symbol="ZPRIM")
        _seed_instrument(conn, iid=794_662, symbol="ZHIST")
        _seed_cik(conn, iid=794_661, cik="0009990061", is_primary=True)
        _seed_cik(conn, iid=794_662, cik="0009990061", is_primary=False)
        _seed_cusip(conn, iid=794_661, cusip="ZZPRIM107")
        _seed_cusip(conn, iid=794_662, cusip="ZZHIST305")

        caveat = ownership_rollup._detect_dual_class_denominator(
            conn, 794_661, denominator_concept="CommonStockSharesOutstanding"
        )
        assert caveat is None


def _seed_class_shares(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    period_end: date,
    shares: str,
    class_member: str = "CommonClassA",
    source_cik: str = "0001652044",
    source_adsh: str = "0001652044-25-000014",
) -> None:
    """Seed one ``instrument_class_shares_outstanding`` row (#788)."""
    conn.execute(
        """
        INSERT INTO instrument_class_shares_outstanding (
            instrument_id, period_end, shares, class_member, source_cik,
            source_adsh, source_form_type, source_fsds_qtr, source_filed_at,
            resolution_method, parser_version
        ) VALUES (%s, %s, %s, %s, %s, %s, '10-K', '2025q1', %s, 'curated',
                  'fsds_class_shares_v1')
        ON CONFLICT (instrument_id, period_end) DO NOTHING
        """,
        (instrument_id, period_end, Decimal(shares), class_member, source_cik, source_adsh, period_end),
    )


class TestPerClassDenominator:
    """#788 read-path swap: a verified FSDS per-class share count replaces the
    combined denominator (GOOGL ÷ Class A, not ÷ combined) and supersedes the
    #1646 caveat — only behind the fail-closed guards. Periods are relative to
    ``date.today()`` so the freshness guard (#1581 548-day bound, clocked off the
    snapshot transaction time) is exercised deterministically, not time-bombed."""

    _IID = 788_500
    # Both the combined denominator and the fresh class row must clear the 548-day
    # staleness bound; 60 days is robustly fresh. The stale case is well past 548.
    _FRESH = date.today() - timedelta(days=60)
    _STALE = date.today() - timedelta(days=900)

    def _setup(self, conn: psycopg.Connection[tuple], *, class_shares: str | None, class_period: date) -> None:
        _seed_instrument(conn, iid=self._IID, symbol="GOOGL")
        # Combined all-class count at a fresh period (so the rollup is not no_data).
        _seed_outstanding(conn, instrument_id=self._IID, shares="12211000000", period_end=self._FRESH)
        # One institutional pie-wedge holder (2.541B → 43.5% of Class A, 20.8% of combined).
        _seed_inst_holding(
            conn,
            accession="0001000000-25-000001",
            instrument_id=self._IID,
            filer_cik="0000102909",
            filer_name="Some Manager",
            filer_type="INV",
            period_of_report=self._FRESH,
            shares="2541000000",
        )
        if class_shares is not None:
            _seed_class_shares(conn, instrument_id=self._IID, period_end=class_period, shares=class_shares)
        conn.commit()

    def test_per_class_denominator_applied(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        self._setup(conn, class_shares="5835000000", class_period=self._FRESH)
        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="GOOGL", instrument_id=self._IID)
        assert rollup.per_class_denominator is not None
        assert rollup.dual_class_denominator is None
        assert rollup.per_class_denominator.per_class_shares == Decimal("5835000000")
        assert rollup.per_class_denominator.combined_shares == Decimal("12211000000")
        # Denominator + source swapped to the per-class FSDS row.
        assert rollup.shares_outstanding == Decimal("5835000000")
        assert rollup.shares_outstanding_as_of == self._FRESH
        assert rollup.shares_outstanding_source.accession_number == "0001652044-25-000014"
        # The institutions slice now divides by Class A → ~43.5%, not ~20.8%.
        inst = next(s for s in rollup.slices if s.category == "institutions")
        assert inst.pct_outstanding > Decimal("0.43")
        assert inst.pct_outstanding < Decimal("0.44")

    def test_stale_class_period_falls_back(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        # Class row > 548 days old → freshness guard fails → keep the combined caveat path.
        self._setup(conn, class_shares="5835000000", class_period=self._STALE)
        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="GOOGL", instrument_id=self._IID)
        assert rollup.per_class_denominator is None
        assert rollup.shares_outstanding == Decimal("12211000000")  # combined preserved

    def test_too_small_class_falls_back(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        # Class 1B < the 2.541B holder → holdings-plausibility guard fails (the
        # %-inflating direction) → fall back to the combined denominator.
        self._setup(conn, class_shares="1000000000", class_period=self._FRESH)
        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="GOOGL", instrument_id=self._IID)
        assert rollup.per_class_denominator is None
        assert rollup.shares_outstanding == Decimal("12211000000")

    def test_no_class_row_no_swap(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        self._setup(conn, class_shares=None, class_period=self._FRESH)
        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="GOOGL", instrument_id=self._IID)
        assert rollup.per_class_denominator is None
        assert rollup.shares_outstanding == Decimal("12211000000")


# --- denominator cross-check SQL readers (#1647 pt5) -------------------------
# Pins the novel SQL the pure `_classify_cross_check` tests can't reach
# (tests/test_denominator_cross_check.py covers the band/subset logic). One DB
# test per new mechanism per the repo's test-tiering rule.


def test_read_shares_outstanding_near_picks_nearest_period(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """`_read_shares_outstanding_near` returns the row whose period_end is NEAREST the
    target — NOT the latest (Codex ckpt-1 HIGH: never compare different instants)."""
    conn = ebull_test_conn
    _seed_instrument(conn, iid=1_647_001, symbol="NEARX")
    for pe, val in ((date(2024, 12, 31), "1000"), (date(2026, 3, 31), "2000")):
        conn.execute(
            """
            INSERT INTO financial_facts_raw (
                instrument_id, taxonomy, concept, unit, period_end, val,
                form_type, filed_date, accession_number, fiscal_year, fiscal_period
            ) VALUES (%s, 'us-gaap', 'CommonStockSharesOutstanding', 'shares', %s, %s,
                      '10-K', %s, %s, %s, 'Q4')
            """,
            (1_647_001, pe, Decimal(val), pe, f"NEAR-{pe}", pe.year),
        )
    near = ownership_rollup._read_shares_outstanding_near(
        conn,
        1_647_001,
        taxonomy="us-gaap",
        concept="CommonStockSharesOutstanding",
        near_period=date(2026, 3, 20),
    )
    assert near is not None
    val, pe = near
    assert pe == date(2026, 3, 31)  # nearest 2026-03-20, NOT the 2024 row
    assert val == Decimal("2000")
    # A concept not on file → None (drives the cross-check `unavailable` path).
    assert (
        ownership_rollup._read_shares_outstanding_near(
            conn,
            1_647_001,
            taxonomy="dei",
            concept="EntityCommonStockSharesOutstanding",
            near_period=date(2026, 3, 20),
        )
        is None
    )


def test_sum_sibling_class_shares_filters_to_period(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """`_sum_sibling_class_shares` sums sibling per-class FSDS counts AT one period_end —
    a sibling whose latest FSDS row is an OLDER quarter must not be mixed in at a stale
    figure (Codex ckpt-2 MED)."""
    conn = ebull_test_conn
    cik = "0001647999"
    target = date(2024, 12, 31)
    for iid, sym, pe, sh in (
        (1_647_010, "CLSA", target, "5835000000"),  # ClassA at target
        (1_647_011, "CLSC", target, "5515000000"),  # ClassC at target
        (1_647_012, "CLSLAG", date(2023, 12, 31), "9999000000"),  # lagging sibling, OLDER period
    ):
        _seed_instrument(conn, iid=iid, symbol=sym)
        conn.execute(
            """
            INSERT INTO instrument_class_shares_outstanding (
                instrument_id, period_end, shares, class_member, source_cik,
                source_adsh, source_form_type, source_fsds_qtr, source_filed_at,
                resolution_method, parser_version
            ) VALUES (%s, %s, %s, 'CommonClassX', %s, %s, '10-K', '2025q1', %s,
                      'curated', 'fsds_class_shares_v1')
            """,
            (iid, pe, Decimal(sh), cik, f"ADSH-{iid}", pe),
        )
    # Only the two siblings AT the target period sum; the lagging 2023 sibling drops out.
    assert ownership_rollup._sum_sibling_class_shares(conn, cik, target) == Decimal("11350000000")
    # A period with no rows → None.
    assert ownership_rollup._sum_sibling_class_shares(conn, cik, date(2020, 1, 1)) is None
