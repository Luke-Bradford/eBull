"""Dual-read parity tests for the rollup endpoint (#840.E).

Per Codex plan-review #5: before flipping
``EBULL_OWNERSHIP_ROLLUP_FROM_CURRENT=1`` in prod, both code paths
must produce identical ``OwnershipRollup`` output for fixture data.
This test seeds equivalent state in BOTH the legacy typed tables AND
the new ``ownership_*_current`` tables, calls
``get_ownership_rollup`` with the flag OFF then ON, and asserts
slice-by-slice parity (same categories, same total_shares per slice,
same provenance fields, same coverage banner).

When this test passes consistently across the AAPL + GME + Vanguard
fixtures we ship, the operator can flip the flag with confidence.
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import psycopg
import psycopg.rows
import pytest

from app.services import ownership_rollup
from app.services.ownership_observations import (
    record_blockholder_observation,
    record_insider_observation,
    record_institution_observation,
    record_treasury_observation,
    refresh_blockholders_current,
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
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _seed_outstanding(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    shares: str,
) -> None:
    period_end = date(2026, 3, 31)
    conn.execute(
        """
        INSERT INTO financial_periods (
            instrument_id, period_end_date, period_type, fiscal_year,
            fiscal_quarter, source, source_ref, reported_currency,
            is_restated, is_derived, normalization_status,
            treasury_shares, filed_date, superseded_at
        ) VALUES (%s, %s, 'Q4', %s, 4, 'sec_xbrl', %s, 'USD',
                  FALSE, FALSE, 'normalized', NULL, %s, NULL)
        ON CONFLICT DO NOTHING
        """,
        (
            instrument_id,
            period_end,
            period_end.year,
            f"OUTSTANDING-{instrument_id}",
            datetime(period_end.year, period_end.month, 1, tzinfo=UTC),
        ),
    )
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
            f"OUTSTANDING-{instrument_id}",
            period_end.year,
        ),
    )


def _seed_legacy_form4_insider(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    cik: str,
    name: str,
    txn_date: date,
    shares: str,
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
            txn_date, txn_code, shares, post_transaction_shares, is_derivative,
            direct_indirect
        ) VALUES (%s, 1, %s, %s, %s, %s, 'P', 100, %s, FALSE, 'D')
        """,
        (accession, instrument_id, cik, name, txn_date, Decimal(shares)),
    )


def _seed_new_form4_insider(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    cik: str,
    name: str,
    txn_date: date,
    shares: str,
) -> None:
    record_insider_observation(
        conn,
        instrument_id=instrument_id,
        holder_cik=cik,
        holder_name=name,
        ownership_nature="direct",
        source="form4",
        source_document_id=accession,
        source_accession=accession,
        source_field=None,
        source_url=None,
        filed_at=datetime.combine(txn_date, datetime.min.time(), tzinfo=UTC),
        period_start=None,
        period_end=txn_date,
        ingest_run_id=uuid4(),
        shares=Decimal(shares),
    )
    refresh_insiders_current(conn, instrument_id=instrument_id)


def _seed_legacy_13f_holding(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    cik: str,
    name: str,
    filer_type: str,
    period_end: date,
    shares: str,
) -> None:
    conn.execute(
        """
        INSERT INTO institutional_filers (cik, name, filer_type)
        VALUES (%s, %s, %s)
        ON CONFLICT (cik) DO UPDATE SET filer_type = EXCLUDED.filer_type
        """,
        (cik, name, filer_type),
    )
    with conn.cursor() as cur:
        cur.execute("SELECT filer_id FROM institutional_filers WHERE cik = %s", (cik,))
        row = cur.fetchone()
    assert row is not None
    filer_id = int(row[0])
    conn.execute(
        """
        INSERT INTO institutional_holdings (
            filer_id, instrument_id, accession_number, period_of_report,
            shares, voting_authority, filed_at
        ) VALUES (%s, %s, %s, %s, %s, 'SOLE', %s)
        """,
        (
            filer_id,
            instrument_id,
            accession,
            period_end,
            Decimal(shares),
            datetime.combine(period_end, datetime.min.time(), tzinfo=UTC),
        ),
    )


def _seed_new_13f_holding(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    cik: str,
    name: str,
    filer_type: str,
    period_end: date,
    shares: str,
) -> None:
    record_institution_observation(
        conn,
        instrument_id=instrument_id,
        filer_cik=cik,
        filer_name=name,
        filer_type=filer_type,
        ownership_nature="economic",
        source="13f",
        source_document_id=accession,
        source_accession=accession,
        source_field=None,
        source_url=None,
        filed_at=datetime.combine(period_end, datetime.min.time(), tzinfo=UTC),
        period_start=None,
        period_end=period_end,
        ingest_run_id=uuid4(),
        shares=Decimal(shares),
        market_value_usd=None,
        voting_authority="SOLE",
    )
    refresh_institutions_current(conn, instrument_id=instrument_id)


def _seed_legacy_13d(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    cik: str,
    name: str,
    submission_type: str,
    aggregate: str,
    filed_at: datetime,
) -> None:
    conn.execute(
        "INSERT INTO blockholder_filers (cik, name) VALUES (%s, %s) ON CONFLICT (cik) DO NOTHING",
        (cik, name),
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
               %s, FALSE, %s, %s, %s
        FROM blockholder_filers WHERE cik = %s
        """,
        (
            accession,
            submission_type,
            status,
            instrument_id,
            cik,
            name,
            Decimal(aggregate),
            filed_at,
            cik,
        ),
    )


def _seed_new_13d(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    cik: str,
    name: str,
    submission_type: str,
    aggregate: str,
    filed_at: datetime,
) -> None:
    source = "13d" if submission_type.startswith("SCHEDULE 13D") else "13g"
    status = "active" if source == "13d" else "passive"
    record_blockholder_observation(
        conn,
        instrument_id=instrument_id,
        reporter_cik=cik,
        reporter_name=name,
        ownership_nature="beneficial",
        submission_type=submission_type,
        status_flag=status,
        source=source,  # type: ignore[arg-type]
        source_document_id=accession,
        source_accession=accession,
        source_field=None,
        source_url=None,
        filed_at=filed_at,
        period_start=None,
        period_end=filed_at.date(),
        ingest_run_id=uuid4(),
        aggregate_amount_owned=Decimal(aggregate),
        percent_of_class=None,
    )
    refresh_blockholders_current(conn, instrument_id=instrument_id)


# ---------------------------------------------------------------------------
# Parity tests
# ---------------------------------------------------------------------------


class TestDualReadParity:
    """Acceptance contract for #840.E: legacy + new read paths
    produce identical ``OwnershipRollup`` for the same logical
    fixture. Operator flips ``EBULL_OWNERSHIP_ROLLUP_FROM_CURRENT=1``
    only after this passes consistently."""

    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        monkeypatch: pytest.MonkeyPatch,
    ) -> psycopg.Connection[tuple]:
        # Default the env var OFF for each test; tests explicitly flip it.
        monkeypatch.delenv("EBULL_OWNERSHIP_ROLLUP_FROM_CURRENT", raising=False)
        conn = ebull_test_conn
        _seed_instrument(conn, iid=842_001, symbol="AAPL")
        _seed_outstanding(conn, instrument_id=842_001, shares="15500000000")
        return conn

    def _seed_aapl_fixture(self, conn: psycopg.Connection[tuple]) -> None:
        # Same logical state in both legacy and new tables.
        _seed_legacy_form4_insider(
            conn,
            instrument_id=842_001,
            accession="ACC-F4-COOK",
            cik="0001214156",
            name="Tim Cook",
            txn_date=date(2026, 1, 21),
            shares="3300000",
        )
        _seed_new_form4_insider(
            conn,
            instrument_id=842_001,
            accession="ACC-F4-COOK",
            cik="0001214156",
            name="Tim Cook",
            txn_date=date(2026, 1, 21),
            shares="3300000",
        )
        _seed_legacy_13f_holding(
            conn,
            instrument_id=842_001,
            accession="ACC-VG-Q1",
            cik="0000102909",
            name="Vanguard Group Inc",
            filer_type="ETF",
            period_end=date(2026, 3, 31),
            shares="1500000000",
        )
        _seed_new_13f_holding(
            conn,
            instrument_id=842_001,
            accession="ACC-VG-Q1",
            cik="0000102909",
            name="Vanguard Group Inc",
            filer_type="ETF",
            period_end=date(2026, 3, 31),
            shares="1500000000",
        )
        _seed_legacy_13d(
            conn,
            instrument_id=842_001,
            accession="ACC-13D-X",
            cik="0009000001",
            name="Activist Holder LLC",
            submission_type="SCHEDULE 13D",
            aggregate="800000000",
            filed_at=datetime(2026, 2, 1, tzinfo=UTC),
        )
        _seed_new_13d(
            conn,
            instrument_id=842_001,
            accession="ACC-13D-X",
            cik="0009000001",
            name="Activist Holder LLC",
            submission_type="SCHEDULE 13D",
            aggregate="800000000",
            filed_at=datetime(2026, 2, 1, tzinfo=UTC),
        )
        # Treasury — both paths.
        conn.execute(
            """
            INSERT INTO financial_periods (
                instrument_id, period_end_date, period_type, fiscal_year,
                fiscal_quarter, source, source_ref, reported_currency,
                is_restated, is_derived, normalization_status,
                treasury_shares, filed_date, superseded_at
            ) VALUES (%s, '2026-03-31', 'Q1', 2026, 1, 'sec_xbrl', 'TREAS-AAPL',
                      'USD', FALSE, FALSE, 'normalized',
                      0, '2026-04-15 00:00:00+00', NULL)
            ON CONFLICT DO NOTHING
            """,
            (842_001,),
        )
        record_treasury_observation(
            conn,
            instrument_id=842_001,
            source="xbrl_dei",
            source_document_id="TREAS-AAPL",
            source_accession=None,
            source_field="TreasuryStockShares",
            source_url=None,
            filed_at=datetime(2026, 4, 15, tzinfo=UTC),
            period_start=None,
            period_end=date(2026, 3, 31),
            ingest_run_id=uuid4(),
            treasury_shares=Decimal("0"),
        )
        refresh_treasury_current(conn, instrument_id=842_001)
        conn.commit()

    def _slice_signature(self, rollup: ownership_rollup.OwnershipRollup) -> dict[str, tuple[int, Decimal]]:
        """Compact rollup signature for parity assertion: per-category
        ``(filer_count, total_shares)``. Holder-level provenance
        differs between paths (synthetic source_row_id, slightly
        different accession-resolution timing) so the comparison
        focuses on what the chart actually renders."""
        return {s.category: (s.filer_count, s.total_shares) for s in rollup.slices}

    def test_aapl_fixture_parity(
        self,
        _setup: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        conn = _setup
        self._seed_aapl_fixture(conn)

        monkeypatch.delenv("EBULL_OWNERSHIP_ROLLUP_FROM_CURRENT", raising=False)
        legacy = ownership_rollup.get_ownership_rollup(conn, symbol="AAPL", instrument_id=842_001)

        monkeypatch.setenv("EBULL_OWNERSHIP_ROLLUP_FROM_CURRENT", "1")
        new = ownership_rollup.get_ownership_rollup(conn, symbol="AAPL", instrument_id=842_001)

        # Per-category signature parity.
        assert self._slice_signature(legacy) == self._slice_signature(new)

        # Headline fields parity.
        assert legacy.shares_outstanding == new.shares_outstanding
        assert legacy.treasury_shares == new.treasury_shares
        assert legacy.residual.shares == new.residual.shares
        assert legacy.coverage.state == new.coverage.state

    def test_dual_nature_for_same_holder_under_flag_on(
        self,
        _setup: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Codex pre-push review for #840.E: when reading from
        ``_current``, a holder with ``direct`` + ``indirect`` rows
        must surface BOTH in the rollup. Earlier shape collapsed
        them via identity-key-only dedup. The fix added
        ``ownership_nature`` to the dedup identity key.

        The legacy rollup path doesn't surface this case (its SQL
        only reads direct Form 4s) so this test is flag-ON only —
        no parity assertion."""
        conn = _setup
        cik = "0001214156"
        run_id = uuid4()
        period = date(2026, 1, 21)
        # Direct + indirect Form 4 rows in _current.
        record_insider_observation(
            conn,
            instrument_id=842_001,
            holder_cik=cik,
            holder_name="Officer A",
            ownership_nature="direct",
            source="form4",
            source_document_id="ACC-DIRECT",
            source_accession="ACC-DIRECT",
            source_field=None,
            source_url=None,
            filed_at=datetime.combine(period, datetime.min.time(), tzinfo=UTC),
            period_start=None,
            period_end=period,
            ingest_run_id=run_id,
            shares=Decimal("3300000"),
        )
        record_insider_observation(
            conn,
            instrument_id=842_001,
            holder_cik=cik,
            holder_name="Officer A",
            ownership_nature="indirect",
            source="form4",
            source_document_id="ACC-INDIRECT",
            source_accession="ACC-INDIRECT",
            source_field=None,
            source_url=None,
            filed_at=datetime.combine(period, datetime.min.time(), tzinfo=UTC),
            period_start=None,
            period_end=period,
            ingest_run_id=run_id,
            shares=Decimal("500000"),
        )
        refresh_insiders_current(conn, instrument_id=842_001)
        conn.commit()

        monkeypatch.setenv("EBULL_OWNERSHIP_ROLLUP_FROM_CURRENT", "1")
        rollup = ownership_rollup.get_ownership_rollup(conn, symbol="AAPL", instrument_id=842_001)

        # Both natures surface in the insiders slice; total_shares
        # carries the SUM of direct + indirect (3.3M + 0.5M = 3.8M).
        insiders = [s for s in rollup.slices if s.category == "insiders"]
        assert len(insiders) == 1
        assert insiders[0].filer_count == 2
        assert insiders[0].total_shares == Decimal("3800000")
        # And the holders list carries two distinct entries — one per
        # nature — since the dedup identity key now includes the
        # ownership_nature axis.
        assert len(insiders[0].holders) == 2

    def test_empty_fixture_parity(
        self,
        _setup: psycopg.Connection[tuple],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No holders seeded — both paths return empty slices and
        residual = shares_outstanding."""
        conn = _setup
        conn.commit()

        monkeypatch.delenv("EBULL_OWNERSHIP_ROLLUP_FROM_CURRENT", raising=False)
        legacy = ownership_rollup.get_ownership_rollup(conn, symbol="AAPL", instrument_id=842_001)

        monkeypatch.setenv("EBULL_OWNERSHIP_ROLLUP_FROM_CURRENT", "1")
        new = ownership_rollup.get_ownership_rollup(conn, symbol="AAPL", instrument_id=842_001)

        assert legacy.slices == new.slices
        assert legacy.residual.shares == new.residual.shares


# ---------------------------------------------------------------------------
# Feature-flag default OFF
# ---------------------------------------------------------------------------


class TestFeatureFlagDefaults:
    def test_default_is_off(self) -> None:
        # No env var → False.
        os.environ.pop("EBULL_OWNERSHIP_ROLLUP_FROM_CURRENT", None)
        assert ownership_rollup._read_from_current_enabled() is False

    @pytest.mark.parametrize("val", ["1", "true", "TRUE", "Yes", "on"])
    def test_truthy_strings_enable(self, val: str, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("EBULL_OWNERSHIP_ROLLUP_FROM_CURRENT", val)
        assert ownership_rollup._read_from_current_enabled() is True

    @pytest.mark.parametrize("val", ["", "0", "false", "no", "off", "disabled"])
    def test_falsy_strings_disable(self, val: str, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("EBULL_OWNERSHIP_ROLLUP_FROM_CURRENT", val)
        assert ownership_rollup._read_from_current_enabled() is False
