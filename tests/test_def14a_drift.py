"""Integration tests for the DEF 14A drift detector (#769 PR 3).

Tests run against the real ``ebull_test`` DB so the cross-table
JOINs (def14a_beneficial_holdings × insider_transactions ×
insider_initial_holdings) exercise actual SQL semantics including
DISTINCT ON and the ILIKE name match.

Each scenario seeds the inputs (DEF 14A holders + matching Form 4
or Form 3 rows) and asserts the alert table contents.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import psycopg
import psycopg.rows
import pytest

from app.services.def14a_drift import (
    CRITICAL_THRESHOLD,
    WARNING_THRESHOLD,
    detect_drift,
    iter_alerts,
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


def _seed_def14a_holder(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    holder_name: str,
    shares: str | None,
    issuer_cik: str = "0000320193",
    holder_role: str | None = "officer",
    as_of: date = date(2026, 3, 1),
    percent: str = "5.5",
) -> None:
    conn.execute(
        """
        INSERT INTO def14a_beneficial_holdings (
            instrument_id, accession_number, issuer_cik,
            holder_name, holder_role, shares, percent_of_class, as_of_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            instrument_id,
            accession,
            issuer_cik,
            holder_name,
            holder_role,
            Decimal(shares) if shares is not None else None,
            Decimal(percent),
            as_of,
        ),
    )


def _seed_insider_filing(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    issuer_cik: str = "0000320193",
) -> None:
    conn.execute(
        """
        INSERT INTO insider_filings (accession_number, instrument_id, document_type, issuer_cik)
        VALUES (%s, %s, '4', %s)
        ON CONFLICT (accession_number) DO NOTHING
        """,
        (accession, instrument_id, issuer_cik),
    )


def _seed_form4_txn(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    filer_cik: str,
    filer_name: str,
    txn_date: date,
    post_transaction_shares: str,
    txn_row_num: int = 1,
) -> None:
    """Seed both insider_filings (parent) and insider_transactions
    (child). Mirrors the schema constraints exactly."""
    _seed_insider_filing(conn, accession=accession, instrument_id=instrument_id)
    conn.execute(
        """
        INSERT INTO insider_transactions (
            accession_number, txn_row_num, instrument_id, filer_cik, filer_name,
            txn_date, txn_code, shares, post_transaction_shares, is_derivative
        ) VALUES (%s, %s, %s, %s, %s, %s, 'P', 100, %s, FALSE)
        """,
        (
            accession,
            txn_row_num,
            instrument_id,
            filer_cik,
            filer_name,
            txn_date,
            Decimal(post_transaction_shares),
        ),
    )


def _seed_form3_baseline(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    filer_cik: str,
    filer_name: str,
    shares: str,
    as_of_date: date = date(2025, 1, 15),
    row_num: int = 1,
) -> None:
    conn.execute(
        """
        INSERT INTO insider_initial_holdings (
            accession_number, row_num, instrument_id, filer_cik, filer_name,
            security_title, is_derivative, direct_indirect, shares, as_of_date
        ) VALUES (%s, %s, %s, %s, %s, 'Common Stock', FALSE, 'D', %s, %s)
        """,
        (
            accession,
            row_num,
            instrument_id,
            filer_cik,
            filer_name,
            Decimal(shares),
            as_of_date,
        ),
    )


# ---------------------------------------------------------------------------
# Happy-path drift outcomes
# ---------------------------------------------------------------------------


class TestDriftOutcomes:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=769_300, symbol="AAPL")
        conn.commit()
        return conn

    def test_clean_reconciliation_emits_no_alert(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """DEF 14A holder shares match Form 4 cumulative within 5%
        — no alert row written."""
        conn = _setup
        _seed_def14a_holder(
            conn,
            instrument_id=769_300,
            accession="DEF-25-001",
            holder_name="John Doe",
            shares="1000000",
        )
        _seed_form4_txn(
            conn,
            accession="F4-25-001",
            instrument_id=769_300,
            filer_cik="0001100001",
            filer_name="John Doe",
            txn_date=date(2026, 2, 15),
            post_transaction_shares="1010000",  # 1% drift, well under WARNING
        )
        conn.commit()

        report = detect_drift(conn)
        conn.commit()

        assert report.holders_evaluated == 1
        assert report.alerts_emitted == 0

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM def14a_drift_alerts")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 0

    def test_5pct_drift_emits_warning(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        _seed_def14a_holder(
            conn,
            instrument_id=769_300,
            accession="DEF-25-002",
            holder_name="Jane Smith",
            shares="1000000",
        )
        # 8% drift (>5%, <25%) -> warning.
        _seed_form4_txn(
            conn,
            accession="F4-25-002",
            instrument_id=769_300,
            filer_cik="0001100002",
            filer_name="Jane Smith",
            txn_date=date(2026, 2, 15),
            post_transaction_shares="920000",
        )
        conn.commit()

        report = detect_drift(conn)
        conn.commit()

        assert report.alerts_emitted == 1
        assert report.alerts_by_severity == {"info": 0, "warning": 1, "critical": 0}

        alerts = list(iter_alerts(conn, instrument_id=769_300))
        assert len(alerts) == 1
        a = alerts[0]
        assert a["severity"] == "warning"
        assert a["matched_filer_cik"] == "0001100002"
        assert a["def14a_shares"] == Decimal("1000000")
        assert a["form4_cumulative"] == Decimal("920000")
        assert a["drift_pct"] is not None
        # 80,000 / 1,000,000 = 0.08
        assert a["drift_pct"] >= WARNING_THRESHOLD
        assert a["drift_pct"] < CRITICAL_THRESHOLD

    def test_30pct_drift_emits_critical(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        _seed_def14a_holder(
            conn,
            instrument_id=769_300,
            accession="DEF-25-003",
            holder_name="Activist Holder",
            shares="2000000",
        )
        # 30% drift -> critical.
        _seed_form4_txn(
            conn,
            accession="F4-25-003",
            instrument_id=769_300,
            filer_cik="0001100003",
            filer_name="Activist Holder",
            txn_date=date(2026, 2, 15),
            post_transaction_shares="1400000",
        )
        conn.commit()

        report = detect_drift(conn)
        conn.commit()

        assert report.alerts_by_severity["critical"] == 1
        alerts = list(iter_alerts(conn, severity="critical"))
        assert alerts[0]["drift_pct"] >= CRITICAL_THRESHOLD

    def test_no_form4_match_emits_info_severity(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """A DEF 14A holder with no matching Form 4 / Form 3 row
        emits an info-severity alert so the operator sees the
        coverage gap."""
        conn = _setup
        _seed_def14a_holder(
            conn,
            instrument_id=769_300,
            accession="DEF-25-004",
            holder_name="Phantom Officer",
            shares="500000",
        )
        # No Form 4 or Form 3 row for "Phantom Officer".
        conn.commit()

        report = detect_drift(conn)
        conn.commit()

        assert report.alerts_emitted == 1
        assert report.alerts_by_severity == {"info": 1, "warning": 0, "critical": 0}
        alerts = list(iter_alerts(conn, instrument_id=769_300))
        assert alerts[0]["severity"] == "info"
        assert alerts[0]["matched_filer_cik"] is None
        assert alerts[0]["form4_cumulative"] is None

    def test_form3_baseline_used_when_no_form4_exists(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        """Officer with only a Form 3 baseline (never traded) — the
        detector falls back to insider_initial_holdings.shares."""
        conn = _setup
        _seed_def14a_holder(
            conn,
            instrument_id=769_300,
            accession="DEF-25-005",
            holder_name="Quiet Officer",
            shares="100000",
        )
        _seed_form3_baseline(
            conn,
            accession="F3-25-005",
            instrument_id=769_300,
            filer_cik="0001100005",
            filer_name="Quiet Officer",
            shares="100000",  # exact match
        )
        conn.commit()

        report = detect_drift(conn)
        conn.commit()

        # Exact match — no drift, no alert.
        assert report.alerts_emitted == 0

    def test_form3_baseline_drift_emits_warning(
        self,
        _setup: psycopg.Connection[tuple],
    ) -> None:
        conn = _setup
        _seed_def14a_holder(
            conn,
            instrument_id=769_300,
            accession="DEF-25-006",
            holder_name="Drifty Officer",
            shares="100000",
        )
        _seed_form3_baseline(
            conn,
            accession="F3-25-006",
            instrument_id=769_300,
            filer_cik="0001100006",
            filer_name="Drifty Officer",
            shares="80000",  # 20% drift
        )
        conn.commit()

        detect_drift(conn)
        conn.commit()

        alerts = list(iter_alerts(conn, instrument_id=769_300))
        assert len(alerts) == 1
        assert alerts[0]["severity"] == "warning"
        assert alerts[0]["form4_cumulative"] == Decimal("80000")


# ---------------------------------------------------------------------------
# Match heuristics + idempotency
# ---------------------------------------------------------------------------


class TestMatchHeuristics:
    def test_role_suffix_in_def14a_name_still_matches_form4(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """``"John Doe, CEO"`` in DEF 14A matches a Form 4 filer
        named ``"John Doe"`` — the matcher strips role suffixes."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=769_310, symbol="A")
        _seed_def14a_holder(
            conn,
            instrument_id=769_310,
            accession="DEF-25-010",
            holder_name="John Doe, CEO",
            shares="500000",
        )
        _seed_form4_txn(
            conn,
            accession="F4-25-010",
            instrument_id=769_310,
            filer_cik="0001100010",
            filer_name="John Doe",
            txn_date=date(2026, 2, 15),
            post_transaction_shares="500000",
        )
        conn.commit()

        report = detect_drift(conn)
        conn.commit()

        # Exact match — no alert.
        assert report.alerts_emitted == 0
        assert report.holders_evaluated == 1

    def test_distinct_on_picks_latest_def14a_per_holder(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Two DEF 14A snapshots for the same holder — the detector
        evaluates only the latest by as_of_date."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=769_320, symbol="A")
        _seed_def14a_holder(
            conn,
            instrument_id=769_320,
            accession="DEF-25-020",
            holder_name="John Doe",
            shares="800000",
            as_of=date(2024, 3, 1),  # older
        )
        _seed_def14a_holder(
            conn,
            instrument_id=769_320,
            accession="DEF-25-021",
            holder_name="John Doe",
            shares="1000000",
            as_of=date(2026, 3, 1),  # latest — picked
        )
        _seed_form4_txn(
            conn,
            accession="F4-25-020",
            instrument_id=769_320,
            filer_cik="0001100020",
            filer_name="John Doe",
            txn_date=date(2026, 2, 15),
            post_transaction_shares="1000000",
        )
        conn.commit()

        report = detect_drift(conn)
        conn.commit()

        # Latest DEF 14A reconciles cleanly — no alert. If older
        # snapshot were used, drift would have flagged warning.
        assert report.holders_evaluated == 1
        assert report.alerts_emitted == 0

    def test_skips_cik_missing_sentinel_rows(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """DEF 14A rows with the CIK-MISSING sentinel are excluded
        from drift evaluation per the design contract — the issuer
        side is incomplete."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=769_330, symbol="A")
        _seed_def14a_holder(
            conn,
            instrument_id=769_330,
            accession="DEF-25-030",
            holder_name="Sentinel Holder",
            shares="500000",
            issuer_cik="CIK-MISSING",
        )
        conn.commit()

        report = detect_drift(conn)
        conn.commit()

        assert report.holders_evaluated == 0
        assert report.alerts_emitted == 0


class TestExactNameMatch:
    """Codex pre-push review caught false positives in the prior
    ILIKE-substring matcher. These tests pin the stricter
    case-insensitive equality (after role-suffix strip)."""

    def test_substring_does_not_falsely_match_longer_name(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """``"Ann"`` (DEF 14A) must NOT match ``"Joanne Smith"``
        (Form 4) — the prior ILIKE pattern matched any substring."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=769_400, symbol="A")
        _seed_def14a_holder(
            conn,
            instrument_id=769_400,
            accession="DEF-25-100",
            holder_name="Ann",
            shares="100000",
        )
        _seed_form4_txn(
            conn,
            accession="F4-25-100",
            instrument_id=769_400,
            filer_cik="0001100100",
            filer_name="Joanne Smith",
            txn_date=date(2026, 2, 15),
            post_transaction_shares="500000",
        )
        conn.commit()

        detect_drift(conn)
        conn.commit()

        # Ann does NOT match Joanne — no Form 4 hit, info severity.
        alerts = list(iter_alerts(conn, instrument_id=769_400))
        assert len(alerts) == 1
        assert alerts[0]["severity"] == "info"
        assert alerts[0]["matched_filer_cik"] is None

    def test_prefix_does_not_falsely_match_jr_suffix(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """``"John Doe"`` (DEF 14A) must NOT match ``"John Doe Jr"``
        (Form 4) — different individuals."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=769_410, symbol="A")
        _seed_def14a_holder(
            conn,
            instrument_id=769_410,
            accession="DEF-25-110",
            holder_name="John Doe",
            shares="500000",
        )
        _seed_form4_txn(
            conn,
            accession="F4-25-110",
            instrument_id=769_410,
            filer_cik="0001100110",
            filer_name="John Doe Jr",
            txn_date=date(2026, 2, 15),
            post_transaction_shares="100000",
        )
        conn.commit()

        detect_drift(conn)
        conn.commit()

        alerts = list(iter_alerts(conn, instrument_id=769_410))
        assert len(alerts) == 1
        assert alerts[0]["severity"] == "info"
        assert alerts[0]["matched_filer_cik"] is None

    def test_dash_suffix_in_form4_filer_name_still_reconciles(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """``"John Doe"`` (DEF 14A, no role suffix) must reconcile
        with ``"John Doe - Director"`` (Form 4). Both sides must
        normalise via the same separator set — Codex pre-push
        review caught the SQL-only-comma normaliser missing the
        dash variants."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=769_415, symbol="A")
        _seed_def14a_holder(
            conn,
            instrument_id=769_415,
            accession="DEF-25-115",
            holder_name="John Doe",
            shares="200000",
        )
        _seed_form4_txn(
            conn,
            accession="F4-25-115",
            instrument_id=769_415,
            filer_cik="0001100115",
            filer_name="John Doe - Director",
            txn_date=date(2026, 2, 15),
            post_transaction_shares="200000",  # exact share match
        )
        conn.commit()

        detect_drift(conn)
        conn.commit()

        # Exact match after dash-suffix strip; no alert.
        alerts = list(iter_alerts(conn, instrument_id=769_415))
        assert alerts == []

    def test_form4_with_null_filer_cik_still_matches_on_name(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Legacy Form 4 rows can have NULL filer_cik (the column is
        nullable per migration 057's ADD COLUMN ... TEXT). An exact
        name match with NULL CIK is still a real reconciliation —
        not a coverage gap. Codex pre-push review caught the prior
        code treating ``cik is None`` as ``unmatched``."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=769_420, symbol="A")
        _seed_def14a_holder(
            conn,
            instrument_id=769_420,
            accession="DEF-25-120",
            holder_name="Legacy Holder",
            shares="100000",
        )
        # Seed an insider_filing parent row first (FK target).
        _seed_insider_filing(conn, accession="F4-25-120", instrument_id=769_420)
        # Insert directly so we can null filer_cik (the test seed
        # helper only takes a string).
        conn.execute(
            """
            INSERT INTO insider_transactions (
                accession_number, txn_row_num, instrument_id,
                filer_cik, filer_name, txn_date, txn_code,
                shares, post_transaction_shares, is_derivative
            ) VALUES (%s, 1, %s, NULL, %s, %s, 'P', 100, %s, FALSE)
            """,
            (
                "F4-25-120",
                769_420,
                "Legacy Holder",
                date(2026, 2, 15),
                Decimal("100000"),
            ),
        )
        conn.commit()

        report = detect_drift(conn)
        conn.commit()

        # Exact match on name, zero drift — no alert. The detector
        # treats the row as matched even though filer_cik is NULL;
        # the prior code emitted a false ``info`` coverage gap.
        assert report.alerts_emitted == 0


class TestStaleAlertCleanup:
    """Codex pre-push review caught alerts staying in the table
    after the underlying drift had reconciled. These tests pin
    the auto-clear behaviour."""

    def test_resolved_drift_removes_existing_alert(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=769_500, symbol="A")
        _seed_def14a_holder(
            conn,
            instrument_id=769_500,
            accession="DEF-25-200",
            holder_name="Resolving Holder",
            shares="1000000",
        )
        # First pass: no Form 4 row -> info-severity alert.
        conn.commit()

        first = detect_drift(conn)
        conn.commit()
        assert first.alerts_emitted == 1
        assert first.alerts_by_severity["info"] == 1

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM def14a_drift_alerts WHERE instrument_id = %s", (769_500,))
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1

        # Now seed an exact-matching Form 4 row.
        _seed_form4_txn(
            conn,
            accession="F4-25-200",
            instrument_id=769_500,
            filer_cik="0001100200",
            filer_name="Resolving Holder",
            txn_date=date(2026, 2, 15),
            post_transaction_shares="1000000",
        )
        conn.commit()

        # Second pass: reconciliation is now clean — alert cleared.
        second = detect_drift(conn)
        conn.commit()
        assert second.alerts_emitted == 0

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM def14a_drift_alerts WHERE instrument_id = %s", (769_500,))
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 0


class TestIdempotency:
    def test_re_running_detector_upserts_in_place(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Running the detector twice on the same data does not
        duplicate alert rows; the existing row's detected_at is
        refreshed."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=769_340, symbol="A")
        _seed_def14a_holder(
            conn,
            instrument_id=769_340,
            accession="DEF-25-040",
            holder_name="Phantom",
            shares="500000",
        )
        conn.commit()

        first = detect_drift(conn)
        conn.commit()
        assert first.alerts_emitted == 1

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT alert_id, detected_at FROM def14a_drift_alerts WHERE instrument_id = %s",
                (769_340,),
            )
            first_row = cur.fetchone()
        assert first_row is not None
        first_id = first_row["alert_id"]
        first_detected = first_row["detected_at"]

        # Re-run.
        second = detect_drift(conn)
        conn.commit()
        assert second.alerts_emitted == 1

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT COUNT(*), MIN(alert_id), MAX(detected_at) FROM def14a_drift_alerts WHERE instrument_id = %s",
                (769_340,),
            )
            row = cur.fetchone()
        assert row is not None
        # One row total — UPSERT, not INSERT.
        assert row["count"] == 1
        assert row["min"] == first_id
        # detected_at was refreshed.
        assert row["max"] >= first_detected
