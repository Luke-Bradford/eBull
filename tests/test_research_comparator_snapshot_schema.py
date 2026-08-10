"""DB invariants for immutable, non-tradable comparator snapshots (#2482)."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import psycopg
import pytest

from app.services.research_comparator_snapshot import (
    COMPARATOR_SYMBOLS,
    FROZEN_FRONTIER,
    SNAPSHOT_ID,
    ComparatorBar,
    ComparatorUnavailable,
    build_snapshot,
    load_comparator_closes,
    store_snapshot,
    verify_stored_snapshot,
)


def _snapshot(close: Decimal = Decimal("101")):
    bar = ComparatorBar(
        bar_date=FROZEN_FRONTIER,
        open=Decimal("100"),
        high=Decimal("102"),
        low=Decimal("99"),
        close=close,
        volume=None,
    )
    return build_snapshot((symbol, 2000 + index, (bar,)) for index, symbol in enumerate(COMPARATOR_SYMBOLS))


@pytest.mark.db
class TestComparatorSnapshotSchema:
    def test_store_is_idempotent_bounded_and_not_tradable(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        snapshot = _snapshot()
        assert store_snapshot(ebull_test_conn, snapshot) == len(COMPARATOR_SYMBOLS)
        assert store_snapshot(ebull_test_conn, snapshot) == len(COMPARATOR_SYMBOLS)

        assert ebull_test_conn.execute(
            """
            SELECT count(*), count(instrument_id), count(resolution_method),
                   count(*) FILTER (WHERE comparator_snapshot_id = %s)
            FROM research_price_series
            """,
            (SNAPSHOT_ID,),
        ).fetchone() == (len(COMPARATOR_SYMBOLS), 0, 0, len(COMPARATOR_SYMBOLS))
        assert ebull_test_conn.execute(
            """
            SELECT count(*), count(adj_close)
            FROM research_price_daily d
            JOIN research_price_series s USING (series_id)
            WHERE s.comparator_snapshot_id = %s
            """,
            (SNAPSHOT_ID,),
        ).fetchone() == (len(COMPARATOR_SYMBOLS), 0)
        assert verify_stored_snapshot(ebull_test_conn) == snapshot

    def test_same_snapshot_id_refuses_changed_source_facts(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        store_snapshot(ebull_test_conn, _snapshot())
        with pytest.raises(RuntimeError, match="mint a new snapshot id"):
            store_snapshot(ebull_test_conn, _snapshot(close=Decimal("100.5")))

    def test_verify_detects_member_provenance_drift(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        store_snapshot(ebull_test_conn, _snapshot())
        ebull_test_conn.execute(
            """
            UPDATE research_comparator_snapshot_members
            SET source_sha256 = repeat('0', 64)
            WHERE snapshot_id = %s AND vendor_symbol = 'SPY'
            """,
            (SNAPSHOT_ID,),
        )
        with pytest.raises(RuntimeError, match="member census/fingerprint"):
            verify_stored_snapshot(ebull_test_conn)

    def test_verify_detects_series_identity_drift(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        store_snapshot(ebull_test_conn, _snapshot())
        ebull_test_conn.execute(
            """
            UPDATE research_price_series SET vendor_symbol = 'SPY-DRIFTED'
            WHERE comparator_snapshot_id = %s AND vendor_symbol = 'SPY'
            """,
            (SNAPSHOT_ID,),
        )
        with pytest.raises(RuntimeError, match="series identity/provenance has drifted"):
            verify_stored_snapshot(ebull_test_conn)

    def test_comparator_snapshot_cannot_resolve_to_a_tradable_instrument(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        seeded_instrument_id: int,
    ) -> None:
        store_snapshot(ebull_test_conn, _snapshot())
        with pytest.raises(psycopg.errors.CheckViolation):
            ebull_test_conn.execute(
                """
                UPDATE research_price_series
                SET instrument_id = %s, resolution_method = 'manual'
                WHERE comparator_snapshot_id = %s
                """,
                (seeded_instrument_id, SNAPSHOT_ID),
            )

    def test_causal_loader_never_reads_beyond_the_requested_date(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
    ) -> None:
        store_snapshot(ebull_test_conn, _snapshot())
        assert load_comparator_closes(
            ebull_test_conn,
            snapshot_id=SNAPSHOT_ID,
            symbol="SPY",
            through_date=FROZEN_FRONTIER,
        ) == {FROZEN_FRONTIER: Decimal("101")}
        with pytest.raises(ComparatorUnavailable, match="has no comparator closes"):
            load_comparator_closes(
                ebull_test_conn,
                snapshot_id=SNAPSHOT_ID,
                symbol="SPY",
                through_date=FROZEN_FRONTIER - timedelta(days=1),
            )
