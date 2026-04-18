"""Unit tests for refresh_cascade (#276 K.1).

Mock-based so no real Claude API is called. Integration of the
service with the scheduler (conn.commit() before cascade, API-key
gate, psycopg aborted-transaction recovery after thesis failure)
is intentionally deferred to K.2/K.3 once a real DB fixture is
cheap — MagicMock cannot model aborted psycopg transactions.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from app.services.refresh_cascade import (
    CascadeOutcome,
    cascade_refresh,
    changed_instruments_from_outcome,
)
from app.services.sec_incremental import RefreshOutcome, RefreshPlan
from app.services.thesis import StaleInstrument

# ---------------------------------------------------------------------------
# changed_instruments_from_outcome
# ---------------------------------------------------------------------------


def _mock_conn_with_cik_lookup(cik_to_instrument: dict[str, int]) -> MagicMock:
    """Mock conn whose SELECT resolves CIKs to instrument_ids."""
    conn = MagicMock()

    def execute_side_effect(sql, params=None):  # type: ignore[no-untyped-def]
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        sql_str = sql if isinstance(sql, str) else str(sql)
        if "external_identifiers" in sql_str:
            # params is (cik_list,) — one tuple arg
            ciks = params[0] if isinstance(params, tuple) else (params or [])
            rows = []
            for cik in ciks:
                if cik in cik_to_instrument:
                    rows.append((cik_to_instrument[cik],))
            cursor.fetchall.return_value = sorted(rows)
        return cursor

    conn.execute.side_effect = execute_side_effect
    return conn


class TestChangedInstrumentsFromOutcome:
    def test_empty_plan_returns_empty_list(self) -> None:
        conn = MagicMock()
        plan = RefreshPlan()
        outcome = RefreshOutcome()
        assert changed_instruments_from_outcome(conn, plan, outcome) == []
        conn.execute.assert_not_called()

    def test_seeds_excluded(self) -> None:
        """Seeds don't cascade — prevents fresh-install Claude storm."""
        conn = _mock_conn_with_cik_lookup({"0000000001": 1})
        plan = RefreshPlan(seeds=["0000000001"])
        outcome = RefreshOutcome(seeded=1)
        result = changed_instruments_from_outcome(conn, plan, outcome)
        assert result == []

    def test_refreshes_mapped_to_instrument_ids(self) -> None:
        conn = _mock_conn_with_cik_lookup({"0000000001": 101, "0000000002": 102})
        plan = RefreshPlan(
            refreshes=[("0000000001", "ACCN-1"), ("0000000002", "ACCN-2")],
        )
        outcome = RefreshOutcome(refreshed=2)
        result = changed_instruments_from_outcome(conn, plan, outcome)
        assert result == [101, 102]

    def test_submissions_only_mapped_to_instrument_ids(self) -> None:
        conn = _mock_conn_with_cik_lookup({"0000000001": 101})
        plan = RefreshPlan(
            submissions_only_advances=[("0000000001", "ACCN-8K")],
        )
        outcome = RefreshOutcome(submissions_advanced=1)
        result = changed_instruments_from_outcome(conn, plan, outcome)
        assert result == [101]

    def test_failed_ciks_excluded(self) -> None:
        """CIKs in outcome.failed drop out of the cascade set."""
        conn = _mock_conn_with_cik_lookup({"0000000001": 101, "0000000002": 102})
        plan = RefreshPlan(
            refreshes=[("0000000001", "A"), ("0000000002", "B")],
        )
        outcome = RefreshOutcome(
            refreshed=1,
            failed=[("0000000002", "RuntimeError")],
        )
        result = changed_instruments_from_outcome(conn, plan, outcome)
        assert result == [101]

    def test_seed_in_refresh_bucket_still_excluded(self) -> None:
        """Defensive: if a CIK appears in both seeds and refreshes
        (planner divergence or manual plan), the seed filter wins —
        no cascade for fresh-install CIKs."""
        conn = _mock_conn_with_cik_lookup({"0000000001": 101, "0000000002": 102})
        plan = RefreshPlan(
            seeds=["0000000001"],
            refreshes=[("0000000001", "A"), ("0000000002", "B")],
        )
        outcome = RefreshOutcome(seeded=1, refreshed=1)
        result = changed_instruments_from_outcome(conn, plan, outcome)
        assert result == [102]

    def test_unpadded_cik_normalized_to_zero_padded(self) -> None:
        """Defensive zfill(10): if a future caller hands us a
        raw-integer CIK string (e.g. '320193'), we still match the
        zero-padded identifier_value stored in external_identifiers
        ('0000320193'). Protects against silent zero-row misses."""
        conn = _mock_conn_with_cik_lookup({"0000320193": 101})
        plan = RefreshPlan(refreshes=[("320193", "ACCN-APPL")])
        outcome = RefreshOutcome(refreshed=1)
        result = changed_instruments_from_outcome(conn, plan, outcome)
        assert result == [101]

    def test_mixed_padded_and_unpadded_cik_dedupe_after_padding(self) -> None:
        """Both unpadded and padded forms of the same CIK collapse
        to one mapping — de-dupe operates on the padded form."""
        conn = _mock_conn_with_cik_lookup({"0000320193": 101})
        plan = RefreshPlan(
            refreshes=[("320193", "A"), ("0000320193", "B")],
        )
        outcome = RefreshOutcome(refreshed=2)
        result = changed_instruments_from_outcome(conn, plan, outcome)
        assert result == [101]


# ---------------------------------------------------------------------------
# cascade_refresh
# ---------------------------------------------------------------------------


class TestCascadeRefresh:
    def test_empty_ids_noop(self) -> None:
        conn = MagicMock()
        client = MagicMock()
        outcome = cascade_refresh(conn, client, [])
        assert outcome == CascadeOutcome(
            instruments_considered=0,
            thesis_refreshed=0,
            rankings_recomputed=False,
        )
        conn.execute.assert_not_called()

    def test_no_stale_instruments_skips_thesis_and_rankings(self) -> None:
        """If find_stale returns nothing, no Claude calls and no rerank."""
        conn = MagicMock()
        client = MagicMock()
        with (
            patch(
                "app.services.refresh_cascade.find_stale_instruments",
                return_value=[],
            ) as stale_mock,
            patch("app.services.refresh_cascade.generate_thesis") as gen_mock,
            patch("app.services.refresh_cascade.compute_rankings") as rank_mock,
        ):
            outcome = cascade_refresh(conn, client, [1, 2, 3])

        stale_mock.assert_called_once_with(conn, tier=None, instrument_ids=[1, 2, 3])
        gen_mock.assert_not_called()
        rank_mock.assert_not_called()
        assert outcome.thesis_refreshed == 0
        assert outcome.rankings_recomputed is False
        assert outcome.instruments_considered == 3

    def test_stale_instruments_trigger_thesis_and_single_rerank(self) -> None:
        """Stale instruments each get generate_thesis; rerank runs once
        at the end, not per-instrument."""
        conn = MagicMock()
        client = MagicMock()
        stale_rows = [
            StaleInstrument(instrument_id=1, symbol="A", reason="event_new_10q"),
            StaleInstrument(instrument_id=2, symbol="B", reason="event_new_8k"),
        ]
        with (
            patch(
                "app.services.refresh_cascade.find_stale_instruments",
                return_value=stale_rows,
            ),
            patch("app.services.refresh_cascade.generate_thesis") as gen_mock,
            patch(
                "app.services.refresh_cascade.compute_rankings",
                return_value=MagicMock(scored=[MagicMock(), MagicMock()]),
            ) as rank_mock,
        ):
            outcome = cascade_refresh(conn, client, [1, 2])

        assert gen_mock.call_count == 2
        rank_mock.assert_called_once_with(conn)
        assert outcome.thesis_refreshed == 2
        assert outcome.rankings_recomputed is True
        assert outcome.failed == ()

    def test_per_instrument_failure_isolated_rerank_still_runs(self) -> None:
        """One instrument's thesis raising must not abort siblings,
        and a successful sibling still triggers the rerank."""
        conn = MagicMock()
        client = MagicMock()
        stale_rows = [
            StaleInstrument(instrument_id=1, symbol="BAD", reason="event_new_10k"),
            StaleInstrument(instrument_id=2, symbol="GOOD", reason="event_new_10q"),
        ]

        def gen_side_effect(iid, conn_, client_):  # type: ignore[no-untyped-def]
            if iid == 1:
                raise RuntimeError("boom")
            return MagicMock()

        with (
            patch(
                "app.services.refresh_cascade.find_stale_instruments",
                return_value=stale_rows,
            ),
            patch(
                "app.services.refresh_cascade.generate_thesis",
                side_effect=gen_side_effect,
            ),
            patch(
                "app.services.refresh_cascade.compute_rankings",
                return_value=MagicMock(scored=[]),
            ) as rank_mock,
        ):
            outcome = cascade_refresh(conn, client, [1, 2])

        assert outcome.thesis_refreshed == 1
        assert ("RuntimeError") in {e[1] for e in outcome.failed}
        rank_mock.assert_called_once()
        assert outcome.rankings_recomputed is True

    def test_all_thesis_fail_no_rerank(self) -> None:
        """If zero theses refreshed, skip the rerank — nothing changed."""
        conn = MagicMock()
        client = MagicMock()
        stale_rows = [StaleInstrument(instrument_id=1, symbol="BAD", reason="event_new_10q")]
        with (
            patch(
                "app.services.refresh_cascade.find_stale_instruments",
                return_value=stale_rows,
            ),
            patch(
                "app.services.refresh_cascade.generate_thesis",
                side_effect=RuntimeError("boom"),
            ),
            patch("app.services.refresh_cascade.compute_rankings") as rank_mock,
        ):
            outcome = cascade_refresh(conn, client, [1])

        assert outcome.thesis_refreshed == 0
        rank_mock.assert_not_called()
        assert outcome.rankings_recomputed is False

    def test_rerank_failure_records_and_returns(self) -> None:
        """compute_rankings raising is captured in failed with sentinel
        instrument_id=-1 and the cascade still returns the thesis
        refresh count."""
        conn = MagicMock()
        client = MagicMock()
        stale_rows = [StaleInstrument(instrument_id=1, symbol="A", reason="event_new_10q")]
        with (
            patch(
                "app.services.refresh_cascade.find_stale_instruments",
                return_value=stale_rows,
            ),
            patch("app.services.refresh_cascade.generate_thesis"),
            patch(
                "app.services.refresh_cascade.compute_rankings",
                side_effect=RuntimeError("scoring broke"),
            ),
        ):
            outcome = cascade_refresh(conn, client, [1])

        assert outcome.thesis_refreshed == 1
        assert outcome.rankings_recomputed is False
        assert (-1, "RuntimeError") in outcome.failed
        # Rollback invoked so the aborted-transaction state from the
        # failed compute_rankings SQL does not leak out of the cascade.
        conn.rollback.assert_called()
