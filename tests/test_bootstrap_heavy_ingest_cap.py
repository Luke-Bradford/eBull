"""#1426 — global heavy-ingest concurrency cap (Postgres OOM memory guard).

The five Phase-C bulk-ingest stages each touch an 84-86-leaf partitioned
table. Running all five concurrently OOM-crashed the 6GB Postgres container
during the P6 clean bootstrap. ``_heavy_ingest_admits`` caps how many run at
once WITHOUT collapsing the #1141 cross-lane parallelism to the serial path.

These are pure-function tests — no DB, no threads.
"""

from __future__ import annotations

from app.services.bootstrap_orchestrator import (
    _HEAVY_INGEST_MAX_CONCURRENCY,
    _HEAVY_INGEST_STAGES,
    _STAGE_LANE_OVERRIDES,
    _heavy_ingest_admits,
)


class TestHeavyIngestStageSet:
    def test_cap_is_two(self) -> None:
        # 2 keeps ~2x parallelism (companyfacts + one ownership/submissions
        # stage) while bounding aggregate PG backend memory. Not 1 (that is
        # the known-bad ~283min serial path #1141 retired); not >2 (the OOM).
        assert _HEAVY_INGEST_MAX_CONCURRENCY == 2

    def test_heavy_set_matches_db_family_ingest_stages(self) -> None:
        # Drift guard: the heavy-ingest set MUST equal exactly the stage
        # keys routed to a ``db_*`` family lane in _STAGE_LANE_OVERRIDES.
        # If a Phase-C ingest stage is renamed/added, the cap must follow —
        # a stale key here would silently leave that stage uncapped.
        db_family_stages = {key for key, lane in _STAGE_LANE_OVERRIDES.items() if lane.startswith("db")}
        assert _HEAVY_INGEST_STAGES == db_family_stages


class TestHeavyIngestAdmits:
    def test_non_heavy_stage_always_admitted(self) -> None:
        # A non-heavy candidate is admitted even when the heavy family is
        # saturated — the cap only governs the partition-touching family.
        saturated = ["sec_companyfacts_ingest", "sec_13f_ingest_from_dataset"]
        assert _heavy_ingest_admits(saturated, "cik_refresh") is True

    def test_heavy_admitted_below_cap(self) -> None:
        assert _heavy_ingest_admits([], "sec_companyfacts_ingest") is True
        assert _heavy_ingest_admits(["sec_companyfacts_ingest"], "sec_nport_ingest_from_dataset") is True

    def test_heavy_rejected_at_cap(self) -> None:
        in_flight = ["sec_companyfacts_ingest", "sec_13f_ingest_from_dataset"]
        assert _heavy_ingest_admits(in_flight, "sec_nport_ingest_from_dataset") is False

    def test_non_heavy_in_flight_does_not_count_toward_cap(self) -> None:
        # Only heavy stages count: one heavy + several non-heavy in flight
        # still admits a second heavy stage.
        in_flight = ["sec_companyfacts_ingest", "cik_refresh", "candle_refresh", "universe_sync"]
        assert _heavy_ingest_admits(in_flight, "sec_13f_ingest_from_dataset") is True
