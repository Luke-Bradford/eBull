"""Tests for /sync/ingest/cik_timing/latest + /sync/ingest/seed_progress.

End-to-end service logic is covered in
``tests/test_fundamentals_observability.py`` against the isolated
``ebull_test`` database. These tests verify only the HTTP plumbing —
response shape, status codes, auth — by stubbing the service
functions, so they can run against any Postgres (including CI's) and
cannot touch the dev DB.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.services.fundamentals_observability import (
    CikTimingPercentiles,
    CikTimingSummary,
    LatestIngestionRun,
    SeedProgressSummary,
    SeedSourceProgress,
    SlowCikEntry,
)


@pytest.mark.integration
def test_cik_timing_latest_empty_returns_null_run_id(clean_client: TestClient) -> None:
    empty = CikTimingSummary(
        ingestion_run_id=None,
        run_source=None,
        run_started_at=None,
        run_finished_at=None,
        run_status=None,
        modes=[],
        slowest=[],
    )
    with patch("app.api.sync.get_cik_timing_summary", return_value=empty):
        resp = clean_client.get("/sync/ingest/cik_timing/latest")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ingestion_run_id"] is None
    assert body["modes"] == []
    assert body["slowest"] == []


@pytest.mark.integration
def test_cik_timing_latest_returns_populated_summary(clean_client: TestClient) -> None:
    now = datetime.now(tz=UTC)
    populated = CikTimingSummary(
        ingestion_run_id=42,
        run_source="sec_edgar",
        run_started_at=now,
        run_finished_at=now,
        run_status="success",
        modes=[
            CikTimingPercentiles(
                mode="seed",
                count=3,
                p50_seconds=1.5,
                p95_seconds=4.6,
                max_seconds=4.8,
                facts_upserted_total=300,
            ),
        ],
        slowest=[
            SlowCikEntry(
                cik="0000000003",
                mode="seed",
                seconds=4.8,
                facts_upserted=100,
                outcome="success",
                finished_at=now,
            ),
        ],
    )
    with patch("app.api.sync.get_cik_timing_summary", return_value=populated):
        resp = clean_client.get("/sync/ingest/cik_timing/latest")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ingestion_run_id"] == 42
    assert body["modes"][0]["mode"] == "seed"
    assert body["modes"][0]["count"] == 3
    assert body["modes"][0]["p50_seconds"] == pytest.approx(1.5)
    assert body["slowest"][0]["cik"] == "0000000003"


@pytest.mark.integration
def test_seed_progress_returns_paused_flag(clean_client: TestClient) -> None:
    now = datetime.now(tz=UTC)
    summary = SeedProgressSummary(
        sources=[
            SeedSourceProgress(
                source="sec.submissions",
                key_description="SEC submissions.json",
                seeded=3_691,
                total=5_134,
            ),
        ],
        latest_run=LatestIngestionRun(
            ingestion_run_id=7,
            source="sec_edgar",
            started_at=now,
            finished_at=now,
            status="success",
            rows_upserted=10_000,
            rows_skipped=0,
        ),
        ingest_paused=True,
    )
    with patch("app.api.sync.get_seed_progress", return_value=summary):
        resp = clean_client.get("/sync/ingest/seed_progress")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ingest_paused"] is True
    assert body["sources"][0]["source"] == "sec.submissions"
    assert body["sources"][0]["seeded"] == 3_691
    assert body["sources"][0]["total"] == 5_134
    assert body["latest_run"]["ingestion_run_id"] == 7


def test_cik_timing_latest_requires_auth() -> None:
    from app.api.sync import router as sync_router
    from app.db import get_conn

    def _mock_conn():  # type: ignore[return]
        yield MagicMock()

    bare = FastAPI()
    bare.include_router(sync_router)
    bare.dependency_overrides[get_conn] = _mock_conn
    with TestClient(bare) as client:
        resp = client.get("/sync/ingest/cik_timing/latest")
    assert resp.status_code in {401, 403}, resp.text


def test_seed_progress_requires_auth() -> None:
    from app.api.sync import router as sync_router
    from app.db import get_conn

    def _mock_conn():  # type: ignore[return]
        yield MagicMock()

    bare = FastAPI()
    bare.include_router(sync_router)
    bare.dependency_overrides[get_conn] = _mock_conn
    with TestClient(bare) as client:
        resp = client.get("/sync/ingest/seed_progress")
    assert resp.status_code in {401, 403}, resp.text
