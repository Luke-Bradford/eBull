"""daily_news_refresh registration wiring (#1750). Pure-logic, no DB."""

from __future__ import annotations

from app.jobs.runtime import _INVOKERS, VALID_JOB_NAMES
from app.jobs.sources import source_for
from app.workers.scheduler import JOB_DAILY_NEWS_REFRESH, SCHEDULED_JOBS


def test_job_registered_in_invokers_and_valid_names() -> None:
    assert JOB_DAILY_NEWS_REFRESH in _INVOKERS
    assert JOB_DAILY_NEWS_REFRESH in VALID_JOB_NAMES


def test_job_in_scheduled_jobs_with_db_lane() -> None:
    entry = next((j for j in SCHEDULED_JOBS if j.name == JOB_DAILY_NEWS_REFRESH), None)
    assert entry is not None
    assert entry.source == "db"
    assert entry.catch_up_on_boot is True


def test_source_for_resolves() -> None:
    assert source_for(JOB_DAILY_NEWS_REFRESH) == "db"
