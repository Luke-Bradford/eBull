"""#1472 PR3 — every LISTEN connection is stamped with a distinct
``application_name`` so ``pg_stat_activity`` shows ownership and the
listener-cardinality probe (``/system/postgres-health``) can detect a
duplicate-instance listener.

Spy on ``psycopg.connect`` in each listener module so the tests assert
the kwarg wiring without opening a real connection.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.db.pg_settings import (
    API_CREDENTIAL_HEALTH_LISTENER_APPLICATION_NAME,
    JOB_REQUEST_LISTENER_APPLICATION_NAME,
    JOBS_CREDENTIAL_HEALTH_LISTENER_APPLICATION_NAME,
    LISTENER_APPLICATION_NAMES,
)


def test_listener_application_names_are_distinct() -> None:
    names = {
        JOB_REQUEST_LISTENER_APPLICATION_NAME,
        JOBS_CREDENTIAL_HEALTH_LISTENER_APPLICATION_NAME,
        API_CREDENTIAL_HEALTH_LISTENER_APPLICATION_NAME,
    }
    assert len(names) == 3  # no two listeners share a label
    assert names == set(LISTENER_APPLICATION_NAMES)


def test_job_request_listener_factory_stamps_application_name(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.jobs import listener

    spy = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(listener.psycopg, "connect", spy)

    listener._default_listen_conn_factory()

    _args, kwargs = spy.call_args
    assert kwargs["application_name"] == JOB_REQUEST_LISTENER_APPLICATION_NAME
    assert kwargs["autocommit"] is True


def test_credential_health_factory_defaults_to_api_label(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.jobs import credential_health_listener as chl

    spy = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(chl.psycopg, "connect", spy)

    chl._default_listen_conn_factory()  # no explicit label → API default

    _args, kwargs = spy.call_args
    assert kwargs["application_name"] == API_CREDENTIAL_HEALTH_LISTENER_APPLICATION_NAME
    assert kwargs["autocommit"] is True


def test_credential_health_factory_honours_jobs_label(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.jobs import credential_health_listener as chl

    spy = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(chl.psycopg, "connect", spy)

    chl._default_listen_conn_factory(JOBS_CREDENTIAL_HEALTH_LISTENER_APPLICATION_NAME)

    _args, kwargs = spy.call_args
    assert kwargs["application_name"] == JOBS_CREDENTIAL_HEALTH_LISTENER_APPLICATION_NAME
