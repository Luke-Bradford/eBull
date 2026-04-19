import httpx
import psycopg
import psycopg.errors
import pytest

from app.services.sync_orchestrator.exception_classifier import classify_exception
from app.services.sync_orchestrator.layer_types import FailureCategory


def _http_error(status: int) -> httpx.HTTPStatusError:
    resp = httpx.Response(status_code=status, text="error")
    return httpx.HTTPStatusError("err", request=httpx.Request("GET", "https://example"), response=resp)


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (401, FailureCategory.AUTH_EXPIRED),
        (403, FailureCategory.AUTH_EXPIRED),
        (429, FailureCategory.RATE_LIMITED),
        (500, FailureCategory.SOURCE_DOWN),
        (502, FailureCategory.SOURCE_DOWN),
        (503, FailureCategory.SOURCE_DOWN),
        (504, FailureCategory.SOURCE_DOWN),
    ],
)
def test_httpx_status_errors(status: int, expected: FailureCategory) -> None:
    assert classify_exception(_http_error(status)) is expected


@pytest.mark.parametrize(
    "exc",
    [
        httpx.ConnectError("unreachable"),
        httpx.ConnectTimeout("timeout"),
        httpx.ReadTimeout("read timeout"),
    ],
)
def test_httpx_transport_errors_map_to_source_down(exc: Exception) -> None:
    assert classify_exception(exc) is FailureCategory.SOURCE_DOWN


def test_unique_violation_maps_to_db_constraint() -> None:
    assert classify_exception(psycopg.errors.UniqueViolation("dup")) is FailureCategory.DB_CONSTRAINT


def test_foreign_key_violation_maps_to_db_constraint() -> None:
    assert classify_exception(psycopg.errors.ForeignKeyViolation("fk")) is FailureCategory.DB_CONSTRAINT


def test_not_null_violation_maps_to_db_constraint() -> None:
    assert classify_exception(psycopg.errors.NotNullViolation("nn")) is FailureCategory.DB_CONSTRAINT


def test_4xx_other_maps_to_internal_error() -> None:
    # 418 is not auth/rate-limit — bucket as internal (stable fallback).
    assert classify_exception(_http_error(418)) is FailureCategory.INTERNAL_ERROR


def test_runtime_error_maps_to_internal_error() -> None:
    assert classify_exception(RuntimeError("surprise")) is FailureCategory.INTERNAL_ERROR


def test_value_error_maps_to_internal_error() -> None:
    # ValueError comes from validation / parsing, not HTTP transport.
    # Bucket as INTERNAL_ERROR (retriable). Schema-drift classification
    # is a higher-level decision that requires payload-shape evidence
    # the classifier does not see.
    assert classify_exception(ValueError("bad")) is FailureCategory.INTERNAL_ERROR


def test_layer_refresh_failed_passes_category_through() -> None:
    # Adapter that knows exactly what failed should be honoured, not
    # re-classified to INTERNAL_ERROR.
    from app.services.sync_orchestrator.layer_types import LayerRefreshFailed

    exc = LayerRefreshFailed(category=FailureCategory.SCHEMA_DRIFT, detail="payload changed")
    assert classify_exception(exc) is FailureCategory.SCHEMA_DRIFT


def test_layer_refresh_failed_data_gap_passes_through() -> None:
    from app.services.sync_orchestrator.layer_types import LayerRefreshFailed

    exc = LayerRefreshFailed(category=FailureCategory.DATA_GAP, detail="empty response")
    assert classify_exception(exc) is FailureCategory.DATA_GAP


def test_operational_error_maps_to_source_down() -> None:
    # Transient DB infrastructure (connection failure, lock timeout,
    # server shutdown) — self-heal via retry budget rather than
    # surfacing as ACTION_NEEDED on first miss.
    assert classify_exception(psycopg.OperationalError("conn refused")) is FailureCategory.SOURCE_DOWN
