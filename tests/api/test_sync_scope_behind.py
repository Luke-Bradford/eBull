"""POST /sync API contract (rewritten for #719).

Pre-#719: this file drove the planner end-to-end through the HTTP
boundary because ``POST /sync`` ran the planner in-process and
returned the plan in the response body. The integration coverage was
genuinely useful — every state-machine input combination produced a
plan the test could inspect.

Post-#719: ``POST /sync`` writes a row to ``pending_job_requests`` and
fires NOTIFY. The plan runs in the jobs process, not the API. These
tests now only assert the API contract: a publisher call with the
correct scope, a 202 response carrying the request_id.

Planner-shape coverage (every-layer-healthy → empty plan, ACTION_NEEDED
upstreams included, etc.) lives in
``tests/test_sync_orchestrator_planner.py`` against the planner
function directly. That's a tighter test surface that doesn't have to
mock the queue or fake an executor.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.integration


def test_post_sync_behind_publishes_request(clean_client: TestClient) -> None:
    """``POST /sync`` writes a queue row and returns the request_id."""
    from app.services.sync_orchestrator import SyncScope as _Scope

    with patch("app.api.sync.publish_sync_request", return_value=42) as pub:
        resp = clean_client.post("/sync", json={"scope": "behind"})
    assert resp.status_code == 202, resp.text
    assert resp.json() == {"request_id": 42}
    scope_arg: _Scope = pub.call_args.args[0]
    assert scope_arg.kind == "behind"


def test_post_sync_empty_body_defaults_to_behind(clean_client: TestClient) -> None:
    """Regression guard: empty body still publishes ``scope='behind'``."""
    from app.services.sync_orchestrator import SyncScope as _Scope

    with patch("app.api.sync.publish_sync_request", return_value=43) as pub:
        resp = clean_client.post("/sync", json={})
    assert resp.status_code == 202, resp.text
    assert resp.json() == {"request_id": 43}
    scope_arg: _Scope = pub.call_args.args[0]
    assert scope_arg.kind == "behind"


def test_post_sync_layer_scope_carries_layer_name(clean_client: TestClient) -> None:
    from app.services.sync_orchestrator import SyncScope as _Scope

    with patch("app.api.sync.publish_sync_request", return_value=44) as pub:
        resp = clean_client.post("/sync", json={"scope": "layer", "layer": "fundamentals"})
    assert resp.status_code == 202, resp.text
    scope_arg: _Scope = pub.call_args.args[0]
    assert scope_arg.kind == "layer"
    assert scope_arg.detail == "fundamentals"


def test_post_sync_layer_scope_without_layer_returns_422(clean_client: TestClient) -> None:
    """The scope/layer / scope/job validation happens before the publish
    call — a missing companion field must 422 without writing a row."""
    with patch("app.api.sync.publish_sync_request") as pub:
        resp = clean_client.post("/sync", json={"scope": "layer"})
    assert resp.status_code == 422, resp.text
    assert pub.call_count == 0
