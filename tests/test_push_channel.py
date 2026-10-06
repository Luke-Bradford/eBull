"""Tests for the ntfy push channel (#3614 item 3). No network: urlopen is stubbed."""

from __future__ import annotations

import urllib.error
from typing import Any

import pytest

from app.system import push_channel
from app.system.push_channel import PushConfig, build_request, config_from_env, send_push


def test_no_topic_means_push_off() -> None:
    assert config_from_env({}) is None
    assert config_from_env({"EBULL_NTFY_TOPIC": "   "}) is None


def test_config_defaults_and_overrides() -> None:
    assert config_from_env({"EBULL_NTFY_TOPIC": "t"}) == PushConfig("https://ntfy.sh", "t", None)
    cfg = config_from_env(
        {"EBULL_NTFY_TOPIC": "t", "EBULL_NTFY_SERVER": "https://n.example/", "EBULL_NTFY_TOKEN": "tk"}
    )
    assert cfg == PushConfig("https://n.example", "t", "tk")


def test_request_wire_shape() -> None:
    req = build_request(
        PushConfig("https://ntfy.sh", "topic", "tk_1"),
        title="Jobs DARK — now",
        message="no job for 31 min · ✓",
        priority=5,
        tags=("rotating_light", "x"),
    )
    assert req.full_url == "https://ntfy.sh/topic"
    assert req.get_method() == "POST"
    assert req.data == "no job for 31 min · ✓".encode()
    headers = {k.lower(): v for k, v in req.header_items()}
    assert headers["title"] == "Jobs DARK ? now"  # header must stay latin-1-safe
    assert headers["priority"] == "5"
    assert headers["tags"] == "rotating_light,x"
    assert headers["authorization"] == "Bearer tk_1"


def test_request_without_token_or_tags_sends_neither_header() -> None:
    req = build_request(PushConfig("https://ntfy.sh", "topic", None), title="t", message="m")
    headers = {k.lower() for k, _ in req.header_items()}
    assert "authorization" not in headers
    assert "tags" not in headers


class _Resp:
    def __init__(self, status: int) -> None:
        self.status = status

    def __enter__(self) -> _Resp:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


def test_send_push_is_a_noop_without_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EBULL_NTFY_TOPIC", raising=False)

    def _boom(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("must not touch the network")

    monkeypatch.setattr(push_channel.urllib.request, "urlopen", _boom)
    assert send_push(title="t", message="m") is False


def test_send_push_true_only_on_2xx(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = PushConfig("https://ntfy.sh", "topic", None)
    monkeypatch.setattr(push_channel.urllib.request, "urlopen", lambda *_a, **_k: _Resp(200))
    assert send_push(title="t", message="m", config=cfg) is True
    monkeypatch.setattr(push_channel.urllib.request, "urlopen", lambda *_a, **_k: _Resp(302))
    assert send_push(title="t", message="m", config=cfg) is False


def test_send_push_never_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fail(*_a: Any, **_k: Any) -> Any:
        raise urllib.error.URLError("down")

    monkeypatch.setattr(push_channel.urllib.request, "urlopen", _fail)
    assert send_push(title="t", message="m", config=PushConfig("https://ntfy.sh", "topic", None)) is False
