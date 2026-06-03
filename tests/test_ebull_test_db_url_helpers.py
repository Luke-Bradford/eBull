"""Pure tests for the C1 (#1447) test-cluster URL helpers.

``_swap_port`` / ``_assert_not_dev_cluster`` decide which Postgres cluster the
whole suite talks to, so a bug here silently re-couples tests to the dev
``ebull`` cluster (the failure C1 exists to prevent). IO-free — no DB needed.
"""

from __future__ import annotations

from urllib.parse import unquote, urlparse

import pytest

from app.config import settings
from tests.fixtures.ebull_test_db import _assert_not_dev_cluster, _swap_port


def test_swap_port_basic() -> None:
    out = _swap_port("postgresql://postgres:postgres@localhost:5432/ebull", "5433")
    assert out == "postgresql://postgres:postgres@localhost:5433/ebull"


def test_swap_port_preserves_percent_encoded_password() -> None:
    # #1448 bot BLOCKING: a password URL-encoded as p%40ss must NOT be rebuilt
    # from urlparse's decoded .password (which would yield a literal '@' and a
    # malformed netloc). The encoded form must survive byte-for-byte.
    src = "postgresql://postgres:p%40s%3As@db.example:5432/ebull"
    out = _swap_port(src, "5433")
    assert out == "postgresql://postgres:p%40s%3As@db.example:5433/ebull"
    parsed = urlparse(out)
    assert parsed.port == 5433
    assert parsed.hostname == "db.example"
    # The encoded form survives byte-for-byte; unquoting yields the real
    # password — proves no corruption of special characters.
    assert parsed.password == "p%40s%3As"
    assert unquote(parsed.password) == "p@s:s"


def test_swap_port_appends_when_no_port() -> None:
    out = _swap_port("postgresql://postgres:postgres@localhost/ebull", "5433")
    assert urlparse(out).port == 5433
    assert urlparse(out).hostname == "localhost"


def test_swap_port_ipv6_host() -> None:
    out = _swap_port("postgresql://postgres:postgres@[::1]:5432/ebull", "5433")
    assert urlparse(out).port == 5433
    assert urlparse(out).hostname == "::1"


def test_assert_not_dev_cluster_rejects_same_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "database_url", "postgresql://postgres:postgres@localhost:5432/ebull")
    with pytest.raises(RuntimeError, match="must run on the SEPARATE"):
        _assert_not_dev_cluster("postgresql://postgres:postgres@localhost:5432/postgres")


def test_assert_not_dev_cluster_rejects_loopback_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    # localhost vs 127.0.0.1 are the SAME local cluster — must still reject.
    monkeypatch.setattr(settings, "database_url", "postgresql://postgres:postgres@localhost:5432/ebull")
    with pytest.raises(RuntimeError):
        _assert_not_dev_cluster("postgresql://postgres:postgres@127.0.0.1:5432/postgres")


def test_assert_not_dev_cluster_accepts_different_port(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "database_url", "postgresql://postgres:postgres@localhost:5432/ebull")
    # 5433 is a different cluster — no raise.
    _assert_not_dev_cluster("postgresql://postgres:postgres@localhost:5433/postgres")
