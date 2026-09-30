"""#3518 — the isolated parse worker: timeout replaces the child, death and memory are retryable, the network is
blocked inside it (spec §4.3, §7). The extract functions are module-level so the spawn child can import them."""

from __future__ import annotations

import gzip
import os
import socket
import time
from pathlib import Path

from app.services.mdna_extraction import ParseOutcome
from app.services.mdna_parse_worker import ParseWorker

_AAPL_10K = Path(__file__).parent / "fixtures" / "sec" / "mdna" / "0000320193-25-000079.htm.gz"


def _sleep(*_a: str) -> ParseOutcome:
    time.sleep(60)
    return ParseOutcome("item_absent", detail="none")


def _die(*_a: str) -> ParseOutcome:
    os._exit(3)


def _oom(*_a: str) -> ParseOutcome:
    raise MemoryError


def _echo(family: str, html: str, accession: str, url: str) -> ParseOutcome:
    return ParseOutcome("parse_failed", detail=f"{family}|{len(html)}|{accession}|{url}")


def _resolve(*_a: str) -> ParseOutcome:
    try:
        socket.getaddrinfo("www.sec.gov", 443)
    except Exception as exc:  # noqa: BLE001
        return ParseOutcome("parse_failed", detail=type(exc).__name__)
    return ParseOutcome("parse_failed", detail="resolved")


def _connect(*_a: str) -> ParseOutcome:
    s = socket.socket()
    try:
        s.connect(("127.0.0.1", 9))
    except Exception as exc:  # noqa: BLE001
        return ParseOutcome("parse_failed", detail=type(exc).__name__)
    finally:
        s.close()
    return ParseOutcome("parse_failed", detail="connected")


def test_real_document_parses_in_the_child() -> None:
    with gzip.open(_AAPL_10K, "rt", newline="") as fh:
        html = fh.read()
    with ParseWorker() as worker:
        out = worker.parse("10-K", html, "0000320193-25-000079", "https://www.sec.gov/a/b.htm")
    assert (out.status, out.full_chars) == ("extracted", 18_015)


def test_timeout_terminates_and_replaces_the_child() -> None:
    with ParseWorker(timeout_s=1, extract=_sleep) as worker:
        first = worker.pid
        t0 = time.monotonic()
        out = worker.parse("10-K", "<html/>", "acc", "u")
        assert time.monotonic() - t0 < 30
        assert out == ParseOutcome("parse_failed", detail="timeout", retryable=True)
        assert worker.pid is not None and worker.pid != first
        assert worker.restarts == 1


def test_child_death_is_retryable_and_the_worker_keeps_serving() -> None:
    with ParseWorker(extract=_die) as worker:
        out = worker.parse("10-Q", "<html/>", "acc", "u")
        assert out == ParseOutcome("parse_failed", detail="worker_died", retryable=True)
        assert worker.restarts == 1
        assert worker.parse("10-Q", "<html/>", "acc", "u").detail == "worker_died"


def test_memory_error_in_the_child_is_retryable() -> None:
    with ParseWorker(extract=_oom) as worker:
        assert worker.parse("10-K", "<html/>", "acc", "u") == ParseOutcome(
            "parse_failed", detail="memory", retryable=True
        )


def test_one_child_serves_many_documents() -> None:
    with ParseWorker(extract=_echo) as worker:
        pid = worker.pid
        for i in range(3):
            assert worker.parse("10-K", "x" * i, f"a{i}", "u").detail == f"10-K|{i}|a{i}|u"
        assert worker.pid == pid and worker.restarts == 0


def test_dns_and_connect_are_blocked_in_the_child() -> None:
    with ParseWorker(extract=_resolve) as worker:
        assert worker.parse("10-K", "", "a", "u").detail == "NetworkBlocked"
    with ParseWorker(extract=_connect) as worker:
        assert worker.parse("10-K", "", "a", "u").detail == "NetworkBlocked"
