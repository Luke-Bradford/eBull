"""Isolated parse worker for ``periodic_report_sections`` (#3518 spec §4.3).

One long-lived child process (``multiprocessing`` spawn) with DNS and socket connects disabled, fed one document
at a time over a pipe. The parent waits ``timeout_s``; on timeout it terminates the child (``terminate``, then
``kill`` after 5 s) and starts a new one. Outcome precedence: timeout -> ``parse_failed`` / ``timeout``; child
death -> ``worker_died``; ``MemoryError`` anywhere -> ``memory``. All three are retryable and share the fetch
backoff. Everything else is the extraction module's mapping (``app/services/mdna_extraction.py``).

Why a process, not a thread: edgartools parses with lxml, which a thread cannot interrupt, and the largest documents
take minutes (spec §8.3). A killed process returns its memory; a stuck thread does not.
"""

from __future__ import annotations

import logging
import multiprocessing
import socket
import warnings
from collections.abc import Callable
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from types import TracebackType
from typing import Any, Final

from app.services.mdna_extraction import ParseOutcome, extract_mdna

logger = logging.getLogger(__name__)

PARSE_TIMEOUT_S: Final = 300  # spec §4.4 (basis: the §8.3 census parse-time distribution)
_KILL_GRACE_S: Final = 5

Extract = Callable[[str, str, str, str], ParseOutcome]


class NetworkBlocked(RuntimeError):
    """Raised by every DNS lookup or socket connect inside the worker child."""


def block_network() -> None:
    """Make DNS lookups and socket connects raise in THIS process. The extraction is offline by contract
    (spec §8.2: 0 network attempts); this turns an accidental fetch into a recorded parse failure."""

    def _deny(*_args: Any, **_kwargs: Any) -> Any:
        raise NetworkBlocked("network access is disabled in the MD&A parse worker")

    socket.getaddrinfo = _deny  # type: ignore[assignment]
    socket.gethostbyname = _deny  # type: ignore[assignment]
    socket.create_connection = _deny  # type: ignore[assignment]
    socket.socket.connect = _deny  # type: ignore[method-assign,assignment]
    socket.socket.connect_ex = _deny  # type: ignore[method-assign,assignment]


def _child_main(conn: Connection, extract: Extract) -> None:
    block_network()
    warnings.simplefilter("ignore")
    # edgartools logs its legacy-parser fallback at WARNING per document; the outcome is what we record.
    logging.getLogger("edgar").setLevel(logging.ERROR)
    while True:
        try:
            job = conn.recv()
        except EOFError:
            return
        if job is None:
            return
        try:
            outcome = extract(*job)
        except MemoryError:
            outcome = ParseOutcome("parse_failed", detail="memory", retryable=True)
        del job
        try:
            conn.send(outcome)
        except MemoryError:
            conn.send(ParseOutcome("parse_failed", detail="memory", retryable=True))


class ParseWorker:
    """Parent handle on the child. Not thread-safe: the job processes one accession at a time."""

    def __init__(self, *, timeout_s: float = PARSE_TIMEOUT_S, extract: Extract = extract_mdna) -> None:
        self._timeout_s = timeout_s
        self._extract = extract
        self._ctx = multiprocessing.get_context("spawn")
        self._proc: BaseProcess | None = None
        self._conn: Connection | None = None
        self.restarts = 0

    def __enter__(self) -> ParseWorker:
        self._start()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()

    @property
    def pid(self) -> int | None:
        return self._proc.pid if self._proc is not None else None

    def _start(self) -> None:
        parent, child = self._ctx.Pipe(duplex=True)
        proc = self._ctx.Process(target=_child_main, args=(child, self._extract), daemon=True, name="mdna-parse")
        proc.start()
        child.close()
        self._proc, self._conn = proc, parent

    def _stop(self) -> None:
        proc, conn = self._proc, self._conn
        self._proc = self._conn = None
        if conn is not None:
            conn.close()
        if proc is None:
            return
        if proc.is_alive():
            proc.terminate()
            proc.join(_KILL_GRACE_S)
        if proc.is_alive():
            proc.kill()
            proc.join()
        proc.close()

    def _replace(self) -> None:
        self._stop()
        self._start()
        self.restarts += 1

    def close(self) -> None:
        conn, proc = self._conn, self._proc
        if conn is not None and proc is not None and proc.is_alive():
            try:
                conn.send(None)
                proc.join(_KILL_GRACE_S)
            except OSError, ValueError:
                pass
        self._stop()

    def parse(self, family: str, html: str, accession: str, source_url: str) -> ParseOutcome:
        if self._conn is None:
            self._start()
        conn = self._conn
        assert conn is not None
        try:
            conn.send((family, html, accession, source_url))
            if not conn.poll(self._timeout_s):
                logger.warning("mdna parse of %s exceeded %ss; replacing the worker", accession, self._timeout_s)
                self._replace()
                return ParseOutcome("parse_failed", detail="timeout", retryable=True)
            outcome = conn.recv()
        except MemoryError:
            self._replace()
            return ParseOutcome("parse_failed", detail="memory", retryable=True)
        except EOFError, OSError:
            logger.warning("mdna parse worker died on %s; replacing it", accession)
            self._replace()
            return ParseOutcome("parse_failed", detail="worker_died", retryable=True)
        if not isinstance(outcome, ParseOutcome):
            raise TypeError(f"parse worker returned {type(outcome).__name__}")
        return outcome
