"""Reviewed fund determinations applied (#3616 slice 3d-3c-2, Design 2): ``refresh_fund_determinations``.

Spec: docs/specs/metrics/2026-10-14-3616-slice3d3c2-fund-determination.md. The format, the rules and the outcome order
are the pure ``app.services.tax_fund_determination``; this step runs them against the sql/493 tables. It runs after
``refresh_fund_evidence`` in both invokers of ``run_tax_matching`` and, per invocation:

1. inserts and commits a ``running`` run;
2. loads every committed determination file and its authority snapshots, validates both, stores them and records each
   as loaded by the run (any failure, or a stored id with another hash, fails the run and raises);
3. fetches each cited SEC document with no stored fetch of the version's hash, and each CIK's filing list when none
   is stored or its newest is over 7 days old; a failed fetch stores nothing and is reported;
4. takes the cutoff, commits the unreviewed governing accessions the newest filing list shows, then records one
   observation per fund security with its document rows and marks the run ``succeeded``, in one transaction.

Matching reads none of it, so no character, run or figure changes.
"""

from __future__ import annotations

import hashlib
import logging
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import psycopg
from psycopg.types.json import Jsonb

from app.config import settings
from app.providers.sec_rate_gate_holder import get_sec_rate_gate
from app.services.tax_fund_determination import (  # pyright: ignore[reportPrivateUsage]
    ACCEPTED_IDENTITY_POLICY_ID,
    FUND_DETERMINATION_POLICY_ID,
    SUBMISSIONS_REFRESH,
    SUBMISSIONS_URL,
    Decision,
    Determination,
    Fetch,
    FundObservation,
    _hash_or_none,
    _is_xml,
    authority_refusal,
    authority_text,
    decide,
    offshore_fund,
    parse_determination,
    unreviewed_accessions,
)
from app.services.tax_fund_evidence import DeadlineExceeded
from app.services.tax_fund_refresh import (  # pyright: ignore[reportPrivateUsage]
    Clock,
    _Deadline,
    _newest,
    _read_funds,
    _request,
    _store,
)

logger = logging.getLogger(__name__)

#: The committed determination files; each authority snapshot is ``authorities/<authority_id>.txt``.
DETERMINATIONS_DIR = Path(__file__).parent / "tax_fund_determinations"
#: Session advisory lock held for the whole step: int.from_bytes(b"taxfdets", "big").
FUND_DETERMINATION_LOCK_KEY = int.from_bytes(b"taxfdets", "big")
#: A fetched SEC document or filing list is abandoned past this size (spec Design 2 step 3).
FETCH_LIMIT = 8 * 1024 * 1024


class DeterminationRunFailed(RuntimeError):
    """The run was set ``failed`` with this reason (spec Design 2 step 2, or a step that raised)."""


@dataclass(frozen=True)
class DeterminationReport:
    run_id: int | None
    #: Determination ids the run loaded.
    loaded: tuple[str, ...]
    #: Fetches stored, by source.
    stored: Mapping[str, int]
    #: Unreviewed accessions this run recorded for the first time.
    discovered: int
    #: ``(tax_security_id, outcome, refusal)`` per observation written.
    observations: tuple[tuple[int, str, str | None], ...]
    failures: tuple[str, ...]


@dataclass(frozen=True)
class _Loaded:
    det: Determination
    file_sha256: str
    #: authority_id → the snapshot's bytes, validated against the determination.
    snapshots: Mapping[str, bytes]


def _load_files(directory: Path) -> list[_Loaded]:
    """Every determination file and its authority snapshots, validated (Design 1); raises on any failure."""
    loaded: list[_Loaded] = []
    for path in sorted(directory.glob("*.json")):
        raw = path.read_bytes()
        det, errs = parse_determination(raw)
        if det is None:
            raise DeterminationRunFailed(f"{path.name}: format errors {errs}")
        snapshots: dict[str, bytes] = {}
        for authority in det["authorities"]:
            aid = authority["authority_id"]  # validated as [a-z0-9_]+, so a plain file name
            try:
                body = (directory / "authorities" / f"{aid}.txt").read_bytes()
            except OSError as exc:
                raise DeterminationRunFailed(f"{path.name}: authority {aid} snapshot unreadable: {exc}") from exc
            refusal = authority_refusal(authority, body)
            if refusal is not None:
                raise DeterminationRunFailed(f"{path.name}: authority {aid} snapshot refused: {refusal}")
            snapshots[aid] = body
        loaded.append(_Loaded(det, hashlib.sha256(raw).hexdigest(), snapshots))
    ciks = [x.det["cik"] for x in loaded]
    if len(ciks) != len(set(ciks)):
        raise DeterminationRunFailed(f"policy v1 holds one version per CIK: {sorted(ciks)}")
    return loaded


def _store_loads(conn: psycopg.Connection[Any], run_id: int, loaded: Sequence[_Loaded], now: datetime) -> None:
    """Store each determination, its snapshots and authority links, and record it as loaded by the run, in one
    transaction. A stored id with another hash, or a link to another snapshot, raises."""
    with conn.transaction():
        for x in loaded:
            did = x.det["determination_id"]
            conn.execute(
                "INSERT INTO tax_fund_determinations (determination_id, policy_id, cik, version, file_sha256, body, "
                "loaded_at) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (did, x.det["policy_id"], x.det["cik"], x.det["version"], x.file_sha256, Jsonb(x.det), now),
            )
            stored = conn.execute(
                "SELECT file_sha256 FROM tax_fund_determinations WHERE determination_id = %s", (did,)
            ).fetchone()
            if stored is None:
                raise DeterminationRunFailed(f"{did}: its CIK is stored under another determination")
            if stored[0] != x.file_sha256:
                raise DeterminationRunFailed(f"{did}: stored with file hash {stored[0]}, the file is {x.file_sha256}")
            for aid, body in x.snapshots.items():
                sha = hashlib.sha256(body).hexdigest()
                conn.execute(
                    "INSERT INTO tax_fund_authority_snapshots (content_sha256, body) VALUES (%s, %s) "
                    "ON CONFLICT DO NOTHING",
                    (sha, body),
                )
                conn.execute(
                    "INSERT INTO tax_fund_determination_authorities (determination_id, authority_id, content_sha256) "
                    "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                    (did, aid, sha),
                )
                linked = conn.execute(
                    "SELECT content_sha256 FROM tax_fund_determination_authorities "
                    "WHERE determination_id = %s AND authority_id = %s",
                    (did, aid),
                ).fetchone()
                if linked is None or linked[0] != sha:
                    raise DeterminationRunFailed(f"{did}: authority {aid} is linked to another snapshot")
            conn.execute(
                "INSERT INTO tax_fund_determination_run_loads (run_id, determination_id) VALUES (%s, %s)",
                (run_id, did),
            )


def _fetches(conn: psycopg.Connection[Any], source: str, key: str) -> list[Fetch]:
    rows = conn.execute(
        "SELECT f.fetch_id, f.fetched_at, d.body FROM tax_fund_fetches f JOIN tax_fund_documents d USING (body_sha256) "
        "WHERE f.source = %s AND f.request_key = %s ORDER BY f.fetched_at, f.fetch_id",
        (source, key),
    ).fetchall()
    return [Fetch(int(fid), at, bytes(body)) for fid, at, body in rows]


def _newest_fetch(conn: psycopg.Connection[Any], source: str, key: str, at: datetime) -> Fetch | None:
    """The newest fetch by ``(fetched_at, fetch_id)`` with ``fetched_at`` ≤ ``at``, with its body."""
    newest = _newest(conn, source, key, at)
    if newest is None:
        return None
    return next(f for f in _fetches(conn, source, key) if f.fetch_id == newest[0])


class _Step:
    def __init__(self, conn: psycopg.Connection[Any], client: httpx.Client, clock: Clock) -> None:
        self.conn, self.client, self.clock = conn, client, clock
        self.deadline = _Deadline(clock)
        self.failures: list[str] = []
        self.stored: Counter[str] = Counter()

    def fetch(self, source: str, url: str) -> None:
        """One SEC GET through the fund step's gate and user agent: HTTP 200 only, at most 8 MiB, strict UTF-8 unless
        the path ends ``.xml``. A failed fetch stores nothing and is recorded."""
        got = _request(
            self.client,
            self.deadline,
            "GET",
            url,
            limit=FETCH_LIMIT,
            headers={"User-Agent": settings.sec_user_agent},
            gate=get_sec_rate_gate().acquire,
        )
        fetched_at = self.clock.now()
        if got.body is None or got.status != 200:
            self.failures.append(f"{source} {url}: {got.error or f'http {got.status}'}")
            return
        if not _is_xml(url):
            try:
                got.body.decode("utf-8", errors="strict")
            except UnicodeDecodeError:
                self.failures.append(f"{source} {url}: not UTF-8")
                return
        try:
            _store(self.conn, source, url, url, fetched_at, got.body)
        except psycopg.Error as exc:
            self.failures.append(f"{source} {url}: refused by the store: {type(exc).__name__}: {exc}")
            return
        self.stored[source] += 1


def _fetch_due(step: _Step, loaded: Sequence[_Loaded]) -> None:
    """Each cited document with no stored fetch of the version's hash (once per URL), then each CIK's filing list
    when none is stored or its newest is over 7 days old (elapsed time)."""
    conn, tried = step.conn, set[str]()
    for x in loaded:
        for d in x.det["documents"]:
            url = d["url"]
            if url in tried:
                continue
            tried.add(url)
            fetches = _fetches(conn, "sec_governing_document", url)
            if not any(_hash_or_none(url, f.body) == d["content_sha256"] for f in fetches):
                step.fetch("sec_governing_document", url)
        url = SUBMISSIONS_URL.format(cik=x.det["cik"])
        newest = _newest(conn, "sec_submissions", url, None)
        if newest is None or step.clock.now() - newest[1] > SUBMISSIONS_REFRESH:
            step.fetch("sec_submissions", url)


def _discover(conn: psycopg.Connection[Any], run_id: int, loaded: Sequence[_Loaded], cutoff: datetime) -> int:
    """Record, in its own committed transaction, each unreviewed governing accession the newest filing list at the
    cutoff shows (spec "Unreviewed accessions"); returns how many were new."""
    new = 0
    with conn.transaction():
        for x in loaded:
            sub = _newest_fetch(conn, "sec_submissions", SUBMISSIONS_URL.format(cik=x.det["cik"]), cutoff)
            if sub is None:
                continue
            for found in unreviewed_accessions(x.det, sub):
                cur = conn.execute(
                    "INSERT INTO tax_fund_determination_unreviewed (determination_id, accession, form, filed, "
                    "first_run_id, submissions_fetch_id) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    (x.det["determination_id"], found.accession, found.form, found.filed, run_id, sub.fetch_id),
                )
                new += cur.rowcount
    return new


def _fund_observation(
    conn: psycopg.Connection[Any], security_id: int, fund_observed_at: datetime | None
) -> FundObservation | None:
    if fund_observed_at is None:
        return None
    row = conn.execute(
        "SELECT observation_id, outcome, cik, lei, isin, mic, symbol, observed_at FROM tax_fund_observations "
        "WHERE tax_security_id = %s AND observed_at = %s AND policy_id = %s",
        (security_id, fund_observed_at, ACCEPTED_IDENTITY_POLICY_ID),
    ).fetchone()
    if row is None:
        return None
    oid, outcome, cik, lei, isin, mic, symbol, observed_at = row
    if observed_at != fund_observed_at:  # the writer's own check (spec Design 2 step 4)
        raise DeterminationRunFailed(f"fund observation {oid} is not at {fund_observed_at.isoformat()}")
    return FundObservation(int(oid), outcome, cik, lei, isin, mic, symbol)


def _check(decision: Decision, det: Determination | None, fetches: Mapping[int, Fetch], cutoff: datetime) -> None:
    """The writer-side checks the DB does not make (spec Design 4): every referenced fetch is at or before the
    cutoff, each document fetch's content hash is the version's, and the offshore pair is ``offshore_fund(body)``."""
    referenced = [*(decision.document_fetch_ids or {}).values()]
    if decision.submissions_fetch_id is not None:
        referenced.append(decision.submissions_fetch_id)
    for fid in referenced:
        if fetches[fid].fetched_at > cutoff:
            raise DeterminationRunFailed(f"fetch {fid} is after the cutoff")
    if decision.outcome != "determined":
        return
    assert det is not None and decision.document_fetch_ids is not None and decision.offshore is not None
    for d in det["documents"]:
        body = fetches[decision.document_fetch_ids[d["doc_id"]]].body
        if _hash_or_none(d["url"], body) != d["content_sha256"]:
            raise DeterminationRunFailed(f"{d['doc_id']}: its fetch is not the version's content")
    if decision.offshore != offshore_fund(det):
        raise DeterminationRunFailed(f"{det['determination_id']}: offshore pair is not offshore_fund(body)")


def _observe(
    conn: psycopg.Connection[Any],
    run_id: int,
    loaded: Sequence[_Loaded],
    fund_observed_at: datetime | None,
    cutoff: datetime,
) -> tuple[tuple[int, str, str | None], ...]:
    """One observation per fund security of the effective generation, with its document rows, and the run set
    ``succeeded`` with the cutoff, in one transaction (spec Design 2 step 4)."""
    read = _read_funds(conn)
    securities = [f.security_id for f in read[1]] if read is not None else []
    by_cik = {x.det["cik"]: x for x in loaded}
    determinations = {cik: x.det for cik, x in by_cik.items()}
    written: list[tuple[int, str, str | None]] = []
    with conn.transaction():
        recorded: dict[str, set[str]] = {}
        for did, accession in conn.execute(
            "SELECT determination_id, accession FROM tax_fund_determination_unreviewed "
            "WHERE determination_id = ANY(%s)",
            ([x.det["determination_id"] for x in loaded],),
        ).fetchall():
            recorded.setdefault(did, set()).add(accession)
        for security_id in securities:
            obs = _fund_observation(conn, security_id, fund_observed_at)
            x = by_cik.get(obs.cik or "") if obs is not None and obs.outcome == "candidate" else None
            document_fetches: dict[str, list[Fetch]] = {}
            submissions: Fetch | None = None
            if x is not None:
                document_fetches = {
                    d["url"]: _fetches(conn, "sec_governing_document", d["url"]) for d in x.det["documents"]
                }
                submissions = _newest_fetch(conn, "sec_submissions", SUBMISSIONS_URL.format(cik=x.det["cik"]), cutoff)
            decision = decide(
                obs,
                determinations,
                document_fetches,
                {aid: authority_text(raw) for aid, raw in x.snapshots.items()} if x is not None else {},
                submissions,
                recorded.get(x.det["determination_id"], set()) if x is not None else set(),
                cutoff,
            )
            by_id = {f.fetch_id: f for fs in document_fetches.values() for f in fs}
            if submissions is not None:
                by_id[submissions.fetch_id] = submissions
            _check(decision, x.det if x is not None else None, by_id, cutoff)
            row = conn.execute(
                "INSERT INTO tax_fund_determination_observations (run_id, policy_id, tax_security_id, "
                "fund_observation_id, determination_id, outcome, refusal, submissions_fetch_id, offshore_fund, "
                "offshore_basis) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING observation_id",
                (
                    run_id,
                    FUND_DETERMINATION_POLICY_ID,
                    security_id,
                    obs.fund_observation_id if obs is not None else None,
                    decision.determination_id,
                    decision.outcome,
                    decision.refusal,
                    decision.submissions_fetch_id,
                    decision.offshore.answer if decision.offshore is not None else None,
                    decision.offshore.basis if decision.offshore is not None else None,
                ),
            ).fetchone()
            assert row is not None
            for doc_id, fetch_id in sorted((decision.document_fetch_ids or {}).items()):
                conn.execute(
                    "INSERT INTO tax_fund_determination_documents (observation_id, doc_id, fetch_id) "
                    "VALUES (%s, %s, %s)",
                    (row[0], doc_id, fetch_id),
                )
            written.append((security_id, decision.outcome, decision.refusal))
        conn.execute(
            "UPDATE tax_fund_determination_runs SET status = 'succeeded', cutoff = %s WHERE run_id = %s",
            (cutoff, run_id),
        )
    return tuple(written)


def refresh_fund_determinations(
    conn: psycopg.Connection[Any],
    fund_observed_at: datetime | None,
    *,
    job_run_id: int | None = None,
    client: httpx.Client | None = None,
    clock: Clock | None = None,
    directory: Path = DETERMINATIONS_DIR,
) -> DeterminationReport:
    """Run Design 2 once, on an autocommit connection. ``fund_observed_at`` is the ``observed_at`` the fund step wrote
    in the same job run, or None if it failed or wrote nothing.

    Fetch failures are returned in ``failures`` (the run still succeeds, and its outcomes show what was missing);
    the job turns them into a failed step. A load failure, or any exception after the run started, sets the run
    ``failed`` and raises ``DeterminationRunFailed``. Another invocation holding the lock fails this one at once,
    with no run."""
    if not conn.autocommit:
        raise ValueError("refresh_fund_determinations needs an autocommit connection: each stored fetch commits alone")
    if client is None:
        with httpx.Client(follow_redirects=False) as owned:
            return refresh_fund_determinations(
                conn, fund_observed_at, job_run_id=job_run_id, client=owned, clock=clock, directory=directory
            )
    locked = conn.execute("SELECT pg_try_advisory_lock(%s)", (FUND_DETERMINATION_LOCK_KEY,)).fetchone()
    if locked is None or not locked[0]:
        return DeterminationReport(None, (), {}, 0, (), ("concurrent refresh",))
    try:
        return _refresh(_Step(conn, client, clock or Clock()), fund_observed_at, job_run_id, directory)
    finally:
        conn.execute("SELECT pg_advisory_unlock(%s)", (FUND_DETERMINATION_LOCK_KEY,))


def _refresh(
    step: _Step, fund_observed_at: datetime | None, job_run_id: int | None, directory: Path
) -> DeterminationReport:
    conn = step.conn
    row = conn.execute(
        "INSERT INTO tax_fund_determination_runs (job_run_id, fund_observed_at, started_at, status) "
        "VALUES (%s, %s, %s, 'running') RETURNING run_id",
        (job_run_id, fund_observed_at, step.clock.now()),
    ).fetchone()
    assert row is not None
    run_id = int(row[0])
    try:
        loaded = _load_files(directory)
        _store_loads(conn, run_id, loaded, step.clock.now())
        try:
            _fetch_due(step, loaded)
        except DeadlineExceeded:
            step.failures.append("deadline: fetches still due were not tried")
        cutoff = step.clock.now()
        discovered = _discover(conn, run_id, loaded, cutoff)
        observations = _observe(conn, run_id, loaded, fund_observed_at, cutoff)
    except Exception as exc:
        reason = str(exc) if isinstance(exc, DeterminationRunFailed) else f"{type(exc).__name__}: {exc}"
        try:
            conn.execute(
                "UPDATE tax_fund_determination_runs SET status = 'failed', failure = %s WHERE run_id = %s",
                (reason, run_id),
            )
        except psycopg.Error:
            logger.error("tax fund determinations: could not record run %s as failed", run_id, exc_info=True)
        raise DeterminationRunFailed(f"run {run_id} failed: {reason}") from exc
    return DeterminationReport(
        run_id,
        tuple(x.det["determination_id"] for x in loaded),
        dict(step.stored),
        discovered,
        observations,
        tuple(step.failures),
    )
