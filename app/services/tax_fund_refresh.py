"""Fund evidence capture and observations (#3616 slice 3d-3c-1, Designs 1-3): ``refresh_fund_evidence``.

Spec: docs/specs/metrics/2026-10-14-3616-slice3d3c1-fund-identity.md. The pure readers and decisions are
``app.services.tax_fund_evidence``; this step fetches the due public documents (SEC N-CEN, GLEIF, OpenFIGI keyless,
GOV.UK), assesses and searches the stored versions of HMRC's reporting-funds list, and records one observation per
fund security into the sql/492 tables. It runs after ``refresh_cover_instances`` in both invokers of
``run_tax_matching`` (Design 4). Matching reads none of it, so no character, run or figure changes.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import threading
import time
import zipfile
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import psycopg
from psycopg.types.json import Jsonb

from app.config import settings
from app.db.snapshot import snapshot_read
from app.providers.sec_rate_gate_holder import get_sec_rate_gate
from app.services.def14a_recipients import symbol_key
from app.services.tax_character import ETF_TYPE_ID
from app.services.tax_fund_evidence import (
    FUND_IDENTITY_POLICY_ID,
    CoverageState,
    DeadlineExceeded,
    Filing,
    IdentityOutcome,
    NcenDoc,
    consolidate,
    figi_jobs,
    figi_request_key,
    identity_outcome,
    ncen_sound,
    ncen_xml_url,
    newest_period,
    padded_cik,
    read_content_item,
    read_figi,
    read_gleif_isins,
    read_gleif_record,
    read_ncen,
    read_reporting_list,
    search_outcome,
    search_version,
)
from app.services.tax_matching import (  # pyright: ignore[reportPrivateUsage]
    _read_inputs,
    security_members,
)

logger = logging.getLogger(__name__)

#: Session advisory lock held for the whole step: int.from_bytes(b"taxfunds", "big").
FUND_EVIDENCE_LOCK_KEY = int.from_bytes(b"taxfunds", "big")
STEP_DEADLINE_S = 20 * 60
REQUEST_CAP_S = 60.0
REFRESH_AFTER = timedelta(days=7)
CURRENT_FOR = timedelta(days=30)
OPENFIGI_SPACING_S = 2.4

GLEIF = "https://api.gleif.org/api/v1/lei-records"
OPENFIGI = "https://api.openfigi.com/v3/mapping"
HMRC_CONTENT_KEY = "/government/publications/approved-offshore-reporting-funds"
HMRC_CONTENT_URL = "https://www.gov.uk/api/content" + HMRC_CONTENT_KEY

_MIB = 1024 * 1024
#: Each source's body limit (Design 1's table); a response is abandoned past it.
BODY_LIMITS: Mapping[str, int] = {
    "sec_ncen": 16 * _MIB,
    "gleif_record": _MIB,
    "gleif_isins": _MIB,
    "openfigi_mapping": _MIB,
    "hmrc_content_item": _MIB,
    "hmrc_reporting_list": 32 * _MIB,
}

#: The identity table's lines in order, ``candidate`` last: how far an outcome's evaluation reached.
_REACH: tuple[IdentityOutcome, ...] = (
    "cik_unestablished",
    "registration_metadata_conflict",
    "registration_unavailable",
    "registration_amended",
    "registration_unusable",
    "registration_final",
    "fund_listing_unmatched",
    "lei_record_unavailable",
    "lei_record_unsupported",
    "isin_unavailable",
    "isin_set_unsupported",
    "isin_set_unusable",
    "isin_mapping_unavailable",
    "isin_unresolved",
    "isin_unestablished",
    "candidate",
)


def _reached(outcome: IdentityOutcome, line: IdentityOutcome) -> bool:
    """True when the outcome's evaluation passed every line before ``line``, so ``line``'s input was evaluated."""
    return _REACH.index(outcome) >= _REACH.index(line)


@dataclass(frozen=True)
class Clock:
    """The wall clock for stored times and the monotonic clock for the deadline; injectable for tests."""

    now: Callable[[], datetime] = lambda: datetime.now(UTC)
    monotonic: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep


@dataclass(frozen=True)
class FundEvidenceReport:
    funds: int
    #: Fetches stored, by source.
    stored: Mapping[str, int]
    assessed: int
    searched: int
    #: ``(tax_security_id, outcome, search_outcome)`` per observation written.
    observations: tuple[tuple[int, str, str | None], ...]
    failures: tuple[str, ...]


@dataclass(frozen=True)
class _Fund:
    security_id: int
    member_ids: tuple[int, ...]
    symbol: str | None
    ciks: frozenset[str]
    filings: tuple[Filing, ...]


class _Deadline:
    """The step's absolute deadline on the monotonic clock (Design 1)."""

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.end = clock.monotonic() + STEP_DEADLINE_S

    def remaining(self) -> float:
        return self.end - self.clock.monotonic()

    def tick(self) -> None:
        if self.remaining() <= 0:
            raise DeadlineExceeded

    def request_cap(self) -> float:
        """A request's total time: the lesser of 60 s and the time remaining."""
        self.tick()
        return min(REQUEST_CAP_S, self.remaining())

    def wait(self, seconds: float) -> None:
        """A rate-gate wait, capped like a request: one longer than the cap is the deadline."""
        if seconds <= 0:
            return
        if seconds > self.request_cap():
            raise DeadlineExceeded
        self.clock.sleep(seconds)


@dataclass(frozen=True)
class _Got:
    body: bytes | None
    error: str | None
    status: int | None = None
    rate_reset: str | None = None


_CUT = "request past its time cap"


def _request(
    client: httpx.Client,
    deadline: _Deadline,
    method: str,
    url: str,
    *,
    limit: int,
    headers: Mapping[str, str] | None = None,
    content: bytes | None = None,
    gate: Callable[[], None] | None = None,
) -> _Got:
    """One request, streamed and abandoned past ``limit`` or the request cap; no redirects. A non-2xx (a 3xx
    included) or a transport error is a failed fetch. Raises ``DeadlineExceeded`` when the step deadline cuts it.

    The exchange, and the rate-gate wait ``gate`` before it, run on a worker thread joined for at most the cap, so
    the cap bounds the request's total time: httpx's ``timeout`` bounds each network operation only, and a peer
    sending a chunk just inside it could otherwise hold a request open for many caps. An abandoned worker is
    cancelled: it sends nothing if it was still waiting on the gate, and stops reading at its next chunk."""
    cap = deadline.request_cap()
    clock = deadline.clock
    stop = clock.monotonic() + cap
    cancelled = threading.Event()
    result: list[_Got] = []

    def exchange() -> _Got:
        if gate is not None:
            gate()
            if cancelled.is_set():
                return _Got(None, _CUT)
        try:
            with client.stream(
                method, url, headers=headers, content=content, timeout=cap, follow_redirects=False
            ) as response:
                reset = response.headers.get("ratelimit-reset")
                if not 200 <= response.status_code < 300:
                    return _Got(None, f"http {response.status_code}", response.status_code, reset)
                chunks: list[bytes] = []
                size = 0
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > limit:
                        return _Got(None, f"body past {limit} bytes", response.status_code, reset)
                    chunks.append(chunk)
                    if cancelled.is_set() or clock.monotonic() > stop:
                        return _Got(None, _CUT, response.status_code, reset)
                return _Got(b"".join(chunks), None, response.status_code, reset)
        except httpx.HTTPError as exc:
            return _Got(None, f"transport {type(exc).__name__}")

    def work() -> None:
        try:
            result.append(exchange())
        except Exception as exc:  # recorded as a failed fetch, never lost on the worker thread
            result.append(_Got(None, f"transport {type(exc).__name__}: {exc}"))

    worker = threading.Thread(target=work, name="tax-fund-fetch", daemon=True)
    worker.start()
    worker.join(cap)
    if not result:
        cancelled.set()
        deadline.tick()
        return _Got(None, _CUT)
    got = result[0]
    if got.body is None and got.status is None or got.error == _CUT:
        deadline.tick()
    return got


def _store(
    conn: psycopg.Connection[Any],
    source: str,
    request_key: str,
    url: str,
    fetched_at: datetime,
    body: bytes,
    *,
    parent: int | None = None,
    rate_reset: str | None = None,
) -> int:
    """The document and its fetch, committed together."""
    sha = hashlib.sha256(body).hexdigest()
    with conn.transaction():
        conn.execute(
            "INSERT INTO tax_fund_documents (body_sha256, body) VALUES (%s, %s) ON CONFLICT DO NOTHING", (sha, body)
        )
        row = conn.execute(
            "INSERT INTO tax_fund_fetches (source, request_key, url, fetched_at, body_sha256, parent_fetch_id, "
            "rate_reset) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING fetch_id",
            (source, request_key, url, fetched_at, sha, parent, rate_reset),
        ).fetchone()
    assert row is not None
    return int(row[0])


def _newest(
    conn: psycopg.Connection[Any], source: str, request_key: str, at: datetime | None
) -> tuple[int, datetime] | None:
    """The greatest ``(fetched_at, fetch_id)`` for the key, at or before ``at`` (any time when None)."""
    row = conn.execute(
        "SELECT fetch_id, fetched_at FROM tax_fund_fetches WHERE source = %s AND request_key = %s "
        "AND (%s::timestamptz IS NULL OR fetched_at <= %s::timestamptz) "
        "ORDER BY fetched_at DESC, fetch_id DESC LIMIT 1",
        (source, request_key, at, at),
    ).fetchone()
    return (int(row[0]), row[1]) if row is not None else None


def _body(conn: psycopg.Connection[Any], fetch_id: int) -> bytes:
    row = conn.execute(
        "SELECT d.body FROM tax_fund_fetches f JOIN tax_fund_documents d USING (body_sha256) WHERE f.fetch_id = %s",
        (fetch_id,),
    ).fetchone()
    assert row is not None
    return bytes(row[0])


def _due(newest: tuple[int, datetime] | None, now: datetime) -> bool:
    """A mutable source is due with no stored fetch, or its newest over 7 days old or dated after now."""
    return newest is None or newest[1] < now - REFRESH_AFTER or newest[1] > now


def _current(newest: tuple[int, datetime] | None, at: datetime) -> bool:
    return newest is not None and newest[1] >= at - CURRENT_FOR


# --- population --------------------------------------------------------------------------------------------------


def _read_funds(conn: psycopg.Connection[Any]) -> tuple[int, list[_Fund]] | None:
    """The effective generation's fund securities (every member type 6) and their members' N-CEN rows, read as
    ``refresh_cover_instances`` reads its selection, in one snapshot; None with no effective generation."""
    with snapshot_read(conn):
        inputs = _read_inputs(conn, bodies=False)
        if inputs is None:
            return None
        members = security_members(inputs)
        funds = {s: m for s, m in members.items() if inputs.characters[s].types == frozenset({ETF_TYPE_ID})}
        rows = conn.execute(
            "SELECT instrument_id, filing_type, provider_filing_id, filing_date, report_date, primary_document_url "
            "FROM filing_events WHERE provider = 'sec' AND instrument_id = ANY(%s) "
            "AND filing_type IN ('N-CEN', 'N-CEN/A')",
            (sorted({i for m in funds.values() for i in m}),),
        ).fetchall()
    by_member: dict[int, list[Filing]] = {}
    for iid, form, accession, filed, report, url in rows:
        by_member.setdefault(iid, []).append(Filing(accession, form, filed, report, url))
    out = [
        _Fund(
            security_id=s,
            member_ids=m,
            symbol=inputs.characters[s].symbol,
            ciks=inputs.characters[s].ciks,
            filings=tuple(f for i in m for f in by_member.get(i, [])),
        )
        for s, m in sorted(funds.items())
    ]
    return inputs.evidence_run_id, out


@dataclass(frozen=True)
class _Target:
    cik: str
    newest: Filing
    url: str


def _ncen_target(fund: _Fund, now: datetime) -> _Target | None:
    """The fund's newest N-CEN and its raw XML URL, when Design 1 lets it be fetched."""
    padded = {padded_cik(c) for c in fund.ciks}
    if len(padded) != 1 or None in padded:
        return None
    cik = str(next(iter(padded)))
    filings = consolidate(fund.filings, now.date())
    if filings is None:
        return None
    _period, newest, _amendments = newest_period(filings)
    url = ncen_xml_url(newest, cik) if newest is not None else None
    return _Target(cik, newest, url) if newest is not None and url is not None else None


# --- the step ----------------------------------------------------------------------------------------------------


class _Run:
    def __init__(self, conn: psycopg.Connection[Any], client: httpx.Client, clock: Clock) -> None:
        self.conn, self.client, self.clock = conn, client, clock
        self.deadline = _Deadline(clock)
        self.failures: list[str] = []
        self.stored: Counter[str] = Counter()
        self.assessed = 0
        self.searched = 0
        self.figi_stopped = False
        self.figi_last: float | None = None

    def fetch(
        self,
        source: str,
        request_key: str,
        method: str,
        url: str,
        sound: Callable[[bytes, datetime], bool],
        *,
        headers: Mapping[str, str] | None = None,
        content: bytes | None = None,
        parent: int | None = None,
        gate: Callable[[], None] | None = None,
    ) -> int | None:
        """Fetch and store one sound response; a failed fetch stores nothing and is recorded."""
        got = _request(
            self.client,
            self.deadline,
            method,
            url,
            limit=BODY_LIMITS[source],
            headers=headers,
            content=content,
            gate=gate,
        )
        fetched_at = self.clock.now()
        if source == "openfigi_mapping" and got.status == 429:
            self.figi_stopped = True
        if got.body is None:
            self.failures.append(f"{source} {request_key}: {got.error}")
            return None
        # An unsound body is deliberately not stored (spec Design 1, "Soundness": a failed fetch stores nothing). A
        # stored fetch is evidence: an observation evaluates a source's newest fetch with no fallback, so storing a
        # transient error page would turn it into an outcome, and sql/492's trigger refuses an unsound content item.
        # Soundness is checked before persistence; an accepted body is passed to the store unchanged, byte for byte.
        # Same contract as `refresh_cover_instances` (3d-3a Design 1). The failure itself is recorded in the job note.
        if not sound(got.body, fetched_at):
            self.failures.append(f"{source} {request_key}: unsound body")
            return None
        try:
            fetch_id = _store(
                self.conn, source, request_key, url, fetched_at, got.body, parent=parent, rate_reset=got.rate_reset
            )
        except psycopg.Error as exc:
            self.failures.append(f"{source} {request_key}: refused by the store: {type(exc).__name__}: {exc}")
            return None
        self.stored[source] += 1
        return fetch_id


def refresh_fund_evidence(
    conn: psycopg.Connection[Any], client: httpx.Client | None = None, clock: Clock | None = None
) -> FundEvidenceReport:
    """Capture the due fund evidence, assess and search the stored list versions, then record one observation per
    fund security (spec Designs 1-3), on an autocommit connection.

    Each stored fetch, assessment and search commits on its own; the observations commit in one transaction after
    every fetch, assessment and search has been tried. A failure never rolls back stored evidence: it is returned in
    ``failures``, which the job turns into a failed step. Another invocation holding the lock fails this one at once.
    """
    if not conn.autocommit:
        raise ValueError("refresh_fund_evidence needs an autocommit connection: each stored fetch commits alone")
    if client is None:
        with httpx.Client(follow_redirects=False) as owned:
            return refresh_fund_evidence(conn, owned, clock)
    locked = conn.execute("SELECT pg_try_advisory_lock(%s)", (FUND_EVIDENCE_LOCK_KEY,)).fetchone()
    if locked is None or not locked[0]:
        return FundEvidenceReport(0, {}, 0, 0, (), ("concurrent refresh",))
    try:
        return _refresh(_Run(conn, client, clock or Clock()))
    finally:
        conn.execute("SELECT pg_advisory_unlock(%s)", (FUND_EVIDENCE_LOCK_KEY,))


def _refresh(run: _Run) -> FundEvidenceReport:
    read = _read_funds(run.conn)
    if read is None:
        return FundEvidenceReport(0, {}, 0, 0, (), ())
    evidence_run_id, funds = read
    try:
        _fetch_all(run, funds)
    except DeadlineExceeded:
        run.failures.append("deadline: fetches still due were not tried")
    observed_at = run.clock.now()
    identities = {f.security_id: _identity(run.conn, f, observed_at) for f in funds}
    keys = {
        (got["isin"], tuple(got["name_keys"])) for outcome, got, _ids in identities.values() if outcome == "candidate"
    }
    try:
        _assess_and_search(run, keys)
    except DeadlineExceeded:
        run.failures.append("deadline: list assessment or search interrupted")
    observations = _observe(run, funds, identities, observed_at, evidence_run_id)
    return FundEvidenceReport(
        len(funds), dict(run.stored), run.assessed, run.searched, observations, tuple(run.failures)
    )


def _fetch_all(run: _Run, funds: list[_Fund]) -> None:
    """Every due fetch, in chain order: each source's due set reads what the earlier sources stored."""
    conn, now = run.conn, run.clock.now()
    targets = {f.security_id: t for f in funds if (t := _ncen_target(f, now)) is not None}
    for target in {t.newest.accession: t for t in targets.values()}.values():
        if _newest(conn, "sec_ncen", target.newest.accession, None) is not None:
            continue  # once per accession
        run.fetch(
            "sec_ncen",
            target.newest.accession,
            "GET",
            target.url,
            lambda body, _at: ncen_sound(body),
            headers={"User-Agent": settings.sec_user_agent},
            gate=get_sec_rate_gate().acquire,
        )

    docs: dict[int, NcenDoc] = {}
    for sid, target in targets.items():
        stored = _newest(conn, "sec_ncen", target.newest.accession, None)
        doc = read_ncen(_body(conn, stored[0]), target.cik) if stored is not None else None
        if doc is not None:
            docs[sid] = doc
    for lei in sorted({d.lei for d in docs.values()}):
        now = run.clock.now()
        if _due(_newest(conn, "gleif_record", lei, None), now):
            run.fetch(
                "gleif_record",
                lei,
                "GET",
                f"{GLEIF}/{lei}",
                lambda body, _at, lei=lei: read_gleif_record(body, lei) is not None,
            )
        if _due(_newest(conn, "gleif_isins", lei, None), run.clock.now()):
            url = str(httpx.URL(f"{GLEIF}/{lei}/isins", params={"page[size]": "200", "page[number]": "1"}))
            run.fetch("gleif_isins", lei, "GET", url, lambda body, _at: read_gleif_isins(body) is not None)

    jobs: dict[str, tuple[list[str], str]] = {}
    for f in funds:
        doc = docs.get(f.security_id)
        if doc is None or f.symbol is None:
            continue
        keyed = [symbol_key(ticker) for _mic, ticker in doc.listings]
        if keyed.count(f.symbol) != 1:
            continue
        stored = _newest(conn, "gleif_isins", doc.lei, run.clock.now())
        parsed = read_gleif_isins(_body(conn, stored[0])) if stored is not None else None
        if parsed is None or not 1 <= len(parsed.entries) <= 10:
            continue
        mic = doc.listings[keyed.index(f.symbol)][0]
        jobs[figi_request_key(parsed.entries, mic)] = (list(parsed.entries), mic)
    for key, (entries, mic) in sorted(jobs.items()):
        if run.figi_stopped or not _due(_newest(conn, "openfigi_mapping", key, None), run.clock.now()):
            continue
        if run.figi_last is not None:
            run.deadline.wait(OPENFIGI_SPACING_S - (run.clock.monotonic() - run.figi_last))
        run.figi_last = run.clock.monotonic()
        run.fetch(
            "openfigi_mapping",
            key,
            "POST",
            OPENFIGI,
            lambda body, _at, n=len(entries): read_figi(body, n) is not None,
            headers={"Content-Type": "application/json"},
            content=json.dumps(figi_jobs(entries, mic)).encode(),
        )

    now = run.clock.now()
    if not any(_identity(conn, f, now)[0] == "candidate" for f in funds):
        return
    if _due(_newest(conn, "hmrc_content_item", HMRC_CONTENT_KEY, None), now):
        run.fetch(
            "hmrc_content_item",
            HMRC_CONTENT_KEY,
            "GET",
            HMRC_CONTENT_URL,
            lambda body, at: read_content_item(body, at) is not None,
        )
    content = _newest(conn, "hmrc_content_item", HMRC_CONTENT_KEY, run.clock.now())
    if content is None or _child(conn, content[0], None) is not None:
        return  # an older childless content fetch is superseded and never retried
    item = read_content_item(_body(conn, content[0]), content[1])
    if item is None:  # stored only when sound; a backstop
        run.failures.append(f"hmrc_content_item fetch {content[0]}: no longer sound")
        return
    run.fetch(
        "hmrc_reporting_list",
        item.attachment_url,
        "GET",
        item.attachment_url,
        lambda body, _at: len(body) == item.file_size and zipfile.is_zipfile(io.BytesIO(body)),
        parent=content[0],
    )


def _child(conn: psycopg.Connection[Any], content_fetch_id: int, at: datetime | None) -> int | None:
    row = conn.execute(
        "SELECT fetch_id FROM tax_fund_fetches WHERE parent_fetch_id = %s "
        "AND (%s::timestamptz IS NULL OR fetched_at <= %s::timestamptz)",
        (content_fetch_id, at, at),
    ).fetchone()
    return int(row[0]) if row is not None else None


def _identity(
    conn: psycopg.Connection[Any], fund: _Fund, at: datetime
) -> tuple[IdentityOutcome, dict[str, Any], dict[str, int]]:
    """The candidate identity at ``at`` from the newest stored fetches, with no fallback: the N-CEN of the newest
    period, and the *current* GLEIF and OpenFIGI fetches (Design 2). Returns the outcome, its evaluated inputs and
    the fetch ids of the inputs it reached."""
    ncen: dict[str, bytes] = {}
    records: dict[str, bytes] = {}
    isin_sets: dict[str, bytes] = {}
    mappings: dict[str, bytes] = {}
    ids: dict[str, int] = {}
    target = _ncen_target(fund, at)
    if target is not None and (n := _newest(conn, "sec_ncen", target.newest.accession, at)) is not None:
        ncen[target.newest.accession] = _body(conn, n[0])
        ids["ncen_fetch_id"] = n[0]
        doc = read_ncen(ncen[target.newest.accession], target.cik)
        if doc is not None:
            if _current(r := _newest(conn, "gleif_record", doc.lei, at), at) and r is not None:
                records[doc.lei] = _body(conn, r[0])
                ids["record_fetch_id"] = r[0]
            if _current(i := _newest(conn, "gleif_isins", doc.lei, at), at) and i is not None:
                isin_sets[doc.lei] = _body(conn, i[0])
                ids["isins_fetch_id"] = i[0]
                parsed = read_gleif_isins(isin_sets[doc.lei])
                keyed = [symbol_key(ticker) for _mic, ticker in doc.listings]
                if parsed is not None and fund.symbol is not None and keyed.count(fund.symbol) == 1:
                    key = figi_request_key(parsed.entries, doc.listings[keyed.index(fund.symbol)][0])
                    if _current(m := _newest(conn, "openfigi_mapping", key, at), at) and m is not None:
                        mappings[key] = _body(conn, m[0])
                        ids["figi_fetch_id"] = m[0]
    outcome, got = identity_outcome(
        ciks=fund.ciks,
        symbol=fund.symbol,
        filings=consolidate(fund.filings, at.date()),
        ncen=ncen,
        records=records,
        isin_sets=isin_sets,
        mappings=mappings,
    )
    reach: dict[str, IdentityOutcome] = {
        "ncen_fetch_id": "registration_amended",
        "record_fetch_id": "lei_record_unsupported",
        "isins_fetch_id": "isin_set_unsupported",
        "figi_fetch_id": "isin_unresolved",
    }
    kept = {col: fid for col, fid in ids.items() if _reached(outcome, reach[col])}
    return outcome, got, kept


def _assess_and_search(run: _Run, keys: set[tuple[str, tuple[str, ...]]]) -> None:
    """Assess every pending list version strictly in ascending fetch order against the largest earlier usable one,
    then search each usable version once per search key (Design 2). Each assessment and each search commits on its
    own; a version's rows are released before the next is read."""
    conn, policy = run.conn, FUND_IDENTITY_POLICY_ID
    versions = conn.execute(
        "SELECT l.fetch_id, v.usable, v.data_rows, v.baseline_rows FROM tax_fund_fetches l "
        "LEFT JOIN tax_fund_list_versions v ON v.list_fetch_id = l.fetch_id AND v.policy_id = %s "
        "WHERE l.source = 'hmrc_reporting_list' ORDER BY l.fetch_id",
        (policy,),
    ).fetchall()
    largest: int | None = None
    for fetch_id, usable, data_rows, baseline in versions:
        rows: tuple[tuple[str, ...], ...] | None = None
        if usable is None:
            assessed = read_reporting_list(_body(conn, fetch_id), largest, run.deadline.tick)
            rows = assessed.rows
            usable, data_rows, baseline = rows is not None, len(rows) if rows is not None else None, largest
            conn.execute(
                "INSERT INTO tax_fund_list_versions (list_fetch_id, policy_id, usable, data_rows, refusal, "
                "baseline_rows, assessed_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (fetch_id, policy, usable, data_rows, assessed.refusal, baseline, run.clock.now()),
            )
            run.assessed += 1
        if not usable or data_rows is None:
            continue
        largest = max(largest or 0, data_rows)
        done = {
            (isin, tuple(names))
            for isin, names in conn.execute(
                "SELECT isin, name_keys FROM tax_fund_list_searches WHERE list_fetch_id = %s AND policy_id = %s",
                (fetch_id, policy),
            ).fetchall()
        }
        missing = sorted(keys - done)
        if missing and rows is None:
            again = read_reporting_list(_body(conn, fetch_id), baseline, run.deadline.tick)
            if again.rows is None or len(again.rows) != data_rows:
                run.failures.append(f"list version {fetch_id}: re-read differs from its assessment ({again.refusal})")
                continue
            rows = again.rows
        for isin, names in missing:
            assert rows is not None
            found = search_version(isin, names, rows, run.deadline.tick)
            conn.execute(
                "INSERT INTO tax_fund_list_searches (policy_id, isin, name_keys, list_fetch_id, searched_at, "
                "identifier_count, identifier_in_text_count, near_identifier_count, name_count, examples) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    policy,
                    isin,
                    list(names),
                    fetch_id,
                    run.clock.now(),
                    found.counts["identifier"],
                    found.counts["identifier_in_text"],
                    found.counts["near_identifier"],
                    found.counts["name"],
                    Jsonb([dict(e) for e in found.examples]),
                ),
            )
            run.searched += 1


def _list_search(
    conn: psycopg.Connection[Any], isin: str, names: list[str], observed_at: datetime
) -> tuple[str, int | None, int | None, list[dict[str, Any]], str | None]:
    """A candidate's list search at ``observed_at``: (search outcome, content fetch, its child list, coverage,
    refusal). Read inside the observations' transaction, so the coverage is the state the trigger checks."""
    policy = FUND_IDENTITY_POLICY_ID
    content = _newest(conn, "hmrc_content_item", HMRC_CONTENT_KEY, observed_at)
    child = _child(conn, content[0], observed_at) if content is not None else None
    states: list[tuple[int, CoverageState]] = [
        (int(fid), state)
        for fid, state in conn.execute(
            "SELECT l.fetch_id, CASE WHEN v.list_fetch_id IS NULL THEN 'pending' WHEN NOT v.usable THEN 'unusable' "
            "WHEN s.list_fetch_id IS NULL THEN 'usable_unsearched' ELSE 'searched' END "
            "FROM tax_fund_fetches l "
            "LEFT JOIN tax_fund_list_versions v ON v.list_fetch_id = l.fetch_id AND v.policy_id = %s "
            "LEFT JOIN tax_fund_list_searches s ON s.list_fetch_id = l.fetch_id AND s.policy_id = %s "
            "AND s.isin = %s AND s.name_keys = %s "
            "WHERE l.source = 'hmrc_reporting_list' AND l.fetched_at <= %s ORDER BY l.fetch_id",
            (policy, policy, isin, names, observed_at),
        ).fetchall()
    ]
    searches: list[dict[str, int]] = [
        {"identifier": a, "identifier_in_text": b, "near_identifier": c, "name": d}
        for a, b, c, d in conn.execute(
            "SELECT s.identifier_count, s.identifier_in_text_count, s.near_identifier_count, s.name_count "
            "FROM tax_fund_list_searches s JOIN tax_fund_fetches l ON l.fetch_id = s.list_fetch_id "
            "WHERE s.policy_id = %s AND s.isin = %s AND s.name_keys = %s AND l.fetched_at <= %s",
            (policy, isin, names, observed_at),
        ).fetchall()
    ]
    state_of = dict(states)
    content_current = _current(content, observed_at)
    child_usable = child is not None and state_of.get(child) in ("usable_unsearched", "searched")
    outcome = search_outcome(
        searches, [s for _f, s in states], content_current=content_current, child_list_usable=child_usable
    )
    refusal = None
    if outcome == "unavailable":
        refusal = (
            "no content fetch"
            if content is None
            else "content fetch not current"
            if not content_current
            else "no list for the content fetch"
            if child is None
            else f"list {state_of.get(child)}"
            if not child_usable
            else "pending version"
            if "pending" in state_of.values()
            else "unsearched version"
        )
    coverage = [{"list_fetch_id": fid, "state": state} for fid, state in states]
    return outcome, (content[0] if content else None), child, coverage, refusal


def _observe(
    run: _Run,
    funds: Iterable[_Fund],
    identities: Mapping[int, tuple[IdentityOutcome, dict[str, Any], dict[str, int]]],
    observed_at: datetime,
    evidence_run_id: int,
) -> tuple[tuple[int, str, str | None], ...]:
    """One observation per fund security, all in one transaction (Design 3)."""
    written: list[tuple[int, str, str | None]] = []
    conn = run.conn
    with conn.transaction():
        for fund in funds:
            outcome, got, ids = identities[fund.security_id]
            search: tuple[str | None, int | None, int | None, list[dict[str, Any]] | None, str | None] = (
                None,
                None,
                None,
                None,
                got.get("refusal"),
            )
            if outcome == "candidate":
                search = _list_search(conn, got["isin"], got["name_keys"], observed_at)
            search_out, content_id, list_id, coverage, refusal = search
            listings = got.get("listings")
            conn.execute(
                "INSERT INTO tax_fund_observations (observed_at, policy_id, evidence_run_id, tax_security_id, "
                "member_ids, symbol, outcome, cik, newest_period, amendment_accessions, ncen_accession, "
                "ncen_report_date, ncen_source_url, ncen_fetch_id, lei, record_fetch_id, isins_fetch_id, "
                "figi_fetch_id, listings, mic, isin_set, trustee_countries, registrant_country, short_period, isin, "
                "name_keys, search_outcome, content_fetch_id, list_fetch_id, refusal, evaluated_at, coverage) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    observed_at,
                    FUND_IDENTITY_POLICY_ID,
                    evidence_run_id,
                    fund.security_id,
                    list(fund.member_ids),
                    fund.symbol,
                    outcome,
                    got.get("cik"),
                    got.get("newest_period"),
                    got.get("amendment_accessions"),
                    got.get("ncen_accession"),
                    got.get("ncen_report_date"),
                    got.get("ncen_source_url"),
                    ids.get("ncen_fetch_id"),
                    got.get("lei"),
                    ids.get("record_fetch_id"),
                    ids.get("isins_fetch_id"),
                    ids.get("figi_fetch_id"),
                    Jsonb(listings) if listings is not None else None,
                    got.get("mic"),
                    got.get("isin_set"),
                    got.get("trustee_countries"),
                    got.get("registrant_country"),
                    got.get("short_period"),
                    got.get("isin"),
                    got.get("name_keys"),
                    search_out,
                    content_id,
                    list_id,
                    refusal,
                    run.clock.now(),
                    Jsonb(coverage) if coverage is not None else None,
                ),
            )
            written.append((fund.security_id, outcome, search_out))
    return tuple(written)
