"""#3381 slice 2: the daily eToro top-investor cohort recorder.

Spec: ``docs/proposals/etl/2026-09-25-3381-investor-cohort-recorder.md``. eToro serves no history of a
public investor's positions, so the recording clock is the dataset. Each call appends ONE
``etoro_investor_snapshots`` row, failed ones included, and in the same transaction every ranking row, the
cohort's new members, one fetch row per attempted member and the parsed top-level positions of every ok
fetch (``sql/425_etoro_investor_cohort.sql``).

The cohort rule is fixed by construction (no published rule exists) and frozen as ``COHORT_RULE_VERSION``:
the first ``COHORT_TOP_N`` rows of the popular-investor ranking by copiers. Membership is STICKY across every
rule version, so an investor who falls out of the top N, goes private or closes is still observed (as
``unavailable``), never silently dropped. Changing a SELECTION constant must mint a new version; the
operational constants (pacing, ceilings) are not part of it.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Final, Protocol

import httpx
import psycopg

from app.providers.implementations.etoro_social import SocialResponse

logger = logging.getLogger(__name__)

# --- selection constants: part of the rule version -------------------------------------------------
COHORT_RULE_VERSION: Final = "pi-lt2y-copiers-top100-sticky-v1"
COHORT_TOP_N: Final = 100
RANKING_PAGE_SIZE: Final = 100
#: ⚠ The walk sorts by ``username``, NOT by copiers. Measured 2026-09-25: paging ``sort=-copiers`` repeated a
#: tied row across the page 4/5 boundary (so skipped another) on 7 of 11 walks, and multi-field sorts were
#: worse; ``sort=username`` — a unique key — paged cleanly on 9 of 9. Ranking by copiers is done locally over
#: the complete census (``_rank_key``), which also makes selection reproducible from the stored rows.
RANKING_PARAMS: Final[Mapping[str, str | int | bool]] = {
    "period": "LastTwoYears",
    "popularInvestor": "true",
    "sort": "username",
    "pageSize": RANKING_PAGE_SIZE,
}

# --- operational constants: NOT part of the rule version -------------------------------------------
#: Sized so a full fetch pass fits in ~20 minutes at the 1.1 s lane pacing. Above it the run refuses
#: loudly (ranking and membership still commit).
MAX_TRACKED: Final = 1000
#: Members erroring in a row before the run aborts: bounds the quota spent against a systemic failure.
MAX_CONSECUTIVE_ERRORS: Final = 10
MAX_RANKING_PAGES: Final = 50
#: Whole-walk attempts when a walk comes back inconsistent (the population moved mid-walk).
RANKING_ATTEMPTS: Final = 3
RECORDER_VERSION: Final = "1"

STATUS_COMPLETE: Final = "complete"
STATUS_PARTIAL: Final = "partial"
STATUS_FAILED: Final = "failed"

OUTCOME_OK: Final = "ok"
OUTCOME_UNAVAILABLE: Final = "unavailable"
OUTCOME_ERROR: Final = "error"

#: "This profile is not publicly served" — private, closed or renamed.
_UNAVAILABLE_STATUSES: Final = frozenset({403, 404})


class SocialSource(Protocol):
    def get_rankings_page(self, params: dict[str, str | int | bool], page: int) -> SocialResponse: ...

    def get_live_portfolio(self, username: str) -> SocialResponse: ...


class InvestorSnapshotPartial(RuntimeError):
    """Raised AFTER a ``partial`` snapshot committed, so the job run records a failure."""


class RankingInconsistent(ValueError):
    """A ranking walk that did not describe one population: worth re-walking, unlike a malformed page."""


class InvestorSnapshotRefused(RuntimeError):
    """The run stopped on a recorder rule (ceiling, consecutive errors, nothing fetched)."""


@dataclass(frozen=True)
class InvestorSnapshotResult:
    snapshot_id: int
    status: str
    ranking_recorded: int
    investors_expected: int
    investors_fetched: int
    investors_unavailable: int
    investors_errored: int
    positions_recorded: int


@dataclass(frozen=True)
class RankRow:
    rank: int
    cid: int
    username: str
    observed_at: datetime
    copiers: int | None
    risk_score: int | None
    gain: int | float | None
    raw: Mapping[str, Any]


@dataclass(frozen=True)
class Ranking:
    #: Ordered by ``_rank_key``; ``RankRow.rank`` is the 1-based position in that order.
    rows: tuple[RankRow, ...]
    envelopes: tuple[Mapping[str, Any], ...]
    total_items: int
    attempts: int = 1


@dataclass(frozen=True)
class Member:
    cid: int
    username: str
    username_from_ranking: bool
    selected_today: bool


@dataclass(frozen=True)
class Fetch:
    member: Member
    observed_at: datetime
    outcome: str
    http_status: int | None
    raw: object
    realized_credit_pct: int | float | None = None
    unrealized_credit_pct: int | float | None = None
    position_count: int | None = None
    social_trade_count: int | None = None
    positions: tuple[tuple[object, ...], ...] = ()


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"expected an integer, got {value!r}")
    return value


def _metric(value: object) -> int | float | None:
    """An optional served metric: a JSON number, else ``None`` (the raw row keeps whatever was served)."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return value


def _number(value: object) -> int | float | None:
    """A parsed position field: a JSON number or null; anything else malforms the member's body."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"expected a number, got {value!r}")
    return value


def _rank_key(row: RankRow) -> tuple[bool, int, int]:
    """Copiers descending (a missing count last), then ``cid`` — a total order, so ties cannot move a member."""
    return (row.copiers is None, -(row.copiers or 0), row.cid)


def _walk_ranking(source: SocialSource) -> Ranking:
    """One walk over every ranking page. ``RankingInconsistent`` for a population that moved mid-walk;
    plain ``ValueError`` for a malformed page."""
    rows: list[RankRow] = []
    envelopes: list[Mapping[str, Any]] = []
    seen: set[int] = set()
    total: int | None = None
    page = 1
    while True:
        if page > MAX_RANKING_PAGES:
            raise ValueError(f"eToro rankings still hasNext after {MAX_RANKING_PAGES} pages")
        response = source.get_rankings_page(dict(RANKING_PARAMS), page)
        body = response.body
        if not isinstance(body, dict):
            raise ValueError(f"eToro rankings page {page} is not an object")
        results = body.get("results")
        pagination = body.get("pagination")
        if not isinstance(results, list) or not isinstance(pagination, dict):
            raise ValueError(f"eToro rankings page {page} lacks results/pagination")
        if pagination.get("page") != page:
            raise ValueError(f"eToro rankings returned page {pagination.get('page')!r}, expected {page}")
        page_total = pagination.get("totalItems")
        if isinstance(page_total, bool) or not isinstance(page_total, int) or page_total < 0:
            raise ValueError(f"eToro rankings returned invalid totalItems {page_total!r}")
        if total is None:
            total = page_total
        elif page_total != total:
            raise RankingInconsistent(f"eToro rankings totalItems changed during pagination: {total} -> {page_total}")
        envelopes.append(pagination)
        for item in results:
            if not isinstance(item, dict):
                raise ValueError("eToro rankings row is not an object")
            cid = _int_or_none(item.get("cid"))
            username = item.get("username")
            if cid is None or not isinstance(username, str) or not username:
                raise ValueError(f"eToro rankings row lacks cid/username: cid={item.get('cid')!r}")
            if cid in seen:
                raise RankingInconsistent(f"eToro rankings repeated cid {cid}")
            seen.add(cid)
            copiers = _metric(item.get("copiers"))
            risk = _metric(item.get("riskScore"))
            rows.append(
                RankRow(
                    rank=0,
                    cid=cid,
                    username=username,
                    observed_at=response.observed_at,
                    copiers=copiers if isinstance(copiers, int) else None,
                    risk_score=risk if isinstance(risk, int) else None,
                    gain=_metric(item.get("gain")),
                    raw=item,
                )
            )
        has_next = pagination.get("hasNext")
        if not isinstance(has_next, bool):
            raise ValueError(f"eToro rankings returned invalid hasNext {has_next!r}")
        if not has_next:
            break
        if not results:
            raise ValueError(f"eToro rankings page {page} is empty but reports hasNext")
        page += 1
    if total is None:  # pragma: no cover - the first page always assigns it
        raise RuntimeError("eToro rankings pagination did not initialise")
    if len(rows) != total:
        raise RankingInconsistent(f"eToro rankings incomplete: received {len(rows)} of {total} rows")
    if not rows:
        raise ValueError("eToro rankings returned zero popular investors")
    ranked = tuple(replace(row, rank=i) for i, row in enumerate(sorted(rows, key=_rank_key), start=1))
    return Ranking(rows=ranked, envelopes=tuple(envelopes), total_items=total)


def fetch_ranking(source: SocialSource) -> Ranking:
    """A complete, consistent ranking census, re-walked up to ``RANKING_ATTEMPTS`` times (spec §Failure
    semantics). A malformed page is not retried: it would fail the same way again."""
    for attempt in range(1, RANKING_ATTEMPTS + 1):
        try:
            return replace(_walk_ranking(source), attempts=attempt)
        except RankingInconsistent as exc:
            if attempt == RANKING_ATTEMPTS:
                raise
            logger.warning("eToro rankings walk %d inconsistent, re-walking: %s", attempt, exc)
    raise AssertionError("unreachable")  # pragma: no cover


def build_fetch_set(
    ranking: Ranking, ledger_cids: frozenset[int], last_usernames: Mapping[int, str]
) -> tuple[Member, ...]:
    """Today's top ``COHORT_TOP_N`` by ``_rank_key`` ∪ every ledger member, in rank order then cid.

    A ledger member absent from today's ranking with no recorded username cannot be addressed; that would
    mean a ledger row was written without the ranking row that selected it, which the single-transaction
    write rules out.
    """
    selected = {row.cid for row in ranking.rows[:COHORT_TOP_N]}
    by_cid = {row.cid: row for row in ranking.rows}
    members: list[Member] = []
    for row in ranking.rows:
        if row.cid in selected or row.cid in ledger_cids:
            members.append(Member(row.cid, row.username, True, row.cid in selected))
    for cid in sorted(ledger_cids - by_cid.keys()):
        username = last_usernames.get(cid)
        if username is None:
            raise RuntimeError(f"cohort member cid {cid} has no recorded username")
        members.append(Member(cid, username, False, False))
    return tuple(members)


def _position_row(item: object) -> tuple[object, ...]:
    if not isinstance(item, dict):
        raise ValueError("position is not an object")
    position_id = _int_or_none(item.get("positionId"))
    instrument_id = _int_or_none(item.get("instrumentId"))
    is_buy = item.get("isBuy")
    if position_id is None or instrument_id is None or not isinstance(is_buy, bool):
        raise ValueError(f"position lacks positionId/instrumentId/isBuy: {item.get('positionId')!r}")
    trailing = item.get("trailingStopLoss")
    opened = item.get("openTimestamp")
    if opened is not None and not isinstance(opened, str):
        raise ValueError(f"openTimestamp is not a string: {opened!r}")
    return (
        position_id,
        instrument_id,
        is_buy,
        _int_or_none(item.get("leverage")),
        _number(item.get("investmentPct")),
        _number(item.get("openRate")),
        None if opened is None else datetime.fromisoformat(opened),
        _number(item.get("netProfit")),
        _number(item.get("stopLossRate")),
        _number(item.get("takeProfitRate")),
        trailing if isinstance(trailing, bool) else None,
        _int_or_none(item.get("socialTradeId")),
        _int_or_none(item.get("parentPositionId")),
    )


def classify_response(member: Member, response: SocialResponse) -> Fetch:
    """One served response → one fetch row. A malformed 2xx body is an ``error`` for THIS member only."""
    if response.status in _UNAVAILABLE_STATUSES:
        return Fetch(member, response.observed_at, OUTCOME_UNAVAILABLE, response.status, response.body)
    if not 200 <= response.status < 300:
        return Fetch(member, response.observed_at, OUTCOME_ERROR, response.status, response.body)
    body = response.body
    try:
        if not isinstance(body, dict):
            raise ValueError("live portfolio body is not an object")
        positions = body.get("positions")
        social = body.get("socialTrades", [])
        if not isinstance(positions, list) or not isinstance(social, list):
            raise ValueError("live portfolio lacks positions/socialTrades lists")
        rows = tuple(_position_row(item) for item in positions)
        if len({row[0] for row in rows}) != len(rows):
            raise ValueError("live portfolio repeats a positionId")
    except ValueError as exc:
        logger.warning("investor %s (cid %d): malformed live portfolio: %s", member.username, member.cid, exc)
        return Fetch(member, response.observed_at, OUTCOME_ERROR, response.status, body)
    return Fetch(
        member,
        response.observed_at,
        OUTCOME_OK,
        response.status,
        body,
        realized_credit_pct=_metric(body.get("realizedCreditPct")),
        unrealized_credit_pct=_metric(body.get("unrealizedCreditPct")),
        position_count=len(rows),
        social_trade_count=len(social),
        positions=rows,
    )


def fetch_member(source: SocialSource, member: Member, clock: Callable[[], datetime]) -> Fetch:
    """Fetch one member. A 401 raises (the whole run is unauthorised); everything else becomes a row."""
    try:
        response = source.get_live_portfolio(member.username)
    except httpx.HTTPStatusError as exc:  # retries exhausted on 429/5xx
        if exc.response.status_code == 401:
            raise
        response = SocialResponse(exc.response.status_code, exc.response.text, clock())
    except httpx.TransportError as exc:
        return Fetch(member, clock(), OUTCOME_ERROR, None, f"{type(exc).__name__}: {exc}")
    if response.status == 401:
        raise httpx.HTTPStatusError(
            "eToro live portfolio answered 401",
            request=httpx.Request("GET", "live-portfolio"),
            response=httpx.Response(401),
        )
    return classify_response(member, response)


# --- persistence ------------------------------------------------------------------------------------

_INSERT_SNAPSHOT_SQL: Final = """
INSERT INTO etoro_investor_snapshots (
    started_at, finished_at, status, cohort_rule_version, request_params, ranking_envelopes, ranking_attempts,
    ranking_pages,
    ranking_total_items, ranking_recorded, investors_expected, investors_attempted, investors_fetched,
    investors_unavailable, investors_errored, error
) VALUES (
    %(started_at)s, %(finished_at)s, %(status)s, %(cohort_rule_version)s, %(request_params)s::jsonb,
    %(ranking_envelopes)s::jsonb, %(ranking_attempts)s, %(ranking_pages)s, %(ranking_total_items)s,
    %(ranking_recorded)s,
    %(investors_expected)s, %(investors_attempted)s, %(investors_fetched)s, %(investors_unavailable)s,
    %(investors_errored)s, %(error)s
)
RETURNING snapshot_id
"""

_COPY_RANKINGS_SQL: Final = (
    "COPY etoro_investor_rankings (snapshot_id, rank, cid, username, observed_at, copiers, risk_score, gain, raw) "
    "FROM STDIN"
)
_COPY_FETCHES_SQL: Final = (
    "COPY etoro_investor_fetches (snapshot_id, cid, username, username_from_ranking, observed_at, selected_today, "
    "outcome, http_status, realized_credit_pct, unrealized_credit_pct, position_count, social_trade_count, raw) "
    "FROM STDIN"
)
_COPY_POSITIONS_SQL: Final = (
    "COPY etoro_investor_positions (snapshot_id, cid, observed_at, position_id, instrument_id, is_buy, leverage, "
    "investment_pct, open_rate, open_timestamp, net_profit, stop_loss_rate, take_profit_rate, trailing_stop_loss, "
    "social_trade_id, parent_position_id) FROM STDIN"
)
_INSERT_COHORT_SQL: Final = """
INSERT INTO etoro_investor_cohort (cohort_rule_version, cid, first_snapshot_id, first_selected_at)
VALUES (%s, %s, %s, %s)
ON CONFLICT (cohort_rule_version, cid) DO NOTHING
"""
_LEDGER_SQL: Final = "SELECT DISTINCT cid FROM etoro_investor_cohort"
# Rankings as well as fetches: a run that aborts mid-fetch still commits its ledger rows, whose members
# may have no fetch row yet — but every ledger row is written with the ranking row that selected it.
_LAST_USERNAMES_SQL: Final = """
SELECT DISTINCT ON (cid) cid, username
FROM (
    SELECT cid, username, observed_at FROM etoro_investor_fetches
    UNION ALL
    SELECT cid, username, observed_at FROM etoro_investor_rankings
) AS seen
ORDER BY cid, observed_at DESC
"""


def _json(value: object) -> str:
    # allow_nan=False: a non-JSON value refuses the write instead of storing an unparseable raw.
    return json.dumps(value, allow_nan=False, sort_keys=True, default=str)


def _read_ledger(conn: psycopg.Connection[Any]) -> tuple[frozenset[int], dict[int, str]]:
    with conn.transaction():
        ledger = frozenset(int(row[0]) for row in conn.execute(_LEDGER_SQL).fetchall())
        usernames = {int(row[0]): str(row[1]) for row in conn.execute(_LAST_USERNAMES_SQL).fetchall()}
    return ledger, usernames


@dataclass
class _Run:
    """Everything collected so far — written whole whatever the outcome."""

    ranking: Ranking | None = None
    members: tuple[Member, ...] | None = None
    fetches: list[Fetch] | None = None


def _write(
    conn: psycopg.Connection[Any],
    run: _Run,
    *,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    params_json: str,
    error: str | None,
) -> int:
    ranking = run.ranking
    fetches = run.fetches if run.members is not None and run.fetches is not None else None
    counts: dict[str, int | None] = dict.fromkeys(
        (
            "investors_expected",
            "investors_attempted",
            "investors_fetched",
            "investors_unavailable",
            "investors_errored",
        )
    )
    if fetches is not None and run.members is not None:
        counts = {
            "investors_expected": len(run.members),
            "investors_attempted": len(fetches),
            "investors_fetched": sum(f.outcome == OUTCOME_OK for f in fetches),
            "investors_unavailable": sum(f.outcome == OUTCOME_UNAVAILABLE for f in fetches),
            "investors_errored": sum(f.outcome == OUTCOME_ERROR for f in fetches),
        }
    with conn.transaction():
        row = conn.execute(
            _INSERT_SNAPSHOT_SQL,
            {
                "started_at": started_at,
                "finished_at": max(started_at, finished_at),
                "status": status,
                "cohort_rule_version": COHORT_RULE_VERSION,
                "request_params": params_json,
                "ranking_envelopes": None if ranking is None else _json(list(ranking.envelopes)),
                "ranking_attempts": None if ranking is None else ranking.attempts,
                "ranking_pages": None if ranking is None else len(ranking.envelopes),
                "ranking_total_items": None if ranking is None else ranking.total_items,
                "ranking_recorded": None if ranking is None else len(ranking.rows),
                **counts,
                "error": error,
            },
        ).fetchone()
        if row is None:  # pragma: no cover - INSERT … RETURNING always returns the row
            raise RuntimeError("etoro_investor_snapshots insert returned no row")
        snapshot_id = int(row[0])
        if ranking is None:
            return snapshot_id
        with conn.cursor() as cur:
            with cur.copy(_COPY_RANKINGS_SQL) as copy:
                for r in ranking.rows:
                    copy.write_row(
                        (
                            snapshot_id,
                            r.rank,
                            r.cid,
                            r.username,
                            r.observed_at,
                            r.copiers,
                            r.risk_score,
                            r.gain,
                            _json(r.raw),
                        )
                    )
            cur.executemany(
                _INSERT_COHORT_SQL,
                [(COHORT_RULE_VERSION, r.cid, snapshot_id, r.observed_at) for r in ranking.rows[:COHORT_TOP_N]],
            )
            if fetches:
                with cur.copy(_COPY_FETCHES_SQL) as copy:
                    for f in fetches:
                        copy.write_row(
                            (
                                snapshot_id,
                                f.member.cid,
                                f.member.username,
                                f.member.username_from_ranking,
                                f.observed_at,
                                f.member.selected_today,
                                f.outcome,
                                f.http_status,
                                f.realized_credit_pct,
                                f.unrealized_credit_pct,
                                f.position_count,
                                f.social_trade_count,
                                _json(f.raw),
                            )
                        )
                with cur.copy(_COPY_POSITIONS_SQL) as copy:
                    for f in fetches:
                        for p in f.positions:
                            copy.write_row((snapshot_id, f.member.cid, f.observed_at, *p))
    return snapshot_id


def record_investor_snapshot(
    conn: psycopg.Connection[Any],
    source: SocialSource,
    *,
    request_params: Mapping[str, object],
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> InvestorSnapshotResult:
    """Collect and append one investor-cohort snapshot (spec §Failure semantics).

    Any abort commits a ``failed`` header carrying everything collected so far, then re-raises the ORIGINAL
    exception so ``classify_exception`` sees its type. A ``partial`` snapshot commits, then raises
    ``InvestorSnapshotPartial`` outside the failure path, so no second header is written.
    """
    started_at = clock()
    params_json = _json({**request_params, "ranking_params": RANKING_PARAMS, "recorder_version": RECORDER_VERSION})
    run = _Run()
    try:
        ledger, last_usernames = _read_ledger(conn)
        ranking = run.ranking = fetch_ranking(source)
        members = run.members = build_fetch_set(ranking, ledger, last_usernames)
        fetches: list[Fetch] = []
        run.fetches = fetches
        if len(members) > MAX_TRACKED:
            raise InvestorSnapshotRefused(f"fetch set {len(members)} exceeds MAX_TRACKED {MAX_TRACKED}")
        consecutive_errors = 0
        for member in members:
            fetch = fetch_member(source, member, clock)
            fetches.append(fetch)
            consecutive_errors = consecutive_errors + 1 if fetch.outcome == OUTCOME_ERROR else 0
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                raise InvestorSnapshotRefused(f"{consecutive_errors} consecutive member fetches errored")
        fetched = sum(f.outcome == OUTCOME_OK for f in fetches)
        if fetched == 0:
            raise InvestorSnapshotRefused(f"no member fetched ok out of {len(fetches)} attempted")
        errored = sum(f.outcome == OUTCOME_ERROR for f in fetches)
        status = STATUS_PARTIAL if errored else STATUS_COMPLETE
        snapshot_id = _write(
            conn, run, started_at=started_at, finished_at=clock(), status=status, params_json=params_json, error=None
        )
    except Exception as exc:
        _record_failure(conn, run, started_at=started_at, finished_at=clock(), params_json=params_json, exc=exc)
        raise
    result = InvestorSnapshotResult(
        snapshot_id=snapshot_id,
        status=status,
        ranking_recorded=len(ranking.rows),
        investors_expected=len(members),
        investors_fetched=fetched,
        investors_unavailable=sum(f.outcome == OUTCOME_UNAVAILABLE for f in fetches),
        investors_errored=errored,
        positions_recorded=sum(len(f.positions) for f in fetches),
    )
    if status == STATUS_PARTIAL:
        raise InvestorSnapshotPartial(
            f"investor snapshot {snapshot_id} committed partial: {errored} of {len(fetches)} member fetches errored"
        )
    return result


def _record_failure(
    conn: psycopg.Connection[Any],
    run: _Run,
    *,
    started_at: datetime,
    finished_at: datetime,
    params_json: str,
    exc: BaseException,
) -> None:
    """Commit a ``failed`` header with everything collected; failing that, a bare one. Best-effort only —
    never allowed to replace the error the caller re-raises."""
    error = f"{type(exc).__name__}: {exc}"
    for attempt in (run, _Run()):
        try:
            snapshot_id = _write(
                conn,
                attempt,
                started_at=started_at,
                finished_at=finished_at,
                status=STATUS_FAILED,
                params_json=params_json,
                error=error,
            )
        except Exception:
            logger.exception("investor snapshot failed (%s) and its failed row could not be written", error)
            continue
        logger.warning("investor snapshot %d failed: %s", snapshot_id, error)
        return


__all__ = [
    "COHORT_RULE_VERSION",
    "COHORT_TOP_N",
    "MAX_CONSECUTIVE_ERRORS",
    "MAX_TRACKED",
    "OUTCOME_ERROR",
    "OUTCOME_OK",
    "OUTCOME_UNAVAILABLE",
    "RANKING_ATTEMPTS",
    "RANKING_PARAMS",
    "STATUS_COMPLETE",
    "STATUS_FAILED",
    "STATUS_PARTIAL",
    "InvestorSnapshotPartial",
    "InvestorSnapshotRefused",
    "InvestorSnapshotResult",
    "RankingInconsistent",
    "Member",
    "build_fetch_set",
    "classify_response",
    "fetch_member",
    "fetch_ranking",
    "record_investor_snapshot",
]
