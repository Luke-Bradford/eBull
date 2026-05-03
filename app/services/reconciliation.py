"""Reconciliation framework — spot-check stored values vs live SEC.

Operator audit 2026-05-03 made the case for a self-healing layer:
the system should be able to know when something isn't right and
flag it without operator hand-curation. This module is the
mechanism.

Contract: ``run_spot_check(conn, sample_size=N)`` picks N random
instruments, runs every registered check against them, logs
findings to ``data_reconciliation_findings``. The check registry
lives in this module — adding a new check is two lines (function +
register call).

This PR ships the framework + ONE check
(``shares_outstanding_freshness``). More checks land as follow-ups
once the framework + first check have shipped and proven the
contract.

The framework is operator-runnable today:

    uv run python scripts/run_reconciliation.py --sample-size 25

Surfaces on the ingest-health page in a follow-up PR.
"""

from __future__ import annotations

import json
import logging
import random
import urllib.request
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows

from app.config import settings
from app.services.cik_raw_filings import cache_database_url, read_cik_raw, store_cik_raw

logger = logging.getLogger(__name__)

# Stored ``as_of_date`` older than this triggers a freshness finding
# even when the value still matches SEC's latest. SEC issuers refresh
# at least once per quarter via 10-Q; >180 days stale means we
# missed at least one filing.
_STALENESS_THRESHOLD = timedelta(days=180)


Severity = Literal["info", "warning", "critical"]


@dataclass(frozen=True)
class Finding:
    """One drift finding from a single check on a single instrument."""

    check_name: str
    severity: Severity
    summary: str
    expected: str | None = None
    observed: str | None = None
    source_url: str | None = None


@dataclass(frozen=True)
class InstrumentSubject:
    """Per-instrument context passed to every check. Pre-resolved so
    each check doesn't repeat the lookups."""

    instrument_id: int
    symbol: str
    cik: str | None  # 10-digit padded; None when no SEC mapping


@dataclass(frozen=True)
class CheckResult:
    """Per-check, per-instrument result. ``findings`` is empty when
    the check passed cleanly."""

    instrument_id: int
    check_name: str
    findings: tuple[Finding, ...]


@dataclass(frozen=True)
class ReconciliationSummary:
    run_id: int
    instruments_checked: int
    findings_emitted: int


# A check function takes the per-instrument subject and returns a
# tuple of findings. Empty tuple = clean pass. The framework wraps
# every call in a try/except so a single misbehaving check doesn't
# abort the whole sweep.
CheckFn = Callable[[psycopg.Connection[Any], InstrumentSubject], tuple[Finding, ...]]


_REGISTRY: dict[str, CheckFn] = {}


def register_check(name: str, fn: CheckFn) -> None:
    """Register a check function under ``name``. Idempotent — re-
    registering with the same name overwrites the prior function.
    """
    _REGISTRY[name] = fn


def registered_checks() -> dict[str, CheckFn]:
    """Snapshot of the registry for tests + introspection."""
    return dict(_REGISTRY)


# ---------------------------------------------------------------------------
# Check 1: shares_outstanding freshness vs SEC submissions.json
# ---------------------------------------------------------------------------


def check_shares_outstanding_freshness(
    conn: psycopg.Connection[Any],
    subject: InstrumentSubject,
) -> tuple[Finding, ...]:
    """Compare our stored ``instrument_share_count_latest.latest_shares``
    against the latest XBRL DEI value SEC publishes for the same CIK.

    Drift sources:

      * Stale ingest — our value lags SEC's by > 1 quarter.
      * Parse bug — our value differs by > 0.1% from SEC's.
      * No CIK — instrument can't be reconciled at all
        (info-severity).

    Source: ``https://data.sec.gov/submissions/CIK{cik}.json`` is
    free-form metadata; the share count itself comes from
    ``https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json`` →
    ``facts.dei.EntityCommonStockSharesOutstanding`` (latest).
    """
    if subject.cik is None:
        return (
            Finding(
                check_name="shares_outstanding_freshness",
                severity="info",
                summary="No SEC CIK — instrument cannot be reconciled.",
                expected=None,
                observed=None,
            ),
        )

    # Stored value
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT latest_shares, as_of_date
            FROM instrument_share_count_latest
            WHERE instrument_id = %s
            """,
            (subject.instrument_id,),
        )
        stored = cur.fetchone()

    # Live SEC value (write-through cache via cik_raw_documents).
    # Fetched BEFORE the missing-stored-value check so that an
    # issuer with a CIK but no DEI fact at SEC (foreign issuer,
    # newly-registered, fund) doesn't trigger a false "missing
    # stored value" warning when SEC also has nothing to compare
    # against — that's a clean no-data case, not a drift signal.
    try:
        sec_latest = _fetch_latest_dei_shares_outstanding(conn, subject.cik)
    except Exception as exc:  # noqa: BLE001 — fetch errors must not abort the sweep
        return (
            Finding(
                check_name="shares_outstanding_freshness",
                severity="info",
                summary=f"SEC fetch failed: {type(exc).__name__}: {exc}",
                source_url=_companyfacts_url(subject.cik),
            ),
        )

    if stored is None or stored.get("latest_shares") is None:
        if sec_latest is None:
            # Both sides empty — nothing to reconcile. Not a drift
            # signal; the instrument has no SEC-side share count to
            # measure against.
            return ()
        return (
            Finding(
                check_name="shares_outstanding_freshness",
                severity="warning",
                summary="No stored shares_outstanding for instrument with SEC CIK.",
                expected="non-null XBRL DEI value at SEC",
                observed="NULL in instrument_share_count_latest",
                source_url=_companyfacts_url(subject.cik),
            ),
        )

    if sec_latest is None:
        return ()  # SEC has no DEI value either — clean

    stored_val = Decimal(stored["latest_shares"])
    sec_val = Decimal(sec_latest)
    findings: list[Finding] = []

    # Freshness check — surface stale ingest even when the value
    # still matches. A multi-quarter-old ``as_of_date`` means the
    # SEC fundamentals ingester hasn't reached this instrument
    # recently, which is a separate failure mode from value drift.
    as_of = stored.get("as_of_date")
    if isinstance(as_of, date):
        age = datetime.now(UTC).date() - as_of
        if age > _STALENESS_THRESHOLD:
            findings.append(
                Finding(
                    check_name="shares_outstanding_freshness",
                    severity="warning",
                    summary=(f"Stored shares_outstanding is stale (as_of_date {as_of.isoformat()}, age {age.days}d)."),
                    expected=f"as_of_date within {_STALENESS_THRESHOLD.days}d",
                    observed=as_of.isoformat(),
                    source_url=_companyfacts_url(subject.cik),
                )
            )

    if stored_val == sec_val:
        return tuple(findings)  # value clean; freshness may still have fired

    # Compute drift fraction — guard divide-by-zero on stored=0.
    if stored_val == 0:
        drift = Decimal("999")
    else:
        drift = abs(sec_val - stored_val) / stored_val

    if drift < Decimal("0.001"):
        # < 0.1% drift — likely rounding / share-class slicing; clean
        return tuple(findings)

    severity: Severity
    if drift < Decimal("0.05"):
        severity = "warning"
    else:
        severity = "critical"

    findings.append(
        Finding(
            check_name="shares_outstanding_freshness",
            severity=severity,
            summary=(f"Drift {drift * 100:.2f}% between stored shares_outstanding and SEC DEI latest."),
            # Share counts are integers; the source NUMERIC(30,6)
            # column stores them with trailing fractional zeros that
            # would otherwise read as ``100000000.000000`` in
            # operator triage. Cast to int for the display.
            expected=str(int(sec_val)),
            observed=str(int(stored_val)),
            source_url=_companyfacts_url(subject.cik),
        )
    )
    return tuple(findings)


def _companyfacts_url(cik_padded: str) -> str:
    return f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json"


# Companyfacts cache TTL. SEC's payload updates roughly daily as new
# filings land; a 24h cache cuts ~95% of fetches in a typical
# spot-check sweep without serving meaningfully stale data — drift
# findings here have a 5% threshold for "warning" so a few hours of
# stale cache is well under the noise floor.
_COMPANYFACTS_CACHE_TTL = timedelta(hours=24)


def _fetch_companyfacts_payload(
    conn: psycopg.Connection[Any],
    cik_padded: str,
) -> dict[str, Any] | None:
    """Return the parsed companyfacts payload for a CIK, using the
    ``cik_raw_documents`` write-through cache.

    Cache miss (or row older than ``_COMPANYFACTS_CACHE_TTL``)
    triggers a fresh SEC fetch; the raw JSON text is then stored
    so a subsequent call within the TTL window is a hot read.

    Both cache READ and WRITE go through SEPARATE short-lived
    connections, NOT the caller's. Two reasons:

      1. Durability — the caller's transaction may roll back
         (``run_spot_check``'s finally-block rollback on
         zero-finding sweeps, or any failure path). A piggy-backed
         cache write would silently disappear. A dedicated
         connection commits on its own when it exits the context
         manager, decoupled from the caller's lifecycle.
      2. Snapshot freshness — the caller's connection may be in a
         transaction whose snapshot was taken before another sweep
         wrote the cache row, hiding the row from a same-connection
         read. A fresh connection per cache read sees committed
         rows immediately.

    The connection-establishment cost is noise compared to the
    30-second SEC fetch a miss pays for.

    Cache failures are best-effort — a write or read failure logs
    and continues so a transient DB hiccup doesn't take the
    reconciliation sweep down.
    """
    dsn = cache_database_url(conn)
    cached = _read_cache(dsn, cik_padded)
    if cached is not None:
        return cached

    req = urllib.request.Request(
        _companyfacts_url(cik_padded),
        headers={"User-Agent": settings.sec_user_agent},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 — fixed SEC URL
        text = resp.read().decode("utf-8")

    _write_cache(dsn, cik_padded, text)

    parsed = json.loads(text)
    return parsed if isinstance(parsed, dict) else None


def _read_cache(dsn: str, cik_padded: str) -> dict[str, Any] | None:
    """Cache read on a fresh connection. Returns ``None`` on miss /
    stale / parse-error / DB error so the caller falls through to a
    fresh SEC fetch."""
    try:
        with psycopg.connect(dsn) as cache_conn:
            cached = read_cik_raw(
                cache_conn,
                cik=cik_padded,
                document_kind="companyfacts_json",
                max_age=_COMPANYFACTS_CACHE_TTL,
            )
    except Exception:  # noqa: BLE001 — cache read must not abort the check
        logger.exception("reconciliation: companyfacts cache read failed for CIK %s", cik_padded)
        return None
    if cached is None:
        return None
    try:
        parsed = json.loads(cached.payload)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _write_cache(dsn: str, cik_padded: str, text: str) -> None:
    """Cache write on a fresh connection. Best-effort — failure
    logs and returns so the caller's flow continues."""
    try:
        with psycopg.connect(dsn) as cache_conn:
            store_cik_raw(
                cache_conn,
                cik=cik_padded,
                document_kind="companyfacts_json",
                payload=text,
                source_url=_companyfacts_url(cik_padded),
            )
            # psycopg3 connection context manager commits on clean exit.
    except Exception:  # noqa: BLE001 — cache write must not abort the check
        logger.exception("reconciliation: companyfacts cache write failed for CIK %s", cik_padded)


def _fetch_latest_dei_shares_outstanding(
    conn: psycopg.Connection[Any],
    cik_padded: str,
) -> int | None:
    """Walk the SEC companyfacts payload and return the latest DEI
    ``EntityCommonStockSharesOutstanding`` value, or ``None`` when
    the concept is absent.

    Picks the row with the highest ``end`` date, tie-broken by
    ``filed`` desc so amended filings overwrite the original.
    Companyfacts publishes the original 10-K row alongside any
    10-K/A re-statement under the same ``end`` date — without the
    ``filed`` tie-break, payload order decides which one wins,
    creating spurious drift findings.

    SEC publishes multiple unit-of-measure variants for a given
    fact; we pick the ``shares`` unit only.

    Routes through the ``cik_raw_documents`` write-through cache so
    a sweep over a thousand instruments doesn't hammer SEC at 10
    req/s — repeated CIKs become hot reads.
    """
    payload = _fetch_companyfacts_payload(conn, cik_padded)
    if payload is None:
        return None
    facts = payload.get("facts", {}).get("dei", {}).get("EntityCommonStockSharesOutstanding")
    if not isinstance(facts, dict):
        return None
    units = facts.get("units", {}).get("shares", [])
    if not isinstance(units, list) or not units:
        return None
    latest = max(units, key=lambda u: (u.get("end") or "", u.get("filed") or ""))
    val = latest.get("val")
    return int(val) if val is not None else None


register_check("shares_outstanding_freshness", check_shares_outstanding_freshness)


# ---------------------------------------------------------------------------
# Run orchestration
# ---------------------------------------------------------------------------


def _pick_subjects(
    conn: psycopg.Connection[Any],
    sample_size: int,
    seed: int,
) -> list[InstrumentSubject]:
    """Random sample of ``sample_size`` instruments for spot-checking.

    Pre-joins ``external_identifiers`` so each check has the CIK
    already resolved. The sample is biased towards instruments WITH
    a CIK because they're the ones a check can meaningfully run
    against — but a fraction without CIK is included so the
    operator sees the no-CIK gap surfaced as info findings.
    """
    rng = random.Random(seed)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT i.instrument_id, i.symbol, ei.identifier_value AS cik
            FROM instruments i
            LEFT JOIN external_identifiers ei
                ON ei.instrument_id = i.instrument_id
               AND ei.provider = 'sec'
               AND ei.identifier_type = 'cik'
               AND ei.is_primary = TRUE
            WHERE i.symbol IS NOT NULL
            -- ORDER BY before sampling: Postgres returns rows in
            -- whatever physical order the planner picks. ``random.sample``
            -- is deterministic on its INPUT order, so without a
            -- stable ORDER BY here the same seed picks different
            -- cohorts across runs — defeating the reproducibility
            -- contract of ``sample_seed``.
            ORDER BY i.instrument_id
            """,
        )
        rows = cur.fetchall()
    if not rows:
        return []
    sample = rng.sample(rows, k=min(sample_size, len(rows)))
    return [
        InstrumentSubject(
            instrument_id=int(r["instrument_id"]),
            symbol=str(r["symbol"]),
            cik=(str(r["cik"]) if r.get("cik") is not None else None),
        )
        for r in sample
    ]


def _start_run(
    conn: psycopg.Connection[Any],
    sample_seed: int,
    triggered_by: Literal["system", "operator", "scheduler"],
) -> int:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            INSERT INTO data_reconciliation_runs (
                sample_seed, triggered_by
            ) VALUES (%s, %s)
            RETURNING run_id
            """,
            (sample_seed, triggered_by),
        )
        row = cur.fetchone()
    return int(row["run_id"])  # type: ignore[index]


def _finalise_run(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
    instruments_checked: int,
    findings_emitted: int,
    error: str | None,
) -> None:
    status: Literal["success", "failed"] = "failed" if error else "success"
    conn.execute(
        """
        UPDATE data_reconciliation_runs
        SET finished_at = NOW(),
            status = %s,
            instruments_checked = %s,
            findings_emitted = %s,
            error = %s
        WHERE run_id = %s
        """,
        (status, instruments_checked, findings_emitted, error, run_id),
    )


def _persist_finding(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
    instrument_id: int,
    finding: Finding,
) -> None:
    conn.execute(
        """
        INSERT INTO data_reconciliation_findings (
            run_id, instrument_id, check_name, severity, summary,
            expected, observed, source_url
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            instrument_id,
            finding.check_name,
            finding.severity,
            finding.summary,
            finding.expected,
            finding.observed,
            finding.source_url,
        ),
    )


def run_spot_check(
    conn: psycopg.Connection[Any],
    *,
    sample_size: int = 25,
    sample_seed: int | None = None,
    triggered_by: Literal["system", "operator", "scheduler"] = "operator",
) -> ReconciliationSummary:
    """Execute every registered check against ``sample_size`` random
    instruments. Returns a summary; findings are persisted to
    ``data_reconciliation_findings``.

    ``sample_seed`` is injectable — passing the same seed on a
    re-run reproduces the same instrument selection, useful for
    "is this finding still there?" triage.
    """
    seed = sample_seed if sample_seed is not None else random.randrange(2**63 - 1)
    run_id = _start_run(conn, seed, triggered_by)
    conn.commit()

    # Snapshot the registry once per run. A concurrent
    # ``register_check`` call after this point cannot make later
    # subjects in the same run execute a different check set than
    # earlier ones — the reproducibility contract of ``sample_seed``
    # extends to "same seed + same code revision = same set of
    # checks run".
    checks = list(registered_checks().items())

    findings_count = 0
    instruments_count = 0
    error: str | None = None
    try:
        subjects = _pick_subjects(conn, sample_size, seed)
        for subject in subjects:
            instruments_count += 1
            for check_name, check_fn in checks:
                try:
                    findings = check_fn(conn, subject)
                except Exception as exc:  # noqa: BLE001 — per-check crash must not abort
                    logger.exception(
                        "reconciliation: check %s raised on instrument %s",
                        check_name,
                        subject.instrument_id,
                    )
                    findings = (
                        Finding(
                            check_name=check_name,
                            severity="info",
                            summary=f"Check raised: {type(exc).__name__}: {exc}",
                        ),
                    )
                for finding in findings:
                    # Commit per-finding so a later constraint
                    # violation can't roll back earlier successful
                    # inserts in the same subject. ``findings_count``
                    # then matches what's actually persisted in
                    # ``data_reconciliation_findings``.
                    _persist_finding(
                        conn,
                        run_id=run_id,
                        instrument_id=subject.instrument_id,
                        finding=finding,
                    )
                    conn.commit()
                    findings_count += 1
    except Exception as exc:  # noqa: BLE001 — record the error then re-raise
        error = f"{type(exc).__name__}: {exc}"
        logger.exception("reconciliation: spot-check sweep raised")
        raise
    finally:
        # Roll back any aborted transaction state before the UPDATE.
        # If a prior _persist_finding raised (e.g., constraint
        # violation, FK race when an instrument got deleted
        # mid-sweep), the connection is in InFailedSqlTransaction
        # state and the finalise UPDATE would itself raise — leaving
        # the run row stuck in 'running' forever. Roll back to a
        # clean transaction first; the original exception is already
        # captured in ``error``.
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001 — defensive; rollback should not raise
            logger.exception("reconciliation: rollback before finalise failed")
        try:
            _finalise_run(
                conn,
                run_id=run_id,
                instruments_checked=instruments_count,
                findings_emitted=findings_count,
                error=error,
            )
            conn.commit()
        except Exception:
            logger.exception("reconciliation: finalise UPDATE failed")
            # Suppress the finalise error only when there's already
            # an exception in flight — otherwise re-raise so the
            # caller sees a real signal that the run row is in an
            # unfinalised state.
            if error is None:
                raise

    return ReconciliationSummary(
        run_id=run_id,
        instruments_checked=instruments_count,
        findings_emitted=findings_count,
    )


def iter_recent_findings(
    conn: psycopg.Connection[Any],
    *,
    limit: int = 100,
    severity_min: Severity | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield recent findings for the operator surface. ``severity_min``
    filters to findings AT OR ABOVE the named severity (info →
    warning → critical)."""
    where = []
    params: list[Any] = []
    if severity_min == "warning":
        where.append("severity IN ('warning', 'critical')")
    elif severity_min == "critical":
        where.append("severity = 'critical'")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    params.append(limit)
    sql = f"""
        SELECT finding_id, run_id, instrument_id, check_name, severity,
               summary, expected, observed, source_url, fetched_at
        FROM data_reconciliation_findings
        {where_sql}
        ORDER BY fetched_at DESC, finding_id DESC
        LIMIT %s
    """  # noqa: S608 — where built from closed enum
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, params)  # type: ignore[arg-type]
        for row in cur.fetchall():
            yield dict(row)
