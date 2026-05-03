"""Filer-seed verification — guard against silent CIK drift.

Operator audit 2026-05-03 (issue #807) flagged the
``institutional_filer_seeds`` table at 14 rows vs the ~5,400-row
13F-HR universe. Scaling the seed list to ~150 names would let the
ownership-card pie chart materially populate, but hand-curated CIK
lists drift fast: migrations 104 + 106 caught a 6-of-10 mis-label
rate on the prior pass with one entirely-hallucinated row.

This module is the verification gate: for every seed row, fetch
SEC's submissions.json and compare the live ``name`` field against
the operator-recorded ``expected_name``. Mismatches surface as
findings the operator can triage in one place rather than
discovering them PR-by-PR.

Architecture:

  * ``verify_seed(conn, cik, expected_name)`` — single-CIK check.
    Returns a ``VerificationResult`` with a typed ``status`` enum.
  * ``verify_all_active(conn)`` — walks every active seed and
    yields one result per CIK.
  * ``submissions.json`` is fetched through the ``cik_raw_documents``
    cache (PR #816) so a sweep over 150 seeds doesn't hammer SEC at
    10 req/sec on every run.

The verification sweep is the prerequisite for the seed-expansion
PR that grows the list to 150 — adding a row that doesn't verify
clean is exactly the failure mode this gate is designed to catch.
"""

from __future__ import annotations

import json
import logging
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Literal

import psycopg
import psycopg.rows

from app.config import settings
from app.services.cik_raw_filings import cache_database_url, read_cik_raw, store_cik_raw

logger = logging.getLogger(__name__)


# Same TTL pattern as the reconciliation companyfacts cache:
# submissions.json updates daily as new filings land; a 24h cache
# cuts ~95% of fetches in a typical sweep without serving
# meaningfully stale data.
_SUBMISSIONS_CACHE_TTL = timedelta(hours=24)


VerificationStatus = Literal["match", "drift", "missing", "fetch_error"]


@dataclass(frozen=True)
class VerificationResult:
    cik: str
    expected_name: str
    sec_name: str | None
    status: VerificationStatus
    detail: str | None = None


def verify_seed(
    conn: psycopg.Connection[Any],
    *,
    cik: str,
    expected_name: str,
) -> VerificationResult:
    """Compare ``expected_name`` against SEC's live entity name for
    ``cik``. Returns a ``VerificationResult`` with typed status.

    Status values:

      * ``match`` — names compare equal under normalisation. Seed
        is healthy.
      * ``drift`` — CIK exists at SEC but the name differs. Either
        the operator's recorded name is stale (re-key) or the CIK
        is wrong (delete + re-add).
      * ``missing`` — submissions.json has no usable name field.
        Rare; possibly a defunct filer.
      * ``fetch_error`` — transient SEC outage / network issue.
        Retry on next sweep.

    Routes through the ``cik_raw_documents`` write-through cache so
    a sweep over the full seed list is a 1-fetch + N-cache-read
    pattern within the TTL window.
    """
    if len(cik) != 10 or not cik.isdigit():
        return VerificationResult(
            cik=cik,
            expected_name=expected_name,
            sec_name=None,
            status="fetch_error",
            detail=f"cik not 10-digit zero-padded: {cik!r}",
        )

    try:
        payload = _fetch_submissions(conn, cik)
    except Exception as exc:  # noqa: BLE001 — fetch errors must not abort the sweep
        return VerificationResult(
            cik=cik,
            expected_name=expected_name,
            sec_name=None,
            status="fetch_error",
            detail=f"{type(exc).__name__}: {exc}",
        )

    sec_name = _extract_entity_name(payload)
    if sec_name is None:
        return VerificationResult(
            cik=cik,
            expected_name=expected_name,
            sec_name=None,
            status="missing",
            detail="submissions.json has no ``name`` field",
        )

    if _names_match(expected_name, sec_name):
        return VerificationResult(
            cik=cik,
            expected_name=expected_name,
            sec_name=sec_name,
            status="match",
        )

    return VerificationResult(
        cik=cik,
        expected_name=expected_name,
        sec_name=sec_name,
        status="drift",
        detail=f"expected={expected_name!r} sec={sec_name!r}",
    )


def verify_all_active(
    conn: psycopg.Connection[Any],
) -> Iterator[VerificationResult]:
    """Yield a verification result for every ``active=TRUE`` seed.
    Uses cached submissions.json reads when available."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT cik, COALESCE(expected_name, label) AS expected_name
            FROM institutional_filer_seeds
            WHERE active = TRUE
            ORDER BY cik
            """,
        )
        rows = cur.fetchall()
    for row in rows:
        yield verify_seed(
            conn,
            cik=str(row["cik"]),  # type: ignore[arg-type]
            expected_name=str(row["expected_name"]),  # type: ignore[arg-type]
        )


def _names_match(expected: str, observed: str) -> bool:
    """Case-insensitive normalised comparison. SEC's ``name`` field
    is reasonably canonical but not perfectly stable — punctuation
    drift ("Inc." vs "Inc"), commas, and trailing periods are
    operator-meaningful but lookup-irrelevant. Strip them so a
    cosmetic-only mismatch doesn't trip the drift detector."""
    return _normalise(expected) == _normalise(observed)


def _normalise(name: str) -> str:
    """Lowercase + strip punctuation + collapse whitespace."""
    out = []
    for ch in name.lower():
        if ch.isalnum() or ch.isspace():
            out.append(ch)
        # else: drop punctuation
    return " ".join("".join(out).split())


def _extract_entity_name(payload: dict[str, Any]) -> str | None:
    """Pull SEC's ``name`` field out of submissions.json. The schema
    is stable: top-level ``name`` carries the canonical entity
    name."""
    name = payload.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return None


# ---------------------------------------------------------------------------
# Cached submissions.json fetch
# ---------------------------------------------------------------------------


def _submissions_url(cik_padded: str) -> str:
    return f"https://data.sec.gov/submissions/CIK{cik_padded}.json"


def _fetch_submissions(
    conn: psycopg.Connection[Any],
    cik_padded: str,
) -> dict[str, Any]:
    """Return the parsed submissions.json payload for a CIK, using
    the ``cik_raw_documents`` write-through cache. Raises on parse
    failure or fetch failure — caller wraps."""
    cached = _read_cache(conn, cik_padded)
    if cached is not None:
        return cached

    req = urllib.request.Request(
        _submissions_url(cik_padded),
        headers={"User-Agent": settings.sec_user_agent},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 — fixed SEC URL
        text = resp.read().decode("utf-8")

    _write_cache(conn, cik_padded, text)

    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError("submissions.json payload was not a JSON object")
    return parsed


def _read_cache(
    conn: psycopg.Connection[Any],
    cik_padded: str,
) -> dict[str, Any] | None:
    """Cache read on a fresh connection — same separation pattern as
    the reconciliation cache (PR #816). Stale / parse-error rows
    return None so the caller falls through to a fresh fetch."""
    try:
        with psycopg.connect(cache_database_url(conn)) as cache_conn:
            cached = read_cik_raw(
                cache_conn,
                cik=cik_padded,
                document_kind="submissions_json",
                max_age=_SUBMISSIONS_CACHE_TTL,
            )
    except Exception:  # noqa: BLE001 — cache read must not abort verification
        logger.exception("filer_seed_verification: cache read failed for CIK %s", cik_padded)
        return None
    if cached is None:
        return None
    try:
        parsed = json.loads(cached.payload)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _write_cache(
    conn: psycopg.Connection[Any],
    cik_padded: str,
    text: str,
) -> None:
    """Cache write on a fresh connection. Best-effort."""
    try:
        with psycopg.connect(cache_database_url(conn)) as cache_conn:
            store_cik_raw(
                cache_conn,
                cik=cik_padded,
                document_kind="submissions_json",
                payload=text,
                source_url=_submissions_url(cik_padded),
            )
    except Exception:  # noqa: BLE001 — cache write must not abort verification
        logger.exception("filer_seed_verification: cache write failed for CIK %s", cik_padded)
