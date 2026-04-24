"""SEC 8-K ``items[]`` extraction from submissions.json (#431).

``submissions.json.filings.recent`` is columnar: ``accessionNumber``,
``form``, ``items``, etc. are parallel arrays indexed by filing. Each
``items`` entry for an 8-K is a comma-separated string like
``"1.01,2.03,9.01"`` (or empty string for non-8-K). This helper parses
the structure into a ``dict[accession -> list[str]]`` so
``_run_cik_upsert`` can UPDATE ``filing_events.items`` without extra
HTTP.
"""

from __future__ import annotations

from typing import Any

import psycopg


def parse_8k_items_by_accession(submissions: dict[str, Any]) -> dict[str, list[str]]:
    """Extract accession_number → list[item_code] for every 8-K /
    8-K/A in ``submissions.filings.recent``. Non-8-K filings are
    skipped. Returns an empty dict if the payload is malformed."""
    filings = submissions.get("filings")
    if not isinstance(filings, dict):
        return {}
    recent = filings.get("recent")
    if not isinstance(recent, dict):
        return {}

    accessions = recent.get("accessionNumber")
    forms = recent.get("form")
    items_col = recent.get("items")
    if not isinstance(accessions, list) or not isinstance(forms, list) or not isinstance(items_col, list):
        return {}

    # Columnar alignment — all three arrays MUST have matching length;
    # a mismatch is malformed submissions, skip rather than raise.
    n = len(accessions)
    if len(forms) != n or len(items_col) != n:
        return {}

    out: dict[str, list[str]] = {}
    for accession, form, raw_items in zip(accessions, forms, items_col, strict=True):
        if not isinstance(accession, str) or not isinstance(form, str):
            continue
        form_stripped = form.strip().upper()
        # Accept 8-K and 8-K/A (amendment).
        if not form_stripped.startswith("8-K"):
            continue
        codes = _split_items(raw_items)
        # Even if items is empty, record the accession so callers can
        # distinguish "parsed and there were no items" from "never
        # parsed". Empty list is the signal.
        out[accession] = codes
    return out


def _split_items(raw: Any) -> list[str]:
    """Split SEC's comma-separated item string into a deduplicated
    list. Ignores whitespace, drops anything that isn't an
    ``N.NN``-shaped code."""
    if not isinstance(raw, str):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for token in raw.split(","):
        code = token.strip()
        if not code:
            continue
        # SEC codes are always "N.NN" or "N.N" (two decimal digits
        # after the dot). Reject anything that doesn't match.
        if "." not in code:
            continue
        left, _, right = code.partition(".")
        if not left.isdigit() or not right.isdigit():
            continue
        if code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


def apply_8k_items_to_filing_events(
    conn: psycopg.Connection[Any],
    items_by_accession: dict[str, list[str]],
) -> int:
    """UPDATE ``filing_events.items`` for every accession we've parsed.

    Only touches rows that already exist (no INSERT). Returns the
    count of rows updated. Empty-list updates are still applied so
    operators can tell "parsed, no items" from "never parsed".
    """
    if not items_by_accession:
        return 0

    # One UPDATE per accession. At daily-cadence volume (tens of
    # 8-Ks across all covered CIKs) this is fine — a bulk UNNEST
    # would be optimal if counts ever climb into the thousands per
    # run, but isn't worth the SQL complexity today. psycopg renders
    # ``list[str]`` as a Postgres ``text[]`` natively.
    updated = 0
    with conn.cursor() as cur:
        for accession, codes in items_by_accession.items():
            cur.execute(
                """
                UPDATE filing_events
                SET items = %s
                WHERE provider = 'sec' AND provider_filing_id = %s
                """,
                (codes, accession),
            )
            updated += cur.rowcount
    return updated
