"""#3899 — repair ``filing_raw_documents`` rows stored while the SEC edge injected its script.

``strip_sec_edge_script`` now runs at both raw body boundaries, so new bodies
arrive clean. Two kinds of row were written before that:

``live`` (default arm)
    A live payload that carries the tag. Rewritten to the stripped text, so the
    stored bytes and any later sweep hash are the document as filed. Each UPDATE
    re-pins ``payload = <the bytes read>``, so a concurrent ``store_raw`` is
    never overwritten. ``fetched_at`` and ``parser_version`` are untouched.
    ``--parse-ab`` runs the Item 403 and Item 402(c) parsers on the stored and
    the stripped DEF 14A body and exits 1 on any difference.

``--swept``
    A born-compacted / swept HTML row hashed since the onset floor. Its
    ``payload_sha256`` may cover an injected token, which the canonical
    re-fetch ``rehydrate_raw_document`` now compares against can never
    reproduce. Each row is re-fetched (shared SEC budget) and classified:
    ``canonical`` (already matches), ``reconstructed`` (matches once a known
    token is re-inserted before the last ``</body>``, the position on all 253
    stored DEF 14A cases), ``gone`` (404/410), ``error`` (fetch raised; re-run) or ``unverifiable``. With
    ``--apply``, a ``reconstructed`` row's hash is re-recorded as the canonical
    body's: the recorded hash was verified first, so the bytes it named are
    unchanged. ``unverifiable`` rows keep their hash and fail loud on rehydrate.

Default is a dry run. Run::

    PYTHONPATH=. uv run python scripts/repair_3899_sec_edge_script.py --parse-ab
    PYTHONPATH=. uv run python scripts/repair_3899_sec_edge_script.py --apply
    PYTHONPATH=. uv run python scripts/repair_3899_sec_edge_script.py --swept
    PYTHONPATH=. uv run python scripts/repair_3899_sec_edge_script.py --swept --apply
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from collections import Counter
from typing import Any

import httpx
import psycopg

from app.config import settings
from app.providers.implementations.sec_edgar import strip_sec_edge_script
from scripts._dev_guard import assert_dev_environment

# A cheap prefilter (strpos, no regex over multi-hundred-MB payloads); the
# shared regex decides what is actually removed.
_MARKER = '<script type="text/javascript"'

# No stored body fetched 2026-07-01 .. 2026-09-01 carries the tag; the first
# stored injected body is from 2026-09-20. The floor is the earlier date.
_ONSET_FLOOR = "2026-09-01"

# Every token seen in a stored body before the live repair, with its window.
# Reproduce (before ``--apply``): SELECT fetched_at, substring(right(payload,
# 4000) from 'src="(/[^"]+)"></script>') FROM filing_raw_documents WHERE
# strpos(payload, '<script type="text/javascript"  src="/') > 0.
_KNOWN_TOKENS = (
    "/KZXDf/sG5k/CARL/EsDR/kDn/OtEab6Q6wD6wDr/dzojDwE/fS9SE/xoFe0k",  # 2026-09-20
    "/N3AEOPt9/dJoipWz/rYwIB5c/dg/YEaQGVVmDJktp27Xmu/SixucGdeLAQ/KXhNJ/AQsW1I",  # 2026-09-27
    "/hEyxvy/tDTQ/C5W/N8y/CEN74LRlKz4/wYOaNzSVtcwhtffaf7/U3FqJk0lAg/cDg9eQ/IuTns",  # 2026-10-03
    "/ve5kkgOKS/1tZ/IOZiCw/m5Q3Q8mpJLpc2XXi/W3QcSzUB/EnhTd/C15D1U",  # 2026-10-04
    "/LsEXsLQkpInDfWOc80pW8f_lmDc/3bb3JX0JLfLmXh/TisvAQ/eAgbG/kUOCEc",  # 2026-10-11
    "/0aVhYt/d/M/o3xvCvj5nUd3/uYm1k2LfppXf2S/MkcUAQ/E1lm/SBIQBUg",  # 2026-10-18
    "/-QOA/64Vm/ig/zTjS/gt8A/m1aEQkGLcNmQ4Q/XiUsWQ/Yl0/HHTAfbn4",  # 2026-10-20 re-fetch
)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


_BODY_CLOSE_RE = re.compile(r"</(?i:body)>")


def _with_token(body: str, token: str) -> str | None:
    """``body`` with the edge tag re-inserted before its last ``</body>``, or None without one.

    The same anchor ``strip_sec_edge_script`` removes at (pinned by
    ``strip_sec_edge_script(_with_token(b, t)) == b`` in the tests).
    """
    closes = list(_BODY_CLOSE_RE.finditer(body))
    if not closes:
        return None
    at = closes[-1].start()
    return f'{body[:at]}<script type="text/javascript"  src="{token}"></script>{body[at:]}'


def classify_swept(recorded_sha: str, canonical: str | None) -> str:
    """``canonical`` / ``reconstructed`` / ``gone`` / ``unverifiable`` for one swept row."""
    if canonical is None:
        return "gone"
    if _sha(canonical) == recorded_sha:
        return "canonical"
    for token in _KNOWN_TOKENS:
        injected = _with_token(canonical, token)
        if injected is not None and _sha(injected) == recorded_sha:
            return "reconstructed"
    return "unverifiable"


def _parse(body: str) -> tuple[str, str]:
    from app.providers.implementations.sec_def14a import (
        parse_beneficial_ownership_table,
        parse_summary_compensation_table,
    )

    out: list[str] = []
    for parse in (parse_beneficial_ownership_table, parse_summary_compensation_table):
        try:
            out.append(repr(parse(body)))
        except Exception as exc:  # noqa: BLE001 — an identical raise on both sides is identical output
            out.append(f"raised {type(exc).__name__}: {exc}")
    return out[0], out[1]


def _live(conn: psycopg.Connection[Any], *, apply: bool, parse_ab: bool) -> int:
    candidates: Counter[str] = Counter()
    changed: Counter[str] = Counter()
    lost_race = 0
    parse_diffs: list[str] = []
    keys = conn.execute(
        "SELECT accession_number, document_kind FROM filing_raw_documents "
        "WHERE payload IS NOT NULL AND strpos(payload, %s) > 0 ORDER BY 2, 1",
        (_MARKER,),
    ).fetchall()
    for accession, kind in keys:
        candidates[kind] += 1
        row = conn.execute(
            "SELECT payload FROM filing_raw_documents WHERE accession_number = %s AND document_kind = %s",
            (accession, kind),
        ).fetchone()
        if row is None or row[0] is None:
            continue
        body = str(row[0])
        clean = strip_sec_edge_script(body)
        if clean == body:
            continue
        changed[kind] += 1
        if parse_ab and kind == "def14a_body" and _parse(body) != _parse(clean):
            parse_diffs.append(accession)
        if apply:
            result = conn.execute(
                "UPDATE filing_raw_documents SET payload = %s "
                "WHERE accession_number = %s AND document_kind = %s AND payload = %s",
                (clean, accession, kind, body),
            )
            lost_race += 1 - result.rowcount

    print(f"=== #3899 live payload repair ({'APPLIED' if apply else 'DRY RUN'}) ===")
    for kind in sorted(candidates):
        print(f"  {kind:<22} marker rows {candidates[kind]:>6}  stripped {changed[kind]:>6}")
    if parse_ab:
        identical = changed["def14a_body"] - len(parse_diffs)
        print(f"  def14a_body parse A/B: {identical} identical, {len(parse_diffs)} differ")
        for accession in parse_diffs:
            print(f"    DIFF {accession}")
    if apply:
        print(f"  rows skipped because the body changed underneath: {lost_race}")
    return 1 if parse_diffs else 0


def _swept(conn: psycopg.Connection[Any], *, apply: bool) -> int:
    from app.providers.implementations.sec_edgar import SecFilingsProvider

    rows = conn.execute(
        """
        SELECT accession_number, document_kind, source_url, payload_sha256
        FROM filing_raw_documents
        WHERE payload IS NULL AND payload_sha256 IS NOT NULL
          AND payload_swept_at >= %s AND source_url ~* '\\.html?$'
        ORDER BY 2, 1
        """,
        (_ONSET_FLOOR,),
    ).fetchall()
    tally: Counter[tuple[str, str]] = Counter()
    rebaselined = 0
    with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
        for accession, kind, url, recorded in rows:
            try:
                canonical = provider.fetch_document_text(str(url))
            except httpx.HTTPError as exc:  # one 5xx/429 or transport failure must not abort the census
                print(f"  fetch error {accession} {kind}: {type(exc).__name__}: {exc}")
                tally[(kind, "error")] += 1
                continue
            verdict = classify_swept(str(recorded), canonical)
            tally[(kind, verdict)] += 1
            if apply and verdict == "reconstructed" and canonical is not None:
                result = conn.execute(
                    "UPDATE filing_raw_documents SET payload_sha256 = %s "
                    "WHERE accession_number = %s AND document_kind = %s AND payload IS NULL AND payload_sha256 = %s",
                    (_sha(canonical), accession, kind, recorded),
                )
                rebaselined += result.rowcount

    print(f"=== #3899 swept-hash check since {_ONSET_FLOOR} ({'APPLIED' if apply else 'DRY RUN'}) ===")
    for (kind, verdict), n in sorted(tally.items()):
        print(f"  {kind:<22} {verdict:<14} {n:>6}")
    if apply:
        print(f"  hashes re-recorded as canonical: {rebaselined}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write (default: dry run)")
    parser.add_argument("--parse-ab", action="store_true", help="live arm: parse DEF 14A stored vs stripped")
    parser.add_argument("--swept", action="store_true", help="check swept HTML hashes against a re-fetch")
    args = parser.parse_args()
    assert_dev_environment()
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        if args.swept:
            return _swept(conn, apply=args.apply)
        return _live(conn, apply=args.apply, parse_ab=args.parse_ab)


if __name__ == "__main__":
    sys.exit(main())
