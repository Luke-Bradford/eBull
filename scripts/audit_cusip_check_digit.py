"""#2304 — census the ``unresolved_13f_cusips`` table by CUSIP check digit.

## Why this exists

OpenFIGI validates the CUSIP mod-10 check digit and answers a failure with
a per-item ``{"error": "Invalid idValue format."}``, not a ``{"warning":
...}`` (probed live 2026-08-06 — see ``.claude/skills/data-sources/openfigi.md``
§4.3). Pre-#2304 the parser folded both into the same terminal
``openfigi_unknown`` tombstone, so a stored identifier the SOURCE REJECTED
is indistinguishable from a security OpenFIGI genuinely has no mapping for.

``--census`` computes that split at RUN TIME rather than repeating a
hand-written figure. A number typed into a comment goes stale silently the
moment the corpus moves; this one cannot.

``--reset-invalid`` emits (does NOT execute) the SQL that returns the
check-digit-invalid ``openfigi_unknown`` rows to ``resolution_status =
NULL`` so the next sweep re-asks the source and writes the truthful label
from the actual response. The check digit selects WHICH rows to re-ask; it
never writes the label itself. Clearing tombstones is a data change and
belongs to the operator.

## Source rule — the check digit

CUSIP Issuer Number check digit, the "modulus 10 double-add-double"
algorithm (ANSI X9.6 / CUSIP Global Services user manual, and the same rule
CINS inherits for its letter-leading prefixes):

  * character values: ``0-9`` → 0-9, ``A-Z`` → 10-35, ``*`` → 36, ``@`` → 37,
    ``#`` → 38;
  * over the first EIGHT characters, left to right, double the value at
    every even position (1-indexed);
  * sum the DECIMAL DIGITS of each result (so 16 contributes 1+6=7);
  * check digit = ``(10 - sum % 10) % 10``.

Usage::

    PYTHONPATH=. uv run python scripts/audit_cusip_check_digit.py --census
    PYTHONPATH=. uv run python scripts/audit_cusip_check_digit.py --reset-invalid
"""

from __future__ import annotations

import argparse
import sys
from typing import Final

import psycopg
from psycopg import sql

from app.config import settings

_RESET_SCOPE: Final[str] = "resolution_status = 'openfigi_unknown'\n   AND source IS NOT NULL"
"""The rows ``--reset-invalid`` is allowed to touch, as one predicate.

Used verbatim by BOTH the SELECT that counts the affected CUSIPs and the
UPDATE that is emitted for them, so the printed count cannot drift from
what the pasted SQL actually changes. They were separate strings and the
source clause was on the UPDATE only.

⚠ ``source IS NOT NULL`` is a SCOPE guard, not a filter on the check
digit. ``unresolved_13f_cusips`` is shared by two producers, and a NULL
``source`` marks the OTHER one's queue (#2213) — resetting such a row to
``NULL`` would hand it back to a worker that did not tombstone it. It
selects nothing away today; run ``--census`` for the current split rather
than trusting a figure written here.
"""

_CHAR_VALUES: Final[dict[str, int]] = {
    **{c: i for i, c in enumerate("0123456789")},
    **{c: 10 + i for i, c in enumerate("ABCDEFGHIJKLMNOPQRSTUVWXYZ")},
    "*": 36,
    "@": 37,
    "#": 38,
}


def cusip_check_digit(first_eight: str) -> int:
    """Return the check digit the mod-10 rule requires for ``first_eight``.

    Raises ``KeyError`` on a character outside the CUSIP alphabet — callers
    that accept arbitrary stored text should use :func:`is_valid_cusip`.
    """
    total = 0
    for position, char in enumerate(first_eight, start=1):
        value = _CHAR_VALUES[char]
        if position % 2 == 0:
            value *= 2
        total += value // 10 + value % 10
    return (10 - total % 10) % 10


def is_valid_cusip(cusip: str) -> bool:
    """True when ``cusip`` is 9 characters in the CUSIP alphabet whose final
    character is the digit the mod-10 rule requires."""
    if len(cusip) != 9 or any(char not in _CHAR_VALUES for char in cusip):
        return False
    return cusip[8].isdigit() and cusip_check_digit(cusip[:8]) == int(cusip[8])


def _census(conn: psycopg.Connection[tuple]) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT resolution_status, cusip, count(*) FROM unresolved_13f_cusips GROUP BY 1, 2",
        )
        rows = cur.fetchall()

    agg: dict[tuple[str | None, bool], list[int]] = {}
    for status, cusip, row_count in rows:
        bucket = agg.setdefault((status, is_valid_cusip(cusip)), [0, 0])
        bucket[0] += 1
        bucket[1] += row_count

    print(f"{'resolution_status':30s} {'check_digit_valid':18s} {'distinct':>9s} {'rows':>9s}")
    for key in sorted(agg, key=lambda k: (str(k[0]), k[1])):
        status, valid = key
        distinct, row_count = agg[key]
        print(f"{str(status):30s} {str(valid):18s} {distinct:9d} {row_count:9d}")

    invalid_unknown = agg.get(("openfigi_unknown", False), [0, 0])
    print(
        f"\nMislabelled by #2304: {invalid_unknown[0]} distinct CUSIPs / {invalid_unknown[1]} rows "
        f"carry a terminal 'openfigi_unknown' verdict for an identifier the source rejects."
    )
    return invalid_unknown[0]


def _reset_invalid(conn: psycopg.Connection[tuple]) -> None:
    """Print the reset SQL. Deliberately does NOT execute it."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT cusip FROM unresolved_13f_cusips WHERE {_RESET_SCOPE}")
        invalid = sorted(row[0] for row in cur.fetchall() if not is_valid_cusip(row[0]))

    print(f"-- {len(invalid)} distinct check-digit-invalid CUSIPs currently tombstoned openfigi_unknown.")
    if not invalid:
        print("-- Nothing to reset.")
        return
    print("-- Reset to NULL so the next sweep re-asks OpenFIGI and writes the label from the")
    print("-- ACTUAL response (openfigi_invalid_identifier), not from this script's inference.")
    print("-- ⚠ Clearing tombstones is a data change. Review before running.")
    print("BEGIN;")
    print("UPDATE unresolved_13f_cusips SET resolution_status = NULL")
    print(f" WHERE {_RESET_SCOPE}")
    print("   AND cusip IN (")
    # ⚠ Quote through psycopg, never an f-string. These values come from filer
    # 13F submissions, so a stored CUSIP containing a single quote would close
    # the literal early and the rest would be pasted into psql as SQL. The
    # script only PRINTS the statement, which makes it feel harmless — it is
    # not: the whole point of this output is that an operator runs it verbatim,
    # so the injection lands one copy-paste later instead of here.
    quoted = [sql.Literal(c).as_string(conn) for c in invalid]
    # Commas BETWEEN chunks only — a trailing comma before ')' is a
    # syntax error, and this output exists to be pasted verbatim.
    lines = [", ".join(quoted[i : i + 8]) for i in range(0, len(quoted), 8)]
    print(",\n".join(f"       {line}" for line in lines))
    print("   );")
    print("COMMIT;")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--census", action="store_true", help="print the check-digit split by resolution_status")
    parser.add_argument("--reset-invalid", action="store_true", help="EMIT (not run) the tombstone-reset SQL")
    args = parser.parse_args()
    if not (args.census or args.reset_invalid):
        parser.error("pass --census or --reset-invalid")

    with psycopg.connect(settings.database_url) as conn:
        if args.census:
            _census(conn)
        if args.reset_invalid:
            _reset_invalid(conn)
    return 0


if __name__ == "__main__":
    sys.exit(main())
