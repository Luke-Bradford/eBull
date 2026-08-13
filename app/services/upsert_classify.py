"""Transient-vs-deterministic discriminator for manifest-parser upsert
exceptions (#1131).

Pre-#1131 every per-source manifest parser treated an upsert exception
as ``failed`` with a 1h backoff. A deterministic constraint violation
on the typed-table upsert (bad date past a CHECK, malformed enum, FK
miss, unique violation under unexpected duplicate state) keeps
refetching the same dead XML from SEC on every worker tick — wasted
fair-use budget; manifest stays ``failed`` forever.

The fix is a discriminator on the psycopg exception class:

  * ``psycopg.errors.OperationalError`` (parent of
    ``SerializationFailure`` + ``DeadlockDetected``, plus connection-
    drop / server-restart shapes) → transient, retry with backoff.
  * Everything else under ``psycopg.Error`` (``IntegrityError``,
    ``DataError``, ``ProgrammingError`` and their subclasses) plus any
    non-DB Python exception (``ValueError`` from a stray cast,
    ``KeyError`` from a typo) → deterministic. Tombstone the row so
    the worker stops re-fetching.

Tagging the exception class into the manifest's ``error`` column also
lets the one-shot ``tombstone_stale_failed_upserts`` backfill skip
transient-shaped rows when promoting pre-#1131 ``failed`` rows to
``tombstoned`` — old-format error strings without class names get
tombstoned by age; new-format ones get filtered precisely.
"""

from __future__ import annotations

import psycopg

# Transient: DB-side infra failure or contention. The parsed payload
# isn't the problem — retrying after backoff is likely to succeed.
# OperationalError is the psycopg3 parent for SerializationFailure,
# DeadlockDetected, connection-drop, and server-restart shapes; one
# isinstance check covers all the retry-worthy classes.
_TRANSIENT_PSYCOPG_BASE: type[BaseException] = psycopg.errors.OperationalError


def is_transient_upsert_error(exc: BaseException) -> bool:
    """Return True when ``exc`` is a transient DB error worth retrying.

    Caller policy on True: return ``_failed_outcome`` so the worker
    schedules a 1h backoff retry. On False: tombstone the row so the
    worker stops re-fetching the same dead payload.

    Discrimination is by psycopg exception class only — error-string
    sniffing is brittle across psycopg / Postgres versions. Anything
    that isn't a ``psycopg.errors.OperationalError`` (or subclass) is
    treated as deterministic, including:

      - ``IntegrityError`` (UniqueViolation / CheckViolation /
        ForeignKeyViolation / NotNullViolation)
      - ``DataError`` (bad date, NUMERIC overflow, enum mismatch)
      - ``ProgrammingError`` (bad SQL — bug on our side, won't self-fix)
      - Non-DB exceptions (``ValueError``, ``KeyError``, ``TypeError``)
        that escape the parser into the upsert path
    """
    return isinstance(exc, _TRANSIENT_PSYCOPG_BASE)


def format_upsert_error(exc: BaseException) -> str:
    """Render an upsert exception for the manifest's ``error`` column.

    Format: ``"upsert error: <ExceptionClass>: <message>"`` so the
    one-shot backfill at :func:`tombstone_stale_failed_upserts` can
    skip transient-shaped rows by class-name match, and operators
    reading the manifest can distinguish constraint violations from
    serialisation failures at a glance.
    """
    return f"upsert error: {type(exc).__name__}: {exc}"
