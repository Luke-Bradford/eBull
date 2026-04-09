"""One-shot migration: move eToro API keys from env vars to the encrypted store.

Reads ETORO_READ_API_KEY (-> label ``api_key``) and optionally
ETORO_WRITE_API_KEY (-> label ``user_key``) from the environment,
resolves the sole operator, and stores the credentials in
broker_credentials with ``environment="demo"`` (the legacy env-var
path was demo-only).

This is the ONLY place in the codebase that is allowed to reference the
old ETORO_*_API_KEY env vars after #100.

Usage:
    ETORO_READ_API_KEY=<key> uv run python scripts/migrate_etoro_credential.py

Idempotent: if the credential already exists, prints a skip message and
exits cleanly.

Hard failures (non-zero exit):
  - No operator exists (run /auth/setup first)
  - Multiple operators exist (ambiguous — manual resolution required)
  - EBULL_SECRETS_KEY not set (encryption layer cannot function)
"""

from __future__ import annotations

import os
import sys

import psycopg

from app.config import settings
from app.services.broker_credentials import (
    CredentialAlreadyExists,
    store_credential,
)
from app.services.operators import (
    AmbiguousOperatorError,
    NoOperatorError,
    sole_operator_id,
)

# Read env vars directly — these fields no longer exist in Settings.
_READ_KEY = os.environ.get("ETORO_READ_API_KEY", "").strip()
_WRITE_KEY = os.environ.get("ETORO_WRITE_API_KEY", "").strip()


def main() -> int:
    if not _READ_KEY and not _WRITE_KEY:
        print("Nothing to migrate: neither ETORO_READ_API_KEY nor ETORO_WRITE_API_KEY is set.")
        return 0

    if not settings.secrets_key:
        print("ERROR: EBULL_SECRETS_KEY is not set. Cannot encrypt credentials.", file=sys.stderr)
        return 1

    try:
        with psycopg.connect(settings.database_url) as conn:
            op_id = sole_operator_id(conn)
    except NoOperatorError:
        print("ERROR: no operator exists. Run /auth/setup first.", file=sys.stderr)
        return 1
    except AmbiguousOperatorError as exc:
        print(f"ERROR: {exc}. Resolve manually.", file=sys.stderr)
        return 1

    migrated = 0

    # Migrate ETORO_READ_API_KEY → label "api_key", environment "demo"
    if _READ_KEY:
        try:
            with psycopg.connect(settings.database_url) as conn:
                store_credential(
                    conn,
                    operator_id=op_id,
                    provider="etoro",
                    label="api_key",
                    environment="demo",
                    plaintext=_READ_KEY,
                )
            print("Migrated ETORO_READ_API_KEY → broker_credentials (etoro/api_key/demo)")
            migrated += 1
        except CredentialAlreadyExists:
            print("Skipped ETORO_READ_API_KEY: credential (etoro/api_key/demo) already exists.")

    # Migrate ETORO_WRITE_API_KEY → label "user_key", environment "demo"
    if _WRITE_KEY:
        try:
            with psycopg.connect(settings.database_url) as conn:
                store_credential(
                    conn,
                    operator_id=op_id,
                    provider="etoro",
                    label="user_key",
                    environment="demo",
                    plaintext=_WRITE_KEY,
                )
            print("Migrated ETORO_WRITE_API_KEY → broker_credentials (etoro/user_key/demo)")
            migrated += 1
        except CredentialAlreadyExists:
            print("Skipped ETORO_WRITE_API_KEY: credential (etoro/user_key/demo) already exists.")

    print(f"Done. {migrated} credential(s) migrated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
