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
  - Bootstrap returns no broker-encryption key. Post-amendment
    2026-05-07 (ADR-0003) the boot states are ``clean_install`` and
    ``normal``; a None key means the persisted root secret is absent
    and no ``EBULL_SECRETS_KEY`` env override is set. Operator must
    add credentials in Settings (which lazy-generates the root
    secret) before re-running this script.
  - EBULL_SECRETS_KEY env override is set but does not match the existing
    ciphertext (``master_key.bootstrap`` raises ``MasterKeyError``)
  - EBULL_SECRETS_KEY env override is malformed (not 32 bytes of base64;
    ``decode_env_key`` raises ``CredentialCryptoConfigError``)

When EBULL_SECRETS_KEY is set and decodes cleanly, ``bootstrap`` installs
that key in the returned ``BootResult`` regardless of whether credentials
already exist, so this script will proceed to write under that key.
"""

from __future__ import annotations

import os
import sys

import psycopg

from app.config import settings
from app.security import master_key
from app.security.master_key import MasterKeyError
from app.security.secrets_crypto import (
    CredentialCryptoConfigError,
    set_active_key,
)
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

    try:
        with psycopg.connect(settings.database_url) as conn:
            # ADR-0003: master_key.bootstrap() must run before any
            # encrypt/decrypt call. Without it secrets_crypto raises
            # CredentialCryptoConfigError because the AESGCM cache is
            # empty. The lifespan does this for the running app; the
            # standalone script must do it itself.
            try:
                boot = master_key.bootstrap(conn)
            except MasterKeyError as exc:
                print(f"ERROR: master-key bootstrap failed: {exc}", file=sys.stderr)
                return 1
            except CredentialCryptoConfigError as exc:
                # Raised by decode_env_key when EBULL_SECRETS_KEY is
                # malformed (not 32 bytes of base64). Surfaces from
                # bootstrap when the env override is set.
                print(f"ERROR: EBULL_SECRETS_KEY is invalid: {exc}", file=sys.stderr)
                return 1

            if boot.broker_encryption_key is None:
                # Bootstrap returned no key. Post-amendment 2026-05-07
                # (ADR-0003) the only boot states are clean_install and
                # normal; if there is no key, stale-cipher soft-revoke
                # has already cleared any rows we cannot decrypt, and
                # the script's only recourse is for the operator to
                # re-add credentials in Settings (which lazy-generates
                # the root secret on first save).
                print(
                    "ERROR: no broker-encryption key. Complete /auth/setup or "
                    "open Settings to add eToro credentials so the first save "
                    "lazy-generates the root secret, then re-run this script.",
                    file=sys.stderr,
                )
                return 1

            set_active_key(boot.broker_encryption_key)

            try:
                op_id = sole_operator_id(conn)
            except NoOperatorError:
                print("ERROR: no operator exists. Run /auth/setup first.", file=sys.stderr)
                return 1
            except AmbiguousOperatorError as exc:
                print(f"ERROR: {exc}. Resolve manually.", file=sys.stderr)
                return 1

            migrated = 0

            # Migrate ETORO_READ_API_KEY → label "api_key", environment "demo".
            # #112: each store_credential call now requires a caller-owned
            # transaction. Wrap each independently so a UniqueViolation on
            # one (CredentialAlreadyExists → handled below) doesn't roll
            # back the other.
            if _READ_KEY:
                try:
                    with conn.transaction():
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
                    with conn.transaction():
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

    except psycopg.Error as exc:
        print(f"ERROR: database error: {exc}", file=sys.stderr)
        return 1

    print(f"Done. {migrated} credential(s) migrated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
