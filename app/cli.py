"""Break-glass recovery CLI for eBull (issue #106 / ADR 0002).

This CLI is **not** the normal onboarding path. The normal way to create
the first operator is the browser-driven setup flow at ``/setup`` (see
ADR 0002 §2). This module exists for cases where the browser path is
unavailable:

  * forgotten password                       -- ``set-password``
  * accidentally wiped operators table       -- ``create-operator``
  * (future, once #102 lands) bricked lockout -- ``clear-lockout``

Subcommands:
  create-operator <username>   recreate an operator from the host shell
  set-password    <username>   reset an existing operator's password

Both prompt for the password interactively (via ``getpass``) so it never
appears in shell history or process listings. ``create-operator`` refuses
to overwrite an existing row without ``--force`` so a fat-finger does not
silently rotate credentials.

Run with::

    uv run python -m app.cli set-password    alice
    uv run python -m app.cli create-operator alice

The CLI talks to the same database as the API server (resolved via
``settings.database_url``), so it must be run with the same env that the
server uses (or with the .env file present in CWD).
"""

from __future__ import annotations

import argparse
import getpass
import sys

import psycopg

from app.config import settings
from app.security.passwords import hash_password

# Minimum interactive-password length. This is independent of the service
# token floor (which is for high-entropy random tokens). 12 chars is a
# reasonable lower bound for a human-typed password assuming Argon2id
# protects against offline brute force.
_MIN_PASSWORD_LEN = 12


def _read_password_twice(prompt: str = "Password: ") -> str:
    pw1 = getpass.getpass(prompt)
    if len(pw1) < _MIN_PASSWORD_LEN:
        print(f"Password must be at least {_MIN_PASSWORD_LEN} characters.", file=sys.stderr)
        sys.exit(2)
    pw2 = getpass.getpass("Confirm: ")
    if pw1 != pw2:
        print("Passwords did not match.", file=sys.stderr)
        sys.exit(2)
    return pw1


def _connect() -> psycopg.Connection[object]:
    return psycopg.connect(settings.database_url)


def _normalise_username(raw: str) -> str:
    """Trim and lower-case the input username.

    The DB enforces ``username = lower(username)`` via CHECK constraint
    (sql/016) so callers must lower-case here too. Mixed-case input is
    accepted at the CLI for ergonomics; it is normalised before any DB
    interaction.
    """
    return raw.strip().lower()


def cmd_create_operator(args: argparse.Namespace) -> int:
    username = _normalise_username(args.username)
    if not username:
        print("Username must not be empty.", file=sys.stderr)
        return 2

    password = _read_password_twice()
    password_hash = hash_password(password)

    # Wrap the read + conditional write in an explicit transaction so a
    # mid-flight error rolls back cleanly instead of leaving an aborted
    # implicit transaction on the connection.
    with _connect() as conn, conn.transaction(), conn.cursor() as cur:
        cur.execute("SELECT operator_id FROM operators WHERE username = %s", (username,))
        existing = cur.fetchone()
        if existing is not None and not args.force:
            print(
                f"Operator '{username}' already exists. Use --force to overwrite the password "
                "(prefer 'set-password' for an existing operator).",
                file=sys.stderr,
            )
            return 1

        if existing is not None:
            cur.execute(
                "UPDATE operators SET password_hash = %s WHERE username = %s",
                (password_hash, username),
            )
            print(f"Password rotated for existing operator '{username}'.")
        else:
            cur.execute(
                "INSERT INTO operators (username, password_hash) VALUES (%s, %s)",
                (username, password_hash),
            )
            print(f"Created operator '{username}'.")
    return 0


def cmd_set_password(args: argparse.Namespace) -> int:
    username = _normalise_username(args.username)
    if not username:
        print("Username must not be empty.", file=sys.stderr)
        return 2

    password = _read_password_twice()
    password_hash = hash_password(password)

    with _connect() as conn, conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "UPDATE operators SET password_hash = %s WHERE username = %s",
            (password_hash, username),
        )
        if cur.rowcount == 0:
            print(f"No operator named '{username}'.", file=sys.stderr)
            return 1
    print(f"Password updated for operator '{username}'.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ebull", description="eBull operator CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_create = sub.add_parser("create-operator", help="create an operator (one row in v1)")
    p_create.add_argument("username")
    p_create.add_argument("--force", action="store_true", help="overwrite existing password")
    p_create.set_defaults(func=cmd_create_operator)

    p_set = sub.add_parser("set-password", help="change an existing operator's password")
    p_set.add_argument("username")
    p_set.set_defaults(func=cmd_set_password)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":  # pragma: no cover - shell entry point
    raise SystemExit(main())
