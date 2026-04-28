"""Integration tests for the side-connection durable audit (#111).

ADR 0001 requires every credential decryption attempt to leave an
audit row on disk regardless of the caller's transaction outcome.
PR #110 left the audit on the caller's transaction (BLOCKING fix
to avoid silent caller-state commits); this PR adds a side-
connection mode so trade-path callers can satisfy the durability
invariant without a documented commit dance.

Tests run against the real ``ebull_test`` Postgres so the side-
connection pool path actually executes against an independent
backend session, and the caller's rollback observably does not
drop the audit row.
"""

from __future__ import annotations

from collections.abc import Iterator
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg_pool import ConnectionPool

from app.security.secrets_crypto import (
    KEY_VERSION_CURRENT,
    encrypt,
    set_active_key,
)
from app.services.broker_credentials import (
    CredentialNotFound,
    load_credential_for_provider_use,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401
from tests.fixtures.ebull_test_db import test_database_url as _test_database_url

pytestmark = pytest.mark.integration


# Fixed 32-byte key for tests. The crypto layer requires an active key
# before encrypt/decrypt; fixture sets one for the duration of the test.
_TEST_KEY = b"\x01" * 32


@pytest.fixture(autouse=True)
def _install_test_crypto_key() -> Iterator[None]:
    set_active_key(_TEST_KEY)
    yield


@pytest.fixture
def audit_pool() -> Iterator[ConnectionPool[psycopg.Connection[object]]]:
    """Pool against the test DB. Closed at the end of the test."""
    pool: ConnectionPool[psycopg.Connection[object]] = ConnectionPool(
        _test_database_url(),
        min_size=1,
        max_size=2,
        open=True,
    )
    try:
        yield pool
    finally:
        pool.close()


def _seed_operator(conn: psycopg.Connection[tuple], operator_id) -> None:
    conn.execute(
        """
        INSERT INTO operators (operator_id, username, password_hash)
        VALUES (%s, %s, 'x')
        ON CONFLICT (operator_id) DO NOTHING
        """,
        (operator_id, f"op_{operator_id.hex[:8]}"),
    )
    conn.commit()


def _seed_credential(
    conn: psycopg.Connection[tuple],
    *,
    operator_id,
    provider: str = "etoro",
    label: str = "api_key",
    environment: str = "demo",
    plaintext: str = "test-secret-1234",
):
    """Insert an active credential row for the given operator. Returns
    the credential id."""
    ciphertext = encrypt(
        plaintext,
        operator_id=operator_id,
        provider=provider,
        label=label,
        key_version=KEY_VERSION_CURRENT,
    )
    row = conn.execute(
        """
        INSERT INTO broker_credentials
            (operator_id, provider, label, environment, ciphertext,
             last_four, key_version)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            operator_id,
            provider,
            label,
            environment,
            ciphertext,
            plaintext[-4:],
            KEY_VERSION_CURRENT,
        ),
    ).fetchone()
    conn.commit()
    assert row is not None
    return row[0]


def _count_audit_rows(
    conn: psycopg.Connection[tuple],
    *,
    operator_id,
    caller: str,
) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM broker_credential_access_log
        WHERE operator_id = %s AND caller = %s
        """,
        (operator_id, caller),
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_audit_durable_when_caller_rolls_back(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    audit_pool: ConnectionPool[psycopg.Connection[object]],
) -> None:
    """The core #111 invariant. Caller opens a transaction, calls
    load_credential_for_provider_use with audit_pool, then ROLLS
    BACK. The audit row written on the side connection must remain
    on disk because it was committed independently."""
    op_id = uuid4()
    _seed_operator(ebull_test_conn, op_id)
    _seed_credential(ebull_test_conn, operator_id=op_id)

    # Caller starts a transaction, loads the credential, rolls back.
    with psycopg.connect(_test_database_url()) as caller_conn:
        # Force an explicit transaction by issuing a SELECT first
        # (autocommit is off by default).
        caller_conn.execute("SELECT 1").fetchone()
        secret = load_credential_for_provider_use(
            caller_conn,
            operator_id=op_id,
            provider="etoro",
            label="api_key",
            environment="demo",
            caller="rollback_durability_test",
            audit_pool=audit_pool,
        )
        assert secret == "test-secret-1234"
        caller_conn.rollback()

    # Audit row must still be on disk (side-connection commit is
    # independent of the caller's rollback).
    assert (
        _count_audit_rows(
            ebull_test_conn,
            operator_id=op_id,
            caller="rollback_durability_test",
        )
        == 1
    )


def test_audit_durable_on_not_found_path(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    audit_pool: ConnectionPool[psycopg.Connection[object]],
) -> None:
    """Failure path 1: no active credential matches. Function raises
    CredentialNotFound. Audit row still written on the side conn."""
    op_id = uuid4()
    _seed_operator(ebull_test_conn, op_id)
    # Deliberately seed nothing — the lookup must raise.

    with psycopg.connect(_test_database_url()) as caller_conn:
        with pytest.raises(CredentialNotFound):
            load_credential_for_provider_use(
                caller_conn,
                operator_id=op_id,
                provider="etoro",
                label="api_key",
                environment="demo",
                caller="not_found_audit_test",
                audit_pool=audit_pool,
            )
        caller_conn.rollback()

    assert (
        _count_audit_rows(
            ebull_test_conn,
            operator_id=op_id,
            caller="not_found_audit_test",
        )
        == 1
    )


def test_audit_records_correct_operator_under_concurrent_calls(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    audit_pool: ConnectionPool[psycopg.Connection[object]],
) -> None:
    """Cross-operator isolation. Operator A's load must produce a
    row attributed to A, and Operator B's load must produce a row
    attributed to B — the side-connection pool must not race the
    operator id when two callers hit it back-to-back. Without a
    proper per-call write, a shared mutable buffer would risk
    misattributing the audit row."""
    op_a = uuid4()
    op_b = uuid4()
    _seed_operator(ebull_test_conn, op_a)
    _seed_operator(ebull_test_conn, op_b)
    _seed_credential(ebull_test_conn, operator_id=op_a, plaintext="secret-A")
    _seed_credential(ebull_test_conn, operator_id=op_b, plaintext="secret-B")

    with psycopg.connect(_test_database_url()) as caller_conn:
        load_credential_for_provider_use(
            caller_conn,
            operator_id=op_a,
            provider="etoro",
            label="api_key",
            environment="demo",
            caller="cross_operator_test",
            audit_pool=audit_pool,
        )
        load_credential_for_provider_use(
            caller_conn,
            operator_id=op_b,
            provider="etoro",
            label="api_key",
            environment="demo",
            caller="cross_operator_test",
            audit_pool=audit_pool,
        )
        caller_conn.commit()

    # Each operator gets exactly one row attributed to them.
    assert _count_audit_rows(ebull_test_conn, operator_id=op_a, caller="cross_operator_test") == 1
    assert _count_audit_rows(ebull_test_conn, operator_id=op_b, caller="cross_operator_test") == 1


def test_audit_pool_none_falls_back_to_caller_conn(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Back-compat: callers that don't pass audit_pool still work,
    and the audit row lands on the caller's transaction. Caller
    must commit to make the row durable.

    This guards the pre-#111 contract for tests / scripts that
    haven't been migrated to the new signature yet."""
    op_id = uuid4()
    _seed_operator(ebull_test_conn, op_id)
    _seed_credential(ebull_test_conn, operator_id=op_id)

    with psycopg.connect(_test_database_url()) as caller_conn:
        load_credential_for_provider_use(
            caller_conn,
            operator_id=op_id,
            provider="etoro",
            label="api_key",
            environment="demo",
            caller="back_compat_test",
            # audit_pool intentionally None
        )
        caller_conn.commit()

    assert (
        _count_audit_rows(
            ebull_test_conn,
            operator_id=op_id,
            caller="back_compat_test",
        )
        == 1
    )
