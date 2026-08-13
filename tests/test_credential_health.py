"""Tests for app.services.credential_health (issue #975, parent #974).

Spec: docs/superpowers/specs/2026-05-06-credential-health-precondition-design.md.

Coverage:
  * _should_change_state — pure logic; full truth table including REJECTED-sticky.
  * get_operator_credential_health — all 4 aggregate states + missing-label cases + precedence.
  * record_row_health_transition (via _do_health_transition for tx control):
    - row UPDATE under FOR UPDATE.
    - last_health_check_at always touched.
    - health_state changes only when _should_change_state allows.
    - operator_credential_health_transitions UPSERT only on REJECTED -> VALID aggregate.
    - NOTIFY emitted only on aggregate movement; payload shape pinned.
  * get_last_recovered_at — missing row, NULL row, present row.
  * Row-factory pinning — aggregate query result shape locked to dict_row.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from typing import Any
from uuid import UUID, uuid4

import psycopg
import psycopg.rows
import pytest

from app.security import secrets_crypto
from app.services.credential_health import (
    NOTIFY_CHANNEL,
    REQUIRED_LABELS_BY_PROVIDER,
    CredentialHealth,
    _do_health_transition,
    _should_change_state,
    get_last_recovered_at,
    get_operator_credential_health,
    notify_aggregate_if_changed,
)
from tests.fixtures.ebull_test_db import (
    ebull_test_conn,  # noqa: F401
    test_database_url,
)


@pytest.fixture(autouse=True)
def _key() -> Iterator[None]:
    """secrets_crypto needs an active key for the broker_credentials encrypt()
    path to work — even though the health module doesn't encrypt, fixtures
    that insert via the service layer would. Set up once per test."""
    secrets_crypto.set_active_key(os.urandom(32))
    yield
    secrets_crypto._reset_for_tests()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _insert_operator(conn: psycopg.Connection[Any]) -> UUID:
    """Insert a synthetic operator row; return its UUID."""
    op_id = uuid4()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO operators (operator_id, username, password_hash) VALUES (%s, %s, %s)",
            (op_id, f"op-{op_id.hex[:8]}", "argon2:dummy"),
        )
    conn.commit()
    return op_id


def _insert_credential(
    conn: psycopg.Connection[Any],
    *,
    operator_id: UUID,
    label: str,
    health_state: str = "untested",
    revoked: bool = False,
    environment: str = "demo",
) -> UUID:
    """Insert a synthetic broker_credentials row at a given health_state."""
    cred_id = uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO broker_credentials
                (id, operator_id, provider, label, environment,
                 ciphertext, last_four, key_version, health_state, revoked_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                cred_id,
                operator_id,
                "etoro",
                label,
                environment,
                b"\x00" * 32,  # bytea ciphertext placeholder; tests don't decrypt
                "abcd",
                1,
                health_state,
                None,
            ),
        )
        if revoked:
            cur.execute(
                "UPDATE broker_credentials SET revoked_at = NOW() WHERE id = %s",
                (cred_id,),
            )
    conn.commit()
    return cred_id


def _seed_pair(
    conn: psycopg.Connection[Any],
    *,
    api_state: str | None,
    user_state: str | None,
) -> UUID:
    """Insert an operator + broker_credentials rows for both labels.

    state=None means "no row present" so the aggregate sees a missing label.
    """
    op_id = _insert_operator(conn)
    if api_state is not None:
        _insert_credential(conn, operator_id=op_id, label="api_key", health_state=api_state)
    if user_state is not None:
        _insert_credential(conn, operator_id=op_id, label="user_key", health_state=user_state)
    return op_id


# ---------------------------------------------------------------------------
# _should_change_state — pure logic, no DB
# ---------------------------------------------------------------------------


class TestShouldChangeState:
    @pytest.mark.parametrize(
        "old,new,source,expected",
        [
            # Untested rows: any change applies regardless of source.
            ("untested", "valid", "probe", True),
            ("untested", "valid", "incidental", True),
            ("untested", "rejected", "probe", True),
            ("untested", "rejected", "incidental", True),
            # Valid -> rejected: any source flips.
            ("valid", "rejected", "probe", True),
            ("valid", "rejected", "incidental", True),
            # Rejected -> valid: ONLY probe source.
            ("rejected", "valid", "probe", True),
            ("rejected", "valid", "incidental", False),
            # Same-state transitions: never.
            ("valid", "valid", "probe", False),
            ("valid", "valid", "incidental", False),
            ("rejected", "rejected", "probe", False),
            ("rejected", "rejected", "incidental", False),
            ("untested", "untested", "probe", False),
            ("untested", "untested", "incidental", False),
        ],
    )
    def test_truth_table(self, old: str, new: str, source: str, expected: bool) -> None:
        assert (
            _should_change_state(old_state=old, new_state=new, source=source)  # type: ignore[arg-type]
            == expected
        )


# ---------------------------------------------------------------------------
# get_operator_credential_health
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestGetOperatorCredentialHealth:
    def test_both_valid_returns_valid(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        op = _seed_pair(ebull_test_conn, api_state="valid", user_state="valid")
        assert get_operator_credential_health(ebull_test_conn, operator_id=op) == CredentialHealth.VALID

    def test_both_untested_returns_untested(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        op = _seed_pair(ebull_test_conn, api_state="untested", user_state="untested")
        assert get_operator_credential_health(ebull_test_conn, operator_id=op) == CredentialHealth.UNTESTED

    def test_both_rejected_returns_rejected(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        op = _seed_pair(ebull_test_conn, api_state="rejected", user_state="rejected")
        assert get_operator_credential_health(ebull_test_conn, operator_id=op) == CredentialHealth.REJECTED

    def test_no_rows_returns_missing(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        op = _seed_pair(ebull_test_conn, api_state=None, user_state=None)
        assert get_operator_credential_health(ebull_test_conn, operator_id=op) == CredentialHealth.MISSING

    def test_one_label_missing_returns_missing(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        # api_key valid, user_key absent -> MISSING
        op = _seed_pair(ebull_test_conn, api_state="valid", user_state=None)
        assert get_operator_credential_health(ebull_test_conn, operator_id=op) == CredentialHealth.MISSING

    def test_rejected_dominates_missing(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """Locked precedence (Codex r3.2): REJECTED > MISSING."""
        op = _seed_pair(ebull_test_conn, api_state="rejected", user_state=None)
        assert get_operator_credential_health(ebull_test_conn, operator_id=op) == CredentialHealth.REJECTED

    def test_rejected_dominates_valid(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        op = _seed_pair(ebull_test_conn, api_state="rejected", user_state="valid")
        assert get_operator_credential_health(ebull_test_conn, operator_id=op) == CredentialHealth.REJECTED

    def test_untested_with_valid_returns_untested(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        # one valid, one untested -> UNTESTED (any_untested is true).
        op = _seed_pair(ebull_test_conn, api_state="valid", user_state="untested")
        assert get_operator_credential_health(ebull_test_conn, operator_id=op) == CredentialHealth.UNTESTED

    def test_revoked_rows_excluded(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """A revoked row should not affect the aggregate."""
        op_id = _insert_operator(ebull_test_conn)
        # Revoked api_key (REJECTED state, but revoked) — should not surface.
        _insert_credential(
            ebull_test_conn,
            operator_id=op_id,
            label="api_key",
            health_state="rejected",
            revoked=True,
        )
        # Active api_key (valid).
        _insert_credential(ebull_test_conn, operator_id=op_id, label="api_key", health_state="valid")
        _insert_credential(ebull_test_conn, operator_id=op_id, label="user_key", health_state="valid")
        assert get_operator_credential_health(ebull_test_conn, operator_id=op_id) == CredentialHealth.VALID

    def test_unknown_provider_raises(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        op_id = _insert_operator(ebull_test_conn)
        with pytest.raises(KeyError):
            get_operator_credential_health(ebull_test_conn, operator_id=op_id, provider="kraken")

    def test_required_labels_constant_locked(self) -> None:
        """Locks the etoro (api_key, user_key) requirement so a future
        provider addition doesn't accidentally relax health for etoro."""
        assert REQUIRED_LABELS_BY_PROVIDER["etoro"] == ("api_key", "user_key")

    def test_environment_scoping_prevents_cross_pollination(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """Codex pre-push r1.2: aggregate must be scoped to environment.

        A demo api_key VALID + real user_key VALID should NOT make
        either env's aggregate VALID — neither has a complete pair.
        """
        op_id = _insert_operator(ebull_test_conn)
        _insert_credential(
            ebull_test_conn,
            operator_id=op_id,
            label="api_key",
            health_state="valid",
            environment="demo",
        )
        _insert_credential(
            ebull_test_conn,
            operator_id=op_id,
            label="user_key",
            health_state="valid",
            environment="real",
        )

        # demo: api_key valid, user_key absent -> MISSING
        assert (
            get_operator_credential_health(ebull_test_conn, operator_id=op_id, environment="demo")
            == CredentialHealth.MISSING
        )
        # real: user_key valid, api_key absent -> MISSING
        assert (
            get_operator_credential_health(ebull_test_conn, operator_id=op_id, environment="real")
            == CredentialHealth.MISSING
        )


# ---------------------------------------------------------------------------
# notify_aggregate_if_changed (used by PUT /broker-credentials/replace)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestNotifyAggregateIfChanged:
    def test_emits_notify_when_aggregate_moves(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """Mutate rows directly, then call helper — NOTIFY should fire."""
        op_id = _seed_pair(ebull_test_conn, api_state="valid", user_state="valid")

        url = test_database_url()
        listen_conn = psycopg.connect(url, autocommit=True)
        sender_conn = psycopg.connect(url)
        try:
            listen_conn.execute(f"LISTEN {NOTIFY_CHANNEL}")

            with sender_conn.transaction():
                # Snapshot before mutation.
                old = get_operator_credential_health(sender_conn, operator_id=op_id, environment="demo")
                assert old == CredentialHealth.VALID

                # Revoke the api_key row (mimics PUT /replace's first step).
                with sender_conn.cursor() as cur:
                    cur.execute(
                        "UPDATE broker_credentials SET revoked_at = NOW() "
                        "WHERE operator_id = %s AND label = 'api_key' AND revoked_at IS NULL",
                        (op_id,),
                    )

                # Notify after mutation.
                notify_aggregate_if_changed(
                    sender_conn,
                    operator_id=op_id,
                    provider="etoro",
                    environment="demo",
                    old_aggregate=old,
                )

            payloads = []
            for n in listen_conn.notifies(timeout=2.0, stop_after=1):
                payloads.append(json.loads(n.payload))

            assert payloads, "expected NOTIFY for VALID -> MISSING transition"
            assert payloads[0]["old_aggregate"] == "valid"
            assert payloads[0]["new_aggregate"] == "missing"
        finally:
            sender_conn.close()
            listen_conn.close()

    def test_no_notify_when_aggregate_unchanged(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """Idempotent: aggregate same before+after means no NOTIFY."""
        op_id = _seed_pair(ebull_test_conn, api_state="valid", user_state="valid")

        url = test_database_url()
        listen_conn = psycopg.connect(url, autocommit=True)
        sender_conn = psycopg.connect(url)
        try:
            listen_conn.execute(f"LISTEN {NOTIFY_CHANNEL}")

            with sender_conn.transaction():
                old = get_operator_credential_health(sender_conn, operator_id=op_id, environment="demo")
                # No mutation between snapshot and helper call.
                notify_aggregate_if_changed(
                    sender_conn,
                    operator_id=op_id,
                    provider="etoro",
                    environment="demo",
                    old_aggregate=old,
                )

            payloads = list(listen_conn.notifies(timeout=0.5, stop_after=1))
            assert payloads == [], f"expected no NOTIFY; got {payloads}"
        finally:
            sender_conn.close()
            listen_conn.close()


# ---------------------------------------------------------------------------
# _do_health_transition — drives row update, transitions row, NOTIFY
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestDoHealthTransition:
    def test_untested_to_valid_via_probe(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        op_id = _seed_pair(ebull_test_conn, api_state="untested", user_state="valid")
        api_id = _credential_id(ebull_test_conn, op_id, "api_key")

        with ebull_test_conn.transaction():
            _do_health_transition(
                ebull_test_conn,
                credential_id=api_id,
                new_state="valid",
                source="probe",
                error_detail=None,
            )

        state = _row_state(ebull_test_conn, api_id)
        assert state["health_state"] == "valid"
        assert state["last_health_check_at"] is not None
        assert state["last_health_error"] is None

    def test_rejected_blocked_by_incidental_success(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """REJECTED-stickiness: incidental 2xx must NOT clear rejected."""
        op_id = _seed_pair(ebull_test_conn, api_state="rejected", user_state="valid")
        api_id = _credential_id(ebull_test_conn, op_id, "api_key")

        with ebull_test_conn.transaction():
            _do_health_transition(
                ebull_test_conn,
                credential_id=api_id,
                new_state="valid",
                source="incidental",
                error_detail=None,
            )

        # Row state is unchanged.
        state = _row_state(ebull_test_conn, api_id)
        assert state["health_state"] == "rejected"
        # last_health_check_at IS touched (we observed the call).
        assert state["last_health_check_at"] is not None

    def test_sticky_skip_preserves_last_health_error(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """PREVENTION (review #981): incidental success on a REJECTED
        row must NOT wipe last_health_error.

        Pre-fix the helper unconditionally rewrote last_health_error
        on every call; this test pins the new contract that the
        column is only touched when an actual transition happens.
        """
        op_id = _seed_pair(ebull_test_conn, api_state="rejected", user_state="valid")
        api_id = _credential_id(ebull_test_conn, op_id, "api_key")

        # Seed a non-NULL error message that the operator should keep
        # seeing in the admin UI.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE broker_credentials SET last_health_error = %s WHERE id = %s",
                ("HTTP 401 Unauthorized — original rejection message", api_id),
            )
        ebull_test_conn.commit()

        # Incidental success that should be sticky-skipped.
        with ebull_test_conn.transaction():
            _do_health_transition(
                ebull_test_conn,
                credential_id=api_id,
                new_state="valid",
                source="incidental",
                error_detail=None,
            )

        state = _row_state(ebull_test_conn, api_id)
        assert state["health_state"] == "rejected"
        assert state["last_health_error"] == "HTTP 401 Unauthorized — original rejection message"

    def test_rejected_cleared_by_probe_success(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        op_id = _seed_pair(ebull_test_conn, api_state="rejected", user_state="valid")
        api_id = _credential_id(ebull_test_conn, op_id, "api_key")

        with ebull_test_conn.transaction():
            _do_health_transition(
                ebull_test_conn,
                credential_id=api_id,
                new_state="valid",
                source="probe",
                error_detail=None,
            )

        state = _row_state(ebull_test_conn, api_id)
        assert state["health_state"] == "valid"

    def test_recovery_writes_transitions_row(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """REJECTED -> VALID at OPERATOR level upserts the transitions row."""
        # Both rows REJECTED: aggregate is REJECTED.
        op_id = _seed_pair(ebull_test_conn, api_state="rejected", user_state="rejected")
        api_id = _credential_id(ebull_test_conn, op_id, "api_key")
        user_id = _credential_id(ebull_test_conn, op_id, "user_key")

        # Clear api_key first via probe success — aggregate stays REJECTED
        # because user_key still rejected. No transitions row yet.
        with ebull_test_conn.transaction():
            _do_health_transition(
                ebull_test_conn,
                credential_id=api_id,
                new_state="valid",
                source="probe",
                error_detail=None,
            )
        assert get_last_recovered_at(ebull_test_conn, operator_id=op_id) is None

        # Clear user_key: aggregate flips to VALID. Transitions row appears.
        with ebull_test_conn.transaction():
            _do_health_transition(
                ebull_test_conn,
                credential_id=user_id,
                new_state="valid",
                source="probe",
                error_detail=None,
            )
        recovered_at = get_last_recovered_at(ebull_test_conn, operator_id=op_id)
        assert recovered_at is not None

    def test_untested_to_valid_does_not_write_transitions_row(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """UNTESTED -> VALID is a fresh-bootstrap path, NOT recovery."""
        op_id = _seed_pair(ebull_test_conn, api_state="untested", user_state="untested")
        api_id = _credential_id(ebull_test_conn, op_id, "api_key")
        user_id = _credential_id(ebull_test_conn, op_id, "user_key")

        with ebull_test_conn.transaction():
            _do_health_transition(
                ebull_test_conn,
                credential_id=api_id,
                new_state="valid",
                source="probe",
                error_detail=None,
            )
            _do_health_transition(
                ebull_test_conn,
                credential_id=user_id,
                new_state="valid",
                source="probe",
                error_detail=None,
            )

        # Operator aggregate moved UNTESTED -> VALID, but the
        # recovery timestamp only matters for AUTH_EXPIRED suppression
        # which has no relevance for an operator that was never REJECTED.
        # No transitions row written.
        assert get_last_recovered_at(ebull_test_conn, operator_id=op_id) is None
        assert get_operator_credential_health(ebull_test_conn, operator_id=op_id) == CredentialHealth.VALID

    def test_rejected_to_untested_via_replace_records_recovery(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """REJECTED -> UNTESTED at aggregate writes the recovery row.

        Mirrors the realistic flow: operator with rejected creds
        revokes the bad row + inserts a fresh untested replacement
        via PUT /replace. The aggregate moves REJECTED -> UNTESTED;
        the recovery timestamp must be set so AUTH_EXPIRED
        suppression kicks in NOW, not waiting for the later
        validate-stored (Codex pre-push r2.1).
        """
        op_id = _seed_pair(ebull_test_conn, api_state="rejected", user_state="rejected")

        with ebull_test_conn.transaction():
            old = get_operator_credential_health(ebull_test_conn, operator_id=op_id)
            assert old == CredentialHealth.REJECTED

            # Simulate two PUT /replace calls that fixed both rejected
            # rows in one operator action: revoke each row and insert
            # a fresh untested replacement. Inlined rather than using
            # _insert_credential because that helper commits, which
            # is forbidden inside an active transaction context.
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    "UPDATE broker_credentials SET revoked_at = NOW() WHERE operator_id = %s AND revoked_at IS NULL",
                    (op_id,),
                )
                for label in ("api_key", "user_key"):
                    cur.execute(
                        """
                        INSERT INTO broker_credentials
                            (id, operator_id, provider, label, environment,
                             ciphertext, last_four, key_version, health_state)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            uuid4(),
                            op_id,
                            "etoro",
                            label,
                            "demo",
                            b"\x00" * 32,
                            "abcd",
                            1,
                            "untested",
                        ),
                    )

            notify_aggregate_if_changed(
                ebull_test_conn,
                operator_id=op_id,
                provider="etoro",
                environment="demo",
                old_aggregate=old,
            )

        # Aggregate is now UNTESTED (both rows untested); recovery
        # timestamp is set because the operator moved OUT of REJECTED.
        # AUTH_EXPIRED suppression kicks in NOW, not waiting for the
        # later validate-stored.
        assert get_operator_credential_health(ebull_test_conn, operator_id=op_id) == CredentialHealth.UNTESTED
        recovered_at = get_last_recovered_at(ebull_test_conn, operator_id=op_id)
        assert recovered_at is not None

    def test_failure_writes_error_detail(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        op_id = _seed_pair(ebull_test_conn, api_state="valid", user_state="valid")
        api_id = _credential_id(ebull_test_conn, op_id, "api_key")

        with ebull_test_conn.transaction():
            _do_health_transition(
                ebull_test_conn,
                credential_id=api_id,
                new_state="rejected",
                source="incidental",
                error_detail="HTTP 401 Unauthorized",
            )

        state = _row_state(ebull_test_conn, api_id)
        assert state["health_state"] == "rejected"
        assert state["last_health_error"] == "HTTP 401 Unauthorized"

    def test_idempotent_same_state_does_not_change_health_state_updated_at(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        op_id = _seed_pair(ebull_test_conn, api_state="valid", user_state="valid")
        api_id = _credential_id(ebull_test_conn, op_id, "api_key")

        before = _row_state(ebull_test_conn, api_id)["health_state_updated_at"]

        with ebull_test_conn.transaction():
            _do_health_transition(
                ebull_test_conn,
                credential_id=api_id,
                new_state="valid",
                source="probe",
                error_detail=None,
            )

        after = _row_state(ebull_test_conn, api_id)
        # health_state_updated_at unchanged (no transition).
        assert after["health_state_updated_at"] == before
        # last_health_check_at WAS bumped (we observed a call).
        assert after["last_health_check_at"] is not None

    def test_revoked_credential_id_skipped(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """A credential_id whose row is revoked should be a no-op + warning."""
        op_id = _insert_operator(ebull_test_conn)
        cred_id = _insert_credential(
            ebull_test_conn,
            operator_id=op_id,
            label="api_key",
            health_state="rejected",
            revoked=True,
        )

        with ebull_test_conn.transaction():
            _do_health_transition(
                ebull_test_conn,
                credential_id=cred_id,
                new_state="valid",
                source="probe",
                error_detail=None,
            )

        # Row state still rejected (no update; the helper bailed early).
        state = _row_state(ebull_test_conn, cred_id)
        assert state["health_state"] == "rejected"

    def test_notify_emitted_on_aggregate_movement(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """LISTEN on a separate connection; assert payload arrives.

        Uses a fresh sender connection rather than ebull_test_conn so
        the fixture's rollback-at-teardown lifecycle can't mask the
        commit semantics under test.
        """
        op_id = _seed_pair(ebull_test_conn, api_state="untested", user_state="untested")
        api_id = _credential_id(ebull_test_conn, op_id, "api_key")
        user_id = _credential_id(ebull_test_conn, op_id, "user_key")

        url = test_database_url()
        listen_conn = psycopg.connect(url, autocommit=True)
        sender_conn = psycopg.connect(url)
        try:
            listen_conn.execute(f"LISTEN {NOTIFY_CHANNEL}")

            with sender_conn.transaction():
                _do_health_transition(
                    sender_conn,
                    credential_id=api_id,
                    new_state="valid",
                    source="probe",
                    error_detail=None,
                )
                _do_health_transition(
                    sender_conn,
                    credential_id=user_id,
                    new_state="valid",
                    source="probe",
                    error_detail=None,
                )
            # Both transitions inside the same outer tx — the second
            # one moves the operator aggregate to VALID.

            payloads: list[dict[str, Any]] = []
            for n in listen_conn.notifies(timeout=2.0, stop_after=2):
                payloads.append(json.loads(n.payload))

            assert any(p["new_aggregate"] == "valid" and p["operator_id"] == str(op_id) for p in payloads), (
                f"expected VALID notify; got {payloads!r}"
            )
        finally:
            sender_conn.close()
            listen_conn.close()

    def test_notify_payload_shape(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        """Pin payload field set so subscribers can rely on it."""
        op_id = _seed_pair(ebull_test_conn, api_state="untested", user_state="untested")
        api_id = _credential_id(ebull_test_conn, op_id, "api_key")
        user_id = _credential_id(ebull_test_conn, op_id, "user_key")

        url = test_database_url()
        listen_conn = psycopg.connect(url, autocommit=True)
        sender_conn = psycopg.connect(url)
        try:
            listen_conn.execute(f"LISTEN {NOTIFY_CHANNEL}")

            with sender_conn.transaction():
                _do_health_transition(
                    sender_conn,
                    credential_id=api_id,
                    new_state="valid",
                    source="probe",
                    error_detail=None,
                )
                _do_health_transition(
                    sender_conn,
                    credential_id=user_id,
                    new_state="valid",
                    source="probe",
                    error_detail=None,
                )

            payloads = []
            for n in listen_conn.notifies(timeout=2.0, stop_after=1):
                payloads.append(json.loads(n.payload))

            assert payloads, "expected at least one notify"
            payload = payloads[0]
            assert set(payload.keys()) == {
                "operator_id",
                "provider",
                "environment",
                "old_aggregate",
                "new_aggregate",
                "at",
            }
            assert payload["provider"] == "etoro"
            assert payload["environment"] == "demo"
        finally:
            sender_conn.close()
            listen_conn.close()


# ---------------------------------------------------------------------------
# get_last_recovered_at
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestGetLastRecoveredAt:
    def test_missing_row_returns_none(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        op_id = uuid4()  # no row anywhere.
        assert get_last_recovered_at(ebull_test_conn, operator_id=op_id) is None

    def test_null_value_returns_none(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        op_id = _insert_operator(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO operator_credential_health_transitions (operator_id, last_recovered_at) VALUES (%s, NULL)",
                (op_id,),
            )
        ebull_test_conn.commit()
        assert get_last_recovered_at(ebull_test_conn, operator_id=op_id) is None

    def test_present_value_returned(
        self,
        ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
    ) -> None:
        op_id = _insert_operator(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO operator_credential_health_transitions "
                "(operator_id, last_recovered_at) VALUES (%s, NOW())",
                (op_id,),
            )
        ebull_test_conn.commit()
        result = get_last_recovered_at(ebull_test_conn, operator_id=op_id)
        assert result is not None


# ---------------------------------------------------------------------------
# Local helpers used by the test classes
# ---------------------------------------------------------------------------


def _credential_id(conn: psycopg.Connection[Any], op_id: UUID, label: str) -> UUID:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM broker_credentials WHERE operator_id = %s AND label = %s AND revoked_at IS NULL",
            (op_id, label),
        )
        row = cur.fetchone()
    assert row is not None
    return row[0]


def _row_state(conn: psycopg.Connection[Any], cred_id: UUID) -> dict[str, Any]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT health_state, health_state_updated_at, "
            "last_health_check_at, last_health_error "
            "FROM broker_credentials WHERE id = %s",
            (cred_id,),
        )
        row = cur.fetchone()
    assert row is not None
    return row
