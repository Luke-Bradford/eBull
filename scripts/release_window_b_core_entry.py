"""Attended, demo-only release of one stranded window-B core authority (#2961).

    uv run python -m scripts.release_window_b_core_entry \\
        --order-id N --operator-id ID --attestation "..."

Spec: ``docs/proposals/execution/2026-09-23-core-window-b-attended-release.md``. The act
is ``app.services.strategy_core_window_b_release.release_window_b_core_entry``; this
file only wires it to the real database, clock and demo credentials. There is no API
endpoint on purpose: an endpoint is reachable by anything holding a session.

⚠ Refuses from a linked git worktree and without a TTY (accident controls, not proof
of attendance). ⚠ Blocks for at least ``WINDOW_B_VISIBILITY_WAIT`` while holding the
core submission lock. ⚠ Makes ZERO broker mutations: its one broker call is the
informational account read.

Exit code 0 on PASS, 1 on a named refusal.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from uuid import UUID

import psycopg
from psycopg.pq import TransactionStatus

from app.config import settings
from app.providers.broker import BrokerProvider
from app.providers.implementations.etoro_broker import EtoroBrokerProvider
from app.security.master_key import ensure_broker_key_loaded
from app.services.broker_credentials import CredentialNotFound, load_credential_with_id_for_provider_use
from app.services.strategy_core_window_b_release import WindowBRefused, release_window_b_core_entry

_CALLER = "core_window_b_release"


@contextmanager
def _order_account_broker(operator_id: UUID, api_key_id: UUID, user_key_id: UUID) -> Iterator[BrokerProvider]:
    """The broker of the account that submitted the order, or a refusal.

    Loaded on its own connection so the release's lock-holding session is never used
    for credential audit writes.
    """
    with psycopg.connect(settings.database_url) as conn:
        ensure_broker_key_loaded(conn)
        loaded = []
        try:
            for label in ("api_key", "user_key"):
                loaded.append(
                    load_credential_with_id_for_provider_use(
                        conn,
                        operator_id=operator_id,
                        provider="etoro",
                        label=label,
                        environment="demo",
                        caller=_CALLER,
                    )
                )
                conn.commit()
        except Exception as exc:
            # Without an `audit_pool` the loader writes its failed-access audit row
            # into THIS transaction; commit it so the refusal keeps its forensic trail.
            if conn.info.transaction_status == TransactionStatus.INTRANS:
                conn.commit()
            if isinstance(exc, CredentialNotFound):
                raise WindowBRefused("window_b_credentials_unresolved", "no live demo credential") from exc
            raise
    if (loaded[0].id, loaded[1].id) != (api_key_id, user_key_id):
        raise WindowBRefused(
            "window_b_credentials_unresolved",
            "the order's credentials are no longer the live demo pair; the witness would read another account",
        )
    with EtoroBrokerProvider(api_key=loaded[0].plaintext, user_key=loaded[1].plaintext, env="demo") as broker:
        yield broker


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--order-id", type=int, required=True)
    parser.add_argument("--operator-id", required=True, help="who is attending, recorded verbatim in the audit")
    parser.add_argument("--attestation", required=True, help="why this release is sound, recorded verbatim")
    args = parser.parse_args(argv)

    with psycopg.connect(settings.database_url) as conn:
        result = release_window_b_core_entry(
            conn,
            order_id=args.order_id,
            operator_id=args.operator_id,
            attestation=args.attestation,
            broker_factory=_order_account_broker,
            environment=settings.etoro_env,
        )
    verdict: dict[str, Any] = {
        "order_id": result.order_id,
        "verdict": "PASS" if result.passed else "REFUSED",
        "slug": result.slug,
        "decision_id": result.decision_id,
        "evidence": result.evidence,
    }
    print(json.dumps(verdict, indent=2, default=str, sort_keys=True))
    return 0 if result.passed else 1


if __name__ == "__main__":
    sys.exit(main())
