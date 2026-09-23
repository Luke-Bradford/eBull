"""Attended, demo-only release of one stranded recommendation claim (#2942).

Two invocations. First SEE the stored payload, then release against its digest:

    uv run python -m scripts.release_recommendation_window_b --order-id N --show
    uv run python -m scripts.release_recommendation_window_b --order-id N \\
        --operator-id ID --payload-sha256 <hex from --show> --attestation "..."

Spec: ``docs/proposals/execution/2026-09-23-recommendation-window-b-attended-release.md``.
The act is ``app.services.recommendation_window_b_release.release_recommendation_window_b``;
this file only wires it to the real database, clock and the ORDER's recorded demo
credentials. There is no API endpoint on purpose.

⚠ PRECONDITION: the jobs daemon has been restarted onto a build containing #2942's
sql/412 change before any release. A row written by older code carries no sender,
park or credential columns (and refuses); an OLD executor paused before its claim
would lack the stale-approval re-check.

⚠ Refuses from a linked git worktree and without a TTY (accident controls, not proof
of attendance). ⚠ Blocks for at least ``WINDOW_B_VISIBILITY_WAIT`` while holding this
recommendation's submission key. ⚠ ZERO broker mutations: its one broker call is the
informational account read.

``--show`` takes no lock and writes nothing. Exit codes: 0 RELEASED, 1 REFUSED <slug>,
2 INDETERMINATE (the terminal COMMIT raised; re-run to learn the outcome).
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

from app.config import settings
from app.providers.broker import BrokerProvider
from app.services.recommendation_window_b_release import (
    CREDENTIALS_UNRESOLVED,
    OUTCOME_INDETERMINATE,
    describe_candidate,
    release_recommendation_window_b,
)
from app.services.strategy_core_window_b_release import WindowBRefused
from scripts.release_window_b_core_entry import _order_account_broker

_CALLER = "recommendation_window_b_release"


@contextmanager
def _recommendation_account_broker(api_key_id: UUID, user_key_id: UUID) -> Iterator[BrokerProvider]:
    """The broker of the account the ORDER was sent to, or a refusal (spec Delta 3).

    The owning operator is resolved from the two recorded rows themselves; the
    attestor's ``--operator-id`` never selects the account. Refuses unless both rows
    exist, are unrevoked, are labelled ``api_key`` / ``user_key``, are demo, share one
    operator, and are each still that operator's live row for the label (checked by
    the shared core factory body).
    """
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(
            """
            SELECT id, operator_id, provider, label, environment, revoked_at
            FROM broker_credentials WHERE id = ANY(%s)
            """,
            ([api_key_id, user_key_id],),
        ).fetchall()
    by_id = {row[0]: row for row in rows}
    api, user = by_id.get(api_key_id), by_id.get(user_key_id)
    if (
        api is None
        or user is None
        or api[5] is not None
        or user[5] is not None
        or (api[2], api[3], api[4]) != ("etoro", "api_key", "demo")
        or (user[2], user[3], user[4]) != ("etoro", "user_key", "demo")
        or api[1] != user[1]
    ):
        raise WindowBRefused(
            CREDENTIALS_UNRESOLVED, "the order's recorded credentials are not one operator's unrevoked demo pair"
        )
    try:
        with _order_account_broker(
            api[1], api_key_id, user_key_id, caller=_CALLER, refusal_slug=CREDENTIALS_UNRESOLVED
        ) as broker:
            yield broker
    except WindowBRefused as exc:
        if exc.slug == "window_b_credentials_unresolved":
            raise WindowBRefused(CREDENTIALS_UNRESOLVED, exc.detail, exc.evidence) from exc
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--order-id", type=int, required=True)
    parser.add_argument("--show", action="store_true", help="print the candidate and its digest; no lock, no write")
    parser.add_argument("--operator-id", help="who is attending, recorded verbatim (attests, does not select)")
    parser.add_argument("--payload-sha256", help="the digest --show printed for this order")
    parser.add_argument("--attestation", help="why this release is sound, recorded verbatim")
    args = parser.parse_args(argv)

    if args.show:
        with psycopg.connect(settings.database_url) as conn:
            described = describe_candidate(conn, args.order_id)
        if described is None:
            print(f"order {args.order_id} not found")
            return 1
        print(json.dumps(described, indent=2, default=str, sort_keys=True))
        return 0 if described["candidate_state"] is not None else 1
    missing = [name for name in ("operator_id", "payload_sha256", "attestation") if getattr(args, name) is None]
    if missing:
        parser.error("a release requires " + ", ".join("--" + name.replace("_", "-") for name in missing))

    conn = psycopg.connect(settings.database_url)
    try:
        result = release_recommendation_window_b(
            conn,
            order_id=args.order_id,
            operator_id=args.operator_id,
            attestation=args.attestation,
            payload_sha256=args.payload_sha256,
            broker_factory=_recommendation_account_broker,
            environment=settings.etoro_env,
        )
    finally:
        # Process exit would drop any session key anyway; close explicitly.
        conn.close()
    if result.passed:
        verdict, code = "RELEASED", 0
    elif result.slug == OUTCOME_INDETERMINATE:
        verdict, code = "INDETERMINATE", 2
    else:
        verdict, code = f"REFUSED {result.slug}", 1
    report: dict[str, Any] = {
        "order_id": result.order_id,
        "verdict": verdict,
        "slug": result.slug,
        "decision_id": result.decision_id,
        "evidence": result.evidence,
    }
    print(json.dumps(report, indent=2, default=str, sort_keys=True))
    return code


if __name__ == "__main__":
    sys.exit(main())
