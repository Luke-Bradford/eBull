"""#3592 slice 4b-ii — invalidate a stored ranking-pot-v2 look (spec §7 "Invalidation").

Appends one ``ranking_pot_looks`` ``invalidation`` row citing a v2 ``result`` (``sql/448``: append-only; an
invalidation carries no verdict, a non-blank note, and cites its result). The result row is never changed: v2's
readers show it as invalidated. It does not undo a wind-down the look already caused (the state machine has no way
back from ``winding_down``), and nothing recomputes the look (a ``recomputation`` writer is out of scope, §10).

Dry run (default) inserts and rolls back, printing what would be written:

    PYTHONPATH=. uv run python -m scripts.ranking_pot_v2_invalidate_look --look-id <id> --note "<why>" \
        --evidence "<issue comment URL or query>"

⚠ ``--apply`` is the supervisor's, from ``~/Dev/eBull`` at ``origin/main``, never the autonomy loop's.

Exit status: 0 when the invalidation would be (or was) written, 1 on a refusal.
"""

from __future__ import annotations

import argparse
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from app.config import settings
from app.services import ranking_pot_v2 as v2
from app.services.ranking_pot_v2_policy import RANKING_POT_V2_POLICY_HASH

Conn = psycopg.Connection[Any]


def invalidate(conn: Conn, *, look_id: int, note: str, evidence: str, apply: bool) -> tuple[str | None, int | None]:
    """(refusal, invalidation look_id). One transaction, committed only with ``apply`` and no refusal; any refusal
    or exception rolls it back."""
    if conn.autocommit:
        raise RuntimeError("invalidate needs a transaction (a dry run on an autocommit connection would persist)")
    try:
        refusal, new_id = _invalidate(conn, look_id=look_id, note=note, evidence=evidence)
    except BaseException:
        conn.rollback()
        raise
    if apply and refusal is None:
        conn.commit()
    else:
        conn.rollback()
    return refusal, new_id


def _invalidate(conn: Conn, *, look_id: int, note: str, evidence: str) -> tuple[str | None, int | None]:
    if not note.strip() or not evidence.strip():
        return "note_and_evidence_required", None
    row = conn.execute(
        "SELECT l.declaration_id, l.look_months, l.endpoint_session, l.kind, d.strategy_id "
        "FROM ranking_pot_looks l JOIN ranking_pot_declarations d USING (declaration_id) WHERE l.look_id = %s",
        (look_id,),
    ).fetchone()
    if row is None:
        return "no_such_look", None
    declaration_id, months, end, kind, strategy_id = row
    if strategy_id != v2.STRATEGY_ID:
        return "not_a_v2_look", None
    if kind != "result":
        return "not_a_result", None
    # The step's look writer's lock: an invalidation never interleaves with a look being computed.
    conn.execute(
        "SELECT 1 FROM ranking_pot_declarations WHERE declaration_id = %s FOR NO KEY UPDATE", (declaration_id,)
    )
    prior = conn.execute(
        "SELECT look_id FROM ranking_pot_looks WHERE cites_look_id = %s AND kind = 'invalidation'", (look_id,)
    ).fetchone()
    if prior is not None:
        return f"already_invalidated ({prior[0]})", None
    new = conn.execute(
        "INSERT INTO ranking_pot_looks (declaration_id, look_months, endpoint_session, kind, cites_look_id, note, "
        "detail, policy_hash, computed_at) VALUES (%s, %s, %s, 'invalidation', %s, %s, %s, %s, now()) "
        "RETURNING look_id",
        (declaration_id, months, end, look_id, note, Jsonb({"evidence": evidence}), RANKING_POT_V2_POLICY_HASH),
    ).fetchone()
    assert new is not None
    return None, int(new[0])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--look-id", type=int, required=True)
    parser.add_argument("--note", required=True, help="why the look is invalid")
    parser.add_argument("--evidence", required=True, help="where the error is recorded (#3592 comment, query)")
    parser.add_argument("--apply", action="store_true", help="commit the invalidation (supervisor only)")
    args = parser.parse_args(argv)
    with psycopg.connect(settings.database_url) as conn:
        refusal, new_id = invalidate(
            conn, look_id=args.look_id, note=args.note, evidence=args.evidence, apply=args.apply
        )
    if refusal is not None:
        print(f"refused: {refusal}")
        return 1
    print(f"{'APPLIED' if args.apply else 'dry run (rolled back)'}: invalidation {new_id} cites look {args.look_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
