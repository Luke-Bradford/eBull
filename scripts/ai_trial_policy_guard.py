"""#3515 §0 rule 1 — does this working tree still run the policy every live v1 declaration froze?

v1's ``declaration_refusal`` refuses every run whose ``policy_hash`` differs from the one its
declaration froze, so a change to a ``POLICY_MODULES`` file or a ``FROZEN_CONSTANTS`` value that
reaches ``main`` halts the live v1 trial at the next jobs reload. Every fund-v1 slice runs this
before merge and pastes the output in its PR:

    PYTHONPATH=. uv run python -m scripts.ai_trial_policy_guard

It prints the working tree's ``policy_hash()`` and, for every v1 declaration NOT yet wound down
(``ai_trial_wind_down``), the stored hash, from the dev DB — the only deployment. A wound-down
declaration no longer runs v1 code, so it is listed but not compared.

Exit status: 0 when every declaration not wound down matches (or none exists), 1 on a mismatch.

⚠ Pre-merge evidence, not a deployment invariant: an unrelated PR can still edit a hashed file.
The runtime backstop is v1's own ``declaration_refusal``, which fails closed.
"""

from __future__ import annotations

import sys

import psycopg

from app.config import settings
from app.services.ai_trial_policy import policy_hash
from app.services.ai_trial_wind_down import V1Declaration, read_v1_declarations


def verdict(tree_hash: str, declarations: list[V1Declaration]) -> tuple[list[str], bool]:
    """The report lines and whether every declaration not wound down matches ``tree_hash``."""
    lines = [f"working tree policy_hash: {tree_hash}"]
    if not declarations:
        lines.append("no v1 declaration: nothing live to protect")
    ok = True
    for d in declarations:
        if d.wound_down:
            lines.append(f"  declaration {d.declaration_id}: wound down ({d.state}), not compared")
            continue
        match = d.policy_hash == tree_hash
        ok = ok and match
        lines.append(
            f"  declaration {d.declaration_id}: {'MATCH' if match else 'MISMATCH'} "
            f"stored={d.policy_hash} outstanding={','.join(d.outstanding())}"
        )
    lines.append(
        "OK" if ok else "REFUSED: a live v1 declaration's policy_hash is missing or differs from the working tree"
    )
    return lines, ok


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        declarations = read_v1_declarations(conn)
    lines, ok = verdict(policy_hash(), declarations)
    print("\n".join(lines))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
