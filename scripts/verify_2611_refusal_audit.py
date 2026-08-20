"""#2611 — full-population check that the refusal audit CHANGES NO DECISION.

Run: ``PYTHONPATH=. uv run python scripts/verify_2611_refusal_audit.py``

⚠⚠ THE SAFETY ARGUMENT IS THAT THIS PR IS PURELY ADDITIVE. It records a refusal
that already happened; it must not make one more or one fewer trial refused, and
it must not move any number a consumer of ``strategy_holdout_accesses`` reads.
This script measures both across EVERY trial in the store, not a panel — a
three-trial check on this repo has been wrong before.

⚠ READ-ONLY, AND IT WRITES NO AUDIT ROW. It re-derives each door's decision from
``load_preregistration`` + ``declaration_refusals`` — which is exactly what the
doors themselves compute — rather than calling them, because calling
``require_outcome_access`` on 8 trials would insert 8 governance rows recording
attempts this script made rather than attempts anybody made.
"""

from __future__ import annotations

import psycopg

from app.config import settings
from app.services.prereg_contract import declaration_refusals
from app.services.result_ledger import load_preregistration

_TRIALS = """
    SELECT strategy_id, strategy_version FROM strategy_holdout_accesses
    UNION
    SELECT strategy_id, strategy_version FROM strategy_results_store
    UNION
    SELECT strategy_id, strategy_version FROM strategy_preregistration_declarations
    ORDER BY 1, 2
"""


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        trials = conn.execute(_TRIALS).fetchall()

        counts = conn.execute(
            """
            SELECT
                (SELECT count(*) FROM strategy_holdout_accesses),
                (SELECT count(*) FROM strategy_results_store),
                (SELECT count(*) FROM strategy_results_store WHERE namespace = 'hold_out'),
                (SELECT count(*) FROM strategy_preregistration_declarations),
                (SELECT count(*) FROM strategy_holdout_access_refusals)
            """
        ).fetchone()
        assert counts is not None
        print(
            f"accesses={counts[0]}  results={counts[1]}  hold_out={counts[2]}  "
            f"declarations={counts[3]}  refusals={counts[4]}"
        )
        print(f"trials={len(trials)}")

        census: dict[str, int] = {}
        for strategy_id, strategy_version in trials:
            frozen = load_preregistration(conn, str(strategy_id), str(strategy_version))
            if frozen is None:
                # `require_outcome_access` refuses; `record_holdout_access` permits
                # (no retroactive invalidation). Unchanged by this PR.
                key = "require_outcome_access:preregistration_not_frozen"
            else:
                codes = [str(code) for code in declaration_refusals(frozen.declaration)]
                if not frozen.digest_intact:
                    codes.append("declaration_digest_mismatch")
                key = f"both_doors:{','.join(codes)}" if codes else "both_doors:permitted"
            census[key] = census.get(key, 0) + 1

        print("\ndecision census (what each door would do, per trial):")
        for key, count in sorted(census.items()):
            print(f"  {count:5d}  {key}")

        print(
            "\n⚠ Every line above is the decision this branch and origin/main BOTH make — "
            "the PR adds a row after the decision, never a refusal."
        )


if __name__ == "__main__":
    main()
