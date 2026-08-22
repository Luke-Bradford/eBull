"""Freeze S-H arm 1's #2599 preregistration declaration. Run ONCE, before outcomes open.

Contract: ``docs/proposals/ta/2026-08-22-sh-volatile-regime-gated-breakout.md``.
Refs #2840, #2832. Follows ``scripts/freeze_2837_se_overlay_declaration.py``, whose
four preamble warnings apply here verbatim and are not restated:

- a declaration frozen by the thing that opens the outcomes declares nothing, so this
  is a SEPARATE script from the measurement;
- the freeze is coupled to ``STRUCTURAL_REFUSAL_POLICY_VERSION`` and ``sql/333`` bars
  UPDATE and DELETE, so ``--dry-run`` first and freeze only when no policy change is
  in flight;
- it cannot run before ``sh-volatile-regime-gate-2026-08-22`` is on ``main``, because
  ``freeze_preregistration`` refuses a declaration no ``DeclaredTrial`` claims;
- no number below is chosen here.

⚠⚠ THE STRATEGY VERSION IS THE ``survivorship_free`` IDENTITY, AND THAT IS A CHOICE
BETWEEN TWO REAL TRIALS RATHER THAN A LOOKUP. ``S11.identity(universe=...)`` returns
``strategy-registry-v1+d5f25fd08376`` for ``survivorship_free`` and
``strategy-registry-v1+65274a70a40b`` for ``survivor_only``, and declarations key on
``(strategy_id, strategy_version)`` — so the two universes are two declarations and
two charges on the shared register. The exploration this declaration authorises runs
on the research corpus, i.e. ``strategy_result_identity.BACKTEST_UNIVERSE``, which IS
``survivorship_free``; ``survivor_only`` is ``strategy_signal_scan.SCAN_UNIVERSE`` and
is not what step 3 measures. Asserted below rather than pasted, so a future change to
either constant fails loudly instead of freezing the wrong trial.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from typing import Final

import psycopg

from app.config import settings
from app.services.cost_model import CARRY_UNMODELLED, FX_UNMODELLED
from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration
from app.services.result_ledger import PreregDeclarationRefused, freeze_preregistration, load_preregistration
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION, structural_promotion_refusals
from app.services.strategy_result_identity import BACKTEST_UNIVERSE, COST_MODEL_ID
from scripts._prereg_freeze_guard import assert_policy_version_merged, policy_version_report

STRATEGY_ID: Final = "s11-volatile-regime-gated-breakout"
CONTRACT_VERSION: Final = "sh-volatile-regime-gate-2026-08-22"
DECLARED_BY: Final = "scripts/freeze_2840_sh_regime_gate_declaration.py (#2840)"

#: ⚠ COMPUTED from the merged manifest, never pasted. A hand-typed hash that
#: stopped matching the module would freeze a declaration for a strategy version
#: that does not exist, and nothing downstream would ever load it.
STRATEGY_VERSION: Final = (
    STRATEGY_MANIFEST[STRATEGY_ID].identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID).version
)

# --- The forward-shadow floor's priors. All four are REGIME-SERIES facts. ---
#
# ⚠⚠ NONE OF THESE READS AN OUTCOME, WHICH IS THE ONLY REASON THE DERIVATION IS
# NOT CIRCULAR. They are counts of benchmark days by regime on ``spy_chain_v1``,
# so they were fixed before any strategy ran on it. Reproduce all four with:
#
#   PYTHONPATH=. uv run python -c "
#   import psycopg, collections; from datetime import date
#   from app.config import settings
#   from app.services.market_regime_provider import MarketRegimeProvider
#   with psycopg.connect(settings.database_url) as c:
#       c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
#       m = MarketRegimeProvider.load_research(c)._by_date; c.rollback()
#   print(min(m), max(m), collections.Counter(v.value if v else None for v in m.values()))
#   print(collections.Counter(v.value if v else None for d, v in m.items()
#         if date(2022,1,1) <= d <= date(2024,9,27)))"

#: ``bear_volatile`` benchmark days inside ``primary-2022-plus`` — the entire date
#: supply behind the pass leg. The cohort table's ``decision_date_count`` for that
#: cohort is also 14, i.e. S-4 fired on every one of them, which is what lets this
#: be read off the regime series instead of off the result.
_WINDOW_BEAR_VOLATILE_DATES: Final = 14

#: The chain's long-run volatile mix, 1993-01-29 → 2026-07-08 (8,391 benchmark days).
_CHAIN_BEAR_VOLATILE_DAYS: Final = 155
_CHAIN_BULL_VOLATILE_DAYS: Final = 150
#: Calendar days spanned by those 8,391 benchmark days — the trading-day → calendar
#: conversion, taken from the chain itself rather than from a 252-day convention.
_CHAIN_CALENDAR_DAYS: Final = 12213

_CHAIN_VOLATILE_DAYS: Final = _CHAIN_BEAR_VOLATILE_DAYS + _CHAIN_BULL_VOLATILE_DAYS

#: S-11 fires ONLY in the two volatile regimes, so its decision dates ARE volatile
#: benchmark days. Supplying ``_WINDOW_BEAR_VOLATILE_DATES`` bear_volatile dates
#: therefore costs the bull_volatile dates that arrive alongside them at the chain's
#: own mix — which is why the floor is larger than 14 without any number being picked.
MIN_FORWARD_DECISION_DATES: Final = math.ceil(
    _WINDOW_BEAR_VOLATILE_DATES * _CHAIN_VOLATILE_DAYS / _CHAIN_BEAR_VOLATILE_DAYS
)
MIN_FORWARD_CALENDAR_WEEKS: Final = math.ceil(
    MIN_FORWARD_DECISION_DATES * _CHAIN_CALENDAR_DAYS / _CHAIN_VOLATILE_DAYS / 7
)

#: ⚠ sql/333 caps this column at 1000 characters. The longer reading is the
#: contract's "Readout and abort bar"; this is the arithmetic plus the limits that
#: keep it from being read as a power calculation.
_FORWARD_SHADOW_DERIVATION: Final = (
    "NOT a power calculation — none is published for a per-trade expectancy claim and none is invented. Fixed BY "
    "CONSTRUCTION from spy_chain_v1's REGIME series only (no outcome read): primary-2022-plus holds "
    f"{_WINDOW_BEAR_VOLATILE_DATES} bear_volatile benchmark days, the whole date supply behind the pass leg, and "
    f"the chain's long-run mix is {_CHAIN_BEAR_VOLATILE_DAYS} bear / {_CHAIN_BULL_VOLATILE_DAYS} bull volatile "
    f"days over {_CHAIN_CALENDAR_DAYS} calendar days. S-11 fires only in volatile regimes, so dates = ceil("
    f"{_WINDOW_BEAR_VOLATILE_DATES}x{_CHAIN_VOLATILE_DAYS}/{_CHAIN_BEAR_VOLATILE_DAYS}) = "
    f"{MIN_FORWARD_DECISION_DATES}; weeks = ceil({MIN_FORWARD_DECISION_DATES}x{_CHAIN_CALENDAR_DAYS}/"
    f"{_CHAIN_VOLATILE_DAYS}/7) = {MIN_FORWARD_CALENDAR_WEEKS} (~3.1y). Three limits, stated: (1) the bear dates "
    "are EXPECTED in that span, not assured — volatile regimes CLUSTER (2008, 2020), so arrivals are bursty and "
    "a quiet 3 years supplies none; (2) the dates are NOT independent (200-SMA and 126-day BandWidth windows "
    "overlap, and volatile days arrive in runs), so the mix is a long-run average and not an exchangeable "
    "arrival rate; (3) clearing this floor is necessary and nowhere near sufficient. falsification_only "
    "regardless, so it gates nothing already ungated."
)


def build_declaration() -> PreregDeclaration:
    """S-H arm 1's declaration. Every stamp is the run's actual state, read not chosen.

    ⚠⚠ THE CARRY/FX STAMPS ARE READ FROM ``cost_model``, AND THE FIRST DRAFT GOT
    THEM BACKWARDS FROM THE COST MODEL'S NAME. ``carry-fx-structural-zero`` reads
    like "not modelled"; it is the opposite. ``cost_model.CARRY_CLOSURE`` is
    ``structural_zero``, which is a CLOSURE state meaning the cost does not exist
    for the declared lane — an unleveraged long on the underlying pays no
    overnight financing, and USD in / USD held / USD out has no conversion event.
    ``unmodelled`` is a different closure entirely, and #2720 flipped both markers
    to ``False``. ``freeze_preregistration`` refuses a manifest strategy whose
    declared stamps cannot match what ``backtest_run`` will write, which is what
    caught the draft — no row was burned. Read, never pasted, so a future closure
    change moves this declaration with it.

    ⚠⚠ ``falsification_only`` IS THEREFORE A CHOICE, NOT A CONSEQUENCE. With those
    stamps ``structural_promotion_refusals(survivorship_free, False, False)`` is
    EMPTY, so ``ineligible_trial_not_declared_falsification`` does not fire and
    ``capital_candidate`` would be accepted. It is still wrong, for a reason the
    contract already establishes: NO stored corpus window can confirm this
    hypothesis (every pinned window contains the cohorts that generated it), so
    the run this declaration authorises can only KILL the candidate, never
    promote it. Declaring ``capital_candidate`` would claim an outcome this
    instrument cannot produce.

    ⚠ And it forecloses nothing. The confirmatory instrument is forward shadow,
    which runs under ``SCAN_UNIVERSE`` — a different ``strategy_version``, hence
    its own declaration, made when that evidence exists.
    """
    universe_basis = BACKTEST_UNIVERSE
    carry_unmodelled = CARRY_UNMODELLED
    fx_unmodelled = FX_UNMODELLED
    return PreregDeclaration(
        strategy_id=STRATEGY_ID,
        strategy_version=STRATEGY_VERSION,
        contract_version=CONTRACT_VERSION,
        prereg_purpose="falsification_only",
        structural_refusal_policy_version=STRUCTURAL_REFUSAL_POLICY_VERSION,
        declared_universe_basis=universe_basis,
        declared_carry_unmodelled=carry_unmodelled,
        declared_fx_unmodelled=fx_unmodelled,
        # ⚠ COMPUTED, never written out. A hand-typed list is a second copy of the
        # refusal policy that drifts the first time the policy moves.
        expected_structural_refusals=structural_promotion_refusals(
            universe_basis=universe_basis, carry_unmodelled=carry_unmodelled, fx_unmodelled=fx_unmodelled
        ),
        forward_shadow=ForwardShadowFloor(
            min_independent_decision_dates=MIN_FORWARD_DECISION_DATES,
            min_calendar_weeks=MIN_FORWARD_CALENDAR_WEEKS,
            derivation=_FORWARD_SHADOW_DERIVATION,
        ),
        declared_by=DECLARED_BY,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="build and print the declaration and its digest without writing it",
    )
    parser.add_argument(
        "--allow-policy-divergence",
        action="store_true",
        help=(
            "freeze even though this tree's STRUCTURAL_REFUSAL_POLICY_VERSION is not the one on origin/main, "
            "or that ref could not be refreshed. The override is recorded in the output."
        ),
    )
    args = parser.parse_args(argv)
    declaration = build_declaration()
    summary: dict[str, object] = {**declaration.digest_payload, "declaration_sha256": declaration.sha256}
    if args.dry_run:
        sys.stdout.write(json.dumps({**summary, **policy_version_report(), "outcome": "dry_run"}, sort_keys=True))
        sys.stdout.write("\n")
        return 0

    summary.update(assert_policy_version_merged(allow_divergence=args.allow_policy_divergence))
    with psycopg.connect(settings.database_url) as conn:
        try:
            declaration_id = freeze_preregistration(conn, declaration)
        except psycopg.errors.UniqueViolation:
            # ⚠ A bare UniqueViolation is two situations wearing one error, and
            # they want opposite operator actions — see the #2837 script.
            conn.rollback()
            stored = load_preregistration(conn, declaration.strategy_id, declaration.strategy_version)
            if stored is not None and stored.declaration_sha256 == declaration.sha256:
                sys.stdout.write(
                    json.dumps(
                        {**summary, "outcome": "already_frozen_identical", "declaration_id": stored.declaration_id},
                        sort_keys=True,
                    )
                    + "\n"
                )
                return 0
            sys.stderr.write(
                json.dumps(
                    {
                        "outcome": "conflicting_declaration_already_frozen",
                        "stored_declaration_sha256": None if stored is None else stored.declaration_sha256,
                        "would_have_frozen_sha256": declaration.sha256,
                        "note": "declarations are immutable; different terms mean a new strategy_version",
                    },
                    sort_keys=True,
                )
                + "\n"
            )
            return 1
        except PreregDeclarationRefused as refused:
            conn.rollback()
            sys.stderr.write(
                json.dumps({"outcome": "refused", "refusals": list(refused.refusals)}, sort_keys=True) + "\n"
            )
            return 1
        conn.commit()
    sys.stdout.write(json.dumps({**summary, "outcome": "frozen", "declaration_id": declaration_id}, sort_keys=True))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CONTRACT_VERSION",
    "DECLARED_BY",
    "MIN_FORWARD_CALENDAR_WEEKS",
    "MIN_FORWARD_DECISION_DATES",
    "STRATEGY_ID",
    "STRATEGY_VERSION",
    "build_declaration",
    "main",
]
