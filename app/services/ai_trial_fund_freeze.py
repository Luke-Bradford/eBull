"""#3515 slice 4 — fund-v1's declaration freeze (fund-v1 spec §0, §6 "Budget", §7, §9).

fund-v1 freezes through ``ai_trial_freeze.freeze_trial`` under ``FUND_TERMS``: its own #2599 row,
document, position managers and genesis event, keyed on fund-v1's ids, so nothing of v1 is read for
writing or written. On top of v1's checks, inside the freeze transaction (``fund_extra``):

* §0 rule 2: v1 must be wound down (``ai_trial_wind_down``); otherwise ``v1_not_wound_down:<detail>``.
* §7 capacity: both legs at ``FUND_CAPITAL_LIMIT`` (``trial_capital_not_spec``, via ``FreezeTerms``).
* §6: the budget fixture, measured by the probe walk in the SAME process just before the freeze
  (``scripts/ai_trial_fund_freeze.py``), must be present (``budget_fixture_not_run``) and coherent
  (``budget_fixture_invalid``): the chosen probe is a pass at ``n`` whose figures are the ones
  written, within the ceiling, on the code's model, system prompt and decision schema.
* §9 / §6: the coverage and real-prompt-size measurements' output (``measurements_missing``).
* §7: per v1 declaration, whether each of v1's hashed modules has the same sha in fund-v1's
  manifest as in that declaration's document (``null`` when the document is not digest-intact or
  lacks the module). It states only that; shared non-hashed modules differ by construction.

The fixture's figures live in this digest-bound document, not in fund-v1's code hash: they are
measured at freeze time on the frozen code (spec §7, amended in this slice). This module and
``ai_trial_freeze`` are freeze-time code bound by the document's ``code_git_sha``, as v1's freeze
is; neither runs per decision, so neither is hashed.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any, Final

import psycopg

from app.services.ai_trial_freeze import FreezeTerms
from app.services.ai_trial_fund_blocks import INPUT_TOKEN_CEILING
from app.services.ai_trial_fund_policy import fund_policy_manifest
from app.services.ai_trial_guard import decision_json_schema
from app.services.ai_trial_intent import declaration_digest
from app.services.ai_trial_invocation import TRIAL_MODEL_ID
from app.services.ai_trial_pack import NonCanonicalValue, canonical_sha256
from app.services.ai_trial_policy import POLICY_MODULES
from app.services.ai_trial_version import FUND_V1, V1
from app.services.ai_trial_wind_down import read_v1_declarations, wind_down_refusal
from app.services.trial_register import DeclaredTrial, TrialExactness

Conn = psycopg.Connection[Any]

FUND_SPEC_PATH: Final = "docs/proposals/execution/2026-09-30-3515-ai-discretionary-fund-v1.md"
FUND_DECLARATION_KIND: Final = "ai-trial-declaration-fund-v1"
#: §7 "Capacity: v1's terms": $3,000 per leg (slots are v1's hashed ``TRIAL_MAX_CONCURRENT_PER_LEG``).
FUND_CAPITAL_LIMIT: Final = Decimal("3000")
#: §0 rule 3, recorded in the document.
START_RULE: Final = (
    "fund-v1 starts only when v1 is fully wound down (spec §0 rule 2); resuming v1 after fund-v1 starts is "
    "not allowed (§0 rule 3). fund-v1's start gate refuses v1_not_wound_down on every run."
)

REFUSE_BUDGET_FIXTURE_NOT_RUN: Final = "budget_fixture_not_run"
REFUSE_BUDGET_FIXTURE_INVALID: Final = "budget_fixture_invalid"
REFUSE_MEASUREMENTS_MISSING: Final = "measurements_missing"
#: The refusals only the in-process measurements clear: a precheck refused by nothing else may measure.
MEASUREMENT_REFUSALS: Final = frozenset({REFUSE_BUDGET_FIXTURE_NOT_RUN, REFUSE_MEASUREMENTS_MISSING})
#: ``outside`` keys: the budget fixture (§6) and the measurement outputs (§9, §6).
MEASUREMENT_KEYS: Final = ("coverage", "prompt_budget")

#: ``trial_register`` holds this entry verbatim; a test pins the two equal.
EXPECTED_FUND_REGISTER_ENTRY: Final = DeclaredTrial(
    trial_id="ai-discretionary-fund-v1",
    description=(
        "#3515 AI-discretionary-fund-v1: v1's daily long picks with point-in-time periodic-report "
        "fundamentals and MD&A text, against its own random-draw control leg on demo; one declared "
        "hypothesis (the arm-minus-control pair unit d, fund-v1 spec §1)."
    ),
    evidence="docs/proposals/execution/2026-09-30-3515-ai-discretionary-fund-v1.md §7 'Declaration and "
    "manifest'; frozen by scripts/ai_trial_fund_freeze.py (#3515)",
    exactness=TrialExactness.EXACT,
    searches=1,
    declared_for=(FUND_V1.arm_strategy_id, FUND_V1.strategy_version),
)


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _has_float(value: object) -> bool:
    if isinstance(value, float):
        return True
    if isinstance(value, Mapping):
        return any(_has_float(v) for v in value.values())
    if isinstance(value, list):
        return any(_has_float(v) for v in value)
    return False


def budget_fixture_refusal(fixture: object) -> str | None:
    """§6: the passing probe the freeze writes is one recorded probe, a pass within the ceiling, at
    ``n``, on the code's model, fund system prompt and decision schema. JSON-exact (no floats), so the
    JSONB round trip keeps the digest."""
    if fixture is None:
        return REFUSE_BUDGET_FIXTURE_NOT_RUN
    if not isinstance(fixture, Mapping) or _has_float(fixture):
        return REFUSE_BUDGET_FIXTURE_INVALID
    n, attempt, tokens, size = (fixture.get(k) for k in ("n", "attempt", "input_tokens", "rendered_prompt_bytes"))
    probes = fixture.get("probes")
    if not (_is_int(n) and _is_int(attempt) and _is_int(tokens) and _is_int(size) and isinstance(probes, list)):
        return REFUSE_BUDGET_FIXTURE_INVALID
    chosen = [p for p in probes if isinstance(p, Mapping) and p.get("n") == n and p.get("attempt") == attempt]
    figures = ("pack_sha256", "rendered_prompt_sha256", "rendered_prompt_bytes", "input_tokens")
    if (
        len(chosen) != 1
        or chosen[0].get("outcome") != "pass"
        or any(chosen[0].get(k) != fixture.get(k) for k in figures)
        or not 0 < tokens <= INPUT_TOKEN_CEILING  # type: ignore[operator]
        or not size > 0  # type: ignore[operator]
        or fixture.get("model_id") != TRIAL_MODEL_ID
        or fixture.get("system_prompt_sha256") != FUND_V1.system_prompt_sha256
        or fixture.get("decision_schema_sha256") != canonical_sha256(decision_json_schema())
        or not isinstance(fixture.get("cli_version"), str)
        or str(fixture.get("cli_version")).startswith("unavailable")
    ):
        return REFUSE_BUDGET_FIXTURE_INVALID
    return None


def measurements_refusal(measurements: object) -> str | None:
    """Each measurement's captured output is a non-empty list of lines."""
    if not isinstance(measurements, Mapping):
        return REFUSE_MEASUREMENTS_MISSING
    for key in MEASUREMENT_KEYS:
        entry = measurements.get(key)
        lines = entry.get("output_lines") if isinstance(entry, Mapping) else None
        if not isinstance(lines, list) or not lines or not all(isinstance(line, str) for line in lines):
            return REFUSE_MEASUREMENTS_MISSING
    return None


def v1_module_parity(
    v1_docs: list[tuple[int, Any, str]], fund_module_sha256: Mapping[str, str]
) -> dict[str, dict[str, bool | None]]:
    """Per v1 declaration id: for each of v1's hashed modules, whether fund-v1's sha equals the one in
    that declaration's document; ``None`` when the document is not digest-intact or lacks the module."""
    parity: dict[str, dict[str, bool | None]] = {}
    for declaration_id, doc, doc_sha256 in v1_docs:
        try:
            intact = declaration_digest(doc) == doc_sha256
        except NonCanonicalValue:
            intact = False
        modules = doc.get("policy_modules") if intact and isinstance(doc, Mapping) else None
        parity[str(declaration_id)] = {
            module: (
                None
                if not isinstance(modules, Mapping) or not isinstance(modules.get(module), str)
                else modules[module] == fund_module_sha256.get(module)
            )
            for module in sorted(POLICY_MODULES)
        }
    return parity


def fund_extra(
    conn: Conn, outside: Mapping[str, Any], config: Mapping[str, dict[str, Any]]
) -> tuple[dict[str, Any], list[str]]:
    """``FreezeTerms.extra`` for fund-v1: runs inside the freeze transaction, after the lock."""
    refusals: list[str] = []
    gate = wind_down_refusal(read_v1_declarations(conn))
    if gate is not None:
        refusals.append(gate)
    fixture = budget_fixture_refusal(outside.get("budget_fixture"))
    if fixture is not None:
        refusals.append(fixture)
    measured = measurements_refusal(outside.get("measurements"))
    if measured is not None:
        refusals.append(measured)
    v1_docs = conn.execute(
        "SELECT declaration_id, doc, doc_sha256 FROM ai_trial_declarations WHERE strategy_id = %s "
        "ORDER BY declaration_id",
        (V1.arm_strategy_id,),
    ).fetchall()
    parity = v1_module_parity([(int(r[0]), r[1], str(r[2])) for r in v1_docs], fund_policy_manifest().module_sha256)
    return {"v1_hashed_module_parity": parity, "start_rule": START_RULE}, refusals


FUND_TERMS: Final = FreezeTerms(
    version=FUND_V1,
    kind=FUND_DECLARATION_KIND,
    spec_path=FUND_SPEC_PATH,
    builder="ai_trial_fund_freeze.FUND_TERMS",
    manifest=fund_policy_manifest,
    register_entry=EXPECTED_FUND_REGISTER_ENTRY,
    capital_limit=FUND_CAPITAL_LIMIT,
    extra=fund_extra,
)

__all__ = [
    "EXPECTED_FUND_REGISTER_ENTRY",
    "FUND_CAPITAL_LIMIT",
    "FUND_DECLARATION_KIND",
    "FUND_SPEC_PATH",
    "FUND_TERMS",
    "MEASUREMENT_KEYS",
    "MEASUREMENT_REFUSALS",
    "budget_fixture_refusal",
    "fund_extra",
    "measurements_refusal",
    "v1_module_parity",
]
