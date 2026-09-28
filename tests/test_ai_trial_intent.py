"""#3471 slice 2b-i — the trial loader's gate map against the paper loader (spec §8).

§8 "Safety gates kept": every non-evidence gate of ``_load_intent`` is mapped explicitly and
tested. The codes are read from both loaders' SOURCE, so a gate added to the paper loader
without a disposition here fails this file rather than silently skipping the trial.
"""

from __future__ import annotations

import ast
import inspect
import textwrap
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any

import pytest

from app.services import ai_trial_intent, strategy_paper_executor
from app.services.ai_trial_intent import (
    PAPER_GATE_MAP,
    TRIAL_TICKET_USD,
    _session_reason,
    declaration_digest,
    load_trial_intent,
)
from app.services.ai_trial_pack import canonical_sha256
from app.services.strategy_paper_executor import _load_intent


def _reason_codes(function: Callable[..., Any], module: Any) -> set[str]:
    """Every refusal code a loader can return: ``return None, "<code>", ...`` statements and
    the second element of each ``(passed, reason)`` pair in its check tuples."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(function)))
    codes: set[str] = set()

    def resolve(node: ast.expr) -> None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            codes.add(node.value)
        elif isinstance(node, ast.Name) and node.id.isupper():
            # A module constant (DEPLOYMENT_CURRENCY_UNSUPPORTED); a lowercase name is the
            # loop variable of `return None, reason, True`, whose values the tuples supply.
            codes.add(str(getattr(module, node.id)))

    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Return)
            and isinstance(node.value, ast.Tuple)
            and len(node.value.elts) >= 2
            and isinstance(node.value.elts[0], ast.Constant)
            and node.value.elts[0].value is None
        ):
            resolve(node.value.elts[1])
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Tuple):
            for pair in node.value.elts:
                if isinstance(pair, ast.Tuple) and len(pair.elts) == 2:
                    resolve(pair.elts[1])
    return codes


PAPER_CODES = _reason_codes(_load_intent, strategy_paper_executor)
TRIAL_CODES = _reason_codes(load_trial_intent, ai_trial_intent) | {"decision_expired", "decision_not_yet_due"}


def test_the_parser_reads_both_loaders() -> None:
    # A parser that finds nothing would pass every assertion below vacuously.
    assert {"instrument_not_tradable", "expectancy_evidence_missing", "quote_stale"} <= PAPER_CODES
    assert {"trial_link_missing", "trial_not_active", "market_session_closed"} <= TRIAL_CODES


def test_every_paper_gate_has_a_disposition() -> None:
    assert PAPER_CODES == PAPER_GATE_MAP.keys()


@pytest.mark.parametrize(("paper_code", "disposition"), sorted(PAPER_GATE_MAP.items()))
def test_kept_and_replacing_codes_are_returned_by_the_trial_loader(paper_code: str, disposition: str) -> None:
    if disposition == "kept":
        assert paper_code in TRIAL_CODES
    elif disposition.startswith("replaced:"):
        assert disposition.removeprefix("replaced:") in TRIAL_CODES
    else:
        assert disposition == "dropped"
        # A dropped gate is an EVIDENCE gate, and the trial must never emit its code.
        assert paper_code not in TRIAL_CODES


def test_the_trial_loader_carries_no_evidence_field() -> None:
    fields = set(ai_trial_intent.TrialIntent.__dataclass_fields__)
    assert not fields & {
        "forecast_id",
        "ranking_member_id",
        "gross_expectancy_ci_low_pct",
        "min_net_expectancy_pct",
        "scan_at",
        "max_scan_age_seconds",
    }
    # Every non-evidence field keeps the paper intent's name, so the shared capacity
    # arithmetic (slice 2b-ii) can read either intent.
    paper_fields = set(strategy_paper_executor._Intent.__dataclass_fields__)
    trial_only = {
        "declaration_id",
        "run_id",
        "decision_id",
        "pair_id",
        "pair_seq",
        "leg",
        "session_date",
        "horizon_days",
        "size_tier",
        "requested_amount",
    }
    assert fields - trial_only <= paper_fields


def test_tickets_are_the_frozen_trial_caps() -> None:
    assert {k: str(v) for k, v in TRIAL_TICKET_USD.items()} == {"full": "250", "half": "125"}


def test_the_declaration_digest_is_the_canonical_json_hash() -> None:
    doc = {"strategy_version": "v1", "strategy_id": "ai-discretionary-v1", "caps": {"full": 250, "stop": 2.5}}
    assert declaration_digest(doc) == canonical_sha256(doc)
    # Key order is not content.
    assert declaration_digest(dict(reversed(list(doc.items())))) == declaration_digest(doc)


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        # 2026-10-05 is a Monday; New York is UTC-4 in October.
        (datetime(2026, 10, 5, 15, 0, tzinfo=UTC), None),
        (datetime(2026, 10, 6, 3, 59, tzinfo=UTC), None),  # 23:59 New York, still the 5th
        (datetime(2026, 10, 6, 4, 0, tzinfo=UTC), "decision_expired"),
        (datetime(2026, 10, 5, 3, 59, tzinfo=UTC), "decision_not_yet_due"),
    ],
)
def test_the_target_session_is_the_new_york_date(now: datetime, expected: str | None) -> None:
    assert _session_reason(date(2026, 10, 5), now=now) == expected
