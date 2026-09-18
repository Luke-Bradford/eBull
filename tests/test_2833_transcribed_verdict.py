"""#2833 — the transcribed constants must say what the sealed payload says.

`SELECTED_CORE_OUTCOME` / `SELECTED_CORE_INSTRUMENT_ID` / `SELECTED_CORE_EVIDENCE_REF`
are HAND-EDITED module constants, deliberately (`strategy_core_selection.py:41-51`:
STATED, never inferred). Hand-edited is exactly why they need a check that reads the
committed artefact instead of restating the same three literals — a test that asserts
`SELECTED_CORE_INSTRUMENT_ID == 3417` would pass just as happily if both the constant and
the test had been typed wrong together.

Pure-logic tier: every assertion reads the committed JSON file and the module. No
starlette client harness, no connection, no migration runner.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.services.strategy_core_selection import (
    _RECOGNISED_OUTCOMES,
    CORE_SELECTION_CANDIDATE_IDS,
    SELECTED_CORE_EVIDENCE_REF,
    SELECTED_CORE_INSTRUMENT_ID,
    SELECTED_CORE_OUTCOME,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _payload() -> dict[str, Any]:
    assert SELECTED_CORE_EVIDENCE_REF is not None, "nothing transcribed yet"
    path = _REPO_ROOT / SELECTED_CORE_EVIDENCE_REF
    assert path.is_file(), f"evidence ref names a file that does not exist: {SELECTED_CORE_EVIDENCE_REF}"
    with path.open() as handle:
        loaded: dict[str, Any] = json.load(handle)
    return loaded


def test_the_evidence_ref_resolves_to_a_committed_file() -> None:
    """A ref is only evidence if it points at something. `classify_core_selection`
    accepts any non-empty string, so a path typo would otherwise read as `ready`."""
    assert _payload()["schema_version"] == "core-selection-2833-v1"


def test_the_transcribed_outcome_is_the_payload_outcome() -> None:
    assert SELECTED_CORE_OUTCOME == _payload()["outcome"]
    assert SELECTED_CORE_OUTCOME in _RECOGNISED_OUTCOMES


def test_the_transcribed_instrument_is_the_payload_selection() -> None:
    """The assertion that matters: which name the sleeve will hold."""
    assert SELECTED_CORE_INSTRUMENT_ID == _payload()["selected_instrument_id"]


def test_the_selected_instrument_is_one_of_the_declared_candidates() -> None:
    """`classify_core_selection` refuses otherwise, but that refusal is only reached
    when something loads the module against a database. This is the cheap gate."""
    assert SELECTED_CORE_INSTRUMENT_ID in CORE_SELECTION_CANDIDATE_IDS


def test_the_payload_is_an_OPENED_verdict_and_not_a_readiness_report() -> None:
    """The verifier emits two different shapes and only one of them may be transcribed.

    Before the boundary it reports readiness — `outcome: "evidence_collecting"`, a
    `common_dates_observed`/`required_common_dates` pair, and deliberately NO candidate
    statistics. Once open it emits `window_dates` and `candidates` instead. Keying on the
    absence of the readiness fields (rather than on `outcome != "evidence_collecting"`
    alone) is what makes "the file is the wrong shape" a failure rather than a KeyError in
    whichever test happened to touch it first.
    """
    payload = _payload()
    assert "candidates" in payload, "a readiness report carries no candidate statistics"
    assert "common_dates_observed" not in payload, "this is the pre-boundary readiness shape"
    assert payload["outcome"] in _RECOGNISED_OUTCOMES
    # Five declared common dates, and the verdict measured after the last of them closed.
    assert len(payload["window_dates"]) == 5
    assert payload["measured_at"] >= payload["window_dates"][-1]


def test_the_selected_candidate_won_on_the_binding_statistic() -> None:
    """The declaration's binding percentile is p75. Assert the selection is the argmin
    over it rather than trusting `selected_instrument_id` alone — if the rule and the
    reported selection ever disagreed, the payload would be internally inconsistent and
    this is the only place that would say so."""
    payload = _payload()
    passing = [c for c in payload["candidates"] if c["verdict"] == "PASS"]
    assert passing, "a 'pass' outcome with no passing candidate is incoherent"
    best = min(passing, key=lambda c: float(c["p75_spread_bps"]))
    assert best["instrument_id"] == payload["selected_instrument_id"]


def test_every_failing_candidate_states_why() -> None:
    """A FAIL with no refusal and a spread under the bar would mean the payload lost the
    reason — `IUSA.L` fails on `fx_unmodelled`, not on cost."""
    for candidate in _payload()["candidates"]:
        if candidate["verdict"] == "FAIL":
            assert candidate["refusals"], f"{candidate['symbol']} FAILs with no stated refusal"


def test_the_payload_pins_the_declaration_it_was_measured_against() -> None:
    """Without these the artefact is a number with no provenance: a later edit to the
    declaration or the rule would not be detectable from the committed result."""
    payload = _payload()
    for key in ("declaration_sha256", "verifier_sha256", "query_sha256", "declaration_commit"):
        assert payload.get(key), f"committed evidence is missing {key}"
