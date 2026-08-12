"""Fail-closed evaluator primitives for the frozen #2582 Schedule 13D trial.

The real outcome command is intentionally unusable until the candidate has an
entry in ``app.services.trial_register``.  That entry is only added after this
code has been reviewed.  Keeping source selection, session selection and return
math here lets us test the causal machinery without querying a single price
bar.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Final

from app.services.market_calendar import us_market_status
from app.services.trial_register import TRIAL_REGISTER
from scripts.verify_2582_schedule13d_preregistration import EXPECTED_SHA256, load_and_verify

TRIAL_ID: Final = "c4-schedule13d-public-catalyst-v1"
ACKNOWLEDGEMENT: Final = "OPEN-2582-SEALED-OUTCOMES"


class OutcomeGateRefusal(RuntimeError):
    """The sealed outcome boundary was not explicitly and correctly opened."""


@dataclass(frozen=True)
class OutcomeGate:
    contract_sha256: str
    trial_register_version: str
    trial_id: str


def require_outcome_gate(*, acknowledgement: str | None, contract_path: Path) -> OutcomeGate:
    """Refuse unless the reviewed contract and declared trial are both exact."""

    if acknowledgement != ACKNOWLEDGEMENT:
        raise OutcomeGateRefusal(
            "sealed outcomes remain closed; pass the exact acknowledgement only after evaluator review"
        )
    _contract, digest = load_and_verify(contract_path)
    if digest != EXPECTED_SHA256:
        raise OutcomeGateRefusal("contract digest does not match the reviewed preregistration")
    if TRIAL_ID not in TRIAL_REGISTER.trial_ids:
        raise OutcomeGateRefusal(
            f"{TRIAL_ID} is absent from {TRIAL_REGISTER.version}; declare the price-data search before reading outcomes"
        )
    return OutcomeGate(digest, TRIAL_REGISTER.version, TRIAL_ID)


def next_regular_session_strictly_after(filing_date: date) -> date:
    """The first NYSE session after the filing civil date; never same-day."""

    candidate = filing_date + timedelta(days=1)
    while us_market_status(candidate) == "closed":
        candidate += timedelta(days=1)
    return candidate


def nth_regular_session(first_session: date, n: int) -> date:
    """Return session ``n`` with ``first_session`` counted as session one."""

    if n < 1:
        raise ValueError("n must be positive")
    if us_market_status(first_session) == "closed":
        raise ValueError("first_session is not a regular trading session")
    candidate = first_session
    found = 1
    while found < n:
        candidate += timedelta(days=1)
        if us_market_status(candidate) != "closed":
            found += 1
    return candidate


def total_return_pct(
    *,
    entry_open: Decimal,
    entry_close: Decimal,
    entry_adj_close: Decimal,
    exit_close: Decimal,
    exit_adj_close: Decimal,
    adverse_cost_bps: int = 50,
) -> Decimal:
    """Causal open-to-close total return using the vendor adjustment factors.

    ``adj_close / close`` is observed at entry and exit.  A split or dividend
    between them changes the factor ratio.  Missing/non-positive inputs refuse;
    silently falling back to raw close would corrupt corporate-action cases.
    """

    values = (entry_open, entry_close, entry_adj_close, exit_close, exit_adj_close)
    if any(not value.is_finite() or value <= 0 for value in values):
        raise ValueError("return inputs must all be finite and positive")
    if adverse_cost_bps < 0:
        raise ValueError("adverse_cost_bps cannot be negative")
    entry_factor = entry_adj_close / entry_close
    exit_factor = exit_adj_close / exit_close
    gross = (exit_close / entry_open) * (exit_factor / entry_factor) - Decimal(1)
    return (gross - Decimal(adverse_cost_bps) / Decimal(10_000)) * Decimal(100)


def bucket(value: Decimal, edges: tuple[Decimal, ...]) -> int:
    """Stable half-open bucket index: values equal to an edge enter its right cell."""

    if not value.is_finite():
        raise ValueError("bucket value must be finite")
    return sum(value >= edge for edge in edges)


def match_tie_break(treatment_accession: str, challenger_accession: str, *, seed: int = 2582) -> str:
    payload = f"{treatment_accession}\x1f{challenger_accession}\x1f{seed}".encode()
    return hashlib.sha256(payload).hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--acknowledgement")
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path("docs/proposals/ta/contracts/schedule13d-public-catalyst-v1.json"),
    )
    args = parser.parse_args(argv)
    gate = require_outcome_gate(acknowledgement=args.acknowledgement, contract_path=args.contract)
    # Deliberate second lock.  This PR establishes and tests the outcome
    # boundary; it does not yet contain the reviewed database evaluator.
    raise OutcomeGateRefusal(
        "gate satisfied but database outcome evaluator is not present; no price query was executed: "
        + json.dumps(gate.__dict__, sort_keys=True)
    )


if __name__ == "__main__":
    raise SystemExit(main())
