from __future__ import annotations

from pathlib import Path

import pytest

from scripts.evaluate_2582_schedule13d_outcomes import ACKNOWLEDGEMENT, OutcomeGateRefusal
from scripts.run_2582_schedule13d_outcomes import main


def test_runner_refuses_before_database_connection_while_trial_is_unregistered() -> None:
    with pytest.raises(OutcomeGateRefusal, match="absent from trial-register"):
        main(
            [
                "--acknowledgement",
                ACKNOWLEDGEMENT,
                "--contract",
                str(Path("docs/proposals/ta/contracts/schedule13d-public-catalyst-v1.json")),
            ]
        )
