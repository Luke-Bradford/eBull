"""#3515 slice 4 — the side-by-side readout (fund-v1 spec §1): every version, never a contrast."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

import scripts.ai_trial_readout as cli
from app.services.ai_trial_readout import ReadoutUnavailable
from app.services.ai_trial_version import FUND_V1, TRIAL_VERSIONS, V1


def test_every_version_is_printed_under_the_caveat_with_refusals_next_to_pairs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "render", lambda readout, *, arm_strategy_id: f"BODY {arm_strategy_id}")
    v1 = SimpleNamespace(
        run_census={"decided": 4, "refused:pack_empty": 2, "refused:budget_breach": 1, "claimed": 1},
        pair_states={"closed": 3, "open": 2},
    )
    text = cli.render_side_by_side([(V1.arm_strategy_id, v1), (FUND_V1.arm_strategy_id, "the trial has not started")])  # type: ignore[list-item]
    lines = text.splitlines()
    assert lines[0] == cli.SIDE_BY_SIDE_CAVEAT
    v1_at = lines.index(f"=== {V1.arm_strategy_id} ===")
    assert lines[v1_at + 1 : v1_at + 3] == ["refused runs 3, next to pairs 5", f"BODY {V1.arm_strategy_id}"]
    fund_at = lines.index(f"=== {FUND_V1.arm_strategy_id} ===")
    assert fund_at > v1_at and lines[fund_at + 1] == "no readout: the trial has not started"
    # §1: no sentence may call one version better.
    assert not any(w in text.lower() for w in ("outperform", "better than", "beats"))


def test_side_by_side_reads_every_registered_version_and_survives_an_unavailable_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[str] = []

    def compute(_conn: Any, *, version: Any) -> Any:
        seen.append(version.arm_strategy_id)
        if version is FUND_V1:
            raise ReadoutUnavailable("no frozen declaration")
        return "READOUT"

    monkeypatch.setattr(cli, "compute_readout", compute)
    conn = MagicMock()
    entries = cli._side_by_side(conn)
    assert seen == [v.arm_strategy_id for v in TRIAL_VERSIONS]
    assert dict(entries) == {V1.arm_strategy_id: "READOUT", FUND_V1.arm_strategy_id: "no frozen declaration"}
    conn.rollback.assert_called_once()


def test_the_json_has_one_schema_per_version() -> None:
    assert cli.side_by_side_json("no frozen declaration") == {"readout": None, "reason": "no frozen declaration"}


@pytest.mark.parametrize(
    "extra",
    [["--arm", FUND_V1.arm_strategy_id], ["--version", "v2"], ["--arm", V1.arm_strategy_id], ["--version", "v1"]],
)
def test_side_by_side_rejects_arm_or_version(extra: list[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        cli.main(["--side-by-side", *extra])
    assert exc.value.code == 2
