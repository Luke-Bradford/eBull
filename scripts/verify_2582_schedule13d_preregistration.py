"""Verify #2582's frozen, outcome-free Schedule 13D candidate contract."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

CONTRACT = Path("docs/proposals/ta/contracts/schedule13d-public-catalyst-v1.json")
EXPECTED_SHA256 = "dd076ab7601eec37b12fd5958c24012b23bb1490e2ccd4d48a62d72f0c87ccf8"


def load_and_verify(path: Path = CONTRACT) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    contract = json.loads(raw)

    assert contract["contract_version"] == "schedule13d-public-catalyst-v1"
    assert contract["decision"] == "historical_falsification_only"
    assert contract["position"]["holding_sessions"] == 10
    assert contract["position"]["stop_loss"] is None
    assert contract["position"]["take_profit"] is None
    assert contract["position"]["round_trip_adverse_cost_bps"] == 50
    assert contract["decision_clock"]["same_close_fill"] == "forbidden"
    assert contract["source"]["public_filing_date_source"] == "sec_filing_manifest.filed_at"
    assert contract["source"]["blockholder_filed_at_policy"].endswith("never_public_decision_clock")
    assert contract["decision_clock"]["filing_date_field"] == "sec_filing_manifest.filed_at_date"
    assert contract["eligibility"]["security_scope"].startswith("current_tradable_etoro_instrument_type_5")
    assert contract["eligibility"]["current_is_tradable_required"] is True
    assert contract["eligibility"]["historical_security_identity_limit"].startswith("current_snapshot_only")
    assert contract["context"]["context_may_gate_primary_result"] is False
    assert contract["context"]["market_series"]["series_id"] == 7713
    assert contract["context"]["market_series"]["allowed_use"].endswith("not_total_return_benchmark")
    assert contract["context"]["item4_policy"].startswith("not_used_in_v1")
    assert contract["challengers"]["matching"]["entry_price_bucket_usd_edges"] == [5, 10, 25, 50, 100]
    assert contract["challengers"]["multiplicity"].startswith("holm_adjust")
    assert contract["acceptance"]["historical_archive_can_promote_capital"] is False
    assert contract["storage"]["duplicate_raw_document"] is False
    assert contract["storage"]["persist_non_firing_poll_rows"] is False
    assert "maximum_drawdown" not in contract["statistics"]["required_reports"]

    stats = contract["statistics"]
    effect = float(stats["minimum_worthwhile_net_effect_pct"]) / 100
    sigma = float(stats["planning_return_standard_deviation_pct"]) / 100
    # z(.975) + z(.8), squared. Ceiling of 7.84888 * 400 is 3,140 for
    # 0.5%; here sigma/effect is 10, producing 784.888 -> 785.
    expected_n = 785
    assert round(7.84888 * (sigma / effect) ** 2) == expected_n
    assert stats["minimum_planning_effective_sample_size"] == expected_n

    digest = hashlib.sha256(raw).hexdigest()
    assert digest == EXPECTED_SHA256, f"frozen contract digest moved: {digest}"
    return contract, digest


def main() -> int:
    contract, digest = load_and_verify()
    print(
        json.dumps(
            {
                "candidate_id": contract["candidate_id"],
                "contract_version": contract["contract_version"],
                "outcomes_read": False,
                "sha256": digest,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
