"""#3592 slice 4b-ii — the v2 readout's row adapter (spec §7 "Readout"): v2's control decision carries
``missing_donor_dtc``, handed to v1's accumulator in its ``missing_donors`` slot; anything else fails closed.
``tests/test_ranking_pot_v2_look_db.py`` runs the readout end to end."""

from __future__ import annotations

import pytest

from app.services import ranking_pot_readout_v2 as ro2
from app.services import ranking_pot_rebalance as rb


def test_v2_control_decisions_map_onto_v1s_missing_donor_slot() -> None:
    controls = {"records": [1, 2], "decision": {"entries": [1, 0], "missing_donor_dtc": [3, 0]}}
    out = ro2._v1_controls(controls)
    assert out["decision"] == {"entries": [1, 0], "missing_donors": [3, 0]}
    assert controls["decision"] == {"entries": [1, 0], "missing_donor_dtc": [3, 0]}  # the stored row is untouched
    assert ro2._v1_controls({"records": [1]}) == {"records": [1]}  # no decision at a non-rebalance session
    for bad in ({"entries": [1]}, {"missing_donors": [0], "missing_donor_dtc": [0]}):
        with pytest.raises(rb.SnapshotIntegrityError):
            ro2._v1_controls({"decision": bad})
