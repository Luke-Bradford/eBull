"""#2602 item 3 — product identity PER POSITION, from the broker's own field.

Source rule (live portal 2026-08-23,
``api-reference/trading--demo/get-account-pnl-and-portfolio-details``): the
open-positions response documents ``settlementTypeID`` as *"Position investment
type. 0 - CFD, 1 - Real Asset, 2 - SWAP, 3 - Crypto MarginTrade, 4 - Future
Contract"*.

⚠ These are pure-logic tests over the vocabulary and the parser. The backfill SQL
and the read path are exercised against a real DB in
``tests/test_2602_position_investment_type_db.py``.
"""

from __future__ import annotations

import pytest

from app.providers.implementations.etoro_broker import _opt_settlement_type_id, _parse_direct_position
from app.services.broker_settlement_arms import (
    UNDERLYING_POSITION_INVESTMENT_TYPE_ID,
    UNDERLYING_SETTLEMENT_TYPE,
    position_investment_type_label,
    position_is_underlying,
)


class TestVocabulary:
    @pytest.mark.parametrize(
        ("settlement_type_id", "label"),
        [
            (0, "CFD"),
            (1, "Real Asset"),
            (2, "SWAP"),
            (3, "Crypto MarginTrade"),
            (4, "Future Contract"),
        ],
    )
    def test_documented_ids_render_the_providers_own_label(self, settlement_type_id: int, label: str) -> None:
        """Verbatim, not normalised into the eligibility vocabulary.

        eToro publishes the two enumerations on two endpoints; an equivalence
        between them would be ours, not theirs.
        """
        assert position_investment_type_label(settlement_type_id) == label

    @pytest.mark.parametrize("unknown", [-1, 5, 99])
    def test_an_undocumented_id_is_not_observed_rather_than_guessed(self, unknown: int) -> None:
        assert position_investment_type_label(unknown) is None
        assert position_is_underlying(unknown) is None

    def test_absent_id_is_not_observed(self) -> None:
        assert position_investment_type_label(None) is None
        assert position_is_underlying(None) is None

    @pytest.mark.parametrize(
        ("settlement_type_id", "expected"),
        [(0, False), (1, True), (2, False), (3, False), (4, False)],
    )
    def test_exactly_one_documented_type_is_ownership_at_full_value(
        self, settlement_type_id: int, expected: bool
    ) -> None:
        """SWAP and Future Contract are derivatives by the provider's own wording;
        Crypto MarginTrade is the real asset but leveraged, which the standing
        no-leverage posture bars."""
        assert position_is_underlying(settlement_type_id) is expected

    def test_unknown_is_tri_state_not_false(self) -> None:
        """The distinction is load-bearing on an operator panel.

        "we did not observe a product type" and "this is a derivative" are
        different facts, and collapsing the first into the second would have the
        panel assert a product identity the broker never reported.
        """
        assert position_is_underlying(None) is not False
        assert position_is_underlying(999) is not False

    def test_the_two_vocabularies_are_kept_distinct(self) -> None:
        """Pin that the position enum is NOT the eligibility string set.

        If someone later "tidies" the labels into ``real``/``cfd``/… this fails —
        which is the point: the eligibility vocabulary has four values and no
        SWAP, so the sets are not interchangeable.
        """
        labels = {position_investment_type_label(i) for i in range(5)}
        assert UNDERLYING_SETTLEMENT_TYPE not in labels
        assert "SWAP" in labels
        assert position_investment_type_label(UNDERLYING_POSITION_INVESTMENT_TYPE_ID) == "Real Asset"


class TestParser:
    def test_reads_the_documented_key(self) -> None:
        assert _opt_settlement_type_id({"settlementTypeID": 1}) == 1

    def test_accepts_the_string_form_the_payload_actually_stores(self) -> None:
        """``raw_payload->>'settlementTypeID'`` round-trips as text, and eToro has
        sent numerics as strings elsewhere in this API."""
        assert _opt_settlement_type_id({"settlementTypeID": "0"}) == 0

    def test_absent_key_is_none(self) -> None:
        assert _opt_settlement_type_id({}) is None
        assert _opt_settlement_type_id({"settlementTypeID": None}) is None

    @pytest.mark.parametrize("bogus", ["", "cfd", [], {}, 3.5j])
    def test_unreadable_value_is_none_and_never_raises(self, bogus: object) -> None:
        """Evidence about the position, not a field the position depends on.

        Refusing to parse the whole position because its investment type is
        malformed would lose the HOLDING over a label.
        """
        assert _opt_settlement_type_id({"settlementTypeID": bogus}) is None

    @pytest.mark.parametrize("fractional", [1.5, 0.5, "1.5", 3.9])
    def test_a_fractional_value_is_rejected_not_truncated(self, fractional: object) -> None:
        """`settlementTypeID` is an ENUMERATION, so `int()` truncation is a lie.

        `int(1.5)` is 1, which the panel renders as "Real Asset" — an ownership
        claim the broker never made, from a payload we could not read.
        Caught at Codex ckpt-2.
        """
        assert _opt_settlement_type_id({"settlementTypeID": fractional}) is None

    @pytest.mark.parametrize("whole", [1.0, 0.0, 4.0])
    def test_a_whole_float_is_admitted(self, whole: float) -> None:
        """JSON has one number type, so a whole value can legally arrive as a
        float — rejecting it would drop evidence the broker did send."""
        assert _opt_settlement_type_id({"settlementTypeID": whole}) == int(whole)

    @pytest.mark.parametrize("out_of_range", [32768, -32769, 999999, "999999", 70000.0])
    def test_a_value_too_large_for_the_column_is_dropped_not_passed_on(self, out_of_range: object) -> None:
        """`broker_positions.settlement_type_id` is SMALLINT (sql/367).

        Passing an out-of-range value on would push the overflow into
        `_upsert_broker_positions`, aborting the WHOLE portfolio sync — every
        position lost over one unreadable label. Caught at Codex ckpt-2 round 2.
        """
        assert _opt_settlement_type_id({"settlementTypeID": out_of_range}) is None

    @pytest.mark.parametrize("boundary", [32767, -32768])
    def test_the_column_boundaries_themselves_are_admitted(self, boundary: int) -> None:
        """Off-by-one guard: the bound is inclusive on both ends."""
        assert _opt_settlement_type_id({"settlementTypeID": boundary}) == boundary

    @pytest.mark.parametrize("truthy", [True, False])
    def test_bool_is_rejected_not_coerced(self, truthy: bool) -> None:
        """``bool`` is an ``int`` subclass in Python, so a naive ``int(value)``
        would store 1 = "Real Asset" for a payload that sent ``true``. Mirrors the
        same guard in ``offers_unleveraged`` on the eligibility side."""
        assert _opt_settlement_type_id({"settlementTypeID": truthy}) is None

    def test_parsed_position_carries_the_identity(self) -> None:
        payload = {
            "positionID": 3308441892,
            "instrumentID": 4238,
            "units": "16.929336",
            "openRate": "512.34",
            "amount": "8672.15",
            "settlementTypeID": 0,
        }
        assert _parse_direct_position(payload).settlement_type_id == 0

    def test_a_position_without_the_key_still_parses(self) -> None:
        payload = {
            "positionID": 1,
            "instrumentID": 2,
            "units": "1",
            "openRate": "10",
            "amount": "10",
        }
        assert _parse_direct_position(payload).settlement_type_id is None
