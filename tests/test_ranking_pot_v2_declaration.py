"""#3592 slice 3c — v2's frozen terms codec, its document and its register entry (pure; spec §8).

The SQL halves (the freeze's reads and writes, the loaders' isolation) are ``tests/test_ranking_pot_freeze_v2_db.py``.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from fractions import Fraction
from typing import Any

import pytest

from app.services import ranking_pot_v2 as v2
from app.services import ranking_pot_v2_policy as policy
from app.services.ai_trial_freeze import Provenance
from app.services.ranking_pot_freeze import S0
from app.services.ranking_pot_freeze_v2 import EXPECTED_REGISTER_ENTRY, alpha_refusal, build_declaration
from app.services.ranking_pot_rebalance import PotDeclaration
from app.services.ranking_pot_sim import K_CONTROLS
from app.services.ranking_pot_v2_declaration import (
    FrozenTerms,
    count_window,
    decode_frozen,
    encode_frozen,
    frozen_terms,
)
from app.services.trial_register import TRIAL_REGISTER

S0_IDS = (11, 12, 13, 14)
READ_AT = datetime(2026, 10, 3, 7, tzinfo=UTC)  # freeze month 2026-10
FLOOR = date(2026, 7, 1)
#: 2024-10..2026-09: 100 rows a month except 2026-05 (5, below the 10 threshold), so the trailing run is 2026-06..09 and
#: the floor the month after its first.
COUNTS = {m: (5 if m == date(2026, 5, 1) else 100) for m in count_window(date(2026, 10, 1), FLOOR)}
TERMS = FrozenTerms(
    scores={11: Decimal("0.1"), 12: Decimal("0.1"), 13: None, 14: None},
    strata={11: 0, 12: 0, 13: v2.UNSCORED_STRATUM, 14: v2.UNSCORED_STRATUM},
    dtc_settlement_date=date(2026, 9, 15),
    dtc_baseline=3,
    dtc_read_at=READ_AT,
    history_floor=FLOOR,
    floor_threshold=Fraction(10),
    month_counts=COUNTS,
)
PROVENANCE = Provenance(code_git_sha="a" * 40, spec_sha256="b" * 64, python_version="3.14.0", refusals=())


def _block(**edits: Any) -> dict[str, Any]:
    doc = encode_frozen(TERMS)
    for path, value in edits.items():
        node = doc
        *parents, leaf = path.split("__")
        for p in parents:
            node = node[p]
        node[leaf] = value
    return doc


def _counts(counts: dict[date, int]) -> list[list[Any]]:
    return [[m.isoformat(), n] for m, n in sorted(counts.items())]


def test_the_fixture_is_what_the_rules_derive() -> None:
    assert count_window(date(2026, 10, 1), FLOOR) == tuple(sorted(COUNTS))
    assert (min(COUNTS), max(COUNTS), len(COUNTS)) == (date(2024, 10, 1), date(2026, 9, 1), 24)
    assert v2.history_floor(COUNTS, freeze_month=date(2026, 10, 1)) == FLOOR
    assert v2.score_strata(S0_IDS, TERMS.scores) == TERMS.strata


def test_round_trip() -> None:
    assert decode_frozen(encode_frozen(TERMS), s0_ids=S0_IDS) == TERMS
    assert TERMS.freeze_month == date(2026, 10, 1)


@pytest.mark.parametrize(
    "edits",
    [
        {"schema": "ranking-pot-v2-frozen-0"},
        {"strata": [[11, 0, "0.1"], [12, 0, "0.1"], [13, 10, None]]},  # S₀ not covered
        {"strata": [[11, 0, "0.1"], [12, 0, "0.1"], [13, 10, None], [14, 10, None], [14, 10, None]]},  # twice
        {"strata": [[11, 0, "0.1"], [12, 0, "0.1"], [13, 9, None], [14, 9, None]]},  # not score_strata
        {"strata": [[11, 0, "0.1"], [12, 0, "0.2"], [13, 10, None], [14, 10, None]]},  # 0.2 is the upper decile
        {"strata": [[11, 0, 0.1], [12, 0, "0.1"], [13, 10, None], [14, 10, None]]},  # a float score
        {"strata": [[11, False, "0.1"], [12, 0, "0.1"], [13, 10, None], [14, 10, None]]},
        {"strata": [[11, 0], [12, 0], [13, 10], [14, 10]]},  # no scores
        {"dtc_baseline__usable": 0},
        {"dtc_baseline__usable": 5},  # more than |S₀|
        {"dtc_baseline__read_at": "2026-10-03T07:00:00"},  # naive
        {"dtc_baseline__settlement_date": "2026-10-04"},  # after the read
        {"dtc_baseline__settlement_date": "2026-09-01"},  # older than 31 days
        {"history_floor__floor": "2026-07-02"},
        {"history_floor__floor": "2026-08-01"},  # not history_floor of the counts
        {"history_floor__threshold": "11/1"},  # not floor_threshold of the counts
        {"history_floor__threshold": 10},
        {"history_floor__month_counts": _counts({m: n for m, n in COUNTS.items() if m != min(COUNTS)})},
        {"history_floor__month_counts": _counts(COUNTS | {date(2026, 10, 1): 7})},  # the freeze month
        {"history_floor__month_counts": _counts(COUNTS | {date(2026, 5, 1): -1})},
        {"history_floor__month_counts": _counts(COUNTS | {date(2026, 5, 1): 50})},  # the floor moves
        {"history_floor__month_counts": []},
    ],
)
def test_a_malformed_block_raises(edits: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="malformed ranking-pot-v2 frozen terms"):
        decode_frozen(_block(**edits), s0_ids=S0_IDS)


def test_a_floor_at_the_freeze_month_is_refused_by_the_codec() -> None:
    """A one-month run: the freeze refuses it, and the reader would too."""
    counts = {m: (100 if m >= date(2026, 9, 1) else 5) for m in count_window(date(2026, 10, 1), date(2026, 10, 1))}
    doc = _block(history_floor__floor="2026-10-01", history_floor__month_counts=_counts(counts))
    with pytest.raises(ValueError, match="before the freeze month"):
        decode_frozen(doc, s0_ids=S0_IDS)


def test_an_extra_or_missing_key_raises() -> None:
    doc = _block()
    doc["extra"] = 1
    with pytest.raises(ValueError):
        decode_frozen(doc, s0_ids=S0_IDS)
    doc = _block()
    del doc["dtc_baseline"]["read_at"]
    with pytest.raises(ValueError):
        decode_frozen(doc, s0_ids=S0_IDS)


def _decl(doc: dict[str, Any]) -> PotDeclaration:
    return PotDeclaration(1, doc, "c" * 64, datetime(2026, 10, 3, tzinfo=UTC), "shadow_only")


def test_the_document_carries_what_sql_463_and_the_jobs_read() -> None:
    s0 = S0(scored_at=datetime(2026, 10, 2, 23, 58, tzinfo=UTC), instrument_ids=S0_IDS)
    doc = build_declaration(
        s0=s0,
        family_seq=2,
        module_sha256={"x.py": "d" * 64},
        constant_repr={},
        policy_hash="e" * 64,
        frozen=TERMS,
        provenance=PROVENANCE,
    )
    terms = doc["terms"]
    # sql/463: execution "none" and book_count = k_controls + 3, as JSON string and integers.
    assert (terms["execution"], terms["book_count"], terms["k_controls"]) == ("none", K_CONTROLS + 3, K_CONTROLS)
    assert (doc["strategy_id"], doc["strategy_version"], doc["family"]) == ("ranking-pot-v2", "v1", "ranking-pot")
    assert (terms["per_look_alpha"], terms["turnover_bar"], terms["dtc_coverage_floor"]) == ("1/160", "1/2", "19/20")
    assert (terms["insider_lookback_months"], terms["dtc_max_age_days"]) == (6, 31)
    assert frozen_terms(_decl(doc)) == TERMS
    with pytest.raises(ValueError, match="not ranking-pot-v2"):
        frozen_terms(_decl(doc | {"strategy_id": "ranking-pot-v1"}))
    with pytest.raises(ValueError, match="absent"):
        frozen_terms(_decl({k: v for k, v in doc.items() if k != "v2"}))


def test_alpha_resolution_bounds_the_family_sequence() -> None:
    """§2: 1/(K + 1) ≤ 1 / (40 · 2^m) holds through m = 7."""
    assert [alpha_refusal(m) for m in (1, 2, 7)] == [None, None, None]
    assert alpha_refusal(8) == "alpha_resolution"


def test_floor_threshold_is_the_floor_rule_s_own() -> None:
    freeze = date(2026, 10, 1)
    counts = {v2.months_back(freeze, k): 100 + k for k in range(1, 25)}
    assert v2.floor_threshold(counts, freeze_month=freeze) == Fraction(112, 10)  # median_low of 101..124 = 112
    assert v2.history_floor(counts, freeze_month=freeze) == v2.months_back(freeze, 23)


def test_register_holds_the_expected_entry_verbatim() -> None:
    assert EXPECTED_REGISTER_ENTRY in TRIAL_REGISTER.trials
    assert TRIAL_REGISTER.trial_for_declaration("ranking-pot-v2", policy.STRATEGY_VERSION) == EXPECTED_REGISTER_ENTRY
