"""Pure-logic tests for the capability-override diff (#531).

``diff_capabilities`` is the decision the admin endpoint renders: which
capabilities of one exchange diverge from the seed default for its asset
class. Kept DB-free (fast tier) — the endpoint's SQL/schema wiring is
guarded separately by the DB-backed test_capability_overrides_endpoint.
"""

from __future__ import annotations

from app.api.capability_overrides_admin import (
    _EMPTY_SEED,
    _SEED_BY_ASSET_CLASS,
    diff_capabilities,
)


def _seed_us_equity() -> dict[str, list[str]]:
    return dict(_SEED_BY_ASSET_CLASS["us_equity"])


def test_at_seed_default_yields_no_diff() -> None:
    diffs = diff_capabilities("us_equity", _seed_us_equity())
    assert diffs == []


def test_provider_reorder_is_not_drift() -> None:
    # ownership seed is ["sec_13f", "sec_13d_13g"]; reversed set is equal.
    current = _seed_us_equity()
    current["ownership"] = ["sec_13d_13g", "sec_13f"]
    assert diff_capabilities("us_equity", current) == []


def test_duplicate_provider_is_not_drift() -> None:
    # A repeated provider resolves identically at runtime (resolver
    # dedups), so it must not register as drift (Codex review #531).
    current = _seed_us_equity()
    current["filings"] = ["sec_edgar", "sec_edgar"]
    assert diff_capabilities("us_equity", current) == []


def test_duplicate_provider_is_deduped_in_output() -> None:
    current = _seed_us_equity()
    current["ownership"] = ["sec_13f", "sec_13f", "extra_provider"]
    diffs = diff_capabilities("us_equity", current)
    assert len(diffs) == 1
    # Output carries each provider once → unique React keys downstream.
    assert diffs[0].current_providers == ["sec_13f", "extra_provider"]


def test_unknown_capability_key_with_providers_surfaces_as_drift() -> None:
    # An operator typo / extra key the resolver ignores must not hide.
    current = _seed_us_equity()
    current["filngs"] = ["sec_edgar"]  # typo of "filings"
    diffs = diff_capabilities("us_equity", current)
    assert len(diffs) == 1
    assert diffs[0].capability == "filngs"
    assert diffs[0].seed_providers == []
    assert diffs[0].current_providers == ["sec_edgar"]


def test_unknown_empty_capability_key_is_not_flagged() -> None:
    # An empty unknown key has no runtime effect and nothing to show.
    current = _seed_us_equity()
    current["bogus"] = []
    assert diff_capabilities("us_equity", current) == []


def test_removed_provider_surfaces_as_drift() -> None:
    current = _seed_us_equity()
    current["filings"] = []
    diffs = diff_capabilities("us_equity", current)
    assert len(diffs) == 1
    d = diffs[0]
    assert d.capability == "filings"
    assert d.seed_providers == ["sec_edgar"]
    assert d.current_providers == []


def test_added_provider_surfaces_as_drift() -> None:
    current = _seed_us_equity()
    current["ownership"] = ["sec_13f", "sec_13d_13g", "custom_provider"]
    diffs = diff_capabilities("us_equity", current)
    assert len(diffs) == 1
    assert diffs[0].capability == "ownership"
    assert "custom_provider" in diffs[0].current_providers


def test_unknown_asset_class_uses_empty_seed() -> None:
    # An exchange whose asset_class has no seed entry is compared against
    # the empty-but-correctly-shaped default — any provider is drift.
    diffs = diff_capabilities("crypto", {"filings": ["some_provider"]})
    assert len(diffs) == 1
    assert diffs[0].capability == "filings"
    assert diffs[0].seed_providers == []
    assert diffs[0].current_providers == ["some_provider"]


def test_empty_capabilities_against_us_equity_seed_flags_every_seeded_cap() -> None:
    # An exchange whose capabilities were wiped to {} diverges on every
    # capability the seed populates (the live exchange-33 case).
    diffs = diff_capabilities("us_equity", {})
    seeded_nonempty = {cap for cap, providers in _SEED_BY_ASSET_CLASS["us_equity"].items() if providers}
    assert {d.capability for d in diffs} == seeded_nonempty


def test_non_dict_capabilities_is_treated_as_empty() -> None:
    # Defensive: a malformed JSONB value (not an object) must not 500;
    # it reads as "no providers" → diverges from any non-empty seed.
    diffs = diff_capabilities("us_equity", None)
    assert {d.capability for d in diffs} == {
        cap for cap, providers in _SEED_BY_ASSET_CLASS["us_equity"].items() if providers
    }


def test_none_asset_class_uses_empty_seed() -> None:
    assert diff_capabilities(None, _EMPTY_SEED) == []
