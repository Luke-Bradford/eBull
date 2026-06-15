"""Pure-logic tests for institutional family identity (#1644 + #1649).

No DB: the registry resolution and the family-reconciliation pre-pass are
pure functions over ``Holder`` / ``_Candidate`` value objects. See
``docs/specs/etl/2026-06-15-institutional-family-identity.md``.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.services import institutional_families as fam
from app.services.institutional_families import (
    InstitutionalFamily,
    _validate_registry,
    resolve_family,
)
from app.services.ownership_rollup import (
    Holder,
    _Candidate,
    _reconcile_institutional_families,
)

OUTSTANDING = Decimal(14_687_356_000)  # ~AAPL


def _holder(cik: str | None, name: str, shares: int, source: str = "13f", filer_type: str | None = "INV") -> Holder:
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source=source,  # type: ignore[arg-type]
        winning_accession=f"acc-{cik or name}",
        winning_edgar_url=None,
        as_of_date=date(2026, 3, 31),
        filer_type=filer_type,
        dropped_sources=(),
    )


def _proxy(name: str, shares: int, *, row_id: int = 1) -> _Candidate:
    return _Candidate(
        source="def14a",
        priority_rank=4,
        filer_cik=None,
        filer_name=name,
        filer_type=None,
        shares=Decimal(shares),
        as_of_date=date(2026, 1, 2),
        accession_number=f"px-{row_id}",
        source_row_id=row_id,
        ownership_nature="beneficial",
    )


# --------------------------------------------------------------------------
# Registry resolution + validation
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("The\xa0Vanguard\xa0Group", "vanguard"),
        ("VANGUARD GROUP INC", "vanguard"),
        ("VANGUARD CAPITAL MANAGEMENT LLC", "vanguard"),
        ("The Vanguard Group 100 Vanguard Blvd. Malvern, PA 19355", "vanguard"),  # address-contaminated
        ("BlackRock,\xa0Inc.", "blackrock"),
        ("BlackRock Fund Advisors", "blackrock"),
        ("STATE STREET CORP", "state_street"),
        ("PRICE T ROWE ASSOCIATES INC /MD/", "t_rowe_price"),
        ("T. Rowe Price Group, Inc.", "t_rowe_price"),
        ("J.P. Morgan Securities", "jpmorgan"),
        ("JPMorgan Chase & Co", "jpmorgan"),
        ("FMR LLC", "fmr"),
        # No false positives:
        ("Tim Cook", None),
        ("Fidelity National Financial", None),  # NOT FMR/Fidelity asset manager
        ("", None),
    ],
)
def test_resolve_family(name: str, expected: str | None) -> None:
    result = resolve_family(None, name)
    assert (result.family_id if result else None) == expected


def test_resolve_family_cik_fast_path() -> None:
    # Vanguard parent CIK resolves regardless of the name string.
    result = resolve_family("0000102909", "anything at all")
    assert result is not None and result.family_id == "vanguard"


def test_registry_validates_clean() -> None:
    # Import already ran _validate_registry(FAMILIES); re-run to be explicit.
    _validate_registry(fam.FAMILIES)


def test_registry_rejects_duplicate_cik() -> None:
    bad = (
        InstitutionalFamily("a", "A", ("aaa",), frozenset({"0000000001"}), "institutions"),
        InstitutionalFamily("b", "B", ("bbb",), frozenset({"0000000001"}), "institutions"),
    )
    with pytest.raises(ValueError, match="in two families"):
        _validate_registry(bad)


def test_registry_rejects_overlapping_patterns() -> None:
    bad = (
        InstitutionalFamily("a", "A", ("black",), frozenset(), "institutions"),
        InstitutionalFamily("b", "B", ("blackrock",), frozenset(), "institutions"),  # "black" ⊂ "blackrock"
    )
    with pytest.raises(ValueError, match="ambiguous patterns"):
        _validate_registry(bad)


def test_resolve_family_ambiguous_match_is_singleton(caplog: pytest.LogCaptureFixture) -> None:
    # Two families with patterns that BOTH match one name → fail-closed to None.
    families = (
        InstitutionalFamily("x", "X", ("alpha",), frozenset(), "institutions"),
        InstitutionalFamily("y", "Y", ("beta",), frozenset(), "institutions"),
    )
    orig = fam.FAMILIES
    try:
        fam.FAMILIES = families  # type: ignore[misc]
        assert resolve_family(None, "Alpha Beta Capital") is None
    finally:
        fam.FAMILIES = orig  # type: ignore[misc]


# --------------------------------------------------------------------------
# Family reconciliation pre-pass
# --------------------------------------------------------------------------


def test_vanguard_proxy_folds_under_13f_sum() -> None:
    """13F family sum (disjoint sub-books) > proxy → 13F wins, proxy folds."""
    survivors = [
        _holder("0000102909", "VANGUARD GROUP INC", 900_000_000),
        _holder("0002100119", "VANGUARD CAPITAL MANAGEMENT LLC", 536_449_741),
    ]
    proxy = [_proxy("The\xa0Vanguard\xa0Group", 1_415_826_462)]
    by_cat, rest_s, rest_b, rest_u, corr = _reconcile_institutional_families(survivors, [], proxy, OUTSTANDING)

    assert rest_s == [] and rest_u == []  # all consumed by the family
    holders = by_cat["institutions"]
    assert len(holders) == 1
    fh = holders[0]
    assert fh.filer_name == "The Vanguard Group"
    assert fh.winning_source == "13f"
    assert fh.shares == Decimal(1_436_449_741)  # SUM of the two sub-CIKs
    assert len(fh.family_members) == 2  # breakdown preserved
    # proxy folded as a dropped source + correction
    assert [d.source for d in fh.dropped_sources] == ["def14a"]
    assert fh.dropped_sources[0].shares == Decimal(1_415_826_462)
    assert len(corr) == 1
    assert corr[0].kind == "def14a_restates_institution"
    assert corr[0].shares_removed == Decimal(1_415_826_462)
    assert corr[0].family_id == "vanguard"


def test_blackrock_proxy_fills_13f_gap() -> None:
    """Proxy consolidated figure > tiny 13F shell → proxy wins, 13F folds."""
    survivors = [_holder("0002012383", "BlackRock, Inc.", 6_122_822)]
    proxy = [_proxy("BlackRock,\xa0Inc.", 1_043_713_019)]
    by_cat, _rs, _rb, rest_u, corr = _reconcile_institutional_families(survivors, [], proxy, OUTSTANDING)

    assert rest_u == []
    holders = by_cat["institutions"]
    assert len(holders) == 1
    fh = holders[0]
    assert fh.winning_source == "def14a"
    assert fh.shares == Decimal(1_043_713_019)
    assert [d.source for d in fh.dropped_sources] == ["13f"]
    assert fh.dropped_sources[0].shares == Decimal(6_122_822)
    assert corr[0].kind == "institutional_family_collapse"
    assert corr[0].shares_removed == Decimal(6_122_822)


def test_garbage_proxy_value_rejected_before_max() -> None:
    """A value > shares_outstanding can never win the family MAX (#1644 LAMR)."""
    survivors = [_holder("0000102909", "VANGUARD GROUP INC", 900_000_000)]
    proxy = [_proxy("The Vanguard Group", 48_300_711_362_250)]  # 48 trillion garbage
    by_cat, _rs, _rb, _ru, corr = _reconcile_institutional_families(survivors, [], proxy, OUTSTANDING)
    # The garbage proxy is rejected before the MAX; only the sane 13F row remains,
    # so the family figure is the 13F value (never the 48T) and no fold/correction.
    assert by_cat["institutions"][0].shares == Decimal(900_000_000)
    assert corr == []


def test_garbage_non_family_proxy_dropped_from_wedge() -> None:
    """Non-family proxy garbage (LAMR Kevin Reilly) is dropped, not kept additive."""
    proxy = [_proxy("Kevin P. Reilly, Jr.", 48_300_711_362_250), _proxy("Jane Director", 1_000_000, row_id=2)]
    _bc, _rs, _rb, rest_u, _corr = _reconcile_institutional_families([], [], proxy, OUTSTANDING)
    names = [c.filer_name for c in rest_u]
    assert "Jane Director" in names  # sane individual kept
    assert "Kevin P. Reilly, Jr." not in names  # garbage dropped


def test_non_curated_holder_passes_through() -> None:
    """Zero regression: a holder in no family is returned unchanged."""
    h = _holder("0000999999", "NORGES BANK", 192_255_086)
    by_cat, rest_s, _rb, _ru, corr = _reconcile_institutional_families([h], [], [], OUTSTANDING)
    assert by_cat == {} and corr == []
    assert rest_s == [h]


def test_single_family_13f_row_collapses_to_family_bucket() -> None:
    """A lone curated-family 13F row still collapses to a family holder so it
    lands in the family bucket (institutions) with its sub-CIK breakdown — no
    cross-channel fold, no correction, same shares (Codex ckpt-2 P1)."""
    h = _holder("0000102909", "VANGUARD GROUP INC", 900_000_000)
    by_cat, rest_s, _rb, _ru, corr = _reconcile_institutional_families([h], [], [], OUTSTANDING)
    assert rest_s == [] and corr == []
    holders = by_cat["institutions"]
    assert len(holders) == 1
    assert holders[0].filer_name == "The Vanguard Group"
    assert holders[0].shares == Decimal(900_000_000)
    assert len(holders[0].family_members) == 1
    assert holders[0].dropped_sources == ()


def test_single_family_13g_row_lands_in_institutions_bucket() -> None:
    """A lone curated-family 13G row is bucketed by the family (institutions),
    NOT left to owner-once which would classify it as a blockholder."""
    h = _holder("0000093751", "STATE STREET CORP", 600_000_000, source="13g", filer_type=None)
    by_cat, _rs, rest_b, _ru, _corr = _reconcile_institutional_families([], [h], [], OUTSTANDING)
    assert rest_b == []
    assert "blockholders" not in by_cat
    assert by_cat["institutions"][0].filer_name == "State Street Corporation"
    assert by_cat["institutions"][0].winning_source == "13g"


def test_duplicate_proxy_rows_collapse_to_max() -> None:
    """Two same-family proxy rows (no 13F) collapse to the MAX, not additive (Codex F7)."""
    proxy = [_proxy("The Vanguard Group", 1_000_000, row_id=1), _proxy("The\xa0Vanguard\xa0Group", 1_200_000, row_id=2)]
    by_cat, _rs, _rb, rest_u, _corr = _reconcile_institutional_families([], [], proxy, OUTSTANDING)
    assert rest_u == []
    holders = by_cat["institutions"]
    assert len(holders) == 1 and holders[0].shares == Decimal(1_200_000)


def test_13f_sum_as_of_is_oldest_member() -> None:
    """A 13F-sum family holder's as_of is the OLDEST member quarter (conservative)."""
    a = _holder("0000102909", "VANGUARD GROUP INC", 500_000_000)
    b = _holder("0002100119", "VANGUARD CAPITAL MANAGEMENT LLC", 600_000_000)
    object.__setattr__(a, "as_of_date", date(2025, 12, 31))
    object.__setattr__(b, "as_of_date", date(2026, 3, 31))
    by_cat, *_ = _reconcile_institutional_families([a, b], [], [], OUTSTANDING)
    assert by_cat["institutions"][0].as_of_date == date(2025, 12, 31)
