"""Institutional manager-family identity for the ownership rollup
(#1644 + #1649).

A large asset-manager files 13F-HR under many CIKs (Vanguard: 11+;
BlackRock: ~50) and is reported in a company's DEF 14A "Security
Ownership" 5%-holder table under a single *family-consolidated* name
("The Vanguard Group", "BlackRock, Inc.") — the consolidated Schedule
13G figure (SEC Reg S-K Item 403). The rollup must treat that family as
**one owner, counted once** at the MAX of its channel estimates, or it
either double-counts (the proxy figure added on top of the summed 13F
sub-books — Vanguard) or undercounts (the proxy is the only complete
figure because the family's main 13F CIK is absent from ingestion —
BlackRock).

This module is the curated, deterministic, auditable family registry
that both the 13F-CIK side and the proxy-name side resolve against, so
they cannot drift. It is intentionally NOT auto-detection (fragile) and
NOT a DB table (no migration needed) — the universe of managers that
appear as >5% proxy holders is small and stable.

Spec: ``docs/specs/etl/2026-06-15-institutional-family-identity.md``.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Final, Literal

from app.services.holder_name_resolver import normalise_name

logger = logging.getLogger(__name__)

FamilyBucket = Literal["institutions", "etfs"]


@dataclass(frozen=True)
class InstitutionalFamily:
    """One asset-manager family.

    ``name_patterns`` are the **load-bearing** matcher (the curated
    ``ciks`` set is a convenience fast-path and is necessarily
    incomplete — new sub-entities file every quarter). Patterns are
    lowercased substrings matched against a whitespace-collapsed,
    role-suffix-stripped holder/filer name, so they are robust to the
    non-breaking-space and trailing-street-address contamination seen in
    ``ownership_def14a_current`` (#1644 evidence). Patterns must be
    specific enough to never match an unrelated holder and must not
    overlap across families (enforced by :func:`_validate_registry`).
    """

    family_id: str
    display_name: str
    name_patterns: tuple[str, ...]
    ciks: frozenset[str]
    bucket: FamilyBucket


# The curated families — seeded from the ``ownership_def14a_current`` >5%
# holder-name distribution on dev (Vanguard / BlackRock / State Street
# dominate). Patterns are deliberately conservative and mutually
# non-overlapping. ``ciks`` carry a few observed parent CIKs as a
# fast-path; resolution falls back to ``name_patterns`` regardless.
#
# All curated families are manager *parents* → ``bucket="institutions"``.
# An ETF-issuer family would set ``bucket="etfs"``; none is curated yet.
FAMILIES: Final[tuple[InstitutionalFamily, ...]] = (
    InstitutionalFamily("vanguard", "The Vanguard Group", ("vanguard",), frozenset({"0000102909"}), "institutions"),
    InstitutionalFamily("blackrock", "BlackRock, Inc.", ("blackrock",), frozenset({"0002012383"}), "institutions"),
    InstitutionalFamily(
        "state_street", "State Street Corporation", ("state street",), frozenset({"0000093751"}), "institutions"
    ),  # noqa: E501
    InstitutionalFamily("geode", "Geode Capital Management", ("geode",), frozenset(), "institutions"),
    InstitutionalFamily("t_rowe_price", "T. Rowe Price", ("t rowe",), frozenset(), "institutions"),
    InstitutionalFamily(
        "capital_group",
        "Capital Group",
        ("capital research", "capital group cos", "capital world investors"),
        frozenset(),
        "institutions",
    ),  # noqa: E501
    InstitutionalFamily("morgan_stanley", "Morgan Stanley", ("morgan stanley",), frozenset(), "institutions"),
    InstitutionalFamily("jpmorgan", "JPMorgan Chase", ("jpmorgan", "jp morgan"), frozenset(), "institutions"),
    InstitutionalFamily("goldman_sachs", "The Goldman Sachs Group", ("goldman sachs",), frozenset(), "institutions"),
    InstitutionalFamily("northern_trust", "Northern Trust", ("northern trust",), frozenset(), "institutions"),
    InstitutionalFamily("wellington", "Wellington Management", ("wellington management",), frozenset(), "institutions"),
    InstitutionalFamily("invesco", "Invesco", ("invesco",), frozenset(), "institutions"),
    # "charles schwab" (NOT bare "schwab") — the firm forms ("CHARLES SCHWAB
    # INVESTMENT MANAGEMENT INC", "Charles Schwab Trust Co") match; the proxy
    # persons "Charles R. Schwab" / "Andrew J. Schwab" / "Mark D. Schwabero" do
    # NOT (the middle initial breaks the contiguous phrase). Codex ckpt-2 caught
    # the bare-surname person-collision.
    InstitutionalFamily("charles_schwab", "Charles Schwab", ("charles schwab",), frozenset(), "institutions"),
    InstitutionalFamily("dimensional", "Dimensional Fund Advisors", ("dimensional fund",), frozenset(), "institutions"),
    # "fmr" as a whole word matches "FMR LLC" / "FMR, LLC" / bare "FMR" (the proxy
    # consolidated forms); word-boundary matching excludes "fmrc …" non-Fidelity.
    InstitutionalFamily("fmr", "FMR LLC (Fidelity)", ("fmr",), frozenset(), "institutions"),
)


def _family_haystack(name: str) -> str:
    """Whitespace-collapsed, role-suffix-stripped, lowercased name for
    substring matching. ``normalise_name`` lowercases + strips the first
    role suffix; ``.replace(".", "")`` drops periods so ``T. Rowe`` →
    ``t rowe`` and ``J.P. Morgan`` → ``jp morgan`` and one period-free
    pattern matches every form; ``split()`` collapses the ``\\xa0``
    non-breaking spaces and runs of whitespace that
    ``ownership_def14a_current`` names carry."""
    return " ".join(normalise_name(name).replace(".", "").split())


def _pattern_matches(pattern: str, haystack: str) -> bool:
    """Whole-word(s) match — a pattern matches only on word boundaries, so a
    short token like ``fmr`` matches ``"fmr llc"`` but NOT ``"fmrc holdings"``,
    and ``schwab`` would not match ``"schwabero"`` (Codex / review bot caught the
    bare-substring over-match class). Multi-word patterns (``state street``) keep
    working — the boundary is around the whole phrase."""
    return re.search(rf"\b{re.escape(pattern)}\b", haystack) is not None


def _validate_registry(families: tuple[InstitutionalFamily, ...]) -> None:
    """Fail-closed at import: no CIK in two families; no name pattern
    shared by or a substring of another family's pattern (Codex spec
    review F4/F6 — an ambiguous pattern could silently relocate a
    holder). Raises ``ValueError`` on violation."""
    seen_cik: dict[str, str] = {}
    for fam in families:
        for cik in fam.ciks:
            if cik in seen_cik:
                raise ValueError(f"CIK {cik} in two families: {seen_cik[cik]} and {fam.family_id}")
            seen_cik[cik] = fam.family_id
    # Cross-family pattern disjointness: no pattern may be a substring of
    # a pattern in a different family (a == b is a substring too).
    pats = [(fam.family_id, p) for fam in families for p in fam.name_patterns]
    for i, (fid_a, pa) in enumerate(pats):
        for fid_b, pb in pats[i + 1 :]:
            if fid_a == fid_b:
                continue
            if pa in pb or pb in pa:
                raise ValueError(f"ambiguous patterns across families: {fid_a}:{pa!r} vs {fid_b}:{pb!r}")


_validate_registry(FAMILIES)

# Built once after validation. CIK → family for the O(1) fast-path.
_CIK_INDEX: Final[dict[str, InstitutionalFamily]] = {cik: fam for fam in FAMILIES for cik in fam.ciks}


def resolve_family(filer_cik: str | None, filer_name: str) -> InstitutionalFamily | None:
    """Resolve a 13F / 13G / proxy holder to its curated family, or
    ``None`` (singleton — today's behaviour, no regression).

    CIK fast-path first, then name-pattern. Fail-closed on an ambiguous
    name that matches >1 family (return ``None`` + WARN — a false
    positive must never silently relocate a holder)."""
    if filer_cik:
        fam = _CIK_INDEX.get(filer_cik.strip())
        if fam is not None:
            return fam
    haystack = _family_haystack(filer_name)
    if not haystack:
        return None
    matches = [fam for fam in FAMILIES if any(_pattern_matches(p, haystack) for p in fam.name_patterns)]
    if not matches:
        return None
    if len(matches) > 1:
        logger.warning(
            "institutional_families: name %r matched >1 family (%s) — resolving to singleton",
            filer_name,
            [m.family_id for m in matches],
        )
        return None
    return matches[0]
