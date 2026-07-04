"""Tests for the SEC form-type allow-list (#1011).

Spec: docs/superpowers/specs/2026-05-08-filing-allow-list-and-raw-retention.md.

Pins the three-tier model: every form an active parser consumes
must be in ``SEC_PARSE_AND_RAW``; future-signal forms in
``SEC_METADATA_ONLY``; the union ``SEC_INGEST_KEEP_FORMS`` is what
``filings_history_seed`` (PR1c #1064 — formerly the bespoke
``bootstrap_filings_history_seed`` wrapper) and
``daily_research_refresh`` pass to ``refresh_filings``.
"""

from __future__ import annotations

from app.services.filings import (
    SEC_INGEST_KEEP_FORMS,
    SEC_METADATA_ONLY,
    SEC_PARSE_AND_RAW,
)


def test_tiers_are_disjoint() -> None:
    """A form belongs to exactly one tier (parse-and-raw OR metadata-only).

    Overlap would mean the spec is ambiguous about whether the
    raw-payload retention sweeper should touch the form.
    """
    overlap = SEC_PARSE_AND_RAW & SEC_METADATA_ONLY
    assert overlap == frozenset(), f"forms in both tiers: {sorted(overlap)}"


def test_union_is_public_constant() -> None:
    assert SEC_INGEST_KEEP_FORMS == SEC_PARSE_AND_RAW | SEC_METADATA_ONLY


def test_active_parser_forms_are_in_parse_and_raw() -> None:
    """Every form a parser SQL filters on must be in
    ``SEC_PARSE_AND_RAW`` so the bootstrap allow-list doesn't
    starve a parser of input rows.

    Pinned forms below are the ones we have explicit parser code
    for today (verified by grep on app/services/*.py for
    ``filing_type IN`` and ``filing_type =`` patterns).
    """
    must_parse = {
        "10-K",
        "10-K/A",
        "10-Q",
        "10-Q/A",
        "8-K",
        "8-K/A",
        "DEF 14A",
        "DEFA14A",
        "DEFM14A",
        "DEFR14A",
        "3",
        "3/A",
        "4",
        "4/A",
        "13F-HR",
        "13F-HR/A",
        "NPORT-P",
        "NPORT-P/A",
        "SCHEDULE 13G",
        "SCHEDULE 13G/A",
        "SCHEDULE 13D",
        "SCHEDULE 13D/A",
        # Legacy short-form 13D/G names (#1013). The blockholder parser
        # accepts both conventions; pre-2024-12-19 EDGAR emits these.
        "SC 13D",
        "SC 13D/A",
        "SC 13G",
        "SC 13G/A",
        # Late-filing notices (Form 12b-25). #1015 — sec_nt parser.
        "NT 10-K",
        "NT 10-Q",
    }
    missing = must_parse - SEC_PARSE_AND_RAW
    assert missing == set(), f"forms missing from SEC_PARSE_AND_RAW (parsers will starve): {sorted(missing)}"


def test_keep_set_covers_blockholder_accepted_forms() -> None:
    """#1013 regression guard — the keep-set MUST be a superset of every
    form name the blockholder parser accepts.

    The #1013 skip-tier cleanup deletes ``filing_events`` rows whose
    ``filing_type`` is not in ``SEC_INGEST_KEEP_FORMS``. The original
    keep-set listed only the long-form ``SCHEDULE 13D/G`` names, but the
    blockholder ingest (``_SUBMISSIONS_INDEX_FORMS``) also consumes the
    legacy short-form ``SC 13D/G`` names (EDGAR renamed SC -> SCHEDULE on
    2024-12-19). A literal NOT-IN-keep-set sweep would have deleted ~131k
    actively-parsed legacy 13D/G rows. Coupling the assertion to the
    parser's own accepted-form set means: if the parser learns a new form
    name, this test fails until the keep-set covers it too — the keep-set
    can never silently drop below what a parser consumes.
    """
    from app.services.blockholders import _SUBMISSIONS_INDEX_FORMS

    missing = _SUBMISSIONS_INDEX_FORMS - SEC_INGEST_KEEP_FORMS
    assert missing == set(), (
        f"blockholder-accepted forms missing from SEC_INGEST_KEEP_FORMS "
        f"(skip-tier cleanup would delete actively-parsed rows): {sorted(missing)}"
    )


def test_legacy_13dg_aliases_in_parse_and_raw() -> None:
    """Explicit pin for the four short-form 13D/G aliases added in #1013."""
    aliases = {"SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"}
    missing = aliases - SEC_PARSE_AND_RAW
    assert missing == set(), f"legacy 13D/G aliases missing from SEC_PARSE_AND_RAW: {sorted(missing)}"


def test_metadata_only_forms_have_no_parser_today() -> None:
    """Sanity check: forms we mark metadata-only should not also
    be in the parse-and-raw set. If a parser lands later, move
    the form between sets in lockstep."""
    # Pick a few representative forms the spec calls out as
    # metadata-only (#1011 Codex round 1).
    metadata_forms = {
        "S-1",
        "S-3",
        "S-4",
        "F-1",
        "F-3",
        "F-4",
        "424B2",
        "424B3",
        "SC TO-T",
        "SC 14D9",
        "DEF 13E-3",
        "25-NSE",
        "15F",
        "5",
        "5/A",
        "144",
        "CORRESP",
        "11-K",
        # NT 10-K / NT 10-Q PROMOTED to SEC_PARSE_AND_RAW (#1015 — sec_nt).
        # NT 20-F stays metadata-only (foreign deadline regime).
        # PRE 14A / PRER14A PROMOTED to SEC_PARSE_AND_RAW (#1892 — sec_pre14a).
        "NT 20-F",
        "20-F",
        "40-F",
        "6-K",
        "13F-NT",
    }
    missing = metadata_forms - SEC_METADATA_ONLY
    assert missing == set(), f"forms missing from SEC_METADATA_ONLY: {sorted(missing)}"
    in_both = metadata_forms & SEC_PARSE_AND_RAW
    assert in_both == set(), f"metadata-only forms also in parse-and-raw: {sorted(in_both)}"


def test_skipped_forms_not_in_keep_list() -> None:
    """Forms the spec marks SKIP must NOT be in the union — including
    them would defeat the allow-list purpose.

    Pinned set is the documented SKIP tier from the spec. Adding a
    form to the keep list later requires moving it out of this test
    deliberately, surfacing the decision in code review.
    """
    skip_forms = {
        "FWP",
        "N-CSR",
        "N-CSRS",
        "D",
        "D/A",
        "S-8",
        "S-8 POS",
    }
    overlap = skip_forms & SEC_INGEST_KEEP_FORMS
    assert overlap == set(), f"forms marked SKIP appear in keep list: {sorted(overlap)}"


def test_spec_skip_table_matches_constants() -> None:
    """Drift guard: every form the spec table declares as SKIP must
    be absent from ``SEC_INGEST_KEEP_FORMS``.

    PR1012 review caught the spec table contradicting the implementation
    (424B*, CORRESP, S-1/S-3/S-4 marked SKIP in the table but METADATA-ONLY
    in code). This test prevents that drift class from re-occurring.

    The spec's authoritative SKIP table is reproduced here; updates to
    either the table or the constants must be reflected in both places.
    """
    spec_skip_forms = {
        "FWP",
        "N-CSR",
        "N-CSRS",
        "D",
        "D/A",
        "S-8",
        "S-8 POS",
    }
    overlap = spec_skip_forms & SEC_INGEST_KEEP_FORMS
    assert overlap == set(), (
        f"forms the spec table marks SKIP appear in SEC_INGEST_KEEP_FORMS: "
        f"{sorted(overlap)} — spec table and code constants have drifted"
    )


def test_keep_list_size_is_bounded() -> None:
    """Pin a size budget so an accidental ``frozenset.update``-style
    over-broad merge fails the test. Current expected count is ~70."""
    assert 50 <= len(SEC_INGEST_KEEP_FORMS) <= 100, (
        f"SEC_INGEST_KEEP_FORMS size {len(SEC_INGEST_KEEP_FORMS)} outside expected range; "
        "if this is intentional, update the test bounds."
    )
