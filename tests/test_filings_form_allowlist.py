"""Tests for the SEC form-type allow-list (#1011).

Spec: docs/superpowers/specs/2026-05-08-filing-allow-list-and-raw-retention.md.

Pins the three-tier model: every form an active parser consumes
must be in ``SEC_PARSE_AND_RAW``; future-signal forms in
``SEC_METADATA_ONLY``; the union ``SEC_INGEST_KEEP_FORMS`` is what
``bootstrap_filings_history_seed`` and ``daily_research_refresh``
pass to ``refresh_filings``.
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
    }
    missing = must_parse - SEC_PARSE_AND_RAW
    assert missing == set(), f"forms missing from SEC_PARSE_AND_RAW (parsers will starve): {sorted(missing)}"


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
        "PRE 14A",
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
        "NT 10-K",
        "NT 10-Q",
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
