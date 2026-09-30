"""#3518 — the hashed MD&A extraction module: normalisation, caption rule, outcome mapping, extractor id, and the
untrimmed real-document fixtures (spec §4.3, §7)."""

from __future__ import annotations

import gzip
import hashlib
from importlib.metadata import version
from pathlib import Path

import pytest

from app.services import mdna_extraction as mx
from app.services.mdna_extraction import (
    CAPTION_WINDOW,
    MDNA_EXTRACTOR_REVISION,
    ParseOutcome,
    caption_present,
    classify_text,
    extract_mdna,
    extractor_id,
    normalise_mdna,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "sec" / "mdna"

#: sha256 of app/services/mdna_extraction.py per revision. ⚠ APPEND a new entry with a revision bump; never edit
#: an existing one. A bump is a new extractor id, which re-extracts every target and must be refused while a
#: fund-v1 declaration is live (#3515 slice 3).
_REVISION_SOURCE_SHA256: dict[int, str] = {
    1: "8100ccae13f24ebf042c3729751820ca60a8cb71129b4c1ff51f570437ef4190",
}


def test_module_source_is_pinned_to_its_revision() -> None:
    digest = hashlib.sha256(Path(mx.__file__).read_bytes()).hexdigest()
    assert MDNA_EXTRACTOR_REVISION == max(_REVISION_SOURCE_SHA256)
    assert digest == _REVISION_SOURCE_SHA256[MDNA_EXTRACTOR_REVISION], (
        "mdna_extraction.py changed: bump MDNA_EXTRACTOR_REVISION and append its hash (spec §4.3)"
    )


def test_extractor_id_names_the_installed_edgartools_and_the_revision() -> None:
    assert extractor_id() == f"edgartools=={version('edgartools')}/mdna-{MDNA_EXTRACTOR_REVISION}"


# --- normalisation (fund-v1 §4) --------------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("a\tb", "a b"),
        ("a b", "a b"),  # NBSP is horizontal whitespace
        ("a \t  b", "a b"),  # each -> one space, then the run collapses
        ("a\r\nb", "a\nb"),  # CR is a line separator, not horizontal: kept until the Cc strip removes it
        ("a\x0bb\x0cc", "abc"),  # VT / FF are vertical: not spaced, then stripped as control characters
        ("a\x00b\x07c", "abc"),
        ("a\n\n\n\nb", "a\n\nb"),
        ("a\n\nb", "a\n\nb"),
        ("  \n a \n  ", "a"),
        ("", ""),
    ],
)
def test_normalise_mdna(raw: str, expected: str) -> None:
    assert normalise_mdna(raw) == expected


# --- caption rule ----------------------------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text",
    [
        "Item 7. Management's Discussion and Analysis of Financial Condition",
        "MANAGEMENT’S DISCUSSION &amp; ANALYSIS",
        "Discussion & Analysis",
        "discussion\nand\nanalysis",  # split across lines
        "Financial Condition and Results of Operations",
        "financial condition &results of operations",
    ],
)
def test_caption_present(text: str) -> None:
    assert caption_present(text)


def test_caption_absent_on_other_items() -> None:
    assert not caption_present("NOTE 2. SUMMARY OF SIGNIFICANT ACCOUNTING POLICIES")


def test_caption_window_counts_after_collapsing_whitespace() -> None:
    caption = "Discussion and Analysis"
    inside = "x" * (CAPTION_WINDOW - len(caption)) + caption  # ends exactly at the window edge
    outside = "x" * (CAPTION_WINDOW - len(caption) + 2) + caption
    assert caption_present(inside)
    assert not caption_present(outside)
    # 3,000 characters of whitespace collapse to one space, so the caption is back inside the window.
    assert caption_present("x" * 10 + " " * 3_000 + caption)


# --- outcome mapping -------------------------------------------------------------------------------------------
def test_classify_text() -> None:
    assert classify_text(None) == ParseOutcome("item_absent", detail="none")
    assert classify_text(" \n\t ") == ParseOutcome("item_absent", detail="blank")
    assert classify_text("\x00\x01") == ParseOutcome("item_absent", detail="blank")
    assert classify_text("Item 2. Controls") == ParseOutcome("parse_failed", detail="caption_absent")
    body = "Item 7.\tDiscussion and Analysis\n\n\n\nText"
    assert classify_text(body) == ParseOutcome("extracted", body=body, full_chars=len(normalise_mdna(body)))


def test_accessor_exception_is_terminal_and_memory_is_retryable(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_a: object) -> str:
        raise KeyError("x" * 500)

    monkeypatch.setattr(mx, "_accessor_text", boom)
    out = extract_mdna("10-K", "<html/>", "acc", "https://x/y.htm")
    assert out.status == "parse_failed" and not out.retryable
    assert out.detail is not None and out.detail.startswith("KeyError: ") and len(out.detail) == len("KeyError: ") + 200

    def oom(*_a: object) -> str:
        raise MemoryError

    monkeypatch.setattr(mx, "_accessor_text", oom)
    assert extract_mdna("10-Q", "<html/>", "acc", "https://x/y.htm") == ParseOutcome(
        "parse_failed", detail="memory", retryable=True
    )


def test_unknown_family_is_a_code_defect() -> None:
    with pytest.raises(ValueError):
        extract_mdna("20-F", "<html/>", "acc", "https://x/y.htm")


# --- untrimmed real primary documents (spec §7, classes from §8.3) ---------------------------------------------
_HEAD_CHARS = 200
_CASES = [
    # (accession, family, status, detail, full_chars, first 200 normalised characters)
    (
        "0000320193-25-000079",  # AAPL 10-K
        "10-K",
        "extracted",
        None,
        18_015,
        "Item 7. Management’s Discussion and Analysis of Financial Condition and Results of Operations\n\nThe "
        "following discussion should be read in conjunction with the consolidated financial statements and acc",
    ),
    (
        "0001326380-26-000055",  # GME 10-Q
        "10-Q",
        "extracted",
        None,
        38_456,
        "Table of Contents\n\nITEM 2. MANAGEMENT'S DISCUSSION AND ANALYSIS OF FINANCIAL CONDITION AND RESULTS OF "
        "OPERATIONS\n\nThe following discussion should be read in conjunction with the information contained ",
    ),
    (
        "0001104659-26-092044",  # CVSA 10-K: the legacy-parser fallback path (no new-parser Part II Item 7)
        "10-K",
        "extracted",
        None,
        57_985,
        "Item 7. Management’s Discussion and Analysis of Financial Condition and Results of Operations\nThis "
        "management’s discussion and analysis of financial condition and results of operations (“MD&A”) should",
    ),
    (
        "0000944695-26-000014",  # THG 10-Q: real MD&A opening with its own section index (TOC-shaped)
        "10-Q",
        "extracted",
        None,
        79_118,
        "MANAGEMENT’S DISCUSSION AND ANALYSIS OF FINANCIAL CONDITION AND RESULTS OF OPERATIONS \n\nTABLE OF "
        "CONTENTS \n\n Introduction\n\n 28\n\n Executive Overview\n\n 28\n\n Description of Segments\n\n 29\n\n "
        "Results of Ope",
    ),
    # ENS 10-Q: the accessor returns a financial-statement note, not MD&A.
    ("0001628280-26-056208", "10-Q", "parse_failed", "caption_absent", None, None),
    (
        "0001628280-26-054343",  # JPM 10-Q: statements follow MD&A with no Item 1 heading — an end overrun (§6)
        "10-Q",
        "extracted",
        None,
        601_000,
        "INTRODUCTION\n\nThe following is Management’s discussion and analysis of the financial condition and "
        "results of operations (“MD&A”) of JPMorgan Chase & Co. (“JPMorganChase” or the “Firm”) for the second",
    ),
]


@pytest.mark.parametrize(("accession", "family", "status", "detail", "full_chars", "head"), _CASES)
def test_real_documents(
    accession: str, family: str, status: str, detail: str | None, full_chars: int | None, head: str | None
) -> None:
    with gzip.open(_FIXTURES / f"{accession}.htm.gz", "rt", newline="") as fh:
        html = fh.read()
    out = extract_mdna(family, html, accession, "https://www.sec.gov/Archives/edgar/data/1/2/doc.htm")
    assert (out.status, out.detail, out.full_chars) == (status, detail, full_chars)
    if out.body is not None:
        assert out.full_chars == len(normalise_mdna(out.body))
        assert normalise_mdna(out.body)[:_HEAD_CHARS] == head
