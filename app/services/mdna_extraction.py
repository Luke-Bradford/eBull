"""MD&A extraction for ``periodic_report_sections`` (#3518) — the HASHED module.

Spec: ``docs/proposals/etl/2026-09-30-3518-periodic-report-sections.md`` §4.3. Everything that decides what text
a row carries lives here and nowhere else: the accessor call, the stub it is fed, the fund-v1 §4 normalisation,
the caption rule and the parse outcome mapping. ``tests/test_3518_mdna_extraction.py`` pins the sha256 of this
file's bytes against ``MDNA_EXTRACTOR_REVISION``, so any edit without a revision bump fails the fast tier (the
#3471 ``POLICY_MODULES`` pattern). A new revision is a new extractor id, and a new id makes every target due again.

⚠ fund-v1 freezes the extractor id it reads (#3515 slice 3 must refuse an edgartools pin change or a revision bump
while a fund-v1 declaration is non-terminal). Do not bump casually.

Source rule (spec §2): 10-K Item 7 and 10-Q Part I Item 2 are both MD&A under Reg S-K Item 303; Exchange Act
Rule 12b-13 keeps the item caption in the filing even when the text is incorporated or omitted, which is what the
caption rule leans on. The caption WINDOW and its variants are by construction, measured in spec §8.3.

This module never touches the network or the database. It runs inside the parse worker's child process
(``app/services/mdna_parse_worker.py``), whose DNS and socket connects raise.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from importlib.metadata import version
from typing import Any, Final, Literal

MDNA_EXTRACTOR_REVISION: Final = 1

#: Base form per original periodic form. Transition reports (Rules 13a-10 / 15d-10) are parsed as their base.
FORM_FAMILY: Final[dict[str, str]] = {"10-K": "10-K", "10-KT": "10-K", "10-Q": "10-Q", "10-QT": "10-Q"}
SECTION_ID: Final[dict[str, str]] = {"10-K": "10-K:Item 7", "10-Q": "10-Q:Part I, Item 2"}

# --- fund-v1 §4 normalisation, in its stated order (moved verbatim from the #3518 census) ---------------------
_VERTICAL = "\n\r\x0b\x0c\x1c\x1d\x1e\x1f\x85  "  # the census carries the last two as literals
_HSPACE = re.compile(r"[^\S" + re.escape(_VERTICAL) + r"]")
_SPACES = re.compile(r" {2,}")
_NL3 = re.compile(r"\n{3,}")


def normalise_mdna(text: str) -> str:
    """fund-v1 §4: horizontal whitespace (Unicode whitespace other than line separators) -> one space each; strip
    remaining control characters (category Cc) except newline; collapse runs of spaces; collapse three or more
    newlines to two; trim. ``full_chars`` is the length of the result. fund-v1 slice 2 imports this."""
    t = _HSPACE.sub(" ", text)
    t = "".join(ch for ch in t if ch == "\n" or unicodedata.category(ch) != "Cc")
    t = _SPACES.sub(" ", t)
    t = _NL3.sub("\n\n", t)
    return t.strip()


# --- caption rule (spec §4.3; Rule 12b-13 item captions), verbatim from the census ------------------------------
CAPTION: Final = re.compile(
    r"discussion\s*(?:and|&(?:amp;)?)\s*analysis|financial\s+condition\s*(?:and|&(?:amp;)?)\s*results\s+of\s+operations",
    re.IGNORECASE,
)
CAPTION_WINDOW: Final = 1_000


def caption_present(normalised: str) -> bool:
    """A caption phrase within the first ``CAPTION_WINDOW`` characters after collapsing ALL whitespace to one space.
    Establishes only that the phrase occurs there — not that it is a heading, the right item, or the end."""
    return bool(CAPTION.search(re.sub(r"\s+", " ", normalised)[:CAPTION_WINDOW]))


def extractor_id() -> str:
    return f"edgartools=={version('edgartools')}/mdna-{MDNA_EXTRACTOR_REVISION}"


# --- outcome -------------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class ParseOutcome:
    """One parse result. ``body`` is the accessor text AS RETURNED (the reader normalises); ``full_chars`` is
    ``len(normalise_mdna(body))``. ``detail`` is NULL exactly when extracted (sql/441)."""

    status: Literal["extracted", "item_absent", "parse_failed"]
    body: str | None = None
    full_chars: int | None = None
    detail: str | None = None
    retryable: bool = False


def classify_text(raw: str | None) -> ParseOutcome:
    """Map the accessor's return value to an outcome (spec §4.3)."""
    if raw is None:
        return ParseOutcome("item_absent", detail="none")
    normalised = normalise_mdna(raw)
    if not normalised:
        # Whitespace only, or nothing left once control characters are stripped (fund-v1's
        # `empty_after_normalisation`): either way there is no text, and `full_chars > 0` would refuse it.
        return ParseOutcome("item_absent", detail="blank")
    if not caption_present(normalised):
        return ParseOutcome("parse_failed", detail="caption_absent")
    return ParseOutcome("extracted", body=raw, full_chars=len(normalised))


class _Stub:
    """The filing object ``TenK`` / ``TenQ`` read: ``form``, ``html()``, ``accession_number`` and (legacy fallback)
    ``base_dir``, a string prefix for image sources that is never fetched (spec §8.2)."""

    def __init__(self, form: str, html: str, accession: str, source_url: str) -> None:
        self.form = form
        self._html = html
        self.accession_number = accession
        self.base_dir = source_url.rsplit("/", 1)[0]

    def html(self) -> str:
        return self._html


def _accessor_text(family: str, html: str, accession: str, source_url: str) -> Any:
    from edgar.company_reports.ten_k import TenK
    from edgar.company_reports.ten_q import TenQ

    stub = _Stub(family, html, accession, source_url)
    if family == "10-K":
        return TenK(stub)["Item 7"]
    return TenQ(stub).get_item_with_part("Part I", "Item 2", markdown=False)


def extract_mdna(family: str, html: str, accession: str, source_url: str) -> ParseOutcome:
    """Run the public edgartools item accessor on ``html`` and classify the result.

    ``MemoryError`` is retryable (``detail='memory'``); any other exception inside the accessor is deterministic
    for the same text and extractor, so it is terminal. Timeout and child death are the worker's to map.
    """
    if family not in SECTION_ID:
        raise ValueError(f"unknown form family {family!r}")
    try:
        raw = _accessor_text(family, html, accession, source_url)
    except MemoryError:
        return ParseOutcome("parse_failed", detail="memory", retryable=True)
    except Exception as exc:  # noqa: BLE001 — every accessor failure is a recorded outcome (spec §4.3)
        return ParseOutcome("parse_failed", detail=f"{type(exc).__name__}: {str(exc)[:200]}")
    if raw is not None and not isinstance(raw, str):
        return ParseOutcome("parse_failed", detail=f"TypeError: accessor returned {type(raw).__name__}")
    return classify_text(raw)
