"""13F Column 6/7 and List of Other Included Managers capture (#3227, tier (IM)).

Spec: ``docs/specs/ownership/2026-10-22-3227-13f-included-managers.md`` § Change 1-2.

Form 13F Special Instructions 7.b, 11.b.vi and 11.b.vii: a ``DFND``/``OTR`` line names, in Column 7, the number the
List of Other Included Managers assigns to each included Manager it is held for. This module is the single source for:

* the sequence-number grammar shared by Column 7 tokens and list entries;
* the capture status of an accession against the summary page's controls (Special Instructions 6.a-6.c);
* the ``thirteen_f_segments`` payload written on each observation;
* the capture record and list writes, and the upsert rule for writers that cannot capture.

Every 13F writer imports from here: the per-filing writers (legacy, manifest, rewash), the bulk data-set path and the
capture-only backfill. The bulk path aggregates segments in SQL; ``tests/test_thirteen_f_capture.py`` pins that the
SQL and :func:`segments_payload` agree on the same lines.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final, Literal
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb

from app.providers.implementations.sec_13f import Segment
from app.providers.implementations.sec_13f import ThirteenFOtherManager as OtherManagerEntry

CaptureSource = Literal["dataset", "xml"]
CaptureStatus = Literal["complete", "incomplete", "invalid"]

# § Change 1: ASCII digits only, 1-9 of them, value >= 1. ``str.isdigit`` would admit non-ASCII digits, and ``int()``
# would admit ``+1``, ``1_0`` and surrounding whitespace, so neither is used.
_SEQUENCE_RE = re.compile(r"[0-9]{1,9}")
_COLUMN7_SPLIT_RE = re.compile(r"[,\s]+")


def parse_sequence_number(raw: str | None) -> int | None:
    """One sequence number under § Change 1's grammar, or None when it breaks it."""
    if raw is None or _SEQUENCE_RE.fullmatch(raw) is None:
        return None
    value = int(raw)
    return value if value >= 1 else None


def parse_column7(raw: str | None) -> tuple[int, ...] | None:
    """Column 7 of one line: the sorted, distinct sequence numbers it names.

    An empty Column 7 names no one and is ``()``. Any token outside the grammar (``0``, ``NONE``, a name) makes the
    whole column invalid, ``None``: the filer's null markers and free-text names identify no Manager (§ Rule 5).
    """
    text = (raw or "").strip()
    if not text:
        return ()
    numbers: set[int] = set()
    for token in _COLUMN7_SPLIT_RE.split(text):
        number = parse_sequence_number(token)
        if number is None:
            return None
        numbers.add(number)
    return tuple(sorted(numbers))


def normalise_discretion(raw: str | None) -> str | None:
    """Column 6 as filed, stripped; blank is None."""
    text = (raw or "").strip()
    return text or None


def other_managers_sort_key(other_managers: tuple[int, ...] | None) -> str:
    """Text sort key for a segment's Column 7. Mirrored in SQL as ``om_sort`` (``COLLATE "C"``): an invalid column
    sorts last because ``~`` follows the digits and the comma."""
    if other_managers is None:
        return "~"
    return ",".join(str(n) for n in other_managers)


def canonical_shares(value: Decimal) -> str:
    """Shares as plain text without trailing zeros; the SQL side is ``trim_scale(x)::text``."""
    text = format(value.normalize(), "f")
    return "0" if text in ("-0", "") else text


def merge_segments(segments: Iterable[Segment]) -> tuple[Segment, ...]:
    """Sum segments by ``(Column 6, Column 7)`` and sort them as the payload stores them."""
    totals: dict[tuple[str | None, tuple[int, ...] | None], Decimal] = {}
    for discretion, other_managers, shares in segments:
        key = (discretion, other_managers)
        totals[key] = totals.get(key, Decimal(0)) + shares
    ordered = sorted(totals.items(), key=lambda item: (item[0][0] or "", other_managers_sort_key(item[0][1])))
    return tuple((discretion, other_managers, shares) for (discretion, other_managers), shares in ordered)


def segments_payload(capture_id: UUID, cusips: Iterable[str], segments: Iterable[Segment]) -> dict[str, Any]:
    """The ``thirteen_f_segments`` JSONB for one observation (§ Change 1)."""
    return {
        "capture_id": str(capture_id),
        "cusips": sorted(set(cusips)),
        "lines": [
            {
                "discretion": discretion,
                "other_managers": list(other_managers) if other_managers is not None else None,
                "shares": canonical_shares(shares),
            }
            for discretion, other_managers, shares in merge_segments(segments)
        ],
    }


def _strip_or_none(value: str | None) -> str | None:
    text = (value or "").strip()
    return text or None


def capture_status(
    entries: Sequence[OtherManagerEntry],
    *,
    expected_list_count: int | None,
    line_count: int,
    expected_line_count: int | None,
    value_total: Decimal,
    expected_value_total: Decimal | None,
) -> CaptureStatus:
    """§ Change 1 status against Special Instructions 6.a (list count), 6.b (line count) and 6.c (value total, within
    half a unit per line because Special Instruction 8 rounds each Column 4 entry to the nearest unit)."""
    numbers = [parse_sequence_number(e.sequence_number_raw) for e in entries]
    if any(n is None for n in numbers) or len(set(numbers)) != len(numbers):
        return "invalid"
    if expected_list_count is None or expected_list_count != len(entries):
        return "incomplete"
    if expected_line_count is None or expected_line_count != line_count:
        return "incomplete"
    if expected_value_total is None or abs(value_total - expected_value_total) > Decimal("0.5") * line_count:
        return "incomplete"
    return "complete"


_DELETE_ENTRIES_SQL = "DELETE FROM institutional_13f_other_managers WHERE accession_number = ANY(%s)"

_INSERT_ENTRY_SQL = """
INSERT INTO institutional_13f_other_managers (
    accession_number, entry_index, sequence_number_raw, sequence_number, cik, name, form13f_file_number
) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

_UPSERT_CAPTURE_SQL = """
INSERT INTO institutional_13f_captures (
    accession_number, capture_id, source, status, list_count, expected_list_count,
    line_count, expected_line_count, value_total, expected_value_total, captured_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, clock_timestamp())
ON CONFLICT (accession_number) DO UPDATE SET
    capture_id = EXCLUDED.capture_id,
    source = EXCLUDED.source,
    status = EXCLUDED.status,
    list_count = EXCLUDED.list_count,
    expected_list_count = EXCLUDED.expected_list_count,
    line_count = EXCLUDED.line_count,
    expected_line_count = EXCLUDED.expected_line_count,
    value_total = EXCLUDED.value_total,
    expected_value_total = EXCLUDED.expected_value_total,
    captured_at = EXCLUDED.captured_at
"""


@dataclass(frozen=True)
class AccessionCapture:
    """Everything a capture of one accession writes, apart from the observations' segments."""

    accession_number: str
    source: CaptureSource
    entries: tuple[OtherManagerEntry, ...]
    expected_list_count: int | None
    line_count: int
    expected_line_count: int | None
    value_total: Decimal
    expected_value_total: Decimal | None
    capture_id: UUID

    @property
    def status(self) -> CaptureStatus:
        return capture_status(
            self.entries,
            expected_list_count=self.expected_list_count,
            line_count=self.line_count,
            expected_line_count=self.expected_line_count,
            value_total=self.value_total,
            expected_value_total=self.expected_value_total,
        )


def new_capture(
    *,
    accession_number: str,
    source: CaptureSource,
    entries: Iterable[OtherManagerEntry],
    expected_list_count: int | None,
    line_count: int,
    expected_line_count: int | None,
    value_total: Decimal,
    expected_value_total: Decimal | None,
) -> AccessionCapture:
    return AccessionCapture(
        accession_number=accession_number,
        source=source,
        entries=tuple(entries),
        expected_list_count=expected_list_count,
        line_count=line_count,
        expected_line_count=expected_line_count,
        value_total=value_total,
        expected_value_total=expected_value_total,
        capture_id=uuid4(),
    )


def write_captures(conn: psycopg.Connection[Any], captures: Sequence[AccessionCapture]) -> None:
    """Replace each accession's capture record and list entries (§ Change 1 "Replacement").

    Runs in the caller's transaction, which also writes the observations' segments with the same ``capture_id``.
    """
    if not captures:
        return
    with conn.cursor() as cur:
        cur.execute(_DELETE_ENTRIES_SQL, ([c.accession_number for c in captures],))
        cur.executemany(
            _INSERT_ENTRY_SQL,
            [
                (
                    c.accession_number,
                    index,
                    e.sequence_number_raw,
                    parse_sequence_number(e.sequence_number_raw),
                    _strip_or_none(e.cik),
                    e.name,
                    _strip_or_none(e.form13f_file_number),
                )
                for c in captures
                for index, e in enumerate(c.entries)
            ],
        )
        cur.executemany(
            _UPSERT_CAPTURE_SQL,
            [
                (
                    c.accession_number,
                    c.capture_id,
                    c.source,
                    c.status,
                    len(c.entries),
                    c.expected_list_count,
                    c.line_count,
                    c.expected_line_count,
                    c.value_total,
                    c.expected_value_total,
                )
                for c in captures
            ],
        )


def recapture_from_stored(
    conn: psycopg.Connection[Any],
    *,
    accession_number: str,
    line_count: int,
    value_total: Decimal,
) -> AccessionCapture | None:
    """A capture for a writer that has the information table but not the primary document (the CUSIP rewash: primary
    documents are swept, ``raw_filings.SWEPT_DOCUMENT_KINDS``).

    The list and the summary-page controls come from the stored capture record; the line count and value total come
    from this parse. Returns None when the accession was never captured, and the writer then writes NULL segments.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT expected_list_count, expected_line_count, expected_value_total "
            "FROM institutional_13f_captures WHERE accession_number = %s",
            (accession_number,),
        )
        record = cur.fetchone()
        if record is None:
            return None
        cur.execute(
            "SELECT sequence_number_raw, cik, name, form13f_file_number FROM institutional_13f_other_managers "
            "WHERE accession_number = %s ORDER BY entry_index",
            (accession_number,),
        )
        entries = [OtherManagerEntry(*row) for row in cur.fetchall()]
    return new_capture(
        accession_number=accession_number,
        source="xml",
        entries=entries,
        expected_list_count=record[0],
        line_count=line_count,
        expected_line_count=record[1],
        value_total=value_total,
        expected_value_total=record[2],
    )


def segments_jsonb(payload: dict[str, Any] | None) -> Jsonb | None:
    return Jsonb(payload) if payload is not None else None


# § Change 1 "Upsert", shared by ``record_institution_observation`` and the bulk drain. A capture-capable write carries
# segments and replaces them. A write that cannot capture (the weekly ``sync_institutions``; the bulk path from an
# archive without ``OTHERMANAGER2.tsv``) carries NULL and keeps the stored segments only when its shares are equal;
# the bulk path first clears segments whose CUSIP set differs (``sec_13f_dataset_ingest``). The read's
# self-consistency check refuses any remaining mismatch.
SEGMENTS_ON_CONFLICT_SQL: Final = """thirteen_f_segments = CASE
        WHEN EXCLUDED.thirteen_f_segments IS NOT NULL THEN EXCLUDED.thirteen_f_segments
        WHEN ownership_institutions_observations.shares = EXCLUDED.shares
            THEN ownership_institutions_observations.thirteen_f_segments
    END"""
