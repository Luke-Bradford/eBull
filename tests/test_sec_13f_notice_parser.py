"""Pure-logic tests for the 13F-NT (Notice) primary_doc parser (#1639).

A Notice has no holdings table — only the filer header + period of report.
``parse_notice_primary_doc`` reads exactly those two fields, reusing the same
namespace-stripping walk + CIK zero-pad as the HR path so the captured
``filer_cik`` joins to ``ownership_institutions_current.filer_cik`` (which is
itself derived from the HR header ``<cik>``).

The fixtures mirror the real Vanguard NT (acc 0000102909-26-002707, verified
2026-06-15): a namespaced ``edgarSubmission`` with the filer CIK in the header
followed by an ``otherManagers`` list of OTHER CIKs — the parser must pick the
header filer CIK, never a sub-manager."""

from __future__ import annotations

from datetime import date

import pytest

from app.providers.implementations.sec_13f import (
    ThirteenFNoticeInfo,
    parse_notice_primary_doc,
)

_NS = "http://www.sec.gov/edgar/thirteenffiler"


def _notice_xml(
    *,
    filer_cik: str = "0000102909",
    period: str = "03-31-2026",
    other_manager_ciks: tuple[str, ...] = ("0002100119", "0002100121"),
    include_period: bool = True,
) -> str:
    """Build a 13F-NT primary_doc.xml mirroring the real SEC shape: namespaced,
    header filer CIK first, then an otherManagers block of OTHER CIKs."""
    period_el = f"<periodOfReport>{period}</periodOfReport>" if include_period else ""
    others = "".join(f"<otherManager><cik>{cik}</cik></otherManager>" for cik in other_manager_ciks)
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="{_NS}">
  <headerData>
    <submissionType>13F-NT</submissionType>
    <filerInfo>
      <filer><credentials><cik>{filer_cik}</cik></credentials></filer>
    </filerInfo>
  </headerData>
  <formData>
    <coverPage>
      {period_el}
      <reportCalendarOrQuarter>{period}</reportCalendarOrQuarter>
    </coverPage>
    <documentInfo>
      <otherIncludedManagersCount>{len(other_manager_ciks)}</otherIncludedManagersCount>
      <otherManagers2Info>{others}</otherManagers2Info>
    </documentInfo>
  </formData>
</edgarSubmission>"""


def test_parses_filer_cik_and_period() -> None:
    notice = parse_notice_primary_doc(_notice_xml())
    assert notice == ThirteenFNoticeInfo(cik="0000102909", period_of_report=date(2026, 3, 31))


def test_picks_header_filer_cik_not_a_sub_manager() -> None:
    """The header filer CIK comes first in document order; the otherManagers
    CIKs that follow must NOT be returned."""
    notice = parse_notice_primary_doc(_notice_xml(filer_cik="0000102909", other_manager_ciks=("0002100119",)))
    assert notice.cik == "0000102909"


def test_period_is_mm_dd_yyyy() -> None:
    notice = parse_notice_primary_doc(_notice_xml(period="12-31-2025"))
    assert notice.period_of_report == date(2025, 12, 31)


def test_zero_pads_short_header_cik() -> None:
    notice = parse_notice_primary_doc(_notice_xml(filer_cik="102909"))
    assert notice.cik == "0000102909"


def test_nt_a_amendment_parses_same_as_nt() -> None:
    """13F-NT/A bodies are structurally identical to 13F-NT — the form
    distinction is in the submission header, not the parsed fields."""
    notice = parse_notice_primary_doc(_notice_xml(period="09-30-2025"))
    assert notice.period_of_report == date(2025, 9, 30)


def test_missing_period_raises() -> None:
    with pytest.raises(ValueError, match="periodOfReport"):
        parse_notice_primary_doc(_notice_xml(include_period=False))


def test_missing_cik_raises() -> None:
    xml = (
        f'<edgarSubmission xmlns="{_NS}"><formData><coverPage>'
        "<periodOfReport>03-31-2026</periodOfReport></coverPage></formData></edgarSubmission>"
    )
    with pytest.raises(ValueError, match="cik"):
        parse_notice_primary_doc(xml)
