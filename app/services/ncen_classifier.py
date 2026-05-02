"""SEC N-CEN annual fund-census ingester + filer-type classifier (#782).

Walks each curated 13F filer in ``institutional_filer_seeds``,
discovers their latest N-CEN annual filing on SEC EDGAR, parses
``investmentCompanyType`` from the primary doc, and stamps the
derived filer-type onto a dedicated
``ncen_filer_classifications`` row. The 13F-HR ingester
(:mod:`app.services.institutional_holdings`) reads this table via
:func:`compose_filer_type` to apply the classification in priority
order (curated ETF list > N-CEN > default).

N-CEN is the SEC's annual census filing for registered investment
companies. The ``<investmentCompanyType>`` field carries one of
six statutory codes:

  * ``N-1A`` — open-end management company (most mutual funds
    and ETFs). Maps to ``INV`` by default; the curated ETF seed
    list (#742) overrides when the CIK is on the ETF list.
  * ``N-2`` — closed-end fund or business development company.
    Maps to ``INV``.
  * ``N-3`` / ``N-4`` / ``N-6`` — variable insurance contracts
    (separate accounts of insurance companies). Map to ``INS``.
  * ``N-5`` — small business investment company. Maps to ``INV``.

Broker-dealer (``BD``) classification is NOT addressable from
N-CEN — broker-dealers file Form ADV / FOCUS reports instead.
That's a separate ticket; v1 stays at the N-CEN-derivable subset.

Idempotent: re-running on the same accession UPSERTs in place;
re-running with a newer accession promotes via UPSERT and
refreshes ``fetched_at``. The downstream 13F-HR ingester reads
this table fresh on every call so a re-classification flows
through to ``institutional_filers.filer_type`` on the next ingest
cycle without a backfill migration.
"""

from __future__ import annotations

import json
import logging
import xml.etree.ElementTree as ET  # noqa: S405 — only used to catch ET.ParseError; SEC EDGAR is the trusted source.
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Final, Literal, Protocol

import psycopg
import psycopg.rows

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Provider contract
# ---------------------------------------------------------------------------


class SecDocFetcher(Protocol):
    """Subset of the SEC EDGAR provider this classifier relies on.

    Same contract as the institutional / blockholder / DEF 14A
    services — the production binding is :class:`app.providers.
    implementations.sec_edgar.SecEdgarProvider`.
    """

    def fetch_document_text(self, absolute_url: str) -> str | None: ...


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


FilerType = Literal["ETF", "INV", "INS", "BD", "OTHER"]


@dataclass(frozen=True)
class NCenClassification:
    """One parsed N-CEN classification."""

    cik: str
    investment_company_type: str  # raw SEC code: 'N-1A' / 'N-2' / 'N-3' / etc.
    derived_filer_type: FilerType
    accession_number: str
    filed_at: datetime


@dataclass(frozen=True)
class ClassifyReport:
    """Per-batch rollup of one classifier pass.

    Counter semantics:

      * ``classifications_written`` — N-CEN parsed and persisted.
      * ``no_ncen_found`` — submissions JSON valid; no N-CEN in the
        recent-filings array (legitimate absence).
      * ``fetch_failures`` — upstream HTTP / JSON parse failure
        (submissions 404, malformed body, primary_doc 404).
      * ``parse_failures`` — primary_doc fetched but XML parse /
        validation failed.
      * ``crash_failures`` — unexpected per-filer exception during
        the classify + upsert + commit block (DB error, network
        timeout escape, etc.). The batch loop catches these so a
        single bad filer doesn't abort the rest.
    """

    filers_seen: int
    classifications_written: int
    no_ncen_found: int
    parse_failures: int
    fetch_failures: int
    crash_failures: int = 0


# ---------------------------------------------------------------------------
# Investment-company-type → filer-type mapping
# ---------------------------------------------------------------------------


_INVESTMENT_COMPANY_TYPE_MAP: Final[dict[str, FilerType]] = {
    # Open-end management company (mutual fund or ETF). Defaults to
    # INV; the curated ETF seed list overrides for known ETF CIKs
    # via the compose function downstream.
    "N-1A": "INV",
    # Closed-end fund / business development company.
    "N-2": "INV",
    # Variable insurance separate accounts.
    "N-3": "INS",
    "N-4": "INS",
    "N-6": "INS",
    # Small business investment company.
    "N-5": "INV",
}


def _derive_filer_type(investment_company_type: str) -> FilerType:
    """Map a raw SEC investment-company-type code to our filer-type
    enum. Unknown codes default to ``OTHER`` so a future SEC enum
    addition surfaces in the data without breaking the classifier.
    """
    return _INVESTMENT_COMPANY_TYPE_MAP.get(investment_company_type.strip(), "OTHER")


# ---------------------------------------------------------------------------
# Pure parser
# ---------------------------------------------------------------------------


def _strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _walk_text(root: ET.Element, name: str) -> str | None:
    for el in root.iter():
        if _strip_ns(el.tag) == name and el.text is not None:
            text = el.text.strip()
            if text:
                return text
    return None


def parse_ncen_primary_doc(xml: str) -> str:
    """Extract ``investmentCompanyType`` from an N-CEN
    ``primary_doc.xml``. Raises ``ValueError`` when the field is
    absent — the caller treats that as a parse failure and
    skips the filer for this run.
    """
    root = ET.fromstring(xml)  # noqa: S314 — SEC EDGAR is the trusted source.
    code = _walk_text(root, "investmentCompanyType")
    if code is None:
        raise ValueError("N-CEN primary_doc.xml is missing <investmentCompanyType>")
    return code


# ---------------------------------------------------------------------------
# URL builders
# ---------------------------------------------------------------------------


_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn_no_dashes}/{filename}"


def _zero_pad_cik(cik: str | int) -> str:
    return str(int(str(cik).strip())).zfill(10)


def _accession_no_dashes(accession_number: str) -> str:
    return accession_number.replace("-", "")


def _submissions_url(cik: str) -> str:
    return _SUBMISSIONS_URL.format(cik=_zero_pad_cik(cik))


def _archive_file_url(cik: str, accession_number: str, filename: str) -> str:
    return _ARCHIVE_URL.format(
        cik_int=int(_zero_pad_cik(cik)),
        accn_no_dashes=_accession_no_dashes(accession_number),
        filename=filename,
    )


# ---------------------------------------------------------------------------
# Submissions index walker
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _NCenAccessionRef:
    accession_number: str
    filed_at: datetime


def _safe_iso_date(text: str | None) -> date | None:
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _safe_iso_datetime(text: str | None) -> datetime | None:
    parsed = _safe_iso_date(text)
    if parsed is None:
        return None
    return datetime(parsed.year, parsed.month, parsed.day, tzinfo=UTC)


def _find_latest_ncen(payload: str) -> _NCenAccessionRef | None:
    """Walk ``data.sec.gov/submissions/CIK{cik}.json`` and return
    the most recent N-CEN accession.

    Returns ``None`` when the payload is malformed OR when no
    N-CEN appears in the recent-filings array. The caller treats
    both cases identically — the filer simply has no N-CEN-derived
    classification on this pass.
    """
    try:
        data: dict[str, Any] = json.loads(payload)
    except json.JSONDecodeError:
        logger.warning("N-CEN classifier: submissions JSON is not valid JSON")
        return None

    filings = data.get("filings", {})
    recent = filings.get("recent", {})
    accessions = recent.get("accessionNumber", [])
    forms = recent.get("form", [])
    filing_dates = recent.get("filingDate", [])

    # The submissions index orders ``recent`` newest-first, so the
    # first N-CEN match is the latest. Defensive: also tolerate
    # ``N-CEN/A`` amendments — same form family.
    for i, accession in enumerate(accessions):
        if i >= len(forms):
            break
        form = str(forms[i]).strip()
        if form not in ("N-CEN", "N-CEN/A"):
            continue
        filed_at = _safe_iso_datetime(filing_dates[i] if i < len(filing_dates) else "")
        if filed_at is None:
            continue
        return _NCenAccessionRef(
            accession_number=str(accession),
            filed_at=filed_at,
        )
    return None


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _list_active_filer_seeds(conn: psycopg.Connection[tuple]) -> list[str]:
    """Same active-seeds query as the 13F-HR ingester. Inlined here
    so :mod:`app.services.institutional_holdings` doesn't need to
    expose its private ``_list_active_filer_seeds`` helper to a
    second consumer."""
    cur = conn.execute("SELECT cik FROM institutional_filer_seeds WHERE active = TRUE ORDER BY cik")
    return [_zero_pad_cik(row[0]) for row in cur.fetchall()]


def _upsert_classification(
    conn: psycopg.Connection[tuple],
    classification: NCenClassification,
) -> None:
    """Idempotent upsert into ``ncen_filer_classifications``."""
    conn.execute(
        """
        INSERT INTO ncen_filer_classifications (
            cik, investment_company_type, derived_filer_type,
            accession_number, filed_at
        ) VALUES (%(cik)s, %(ict)s, %(ft)s, %(accession)s, %(filed_at)s)
        ON CONFLICT (cik) DO UPDATE SET
            investment_company_type = EXCLUDED.investment_company_type,
            derived_filer_type = EXCLUDED.derived_filer_type,
            accession_number = EXCLUDED.accession_number,
            filed_at = EXCLUDED.filed_at,
            fetched_at = NOW()
        """,
        {
            "cik": classification.cik,
            "ict": classification.investment_company_type,
            "ft": classification.derived_filer_type,
            "accession": classification.accession_number,
            "filed_at": classification.filed_at,
        },
    )


# ---------------------------------------------------------------------------
# Per-filer driver
# ---------------------------------------------------------------------------


_OutcomeKind = Literal["classified", "no_ncen", "fetch_failed", "parse_failed"]


@dataclass(frozen=True)
class _FilerOutcome:
    """Per-filer outcome with a structured ``kind`` so the batch
    loop can route counters correctly without parsing error
    strings. Codex pre-push review caught the prior version that
    keyed on ``"fetch" in error`` substring matching, which
    incorrectly bucketed JSON-404 outcomes as parse failures.
    """

    kind: _OutcomeKind
    classification: NCenClassification | None
    error: str | None


def _classify_single_filer(
    sec: SecDocFetcher,
    *,
    cik: str,
) -> _FilerOutcome:
    """Per-filer driver. Never raises — returns an outcome record
    so the batch loop can continue past failures."""
    submissions_payload = sec.fetch_document_text(_submissions_url(cik))
    if submissions_payload is None:
        return _FilerOutcome(
            kind="fetch_failed",
            classification=None,
            error="submissions JSON 404/error",
        )

    # Distinguish malformed JSON (real upstream failure) from
    # well-formed JSON containing no N-CEN entries (legitimate
    # absence). _find_latest_ncen previously collapsed both into
    # None — Codex pre-push review caught the lost signal.
    try:
        json.loads(submissions_payload)
    except json.JSONDecodeError:
        return _FilerOutcome(
            kind="fetch_failed",
            classification=None,
            error="submissions JSON malformed",
        )

    ref = _find_latest_ncen(submissions_payload)
    if ref is None:
        # JSON parsed cleanly but no N-CEN appears in the recent
        # filings array — common case for the curated 13F seed
        # list, many activist funds don't file N-CEN.
        return _FilerOutcome(kind="no_ncen", classification=None, error=None)

    primary_url = _archive_file_url(cik, ref.accession_number, "primary_doc.xml")
    primary_xml = sec.fetch_document_text(primary_url)
    if primary_xml is None:
        return _FilerOutcome(
            kind="fetch_failed",
            classification=None,
            error=f"primary_doc.xml 404 for accession {ref.accession_number}",
        )

    try:
        ict = parse_ncen_primary_doc(primary_xml)
    except (ValueError, ET.ParseError) as exc:
        return _FilerOutcome(
            kind="parse_failed",
            classification=None,
            error=f"parse failed: {exc}",
        )

    return _FilerOutcome(
        kind="classified",
        classification=NCenClassification(
            cik=cik,
            investment_company_type=ict,
            derived_filer_type=_derive_filer_type(ict),
            accession_number=ref.accession_number,
            filed_at=ref.filed_at,
        ),
        error=None,
    )


# ---------------------------------------------------------------------------
# Public batch entry point
# ---------------------------------------------------------------------------


def classify_filers_via_ncen(
    conn: psycopg.Connection[tuple],
    sec: SecDocFetcher,
    *,
    ciks: list[str] | None = None,
) -> ClassifyReport:
    """Walk active filer seeds (or an explicit ``ciks`` list) and
    classify each via N-CEN.

    Commits per-filer so a mid-batch crash leaves a partial
    persistent state (classifications already written stay; later
    filers are simply not yet processed).
    """
    if ciks is None:
        ciks = _list_active_filer_seeds(conn)
    if not ciks:
        logger.info("N-CEN classifier: no active filer seeds; nothing to do")
        return ClassifyReport(
            filers_seen=0,
            classifications_written=0,
            no_ncen_found=0,
            parse_failures=0,
            fetch_failures=0,
        )

    classifications_written = 0
    no_ncen_found = 0
    parse_failures = 0
    fetch_failures = 0
    crash_failures = 0

    for cik in ciks:
        cik_padded = _zero_pad_cik(cik)
        # Per-filer crash isolation wraps the FULL block — classify
        # + upsert + commit. A DB error during the upsert or commit
        # must not abort the rest of the batch. Codex pre-push
        # review caught the prior version which had only
        # _classify_single_filer inside the try.
        try:
            outcome = _classify_single_filer(sec, cik=cik_padded)
            if outcome.classification is not None:
                _upsert_classification(conn, outcome.classification)
                conn.commit()
        except Exception:  # noqa: BLE001 — per-filer crash must not abort batch
            logger.exception("N-CEN classifier: filer %s raised; continuing batch", cik_padded)
            conn.rollback()
            crash_failures += 1
            continue

        # Counter routing keys on the structured outcome kind, not
        # error-string substring matching. Codex pre-push review
        # caught the prior 'fetch' substring check that bucketed
        # JSON-404 outcomes as parse failures.
        if outcome.kind == "classified":
            classifications_written += 1
        elif outcome.kind == "no_ncen":
            no_ncen_found += 1
        elif outcome.kind == "fetch_failed":
            fetch_failures += 1
        else:  # 'parse_failed'
            parse_failures += 1

    return ClassifyReport(
        filers_seen=len(ciks),
        classifications_written=classifications_written,
        no_ncen_found=no_ncen_found,
        parse_failures=parse_failures,
        fetch_failures=fetch_failures,
        crash_failures=crash_failures,
    )


# ---------------------------------------------------------------------------
# Compose function — used by the 13F-HR ingester
# ---------------------------------------------------------------------------


def compose_filer_type(
    conn: psycopg.Connection[tuple],
    cik: str,
) -> FilerType:
    """Return the canonical filer-type for a CIK in priority order:

      1. Curated ETF seed list (#742) → ``ETF``.
      2. N-CEN classification → derived_filer_type.
      3. Default → ``INV``.

    Read by :func:`app.services.institutional_holdings.
    classify_filer_type` on every 13F-HR upsert so the latest
    N-CEN classification flows through automatically without a
    backfill migration. Read-only — never writes.
    """
    cik_padded = _zero_pad_cik(cik)

    # 1. ETF curated list — highest precedence.
    cur = conn.execute(
        "SELECT 1 FROM etf_filer_cik_seeds WHERE cik = %s AND active = TRUE LIMIT 1",
        (cik_padded,),
    )
    if cur.fetchone() is not None:
        return "ETF"

    # 2. N-CEN classification.
    cur = conn.execute(
        "SELECT derived_filer_type FROM ncen_filer_classifications WHERE cik = %s LIMIT 1",
        (cik_padded,),
    )
    row = cur.fetchone()
    if row is not None:
        return row[0]  # type: ignore[no-any-return]

    # 3. Default.
    return "INV"


# ---------------------------------------------------------------------------
# Reader (exposed for ad-hoc admin queries)
# ---------------------------------------------------------------------------


def iter_classifications(
    conn: psycopg.Connection[tuple],
    *,
    derived_filer_type: FilerType | None = None,
    limit: int = 100,
) -> Iterator[dict[str, Any]]:
    """Yield N-CEN classification rows in fetched_at-DESC order.

    ``derived_filer_type`` filter scopes to one bucket (e.g.
    ``"INS"`` to audit insurance-product mappings); ``None``
    returns all.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT cik, investment_company_type, derived_filer_type,
                   accession_number, filed_at, fetched_at
            FROM ncen_filer_classifications
            WHERE (%(ft)s::TEXT IS NULL OR derived_filer_type = %(ft)s::TEXT)
            ORDER BY fetched_at DESC, cik DESC
            LIMIT %(limit)s
            """,
            {"ft": derived_filer_type, "limit": limit},
        )
        for row in cur.fetchall():
            yield dict(row)
