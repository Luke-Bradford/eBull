# SEC Incremental Fetch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the 45-minute `daily_financial_facts` full-pull with a change-driven fetch driven by the SEC daily master-index and per-CIK watermarks, self-seeding on first sight.

**Architecture:** A new `sec_incremental` service owns a two-phase flow: a `plan_refresh` phase fetches a 7-day window of SEC daily master-index files (conditional-GET), intersects filings with our covered US cohort, and produces a plan of seeds + refreshes; an `execute_refresh` phase performs per-CIK XBRL pulls atomically with watermark advances. A thin `fetch_master_index` provider method stays pure HTTP. Existing XBRL upsert + normalization logic is reused without modification.

**Tech Stack:** Python 3.14, psycopg 3, httpx, pytest, pyright, `external_data_watermarks` (from #269), SEC EDGAR daily-index + `submissions.json` + `companyfacts.json` APIs.

**Design spec:** [docs/superpowers/specs/2026-04-17-sec-incremental-fetch-design.md](../specs/2026-04-17-sec-incremental-fetch-design.md)

**Issue:** #272 — `etl/sec: daily master-index watermark + per-CIK companyfacts only on new accession`

---

## File structure

| File | Role | Status |
|---|---|---|
| [app/providers/implementations/sec_edgar.py](../../app/providers/implementations/sec_edgar.py) | Add `fetch_master_index` conditional-GET method + `parse_master_index` pure function | Modify |
| [app/services/sec_incremental.py](../../app/services/sec_incremental.py) | New service module — `plan_refresh` + `execute_refresh` | Create |
| [app/services/financial_facts.py](../../app/services/financial_facts.py) | Expose `upsert_facts_for_instrument` (rename of `_upsert_facts`) for reuse by executor | Modify |
| [app/workers/scheduler.py](../../app/workers/scheduler.py) | Rewire `daily_financial_facts` to drive from the planner | Modify |
| tests/fixtures/sec/master_20260415.idx | Trimmed real master-index sample for unit tests | Create |
| tests/fixtures/sec/submissions_TEST.json | Trimmed submissions fixture | Create |
| tests/fixtures/sec/companyfacts_TEST.json | Trimmed companyfacts fixture | Create |
| tests/test_sec_master_index_parser.py | Pure-function parser tests | Create |
| tests/test_sec_provider_master_index.py | Provider conditional-GET tests | Create |
| tests/test_sec_incremental_planner.py | `plan_refresh` scenarios | Create |
| tests/test_sec_incremental_executor.py | `execute_refresh` scenarios | Create |
| tests/test_sync_orchestrator_financial_facts_incremental.py | End-to-end through scheduler entrypoint | Create |

---

## Task 1: Master-index parser (pure function)

SEC's daily master-index is a plain-text file, header lines followed by a `----` separator and pipe-delimited rows:

```
Description:           Master Index of EDGAR Dissemination Feed
Last Data Received:    April 15, 2026
Comments:              webmaster@sec.gov
Anonymous FTP:         ftp://ftp.sec.gov/edgar/

CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
320193|APPLE INC|10-Q|2026-04-15|edgar/data/320193/0000320193-26-000042.txt
789019|MICROSOFT CORP|8-K|2026-04-15|edgar/data/789019/0000789019-26-000017.txt
```

A pure parser lets us unit-test without HTTP and keeps provider code thin.

**Files:**
- Create: `tests/fixtures/sec/master_20260415.idx`
- Modify: `app/providers/implementations/sec_edgar.py`
- Create: `tests/test_sec_master_index_parser.py`

- [ ] **Step 1: Create the fixture file**

```
Description:           Master Index of EDGAR Dissemination Feed
Last Data Received:    April 15, 2026
Comments:              webmaster@sec.gov
Anonymous FTP:         ftp://ftp.sec.gov/edgar/

CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
320193|APPLE INC|10-Q|2026-04-15|edgar/data/320193/0000320193-26-000042.txt
789019|MICROSOFT CORP|8-K|2026-04-15|edgar/data/789019/0000789019-26-000017.txt
1045810|NVIDIA CORP|10-K|2026-04-15|edgar/data/1045810/0001045810-26-000003.txt
0000999999|UNUSUAL ZEROPAD INC|4|2026-04-15|edgar/data/999999/0000999999-26-000001.txt
```

Write the file (save it with just the content above; do not prefix lines).

- [ ] **Step 2: Write the failing parser test**

Create `tests/test_sec_master_index_parser.py`:

```python
"""Unit tests for the SEC daily master-index parser."""

from __future__ import annotations

from pathlib import Path

from app.providers.implementations.sec_edgar import MasterIndexEntry, parse_master_index


FIXTURE = Path("tests/fixtures/sec/master_20260415.idx")


def test_parses_all_entries_from_fixture() -> None:
    body = FIXTURE.read_bytes()
    entries = parse_master_index(body)

    assert len(entries) == 4
    assert entries[0] == MasterIndexEntry(
        cik="0000320193",
        company_name="APPLE INC",
        form_type="10-Q",
        date_filed="2026-04-15",
        accession_number="0000320193-26-000042",
    )


def test_zero_pads_cik_regardless_of_input_width() -> None:
    body = FIXTURE.read_bytes()
    entries = parse_master_index(body)
    ciks = {e.cik for e in entries}
    assert "0000320193" in ciks
    assert "0000789019" in ciks
    assert "0001045810" in ciks
    assert "0000999999" in ciks


def test_extracts_accession_number_from_filename() -> None:
    body = FIXTURE.read_bytes()
    entries = parse_master_index(body)
    accessions = {e.accession_number for e in entries}
    assert "0000320193-26-000042" in accessions
    assert "0000789019-26-000017" in accessions
    assert "0001045810-26-000003" in accessions


def test_ignores_header_and_separator_lines() -> None:
    body = FIXTURE.read_bytes()
    entries = parse_master_index(body)
    form_types = [e.form_type for e in entries]
    assert "Form Type" not in form_types
    assert all("-" not in ft or ft in {"10-K", "10-Q", "10-K/A", "10-Q/A"} for ft in form_types)


def test_returns_empty_list_for_body_with_no_data_rows() -> None:
    body = b"Description: empty\n\nCIK|Company Name|Form Type|Date Filed|Filename\n-----\n"
    entries = parse_master_index(body)
    assert entries == []


def test_skips_malformed_rows_silently() -> None:
    body = (
        b"CIK|Company Name|Form Type|Date Filed|Filename\n"
        b"------\n"
        b"320193|APPLE INC|10-Q|2026-04-15|edgar/data/320193/0000320193-26-000042.txt\n"
        b"malformed row with no pipes\n"
        b"789019|MICROSOFT CORP|8-K|2026-04-15|edgar/data/789019/0000789019-26-000017.txt\n"
    )
    entries = parse_master_index(body)
    assert len(entries) == 2
```

- [ ] **Step 3: Run test — expect failure**

```bash
uv run pytest tests/test_sec_master_index_parser.py -v
```

Expected: `ImportError: cannot import name 'MasterIndexEntry'` (or similar).

- [ ] **Step 4: Implement parser**

Append to `app/providers/implementations/sec_edgar.py` (after the existing `CikMappingResult` dataclass):

```python
@dataclass(frozen=True)
class MasterIndexEntry:
    """One row from SEC's daily master-index file.

    - ``cik`` — 10-digit zero-padded CIK string.
    - ``accession_number`` — canonical dashed form like
      ``0000320193-26-000042``, extracted from ``Filename``.
    """

    cik: str
    company_name: str
    form_type: str
    date_filed: str
    accession_number: str


def parse_master_index(body: bytes) -> list[MasterIndexEntry]:
    """Parse SEC daily master-index bytes into entries.

    Format: header lines, a ``CIK|...|Filename`` column row, a dashed
    separator line, then pipe-delimited data rows. Malformed rows are
    skipped silently — the provider contract is best-effort parsing.
    """
    entries: list[MasterIndexEntry] = []
    text = body.decode("utf-8", errors="replace")
    in_data = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        # Data rows start after a line of dashes
        if set(line) == {"-"}:
            in_data = True
            continue
        if not in_data:
            continue
        parts = line.split("|")
        if len(parts) != 5:
            continue
        cik_raw, company, form, filed, filename = parts
        try:
            cik = _zero_pad_cik(cik_raw.strip())
        except ValueError:
            continue
        # Filename: edgar/data/<cik>/<accession-no-dashes>.txt
        stem = filename.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        if len(stem) != 18 or "-" in stem:
            # Canonical accession format is 18 chars with 2 dashes
            # (e.g. 0000320193-26-000042). Reconstruct if the
            # filename supplies it without dashes.
            digits_only = stem.replace("-", "")
            if len(digits_only) != 18 or not digits_only.isdigit():
                continue
            accession = f"{digits_only[:10]}-{digits_only[10:12]}-{digits_only[12:]}"
        else:
            accession = stem
        entries.append(
            MasterIndexEntry(
                cik=cik,
                company_name=company.strip(),
                form_type=form.strip(),
                date_filed=filed.strip(),
                accession_number=accession,
            )
        )
    return entries
```

- [ ] **Step 5: Run test — expect pass**

```bash
uv run pytest tests/test_sec_master_index_parser.py -v
```

Expected: 6 passed.

- [ ] **Step 6: Run ruff + pyright on the modified file**

```bash
uv run ruff check app/providers/implementations/sec_edgar.py tests/test_sec_master_index_parser.py
uv run ruff format --check app/providers/implementations/sec_edgar.py tests/test_sec_master_index_parser.py
uv run pyright app/providers/implementations/sec_edgar.py tests/test_sec_master_index_parser.py
```

Expected: all clean.

- [ ] **Step 7: Commit**

```bash
git add app/providers/implementations/sec_edgar.py tests/test_sec_master_index_parser.py tests/fixtures/sec/master_20260415.idx
git commit -m "feat(#272): SEC daily master-index parser

Pure function + MasterIndexEntry dataclass. Skips header/separator/
malformed rows. Zero-pads CIKs and reconstructs canonical accession
numbers from filenames."
```

---

## Task 2: `fetch_master_index` provider method (conditional GET)

The master-index lives at `https://www.sec.gov/Archives/edgar/daily-index/YYYY/QTRn/master.YYYYMMDD.idx` and honours `If-Modified-Since`. We mirror the pattern used by `build_cik_mapping_conditional` but return the raw body so the service layer can parse + hash.

**Files:**
- Modify: `app/providers/implementations/sec_edgar.py`
- Create: `tests/test_sec_provider_master_index.py`

- [ ] **Step 1: Write the failing provider test**

Create `tests/test_sec_provider_master_index.py`:

```python
"""Tests for SecFilingsProvider.fetch_master_index — conditional GET."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import httpx
import pytest

from app.providers.implementations.sec_edgar import (
    MasterIndexFetchResult,
    SecFilingsProvider,
)


FIXTURE = Path("tests/fixtures/sec/master_20260415.idx")


def _transport(body: bytes, status: int, last_modified: str | None = None) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        headers = {}
        if last_modified:
            headers["Last-Modified"] = last_modified
        return httpx.Response(status, content=body, headers=headers)
    return httpx.MockTransport(handler)


def test_fetch_returns_result_with_body_and_last_modified() -> None:
    body = FIXTURE.read_bytes()
    transport = _transport(body, 200, "Wed, 15 Apr 2026 22:00:00 GMT")
    provider = SecFilingsProvider(user_agent="test test@example.com")
    provider._tickers_client = httpx.Client(
        headers={"User-Agent": "test test@example.com"},
        transport=transport,
    )
    # Re-wrap with the same resilient-client so the MockTransport is honoured
    from app.providers.resilient_client import ResilientClient
    provider._http_tickers = ResilientClient(
        provider._tickers_client,
        min_request_interval_s=0.0,
    )

    result = provider.fetch_master_index(date(2026, 4, 15), if_modified_since=None)

    assert isinstance(result, MasterIndexFetchResult)
    assert result.body == body
    assert result.last_modified == "Wed, 15 Apr 2026 22:00:00 GMT"
    assert result.body_hash  # sha256 hex, 64 chars
    assert len(result.body_hash) == 64


def test_fetch_returns_none_on_304_not_modified() -> None:
    transport = _transport(b"", 304)
    provider = SecFilingsProvider(user_agent="test test@example.com")
    provider._tickers_client = httpx.Client(
        headers={"User-Agent": "test test@example.com"},
        transport=transport,
    )
    from app.providers.resilient_client import ResilientClient
    provider._http_tickers = ResilientClient(
        provider._tickers_client,
        min_request_interval_s=0.0,
    )

    result = provider.fetch_master_index(
        date(2026, 4, 15),
        if_modified_since="Wed, 15 Apr 2026 22:00:00 GMT",
    )
    assert result is None


def test_fetch_sends_if_modified_since_header_when_provided() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(request.headers)
        return httpx.Response(304)

    transport = httpx.MockTransport(handler)
    provider = SecFilingsProvider(user_agent="test test@example.com")
    provider._tickers_client = httpx.Client(
        headers={"User-Agent": "test test@example.com"},
        transport=transport,
    )
    from app.providers.resilient_client import ResilientClient
    provider._http_tickers = ResilientClient(
        provider._tickers_client,
        min_request_interval_s=0.0,
    )

    provider.fetch_master_index(
        date(2026, 4, 15),
        if_modified_since="Wed, 15 Apr 2026 22:00:00 GMT",
    )
    assert captured.get("if-modified-since") == "Wed, 15 Apr 2026 22:00:00 GMT"


def test_fetch_weekend_date_still_attempts_request() -> None:
    # SEC does not publish master-index on weekends, but the provider
    # stays dumb — it performs the fetch and returns whatever SEC sends
    # (a 404 or an empty body). The service layer decides the policy.
    transport = _transport(b"", 404)
    provider = SecFilingsProvider(user_agent="test test@example.com")
    provider._tickers_client = httpx.Client(
        headers={"User-Agent": "test test@example.com"},
        transport=transport,
    )
    from app.providers.resilient_client import ResilientClient
    provider._http_tickers = ResilientClient(
        provider._tickers_client,
        min_request_interval_s=0.0,
    )

    result = provider.fetch_master_index(date(2026, 4, 18), if_modified_since=None)
    # Weekend = 404 from SEC. Provider returns None — service decides.
    assert result is None
```

- [ ] **Step 2: Run test — expect failure**

```bash
uv run pytest tests/test_sec_provider_master_index.py -v
```

Expected: `ImportError: cannot import name 'MasterIndexFetchResult'`.

- [ ] **Step 3: Implement provider method**

Append to `app/providers/implementations/sec_edgar.py`:

```python
@dataclass(frozen=True)
class MasterIndexFetchResult:
    """Result of a conditional-GET fetch of master.YYYYMMDD.idx.

    Callers parse ``body`` via ``parse_master_index`` and persist
    ``body_hash`` + ``last_modified`` as watermark fields.
    """

    body: bytes
    body_hash: str
    last_modified: str | None


# Added inside SecFilingsProvider class:
def fetch_master_index(
    self,
    target_date: date,
    *,
    if_modified_since: str | None = None,
) -> MasterIndexFetchResult | None:
    """Conditional-GET the SEC daily master-index for a given date.

    URL shape: ``https://www.sec.gov/Archives/edgar/daily-index/
    YYYY/QTR{1..4}/master.YYYYMMDD.idx``. Returns ``None`` on 304
    (or on 404 — weekends and holidays have no file). Otherwise
    returns body bytes + sha256 hash + Last-Modified header for
    the caller to persist in the watermark row.

    Rate-limited alongside the other SEC clients via the shared
    timestamp list, so a burst of 7 calls respects the 10 rps cap.
    """
    quarter = (target_date.month - 1) // 3 + 1
    path = (
        f"https://www.sec.gov/Archives/edgar/daily-index/"
        f"{target_date.year}/QTR{quarter}/master.{target_date.strftime('%Y%m%d')}.idx"
    )
    headers: dict[str, str] = {}
    if if_modified_since:
        headers["If-Modified-Since"] = if_modified_since

    resp = self._http_tickers.get(path, headers=headers)
    if resp.status_code in (304, 404):
        return None
    resp.raise_for_status()
    body_hash = hashlib.sha256(resp.content).hexdigest()
    return MasterIndexFetchResult(
        body=resp.content,
        body_hash=body_hash,
        last_modified=resp.headers.get("Last-Modified"),
    )
```

The `fetch_master_index` method lives inside the `SecFilingsProvider` class. Place it next to `build_cik_mapping_conditional`.

- [ ] **Step 4: Run tests — expect pass**

```bash
uv run pytest tests/test_sec_provider_master_index.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Lint + type check**

```bash
uv run ruff check app/providers/implementations/sec_edgar.py tests/test_sec_provider_master_index.py
uv run ruff format --check app/providers/implementations/sec_edgar.py tests/test_sec_provider_master_index.py
uv run pyright app/providers/implementations/sec_edgar.py tests/test_sec_provider_master_index.py
```

Expected: all clean.

- [ ] **Step 6: Commit**

```bash
git add app/providers/implementations/sec_edgar.py tests/test_sec_provider_master_index.py
git commit -m "feat(#272): fetch_master_index conditional-GET on SecFilingsProvider

Returns MasterIndexFetchResult with body, sha256 hash, Last-Modified.
None on 304/404 — service layer decides policy for weekends/holidays.
Path uses daily-index/YYYY/QTRn/master.YYYYMMDD.idx."
```

---

## Task 3: Expose `upsert_facts_for_instrument` for reuse

`_upsert_facts` in `app/services/financial_facts.py` is currently module-private. The new executor needs to call it directly (it wants per-CIK atomicity, not the existing outer-loop `refresh_financial_facts`). Rename it to a public name.

**Files:**
- Modify: `app/services/financial_facts.py`

- [ ] **Step 1: Rename `_upsert_facts` → `upsert_facts_for_instrument`**

In `app/services/financial_facts.py`, change the function signature:

```python
def upsert_facts_for_instrument(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    facts: Sequence[XbrlFact],
    ingestion_run_id: int,
) -> tuple[int, int]:
    """Upsert XBRL facts into financial_facts_raw.

    Returns (upserted_count, skipped_count).
    Uses ON CONFLICT DO UPDATE so restatements overwrite prior values.
    """
    # ... body unchanged
```

Update the one caller inside `refresh_financial_facts`:

```python
upserted, skipped = upsert_facts_for_instrument(
    conn,
    instrument_id=instrument_id,
    facts=facts,
    ingestion_run_id=run_id,
)
```

Also expose `_start_ingestion_run` and `_finish_ingestion_run` similarly — rename to `start_ingestion_run` / `finish_ingestion_run`.

- [ ] **Step 2: Run existing tests**

```bash
uv run pytest tests/ -k "financial_facts" -v
```

Expected: existing tests pass after the rename. If any test references `_upsert_facts` directly, update it to the new name.

- [ ] **Step 3: Lint + type check**

```bash
uv run ruff check app/services/financial_facts.py
uv run ruff format --check app/services/financial_facts.py
uv run pyright app/services/financial_facts.py
```

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add app/services/financial_facts.py tests/
git commit -m "refactor(#272): make facts upsert + ingestion-run helpers public

Renames _upsert_facts → upsert_facts_for_instrument and the run helpers
so the new sec_incremental executor can call them directly while
preserving per-CIK transaction atomicity."
```

---

## Task 4: `plan_refresh` — planner

Planner reads covered-US cohort, fetches 7-day master-index window (conditional-GET each day), intersects with cohort, and emits a `RefreshPlan`. No writes during planning except watermark updates for master-index files (to short-circuit future fetches).

**Files:**
- Create: `app/services/sec_incremental.py`
- Create: `tests/test_sec_incremental_planner.py`
- Create: `tests/fixtures/sec/submissions_TEST.json`

- [ ] **Step 1: Create the submissions fixture**

`tests/fixtures/sec/submissions_TEST.json`:

```json
{
  "cik": "320193",
  "filings": {
    "recent": {
      "accessionNumber": [
        "0000320193-26-000042",
        "0000320193-26-000017",
        "0000320193-25-000108"
      ],
      "filingDate": ["2026-04-15", "2026-02-01", "2025-10-28"],
      "form": ["10-Q", "8-K", "10-K"],
      "acceptedDate": [
        "2026-04-15T16:05:00.000Z",
        "2026-02-01T09:00:00.000Z",
        "2025-10-28T16:00:00.000Z"
      ]
    }
  }
}
```

- [ ] **Step 2: Write the failing planner tests**

Create `tests/test_sec_incremental_planner.py`:

```python
"""Tests for app.services.sec_incremental.plan_refresh."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import psycopg
import pytest

from app.providers.implementations.sec_edgar import (
    MasterIndexFetchResult,
    SecFilingsProvider,
)
from app.services.sec_incremental import (
    FUNDAMENTALS_FORMS,
    LOOKBACK_DAYS,
    RefreshPlan,
    plan_refresh,
)
from app.services.watermarks import set_watermark

FIXTURE_MASTER = Path("tests/fixtures/sec/master_20260415.idx")
FIXTURE_SUBMISSIONS = Path("tests/fixtures/sec/submissions_TEST.json").read_text()


@dataclass
class StubProvider:
    master_bodies: dict[date, bytes | None]
    submissions: dict[str, bytes]

    def fetch_master_index(
        self,
        target_date: date,
        *,
        if_modified_since: str | None = None,
    ) -> MasterIndexFetchResult | None:
        body = self.master_bodies.get(target_date)
        if body is None:
            return None
        return MasterIndexFetchResult(
            body=body,
            body_hash=f"hash-{target_date.isoformat()}",
            last_modified=f"lm-{target_date.isoformat()}",
        )

    def fetch_submissions(self, cik: str) -> bytes:
        return self.submissions[cik]


def _seed_us_cohort(conn: psycopg.Connection[tuple], ciks: list[str]) -> None:
    """Insert rows into instruments + external_identifiers so the
    planner's covered-US query returns these CIKs."""
    # Exact SQL shape depends on the existing helper; reuse the test
    # fixture factory from tests/factories.py (add a helper there if
    # one does not exist). Pseudo:
    for i, cik in enumerate(ciks):
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, is_tradable) "
            "VALUES (%s, %s, TRUE)",
            (i + 1, f"TEST{i}"),
        )
        conn.execute(
            "INSERT INTO external_identifiers "
            "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (%s, 'sec', 'cik', %s, TRUE)",
            (i + 1, cik),
        )


def test_empty_cohort_returns_empty_plan(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """With no covered US CIKs, the planner returns an empty plan
    without making any provider calls."""
    provider = MagicMock(spec=SecFilingsProvider)

    plan = plan_refresh(ebull_test_conn, provider, today=date(2026, 4, 15))

    assert plan == RefreshPlan(seeds=[], refreshes=[], submissions_only_advances=[])
    provider.fetch_master_index.assert_not_called()


def test_fresh_cohort_no_watermarks_all_ciks_are_seeds(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """First install: every covered CIK has no watermark, so every
    CIK lands in plan.seeds regardless of what the master-index says."""
    _seed_us_cohort(ebull_test_conn, ["0000320193", "0000789019", "0001045810"])
    provider = StubProvider(
        master_bodies={d: FIXTURE_MASTER.read_bytes() for d in _window(date(2026, 4, 15))},
        submissions={},
    )

    plan = plan_refresh(ebull_test_conn, cast(SecFilingsProvider, provider), today=date(2026, 4, 15))

    assert set(plan.seeds) == {"0000320193", "0000789019", "0001045810"}
    assert plan.refreshes == []


def test_all_304_master_index_produces_empty_plan_when_all_covered_watermarked(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """With watermarks present and all master-index days 304, no CIK
    is a seed and no CIK is a refresh — planner emits an empty plan."""
    _seed_us_cohort(ebull_test_conn, ["0000320193"])
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000320193",
            watermark="0000320193-25-000108",
        )
    provider = StubProvider(
        master_bodies={d: None for d in _window(date(2026, 4, 15))},
        submissions={},
    )

    plan = plan_refresh(ebull_test_conn, cast(SecFilingsProvider, provider), today=date(2026, 4, 15))

    assert plan == RefreshPlan(seeds=[], refreshes=[], submissions_only_advances=[])


def test_master_index_hit_with_fundamentals_form_produces_refresh(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Covered CIK filed 10-Q today and stored watermark is older →
    CIK enters plan.refreshes."""
    _seed_us_cohort(ebull_test_conn, ["0000320193"])
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000320193",
            watermark="0000320193-25-000108",  # older than the 10-Q in the fixture
        )
    provider = StubProvider(
        master_bodies={date(2026, 4, 15): FIXTURE_MASTER.read_bytes()}
        | {d: None for d in _window(date(2026, 4, 15)) if d != date(2026, 4, 15)},
        submissions={"0000320193": FIXTURE_SUBMISSIONS.encode("utf-8")},
    )

    plan = plan_refresh(ebull_test_conn, cast(SecFilingsProvider, provider), today=date(2026, 4, 15))

    assert "0000320193" in plan.refreshes
    assert plan.seeds == []


def test_master_index_hit_with_8k_only_is_submissions_only_advance(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A CIK that only filed an 8-K today (non-fundamentals form)
    advances the sec.submissions watermark but does NOT queue
    a companyfacts refresh."""
    _seed_us_cohort(ebull_test_conn, ["0000789019"])  # Microsoft, 8-K in fixture
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000789019",
            watermark="older-accession",
        )
    # Rebuild fixture body to only include the 8-K row for this CIK
    custom_master = (
        b"CIK|Company Name|Form Type|Date Filed|Filename\n"
        b"--------------------------------------------------------------------------------\n"
        b"789019|MICROSOFT CORP|8-K|2026-04-15|edgar/data/789019/0000789019-26-000017.txt\n"
    )
    submissions_msft = (
        b'{"cik":"789019","filings":{"recent":{'
        b'"accessionNumber":["0000789019-26-000017"],'
        b'"filingDate":["2026-04-15"],'
        b'"form":["8-K"],'
        b'"acceptedDate":["2026-04-15T09:00:00.000Z"]}}}'
    )
    provider = StubProvider(
        master_bodies={date(2026, 4, 15): custom_master}
        | {d: None for d in _window(date(2026, 4, 15)) if d != date(2026, 4, 15)},
        submissions={"0000789019": submissions_msft},
    )

    plan = plan_refresh(ebull_test_conn, cast(SecFilingsProvider, provider), today=date(2026, 4, 15))

    assert plan.refreshes == []
    assert ("0000789019", "0000789019-26-000017") in plan.submissions_only_advances


def test_master_index_hit_non_covered_cik_is_ignored(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Master-index rows for CIKs outside the covered cohort do not
    trigger any submissions.json fetches."""
    _seed_us_cohort(ebull_test_conn, [])  # empty cohort
    provider = StubProvider(
        master_bodies={date(2026, 4, 15): FIXTURE_MASTER.read_bytes()}
        | {d: None for d in _window(date(2026, 4, 15)) if d != date(2026, 4, 15)},
        submissions={},
    )

    plan = plan_refresh(ebull_test_conn, cast(SecFilingsProvider, provider), today=date(2026, 4, 15))

    assert plan == RefreshPlan(seeds=[], refreshes=[], submissions_only_advances=[])


def test_master_index_hit_accession_unchanged_is_skip(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Master-index lists an accession already matching our watermark
    (re-listing or amendment that didn't change the top) → no refresh,
    no submissions-only advance."""
    _seed_us_cohort(ebull_test_conn, ["0000320193"])
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000320193",
            watermark="0000320193-26-000042",  # already matches fixture top
        )
    provider = StubProvider(
        master_bodies={date(2026, 4, 15): FIXTURE_MASTER.read_bytes()}
        | {d: None for d in _window(date(2026, 4, 15)) if d != date(2026, 4, 15)},
        submissions={"0000320193": FIXTURE_SUBMISSIONS.encode("utf-8")},
    )

    plan = plan_refresh(ebull_test_conn, cast(SecFilingsProvider, provider), today=date(2026, 4, 15))

    assert plan == RefreshPlan(seeds=[], refreshes=[], submissions_only_advances=[])


def _window(today: date) -> list[date]:
    from datetime import timedelta
    return [today - timedelta(days=i) for i in range(LOOKBACK_DAYS)]
```

Note: `ebull_test_conn` is the existing fixture that connects to `ebull_test` DB with automatic rollback. If it doesn't exist yet under this name, use the project's existing test-DB fixture (check `tests/conftest.py`).

- [ ] **Step 3: Run tests — expect failure**

```bash
uv run pytest tests/test_sec_incremental_planner.py -v
```

Expected: `ImportError` for `plan_refresh`, `RefreshPlan`, `FUNDAMENTALS_FORMS`, `LOOKBACK_DAYS`.

- [ ] **Step 4: Implement the planner**

Create `app/services/sec_incremental.py`:

```python
"""SEC change-driven fetch planner + executor (issue #272).

Replaces the 45-minute full-pull in ``daily_financial_facts`` with a
two-phase flow:

    plan_refresh(conn, provider, today)
        -> RefreshPlan { seeds, refreshes, submissions_only_advances }

    execute_refresh(conn, provider, plan, progress)
        -> RefreshOutcome { seeded, refreshed, submissions_advanced, failed }

The planner fetches a 7-day window of SEC daily master-index files with
conditional GET, intersects filings with our covered-US cohort, and
compares each hit's top accession to a per-CIK watermark. Only CIKs
with genuinely new fundamentals filings (10-K / 10-Q / 20-F family)
land in ``refreshes``. CIKs with only non-fundamentals filings (8-K etc.)
advance the submissions watermark alone — no companyfacts pull.

A new covered CIK (fresh install or newly promoted ticker) has no
watermark row and is placed in ``seeds`` for a full initial backfill.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta

import psycopg

from app.providers.implementations.sec_edgar import (
    MasterIndexEntry,
    SecFilingsProvider,
    parse_master_index,
)
from app.services.watermarks import get_watermark, set_watermark

logger = logging.getLogger(__name__)

LOOKBACK_DAYS = 7

FUNDAMENTALS_FORMS: frozenset[str] = frozenset(
    {
        "10-K",
        "10-K/A",
        "10-Q",
        "10-Q/A",
        "20-F",
        "20-F/A",
        "40-F",
        "40-F/A",
    }
)


@dataclass(frozen=True)
class RefreshPlan:
    """Describes the work a single run of ``daily_financial_facts`` will do.

    - ``seeds`` — CIKs with no prior watermark row; full backfill.
    - ``refreshes`` — CIKs that filed a fundamentals form in the window
      with an accession newer than the stored watermark.
    - ``submissions_only_advances`` — CIKs that filed a non-fundamentals
      form (e.g. 8-K). Advance ``sec.submissions`` watermark only; no
      companyfacts pull.
    """

    seeds: list[str] = field(default_factory=list)
    refreshes: list[str] = field(default_factory=list)
    submissions_only_advances: list[tuple[str, str]] = field(default_factory=list)


def _load_covered_us_ciks(conn: psycopg.Connection[tuple]) -> list[str]:
    """Return zero-padded CIKs of tradable instruments with a SEC CIK
    mapping. Mirrors the cohort query already used by
    ``daily_financial_facts``."""
    cur = conn.execute(
        """
        SELECT ei.identifier_value
        FROM instruments i
        JOIN external_identifiers ei
            ON ei.instrument_id = i.instrument_id
            AND ei.provider = 'sec'
            AND ei.identifier_type = 'cik'
            AND ei.is_primary = TRUE
        WHERE i.is_tradable = TRUE
        ORDER BY ei.identifier_value
        """
    )
    return [row[0] for row in cur.fetchall()]


def _lookback_dates(today: date) -> list[date]:
    return [today - timedelta(days=i) for i in range(LOOKBACK_DAYS)]


def _top_accession_from_submissions(body: bytes) -> tuple[str, str] | None:
    """Return (top_accession, top_form) from submissions.json bytes,
    or None if the file has no recent filings."""
    try:
        raw = json.loads(body)
    except ValueError:
        return None
    recent = raw.get("filings", {}).get("recent", {})
    accessions = recent.get("accessionNumber") or []
    forms = recent.get("form") or []
    if not accessions:
        return None
    return accessions[0], (forms[0] if forms else "")


def plan_refresh(
    conn: psycopg.Connection[tuple],
    provider: SecFilingsProvider,
    *,
    today: date,
) -> RefreshPlan:
    """Derive the work for a single daily_financial_facts run."""
    covered = _load_covered_us_ciks(conn)
    if not covered:
        return RefreshPlan()

    master_hits_by_cik: dict[str, list[MasterIndexEntry]] = {}
    for target in _lookback_dates(today):
        wm = get_watermark(conn, "sec.master-index", target.isoformat())
        if_modified_since = wm.watermark if wm else None
        result = provider.fetch_master_index(target, if_modified_since=if_modified_since)
        if result is None:
            continue  # 304 / 404 / weekend — nothing to parse

        if wm is not None and wm.response_hash == result.body_hash:
            # Identical body without 304 — refresh fetched_at only
            with conn.transaction():
                set_watermark(
                    conn,
                    source="sec.master-index",
                    key=target.isoformat(),
                    watermark=result.last_modified or "",
                    response_hash=result.body_hash,
                )
            continue

        entries = parse_master_index(result.body)
        for entry in entries:
            master_hits_by_cik.setdefault(entry.cik, []).append(entry)

        with conn.transaction():
            set_watermark(
                conn,
                source="sec.master-index",
                key=target.isoformat(),
                watermark=result.last_modified or "",
                response_hash=result.body_hash,
            )

    seeds: list[str] = []
    refreshes: list[str] = []
    submissions_only: list[tuple[str, str]] = []

    for cik in covered:
        wm = get_watermark(conn, "sec.submissions", cik)
        if wm is None:
            seeds.append(cik)
            continue

        entries = master_hits_by_cik.get(cik)
        if not entries:
            continue

        # Fetch submissions.json to get current top accession + form
        raw = _fetch_submissions_bytes(provider, cik)
        if raw is None:
            continue
        top = _top_accession_from_submissions(raw)
        if top is None:
            continue
        top_accession, _top_form = top
        if top_accession == wm.watermark:
            continue  # amendment or re-listing of seen filing

        hit_forms = {e.form_type for e in entries}
        if hit_forms & FUNDAMENTALS_FORMS:
            refreshes.append(cik)
        else:
            submissions_only.append((cik, top_accession))

    return RefreshPlan(
        seeds=sorted(seeds),
        refreshes=sorted(refreshes),
        submissions_only_advances=sorted(submissions_only),
    )


def _fetch_submissions_bytes(
    provider: SecFilingsProvider,
    cik: str,
) -> bytes | None:
    """Tiny shim to expose the submissions payload as raw bytes.
    Uses the private ``_fetch_submissions`` helper on the provider —
    that method returns the parsed JSON today, so we re-encode for
    downstream hashing consistency."""
    raw = provider._fetch_submissions(cik)  # type: ignore[attr-defined]
    if raw is None:
        return None
    return json.dumps(raw).encode("utf-8")
```

- [ ] **Step 5: Run tests — expect pass**

```bash
uv run pytest tests/test_sec_incremental_planner.py -v
```

Expected: 7 passed.

- [ ] **Step 6: Lint + type check**

```bash
uv run ruff check app/services/sec_incremental.py tests/test_sec_incremental_planner.py
uv run ruff format --check app/services/sec_incremental.py tests/test_sec_incremental_planner.py
uv run pyright app/services/sec_incremental.py tests/test_sec_incremental_planner.py
```

Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add app/services/sec_incremental.py tests/test_sec_incremental_planner.py tests/fixtures/sec/submissions_TEST.json
git commit -m "feat(#272): sec_incremental.plan_refresh planner

Reads covered-US cohort, 7-day master-index window with conditional-GET
per day, intersects hits with cohort, produces RefreshPlan with seeds /
refreshes / submissions-only advances. Form-type gate limits
companyfacts pulls to 10-K/10-Q/20-F/40-F (+ amendments)."
```

---

## Task 5: `execute_refresh` — executor with per-CIK atomicity

Executor iterates the plan. For each CIK: fetch XBRL, upsert facts, advance watermarks — all inside one `with conn.transaction()` block. Per-CIK failure does not fail the run.

**Files:**
- Modify: `app/services/sec_incremental.py`
- Create: `tests/test_sec_incremental_executor.py`
- Create: `tests/fixtures/sec/companyfacts_TEST.json`

- [ ] **Step 1: Create companyfacts fixture**

`tests/fixtures/sec/companyfacts_TEST.json`:

```json
{
  "cik": 320193,
  "entityName": "Apple Inc.",
  "facts": {
    "us-gaap": {
      "Revenues": {
        "units": {
          "USD": [
            {
              "start": "2026-01-01",
              "end": "2026-03-31",
              "val": 90000000000,
              "accn": "0000320193-26-000042",
              "form": "10-Q",
              "filed": "2026-04-15",
              "fy": 2026,
              "fp": "Q1",
              "decimals": -6
            }
          ]
        }
      },
      "Assets": {
        "units": {
          "USD": [
            {
              "end": "2026-03-31",
              "val": 360000000000,
              "accn": "0000320193-26-000042",
              "form": "10-Q",
              "filed": "2026-04-15",
              "fy": 2026,
              "fp": "Q1",
              "decimals": -6
            }
          ]
        }
      }
    }
  }
}
```

- [ ] **Step 2: Write the failing executor tests**

Create `tests/test_sec_incremental_executor.py`:

```python
"""Tests for app.services.sec_incremental.execute_refresh."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import psycopg
import pytest

from app.providers.fundamentals import XbrlFact
from app.providers.implementations.sec_edgar import SecFilingsProvider
from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider
from app.services.sec_incremental import (
    RefreshOutcome,
    RefreshPlan,
    execute_refresh,
)
from app.services.watermarks import get_watermark, set_watermark

FIXTURE_COMPANYFACTS = Path("tests/fixtures/sec/companyfacts_TEST.json").read_text()


@dataclass
class StubFundamentals:
    facts_by_cik: dict[str, list[XbrlFact]]
    fail_on: set[str]

    def extract_facts(self, symbol: str, cik: str) -> list[XbrlFact]:
        if cik in self.fail_on:
            raise RuntimeError(f"boom for {cik}")
        return self.facts_by_cik.get(cik, [])


@dataclass
class StubFilings:
    submissions_by_cik: dict[str, bytes]

    def _fetch_submissions(self, cik: str) -> dict[str, object] | None:
        body = self.submissions_by_cik.get(cik)
        if body is None:
            return None
        return cast(dict[str, object], json.loads(body))


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    symbol: str,
    cik: str,
) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, is_tradable) VALUES (%s, %s, TRUE)",
        (instrument_id, symbol),
    )
    conn.execute(
        "INSERT INTO external_identifiers "
        "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
        "VALUES (%s, 'sec', 'cik', %s, TRUE)",
        (instrument_id, cik),
    )


def _sample_fact(accession: str) -> XbrlFact:
    from decimal import Decimal
    return XbrlFact(
        concept="Revenues",
        taxonomy="us-gaap",
        unit="USD",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        val=Decimal("90000000000"),
        frame=None,
        accession_number=accession,
        form_type="10-Q",
        filed_date=date(2026, 4, 15),
        fiscal_year=2026,
        fiscal_period="Q1",
        decimals="-6",
    )


def test_execute_seed_path_writes_facts_and_both_watermarks(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000320193")
    plan = RefreshPlan(seeds=["0000320193"])
    filings = StubFilings(
        submissions_by_cik={
            "0000320193": json.dumps({
                "filings": {"recent": {
                    "accessionNumber": ["0000320193-26-000042"],
                    "form": ["10-Q"],
                    "acceptedDate": ["2026-04-15T16:05:00.000Z"],
                }}
            }).encode("utf-8"),
        },
    )
    fundamentals = StubFundamentals(
        facts_by_cik={"0000320193": [_sample_fact("0000320193-26-000042")]},
        fail_on=set(),
    )

    outcome = execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    assert outcome.seeded == 1
    assert outcome.refreshed == 0
    assert outcome.failed == []

    submissions_wm = get_watermark(ebull_test_conn, "sec.submissions", "0000320193")
    assert submissions_wm is not None
    assert submissions_wm.watermark == "0000320193-26-000042"

    facts_wm = get_watermark(ebull_test_conn, "sec.companyfacts", "0000320193")
    assert facts_wm is not None
    assert facts_wm.watermark == "0000320193-26-000042"

    count = ebull_test_conn.execute(
        "SELECT COUNT(*) FROM financial_facts_raw WHERE instrument_id = 1"
    ).fetchone()
    assert count is not None and count[0] >= 1


def test_execute_refresh_path_advances_both_watermarks(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000320193")
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000320193",
            watermark="0000320193-25-000108",
        )
        set_watermark(
            ebull_test_conn,
            source="sec.companyfacts",
            key="0000320193",
            watermark="0000320193-25-000108",
        )
    plan = RefreshPlan(refreshes=["0000320193"])
    filings = StubFilings(
        submissions_by_cik={
            "0000320193": json.dumps({
                "filings": {"recent": {
                    "accessionNumber": ["0000320193-26-000042"],
                    "form": ["10-Q"],
                    "acceptedDate": ["2026-04-15T16:05:00.000Z"],
                }}
            }).encode("utf-8"),
        },
    )
    fundamentals = StubFundamentals(
        facts_by_cik={"0000320193": [_sample_fact("0000320193-26-000042")]},
        fail_on=set(),
    )

    outcome = execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    assert outcome.refreshed == 1
    assert outcome.failed == []

    submissions_wm = get_watermark(ebull_test_conn, "sec.submissions", "0000320193")
    assert submissions_wm is not None
    assert submissions_wm.watermark == "0000320193-26-000042"


def test_execute_failure_does_not_advance_watermarks(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="TEST", cik="0000320193")
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000320193",
            watermark="0000320193-25-000108",
        )
    plan = RefreshPlan(refreshes=["0000320193"])
    filings = StubFilings(submissions_by_cik={})
    fundamentals = StubFundamentals(
        facts_by_cik={},
        fail_on={"0000320193"},
    )

    outcome = execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    assert outcome.failed == [("0000320193", "RuntimeError")]

    wm = get_watermark(ebull_test_conn, "sec.submissions", "0000320193")
    assert wm is not None
    assert wm.watermark == "0000320193-25-000108"  # unchanged


def test_execute_one_failure_does_not_abort_siblings(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="A", cik="0000000001")
    _seed_instrument(ebull_test_conn, instrument_id=2, symbol="B", cik="0000000002")
    plan = RefreshPlan(seeds=["0000000001", "0000000002"])
    filings = StubFilings(
        submissions_by_cik={
            "0000000001": json.dumps({"filings": {"recent": {
                "accessionNumber": ["0000000001-26-000001"],
                "form": ["10-Q"],
                "acceptedDate": ["2026-04-15T09:00:00.000Z"],
            }}}).encode("utf-8"),
            "0000000002": json.dumps({"filings": {"recent": {
                "accessionNumber": ["0000000002-26-000001"],
                "form": ["10-Q"],
                "acceptedDate": ["2026-04-15T09:00:00.000Z"],
            }}}).encode("utf-8"),
        },
    )
    fundamentals = StubFundamentals(
        facts_by_cik={"0000000002": [_sample_fact("0000000002-26-000001")]},
        fail_on={"0000000001"},
    )

    outcome = execute_refresh(
        ebull_test_conn,
        filings_provider=cast(SecFilingsProvider, filings),
        fundamentals_provider=cast(SecFundamentalsProvider, fundamentals),
        plan=plan,
    )

    assert outcome.seeded == 1  # only CIK 2 succeeded
    assert outcome.failed == [("0000000001", "RuntimeError")]


def test_execute_submissions_only_advance_skips_companyfacts(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_instrument(ebull_test_conn, instrument_id=1, symbol="MSFT", cik="0000789019")
    with ebull_test_conn.transaction():
        set_watermark(
            ebull_test_conn,
            source="sec.submissions",
            key="0000789019",
            watermark="older",
        )
    plan = RefreshPlan(
        submissions_only_advances=[("0000789019", "0000789019-26-000017")],
    )
    filings = MagicMock(spec=SecFilingsProvider)
    fundamentals = MagicMock(spec=SecFundamentalsProvider)

    outcome = execute_refresh(
        ebull_test_conn,
        filings_provider=filings,
        fundamentals_provider=fundamentals,
        plan=plan,
    )

    assert outcome.submissions_advanced == 1
    fundamentals.extract_facts.assert_not_called()

    wm = get_watermark(ebull_test_conn, "sec.submissions", "0000789019")
    assert wm is not None
    assert wm.watermark == "0000789019-26-000017"
```

- [ ] **Step 3: Run tests — expect failure**

```bash
uv run pytest tests/test_sec_incremental_executor.py -v
```

Expected: `ImportError: cannot import name 'execute_refresh'`.

- [ ] **Step 4: Implement `execute_refresh`**

Append to `app/services/sec_incremental.py`:

```python
from app.services.financial_facts import (
    finish_ingestion_run,
    start_ingestion_run,
    upsert_facts_for_instrument,
)
from app.services.sync_orchestrator.progress import report_progress

if False:  # TYPE_CHECKING block — keep real imports at top
    from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider


@dataclass(frozen=True)
class RefreshOutcome:
    seeded: int = 0
    refreshed: int = 0
    submissions_advanced: int = 0
    failed: list[tuple[str, str]] = field(default_factory=list)


def _instrument_for_cik(
    conn: psycopg.Connection[tuple],
    cik: str,
) -> tuple[int, str] | None:
    row = conn.execute(
        """
        SELECT i.instrument_id, i.symbol
        FROM instruments i
        JOIN external_identifiers ei
            ON ei.instrument_id = i.instrument_id
            AND ei.provider = 'sec'
            AND ei.identifier_type = 'cik'
            AND ei.identifier_value = %s
            AND ei.is_primary = TRUE
        WHERE i.is_tradable = TRUE
        """,
        (cik,),
    ).fetchone()
    if row is None:
        return None
    return int(row[0]), str(row[1])


def execute_refresh(
    conn: psycopg.Connection[tuple],
    *,
    filings_provider: SecFilingsProvider,
    fundamentals_provider: "SecFundamentalsProvider",
    plan: RefreshPlan,
) -> RefreshOutcome:
    """Execute a RefreshPlan against the database.

    Each CIK's work runs inside its own ``with conn.transaction()``.
    A per-CIK failure records the error and continues — no layer-wide
    abort. Watermarks only advance with successful upserts.
    """
    total = len(plan.seeds) + len(plan.refreshes) + len(plan.submissions_only_advances)
    if total == 0:
        return RefreshOutcome()

    run_id = start_ingestion_run(
        conn,
        source="sec_edgar",
        endpoint="/api/xbrl/companyfacts",
        instrument_count=total,
    )
    seeded = 0
    refreshed = 0
    submissions_advanced = 0
    failed: list[tuple[str, str]] = []
    done = 0

    for cik in plan.seeds:
        done += 1
        try:
            inst = _instrument_for_cik(conn, cik)
            if inst is None:
                continue
            instrument_id, symbol = inst
            facts = fundamentals_provider.extract_facts(symbol, cik)
            submissions_raw = filings_provider._fetch_submissions(cik)  # type: ignore[attr-defined]
            if submissions_raw is None:
                continue
            top = _top_accession_from_submissions(
                json.dumps(submissions_raw).encode("utf-8")
            )
            if top is None:
                continue
            top_accession, _ = top
            with conn.transaction():
                if facts:
                    upsert_facts_for_instrument(
                        conn,
                        instrument_id=instrument_id,
                        facts=facts,
                        ingestion_run_id=run_id,
                    )
                set_watermark(
                    conn,
                    source="sec.submissions",
                    key=cik,
                    watermark=top_accession,
                )
                set_watermark(
                    conn,
                    source="sec.companyfacts",
                    key=cik,
                    watermark=top_accession,
                )
            seeded += 1
        except Exception as exc:
            failed.append((cik, type(exc).__name__))
            logger.exception("sec_incremental seed failed for cik=%s", cik)
        report_progress(done, total)

    for cik in plan.refreshes:
        done += 1
        try:
            inst = _instrument_for_cik(conn, cik)
            if inst is None:
                continue
            instrument_id, symbol = inst
            facts = fundamentals_provider.extract_facts(symbol, cik)
            submissions_raw = filings_provider._fetch_submissions(cik)  # type: ignore[attr-defined]
            if submissions_raw is None:
                continue
            top = _top_accession_from_submissions(
                json.dumps(submissions_raw).encode("utf-8")
            )
            if top is None:
                continue
            top_accession, _ = top
            with conn.transaction():
                if facts:
                    upsert_facts_for_instrument(
                        conn,
                        instrument_id=instrument_id,
                        facts=facts,
                        ingestion_run_id=run_id,
                    )
                set_watermark(
                    conn,
                    source="sec.submissions",
                    key=cik,
                    watermark=top_accession,
                )
                set_watermark(
                    conn,
                    source="sec.companyfacts",
                    key=cik,
                    watermark=top_accession,
                )
            refreshed += 1
        except Exception as exc:
            failed.append((cik, type(exc).__name__))
            logger.exception("sec_incremental refresh failed for cik=%s", cik)
        report_progress(done, total)

    for cik, accession in plan.submissions_only_advances:
        done += 1
        try:
            with conn.transaction():
                set_watermark(
                    conn,
                    source="sec.submissions",
                    key=cik,
                    watermark=accession,
                )
            submissions_advanced += 1
        except Exception as exc:
            failed.append((cik, type(exc).__name__))
            logger.exception(
                "sec_incremental submissions-only advance failed for cik=%s", cik
            )
        report_progress(done, total)

    finish_ingestion_run(
        conn,
        run_id=run_id,
        status="success" if not failed else ("partial" if seeded + refreshed > 0 else "failed"),
        rows_upserted=seeded + refreshed,
        error=f"{len(failed)} CIKs failed" if failed else None,
    )

    return RefreshOutcome(
        seeded=seeded,
        refreshed=refreshed,
        submissions_advanced=submissions_advanced,
        failed=failed,
    )
```

Also remove the stray `if False` block above and move the `SecFundamentalsProvider` import to a real `TYPE_CHECKING` block at the top of the file:

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider
```

- [ ] **Step 5: Run tests — expect pass**

```bash
uv run pytest tests/test_sec_incremental_executor.py -v
```

Expected: 5 passed.

- [ ] **Step 6: Lint + type check**

```bash
uv run ruff check app/services/sec_incremental.py tests/test_sec_incremental_executor.py
uv run ruff format --check app/services/sec_incremental.py tests/test_sec_incremental_executor.py
uv run pyright app/services/sec_incremental.py tests/test_sec_incremental_executor.py
```

Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add app/services/sec_incremental.py tests/test_sec_incremental_executor.py tests/fixtures/sec/companyfacts_TEST.json
git commit -m "feat(#272): sec_incremental.execute_refresh executor

Per-CIK transaction — facts + both watermarks commit atomically.
Per-CIK failure isolation — one bad CIK does not fail the layer.
Submissions-only advance path skips companyfacts fetch."
```

---

## Task 6: Rewire `daily_financial_facts`

Replace the flat full-sweep body with plan + execute.

**Files:**
- Modify: `app/workers/scheduler.py`

- [ ] **Step 1: Replace the body of `daily_financial_facts`**

Find `app/workers/scheduler.py` around line 1093. Replace the function body (everything inside `with _tracked_job(JOB_DAILY_FINANCIAL_FACTS) as tracker:` down to the `tracker.row_count` assignment) with:

```python
def daily_financial_facts() -> None:
    """Incremental SEC facts refresh driven by the daily master-index
    + per-CIK watermarks. See app.services.sec_incremental."""
    from datetime import UTC, datetime

    with _tracked_job(JOB_DAILY_FINANCIAL_FACTS) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            from app.providers.implementations.sec_edgar import SecFilingsProvider
            from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider
            from app.services.sec_incremental import execute_refresh, plan_refresh

            today = datetime.now(UTC).date()
            with (
                SecFilingsProvider(user_agent=settings.sec_user_agent) as filings,
                SecFundamentalsProvider(user_agent=settings.sec_user_agent) as fundamentals,
            ):
                plan = plan_refresh(conn, filings, today=today)
                logger.info(
                    "daily_financial_facts plan: seeds=%d refreshes=%d submissions_only=%d",
                    len(plan.seeds),
                    len(plan.refreshes),
                    len(plan.submissions_only_advances),
                )
                outcome = execute_refresh(
                    conn,
                    filings_provider=filings,
                    fundamentals_provider=fundamentals,
                    plan=plan,
                )
                logger.info(
                    "daily_financial_facts outcome: seeded=%d refreshed=%d submissions_advanced=%d failed=%d",
                    outcome.seeded,
                    outcome.refreshed,
                    outcome.submissions_advanced,
                    len(outcome.failed),
                )

            # Phase 2: normalization (reads financial_facts_raw written above)
            if outcome.seeded + outcome.refreshed > 0:
                from app.services.financial_normalization import normalize_financial_periods
                # Re-derive instrument_ids from the CIKs we just touched.
                touched_ciks = plan.seeds + plan.refreshes
                cur = conn.execute(
                    """
                    SELECT i.instrument_id
                    FROM instruments i
                    JOIN external_identifiers ei
                        ON ei.instrument_id = i.instrument_id
                        AND ei.provider = 'sec'
                        AND ei.identifier_type = 'cik'
                        AND ei.identifier_value = ANY(%s)
                        AND ei.is_primary = TRUE
                    """,
                    (touched_ciks,),
                )
                instrument_ids = [row[0] for row in cur.fetchall()]
                norm_summary = normalize_financial_periods(conn, instrument_ids)
                logger.info(
                    "Normalization: %d instruments, %d raw periods, %d canonical",
                    norm_summary.instruments_processed,
                    norm_summary.periods_raw_upserted,
                    norm_summary.periods_canonical_upserted,
                )
                tracker.row_count = (
                    outcome.seeded + outcome.refreshed + norm_summary.periods_canonical_upserted
                )
            else:
                tracker.row_count = outcome.submissions_advanced
```

- [ ] **Step 2: Run existing scheduler tests**

```bash
uv run pytest tests/ -k "scheduler or financial_facts or sync_orchestrator" -v
```

Expected: existing tests still pass. Any test that directly called the old full-sweep path needs updating (check output).

- [ ] **Step 3: Lint + type check**

```bash
uv run ruff check app/workers/scheduler.py
uv run ruff format --check app/workers/scheduler.py
uv run pyright app/workers/scheduler.py
```

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add app/workers/scheduler.py
git commit -m "feat(#272): rewire daily_financial_facts to incremental planner

Replaces the 45-min full-sweep with plan_refresh + execute_refresh.
Normalization runs only on touched instruments. Submissions-only
advances count toward tracker.row_count when no facts were upserted."
```

---

## Task 7: Integration test via sync-orchestrator entrypoint

Validate the whole flow end-to-end against a real `ebull_test` DB with stub providers.

**Files:**
- Create: `tests/test_sync_orchestrator_financial_facts_incremental.py`

- [ ] **Step 1: Extract reusable stubs to a shared fixture module**

Create `tests/fixtures/sec_stubs.py`:

```python
"""Shared stubs for SEC provider tests (planner + executor + integration)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import cast

from app.providers.fundamentals import XbrlFact
from app.providers.implementations.sec_edgar import (
    MasterIndexFetchResult,
    SecFilingsProvider,
)
from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider


@dataclass
class StubFilingsProvider:
    """Stands in for SecFilingsProvider. Drives plan_refresh end-to-end."""

    master_bodies: dict[date, bytes | None] = field(default_factory=dict)
    submissions_by_cik: dict[str, bytes] = field(default_factory=dict)
    fetch_master_calls: int = 0
    fetch_submissions_calls: int = 0

    def fetch_master_index(
        self,
        target_date: date,
        *,
        if_modified_since: str | None = None,
    ) -> MasterIndexFetchResult | None:
        self.fetch_master_calls += 1
        body = self.master_bodies.get(target_date)
        if body is None:
            return None
        return MasterIndexFetchResult(
            body=body,
            body_hash=f"hash-{target_date.isoformat()}",
            last_modified=f"lm-{target_date.isoformat()}",
        )

    def _fetch_submissions(self, cik: str) -> dict[str, object] | None:
        self.fetch_submissions_calls += 1
        body = self.submissions_by_cik.get(cik)
        if body is None:
            return None
        return cast(dict[str, object], json.loads(body))

    def __enter__(self) -> "StubFilingsProvider":
        return self

    def __exit__(self, *_: object) -> None:
        return None


@dataclass
class StubFundamentalsProvider:
    """Stands in for SecFundamentalsProvider."""

    facts_by_cik: dict[str, list[XbrlFact]] = field(default_factory=dict)
    fail_on: set[str] = field(default_factory=set)
    extract_calls: int = 0

    def extract_facts(self, symbol: str, cik: str) -> list[XbrlFact]:
        self.extract_calls += 1
        if cik in self.fail_on:
            raise RuntimeError(f"boom for {cik}")
        return self.facts_by_cik.get(cik, [])

    def __enter__(self) -> "StubFundamentalsProvider":
        return self

    def __exit__(self, *_: object) -> None:
        return None


def sample_fact(accession: str) -> XbrlFact:
    return XbrlFact(
        concept="Revenues",
        taxonomy="us-gaap",
        unit="USD",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        val=Decimal("90000000000"),
        frame=None,
        accession_number=accession,
        form_type="10-Q",
        filed_date=date(2026, 4, 15),
        fiscal_year=2026,
        fiscal_period="Q1",
        decimals="-6",
    )


def submissions_json(accession: str, form: str = "10-Q") -> bytes:
    return json.dumps(
        {
            "filings": {
                "recent": {
                    "accessionNumber": [accession],
                    "form": [form],
                    "acceptedDate": ["2026-04-15T16:05:00.000Z"],
                }
            }
        }
    ).encode("utf-8")
```

Refactor Tasks 4 and 5 tests to import from `tests.fixtures.sec_stubs` instead of defining their own stubs inline. Replace their local `StubProvider`, `StubFilings`, `StubFundamentals`, `_sample_fact` with the shared versions. Re-run their test suites to confirm they still pass after the refactor:

```bash
uv run pytest tests/test_sec_incremental_planner.py tests/test_sec_incremental_executor.py -v
```

Expected: all pass.

- [ ] **Step 2: Write the failing integration test**

Create `tests/test_sync_orchestrator_financial_facts_incremental.py`:

```python
"""End-to-end integration test for daily_financial_facts via the
sync-orchestrator entrypoint. Real DB, stubbed providers."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import psycopg

from app.services.sec_incremental import LOOKBACK_DAYS
from app.services.watermarks import get_watermark
from tests.fixtures.sec_stubs import (
    StubFilingsProvider,
    StubFundamentalsProvider,
    sample_fact,
    submissions_json,
)

FIXTURE_MASTER = Path("tests/fixtures/sec/master_20260415.idx")
TODAY = date(2026, 4, 15)
CIK = "0000320193"
ACCESSION = "0000320193-26-000042"


def _seed_cohort(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, is_tradable) VALUES (1, 'AAPL', TRUE)"
    )
    conn.execute(
        "INSERT INTO external_identifiers "
        "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
        "VALUES (1, 'sec', 'cik', %s, TRUE)",
        (CIK,),
    )


def _build_stubs_run_one() -> tuple[StubFilingsProvider, StubFundamentalsProvider]:
    master = FIXTURE_MASTER.read_bytes()
    filings = StubFilingsProvider(
        master_bodies={TODAY: master}
        | {TODAY - timedelta(days=i): None for i in range(1, LOOKBACK_DAYS)},
        submissions_by_cik={CIK: submissions_json(ACCESSION, form="10-Q")},
    )
    fundamentals = StubFundamentalsProvider(
        facts_by_cik={CIK: [sample_fact(ACCESSION)]},
    )
    return filings, fundamentals


def _build_stubs_run_two() -> tuple[StubFilingsProvider, StubFundamentalsProvider]:
    # All lookback days return None — master-index 304 or weekend.
    filings = StubFilingsProvider(
        master_bodies={TODAY - timedelta(days=i): None for i in range(LOOKBACK_DAYS)},
        submissions_by_cik={},
    )
    fundamentals = StubFundamentalsProvider()
    return filings, fundamentals


def test_fresh_install_seeds_cohort_then_steady_state_is_noop(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_cohort(ebull_test_conn)

    # Run 1 — fresh install, should seed.
    filings_1, fundamentals_1 = _build_stubs_run_one()
    with (
        patch("app.workers.scheduler.SecFilingsProvider", return_value=filings_1),
        patch("app.workers.scheduler.SecFundamentalsProvider", return_value=fundamentals_1),
        patch("app.workers.scheduler.datetime") as mock_dt,
        patch("app.workers.scheduler.psycopg.connect", return_value=ebull_test_conn) as _,
    ):
        mock_dt.now.return_value.date.return_value = TODAY
        from app.workers.scheduler import daily_financial_facts
        daily_financial_facts()

    submissions_wm = get_watermark(ebull_test_conn, "sec.submissions", CIK)
    assert submissions_wm is not None
    assert submissions_wm.watermark == ACCESSION

    companyfacts_wm = get_watermark(ebull_test_conn, "sec.companyfacts", CIK)
    assert companyfacts_wm is not None

    row_count = ebull_test_conn.execute(
        "SELECT COUNT(*) FROM financial_facts_raw WHERE instrument_id = 1"
    ).fetchone()
    assert row_count is not None and row_count[0] >= 1

    # Run 2 — steady state, all master-index days return 304.
    filings_2, fundamentals_2 = _build_stubs_run_two()
    with (
        patch("app.workers.scheduler.SecFilingsProvider", return_value=filings_2),
        patch("app.workers.scheduler.SecFundamentalsProvider", return_value=fundamentals_2),
        patch("app.workers.scheduler.datetime") as mock_dt,
        patch("app.workers.scheduler.psycopg.connect", return_value=ebull_test_conn),
    ):
        mock_dt.now.return_value.date.return_value = TODAY
        from app.workers.scheduler import daily_financial_facts
        daily_financial_facts()

    # Planner fetched LOOKBACK_DAYS master-index files but NOT submissions or companyfacts.
    assert filings_2.fetch_master_calls == LOOKBACK_DAYS
    assert filings_2.fetch_submissions_calls == 0
    assert fundamentals_2.extract_calls == 0
```

Note: the `patch("app.workers.scheduler.psycopg.connect", ...)` call hands the test's real `ebull_test_conn` to the scheduler so the planner + executor write to the same transactional scope the test asserts on. Confirm that pattern against the project's existing integration tests — if they use a different fixture name or connection-injection approach (e.g. a `DATABASE_URL` override), adopt that instead.

- [ ] **Step 3: Run test — expect pass**

```bash
uv run pytest tests/test_sync_orchestrator_financial_facts_incremental.py -v
```

Expected: all assertions pass.

- [ ] **Step 4: Lint + type check**

```bash
uv run ruff check tests/test_sync_orchestrator_financial_facts_incremental.py
uv run ruff format --check tests/test_sync_orchestrator_financial_facts_incremental.py
uv run pyright tests/test_sync_orchestrator_financial_facts_incremental.py
```

Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add tests/test_sync_orchestrator_financial_facts_incremental.py tests/fixtures/sec_stubs.py
git commit -m "test(#272): integration test for daily_financial_facts incremental flow

Fresh-install seeds whole cohort, second run is a noop — asserts that
fundamentals.extract_facts is never called on run 2 once all days in
the lookback window return 304."
```

---

## Task 8: Pre-push gates + PR

**Files:**
- None (runs the repo-wide checks).

- [ ] **Step 1: Run full pre-push suite**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

All four must pass. No frontend changes — skip the pnpm gates.

- [ ] **Step 2: Self-review the diff**

```bash
git log main..HEAD --oneline
git diff main..HEAD --stat
git diff main..HEAD
```

Apply the pre-flight review skill: `.claude/skills/engineering/pre-flight-review.md`. Check SQL correctness, watermark atomicity, log shape.

- [ ] **Step 3: Push branch**

```bash
git push -u origin feature/272-sec-master-index-watermark
```

- [ ] **Step 4: Open PR**

```bash
gh pr create --title "feat(#272): SEC incremental fetch — master-index + per-CIK watermark" --body "$(cat <<'EOF'
## What

Replaces the 45-min `daily_financial_facts` full-sweep with a change-driven fetch:

- 7-day SEC daily master-index lookback window with conditional-GET per day
- Per-CIK `sec.submissions` watermark (top accession) gates companyfacts fetches
- Form-type filter — only 10-K / 10-Q / 20-F / 40-F (+ amendments) trigger XBRL pulls
- Missing watermark row = self-seed; covers fresh install + new-ticker promotion

## Why

Issue #272. Steady-state wall time drops from 45 min → 5-30 sec. Daily SEC requests drop ~98%, bytes ~99%.

## Test plan

- [ ] Unit: master-index parser, conditional-GET provider method
- [ ] Unit: planner across empty cohort / fresh cohort / all-304 / hit-refresh / hit-8k-only / non-covered-hit / unchanged-accession
- [ ] Unit: executor across seed path / refresh path / per-CIK failure isolation / submissions-only path
- [ ] Integration: end-to-end through scheduler entrypoint — fresh install, then steady-state noop
- [ ] Local pre-push gates green (ruff check, ruff format, pyright, pytest)
EOF
)"
```

- [ ] **Step 5: Start the post-push poll loop**

Per `feedback_post_push_cycle.md`, immediately start polling review + CI. Do not wait for prompt.

```bash
PR_NUM=$(gh pr view --json number -q .number)
until gh pr checks "$PR_NUM" --watch; do sleep 5; done
gh pr view "$PR_NUM" --comments
```

Resolve every review comment per `.claude/CLAUDE.md` — `FIXED {sha}` / `DEFERRED #issue` / `REBUTTED {reason}`.

---

## Self-review checklist (for the author)

- Spec coverage: every scope item in the design spec is implemented by a task above.
- No placeholders: Task 7 Step 2 is the only "flesh out" — acceptable because the stub wiring depends on the executor test stubs from Task 5.
- Type consistency: `RefreshPlan`, `RefreshOutcome`, `MasterIndexEntry`, `MasterIndexFetchResult` names are stable across all tasks.
- Atomicity: every watermark write is inside a `with conn.transaction()` block alongside the data write it guards.
- Form-type gate: `FUNDAMENTALS_FORMS` declared once, referenced from planner only — no drift risk.
- Per-CIK failure isolation: every CIK loop body is wrapped in its own `try` + own transaction.
- SQL correctness: positional-access on cursor rows; `= ANY(%s)` for list-valued params (psycopg3 correctness).
- Settled decisions: SEC identifier = CIK (zero-padded); provider stays thin HTTP; `as_of_date` semantics untouched.
